package sessionmanager_test

import (
	"context"
	"errors"
	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type retirementNativeFactory struct {
	mu    sync.Mutex
	root  string
	hosts map[string]*accountingIntegrationNative
}

func (f *retirementNativeFactory) Provider() string { return "muse" }
func (f *retirementNativeFactory) Launch(_ context.Context, q core.ProviderLaunchRequest) (core.RunnerHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.hosts[q.SessionID]
	if n == nil {
		identity := "native-" + q.SessionID
		n = &accountingIntegrationNative{root: f.root, nativeID: identity, foldID: identity, total: 9, sequence: 1}
		f.hosts[q.SessionID] = n
	}
	return n, nil
}
func (f *retirementNativeFactory) Restore(_ context.Context, q core.ProviderHostRecord) (core.RunnerHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.hosts {
		if q.Snapshot.MuseMSP != nil && q.Snapshot.MuseMSP.SessionID == n.NativeIdentity() {
			return n, nil
		}
	}
	return nil, core.ErrProviderHostNotFound
}

// Actual core -> selected factory/runtime -> real worker cancel/join preserves
// its full account. Stopping one of two distinct physical fixture sessions
// leaves the other input/account worker active; Close joins before feed cancel
// and a fresh real host/runtime restores the still-running session. Only native
// I/O is controlled, no model/provider process or caller registration is claimed.
func TestNativeObserverRetirementRealUnrelatedSessionAndClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	service, e := goal.Open(filepath.Join(root, "goals"), accountingIntegrationAuthority{})
	if e != nil {
		t.Fatal(e)
	}
	native := &retirementNativeFactory{root: root, hosts: map[string]*accountingIntegrationNative{}}
	system := accountingOpenSystem(t, ctx, root, service, native)
	var ids []string
	var leases []core.ControllerLease
	for _, owner := range []core.OwnerKey{"parent/primary", "run/RUN-UNRELATED"} {
		a, e := system.host.Attach(ctx, core.AttachRequest{Owner: owner, Provider: "muse"})
		if e != nil {
			t.Fatal(e)
		}
		ids = append(ids, a.Session.SessionID)
		l, _, e := system.host.Input(ctx, a.Session.SessionID)
		if e != nil {
			t.Fatal(e)
		}
		leases = append(leases, l)
	}
	before, e := system.store.LegacySnapshot(ctx, leases[0])
	if e != nil {
		t.Fatal(e)
	}
	if _, e = system.host.Stop(ctx, ids[0]); e != nil {
		t.Fatal(e)
	}
	after, e := system.store.LegacySnapshot(ctx, leases[0])
	if e != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("intentional retirement changed full retained account: %+v %+v %v", before, after, e)
	}
	native.hosts[ids[1]].setTotal(25)
	if e = system.factory.CheckBudget(ctx, leases[1]); e != nil {
		t.Fatalf("unrelated actual observer retired: %v", e)
	}
	other, e := system.store.LegacySnapshot(ctx, leases[1])
	if e != nil || other.TokenUsageTotal != 25 {
		t.Fatalf("unrelated actual account stopped progressing: %+v %v", other, e)
	}
	native.hosts[ids[0]].mu.Lock()
	firstStops := native.hosts[ids[0]].stops
	native.hosts[ids[0]].mu.Unlock()
	native.hosts[ids[1]].mu.Lock()
	otherStops := native.hosts[ids[1]].stops
	native.hosts[ids[1]].mu.Unlock()
	if firstStops != 1 || otherStops != 0 {
		t.Fatal("one Stop affected another native session")
	}
	system.host.Close()
	retained, e := system.store.LegacySnapshot(ctx, leases[1])
	if e != nil || !reflect.DeepEqual(other, retained) {
		t.Fatalf("Close tainted/reset retained accounting: %+v %v", retained, e)
	}
	if e = system.factory.Close(ctx); e != nil {
		t.Fatal(e)
	}
	restored := accountingOpenSystem(t, ctx, root, service, native)
	if e = restored.host.Recover(ctx); e != nil {
		t.Fatalf("joined Close prevented healthy restart: %v", e)
	}
	current, input, e := restored.host.Input(ctx, ids[1])
	if e != nil || input == nil || current.Epoch <= leases[1].Epoch {
		t.Fatalf("healthy unrelated restart unavailable: %+v %v", current, e)
	}
}

// A real unexpected transport EOF while the controller is still current must
// taint its real account. Later intentional observer retirement/Stop preserves
// that refusal, without clearing counters/ledger or calling it a healthy resume.
func TestNativeObserverUnknownLossRetainsUncertainty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := t.TempDir()
	service, e := goal.Open(filepath.Join(root, "goals"), accountingIntegrationAuthority{})
	if e != nil {
		t.Fatal(e)
	}
	native := &accountingIntegrationNative{root: root, foldID: "native-fixture", total: 9, sequence: 1}
	system := accountingOpenSystem(t, ctx, root, service, native)
	a, e := system.host.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if e != nil {
		t.Fatal(e)
	}
	lease, _, e := system.host.Input(ctx, a.Session.SessionID)
	if e != nil {
		t.Fatal(e)
	}
	native.mu.Lock()
	feeds := append([]*accountingIntegrationFeed(nil), native.feeds...)
	native.mu.Unlock()
	for _, feed := range feeds {
		feed.Close()
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		_, e = system.store.LegacySnapshot(ctx, lease)
		if errors.Is(e, budget.ErrAccountingUncertain) {
			break
		}
		if e != nil {
			t.Fatalf("unexpected loss failed for another reason: %v", e)
		}
		select {
		case <-deadline.C:
			t.Fatal("unexpected live-lease EOF was not marked uncertain")
		case <-poll.C:
		}
	}
	if _, e = system.host.Stop(ctx, a.Session.SessionID); e != nil {
		t.Fatal(e)
	}
	if _, e = system.store.LegacySnapshot(ctx, lease); !errors.Is(e, budget.ErrAccountingUncertain) {
		t.Fatalf("intentional retirement healed prior unknown loss: %v", e)
	}
}
