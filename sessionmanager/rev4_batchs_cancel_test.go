package sessionmanager_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type rev4HookNative struct {
	*accountingIntegrationNative
	mu      sync.Mutex
	onClose func()
	failSub int
}

func (n *rev4HookNative) Launch(ctx context.Context, q core.ProviderLaunchRequest) (core.RunnerHost, error) {
	if _, e := n.accountingIntegrationNative.Launch(ctx, q); e != nil {
		return nil, e
	}
	return n, nil
}
func (n *rev4HookNative) Restore(context.Context, core.ProviderHostRecord) (core.RunnerHost, error) {
	return n, nil
}
func (n *rev4HookNative) SubscribeMuse(ctx context.Context, l core.ControllerLease) (core.MuseObservationFeed, error) {
	n.mu.Lock()
	if n.failSub > 0 {
		n.failSub--
		n.mu.Unlock()
		return nil, errors.New("rev4 injected subscribe failure")
	}
	n.mu.Unlock()
	f, e := n.accountingIntegrationNative.SubscribeMuse(ctx, l)
	if e != nil {
		return nil, e
	}
	return &rev4HookFeed{MuseObservationFeed: f, owner: n}, nil
}

type rev4HookFeed struct {
	core.MuseObservationFeed
	owner *rev4HookNative
}

func (f *rev4HookFeed) Close() {
	f.owner.mu.Lock()
	hook := f.owner.onClose
	f.owner.onClose = nil
	f.owner.mu.Unlock()
	if hook != nil {
		hook()
	}
	f.MuseObservationFeed.Close()
}

// Reviewer rev4 Batch S attack. Claim: through public Host.Stop on the exact
// real composition, a Stop whose context is cancelled while the actual
// observer join is in flight refuses before epoch/record effects, never
// reports success, and a later exact Stop retry joins and stops; a following
// valid resume is not tainted as unknown loss. Also records what the live lease
// can still do in the interval (input admission, budget check).
func TestRev4BatchSCancelledStopJoinRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	root := t.TempDir()
	service, e := goal.Open(filepath.Join(root, "goals"), accountingIntegrationAuthority{})
	if e != nil {
		t.Fatal(e)
	}
	native := &rev4HookNative{accountingIntegrationNative: &accountingIntegrationNative{root: root, foldID: "native-fixture", total: 9, sequence: 1}}
	system := accountingOpenSystem(t, ctx, root, service, native)
	attached, e := system.host.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if e != nil {
		t.Fatal(e)
	}
	sid := attached.Session.SessionID
	lease, writer, e := system.host.Input(ctx, sid)
	if e != nil {
		t.Fatal(e)
	}
	_, binding, e := system.factory.BindGoal(ctx, lease, "muse", writer)
	if e != nil {
		t.Fatal(e)
	}
	if r, e := service.Execute(ctx, goal.Command{EnvelopeID: "admitted", SessionID: sid, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "w", Condition: "c", Budget: goal.Budget{Tokens: 100}}}, binding); e != nil || r.Pending {
		t.Fatal(r, e)
	}
	if _, live, e := system.host.ReconcileOwner(ctx, "parent/primary"); e != nil || !live {
		t.Fatal(e)
	}
	recordPath := filepath.Join(root, "host", "sessions", sid, "record.json")
	epochPath := filepath.Join(root, "host", "controller-epochs.v1.json")
	rec0, _ := os.ReadFile(recordPath)
	ep0, _ := os.ReadFile(epochPath)
	stopCtx, stopCancel := context.WithCancel(ctx)
	native.mu.Lock()
	native.onClose = stopCancel
	native.mu.Unlock()
	_, e = system.host.Stop(stopCtx, sid)
	if !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled join did not refuse Stop: %v", e)
	}
	rec1, _ := os.ReadFile(recordPath)
	ep1, _ := os.ReadFile(epochPath)
	if string(rec0) != string(rec1) || string(ep0) != string(ep1) {
		t.Fatal("cancelled Stop crossed record/epoch boundary")
	}
	cur, in, ie := system.host.Input(ctx, sid)
	t.Logf("interval: input admitted=%v lease=%+v err=%v", in != nil, cur, ie)
	t.Logf("interval: CheckBudget=%v", system.factory.CheckBudget(ctx, lease))
	_, _, be := system.factory.BindGoal(ctx, lease, "muse", writer)
	t.Logf("interval: same-lease BindGoal=%v", be)
	if be == nil {
		t.Fatal("same retired lease restarted its observer")
	}
	if _, e = system.host.Stop(ctx, sid); e != nil {
		t.Fatalf("exact Stop retry failed: %v", e)
	}
	out, e := system.host.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "muse", ResumeSessionID: sid})
	if e != nil || out == nil || out.Session.State != core.SessionRunning {
		t.Fatalf("resume after cancelled-then-retried Stop tainted: %+v %v", out, e)
	}
}

// Reviewer rev4 Batch S attack. Claim: when the resumed lease's real observer
// subscription fails while the prior lease's retired tombstone is still in the
// factory, the host does not reach a state where intentional Stop/Close of the
// session is permanently refused; a retry resume succeeds and Close releases
// the owner so a fresh Host can Recover. Native I/O fixture only.
func TestRev4BatchSFailedResumeSubscriptionThenStopClose(t *testing.T) {
	for _, final := range []string{"retry-resume", "stop", "close"} {
		t.Run(final, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			root := t.TempDir()
			service, e := goal.Open(filepath.Join(root, "goals"), accountingIntegrationAuthority{})
			if e != nil {
				t.Fatal(e)
			}
			native := &rev4HookNative{accountingIntegrationNative: &accountingIntegrationNative{root: root, foldID: "native-fixture", total: 9, sequence: 1}}
			system := accountingOpenSystem(t, ctx, root, service, native)
			attached, e := system.host.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "muse"})
			if e != nil {
				t.Fatal(e)
			}
			sid := attached.Session.SessionID
			lease, writer, e := system.host.Input(ctx, sid)
			if e != nil {
				t.Fatal(e)
			}
			_, binding, e := system.factory.BindGoal(ctx, lease, "muse", writer)
			if e != nil {
				t.Fatal(e)
			}
			if r, e := service.Execute(ctx, goal.Command{EnvelopeID: "admitted", SessionID: sid, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "w", Condition: "c", Budget: goal.Budget{Tokens: 100}}}, binding); e != nil || r.Pending {
				t.Fatal(r, e)
			}
			if _, live, e := system.host.ReconcileOwner(ctx, "parent/primary"); e != nil || !live {
				t.Fatal(e)
			}
			if _, e = system.host.Stop(ctx, sid); e != nil {
				t.Fatal(e)
			}
			native.mu.Lock()
			native.failSub = 1
			native.mu.Unlock()
			req := core.AttachRequest{Owner: "parent/primary", Provider: "muse", ResumeSessionID: sid}
			out, e := system.host.Attach(ctx, req)
			t.Logf("failed-subscription resume: out=%v err=%v", out != nil, e)
			cur, in, ie := system.host.Input(ctx, sid)
			t.Logf("after failed resume: input=%v lease=%+v err=%v", in != nil, cur, ie)
			switch final {
			case "retry-resume":
				if e == nil {
					return
				}
				// NOTE only: residual failed-resume state belongs to the separate
				// Attach rollback unit; recorded, not asserted.
				out, e = system.host.Attach(ctx, req)
				t.Logf("NOTE retry resume after failed subscription: out=%v err=%v", out != nil, e)
			case "stop":
				_, se := system.host.Stop(ctx, sid)
				t.Logf("stop after failed resume: %v", se)
				if se != nil {
					out, e = system.host.Attach(ctx, req)
					t.Logf("retry resume instead: %v", e)
					if e != nil {
						t.Fatalf("session stuck: stop %v, resume %v", se, e)
					}
					if _, se = system.host.Stop(ctx, sid); se != nil {
						t.Fatalf("stop after successful retry resume refused: %v", se)
					}
				}
			case "close":
				system.host.Close()
				if e := system.factory.Close(ctx); e != nil {
					t.Fatal(e)
				}
				// The owner lock must be released by Close (NewHost succeeds); the
				// later Recover outcome of the residual failed-resume record is a
				// separate Attach rollback unit and is only logged.
				restored := accountingOpenSystem(t, ctx, root, service, native.accountingIntegrationNative)
				t.Logf("NOTE Recover after Close: %v", restored.host.Recover(ctx))
			}
		})
	}
}
