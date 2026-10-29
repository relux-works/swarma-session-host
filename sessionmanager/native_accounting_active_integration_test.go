package sessionmanager_test

import (
	"context"
	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	run "github.com/relux-works/swarma-session-host/sessionrun"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type accountingIntegrationSystem struct {
	host    *core.SessionHost
	factory *budget.Factory
	store   *budget.Store
	runtime *run.Runtime
}

func accountingOpenSystem(t *testing.T, ctx context.Context, root string, service *goal.Service, native core.ProviderHostFactory) *accountingIntegrationSystem {
	t.Helper()
	store, e := budget.Open(filepath.Join(root, "budget"))
	if e != nil {
		t.Fatal(e)
	}
	runtime := run.New(store, core.HostSettings{GoalAckTimeout: time.Second})
	factory, e := budget.NewFactory(ctx, service, store, runtime.AccountingSource())
	if e != nil {
		t.Fatal(e)
	}
	if e = runtime.BindBudget(factory); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = factory.Close(context.Background()) })
	codec, e := core.NewLegacyRecordCodecWithMuseAccounting(runtime)
	if e != nil {
		t.Fatal(e)
	}
	hostRoot := filepath.Join(root, "host")
	layout := core.Layout{Namespace: "fixture", StateDir: hostRoot, SessionsDir: filepath.Join(hostRoot, "sessions"), LockFile: filepath.Join(hostRoot, "manager.lock"), NoticesDir: filepath.Join(root, "notices")}
	notices, e := core.NewSessionNoticeStore(layout)
	if e != nil {
		t.Fatal(e)
	}
	host, e := core.NewHost(core.HostOptions{Layout: layout, Config: accountingIntegrationConfig{}, Codec: codec, Goals: service, GoalBindings: factory, OwnerRuns: runtime, Notices: notices, Factories: []core.ProviderHostFactory{native}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(host.Close)
	if e = runtime.BindHost(host); e != nil {
		t.Fatal(e)
	}
	return &accountingIntegrationSystem{host, factory, store, runtime}
}

// A real admitted two-revision goal history and matched threshold pause survive
// actual Host.Close/NewHost/Recover through the real renderer, store, factory,
// full codec and sole writer. Same-goal revisions retain the original9-token
// baseline. A matched pause has no refusal outcome (the actual budget check
// clears PauseCode); full state equality below retains that distinction.
// The real goal service requires a fresh receipt for the new controller and
// rebinds the goal, then the real budget guard pauses it again; assert those
// two acknowledged effects and the new exact epoch, rather than forbidding
// that existing recovery contract.
// Only the native I/O boundary is controlled; this
// does not qualify a real provider process or the external caller registrar.
func TestNativeAccountingRealCompositionRestoresLedgerBaselineAndPause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	service, e := goal.Open(filepath.Join(root, "goals"), accountingIntegrationAuthority{})
	if e != nil {
		t.Fatal(e)
	}
	native := &accountingIntegrationNative{root: root, foldID: "native-fixture", total: 9, sequence: 1}
	system := accountingOpenSystem(t, ctx, root, service, native)
	attachment, e := system.host.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if e != nil {
		t.Fatal(e)
	}
	sid := attachment.Session.SessionID
	lease, writer, e := system.host.Input(ctx, sid)
	if e != nil {
		t.Fatal(e)
	}
	_, binding, e := system.factory.BindGoal(ctx, lease, "muse", writer)
	if e != nil {
		t.Fatal(e)
	}
	for revision, condition := range []string{"first accepted condition", "retained accepted condition"} {
		receipt, e := service.Execute(ctx, goal.Command{EnvelopeID: condition, SessionID: sid, Namespace: goal.Assignment, Action: goal.Set, ExpectedRevision: revision, Content: &goal.Content{Objective: "retained work", Condition: condition, Budget: goal.Budget{Tokens: 100}}}, binding)
		if e != nil || receipt.Pending {
			t.Fatalf("actual admitted revision%d failed: %+v %v", revision, receipt, e)
		}
		if _, live, e := system.host.ReconcileOwner(ctx, "parent/primary"); e != nil || !live {
			t.Fatalf("physical metadata did not ACK admitted head: %v", e)
		}
		if revision == 0 {
			native.setTotal(30)
		} else {
			native.setTotal(160)
		}
		if e = system.factory.CheckBudget(ctx, lease); e != nil {
			t.Fatalf("actual native accounting check: %v", e)
		}
	}
	before, e := system.store.LegacySnapshot(ctx, lease)
	if e != nil || before.TokenUsageTotal != 160 || before.TokenUsageBaseline != 9 || !before.BudgetPaused || before.BudgetPauseOutcome != nil || len(before.GoalLedger) < 2 {
		t.Fatalf("real admitted pause/history missing: %+v %v", before, e)
	}
	if _, e = system.host.Session(ctx, sid); e != nil {
		t.Fatal(e)
	}
	system.host.Close()
	if e = system.factory.Close(ctx); e != nil {
		t.Fatal(e)
	}
	writes := native.writeCount()
	restored := accountingOpenSystem(t, ctx, root, service, native)
	if e = restored.host.Recover(ctx); e != nil {
		t.Fatalf("actual active accounting Restore failed: %v", e)
	}
	current, _, e := restored.host.Input(ctx, sid)
	if e != nil || current.Epoch <= lease.Epoch {
		t.Fatalf("restored controller not fresh: %+v %v", current, e)
	}
	head, headErr := service.Snapshot(sid, goal.Assignment)
	if headErr != nil || head.AppliedReceipt == nil || head.AppliedReceipt.Epoch != current.Epoch || head.Attempt != nil || head.Rollback != nil {
		t.Fatalf("new controller lacks its own settled goal receipt: %+v %v", head, headErr)
	}
	after, e := restored.store.LegacySnapshot(ctx, current)
	if e != nil || !reflect.DeepEqual(before.LegacyMuseNativeAccounting, after.LegacyMuseNativeAccounting) || native.writeCount() != writes+2 {
		t.Fatalf("Restore reset baseline/ledger/pause or violated exact receipt rebind effects: before%+v after%+v %v writes%d->%d", before, after, e, writes, native.writeCount())
	}
}
