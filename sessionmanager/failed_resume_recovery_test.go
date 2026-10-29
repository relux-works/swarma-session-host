package sessionmanager_test

import (
	"context"
	"errors"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Actual stopped resume with one real subscription failure retains native
// custody and denies public input until retry or Recover restores accounting
// and receipt-checked goal binding. Control has no injected failure. Native
// I/O only is controlled; no provider process or external authority is proved.
func TestFailedResumeSubscriptionRecovery(t *testing.T) {
	for _, mode := range []string{"valid", "retry", "recover", "reopen"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			root := t.TempDir()
			service, e := goal.Open(filepath.Join(root, "goals"), accountingIntegrationAuthority{})
			if e != nil {
				t.Fatal(e)
			}
			native := &rev4HookNative{accountingIntegrationNative: &accountingIntegrationNative{root: root, foldID: "native-fixture", total: 9, sequence: 1}}
			system := accountingOpenSystem(t, ctx, root, service, native)
			att, e := system.host.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "muse"})
			if e != nil {
				t.Fatal(e)
			}
			sid := att.Session.SessionID
			lease, writer, e := system.host.Input(ctx, sid)
			if e != nil {
				t.Fatal(e)
			}
			_, binding, e := system.factory.BindGoal(ctx, lease, "muse", writer)
			if e != nil {
				t.Fatal(e)
			}
			if r, e := service.Execute(ctx, goal.Command{EnvelopeID: "admitted", SessionID: sid, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "work", Condition: "result", Budget: goal.Budget{Tokens: 100}}}, binding); e != nil || r.Pending {
				t.Fatal(r, e)
			}
			if _, live, e := system.host.ReconcileOwner(ctx, "parent/primary"); e != nil || !live {
				t.Fatal(e)
			}
			// Retain a real two-revision same-objective budget history and pause,
			// rather than proving only the zero/default accounting fields.
			native.setTotal(30)
			if r, e := service.Execute(ctx, goal.Command{EnvelopeID: "second-revision", SessionID: sid, Namespace: goal.Assignment, Action: goal.Set, ExpectedRevision: 1, Content: &goal.Content{Objective: "work", Condition: "second result", Budget: goal.Budget{Tokens: 100}}}, binding); e != nil || r.Pending {
				t.Fatal(r, e)
			}
			if _, live, e := system.host.ReconcileOwner(ctx, "parent/primary"); e != nil || !live {
				t.Fatal(e)
			}
			native.setTotal(160)
			if e = system.factory.CheckBudget(ctx, lease); e != nil {
				t.Fatal(e)
			}
			before, e := system.store.LegacySnapshot(ctx, lease)
			if e != nil {
				t.Fatal(e)
			}
			if before.TokenUsageBaseline != 9 || before.TokenUsageTotal != 160 || !before.BudgetPaused || len(before.GoalLedger) < 2 {
				t.Fatalf("nontrivial accounting fixture not admitted: %+v", before)
			}
			if _, e = system.host.Stop(ctx, sid); e != nil {
				t.Fatal(e)
			}
			if mode != "valid" {
				native.mu.Lock()
				native.failSub = 1
				native.mu.Unlock()
			}
			q := core.AttachRequest{Owner: "parent/primary", Provider: "muse", ResumeSessionID: sid}
			att, e = system.host.Attach(ctx, q)
			if mode == "valid" {
				if e != nil || att == nil {
					t.Fatal("valid stopped resume refused", e)
				}
				return
			}
			if e == nil || att != nil || !strings.Contains(e.Error(), "rev4 injected subscribe failure") {
				t.Fatalf("injected subscription failure missing: %v %+v", e, att)
			}
			if _, w, e := system.host.Input(ctx, sid); e == nil || w != nil {
				t.Fatalf("failed resume exposed input: %v", e)
			}
			native.accountingIntegrationNative.mu.Lock()
			launches, stops := native.launches, native.stops
			native.accountingIntegrationNative.mu.Unlock()
			writes := native.writeCount()
			var err error
			switch mode {
			case "retry":
				att, err = system.host.Attach(ctx, q)
			case "recover":
				err = system.host.Recover(ctx)
			case "reopen":
				system.host.Close()
				if e := system.factory.Close(ctx); e != nil {
					t.Fatal(e)
				}
				system = accountingOpenSystem(t, ctx, root, service, native)
				err = system.host.Recover(ctx)
			}
			if err != nil {
				t.Fatalf("failed resume %s did not recover actual retained native: capability=%v error=%v", mode, errors.Is(err, core.ErrHostCapabilityUnavailable), err)
			}
			current, input, e := system.host.Input(ctx, sid)
			if e != nil || input == nil || current.Epoch <= lease.Epoch {
				t.Fatalf("recovered input not current: %+v %v", current, e)
			}
			native.accountingIntegrationNative.mu.Lock()
			afterLaunches, afterStops := native.launches, native.stops
			native.accountingIntegrationNative.mu.Unlock()
			if afterLaunches != launches || afterStops != stops {
				t.Fatalf("recovery duplicated native launch/stop: %d/%d -> %d/%d", launches, stops, afterLaunches, afterStops)
			}
			if native.writeCount() != writes+2 {
				t.Fatalf("recovery must only rebind and re-pause with current receipts: writes%d->%d", writes, native.writeCount())
			}
			after, e := system.store.LegacySnapshot(ctx, current)
			if e != nil || !reflect.DeepEqual(before.LegacyMuseNativeAccounting, after.LegacyMuseNativeAccounting) {
				t.Fatalf("recovery reset full accounting: before%+v after%+v error%v", before, after, e)
			}
			head, e := service.Snapshot(sid, goal.Assignment)
			if e != nil || head.Attempt != nil || head.Rollback != nil || head.Acknowledged != nil || head.AppliedReceipt == nil || head.AppliedReceipt.Epoch != current.Epoch || head.Applied == nil || *head.Applied != head.Latest.Ref {
				t.Fatalf("recovery lacks actual settled current goal receipt: %+v %v", head, e)
			}
			view, e := system.host.Session(ctx, sid)
			if e != nil || view.State != core.SessionRunning || view.OwnerRunError != "" {
				t.Fatalf("recovered record not acknowledged Running: %+v %v", view, e)
			}
		})
	}
}
