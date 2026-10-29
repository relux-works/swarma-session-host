package sessionmanager_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Actual codec/runtime/goal/budget journal supports empty and admitted-active
// stopped resume, ordinary and staged. A changed real assignment head refuses
// specifically before Launch/epoch/record/client effects. Only native I/O is
// controlled; this is not provider-process or external-caller qualification.
func TestNativeAccountingRealStoppedResumePreflight(t *testing.T) {
	for _, staged := range []bool{false, true} {
		for _, mode := range []string{"empty-valid", "active-valid", "changed-goal"} {
			t.Run(map[bool]string{false: "ordinary", true: "staged"}[staged]+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				root := t.TempDir()
				service, e := goal.Open(filepath.Join(root, "goals"), accountingIntegrationAuthority{})
				if e != nil {
					t.Fatal(e)
				}
				native := &accountingIntegrationNative{root: root, foldID: "native-fixture", total: 9, sequence: 1}
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
				if mode != "empty-valid" {
					_, binding, e := system.factory.BindGoal(ctx, lease, "muse", writer)
					if e != nil {
						t.Fatal(e)
					}
					receipt, e := service.Execute(ctx, goal.Command{EnvelopeID: "admitted", SessionID: sid, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "retained work", Condition: "retained result", Budget: goal.Budget{Tokens: 100}}}, binding)
					if e != nil || receipt.Pending {
						t.Fatalf("valid admitted fixture failed: %+v %v", receipt, e)
					}
					if _, live, e := system.host.ReconcileOwner(ctx, "parent/primary"); e != nil || !live {
						t.Fatal("actual admitted ref not committed", e)
					}
				}
				if _, e = system.host.Stop(ctx, sid); e != nil {
					t.Fatal(e)
				}
				recordPath := filepath.Join(root, "host", "sessions", sid, "record.json")
				epochPath := filepath.Join(root, "host", "controller-epochs.v1.json")
				before, e := os.ReadFile(recordPath)
				if e != nil {
					t.Fatal(e)
				}
				epoch, e := os.ReadFile(epochPath)
				if e != nil {
					t.Fatal(e)
				}
				if mode == "changed-goal" {
					receipt, e := service.Handle(ctx, goal.Command{EnvelopeID: "new-admitted-head", SessionID: sid, Namespace: goal.Assignment, Action: goal.Set, ExpectedRevision: 1, Content: &goal.Content{Objective: "changed work", Condition: "changed result", Budget: goal.Budget{Tokens: 100}}})
					if e != nil || !receipt.Pending {
						t.Fatalf("actual changed journal fixture failed: %+v %v", receipt, e)
					}
				}
				native.mu.Lock()
				launches := native.launches
				native.mu.Unlock()
				writes := native.writeCount()
				request := core.AttachRequest{Owner: "parent/primary", Provider: "muse", ResumeSessionID: sid}
				var out *core.Attachment
				if staged {
					var prepared core.AttachmentPreparation
					prepared, e = system.host.PrepareAttach(ctx, request)
					if e == nil {
						out, e = system.host.CommitAttach(ctx, core.AttachmentCommit{Token: prepared.Token, Lease: prepared.Lease, BaseSHA256: prepared.BaseSHA256})
					}
				} else {
					out, e = system.host.Attach(ctx, request)
				}
				native.mu.Lock()
				actualLaunches := native.launches
				native.mu.Unlock()
				if mode == "changed-goal" {
					after, readErr := os.ReadFile(recordPath)
					actualEpoch, epochErr := os.ReadFile(epochPath)
					if !errors.Is(e, budget.ErrAccountingUncertain) || out != nil || actualLaunches != launches || native.writeCount() != writes || readErr != nil || epochErr != nil || string(before) != string(after) || string(epoch) != string(actualEpoch) {
						t.Fatalf("invalid stopped source crossed preflight: %v launches%d->%d writes%d->%d recordEqual%v epochEqual%v", e, launches, actualLaunches, writes, native.writeCount(), string(before) == string(after), string(epoch) == string(actualEpoch))
					}
					if _, writer, e := system.host.Input(ctx, sid); e == nil || writer != nil {
						t.Fatal("refused stopped resume exposed input")
					}
					return
				}
				if e != nil || out == nil || out.Session.SessionID != sid || out.Session.State != core.SessionRunning || actualLaunches != launches+1 {
					_, sinkErr := system.store.LegacySnapshot(ctx, lease)
					t.Logf("retained old-lease accounting projection after failed resume: %v", sinkErr)
					t.Fatalf("valid stopped source failed resume: %+v %v launches%d->%d", out, e, launches, actualLaunches)
				}
				current, input, e := system.host.Input(ctx, sid)
				if e != nil || input == nil || current.Epoch <= lease.Epoch {
					t.Fatalf("valid resumed controller not admitted: %+v %v", current, e)
				}
			})
		}
	}
}
