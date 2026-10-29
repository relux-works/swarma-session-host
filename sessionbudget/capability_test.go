package sessionbudget

import (
	"context"
	"errors"
	"testing"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// The real host wrapper retains observation methods even for a backend whose
// existing native-goal capability is false. This typed fixture mirrors that
// method-set distinction; it does not qualify core wrapper forwarding.
type nonNativeGoalWriter struct{ *nativeFixture }

func (nonNativeGoalWriter) SupportsNativeGoals() bool { return false }

// The actual service's absent-head reconciliation needs no delivery callback,
// native observation or accounting allocation. Active/pending heads still fail.
func TestNonNativeGoalBindingUsesRealAbsentHeadReconciliation(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	writer := nonNativeGoalWriter{h.native}
	owner, b, e := h.factory.BindGoal(ctx, h.lease, "muse", writer)
	if e != nil {
		t.Fatal(e)
	}
	activation, e := h.s.Reconcile(owner, b)
	if e != nil || activation == nil || activation.Pending || activation.Ref != nil {
		t.Fatalf("real absent-head reconciliation refused: %+v %v", activation, e)
	}
	if h.account(t) != nil || h.native.writes != 0 {
		t.Fatal("absent head manufactured native accounting or delivery")
	}
	_, e = h.s.Handle(ctx, goal.Command{EnvelopeID: "pending", SessionID: h.lease.SessionID, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "work", Condition: "holder accepts", Budget: goal.Budget{Tokens: 50}}})
	if e != nil {
		t.Fatal(e)
	}
	if _, binding, e := h.factory.BindGoal(ctx, h.lease, "muse", writer); binding != nil || !errors.Is(e, core.ErrHostCapabilityUnavailable) {
		t.Fatalf("active/pending head entered native binding: %v", e)
	}
	if _, e = h.s.Reconcile(owner, b); !errors.Is(e, core.ErrHostCapabilityUnavailable) {
		t.Fatalf("retained non-native binding fabricated delivery: %v", e)
	}
	head, e := h.s.Snapshot(h.lease.SessionID, goal.Assignment)
	if e != nil {
		t.Fatal(e)
	}
	if head.Applied != nil || head.AppliedReceipt != nil || h.native.writes != 0 || h.account(t) != nil {
		t.Fatal("unsupported active goal promoted, wrote, or allocated an account")
	}
}
