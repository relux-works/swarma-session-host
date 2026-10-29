package sessiongoal

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"reflect"
	"strings"
	"testing"
)

// The real core disable-plane call clears native state without accepting or
// deleting the stored head. Re-enable in the SAME controller must get a fresh
// input/command identity, so an old apply receipt cannot acknowledge restoration.
func TestGoalPlaneDisableRetainsHeadAndRestoresWithFreshReceipt(t *testing.T) {
	s, _, _ := fixture(t)
	w := newWriter()
	b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	r, e := s.Execute(caller("launcher"), command("set", 0), b)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Rollback(&core.GoalEnvelope{Owner: Owner("session-a", Assignment), Clear: true}); e != nil {
		t.Fatal(e)
	}
	h, e := s.Snapshot("session-a", Assignment)
	if e != nil || h.Latest.Ref != r.Ref || h.Latest.State != Active || h.Latest.Acceptance != nil || h.Applied != nil || h.Rollback != nil {
		t.Fatalf("disabled plane corrupted or stranded head: %+v %v", h, e)
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); e != nil {
		t.Fatal(e)
	}
	h, e = s.Snapshot("session-a", Assignment)
	if e != nil || h.Applied == nil || *h.Applied != r.Ref || w.submits != 3 || w.inputs[0].ID == w.inputs[2].ID || w.inputs[0].Payload.CodexTurn.CommandID == w.inputs[2].Payload.CodexTurn.CommandID {
		t.Fatalf("restoration borrowed old input identity/receipt: %+v %v submits=%d", h, e, w.submits)
	}
}

// A Muse retry reuses the exact persisted command, including set/edit choice,
// even if the new native observation now reports the requested objective.
// Its stable input ID must never hide a changed payload behind an old receipt.
func TestGoalMuseRetryRetainsExactSubmittedPayload(t *testing.T) {
	s, _, _ := fixture(t)
	w := &nativeWriterFixture{writerFixture: newWriter(), native: core.NativeGoalObservation{SessionID: "session-a", Epoch: 1, Objective: "previous", Status: "active"}}
	w.awaitErr = errors.New("ack unavailable")
	b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "muse", w, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	r, e := s.Execute(caller("launcher"), command("set", 0), b)
	if !errors.Is(e, w.awaitErr) || len(w.inputs) != 1 || w.inputs[0].Payload.MuseGoal.Edit == nil {
		t.Fatalf("missing initial edit/error: %v", e)
	}
	w.native.Objective = RevisionToken(r.Ref) + " " + command("unused", 0).Content.Condition
	w.awaitErr = nil
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); e != nil {
		t.Fatal(e)
	}
	if len(w.inputs) != 2 || !reflect.DeepEqual(w.inputs[0], w.inputs[1]) {
		t.Fatal("same-attempt retry changed submitted Muse payload or identity")
	}
}

// Known rollback acknowledgement after an apply's budget-commit error retires
// the old effect, while retaining the original cause. Reopen can restore the
// pending head with a fresh identity; the paired failed-rollback root refuses.
func TestGoalKnownPostAcknowledgementRollbackRecovers(t *testing.T) {
	s, a, root := fixture(t)
	w := newWriter()
	cause := errors.New("budget commit after native acknowledgement")
	budget := &postAcknowledgementBudget{err: cause}
	b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, budget)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Execute(caller("launcher"), command("set", 0), b); !errors.Is(e, cause) {
		t.Fatalf("post-ack cause lost: %v", e)
	}
	h, e := s.Snapshot("session-a", Assignment)
	if e != nil || h.Applied != nil || h.Attempt != nil || h.Rollback != nil || len(h.Rollbacks) != 1 {
		t.Fatalf("known rollback not retired with receipt: %+v %v", h, e)
	}
	s, e = Open(root, a)
	if e != nil {
		t.Fatal(e)
	}
	b, e = s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); e != nil {
		t.Fatal(e)
	}
	if w.submits != 3 || w.inputs[0].ID == w.inputs[2].ID {
		t.Fatal("recovery borrowed pre-rollback apply identity")
	}
}

// A failed newer assignment restores the actual previously applied objective,
// retaining that exact rollback receipt, then permits a fresh apply of the
// still-pending newer head. It must never mark the failed revision applied.
func TestGoalRollbackRestoresPriorAppliedRevision(t *testing.T) {
	s, _, _ := fixture(t)
	w := newWriter()
	lease := core.ControllerLease{SessionID: "session-a", Epoch: 1}
	b, e := s.NewBinding(context.Background(), lease, Assignment, "codex", w, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	first, e := s.Execute(caller("launcher"), command("first", 0), b)
	if e != nil {
		t.Fatal(e)
	}
	cause := errors.New("new budget commit failed")
	b, e = s.NewBinding(context.Background(), lease, Assignment, "codex", w, &postAcknowledgementBudget{err: cause})
	if e != nil {
		t.Fatal(e)
	}
	next := command("second", 1)
	next.Content.Objective = "different newer objective"
	if _, e = s.Execute(caller("launcher"), next, b); !errors.Is(e, cause) {
		t.Fatalf("new revision error lost: %v", e)
	}
	h, e := s.Snapshot("session-a", Assignment)
	if e != nil || h.Applied == nil || *h.Applied != first.Ref || h.Latest.Ref.Revision != 2 || len(h.Rollbacks) != 1 || h.Rollbacks[0].Target == nil || *h.Rollbacks[0].Target != first.Ref || w.submits != 3 || !strings.Contains(w.inputs[2].Payload.CodexTurn.Start.Text, "Preserve the session") || strings.Contains(w.inputs[2].Payload.CodexTurn.Start.Text, next.Content.Objective) {
		t.Fatalf("prior applied objective/receipt not restored: %+v %v", h, e)
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); e != nil {
		t.Fatal(e)
	}
	h, e = s.Snapshot("session-a", Assignment)
	if e != nil || h.Applied == nil || *h.Applied != h.Latest.Ref || w.submits != 4 || w.inputs[1].ID == w.inputs[3].ID {
		t.Fatalf("pending newer head did not get fresh application: %+v %v", h, e)
	}
}
