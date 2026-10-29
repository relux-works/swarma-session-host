package sessiongoal

import (
	"context"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

// A crash after the real checked Apply but before source promotion leaves a
// durable verified acknowledgement. Reopen may promote that exact receipt and
// rebind under the new controller. The paired missing-ACK restart test refuses
// an uncertain old attempt and performs no new input.
func TestGoalDurableAcknowledgementRecoversBeforePromotion(t *testing.T) {
	s, a, root := fixture(t)
	r, e := s.Handle(caller("launcher"), command("set", 0))
	if e != nil {
		t.Fatal(e)
	}
	w := newWriter()
	b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Apply(&core.GoalEnvelope{Owner: Owner("session-a", Assignment), Ref: &r.Ref}); e != nil {
		t.Fatal(e)
	}
	h, e := s.Snapshot("session-a", Assignment)
	if e != nil || h.Applied != nil || h.Acknowledged == nil {
		t.Fatalf("crash-window fixture lacks real checked acknowledgement: %+v %v", h, e)
	}
	s, e = Open(root, a)
	if e != nil {
		t.Fatal(e)
	}
	fresh := newWriter()
	b, e = s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 2}, Assignment, "codex", fresh, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); e != nil {
		t.Fatalf("durable checked acknowledgement was stranded: %v", e)
	}
	h, e = s.Snapshot("session-a", Assignment)
	if e != nil || h.Applied == nil || *h.Applied != r.Ref || h.AppliedReceipt == nil || h.AppliedReceipt.Epoch != 2 || len(h.Applications) != 2 || fresh.submits != 1 {
		t.Fatalf("known acknowledgement/new lease receipts lost: %+v %v", h, e)
	}
}
