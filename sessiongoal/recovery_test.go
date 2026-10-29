package sessiongoal

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"testing"
)

// An acknowledged revision is not blindly assumed live after a controller
// replacement: a resolved previous attempt may rebind under the new real lease
// and keeps both applications; unresolved attempts remain closed in the paired
// uncertain-restart test.
func TestGoalReconcilesResolvedRevisionUnderNewController(t *testing.T) {
	s, a, root := fixture(t)
	w := newWriter()
	b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Execute(caller("launcher"), command("set", 0), b); e != nil {
		t.Fatal(e)
	}
	s, e = Open(root, a)
	if e != nil {
		t.Fatal(e)
	}
	w2 := newWriter()
	b, e = s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 2}, Assignment, "codex", w2, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); e != nil {
		t.Fatal(e)
	}
	head, e := s.Snapshot("session-a", Assignment)
	if e != nil || head.AppliedReceipt == nil || head.AppliedReceipt.Epoch != 2 || len(head.Applications) != 2 || w2.submits != 1 {
		t.Fatalf("controller recovery lost journal/ack: %+v %v", head, e)
	}
	// A prior applied receipt must not be reused as the new controllers pending
	// acknowledgement. A failed rebind remains retryable under its exact lease.
	third := newWriter()
	third.awaitErr = errors.New("new controller acknowledgement unavailable")
	b, e = s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 3}, Assignment, "codex", third, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); !errors.Is(e, third.awaitErr) {
		t.Fatalf("new controller failure lost: %v", e)
	}
	head, e = s.Snapshot("session-a", Assignment)
	if e != nil || head.Acknowledged != nil {
		t.Fatalf("old applied receipt became pending acknowledgement: %+v %v", head, e)
	}
	third.awaitErr = nil
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); e != nil {
		t.Fatalf("same-lease rebind retry was stranded: %v", e)
	}
	if third.submits != 2 || third.inputs[0].ID != third.inputs[1].ID {
		t.Fatal("same-attempt controller retry changed identity")
	}
}

// A working-goal tool can store its own plans, but cannot obtain a binding to
// the provider's single assignment-goal plane. The nearby assignment delivery
// positive and working-store positive establish both supported capabilities.
func TestGoalWorkingNamespaceCannotWriteNativeAssignment(t *testing.T) {
	s, _, _ := fixture(t)
	c := command("working", 0)
	c.Namespace = Working
	receipt, e := s.Handle(caller("agent"), c)
	if e != nil || receipt.Pending {
		t.Fatalf("working goal store failed: %+v %v", receipt, e)
	}
	w := newWriter()
	if b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Working, "codex", w, budgetFixture{}); b != nil || !errors.Is(e, ErrBindingUnavailable) || w.submits != 0 {
		t.Fatalf("working tool acquired assignment writer: %v", e)
	}
}

// The journal's existing valid0600 file is readable. A mode change is refused
// without replacement; this destructive probe is confined to an owned fixture.
func TestGoalJournalPrivateModeRefusal(t *testing.T) {
	s, a, root := fixture(t)
	if _, e := s.Handle(caller("launcher"), command("set", 0)); e != nil {
		t.Fatal(e)
	}
	if _, e := Open(root, a); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(root, "journal.json")
	if e := os.Chmod(path, 0644); e != nil {
		t.Fatal(e)
	}
	before := bytesAt(root)
	if _, e := Open(root, a); !errors.Is(e, ErrJournalInvalid) || bytesAt(root) != before {
		t.Fatalf("foreign journal mode admitted/replaced: %v", e)
	}
}

type postAcknowledgementBudget struct {
	calls        int
	err          error
	failRollback bool
	writer       *writerFixture
}

func (b *postAcknowledgementBudget) WithBudget(ctx context.Context, lease core.ControllerLease, revision Revision, fn func() error) error {
	b.calls++
	if b.calls == 2 && b.failRollback {
		b.writer.awaitErr = errors.New("rollback acknowledgement unavailable")
	}
	if err := fn(); err != nil {
		return err
	}
	if b.calls == 1 {
		return b.err
	}
	return nil
}

// A trusted budget supplier can fail after the native effect was acknowledged.
// Preserve that cause, attempt the prior no-goal rollback with a distinct receipt,
// and retain an explicit unresolved obligation across restart; never claim an
// applied assignment or silently replay the old input identity after rollback.
func TestGoalPostAcknowledgementFailureRetainsRollback(t *testing.T) {
	s, a, root := fixture(t)
	w := newWriter()
	cause := errors.New("budget commit failed after acknowledgement")
	budget := &postAcknowledgementBudget{err: cause, failRollback: true, writer: w}
	b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, budget)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Execute(caller("launcher"), command("set", 0), b); !errors.Is(e, cause) {
		t.Fatalf("post-ack cause lost: %v", e)
	}
	head, e := s.Snapshot("session-a", Assignment)
	if e != nil || head.Applied != nil || head.Rollback == nil || head.Rollback.Receipt == nil || w.submits != 2 || w.inputs[0].ID == w.inputs[1].ID || w.inputs[1].Payload.CodexTurn == nil {
		t.Fatalf("rollback obligation/receipt lost: %+v %v submits=%d", head, e, w.submits)
	}
	s, e = Open(root, a)
	if e != nil {
		t.Fatal(e)
	}
	before := bytesAt(root)
	if _, e = s.Handle(caller("launcher"), command("next", 1)); !errors.Is(e, ErrDeliveryUncertain) || bytesAt(root) != before {
		t.Fatalf("unresolved rollback admitted new command: %v", e)
	}
	fresh := newWriter()
	b, e = s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 2}, Assignment, "codex", fresh, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); !errors.Is(e, ErrDeliveryUncertain) || fresh.submits != 0 {
		t.Fatalf("rollback recovery blindly delivered: %v", e)
	}
}
