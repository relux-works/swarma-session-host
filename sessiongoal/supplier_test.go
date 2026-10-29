package sessiongoal

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

type swallowingAuthority struct{ inner Authority }

func (a swallowingAuthority) WithVerifiedCaller(ctx context.Context, session string, fn func(VerifiedCaller) error) error {
	_ = a.inner.WithVerifiedCaller(ctx, session, fn)
	return nil
}

type swallowingBudget struct{}

func (swallowingBudget) WithBudget(ctx context.Context, lease core.ControllerLease, revision Revision, fn func() error) error {
	_ = fn()
	return nil
}

// Callback suppliers cannot turn policy or receipt errors into successful host
// outcomes by dropping the callback's error. The ordinary admitted supplier is
// paired in the authority and receipt positive cases.
func TestGoalSuppliersCannotSuppressRefusals(t *testing.T) {
	s, a, root := fixture(t)
	s.authority = swallowingAuthority{inner: a}
	before := bytesAt(root)
	if _, e := s.Handle(caller("agent"), command("forbidden", 0)); !errors.Is(e, ErrForbidden) || bytesAt(root) != before {
		t.Fatalf("supplier suppressed policy refusal: %v", e)
	}
	w := newWriter()
	w.awaitErr = errors.New("retained receipt not acknowledged")
	b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, swallowingBudget{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Execute(caller("launcher"), command("set", 0), b); !errors.Is(e, w.awaitErr) {
		t.Fatalf("supplier suppressed receipt error: %v", e)
	}
	head, e := s.Snapshot("session-a", Assignment)
	if e != nil || head.Applied != nil {
		t.Fatalf("suppressed error promoted goal: %v", e)
	}
}
