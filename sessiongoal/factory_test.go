package sessiongoal

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

// The actual signed host factory interface composes the service, exact session
// writer/lease/provider, and budget port. This is an in-process library consumer,
// not a provider launch, bridge authentication or live Attach qualification.
func TestGoalActualCoreBindingFactory(t *testing.T) {
	s, _, _ := fixture(t)
	w := newWriter()
	factory := BindingFactory{Service: s, Budget: budgetFixture{}}
	options := core.HostOptions{Goals: s, GoalBindings: factory}
	owner, b, e := options.GoalBindings.BindGoal(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, "codex", w)
	if e != nil || owner != Owner("session-a", Assignment) {
		t.Fatalf("exact session factory failed: %v", e)
	}
	if _, e = s.Execute(caller("launcher"), command("set", 0), b); e != nil || w.submits != 1 {
		t.Fatalf("factory binding did not deliver: %v", e)
	}
	if _, b, e = (BindingFactory{}).BindGoal(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, "codex", w); !errors.Is(e, ErrBindingUnavailable) || b != nil {
		t.Fatalf("missing service supplied a binding: %v", e)
	}
}
