package sessionbudget

import (
	"context"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// CheckBudget joins the existing exact-lease accounting binding before a run
// attests its terminal. It does not create a binding, hydrate a new identity or
// supply a second accounting engine. The existing Check owns observation,
// threshold pause, matched receipt and durable original-image commit.
func (f *Factory) CheckBudget(ctx context.Context, l core.ControllerLease) error {
	if f == nil {
		return ErrAccountingUnavailable
	}
	f.mu.Lock()
	if f.closed || f.life.Err() != nil {
		f.mu.Unlock()
		return core.ErrHostClosed
	}
	b := f.bindings[l]
	if b != nil && b.retiring {
		f.mu.Unlock()
		return core.ErrStaleInputController
	}
	f.mu.Unlock()
	if b == nil || b.provider != "muse" {
		return ErrAccountingUnavailable
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(f.life, cancel)
	defer cancel()
	defer stop()
	return b.Check(call)
}

// AssignmentHead reads the actual goal service selected by this factory for
// restore validation. It grants no caller rights and performs no reconciliation.
func (f *Factory) AssignmentHead(session string) (goal.Head, error) {
	if f == nil {
		return goal.Head{}, ErrAccountingUnavailable
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.life.Err() != nil {
		return goal.Head{}, core.ErrHostClosed
	}
	return f.service.Snapshot(session, goal.Assignment)
}
