package sessiongoal

import (
	"context"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// BindingFactory implements the host core factory for an admitted session.
// Budget must preserve that actual providers existing budget behavior; absence
// fails closed when reconciliation attempts a native effect. The factory does
// not authenticate commands or replace the hosts configured goal-plane gate.
type BindingFactory struct {
	Service *Service
	Budget  BudgetBinding
}

var _ core.GoalBindingFactory = BindingFactory{}

func (f BindingFactory) BindGoal(ctx context.Context, lease core.ControllerLease, provider string, writer core.HostInputWriter) (core.OwnerKey, core.GoalBinding, error) {
	if f.Service == nil {
		return "", nil, ErrBindingUnavailable
	}
	binding, err := f.Service.NewBinding(ctx, lease, Assignment, provider, writer, f.Budget)
	if err != nil {
		return "", nil, err
	}
	return Owner(lease.SessionID, Assignment), binding, nil
}
