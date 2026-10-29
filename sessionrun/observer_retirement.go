package sessionrun

import (
	"context"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// RetireGoalObserver forwards only to the actual bound accounting factory.
// Core invokes it under its captured admission before epoch revocation, without
// holding a map/writer fence across the join. No native/provider ACK is minted.
func (r *Runtime) RetireGoalObserver(ctx context.Context, l core.ControllerLease) (bool, error) {
	if r == nil {
		return false, core.ErrHostCapabilityUnavailable
	}
	r.mu.Lock()
	factory := r.budgets
	r.mu.Unlock()
	if factory == nil {
		return false, core.ErrHostCapabilityUnavailable
	}
	return factory.RetireGoalObserver(ctx, l)
}
