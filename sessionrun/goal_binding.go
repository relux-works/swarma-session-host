package sessionrun

import (
	"context"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

var _ core.GoalBindingFactory = (*Runtime)(nil)

// BindGoal composes the same budget/service supplier with actual provider
// rendering. Physical identity comes from the current host writer even before
// attachment publication; no managed snapshot or caller string supplies it.
func (r *Runtime) BindGoal(ctx context.Context, l core.ControllerLease, provider string, w core.HostInputWriter) (core.OwnerKey, core.GoalBinding, error) {
	r.mu.Lock()
	budgets := r.budgets
	r.mu.Unlock()
	if budgets == nil || w == nil {
		return "", nil, core.ErrHostCapabilityUnavailable
	}
	if provider == "muse" {
		if native, ok := w.(interface{ SupportsNativeGoals() bool }); ok && !native.SupportsNativeGoals() {
			physical, ok := w.(core.ProviderNativeIdentity)
			if !ok {
				return "", nil, core.ErrHostCapabilityUnavailable
			}
			renderer, err := NewPTYDeliveryFactory(physical.NativeIdentity())
			if err != nil {
				return "", nil, err
			}
			return budgets.BindRenderedTextGoal(ctx, l, provider, w, renderer)
		}
	}
	return budgets.BindGoal(ctx, l, provider, w)
}
