package sessionbudget

import (
	"context"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// BindRenderedTextGoal exposes this factory's real service and selected-budget
// receipt supplier to a provider renderer. A non-native Muse writer must prove
// its actual capability; text budgets are rendered instructions, not MSP hard
// accounting. Missing capability/renderer remains explicitly unavailable.
func (f *Factory) BindRenderedTextGoal(ctx context.Context, l core.ControllerLease, provider string, w core.HostInputWriter, renderer goal.RevisionDeliveryFactory) (core.OwnerKey, core.GoalBinding, error) {
	if l.SessionID == "" || l.Epoch == 0 || missing(w) || missing(renderer) {
		return "", nil, ErrAccountingUnavailable
	}
	capability, ok := w.(nativeGoalCapability)
	if provider != "muse" || !ok || capability.SupportsNativeGoals() {
		return "", nil, core.ErrHostCapabilityUnavailable
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.life.Err() != nil {
		return "", nil, core.ErrHostClosed
	}
	b := &binding{store: f.store, lease: l, provider: provider, writer: w, trace: &deliveryTrace{lease: l}, textOnly: true, life: f.life}
	// Selected positive/unlimited validation and actual traced input/receipt/ACK
	// checks are shared with the existing provider supplier, without an account.
	out, err := f.service.NewDeliveryBinding(ctx, l, goal.Assignment, provider, traceWriter(w, b.trace), b, renderer)
	if err == nil {
		// Match the existing non-native transition: stop only this factory's former
		// observer, without clearing its retained account or canonical goal history.
		for old, prior := range f.bindings {
			if old.SessionID == l.SessionID && old != l {
				prior.stop()
				delete(f.bindings, old)
			}
		}
	}
	return goal.Owner(l.SessionID, goal.Assignment), out, err
}
