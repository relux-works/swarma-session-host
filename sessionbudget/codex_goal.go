package sessionbudget

import (
	"context"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// BindCodexGoal uses this factory's actual service and selected text budget.
// Codex has no native goal/accounting install sink. The renderer supplies exact
// ordinary or combined input; only the service's checked receipt can promote it.
func (f *Factory) BindCodexGoal(ctx context.Context, l core.ControllerLease, w core.HostInputWriter, renderer goal.RevisionDeliveryFactory) (core.OwnerKey, core.GoalBinding, error) {
	if l.SessionID == "" || l.Epoch == 0 || missing(w) || missing(renderer) {
		return "", nil, ErrAccountingUnavailable
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.life.Err() != nil {
		return "", nil, core.ErrHostClosed
	}
	b := &binding{store: f.store, lease: l, provider: "codex", writer: w, trace: &deliveryTrace{lease: l}, textOnly: true, life: f.life}
	out, err := f.service.NewDeliveryBinding(ctx, l, goal.Assignment, "codex", traceWriter(w, b.trace), b, renderer)
	return goal.Owner(l.SessionID, goal.Assignment), out, err
}
