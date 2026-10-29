package sessionbudget

import (
	"context"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Native observation method sets alone do not imply native goal support: the
// actual host writer may wrap a PTY or text backend. This optional capability
// forwards the existing backend value; it grants no lease or caller authority.
type nativeGoalCapability interface{ SupportsNativeGoals() bool }

func (f *Factory) nonNativeMuseBinding(ctx context.Context, l core.ControllerLease, w core.HostInputWriter) (core.OwnerKey, core.GoalBinding, error) {
	head, e := f.service.Snapshot(l.SessionID, goal.Assignment)
	if e != nil {
		return "", nil, e
	}
	if head.Attempt != nil || head.Rollback != nil || head.Acknowledged != nil || head.Latest.Ref.Revision > 0 && (head.Latest.State == goal.Active || head.Applied == nil || *head.Applied != head.Latest.Ref) {
		return "", nil, core.ErrHostCapabilityUnavailable
	}
	// The absent/already applied nonactive head follows the real goal service's
	// no-delivery reconciliation. A later active command still reaches an actual
	// receipt binding and refuses before invoking its native delivery callback.
	binding, e := f.service.NewBinding(ctx, l, goal.Assignment, "muse", w, unavailableNativeBudget{lease: l, life: f.life})
	if e != nil {
		return "", nil, e
	}
	// Retire only this factory's prior observer worker; no retained account or
	// canonical head is cleared to accommodate a backend capability difference.
	for old, prior := range f.bindings {
		if old.SessionID == l.SessionID && old != l {
			prior.stop()
			delete(f.bindings, old)
		}
	}
	return goal.Owner(l.SessionID, goal.Assignment), binding, nil
}

type unavailableNativeBudget struct {
	lease core.ControllerLease
	life  context.Context
}

func (b unavailableNativeBudget) WithBudget(ctx context.Context, l core.ControllerLease, _ goal.Revision, _ func() error) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if b.life.Err() != nil {
		return core.ErrHostClosed
	}
	if l != b.lease {
		return core.ErrStaleInputController
	}
	return core.ErrHostCapabilityUnavailable
}
