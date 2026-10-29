package sessionmanager

import (
	"context"
	"errors"
)

// ErrGoalNativeDeferred denotes only a proved no-Submit delivery. It preserves
// Pending without an applied reference. An unknown/post-write failure must not
// contain only this sentinel; the real receipt service checks that boundary.
var ErrGoalNativeDeferred = errors.New("session goal awaits a checked combined owner turn")

// OwnerRunGoalBindingFactory binds combined owner work to the actual service's
// fresh receipt transaction. It returns the concrete binding belonging to that
// service, not a wrapper claiming canonical authority. Any provider start uses
// the supplied cut's exact writer inside that fresh checked delivery scope.
type OwnerRunGoalBindingFactory interface {
	BindOwnerRunGoal(context.Context, CapturedOwnerRun, ProviderRunRequest) (OwnerKey, GoalBinding, error)
}

// The real service joins its budget callback and delivery errors. Accept only
// an error tree whose every leaf is the same no-effect sentinel, never a joined
// uncertainty/cancellation/storage failure merely containing that sentinel.
func onlyGoalNativeDeferred(err error) bool {
	if err == nil {
		return false
	}
	if err == ErrGoalNativeDeferred {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !onlyGoalNativeDeferred(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyGoalNativeDeferred(wrapped.Unwrap())
	}
	return false
}
