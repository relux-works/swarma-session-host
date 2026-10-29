// Package sessionbudget retains provider-specific accounting around admitted
// session-goal delivery. A spending ceiling is host accounting, not a native
// Muse goal/set parameter or a hard-cap claim for other providers.
package sessionbudget

import (
	"errors"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	"reflect"
)

var (
	ErrBudgetInvalid         = errors.New("invalid selected provider budget")
	ErrAccountingUnavailable = errors.New("provider accounting identity is unavailable")
	ErrAccountingUncertain   = errors.New("provider accounting outcome is unresolved")
	ErrCounterRegression     = errors.New("native cumulative usage regressed")
)

// SelectBudget preserves absent/fallback, positive and explicit unlimited
// selection. Upstream goal admission is still required for unlimited revisions.
func SelectBudget(selected *goal.Budget, fallback int64) (int64, error) {
	if selected == nil {
		if fallback < 0 {
			return 0, ErrBudgetInvalid
		}
		return fallback, nil
	}
	if selected.Unlimited {
		if selected.Tokens != 0 {
			return 0, ErrBudgetInvalid
		}
		return 0, nil
	}
	if selected.Tokens <= 0 {
		return 0, ErrBudgetInvalid
	}
	return selected.Tokens, nil
}

func missing(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Func, reflect.Chan, reflect.Slice:
		return v.IsNil()
	}
	return false
}
