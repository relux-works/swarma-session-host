package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// The no-write exception accepts only the real deferred sentinel's full error
// tree. A joined storage/cancellation/uncertainty failure cannot borrow it.
func TestOnlyGoalNativeDeferredRejectsMixedFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"single", ErrGoalNativeDeferred, true},
		{"wrapped", fmt.Errorf("delivery: %w", ErrGoalNativeDeferred), true},
		{"callback-join", errors.Join(ErrGoalNativeDeferred, ErrGoalNativeDeferred), true},
		{"nil", nil, false},
		{"unknown", errors.New(ErrGoalNativeDeferred.Error()), false},
		{"uncertain", errors.Join(ErrGoalNativeDeferred, ErrLegacyCommitUncertain), false},
		{"canceled", errors.Join(ErrGoalNativeDeferred, context.Canceled), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := onlyGoalNativeDeferred(tc.err); got != tc.want {
				t.Fatalf("no-write classification = %v, want %v", got, tc.want)
			}
		})
	}
}
