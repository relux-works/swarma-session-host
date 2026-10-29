package sessionrun

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
	"time"
)

type pendingFixture struct {
	errors []error
	calls  int
}

func (f *pendingFixture) ResolveMusePending(context.Context, core.ControllerLease) error {
	f.calls++
	if len(f.errors) == 0 {
		return nil
	}
	e := f.errors[0]
	f.errors = f.errors[1:]
	return e
}

// Retry admission follows the original typed class, never error text; only an
// explicit relist gets up to three passes. Context and mixed classes fail closed.
func TestMusePendingResolutionUsesTypedBoundedClasses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		errs  []error
		calls int
		class pendingClass
	}{
		{"resolved", nil, 1, pendingResolved}, {"relist_then_valid", []error{core.ErrMusePendingRelist, nil}, 2, pendingResolved}, {"bounded", []error{core.ErrMusePendingRelist, core.ErrMusePendingRelist, core.ErrMusePendingRelist}, 3, pendingRelist}, {"abort", []error{core.ErrMusePendingAbort}, 1, pendingAbort}, {"stale", []error{core.ErrMusePendingStaleBlock}, 1, pendingStale}, {"mixed", []error{errors.Join(core.ErrMusePendingAbort, core.ErrMusePendingRelist)}, 1, pendingUnknown}, {"cancelled", []error{errors.Join(core.ErrMusePendingRelist, context.Canceled)}, 1, pendingUnknown}, {"text_only", []error{errors.New("relist")}, 1, pendingUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &pendingFixture{errors: tc.errs}
			err := resolvePending(context.Background(), f, core.ControllerLease{SessionID: "session", Epoch: 1}, time.Second)
			if f.calls != tc.calls || pendingErrorClass(err) != tc.class {
				t.Fatalf("typed pending class changed: calls %d err %v class %d", f.calls, err, pendingErrorClass(err))
			}
		})
	}
}
