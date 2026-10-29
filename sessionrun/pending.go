package sessionrun

import (
	"context"
	"errors"
	"time"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type pendingClass uint8

const (
	pendingResolved pendingClass = iota
	pendingAbort
	pendingRelist
	pendingStale
	pendingUnknown
)

func pendingErrorClass(err error) pendingClass {
	if err == nil {
		return pendingResolved
	}
	abort, relist, stale := errors.Is(err, core.ErrMusePendingAbort), errors.Is(err, core.ErrMusePendingRelist), errors.Is(err, core.ErrMusePendingStaleBlock)
	n := 0
	for _, yes := range []bool{abort, relist, stale} {
		if yes {
			n++
		}
	}
	if n != 1 || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return pendingUnknown
	}
	if stale {
		return pendingStale
	}
	if errors.Is(err, core.ErrStaleInputController) {
		return pendingUnknown
	}
	if abort {
		return pendingAbort
	}
	return pendingRelist
}

type pendingResult struct {
	epoch uint64
	err   error
}

// Only an explicit relist class schedules another pass. The acknowledgement
// timeout is the supplied host setting, never a spending budget or turn timer.
func resolvePending(ctx context.Context, resolver core.MusePendingResolver, l core.ControllerLease, timeout time.Duration) error {
	for pass := 0; pass < 3; pass++ {
		call, cancel := context.WithTimeout(ctx, timeout)
		err := resolver.ResolveMusePending(call, l)
		cancel()
		if pendingErrorClass(err) != pendingRelist {
			return err
		}
		if pass == 2 {
			return errors.Join(core.ErrMusePendingRelist, err)
		}
	}
	return nil
}
func pendingAbortResult(err error) *core.ProviderRunResult {
	switch pendingErrorClass(err) {
	case pendingResolved, pendingStale:
		return nil
	case pendingRelist:
		return aborted("request_inventory_unavailable")
	case pendingAbort:
		reason := "request_resolution_failed"
		var native *core.MuseHostError
		if errors.As(err, &native) {
			switch native.Code {
			case "approval_deny_unavailable":
				reason = "approval_unresolvable"
			case "user_input_unresolvable":
				reason = "user_input_unresolvable"
			}
		}
		return aborted(reason)
	default:
		return aborted("request_resolution_failed")
	}
}
