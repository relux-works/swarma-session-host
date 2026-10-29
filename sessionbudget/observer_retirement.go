package sessionbudget

import (
	"context"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// RetireGoalObserver cancels and joins the actual registered native observer
// for exactly l, before its controller is intentionally revoked. The caller
// retains host admission/order ownership. This method neither changes an epoch
// nor writes or clears accounting, pending effects, ledger or uncertainty.
// True certifies that exact worker is joined (including an idempotent retry).
// False,nil reports no registered native worker, not a provider retirement ACK.
func (f *Factory) RetireGoalObserver(ctx context.Context, l core.ControllerLease) (bool, error) {
	if f == nil || ctx == nil {
		return false, ErrAccountingUnavailable
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if l.SessionID == "" || l.Epoch == 0 {
		return false, core.ErrStaleInputController
	}
	f.mu.Lock()
	if err := ctx.Err(); err != nil {
		f.mu.Unlock()
		return false, err
	}
	if f.closed || f.life.Err() != nil {
		f.mu.Unlock()
		return false, core.ErrHostClosed
	}
	b := f.bindings[l]
	if b == nil {
		for other, sibling := range f.bindings {
			if other.SessionID == l.SessionID {
				// A failed newer binding may leave only an older intentional
				// retirement tombstone. Ignore it only after its real worker
				// joined; keep the tombstone's same-lease restart refusal.
				if other.Epoch < l.Epoch && sibling.lease == other && sibling.retiring && sibling.done != nil {
					select {
					case <-sibling.done:
						continue
					default:
					}
				}
				f.mu.Unlock()
				return false, core.ErrStaleInputController
			}
		}
		f.mu.Unlock()
		return false, nil
	}
	if b.lease != l {
		f.mu.Unlock()
		return false, core.ErrStaleInputController
	}
	if b.done == nil {
		f.mu.Unlock()
		return false, nil
	}
	// Retain a tombstone until a genuinely newer binding replaces it. Releasing
	// the mutex to join cannot allow this same lease to restart its observer.
	b.retiring = true
	b.stop() // existing source: cancel watch context BEFORE closing its feed
	done := b.done
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-done:
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return true, nil
	}
}
