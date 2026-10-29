package sessionmanager

import (
	"context"
	"errors"
)

var _ MusePendingResolver = (*sessionInput)(nil)

// ResolveMusePending holds the actual host writer fence and durable I/O
// admission for the whole native pass. No submission or receipt is created.
func (h *sessionInput) ResolveMusePending(ctx context.Context, lease ControllerLease) error {
	h.mu.Lock()
	epochCtx := h.epochCtx
	h.mu.Unlock()
	joint, cancel := bindInputContext(epochCtx, ctx)
	defer cancel()
	stale := func(err error) error {
		if errors.Is(err, ErrStaleInputController) {
			return errors.Join(ErrMusePendingStaleBlock, err)
		}
		return err
	}
	if err := h.requireLease(joint, lease); err != nil {
		return stale(err)
	}
	if err, joined := h.joinMusePendingScope(joint, lease); joined {
		return stale(err)
	}
	defer h.unlock()
	if err := h.requireLease(joint, lease); err != nil {
		return stale(err)
	}
	release, err := h.acquireIO(joint, lease)
	if err != nil {
		return stale(err)
	}
	defer release()
	if err := h.requireLease(joint, lease); err != nil {
		return stale(err)
	}
	backend, ok := h.backend.(interface{ ResolveMusePending(context.Context) error })
	if !ok {
		return ErrHostCapabilityUnavailable
	}
	if err := backend.ResolveMusePending(joint); err != nil {
		return err
	}
	return stale(h.requireLease(joint, lease))
}
