package sessionhost

import (
	"context"
	"errors"

	"github.com/relux-works/swarma-session-host/internal/admission"
)

// checkAdmission runs only on the serial dispatcher. External observation never
// holds the state lock. The next automatic write must pass; status/owned stop do
// not depend on a successful observer. Loss is a persistent, unreleasable fence.
func (h *Host) checkAdmission() error {
	if h.cfg.Admission == nil {
		return nil
	}
	h.mu.Lock()
	prior := h.state.Admission
	if prior == nil || prior.Failed || h.err != nil || h.ctx.Err() != nil {
		h.mu.Unlock()
		return errors.New("admission_reconciliation_required")
	}
	saved := *prior
	instance := h.state.RunnerInstance
	native := ""
	if h.state.Protocol != nil {
		native = h.state.Protocol.View.NativeSession
	}
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(h.ctx, h.cfg.IOTimeout)
	defer cancel()
	proof, err := admission.Verify(ctx, *h.cfg.Admission, &saved, instance, saved.Record.Binding.Harness.PID)
	if native != "" && native != saved.Record.Binding.NativeSession {
		err = errors.New("admission_native_session_conflict")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err != nil {
		next := cloneState(h.state)
		next.Admission.Failed = true
		next.Fence = "unknown"
		next.InputState = InputUnknown
		h.writeBlocked = true
		if persistErr := h.persistLocked(next); persistErr != nil {
			return persistErr
		}
		return err
	}
	if proof == saved {
		return nil
	}
	next := cloneState(h.state)
	next.Admission = &proof
	return h.persistLocked(next)
}
