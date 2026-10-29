package sessionmanager

import (
	"context"
	"errors"
	"reflect"
	"time"
)

// Called with mu held, returns with it held. The injected service can perform
// receipt-checked native I/O without holding the host map lock. A reservation
// excludes another attach until the exact controller is revalidated.
func (h *SessionHost) reconcileHostGoalLocked(ctx context.Context, id string) error {
	return h.reconcileHostGoalPhaseLocked(ctx, id, nil, ProviderRunRequest{})
}

func (h *SessionHost) reconcileHostGoalPhaseLocked(ctx context.Context, id string, run *CapturedOwnerRun, request ProviderRunRequest) error {
	if absentHostPort(h.options.Goals) {
		return nil
	}
	if absentHostPort(h.options.GoalBindings) || h.goalReconciling[id] {
		return ErrHostCapabilityUnavailable
	}
	record := h.sessions[id]
	input := h.inputs[id]
	if input == nil {
		return ErrHostCapabilityUnavailable
	}
	lease, err := input.CurrentLease(ctx, id)
	if err != nil {
		return err
	}
	if run != nil && (run.Lease != lease || run.Input != input || run.Record.SessionID != id || run.Record.Provider != record.Provider || run.Record.MaterializationCtxID != "") {
		return ErrHostCapabilityUnavailable
	}
	if h.goalReconciling == nil {
		h.goalReconciling = map[string]bool{}
	}
	h.goalReconciling[id] = true
	operation := h.beginOwnerOperationLocked(record.Owner)
	h.mu.Unlock()
	var owner OwnerKey
	var binding GoalBinding
	if factory, ok := h.options.GoalBindings.(OwnerRunGoalBindingFactory); run != nil && ok && !absentHostPort(factory) {
		owner, binding, err = factory.BindOwnerRunGoal(ctx, *run, request)
	} else {
		owner, binding, err = h.options.GoalBindings.BindGoal(ctx, lease, record.Provider, input)
	}
	if err == nil && run != nil {
		if _, combined := h.options.GoalBindings.(OwnerRunGoalBindingFactory); !combined {
			// An ordinary supplier must not send a pending goal-only first input
			// in place of the missing combined owner renderer.
			read, readErr := h.options.Goals.Get(owner)
			if readErr != nil {
				err = readErr
			} else if read == nil || read.Pending {
				err = ErrHostCapabilityUnavailable
			}
		}
	}
	var activation *GoalActivation
	if err == nil && (owner == "" || absentHostPort(binding)) {
		err = ErrHostCapabilityUnavailable
	}
	disabled := false
	if err == nil && !absentHostPort(h.options.Gate) {
		var reason string
		reason, err = h.options.Gate.DisabledGoalPlane(owner)
		disabled = err == nil && reason != ""
		if disabled {
			// Preserve the original kill switch: the stored goal head is untouched.
			// An already-bound native goal must acknowledge clear before the host
			// forgets its applied reference. A missing clear sink fails closed.
			if record.AppliedGoalRef != nil {
				err = binding.Rollback(&GoalEnvelope{Owner: owner, Clear: true})
			}
			activation = &GoalActivation{}
		}
	}
	if err == nil && !disabled {
		activation, err = h.options.Goals.Reconcile(owner, binding)
		if err == nil && (activation == nil || activation.Pending) {
			err = ErrHostCapabilityUnavailable
		}
	}
	h.mu.Lock()
	h.endOwnerOperationLocked(record.Owner, operation)
	delete(h.goalReconciling, id)
	current, leaseErr := input.CurrentLease(ctx, id)
	if h.closed || h.inputs[id] != input || leaseErr != nil || current != lease || h.sessions[id].State == SessionStopped {
		return errors.Join(err, leaseErr, ErrStaleInputController)
	}
	record = h.sessions[id]
	if activation != nil && activation.Pending && activation.Ref != nil && onlyGoalNativeDeferred(err) {
		// Only the actual no-Submit source can defer. Do not publish the queued ref
		// as applied, and do not revoke a usable input merely to await owner work.
		if run != nil {
			return err
		}
		return nil
	}
	if err != nil {
		// Keep the real managed runner for recovery; do not force-stop it or claim
		// an uncommitted native outcome. Revoke input before recording failure.
		clean, cancel := context.WithTimeout(h.life, 30*time.Second)
		defer cancel()
		revokeErr := input.revokeAndDrain(clean)
		delete(h.inputs, id)
		record.State = SessionRecovering
		record.OwnerRunError = "goal reconciliation failed"
		record.UpdatedAt = time.Now().UTC()
		commitErr := h.updateRecord(clean, record)
		if commitErr == nil {
			h.sessions[id] = record
		}
		return errors.Join(err, revokeErr, commitErr)
	}
	if err := h.goalRecoverySettledLocked(ctx, id); err != nil {
		return err
	}
	if disabled {
		record.AppliedGoalRef = nil
	}
	if activation.Ref != nil {
		ref := *activation.Ref
		record.AppliedGoalRef = &ref
	}
	if record.OwnerRunError == "goal reconciliation failed" {
		record.State = SessionRunning
		record.OwnerRunError = ""
	}
	if reflect.DeepEqual(record, h.sessions[id]) {
		return nil
	}
	record.UpdatedAt = time.Now().UTC()
	if err = h.updateRecord(ctx, record); err != nil {
		return err
	}
	h.sessions[id] = record
	return nil
}
