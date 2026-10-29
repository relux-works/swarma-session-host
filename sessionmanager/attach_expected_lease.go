package sessionmanager

import "context"

// Called under the existing owner/map reservation before adoption, epoch
// allocation, native launch, snapshot, persistence or attachment publication.
// Never calls State or infers holder authority from a routing key or PID.
func (h *SessionHost) checkExpectedOwnerLeaseLocked(ctx context.Context, owner OwnerKey, expected *ControllerLease) error {
	if expected == nil {
		return nil
	}
	pin := *expected
	if (pin.SessionID == "") != (pin.Epoch == 0) {
		return ErrInvalidInput
	}
	var current RunnerRecord
	for _, record := range h.sessions {
		if record.Owner == owner && record.State != SessionStopped {
			if current.SessionID != "" {
				return ErrLegacyCommitUncertain
			}
			current = record
		}
	}
	if pin.SessionID == "" {
		if current.SessionID != "" {
			return ErrStaleInputController
		}
		return nil
	}
	if current.SessionID != pin.SessionID || current.GracefulRetirement || current.ConditionalStop != nil || h.records.uncertain[current.SessionID] {
		return ErrStaleInputController
	}
	input := h.inputs[current.SessionID]
	if input == nil {
		return ErrStaleInputController
	}
	actual, e := input.CurrentLease(ctx, current.SessionID)
	if e != nil {
		return e
	}
	if actual != pin || h.attachmentControllers[current.SessionID] != pin {
		return ErrStaleInputController
	}
	release, e := h.epochs.beginIO(ctx, pin)
	if e != nil {
		return e
	}
	release()
	return nil
}
