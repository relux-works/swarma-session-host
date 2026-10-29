package sessionmanager

import (
	"context"
	"fmt"
	"strings"
)

var _ RecordMetadataCommitter = (*SessionHost)(nil)

// CommitRecordMetadata changes only the five accepted groups. It uses the
// current committed native image, without asking a runner for a new snapshot.
// The controller's shared I/O fence spans validation, sole write, observation
// and private acknowledgement. A failed attempted write revokes actual input.
func (h *SessionHost) CommitRecordMetadata(ctx context.Context, request RecordMetadataCommit) (RecordMetadataReceipt, error) {
	if err := h.lockContext(ctx); err != nil {
		return RecordMetadataReceipt{}, err
	}
	defer h.mu.Unlock()
	id := request.Lease.SessionID
	if h.attachmentChanging(id) {
		return RecordMetadataReceipt{}, ErrLegacyCommitUncertain
	}
	if strings.TrimSpace(request.ID) == "" {
		return RecordMetadataReceipt{}, ErrInvalidInput
	}
	if _, pending := h.pendingInitial[id]; pending || h.records.uncertain[id] {
		return RecordMetadataReceipt{}, ErrLegacyCommitUncertain
	}
	record, ok := h.sessions[id]
	if !ok {
		return RecordMetadataReceipt{}, ErrProviderHostNotFound
	}
	if request.Owner != record.Owner || request.Provider != record.Provider {
		return RecordMetadataReceipt{}, ErrLegacyViewStale
	}
	lease, err := h.issuedMetadataLease(ctx, id)
	if err != nil {
		return RecordMetadataReceipt{}, err
	}
	if lease != request.Lease {
		return RecordMetadataReceipt{}, ErrStaleInputController
	}
	release, err := h.epochs.beginIO(ctx, request.Lease)
	if err != nil {
		return RecordMetadataReceipt{}, err
	}
	defer release()
	base, ok := h.records.bases[id]
	if !ok || request.BaseSHA256 != base.SHA256 {
		return RecordMetadataReceipt{}, ErrLegacyViewStale
	}
	path, err := h.records.path(id)
	if err != nil {
		return RecordMetadataReceipt{}, err
	}
	if err := h.records.matchesCurrent(path, base); err != nil {
		return RecordMetadataReceipt{}, err
	}
	current, err := h.records.decode(ctx, base)
	if err != nil {
		return RecordMetadataReceipt{}, err
	}
	if current.SessionID != id || current.Owner != request.Owner || current.Provider != request.Provider {
		return RecordMetadataReceipt{}, ErrLegacyViewStale
	}
	next, err := applyRecordMetadata(current, request.Patch)
	if err != nil {
		return RecordMetadataReceipt{}, err
	}
	if _, used := h.metadataIDs[request.Lease][request.ID]; used {
		return RecordMetadataReceipt{}, ErrInputIDConflict
	}
	if h.metadataIDs == nil {
		h.metadataIDs = map[ControllerLease]map[string]struct{}{}
	}
	if h.metadataIDs[request.Lease] == nil {
		h.metadataIDs[request.Lease] = map[string]struct{}{}
	}
	h.metadataIDs[request.Lease][request.ID] = struct{}{}
	if err := h.updateRecord(ctx, next); err != nil {
		return RecordMetadataReceipt{}, err
	}
	h.sessions[id] = next
	return RecordMetadataReceipt{Lease: request.Lease, ID: request.ID, Owner: request.Owner, Provider: request.Provider, BaseSHA256: base.SHA256, CommittedSHA256: h.records.bases[id].SHA256}, nil
}

func applyRecordMetadata(current RunnerRecord, patch RecordMetadataPatch) (RunnerRecord, error) {
	if patch.AppliedGoalRef == nil && patch.ConditionalStop == nil && patch.OwnerRunError == nil && patch.Materialization == nil && patch.Eviction == nil {
		return RunnerRecord{}, ErrInvalidInput
	}
	if patch.ConditionalStop == nil && (current.ConditionalStop != nil || current.GracefulRetirement) {
		return RunnerRecord{}, ErrInvalidInput
	}
	next, err := cloneRunnerRecord(current)
	if err != nil {
		return RunnerRecord{}, err
	}
	if group := patch.AppliedGoalRef; group != nil {
		next.AppliedGoalRef = nil
		if group.Value != nil {
			value := *group.Value
			if strings.TrimSpace(value.ID) == "" || value.Revision < 1 {
				return RunnerRecord{}, fmt.Errorf("%w: invalid applied revision", ErrInvalidInput)
			}
			next.AppliedGoalRef = &value
		}
	}
	if group := patch.OwnerRunError; group != nil {
		next.OwnerRunError = group.Value
	}
	if group := patch.Materialization; group != nil {
		if group.ProviderHostPendingFork && group.CtxID == "" {
			return RunnerRecord{}, fmt.Errorf("%w: pending fork requires materialization", ErrInvalidInput)
		}
		next.MaterializationCtxID = group.CtxID
		next.ProviderHostPendingFork = group.ProviderHostPendingFork
	}
	if group := patch.Eviction; group != nil {
		next.Eviction = nil
		if group.Value != nil {
			value := *group.Value
			provider, err := normalizeProvider(value.ByProvider)
			if err != nil || provider != value.ByProvider || value.At.IsZero() {
				return RunnerRecord{}, fmt.Errorf("%w: invalid eviction", ErrInvalidInput)
			}
			next.Eviction = &value
		}
	}
	if patch.ConditionalStop != nil {
		if err := applyConditionalMetadata(current, &next, *patch.ConditionalStop); err != nil {
			return RunnerRecord{}, err
		}
	}
	return next, nil
}

// This derives the phase from checked committed state, never from the caller's
// expected value. Previous is host-captured recovery data, not a replacement
// for current native or other metadata facts on rollback.
func recordMetadataPhase(record RunnerRecord) (ConditionalStopPhase, error) {
	intent := record.ConditionalStop
	if intent == nil {
		if record.GracefulRetirement {
			return ConditionalStopPhaseRetirement, nil
		}
		return ConditionalStopPhaseNone, nil
	}
	previous := intent.Previous
	if !record.GracefulRetirement || intent.Precondition != "not-busy" || previous == nil || previous.ConditionalStop != nil || previous.GracefulRetirement || previous.State != SessionRunning || previous.SessionID != record.SessionID || previous.Owner != record.Owner || previous.Provider != record.Provider || previous.Contract != record.Contract || previous.ProjectDir != record.ProjectDir || previous.NamespaceFingerprint != record.NamespaceFingerprint || previous.CreatedAt != record.CreatedAt {
		return "", ErrInvalidInput
	}
	if err := validateNeutralRunnerRecord(*previous); err != nil {
		return "", fmt.Errorf("%w: invalid conditional previous: %v", ErrInvalidInput, err)
	}
	switch {
	case intent.Phase == "" && !intent.Signalled:
		return ConditionalStopPhaseIntent, nil
	case intent.Phase == "signal-attempt" && !intent.Signalled:
		return ConditionalStopPhaseAttempt, nil
	case intent.Phase == "signal-attempt" && intent.Signalled:
		return ConditionalStopPhaseSent, nil
	default:
		return "", ErrInvalidInput
	}
}

func applyConditionalMetadata(current RunnerRecord, next *RunnerRecord, patch ConditionalStopMetadata) error {
	phase, err := recordMetadataPhase(current)
	if err != nil || patch.ExpectedPhase != phase {
		return ErrInvalidInput
	}
	switch {
	case phase == ConditionalStopPhaseNone && patch.NextPhase == ConditionalStopPhaseIntent:
		if current.State != SessionRunning {
			return ErrInvalidInput
		}
		previous, err := cloneRunnerRecord(current)
		if err != nil {
			return err
		}
		next.ConditionalStop = &ConditionalStopRecord{Precondition: "not-busy", Previous: &previous}
		next.GracefulRetirement = true
	case phase == ConditionalStopPhaseIntent && patch.NextPhase == ConditionalStopPhaseAttempt:
		next.ConditionalStop.Phase = "signal-attempt"
		next.GracefulRetirement = true
	case (phase == ConditionalStopPhaseIntent || phase == ConditionalStopPhaseAttempt) && patch.NextPhase == ConditionalStopPhaseRollback:
		next.GracefulRetirement = current.ConditionalStop.Previous.GracefulRetirement
		next.ConditionalStop = nil
	case phase == ConditionalStopPhaseAttempt && patch.NextPhase == ConditionalStopPhaseSent:
		next.ConditionalStop.Signalled = true
		next.GracefulRetirement = true
	case (phase == ConditionalStopPhaseAttempt || phase == ConditionalStopPhaseSent) && patch.NextPhase == ConditionalStopPhaseRetirement:
		next.ConditionalStop = nil
		next.GracefulRetirement = true
	default:
		return ErrInvalidInput
	}
	return nil
}
