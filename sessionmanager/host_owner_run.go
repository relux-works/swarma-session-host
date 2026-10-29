package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type runOwnerRetiredError struct{}

func (*runOwnerRetiredError) Error() string {
	return "tracked execution has moved; RUN-owned session admission is retired"
}
func refuseRetiredOwner(owner OwnerKey) error {
	if strings.HasPrefix(string(owner), "run/RUN-") {
		return &runOwnerRetiredError{}
	}
	return nil
}

type ownerRunPersistError struct {
	sessionID string
	err       error
}

func (e *ownerRunPersistError) Error() string {
	return fmt.Sprintf("persisting managed owner run for session %s: %v", e.sessionID, e.err)
}
func (e *ownerRunPersistError) Unwrap() error { return e.err }

const ownerRunPersistMessage = "managed owner run outcome could not be persisted"

func ownerRunErrorMessage(err error) string {
	var persist *ownerRunPersistError
	if errors.As(err, &persist) || errors.Is(err, ErrLegacyCommitUncertain) || errors.Is(err, ErrLegacyViewStale) || errors.Is(err, ErrLegacyCodecMismatch) {
		return ownerRunPersistMessage
	}
	return err.Error()
}

type ownerRunActivity struct {
	count int
	idle  chan struct{}
}

func (h *SessionHost) beginOwnerRunLocked(owner OwnerKey) {
	if h.ownerRuns == nil {
		h.ownerRuns = map[OwnerKey]*ownerRunActivity{}
	}
	a := h.ownerRuns[owner]
	if a == nil {
		a = &ownerRunActivity{idle: make(chan struct{})}
		h.ownerRuns[owner] = a
	}
	a.count++
}
func (h *SessionHost) endOwnerRunLocked(owner OwnerKey) {
	a := h.ownerRuns[owner]
	if a == nil {
		return
	}
	a.count--
	if a.count == 0 {
		delete(h.ownerRuns, owner)
		close(a.idle)
	}
}
func (h *SessionHost) lockOwnerActivation(ctx context.Context, owner OwnerKey) error {
	for {
		if err := h.lockOwnerOperation(ctx, owner, false); err != nil {
			return err
		}
		a := h.ownerRuns[owner]
		if a == nil || a.count == 0 {
			return nil
		}
		idle := a.idle
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.life.Done():
			return ErrHostClosed
		case <-idle:
		}
	}
}

// OwnerRunState is a read of the real current controller and committed cut for
// the local renderer supplier. It grants no attachment or remote caller right.
// The epoch read fence spans disk comparison only, never renderer/native waits.
func (h *SessionHost) OwnerRunState(ctx context.Context, lease ControllerLease) (CapturedOwnerRun, error) {
	if err := h.lockContext(ctx); err != nil {
		return CapturedOwnerRun{}, err
	}
	defer h.mu.Unlock()
	return h.ownerRunStateLocked(ctx, lease)
}
func (h *SessionHost) ownerRunStateLocked(ctx context.Context, lease ControllerLease) (CapturedOwnerRun, error) {
	id := lease.SessionID
	record, ok := h.sessions[id]
	if !ok {
		return CapturedOwnerRun{}, ErrProviderHostNotFound
	}
	internalRestore := h.nativeAccountingRestores[id] != nil && h.nativeAccountingAdmitted(ctx, id)
	internalGoalRecovery := h.goalRecoveryAdmittedLocked(ctx, id)
	if record.State != SessionRunning && !(record.State == SessionRecovering && (internalRestore || internalGoalRecovery)) || record.GracefulRetirement || record.ConditionalStop != nil || !h.nativeAccountingAdmitted(ctx, id) || !internalRestore && (h.attachmentChanging(id) || h.freshAttachmentReserved(id)) {
		return CapturedOwnerRun{}, ErrHostCapabilityUnavailable
	}
	if h.records.uncertain[id] {
		return CapturedOwnerRun{}, ErrLegacyCommitUncertain
	}
	if _, pending := h.pendingInitial[id]; pending {
		return CapturedOwnerRun{}, ErrLegacyCommitUncertain
	}
	input := h.inputs[id]
	if input == nil {
		return CapturedOwnerRun{}, ErrStaleInputController
	}
	current, err := input.CurrentLease(ctx, id)
	if err != nil {
		return CapturedOwnerRun{}, err
	}
	if current != lease {
		return CapturedOwnerRun{}, ErrStaleInputController
	}
	native := h.runners[id]
	if absentHostPort(native) {
		return CapturedOwnerRun{}, ErrHostCapabilityUnavailable
	}
	release, err := h.epochs.beginIO(ctx, lease)
	if err != nil {
		return CapturedOwnerRun{}, err
	}
	defer release()
	base, ok := h.records.bases[id]
	if !ok {
		return CapturedOwnerRun{}, ErrLegacyViewStale
	}
	path, err := h.records.path(id)
	if err != nil {
		return CapturedOwnerRun{}, err
	}
	if err = h.records.matchesCurrent(path, base); err != nil {
		return CapturedOwnerRun{}, err
	}
	return CapturedOwnerRun{Lease: lease, Input: input, Record: copyRunnerRecord(record), Committed: copyLegacyRecordImage(base), Native: native}, nil
}

// RunOwner preserves the original admission/persistence ordering, using the
// captured renderer rather than an unassigned legacy provider call. A run owns
// no map lock or cross-process I/O fence while waiting for native completion.
func (h *SessionHost) RunOwner(ctx context.Context, id, attachmentID string, q ProviderRunRequest) (result *ProviderRunResult, outcomeErr error) {
	id = strings.TrimSpace(id)
	if err := h.lockContext(ctx); err != nil {
		return nil, err
	}
	retired := refuseRetiredOwner(h.sessions[id].Owner)
	h.mu.Unlock()
	// Historical execution is immutable, including refusal-reason persistence.
	if retired != nil {
		return nil, retired
	}
	var admitted *CapturedOwnerRun
	var access *attachmentAccess
	var active bool
	var owner OwnerKey
	var finish func()
	defer func() {
		h.mu.Lock()
		if outcomeErr != nil && admitted != nil {
			clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			current, release, liveErr := h.admitOwnerRunCommitLocked(clean, *admitted)
			if liveErr == nil {
				reason := ownerRunErrorMessage(outcomeErr)
				if current.OwnerRunError != reason {
					next := copyRunnerRecord(current)
					next.OwnerRunError = reason
					next.UpdatedAt = time.Now().UTC()
					if err := h.updateRecord(clean, next); err != nil {
						outcomeErr = errors.Join(outcomeErr, &ownerRunPersistError{sessionID: id, err: err})
					} else {
						h.sessions[id] = next
					}
				}
				release()
			}
			cancel()
		}
		if active {
			h.endOwnerRunLocked(owner)
		}
		h.mu.Unlock()
		if outcomeErr != nil && filepath.IsAbs(h.options.Layout.OperatorLogFile) {
			_ = RecordOperatorError(h.options.Layout, "owner_run_failed", outcomeErr, map[string]any{"session_id": id})
		}
		if access != nil {
			_ = access.Release()
		}
		if finish != nil {
			finish()
		}
		if active {
			h.attachmentOps.Done()
		}
	}()
	if strings.TrimSpace(q.Prompt) == "" {
		return nil, fmt.Errorf("managed owner prompt is required")
	}
	var err error
	access, _, err = h.acquireAttachment(ctx, id, strings.TrimSpace(attachmentID))
	if err != nil {
		return nil, err
	}
	if err = h.lockContext(ctx); err != nil {
		return nil, err
	}
	owner = h.sessions[id].Owner
	h.mu.Unlock()
	if err = h.lockOwnerOperation(ctx, owner, true); err != nil {
		return nil, err
	}
	if absentHostPort(h.options.OwnerRuns) {
		h.mu.Unlock()
		return nil, fmt.Errorf("%w: captured owner renderer", ErrHostCapabilityUnavailable)
	}
	if err = refuseRetiredOwner(h.sessions[id].Owner); err != nil {
		h.mu.Unlock()
		return nil, err
	}
	initial, err := h.ownerRunStateLocked(ctx, access.Lease())
	if err != nil {
		h.mu.Unlock()
		return nil, err
	}
	admitted = &initial

	if err = h.reconcileHostGoalPhaseLocked(ctx, id, &initial, q); err != nil {
		h.mu.Unlock()
		return nil, fmt.Errorf("reconciling owner goal before managed run: %w", err)
	}
	// The original materialization writer lease is not a generic path permission.
	// It must be supplied by the actual catalog implementation before turn/start.
	if h.sessions[id].MaterializationCtxID != "" {
		h.mu.Unlock()
		return nil, fmt.Errorf("%w: materialization provider writer lease", ErrHostCapabilityUnavailable)
	}
	cut, err := h.ownerRunStateLocked(ctx, access.Lease())
	if err != nil {
		h.mu.Unlock()
		return nil, err
	}
	admitted = &cut
	supplier := h.options.OwnerRuns
	h.beginOwnerRunLocked(owner)
	h.attachmentOps.Add(1)
	active = true
	call, end := h.attachmentContext(ctx)
	finish = end
	h.mu.Unlock()
	result, outcomeErr = supplier.RunCapturedOwner(call, cut, q)
	if outcomeErr == nil && result == nil {
		outcomeErr = fmt.Errorf("%w: captured renderer returned no result", ErrHostCapabilityUnavailable)
	}
	h.mu.Lock()
	clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	current, release, liveErr := h.admitOwnerRunCommitLocked(clean, cut)
	if liveErr != nil {
		cancel()
		h.mu.Unlock()
		return result, errors.Join(outcomeErr, liveErr)
	}
	if outcomeErr == nil {
		result, outcomeErr = h.resolveOwnerGoalAckLocked(current, result)
	}
	next := copyRunnerRecord(current)
	next.UpdatedAt = time.Now().UTC()
	next.OwnerRunError = ""
	if outcomeErr != nil {
		next.OwnerRunError = ownerRunErrorMessage(outcomeErr)
	}
	snapshot, saveErr := cut.Native.Snapshot()
	if saveErr == nil {
		snapshot, saveErr = snapshotWithCommittedAccounting(next, snapshot)
	}
	if saveErr == nil {
		next.ProviderHost = ProviderHostRecord{Provider: next.Provider, Snapshot: copyRunnerSnapshot(snapshot)}
		saveErr = h.updateRecord(clean, next)
	}
	if saveErr != nil {
		outcomeErr = errors.Join(outcomeErr, &ownerRunPersistError{sessionID: id, err: saveErr})
	} else {
		h.sessions[id] = next
	}
	release()
	cancel()
	h.mu.Unlock()
	return result, outcomeErr
}

// The original scanner-timeout resolution trusts only the actual latest applied
// service head, never a native banner, pending receipt or stale applied revision.
func (h *SessionHost) resolveOwnerGoalAckLocked(record RunnerRecord, result *ProviderRunResult) (*ProviderRunResult, error) {
	if result == nil || result.ProviderStatus != "never_acknowledged" || record.AppliedGoalRef == nil || absentHostPort(h.options.Goals) {
		return result, nil
	}
	read, err := h.options.Goals.Get(record.Owner)
	if err != nil {
		return result, fmt.Errorf("checking goal acknowledgement ledger after scanner timeout: %w", err)
	}
	if read == nil || read.Pending || read.Latest == nil || read.Applied == nil || *read.Latest != *record.AppliedGoalRef || *read.Applied != *record.AppliedGoalRef {
		return result, nil
	}
	resolved := *result
	resolved.ExitCode = 0
	resolved.Status = ProviderRunComplete
	resolved.ProviderStatus = "ack_detection_disagreement"
	resolved.AckSource = "ledger"
	resolved.Detail = "Claude acknowledgement marker lagged the daemon ledger"
	resolved.Recoverable = false
	return &resolved, nil
}

// Every durable run effect is admitted against the captured incarnation. The
// short cross-process fence covers only snapshot/commit, never the provider run
// or native acknowledgement wait. Detach after admission does not revoke a turn;
// Stop, replacement, uncertain storage and pre-admission refusal do.
func (h *SessionHost) admitOwnerRunCommitLocked(ctx context.Context, cut CapturedOwnerRun) (RunnerRecord, func(), error) {
	id := cut.Lease.SessionID
	current, ok := h.sessions[id]
	input := h.inputs[id]
	if h.closed || !ok || current.State != SessionRunning || current.GracefulRetirement || current.ConditionalStop != nil || refuseRetiredOwner(current.Owner) != nil || h.records.uncertain[id] || input == nil || cut.Input != input || h.runners[id] != cut.Native || current.SessionID != cut.Record.SessionID || current.Owner != cut.Record.Owner || current.Provider != cut.Record.Provider || current.Contract != cut.Record.Contract || current.ProjectDir != cut.Record.ProjectDir || current.NamespaceFingerprint != cut.Record.NamespaceFingerprint || !current.CreatedAt.Equal(cut.Record.CreatedAt) || current.ProviderHost.Snapshot.Kind != cut.Record.ProviderHost.Snapshot.Kind {
		return RunnerRecord{}, nil, ErrStaleInputController
	}
	lease, e := input.CurrentLease(ctx, id)
	if e != nil || lease != cut.Lease {
		return RunnerRecord{}, nil, errors.Join(e, ErrStaleInputController)
	}
	release, e := h.epochs.beginIO(ctx, cut.Lease)
	if e != nil {
		return RunnerRecord{}, nil, e
	}
	base, ok := h.records.bases[id]
	if !ok {
		release()
		return RunnerRecord{}, nil, ErrLegacyViewStale
	}
	path, e := h.records.path(id)
	if e == nil {
		e = h.records.matchesCurrent(path, base)
	}
	if e != nil {
		release()
		return RunnerRecord{}, nil, e
	}
	return current, release, nil
}
