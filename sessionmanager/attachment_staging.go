package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

// Classification and operation phase are host-private. A scalar token supplied
// by a caller never creates a reservation or changes fresh/reused ownership.
type preparedAttachment struct {
	token               AttachmentPreparationToken
	request             AttachRequest
	record              RunnerRecord
	runner              RunnerHost
	lease               ControllerLease
	fresh               bool
	phase               string
	writer              *attachmentWriter
	abortBase           string
	stopped             bool
	drained             bool
	finalBase           LegacyRecordInput
	discard             attachmentDiscardState
	cleanupFinalAttempt bool
	adoption            bool
}

var _ StagedAttachmentHost = (*SessionHost)(nil)

func (h *SessionHost) attachmentReserved(owner OwnerKey, id string) bool {
	for session, scope := range h.nativeAccountingRestores {
		if scope.owner == owner || id != "" && id == session {
			return true
		}
	}
	for session := range h.goalReconciling {
		if record := h.sessions[session]; record.Owner == owner || id != "" && session == id {
			return true
		}
	}
	for _, p := range h.preparations {
		if p.record.Owner == owner || id != "" && p.record.SessionID == id {
			return true
		}
	}
	return false
}

// Prepared Session/metadata cuts remain available to the fork's existing
// ports; native launch, commit, abort and failed recovery phases exclude them.
func (h *SessionHost) attachmentChanging(id string) bool {
	if h.nativeAccountingRestores[id] != nil {
		return true
	}
	for _, p := range h.preparations {
		if p.record.SessionID == id && p.phase != "prepared" {
			return true
		}
	}
	return false
}

func (h *SessionHost) freshAttachmentReserved(id string) bool {
	for _, p := range h.preparations {
		if p.fresh && p.record.SessionID == id {
			return true
		}
	}
	return false
}

// Preserve the operation's private codec context while binding cancellation
// to both its caller and this host lifetime. Input receipt ownership itself
// stays in the existing host writer, not this request context.
func (h *SessionHost) attachmentContext(ctx context.Context) (context.Context, func()) {
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(h.life, cancel)
	if h.life.Err() != nil {
		cancel()
	}
	return call, func() { stop(); cancel() }
}

// Reserve under the host lock; launch/snapshot outside it; revalidate and
// acknowledge actual bytes before returning a provisional writer, never a client.
func (h *SessionHost) PrepareAttach(ctx context.Context, q AttachRequest) (AttachmentPreparation, error) {
	if h == nil {
		return AttachmentPreparation{}, ErrHostClosed
	}
	ctx, finish := h.attachmentContext(ctx)
	defer finish()
	if err := h.lockOwnerOperation(ctx, q.Owner, true); err != nil {
		return AttachmentPreparation{}, err
	}
	h.attachmentOps.Add(1)
	defer h.attachmentOps.Done()
	p, epoch, err := h.reserveAttachment(ctx, q)
	h.mu.Unlock()
	if err != nil {
		return AttachmentPreparation{}, err
	}
	if p.adoption {
		p, epoch, err = h.prepareAttachmentAdoption(ctx, p)
		if err != nil {
			return AttachmentPreparation{}, err
		}
	}
	q = p.request
	if p.runner == nil {
		launch := ProviderLaunchRequest{SessionID: p.record.SessionID, Owner: p.record.Owner, Launch: copyLaunchSpec(q.Launch)}
		if !p.fresh {
			prior := p.record.ProviderHost
			prior.Snapshot = copyRunnerSnapshot(prior.Snapshot)
			launch.Prior = &prior
			launch.PriorMustFork = p.record.ProviderHostPendingFork
		}
		runner, launchErr := h.factory(q.Provider).Launch(ctx, launch)
		if absentHostPort(runner) {
			if launchErr == nil {
				launchErr = fmt.Errorf("nil launched runner")
			}
		}
		h.mu.Lock()
		if runner != nil {
			p.runner = runner
			h.runners[p.record.SessionID] = runner
		}
		if launchErr != nil {
			p.phase = "failed"
			h.mu.Unlock()
			return AttachmentPreparation{}, launchErr
		}
		h.mu.Unlock()
	}
	snapshot, snapshotErr := p.runner.Snapshot()
	h.mu.Lock()
	defer h.mu.Unlock()
	if snapshotErr != nil {
		p.phase = "failed"
		return AttachmentPreparation{}, snapshotErr
	}
	if h.closed || ctx.Err() != nil || p.phase != "preparing" || !h.epochs.admitsContext(ctx, p.record.SessionID, epoch) {
		p.phase = "failed"
		return AttachmentPreparation{}, errors.Join(ctx.Err(), ErrStaleInputController)
	}
	r := p.record
	snapshot, err = snapshotWithCommittedAccounting(r, snapshot)
	if err != nil {
		p.phase = "failed"
		return AttachmentPreparation{}, err
	}
	r.ProviderHost = ProviderHostRecord{Provider: r.Provider, Snapshot: copyRunnerSnapshot(snapshot)}
	r.State = SessionRunning
	r.Liveness = SessionLivenessLive
	r.LivenessDetail = "loaded provider host is live"
	r.GracefulRetirement = false
	r.Eviction = nil
	r.UpdatedAt = time.Now().UTC()
	release, fenceErr := h.epochs.beginIO(ctx, p.lease)
	if fenceErr != nil {
		p.phase = "failed"
		return AttachmentPreparation{}, fenceErr
	}
	if p.fresh {
		h.pendingInitial[r.SessionID] = pendingInitialRunner{record: copyRunnerRecord(r), epoch: epoch}
		err = h.records.create(ctx, r)
	} else {
		err = h.updateRecord(ctx, r)
	}
	release()
	if err != nil {
		p.record = copyRunnerRecord(r)
		p.phase = "failed"
		return AttachmentPreparation{}, err
	}
	delete(h.pendingInitial, r.SessionID)
	h.sessions[r.SessionID] = r
	p.record = copyRunnerRecord(r)
	input := h.inputs[r.SessionID]
	if input == nil {
		backend, ok := p.runner.(hostInputBackend)
		if !ok {
			p.phase = "failed"
			return AttachmentPreparation{}, ErrHostCapabilityUnavailable
		}
		input, err = h.installInput(ctx, r.SessionID, epoch, backend)
		if err != nil {
			p.phase = "failed"
			return AttachmentPreparation{}, err
		}
		h.inputs[r.SessionID] = input
	}
	p.lease, err = input.CurrentLease(ctx, r.SessionID)
	if err != nil {
		p.phase = "failed"
		return AttachmentPreparation{}, err
	}
	if err := h.restoreNativeAccountingInstalledLocked(ctx, r.SessionID); err != nil {
		p.phase = "failed"
		return AttachmentPreparation{}, err
	}
	p.record = copyRunnerRecord(h.sessions[r.SessionID])
	p.writer = newAttachmentWriter(h.life, input)
	p.phase = "prepared"
	return AttachmentPreparation{Token: p.token, Session: h.view(r), Lease: p.lease, Writer: p.writer.capability(), BaseSHA256: h.records.bases[r.SessionID].SHA256}, nil
}

func (h *SessionHost) reserveAttachment(ctx context.Context, q AttachRequest) (*preparedAttachment, ControllerEpoch, error) {
	if err := h.checkExpectedOwnerLeaseLocked(ctx, q.Owner, q.ExpectedOwnerLease); err != nil {
		return nil, 0, err
	}
	if !absentHostPort(h.options.Goals) && absentHostPort(h.options.GoalBindings) {
		return nil, 0, ErrHostCapabilityUnavailable
	}
	if q.Owner == "" || q.MaterializationCtxID != "" || q.Adopt && (q.Resume || q.ResumeSessionID != "") || q.ProjectDir != "" && !filepath.IsAbs(q.ProjectDir) || (q.ProjectDir == "") != (q.NamespaceFingerprint == "") {
		return nil, 0, ErrInvalidInput
	}
	provider, err := normalizeProvider(q.Provider)
	if err != nil || provider != q.Provider || h.factory(provider) == nil {
		return nil, 0, ErrHostCapabilityUnavailable
	}
	if err := validateLaunchPayload(provider, q.Launch); err != nil {
		return nil, 0, err
	}

	if h.attachmentReserved(q.Owner, q.ResumeSessionID) {
		return nil, 0, ErrLegacyCommitUncertain
	}
	for id, p := range h.pendingInitial {
		if p.record.Owner == q.Owner || id == q.ResumeSessionID {
			return nil, 0, ErrLegacyCommitUncertain
		}
	}
	var record RunnerRecord
	if q.ResumeSessionID != "" {
		record = h.sessions[q.ResumeSessionID]
		if record.SessionID == "" || record.Owner != q.Owner || record.Provider != provider {
			return nil, 0, ErrProviderHostNotFound
		}
		if err := h.checkExplicitResumeOwnerSlot(record, provider); err != nil {
			return nil, 0, err
		}
	}
	if record.SessionID == "" {
		for _, r := range h.sessions {
			if r.Owner == q.Owner && r.State != SessionStopped {
				record = r
				break
			}
		}
	}
	if record.SessionID != "" {
		if h.records.uncertain[record.SessionID] || record.ConditionalStop != nil {
			return nil, 0, ErrLegacyCommitUncertain
		}
		if record.Provider != provider {
			return nil, 0, providerSlotConflict(record, provider)
		}
		if record.GracefulRetirement && record.State != SessionStopped {
			return nil, 0, ErrHostCapabilityUnavailable
		}
		if q.Adopt && (h.view(record).AttachedClients != 0 || record.GracefulRetirement || record.ConditionalStop != nil || record.ProviderHostPendingFork) {
			return nil, 0, ErrInvalidInput
		}
		base, ok := h.records.bases[record.SessionID]
		if !ok {
			return nil, 0, ErrLegacyViewStale
		}
		path, err := h.records.path(record.SessionID)
		if err != nil {
			return nil, 0, err
		}
		if err := h.records.matchesCurrent(path, base); err != nil {
			return nil, 0, err
		}
	}
	fresh := record.SessionID == ""
	if fresh {
		ack, ok := h.options.Codec.(LegacyRecordDiscardAcknowledger)
		if !ok || absentHostPort(ack) {
			return nil, 0, ErrHostCapabilityUnavailable
		}
		id, err := opaqueID("SES")
		if err != nil {
			return nil, 0, err
		}
		now := time.Now().UTC()
		record = RunnerRecord{Contract: sessionRecordContract, SessionID: id, Owner: q.Owner, Provider: provider, ProjectDir: q.ProjectDir, NamespaceFingerprint: q.NamespaceFingerprint, State: SessionRunning, Liveness: SessionLivenessLive, CreatedAt: now, UpdatedAt: now}
	}
	if q.Adopt && !fresh {
		ack, ok := h.options.Codec.(LegacyRecordDiscardAcknowledger)
		if !ok || absentHostPort(ack) {
			return nil, 0, ErrHostCapabilityUnavailable
		}
		token, err := opaqueID("PREP")
		if err != nil {
			return nil, 0, err
		}
		p := &preparedAttachment{token: AttachmentPreparationToken(token), request: q, record: copyRunnerRecord(record), runner: h.runners[record.SessionID], adoption: true, phase: "adoption"}
		if h.preparations == nil {
			h.preparations = map[AttachmentPreparationToken]*preparedAttachment{}
		}
		h.preparations[p.token] = p
		return p, 0, nil
	}
	runner := h.runners[record.SessionID]
	if !fresh && runner == nil {
		if err := validateCodexBudgetRelaunch(record.ProviderHost); err != nil {
			return nil, 0, err
		}
		if err := h.validateNativeAccountingRestoreLocked(ctx, record.SessionID); err != nil {
			return nil, 0, err
		}
	}
	var epoch ControllerEpoch
	if input := h.inputs[record.SessionID]; input != nil {
		lease, err := input.CurrentLease(ctx, record.SessionID)
		if err != nil {
			return nil, 0, err
		}
		epoch = lease.Epoch
	} else {
		epoch, err = h.epochs.reserveContext(ctx, record.SessionID)
		if err != nil {
			return nil, 0, err
		}
	}
	token, err := opaqueID("PREP")
	if err != nil {
		return nil, 0, err
	}
	q.Launch = copyLaunchSpec(q.Launch)
	p := &preparedAttachment{token: AttachmentPreparationToken(token), request: q, record: copyRunnerRecord(record), runner: runner, fresh: fresh, phase: "preparing", lease: ControllerLease{SessionID: record.SessionID, Epoch: epoch}}
	if h.preparations == nil {
		h.preparations = map[AttachmentPreparationToken]*preparedAttachment{}
	}
	h.preparations[p.token] = p
	return p, epoch, nil
}

func (h *SessionHost) checkAttachment(ctx context.Context, token AttachmentPreparationToken, lease ControllerLease, hash string, abort bool) (*preparedAttachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := h.preparations[token]
	if p == nil || p.token != token || p.lease != lease || hash == "" || !h.epochs.admitsContext(ctx, lease.SessionID, lease.Epoch) {
		return nil, ErrStaleInputController
	}
	if abort && p.phase == "aborting" && p.abortBase == hash {
		return p, nil
	}
	if p.phase != "prepared" || h.records.uncertain[lease.SessionID] || h.sessions[lease.SessionID].ConditionalStop != nil {
		return nil, ErrLegacyCommitUncertain
	}
	if h.sessions[lease.SessionID].State != SessionRunning {
		return nil, ErrHostCapabilityUnavailable
	}
	base := h.records.bases[lease.SessionID]
	if base.SHA256 != hash {
		return nil, ErrLegacyViewStale
	}
	path, err := h.records.path(lease.SessionID)
	if err != nil {
		return nil, err
	}
	if err := h.records.matchesCurrent(path, base); err != nil {
		return nil, err
	}
	return p, nil
}

func (h *SessionHost) CommitAttach(ctx context.Context, q AttachmentCommit) (*Attachment, error) {
	if h == nil {
		return nil, ErrHostClosed
	}
	ctx, finish := h.attachmentContext(ctx)
	defer finish()
	if err := h.lockContext(ctx); err != nil {
		return nil, err
	}
	h.attachmentOps.Add(1)
	defer h.attachmentOps.Done()
	p, err := h.checkAttachment(ctx, q.Token, q.Lease, q.BaseSHA256, false)
	if err != nil {
		h.mu.Unlock()
		return nil, err
	}
	p.phase = "committing"
	h.mu.Unlock()
	err = p.writer.close(ctx)
	h.mu.Lock()
	defer h.mu.Unlock()
	p.phase = "prepared"
	if err != nil {
		return nil, err
	}
	if h.closed {
		return nil, ErrHostClosed
	}
	if _, err := h.checkAttachment(ctx, q.Token, q.Lease, q.BaseSHA256, false); err != nil {
		return nil, err
	}
	if err := h.reconcileHostGoalLocked(ctx, p.record.SessionID); err != nil {
		p.phase = "failed"
		return nil, err
	}
	release, err := h.epochs.beginIO(ctx, q.Lease)
	if err != nil {
		return nil, err
	}
	defer release()
	h.rememberAttachmentController(p.record.SessionID, p.lease.Epoch)
	attachment, err := h.attachmentDetailsLocked(ctx, p.record.SessionID)
	if err != nil {
		return nil, err
	}
	id, err := h.issueAttachment(ctx, p.record.SessionID)
	if err != nil {
		return nil, err
	}
	delete(h.preparations, q.Token)
	h.nudgeSessionNoticeDelivery()
	attachment.AttachmentID = id
	attachment.Session = h.view(h.sessions[p.record.SessionID])
	return attachment, nil
}

func (h *SessionHost) AbortAttach(ctx context.Context, q AttachmentAbort) (AttachmentAbortReceipt, error) {
	if h == nil {
		return AttachmentAbortReceipt{}, ErrHostClosed
	}
	ctx, finish := h.attachmentContext(ctx)
	defer finish()
	if err := h.lockContext(ctx); err != nil {
		return AttachmentAbortReceipt{}, err
	}
	h.attachmentOps.Add(1)
	defer h.attachmentOps.Done()
	p, err := h.checkAttachment(ctx, q.Token, q.Lease, q.BaseSHA256, true)
	if err != nil {
		h.mu.Unlock()
		return AttachmentAbortReceipt{}, err
	}
	if p.phase == "aborting-busy" {
		h.mu.Unlock()
		return AttachmentAbortReceipt{}, ErrLegacyCommitUncertain
	}
	p.phase = "aborting-busy"
	p.abortBase = q.BaseSHA256
	release, fenceErr := h.epochs.beginIO(ctx, q.Lease)
	if fenceErr != nil {
		p.phase = "aborting"
		h.mu.Unlock()
		return AttachmentAbortReceipt{}, fenceErr
	}
	defer release()
	if h.records.uncertain[p.record.SessionID] {
		expected := h.records.pendingReplacement[p.record.SessionID]
		recovered, recoverErr := h.records.recoverAttachmentReplacement(ctx, p.record.SessionID)
		if recoverErr != nil {
			p.phase = "aborting"
			h.mu.Unlock()
			return AttachmentAbortReceipt{}, recoverErr
		}
		h.sessions[p.record.SessionID] = recovered
		if p.cleanupFinalAttempt && h.records.bases[p.record.SessionID].SHA256 == expected.SHA256 {
			p.finalBase = copyLegacyRecordImage(h.records.bases[p.record.SessionID])
		}
	}
	input := h.inputs[p.record.SessionID]
	h.mu.Unlock()
	err = p.writer.abort(ctx)
	if err == nil && p.fresh && !p.drained {
		if input == nil {
			err = ErrStaleInputController
		} else {
			err = input.revokeAndDrain(ctx)
			if err == nil {
				p.drained = true
			}
		}
	}
	if err == nil && p.fresh && !p.stopped {
		err = p.runner.Stop(ctx)
		if err == nil {
			p.stopped = true
		}
	}
	var snapshot RunnerSnapshot
	if err == nil && p.finalBase.SHA256 == "" {
		snapshot, err = p.runner.Snapshot()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	p.phase = "aborting"
	if err != nil {
		return AttachmentAbortReceipt{}, err
	}
	if h.closed || ctx.Err() != nil || !h.epochs.admitsContext(ctx, q.Lease.SessionID, q.Lease.Epoch) {
		return AttachmentAbortReceipt{}, errors.Join(ctx.Err(), ErrStaleInputController)
	}
	if p.finalBase.SHA256 == "" {
		r := h.sessions[p.record.SessionID]
		r.ProviderHost = ProviderHostRecord{Provider: r.Provider, Snapshot: copyRunnerSnapshot(snapshot)}
		r.State = SessionRecovering
		if p.fresh {
			r.State = SessionStopped
			r.Liveness = SessionLivenessDead
		}
		r.UpdatedAt = time.Now().UTC()
		p.cleanupFinalAttempt = true
		if err := h.updateRecord(ctx, r); err != nil {
			return AttachmentAbortReceipt{}, err
		}
		h.sessions[r.SessionID] = r
		p.finalBase = copyLegacyRecordImage(h.records.bases[r.SessionID])
	}
	receipt := AttachmentAbortReceipt{Token: q.Token, Lease: q.Lease, BaseSHA256: q.BaseSHA256, Disposition: AttachmentAbortPreservedRecovering, CommittedSHA256: p.finalBase.SHA256}
	if p.fresh {
		err = h.records.discardAttachment(ctx, string(h.options.Layout.Namespace), p.finalBase, &p.discard)
		if err != nil {
			return AttachmentAbortReceipt{}, errors.Join(ErrLegacyCommitUncertain, err)
		}
		delete(h.sessions, p.record.SessionID)
		delete(h.runners, p.record.SessionID)
		delete(h.inputs, p.record.SessionID)
		receipt.Disposition = AttachmentAbortDiscardedFresh
		receipt.CommittedSHA256 = ""
		receipt.DiscardedSHA256 = p.finalBase.SHA256
	}
	delete(h.preparations, q.Token)
	return receipt, nil
}

// Recovery first closes every provisional capability without holding h.mu.
// Checked abort retries resolve owned disposal before invalidating its token.
func (h *SessionHost) invalidateAttachmentPreparations(ctx context.Context) error {
	if err := h.lockContext(ctx); err != nil {
		return err
	}
	var writers []*attachmentWriter
	for _, p := range h.preparations {
		if p.phase == "preparing" || p.phase == "adoption" || p.phase == "committing" || p.phase == "aborting-busy" {
			h.mu.Unlock()
			return ErrLegacyCommitUncertain
		}
		if p.phase == "aborting" {
			h.mu.Unlock()
			if _, err := h.AbortAttach(ctx, AttachmentAbort{Token: p.token, Lease: p.lease, BaseSHA256: p.abortBase}); err != nil {
				return err
			}
			return h.invalidateAttachmentPreparations(ctx)
		}
		if p.phase == "failed" && h.pendingInitial[p.record.SessionID].record.SessionID == "" && h.sessions[p.record.SessionID].SessionID == "" {
			if p.runner == nil {
				h.mu.Unlock()
				return ErrLegacyCommitUncertain
			}
			h.mu.Unlock()
			snapshot, err := p.runner.Snapshot()
			if err != nil {
				return err
			}
			if err := h.lockContext(ctx); err != nil {
				return err
			}
			if p.phase != "failed" {
				h.mu.Unlock()
				return ErrLegacyCommitUncertain
			}
			r := copyRunnerRecord(p.record)
			r.ProviderHost = ProviderHostRecord{Provider: r.Provider, Snapshot: copyRunnerSnapshot(snapshot)}
			r.UpdatedAt = time.Now().UTC()
			h.pendingInitial[r.SessionID] = pendingInitialRunner{record: r, epoch: p.lease.Epoch}
			if err := h.records.create(ctx, r); err != nil {
				h.mu.Unlock()
				return err
			}
			h.sessions[r.SessionID] = r
			delete(h.pendingInitial, r.SessionID)
		}
		p.phase = "invalid"
		if p.writer != nil {
			writers = append(writers, p.writer)
		}
	}
	h.mu.Unlock()
	for _, w := range writers {
		if err := w.abort(ctx); err != nil {
			return err
		}
	}
	if err := h.lockContext(ctx); err != nil {
		return err
	}
	defer h.mu.Unlock()
	for _, p := range h.preparations {
		if p.phase != "invalid" {
			continue
		}
		if input := h.inputs[p.record.SessionID]; input != nil {
			_, err := input.CurrentLease(ctx, p.record.SessionID)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// An actual external reservation already revoked/drained this
				// expired input. Recovery must issue from the durable new epoch.
				if !errors.Is(err, ErrStaleInputController) {
					return err
				}
				delete(h.inputs, p.record.SessionID)
			}
		}
	}
	return nil
}
