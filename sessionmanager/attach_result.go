package sessionmanager

import "context"

// AttachmentResultReceipt is local custody of one Attach result which has not
// yet been returned to its caller. Only its originating host can mint/consume
// the registered pointer; none of these facts can be supplied in a wire body.
type AttachmentResultReceipt struct {
	attachmentID string
	lease        ControllerLease
	record       RunnerRecord
	input        *sessionInput
	runner       RunnerHost
	fresh        bool
	refused      bool
	rollingBack  bool
}

// AttachWithResult performs the ordinary Attach transaction. It additionally
// captures actual provenance before releasing admission, allowing an enclosing
// verified-source transaction to unwind its own unpublished result afterward.
func (h *SessionHost) AttachWithResult(ctx context.Context, request AttachRequest) (*Attachment, *AttachmentResultReceipt, error) {
	if err := h.lockOwnerOperation(ctx, request.Owner, true); err != nil {
		return nil, nil, err
	}
	defer h.mu.Unlock()
	receipt := &AttachmentResultReceipt{}
	attachment, err := h.attachLocked(ctx, request, receipt)
	if err != nil {
		return nil, nil, err
	}
	receipt.attachmentID = attachment.AttachmentID
	receipt.lease = h.issuedAttachments[attachment.AttachmentID]
	receipt.record = copyRunnerRecord(h.sessions[attachment.Session.SessionID])
	receipt.input = h.inputs[attachment.Session.SessionID]
	receipt.runner = h.runners[attachment.Session.SessionID]
	if h.attachResults == nil {
		h.attachResults = map[*AttachmentResultReceipt]struct{}{}
	}
	h.attachResults[receipt] = struct{}{}
	return attachment, receipt, nil
}

// CommitAttachResult releases only result custody after the enclosing source,
// response projection and cancellation checks succeed. It grants no authority.
func (h *SessionHost) CommitAttachResult(ctx context.Context, r *AttachmentResultReceipt) error {
	if err := h.lockContext(ctx); err != nil {
		return err
	}
	defer h.mu.Unlock()
	if _, ok := h.attachResults[r]; !ok || r == nil {
		return ErrInvalidInput
	}
	if r.rollingBack {
		return ErrLegacyCommitUncertain
	}
	if h.attachments[r.attachmentID] != r.lease.SessionID {
		return ErrStaleInputController
	}
	if err := h.checkAttachResultCurrentLocked(ctx, r); err != nil {
		return err
	}
	delete(h.attachResults, r)
	h.releaseRefusedAttachResultsLocked(r, nil)
	return nil
}

// Captured incarnation identity is independent of later snapshot/accounting
// updates. A retained result never transfers ownership to another lease/runner.
func sameAttachResultIncarnation(a, b *AttachmentResultReceipt) bool {
	return a.lease == b.lease && a.record.Owner == b.record.Owner && a.record.Provider == b.record.Provider && a.record.Contract == b.record.Contract && a.record.CreatedAt.Equal(b.record.CreatedAt) && a.input == b.input && sameProxyRunner(a.runner, b.runner)
}

// Successful publication or conditional cleanup resolves refused custody for
// this incarnation. Still-pending and other in-flight receipts remain registered.
func (h *SessionHost) releaseRefusedAttachResultsLocked(current, origin *AttachmentResultReceipt) {
	for other := range h.attachResults {
		if other.refused && sameAttachResultIncarnation(current, other) && (!other.rollingBack || other == current || other == origin) {
			delete(h.attachResults, other)
		}
	}
}

func (h *SessionHost) checkAttachResultCurrentLocked(ctx context.Context, r *AttachmentResultReceipt) error {
	id := r.lease.SessionID
	current, ok := h.sessions[id]
	if !ok || current.State != SessionRunning || current.GracefulRetirement || current.ConditionalStop != nil || current.Owner != r.record.Owner || current.Provider != r.record.Provider || current.Contract != r.record.Contract || !current.CreatedAt.Equal(r.record.CreatedAt) || h.attachmentControllers[id] != r.lease || !sameProxyRunner(h.runners[id], r.runner) || h.inputs[id] != r.input {
		return ErrStaleInputController
	}
	if h.records.uncertain[id] {
		return ErrLegacyCommitUncertain
	}
	release, err := h.epochs.beginIO(ctx, r.lease)
	if err != nil {
		return err
	}
	release()
	return nil
}

// RollbackAttachResult removes only its own undisclosed client. Retirement is
// conditional on host-captured new-record provenance and the exact current
// issued controller/native/input/incarnation, with no other client or work.
// A cleanup failure retains custody and the original durable uncertainty.
func (h *SessionHost) RollbackAttachResult(ctx context.Context, r *AttachmentResultReceipt) error {
	if err := h.lockContext(ctx); err != nil {
		return err
	}
	defer h.mu.Unlock()
	if _, ok := h.attachResults[r]; !ok || r == nil {
		return ErrInvalidInput
	}
	id := r.lease.SessionID
	if h.attachments[r.attachmentID] == id {
		delete(h.attachments, r.attachmentID)
	}
	if r.rollingBack {
		return ErrLegacyCommitUncertain
	}
	r.refused = true
	r.rollingBack = true
	defer func() { r.rollingBack = false }()
	if err := h.checkAttachResultCurrentLocked(ctx, r); err != nil {
		return err
	}
	current := h.sessions[id]
	origin := r
	if !r.fresh {
		origin = nil
		for other := range h.attachResults {
			if other.fresh && other.refused && sameAttachResultIncarnation(r, other) {
				if other.rollingBack {
					return ErrLegacyCommitUncertain
				}
				if err := h.checkAttachResultCurrentLocked(ctx, other); err != nil {
					return err
				}
				origin = other
				break
			}
		}
		if origin == nil {
			delete(h.attachResults, r)
			return nil
		}
		origin.rollingBack = true
		defer func() { origin.rollingBack = false }()
	}
	for attachment, session := range h.attachments {
		if session == id {
			for other := range h.attachResults {
				if other != r && other.attachmentID == attachment {
					// Another unpublished result is not a genuine surviving client.
					// Its Commit/Rollback re-evaluates this refused fresh origin.
					return ErrLegacyCommitUncertain
				}
			}
			h.releaseRefusedAttachResultsLocked(r, origin)
			return nil
		}
	}
	for access := range h.attachmentAccesses {
		if access.lease == r.lease {
			h.releaseRefusedAttachResultsLocked(r, origin)
			return nil
		}
	}
	if h.attachmentReserved(current.Owner, id) || h.ownerRuns[current.Owner] != nil || h.ownerOperations[current.Owner] != nil || h.ownerActivations[current.Owner] != nil {
		return ErrLegacyCommitUncertain
	}
	// Validation and this original conditional stop remain one map transaction.
	// The stop helper keeps existing observer join/reservation/durable ACK order.
	if _, err := h.stopWithDetailLocked(ctx, id, "unpublished attachment result rolled back"); err != nil {
		return err
	}
	h.releaseRefusedAttachResultsLocked(r, origin)
	return nil
}
