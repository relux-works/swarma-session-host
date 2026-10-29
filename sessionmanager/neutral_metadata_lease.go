package sessionmanager

import "context"

var _ HostInputController = (*SessionHost)(nil)

// CurrentLease returns only this host's issued controller. Before a native
// restore it may reserve a metadata-only epoch; that epoch has no input writer.
// A stale issued epoch is an explicit refusal, never an invitation to borrow
// the durable journal's newer head or silently grant a replacement controller.
func (h *SessionHost) CurrentLease(ctx context.Context, id string) (ControllerLease, error) {
	if err := h.lockContext(ctx); err != nil {
		return ControllerLease{}, err
	}
	defer h.mu.Unlock()
	if h.freshAttachmentReserved(id) {
		return ControllerLease{}, ErrLegacyCommitUncertain
	}
	if _, pending := h.pendingInitial[id]; pending || h.records.uncertain[id] {
		return ControllerLease{}, ErrLegacyCommitUncertain
	}
	record, ok := h.sessions[id]
	if !ok {
		return ControllerLease{}, ErrProviderHostNotFound
	}
	base, ok := h.records.bases[id]
	if !ok {
		return ControllerLease{}, ErrLegacyViewStale
	}
	path, err := h.records.path(id)
	if err != nil {
		return ControllerLease{}, err
	}
	if err := h.records.matchesCurrent(path, base); err != nil {
		return ControllerLease{}, err
	}
	current, err := h.records.decode(ctx, base)
	if err != nil {
		return ControllerLease{}, err
	}
	if current.SessionID != id || current.Owner != record.Owner || current.Provider != record.Provider {
		return ControllerLease{}, ErrLegacyViewStale
	}
	if _, err := recordMetadataPhase(current); err != nil {
		return ControllerLease{}, err
	}
	if input := h.inputs[id]; input != nil {
		return input.CurrentLease(ctx, id)
	}
	if lease, ok := h.metadataLeases[id]; ok {
		release, err := h.epochs.beginIO(ctx, lease)
		if err != nil {
			return ControllerLease{}, err
		}
		release()
		return lease, nil
	}
	epoch, err := h.epochs.reserveContext(ctx, id)
	if err != nil {
		return ControllerLease{}, err
	}
	lease := ControllerLease{SessionID: id, Epoch: epoch}
	if h.metadataLeases == nil {
		h.metadataLeases = map[string]ControllerLease{}
	}
	h.metadataLeases[id] = lease
	return lease, nil
}

// Commit never issues authority on the caller's behalf. The shared durable
// fence in CommitRecordMetadata rechecks this actual issued lease through ack.
func (h *SessionHost) issuedMetadataLease(ctx context.Context, id string) (ControllerLease, error) {
	if input := h.inputs[id]; input != nil {
		return input.CurrentLease(ctx, id)
	}
	lease, ok := h.metadataLeases[id]
	if !ok {
		return ControllerLease{}, ErrStaleInputController
	}
	return lease, nil
}
