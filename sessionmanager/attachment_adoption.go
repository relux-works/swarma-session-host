package sessionmanager

import (
	"context"
	"time"
)

// Reserve the occupied owner while observing the actual native identity outside
// the host lock. Only definite physical absence and an acknowledged stopped
// record permit replacing that reservation with a fresh native preparation.
func (h *SessionHost) prepareAttachmentAdoption(ctx context.Context, p *preparedAttachment) (*preparedAttachment, ControllerEpoch, error) {
	absent, err := observeDeadAdoptionIncumbent(ctx, p.record, p.runner)
	h.mu.Lock()
	if err != nil || !absent {
		delete(h.preparations, p.token)
		h.mu.Unlock()
		if err != nil {
			return nil, 0, err
		}
		return nil, 0, ErrInvalidInput
	}
	if h.closed || ctx.Err() != nil || p.phase != "adoption" {
		p.phase = "failed"
		h.mu.Unlock()
		return nil, 0, ErrHostClosed
	}
	current := h.sessions[p.record.SessionID]
	if current.SessionID != p.record.SessionID || current.Owner != p.record.Owner || h.view(current).AttachedClients != 0 || current.GracefulRetirement || current.ConditionalStop != nil || current.ProviderHostPendingFork {
		delete(h.preparations, p.token)
		h.mu.Unlock()
		return nil, 0, ErrLegacyViewStale
	}
	stopped := copyRunnerRecord(current)
	stopped.ProviderHost.Snapshot.Codex.Stopped = true
	stopped.State = SessionStopped
	stopped.Liveness = SessionLivenessDead
	stopped.LivenessDetail = "recorded native process, process group and socket are absent"
	stopped.UpdatedAt = time.Now().UTC()
	if err := h.updateRecord(ctx, stopped); err != nil {
		p.phase = "failed"
		h.mu.Unlock()
		return nil, 0, err
	}
	h.sessions[current.SessionID] = stopped
	delete(h.metadataLeases, current.SessionID)
	input := h.inputs[current.SessionID]
	h.mu.Unlock()
	if input != nil {
		if err := input.revokeAndDrain(ctx); err != nil {
			h.mu.Lock()
			p.phase = "failed"
			h.mu.Unlock()
			return nil, 0, err
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.inputs, current.SessionID)
	if h.closed || ctx.Err() != nil {
		p.phase = "failed"
		return nil, 0, ErrHostClosed
	}
	// This is one locked transition: no concurrent Attach sees a free owner
	// between the acknowledged dead incumbent and its new reservation.
	delete(h.preparations, p.token)
	return h.reserveAttachment(ctx, p.request)
}
