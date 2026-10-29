package sessionmanager

import (
	"context"
	"encoding/json"
)

// This context token is private to the module's transport and never crosses a
// renderer port. It asks dispatch to seed a ring at the resume response frame,
// before any following live frame can be published.
type museObservationResumeKey struct{}

func (b *museInputBackend) NativeIdentity() string { return b.host.sessionID }
func (b *museInputBackend) SubscribeMuse(ctx context.Context, lease ControllerLease) (MuseObservationFeed, error) {
	c := b.host.client
	if err := b.host.ensureRunning(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	ring := c.observations
	// A canceled/failed initial read exposed no feed or sequence. A new
	// subscription may retry that read; an already exposed stream never resets.
	if ring != nil {
		ring.mu.Lock()
		unexposed := !ring.ready && ring.terminal != nil
		ring.mu.Unlock()
		if unexposed && ring.lease == lease {
			ring = nil
		}
	}
	if ring != nil && ring.lease != lease {
		if ring.lease.SessionID != lease.SessionID || ring.lease.Epoch >= lease.Epoch {
			c.mu.Unlock()
			return nil, ErrStaleInputController
		}
		ring.close(ErrStaleInputController)
		ring = nil
	}
	initialize := ring == nil
	if initialize {
		ring = newMuseObservationRing(lease, b.host.sessionID)
		ring.requestedApproval, ring.requestedModel = b.host.requestedApproval, b.host.requestedModel
		c.observations = ring
	}
	c.mu.Unlock()
	if initialize {
		var result MSPSessionResumeResult
		read, cancel := b.host.goalAckContext(ctx)
		_, err := c.callSequenced(context.WithValue(read, museObservationResumeKey{}, ring), "session/resume", MSPSessionResumeParams{CommandID: museUUIDv7(), SessionID: b.host.sessionID, History: MSPHistorySnapshot}, &result)
		cancel()
		if err != nil {
			ring.close(err)
			return nil, err
		}
	}
	return ring.subscribe(ctx, lease)
}

func (c *MSPClient) publishMuseObservation(event MSPNotification) {
	c.mu.Lock()
	ring := c.observations
	c.mu.Unlock()
	if ring != nil {
		ring.publish(event)
	}
}

func (r *museObservationRing) restoreResponse(raw json.RawMessage, strict bool) error {
	if strict {
		if err := museStrictDecodeMethodResult("session/resume", raw); err != nil {
			return err
		}
	}
	var result MSPSessionResumeResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return ErrMuseObservationInvalid
	}
	if result.Session.SessionID != r.nativeID {
		return ErrMuseObservationInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal != nil {
		return r.terminal
	}
	if r.ready {
		return ErrMuseObservationInvalid
	}
	// Conversion and ready publication share the producer lock. dispatch invokes
	// this before waking the RPC waiter and before reading a subsequent live frame.
	fold, err := convertMuseRestoreObservations(result, r.requestedApproval, r.requestedModel)
	if err != nil {
		r.appendLocked(nil, "restore-incomplete")
	} else {
		r.appendValueLocked(MuseObservation{RestoreFold: fold})
		r.foldCursor = fold.Cursor
		r.restoreUsageTotal = fold.Cumulative.TotalTokens
	}
	r.ready = true
	r.signalLocked()
	return nil
}
