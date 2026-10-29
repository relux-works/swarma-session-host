package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const ErrorSessionTakeoverObservationChanged = "session_takeover_observation_changed"
const ErrorSessionTakeoverNotApplicable = "session_takeover_not_applicable"
const ErrorSessionLivenessUnknown = "session_liveness_unknown"

// SessionObservation is exactly the disclosure, excluding unrelated UpdatedAt.
type SessionObservation struct {
	Provider        string          `json:"provider"`
	State           SessionState    `json:"state"`
	Liveness        SessionLiveness `json:"liveness,omitempty"`
	LivenessDetail  string          `json:"liveness_detail,omitempty"`
	AttachedClients int             `json:"attached_clients"`
}

func ObservationOf(v SessionView) SessionObservation {
	return SessionObservation{v.Provider, v.State, v.Liveness, v.LivenessDetail, v.AttachedClients}
}
func (o SessionObservation) equal(v SessionObservation) bool { return o == v }
func (o SessionObservation) describe() string {
	return fmt.Sprintf("provider=%s state=%s liveness=%s (%s) attached_clients=%d", o.Provider, o.State, o.Liveness, o.LivenessDetail, o.AttachedClients)
}

// Owner is already verified in-process routing, not a new caller grant.
type TakeoverRequest struct {
	Owner     OwnerKey
	Provider  string
	SessionID string
	Observed  SessionObservation
}

func (h *SessionHost) TakeOverOwnerSlot(ctx context.Context, q TakeoverRequest) (outcome SessionView, outcomeErr error) {
	defer func() {
		if outcomeErr != nil && filepath.IsAbs(h.options.Layout.OperatorLogFile) {
			event := "session_takeover_failed"
			var p *PublicError
			if errors.As(outcomeErr, &p) {
				event = "session_takeover_refused"
			}
			_ = RecordOperatorError(h.options.Layout, event, outcomeErr, map[string]any{"session_id": q.SessionID})
		}
	}()
	if strings.TrimSpace(string(q.Owner)) == "" {
		return SessionView{}, ErrInvalidInput
	}
	if e := refuseRetiredOwner(q.Owner); e != nil {
		return SessionView{}, e
	}
	provider, e := normalizeProvider(q.Provider)
	if e != nil {
		return SessionView{}, e
	}
	if h.factory(provider) == nil {
		return SessionView{}, fmt.Errorf("provider host factory %q is not registered", provider)
	}
	if e = h.lockOwnerOperation(ctx, q.Owner, true); e != nil {
		return SessionView{}, e
	}
	defer h.mu.Unlock()
	changed := func(message string) error {
		return &PublicError{Code: ErrorSessionTakeoverObservationChanged, Message: message}
	}
	var r RunnerRecord
	for _, candidate := range h.sessions {
		if candidate.Owner == q.Owner && candidate.State != SessionStopped {
			if r.SessionID != "" {
				return SessionView{}, ErrLegacyCommitUncertain
			}
			r = candidate
		}
	}
	if r.SessionID == "" || r.SessionID != q.SessionID {
		return SessionView{}, changed(fmt.Sprintf("owner %s is no longer held by session %s; re-run the wrapper so the disclosure matches the current owner slot", q.Owner, q.SessionID))
	}
	if r.Provider == provider {
		return SessionView{}, &PublicError{Code: ErrorSessionTakeoverNotApplicable, Message: fmt.Sprintf("session %s already belongs to provider %s; attach to it instead of taking it over", r.SessionID, provider)}
	}
	if h.attachmentReserved(q.Owner, r.SessionID) || h.records.uncertain[r.SessionID] || r.ConditionalStop != nil || r.GracefulRetirement {
		return SessionView{}, ErrLegacyCommitUncertain
	}
	released, e := h.reconcileTakeoverLivenessLocked(ctx, r)
	if e != nil {
		return SessionView{}, e
	}
	r = h.sessions[r.SessionID]
	if released {
		return SessionView{}, changed(fmt.Sprintf("session %s was observed dead and released the owner slot on its own; re-run the wrapper, which now attaches without a takeover", r.SessionID))
	}
	if r.Liveness == SessionLivenessUnknown {
		return SessionView{}, &PublicError{Code: ErrorSessionLivenessUnknown, Message: fmt.Sprintf("session %s keeps the owner slot because provider liveness is unknown: %s", r.SessionID, r.LivenessDetail)}
	}
	current := ObservationOf(h.view(r))
	if !current.equal(q.Observed) {
		return SessionView{}, changed(fmt.Sprintf("session %s changed after the takeover was disclosed (disclosed: %s; now: %s); nothing was stopped, re-run the wrapper", r.SessionID, q.Observed.describe(), current.describe()))
	}
	r.Eviction = &SessionEvictionNotice{ByProvider: provider, At: time.Now().UTC(), Reason: fmt.Sprintf("the %s owner slot was taken over by provider %s at this terminal", q.Owner, provider)}
	// The notice is committed with original Stop intent, never an earlier success.
	original := h.sessions[r.SessionID]
	h.sessions[r.SessionID] = r
	v, e := h.stopWithDetailLocked(ctx, r.SessionID, "")
	if e != nil {
		current := h.sessions[r.SessionID]
		current.Eviction = original.Eviction
		h.sessions[r.SessionID] = current
		return SessionView{}, e
	}
	if filepath.IsAbs(h.options.Layout.OperatorLogFile) {
		_ = RecordOperatorInfo(h.options.Layout, "session_lifecycle", map[string]any{"session_id": r.SessionID, "provider": r.Provider, "transition": "evicted", "taken_over_by": provider})
	}
	return v, nil
}

func (h *SessionHost) persistTakeoverLivenessLocked(ctx context.Context, r RunnerRecord, o sessionLivenessObservation) error {
	if r.Liveness == o.State && r.LivenessDetail == o.Detail {
		return nil
	}
	r.Liveness = o.State
	r.LivenessDetail = o.Detail
	r.UpdatedAt = time.Now().UTC()
	if e := h.updateRecord(ctx, r); e != nil {
		return e
	}
	h.sessions[r.SessionID] = r
	return nil
}
func (h *SessionHost) reconcileTakeoverLivenessLocked(ctx context.Context, r RunnerRecord) (bool, error) {
	if native := h.runners[r.SessionID]; native != nil {
		state, e := native.State(ctx)
		if e != nil {
			return false, h.persistTakeoverLivenessLocked(ctx, r, sessionLivenessObservation{SessionLivenessUnknown, "loaded provider host state could not be read: " + e.Error()})
		}
		if state == ProviderHostRunning {
			return false, h.persistTakeoverLivenessLocked(ctx, r, sessionLivenessObservation{SessionLivenessLive, "loaded provider host is live"})
		}
	}
	o, applicable := observeHostProviderLiveness(ctx, r)
	if !applicable {
		return false, nil
	}
	if o.State == SessionLivenessDead && (h.view(r).AttachedClients > 0 || h.ownerRuns[r.Owner] != nil || h.ownerActivations[r.Owner] != nil) {
		o = sessionLivenessObservation{SessionLivenessUnknown, "dead observation could not release the slot while an attachment, owner run, or goal activation is active"}
	}
	if o.State != SessionLivenessDead {
		return false, h.persistTakeoverLivenessLocked(ctx, r, o)
	}
	r.State = SessionStopped
	r.Liveness = o.State
	r.LivenessDetail = o.Detail
	r.UpdatedAt = time.Now().UTC()
	if e := h.updateRecord(ctx, r); e != nil {
		return false, e
	}
	h.sessions[r.SessionID] = r
	return true, nil
}
