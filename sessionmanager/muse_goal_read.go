package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// museGoalReader reads only provider-native identity, goals, usage and stated
// execution terms. It carries no canonical owner, revision, ledger or writer.
type museGoalReader struct {
	client    *MSPClient
	sessionID string
}

func (r *museGoalReader) snapshotSessionID() string { return r.sessionID }

type museGoalRead struct {
	Known      bool
	Present    bool
	Cleared    bool
	Objective  string
	Status     string
	TokenUse   int64
	Cumulative *MuseRestoreUsage
	// EffectiveApprovalMode and EffectiveModelID carry the snapshot
	// state's stated effective terms (r24 S11), empty when the state
	// omits them. The apply site folds them with the session
	// projection's echoes into the reconfiguration provenance.
	EffectiveApprovalMode string
	EffectiveModelID      string
}

type museSnapshotEnvelope struct {
	State struct {
		Goal       json.RawMessage `json:"goal"`
		TokenUsage json.RawMessage `json:"tokenUsage"`
	} `json:"state"`
}

type museViewPageResult struct {
	Events     json.RawMessage `json:"events"`
	NextCursor json.RawMessage `json:"nextCursor"`
}

func (h *museGoalReader) readResumeGoal(ctx context.Context, preference MSPHistoryPreference) (MSPSessionResumeResult, museGoalRead, uint64, error) {
	params := MSPSessionResumeParams{CommandID: museUUIDv7(), SessionID: h.snapshotSessionID(), History: preference}
	resumed, responseSequence, err := h.client.SessionResumeWithSequence(ctx, params)
	if err != nil {
		return resumed, museGoalRead{}, 0, err
	}
	if resumed.Session.SessionID != params.SessionID {
		return resumed, museGoalRead{}, responseSequence, &MuseHostError{Code: "session_identity", Cause: fmt.Errorf("session/resume returned %q, expected %q", resumed.Session.SessionID, params.SessionID)}
	}
	if resumed.History.Mode == "snapshot" {
		goal, parseErr := museGoalFromSnapshot(resumed.History.Snapshot)
		if parseErr != nil {
			return resumed, museGoalRead{}, responseSequence, &MuseHostError{Code: "goal_snapshot_unavailable", Cause: errors.Join(ErrMuseGoalSnapshotUnavailable, parseErr)}
		}
		return resumed, goal, responseSequence, nil
	}

	goal, pageSequence, pageErr := h.readGoalFromPages(ctx)
	if pageErr == nil {
		return resumed, goal, pageSequence, nil
	}
	// A paged identity failure keeps its typed code (r31 S5, M30-3):
	// wrapping it as goal_snapshot_unavailable would hide which
	// attribution failed. Every other page error still wraps below.
	var pageTyped *MuseHostError
	if errors.As(pageErr, &pageTyped) && pageTyped.Code == "session_identity" {
		return resumed, museGoalRead{}, pageSequence, pageErr
	}
	var serverError *MSPServerError
	if errors.As(pageErr, &serverError) && (serverError.Code == -32040 || serverError.Code == -32042) {
		anchored, anchoredSequence, anchorErr := h.readAnchoredGoal(ctx, params.SessionID)
		if anchorErr != nil {
			return resumed, museGoalRead{}, anchoredSequence, anchorErr
		}
		return resumed, anchored, anchoredSequence, nil
	}
	return resumed, museGoalRead{}, pageSequence, &MuseHostError{Code: "goal_snapshot_unavailable", Cause: errors.Join(ErrMuseGoalSnapshotUnavailable, pageErr)}
}

func (h *museGoalReader) readAnchoredGoal(ctx context.Context, sessionID string) (museGoalRead, uint64, error) {
	resumed, sequence, err := h.client.SessionResumeWithSequence(ctx, MSPSessionResumeParams{
		CommandID: museUUIDv7(), SessionID: sessionID, History: MSPHistoryAnchored,
	})
	if err != nil {
		return museGoalRead{}, 0, err
	}
	if resumed.Session.SessionID != sessionID || resumed.History.Mode != "anchoredSnapshot" {
		return museGoalRead{}, sequence, &MuseHostError{Code: "goal_snapshot_unavailable", Cause: fmt.Errorf("anchored resume served mode %q for session %q", resumed.History.Mode, resumed.Session.SessionID)}
	}
	goal, err := museGoalFromSnapshot(resumed.History.Snapshot)
	if err != nil {
		return museGoalRead{}, sequence, &MuseHostError{Code: "goal_snapshot_unavailable", Cause: errors.Join(ErrMuseGoalSnapshotUnavailable, err)}
	}
	return goal, sequence, nil
}

func (h *museGoalReader) readGoalFromPages(ctx context.Context) (museGoalRead, uint64, error) {
	const pageLimit = 1000
	const maximumPages = 256
	var cursor *string
	for page := 0; page < maximumPages; page++ {
		params := map[string]any{"sessionId": h.snapshotSessionID(), "direction": "backward", "limit": pageLimit}
		if cursor != nil {
			params["cursor"] = *cursor
		}
		var result museViewPageResult
		sequence, err := h.client.callSequenced(ctx, "view/page", params, &result)
		if err != nil {
			return museGoalRead{}, sequence, err
		}
		if len(result.Events) == 0 || len(result.NextCursor) == 0 {
			return museGoalRead{}, sequence, &MSPProtocolError{Reason: "view/page result omitted required events or nextCursor"}
		}
		var events []struct {
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(result.Events, &events); err != nil {
			return museGoalRead{}, sequence, &MSPProtocolError{Reason: "view/page events are malformed", Cause: err}
		}
		for i := len(events) - 1; i >= 0; i-- {
			if strings.Contains(strings.ToLower(events[i].Method), "compaction") {
				return museGoalRead{}, sequence, &MSPServerError{Code: -32042, Message: "compaction boundary requires anchored snapshot"}
			}
			if events[i].Method == "session/goalChanged" {
				// The paged event carries its own session attribution,
				// and only this session's goal may be adopted (r31 S5,
				// M30-3): a foreign or unreadable session is a typed
				// identity failure, exactly as the live resume path
				// refuses a foreign session. Identity is checked before
				// content: an unattributable goal block is never read.
				if idErr := musePagedGoalChangedSessionError(events[i].Params, h.snapshotSessionID()); idErr != nil {
					return museGoalRead{}, sequence, idErr
				}
				goal, err := museGoalFromGoalChangedParams(events[i].Params)
				return goal, sequence, err
			}
		}
		if bytes.Equal(bytes.TrimSpace(result.NextCursor), []byte("null")) {
			return museGoalRead{Known: true, Present: true, Cleared: true}, sequence, nil
		}
		var next string
		if err := json.Unmarshal(result.NextCursor, &next); err != nil || next == "" {
			return museGoalRead{}, sequence, &MSPProtocolError{Reason: "view/page nextCursor is not an opaque string or null", Cause: err}
		}
		cursor = &next
	}
	return museGoalRead{}, 0, &MuseHostError{Code: "goal_snapshot_unavailable", Cause: errors.New("view/page exceeded the bounded backward-page count")}
}

func museGoalFromSnapshot(raw json.RawMessage) (museGoalRead, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return museGoalRead{}, fmt.Errorf("decode resume snapshot: %w", err)
	}
	stateRaw, ok := envelope["state"]
	if !ok || len(stateRaw) == 0 || bytes.Equal(bytes.TrimSpace(stateRaw), []byte("null")) {
		return museGoalRead{}, errors.New("resume snapshot has no state object")
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(stateRaw, &state); err != nil {
		return museGoalRead{}, fmt.Errorf("decode resume snapshot state: %w", err)
	}
	goalRaw, ok := state["goal"]
	if !ok || len(goalRaw) == 0 {
		return museGoalRead{}, errors.New("resume snapshot state omitted goal")
	}
	goal, err := museGoalFromRaw(goalRaw)
	if err != nil {
		return museGoalRead{}, err
	}
	goal.Known = true
	// The snapshot state's effective terms ride the read into the
	// reconfiguration provenance (r24 S11). They are advisory: a
	// malformed block stays unstated rather than failing the goal
	// read, exactly as an omitted one does.
	if modeRaw, ok := state["approvalMode"]; ok {
		if mode, stated := museEffectiveApprovalMode(modeRaw); stated {
			goal.EffectiveApprovalMode = mode
		}
	}
	if modelRaw, ok := state["effectiveModel"]; ok {
		if model, stated := museEffectiveModelID(modelRaw); stated {
			goal.EffectiveModelID = model
		}
	}
	if usageRaw, ok := state["tokenUsage"]; ok && len(usageRaw) != 0 && !bytes.Equal(bytes.TrimSpace(usageRaw), []byte("null")) {
		var usage struct {
			TotalTokens int64 `json:"totalTokens"`
		}
		if err := json.Unmarshal(usageRaw, &usage); err != nil || usage.TotalTokens < 0 {
			return museGoalRead{}, errors.New("resume snapshot tokenUsage is malformed")
		}
		goal.TokenUse = usage.TotalTokens
		goal.Cumulative, err = museNativeCumulativeUsage(usageRaw)
		if err != nil {
			return museGoalRead{}, err
		}
	}
	return goal, nil
}

// musePagedGoalChangedSessionError attributes one paged session/goalChanged
// event to this session (r31 S5, M30-3). A foreign sessionId — or params
// whose sessionId cannot be read at all — is a typed session_identity
// failure, the paged twin of the live resume identity refusal: the
// view/page result is session-scoped, so a non-conforming page never
// donates its goal to this session.
func musePagedGoalChangedSessionError(raw json.RawMessage, want string) error {
	var envelope struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return &MuseHostError{Code: "session_identity", Cause: fmt.Errorf("paged goalChanged session is unreadable: %w", err)}
	}
	if envelope.SessionID != want {
		return &MuseHostError{Code: "session_identity", Cause: fmt.Errorf("paged goalChanged named session %q, expected %q", envelope.SessionID, want)}
	}
	return nil
}

func museGoalFromGoalChangedParams(raw json.RawMessage) (museGoalRead, error) {
	var params map[string]json.RawMessage
	if err := json.Unmarshal(raw, &params); err != nil {
		return museGoalRead{}, fmt.Errorf("decode paged goalChanged params: %w", err)
	}
	goalRaw, ok := params["goal"]
	if !ok || len(goalRaw) == 0 {
		return museGoalRead{}, errors.New("paged goalChanged omitted goal")
	}
	goal, err := museGoalFromRaw(goalRaw)
	if err != nil {
		return museGoalRead{}, err
	}
	goal.Known = true
	return goal, nil
}

func museGoalFromRaw(raw json.RawMessage) (museGoalRead, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return museGoalRead{Known: true, Present: true, Cleared: true}, nil
	}
	var goal struct {
		Objective string `json:"objective"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(raw, &goal); err != nil {
		return museGoalRead{}, fmt.Errorf("decode goal state: %w", err)
	}
	if strings.TrimSpace(goal.Objective) == "" || strings.TrimSpace(goal.Status) == "" {
		return museGoalRead{}, errors.New("goal state omitted objective or status")
	}
	return museGoalRead{Known: true, Present: true, Objective: goal.Objective, Status: goal.Status}, nil
}

// museEffectiveApprovalMode reads the mode member of one effective approval
// mode state block. The block arrives inside a result the strict client
// already validated, so only its JSON shape is checked here, never the
// schema again. It reports false when the block is absent, null, or
// carries no mode: an unstated effective mode is report-only, never
// invented.
func museEffectiveApprovalMode(raw json.RawMessage) (string, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", false
	}
	var state struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(trimmed, &state); err != nil {
		return "", false
	}
	if strings.TrimSpace(state.Mode) == "" {
		return "", false
	}
	return state.Mode, true
}

// museEffectiveModelID reads the modelId member of one effective-model
// state block (the snapshot state's effectiveModel). Like
// museEffectiveApprovalMode it reports false when the block is absent,
// null, or carries no model: an unstated effective model is report-only,
// never invented.
func museEffectiveModelID(raw json.RawMessage) (string, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", false
	}
	var state struct {
		ModelID string `json:"modelId"`
	}
	if err := json.Unmarshal(trimmed, &state); err != nil {
		return "", false
	}
	if strings.TrimSpace(state.ModelID) == "" {
		return "", false
	}
	return state.ModelID, true
}
