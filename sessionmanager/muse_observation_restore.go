package sessionmanager

import (
	"encoding/json"
	"strings"
)

// A folded snapshot contains cumulative and current facts, not the source
// ranges/per-completion counters of live notifications. D4b projects it once
// as a distinct reconciliation base at the native response boundary.
func convertMuseRestoreObservations(result MSPSessionResumeResult, requestedApproval, requestedModel string) (*MuseRestoreFold, error) {
	if result.History.Mode != "snapshot" && result.History.Mode != "anchoredSnapshot" {
		return nil, ErrMuseObservationUncertain
	}
	defs, _, err := musePinnedSchema()
	if err != nil {
		return nil, err
	}
	def, ok := defs["ViewSnapshot"].(map[string]any)
	if !ok || museValidatePinnedParams("restore snapshot", def, result.History.Snapshot, false) != nil {
		return nil, ErrMuseObservationInvalid
	}
	var snapshot struct {
		SchemaVersion int    `json:"schemaVersion"`
		ViewCursor    string `json:"viewCursor"`
		State         struct {
			TokenUsage MSPCumulativeTokenUsage `json:"tokenUsage"`
			Items      []json.RawMessage       `json:"items"`
			ActiveTurn *struct {
				CommandID string `json:"commandId"`
				TurnID    string `json:"turnId"`
			} `json:"activeTurn"`
		} `json:"state"`
	}
	if json.Unmarshal(result.History.Snapshot, &snapshot) != nil || snapshot.SchemaVersion != 1 || snapshot.ViewCursor != result.ViewCursor {
		return nil, ErrMuseObservationUncertain
	}
	fold := &MuseRestoreFold{SessionID: result.Session.SessionID, Cursor: snapshot.ViewCursor, Cumulative: MuseRestoreUsage{OutputTokens: snapshot.State.TokenUsage.OutputTokens, PromptTokens: snapshot.State.TokenUsage.PromptTokens, TotalTokens: snapshot.State.TokenUsage.TotalTokens}, Items: snapshot.State.Items}
	goal, err := museGoalFromSnapshot(result.History.Snapshot)
	if err != nil {
		return nil, ErrMuseObservationInvalid
	}
	fold.EffectiveApprovalMode, fold.EffectiveModelID = museStatedNativeTerms(result.Session, goal, requestedApproval, requestedModel)
	if result.Session.ActiveTurnID != nil {
		fold.SessionActiveTurnID = *result.Session.ActiveTurnID
	}
	if t := snapshot.State.ActiveTurn; t != nil {
		fold.ActiveTurn = &MuseRestoreTurnRef{CommandID: t.CommandID, TurnID: t.TurnID}
	}
	return copyMuseRestoreFold(fold)
}

// A backend supplies data, never validation authority. Both the ring and final
// host admission copy and validate native Item values without live envelopes.
func copyMuseRestoreFold(src *MuseRestoreFold) (*MuseRestoreFold, error) {
	if src == nil || strings.TrimSpace(src.SessionID) == "" || strings.TrimSpace(src.Cursor) == "" || src.Cumulative.OutputTokens < 0 || src.Cumulative.PromptTokens < 0 || src.Cumulative.TotalTokens < 0 || src.Items == nil {
		return nil, ErrMuseObservationInvalid
	}
	out := &MuseRestoreFold{SessionID: src.SessionID, Cursor: src.Cursor, Cumulative: src.Cumulative, Items: make([]json.RawMessage, len(src.Items)), EffectiveApprovalMode: src.EffectiveApprovalMode, EffectiveModelID: src.EffectiveModelID, SessionActiveTurnID: src.SessionActiveTurnID}
	if t := src.ActiveTurn; t != nil {
		if strings.TrimSpace(t.CommandID) == "" || strings.TrimSpace(t.TurnID) == "" {
			return nil, ErrMuseObservationInvalid
		}
		out.ActiveTurn = &MuseRestoreTurnRef{CommandID: t.CommandID, TurnID: t.TurnID}
	}
	defs, _, err := musePinnedSchema()
	if err != nil {
		return nil, err
	}
	def, ok := defs["Item"].(map[string]any)
	if !ok {
		return nil, ErrMuseObservationInvalid
	}
	seen := map[string]bool{}
	for i, raw := range src.Items {
		if museValidatePinnedParams("restore item", def, raw, false) != nil {
			return nil, ErrMuseObservationInvalid
		}
		var id struct {
			ItemID   string `json:"itemId"`
			Kind     string `json:"kind"`
			Revision int64  `json:"revision"`
		}
		if json.Unmarshal(raw, &id) != nil || strings.TrimSpace(id.ItemID) == "" || seen[id.ItemID] {
			return nil, ErrMuseObservationInvalid
		}
		switch id.Kind {
		case "agentMessage", "subagent", "workflow", "reminderChild":
			if id.Revision < 1 {
				return nil, ErrMuseObservationInvalid
			}
		}
		seen[id.ItemID] = true
		out.Items[i] = append(json.RawMessage(nil), raw...)
	}
	return out, nil
}
