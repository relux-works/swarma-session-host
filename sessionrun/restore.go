package sessionrun

import (
	"bytes"
	"encoding/json"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// The full original field group is copied before reconciliation. Identity,
// baseline, goal history and enforcement latches are never reconstructed from
// an event, nor cleared because the new native reader has just started.
func restoreMuseAccounting(prior core.LegacyMuseAccountingSnapshot, f *core.MuseRestoreFold) (core.LegacyMuseAccountingSnapshot, error) {
	if f == nil || prior.GoalTransport != "native" || f.SessionID != prior.SessionID || f.Cursor == "" || f.Items == nil || f.Cumulative.TotalTokens < 0 || f.Cumulative.PromptTokens < 0 || f.Cumulative.OutputTokens < 0 {
		return prior, core.ErrMuseObservationInvalid
	}
	if f.Cumulative.TotalTokens < prior.TokenUsageTotal {
		return prior, core.ErrMuseObservationUncertain
	}
	raw, e := json.Marshal(prior)
	if e != nil {
		return prior, e
	}
	var next core.LegacyMuseAccountingSnapshot
	if e = json.Unmarshal(raw, &next); e != nil {
		return prior, e
	}
	if f.Cumulative.TotalTokens > next.TokenUsageTotal {
		next.TokenUsageTotal = f.Cumulative.TotalTokens
	}
	next.ActiveTurnID = ""
	if f.ActiveTurn != nil {
		next.ActiveTurnID = f.ActiveTurn.TurnID
	}
	if f.SessionActiveTurnID != "" {
		next.ActiveTurnID = f.SessionActiveTurnID
	}
	return next, nil
}

func sameRestoreFold(a, b *core.MuseRestoreFold) bool {
	if a.SessionID != b.SessionID || a.SessionActiveTurnID != b.SessionActiveTurnID || a.Cursor != b.Cursor || a.Cumulative != b.Cumulative || a.EffectiveApprovalMode != b.EffectiveApprovalMode || a.EffectiveModelID != b.EffectiveModelID || len(a.Items) != len(b.Items) || (a.ActiveTurn == nil) != (b.ActiveTurn == nil) {
		return false
	}
	if a.ActiveTurn != nil && *a.ActiveTurn != *b.ActiveTurn {
		return false
	}
	for i := range a.Items {
		if !bytes.Equal(a.Items[i], b.Items[i]) {
			return false
		}
	}
	return true
}
func copyRestoreFold(src *core.MuseRestoreFold) *core.MuseRestoreFold {
	out := &core.MuseRestoreFold{SessionID: src.SessionID, Cursor: src.Cursor, Cumulative: src.Cumulative, Items: make([]json.RawMessage, len(src.Items)), EffectiveApprovalMode: src.EffectiveApprovalMode, EffectiveModelID: src.EffectiveModelID, SessionActiveTurnID: src.SessionActiveTurnID}
	for i, raw := range src.Items {
		out.Items[i] = append(json.RawMessage(nil), raw...)
	}
	if src.ActiveTurn != nil {
		out.ActiveTurn = &core.MuseRestoreTurnRef{CommandID: src.ActiveTurn.CommandID, TurnID: src.ActiveTurn.TurnID}
	}
	return out
}
