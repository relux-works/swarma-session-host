package sessionmanager

import "encoding/json"

// The whitelist copies native data, never an arbitrary MSPMethod implementer.
// This is a native wire-payload decoder, not a canonical DTO/JSON bridge.
func copyMuseNotification(event MSPNotification) (MSPNotification, string, error) {
	switch n := event.(type) {
	case *MSPSessionGoalChangedNotification:
		if n == nil {
			break
		}
		return MSPSessionGoalChangedNotification{SessionID: n.SessionID, Goal: n.Goal, SourceRange: append(json.RawMessage(nil), n.SourceRange...), ViewCursor: n.ViewCursor}, n.SessionID, nil
	case MSPSessionGoalChangedNotification:
		return copyMuseNotification(&n)
	case *MSPSessionStatusChangedNotification:
		if n == nil {
			break
		}
		return MSPSessionStatusChangedNotification{Attention: append([]string(nil), n.Attention...), SessionID: n.SessionID, Status: n.Status, ViewCursor: n.ViewCursor}, n.SessionID, nil
	case MSPSessionStatusChangedNotification:
		return copyMuseNotification(&n)
	case *MSPSessionTokenUsageNotification:
		if n == nil {
			break
		}
		return MSPSessionTokenUsageNotification{Cumulative: copyMuseCumulative(n.Cumulative), DurationMs: copyObservationPointer(n.DurationMs), FinishReason: n.FinishReason, ModelID: copyObservationPointer(n.ModelID), PromptTokens: n.PromptTokens, SessionID: n.SessionID, SourceRange: append(json.RawMessage(nil), n.SourceRange...), TotalTokens: n.TotalTokens, TurnID: n.TurnID, Usage: copyMuseTokenUsage(n.Usage), ViewCursor: n.ViewCursor}, n.SessionID, nil
	case MSPSessionTokenUsageNotification:
		return copyMuseNotification(&n)
	case *MSPTurnStartedNotification:
		if n == nil {
			break
		}
		return &MSPTurnStartedNotification{CommandID: n.CommandID, SessionID: n.SessionID, SourceRange: append(json.RawMessage(nil), n.SourceRange...), TurnID: n.TurnID, ViewCursor: n.ViewCursor}, n.SessionID, nil
	case MSPTurnStartedNotification:
		return copyMuseNotification(&n)
	case *MSPTurnCompletedNotification:
		if n == nil {
			break
		}
		out := &MSPTurnCompletedNotification{DurationMs: copyObservationPointer(n.DurationMs), Error: copyObservationPointer(n.Error), Reason: n.Reason, SessionID: n.SessionID, SourceRange: append(json.RawMessage(nil), n.SourceRange...), Terminal: n.Terminal, TimeToFirstTokenMs: copyObservationPointer(n.TimeToFirstTokenMs), TurnID: n.TurnID, ViewCursor: n.ViewCursor}
		if n.Usage != nil {
			v := copyMuseTokenUsage(*n.Usage)
			out.Usage = &v
		}
		return out, n.SessionID, nil
	case MSPTurnCompletedNotification:
		return copyMuseNotification(&n)
	case *MSPTurnRetractedNotification:
		if n == nil {
			break
		}
		return MSPTurnRetractedNotification{CommandID: n.CommandID, SessionID: n.SessionID, SourceRange: append(json.RawMessage(nil), n.SourceRange...), TurnID: n.TurnID, ViewCursor: n.ViewCursor}, n.SessionID, nil
	case MSPTurnRetractedNotification:
		return copyMuseNotification(&n)
	case *MSPTurnRetryScheduledNotification:
		if n == nil {
			break
		}
		return MSPTurnRetryScheduledNotification{Attempt: n.Attempt, MaxAttempts: n.MaxAttempts, NextAttempt: n.NextAttempt, Reason: n.Reason, RetryDelayMs: n.RetryDelayMs, SessionID: n.SessionID, SourceRange: append(json.RawMessage(nil), n.SourceRange...), TurnID: n.TurnID, ViewCursor: n.ViewCursor}, n.SessionID, nil
	case MSPTurnRetryScheduledNotification:
		return copyMuseNotification(&n)
	case *MSPTurnUnqueuedNotification:
		if n == nil {
			break
		}
		return MSPTurnUnqueuedNotification{CommandID: n.CommandID, SessionID: n.SessionID, SourceRange: append(json.RawMessage(nil), n.SourceRange...), TurnID: n.TurnID, ViewCursor: n.ViewCursor}, n.SessionID, nil
	case MSPTurnUnqueuedNotification:
		return copyMuseNotification(&n)
	case MSPRawNotification:
		if !museNotificationRequiresParamsObject(n.Name) {
			break
		}
		if err := museStrictDecodeRawNotification(n.Name, n.Params); err != nil {
			break
		}
		switch n.Name {
		case "item/started", "item/updated", "item/completed":
			if _, err := museStrictDecodeItemEnvelope(n.Name, n.Params); err != nil {
				return nil, "", ErrMuseObservationInvalid
			}
		}
		var binding struct {
			SessionID string `json:"sessionId"`
		}
		if json.Unmarshal(n.Params, &binding) != nil || binding.SessionID == "" {
			break
		}
		return MSPRawNotification{Name: n.Name, Params: append(json.RawMessage(nil), n.Params...)}, binding.SessionID, nil
	case *MSPRawNotification:
		if n != nil {
			return copyMuseNotification(*n)
		}
	case MSPServerRequest:
		if err := museStrictDecodeServerRequest(n.Name, n.Params); err != nil {
			break
		}
		var binding struct {
			SessionID string `json:"sessionId"`
		}
		if json.Unmarshal(n.Params, &binding) != nil || binding.SessionID == "" {
			break
		}
		return MSPServerRequest{ID: append(json.RawMessage(nil), n.ID...), Name: n.Name, Params: append(json.RawMessage(nil), n.Params...)}, binding.SessionID, nil
	case *MSPServerRequest:
		if n != nil {
			return copyMuseNotification(*n)
		}
	}
	return nil, "", ErrMuseObservationInvalid
}
func copyObservationPointer[T any](src *T) *T {
	if src == nil {
		return nil
	}
	v := *src
	return &v
}
func copyMuseTokenUsage(n MSPTokenUsage) MSPTokenUsage {
	return MSPTokenUsage{CacheReadTokens: copyObservationPointer(n.CacheReadTokens), CacheWriteTokens: copyObservationPointer(n.CacheWriteTokens), CachedTokens: n.CachedTokens, InputTokens: n.InputTokens, OutputTokens: n.OutputTokens, ReasoningTokens: n.ReasoningTokens}
}
func copyMuseCumulative(n MSPCumulativeTokenUsage) MSPCumulativeTokenUsage {
	return MSPCumulativeTokenUsage{OutputTokens: n.OutputTokens, PromptTokens: n.PromptTokens, TotalTokens: n.TotalTokens, CacheReadTokens: copyObservationPointer(n.CacheReadTokens), CacheWriteTokens: copyObservationPointer(n.CacheWriteTokens), Cost: copyObservationPointer(n.Cost)}
}

// Cursors are opaque. Equality detects replay at/after the folded boundary;
// native live frames are otherwise consumed in the reliable reader's order.
// Ephemeral records have no durable cursor and do not advance this check.
func museDurableObservationCursor(event MSPNotification) string {
	switch n := event.(type) {
	case MSPSessionGoalChangedNotification:
		if len(n.SourceRange) > 0 {
			return n.ViewCursor
		}
	case MSPSessionTokenUsageNotification:
		if len(n.SourceRange) > 0 {
			return n.ViewCursor
		}
	case *MSPTurnStartedNotification:
		if len(n.SourceRange) > 0 {
			return n.ViewCursor
		}
	case *MSPTurnCompletedNotification:
		if len(n.SourceRange) > 0 {
			return n.ViewCursor
		}
	case MSPTurnRetractedNotification:
		if len(n.SourceRange) > 0 {
			return n.ViewCursor
		}
	case MSPTurnRetryScheduledNotification:
		if len(n.SourceRange) > 0 {
			return n.ViewCursor
		}
	case MSPTurnUnqueuedNotification:
		if len(n.SourceRange) > 0 {
			return n.ViewCursor
		}
	case MSPRawNotification:
		var p struct {
			ViewCursor  string          `json:"viewCursor"`
			SourceRange json.RawMessage `json:"sourceRange"`
		}
		if json.Unmarshal(n.Params, &p) == nil && len(p.SourceRange) > 0 && string(p.SourceRange) != "null" {
			return p.ViewCursor
		}
	}
	return ""
}
