package sessionmanager

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Lifecycle drift needs explicit same-thread identity even after subscription.
// Missing thread IDs cannot nominate a replacement writer from unrelated replay.
func (c *codexRPCClient) nativeTurnEvidenceFrom(
	notifications []codexRPCNotification,
	threadID string,
	turnID string,
	current ProviderUsage,
	allowAcknowledgementReconciliation bool,
) (ProviderUsage, string, []string, string) {
	candidates := make(map[string]bool)
	expectedSeen := false
	malformedEvidence := false
	for _, notification := range notifications {
		if notification.Method != "turn/started" &&
			notification.Method != "turn/completed" {
			continue
		}
		var params struct {
			ThreadID string    `json:"threadId"`
			Turn     codexTurn `json:"turn"`
		}
		if json.Unmarshal(notification.Params, &params) != nil {
			var identity struct {
				ThreadID string `json:"threadId"`
			}
			if json.Unmarshal(notification.Params, &identity) == nil && identity.ThreadID == threadID {
				malformedEvidence = true
			}
			continue
		}
		if params.ThreadID != threadID || strings.TrimSpace(params.Turn.ID) == "" {
			if params.ThreadID == threadID {
				malformedEvidence = true
			}
			continue
		}
		// Even an unusable status cannot erase explicitly observed presence of
		// our expected turn and thereby authorize a replacement writer.
		if params.Turn.ID == turnID {
			expectedSeen = true
		}
		if !codexLifecycleStatusAgrees(notification.Method, params.Turn.Status) {
			malformedEvidence = true
			continue
		}
		if params.Turn.ID != turnID {
			candidates[params.Turn.ID] = true
		}
	}
	candidateTurnID := ""
	if allowAcknowledgementReconciliation && !malformedEvidence &&
		!expectedSeen &&
		len(candidates) == 1 {
		for candidate := range candidates {
			candidateTurnID = candidate
		}
	}
	effectiveTurnID := turnID
	if candidateTurnID != "" {
		effectiveTurnID = candidateTurnID
	}

	status := ""
	methods := make([]string, 0, len(notifications))
	for _, notification := range notifications {
		methodLabel := notification.Method
		switch notification.Method {
		case "turn/started", "turn/completed":
			var params struct {
				ThreadID string    `json:"threadId"`
				Turn     codexTurn `json:"turn"`
			}
			if json.Unmarshal(notification.Params, &params) == nil {
				methodLabel = fmt.Sprintf(
					"%s[thread=%s,turn=%s,status=%s]",
					notification.Method,
					c.managedIdentityRelation(params.ThreadID, threadID),
					identityRelation(params.Turn.ID, effectiveTurnID),
					strings.TrimSpace(params.Turn.Status),
				)
			}
			if c.matchesManagedTurn(
				params.ThreadID,
				params.Turn.ID,
				threadID,
				effectiveTurnID,
				false,
			) &&
				codexLifecycleStatusAgrees(notification.Method, params.Turn.Status) &&
				codexTurnStatusRank(params.Turn.Status) > codexTurnStatusRank(status) {
				status = params.Turn.Status
			}
		case "thread/tokenUsage/updated":
			var params struct {
				ThreadID   string `json:"threadId"`
				TurnID     string `json:"turnId"`
				TokenUsage struct {
					Last struct {
						InputTokens           int64 `json:"inputTokens"`
						CachedInputTokens     int64 `json:"cachedInputTokens"`
						CacheWriteInputTokens int64 `json:"cacheWriteInputTokens"`
						OutputTokens          int64 `json:"outputTokens"`
						ReasoningOutputTokens int64 `json:"reasoningOutputTokens"`
						TotalTokens           int64 `json:"totalTokens"`
					} `json:"last"`
				} `json:"tokenUsage"`
			}
			if json.Unmarshal(notification.Params, &params) != nil ||
				!c.matchesManagedTurn(
					params.ThreadID,
					params.TurnID,
					threadID,
					effectiveTurnID,
					true,
				) {
				continue
			}
			current = ProviderUsage{
				InputTokens:           params.TokenUsage.Last.InputTokens,
				CachedInputTokens:     params.TokenUsage.Last.CachedInputTokens,
				CacheCreationTokens:   params.TokenUsage.Last.CacheWriteInputTokens,
				OutputTokens:          params.TokenUsage.Last.OutputTokens,
				ReasoningOutputTokens: params.TokenUsage.Last.ReasoningOutputTokens,
				TotalTokens:           params.TokenUsage.Last.TotalTokens,
			}
		}
		methods = append(methods, methodLabel)
	}
	return current, status, methods, candidateTurnID
}
