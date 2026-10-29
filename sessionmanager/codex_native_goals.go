package sessionmanager

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// codexGoalsFeatureKey is the Codex feature that owns native thread goals: the
// model-facing create_goal/update_goal/get_goal tools and the harness goal
// continuation that starts a new turn the moment the previous one ends.
//
// Managed Codex sessions always run with it off. The board-goal-v2 plane is the
// only goal authority, and board run notices plus the spawn recovery policy are
// the only wake sources. With the feature on, a waiting orchestrator is
// restarted tens of milliseconds after every turn and rereads its whole context
// each time (measured repeated compaction in a long-running session), and the model
// can create goals the board never sees.
const (
	codexGoalsFeatureKey       = "features.goals"
	codexGoalsFeatureName      = "goals"
	codexGoalsDisabledOverride = codexGoalsFeatureKey + "=false"
	codexAppServerSubcommand   = "app-server"
)

// codexHostArgvWithNativeGoalsDisabled returns the managed app-server argv with
// every caller-supplied spelling of the goals feature removed and exactly one
// `-c features.goals=false` placed ahead of the app-server subcommand, where
// Codex reads global config overrides. A composition cannot re-enable the
// feature: the override is the manager's, not the plan's.
func codexHostArgvWithNativeGoalsDisabled(argv []string) []string {
	result := make([]string, 0, len(argv)+2)
	subcommandAt := -1
	for index := 0; index < len(argv); index++ {
		arg := argv[index]
		if subcommandAt < 0 && arg == codexAppServerSubcommand {
			subcommandAt = len(result)
		}
		if subcommandAt < 0 {
			hasValue := index+1 < len(argv)
			switch {
			case (arg == "-c" || arg == "--config") && hasValue:
				if codexOverrideKey(argv[index+1]) == codexGoalsFeatureKey {
					index++
					continue
				}
				result = append(result, arg, argv[index+1])
				index++
				continue
			case strings.HasPrefix(arg, "--config="):
				if codexOverrideKey(strings.TrimPrefix(arg, "--config=")) == codexGoalsFeatureKey {
					continue
				}
			case (arg == "--enable" || arg == "--disable") && hasValue:
				if strings.TrimSpace(argv[index+1]) == codexGoalsFeatureName {
					index++
					continue
				}
				result = append(result, arg, argv[index+1])
				index++
				continue
			case strings.HasPrefix(arg, "--enable=") || strings.HasPrefix(arg, "--disable="):
				_, value, _ := strings.Cut(arg, "=")
				if strings.TrimSpace(value) == codexGoalsFeatureName {
					continue
				}
			}
		}
		result = append(result, arg)
	}
	override := []string{"-c", codexGoalsDisabledOverride}
	if subcommandAt < 0 {
		return append(override, result...)
	}
	withOverride := make([]string, 0, len(result)+len(override))
	withOverride = append(withOverride, result[:subcommandAt]...)
	withOverride = append(withOverride, override...)
	return append(withOverride, result[subcommandAt:]...)
}

func codexOverrideKey(value string) string {
	key, _, ok := strings.Cut(value, "=")
	if !ok {
		return ""
	}
	return strings.TrimSpace(key)
}

// deliverCodexThreadText puts one text message into a Codex thread: into the
// in-progress turn when there is one (turn/steer surfaces it when the current
// tool call returns, which also interrupts clock.sleep), otherwise onto the
// thread queue, which the app-server drains itself when the thread is idle,
// with or without a connected client. An app-server without thread/queue/add
// gets turn/start instead.
func deliverCodexThreadText(ctx context.Context, client *codexRPCClient, threadID, text string) error {
	input := []codexTextInput{{Type: "text", Text: text, TextElements: []any{}}}
	if _, err := client.request(ctx, "thread/resume", map[string]any{
		"threadId":               threadID,
		"persistExtendedHistory": true,
	}); err != nil && !codexResumeHasNoRollout(err, threadID) {
		return fmt.Errorf("thread/resume before delivery: %w", err)
	}
	raw, err := client.request(ctx, "thread/read", map[string]any{"threadId": threadID, "includeTurns": true})
	if err == nil {
		var response struct {
			Thread codexThread `json:"thread"`
		}
		if json.Unmarshal(raw, &response) == nil && response.Thread.Status.Type == "active" {
			for index := len(response.Thread.Turns) - 1; index >= 0; index-- {
				turn := response.Thread.Turns[index]
				if turn.Status != "inProgress" {
					continue
				}
				if _, steerErr := client.request(ctx, "turn/steer", codexTurnSteerParams{
					ThreadID:       threadID,
					ExpectedTurnID: turn.ID,
					Input:          input,
				}); steerErr == nil {
					return nil
				}
				break
			}
		}
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	_, queueErr := client.request(ctx, "thread/queue/add", map[string]any{
		"threadId":            threadID,
		"clientUserMessageId": fmt.Sprintf("task-board-goal-%x", nonce[:]),
		"input":               input,
	})
	if queueErr == nil {
		return nil
	}
	if !codexMethodUnsupported(queueErr) {
		return fmt.Errorf("thread/queue/add: %w", queueErr)
	}
	if _, err := client.request(ctx, "turn/start", codexTurnStartParams{ThreadID: threadID, Input: input}); err != nil {
		return fmt.Errorf("turn/start after unsupported thread/queue/add: %w", err)
	}
	return nil
}

// A newly started live thread has no rollout until its first user message.
// Codex 0.162.1 cannot resume that thread yet, but it can accept delivery on it.
// Match the typed response and exact identity; storage or transport failures
// must never be reinterpreted as an absent rollout.
func codexResumeHasNoRollout(err error, threadID string) bool {
	var failure *codexRPCError
	return errors.As(err, &failure) && failure.Method == "thread/resume" &&
		failure.Code == -32600 && failure.Message == "no rollout found for thread id "+threadID
}

func codexMethodUnsupported(err error) bool {
	message := err.Error()
	return strings.Contains(message, "-32601") || strings.Contains(message, "unknown variant")
}

// codexBlockedReplyPrefix is how a managed Codex run reports a Stop-The-Line
// boundary now that it has no native goal status to set: its final message
// starts with this prefix and names the exact blocker.
const codexBlockedReplyPrefix = "BLOCKED:"

// codexTurnBlockedReply returns the blocker named by the turn's last agent
// message when that message starts with BLOCKED:.
func codexTurnBlockedReply(turn *codexTurn) (string, bool) {
	if turn == nil {
		return "", false
	}
	for index := len(turn.Items) - 1; index >= 0; index-- {
		var item struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(turn.Items[index], &item) != nil || item.Type != "agentMessage" {
			continue
		}
		text := strings.TrimSpace(item.Text)
		if !strings.HasPrefix(text, codexBlockedReplyPrefix) {
			return "", false
		}
		blocker := strings.TrimSpace(strings.TrimPrefix(text, codexBlockedReplyPrefix))
		if blocker == "" {
			blocker = "the run replied BLOCKED: without naming the blocker"
		}
		return blocker, true
	}
	return "", false
}

// codexTurnUsageLimited reports whether a failed turn failed on the provider's
// own usage limit, the signal the native goal status used to carry.
func codexTurnUsageLimited(turn *codexTurn) bool {
	if turn == nil || len(turn.Error) == 0 {
		return false
	}
	text := strings.ToLower(string(turn.Error))
	for _, marker := range []string{"usagelimit", "usage_limit", "usage limit", "usagelimitexceeded"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}
