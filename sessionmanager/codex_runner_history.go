package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

// A decoded nil slice means absent/null history, not a known empty history.
// Validate every row before using absence of work to admit a native operation.
// Duplicate IDs within one response are ambiguous even if statuses agree.
func validateCodexTurnHistory(turns []codexTurn) error {
	if turns == nil {
		return ErrInputAcknowledgementMismatch
	}
	seen := make(map[string]bool, len(turns))
	for _, turn := range turns {
		if strings.TrimSpace(turn.ID) == "" || !knownCodexTurnStatus(turn.Status) || seen[turn.ID] {
			return ErrInputAcknowledgementMismatch
		}
		seen[turn.ID] = true
	}
	return nil
}

func knownCodexTurnStatus(status string) bool {
	switch status {
	case "inProgress", "completed", "failed", "interrupted":
		return true
	default:
		return false
	}
}

// Resume and list may repeat an ID, but conflicting snapshots cannot authorize
// a start/steer. Typed not-materialized refusal is handled before this check.
func codexTurnHistoriesAgree(groups ...[]codexTurn) bool {
	seen := map[string]string{}
	for _, turns := range groups {
		for _, turn := range turns {
			if status, ok := seen[turn.ID]; ok && status != turn.Status {
				return false
			}
			seen[turn.ID] = turn.Status
		}
	}
	return true
}

func codexLifecycleStatusAgrees(method, status string) bool {
	switch method {
	case "turn/started":
		return status == "inProgress"
	case "turn/completed":
		return status == "completed" || status == "failed" || status == "interrupted"
	default:
		return false
	}
}

// Native assignment/owner callers require an actual array. Keep transport and
// typed provider failures intact for the exact fresh-thread bootstrap rule.
func listCodexRunnerTurns(ctx context.Context, client *codexRPCClient, threadID string) ([]codexTurn, error) {
	turns, err := listCodexThreadTurns(ctx, client, threadID)
	if err != nil {
		var syntax *json.SyntaxError
		var shape *json.UnmarshalTypeError
		if errors.As(err, &syntax) || errors.As(err, &shape) {
			return nil, ErrInputAcknowledgementMismatch
		}
		return nil, err
	}
	if err := validateCodexTurnHistory(turns); err != nil {
		return nil, err
	}
	return turns, nil
}

// Active-only flags cannot agree with idle, even alongside terminal history.
// Other status types still need the caller's separate known-status decision.
func codexIdleStatusConsistent(status codexThreadStatus) bool {
	return status.Type != "idle" || len(status.ActiveFlags) == 0
}
