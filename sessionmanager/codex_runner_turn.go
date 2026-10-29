package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// executeTurn retains the receipt connection for the later owner observer.
// Closing and re-resuming between the write and polling would lose its original
// evidence window. No canonical state or pending directive is interpreted here.
func (r *codexRunner) executeTurn(ctx context.Context, input InputSubmission, receipt InputReceipt) (out InputObservation, wrote bool, resultErr error) {
	r.mu.Lock()
	snapshot := r.snapshot
	alreadyOwned := r.ownerClient != nil || snapshot.ManagedTurnID != ""
	r.mu.Unlock()
	if alreadyOwned {
		return out, false, fmt.Errorf("%w: managed Codex assignment already owns a receipt connection", ErrInputAcknowledgementMismatch)
	}
	threadID := snapshot.ThreadID
	savedHistory := snapshot.ManagedTurnID != "" || snapshot.LastCompletedTurnID != "" || snapshot.InheritedLastTurnID != "" || snapshot.ForkedFromID != "" || snapshot.BaseRolloutSHA256 != "" || (strings.TrimSpace(snapshot.InheritedRollout.SHA256) != "" && snapshot.InheritedRollout.Bytes >= 0)
	client, err := r.dialInitialized(ctx)
	if err != nil {
		return out, false, err
	}
	retained := false
	defer func() {
		if !retained {
			r.releaseClient(client)
		}
	}()
	raw, err := client.request(ctx, "thread/resume", map[string]any{"threadId": threadID, "persistExtendedHistory": true})
	noRollout := codexResumeHasNoRollout(err, threadID)
	if noRollout {
		r.markConsumerEvidenceIncomplete()
	}
	if err != nil && (!noRollout || savedHistory) {
		return out, false, fmt.Errorf("subscribing native Codex assignment: %w", err)
	}
	var resume struct {
		Thread codexThread `json:"thread"`
	}
	if !noRollout {
		if json.Unmarshal(raw, &resume) != nil || resume.Thread.ID != threadID {
			return out, false, fmt.Errorf("%w: native Codex resume thread", ErrInputAcknowledgementMismatch)
		}
		if !codexIdleStatusConsistent(resume.Thread.Status) {
			return out, false, ErrInputAcknowledgementMismatch
		}
		if err := validateCodexTurnHistory(resume.Thread.Turns); err != nil {
			return out, false, err
		}
		client.subscribedThreadID = threadID
	}
	turns, listErr := listCodexRunnerTurns(ctx, client, threadID)
	var failure *codexRPCError
	unmaterialized := listErr != nil && !savedHistory && errors.As(listErr, &failure) && failure.Method == "thread/turns/list" && failure.Code == -32600 && failure.Message == fmt.Sprintf("thread %s is not materialized yet; thread/turns/list is unavailable before first user message", threadID)
	fresh := noRollout && unmaterialized
	if unmaterialized && (noRollout || resume.Thread.Status.Type == "idle" && resume.Thread.Turns != nil && len(resume.Thread.Turns) == 0) {
		listErr = nil
	}
	if listErr != nil {
		return out, false, fmt.Errorf("probing native Codex assignment turn identity: %w", listErr)
	}
	if !codexTurnHistoriesAgree(resume.Thread.Turns, turns) {
		return out, false, ErrInputAcknowledgementMismatch
	}
	if noRollout && !fresh {
		return out, false, fmt.Errorf("%w: history contradicts refused resume", ErrInputAcknowledgementMismatch)
	}
	command := input.Payload.CodexTurn
	nativeInput := []codexTextInput{{Type: "text", Text: command.Start.Text, TextElements: []any{}}}
	var turnID string
	started := false
	switch {
	case fresh || resume.Thread.Status.Type == "idle":
		for _, group := range [][]codexTurn{resume.Thread.Turns, turns} {
			for _, turn := range group {
				if turn.Status == "inProgress" {
					return out, false, fmt.Errorf("%w: idle Codex thread exposes an existing writer", ErrInputAcknowledgementMismatch)
				}
			}
		}
		before := client.drainNotifications()
		if fresh && codexNotificationsContradictFreshThread(before, threadID) {
			return out, false, fmt.Errorf("%w: events contradict fresh Codex thread", ErrInputAcknowledgementMismatch)
		}
		wrote = true
		raw, err = client.request(ctx, "turn/start", codexTurnStartParams{ThreadID: threadID, Input: nativeInput, Cwd: snapshot.ProjectDir})
		if err != nil {
			return out, true, fmt.Errorf("starting native Codex assignment: %w", err)
		}
		var reply struct {
			Turn codexTurn `json:"turn"`
		}
		if json.Unmarshal(raw, &reply) != nil || strings.TrimSpace(reply.Turn.ID) == "" {
			return out, true, fmt.Errorf("%w: native Codex start receipt", ErrInputAcknowledgementMismatch)
		}
		turnID = reply.Turn.ID
		started = true
	case resume.Thread.Status.Type == "active":
		turnID = uniqueInProgressCodexTurnID(resume.Thread.Turns, turns)
		if turnID == "" {
			return out, false, fmt.Errorf("%w: native Codex active thread lacks one exact turn", ErrInputAcknowledgementMismatch)
		}
		client.drainNotifications()
		wrote = true
		raw, err = client.request(ctx, "turn/steer", codexTurnSteerParams{ThreadID: threadID, ExpectedTurnID: turnID, Input: nativeInput})
		if err != nil {
			return out, true, fmt.Errorf("steering native Codex assignment: %w", err)
		}
		var reply struct {
			TurnID string `json:"turnId"`
		}
		if json.Unmarshal(raw, &reply) != nil || reply.TurnID != turnID {
			return out, true, fmt.Errorf("%w: native Codex steer receipt", ErrInputAcknowledgementMismatch)
		}
	default:
		return out, false, fmt.Errorf("%w: native Codex thread status %q", ErrInputAcknowledgementMismatch, resume.Thread.Status.Type)
	}
	if err := ctx.Err(); err != nil {
		return out, wrote, err
	}
	r.mu.Lock()
	if r.retiring {
		r.mu.Unlock()
		return out, wrote, context.Canceled
	}
	r.snapshot.ManagedTurnID = turnID
	r.ownerClient = client
	r.ownerStarted = started
	retained = true
	r.mu.Unlock()
	return InputObservation{Receipt: receipt, Kind: "provider-ack", Correlation: command.CommandID, CodexTurn: &CodexTurnObservation{CommandID: command.CommandID, ThreadID: threadID, TurnID: turnID, StartedNewTurn: started}}, wrote, nil
}

// A refused resume has no subscription binding. Only explicit same-thread
// evidence can contradict the exact not-materialized bootstrap verdict.
func codexNotificationsContradictFreshThread(notifications []codexRPCNotification, threadID string) bool {
	for _, notification := range notifications {
		var params struct {
			ThreadID string            `json:"threadId"`
			Thread   codexThread       `json:"thread"`
			Status   codexThreadStatus `json:"status"`
		}
		if json.Unmarshal(notification.Params, &params) != nil {
			var identity struct {
				ThreadID string `json:"threadId"`
				Thread   struct {
					ID string `json:"id"`
				} `json:"thread"`
			}
			if json.Unmarshal(notification.Params, &identity) == nil && (identity.ThreadID == threadID || identity.Thread.ID == threadID) {
				return true
			}
			continue
		}
		switch notification.Method {
		case "turn/started", "turn/completed", "item/started", "item/completed", "thread/tokenUsage/updated":
			if params.ThreadID == threadID {
				return true
			}
		case "thread/status/changed":
			if params.ThreadID == threadID && (params.Status.Type != "idle" || !codexIdleStatusConsistent(params.Status)) {
				return true
			}
		case "thread/started":
			if params.Thread.ID == threadID && (params.Thread.Status.Type != "idle" || !codexIdleStatusConsistent(params.Thread.Status) || validateCodexTurnHistory(params.Thread.Turns) != nil || len(params.Thread.Turns) != 0) {
				return true
			}
		}
	}
	return false
}
