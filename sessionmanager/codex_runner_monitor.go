package sessionmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

func (h *codexRunner) startAutoContinue() {
	h.mu.Lock()
	options := h.snapshot.AutoContinue
	if !options.Enabled || h.monitorDone != nil || h.retiring {
		h.mu.Unlock()
		return
	}
	if options.DelaySeconds <= 0 {
		options.DelaySeconds = 20
	}
	if options.MaxNudgesPerTurn <= 0 {
		options.MaxNudgesPerTurn = 2
	}
	if strings.TrimSpace(options.Prompt) == "" {
		options.Prompt = "Continue unless you are blocked by a real ambiguity, approval boundary, or missing external input."
	}
	ctx, cancel := context.WithCancel(h.life)
	h.monitorCancel = cancel
	h.monitorDone = make(chan struct{})
	h.mu.Unlock()
	go h.runAutoContinue(ctx, options)
}

func (h *codexRunner) runAutoContinue(
	ctx context.Context,
	options CodexAutoContinueOptions,
) {
	ticker := time.NewTicker(codexAutoContinuePollInterval)
	defer ticker.Stop()
	defer close(h.monitorDone)
	state := &codexAutoContinueState{}
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := h.pollAutoContinue(ctx, options, state, now); err != nil {
				h.logf("auto-continue poll failed: %v", err)
			}
		}
	}
}

func (h *codexRunner) pollAutoContinue(
	ctx context.Context,
	options CodexAutoContinueOptions,
	state *codexAutoContinueState,
	now time.Time,
) error {
	call, finish, err := h.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if err := historyguard.LockMutex(call, &h.operation); err != nil {
		return err
	}
	defer h.operation.Unlock()
	ctx = call
	h.mu.Lock()
	threadID := h.snapshot.ThreadID
	uncertain := h.uncertain
	h.mu.Unlock()
	if uncertain {
		return ErrInputAcknowledgementMismatch
	}
	client, err := h.dialInitialized(ctx)
	if err != nil {
		return err
	}
	defer h.releaseClient(client)
	raw, err := client.request(ctx, "thread/read", map[string]any{
		"threadId":     threadID,
		"includeTurns": true,
	})
	if err != nil {
		return err
	}
	var response struct {
		Thread codexThread `json:"thread"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	thread := response.Thread
	if thread.ID != threadID {
		return ErrInputAcknowledgementMismatch
	}
	if err := validateCodexTurnHistory(thread.Turns); err != nil {
		return err
	}
	if thread.Status.Type != "active" {
		state.waitingSince = time.Time{}
		return nil
	}
	activeTurnID := uniqueInProgressCodexTurnID(thread.Turns)
	if activeTurnID == "" {
		state.waitingSince = time.Time{}
		return nil
	}
	var turn *codexTurn
	for index := len(thread.Turns) - 1; index >= 0; index-- {
		if thread.Turns[index].ID == activeTurnID && thread.Turns[index].Status == "inProgress" {
			turn = &thread.Turns[index]
			break
		}
	}
	if turn == nil {
		state.waitingSince = time.Time{}
		return nil
	}
	if state.turnID != turn.ID {
		*state = codexAutoContinueState{turnID: turn.ID}
	}
	flags := make(map[string]bool, len(thread.Status.ActiveFlags))
	for _, flag := range thread.Status.ActiveFlags {
		flags[flag] = true
	}
	if flags["waitingOnApproval"] {
		state.waitingSince = time.Time{}
		return nil
	}
	if !flags["waitingOnUserInput"] {
		state.waitingSince = time.Time{}
		return nil
	}
	if state.waitingSince.IsZero() {
		state.waitingSince = now
		return nil
	}
	delay := time.Duration(options.DelaySeconds) * time.Second
	if now.Sub(state.waitingSince) < delay ||
		(!state.lastNudged.IsZero() && now.Sub(state.lastNudged) < delay) ||
		state.nudges >= options.MaxNudgesPerTurn {
		return nil
	}
	if _, err := client.request(ctx, "turn/steer", map[string]any{
		"threadId":       threadID,
		"expectedTurnId": turn.ID,
		"input": []map[string]any{{
			"type":          "text",
			"text":          options.Prompt,
			"text_elements": []any{},
		}},
	}); err != nil {
		return err
	}
	state.nudges++
	state.lastNudged = now
	state.waitingSince = now
	h.logf("auto-continue nudged thread=%s turn=%s count=%d", threadID, turn.ID, state.nudges)
	return nil
}

func (h *codexRunner) logf(format string, values ...any) {
	h.mu.Lock()
	path := h.snapshot.LogPath
	h.mu.Unlock()
	if strings.TrimSpace(path) == "" {
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = fmt.Fprintf(
		file,
		"%s task-board-session-manager: %s\n",
		time.Now().UTC().Format(time.RFC3339Nano),
		fmt.Sprintf(format, values...),
	)
}
