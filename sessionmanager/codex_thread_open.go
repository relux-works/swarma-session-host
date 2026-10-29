package sessionmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// openCodexRunnerThread keeps the original start/adoption policy and public
// resume refusals on the resolved thread. It does not interpret owner authority.
func openCodexRunnerThread(ctx context.Context, client *codexRPCClient, snapshot CodexRunnerSnapshot, launch LaunchSpec, model string) (string, []string, error) {
	params := map[string]any{"cwd": launch.Cwd}
	if model != "" {
		params["model"] = model
	}
	if launch.ToolPolicy.Approval != "" {
		params["approvalPolicy"] = launch.ToolPolicy.Approval
	}
	if launch.ToolPolicy.Sandbox != "" {
		params["sandbox"] = launch.ToolPolicy.Sandbox
	}
	method := "thread/start"
	threadID := snapshot.ThreadID
	if threadID != "" {
		method = "thread/resume"
		params["threadId"] = threadID
	}
	raw, err := client.request(ctx, method, params)
	if err != nil {
		if threadID != "" {
			return "", nil, &PublicError{Code: CodexErrorThreadResumeFailed, Message: fmt.Sprintf("Codex thread %s exists in the selected store but app-server could not resume it; verify it with \u0060codex resume %s\u0060, inspect \u0060task-board session logs\u0060, and retry", threadID, threadID)}
		}
		return "", nil, fmt.Errorf("starting manager-owned Codex thread: %w", err)
	}
	var response struct {
		Thread             codexThread `json:"thread"`
		InstructionSources []string    `json:"instructionSources"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		if threadID != "" {
			return "", nil, &PublicError{Code: CodexErrorThreadResumeFailed, Message: fmt.Sprintf("decoding app-server resume response for Codex thread %s: %v", threadID, err)}
		}
		return "", nil, fmt.Errorf("decoding thread/start response: %w", err)
	}
	if threadID != "" {
		if !strings.EqualFold(strings.TrimSpace(response.Thread.ID), threadID) {
			return "", nil, &PublicError{Code: CodexErrorThreadResumeFailed, Message: fmt.Sprintf("app-server resumed Codex thread %q instead of requested thread %s", response.Thread.ID, threadID)}
		}
	} else {
		threadID = response.Thread.ID
		if strings.TrimSpace(threadID) == "" {
			return "", nil, fmt.Errorf("thread/start returned no thread ID")
		}
	}
	return threadID, append([]string(nil), response.InstructionSources...), nil
}
