package sessionmanager

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// BuilderToolObservation reads Codex's own persisted turn model.  The MCP shim
// is intentionally not consulted here: a native shell/file/web item or a call
// to another MCP server exists in this provider view even though it can never
// appear in the repository gateway's trace.
func (h *codexRunner) BuilderToolObservation(ctx context.Context) (BuilderToolObservation, error) {
	h.mu.Lock()
	confined := h.builderConfinement != nil
	stopped := h.snapshot.Stopped
	threadID := strings.TrimSpace(h.snapshot.ThreadID)
	turnID := strings.TrimSpace(h.snapshot.LastCompletedTurnID)
	if turnID == "" {
		turnID = strings.TrimSpace(h.snapshot.ManagedTurnID)
	}
	h.mu.Unlock()
	if !confined {
		return BuilderToolObservation{}, fmt.Errorf(
			"Codex host was not launched as a confined context builder")
	}
	if stopped {
		return BuilderToolObservation{Reported: true, Disconnected: true}, nil
	}
	if threadID == "" || turnID == "" {
		return BuilderToolObservation{}, fmt.Errorf(
			"Codex adapter has no manager-owned builder turn identity")
	}
	ctx, finish, err := h.begin(ctx)
	if err != nil {
		return BuilderToolObservation{}, err
	}
	defer finish()
	client, err := h.dialInitialized(ctx)
	if err != nil {
		return BuilderToolObservation{Reported: true, Disconnected: true}, nil
	}
	defer h.releaseClient(client)
	thread, err := readCodexThread(ctx, client, threadID)
	if err != nil {
		return BuilderToolObservation{Reported: true, Disconnected: true}, nil
	}
	if thread == nil || thread.ID != threadID {
		return BuilderToolObservation{Reported: true, UnknownShapes: 1}, nil
	}
	turn := findCodexTurn(thread, turnID)
	if turn == nil {
		return BuilderToolObservation{Reported: true, UnknownShapes: 1}, nil
	}
	observation := observeCodexBuilderItems(turn.Items)
	observation.Reported = true
	if strings.TrimSpace(turn.Status) != "completed" {
		observation.Disconnected = true
	}
	return observation, nil
}

func observeCodexBuilderItems(items []json.RawMessage) BuilderToolObservation {
	observation := BuilderToolObservation{Reported: true}
	for _, raw := range items {
		var item struct {
			Type      string  `json:"type"`
			ID        string  `json:"id"`
			Server    string  `json:"server"`
			Tool      string  `json:"tool"`
			Status    string  `json:"status"`
			Namespace *string `json:"namespace"`
		}
		if err := json.Unmarshal(raw, &item); err != nil || strings.TrimSpace(item.Type) == "" {
			observation.UnknownShapes++
			continue
		}
		switch item.Type {
		case "userMessage", "agentMessage", "reasoning", "plan":
			// Provider-native inert items.  Their schemas contain no executable
			// tool identity and cannot represent repository access.
		case "mcpToolCall":
			if strings.TrimSpace(item.ID) == "" ||
				strings.TrimSpace(item.Server) == "" ||
				strings.TrimSpace(item.Tool) == "" ||
				!codexBuilderToolStatusKnown(item.Status) {
				observation.UnknownShapes++
				continue
			}
			if item.Server == builderGatewayServerName && builderGatewayShimServesTool(item.Tool) {
				observation.GatewayCalls++
			} else {
				observation.ForeignToolCalls++
			}
		case "dynamicToolCall":
			if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.Tool) == "" ||
				!codexBuilderToolStatusKnown(item.Status) ||
				(item.Namespace != nil && strings.TrimSpace(*item.Namespace) != "") {
				observation.UnknownShapes++
				continue
			}
			if builderGatewayShimServesTool(item.Tool) {
				observation.GatewayCalls++
			} else {
				observation.ForeignToolCalls++
			}
		case "hookPrompt", "commandExecution", "fileChange",
			"collabAgentToolCall", "subAgentActivity", "webSearch", "imageView",
			"sleep", "imageGeneration":
			observation.ForeignToolCalls++
		default:
			// New passive and executable item kinds are both protocol drift until
			// this adapter classifies their current provider schema.
			observation.UnknownShapes++
		}
	}
	return observation
}

func codexBuilderToolStatusKnown(status string) bool {
	switch strings.TrimSpace(status) {
	case "completed", "failed":
		return true
	default:
		return false
	}
}

// BuilderToolObservation reads only records appended after the captured launch
// boundary.  It classifies provider-native tool_use records independently of
// the MCP shim and treats an incomplete transcript or process exit as a
// disconnect rather than as an empty tool stream.
func (h *claudeHost) BuilderToolObservation(_ context.Context) (BuilderToolObservation, error) {
	h.mu.Lock()
	confined := h.builderConfinement != nil
	path := h.builderTranscriptPath
	offset := h.builderTranscriptOffset
	stopped := h.stopped || h.snapshot.Stopped
	h.mu.Unlock()
	if !confined {
		return BuilderToolObservation{}, fmt.Errorf(
			"Claude host was not launched as a confined context builder")
	}
	if strings.TrimSpace(path) == "" {
		return BuilderToolObservation{}, fmt.Errorf(
			"Claude adapter captured no builder transcript boundary")
	}
	observation, terminal, err := observeClaudeBuilderTranscript(path, offset)
	if err != nil {
		if stopped || os.IsNotExist(err) {
			return BuilderToolObservation{Reported: true, Disconnected: true}, nil
		}
		return BuilderToolObservation{}, err
	}
	observation.Reported = true
	if stopped {
		observation.Disconnected = true
	} else if !terminal {
		// Finalization raced a still-active or structurally incomplete provider
		// stream.  It is unclassifiable evidence, not an observed zero-call run.
		observation.UnknownShapes++
	}
	return observation, nil
}

func observeClaudeBuilderTranscript(path string, offset int64) (BuilderToolObservation, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return BuilderToolObservation{}, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return BuilderToolObservation{}, false, err
	}
	if offset < 0 || info.Size() < offset {
		return BuilderToolObservation{}, false, fmt.Errorf(
			"Claude builder transcript shrank below its launch boundary")
	}
	if remaining := info.Size() - offset; remaining > claudeTranscriptSizeLimit {
		return BuilderToolObservation{}, false, fmt.Errorf(
			"Claude builder transcript suffix is %d bytes, above the adapter limit", remaining)
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return BuilderToolObservation{}, false, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, claudeTranscriptSizeLimit+1))
	if err != nil {
		return BuilderToolObservation{}, false, err
	}
	observation, terminal := inspectClaudeBuilderTranscript(raw)
	return observation, terminal, nil
}

func inspectClaudeBuilderTranscript(raw []byte) (BuilderToolObservation, bool) {
	observation := BuilderToolObservation{Reported: true}
	toolUses := make(map[string]struct{})
	terminal := false
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), claudeTranscriptSizeLimit)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record struct {
			Type    string `json:"type"`
			Message *struct {
				Role       string          `json:"role"`
				StopReason string          `json:"stop_reason"`
				Content    json.RawMessage `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(line, &record); err != nil || strings.TrimSpace(record.Type) == "" {
			observation.UnknownShapes++
			continue
		}
		if record.Message == nil {
			switch record.Type {
			case "attachment", "file-history-snapshot", "progress", "queue-operation",
				"summary", "system", "custom-title":
			default:
				observation.UnknownShapes++
			}
			continue
		}
		if record.Type != "assistant" && record.Type != "user" {
			observation.UnknownShapes++
			continue
		}
		observeClaudeBuilderContent(record.Message.Content, toolUses, &observation)
		if record.Type == "assistant" {
			if _, ok := claudeTerminalStopReasons[strings.TrimSpace(record.Message.StopReason)]; ok {
				terminal = true
			}
		}
	}
	if scanner.Err() != nil {
		observation.UnknownShapes++
	}
	return observation, terminal
}

func observeClaudeBuilderContent(
	content json.RawMessage,
	toolUses map[string]struct{},
	observation *BuilderToolObservation,
) {
	trimmed := bytes.TrimSpace(content)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		observation.UnknownShapes++
		return
	}
	if trimmed[0] == '"' {
		return
	}
	if trimmed[0] != '[' {
		observation.UnknownShapes++
		return
	}
	var blocks []json.RawMessage
	if err := json.Unmarshal(trimmed, &blocks); err != nil {
		observation.UnknownShapes++
		return
	}
	for _, raw := range blocks {
		var block struct {
			Type      string `json:"type"`
			ID        string `json:"id"`
			Name      string `json:"name"`
			ToolUseID string `json:"tool_use_id"`
		}
		if err := json.Unmarshal(raw, &block); err != nil || strings.TrimSpace(block.Type) == "" {
			observation.UnknownShapes++
			continue
		}
		switch block.Type {
		case "text", "thinking":
		case "tool_use", "mcp_tool_use":
			id := strings.TrimSpace(block.ID)
			name := strings.TrimSpace(block.Name)
			if id == "" || name == "" {
				observation.UnknownShapes++
				continue
			}
			toolUses[id] = struct{}{}
			if isClaudeBuilderGatewayTool(name) {
				observation.GatewayCalls++
			} else {
				observation.ForeignToolCalls++
			}
		case "server_tool_use":
			observation.ForeignToolCalls++
		case "tool_result":
			if _, ok := toolUses[strings.TrimSpace(block.ToolUseID)]; !ok {
				observation.UnknownShapes++
			}
		default:
			observation.UnknownShapes++
		}
	}
}

func isClaudeBuilderGatewayTool(name string) bool {
	for _, allowed := range claudeBuilderGatewayToolNames() {
		if name == allowed {
			return true
		}
	}
	return false
}
