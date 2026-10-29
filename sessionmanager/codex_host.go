package sessionmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	codexHostSnapshotContract = "task-board.codex-provider-host-v1"
	codexRPCRequestTimeout    = 15 * time.Second
	codexRPCReadLimit         = 64 << 20
	codexHostReadyTimeout     = 10 * time.Second
	codexOwnerPollPeriod      = 500 * time.Millisecond
	codexOwnerIdleLimit       = 10 * time.Second
	codexOwnerTurnVisibility  = 5 * time.Second
)

type codexRPCClient struct {
	gate               *codexGateClient
	conn               *websocket.Conn
	nextID             int64
	notifications      []codexRPCNotification
	subscribedThreadID string
}

type codexRPCRequest struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type codexRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Method  string `json:"-"`
}

func (e *codexRPCError) Error() string {
	return fmt.Sprintf("%s request failed (%d): %s", e.Method, e.Code, e.Message)
}

type codexRPCNotification struct {
	Method string
	Params json.RawMessage
}

type codexThreadGoal struct {
	ThreadID  string `json:"threadId"`
	Objective string `json:"objective"`
	Status    string `json:"status"`
}

type codexThread struct {
	ID           string            `json:"id"`
	SessionID    string            `json:"sessionId"`
	ForkedFromID string            `json:"forkedFromId"`
	CLIVersion   string            `json:"cliVersion"`
	Path         string            `json:"path"`
	Ephemeral    bool              `json:"ephemeral"`
	Status       codexThreadStatus `json:"status"`
	Turns        []codexTurn       `json:"turns"`
}

type codexThreadStatus struct {
	Type        string   `json:"type"`
	ActiveFlags []string `json:"activeFlags"`
}

type codexTurn struct {
	ID     string            `json:"id"`
	Status string            `json:"status"`
	Items  []json.RawMessage `json:"items"`
	Error  json.RawMessage   `json:"error"`
}

func dialCodexRPC(ctx context.Context, serverURL string) (*codexRPCClient, error) {
	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(connectCtx, serverURL, nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(codexRPCReadLimit)
	return &codexRPCClient{conn: conn, nextID: 1}, nil
}

func (c *codexRPCClient) close() {
	if c != nil && c.conn != nil {
		if c.gate != nil {
			c.gate.lost()
		}
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
	}
}

func (c *codexRPCClient) request(
	parent context.Context,
	method string,
	params any,
) (json.RawMessage, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("Codex app-server connection is not open")
	}
	id := fmt.Sprintf("task-board-session-manager-%d", c.nextID)
	c.nextID++
	ctx, cancel := context.WithTimeout(parent, codexRPCRequestTimeout)
	defer cancel()
	frame := codexRPCRequest{ID: id, Method: method, Params: params}
	write := func() error { return wsjson.Write(ctx, c.conn, frame) }
	var writeErr error
	if c.gate != nil {
		b, _ := json.Marshal(frame)
		writeErr = c.gate.write(ctx, b, true, write)
	} else {
		writeErr = write()
	}
	if err := writeErr; err != nil {
		return nil, err
	}
	for {
		var raw map[string]json.RawMessage
		if err := wsjson.Read(ctx, c.conn, &raw); err != nil {
			return nil, err
		}
		if c.gate != nil {
			b, _ := json.Marshal(raw)
			c.gate.observe(b)
		}
		if methodRaw := raw["method"]; len(methodRaw) > 0 {
			var notification codexRPCNotification
			if json.Unmarshal(methodRaw, &notification.Method) == nil {
				notification.Params = codexCloneRaw(raw["params"])
				c.notifications = append(c.notifications, notification)
			}
		}
		idRaw := raw["id"]
		if len(idRaw) == 0 {
			continue
		}
		var responseID string
		if json.Unmarshal(idRaw, &responseID) != nil || responseID != id {
			continue
		}
		if failure := raw["error"]; len(failure) > 0 && string(failure) != "null" {
			var rpcFailure codexRPCError
			if json.Unmarshal(failure, &rpcFailure) == nil && rpcFailure.Message != "" {
				rpcFailure.Method = method
				return nil, &rpcFailure
			}
			return nil, fmt.Errorf("%s request failed: %s", method, failure)
		}
		return raw["result"], nil
	}
}

func (c *codexRPCClient) notify(
	ctx context.Context,
	method string,
	params any,
) error {
	frame := map[string]any{"method": method, "params": params}
	write := func() error { return wsjson.Write(ctx, c.conn, frame) }
	if c.gate != nil {
		b, _ := json.Marshal(frame)
		return c.gate.write(ctx, b, true, write)
	}
	return write()
}

func (c *codexRPCClient) drainNotifications() []codexRPCNotification {
	if c == nil || len(c.notifications) == 0 {
		return nil
	}
	drained := append([]codexRPCNotification(nil), c.notifications...)
	c.notifications = c.notifications[:0]
	return drained
}

func (c *codexRPCClient) managedTurnEvidence(
	threadID string,
	turnID string,
	current ProviderUsage,
	allowAcknowledgementReconciliation bool,
) (ProviderUsage, string, []string, string) {
	return c.turnEvidenceFrom(
		c.drainNotifications(),
		threadID,
		turnID,
		current,
		allowAcknowledgementReconciliation,
	)
}

// turnEvidenceFrom is managedTurnEvidence over notifications the caller already
// drained.
//
// The split exists because the drain is destructive and two consumers need the
// same batch: turn correlation, and consumer-repository-observer-v1. Letting the
// observer drain separately would race the correlator for the same events and
// silently halve both views.
func (c *codexRPCClient) turnEvidenceFrom(
	notifications []codexRPCNotification,
	threadID string,
	turnID string,
	current ProviderUsage,
	allowAcknowledgementReconciliation bool,
) (ProviderUsage, string, []string, string) {
	candidates := make(map[string]bool)
	expectedSeen := false
	for _, notification := range notifications {
		if notification.Method != "turn/started" &&
			notification.Method != "turn/completed" {
			continue
		}
		var params struct {
			ThreadID string    `json:"threadId"`
			Turn     codexTurn `json:"turn"`
		}
		if json.Unmarshal(notification.Params, &params) != nil ||
			(params.ThreadID != threadID && !(params.ThreadID == "" && c.subscribedThreadID == threadID)) ||
			strings.TrimSpace(params.Turn.ID) == "" {
			continue
		}
		if params.Turn.ID == turnID {
			expectedSeen = true
		} else {
			candidates[params.Turn.ID] = true
		}
	}
	candidateTurnID := ""
	if allowAcknowledgementReconciliation &&
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

// A later provider event can contradict the unmaterialized preflight verdict.
// Only explicit same-thread evidence applies on a connection without a bind.
func legacyCodexNotificationsContradictFreshThread(notifications []codexRPCNotification, threadID string) bool {
	for _, notification := range notifications {
		var params struct {
			ThreadID string            `json:"threadId"`
			Thread   codexThread       `json:"thread"`
			Status   codexThreadStatus `json:"status"`
		}
		if json.Unmarshal(notification.Params, &params) != nil {
			continue
		}
		switch notification.Method {
		case "turn/started", "turn/completed", "item/started", "item/completed", "thread/tokenUsage/updated":
			if params.ThreadID == threadID {
				return true
			}
		case "thread/status/changed":
			if params.ThreadID == threadID && params.Status.Type != "idle" {
				return true
			}
		case "thread/started":
			if params.Thread.ID == threadID && (params.Thread.Status.Type != "idle" || len(params.Thread.Turns) != 0) {
				return true
			}
		}
	}
	return false
}

func codexTurnStatusRank(status string) int {
	switch strings.TrimSpace(status) {
	case "completed", "failed", "interrupted":
		return 2
	case "inProgress":
		return 1
	default:
		return 0
	}
}

func (c *codexRPCClient) managedIdentityRelation(
	notificationThreadID string,
	expectedThreadID string,
) string {
	if notificationThreadID == "" {
		if c.subscribedThreadID == expectedThreadID {
			return "implicit-bound"
		}
		return "missing-unbound"
	}
	return identityRelation(notificationThreadID, expectedThreadID)
}

func identityRelation(actual string, expected string) string {
	if actual == "" {
		return "missing"
	}
	if actual == expected {
		return "match"
	}
	return "mismatch"
}

func (c *codexRPCClient) matchesManagedTurn(
	notificationThreadID string,
	notificationTurnID string,
	expectedThreadID string,
	expectedTurnID string,
	allowImplicitTurn bool,
) bool {
	if notificationThreadID == "" {
		notificationThreadID = c.subscribedThreadID
	}
	if notificationTurnID == "" &&
		allowImplicitTurn &&
		notificationThreadID == c.subscribedThreadID {
		notificationTurnID = expectedTurnID
	}
	return notificationThreadID == expectedThreadID &&
		notificationTurnID == expectedTurnID
}

func reserveLoopbackWebsocketURL() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer listener.Close()
	return "ws://" + listener.Addr().String(), nil
}

func waitForCodexHostReady(
	ctx context.Context,
	serverURL string,
	timeout time.Duration,
) error {
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return err
	}
	parsed.Scheme = "http"
	parsed.Path = "/readyz"
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: time.Second}
	for {
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
		if requestErr != nil {
			return requestErr
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if requestErr != nil {
				return requestErr
			}
			return fmt.Errorf("ready endpoint returned %s", response.Status)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func findCodexTurn(thread *codexThread, turnID string) *codexTurn {
	if thread == nil {
		return nil
	}
	for index := range thread.Turns {
		if thread.Turns[index].ID == turnID {
			return &thread.Turns[index]
		}
	}
	return nil
}

func findCodexTurnInList(turns []codexTurn, turnID string) *codexTurn {
	for index := range turns {
		if turns[index].ID == turnID {
			return &turns[index]
		}
	}
	return nil
}

func uniqueInProgressCodexTurnID(groups ...[]codexTurn) string {
	ids := make(map[string]bool)
	for _, turns := range groups {
		for _, turn := range turns {
			if turn.Status == "inProgress" && strings.TrimSpace(turn.ID) != "" {
				ids[turn.ID] = true
			}
		}
	}
	if len(ids) != 1 {
		return ""
	}
	for id := range ids {
		return id
	}
	return ""
}

func listCodexThreadTurns(
	ctx context.Context,
	client *codexRPCClient,
	threadID string,
) ([]codexTurn, error) {
	raw, err := client.request(ctx, "thread/turns/list", map[string]any{
		"threadId":      threadID,
		"limit":         20,
		"sortDirection": "desc",
		"itemsView":     "notLoaded",
	})
	if err != nil {
		return nil, err
	}
	var response struct {
		Data []codexTurn `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decoding thread/turns/list: %w", err)
	}
	return response.Data, nil
}

func codexCloneRaw(b []byte) []byte { return append([]byte(nil), b...) }

func readCodexOwnerThread(
	ctx context.Context,
	client *codexRPCClient,
	threadID string,
) (*codexThread, error) {
	threadRaw, err := client.request(ctx, "thread/read", map[string]any{
		"threadId":     threadID,
		"includeTurns": true,
	})
	if err != nil {
		return nil, fmt.Errorf("reading managed Codex owner thread: %w", err)
	}
	var threadResponse struct {
		Thread codexThread `json:"thread"`
	}
	if err := json.Unmarshal(threadRaw, &threadResponse); err != nil {
		return nil, fmt.Errorf("decoding managed Codex owner thread: %w", err)
	}
	return &threadResponse.Thread, nil
}
