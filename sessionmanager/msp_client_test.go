package sessionmanager

// The original wire roots exercise the actual external client over isolated
// in-process pipes: typed success/error/correlation, cancellation recovery,
// malformed frames, close and write-drain behavior. No provider process or
// real credential is involved; native launch and schema capture qualification
// are separate integration coverage.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/build"
	"go/parser"
	"go/token"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeMSPFrame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"`
	Error   *MSPServerError `json:"error"`
}

type fakeMSPPeer struct {
	client       *MSPClient
	requests     *bufio.Reader
	requestInput *io.PipeReader
	responseOut  *io.PipeWriter
}

type observedMSPWriter struct {
	io.WriteCloser
	started      chan struct{}
	finished     chan struct{}
	startedOnce  sync.Once
	finishedOnce sync.Once
	mu           sync.Mutex
	total        int
	written      int
}

func (w *observedMSPWriter) Write(p []byte) (int, error) {
	w.startedOnce.Do(func() {
		w.mu.Lock()
		w.total = len(p)
		w.mu.Unlock()
		close(w.started)
	})
	n, err := w.WriteCloser.Write(p)
	w.mu.Lock()
	w.written += n
	if w.written >= w.total {
		w.finishedOnce.Do(func() { close(w.finished) })
	}
	w.mu.Unlock()
	return n, err
}

func newBlockedMSPClient(t *testing.T, osPipes bool) (*MSPClient, <-chan struct{}, <-chan struct{}, func()) {
	t.Helper()
	var requestReader io.ReadCloser
	var requestWriter io.WriteCloser
	var responseReader io.ReadCloser
	var responseWriter io.WriteCloser
	if osPipes {
		var err error
		requestReaderFile, requestWriterFile, err := os.Pipe()
		if err != nil {
			t.Fatalf("create request OS pipe: %v", err)
		}
		responseReaderFile, responseWriterFile, err := os.Pipe()
		if err != nil {
			_ = requestReaderFile.Close()
			_ = requestWriterFile.Close()
			t.Fatalf("create response OS pipe: %v", err)
		}
		requestReader, requestWriter = requestReaderFile, requestWriterFile
		responseReader, responseWriter = responseReaderFile, responseWriterFile
	} else {
		requestReaderPipe, requestWriterPipe := io.Pipe()
		responseReaderPipe, responseWriterPipe := io.Pipe()
		requestReader, requestWriter = requestReaderPipe, requestWriterPipe
		responseReader, responseWriter = responseReaderPipe, responseWriterPipe
	}
	writer := &observedMSPWriter{
		WriteCloser: requestWriter,
		started:     make(chan struct{}),
		finished:    make(chan struct{}),
	}
	client := NewMSPClient(responseReader, writer)
	unblockServer := func() {
		_ = requestReader.Close()
		_ = responseWriter.Close()
	}
	t.Cleanup(unblockServer)
	t.Cleanup(func() { _ = client.Close() })
	return client, writer.started, writer.finished, unblockServer
}

func startLargeBlockedTurn(t *testing.T, client *MSPClient, ctx context.Context, writeStarted, writeFinished <-chan struct{}) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := client.TurnStart(ctx, MSPTurnStartParams{
			CommandID: "blocked-turn", SessionID: "session-1",
			Input:  []MSPTurnInputPart{{Type: "text", Text: strings.Repeat("x", 1<<20)}},
			IfBusy: MSPIfBusyQueue,
		})
		done <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("request writer was not entered")
	}
	select {
	case <-writeFinished:
		t.Fatal("large turn/start write completed although the fake server never read stdin")
	// NOTHING-happens window (r44): the blocked 1 MiB write must stay blocked while stdin is unread.
	case <-time.After(25 * time.Millisecond):
	}
	return done
}

func newFakeMSPPeer(t *testing.T) *fakeMSPPeer {
	t.Helper()
	requestReader, requestWriter := io.Pipe()
	responseReader, responseWriter := io.Pipe()
	peer := &fakeMSPPeer{
		client:       NewMSPClient(responseReader, requestWriter),
		requests:     bufio.NewReader(requestReader),
		requestInput: requestReader,
		responseOut:  responseWriter,
	}
	t.Cleanup(func() {
		_ = peer.client.Close()
		_ = peer.requestInput.Close()
		_ = peer.responseOut.Close()
	})
	return peer
}

func (p *fakeMSPPeer) readFrame(t *testing.T) fakeMSPFrame {
	t.Helper()
	type readResult struct {
		frame fakeMSPFrame
		err   error
	}
	read := make(chan readResult, 1)
	go func() {
		line, err := p.requests.ReadBytes('\n')
		if err != nil {
			read <- readResult{err: err}
			return
		}
		var frame fakeMSPFrame
		err = json.Unmarshal(line, &frame)
		read <- readResult{frame: frame, err: err}
	}()
	select {
	case result := <-read:
		if result.err != nil {
			t.Fatalf("read client frame: %v", result.err)
		}
		if result.frame.JSONRPC != mspJSONRPCVersion {
			t.Fatalf("client jsonrpc = %q, want %q", result.frame.JSONRPC, mspJSONRPCVersion)
		}
		return result.frame
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for client frame")
		return fakeMSPFrame{}
	}
}

func (p *fakeMSPPeer) respond(t *testing.T, id json.RawMessage, result any) {
	t.Helper()
	p.writeFrame(t, map[string]any{
		"jsonrpc": mspJSONRPCVersion,
		"id":      id,
		"result":  result,
	})
}

func (p *fakeMSPPeer) respondError(t *testing.T, id json.RawMessage, code int, message string, data any) {
	t.Helper()
	encodedData, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	p.writeFrame(t, struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   *MSPServerError `json:"error"`
	}{
		JSONRPC: mspJSONRPCVersion,
		ID:      id,
		Error: &MSPServerError{
			Code:    code,
			Message: message,
			Data:    encodedData,
		},
	})
}

func (p *fakeMSPPeer) notification(t *testing.T, method string, params any) {
	t.Helper()
	p.writeFrame(t, map[string]any{
		"jsonrpc": mspJSONRPCVersion,
		"method":  method,
		"params":  params,
	})
}

func (p *fakeMSPPeer) writeFrame(t *testing.T, frame any) {
	t.Helper()
	encoded, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.responseOut.Write(append(encoded, '\n')); err != nil {
		t.Fatalf("write server frame: %v", err)
	}
}

func TestMSPClientTypedMethods(t *testing.T) {
	provider := "echo"
	workspace := "/workspace"
	falseValue := false
	trueValue := true
	cases := []struct {
		name          string
		method        string
		params        any
		wantParams    map[string]any
		response      any
		invoke        func(*MSPClient, context.Context) (any, error)
		wantType      reflect.Type
		checkResponse func(*testing.T, any)
		initialized   bool
	}{
		{
			name:   "initialize",
			method: "initialize",
			params: MSPInitializeParams{
				ClientInfo:   MSPClientInfo{Name: "task_board", Version: "test"},
				Capabilities: &MSPClientCapabilities{ExperimentalAPI: &falseValue},
			},
			wantParams: map[string]any{
				"clientInfo":   map[string]any{"name": "task_board", "version": "test"},
				"capabilities": map[string]any{"experimentalApi": false},
			},
			response: map[string]any{
				"experimentalApi": false, "grantedCapabilities": []string{},
				"museHome": "/tmp/muse", "platformFamily": "unix", "platformOs": "macos",
				"schema":     map[string]any{"fingerprint": "sha256:test", "version": 1},
				"serverInfo": map[string]any{"name": "muse", "version": "1.4.0"}, "userAgent": "test-agent",
			},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.Initialize(ctx, MSPInitializeParams{
					ClientInfo:   MSPClientInfo{Name: "task_board", Version: "test"},
					Capabilities: &MSPClientCapabilities{ExperimentalAPI: &falseValue},
				})
			},
			wantType: reflect.TypeOf(MSPInitializeResult{}),
			checkResponse: func(t *testing.T, value any) {
				got := value.(MSPInitializeResult)
				if got.Schema.Fingerprint != "sha256:test" || got.ServerInfo.Version != "1.4.0" {
					t.Fatalf("initialize result = %#v", got)
				}
			},
			initialized: true,
		},
		{
			name:       "session-start",
			method:     "session/start",
			params:     MSPSessionStartParams{CommandID: "cmd-start", ProviderID: &provider, WorkspaceRoot: workspace},
			wantParams: map[string]any{"commandId": "cmd-start", "providerId": "echo", "workspaceRoot": workspace},
			response:   map[string]any{"session": testMSPSession("session-started"), "viewCursor": "cursor-1"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.SessionStart(ctx, MSPSessionStartParams{CommandID: "cmd-start", ProviderID: &provider, WorkspaceRoot: workspace})
			},
			wantType: reflect.TypeOf(MSPSessionStartResult{}),
			checkResponse: func(t *testing.T, value any) {
				if got := value.(MSPSessionStartResult).Session.SessionID; got != "session-started" {
					t.Fatalf("session/start result sessionId = %q", got)
				}
			},
		},
		{
			name:       "session-resume",
			method:     "session/resume",
			params:     MSPSessionResumeParams{CommandID: "cmd-resume", SessionID: "session-existing", History: MSPHistoryInline},
			wantParams: map[string]any{"commandId": "cmd-resume", "sessionId": "session-existing", "history": "inline"},
			response: map[string]any{
				"history":         map[string]any{"items": []any{}, "mode": "inline", "snapshot": nil},
				"pendingRequests": []any{}, "session": testMSPSession("session-existing"), "viewCursor": "cursor-2",
			},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.SessionResume(ctx, MSPSessionResumeParams{CommandID: "cmd-resume", SessionID: "session-existing", History: MSPHistoryInline})
			},
			wantType: reflect.TypeOf(MSPSessionResumeResult{}),
			checkResponse: func(t *testing.T, value any) {
				if got := value.(MSPSessionResumeResult).Session.SessionID; got != "session-existing" {
					t.Fatalf("session/resume result sessionId = %q", got)
				}
			},
		},
		{
			name: "goal-set", method: "goal/set",
			params:     MSPGoalSetParams{CommandID: "cmd-goal-set", SessionID: "session-1", Objective: "ship"},
			wantParams: map[string]any{"commandId": "cmd-goal-set", "sessionId": "session-1", "objective": "ship"},
			response:   map[string]any{"commandId": "cmd-goal-set", "status": "accepted", "turnId": "turn-1"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.GoalSet(ctx, MSPGoalSetParams{CommandID: "cmd-goal-set", SessionID: "session-1", Objective: "ship"})
			},
			wantType: reflect.TypeOf(MSPGoalCommandResult{}),
			checkResponse: func(t *testing.T, value any) {
				if got := value.(MSPGoalCommandResult).TurnID; got != "turn-1" {
					t.Fatalf("goal/set result turnId = %q", got)
				}
			},
		},
		{
			name: "goal-edit", method: "goal/edit",
			params:     MSPGoalEditParams{CommandID: "cmd-goal-edit", SessionID: "session-1", Objective: "ship faster"},
			wantParams: map[string]any{"commandId": "cmd-goal-edit", "sessionId": "session-1", "objective": "ship faster"},
			response:   map[string]any{"commandId": "cmd-goal-edit", "status": "accepted"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.GoalEdit(ctx, MSPGoalEditParams{CommandID: "cmd-goal-edit", SessionID: "session-1", Objective: "ship faster"})
			},
			wantType: reflect.TypeOf(MSPGoalCommandResult{}),
		},
		{
			name: "goal-pause", method: "goal/pause",
			params:     MSPGoalPauseParams{CommandID: "cmd-goal-pause", SessionID: "session-1"},
			wantParams: map[string]any{"commandId": "cmd-goal-pause", "sessionId": "session-1"},
			response:   map[string]any{"commandId": "cmd-goal-pause", "status": "accepted"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.GoalPause(ctx, MSPGoalPauseParams{CommandID: "cmd-goal-pause", SessionID: "session-1"})
			},
			wantType: reflect.TypeOf(MSPGoalCommandResult{}),
		},
		{
			name: "goal-resume", method: "goal/resume",
			params:     MSPGoalResumeParams{CommandID: "cmd-goal-resume", SessionID: "session-1"},
			wantParams: map[string]any{"commandId": "cmd-goal-resume", "sessionId": "session-1"},
			response:   map[string]any{"commandId": "cmd-goal-resume", "status": "accepted"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.GoalResume(ctx, MSPGoalResumeParams{CommandID: "cmd-goal-resume", SessionID: "session-1"})
			},
			wantType: reflect.TypeOf(MSPGoalCommandResult{}),
		},
		{
			name: "goal-clear", method: "goal/clear",
			params:     MSPGoalClearParams{CommandID: "cmd-goal-clear", SessionID: "session-1"},
			wantParams: map[string]any{"commandId": "cmd-goal-clear", "sessionId": "session-1"},
			response:   map[string]any{"commandId": "cmd-goal-clear", "status": "accepted"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.GoalClear(ctx, MSPGoalClearParams{CommandID: "cmd-goal-clear", SessionID: "session-1"})
			},
			wantType: reflect.TypeOf(MSPGoalCommandResult{}),
		},
		{
			name: "turn-start-queue", method: "turn/start",
			params:     MSPTurnStartParams{CommandID: "cmd-turn-queue", SessionID: "session-1", Input: []MSPTurnInputPart{{Type: "text", Text: "hello"}}, IfBusy: MSPIfBusyQueue},
			wantParams: map[string]any{"commandId": "cmd-turn-queue", "sessionId": "session-1", "input": []any{map[string]any{"type": "text", "text": "hello"}}, "ifBusy": "queue"},
			response:   map[string]any{"commandId": "cmd-turn-queue", "disposition": "queued", "startedNewTurn": false, "status": "accepted", "turnId": "turn-queue"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.TurnStart(ctx, MSPTurnStartParams{CommandID: "cmd-turn-queue", SessionID: "session-1", Input: []MSPTurnInputPart{{Type: "text", Text: "hello"}}, IfBusy: MSPIfBusyQueue})
			},
			wantType: reflect.TypeOf(MSPTurnStartResult{}),
		},
		{
			name: "turn-start-steer", method: "turn/start",
			params:     MSPTurnStartParams{CommandID: "cmd-turn-steer", SessionID: "session-1", Input: []MSPTurnInputPart{{Type: "text", Text: "hello"}}, IfBusy: MSPIfBusySteer},
			wantParams: map[string]any{"commandId": "cmd-turn-steer", "sessionId": "session-1", "input": []any{map[string]any{"type": "text", "text": "hello"}}, "ifBusy": "steer"},
			response:   map[string]any{"commandId": "cmd-turn-steer", "disposition": "steered", "startedNewTurn": false, "status": "accepted", "turnId": "turn-active"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.TurnStart(ctx, MSPTurnStartParams{CommandID: "cmd-turn-steer", SessionID: "session-1", Input: []MSPTurnInputPart{{Type: "text", Text: "hello"}}, IfBusy: MSPIfBusySteer})
			},
			wantType: reflect.TypeOf(MSPTurnStartResult{}),
		},
		{
			name: "turn-start-replace", method: "turn/start",
			params:     MSPTurnStartParams{CommandID: "cmd-turn-replace", SessionID: "session-1", Input: []MSPTurnInputPart{{Type: "text", Text: "hello"}}, IfBusy: MSPIfBusyReplace},
			wantParams: map[string]any{"commandId": "cmd-turn-replace", "sessionId": "session-1", "input": []any{map[string]any{"type": "text", "text": "hello"}}, "ifBusy": "replace"},
			response:   map[string]any{"commandId": "cmd-turn-replace", "disposition": "started", "startedNewTurn": true, "status": "accepted", "turnId": "turn-new"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.TurnStart(ctx, MSPTurnStartParams{CommandID: "cmd-turn-replace", SessionID: "session-1", Input: []MSPTurnInputPart{{Type: "text", Text: "hello"}}, IfBusy: MSPIfBusyReplace})
			},
			wantType: reflect.TypeOf(MSPTurnStartResult{}),
		},
		{
			name: "turn-steer", method: "turn/steer",
			params:     MSPTurnSteerParams{CommandID: "cmd-steer", SessionID: "session-1", ExpectedTurnID: "turn-active", Input: []MSPTurnInputPart{{Type: "text", Text: "more"}}, ReasoningEffort: "high"},
			wantParams: map[string]any{"commandId": "cmd-steer", "sessionId": "session-1", "expectedTurnId": "turn-active", "input": []any{map[string]any{"type": "text", "text": "more"}}, "reasoningEffort": "high"},
			response:   map[string]any{"commandId": "cmd-steer", "status": "accepted", "turnId": "turn-active"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.TurnSteer(ctx, MSPTurnSteerParams{CommandID: "cmd-steer", SessionID: "session-1", ExpectedTurnID: "turn-active", Input: []MSPTurnInputPart{{Type: "text", Text: "more"}}, ReasoningEffort: "high"})
			},
			wantType: reflect.TypeOf(MSPTurnSteerResult{}),
		},
		{
			name: "turn-interrupt", method: "turn/interrupt",
			params:     MSPTurnInterruptParams{CommandID: "cmd-interrupt", SessionID: "session-1", TurnID: "turn-active", Retract: true},
			wantParams: map[string]any{"commandId": "cmd-interrupt", "sessionId": "session-1", "turnId": "turn-active", "retract": true},
			response:   map[string]any{"commandId": "cmd-interrupt", "status": "accepted", "turnId": "turn-active"},
			invoke: func(client *MSPClient, ctx context.Context) (any, error) {
				return client.TurnInterrupt(ctx, MSPTurnInterruptParams{CommandID: "cmd-interrupt", SessionID: "session-1", TurnID: "turn-active", Retract: true})
			},
			wantType: reflect.TypeOf(MSPTurnInterruptResult{}),
		},
	}
	_ = trueValue

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			peer := newFakeMSPPeer(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			type callResult struct {
				value any
				err   error
			}
			done := make(chan callResult, 1)
			go func() {
				value, err := test.invoke(peer.client, ctx)
				done <- callResult{value: value, err: err}
			}()

			request := peer.readFrame(t)
			if request.Method != test.method {
				t.Fatalf("request method = %q, want %q", request.Method, test.method)
			}
			var gotParams map[string]any
			if err := json.Unmarshal(request.Params, &gotParams); err != nil {
				t.Fatalf("decode %s params: %v", test.method, err)
			}
			if !reflect.DeepEqual(gotParams, test.wantParams) {
				t.Fatalf("%s params = %#v, want %#v", test.method, gotParams, test.wantParams)
			}
			peer.respond(t, request.ID, test.response)
			if test.initialized {
				initialized := peer.readFrame(t)
				if initialized.Method != "initialized" || len(initialized.ID) != 0 || len(initialized.Params) != 0 {
					t.Fatalf("initialize follow-up = %#v, want response-free initialized notification", initialized)
				}
			}
			select {
			case result := <-done:
				if result.err != nil {
					t.Fatalf("%s: %v", test.method, result.err)
				}
				if got := reflect.TypeOf(result.value); got != test.wantType {
					t.Fatalf("%s result type = %v, want %v", test.method, got, test.wantType)
				}
				if test.checkResponse != nil {
					test.checkResponse(t, result.value)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s did not resolve", test.method)
			}
		})
	}
}

func TestMSPClientFilesUseOnlyStandardLibraryImports(t *testing.T) {
	for _, filename := range []string{"msp_client.go", "msp_types.go", "msp_smoke_test.go"} {
		parsed, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports from %s: %v", filename, err)
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("decode import path in %s: %v", filename, err)
			}
			pkg, err := build.Default.Import(importPath, ".", build.FindOnly)
			if err != nil || !pkg.Goroot {
				t.Errorf("%s imports non-standard package %q (Goroot=%t, error=%v)", filename, importPath, pkg != nil && pkg.Goroot, err)
			}
		}
	}
}

func TestMSPClientReturnsTypedServerError(t *testing.T) {
	peer := newFakeMSPPeer(t)
	done := make(chan error, 1)
	go func() {
		_, err := peer.client.GoalSet(context.Background(), MSPGoalSetParams{
			CommandID: "cmd-invalid", SessionID: "session-1", Objective: "",
		})
		done <- err
	}()
	request := peer.readFrame(t)
	peer.respondError(t, request.ID, -32602, "invalidParams", map[string]any{"kind": "invalidParams"})
	err := <-done
	var serverErr *MSPServerError
	if !errors.As(err, &serverErr) {
		t.Fatalf("error = %T %v, want *MSPServerError", err, err)
	}
	if serverErr.Code != -32602 || serverErr.Message != "invalidParams" || !strings.Contains(string(serverErr.Data), "invalidParams") {
		t.Fatalf("server error = %#v", serverErr)
	}
}

func TestMSPClientCorrelatesOutOfOrderResponses(t *testing.T) {
	peer := newFakeMSPPeer(t)
	type callResult struct {
		name  string
		value any
		err   error
	}
	done := make(chan callResult, 2)
	go func() {
		value, err := peer.client.SessionStart(context.Background(), MSPSessionStartParams{CommandID: "start"})
		done <- callResult{name: "session/start", value: value, err: err}
	}()
	go func() {
		value, err := peer.client.GoalSet(context.Background(), MSPGoalSetParams{CommandID: "goal", SessionID: "session-1", Objective: "x"})
		done <- callResult{name: "goal/set", value: value, err: err}
	}()
	first := peer.readFrame(t)
	second := peer.readFrame(t)
	if first.ID == nil || second.ID == nil || bytesEqual(first.ID, second.ID) {
		t.Fatalf("request ids are not unique: %s and %s", first.ID, second.ID)
	}
	responseFor := func(request fakeMSPFrame) any {
		switch request.Method {
		case "session/start":
			return map[string]any{"session": testMSPSession("started"), "viewCursor": "c"}
		case "goal/set":
			return map[string]any{"commandId": "goal", "status": "accepted"}
		default:
			t.Fatalf("unexpected request method %q", request.Method)
			return nil
		}
	}
	peer.respond(t, second.ID, responseFor(second))
	peer.respond(t, first.ID, responseFor(first))
	results := map[string]any{}
	for range 2 {
		result := <-done
		if result.err != nil {
			t.Fatalf("%s: %v", result.name, result.err)
		}
		results[result.name] = result.value
	}
	if got := results["session/start"].(MSPSessionStartResult).Session.SessionID; got != "started" {
		t.Fatalf("session/start result = %q", got)
	}
	if got := results["goal/set"].(MSPGoalCommandResult).CommandID; got != "goal" {
		t.Fatalf("goal/set result = %q", got)
	}
}

func TestMSPClientInterleavesNotificationsWithResponses(t *testing.T) {
	peer := newFakeMSPPeer(t)
	done := make(chan error, 1)
	go func() {
		_, err := peer.client.SessionStart(context.Background(), MSPSessionStartParams{CommandID: "start"})
		done <- err
	}()
	request := peer.readFrame(t)
	peer.notification(t, "session/goalChanged", map[string]any{
		"sessionId": "session-1", "goal": map[string]any{"objective": "ship", "percentComplete": 0, "status": "active"},
		"sourceRange": map[string]any{}, "viewCursor": "cursor-1",
	})
	peer.respond(t, request.ID, map[string]any{"session": testMSPSession("session-1"), "viewCursor": "cursor-2"})
	if err := <-done; err != nil {
		t.Fatalf("session/start: %v", err)
	}
	select {
	case notification := <-peer.client.Notifications():
		goal, ok := notification.(*MSPSessionGoalChangedNotification)
		if !ok || goal.SessionID != "session-1" || !goal.Goal.Present || goal.Goal.Null || goal.Goal.Value.Objective != "ship" {
			t.Fatalf("notification = %#v", notification)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session/goalChanged was not delivered")
	}
}

func TestMSPClientDecodesGoalStatusTokenAndTurnNotifications(t *testing.T) {
	peer := newFakeMSPPeer(t)
	cases := []struct {
		method string
		params any
		want   reflect.Type
	}{
		{"session/goalChanged", map[string]any{"sessionId": "s", "goal": nil, "sourceRange": map[string]any{}, "viewCursor": "1"}, reflect.TypeOf(&MSPSessionGoalChangedNotification{})},
		{"session/statusChanged", map[string]any{"sessionId": "s", "status": "loaded", "viewCursor": nil}, reflect.TypeOf(&MSPSessionStatusChangedNotification{})},
		{"session/tokenUsage", map[string]any{
			"cumulative":   map[string]any{"outputTokens": 2, "promptTokens": 3, "totalTokens": 5},
			"promptTokens": 3, "sessionId": "s", "sourceRange": map[string]any{}, "totalTokens": 5,
			"turnId": "t", "usage": map[string]any{"cachedTokens": 0, "inputTokens": 3, "outputTokens": 2, "reasoningTokens": 0}, "viewCursor": "2",
		}, reflect.TypeOf(&MSPSessionTokenUsageNotification{})},
		{"turn/started", map[string]any{"commandId": "t", "sessionId": "s", "sourceRange": map[string]any{}, "turnId": "t", "viewCursor": "3"}, reflect.TypeOf(&MSPTurnStartedNotification{})},
		{"turn/completed", map[string]any{"sessionId": "s", "sourceRange": map[string]any{}, "terminal": "failed", "turnId": "t", "viewCursor": "4", "error": map[string]any{"kind": "provider", "message": "failed", "retryable": false}}, reflect.TypeOf(&MSPTurnCompletedNotification{})},
		{"turn/retracted", map[string]any{"commandId": "t", "sessionId": "s", "sourceRange": map[string]any{}, "turnId": "t", "viewCursor": "5"}, reflect.TypeOf(MSPRawNotification{})},
		{"turn/retryScheduled", map[string]any{"attempt": 1, "maxAttempts": 2, "nextAttempt": 2, "reason": "retry", "retryDelayMs": 10, "sessionId": "s", "sourceRange": map[string]any{}, "turnId": "t", "viewCursor": "6"}, reflect.TypeOf(MSPRawNotification{})},
		{"turn/unqueued", map[string]any{"commandId": "t", "sessionId": "s", "sourceRange": map[string]any{}, "turnId": "t", "viewCursor": "7"}, reflect.TypeOf(&MSPTurnUnqueuedNotification{})},
	}
	for _, test := range cases {
		peer.notification(t, test.method, test.params)
	}
	for _, test := range cases {
		select {
		case got := <-peer.client.Notifications():
			if reflect.TypeOf(got) != test.want || got.MSPMethod() != test.method {
				t.Fatalf("notification = %T %q, want %v %q", got, got.MSPMethod(), test.want, test.method)
			}
			if goal, ok := got.(*MSPSessionGoalChangedNotification); ok && (!goal.Goal.Present || !goal.Goal.Null) {
				t.Fatalf("explicit goal null lost: %#v", goal.Goal)
			}
			if status, ok := got.(*MSPSessionStatusChangedNotification); ok && (!status.ViewCursor.Present || !status.ViewCursor.Null) {
				t.Fatalf("nullable status cursor lost: %#v", status.ViewCursor)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s notification was not delivered", test.method)
		}
	}
}

func TestMSPClientDeliversUnknownNotificationAsRaw(t *testing.T) {
	peer := newFakeMSPPeer(t)
	peer.notification(t, "session/newFutureEvent", map[string]any{"value": "kept"})
	select {
	case got := <-peer.client.Notifications():
		raw, ok := got.(MSPRawNotification)
		if !ok || raw.MSPMethod() != "session/newFutureEvent" || !strings.Contains(string(raw.Params), "kept") {
			t.Fatalf("unknown notification = %#v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unknown notification was not delivered")
	}
}

func TestMSPClientReturnsDistinctErrorWhenServerExitsMidRequest(t *testing.T) {
	peer := newFakeMSPPeer(t)
	done := make(chan error, 1)
	go func() {
		_, err := peer.client.SessionStart(context.Background(), MSPSessionStartParams{CommandID: "start"})
		done <- err
	}()
	_ = peer.readFrame(t)
	if err := peer.responseOut.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var exitErr *MSPServerExitedError
		if !errors.Is(err, ErrMSPServerExited) || !errors.As(err, &exitErr) {
			t.Fatalf("server-exit error = %T %v, want distinguishable MSPServerExitedError", err, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not fail after server exit")
	}
}

func TestMSPClientCancellationDropsLateResponseWithoutPoisoningConnection(t *testing.T) {
	peer := newFakeMSPPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() {
		_, err := peer.client.SessionStart(ctx, MSPSessionStartParams{CommandID: "cancel-me"})
		cancelled <- err
	}()
	late := peer.readFrame(t)
	cancel()
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request error = %v, want context.Canceled", err)
	}
	peer.respond(t, late.ID, map[string]any{"session": testMSPSession("late"), "viewCursor": "late"})

	done := make(chan error, 1)
	go func() {
		_, err := peer.client.GoalSet(context.Background(), MSPGoalSetParams{CommandID: "next", SessionID: "s", Objective: "x"})
		done <- err
	}()
	next := peer.readFrame(t)
	peer.respond(t, next.ID, map[string]any{"commandId": "next", "status": "accepted"})
	if err := <-done; err != nil {
		t.Fatalf("next request after late response: %v", err)
	}
}

func TestMSPClientRejectsWrongJSONRPCVersion(t *testing.T) {
	peer := newFakeMSPPeer(t)
	done := make(chan error, 1)
	go func() {
		_, err := peer.client.GoalSet(context.Background(), MSPGoalSetParams{CommandID: "cmd", SessionID: "s", Objective: "x"})
		done <- err
	}()
	request := peer.readFrame(t)
	peer.writeFrame(t, map[string]any{"jsonrpc": "1.0", "id": request.ID, "result": map[string]any{"commandId": "cmd", "status": "accepted"}})
	err := <-done
	var protocolErr *MSPProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("error = %T %v, want *MSPProtocolError", err, err)
	}
}

func TestMSPClientRejectsServerInitiatedRequest(t *testing.T) {
	peer := newFakeMSPPeer(t)
	done := make(chan error, 1)
	// r43 R42-F1 class: the 250 ms backstop used to race the client's own
	// reader goroutine, so a slow schedule surfaced DeadlineExceeded where
	// the test demands a protocol error. Explicit cancellation keeps the
	// hang backstop (a wedged client still fails at the package bound)
	// without racing the scripted response.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, err := peer.client.GoalSet(ctx, MSPGoalSetParams{CommandID: "cmd", SessionID: "s", Objective: "x"})
		done <- err
	}()
	request := peer.readFrame(t)
	peer.writeFrame(t, map[string]any{"jsonrpc": mspJSONRPCVersion, "id": 99, "method": "server/request", "params": map[string]any{}})
	_ = request
	err := <-done
	var protocolErr *MSPProtocolError
	if !errors.As(err, &protocolErr) || !strings.Contains(protocolErr.Error(), "server-initiated") {
		t.Fatalf("server request error = %T %v", err, err)
	}
}

func TestMSPClientRefusesCallsAfterClose(t *testing.T) {
	peer := newFakeMSPPeer(t)
	if err := peer.client.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := peer.client.GoalSet(context.Background(), MSPGoalSetParams{CommandID: "cmd", SessionID: "s", Objective: "x"})
	if !errors.Is(err, ErrMSPClientClosed) {
		t.Fatalf("call after close error = %v, want ErrMSPClientClosed", err)
	}
	peer.client.mu.Lock()
	nextID, pending := peer.client.nextID, len(peer.client.pending)
	peer.client.mu.Unlock()
	if nextID != 0 || pending != 0 {
		t.Fatalf("closed client admitted a request: nextID=%d pending=%d", nextID, pending)
	}
}

func TestMSPClientShutdownClosesInputAndWaitsForServerEOF(t *testing.T) {
	peer := newFakeMSPPeer(t)
	serverStopped := make(chan error, 1)
	go func() {
		_, err := peer.requests.ReadByte()
		if !errors.Is(err, io.EOF) {
			serverStopped <- fmt.Errorf("server input ended with %v, want EOF", err)
			return
		}
		serverStopped <- peer.responseOut.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := peer.client.Shutdown(ctx); err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	if err := <-serverStopped; err != nil {
		t.Fatalf("fake server shutdown: %v", err)
	}
	select {
	case <-peer.client.Done():
	default:
		t.Fatal("client returned from Shutdown before its reader stopped")
	}
}

func TestMSPClientCanceledWhileRequestWriteBlocked(t *testing.T) {
	for _, test := range []struct {
		name   string
		osPipe bool
	}{{name: "io-pipe"}, {name: "os-pipe", osPipe: true}} {
		t.Run(test.name, func(t *testing.T) {
			client, writeStarted, writeFinished, unblockServer := newBlockedMSPClient(t, test.osPipe)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			callDone := startLargeBlockedTurn(t, client, ctx, writeStarted, writeFinished)

			var callErr error
			select {
			case callErr = <-callDone:
			case <-time.After(time.Second):
				t.Error("TurnStart did not return by its context deadline while Write was blocked")
				unblockServer()
				select {
				case callErr = <-callDone:
				case <-time.After(time.Second):
					t.Fatal("TurnStart remained blocked after the fake server was released")
				}
			}
			unblockServer()
			if !errors.Is(callErr, context.DeadlineExceeded) {
				t.Errorf("TurnStart error = %v, want context.DeadlineExceeded", callErr)
			}
		})
	}
}

func TestMSPClientClosePreemptsBlockedRequestWrite(t *testing.T) {
	for _, test := range []struct {
		name   string
		osPipe bool
	}{{name: "io-pipe"}, {name: "os-pipe", osPipe: true}} {
		t.Run(test.name, func(t *testing.T) {
			client, writeStarted, writeFinished, unblockServer := newBlockedMSPClient(t, test.osPipe)
			callDone := startLargeBlockedTurn(t, client, context.Background(), writeStarted, writeFinished)
			closeDone := make(chan error, 1)
			go func() { closeDone <- client.Close() }()

			var closeErr error
			select {
			case closeErr = <-closeDone:
			case <-time.After(time.Second):
				t.Error("Close waited behind the blocked request Write")
				unblockServer()
				select {
				case closeErr = <-closeDone:
				case <-time.After(time.Second):
					t.Fatal("Close remained blocked after the fake server was released")
				}
			}
			if closeErr != nil {
				t.Errorf("Close returned %v", closeErr)
			}
			select {
			case err := <-callDone:
				if !errors.Is(err, ErrMSPClientClosed) {
					t.Errorf("TurnStart after Close = %v, want ErrMSPClientClosed", err)
				}
			case <-time.After(time.Second):
				t.Fatal("TurnStart did not return after Close")
			}
		})
	}
}

func TestMSPClientShutdownPreemptsBlockedRequestWrite(t *testing.T) {
	for _, test := range []struct {
		name   string
		osPipe bool
	}{{name: "io-pipe"}, {name: "os-pipe", osPipe: true}} {
		t.Run(test.name, func(t *testing.T) {
			client, writeStarted, writeFinished, unblockServer := newBlockedMSPClient(t, test.osPipe)
			callDone := startLargeBlockedTurn(t, client, context.Background(), writeStarted, writeFinished)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			shutdownDone := make(chan error, 1)
			go func() { shutdownDone <- client.Shutdown(ctx) }()

			var shutdownErr error
			select {
			case shutdownErr = <-shutdownDone:
			case <-time.After(time.Second):
				t.Error("Shutdown waited behind the blocked request Write")
				unblockServer()
				select {
				case shutdownErr = <-shutdownDone:
				case <-time.After(time.Second):
					t.Fatal("Shutdown remained blocked after the fake server was released")
				}
			}
			if !errors.Is(shutdownErr, context.DeadlineExceeded) {
				t.Errorf("Shutdown error = %v, want context.DeadlineExceeded", shutdownErr)
			}
			select {
			case err := <-callDone:
				if !errors.Is(err, ErrMSPClientClosed) {
					t.Errorf("TurnStart during Shutdown = %v, want ErrMSPClientClosed", err)
				}
			case <-time.After(time.Second):
				t.Fatal("TurnStart did not return after Shutdown")
			}
		})
	}
}

func TestMSPClientRejectsNullResult(t *testing.T) {
	peer := newFakeMSPPeer(t)
	done := make(chan error, 1)
	go func() {
		_, err := peer.client.GoalSet(context.Background(), MSPGoalSetParams{CommandID: "null", SessionID: "s", Objective: "x"})
		done <- err
	}()
	request := peer.readFrame(t)
	peer.respond(t, request.ID, nil)
	if err := <-done; !hasMSPProtocolError(err) {
		t.Fatalf("result:null error = %T %v, want *MSPProtocolError", err, err)
	}
}

func TestMSPClientRejectsMissingResult(t *testing.T) {
	peer := newFakeMSPPeer(t)
	done := make(chan error, 1)
	go func() {
		_, err := peer.client.GoalSet(context.Background(), MSPGoalSetParams{CommandID: "missing", SessionID: "s", Objective: "x"})
		done <- err
	}()
	request := peer.readFrame(t)
	peer.writeFrame(t, map[string]any{"jsonrpc": mspJSONRPCVersion, "id": request.ID})
	if err := <-done; !hasMSPProtocolError(err) {
		t.Fatalf("missing-result error = %T %v, want *MSPProtocolError", err, err)
	}
}

func TestMSPClientRejectsMalformedFrame(t *testing.T) {
	peer := newFakeMSPPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := peer.client.GoalSet(ctx, MSPGoalSetParams{CommandID: "malformed", SessionID: "s", Objective: "x"})
		done <- err
	}()
	_ = peer.readFrame(t)
	if _, err := peer.responseOut.Write([]byte("{not json\n")); err != nil {
		t.Fatalf("write malformed server frame: %v", err)
	}
	select {
	case err := <-done:
		if !hasMSPProtocolError(err) || errors.Is(err, ErrMSPServerExited) {
			t.Fatalf("malformed-frame error = %T %v, want distinct *MSPProtocolError", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("call did not fail after malformed server frame")
	}
	select {
	case <-peer.client.Done():
	case <-time.After(time.Second):
		t.Fatal("reader did not stop after malformed server frame")
	}
}

func TestMSPClientRejectsInvalidErrorIDForEveryPendingCall(t *testing.T) {
	for _, test := range []struct {
		name string
		id   json.RawMessage
	}{{name: "null", id: json.RawMessage("null")}, {name: "string", id: json.RawMessage(`"never-sent"`)}} {
		t.Run(test.name, func(t *testing.T) {
			peer := newFakeMSPPeer(t)
			// r43 R42-F1 class: as in TestMSPClientRejectsServerInitiatedRequest,
			// the 500 ms backstop raced the client reader; explicit
			// cancellation plus the one-second result selects below keep the
			// hang backstop without the race.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type result struct {
				method string
				err    error
			}
			done := make(chan result, 2)
			go func() {
				_, err := peer.client.SessionStart(ctx, MSPSessionStartParams{CommandID: "start"})
				done <- result{method: "session/start", err: err}
			}()
			go func() {
				_, err := peer.client.GoalSet(ctx, MSPGoalSetParams{CommandID: "goal", SessionID: "s", Objective: "x"})
				done <- result{method: "goal/set", err: err}
			}()
			first := peer.readFrame(t)
			second := peer.readFrame(t)
			if first.Method == second.Method {
				t.Fatalf("expected two different pending methods, got %q and %q", first.Method, second.Method)
			}
			peer.writeFrame(t, map[string]any{
				"jsonrpc": mspJSONRPCVersion,
				"id":      test.id,
				"error":   map[string]any{"code": -32700, "message": "invalid request id"},
			})
			for range 2 {
				select {
				case got := <-done:
					if !hasMSPProtocolError(got.err) || errors.Is(got.err, ErrMSPServerExited) {
						t.Errorf("%s error = %T %v, want *MSPProtocolError", got.method, got.err, got.err)
					}
				case <-time.After(time.Second):
					t.Fatal("pending call did not fail after invalid error response id")
				}
			}
		})
	}
}

func hasMSPProtocolError(err error) bool {
	var protocolErr *MSPProtocolError
	return errors.As(err, &protocolErr)
}

func testMSPSession(id string) map[string]any {
	return map[string]any{
		"activeTurnId":  nil,
		"createdAt":     "2026-09-28T00:00:00Z",
		"forkedFrom":    nil,
		"modelId":       nil,
		"path":          "/tmp/muse/session.jsonl",
		"providerId":    "echo",
		"sessionId":     id,
		"status":        "loaded",
		"turnCount":     0,
		"updatedAt":     "2026-09-28T00:00:00Z",
		"workspaceRoot": "/workspace",
	}
}

func bytesEqual(a, b []byte) bool { return reflect.DeepEqual(a, b) }
