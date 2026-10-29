package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These original source cases distinguish gateway, foreign and unknown native
// provider events independently of the gateway trace.
func TestCodexBuilderObservationClassifiesNativeProviderItems(t *testing.T) {
	observation := observeCodexBuilderItems([]json.RawMessage{
		json.RawMessage(`{"id":"g1","type":"mcpToolCall","server":"task-board-repository-evidence","tool":"read","status":"completed"}`),
		json.RawMessage(`{"id":"g2","type":"mcpToolCall","server":"task-board-repository-evidence","tool":"search","status":"failed"}`),
		json.RawMessage(`{"id":"f1","type":"mcpToolCall","server":"foreign","tool":"read","status":"completed"}`),
		json.RawMessage(`{"id":"f2","type":"commandExecution","status":"completed"}`),
		json.RawMessage(`{"id":"u1","type":"futurePassiveItem"}`),
	})
	if observation.GatewayCalls != 2 || observation.ForeignToolCalls != 2 ||
		observation.UnknownShapes != 1 {
		t.Fatalf("Codex native observation = %+v", observation)
	}
}

func TestClaudeBuilderObservationClassifiesNativeProviderBlocks(t *testing.T) {
	raw := []byte(`{"type":"assistant","message":{"role":"assistant","stop_reason":"tool_use","content":[` +
		`{"type":"tool_use","id":"g1","name":"mcp__task-board-repository-evidence__read"},` +
		`{"type":"tool_use","id":"f1","name":"Bash"},` +
		`{"type":"future_tool_use","id":"u1","name":"Read"}]}}` + "\n" +
		`{"type":"user","message":{"role":"user","content":[` +
		`{"type":"tool_result","tool_use_id":"g1"},` +
		`{"type":"tool_result","tool_use_id":"missing"}]}}` + "\n" +
		`{"type":"assistant","message":{"role":"assistant","stop_reason":"end_turn",` +
		`"content":[{"type":"text","text":"ok"}]}}` + "\n")
	observation, terminal := inspectClaudeBuilderTranscript(raw)
	if !terminal || observation.GatewayCalls != 1 || observation.ForeignToolCalls != 1 ||
		observation.UnknownShapes != 2 {
		t.Fatalf("Claude native observation = %+v terminal=%t", observation, terminal)
	}
}

// A loopback of the original RPC source exercises the real runner reader and
// releases its tracked client. No provider executable/model is started.
func TestCodexBuilderObservationReadsOwnedNativeTurn(t *testing.T) {
	for _, mode := range []string{"valid", "foreign-thread", "missing-turn", "incomplete", "foreign-tool", "unknown", "unreadable", "unconfined", "missing-identity", "stopped"} {
		t.Run(mode, func(t *testing.T) {
			reads := 0
			server := newCodexContextRPCServer(t, func(q codexRPCRequest) (any, []map[string]any, error) {
				if q.Method == "initialize" {
					return map[string]any{}, nil, nil
				}
				if q.Method != "thread/read" {
					return nil, nil, fmt.Errorf("unexpected method %s", q.Method)
				}
				reads++
				var params struct {
					ThreadID     string `json:"threadId"`
					IncludeTurns bool   `json:"includeTurns"`
				}
				encoded, encodeErr := json.Marshal(q.Params)
				if encodeErr != nil {
					t.Errorf("native request encoding: %v", encodeErr)
				}
				if err := json.Unmarshal(encoded, &params); err != nil || params.ThreadID != "builder-thread" || !params.IncludeTurns {
					t.Errorf("wrong native source request: %s", q.Params)
				}
				if mode == "unreadable" {
					return nil, nil, fmt.Errorf("native read unavailable")
				}
				id, turnID, status := "builder-thread", "builder-turn", "completed"
				if mode == "foreign-thread" {
					id = "foreign-thread"
				}
				if mode == "missing-turn" {
					turnID = "foreign-turn"
				}
				if mode == "incomplete" {
					status = "inProgress"
				}
				item := json.RawMessage(`{"id":"g1","type":"mcpToolCall","server":"task-board-repository-evidence","tool":"read","status":"completed"}`)
				if mode == "foreign-tool" {
					item = json.RawMessage(`{"id":"f1","type":"commandExecution"}`)
				}
				if mode == "unknown" {
					item = json.RawMessage(`{"type":"futurePassiveItem"}`)
				}
				return map[string]any{"thread": map[string]any{"id": id, "turns": []any{map[string]any{"id": turnID, "status": status, "items": []json.RawMessage{item}}}}}, nil, nil
			})
			defer server.Close()
			runner, err := newCodexRunner(CodexRunnerSnapshot{Contract: codexHostSnapshotContract, PID: os.Getpid(), URL: "ws" + strings.TrimPrefix(server.URL, "http"), ThreadID: "builder-thread", LastCompletedTurnID: "builder-turn", ProjectDir: t.TempDir(), CodexHome: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer runner.closeObservation()
			runner.builderConfinement = &BuilderConfinementResult{Mechanism: "fixture"}
			if mode == "unconfined" {
				runner.builderConfinement = nil
			}
			if mode == "missing-identity" {
				runner.snapshot.LastCompletedTurnID = ""
			}
			if mode == "stopped" {
				runner.snapshot.Stopped = true
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := runner.BuilderToolObservation(ctx)
			if mode == "unconfined" || mode == "missing-identity" {
				if err == nil || got.Reported || reads != 0 {
					t.Fatalf("missing native custody accepted: %+v %v reads=%d", got, err, reads)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "valid" && (!got.Reported || got.GatewayCalls != 1 || got.ForeignToolCalls != 0 || got.UnknownShapes != 0 || got.Disconnected) {
				t.Fatalf("owned native turn not observed: %+v", got)
			}
			if (mode == "foreign-thread" || mode == "missing-turn" || mode == "unknown") && got.UnknownShapes != 1 {
				t.Fatalf("foreign or unknown native source accepted: %+v", got)
			}
			if (mode == "incomplete" || mode == "unreadable" || mode == "stopped") && !got.Disconnected {
				t.Fatalf("unsettled native stream accepted: %+v", got)
			}
			if mode == "foreign-tool" && got.ForeignToolCalls != 1 {
				t.Fatalf("native bypass lost: %+v", got)
			}
			runner.mu.Lock()
			remaining := len(runner.clients)
			runner.mu.Unlock()
			if remaining != 0 {
				t.Fatalf("native observer leaked %d tracked clients", remaining)
			}
			if mode != "stopped" && reads != 1 {
				t.Fatalf("native observation did not use exactly its reader: %d", reads)
			}
		})
	}
}

// Only the suffix captured before launch belongs to this builder. A completed
// transcript is a nearby admitted control for missing, shrinking or drifting data.
func TestClaudeBuilderObservationReadsCapturedNativeSuffix(t *testing.T) {
	for _, mode := range []string{"valid", "foreign", "unknown", "incomplete", "missing", "shrunk", "unconfined", "stopped"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "builder.jsonl")
			inherited := `{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"old","name":"Bash"}]}}` + "\n"
			tool := "mcp__task-board-repository-evidence__read"
			if mode == "foreign" {
				tool = "Bash"
			}
			suffix := fmt.Sprintf(`{"type":"assistant","message":{"role":"assistant","stop_reason":"end_turn","content":[{"type":"tool_use","id":"new","name":%q}]}}`+"\n", tool)
			if mode == "unknown" {
				suffix = `{"type":"future_event"}` + "\n"
			}
			if mode == "incomplete" {
				suffix = `{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working"}]}}` + "\n"
			}
			if err := os.WriteFile(path, []byte(inherited+suffix), 0600); err != nil {
				t.Fatal(err)
			}
			host := &claudeHost{builderConfinement: &BuilderConfinementResult{Mechanism: "fixture"}, builderTranscriptPath: path, builderTranscriptOffset: int64(len(inherited))}
			if mode == "missing" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "shrunk" {
				if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "unconfined" {
				host.builderConfinement = nil
			}
			if mode == "stopped" {
				host.stopped = true
			}
			got, err := host.BuilderToolObservation(context.Background())
			if mode == "shrunk" || mode == "unconfined" {
				if err == nil || got.Reported {
					t.Fatalf("invalid native boundary accepted: %+v %v", got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "valid" && (!got.Reported || got.GatewayCalls != 1 || got.ForeignToolCalls != 0 || got.UnknownShapes != 0 || got.Disconnected) {
				t.Fatalf("native suffix/inherited boundary lost: %+v", got)
			}
			if mode == "foreign" && got.ForeignToolCalls != 1 {
				t.Fatalf("native bypass lost: %+v", got)
			}
			if (mode == "unknown" || mode == "incomplete") && got.UnknownShapes == 0 {
				t.Fatalf("incomplete or unknown native source accepted: %+v", got)
			}
			if (mode == "missing" || mode == "stopped") && !got.Disconnected {
				t.Fatalf("disconnected native source accepted: %+v", got)
			}
		})
	}
}

// Real Unix/MCP and provider-native transcript readers meet only at the
// completeness gate. A hidden extra native call cannot be erased by shim count.
func TestBuilderGatewayReconcilesActualNativeTranscript(t *testing.T) {
	for _, mode := range []string{"valid", "hidden-call", "foreign", "unknown", "missing", "unconfined"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeObservationGateway(t, "CTX-260731-ob0001")
			script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
			if err := RunBuilderGatewayShim(f.client, strings.NewReader(script), io.Discard); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "native.jsonl")
			block := `{"type":"tool_use","id":"g1","name":"mcp__task-board-repository-evidence__read"}`
			if mode == "hidden-call" {
				block += `,{"type":"tool_use","id":"g2","name":"mcp__task-board-repository-evidence__read"}`
			}
			if mode == "foreign" {
				block += `,{"type":"tool_use","id":"f1","name":"Bash"}`
			}
			if mode == "unknown" {
				block += `,{"type":"future_tool_use"}`
			}
			raw := `{"type":"assistant","message":{"role":"assistant","stop_reason":"end_turn","content":[` + block + `]}}` + "\n"
			if mode != "missing" {
				if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
					t.Fatal(err)
				}
			}
			native := &claudeHost{builderConfinement: &BuilderConfinementResult{Mechanism: "fixture"}, builderTranscriptPath: path}
			if mode == "unconfined" {
				native.builderConfinement = nil
			}
			// Replace the fixture's declared observer with the ACTUAL provider method.
			if err := f.manager.BindBuilderGatewayObserver(f.ctxID, native); err != nil {
				t.Fatal(err)
			}
			result, err := f.manager.FinalizeBuilderGateway(context.Background(), f.ctxID)
			if mode == "valid" {
				if err != nil || result.Attestation.EventCount != 1 {
					t.Fatalf("actual native+gateway control refused: %+v %v", result, err)
				}
				if _, _, err := f.manager.BuilderAccessTrace(f.ctxID); err != nil {
					t.Fatal(err)
				}
			} else {
				code := ContextErrorBuilderTraceUnavailable
				if mode == "foreign" {
					code = ContextErrorBuilderToolIsolationUnavailable
				}
				if !builderIsContextErrorCode(err, code) || result.Attestation.ContractVersion != "" {
					t.Fatalf("native discrepancy minted evidence: %+v %v", result, err)
				}
				if _, _, err := f.manager.BuilderAccessTrace(f.ctxID); err == nil {
					t.Fatal("refused native discrepancy recovered usable trace")
				}
			}
		})
	}
}

// Use the real runtime Start path so terminal reload names the actual trace,
// rather than the old fixture's deliberately standalone trace file.
func newNativeObservationGateway(t *testing.T, id string) *gatewaySessionFixture {
	t.Helper()
	command, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewBuilderGatewayRuntime(BuilderGatewayRuntimeOptions{Layout: Layout{StateDir: t.TempDir(), SocketPath: shortSocketPath(t)}, ShimCommand: command})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	endpoint, err := runtime.StartBuilderGateway(context.Background(), &ContextRecord{ID: id, Provider: "claude", Lineage: ContextLineage{ProducedByRunID: "RUN-260731-abc123"}}, gatewayRepo(t), NewContextBuilderAllowlist([]string{"docs"}))
	if err != nil {
		t.Fatal(err)
	}
	client := NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
	t.Cleanup(func() { _ = client.Disconnect() })
	return &gatewaySessionFixture{manager: runtime, ctxID: id, client: client}
}

// Original receiving witnesses retained under their original names. These two
// use declared independent observer fixtures to qualify reconciler/shim wiring;
// actual native-source acquisition is tested separately above.
func TestManagerFinalizationUsesProviderOwnedBypassEvidence(t *testing.T) {
	tests := []struct {
		name        string
		observation BuilderToolObservation
		observerErr error
		wantCode    string
	}{
		{
			name: "foreign provider tool",
			observation: BuilderToolObservation{
				Reported: true, GatewayCalls: 1, ForeignToolCalls: 1,
			},
			wantCode: ContextErrorBuilderToolIsolationUnavailable,
		},
		{
			name: "unknown provider shape",
			observation: BuilderToolObservation{
				Reported: true, GatewayCalls: 1, UnknownShapes: 1,
			},
			wantCode: ContextErrorBuilderTraceUnavailable,
		},
		{
			name:        "provider observation unavailable",
			observerErr: errors.New("native stream unreadable"),
			wantCode:    ContextErrorBuilderTraceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGatewaySessionFixture(t, "CTX-260731-po0001")
			script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` +
				`{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
			if err := RunBuilderGatewayShim(
				fixture.client, strings.NewReader(script), io.Discard,
			); err != nil {
				t.Fatal(err)
			}
			fixture.observer.observation = &test.observation
			fixture.observer.err = test.observerErr
			_, err := fixture.manager.CloseBuilderGateway(fixture.ctxID)
			if !builderIsContextErrorCode(err, test.wantCode) {
				t.Fatalf("finalization = %v, want %s", err, test.wantCode)
			}
		})
	}
}

func TestShimFailsClosedForIDLessAndStructurallyUnknownFrames(t *testing.T) {
	fixture := newGatewaySessionFixture(t, "CTX-260731-po0002")
	script := `{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}` + "\n" +
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` +
		`{"name":"read","arguments":{"path":"docs/guide.md"},` +
		`"_meta":{"progressToken":"claude-call-1"}}}` + "\n" +
		`{"jsonrpc":"2.0","method":"tools/call","params":` +
		`{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n" +
		`{"jsonrpc":"2.0","method":"notifications/future","params":{}}` + "\n" +
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":` +
		`{"name":"read","arguments":{"path":"docs/guide.md","future":true}}}` + "\n"
	if err := RunBuilderGatewayShim(
		fixture.client, strings.NewReader(script), io.Discard,
	); err != nil {
		t.Fatal(err)
	}
	if got := fixture.gateway.RecordedEvents(); got != 1 {
		t.Fatalf("unknown frames minted %d trace events, want only the one valid call", got)
	}
	fixture.observer.observation = &BuilderToolObservation{Reported: true, GatewayCalls: 1}
	_, err := fixture.manager.CloseBuilderGateway(fixture.ctxID)
	if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("unknown id-less/extra-field shapes = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}
