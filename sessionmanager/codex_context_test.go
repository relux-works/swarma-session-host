package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"io"
)

// These relocated provider tests keep their original names and behavioral assertions.
// The policy probe below isolates native mechanism; canonical decisions are tested in the consumer adapter.
// This case preserves its original protocol/refusal assertions on typed native records.
func TestCodexProtocolBuildersKeepForkAndTurnSandboxShapesSeparate(t *testing.T) {
	fork, err := json.Marshal(codexThreadForkParams{
		ThreadID:    "thread-base",
		LastTurnID:  "turn-base",
		Permissions: "assay",
	})
	if err != nil {
		t.Fatal(err)
	}
	steer, err := json.Marshal(codexTurnSteerParams{
		ThreadID:       "thread-child",
		ExpectedTurnID: "turn-active",
		Input:          []codexTextInput{{Type: "text", Text: "prompt", TextElements: []any{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := json.Marshal(codexTurnStartParams{
		ThreadID: "thread-child",
		Input:    []codexTextInput{{Type: "text", Text: "prompt", TextElements: []any{}}},
		SandboxPolicy: &codexReadOnlySandboxPolicy{
			Type:          "readOnly",
			NetworkAccess: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"sandboxPolicy", "outputSchema"} {
		if strings.Contains(string(fork), forbidden) {
			t.Fatalf("thread/fork leaked turn field %q: %s", forbidden, fork)
		}
	}
	for _, forbidden := range []string{"lastTurnId", `"sandbox"`} {
		if strings.Contains(string(turn), forbidden) {
			t.Fatalf("turn/start leaked fork field %q: %s", forbidden, turn)
		}
	}
	if !strings.Contains(string(fork), `"lastTurnId":"turn-base"`) ||
		!strings.Contains(string(turn), `"sandboxPolicy"`) ||
		!strings.Contains(string(steer), `"expectedTurnId":"turn-active"`) ||
		strings.Contains(string(steer), `"cwd"`) ||
		strings.Contains(string(steer), `"sandboxPolicy"`) {
		t.Fatalf("typed RPC shapes fork=%s turn=%s steer=%s", fork, turn, steer)
	}
}

// This case preserves its original protocol/refusal assertions on typed native records.
func TestValidateCodexForkBaseRefusesUnsafeRollouts(t *testing.T) {
	tests := []struct {
		name   string
		thread *codexThread
		want   string
	}{
		{
			name:   "empty",
			thread: &codexThread{ID: "base"},
			want:   codexContextBaseNotForkable,
		},
		{
			name: "interrupted",
			thread: &codexThread{ID: "base", Turns: []codexTurn{{
				ID: "turn-base", Status: "interrupted",
			}}},
			want: codexContextBaseNotForkable,
		},
		{
			name: "advanced by active turn",
			thread: &codexThread{ID: "base", Turns: []codexTurn{
				{ID: "turn-base", Status: "completed"},
				{ID: "turn-dangling", Status: "inProgress"},
			}},
			want: codexContextBaseNotForkable,
		},
		{
			name: "compacted",
			thread: &codexThread{ID: "base", Turns: []codexTurn{{
				ID:     "turn-base",
				Status: "completed",
				Items:  []json.RawMessage{json.RawMessage(`{"type":"contextCompaction"}`)},
			}}},
			want: "context_compacted",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateCodexForkBase(test.thread, "turn-base")
			assertPublicContextCode(t, err, test.want)
		})
	}
}

// This case preserves its original protocol/refusal assertions on typed native records.
func TestCodexForkRefusesABaseRolloutCarryingTheAuthoritativeCompactionRecord(t *testing.T) {
	const (
		baseThread = "019f-compacted-base"
		baseTurn   = "turn-base"
	)
	sourceHome := t.TempDir()
	writeCodexContextRollout(t, sourceHome, baseThread, strings.Join([]string{
		`{"timestamp":"2026-07-30T00:00:00Z","type":"event_msg","payload":{"type":"token_count"}}`,
		`{"timestamp":"2026-07-30T00:00:01Z","type":"compacted","payload":{"message":"","replacement_history":[{"type":"message","role":"user","content":[]}]}}`,
		"",
	}, "\n"))
	server := newCodexContextRPCServer(t, func(request codexRPCRequest) (any, []map[string]any, error) {
		t.Errorf("compacted base reached the provider via %s", request.Method)
		return nil, nil, fmt.Errorf("unexpected provider IO")
	})
	defer server.Close()
	plan := &CodexContextForkPlan{
		Contract:     codexContextForkPlanContract,
		PrimaryCtxID: "CTX-260730-aa0011",
		Primary:      testCodexContextHostRecord(t, server.URL, sourceHome, baseThread, baseTurn),
	}

	got, err := (&CodexHostFactory{contextPolicy: codexPolicyProbe{}}).materializeContextFork(
		context.Background(),
		plan,
		t.TempDir(),
		t.TempDir(),
	)
	if got != nil {
		t.Fatalf("materializeContextFork() = %#v, want nil", got)
	}
	assertPublicContextCode(t, err, "context_compacted")
	if !strings.Contains(err.Error(), "compacted") {
		t.Fatalf("refusal = %v, want the authoritative %s marker named",
			err, "compacted")
	}
}

// This case preserves its original protocol/refusal assertions on typed native records.
func TestCodexContextForkMaterializerPreservesBaseAndInjectsAllLineage(t *testing.T) {
	const (
		baseThread  = "019f-base-thread"
		baseTurn    = "turn-base"
		childThread = "019f-child-thread"
	)
	sourceHome := t.TempDir()
	baseRollout := writeCodexContextRollout(t, sourceHome, baseThread, "immutable-base\n")
	before, err := sha256File(baseRollout)
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu              sync.Mutex
		forkParams      map[string]any
		injectedParams  map[string]any
		experimentalAPI bool
	)
	server := newCodexContextRPCServer(t, func(request codexRPCRequest) (any, []map[string]any, error) {
		switch request.Method {
		case "initialize":
			params := request.Params.(map[string]any)
			capabilities, _ := params["capabilities"].(map[string]any)
			mu.Lock()
			experimentalAPI, _ = capabilities["experimentalApi"].(bool)
			mu.Unlock()
			return map[string]any{}, nil, nil
		case "thread/read":
			return map[string]any{"thread": testCodexThread(baseThread, "", baseTurn, "completed", nil)}, nil, nil
		case "thread/fork":
			params := request.Params.(map[string]any)
			mu.Lock()
			forkParams = params
			mu.Unlock()
			writeCodexContextRollout(t, sourceHome, childThread, "fork-child\n")
			return map[string]any{"thread": testCodexThread(childThread, baseThread, baseTurn, "completed", nil)}, nil, nil
		case "thread/inject_items":
			mu.Lock()
			injectedParams = request.Params.(map[string]any)
			mu.Unlock()
			return map[string]any{}, nil, nil
		default:
			return nil, nil, fmt.Errorf("unexpected method %s", request.Method)
		}
	})
	defer server.Close()
	source := testCodexContextHostRecord(t, server.URL, sourceHome, baseThread, baseTurn)
	plan := &CodexContextForkPlan{
		Contract:     codexContextForkPlanContract,
		PrimaryCtxID: "CTX-260730-aa0001",
		Primary:      source,
		Supplemental: []CodexSupplementalContext{
			{CtxID: "CTX-260730-aa0002", Handoff: "security decision"},
			{CtxID: "CTX-260730-aa0003", Handoff: "API decision"},
		},
	}
	targetHome := t.TempDir()
	got, err := (&CodexHostFactory{contextPolicy: codexPolicyProbe{}}).materializeContextFork(
		context.Background(),
		plan,
		targetHome,
		t.TempDir(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.ThreadID != childThread ||
		got.ForkedFromID != baseThread ||
		got.LastTurnID != baseTurn ||
		got.BaseRolloutSHA256 != before {
		t.Fatalf("fork materialization = %#v", got)
	}
	after, err := sha256File(baseRollout)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("base rollout changed from %s to %s", before, after)
	}
	childRollout, err := findCodexThreadRollout(filepath.Join(targetHome, "sessions"), childThread)
	if err != nil {
		t.Fatalf("private child rollout was not copied: %v", err)
	}
	// The fork point is the copied file exactly as it stood before the child ran
	// anything, so it has to describe that file and not the base's.
	copiedDigest, err := sha256File(childRollout)
	if err != nil {
		t.Fatal(err)
	}
	childInfo, err := os.Stat(childRollout)
	if err != nil {
		t.Fatal(err)
	}
	if got.InheritedRollout.SHA256 != copiedDigest ||
		got.InheritedRollout.Bytes != childInfo.Size() {
		t.Fatalf(
			"fork point = %#v, want the copied child rollout (%d bytes, %s)",
			got.InheritedRollout, childInfo.Size(), copiedDigest,
		)
	}
	mu.Lock()
	defer mu.Unlock()
	if !experimentalAPI {
		t.Fatal("Codex initialize did not advertise experimentalApi required by thread/fork")
	}
	if forkParams["threadId"] != baseThread || forkParams["lastTurnId"] != baseTurn {
		t.Fatalf("thread/fork params = %#v", forkParams)
	}
	items, _ := injectedParams["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("injected items = %#v", injectedParams["items"])
	}
	encoded, _ := json.Marshal(items)
	if !strings.Contains(string(encoded), "CTX-260730-aa0002") ||
		!strings.Contains(string(encoded), "CTX-260730-aa0003") ||
		strings.Index(string(encoded), "aa0002") > strings.Index(string(encoded), "aa0003") {
		t.Fatalf("supplemental lineage order was not preserved: %s", encoded)
	}
}

// This case preserves its original protocol/refusal assertions on typed native records.
func TestCodexContextForkSucceedsAcrossAMateriallyOlderBuild(t *testing.T) {
	const (
		baseThread  = "019f-drift-base"
		baseTurn    = "turn-drift-base"
		childThread = "019f-drift-child"
	)
	sourceHome := t.TempDir()
	baseRollout := writeCodexContextRollout(t, sourceHome, baseThread, "older-build-base\n")
	before, err := sha256File(baseRollout)
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu        sync.Mutex
		forkCalls int
	)
	server := newCodexContextRPCServer(t, func(request codexRPCRequest) (any, []map[string]any, error) {
		switch request.Method {
		case "initialize":
			return map[string]any{}, nil, nil
		case "thread/read":
			thread := testCodexThread(baseThread, "", baseTurn, "completed", nil)
			thread["cliVersion"] = "0.130.0"
			return map[string]any{"thread": thread}, nil, nil
		case "thread/fork":
			mu.Lock()
			forkCalls++
			mu.Unlock()
			writeCodexContextRollout(t, sourceHome, childThread, "drift-child\n")
			child := testCodexThread(childThread, baseThread, baseTurn, "completed", nil)
			child["cliVersion"] = "0.130.0"
			return map[string]any{"thread": child}, nil, nil
		default:
			return nil, nil, fmt.Errorf("unexpected method %s", request.Method)
		}
	})
	defer server.Close()

	got, err := (&CodexHostFactory{contextPolicy: codexPolicyProbe{}}).materializeContextFork(
		context.Background(),
		&CodexContextForkPlan{
			Contract:     codexContextForkPlanContract,
			PrimaryCtxID: "CTX-260730-dd0002",
			Primary: testCodexContextHostRecord(
				t, server.URL, sourceHome, baseThread, baseTurn, "0.104.0",
			),
		},
		t.TempDir(),
		t.TempDir(),
	)
	if err != nil {
		t.Fatalf("a materially older base was refused instead of forked: %v", err)
	}
	if got.ThreadID != childThread || got.ForkedFromID != baseThread {
		t.Fatalf("fork materialization = %#v", got)
	}
	if got.ProviderVersion != "0.130.0" {
		t.Fatalf(
			"the materialization records %q; the child belongs to the build that forked it",
			got.ProviderVersion,
		)
	}
	mu.Lock()
	defer mu.Unlock()
	if forkCalls != 1 {
		t.Fatalf("thread/fork was attempted %d time(s), want 1", forkCalls)
	}
	after, err := sha256File(baseRollout)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("the older base rollout changed from %s to %s", before, after)
	}
}

// This case preserves its original protocol/refusal assertions on typed native records.
func TestCodexRestartAppServerArgsReplaceRecordedEndpoint(t *testing.T) {
	got := codexRestartAppServerArgs(
		[]string{
			"app-server",
			"--strict-config",
			"--listen",
			"ws://127.0.0.1:1111",
			"--listen=ws://127.0.0.1:2222",
			"--stdio",
		},
		"ws://127.0.0.1:3333",
		"--disable",
		"apps",
	)
	want := []string{
		"app-server",
		"--strict-config",
		"--disable",
		"apps",
		"--listen",
		"ws://127.0.0.1:3333",
	}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("restart args = %#v, want %#v", got, want)
	}
}

// This case preserves its original protocol/refusal assertions on typed native records.
func TestCodexContextAssaySucceedsAcrossAMateriallyOlderBuild(t *testing.T) {
	const (
		baseThread    = "019f-assay-drift-base"
		baseTurn      = "turn-assay-drift-base"
		forkThread    = "019f-assay-drift-fork"
		assayTurn     = "turn-assay-drift-answer"
		recordedBuild = "0.104.0"
		activeBuild   = "0.130.0"
	)
	sourceHome := t.TempDir()
	writeCodexContextRollout(t, sourceHome, baseThread, "older-assay-base\n")

	var (
		mu              sync.Mutex
		forkCalls       int
		startedReported bool
		turnStarted     bool
	)
	server := newCodexContextRPCServer(t, func(request codexRPCRequest) (any, []map[string]any, error) {
		mu.Lock()
		defer mu.Unlock()
		switch request.Method {
		case "initialize":
			return map[string]any{}, nil, nil
		case "thread/read":
			params := request.Params.(map[string]any)
			threadID, _ := params["threadId"].(string)
			if threadID == baseThread {
				thread := testCodexThread(baseThread, "", baseTurn, "completed", nil)
				thread["cliVersion"] = activeBuild
				return map[string]any{"thread": thread}, nil, nil
			}
			if threadID != forkThread {
				return nil, nil, fmt.Errorf("unknown thread %q", threadID)
			}
			var notifications []map[string]any
			if !startedReported {
				startedReported = true
				started := testCodexThread(forkThread, baseThread, baseTurn, "completed", nil)
				started["cliVersion"] = activeBuild
				notifications = append(notifications, map[string]any{
					"method": "thread/started",
					"params": map[string]any{"thread": started},
				})
			}
			turnID := baseTurn
			var items []json.RawMessage
			if turnStarted {
				turnID = assayTurn
				items = []json.RawMessage{
					json.RawMessage(`{"id":"answer","type":"agentMessage","text":"{\"score\":1}"}`),
				}
			}
			thread := testCodexThread(forkThread, baseThread, turnID, "completed", items)
			thread["cliVersion"] = activeBuild
			return map[string]any{"thread": thread}, notifications, nil
		case "thread/fork":
			forkCalls++
			thread := testCodexThread(forkThread, baseThread, baseTurn, "completed", nil)
			thread["cliVersion"] = activeBuild
			return map[string]any{
				"thread":                  thread,
				"activePermissionProfile": map[string]any{"id": codexContextAssayPermission},
			}, nil, nil
		case "mcpServerStatus/list":
			return map[string]any{"data": []any{}}, nil, nil
		case "turn/start":
			turnStarted = true
			return map[string]any{
				"turn": map[string]any{"id": assayTurn, "status": "inProgress"},
			}, nil, nil
		case "turn/interrupt", "thread/delete":
			return map[string]any{}, nil, nil
		default:
			return nil, nil, fmt.Errorf("unexpected method %s", request.Method)
		}
	})
	defer server.Close()

	result, err := testCodexAssayFactory().RunContextAssay(
		context.Background(),
		testCodexContextHostRecord(
			t, server.URL, sourceHome, baseThread, baseTurn, recordedBuild,
		),
		CodexContextAssayRequest{
			BaseCtxID:    "CTX-260731-ass001",
			Prompt:       "return a score without tools",
			OutputSchema: json.RawMessage(`{"type":"object"}`),
		},
	)
	if err != nil {
		t.Fatalf("a materially older base was refused before the assay could prove it usable: %v", err)
	}
	if result.Attestation.ProviderVersion != activeBuild ||
		result.Attestation.ForkThreadID != forkThread ||
		string(result.Output) != `{"score":1}` {
		t.Fatalf("cross-build assay result = %#v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if forkCalls != 1 {
		t.Fatalf("thread/fork was attempted %d time(s), want 1", forkCalls)
	}
}

// This case preserves its original protocol/refusal assertions on typed native records.
func TestCodexContextAssayRetriesInterruptedTurnsOnlyOnFreshForks(t *testing.T) {
	tests := []struct {
		name              string
		completeOnAttempt int
		wantUnavailable   bool
	}{
		{
			name:              "third fresh fork succeeds",
			completeOnAttempt: codexContextAssayMaxAttempts,
		},
		{
			name:            "fresh fork retry budget is bounded",
			wantUnavailable: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const (
				baseThread = "019f-assay-retry-base"
				baseTurn   = "turn-assay-retry-base"
				version    = "0.146.0"
			)
			sourceHome := t.TempDir()
			baseRollout := writeCodexContextRollout(
				t,
				sourceHome,
				baseThread,
				"immutable-assay-retry-base\n",
			)
			beforeDigest, err := sha256File(baseRollout)
			if err != nil {
				t.Fatal(err)
			}
			var (
				mu             sync.Mutex
				forkCalls      int
				forkAttempt    = map[string]int{}
				threadStarted  = map[string]bool{}
				activeTurn     = map[string]string{}
				turnStartCount = map[string]int{}
				deleteCount    = map[string]int{}
			)
			server := newCodexContextRPCServer(
				t,
				func(request codexRPCRequest) (any, []map[string]any, error) {
					mu.Lock()
					defer mu.Unlock()
					switch request.Method {
					case "initialize":
						return map[string]any{}, nil, nil
					case "thread/fork":
						forkCalls++
						forkID := fmt.Sprintf("019f-assay-retry-fork-%d", forkCalls)
						forkAttempt[forkID] = forkCalls
						thread := testCodexThread(
							forkID,
							baseThread,
							baseTurn,
							"completed",
							nil,
						)
						thread["cliVersion"] = version
						return map[string]any{
							"thread":                  thread,
							"activePermissionProfile": map[string]any{"id": codexContextAssayPermission},
						}, nil, nil
					case "thread/read":
						params := request.Params.(map[string]any)
						threadID, _ := params["threadId"].(string)
						if threadID == baseThread {
							thread := testCodexThread(
								baseThread,
								"",
								baseTurn,
								"completed",
								nil,
							)
							thread["cliVersion"] = version
							return map[string]any{"thread": thread}, nil, nil
						}
						attempt := forkAttempt[threadID]
						if attempt == 0 {
							return nil, nil, fmt.Errorf("unknown fork thread %q", threadID)
						}
						var notifications []map[string]any
						if !threadStarted[threadID] {
							threadStarted[threadID] = true
							started := testCodexThread(
								threadID,
								baseThread,
								baseTurn,
								"completed",
								nil,
							)
							started["cliVersion"] = version
							notifications = append(notifications, map[string]any{
								"method": "thread/started",
								"params": map[string]any{"thread": started},
							})
						}
						turnID := activeTurn[threadID]
						if turnID == "" {
							thread := testCodexThread(
								threadID,
								baseThread,
								baseTurn,
								"completed",
								nil,
							)
							thread["cliVersion"] = version
							return map[string]any{"thread": thread}, notifications, nil
						}
						status := "interrupted"
						var items []json.RawMessage
						if attempt == test.completeOnAttempt {
							status = "completed"
							items = []json.RawMessage{
								json.RawMessage(`{"id":"answer","type":"agentMessage","text":"{\"score\":1}"}`),
							}
						}
						thread := testCodexThread(
							threadID,
							baseThread,
							turnID,
							status,
							items,
						)
						thread["cliVersion"] = version
						return map[string]any{"thread": thread}, notifications, nil
					case "mcpServerStatus/list":
						return map[string]any{"data": []any{}}, nil, nil
					case "turn/start":
						params := request.Params.(map[string]any)
						threadID, _ := params["threadId"].(string)
						turnStartCount[threadID]++
						if turnStartCount[threadID] != 1 {
							return nil, nil, fmt.Errorf(
								"second assay turn started on fork %s",
								threadID,
							)
						}
						turnID := fmt.Sprintf(
							"turn-assay-retry-%d",
							forkAttempt[threadID],
						)
						activeTurn[threadID] = turnID
						return map[string]any{
							"turn": map[string]any{
								"id":     turnID,
								"status": "inProgress",
							},
						}, nil, nil
					case "turn/interrupt":
						return map[string]any{}, nil, nil
					case "thread/delete":
						params := request.Params.(map[string]any)
						threadID, _ := params["threadId"].(string)
						deleteCount[threadID]++
						return map[string]any{}, nil, nil
					default:
						return nil, nil, fmt.Errorf(
							"unexpected method %s",
							request.Method,
						)
					}
				},
			)
			defer server.Close()

			source := testCodexContextHostRecord(
				t,
				server.URL,
				sourceHome,
				baseThread,
				baseTurn,
			)
			source.Snapshot.Codex.ProviderVersion = version
			factory := &CodexHostFactory{contextPolicy: codexPolicyProbe{},
				assayStabilityWindow: time.Millisecond,
				assaySource: func(
					ctx context.Context,
					source *CodexRunnerSnapshot,
				) (*codexRPCClient, func(), error) {
					client, err := dialCodexTestSource(ctx, source)
					return client, func() {}, err
				},
			}
			result, err := factory.RunContextAssay(
				context.Background(),
				source,
				CodexContextAssayRequest{
					BaseCtxID:    "CTX-260730-cc0099",
					Prompt:       "return a score without tools",
					OutputSchema: json.RawMessage(`{"type":"object"}`),
				},
			)
			if test.wantUnavailable {
				assertPublicContextCode(
					t,
					err,
					"context_assay_isolation_unavailable",
				)
				if !strings.Contains(
					err.Error(),
					fmt.Sprintf(
						"after %d fresh-fork attempts",
						codexContextAssayMaxAttempts,
					),
				) {
					t.Fatalf("bounded retry evidence = %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if result.Attestation.ForkThreadID !=
					fmt.Sprintf(
						"019f-assay-retry-fork-%d",
						codexContextAssayMaxAttempts,
					) ||
					result.Attestation.ToolEventCount != 0 ||
					string(result.Output) != `{"score":1}` {
					t.Fatalf("retry assay result = %#v", result)
				}
			}
			afterDigest, err := sha256File(baseRollout)
			if err != nil {
				t.Fatal(err)
			}
			if beforeDigest != afterDigest {
				t.Fatalf(
					"retry polluted base rollout: before=%s after=%s",
					beforeDigest,
					afterDigest,
				)
			}
			mu.Lock()
			defer mu.Unlock()
			if forkCalls != codexContextAssayMaxAttempts {
				t.Fatalf(
					"thread/fork calls = %d, want %d",
					forkCalls,
					codexContextAssayMaxAttempts,
				)
			}
			if len(turnStartCount) != codexContextAssayMaxAttempts {
				t.Fatalf("turn/start threads = %#v", turnStartCount)
			}
			for threadID, count := range turnStartCount {
				if count != 1 {
					t.Fatalf(
						"turn/start count for %s = %d, want 1",
						threadID,
						count,
					)
				}
				if deleteCount[threadID] != 1 {
					t.Fatalf(
						"thread/delete count for %s = %d, want 1",
						threadID,
						deleteCount[threadID],
					)
				}
			}
		})
	}
}

// This case preserves its original protocol/refusal assertions on typed native records.
func TestCodexContextAssayAttestsZeroToolsAndRejectsBypass(t *testing.T) {
	tests := []struct {
		name         string
		item         json.RawMessage
		profile      string
		mcpInventory bool
		wantCode     string
	}{
		{
			name:    "zero tool evidence",
			item:    json.RawMessage(`{"id":"answer","type":"agentMessage","text":"{\"score\":1}"}`),
			profile: codexContextAssayPermission,
		},
		{
			name:     "deny policy bypass attempt",
			item:     json.RawMessage(`{"id":"cmd","type":"commandExecution","status":"completed"}`),
			profile:  codexContextAssayPermission,
			wantCode: "context_assay_tool_use_detected",
		},
		{
			name:     "permission profile drift",
			item:     json.RawMessage(`{"id":"answer","type":"agentMessage","text":"{\"score\":1}"}`),
			profile:  ":workspace",
			wantCode: "context_assay_isolation_unavailable",
		},
		{
			name:         "external MCP inventory refuses attestation",
			item:         json.RawMessage(`{"id":"answer","type":"agentMessage","text":"{\"score\":1}"}`),
			profile:      codexContextAssayPermission,
			mcpInventory: true,
			wantCode:     "context_assay_isolation_unavailable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const (
				baseThread = "019f-assay-base"
				baseTurn   = "turn-assay-base"
				forkThread = "019f-assay-fork"
				assayTurn  = "turn-assay"
			)
			sourceHome := t.TempDir()
			writeCodexContextRollout(t, sourceHome, baseThread, "immutable-assay-base\n")
			var (
				forkRequest     map[string]any
				forkCalls       int
				threadStarted   bool
				inventoryProbed bool
				turnStarted     bool
				postStartReads  int
			)
			server := newCodexContextRPCServer(t, func(request codexRPCRequest) (any, []map[string]any, error) {
				switch request.Method {
				case "initialize":
					return map[string]any{}, nil, nil
				case "thread/read":
					params := request.Params.(map[string]any)
					if params["threadId"] == baseThread {
						return map[string]any{"thread": testCodexThread(
							baseThread, "", baseTurn, "completed", nil,
						)}, nil, nil
					}
					if !threadStarted {
						threadStarted = true
						return map[string]any{"thread": testCodexThread(
								forkThread, baseThread, baseTurn, "completed", nil,
							)}, []map[string]any{
								{
									"method": "mcpServer/startupStatus/updated",
									"params": map[string]any{
										"threadId": forkThread,
										"name":     "codex_apps",
										"status":   "starting",
									},
								},
								{
									"method": "thread/started",
									"params": map[string]any{
										"thread": testCodexThread(
											forkThread, baseThread, baseTurn, "completed", nil,
										),
									},
								},
							}, nil
					}
					if !turnStarted {
						return map[string]any{"thread": testCodexThread(
							forkThread, baseThread, baseTurn, "completed", nil,
						)}, nil, nil
					}
					postStartReads++
					if postStartReads == 1 {
						return map[string]any{"thread": testCodexThread(
							forkThread, baseThread, baseTurn, "completed", nil,
						)}, nil, nil
					}
					return map[string]any{"thread": testCodexThread(
						forkThread, baseThread, assayTurn, "completed", []json.RawMessage{test.item},
					)}, nil, nil
				case "thread/fork":
					forkCalls++
					forkRequest = request.Params.(map[string]any)
					return map[string]any{
						"thread":                  testCodexThread(forkThread, baseThread, baseTurn, "completed", nil),
						"activePermissionProfile": map[string]any{"id": test.profile},
					}, nil, nil
				case "mcpServerStatus/list":
					inventoryProbed = true
					data := []any{}
					if test.mcpInventory {
						data = append(data, map[string]any{
							"name":              "codex_apps",
							"tools":             map[string]any{"search": map[string]any{}},
							"resources":         []any{},
							"resourceTemplates": []any{},
						})
					}
					return map[string]any{"data": data}, []map[string]any{{
						"method": "mcpServer/startupStatus/updated",
						"params": map[string]any{
							"threadId": forkThread,
							"name":     "codex_apps",
							"status":   "ready",
						},
					}}, nil
				case "turn/start":
					if !threadStarted || !inventoryProbed {
						return nil, nil, fmt.Errorf(
							"turn/start raced fork readiness: thread_started=%t inventory_probed=%t",
							threadStarted,
							inventoryProbed,
						)
					}
					params := request.Params.(map[string]any)
					if _, present := params["permissions"]; present {
						return nil, nil, fmt.Errorf(
							"turn must inherit the fork-attested profile, got permissions=%#v",
							params["permissions"],
						)
					}
					if _, present := params["approvalPolicy"]; present {
						return nil, nil, fmt.Errorf(
							"turn must inherit fork approval policy, got approvalPolicy=%#v",
							params["approvalPolicy"],
						)
					}
					sandboxPolicy, _ := params["sandboxPolicy"].(map[string]any)
					if sandboxPolicy["type"] != "readOnly" ||
						sandboxPolicy["networkAccess"] != false {
						return nil, nil, fmt.Errorf(
							"turn sandbox policy = %#v",
							params["sandboxPolicy"],
						)
					}
					turnStarted = true
					return map[string]any{"turn": map[string]any{
						"id": assayTurn, "status": "inProgress",
					}}, nil, nil
				case "turn/interrupt", "thread/delete":
					return map[string]any{}, nil, nil
				default:
					return nil, nil, fmt.Errorf("unexpected method %s", request.Method)
				}
			})
			defer server.Close()
			factory := &CodexHostFactory{contextPolicy: codexPolicyProbe{},
				assayStabilityWindow: time.Millisecond,
				assaySource: func(
					ctx context.Context,
					source *CodexRunnerSnapshot,
				) (*codexRPCClient, func(), error) {
					client, err := dialCodexTestSource(ctx, source)
					return client, func() {}, err
				},
			}
			result, err := factory.RunContextAssay(
				context.Background(),
				testCodexContextHostRecord(t, server.URL, sourceHome, baseThread, baseTurn),
				CodexContextAssayRequest{
					BaseCtxID: "CTX-260730-cc0001",
					Prompt:    "return score without tools",
					OutputSchema: json.RawMessage(
						`{"type":"object","required":["score"],"properties":{"score":{"type":"integer"}}}`,
					),
				},
			)
			if test.wantCode != "" {
				assertPublicContextCode(t, err, test.wantCode)
				if forkCalls != 1 {
					t.Fatalf(
						"non-interrupt refusal retried thread/fork %d time(s)",
						forkCalls,
					)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Attestation.ToolEventCount != 0 ||
				result.Attestation.BaseThreadID != baseThread ||
				result.Attestation.ForkThreadID != forkThread ||
				result.Attestation.ForkedFromID != baseThread ||
				result.Attestation.TurnID != assayTurn ||
				result.Attestation.PolicyDigest == "" ||
				string(result.Output) != `{"score":1}` {
				t.Fatalf("assay result = %#v", result)
			}
			if forkCalls != 1 {
				t.Fatalf("successful assay fork calls = %d, want 1", forkCalls)
			}
			if forkRequest["permissions"] != codexContextAssayPermission {
				t.Fatalf("fork permissions = %#v", forkRequest["permissions"])
			}
			if forkRequest["approvalPolicy"] != "never" {
				t.Fatalf("fork approval policy = %#v", forkRequest["approvalPolicy"])
			}
			if _, present := forkRequest["ephemeral"]; present {
				t.Fatalf("observable assay fork must be cleaned up, not ephemeral: %#v", forkRequest)
			}
			if forkRequest["deferGoalContinuation"] != true {
				t.Fatalf("observable assay fork must defer native continuation: %#v", forkRequest)
			}
			roots, ok := forkRequest["runtimeWorkspaceRoots"].([]any)
			if !ok || len(roots) != 0 {
				t.Fatalf("assay runtime workspace roots = %#v", forkRequest["runtimeWorkspaceRoots"])
			}
			config, _ := forkRequest["config"].(map[string]any)
			apps, _ := config["apps"].(map[string]any)
			defaults, _ := apps["_default"].(map[string]any)
			if defaults["enabled"] != false {
				t.Fatalf("assay fork did not disable all app tools: %#v", forkRequest)
			}
		})
	}
}

// This case preserves its original protocol/refusal assertions on typed native records.
func TestCodexAssayItemClassifierIsClosedOverCurrentProtocol(t *testing.T) {
	passive := []json.RawMessage{
		json.RawMessage(`{"id":"user","type":"userMessage","text":"quiz"}`),
		json.RawMessage(`{"id":"reason","type":"reasoning"}`),
		json.RawMessage(`{"id":"answer","type":"agentMessage","text":"{\"score\":1}"}`),
	}
	output, toolCount, err := inspectCodexAssayItems(passive)
	if err != nil || toolCount != 0 || string(output) != `{"score":1}` {
		t.Fatalf("passive items output=%s tools=%d err=%v", output, toolCount, err)
	}
	for _, itemType := range []string{
		"hookPrompt",
		"commandExecution",
		"fileChange",
		"mcpToolCall",
		"dynamicToolCall",
		"collabAgentToolCall",
		"subAgentActivity",
		"webSearch",
		"imageView",
		"sleep",
		"imageGeneration",
	} {
		_, toolCount, err := inspectCodexAssayItems([]json.RawMessage{
			json.RawMessage(fmt.Sprintf(`{"id":"tool","type":%q}`, itemType)),
		})
		if err != nil || toolCount != 1 {
			t.Fatalf("tool type %s count=%d err=%v", itemType, toolCount, err)
		}
	}
	_, _, err = inspectCodexAssayItems([]json.RawMessage{
		json.RawMessage(`{"id":"future","type":"futureItem"}`),
	})
	assertPublicContextCode(t, err, "context_assay_isolation_unavailable")
}

func newCodexContextRPCServer(
	t *testing.T,
	handle func(codexRPCRequest) (any, []map[string]any, error),
) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "")
		for {
			var request codexRPCRequest
			if err := wsjson.Read(context.Background(), connection, &request); err != nil {
				return
			}
			if request.Method == "initialized" {
				continue
			}
			result, notifications, handleErr := handle(request)
			for _, notification := range notifications {
				if err := wsjson.Write(context.Background(), connection, notification); err != nil {
					return
				}
			}
			response := map[string]any{"id": request.ID, "result": result}
			if handleErr != nil {
				response = map[string]any{
					"id":    request.ID,
					"error": map[string]any{"code": -32601, "message": handleErr.Error()},
				}
			}
			if err := wsjson.Write(context.Background(), connection, response); err != nil {
				return
			}
		}
	}))
}

func testCodexAssayFactory() *CodexHostFactory {
	return &CodexHostFactory{contextPolicy: codexPolicyProbe{},
		assayStabilityWindow: time.Millisecond,
		assaySource: func(
			ctx context.Context,
			source *CodexRunnerSnapshot,
		) (*codexRPCClient, func(), error) {
			client, err := dialCodexTestSource(ctx, source)
			return client, func() {}, err
		},
	}
}

func testCodexThread(
	id string,
	forkedFromID string,
	turnID string,
	status string,
	items []json.RawMessage,
) map[string]any {
	return map[string]any{
		"id":           id,
		"forkedFromId": forkedFromID,
		"cliVersion":   "test-v1",
		"status":       map[string]any{"type": map[bool]string{true: "idle", false: "active"}[status == "completed"]},
		"turns": []map[string]any{{
			"id": turnID, "status": status, "items": items,
		}},
	}
}

func testCodexContextHostRecord(
	t *testing.T,
	serverURL string,
	codexHome string,
	threadID string,
	lastTurnID string,
	providerVersion ...string,
) ProviderHostRecord {
	t.Helper()
	recorded := "test-v1"
	if len(providerVersion) > 0 && providerVersion[0] != "" {
		recorded = providerVersion[0]
	}
	snapshot := CodexRunnerSnapshot{
		Contract:            codexHostSnapshotContract,
		PID:                 os.Getpid(),
		URL:                 websocketTestURL(serverURL),
		ThreadID:            threadID,
		ProjectDir:          t.TempDir(),
		CodexHome:           codexHome,
		ProviderVersion:     recorded,
		LastCompletedTurnID: lastTurnID,
	}
	return ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &snapshot}}
}

func writeCodexContextRollout(
	t *testing.T,
	codexHome string,
	threadID string,
	content string,
) string {
	t.Helper()
	path := filepath.Join(
		codexHome,
		"sessions",
		"2026",
		"07",
		"30",
		"rollout-2026-07-30T00-00-00-"+threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func websocketTestURL(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http")
}

func assertPublicContextCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want code %s", want)
	}
	var contextErr *CodexContextError
	if errors.As(err, &contextErr) && contextErr.Code == want {
		return
	}
	var publicErr *PublicError
	if errors.As(err, &publicErr) && publicErr.Code == want {
		return
	}
	t.Fatalf("error = %v, want code %s", err, want)
}

// codexPolicyProbe is a test-only consumer port. It asserts real filesystem
// observations and returns a controlled compaction fact for the wire fixture;
// it claims no canonical retention/retry authority or production implementation.
type codexPolicyProbe struct{}

func (codexPolicyProbe) RequirePayloadPresent(r CodexPresenceReport, _ time.Time, _ ContextResolutionMode) error {
	if r.Presence != "present" {
		return fmt.Errorf("payload probe %s: %s", r.Presence, r.Detail)
	}
	return nil
}
func (codexPolicyProbe) ResumeUnusable(a CodexResumeAttempt) error { return a.Cause }
func (codexPolicyProbe) InspectTranscript(r io.Reader) (string, string, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256(b)
	marker := ""
	if bytes.Contains(b, []byte(`"type":"compacted"`)) {
		marker = "compacted"
	}
	return hex.EncodeToString(sum[:]), marker, nil
}
func dialCodexTestSource(ctx context.Context, s *CodexRunnerSnapshot) (*codexRPCClient, error) {
	c, err := dialCodexRPC(ctx, s.URL)
	if err != nil {
		return nil, err
	}
	if err = initializeCodexRunnerRPC(ctx, c); err != nil {
		c.close()
		return nil, err
	}
	return c, nil
}
