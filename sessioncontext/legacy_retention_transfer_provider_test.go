package sessioncontext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The public factory's real typed launch route invokes the same native fork
// materializer as the original in-package tests. No provider algorithm is copied.
// These absence cases must refuse before any RPC or executable start.
func legacyRetentionTransferMaterializeCodexFork(ctx context.Context, p *sm.CodexContextForkPlan, home, project string) (sm.RunnerHost, error) {
	variants := sm.LaunchVariants{ManagedHost: sm.LaunchArgv{Kind: "codex-app-server", Argv: []string{"app-server"}}}
	plan := sm.PrimarySessionLaunchPlan{Contract: sm.PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "codex", ProjectDir: project, Executable: "/bin/false", LaunchVariants: variants}
	prior := sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex-fork", CodexFork: p}}
	return sm.NewCodexHostFactory(sm.Layout{SessionsDir: home}, ProviderPolicy{}).Launch(ctx, sm.ProviderLaunchRequest{SessionID: "retention-child", Launch: sm.LaunchSpec{Executable: plan.Executable, Cwd: project, Codex: &sm.CodexPreparedLaunch{Plan: plan}}, Prior: &prior, PriorMustFork: true})
}

// TestCodexForkRefusesAVanishedPayloadBeforeAnyRPC drives the real materialization path with a rollout that is not there.
func TestCodexForkRefusesAVanishedPayloadBeforeAnyRPC(t *testing.T) {
	codexHome := t.TempDir()
	// An archived copy and no live one. Codex archives by moving, so this is the
	// ordinary shape of a rollout an operator archived and then tried to fork.
	archived := legacyRetentionTransferWriteArchived(t, codexHome, legacyRetentionTransferOwnedThread, "archived\n")

	_, err := legacyRetentionTransferMaterializeCodexFork(
		context.Background(),
		&sm.CodexContextForkPlan{
			Contract:     "task-board.codex-context-fork-plan-v1",
			PrimaryCtxID: legacyRetentionTransferCtxID,
			Primary: legacyRetentionTransferCodexHostRecord(
				t, "http://127.0.0.1:1", codexHome, legacyRetentionTransferOwnedThread, "turn-owned",
			),
			PrimaryMode: "auto",
		},
		t.TempDir(),
		t.TempDir(),
	)

	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != "context_payload_vanished" {
		t.Fatalf("a vanished Codex rollout did not produce the handled path: %v", err)
	}
	if !typed.RetryNextCandidate {
		t.Fatal("an auto-resolved refusal must still allow the next ranked candidate")
	}
	// The message has to be actionable, which for an archived rollout means
	// saying the bytes are in the archive rather than gone.
	if !strings.Contains(typed.Message, "archived") {
		t.Fatalf("the refusal does not mention the surviving archived rollout: %s", typed.Message)
	}
	if !strings.Contains(typed.Message, string("payload_vanished")) {
		t.Fatalf("the refusal names no recovery reason: %s", typed.Message)
	}
	var unusable *contextPayloadUnusableError
	if !errors.As(err, &unusable) || unusable.Reason != "payload_vanished" {
		t.Fatalf("the refusal is not routable to the registry recovery path: %v", err)
	}
	legacyRetentionTransferAssertSurvives(t, archived, "a refused fork must not delete anything")
}

// TestCodexForkRefusesAnEmptyStoreWithAnAttributableMessage covers the other vanished shape: nothing anywhere.
func TestCodexForkRefusesAnEmptyStoreWithAnAttributableMessage(t *testing.T) {
	_, err := legacyRetentionTransferMaterializeCodexFork(
		context.Background(),
		&sm.CodexContextForkPlan{
			Contract:     "task-board.codex-context-fork-plan-v1",
			PrimaryCtxID: legacyRetentionTransferCtxID,
			Primary: legacyRetentionTransferCodexHostRecord(
				t, "http://127.0.0.1:1", t.TempDir(), legacyRetentionTransferOwnedThread, "turn-owned",
			),
		},
		t.TempDir(),
		t.TempDir(),
	)
	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != "context_payload_vanished" {
		t.Fatalf("an empty Codex store did not produce the handled path: %v", err)
	}
	// Codex has no known deletion horizon, so the refusal must say the board
	// cannot attribute the loss and name the candidates. Both wrong answers are
	// asserted against: "wait for the provider timer" sends an operator to wait
	// for a timer that does not exist, and "it must have been removed from
	// outside the provider" states a certainty an unobserved-pruning provider
	// does not support.
	for _, want := range []string{
		"no cleanup by this provider is documented or has been observed",
		"no provider retention schedule to attribute the loss to",
	} {
		if !strings.Contains(typed.Message, want) {
			t.Fatalf("the refusal does not say %q: %s", want, typed.Message)
		}
	}
	if strings.Contains(typed.Message, "removed outside the board") {
		t.Fatalf("the refusal claims an attribution the board cannot make: %s", typed.Message)
	}
	if typed.RetryNextCandidate {
		t.Fatal("an explicitly named context must not silently fall through to another")
	}
}

// The original absence/corruption scenarios use actual isolated transcript
// paths. A declared version fixture supplies only the public launch prerequisite;
// the real materializer and policy decide the refusal, never this fixture.
type legacyRetentionTransferClaudeFixture struct {
	home, root, baseDir, baseUUID, basePath string
	layout                                  sm.Layout
	clock                                   func() time.Time
}

func legacyRetentionTransferNewClaudeFixture(t *testing.T) *legacyRetentionTransferClaudeFixture {
	t.Helper()
	root, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	f := &legacyRetentionTransferClaudeFixture{root: root, home: filepath.Join(root, "claude-home"), baseDir: filepath.Join(root, "project"), baseUUID: "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa", layout: legacyRetentionTransferLayout(t, "claude-retention")}
	f.layout.SessionsDir = filepath.Join(f.root, "sessions")
	now := time.Now().UTC()
	f.clock = func() time.Time { return now }
	t.Setenv("CLAUDE_CONFIG_DIR", f.home)
	if e = os.MkdirAll(f.baseDir, 0700); e != nil {
		t.Fatal(e)
	}
	slug, e := sm.ClaudeProjectSlug(f.baseDir)
	if e != nil {
		t.Fatal(e)
	}
	f.basePath = filepath.Join(f.home, "projects", slug, f.baseUUID+".jsonl")
	if e = os.MkdirAll(filepath.Dir(f.basePath), 0700); e != nil {
		t.Fatal(e)
	}
	// Both deleted-base tests remove this file; the stale-build test overwrites
	// it with the original malformed bytes before any attempted transcript read.
	raw := fmt.Sprintf("{\"type\":\"user\",\"uuid\":\"u0\",\"sessionId\":%q,\"cwd\":%q,\"version\":\"2.1.220\",\"message\":{\"role\":\"user\",\"content\":\"base question\"}}\n", f.baseUUID, f.baseDir)
	if e = os.WriteFile(f.basePath, []byte(raw), 0600); e != nil {
		t.Fatal(e)
	}
	return f
}
func (f *legacyRetentionTransferClaudeFixture) dir(name string) string {
	p := filepath.Join(f.root, name)
	if e := os.MkdirAll(p, 0700); e != nil {
		panic(e)
	}
	return p
}
func (f *legacyRetentionTransferClaudeFixture) plan(target string) *sm.ClaudeContextForkPlan {
	b, e := os.ReadFile(f.basePath)
	digest := ""
	if e == nil {
		x := sha256.Sum256(b)
		digest = hex.EncodeToString(x[:])
	}
	return &sm.ClaudeContextForkPlan{Contract: "task-board.claude-context-fork-plan-v1", ChildCtxID: "CTX-260730-c00002", PrimaryCtxID: "CTX-260730-c00001", BaseSessionUUID: f.baseUUID, BaseProjectDir: f.baseDir, BaseSHA256: digest, BaseRecords: 1, BaseFormatVersion: "2.1.220", TargetProjectDir: target, PreparedAt: f.clock()}
}
func (f *legacyRetentionTransferClaudeFixture) assertNoTranscriptFor(t *testing.T, project string) {
	t.Helper()
	slug, e := sm.ClaudeProjectSlug(project)
	if e != nil {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(filepath.Join(f.home, "projects", slug))
	if os.IsNotExist(e) {
		return
	}
	if e != nil {
		t.Fatal(e)
	}
	for _, entry := range entries {
		t.Fatalf("refused materialization left %q under %s", entry.Name(), slug)
	}
}
func legacyRetentionTransferMaterializeClaudeFork(p *sm.ClaudeContextForkPlan, l sm.Layout, project, active string, _ func() time.Time) (sm.RunnerHost, error) {
	dir := filepath.Join(l.StateDir, "retention-version-fixture")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	executable := filepath.Join(dir, "claude")
	script := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf '%s\\n' '" + active + "'; exit 0; fi\nprintf '%s\\n' '{\"result\":\"native goal available\"}'\n"
	if e := os.WriteFile(executable, []byte(script), 0700); e != nil {
		return nil, e
	}
	launch := sm.PrimarySessionLaunchPlan{Contract: sm.PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "claude", ProjectDir: project, Executable: executable, LaunchVariants: sm.LaunchVariants{ManagedHost: sm.LaunchArgv{Kind: "claude-pty"}}}
	prior := sm.ProviderHostRecord{Provider: "claude", Snapshot: sm.RunnerSnapshot{Kind: "claude-fork", ClaudeFork: p}}
	host, e := sm.NewClaudeHostFactory(l).Launch(context.Background(), sm.ProviderLaunchRequest{SessionID: "retention-child", Launch: sm.LaunchSpec{Claude: &sm.ClaudePreparedLaunch{Contract: sm.ClaudeSessionLaunchContract, Plan: launch}}, Prior: &prior, PriorMustFork: true})
	return host, ClaudePolicyError(e)
}

// TestClaudeForkRefusesAProviderDeletedBaseAsANormalPath is the Claude half.
func TestClaudeForkRefusesAProviderDeletedBaseAsANormalPath(t *testing.T) {
	fixture := legacyRetentionTransferNewClaudeFixture(t)
	targetDir := fixture.dir("worktree")
	plan := fixture.plan(targetDir)
	plan.PrimaryReadyAt = fixture.clock().Add(-31 * 24 * time.Hour)

	if err := os.Remove(fixture.basePath); err != nil {
		t.Fatal(err)
	}

	_, err := legacyRetentionTransferMaterializeClaudeFork(
		plan, fixture.layout, targetDir, "2.1.220", fixture.clock)

	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != "context_payload_vanished" {
		t.Fatalf("a provider-deleted Claude base did not produce the handled path: %v", err)
	}
	if !strings.Contains(typed.Message, "gone rather than misplaced") {
		t.Fatalf("the refusal does not attribute the loss to the 30-day timer: %s", typed.Message)
	}
	if !strings.Contains(typed.Message, string("payload_vanished")) {
		t.Fatalf("the refusal names no recovery reason: %s", typed.Message)
	}
	fixture.assertNoTranscriptFor(t, targetDir)
}

// TestClaudeForkInsideTheRetentionWindowReportsAnOutsideRemoval pins the other half of the same message.
func TestClaudeForkInsideTheRetentionWindowReportsAnOutsideRemoval(t *testing.T) {
	fixture := legacyRetentionTransferNewClaudeFixture(t)
	targetDir := fixture.dir("worktree")
	plan := fixture.plan(targetDir)
	plan.PrimaryReadyAt = fixture.clock().Add(-2 * 24 * time.Hour)

	if err := os.Remove(fixture.basePath); err != nil {
		t.Fatal(err)
	}

	_, err := legacyRetentionTransferMaterializeClaudeFork(
		plan, fixture.layout, targetDir, "2.1.220", fixture.clock)

	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != "context_payload_vanished" {
		t.Fatalf("a vanished Claude base did not produce the handled path: %v", err)
	}
	if !strings.Contains(typed.Message, "outside the provider's own cleanup") {
		t.Fatalf("an early loss was blamed on provider retention: %s", typed.Message)
	}
}

// TestClaudeForkAttemptsAStaleBuildBeforeReportingTheGap is the stale-binary case.
func TestClaudeForkAttemptsAStaleBuildBeforeReportingTheGap(t *testing.T) {
	fixture := legacyRetentionTransferNewClaudeFixture(t)
	targetDir := fixture.dir("worktree")
	plan := fixture.plan(targetDir)
	plan.BaseFormatVersion = "1.0.4"

	// Present but unreadable: the file exists, so the presence probe passes and
	// the failure comes from the read the adapter actually attempted.
	if err := os.WriteFile(fixture.basePath, []byte("{not json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	plan.BaseSHA256 = ""

	_, err := legacyRetentionTransferMaterializeClaudeFork(
		plan, fixture.layout, targetDir, "2.1.220", fixture.clock)

	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != "context_provider_incompatible" {
		t.Fatalf("a failed read on a materially older base did not fall back: %v", err)
	}
	if !strings.Contains(typed.Message, string("material_drift")) {
		t.Fatalf("the fallback does not name the build gap: %s", typed.Message)
	}
	if !strings.Contains(typed.Message, "1.0.4") || !strings.Contains(typed.Message, "2.1.220") {
		t.Fatalf("the fallback does not name both builds: %s", typed.Message)
	}
	if !strings.Contains(typed.Message, "attempted and never assumed") {
		t.Fatalf("the fallback does not report that a resume was attempted: %s", typed.Message)
	}
	var unusable *contextPayloadUnusableError
	if !errors.As(err, &unusable) ||
		unusable.Reason != "provider_incompatible" {
		t.Fatalf("the fallback is not routable to the registry recovery path: %v", err)
	}
}

func legacyRetentionTransferRPCServer(
	t *testing.T,
	handle func(legacyRetentionTransferRPCRequest) (any, []map[string]any, error),
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
			var request legacyRetentionTransferRPCRequest
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

func legacyRetentionTransferThread(
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

type legacyRetentionTransferRPCRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// The actual native RPC must attempt thread/fork across the build gap before
// the Service records and reports the original incompatible-payload recovery.
func TestCodexResumeFailureAcrossBuildsIsRecoveredInTheRegistry(t *testing.T) {
	const (
		baseThread = "019f-drift-recovery-base"
		baseTurn   = "turn-drift-recovery-base"
	)
	codexHome := t.TempDir()
	legacyRetentionTransferWriteLive(t, codexHome, baseThread, "older-build-base\n")
	var forkCalls int
	server := legacyRetentionTransferRPCServer(t, func(request legacyRetentionTransferRPCRequest) (any, []map[string]any, error) {
		switch request.Method {
		case "initialize":
			return map[string]any{}, nil, nil
		case "thread/read":
			thread := legacyRetentionTransferThread(baseThread, "", baseTurn, "completed", nil)
			thread["cliVersion"] = "0.130.0"
			return map[string]any{"thread": thread}, nil, nil
		case "thread/fork":
			forkCalls++
			return nil, nil, errors.New("method not found")
		default:
			return nil, nil, errors.New("unexpected method " + request.Method)
		}
	})
	defer server.Close()

	_, adapterErr := legacyRetentionTransferMaterializeCodexFork(
		context.Background(),
		&sm.CodexContextForkPlan{
			Contract:     "task-board.codex-context-fork-plan-v1",
			PrimaryCtxID: legacyRetentionTransferCtxID,
			Primary: legacyRetentionTransferCodexHostRecord(
				t, server.URL, codexHome, baseThread, baseTurn, "0.104.0",
			),
		},
		t.TempDir(),
		t.TempDir(),
	)
	if forkCalls != 1 {
		t.Fatalf("thread/fork was attempted %d time(s); the resume must be tried, not predicted", forkCalls)
	}

	record := legacyRetentionTransferContext(legacyRetentionTransferCtxID, true)
	record.State = "ready"
	authority := legacyRetentionTransferNewAuthority(record)
	manager := legacyRetentionTransferOpen(t, legacyRetentionTransferLayout(t, "codex-drift-recovery"), authority)

	err := manager.RecoverUnusable(context.Background(), adapterErr)

	var typed *ContextError
	if !errors.As(err, &typed) ||
		typed.Code != "context_provider_incompatible" {
		t.Fatalf("a refused cross-build fork did not fall back to rebuild: %v", err)
	}
	if !strings.Contains(typed.Message, "marked stale") ||
		!strings.Contains(typed.Message, "rebuilt from cold") {
		t.Fatalf("the caller was not told what the recovery did: %s", typed.Message)
	}
	stored, storeErr := authority.Context(context.Background(), legacyRetentionTransferCtxID)
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	if stored.State != "stale" ||
		stored.StateReason != "provider_incompatible" {
		t.Fatalf("the registry still serves a base this build cannot fork: %+v", stored)
	}
}
