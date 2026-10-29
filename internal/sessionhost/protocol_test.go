package sessionhost_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// This child implements bounded synthetic RPC examples, not a real harness.
func TestProtocolProcess(t *testing.T) {
	mode := os.Getenv("SESSION_HOST_RPC_FIXTURE")
	if mode == "" {
		return
	}
	write := func(v any) {
		if json.NewEncoder(os.Stdout).Encode(v) != nil {
			os.Exit(3)
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var r struct {
			ID     uint64         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if json.Unmarshal(scanner.Bytes(), &r) != nil {
			os.Exit(4)
		}
		switch r.Method {
		case "initialize":
			write(map[string]any{"id": r.ID, "result": map[string]any{"userAgent": "synthetic", "platformFamily": "unix", "platformOs": "macos"}})
		case "initialized":
		case "thread/start":
			write(map[string]any{"id": r.ID, "result": map[string]any{"thread": map[string]any{"id": "synthetic-thread"}, "model": r.Params["model"], "modelProvider": r.Params["modelProvider"]}})
		case "turn/start":
			turn := fmt.Sprintf("turn-%d", r.ID)
			write(map[string]any{"id": r.ID, "result": map[string]any{"turn": map[string]any{"id": turn, "status": "inProgress"}}})
			if mode == "approval" {
				write(map[string]any{"id": "server-request", "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "synthetic-thread"}})
				continue
			}
			if mode == "gated" {
				deadline := time.Now().Add(10 * time.Second)
				for {
					if _, err := os.Stat(filepath.Join(".", "complete-"+turn)); err == nil {
						break
					}
					if time.Now().After(deadline) {
						os.Exit(5)
					}
					time.Sleep(time.Millisecond)
				}
			}
			write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "synthetic-thread", "turn": map[string]any{"id": turn, "status": "completed"}}})
		default:
			os.Exit(6)
		}
	}
	os.Exit(0)
}

func setupProtocol(t *testing.T, mode string) (sessionhost.Config, *runner.Server, string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "session-protocol-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	rc := runner.Config{Socket: filepath.Join(dir, "r.sock"), Execution: "protocol-execution", IOTimeout: 200 * time.Millisecond,
		Plan: runner.Plan{Binary: binary, Args: []string{"-test.run=^TestProtocolProcess$"}, Cwd: dir, Env: []string{"SESSION_HOST_RPC_FIXTURE=" + mode, "GORACE=atexit_sleep_ms=0"}}}
	s, err := runner.Start(rc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := sessionhost.Config{StateDir: filepath.Join(dir, "state"), Socket: rc.Socket, Execution: rc.Execution,
		Selection:    sessionhost.Selection{Vendor: "unfamiliar-provider", Model: "unfamiliar-model", Reasoning: "unfamiliar-effort"},
		ProtocolName: "codex", PollInterval: 10 * time.Millisecond, IOTimeout: time.Second}
	cfg.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
		return codexadapter.OpenDriver(ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create, Selection: ap.Selection{Provider: cfg.Selection.Vendor, Model: cfg.Selection.Model, Effort: cfg.Selection.Reasoning}, Workspace: dir}, saved)
	}
	return cfg, s, dir
}

func awaitProtocol(t *testing.T, h *sessionhost.Host, claim string, ready func(sessionhost.State) bool) sessionhost.State {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s := h.Snapshot()
		if ready(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s: %+v", claim, h.Snapshot())
	return sessionhost.State{}
}

// Native acceptance stays unknown until matching completion. Known-idle host
// replacement keeps the child/thread and sends only the second turn: three
// initialization writes plus two turns, independent from notice sequences 1/2.
func TestProtocolHostAcceptanceAndReplacement(t *testing.T) {
	cfg, _, dir := setupProtocol(t, "gated")
	h := openHost(t, cfg)
	awaitProtocol(t, h, "initial idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "synthetic/one"}); err != nil {
		t.Fatal(err)
	}
	accepted := awaitProtocol(t, h, "native accepted", func(s sessionhost.State) bool { return len(s.Records) == 1 && s.Records[0].NativeAccepted })
	if accepted.Records[0].State != "unknown" || accepted.Records[0].Outcome != "" || accepted.InputState != sessionhost.InputBusy {
		t.Fatal("acceptance inferred completion")
	}
	if err := os.WriteFile(filepath.Join(dir, "complete-turn-3"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	first := await(t, h, "one", "applied")
	before := inspectRunner(t, cfg)
	if first.Sequence != 1 || first.Protocol.WireSequence != 4 || before.Writes != 4 || first.Records[0].NativeTurn != "turn-3" {
		t.Fatal("notice and wire sequence mixed")
	}
	h.Close()
	next := openHost(t, cfg)
	restored := awaitProtocol(t, next, "restored idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if restored.Protocol.View.NativeSession != "synthetic-thread" || restored.Epoch != first.Epoch+1 {
		t.Fatal("native identity or controller changed incorrectly")
	}
	if err := next.Fence(false); err != nil {
		t.Fatal(err)
	}
	if _, err := next.Enqueue(sessionhost.Notice{ID: "two", Reference: "synthetic/two"}); err != nil {
		t.Fatal(err)
	}
	if inspectRunner(t, cfg).Writes != 4 {
		t.Fatal("held input wrote")
	}
	if err := os.WriteFile(filepath.Join(dir, "complete-turn-4"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := next.Fence(true); err != nil {
		t.Fatal(err)
	}
	final := await(t, next, "two", "applied")
	after := inspectRunner(t, cfg)
	if after.PID != before.PID || after.Instance != before.Instance || after.Writes != 5 || final.Sequence != 2 || final.Protocol.WireSequence != 5 || final.Records[1].NativeTurn != "turn-4" {
		t.Fatal("host replacement repeated initialization/input or replaced child")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if r, err := next.Stop(ctx); err != nil || r.Code != "stopped" {
		t.Fatalf("owned stop: %s %v", r.Code, err)
	}
}

// A server approval request closes automatic input, retains the next notice,
// and produces neither a completion nor an approval response, including restore.
func TestProtocolApprovalNeverAnswered(t *testing.T) {
	cfg, _, _ := setupProtocol(t, "approval")
	h := openHost(t, cfg)
	awaitProtocol(t, h, "idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	h.Enqueue(sessionhost.Notice{ID: "one", Reference: "synthetic/one"})
	state := awaitProtocol(t, h, "approval", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputApproval })
	if state.Records[0].State != "unknown" || inspectRunner(t, cfg).Writes != 4 {
		t.Fatal("approval completed or wrote an answer")
	}
	if len(state.Protocol.Attention) != 1 || state.Protocol.Attention[0].Kind != ap.AuthorityRequired || !state.Protocol.Attention[0].OwnerOnly {
		t.Fatal("approval has no typed owner escalation observation")
	}
	if err := h.Fence(true); err == nil || err.Error() != "input_ineligible" {
		t.Fatalf("approval release: %v", err)
	}
	h.Close()
	next := openHost(t, cfg)
	awaitProtocol(t, next, "restored approval", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputApproval })
	if len(next.Snapshot().Protocol.Attention) != 1 || next.Snapshot().Protocol.Attention[0].Key != state.Protocol.Attention[0].Key {
		t.Fatal("restore lost or duplicated escalation")
	}
	if _, err := next.Enqueue(sessionhost.Notice{ID: "two", Reference: "synthetic/two"}); err != nil {
		t.Fatal(err)
	}
	if inspectRunner(t, cfg).Writes != 4 || next.Snapshot().Records[1].State != "queued" {
		t.Fatal("approval caused retry")
	}
}

// Isolated journal snapshots plant the exact durable boundary after Prepare but
// before the host recorded its write reply. A written receipt reconciles through
// matching native completion; missing/conflicting effects never get resent.
func TestProtocolOutboxRecoveryNeverResends(t *testing.T) {
	for _, effect := range []string{"written", "missing", "conflict"} {
		t.Run(effect, func(t *testing.T) {
			cfg, _, _ := setupProtocol(t, "normal")
			h := openHost(t, cfg)
			before := awaitProtocol(t, h, "idle", func(s sessionhost.State) bool {
				return s.InputState == sessionhost.InputIdle && len(s.Protocol.Pending) == 0
			})
			h.Close()
			driver, offset, err := cfg.ProtocolFactory(before.Protocol.Checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := driver.Observe(ap.Chunk{Offset: offset, AtTail: true}); err != nil {
				t.Fatal(err)
			}
			transition, err := driver.Prepare(ap.Input{ID: "one", Reference: "synthetic/one", Mode: ap.NextTurn})
			if err != nil {
				t.Fatal(err)
			}
			if len(transition.Actions) != 1 {
				t.Fatal("missing fixture action")
			}
			action := transition.Actions[0]
			hash := sha256.Sum256(action.Bytes)
			before.Sequence = 1
			notice := sessionhost.Notice{ID: "one", Reference: "synthetic/one"}
			encoded, _ := json.Marshal(struct {
				Notice    sessionhost.Notice
				Selection sessionhost.Selection
			}{notice, cfg.Selection})
			noticeHash := sha256.Sum256(encoded)
			before.Records = []sessionhost.Record{{Notice: notice, Digest: hex.EncodeToString(noticeHash[:]), Sequence: 1, State: "unknown"}}
			before.Protocol.Checkpoint = transition.Checkpoint
			before.Protocol.View = transition.View
			before.Protocol.WireSequence = 4
			before.Protocol.Pending = []sessionhost.WireAction{{Sequence: 4, Key: action.Key, InputID: action.InputID, Data: action.Bytes, Digest: hex.EncodeToString(hash[:])}}
			before.InputState = sessionhost.InputBusy
			if effect != "missing" {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				client, err := runner.Dial(ctx, cfg.Socket)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				before.Epoch++
				claim, err := client.Call(ctx, runner.Request{Op: "claim", Execution: cfg.Execution, Epoch: before.Epoch})
				for err == nil && claim.Code == "controller_conflict" && ctx.Err() == nil {
					time.Sleep(time.Millisecond)
					claim, err = client.Call(ctx, runner.Request{Op: "claim", Execution: cfg.Execution, Epoch: before.Epoch})
				}
				if err != nil || claim.Code != "ok" {
					t.Fatalf("fixture claim %s %v", claim.Code, err)
				}
				data := action.Bytes
				if effect == "conflict" {
					data = bytes.ReplaceAll(data, []byte("synthetic/one"), []byte("synthetic/other"))
				}
				written, err := client.Call(ctx, runner.Request{Op: "write", Execution: cfg.Execution, Epoch: before.Epoch, Sequence: 4, Data: data})
				if err != nil || written.Code != "written" {
					t.Fatalf("fixture write %s %v", written.Code, err)
				}
				_, err = client.Call(ctx, runner.Request{Op: "release", Execution: cfg.Execution, Epoch: before.Epoch})
				client.Close()
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}
			// This direct write changes only an owned fault-injection fixture; it
			// deliberately bypasses the store to plant an already-committed snapshot.
			data, err := json.Marshal(before)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cfg.StateDir, "state.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			next := openHost(t, cfg)
			var after sessionhost.State
			if effect == "written" {
				after = await(t, next, "one", "applied")
				if !after.Records[0].NativeAccepted || len(after.Protocol.Pending) != 0 {
					t.Fatal("written receipt was not reconciled")
				}
			} else {
				wantReceipt := "write_missing"
				if effect == "conflict" {
					wantReceipt = "sequence_conflict"
				}
				after = awaitProtocol(t, next, "closed reconciliation", func(s sessionhost.State) bool {
					return s.Records[0].State == "applied" || (s.Fence == "unknown" && len(s.Protocol.Pending) == 1 && s.Protocol.Pending[0].Receipt == wantReceipt)
				})
				if after.Records[0].State == "applied" {
					t.Fatal("missing or conflicting effect was replayed")
				}
				if len(after.Protocol.Pending) != 1 || after.Records[0].State != "unknown" {
					t.Fatal("uncertain/conflicting intent was retired")
				}
			}
			want := 4
			if effect == "missing" {
				want = 3
			}
			status := inspectRunner(t, cfg)
			if status.Writes != want || status.Instance != before.RunnerInstance || after.Fence != "unknown" {
				t.Fatalf("recovery wrote or opened input: writes=%d fence=%s", status.Writes, after.Fence)
			}
			if err := next.Fence(true); err == nil || err.Error() != "reconciliation_required" {
				t.Fatalf("uncertain fence released: %v", err)
			}
			snapshot := next.Snapshot()
			snapshot.Protocol.Checkpoint[0] = '!'
			snapshot.Protocol.Pending = nil
			if !json.Valid(next.Snapshot().Protocol.Checkpoint) {
				t.Fatal("snapshot aliases private checkpoint")
			}
		})
	}
}
