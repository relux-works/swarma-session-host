package sessionhost_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
			if mode == "native-gated" {
				deadline := time.Now().Add(10 * time.Second)
				for {
					if _, err := os.Stat("allow-thread-start"); err == nil {
						break
					}
					if time.Now().After(deadline) {
						os.Exit(7)
					}
					time.Sleep(time.Millisecond)
				}
			}
			result := map[string]any{"thread": map[string]any{"id": "synthetic-thread"}, "model": r.Params["model"], "modelProvider": r.Params["modelProvider"]}
			if r.Params["approvalPolicy"] != nil {
				result["approvalPolicy"] = r.Params["approvalPolicy"]
				result["cwd"] = r.Params["cwd"]
				result["sandbox"] = map[string]string{"type": "dangerFullAccess"}
			}
			write(map[string]any{"id": r.ID, "result": result})
			if mode == "eof-live" {
				deadline := time.Now().Add(10 * time.Second)
				for {
					if _, err := os.Stat("close-output"); err == nil {
						break
					}
					if time.Now().After(deadline) {
						os.Exit(7)
					}
					time.Sleep(time.Millisecond)
				}
				os.Stdout.Close()
				if os.WriteFile("output-closed", nil, 0600) != nil {
					os.Exit(8)
				}
				io.Copy(io.Discard, os.Stdin)
				// Cancellation now closes the runner's stdin writer. Remain
				// alive until the test has separately observed output EOF.
				for {
					if _, err := os.Stat("exit-after-output"); err == nil {
						break
					}
					if time.Now().After(deadline) {
						os.Exit(7)
					}
					time.Sleep(time.Millisecond)
				}
				os.Exit(0)
			}
			if mode == "closed-stdin" {
				os.Stdin.Close()
				if os.WriteFile("input-closed", nil, 0600) != nil {
					os.Exit(8)
				}
				time.Sleep(30 * time.Second)
				os.Exit(0)
			}
		case "turn/start":
			turn := fmt.Sprintf("turn-%d", r.ID)
			write(map[string]any{"id": r.ID, "result": map[string]any{"turn": map[string]any{"id": turn, "status": "inProgress"}}})
			if mode == "unsupported-work-item" {
				write(map[string]any{"method": "item/started", "params": map[string]any{"threadId": "synthetic-thread", "turnId": turn, "item": map[string]any{"id": "item-a", "type": "futureTool", "output": "private-refused-tool-canary"}}})
			}
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
	return setupProtocolNoticeMode(t, mode, sessionhost.SyntheticNotice)
}

func setupProtocolNoticeMode(t *testing.T, mode, noticeMode string) (sessionhost.Config, *runner.Server, string) {
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
		NoticeMode:   noticeMode,
		Selection:    sessionhost.Selection{Vendor: "unfamiliar-provider", Model: "unfamiliar-model", Reasoning: "unfamiliar-effort"},
		ProtocolName: "codex", PollInterval: 10 * time.Millisecond, IOTimeout: time.Second}
	cfg.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
		opts := codexadapter.DriverOptions{NoticeMode: noticeMode}
		if noticeMode == sessionhost.WorkReference {
			opts.PermissionPolicy = codexadapter.YoloPolicy
		}
		return codexadapter.OpenDriverWithOptions(ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create, Selection: ap.Selection{Provider: cfg.Selection.Vendor, Model: cfg.Selection.Model, Effort: cfg.Selection.Reasoning}, Workspace: dir}, opts, saved)
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
	for _, mode := range []string{sessionhost.SyntheticNotice, sessionhost.WorkReference} {
		t.Run(mode, func(t *testing.T) { protocolHostAcceptanceAndReplacement(t, mode) })
	}
}

func protocolHostAcceptanceAndReplacement(t *testing.T, mode string) {
	cfg, _, dir := setupProtocolNoticeMode(t, "gated", mode)
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
	for _, mode := range []string{sessionhost.SyntheticNotice, sessionhost.WorkReference} {
		t.Run(mode, func(t *testing.T) { protocolApprovalNeverAnswered(t, mode) })
	}
}

func protocolApprovalNeverAnswered(t *testing.T, mode string) {
	cfg, _, _ := setupProtocolNoticeMode(t, "approval", mode)
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
// matching native completion; missing/unknown/conflicting effects never resend.
// Additional snapshots cover pre-Prepare intent and receipt retirement before
// native catch-up; only the written controls resolve, without another write.
func TestProtocolOutboxRecoveryNeverResends(t *testing.T) {
	for _, mode := range []string{sessionhost.SyntheticNotice, sessionhost.WorkReference} {
		t.Run(mode, func(t *testing.T) { protocolOutboxRecoveryNeverResends(t, mode) })
	}
}

func protocolOutboxRecoveryNeverResends(t *testing.T, noticeMode string) {
	for _, effect := range []string{"written", "missing", "conflict", "unknown", "intent", "retired"} {
		t.Run(effect, func(t *testing.T) {
			mode := "normal"
			if effect == "unknown" {
				mode = "closed-stdin"
			}
			cfg, _, dir := setupProtocolNoticeMode(t, mode, noticeMode)
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
			if noticeMode == sessionhost.WorkReference {
				encoded, _ = json.Marshal(struct {
					Mode      string
					Notice    sessionhost.Notice
					Selection sessionhost.Selection
				}{sessionhost.WorkReference, notice, cfg.Selection})
			}
			noticeHash := sha256.Sum256(encoded)
			before.Records = []sessionhost.Record{{Notice: notice, Digest: hex.EncodeToString(noticeHash[:]), Sequence: 1, State: "unknown"}}
			originalCheckpoint, originalView := before.Protocol.Checkpoint, before.Protocol.View
			before.Protocol.Checkpoint = transition.Checkpoint
			before.Protocol.View = transition.View
			before.Protocol.WireSequence = 4
			before.Protocol.Pending = []sessionhost.WireAction{{Sequence: 4, Key: action.Key, InputID: action.InputID, Data: action.Bytes, Digest: hex.EncodeToString(hash[:])}}
			before.InputState = sessionhost.InputBusy
			if effect == "intent" {
				before.Protocol.Checkpoint, before.Protocol.View = originalCheckpoint, originalView
				before.Protocol.WireSequence, before.Protocol.Pending = 3, nil
				before.InputState = sessionhost.InputIdle
			}
			if effect != "missing" && effect != "intent" {
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
				if effect == "unknown" {
					deadline := time.Now().Add(time.Second)
					for {
						if _, err := os.Stat(filepath.Join(dir, "input-closed")); err == nil {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("fixture stdin not closed")
						}
						time.Sleep(time.Millisecond)
					}
				}
				written, err := client.Call(ctx, runner.Request{Op: "write", Execution: cfg.Execution, Epoch: before.Epoch, Sequence: 4, Data: data})
				wantCode := "written"
				if effect == "unknown" {
					wantCode = "write_unknown"
				}
				if err != nil || written.Code != wantCode {
					t.Fatalf("fixture write %s %v", written.Code, err)
				}
				_, err = client.Call(ctx, runner.Request{Op: "release", Execution: cfg.Execution, Epoch: before.Epoch})
				client.Close()
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}
			if effect == "retired" {
				before.Protocol.Pending = nil
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
			if effect == "written" || effect == "retired" {
				after = await(t, next, "one", "applied")
				if !after.Records[0].NativeAccepted || len(after.Protocol.Pending) != 0 {
					t.Fatal("written receipt was not reconciled")
				}
			} else if effect == "intent" {
				after = awaitProtocol(t, next, "pre-Prepare intent closed", func(s sessionhost.State) bool { return s.Fence == "unknown" && s.InputState == sessionhost.InputIdle })
				time.Sleep(30 * time.Millisecond)
				after = next.Snapshot()
				if after.Records[0].State != "unknown" || len(after.Protocol.Pending) != 0 {
					t.Fatal("pre-Prepare intent was submitted")
				}
			} else {
				wantReceipt := "write_missing"
				if effect == "unknown" {
					wantReceipt = "write_unknown"
				}
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
			if effect == "missing" || effect == "intent" {
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

// A live child closing only stdout must permanently close automatic delivery,
// including host replacement. The ordinary live-output variant delivers once.
func TestProtocolLiveOutputTermination(t *testing.T) {
	for _, mode := range []string{sessionhost.SyntheticNotice, sessionhost.WorkReference} {
		t.Run(mode, func(t *testing.T) { protocolLiveOutputTermination(t, mode) })
	}
}

func protocolLiveOutputTermination(t *testing.T, noticeMode string) {
	for _, ended := range []bool{false, true} {
		t.Run(fmt.Sprint(ended), func(t *testing.T) {
			mode := "normal"
			if ended {
				mode = "eof-live"
			}
			cfg, _, dir := setupProtocolNoticeMode(t, mode, noticeMode)
			h := openHost(t, cfg)
			awaitProtocol(t, h, "initial idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
			initial := inspectRunner(t, cfg)
			if ended {
				if err := os.WriteFile(filepath.Join(dir, "close-output"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				awaitProtocol(t, h, "EOF closes fence", func(s sessionhost.State) bool {
					return s.Fence == "unknown" && s.InputState == sessionhost.InputUnknown
				})
				if st := inspectRunner(t, cfg); st.Exited || !st.OutputEnded {
					t.Fatalf("EOF confused with process exit: %+v", st)
				}
			}
			if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "synthetic/one"}); err != nil {
				t.Fatal(err)
			}
			if !ended {
				await(t, h, "one", "applied")
			}
			h.Close()
			next := openHost(t, cfg)
			if ended {
				awaitProtocol(t, next, "restored EOF fence", func(s sessionhost.State) bool {
					return s.Fence == "unknown" && s.InputState == sessionhost.InputUnknown
				})
				if err := next.Fence(true); err == nil || err.Error() != "reconciliation_required" {
					t.Fatalf("EOF released: %v", err)
				}
				if _, err := next.Enqueue(sessionhost.Notice{ID: "two", Reference: "synthetic/two"}); err != nil {
					t.Fatal(err)
				}
				time.Sleep(50 * time.Millisecond)
				if s := next.Snapshot(); s.Records[0].State != "queued" || s.Records[1].State != "queued" {
					t.Fatal("EOF dispatched queued input")
				}
			} else {
				awaitProtocol(t, next, "live restored idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
				if _, err := next.Enqueue(sessionhost.Notice{ID: "two", Reference: "synthetic/two"}); err != nil {
					t.Fatal(err)
				}
				await(t, next, "two", "applied")
			}
			after := inspectRunner(t, cfg)
			want := initial.Writes
			if !ended {
				want += 2
			}
			if after.Writes != want || after.PID != initial.PID || after.Instance != initial.Instance || after.Exited {
				t.Fatalf("unexpected post-replacement effects: %+v", after)
			}
			if ended {
				if err := os.WriteFile(filepath.Join(dir, "exit-after-output"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
