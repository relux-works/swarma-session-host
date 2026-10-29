package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type nativeCodexFixture struct {
	mu           sync.Mutex
	frames       []codexRPCRequest
	changed      chan struct{}
	mode         string
	runner       *codexRunner
	startWritten chan struct{}
}

func newNativeCodexFixture(t *testing.T, mode string) (context.Context, *nativeCodexFixture, *sessionInput, ControllerLease) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	f := &nativeCodexFixture{mode: mode, changed: make(chan struct{}), startWritten: make(chan struct{}, 1)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		for {
			var req codexRPCRequest
			if err := wsjson.Read(ctx, c, &req); err != nil {
				return
			}
			f.mu.Lock()
			f.frames = append(f.frames, req)
			close(f.changed)
			f.changed = make(chan struct{})
			f.mu.Unlock()
			if req.Method == "initialized" {
				continue
			}
			result := any(map[string]any{})
			var failure *codexRPCError
			thread := map[string]any{"id": "native-thread", "status": map[string]any{"type": "idle"}, "turns": []any{}}
			switch req.Method {
			case "initialize":
			case "thread/resume":
				if mode == "foreign-thread" {
					thread["id"] = "foreign"
				}
				if mode == "active" || mode == "foreign-steer" || mode == "ambiguous" {
					thread["status"] = map[string]any{"type": "active"}
				}
				if strings.HasPrefix(mode, "no-rollout") || mode == "wrong-rollout" {
					message := "no rollout found for thread id native-thread"
					if mode == "wrong-rollout" {
						message += " suffix"
					}
					failure = &codexRPCError{Code: -32600, Message: message}
				}
				result = map[string]any{"thread": thread}
			case "thread/turns/list":
				turns := []any{}
				if mode == "active" || mode == "foreign-steer" || mode == "ambiguous" || mode == "idle-busy-list" {
					turns = append(turns, map[string]any{"id": "native-turn", "status": "inProgress"})
				}
				if mode == "ambiguous" {
					turns = append(turns, map[string]any{"id": "other-turn", "status": "inProgress"})
				}
				if strings.HasPrefix(mode, "no-rollout") {
					failure = &codexRPCError{Code: -32600, Message: "thread native-thread is not materialized yet; thread/turns/list is unavailable before first user message"}
				}
				result = map[string]any{"data": turns}
			case "turn/start":
				select {
				case f.startWritten <- struct{}{}:
				default:
				}
				if mode == "timeout" {
					<-ctx.Done()
					return
				}
				id := "native-turn"
				if mode == "missing-turn" {
					id = ""
				}
				result = map[string]any{"turn": map[string]any{"id": id, "status": "inProgress"}}
			case "turn/steer":
				id := "native-turn"
				if mode == "foreign-steer" {
					id = "foreign"
				}
				result = map[string]any{"turnId": id}
			case "thread/read":
				if mode == "owner-foreign" {
					thread["id"] = "foreign"
				}
				if mode == "complete" || mode == "owner-foreign" {
					thread["turns"] = []any{map[string]any{"id": "native-turn", "status": "completed"}}
				}
				if strings.HasPrefix(mode, "monitor") {
					thread["status"] = map[string]any{"type": "active", "activeFlags": []string{"waitingOnUserInput"}}
					thread["turns"] = []any{map[string]any{"id": "native-turn", "status": "inProgress"}}
					if mode == "monitor-approval" {
						thread["status"] = map[string]any{"type": "active", "activeFlags": []string{"waitingOnUserInput", "waitingOnApproval"}}
					}
					if mode == "monitor-foreign" {
						thread["id"] = "foreign"
					}
				}
				result = map[string]any{"thread": thread}
			case "thread/queue/add":
			default:
				t.Errorf("unexpected native method %s", req.Method)
				return
			}
			if mode == "no-rollout-observed-history" && req.Method == "thread/turns/list" {
				if wsjson.Write(ctx, c, map[string]any{"method": "turn/started", "params": map[string]any{"threadId": "native-thread", "turn": map[string]any{"id": "existing", "status": "inProgress"}}}) != nil {
					return
				}
			}
			reply := map[string]any{"id": req.ID, "result": result}
			if failure != nil {
				delete(reply, "result")
				reply["error"] = failure
			}
			if wsjson.Write(ctx, c, reply) != nil {
				return
			}
		}
	}))
	t.Cleanup(func() {
		cancel()
		if f.runner != nil {
			f.runner.closeObservation()
		}
		server.Close()
	})
	snapshot := CodexRunnerSnapshot{Contract: codexHostSnapshotContract, PID: os.Getpid(), URL: "ws" + strings.TrimPrefix(server.URL, "http"), ThreadID: "native-thread", ProjectDir: t.TempDir(), CodexHome: t.TempDir()}
	var err error
	f.runner, err = newCodexRunner(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	h, err := newSessionInput(ctx, "native-codex", 1, f.runner)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, f, h, ControllerLease{SessionID: "native-codex", Epoch: 1}
}
func (f *nativeCodexFixture) methods() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, r := range f.frames {
		out = append(out, r.Method)
	}
	return out
}
func (f *nativeCodexFixture) writeCount() int {
	n := 0
	for _, m := range f.methods() {
		if m == "turn/start" || m == "turn/steer" || m == "thread/queue/add" {
			n++
		}
	}
	return n
}

// Actual native RPC receipts bind the start/unique-active steer and command
// retries perform one physical write. This does not qualify fork goal waiters.
func TestCodexRunnerTurnInputRoutesAndReceipts(t *testing.T) {
	for _, mode := range []string{"idle", "active", "no-rollout"} {
		t.Run(mode, func(t *testing.T) {
			ctx, f, h, lease := newNativeCodexFixture(t, mode)
			input := codexHostSubmission(lease, "input", "command", "prepared assignment")
			r, err := h.Submit(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			out, err := h.Await(ctx, r)
			if err != nil || out.CodexTurn == nil || out.CodexTurn.ThreadID != "native-thread" || out.CodexTurn.TurnID != "native-turn" || out.CodexTurn.StartedNewTurn != (mode != "active") {
				t.Fatalf("native receipt %#v / %v", out, err)
			}
			input.ID = "retry"
			if got, err := h.Submit(ctx, input); err != nil || got != r || f.writeCount() != 1 {
				t.Fatalf("native duplicate wrote twice: %v / %v / %v", got, err, f.methods())
			}
			if mode == "no-rollout" {
				_, incomplete := f.runner.ConsumerNotifications()
				if !incomplete {
					t.Fatal("refused resume claimed complete evidence")
				}
			}
		})
	}
}

// Ordinary Text keeps the existing queue delivery path and cannot masquerade as
// the assignment's typed CodexTurn receipt, beside actual start/steer positives.
func TestCodexRunnerOrdinaryTextUsesOriginalRoute(t *testing.T) {
	ctx, f, h, lease := newNativeCodexFixture(t, "idle")
	r, err := h.Submit(ctx, textInput(lease, "ordinary", "ordinary text"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Await(ctx, r)
	if err != nil || out.CodexTurn != nil || out.Kind != "written" {
		t.Fatalf("ordinary receipt %#v / %v", out, err)
	}
	methods := strings.Join(f.methods(), ",")
	if strings.Contains(methods, "turn/start") || strings.Contains(methods, "thread/turns/list") || !strings.Contains(methods, "thread/queue/add") || f.writeCount() != 1 {
		t.Fatalf("ordinary Text took assignment route: %s", methods)
	}
}

// Missing/foreign/ambiguous identities and an inexact no-rollout verdict refuse
// without an acknowledgement; preflight refusals create no native write.
func TestCodexRunnerTurnRefusalsNeverAcknowledge(t *testing.T) {
	for _, mode := range []string{"foreign-thread", "ambiguous", "idle-busy-list", "wrong-rollout", "missing-turn", "foreign-steer"} {
		t.Run(mode, func(t *testing.T) {
			ctx, f, h, lease := newNativeCodexFixture(t, mode)
			r, err := h.Submit(ctx, codexHostSubmission(lease, "input", "command", "prepared assignment"))
			if err != nil {
				t.Fatal(err)
			}
			out, err := h.Await(ctx, r)
			if err == nil || out.CodexTurn != nil || out.Kind == "provider-ack" {
				t.Fatalf("native refusal acknowledged: %#v / %v", out, err)
			}
			want := 0
			if mode == "missing-turn" || mode == "foreign-steer" {
				want = 1
			}
			if f.writeCount() != want {
				t.Fatalf("refusal wrote %d, want %d: %v", f.writeCount(), want, f.methods())
			}
		})
	}
}

// The no-rollout escape requires a genuinely fresh native image and no observed
// same-thread lifecycle. Every saved-history fact independently forbids a start.
// The no-history/no-rollout positive is exercised by the routing table above.
func TestCodexRunnerNoRolloutCannotDiscardHistory(t *testing.T) {
	for _, fact := range []string{"managed", "completed", "inherited-turn", "fork", "base-sha", "prefix", "observed"} {
		t.Run(fact, func(t *testing.T) {
			mode := "no-rollout"
			if fact == "observed" {
				mode = "no-rollout-observed-history"
			}
			ctx, f, h, lease := newNativeCodexFixture(t, mode)
			f.runner.mu.Lock()
			s := &f.runner.snapshot
			switch fact {
			case "managed":
				s.ManagedTurnID = "old"
			case "completed":
				s.LastCompletedTurnID = "old"
			case "inherited-turn":
				s.InheritedLastTurnID = "old"
			case "fork":
				s.ForkedFromID = "base"
			case "base-sha":
				s.BaseRolloutSHA256 = "known"
			case "prefix":
				s.InheritedRollout = CodexRolloutPrefix{Bytes: 0, SHA256: "known-empty-prefix"}
			}
			f.runner.mu.Unlock()
			r, err := h.Submit(ctx, codexHostSubmission(lease, "input", "command", "prepared assignment"))
			if err != nil {
				t.Fatal(err)
			}
			out, err := h.Await(ctx, r)
			if err == nil || out.CodexTurn != nil || out.Kind == "provider-ack" || f.writeCount() != 0 {
				t.Fatalf("history discarded by native bootstrap: %#v / %v / %v", out, err, f.methods())
			}
			if _, incomplete := f.runner.ConsumerNotifications(); !incomplete && strings.Contains(strings.Join(f.methods(), ","), "thread/resume") {
				t.Fatal("refused resume with saved history claimed complete evidence")
			}
		})
	}
}

// Complete actual request frames carry only native command fields and prepared
// text. These transport fixtures do not yet prove canonical fork input exclusion.
func TestCodexRunnerTurnRequestBytesAreNative(t *testing.T) {
	ctx, f, h, lease := newNativeCodexFixture(t, "idle")
	r, err := h.Submit(ctx, codexHostSubmission(lease, "input", "command", "prepared assignment"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Await(ctx, r); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, frame := range f.frames {
		raw, err := json.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"OwnerRun", "PendingGoalDirective", "GoalTokenBudget", "PRIMARY-GOAL-", "BoardGoalOwner", "predicate", "token_budget"} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatalf("canonical content in complete native frame: %s", forbidden)
			}
		}
		if frame.Method == "turn/start" {
			params := frame.Params.(map[string]any)
			if len(params) != 4 || params["threadId"] != "native-thread" || params["cwd"] == nil || params["input"] == nil || params["runtimeWorkspaceRoots"] != nil {
				t.Fatalf("unexpected native start shape: %#v", params)
			}
		}
	}
}

// A native timeout retains an explicit failed receipt; neither a same-command
// retry nor a fresh command may create another write from an uncertain outcome.
func TestCodexRunnerUnknownReceiptFencesFreshWrites(t *testing.T) {
	ctx, f, h, lease := newNativeCodexFixture(t, "timeout")
	f.runner.ackTimeout = 20 * time.Millisecond
	input := codexHostSubmission(lease, "input", "command", "prepared assignment")
	r, err := h.Submit(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Await(ctx, r)
	if !errors.Is(err, context.DeadlineExceeded) || out.Kind != "timeout" || out.CodexTurn != nil {
		t.Fatalf("unknown receipt %#v / %v", out, err)
	}
	input.ID = "retry"
	if got, err := h.Submit(ctx, input); err != nil || got != r {
		t.Fatalf("same command did not retain receipt: %v / %v", got, err)
	}
	fresh, err := h.Submit(ctx, codexHostSubmission(lease, "fresh", "new-command", "new prepared assignment"))
	if err != nil {
		t.Fatal(err)
	}
	if out, err := h.Await(ctx, fresh); err == nil || out.CodexTurn != nil || out.Kind == "provider-ack" {
		t.Fatalf("unknown result retried with fresh command: %#v / %v", out, err)
	}
	ordinary, err := h.Submit(ctx, textInput(lease, "ordinary-after-timeout", "must not write"))
	if err != nil {
		t.Fatal(err)
	}
	if out, err := h.Await(ctx, ordinary); !errors.Is(err, ErrInputAcknowledgementMismatch) || out.Kind == "written" {
		t.Fatalf("ordinary text bypassed uncertain assignment: %#v / %v", out, err)
	}
	if f.writeCount() != 1 {
		t.Fatalf("unknown outcome caused another native write: %v", f.methods())
	}
}

// The native boundary rejects mismatched caller receipt identity before dialing;
// the positive host-routed cases above supply the exact lease and input ID.
func TestCodexRunnerRejectsForeignInputReceipt(t *testing.T) {
	ctx, f, _, lease := newNativeCodexFixture(t, "idle")
	input := codexHostSubmission(lease, "input", "command", "prepared assignment")
	for _, receipt := range []InputReceipt{
		{SessionID: "foreign", Epoch: lease.Epoch, ID: input.ID},
		{SessionID: lease.SessionID, Epoch: lease.Epoch + 1, ID: input.ID},
		{SessionID: lease.SessionID, Epoch: lease.Epoch, ID: "foreign"},
	} {
		if _, err := f.runner.ExecuteInput(ctx, input, receipt); !errors.Is(err, ErrInputAcknowledgementMismatch) {
			t.Fatalf("foreign receipt admitted: %#v / %v", receipt, err)
		}
	}
	if len(f.methods()) != 0 {
		t.Fatalf("foreign receipt performed native RPC: %v", f.methods())
	}
}

// The native command cache independently preserves the first exact receipt on
// retries, even when called below the host cache. Changed text cannot join it.
func TestCodexRunnerCommandRetriesRetainNativeReceipt(t *testing.T) {
	ctx, f, _, lease := newNativeCodexFixture(t, "idle")
	input := codexHostSubmission(lease, "input", "command", "prepared assignment")
	receipt := InputReceipt{SessionID: lease.SessionID, Epoch: lease.Epoch, ID: input.ID}
	observe, err := f.runner.ExecuteInput(ctx, input, receipt)
	if err != nil {
		t.Fatal(err)
	}
	first, err := observe(ctx)
	if err != nil || first.CodexTurn == nil {
		t.Fatalf("first native receipt: %#v / %v", first, err)
	}
	input.ID = "retry"
	observe, err = f.runner.ExecuteInput(ctx, input, InputReceipt{SessionID: lease.SessionID, Epoch: lease.Epoch, ID: input.ID})
	if err != nil {
		t.Fatal(err)
	}
	second, err := observe(ctx)
	if err != nil || second.Receipt != first.Receipt || second.CodexTurn == nil || *second.CodexTurn != *first.CodexTurn || f.writeCount() != 1 {
		t.Fatalf("native command retry lost original receipt: %#v / %v / %v", second, err, f.methods())
	}
	input.Payload.CodexTurn.Start.Text = "changed"
	if _, err := f.runner.ExecuteInput(ctx, input, InputReceipt{SessionID: lease.SessionID, Epoch: lease.Epoch, ID: input.ID}); !errors.Is(err, ErrInputIDConflict) {
		t.Fatalf("changed native command joined: %v", err)
	}
	if f.writeCount() != 1 {
		t.Fatalf("changed native command wrote: %v", f.methods())
	}
}

// Joining the acknowledged assignment observes its completion on the retained
// connection. It never starts/steers a second assignment or consumes the prompt.
func TestCodexRunnerOwnerJoinsAcceptedAssignment(t *testing.T) {
	ctx, f, h, lease := newNativeCodexFixture(t, "complete")
	if _, err := f.runner.RunOwner(ctx, ProviderRunRequest{Prompt: "before receipt"}); !errors.Is(err, ErrInputAcknowledgementMismatch) {
		t.Fatalf("owner without assignment receipt: %v", err)
	}
	r, err := h.Submit(ctx, codexHostSubmission(lease, "input", "command", "prepared assignment"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Await(ctx, r); err != nil {
		t.Fatal(err)
	}
	result, err := f.runner.RunOwner(ctx, ProviderRunRequest{Prompt: "must not submit again"})
	if err != nil || result == nil || result.Status != ProviderRunComplete || f.writeCount() != 1 {
		t.Fatalf("join submitted or lost assignment: %+v / %v / %v", result, err, f.methods())
	}
	s, err := f.runner.Snapshot()
	if err != nil || s.Codex.ManagedTurnID != "" || s.Codex.LastCompletedTurnID != "native-turn" {
		t.Fatalf("completed native identity not retained: %+v / %v", s, err)
	}
	if _, err := f.runner.RunOwner(ctx, ProviderRunRequest{Prompt: "no new receipt"}); !errors.Is(err, ErrInputAcknowledgementMismatch) {
		t.Fatalf("completed owner started a successor: %v", err)
	}
}

// A recovered managed writer is joined, never re-submitted under a new command.
// A foreign thread/read containing the same turn ID cannot complete its owner.
func TestCodexRunnerOwnerRejectsForeignReadAndDuplicateAssignment(t *testing.T) {
	t.Run("already managed", func(t *testing.T) {
		ctx, f, h, lease := newNativeCodexFixture(t, "active")
		f.runner.snapshot.ManagedTurnID = "native-turn"
		r, err := h.Submit(ctx, codexHostSubmission(lease, "input", "command", "must not submit again"))
		if err != nil {
			t.Fatal(err)
		}
		if out, err := h.Await(ctx, r); !errors.Is(err, ErrInputAcknowledgementMismatch) || out.CodexTurn != nil || f.writeCount() != 0 {
			t.Fatalf("recovered writer received another assignment: %#v / %v / %v", out, err, f.methods())
		}
	})
	t.Run("foreign read", func(t *testing.T) {
		ctx, f, h, lease := newNativeCodexFixture(t, "owner-foreign")
		r, err := h.Submit(ctx, codexHostSubmission(lease, "input", "command", "prepared assignment"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Await(ctx, r); err != nil {
			t.Fatal(err)
		}
		result, err := f.runner.RunOwner(ctx, ProviderRunRequest{Prompt: "join only"})
		if !errors.Is(err, ErrInputAcknowledgementMismatch) || result != nil || f.writeCount() != 1 {
			t.Fatalf("foreign read completed native owner: %+v / %v / %v", result, err, f.methods())
		}
	})
}

// Native auto-continue preserves the user-input/approval rule and refuses a
// foreign thread or an uncertain previous write before it can steer.
func TestCodexRunnerAutoContinueNativeIdentity(t *testing.T) {
	for _, mode := range []string{"monitor", "monitor-approval", "monitor-foreign", "monitor-uncertain"} {
		t.Run(mode, func(t *testing.T) {
			ctx, f, _, _ := newNativeCodexFixture(t, mode)
			f.runner.uncertain = mode == "monitor-uncertain"
			now := time.Now()
			state := &codexAutoContinueState{turnID: "native-turn", waitingSince: now.Add(-2 * time.Second)}
			err := f.runner.pollAutoContinue(ctx, CodexAutoContinueOptions{DelaySeconds: 1, MaxNudgesPerTurn: 2, Prompt: "Continue."}, state, now)
			want := 0
			if mode == "monitor" {
				want = 1
			}
			if mode == "monitor-foreign" || mode == "monitor-uncertain" {
				if !errors.Is(err, ErrInputAcknowledgementMismatch) {
					t.Fatalf("unsafe auto-continue lacks typed refusal: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if f.writeCount() != want {
				t.Fatalf("native monitor writes %d want %d: %v", f.writeCount(), want, f.methods())
			}
		})
	}
}
