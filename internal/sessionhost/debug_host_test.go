package sessionhost

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

// debugProtocolConfig mirrors the external protocol fixture setup for in-package
// tests: the synthetic app-server child from this test binary behind the real
// runner, with the real Codex driver and synthetic notice mode.
func debugProtocolConfig(t *testing.T, mode string) (Config, string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "debug-host-")
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
	cfg := Config{StateDir: filepath.Join(dir, "state"), Socket: rc.Socket, Execution: rc.Execution,
		Selection:    Selection{Vendor: "unfamiliar-provider", Model: "unfamiliar-model", Reasoning: "unfamiliar-effort"},
		ProtocolName: "codex", PollInterval: 10 * time.Millisecond, IOTimeout: time.Second}
	cfg.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
		return codexadapter.OpenDriver(ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create, Selection: ap.Selection{Provider: cfg.Selection.Vendor, Model: cfg.Selection.Model, Effort: cfg.Selection.Reasoning}, Workspace: dir}, saved)
	}
	return cfg, dir
}
func debugOpenHost(t *testing.T, cfg Config) *Host {
	t.Helper()
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}
func debugAwait(t *testing.T, h *Host, claim string, ready func(State) bool) State {
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
	return State{}
}
func debugRunnerStatus(t *testing.T, cfg Config) runner.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := runner.Dial(ctx, cfg.Socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	status, err := client.Call(ctx, runner.Request{Op: "status", Execution: cfg.Execution})
	if err != nil || status.Code != "ok" {
		t.Fatalf("runner status: %+v %v", status, err)
	}
	return status
}

// debugNotifierFixture captures content-free notifier intents under its own lock.
type debugNotifierFixture struct {
	mu   sync.Mutex
	seen []debugNotification
}

func (n *debugNotifierFixture) accept(d debugNotification) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seen = append(n.seen, d)
}
func (n *debugNotifierFixture) all() []debugNotification {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]debugNotification(nil), n.seen...)
}
func debugInstallNotifier(h *Host, n *debugNotifierFixture) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.debugNotify = n.accept
}

// debugResumeWhenEligible retries the explicit resume while replacement
// recovery is still settling; it never bypasses the eligibility predicate.
func debugResumeWhenEligible(t *testing.T, h *Host, token string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := h.debugControl(token, "resume_automation", h.Snapshot().Debug.WriterRevision)
		if err == nil {
			return
		}
		if err.Error() != "input_fenced" || time.Now().After(deadline) {
			t.Fatalf("resume: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func debugHostGrant(t *testing.T, h *Host, rights ...string) string {
	t.Helper()
	epoch := h.Snapshot().Epoch
	token, err := h.debugIssueGrant(debugAuthority{"manager", "manager", epoch}, rights)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.debugAttachView(token); err != nil {
		t.Fatal(err)
	}
	return token
}

// TerminalFactBlocksQueuedNotice (actual host dispatch) and the state/signal
// portion of TerminalNotificationAndTailExpiry: with writer automatic and no
// claim, the verified terminal turn commits its fact, block and notification
// intent before the queued second notice can reserve; repeated dispatch, ack
// only, detach, tail expiry and host replacement all leave zero further writes
// while fact, debt, ack and original observation age survive; an explicit
// current permit plus eligible resume delivers the queued notice exactly once;
// the stale duplicate control is V11 with no extra write. Detach leaves the
// owned child alive. Healthy-sink timing/escalation is E, not this fixture.
func TestTerminalFactBlocksQueuedNoticeHostDispatch(t *testing.T) {
	cfg, dir := debugProtocolConfig(t, "gated")
	on := true
	cfg.DebugAttach = &on
	h := debugOpenHost(t, cfg)
	debugWant(t, h.debugRegister(debugRegistry{"manager", "owner"}), "")
	notifier := &debugNotifierFixture{}
	debugInstallNotifier(h, notifier)
	debugAwait(t, h, "initial idle", func(s State) bool { return s.InputState == InputIdle })
	for _, id := range []string{"one", "two"} {
		if _, err := h.Enqueue(Notice{ID: id, Reference: "synthetic/" + id}); err != nil {
			t.Fatal(err)
		}
	}
	debugAwait(t, h, "first accepted", func(s State) bool { return len(s.Records) == 2 && s.Records[0].NativeAccepted })
	if n := notifier.all(); len(n) != 0 {
		t.Fatal("notification before any terminal fact")
	}
	if err := os.WriteFile(filepath.Join(dir, "complete-turn-3"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	blocked := debugAwait(t, h, "terminal fact", func(s State) bool {
		return s.Records[0].State == "applied" && len(s.Debug.Results) == 1 && s.InputState == InputIdle
	})
	// Ordinary status carries the fact's class and flags only; the private
	// custody route keeps its identity, native correlation and observation time.
	public := blocked.Debug.Results[0]
	if public.Permitted || !public.NotificationPending || public.Input != "" || public.ID != "" || public.Thread != "" || public.Turn != "" || !public.ObservedAt.IsZero() || public.Revision != 0 || public.Outcome != "completed" {
		t.Fatalf("public fact %+v", public)
	}
	fact := debugState(h).Debug.Results[0]
	if fact.Input != "one" || fact.Turn != "turn-3" || fact.Thread != "synthetic-thread" || fact.ObservedAt.IsZero() || fact.ID == "" || fact.Revision != 1 {
		t.Fatalf("private fact lost its correlation: %+v", fact)
	}
	intents := notifier.all()
	if len(intents) != 1 || intents[0].Kind != "terminal_observed" || intents[0].Result != fact.ID || !intents[0].ObservedAt.Equal(fact.ObservedAt) || intents[0].Outcome != "completed" {
		t.Fatalf("initial content-free intent: %+v", intents)
	}
	if encoded, _ := json.Marshal(intents[0]); bytes.Contains(encoded, []byte("synthetic/")) {
		t.Fatal("notification carried notice content")
	}
	for range 3 {
		h.wake()
	}
	time.Sleep(50 * time.Millisecond)
	if status := debugRunnerStatus(t, cfg); status.Writes != 4 || h.Snapshot().Records[1].State != "queued" {
		t.Fatalf("queued notice overtook the terminal block: writes=%d", status.Writes)
	}
	viewer := debugHostGrant(t, h, "view")
	control, err := h.debugControlView(viewer)
	debugWant(t, err, "")
	if len(control.Results) != 1 || control.Results[0].ID != fact.ID || control.Results[0].Turn != "turn-3" {
		t.Fatalf("control view lacks the exact fact for disposition: %+v", control.Results)
	}
	debugWant(t, h.debugDisposition(viewer, control.Results[0].ID, control.Results[0].Revision, true, false), "")
	time.Sleep(50 * time.Millisecond)
	acked := h.Snapshot().Debug.Results[0]
	if !acked.Ack || acked.Permitted || acked.NotificationPending || debugRunnerStatus(t, cfg).Writes != 4 {
		t.Fatal("acknowledgement permitted delivery or lost the debt record")
	}
	debugWant(t, h.debugControl(viewer, "detach", 0), "")
	h.mu.Lock()
	tail := h.debug.tail
	h.mu.Unlock()
	tail.mu.Lock()
	tail.now = func() time.Time { return time.Now().Add(debugTailAge) }
	tail.mu.Unlock()
	tail.expire()
	time.Sleep(50 * time.Millisecond)
	if status := debugRunnerStatus(t, cfg); status.Writes != 4 || status.Exited {
		t.Fatal("detach or tail expiry released the block or stopped the child")
	}
	h.Close()
	cfg.DebugAttach = nil
	next := debugOpenHost(t, cfg)
	debugWant(t, next.debugRegister(debugRegistry{"manager", "owner"}), "")
	replacementNotifier := &debugNotifierFixture{}
	debugInstallNotifier(next, replacementNotifier)
	restored := debugAwait(t, next, "restored idle", func(s State) bool { return s.InputState == InputIdle && len(s.Protocol.Pending) == 0 })
	if len(restored.Debug.Results) != 1 || !restored.Debug.Results[0].Ack || restored.Debug.Results[0].Permitted || restored.Debug.Writer != "held" || restored.Debug.Results[0].Turn != "" {
		t.Fatalf("replacement public fact: %+v", restored.Debug)
	}
	survivor := debugState(next).Debug.Results[0]
	if !survivor.Ack || survivor.Permitted || !survivor.ObservedAt.Equal(fact.ObservedAt) || survivor.Revision != fact.Revision+1 || survivor.ID != fact.ID {
		t.Fatalf("replacement lost fact/ack/age: %+v", survivor)
	}
	time.Sleep(50 * time.Millisecond)
	if debugRunnerStatus(t, cfg).Writes != 4 {
		t.Fatal("replacement reserved the blocked notice")
	}
	if err := os.WriteFile(filepath.Join(dir, "complete-turn-4"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	writer := debugHostGrant(t, next, "view", "writer_control")
	debugWant(t, next.debugDisposition(writer, survivor.ID, survivor.Revision, false, true), "")
	debugResumeWhenEligible(t, next, writer)
	delivered := debugAwait(t, next, "permitted delivery", func(s State) bool { return s.Records[1].State == "applied" })
	if debugRunnerStatus(t, cfg).Writes != 5 || len(delivered.Debug.Results) != 2 || delivered.Debug.Results[1].Permitted {
		t.Fatal("permitted notice did not deliver exactly once with its own new blocking fact")
	}
	debugWant(t, next.debugDisposition(writer, survivor.ID, survivor.Revision, false, true), "input_observation_stale")
	time.Sleep(50 * time.Millisecond)
	if debugRunnerStatus(t, cfg).Writes != 5 {
		t.Fatal("stale duplicate control created an extra write")
	}
	if intents := replacementNotifier.all(); len(intents) < 1 || intents[len(intents)-1].Result != debugState(next).Debug.Results[1].ID {
		t.Fatalf("second fact not announced: %+v", intents)
	}
}

// AdmissionResumeImmutability / DetachModeCompatibility (host): a legacy
// disabled execution creates no private directory and no debug member while
// behaving as before; an enabled detached execution captures the handshake
// frames into bounded custody with no viewer and refuses a later attempt to
// flip the bit on replacement.
func TestDisabledExecutionHasNoDebugCustody(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "enabled-detached"}[enabled], func(t *testing.T) {
			cfg, _ := debugProtocolConfig(t, "normal")
			if enabled {
				cfg.DebugAttach = &enabled
			}
			h := debugOpenHost(t, cfg)
			debugAwait(t, h, "idle", func(s State) bool { return s.InputState == InputIdle })
			if _, err := h.Enqueue(Notice{ID: "one", Reference: "synthetic/one"}); err != nil {
				t.Fatal(err)
			}
			debugAwait(t, h, "applied", func(s State) bool { return len(s.Records) == 1 && s.Records[0].State == "applied" })
			disk, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			_, custodyErr := os.Lstat(filepath.Join(cfg.StateDir, "debug-attach"))
			if !enabled {
				if bytes.Contains(disk, []byte(`"debug`)) || !os.IsNotExist(custodyErr) || h.debug != nil {
					t.Fatal("disabled execution created debug members or custody")
				}
				return
			}
			if custodyErr != nil || !bytes.Contains(disk, []byte(`"debug_attach": true`)) {
				t.Fatal("enabled execution lacks private custody")
			}
			h.mu.Lock()
			captured := h.debug.tail.frontier()
			h.mu.Unlock()
			if captured == 0 {
				t.Fatal("detached enabled execution did not capture from spawn")
			}
			h.Close()
			off := false
			cfg.DebugAttach = &off
			if replacement, err := Open(cfg); err == nil || err.Error() != "attach_admission_conflict" {
				if replacement != nil {
					replacement.Close()
				}
				t.Fatalf("bit flip on replacement: %v", err)
			}
		})
	}
}

// orderAdapter is a minimal non-protocol adapter for the barrier runner below.
type orderAdapter struct{}

func (orderAdapter) Name() string           { return "order-fixture" }
func (orderAdapter) InputState() InputState { return InputIdle }
func (orderAdapter) Encode(n Notice, s Selection) ([]byte, error) {
	return []byte("notice:" + n.ID + "\n"), nil
}
func (orderAdapter) Applied(output []byte, n Notice, s Selection) (bool, error) {
	return bytes.Contains(output, []byte("applied:"+n.ID+"\n")), nil
}

// barrierRunner is an owned synthetic runner endpoint whose write reply waits
// at a deterministic barrier. It records every write frame to prove whole-frame
// ordering and never spawns a process.
type barrierRunner struct {
	mu       sync.Mutex
	writes   [][]byte
	output   []byte
	started  chan struct{}
	release  chan struct{}
	stops    int
	instance string
}

func debugBarrierRunner(t *testing.T, socket string) *barrierRunner {
	t.Helper()
	listener, err := localipc.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	r := &barrierRunner{started: make(chan struct{}, 8), release: make(chan struct{}), instance: strings.Repeat("c", 32)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.AcceptUnix()
			if err != nil {
				return
			}
			go r.serve(conn)
		}
	}()
	t.Cleanup(func() { listener.Close(); <-done })
	return r
}
func (r *barrierRunner) serve(conn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
}) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var epoch uint64
	for {
		var req runner.Request
		if localipc.Read(reader, &req) != nil {
			return
		}
		resp := runner.Response{Code: "ok", Instance: r.instance, PID: 1, ProtocolVersion: runner.ProtocolVersion, WriteReceiptVersion: runner.WriteReceiptVersion}
		r.mu.Lock()
		switch req.Op {
		case "claim":
			epoch = req.Epoch
		case "write":
			r.writes = append(r.writes, bytes.Clone(req.Data))
			r.mu.Unlock()
			select {
			case r.started <- struct{}{}:
			default:
			}
			<-r.release
			r.mu.Lock()
			resp.Code = "written"
			r.output = append(r.output, []byte("applied:"+strings.TrimSuffix(strings.TrimPrefix(string(req.Data), "notice:"), "\n")+"\n")...)
		case "read":
			if req.Offset <= len(r.output) {
				resp.Data = bytes.Clone(r.output[req.Offset:])
			}
		case "stop":
			r.stops++
			resp.Code, resp.Exited = "stopped", true
		case "release":
			resp.Code = "released"
		}
		resp.Epoch = epoch
		resp.Writes = len(r.writes)
		resp.Offset = len(r.output)
		resp.AtTail = true
		r.mu.Unlock()
		if localipc.Write(conn, resp) != nil {
			return
		}
	}
}
func (r *barrierRunner) count() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.writes), r.stops
}
func debugBarrierHost(t *testing.T) (*Host, *barrierRunner, *inertEffector) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "debug-order-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	r := debugBarrierRunner(t, filepath.Join(dir, "r.sock"))
	on := true
	cfg := Config{DebugAttach: &on, StateDir: filepath.Join(dir, "state"), Socket: filepath.Join(dir, "r.sock"), Execution: "order-execution", Adapter: orderAdapter{}, IOTimeout: 5 * time.Second, Selection: Selection{Vendor: "provider", Model: "model", Reasoning: "effort"}}
	h := debugOpenHost(t, cfg)
	debugWant(t, h.debugRegister(debugRegistry{"manager", "owner"}), "")
	e := &inertEffector{}
	h.mu.Lock()
	h.debugEffects = e
	h.mu.Unlock()
	return h, r, e
}

// ClaimVsNoticeBothOrders: with the notice reservation first, the in-flight
// notice settles as exactly one whole-frame write while a claim proceeds
// without waiting on the blocked I/O and text is refused until eligible; with
// the claim first, the queued notice reserves nothing, exactly one fresh
// authorized text input is accepted, and yield-and-resume then delivers the
// notice once. Stop and revoke on either side of the issue marker leave at most
// that one effect and never resend.
func TestClaimVsNoticeBothOrdersHost(t *testing.T) {
	t.Run("notice-first", func(t *testing.T) {
		h, r, e := debugBarrierHost(t)
		if _, err := h.Enqueue(Notice{ID: "n1", Reference: "synthetic/n1"}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-r.started:
		case <-time.After(5 * time.Second):
			t.Fatal("notice write never started")
		}
		claimed := make(chan error, 1)
		var token string
		go func() {
			defer close(claimed)
			tok, err := h.debugIssueGrant(debugAuthority{"manager", "manager", h.Snapshot().Epoch}, []string{"view", "writer_control", "text", "stop"})
			if err == nil {
				err = h.debugAttachView(tok)
			}
			if err == nil {
				err = h.debugControl(tok, "claim", 0)
			}
			token = tok
			claimed <- err
		}()
		select {
		case err := <-claimed:
			debugWant(t, err, "")
		case <-time.After(2 * time.Second):
			t.Fatal("claim blocked behind runner I/O")
		}
		revision := h.Snapshot().Debug.WriterRevision
		debugWant(t, debugText(h, token, "early", "too early", revision, ""), "input_fenced")
		if e.count() != 0 {
			t.Fatal("text issued while the notice was in flight")
		}
		close(r.release)
		debugAwait(t, h, "notice settled", func(s State) bool { return s.Records[0].State == "applied" })
		writes, _ := r.count()
		if writes != 1 || !bytes.Equal(r.writes[0], []byte("notice:n1\n")) {
			t.Fatalf("notice frames %q", r.writes)
		}
		if h.Snapshot().Debug.Writer != "claimed" {
			t.Fatal("settled notice retracted the claim")
		}
		record, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "after", Text: "now eligible", ExpectedRevision: revision})
		debugWant(t, err, "")
		if record.Stage != "write_known" || e.count() != 1 {
			t.Fatal("eligible text after settlement not accepted once")
		}
		if writes, _ := r.count(); writes != 1 {
			t.Fatal("text input caused a runner write in this inert fixture")
		}
	})
	t.Run("claim-first", func(t *testing.T) {
		h, r, e := debugBarrierHost(t)
		token := debugHostGrant(t, h, "view", "writer_control", "text", "stop")
		debugWant(t, h.debugControl(token, "claim", 0), "")
		revision := h.Snapshot().Debug.WriterRevision
		if _, err := h.Enqueue(Notice{ID: "n1", Reference: "synthetic/n1"}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-r.started:
			t.Fatal("claimed writer let the notice reserve")
		case <-time.After(100 * time.Millisecond):
		}
		record, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "fresh", Text: "fresh input", ExpectedRevision: revision})
		debugWant(t, err, "")
		if record.Stage != "write_known" || e.count() != 1 || h.Snapshot().Records[0].State != "queued" {
			t.Fatal("claim-first did not permit exactly one fresh input while holding the notice")
		}
		close(r.release)
		debugWant(t, h.debugControl(token, "yield_and_resume", h.Snapshot().Debug.WriterRevision), "")
		debugAwait(t, h, "resumed notice", func(s State) bool { return s.Records[0].State == "applied" })
		if writes, _ := r.count(); writes != 1 {
			t.Fatalf("resumed notice delivered %d times", writes)
		}
	})
	t.Run("stop-after-issue", func(t *testing.T) {
		h, r, _ := debugBarrierHost(t)
		if _, err := h.Enqueue(Notice{ID: "n1", Reference: "synthetic/n1"}); err != nil {
			t.Fatal(err)
		}
		<-r.started
		token := debugHostGrant(t, h, "view", "stop")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// The stop intent is durable before the runner call; the runner call
		// itself queues behind the in-flight write on the single controller
		// connection, so the barrier is released once the intent is visible.
		stopped := make(chan error, 1)
		go func() { stopped <- h.debugStop(ctx, token) }()
		debugAwait(t, h, "stop intent durable", func(s State) bool { return s.StopRequested })
		close(r.release)
		debugWant(t, <-stopped, "")
		settled := debugAwait(t, h, "late result reconciled", func(s State) bool { return s.Records[0].State != "unknown" || s.Fence == "unknown" })
		writes, stops := r.count()
		if writes != 1 || stops != 1 || !settled.StopRequested || settled.Debug.Writer != "stopped" {
			t.Fatalf("stop after issue: writes=%d stops=%d %+v", writes, stops, settled.Debug)
		}
		if _, err := h.Enqueue(Notice{ID: "n2", Reference: "synthetic/n2"}); err == nil || err.Error() != "session_stopped" {
			t.Fatalf("stopped host accepted a notice: %v", err)
		}
	})
	t.Run("revoke-before-issue", func(t *testing.T) {
		h, r, e := debugBarrierHost(t)
		token := debugHostGrant(t, h, "view", "writer_control", "text")
		debugWant(t, h.debugControl(token, "claim", 0), "")
		revision := h.Snapshot().Debug.WriterRevision
		debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", h.Snapshot().Epoch}, token), "")
		_, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "fresh", Text: "x", ExpectedRevision: revision})
		debugWant(t, err, "attach_grant_revoked")
		writes, _ := r.count()
		if e.count() != 0 || writes != 0 || h.Snapshot().Debug.Writer != "held" {
			t.Fatal("revoked writer still issued or resumed")
		}
	})
}

// StopDuringAttach (owned child): plain detach of the sole viewer leaves the
// real runner child alive and unstopped, while an explicit view+stop grant
// stops it through the owned-child path with the writer fenced.
func TestDetachLeavesChildAliveStopRightStops(t *testing.T) {
	cfg, _ := debugProtocolConfig(t, "normal")
	on := true
	cfg.DebugAttach = &on
	h := debugOpenHost(t, cfg)
	debugWant(t, h.debugRegister(debugRegistry{"manager", "owner"}), "")
	debugAwait(t, h, "idle", func(s State) bool { return s.InputState == InputIdle })
	viewer := debugHostGrant(t, h, "view", "writer_control", "text")
	debugWant(t, h.debugControl(viewer, "claim", 0), "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	debugWant(t, h.debugStop(ctx, viewer), "input_right_required")
	debugWant(t, h.debugControl(viewer, "detach", h.Snapshot().Debug.WriterRevision), "")
	time.Sleep(30 * time.Millisecond)
	if status := debugRunnerStatus(t, cfg); status.Exited || h.Snapshot().StopRequested {
		t.Fatal("detach stopped the child")
	}
	stopper := debugHostGrant(t, h, "view", "stop")
	debugWant(t, h.debugStop(ctx, stopper), "")
	if status := debugRunnerStatus(t, cfg); !status.Exited || !h.Snapshot().StopRequested || h.Snapshot().Debug.Writer != "stopped" {
		t.Fatalf("stop right did not stop the owned child: %+v", status)
	}
}
