package sessionhost

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

// These are the independent review's reproduction tests for the three
// host-private bypasses it found (failed-delivery token retention, stale-thread
// answer, pending protocol outbox) together with its positive controls,
// retained verbatim in behaviour and reformatted only. Each must pass.

// A failed grant delivery must revoke the abandoned capability; a successful
// delivery is exercised by the candidate's real manager-child positive control.
func TestAttachFailedGrantDeliveryRevokesToken(t *testing.T) {
	h, _ := debugEffectFixture(t)
	token := debugToken(t, h, "view", "writer_control")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	err = h.debugDeliverGrant(token, w)
	debugWant(t, err, "attach_binding_unavailable")
	err = h.debugAttachView(token)
	if err == nil {
		t.Fatal("B12: failed delivery left the abandoned token usable for a fresh attachment")
	}
	debugWant(t, err, "attach_unauthorized")
}

// Currency is checked against the original interaction's native thread as
// well as the fresh grant; replacing the thread must not make old answers valid.
func TestAttachAnswerOldThreadWithFreshGrant(t *testing.T) {
	h, e := debugEffectFixture(t)
	setCurrentThread(t, h, "thread-a")
	offered := []string{"allow", "deny"}
	interaction, err := h.debugObserveInteraction("thread-a", "turn-a", 11, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	old := debugView(t, h, "view")
	setCurrentThread(t, h, "thread-b")
	_, err = h.debugRead(old, 0)
	debugWant(t, err, "attach_grant_revoked")
	current, revision := debugWriter(t, h, "view", "writer_control", "native_response")
	before := h.debug.journal.sequence
	_, err = h.debugInput(current, debugInputIntent{Kind: "answer_interaction", ID: "old-thread-answer", ExpectedRevision: revision, ExpectedTurn: "turn-a", Interaction: interaction, RequestID: 11, OptionsDigest: debugOptionsDigest(offered), Option: "allow"})
	if err == nil || err.Error() != "interaction_stale" || e.count() != 0 || h.debug.journal.sequence != before || h.state.Debug.Interactions[0].State != "pending" {
		t.Fatalf("B18: old-thread interaction answered under fresh thread-b grant: err=%v effects=%d stage=%s sequence_delta=%d; want V21, no CAS/frame/effect", err, e.count(), h.state.Debug.Interactions[0].State, h.debug.journal.sequence-before)
	}
}

// The detector proves a real inherited Unix socket remains usable, while a
// subordinate started with an actual Host controller and journal open inherits
// none of this test process's private socket, FIFO or regular-file descriptors.
// Limit: the private set is the descriptors open at the moment of the census;
// it is compared by device/inode, which identifies an object only while open.
func TestAttachControllerDescriptorCustody(t *testing.T) {
	baseline := debugSubordinate(t)
	cfg, _ := debugProtocolConfig(t, "normal")
	on := true
	cfg.DebugAttach = &on
	h := debugOpenHost(t, cfg)
	debugWant(t, h.debugRegister(debugRegistry{"manager", "owner"}), "")
	debugAwait(t, h, "idle actual host", func(s State) bool { return s.InputState == InputIdle })
	token := debugHostGrant(t, h, "view", "writer_control")
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Model the required noninheritable default; the typed detector below
	// deliberately routes the same socket through ExtraFiles instead.
	syscall.CloseOnExec(pair[0])
	syscall.CloseOnExec(pair[1])
	source, sink := os.NewFile(uintptr(pair[0]), "controller-sentinel-source"), os.NewFile(uintptr(pair[1]), "controller-sentinel-sink")
	defer source.Close()
	defer sink.Close()
	if _, err = sink.Write([]byte("controller-sentinel-custody\n")); err != nil {
		t.Fatal(err)
	}
	// An inode the no-custody baseline subordinate already holds is an object a
	// Go process opens for itself (for example the runtime's own cgroup CPU
	// limit file), not host custody inherited from this process; leave it out.
	shared := map[debugInode]bool{}
	for _, fd := range baseline.FDs {
		shared[debugInode{fd.Dev, fd.Ino}] = true
	}
	// The census is a names-only listing plus one Fstat per descriptor: a
	// listing that also stats each entry by path fails as a whole where one
	// descriptor has no path metadata, before any custody comparison runs.
	// Every socket, FIFO and regular file this process holds is private,
	// whatever its number; the standard descriptors are not exempt.
	var private []debugInode
	custody := 0
	for _, own := range debugOwnDescriptors(t) {
		if (own.Type == "socket" || own.Type == "fifo" || own.Type == "regular") && !shared[own.Inode] {
			private = append(private, own.Inode)
			if own.FD > 2 {
				custody++
			}
		}
	}
	// This covers the active controller opened by Host.Open, in addition to
	// the journal and explicit controller sentinel; only this fixture's FDs.
	// The lower bound counts descriptors above the standard three so that this
	// process's own stdio cannot satisfy it; it exempts nothing from the check.
	if custody < 4 {
		t.Fatal("fixture has no actual controller/private FD census")
	}
	// Positive control of the census itself: both live sentinel sockets are in it.
	for _, sentinel := range []*os.File{source, sink} {
		want, seen := debugInodeOf(t, sentinel), false
		for _, inode := range private {
			if inode == want {
				seen = true
			}
		}
		if !seen {
			t.Fatal("descriptor census missed a live sentinel socket")
		}
	}
	debugAssertSubordinateClean(t, debugSubordinate(t), baseline, []string{token}, private)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), binary, "-test.run=^TestAttachControllerProbeProcess$")
	cmd.Dir = filepath.Clean(debugProbeDir(t))
	cmd.Env = []string{"SESSION_HOST_CONTROLLER_PROBE=1", "GORACE=atexit_sleep_ms=0"}
	cmd.ExtraFiles = []*os.File{source}
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "controller-sentinel-custody") {
		t.Fatalf("typed detector failed: %v %s", err, out)
	}
}

// Only the detector child reads its deliberately inherited synthetic socket;
// the ordinary subordinate uses the existing full FD/argv/environment probe.
func TestAttachControllerProbeProcess(t *testing.T) {
	if os.Getenv("SESSION_HOST_CONTROLLER_PROBE") != "1" {
		return
	}
	var st syscall.Stat_t
	if syscall.Fstat(3, &st) != nil || debugProbeType(uint32(st.Mode)) != "socket" {
		os.Exit(31)
	}
	f := os.NewFile(3, "controller-sentinel")
	b := make([]byte, 128)
	n, err := f.Read(b)
	if err != nil || string(b[:n]) != "controller-sentinel-custody\n" {
		os.Exit(32)
	}
	fmt.Print(string(b[:n]))
	os.Exit(0)
}

// The private input gate must share the existing protocol outbox exclusion:
// an idle view is insufficient while a committed host control frame is pending.
func TestAttachPrivateInputCannotPassPendingProtocolOutbox(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			h, e := debugEffectFixture(t)
			h.state.Protocol = &ProtocolState{BoundaryVersion: ap.Version, ObservedRelease: "schematic", Checkpoint: []byte(`{}`), View: ap.View{NativeSession: "thread", Activity: ap.Idle}}
			h.driver = factDriver{ap.Descriptor{Version: ap.Version, ObservedRelease: "schematic", Name: "fixture", PrimaryTransport: "pipe"}}
			transition := ap.Transition{Checkpoint: []byte(`{}`), View: h.state.Protocol.View}
			if pending {
				transition.Actions = []ap.Action{{Key: "reserved", Bytes: []byte("control\n")}}
			}
			h.mu.Lock()
			transitionErr := h.persistTransitionLocked(transition)
			h.mu.Unlock()
			debugWant(t, transitionErr, "")
			if !validProtocolState(h.state) {
				t.Fatal("test precondition: protocol state invalid")
			}
			if pending && len(h.state.Protocol.Pending) != 1 {
				t.Fatal("production transition did not commit control outbox")
			}
			token, revision := debugWriter(t, h, "view", "writer_control", "text")
			before := h.debug.journal.sequence
			_, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "after-control", Text: "a distinct frame", ExpectedRevision: revision})
			if !pending {
				debugWant(t, err, "")
				if e.count() != 1 {
					t.Fatal("nearby valid input not admitted")
				}
			} else if err == nil || err.Error() != "input_fenced" || e.count() != 0 || h.debug.journal.sequence != before {
				t.Fatalf("B16: pending protocol frame did not fence private input: err=%v effects=%d sequence_delta=%d; want V12 and zero new input/effect", err, e.count(), h.debug.journal.sequence-before)
			}
		})
	}
}

// A fresh interaction belonging to the current nonempty thread is the valid
// neighbor for the stale-thread answer attack; rejecting all answers cannot pass.
func TestAttachCurrentThreadAnswerPositive(t *testing.T) {
	h, e := debugEffectFixture(t)
	setCurrentThread(t, h, "thread-b")
	offered := []string{"allow", "deny"}
	id, err := h.debugObserveInteraction("thread-b", "turn-b", 12, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	token, rev := debugWriter(t, h, "view", "writer_control", "native_response")
	r, err := h.debugInput(token, debugInputIntent{Kind: "answer_interaction", ID: "current", ExpectedRevision: rev, ExpectedTurn: "turn-b", Interaction: id, RequestID: 12, OptionsDigest: debugOptionsDigest(offered), Option: "allow"})
	debugWant(t, err, "")
	if e.count() != 1 || r.Stage != "write_known" {
		t.Fatal("fresh current-thread answer did not issue once")
	}
}

// A terminal question can expire from the volatile tail without losing its
// durable block, notification debt or original age, including after replacement.
func TestAttachTerminalDebtSurvivesQuestionTTL(t *testing.T) {
	h := debugFixture(t)
	now := time.Now()
	h.debug.now = func() time.Time { return now }
	h.debug.tail.now = h.debug.now
	var notifications []debugNotification
	h.debugNotify = func(n debugNotification) { notifications = append(notifications, n) }
	h.debug.tail.observe([]byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"terminal-question-canary"}}`))
	next := cloneState(h.state)
	debugWant(t, h.debugTerminalLocked(&next, "assignment", "thread", "turn", "completed"), "")
	debugWant(t, h.persistLocked(next), "")
	h.debugAnnounceLocked(0)
	if len(notifications) != 1 || notifications[0].Kind != "terminal_observed" {
		t.Fatal("missing immediate terminal intent")
	}
	encoded, _ := json.Marshal(notifications)
	if bytes.Contains(encoded, []byte("terminal-question-canary")) {
		t.Fatal("question leaked to notifier")
	}
	original := h.state.Debug.Results[0].ObservedAt
	now = now.Add(899 * time.Second)
	token := debugView(t, h, "view")
	r, err := h.debugRead(token, 0)
	debugWant(t, err, "")
	if !bytes.Contains(r.Event, []byte("terminal-question-canary")) {
		t.Fatal("question unavailable before TTL")
	}
	debugWant(t, h.debugControl(token, "detach", 0), "")
	now = now.Add(time.Second)
	token = debugView(t, h, "view")
	r, err = h.debugRead(token, 0)
	debugWant(t, err, "")
	if len(r.Event) != 0 || r.Cause != "age" || r.GapThrough == 0 {
		t.Fatal("expired question restored or gap missing")
	}
	if h.debugAutomaticLocked() || !h.state.Debug.Results[0].NotificationPending || !h.state.Debug.Results[0].ObservedAt.Equal(original) {
		t.Fatal("TTL changed debt or permission")
	}
	replacement, err := debugReplace(t, h)
	debugWant(t, err, "")
	replacement.debug.now = func() time.Time { return now }
	fresh := debugView(t, replacement, "view")
	r, err = replacement.debugRead(fresh, 0)
	debugWant(t, err, "")
	if len(r.Event) != 0 || r.Cause != "host_replaced" || len(notifications) != 2 || !notifications[1].ObservedAt.Equal(original) {
		t.Fatal("replacement invented text or reset original debt age")
	}
	summary, err := replacement.debugSummaryJSON()
	debugWant(t, err, "")
	var parsed debugSummary
	if json.Unmarshal(summary, &parsed) != nil || parsed.OldestDebtSeconds != 900 || parsed.Results.Pending != 1 || replacement.debugAutomaticLocked() {
		t.Fatal("replacement reset debt or block")
	}
	if bytes.Contains(debugDisk(t, replacement), []byte("terminal-question-canary")) {
		t.Fatal("question entered durable snapshot")
	}
}

// Feed an authoritative view change through the delivered transition commit,
// using the candidate's existing driver fixture, rather than editing the grant
// predicate or directly rewriting an already-bound thread in the test.
func setCurrentThread(t *testing.T, h *Host, thread string) {
	t.Helper()
	if h.state.Protocol == nil {
		h.state.Protocol = &ProtocolState{BoundaryVersion: ap.Version, ObservedRelease: "schematic", Checkpoint: []byte(`{}`), View: ap.View{Activity: ap.Unknown}}
	}
	h.driver = factDriver{ap.Descriptor{Version: ap.Version, ObservedRelease: "schematic", Name: "fixture", PrimaryTransport: "pipe"}}
	h.mu.Lock()
	err := h.persistTransitionLocked(ap.Transition{Checkpoint: []byte(`{}`), View: ap.View{NativeSession: thread, Activity: ap.Idle}})
	h.mu.Unlock()
	debugWant(t, err, "")
	if !validProtocolState(h.state) {
		t.Fatal("authoritative fixture transition produced invalid protocol state")
	}
}

// Pending protocol outbox fences both typed input kinds (C15): with a committed
// control frame pending, text is V12 and an otherwise valid answer is V12 with
// the interaction still pending, zero effects and an unchanged journal; once
// the outbox drains both are admitted at the same checkpoint.
func TestPrivateInputFencedByPendingProtocolOutbox(t *testing.T) {
	h, e := debugEffectFixture(t)
	setCurrentThread(t, h, "thread")
	offered := []string{"allow", "deny"}
	interaction, err := h.debugObserveInteraction("thread", "turn-1", 5, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	h.mu.Lock()
	err = h.persistTransitionLocked(ap.Transition{Checkpoint: []byte(`{}`), View: ap.View{NativeSession: "thread", Activity: ap.Idle}, Actions: []ap.Action{{Key: "pending-control", Bytes: []byte("control\n")}}})
	h.mu.Unlock()
	debugWant(t, err, "")
	token, revision := debugWriter(t, h, "view", "writer_control", "text", "native_response")
	before := h.debug.journal.sequence
	answer := debugInputIntent{Kind: "answer_interaction", ID: "answer", ExpectedRevision: revision, ExpectedTurn: "turn-1", Interaction: interaction, RequestID: 5, OptionsDigest: debugOptionsDigest(offered), Option: "allow"}
	_, err = h.debugInput(token, answer)
	debugWant(t, err, "input_fenced")
	debugWant(t, debugText(h, token, "text", "hello", revision, ""), "input_fenced")
	if e.count() != 0 || h.debug.journal.sequence != before || h.state.Debug.Interactions[0].State != "pending" || len(h.state.Debug.Inputs) != 0 {
		t.Fatal("pending outbox did not fence typed input before reservation")
	}
	// Drain the outbox through the private state (the outbox owner retires the
	// pending frame after its receipt) and retry at the same checkpoint.
	drained := cloneState(h.state)
	drained.Protocol.Pending = nil
	debugWant(t, h.persistLocked(drained), "")
	record, err := h.debugInput(token, answer)
	debugWant(t, err, "")
	if record.Stage != "write_known" || e.count() != 1 || h.state.Debug.Interactions[0].State != "answer_reserved" {
		t.Fatal("drained outbox did not admit the same answer")
	}
}

// Interaction thread currency (C17): after the authoritative thread changes, the
// retained request is a stale target for any current grant (V21, still
// pending, zero effects, unchanged journal), while a request observed on the
// current thread is answered once by the same grant.
func TestAnswerRequiresCurrentInteractionThread(t *testing.T) {
	h, e := debugEffectFixture(t)
	setCurrentThread(t, h, "thread-a")
	offered := []string{"allow", "deny"}
	stale, err := h.debugObserveInteraction("thread-a", "turn-a", 21, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	setCurrentThread(t, h, "thread-b")
	current, err := h.debugObserveInteraction("thread-b", "turn-b", 22, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	token, revision := debugWriter(t, h, "view", "writer_control", "native_response")
	before := h.debug.journal.sequence
	_, err = h.debugInput(token, debugInputIntent{Kind: "answer_interaction", ID: "stale-answer", ExpectedRevision: revision, ExpectedTurn: "turn-a", Interaction: stale, RequestID: 21, OptionsDigest: debugOptionsDigest(offered), Option: "allow"})
	debugWant(t, err, "interaction_stale")
	if e.count() != 0 || h.debug.journal.sequence != before || h.state.Debug.Interactions[0].State != "pending" || len(h.state.Debug.Inputs) != 0 {
		t.Fatal("stale-thread request reached CAS, journal or effect")
	}
	record, err := h.debugInput(token, debugInputIntent{Kind: "answer_interaction", ID: "current-answer", ExpectedRevision: revision, ExpectedTurn: "turn-b", Interaction: current, RequestID: 22, OptionsDigest: debugOptionsDigest(offered), Option: "deny"})
	debugWant(t, err, "")
	if record.Stage != "write_known" || e.count() != 1 || h.state.Debug.Interactions[1].State != "answer_reserved" {
		t.Fatal("current-thread request was not answered once")
	}
}

// Failed delivery rollback (C07/C13): a broken pipe, a non-pipe recipient
// descriptor or no destination at all refuses with the exact class and revokes
// the abandoned token (attach V04, read V05, no slot), journals the revocation
// in protected capacity, and leaves an unrelated attached grant and the writer
// untouched; the same token delivered over a live pipe is read back and
// attaches, and an unknown token is a plain refusal that revokes nothing.
func TestFailedGrantDeliveryRevokesToken(t *testing.T) {
	h, e := debugEffectFixture(t)
	unrelated, revision := debugWriter(t, h, "view", "writer_control", "text")
	for _, mode := range []string{"broken-pipe", "regular-file", "nil-destination"} {
		t.Run(mode, func(t *testing.T) {
			token := debugToken(t, h, "view", "writer_control")
			revokes := h.debug.journal.revokes
			var to *os.File
			want := "attach_binding_unavailable"
			switch mode {
			case "broken-pipe":
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				r.Close()
				to = w
			case "regular-file":
				f, err := os.CreateTemp(debugProbeDir(t), "not-a-pipe-")
				if err != nil {
					t.Fatal(err)
				}
				to = f
				want = "input_invalid"
			case "nil-destination":
				to = nil
				want = "input_invalid"
			}
			debugWant(t, h.debugDeliverGrant(token, to), want)
			if _, live := h.debug.grants[token]; live || h.debug.journal.revokes != revokes+1 {
				t.Fatal("failed delivery left the token live or unjournaled")
			}
			debugWant(t, h.debugAttachView(token), "attach_unauthorized")
			_, err := h.debugRead(token, 0)
			debugWant(t, err, "attach_grant_revoked")
			if h.debug.viewer != unrelated || h.state.Debug.Writer != "claimed" || h.state.Debug.WriterRevision != revision {
				t.Fatal("rollback disturbed the unrelated attached writer")
			}
			debugWant(t, h.debugDeliverGrant(token, to), "attach_grant_revoked")
		})
	}
	debugWant(t, h.debugControl(unrelated, "detach", revision), "")
	revokes := h.debug.journal.revokes
	debugWant(t, h.debugDeliverGrant("absent-token", nil), "attach_grant_revoked")
	if h.debug.journal.revokes != revokes || len(h.debug.grants) != 0 {
		t.Fatal("unknown token revoked something else")
	}
	delivered := debugToken(t, h, "view")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	debugWant(t, h.debugDeliverGrant(delivered, w), "")
	line := make([]byte, len(delivered)+1)
	if _, err := r.Read(line); err != nil || string(line) != delivered+"\n" {
		t.Fatal("live delivery did not carry the token")
	}
	debugWant(t, h.debugAttachView(string(line[:len(delivered)])), "")
	if e.count() != 0 {
		t.Fatal("delivery produced an effect")
	}
}
