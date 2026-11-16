package sessionhost

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

// AttachUnadvertised / DetachModeCompatibility (host portion): a legacy
// disabled execution serializes without any debug member, creates no private
// custody directory and refuses every grant with public V01, while an enabled
// execution without a viewer already captures into bounded custody from spawn
// and a later current grant can read it. Launcher preflight is C, not here.
func TestAttachUnadvertisedAndDetachedCustody(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := openStore(filepath.Join(root, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	legacy := State{Version: 1, Execution: "fixture", RunnerInstance: "runner", Adapter: "fixture", Selection: Selection{"provider", "model", "effort"}, Epoch: 1, Fence: "open", InputState: InputIdle}
	if err := s.write(legacy); err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(filepath.Join(s.dir, "state.json"))
	if err != nil || bytes.Contains(disk, []byte("debug")) {
		t.Fatalf("disabled execution serialized debug members: %s %v", disk, err)
	}
	if _, err := os.Lstat(filepath.Join(s.dir, "debug-attach")); !os.IsNotExist(err) {
		t.Fatal("disabled execution created debug custody")
	}
	h := &Host{store: s, state: legacy, debugRegistry: debugRegistry{"manager", "owner"}}
	_, err = h.debugIssueGrant(debugAuthority{"manager", "manager", 1}, []string{"view"})
	debugWant(t, err, "attach_not_enabled")
	debugWant(t, h.debugAttachView("absent"), "attach_not_enabled")
	if _, err := h.debugRead("absent", 0); err == nil || err.Error() != "attach_not_enabled" {
		t.Fatalf("disabled read: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(s.dir, "debug-attach")); !os.IsNotExist(err) {
		t.Fatal("refused grant created debug custody")
	}
	// Enabled but detached: custody and capture exist before any viewer.
	enabled := debugFixture(t)
	if _, err := os.Stat(filepath.Join(enabled.debug.journal.dir, "input.journal")); err != nil {
		t.Fatal("enabled detached execution has no private journal")
	}
	enabled.debug.tail.observe([]byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"detached-capture-canary"}}`))
	token := debugView(t, enabled, "view")
	read, err := enabled.debugRead(token, 0)
	debugWant(t, err, "")
	if !bytes.Contains(read.Event, []byte("detached-capture-canary")) {
		t.Fatal("capture did not begin before attachment")
	}
}

// AttachGenerationAndIncarnation (host portion) / GrantAllowlistAndRevocation:
// with the registered owner delegating to the registered manager, exact
// current binding evidence yields a grant and a view; unregistered actors or
// roles are V04, missing binding evidence V06, positively different execution,
// runner, epoch or native thread V07, and an expired grant V05. No refusal
// discloses tail bytes, steals the slot or journals an effect.
func TestAttachGenerationAndIncarnationHostPortion(t *testing.T) {
	h := debugFixture(t)
	h.state.Protocol = &ProtocolState{View: ap.View{NativeSession: "thread-a"}}
	owner, manager := debugAuthority{"owner", "owner", 1}, debugAuthority{"manager", "manager", 1}
	exact := debugBinding{execution: "fixture", runner: "runner", thread: "thread-a", epoch: 1}
	h.debug.tail.observe([]byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"binding-body-canary"}}`))
	token, err := h.debugIssueGrantTo(owner, manager, exact, []string{"view"})
	debugWant(t, err, "")
	debugWant(t, h.debugAttachView(token), "")
	read, err := h.debugRead(token, 0)
	debugWant(t, err, "")
	if !bytes.Contains(read.Event, []byte("binding-body-canary")) {
		t.Fatal("exact binding did not open the view")
	}
	used, grants := h.debug.journal.used, len(h.debug.grants)
	cases := []struct {
		name, want       string
		issuer, receiver debugAuthority
		binding          debugBinding
	}{
		{"unregistered-recipient", "attach_unauthorized", owner, debugAuthority{"stranger", "manager", 1}, exact},
		{"label-without-registration", "attach_unauthorized", debugAuthority{"stranger", "owner", 1}, manager, exact},
		{"wrong-role", "attach_unauthorized", owner, debugAuthority{"manager", "owner", 1}, exact},
		{"missing-execution", "attach_binding_unavailable", owner, manager, debugBinding{runner: "runner", epoch: 1}},
		{"missing-runner", "attach_binding_unavailable", owner, manager, debugBinding{execution: "fixture", epoch: 1}},
		{"missing-epoch", "attach_binding_unavailable", owner, manager, debugBinding{execution: "fixture", runner: "runner"}},
		{"other-execution", "attach_binding_conflict", owner, manager, debugBinding{execution: "other", runner: "runner", epoch: 1}},
		{"other-runner", "attach_binding_conflict", owner, manager, debugBinding{execution: "fixture", runner: "replaced-runner", epoch: 1}},
		{"other-thread", "attach_binding_conflict", owner, manager, debugBinding{execution: "fixture", runner: "runner", thread: "thread-b", epoch: 1}},
		// The host knows thread-a; evidence that omits the thread is incomplete.
		{"missing-thread-while-known", "attach_binding_unavailable", owner, manager, debugBinding{execution: "fixture", runner: "runner", epoch: 1}},
		{"old-epoch", "attach_binding_conflict", debugAuthority{"owner", "owner", 2}, debugAuthority{"manager", "manager", 2}, debugBinding{execution: "fixture", runner: "runner", thread: "thread-a", epoch: 2}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			denied, err := h.debugIssueGrantTo(c.issuer, c.receiver, c.binding, []string{"view"})
			debugWant(t, err, c.want)
			if denied != "" || len(h.debug.grants) != grants || h.debug.viewer != token || h.debug.journal.used != used || len(h.state.Debug.Inputs) != 0 {
				t.Fatal("refused grant minted authority, moved the slot or journaled an effect")
			}
			read, err := h.debugRead(denied, 0)
			if err == nil || len(read.Event) != 0 {
				t.Fatal("refused grant disclosed the tail")
			}
		})
	}
	// While the host knows no native thread, evidence naming one cannot be
	// confirmed (V06) and thread-less evidence is the complete form; the grant
	// issued under thread-a is no longer current once the thread is gone.
	h.state.Protocol = nil
	_, err = h.debugIssueGrantTo(owner, manager, debugBinding{execution: "fixture", runner: "runner", thread: "thread-a", epoch: 1}, []string{"view"})
	debugWant(t, err, "attach_binding_unavailable")
	threadless, err := h.debugIssueGrantTo(owner, manager, debugBinding{execution: "fixture", runner: "runner", epoch: 1}, []string{"view"})
	debugWant(t, err, "")
	_, err = h.debugRead(token, 0)
	debugWant(t, err, "attach_grant_revoked")
	debugWant(t, h.debugAttachView(threadless), "")
	debugWant(t, h.debugControl(threadless, "detach", 0), "")
	threadless, err = h.debugIssueGrantTo(owner, manager, debugBinding{execution: "fixture", runner: "runner", epoch: 1}, []string{"view"})
	debugWant(t, err, "")
	token, err = h.debugIssueGrantTo(owner, manager, debugBinding{execution: "fixture", runner: "runner", epoch: 1}, []string{"view"})
	debugWant(t, err, "")
	debugWant(t, h.debugAttachView(token), "")
	now := time.Now()
	h.debug.now = func() time.Time { return now }
	now = now.Add(debugGrantLifetime)
	_, err = h.debugRead(token, 0)
	debugWant(t, err, "attach_grant_revoked")
	debugWant(t, h.debugAttachView(threadless), "attach_unauthorized")
	if h.debug.viewer != "" || len(h.debug.grants) != 0 {
		t.Fatal("expired grants retained slot or allocation")
	}
}

// SecondAttacherAndWriter (rights revisions) / owner takeover: an authorized
// caller with current rights but no claim is V09, a stale prior writer revision
// V11 with no writer or terminal mutation, and owner takeover requires the
// explicit owner revocation of the manager grant before the owner's own fresh
// grant occupies the slot and claims a new revision.
func TestSecondAttacherAndWriterRevisions(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control", "text")
	debugWant(t, debugText(h, token, "reply", "hello", h.state.Debug.WriterRevision, ""), "input_writer_required")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	current := h.state.Debug.WriterRevision
	debugWant(t, h.debugControl(token, "yield", current), "")
	before := cloneState(h.state)
	used := h.debug.journal.used
	debugWant(t, h.debugControl(token, "claim", 0), "")
	debugWant(t, h.debugControl(token, "renew", current), "input_observation_stale")
	debugWant(t, h.debugControl(token, "resume_automation", current), "input_observation_stale")
	if h.state.Debug.Writer != "claimed" || h.state.Debug.WriterRevision != before.Debug.WriterRevision+1 || used == h.debug.journal.used {
		t.Fatal("stale revision mutated writer or the valid claim was not journaled")
	}
	ownerToken, err := h.debugIssueGrantTo(debugAuthority{"owner", "owner", 1}, debugAuthority{"owner", "owner", 1}, h.debugBindingLocked(), []string{"view", "writer_control"})
	debugWant(t, err, "")
	debugWant(t, h.debugAttachView(ownerToken), "attach_in_use")
	debugWant(t, h.debugControl(ownerToken, "claim", 0), "attach_unauthorized")
	if h.debug.viewer != token || h.state.Debug.Writer != "claimed" {
		t.Fatal("owner attempt without takeover displaced the manager")
	}
	revision := h.state.Debug.WriterRevision
	debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", 1}, token), "")
	if h.state.Debug.Writer != "held" || h.state.Debug.WriterRevision != revision+1 || h.debug.viewer != "" {
		t.Fatal("takeover revocation did not hold the writer with a new revision")
	}
	debugWant(t, h.debugAttachView(ownerToken), "")
	debugWant(t, h.debugControl(ownerToken, "claim", 0), "")
	if h.state.Debug.Writer != "claimed" || h.state.Debug.WriterRevision != revision+2 {
		t.Fatal("owner claim did not take a fresh revision")
	}
	_, err = h.debugRead(token, 0)
	debugWant(t, err, "attach_grant_revoked")
}

// LeaseYieldTimeoutDetach (lease arithmetic and explicit resume): under a fake
// monotonic clock a renewal within the 20-second interval keeps the 60-second
// lease, the lease expires exactly at 60 seconds after the last renewal into
// held, a renewal after expiry is V09, yield-and-resume and detach-with-resume
// resume only from an eligible state and leave writer/view unchanged when a
// terminal fact or fence forbids it.
func TestLeaseYieldTimeoutDetachResume(t *testing.T) {
	h := debugFixture(t)
	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	h.debug.now = func() time.Time { return now }
	h.debug.tail.now = h.debug.now
	token := debugView(t, h, "view", "writer_control")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	now = now.Add(20 * time.Second)
	debugWant(t, h.debugControl(token, "renew", h.state.Debug.WriterRevision), "")
	now = now.Add(59 * time.Second)
	h.debugExpireLeaseLocked()
	if h.state.Debug.Writer != "claimed" {
		t.Fatal("lease expired before 60 seconds after renewal")
	}
	now = now.Add(time.Second)
	h.debugExpireLeaseLocked()
	if h.state.Debug.Writer != "held" {
		t.Fatal("lease did not expire at 60 seconds")
	}
	debugWant(t, h.debugControl(token, "renew", h.state.Debug.WriterRevision), "input_writer_required")
	for _, op := range []string{"yield_and_resume", "detach_with_resume"} {
		t.Run(op, func(t *testing.T) {
			h := debugFixture(t)
			token := debugView(t, h, "view", "writer_control")
			debugWant(t, h.debugControl(token, "claim", 0), "")
			h.state.Debug.Results = []debugResult{{ID: "result", Thread: "thread", Turn: "turn", Outcome: "completed", Revision: 1, NotificationPending: true}}
			debugWant(t, h.persistLocked(h.state), "")
			before := cloneState(h.state)
			used := h.debug.journal.used
			debugWant(t, h.debugControl(token, op, before.Debug.WriterRevision), "input_fenced")
			if !reflect.DeepEqual(before, h.state) || h.debug.viewer != token || used != h.debug.journal.used {
				t.Fatal("ineligible resume changed writer, view or journal")
			}
			debugWant(t, h.debugDisposition(token, "result", 1, false, true), "")
			debugWant(t, h.debugControl(token, op, h.state.Debug.WriterRevision), "")
			if h.state.Debug.Writer != "automatic" {
				t.Fatal("eligible resume did not resume")
			}
			if op == "detach_with_resume" && (h.debug.viewer != "" || len(h.debug.grants) != 0) {
				t.Fatal("detach-with-resume retained the attachment")
			}
			if op == "yield_and_resume" && h.debug.viewer != token {
				t.Fatal("yield-and-resume closed the view")
			}
		})
	}
}

// Private refusal results carry only the fixed code; journaled control intents
// never include the bearer token, and the content-free rejection counters
// contain no attempted body. A nearby accepted control proves the journal
// still records attributed intents.
func TestGrantMaterialNeverLeavesCustody(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control", "text")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	debugWant(t, debugText(h, token, "reply", "forbidden-body-canary", h.state.Debug.WriterRevision, ""), "input_operation_unsupported")
	summary, err := h.debugSummaryJSON()
	debugWant(t, err, "")
	journal, err := os.ReadFile(filepath.Join(h.debug.journal.dir, "input.journal"))
	if err != nil {
		t.Fatal(err)
	}
	for _, sink := range [][]byte{summary, journal, debugDisk(t, h)} {
		if bytes.Contains(sink, []byte(token)) || bytes.Contains(sink, []byte("forbidden-body-canary")) {
			t.Fatalf("grant or rejected body left custody: %s", sink)
		}
	}
	var parsed debugSummary
	if err := json.Unmarshal(summary, &parsed); err != nil || parsed.Rejections["input_operation_unsupported"] != 1 || !parsed.Attached {
		t.Fatalf("summary lost bounded rejection evidence: %s", summary)
	}
	if !strings.Contains(string(journal), `"kind":"claim"`) {
		t.Fatal("accepted control not journaled")
	}
}
