package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

// This driver models local metadata/dispatch only. Its declared thread/release
// are fixture values, not native or Source enrollment qualification.
type journalDriver struct {
	view         ap.View
	prepares     atomic.Int32
	observations atomic.Int32
}

func (*journalDriver) Descriptor() ap.Descriptor {
	return ap.Descriptor{Version: ap.Version, ObservedRelease: "0.167.0", Name: "codex", PrimaryTransport: "pipe", Capabilities: ap.Capabilities{NextTurn: true}}
}
func (*journalDriver) NoticeMode() string { return WorkReference }
func (d *journalDriver) Snapshot() ap.Transition {
	if d.view.NativeSession == "" {
		d.view = ap.View{NativeSession: "thread-1", Activity: ap.Idle}
	}
	return ap.Transition{Checkpoint: json.RawMessage(`{"fixture":true}`), View: d.view}
}
func (d *journalDriver) Begin() (ap.Transition, error) { return d.Snapshot(), nil }
func (d *journalDriver) Observe(ap.Chunk) (ap.Transition, error) {
	d.observations.Add(1)
	return d.Snapshot(), nil
}
func (d *journalDriver) Prepare(input ap.Input) (ap.Transition, error) {
	d.prepares.Add(1)
	d.view = ap.View{InputID: input.ID, NativeSession: "thread-1", Activity: ap.Busy}
	t := d.Snapshot()
	t.Actions = []ap.Action{{Key: "fixture-" + input.ID, InputID: input.ID, Bytes: []byte("fixture\n")}}
	return t, nil
}

func journalUnit(t *testing.T, install bool) (*Host, NotificationRequest) {
	t.Helper()
	q := notificationFixture("trigger")
	s, err := openStore(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &journalDriver{}
	snapshot := d.Snapshot()
	h := &Host{store: s, ctx: ctx, cancel: cancel, kick: make(chan struct{}, 1), driver: d,
		state: State{Version: 3, NoticeMode: WorkReference, Execution: q.Binding.Execution, RunnerInstance: q.Binding.RunnerInstance, Adapter: "codex", Epoch: 1,
			Selection: Selection{Vendor: "provider", Model: "model", Reasoning: "effort"}, Fence: "open", InputState: InputIdle,
			Protocol: &ProtocolState{BoundaryVersion: ap.Version, ObservedRelease: "0.167.0", Checkpoint: snapshot.Checkpoint, View: snapshot.View}}}
	h.cfg = Config{Execution: h.state.Execution, NoticeMode: WorkReference, Selection: h.state.Selection}
	if err := s.write(h.state); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); s.close() })
	if install {
		if err := h.installNotificationBinding(q.Binding); err != nil {
			t.Fatal(err)
		}
	}
	return h, q
}

func journalCall(t *testing.T, h *Host, q NotificationRequest, result, reason string) NotificationResponse {
	t.Helper()
	r, err := h.notificationRequest(q)
	if err != nil || r.Result != result || r.Reason != reason {
		t.Fatalf("want %s/%s: %+v %v", result, reason, r, err)
	}
	if _, err := EncodeNotificationResponse(r, q); err != nil {
		t.Fatalf("invalid response shape: %v", err)
	}
	return r
}

func journalBytes(t *testing.T, h *Host) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(h.store.dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Acceptance persists the complete correlation and queued intent together before
// the caller receives custody. Read-only reconciliation works while held and
// does not invent absent/not-applied or mailbox receipt semantics.
func TestNotificationJournalAcceptanceAndLookup(t *testing.T) {
	h, q := journalUnit(t, false)
	journalCall(t, h, q, "unresolved", "observation-unavailable")
	if err := h.installNotificationBinding(q.Binding); err != nil {
		t.Fatal(err)
	}
	r := journalCall(t, h, q, "accepted", "")
	if r.Attempt.NativeStage != "not-evidenced" || r.Attempt.Sequence != "1" {
		t.Fatal("wrong initial evidence")
	}
	s, exists, err := h.store.read()
	if err != nil || !exists || s.Version != notificationStateVersion || len(s.Records) != 1 || len(s.Notification.Attempts) != 1 || s.Notification.Binding != q.Binding {
		t.Fatalf("durable combined intent: %v", err)
	}
	before := journalBytes(t, h)
	h.state.Fence = "held"
	for _, method := range []string{"trigger", "lookup_attempt"} {
		q.Method = method
		got := journalCall(t, h, q, "existing-attempt", "")
		if !reflect.DeepEqual(got.Attempt, r.Attempt) {
			t.Fatal("same attempt changed")
		}
	}
	q.NoticeID = "absent"
	journalCall(t, h, q, "unresolved", "attempt-unresolved")
	if !bytes.Equal(before, journalBytes(t, h)) || len(h.state.Records) != 1 {
		t.Fatal("lookup/retry wrote state")
	}
	cloned := h.Snapshot()
	cloned.Notification.Binding.EnrollmentRef = "changed"
	cloned.Notification.Attempts[0].NoticeID = "changed"
	if h.state.Notification.Binding != s.Notification.Binding || h.state.Notification.Attempts[0].NoticeID == "changed" {
		t.Fatal("snapshot aliases correlation")
	}
}

// Same-token concurrent calls have one durable acceptance and one retained
// identity. A different token cannot open a second unresolved automatic attempt.
func TestNotificationJournalAtomicConcurrency(t *testing.T) {
	h, q := journalUnit(t, true)
	var wg sync.WaitGroup
	results := make(chan NotificationResponse, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			call := q
			call.RequestID = fmt.Sprintf("call-%d", i)
			r, err := h.notificationRequest(call)
			if err != nil {
				t.Error(err)
			}
			results <- r
		}(i)
	}
	wg.Wait()
	close(results)
	accepted, existing := 0, 0
	for r := range results {
		switch r.Result {
		case "accepted":
			accepted++
		case "existing-attempt":
			existing++
		default:
			t.Errorf("unexpected %s", r.Result)
		}
	}
	if accepted != 1 || existing != 23 || len(h.state.Records) != 1 || len(h.state.Notification.Attempts) != 1 {
		t.Fatal("duplicate/absent acceptance", accepted, existing)
	}
	before := journalBytes(t, h)
	q.NoticeID = "second"
	journalCall(t, h, q, "refused", "input-not-ready")
	if !bytes.Equal(before, journalBytes(t, h)) {
		t.Fatal("second unresolved attempt mutated journal")
	}
}

// Each new-input gate rejects for its intended reason beside the acceptance
// control above. Refusal leaves disk, record count and sequence unchanged.
func TestNotificationJournalGates(t *testing.T) {
	for _, tc := range []struct {
		name, result, reason string
		mutate               func(*Host)
	}{
		{"held", "refused", "external-held", func(h *Host) { h.state.Fence = "held" }},
		{"busy", "refused", "input-not-ready", func(h *Host) { h.state.InputState = InputBusy }},
		{"draft", "refused", "input-not-ready", func(h *Host) { h.state.InputState = InputDraft }},
		{"approval", "refused", "input-not-ready", func(h *Host) { h.state.InputState = InputApproval }},
		{"unknown-input", "refused", "input-not-ready", func(h *Host) { h.state.InputState = InputUnknown }},
		{"security", "refused", "security-stopped", func(h *Host) { h.state.SecurityStopped = true; h.state.InputState = InputSecurityStop }},
		{"stop", "refused", "input-not-ready", func(h *Host) { h.state.StopRequested = true }},
		{"unknown-fence", "unresolved", "observation-unavailable", func(h *Host) { h.state.Fence = "unknown" }},
		{"recovering", "unresolved", "observation-unavailable", func(h *Host) { h.recovering = true }},
		{"write-blocked", "unresolved", "observation-unavailable", func(h *Host) { h.writeBlocked = true }},
		{"protocol-failed", "unresolved", "observation-unavailable", func(h *Host) { h.protocolFailed = true }},
		{"cancelled", "unresolved", "observation-unavailable", func(h *Host) { h.cancel() }},
		{"ordinary-unknown", "refused", "input-not-ready", func(h *Host) {
			n := Notice{ID: "ordinary", Reference: "original-work"}
			h.state.Sequence = 1
			h.state.Records = []Record{{Notice: n, Digest: digestNoticeMode(n, h.state.Selection, WorkReference), Sequence: 1, State: "unknown"}}
		}},
		{"pending", "refused", "input-not-ready", func(h *Host) {
			h.state.Protocol.WireSequence = 1
			data := []byte("initialization\n")
			h.state.Protocol.Pending = []WireAction{{Sequence: 1, Key: "pending", Data: data, Digest: wireDigest(data)}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, q := journalUnit(t, true)
			before := journalBytes(t, h)
			tc.mutate(h)
			priorCount, priorSequence := len(h.state.Records), h.state.Sequence
			if !validProtocolState(h.state) {
				t.Fatal("gate fixture violates the protocol state contract")
			}
			journalCall(t, h, q, tc.result, tc.reason)
			if !bytes.Equal(before, journalBytes(t, h)) || len(h.state.Records) != priorCount || h.state.Sequence != priorSequence {
				t.Fatal("forbidden partial intent")
			}
		})
	}
}

// Binding/workflow changes cannot borrow retained custody; an ordinary legacy
// notice with the same ID is not relabeled as an automatic notification.
func TestNotificationJournalBindingAndLegacyConflict(t *testing.T) {
	for _, tc := range []struct {
		reason string
		mutate func(*NotificationBinding)
	}{
		{"workflow-conflict", func(b *NotificationBinding) { b.WorkflowReference = "other" }},
		{"workflow-conflict", func(b *NotificationBinding) { b.WorkflowSHA256 = strings.Repeat("b", 64) }},
		{"stale-binding", func(b *NotificationBinding) { b.ControllerEpoch = "2" }},
		{"stale-binding", func(b *NotificationBinding) { b.ConsumerGeneration = "other" }},
		{"binding-conflict", func(b *NotificationBinding) { b.MemberRef = "other" }},
		{"binding-conflict", func(b *NotificationBinding) { b.EnrollmentRef = "other" }},
		{"binding-conflict", func(b *NotificationBinding) { b.Execution = "other" }},
		{"binding-conflict", func(b *NotificationBinding) { b.RunnerInstance = "other" }},
		{"binding-conflict", func(b *NotificationBinding) { b.NativeThread = "other" }},
		{"binding-conflict", func(b *NotificationBinding) { b.ConnectionRef = "other" }},
		{"binding-conflict", func(b *NotificationBinding) { b.ConfigurationRef = "other" }},
		{"binding-conflict", func(b *NotificationBinding) { b.ProfileLockRef = "other" }},
	} {
		h, q := journalUnit(t, true)
		journalCall(t, h, q, "accepted", "")
		before := journalBytes(t, h)
		tc.mutate(&q.Binding)
		journalCall(t, h, q, "refused", tc.reason)
		if !bytes.Equal(before, journalBytes(t, h)) {
			t.Fatal("conflicting retry wrote")
		}
	}
	h, q := journalUnit(t, true)
	n := Notice{ID: q.NoticeID, Reference: q.Binding.WorkflowReference}
	h.state.Sequence = 1
	h.state.Records = []Record{{Notice: n, Digest: digestNoticeMode(n, h.state.Selection, WorkReference), Sequence: 1, State: "queued"}}
	if err := h.store.write(h.state); err != nil {
		t.Fatal(err)
	}
	before := journalBytes(t, h)
	journalCall(t, h, q, "refused", "invalid-attempt")
	if !bytes.Equal(before, journalBytes(t, h)) || len(h.state.Notification.Attempts) != 0 {
		t.Fatal("legacy notice reinterpreted")
	}
}

// Native stages come only from retained record evidence. Unsupported failure or
// interruption never becomes a definitive refusal or a made-up native stage.
func TestNotificationJournalRetainedStagesAndCount(t *testing.T) {
	h, q := journalUnit(t, true)
	journalCall(t, h, q, "accepted", "")
	for _, tc := range []struct {
		state, outcome, stage string
		accepted              bool
	}{
		{"queued", "", "not-evidenced", false}, {"unknown", "", "write-unresolved", false},
		{"unknown", "", "native-accepted", true}, {"applied", "completed", "native-completed", true},
	} {
		r := &h.state.Records[0]
		r.State = tc.state
		r.Outcome = tc.outcome
		r.NativeAccepted = tc.accepted
		if tc.accepted {
			r.NativeSession = q.Binding.NativeThread
		}
		got := journalCall(t, h, q, "existing-attempt", "")
		if got.Attempt.NativeStage != tc.stage {
			t.Fatal("wrong stage", got.Attempt.NativeStage)
		}
	}
	for _, outcome := range []string{"failed", "interrupted"} {
		h.state.Records[0].State = "unknown"
		h.state.Records[0].Outcome = outcome
		journalCall(t, h, q, "unresolved", "attempt-unresolved")
	}
	h, q = journalUnit(t, true)
	for i := 1; i <= MaxRecords; i++ {
		id := fmt.Sprintf("notice-%d", i)
		n := Notice{ID: id, Reference: q.Binding.WorkflowReference}
		h.state.Records = append(h.state.Records, Record{Notice: n, Digest: digestNoticeMode(n, h.state.Selection, WorkReference), Sequence: uint64(i), State: "applied", NativeAccepted: true, NativeSession: q.Binding.NativeThread, Outcome: "completed"})
		h.state.Notification.Attempts = append(h.state.Notification.Attempts, notificationIntent{NoticeID: id, Sequence: uint64(i)})
	}
	h.state.Sequence = MaxRecords
	if err := h.store.write(h.state); err != nil {
		t.Fatal(err)
	}
	before := journalBytes(t, h)
	journalCall(t, h, q, "refused", "capacity-full")
	q.NoticeID = "notice-32"
	h.state.Fence = "held"
	journalCall(t, h, q, "existing-attempt", "")
	if !bytes.Equal(before, journalBytes(t, h)) || len(h.state.Notification.Attempts) != 32 {
		t.Fatal("eviction/extra effect at capacity")
	}
}

// A known encoded-byte rejection happens before the pending marker or intent.
// Completed records use bounded adapter metadata; JSON escaping consumes the
// encoded store budget while the checkpoint itself remains within its limit.
func TestNotificationJournalByteCapacity(t *testing.T) {
	h, q := journalUnit(t, true)
	q.NoticeID = strings.Repeat("n", 128)
	for i := 1; i < MaxRecords; i++ {
		id := fmt.Sprintf("%0128d", i)
		n := Notice{ID: id, Reference: q.Binding.WorkflowReference}
		h.state.Records = append(h.state.Records, Record{Notice: n, Digest: digestNoticeMode(n, h.state.Selection, WorkReference), Sequence: uint64(i), State: "applied", NativeAccepted: true, NativeSession: q.Binding.NativeThread, Outcome: "completed"})
		h.state.Notification.Attempts = append(h.state.Notification.Attempts, notificationIntent{NoticeID: id, Sequence: uint64(i)})
	}
	h.state.Sequence = uint64(len(h.state.Records))
	found := false
	for width := 0; width <= 128 && !found; width++ {
		for i := range h.state.Records {
			h.state.Records[i].NativeTurn = strings.Repeat("<", width)
			h.state.Records[i].NativeReceipt = strings.Repeat("<", width)
		}
		makeCheckpoint := func(n int) json.RawMessage { return json.RawMessage(strconv.Quote(strings.Repeat("x", n))) }
		h.state.Protocol.Checkpoint = makeCheckpoint(0)
		if !notificationStateFits(h.state) {
			continue
		}
		maxPad := ap.MaxCheckpoint - 2
		h.state.Protocol.Checkpoint = makeCheckpoint(maxPad)
		if notificationStateFits(h.state) {
			continue
		}
		lo, hi := 0, maxPad
		for lo < hi {
			mid := (lo + hi + 1) / 2
			h.state.Protocol.Checkpoint = makeCheckpoint(mid)
			if notificationStateFits(h.state) {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		h.state.Protocol.Checkpoint = makeCheckpoint(lo)
		found = true
	}
	if !found || !validProtocolState(h.state) {
		t.Fatal("valid budget-edge fixture not established")
	}
	if err := h.store.write(h.state); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.read(); err != nil {
		t.Fatalf("edge fixture unreadable: %v", err)
	}
	before := journalBytes(t, h)
	if len(before) != localipc.MaxFrame || len(h.state.Records) != MaxRecords-1 {
		t.Fatal("byte-bound control did not isolate remaining count capacity")
	}
	journalCall(t, h, q, "refused", "capacity-full")
	if h.err != nil || !bytes.Equal(before, journalBytes(t, h)) || len(h.state.Records) != MaxRecords-1 {
		t.Fatal("capacity rejection wrote/fenced")
	}
	if _, err := os.Lstat(filepath.Join(h.store.dir, "state.pending")); !os.IsNotExist(err) {
		t.Fatal("capacity rejection began a transaction")
	}
}

// Pending persistence is UNKNOWN, never a negative acceptance proof. A restored
// complete intent survives loss, and a missing binding or new call ID cannot
// rearm it. A separately lost lifecycle also refuses same-binding reinstall.
func TestNotificationJournalFailureAndAttachmentLoss(t *testing.T) {
	h, q := journalUnit(t, true)
	before := journalBytes(t, h)
	if err := os.WriteFile(filepath.Join(h.store.dir, "state.pending"), []byte("fixture-interruption\n"), 0600); err != nil {
		t.Fatal(err)
	}
	journalCall(t, h, q, "unresolved", "attempt-unresolved")
	if h.err == nil || h.state.Fence != "unknown" || len(h.state.Records) != 0 || !bytes.Equal(before, journalBytes(t, h)) {
		t.Fatal("uncertain persistence did not preserve fence")
	}
	if _, _, err := h.store.read(); err == nil || err.Error() != "state_update_unknown" {
		t.Fatal("pending journal treated as absence", err)
	}
	h, q = journalUnit(t, true)
	journalCall(t, h, q, "accepted", "")
	h.loseNotificationBinding()
	if err := h.installNotificationBinding(q.Binding); err != ErrNotificationUnresolved {
		t.Fatal("lost lifecycle automatically rearmed")
	}
	q.RequestID = "fresh-call"
	journalCall(t, h, q, "unresolved", "observation-unavailable")
	if len(h.state.Records) != 1 || len(h.state.Notification.Attempts) != 1 || h.state.Fence != "unknown" {
		t.Fatal("lost original obligation")
	}
}

// Legacy state stays readable. Broker correlation requires its explicit version
// and complete ordered references; downgrade, future and corrupt forms reject.
func TestNotificationJournalStateAdmission(t *testing.T) {
	h, q := journalUnit(t, false)
	if _, exists, err := h.store.read(); err != nil || !exists {
		t.Fatal("legacy work state rejected", err)
	}
	if err := h.installNotificationBinding(q.Binding); err != nil {
		t.Fatal(err)
	}
	journalCall(t, h, q, "accepted", "")
	good := cloneState(h.state)
	for _, mutate := range []func(*State){
		func(s *State) { s.Version = 3 }, func(s *State) { s.Version = 99 }, func(s *State) { s.Notification = nil },
		func(s *State) { s.Notification.Binding.ControllerEpoch = "2" }, func(s *State) { s.Notification.Binding.Execution = "other" },
		func(s *State) { s.Notification.Attempts[0].Sequence = 0 }, func(s *State) { s.Notification.Attempts[0].Sequence = 2 },
		func(s *State) { s.Notification.Attempts[0].NoticeID = "other" }, func(s *State) { s.Notification.Binding.WorkflowReference = "other" },
		func(s *State) { s.Notification.Attempts = append(s.Notification.Attempts, s.Notification.Attempts[0]) },
	} {
		s := cloneState(good)
		mutate(&s)
		if err := h.store.write(s); err != nil {
			t.Fatal(err)
		}
		if _, _, err := h.store.read(); err == nil || err.Error() != "state_corrupt" {
			t.Fatal("bad broker state accepted", err)
		}
	}
	if err := h.store.write(good); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.read(); err != nil {
		t.Fatal("valid broker control rejected", err)
	}
}

// The record guard remains necessary even if an unrelated fixture sets the
// ordinary fence open. With no active binding it must not invoke Prepare; the
// matching installed-binding control invokes the same actual scheduler method.
func TestNotificationJournalSchedulerBindingGuard(t *testing.T) {
	for _, active := range []bool{false, true} {
		h, q := journalUnit(t, true)
		journalCall(t, h, q, "accepted", "")
		if !active {
			h.notificationBinding = nil
		}
		prepared := h.prepareProtocol()
		if prepared != active {
			t.Fatalf("active=%t prepared=%t", active, prepared)
		}
		if got := h.driver.(*journalDriver).prepares.Load(); (got == 1) != active {
			t.Fatalf("active=%t calls=%d", active, got)
		}
		if !active && h.state.Records[0].State != "queued" {
			t.Fatal("unadmitted notification advanced")
		}
	}
}

// A prepared, persisted action still needs the active attachment at the write
// boundary. Removing only the binding (leaving the ordinary fence open) must
// retain the outbox and produce zero actual runner writes. The active control
// retires that same kind of outbox with exactly one write to an owned inert child.
func TestNotificationJournalOutboxBindingGuard(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "notification-outbox-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(dir) })
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			socket := filepath.Join(dir, "r.sock")
			s, err := runner.Start(runner.Config{Socket: socket, Execution: "execution-1", IOTimeout: time.Second,
				Plan: runner.Plan{Binary: binary, Args: []string{"-test.run=^TestNotificationJournalChild$"}, Cwd: dir, Env: []string{"SESSION_HOST_NOTIFICATION_CHILD=1", "GORACE=atexit_sleep_ms=0"}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("owned child-start pid=%d", s.ChildPID())
			t.Cleanup(func() { s.Close(); t.Logf("owned child-end pid=%d", s.ChildPID()) })
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			client, err := runner.Dial(ctx, socket)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { client.Close() })
			claim, err := client.Call(ctx, runner.Request{Op: "claim", Execution: "execution-1", Epoch: 1})
			if err != nil || claim.Code != "ok" {
				t.Fatal("fixture claim", err)
			}
			h, q := journalUnit(t, false)
			h.client, h.cfg.IOTimeout = client, time.Second
			h.state.RunnerInstance, q.Binding.RunnerInstance = claim.Instance, claim.Instance
			if err := h.installNotificationBinding(q.Binding); err != nil {
				t.Fatal(err)
			}
			journalCall(t, h, q, "accepted", "")
			if !h.prepareProtocol() || len(h.state.Protocol.Pending) != 1 {
				t.Fatal("prepared outbox control not established")
			}
			before := journalBytes(t, h)
			if !active {
				h.notificationBinding = nil
			}
			settled := h.reconcileOutbox()
			status, err := h.protocolCall(runner.Request{Op: "status", Execution: h.cfg.Execution, Epoch: h.state.Epoch})
			want := 0
			if active {
				want = 1
			}
			if err != nil || status.Writes != want {
				t.Fatalf("active=%t writes=%d want=%d error=%v", active, status.Writes, want, err)
			}
			if settled != active {
				t.Fatalf("active=%t settled=%t", active, settled)
			}
			if !active && (!bytes.Equal(before, journalBytes(t, h)) || len(h.state.Protocol.Pending) != 1 || h.state.Records[0].State != "unknown") {
				t.Fatal("unadmitted action lost its unresolved journal")
			}
		})
	}
}

// Only this explicitly owned inert Go child runs; no Codex/Source custody or
// real notification admission is implied. Runner Close owns its EOF/reap cleanup.
func TestNotificationJournalChild(t *testing.T) {
	if os.Getenv("SESSION_HOST_NOTIFICATION_CHILD") != "1" {
		return
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

func journalAwait(t *testing.T, claim string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(claim)
}

// An actual replacement Host starts its worker against an inert owned runner,
// retains the old intent, and performs zero writes without renewed admission.
// A fresh matching fixture attachment is a nearby one-write positive control.
func TestNotificationJournalOwnedReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "notification-owned-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(dir) })
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			s, err := runner.Start(runner.Config{Socket: filepath.Join(dir, "r.sock"), Execution: "execution-1", IOTimeout: time.Second,
				Plan: runner.Plan{Binary: binary, Args: []string{"-test.run=^TestNotificationJournalChild$"}, Cwd: dir, Env: []string{"SESSION_HOST_NOTIFICATION_CHILD=1", "GORACE=atexit_sleep_ms=0"}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("owned child-start pid=%d", s.ChildPID())
			t.Cleanup(func() { s.Close(); t.Logf("owned child-end pid=%d", s.ChildPID()) })
			cfg := Config{StateDir: filepath.Join(dir, "state"), Socket: filepath.Join(dir, "r.sock"), Execution: "execution-1", NoticeMode: WorkReference,
				Selection: Selection{Vendor: "provider", Model: "model", Reasoning: "effort"}, ProtocolName: "codex", IOTimeout: time.Second, PollInterval: time.Millisecond}
			cfg.ProtocolFactory = func([]byte) (ap.Driver, uint64, error) { return &journalDriver{}, 0, nil }
			h, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			q := notificationFixture("trigger")
			snapshot := h.Snapshot()
			q.Binding.ControllerEpoch = strconv.FormatUint(snapshot.Epoch, 10)
			q.Binding.RunnerInstance = snapshot.RunnerInstance
			if replacement {
				h.Close()
				store, err := openStore(cfg.StateDir)
				if err != nil {
					t.Fatal(err)
				}
				state, _, err := store.read()
				if err != nil {
					t.Fatal(err)
				}
				// Model durable acceptance followed by interruption before dispatch:
				// same actual metadata/store, no worker and no synthetic native write.
				producer := &Host{state: state, store: store, ctx: context.Background(), driver: &journalDriver{}}
				if err := producer.installNotificationBinding(q.Binding); err != nil {
					t.Fatal(err)
				}
				journalCall(t, producer, q, "accepted", "")
				store.close()
				h, err = Open(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if h.Snapshot().Fence != "unknown" || h.Snapshot().Epoch != snapshot.Epoch+1 {
					t.Fatal("restored broker regained input")
				}
				journalCall(t, h, q, "unresolved", "observation-unavailable")
				fresh := q.Binding
				fresh.ControllerEpoch = strconv.FormatUint(h.Snapshot().Epoch, 10)
				if err := h.installNotificationBinding(fresh); err != ErrNotificationUnresolved {
					t.Fatal("replacement silently migrated binding")
				}
			} else {
				if err := h.installNotificationBinding(q.Binding); err != nil {
					t.Fatal(err)
				}
				journalCall(t, h, q, "accepted", "")
			}
			t.Cleanup(func() { h.Close() })
			d := h.driver.(*journalDriver)
			journalAwait(t, "replacement worker observation", func() bool { return d.observations.Load() > 0 })
			want := 0
			if !replacement {
				want = 1
			}
			journalAwait(t, "owned runner write control", func() bool {
				st := h.Snapshot()
				r, err := h.protocolCall(runner.Request{Op: "status", Execution: cfg.Execution, Epoch: st.Epoch})
				return err == nil && r.Writes == want
			})
			if replacement {
				st := h.Snapshot()
				if len(st.Records) != 1 || st.Records[0].State != "queued" || st.Notification.Binding != q.Binding || len(st.Notification.Attempts) != 1 || d.prepares.Load() != 0 {
					t.Fatal("replacement lost/replayed original intent")
				}
			}
		})
	}
}
