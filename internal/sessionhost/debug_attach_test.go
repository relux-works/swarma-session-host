package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

func debugFixture(t *testing.T) *Host {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := openStore(filepath.Join(root, "state"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	h := &Host{store: s, ctx: ctx, cancel: cancel, kick: make(chan struct{}, 1), state: State{Version: 1, Execution: "fixture", RunnerInstance: "runner", Adapter: "fixture", Selection: Selection{"provider", "model", "effort"}, Epoch: 1, Fence: "open", InputState: InputIdle, DebugAttach: true, Debug: &debugMetadata{Writer: "automatic"}}}
	if err := h.openDebug(false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); h.debug.close(); s.close() })
	return h
}
func debugToken(t *testing.T, h *Host, rights ...string) string {
	t.Helper()
	token, err := h.debugIssueGrant(debugAuthority{"manager", "manager", h.state.Epoch}, rights)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func debugView(t *testing.T, h *Host, rights ...string) string {
	t.Helper()
	token := debugToken(t, h, rights...)
	if err := h.debugAttachView(token); err != nil {
		t.Fatal(err)
	}
	return token
}
func debugWant(t *testing.T, err error, code string) {
	t.Helper()
	if code == "" {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if err == nil || err.Error() != code {
		t.Fatalf("want %q, got %v", code, err)
	}
}
func debugDisk(t *testing.T, h *Host) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(h.store.dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestAdmissionResumeImmutabilitySkeleton(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			var s State
			debugWant(t, admitDebug(&s, false, &enabled), "")
			if s.DebugAttach != enabled {
				t.Fatal("admission bit")
			}
			before, _ := json.Marshal(s)
			changed := !enabled
			debugWant(t, admitDebug(&s, true, &changed), "attach_admission_conflict")
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("conflict mutated execution")
			}
			debugWant(t, admitDebug(&s, true, &enabled), "")
			debugWant(t, admitDebug(&s, true, nil), "")
			if s.DebugAttach != enabled {
				t.Fatal("replacement changed bit")
			}
		})
	}
	legacy := State{}
	on := true
	debugWant(t, admitDebug(&legacy, true, &on), "attach_admission_conflict")
	h := debugFixture(t)
	debugWant(t, h.debugResume(false), "attach_admission_conflict")
	debugWant(t, h.debugResume(true), "")
}
func TestLegacyBytesAndDigestsSkeleton(t *testing.T) {
	// Literal baseline bytes produced by State at 52e82319, before adding fields.
	s := State{Version: 1, Execution: "fixture", RunnerInstance: "runner", Adapter: "fixture", Selection: Selection{"provider", "model", "effort"}, Epoch: 1, Fence: "open", InputState: InputIdle}
	want := `{"version":1,"execution":"fixture","runner_instance":"runner","adapter":"fixture","selection":{"vendor":"provider","model":"model","reasoning":"effort"},"epoch":1,"sequence":0,"fence":"open","records":null,"stop_requested":false,"input_state":"idle","security_stopped":false}`
	got, err := json.Marshal(s)
	if err != nil || string(got) != want {
		t.Fatalf("legacy bytes: %s %v", got, err)
	}
	if !validDebugState(s) {
		t.Fatal("legacy invalid")
	}
	h := &Host{state: s}
	if !h.debugAutomaticLocked() {
		t.Fatal("disabled automation changed")
	}
	debugWant(t, h.debugJournalLocked("notice", "one", nil, false), "")
	_, err = h.debugIssueGrant(debugAuthority{}, []string{"view"})
	debugWant(t, err, "attach_not_enabled")
	s.Debug = &debugMetadata{Writer: "automatic"}
	if validDebugState(s) {
		t.Fatal("debug metadata on disabled execution")
	}
}
func TestSecondAttacherAndWriterSkeleton(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control", "text")
	second := debugToken(t, h, "view", "writer_control")
	debugWant(t, h.debugAttachView(second), "attach_in_use")
	if h.debug.viewer != token {
		t.Fatal("second attach stole slot")
	}
	debugWant(t, h.debugInput(token, "follow_up", 0), "input_writer_required")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	before := debugDisk(t, h)
	debugWant(t, h.debugControl(token, "claim", 0), "input_writer_required")
	if !bytes.Equal(before, debugDisk(t, h)) {
		t.Fatal("second claim mutated writer")
	}
	debugWant(t, h.debugControl(second, "claim", 0), "attach_unauthorized")
	debugWant(t, h.debugControl(token, "detach", 0), "")
	debugWant(t, h.debugAttachView(second), "")
	if h.state.Debug.Writer != "held" || h.state.StopRequested {
		t.Fatal("detach resumed or stopped")
	}
}
func TestGrantRightsByOperationSkeleton(t *testing.T) {
	for _, op := range []string{"claim", "renew", "yield", "resume_automation", "ack", "disposition", "follow_up", "answer_interaction", "stop"} {
		t.Run(op, func(t *testing.T) {
			for _, sufficient := range []bool{false, true} {
				t.Run(fmt.Sprint(sufficient), func(t *testing.T) {
					h := debugFixture(t)
					rights := []string{"view"}
					if sufficient {
						switch op {
						case "follow_up":
							rights = append(rights, "writer_control", "text")
						case "answer_interaction":
							rights = append(rights, "writer_control", "native_response")
						case "stop":
							rights = append(rights, "stop")
						case "ack":
						default:
							rights = append(rights, "writer_control")
						}
					}
					token := debugView(t, h, rights...)
					if op == "renew" || op == "yield" || op == "follow_up" || op == "answer_interaction" {
						h.state.Debug.Writer = "claimed"
						h.debug.leaseUntil = time.Now().Add(time.Minute)
					}
					h.state.Debug.Results = []debugResult{{ID: "result", Thread: "thread", Turn: "turn", Outcome: "completed", Revision: 1, NotificationPending: true}}
					if op == "resume_automation" {
						h.state.Debug.Results[0].Permitted = true
					}
					debugWant(t, h.persistLocked(h.state), "")
					before := cloneState(h.state)
					used := h.debug.journal.used
					var err error
					switch op {
					case "ack":
						err = h.debugDisposition(token, "result", 1, true, false)
					case "disposition":
						err = h.debugDisposition(token, "result", 1, false, true)
					case "follow_up", "answer_interaction":
						err = h.debugInput(token, op, 0)
					case "stop":
						ctx, cancel := context.WithTimeout(t.Context(), time.Second)
						defer cancel()
						err = h.debugStop(ctx, token)
					default:
						err = h.debugControl(token, op, 0)
					}
					if !sufficient && op != "ack" {
						debugWant(t, err, "input_right_required")
						if !reflect.DeepEqual(before, h.state) || used != h.debug.journal.used {
							t.Fatal("denied operation mutated state/journal")
						}
						return
					}
					expected := ""
					if op == "follow_up" {
						expected = "input_operation_unsupported"
					}
					if op == "answer_interaction" {
						expected = "interaction_unsupported"
					}
					debugWant(t, err, expected)
				})
			}
		})
	}
	h := debugFixture(t)
	_, err := h.debugIssueGrant(debugAuthority{"foreign", "worker", 1}, []string{"view"})
	debugWant(t, err, "attach_unauthorized")
	_, err = h.debugIssueGrant(debugAuthority{"manager", "manager", 1}, []string{"view", "admin"})
	debugWant(t, err, "input_invalid")
	noView := debugToken(t, h, "text")
	debugWant(t, h.debugAttachView(noView), "attach_unauthorized")
}
func TestLeaseYieldTimeoutDetachSkeleton(t *testing.T) {
	for _, mode := range []string{"view_detach", "writer_detach", "yield", "timeout", "expiry"} {
		t.Run(mode, func(t *testing.T) {
			h := debugFixture(t)
			now := time.Now()
			h.debug.now = func() time.Time { return now }
			h.debug.tail.now = h.debug.now
			token := debugView(t, h, "view", "writer_control")
			if mode != "view_detach" {
				debugWant(t, h.debugControl(token, "claim", 0), "")
			}
			switch mode {
			case "yield":
				debugWant(t, h.debugControl(token, "yield", h.state.Debug.WriterRevision), "")
			case "timeout":
				now = now.Add(60 * time.Second)
				h.debugExpireLeaseLocked()
			case "expiry":
				now = now.Add(5 * time.Minute)
				_, err := h.debugRead(token, 0)
				debugWant(t, err, "attach_grant_revoked")
			default:
				debugWant(t, h.debugControl(token, "detach", 0), "")
			}
			want := "held"
			if mode == "view_detach" {
				want = "automatic"
			}
			if h.state.Debug.Writer != want || h.state.StopRequested {
				t.Fatalf("%s %+v", mode, h.state.Debug)
			}
		})
	}
}
func TestStopRightIndependentOfWriterSkeleton(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "stop")
	h.state.InputState = InputBusy
	h.state.Debug.Writer = "held"
	h.debug.journal.used = debugJournalCap - debugControlReserve - 2000
	h.debug.journal.reserved = 2000
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	debugWant(t, h.debugStop(ctx, token), "")
	if !h.state.StopRequested || h.state.Debug.Retired {
		t.Fatal("stop mistaken for retirement")
	}
	if _, err := os.Stat(filepath.Join(h.debug.journal.dir, "input.journal")); err != nil {
		t.Fatal("stop purged journal")
	}
}
func TestBusyAndUnsupportedInputSkeleton(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control", "text")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	rev := h.state.Debug.WriterRevision
	debugWant(t, h.debugInput(token, "follow_up", rev-1), "input_observation_stale")
	h.state.InputState = InputBusy
	debugWant(t, h.debugInput(token, "follow_up", rev), "input_busy")
	h.state.InputState = InputIdle
	debugWant(t, h.debugInput(token, "follow_up", rev), "input_operation_unsupported")
	debugWant(t, h.debugInput(token, "answer_interaction", rev), "input_right_required")
	h.state.Fence = "unknown"
	debugWant(t, h.debugInput(token, "follow_up", rev), "input_fenced")
	h.state.StopRequested = true
	debugWant(t, h.debugInput(token, "follow_up", rev), "input_stopped")
}
func TestJournalSenderTimeBeforeEffectSkeleton(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	data, err := os.ReadFile(filepath.Join(h.debug.journal.dir, "input.journal"))
	if err != nil {
		t.Fatal(err)
	}
	var e debugJournalEntry
	if err := json.Unmarshal(bytes.TrimSpace(data), &e); err != nil {
		t.Fatal(err)
	}
	if e.Actor != "manager" || e.Role != "manager" || e.Sequence != 1 || e.At.IsZero() || e.Execution != "fixture" || e.Epoch != 1 || e.Kind != "claim" || e.Stage != "accepted" || e.Digest == "" || e.Observation != InputIdle {
		t.Fatalf("missing attribution: %+v", e)
	}
	before := cloneState(h.state)
	h.debug.journal.file.Close()
	debugWant(t, h.debugControl(token, "yield", h.state.Debug.WriterRevision), "input_storage_unavailable")
	if h.state.Debug.Writer != before.Debug.Writer || h.state.Debug.WriterRevision != before.Debug.WriterRevision {
		t.Fatal("failed durability accepted control")
	}
}
func TestJournalCapAndReserveSkeleton(t *testing.T) {
	for _, fraction := range []int{123456789, 123456000} {
		for _, delta := range []int64{-1, 0, 1} {
			t.Run(fmt.Sprintf("fraction-%d/boundary-%d", fraction, delta), func(t *testing.T) {
				h := debugFixture(t)
				j := h.debug.journal
				now := time.Date(2026, 11, 9, 0, 0, 0, fraction, time.UTC)
				j.started = now
				e := debugJournalEntry{Actor: "host", Role: "host", Kind: "notice", ID: "one", Execution: "fixture", Epoch: 1, Digest: wireDigest(nil)}
				// Independently encode this fixture's complete host-assigned entry.
				probe := e
				probe.Contract = debugContract
				probe.Sequence = 1
				probe.At = now.UTC()
				probe.Stage = "accepted"
				b, err := json.Marshal(probe)
				if err != nil {
					t.Fatal(err)
				}
				cost := int64(len(b) + 1 + debugReceiptReserve)
				j.used = debugJournalCap - debugControlReserve - cost + delta
				before := j.used
				want := ""
				if delta > 0 {
					want = "input_capacity_exceeded"
				}
				debugWant(t, j.append(e, false, now), want)
				if delta > 0 {
					if j.used != before || j.reserved != 0 || j.sequence != 0 {
						t.Fatal("overflow mutated journal")
					}
				} else {
					if j.used+j.reserved != debugJournalCap-debugControlReserve+delta {
						t.Fatal("encoded frontier differs")
					}
					debugWant(t, j.append(e, false, now), "input_capacity_exceeded")
				}
				e.Kind = "stop"
				debugWant(t, j.append(e, true, now), "")
				if j.used+j.reserved > debugJournalCap {
					t.Fatal("protected overflow")
				}
			})
		}
	}
}
func TestJournalPrivacyAndCrashSkeleton(t *testing.T) {
	for _, mode := range []string{"valid", "mode", "symlink", "directory", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(root, "debug-attach")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "input.journal")
			switch mode {
			case "symlink":
				if err := os.Symlink(filepath.Join(root, "target"), path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			default:
				perm := os.FileMode(0600)
				if mode == "mode" {
					perm = 0644
				}
				if err := os.WriteFile(path, []byte("private-input"), perm); err != nil {
					t.Fatal(err)
				}
			}
			j, err := openDebugJournal(root, mode == "replacement")
			if mode == "valid" || mode == "replacement" {
				debugWant(t, err, "")
				defer j.close()
				if mode == "replacement" {
					debugWant(t, j.append(debugJournalEntry{}, false, time.Now()), "input_storage_unavailable")
				}
			} else {
				debugWant(t, err, "input_storage_unavailable")
			}
		})
	}
}
func TestRetirementPurgeProofSkeleton(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control")
	h.debug.tail.observe([]byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"output-canary"}}`))
	debugWant(t, h.debugControl(token, "claim", 0), "")
	debugWant(t, h.debugRetire(), "input_fenced")
	h.state.StopRequested = true
	h.state.Records = []Record{{State: "unknown"}}
	debugWant(t, h.debugRetire(), "input_effect_unknown")
	h.state.Records = nil
	path := filepath.Join(h.debug.journal.dir, "input.journal")
	heldFD, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer heldFD.Close()
	// The host owns no leaked reader descriptor; a separate fixture FD witnesses
	// the stated logical-unlink limit rather than claiming physical erasure.
	extra := filepath.Join(h.debug.journal.dir, "unrecognized.pending")
	if err := os.WriteFile(extra, []byte("pending-canary"), 0600); err != nil {
		t.Fatal(err)
	}
	debugWant(t, h.debugRetire(), "input_storage_unavailable")
	if h.state.Debug.Retired {
		t.Fatal("purge failure declared retirement")
	}
	if err := os.Remove(extra); err != nil {
		t.Fatal(err)
	}
	debugWant(t, h.debugRetire(), "")
	if !h.state.Debug.Retired || h.debug.tail.used != 0 || h.debug.tail.events.Len() != 0 || len(h.debug.grants) != 0 || h.debug.journal.file != nil {
		t.Fatal("retirement left host custody")
	}
	if _, err := os.Stat(h.debug.journal.dir); !os.IsNotExist(err) {
		t.Fatal("journal remains accessible")
	}
	_, err = h.debugRead(token, 0)
	debugWant(t, err, "attach_grant_revoked")
	if strings.Contains(string(debugDisk(t, h)), "canary") {
		t.Fatal("body in snapshot")
	}
}
func TestTerminalFactBlocksQueuedNoticeSkeleton(t *testing.T) {
	h := debugFixture(t)
	// This is the same host transition hook used before protocol completion idle
	// is committed. A view/claim is deliberately absent when the fact arrives.
	next := cloneState(h.state)
	debugWant(t, h.debugTerminalLocked(&next, "thread", "turn", "completed"), "")
	debugWant(t, h.persistLocked(next), "")
	if h.debugAutomaticLocked() {
		t.Fatal("terminal did not block automatic delivery")
	}
	token := debugView(t, h, "view", "writer_control")
	r := h.state.Debug.Results[0]
	debugWant(t, h.debugDisposition(token, r.ID, r.Revision, true, false), "")
	if h.debugAutomaticLocked() {
		t.Fatal("ack disposition conflated")
	}
	debugWant(t, h.debugDisposition(token, r.ID, r.Revision, false, true), "input_observation_stale")
	debugWant(t, h.debugDisposition(token, r.ID, r.Revision+1, false, true), "")
	if !h.debugAutomaticLocked() {
		t.Fatal("permitted result still blocks")
	}
	restored, exists, err := h.store.read()
	debugWant(t, err, "")
	if !exists || !restored.DebugAttach || len(restored.Debug.Results) != 1 || !restored.Debug.Results[0].Ack || !restored.Debug.Results[0].Permitted {
		t.Fatal("terminal fact not durable")
	}
	copy := h.Snapshot()
	copy.Debug.Results[0].Permitted = false
	if !h.state.Debug.Results[0].Permitted {
		t.Fatal("snapshot aliases facts")
	}
}
func TestClaimVsNoticeBothOrdersSkeleton(t *testing.T) {
	for _, claimFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(claimFirst), func(t *testing.T) {
			h := debugFixture(t)
			token := debugView(t, h, "view", "writer_control")
			if claimFirst {
				debugWant(t, h.debugControl(token, "claim", 0), "")
			}
			if h.debugAutomaticLocked() == claimFirst {
				t.Fatal("wrong notice eligibility")
			}
			if !claimFirst {
				debugWant(t, h.debugJournalLocked("notice", "reserved-first", nil, false), "")
				debugWant(t, h.debugControl(token, "claim", 0), "")
			}
			if h.debugAutomaticLocked() {
				t.Fatal("claim did not hold subsequent notice")
			}
		})
	}
}
func TestPublicPrivateRegistryPartitionSkeleton(t *testing.T) {
	codes := []string{"input_writer_required", "input_busy", "input_observation_stale", "input_fenced", "input_stopped", "input_invalid", "input_conflict", "input_capacity_exceeded", "input_storage_unavailable", "input_effect_unknown", "input_operation_unsupported", "interaction_unsupported", "interaction_stale", "interaction_owner_required", "input_right_required"}
	for _, code := range codes {
		b, err := debugRefusal(code)
		debugWant(t, err, "")
		want := `{"contract":"host-debug-attach/1","ok":false,"code":"` + code + `","details":{}}` + "\n"
		if string(b) != want {
			t.Fatal(string(b))
		}
	}
	for _, code := range []string{"attach_in_use", "attach_not_enabled", "unknown"} {
		_, err := debugRefusal(code)
		debugWant(t, err, "input_invalid")
	}
}

// Concurrent slow-reader/observer exercise checks race safety and bounded
// retained storage. It does not model real native scheduling or descriptors.
func TestTailBytesAgeAndSlowViewerSkeleton(t *testing.T) {
	now := time.Now()
	r := &debugTail{now: func() time.Time { return now }, epoch: 1, execution: "fixture"}
	frame := []byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"` + strings.Repeat("x", 60_000) + `"}}`)
	for i := 0; i < 300; i++ {
		r.observe(frame)
	}
	if r.used > debugTailBytes || r.events.Len() == 0 || r.lost == 0 {
		t.Fatalf("unbounded or empty tail: %d", r.used)
	}
	gap := r.read(0)
	if gap.Cause != "capacity" || gap.GapThrough == 0 {
		t.Fatal("capacity gap missing")
	}
	control := r.read(gap.GapThrough)
	if !bytes.Contains(control.Event, []byte("structured_payload")) {
		t.Fatal("no surviving payload")
	}
	now = now.Add(899 * time.Second)
	if len(r.read(gap.GapThrough).Event) == 0 {
		t.Fatal("early TTL eviction")
	}
	now = now.Add(time.Second)
	r.expire()
	if r.used != 0 || r.events.Len() != 0 || r.read(0).Cause != "age" {
		t.Fatal("900s bound")
	}
	concurrent := &debugTail{now: time.Now, epoch: 1, execution: "fixture"}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				concurrent.observe(frame)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			concurrent.read(0)
		}
	}()
	wg.Wait()
	if concurrent.used > debugTailBytes {
		t.Fatal("concurrent overflow")
	}
	concurrent.close()
	if concurrent.used != 0 {
		t.Fatal("closed tail retains bytes")
	}
}
func TestTailSilenceExpiryDoneAwareSkeleton(t *testing.T) {
	h := debugFixture(t)
	now := time.Now()
	h.debug.tail.now = func() time.Time { return now }
	h.debug.tail.observe([]byte(`{"method":"item/completed","params":{}}`))
	if delay := h.debug.tail.expiryDelay(); delay != 900*time.Second {
		t.Fatalf("expiry delay %v", delay)
	}
	now = now.Add(900 * time.Second)
	h.wg.Add(1)
	go h.debugExpiryTimerLoop()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		h.debug.tail.mu.Lock()
		used := h.debug.tail.used
		h.debug.tail.mu.Unlock()
		if used == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("silence expiry did not run")
		case <-ticker.C:
		}
	}
	h.cancel()
	done := make(chan struct{})
	go func() { h.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("expiry did not join")
	}
}

func TestTerminalFactTransitionSkeleton(t *testing.T) {
	h := debugFixture(t)
	n := Notice{"one", "synthetic/one"}
	h.state.Sequence = 1
	h.state.Records = []Record{{Notice: n, Digest: digestNotice(n, h.state.Selection), Sequence: 1, State: "unknown", NativeAccepted: true, NativeSession: "thread", NativeTurn: "turn"}}
	d := factDriver{ap.Descriptor{Version: ap.Version, ObservedRelease: "schematic", Name: "fixture", PrimaryTransport: "pipe", Capabilities: ap.Capabilities{NativeTurnIdentity: true}}}
	h.driver = d
	h.state.Protocol = &ProtocolState{BoundaryVersion: ap.Version, ObservedRelease: "schematic", Checkpoint: json.RawMessage(`{}`), View: ap.View{InputID: "one", NativeSession: "thread", CurrentTurn: "turn", Activity: ap.Busy}}
	transition := ap.Transition{Checkpoint: json.RawMessage(`{}`), View: ap.View{InputID: "one", NativeSession: "thread", CurrentTurn: "turn", Activity: ap.Idle}, Facts: []ap.Fact{{Kind: ap.TurnCompleted, InputID: "one", NativeSession: "thread", NativeID: "turn"}}}
	debugWant(t, h.persistTransitionLocked(transition), "")
	if h.state.InputState != InputIdle || h.debugAutomaticLocked() || len(h.state.Debug.Results) != 1 {
		t.Fatal("idle escaped without durable terminal block")
	}
	debugWant(t, h.persistTransitionLocked(transition), "")
	if len(h.state.Debug.Results) != 1 {
		t.Fatal("terminal retry duplicates fact")
	}
	h2 := debugFixture(t)
	h2.state = cloneState(h.state)
	h2.driver = d
	h2.debug.journal.file.Close()
	transition.Facts[0].InputID = "one"
	h2.state.Records[0].Outcome = ""
	h2.state.Records[0].State = "unknown"
	h2.state.InputState = InputBusy
	debugWant(t, h2.persistTransitionLocked(transition), "input_storage_unavailable")
	if h2.state.InputState == InputIdle {
		t.Fatal("failed fact commit published eligible idle")
	}
}

func TestGrantAllowlistAndRevocationSkeleton(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	before := debugDisk(t, h)
	debugWant(t, h.debugRevoke(debugAuthority{"other", "manager", 1}, token), "input_right_required")
	if !bytes.Equal(before, debugDisk(t, h)) || h.debug.viewer != token {
		t.Fatal("unauthorized revoke mutated slot")
	}
	debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", 1}, token), "")
	if h.debug.viewer != "" || h.state.Debug.Writer != "held" {
		t.Fatal("revoke left authority")
	}
	_, err := h.debugRead(token, 0)
	debugWant(t, err, "attach_grant_revoked")
	fresh := debugToken(t, h, "view")
	debugWant(t, h.debugAttachView(fresh), "")
}
func TestJournalCapActualStorageSkeleton(t *testing.T) {
	h := debugFixture(t)
	j := h.debug.journal
	e := debugJournalEntry{Actor: "manager", Role: "manager", Kind: "follow_up", ID: "fixture-input", Execution: "fixture", Epoch: 1, Digest: wireDigest([]byte("input-canary")), Body: bytes.Repeat([]byte("x"), 3000)}
	accepted := 0
	for {
		err := j.append(e, false, time.Now())
		if err != nil {
			debugWant(t, err, "input_capacity_exceeded")
			break
		}
		accepted++
		if accepted > 2000 {
			t.Fatal("journal failed to bound")
		}
	}
	info, err := j.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if accepted == 0 || info.Size() != j.used || info.Size()+j.reserved > debugJournalCap-debugControlReserve {
		t.Fatal("actual storage and reservation diverged")
	}
	e.Body = nil
	e.Kind = "stop"
	debugWant(t, j.append(e, true, time.Now()), "")
	if j.used+j.reserved > debugJournalCap {
		t.Fatal("control bypassed total cap")
	}
}
func TestHostLossTailGapSkeleton(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view", "writer_control")
	debugWant(t, h.debugControl(token, "claim", 0), "")
	h.debug.tail.observe([]byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"lost-canary"}}`))
	h.debug.close()
	h.state.Epoch++
	debugWant(t, h.openDebug(true), "")
	_, err := h.debugRead(token, 0)
	debugWant(t, err, "attach_grant_revoked")
	if h.state.Debug.Writer != "held" {
		t.Fatal("replacement restored writer")
	}
	fresh := debugView(t, h, "view")
	read, err := h.debugRead(fresh, 0)
	debugWant(t, err, "")
	if read.Cause != "host_replaced" || len(read.Event) != 0 {
		t.Fatal("history loss not reported")
	}
	h.debug.tail.observe([]byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"new-canary"}}`))
	read, err = h.debugRead(fresh, 0)
	debugWant(t, err, "")
	if !bytes.Contains(read.Event, []byte("new-canary")) || bytes.Contains(read.Event, []byte("lost-canary")) {
		t.Fatal("replacement resurrected history")
	}
	off := false
	debugWant(t, h.persistLocked(State{DebugAttach: off}), "attach_admission_conflict")
}

func TestTailBytesExactBoundarySkeleton(t *testing.T) {
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	r := &debugTail{now: func() time.Time { return now }, epoch: 1, execution: "fixture"}
	frame := func(n int) []byte {
		return []byte(`{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"` + strings.Repeat("x", n) + `"}}`)
	}
	for debugTailBytes-r.used > 62_000 {
		r.observe(frame(60_000))
	}
	// Independently measure the allocated retained events, then fit one bounded
	// frame exactly into the remaining encoded-envelope + metadata budget.
	var allocated int
	for e := r.events.Front(); e != nil; e = e.Next() {
		allocated += cap(e.Value.(*debugEvent).data) + 128
	}
	if allocated != r.used {
		t.Fatal("unaccounted retained allocation")
	}
	base, _ := json.Marshal(debugEnvelope{Contract: debugContract, Kind: "structured_payload", Epoch: 1, Sequence: r.next + 1, At: now, Execution: "fixture", Thread: "thread", Turn: "turn", Payload: frame(0)})
	filler := debugTailBytes - allocated - len(base) - 1 - 128
	if filler < 0 || filler > 60_000 {
		t.Fatalf("invalid fixture remainder %d", filler)
	}
	r.observe(frame(filler))
	if r.used != debugTailBytes || r.lost != 0 {
		t.Fatalf("exact capacity not preserved: used=%d lost=%d", r.used, r.lost)
	}
	r.observe(frame(0))
	if r.used > debugTailBytes || r.lost == 0 || r.read(0).Cause != "capacity" {
		t.Fatal("next event failed to evict")
	}
}
func TestGrantAfterCloseAndStaleViewSkeleton(t *testing.T) {
	h := debugFixture(t)
	token := debugView(t, h, "view")
	h.debug.tail.observe([]byte(`{"method":"item/completed","params":{}}`))
	live, err := h.debugRead(token, 0)
	debugWant(t, err, "")
	if live.Status != "live" {
		t.Fatal(live.Status)
	}
	h.protocolFailed = true
	stale, err := h.debugRead(token, 0)
	debugWant(t, err, "")
	if stale.Status != "stale" || len(stale.Event) == 0 {
		t.Fatal("lost stale diagnostic")
	}
	h.cancel()
	h.debug.close()
	_, err = h.debugRead(token, 0)
	debugWant(t, err, "attach_grant_revoked")
	_, err = h.debugIssueGrant(debugAuthority{"manager", "manager", 1}, []string{"view"})
	debugWant(t, err, "attach_binding_unavailable")
}

func TestClaimVsNoticeResumeFencesSkeleton(t *testing.T) {
	for _, fence := range []string{"eligible", "outbox", "unknown_input", "terminal", "busy", "security", "recovering"} {
		t.Run(fence, func(t *testing.T) {
			h := debugFixture(t)
			token := debugView(t, h, "view", "writer_control")
			debugWant(t, h.debugControl(token, "claim", 0), "")
			revision := h.state.Debug.WriterRevision
			h.state.Protocol = &ProtocolState{BoundaryVersion: ap.Version, ObservedRelease: "schematic", Checkpoint: json.RawMessage(`{}`), View: ap.View{NativeSession: "thread", Activity: ap.Idle}}
			switch fence {
			case "outbox":
				h.state.Protocol.WireSequence = 1
				h.state.Protocol.Pending = []WireAction{{Sequence: 1, Key: "reserved", Data: []byte("control\n"), Digest: wireDigest([]byte("control\n"))}}
			case "unknown_input":
				h.state.Records = []Record{{State: "unknown"}}
			case "terminal":
				h.state.Debug.Results = []debugResult{{ID: "result", Revision: 1}}
			case "busy":
				h.state.InputState = InputBusy
			case "security":
				h.state.SecurityStopped = true
			case "recovering":
				h.recovering = true
			}
			before := cloneState(h.state)
			used := h.debug.journal.used
			err := h.debugControl(token, "resume_automation", revision)
			if fence == "eligible" {
				debugWant(t, err, "")
				if h.state.Debug.Writer != "automatic" {
					t.Fatal("eligible resume blocked")
				}
				return
			}
			debugWant(t, err, "input_fenced")
			if !reflect.DeepEqual(before, h.state) || used != h.debug.journal.used {
				t.Fatal("refused resume changed ownership/journal")
			}
		})
	}
}
