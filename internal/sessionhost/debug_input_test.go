package sessionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

// inertEffector is the B fixture implementation of the private effect seam. It
// counts and retains issued frames, can block at a deterministic barrier and
// can return a lost reply. It is not a native adapter and writes nothing.
type inertEffector struct {
	mu        sync.Mutex
	frames    [][]byte
	issued    int
	code      string
	err       error
	encodeErr error
	block     chan struct{}
	started   chan struct{}
	classes   []string
}

func (e *inertEffector) debugEncode(in debugInputIntent) ([]byte, error) {
	if e.encodeErr != nil {
		return nil, e.encodeErr
	}
	var frame []byte
	var err error
	if in.Kind == "answer_interaction" {
		frame, err = json.Marshal(struct {
			ID     uint64 `json:"id"`
			Result struct {
				Answer string `json:"answer"`
			} `json:"result"`
		}{ID: in.RequestID, Result: struct {
			Answer string `json:"answer"`
		}{in.Option}})
	} else {
		frame, err = json.Marshal(struct {
			Method string `json:"method"`
			Params struct {
				Text string `json:"text"`
			} `json:"params"`
		}{Method: "turn/start", Params: struct {
			Text string `json:"text"`
		}{in.Text}})
	}
	if err != nil {
		return nil, err
	}
	return append(frame, '\n'), nil
}
func (e *inertEffector) debugIssue(ctx context.Context, effect debugEffect) (string, error) {
	e.mu.Lock()
	started, block := e.started, e.block
	e.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.issued++
	e.frames = append(e.frames, bytes.Clone(effect.Frame))
	if e.err != nil {
		return "", e.err
	}
	if e.code == "" {
		return "written", nil
	}
	return e.code, nil
}
func (e *inertEffector) debugClasses() []string { return e.classes }
func (e *inertEffector) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.issued
}

func debugEffectFixture(t *testing.T) (*Host, *inertEffector) {
	t.Helper()
	h := debugFixture(t)
	e := &inertEffector{classes: []string{"item/tool/requestUserInput"}}
	h.debugEffects = e
	h.cfg.IOTimeout = 10 * time.Second
	return h, e
}
func debugWriter(t *testing.T, h *Host, rights ...string) (string, uint64) {
	t.Helper()
	token := debugView(t, h, rights...)
	debugWant(t, h.debugControl(token, "claim", 0), "")
	return token, h.state.Debug.WriterRevision
}

// JournalSenderTimeBeforeEffect (inert text): an accepted follow-up has host
// sender/time/sequence/target/revision attribution and its whole typed intent
// durable before the single inert effect, the receipt consumes its reservation
// and the intent type carries no caller-supplied actor or time. Each failed
// durability boundary (journal write, sync, snapshot commit) refuses with zero
// effect and no accepted record; stop and notice intents follow the same rule.
func TestJournalSenderTimeBeforeEffectInertText(t *testing.T) {
	h, e := debugEffectFixture(t)
	now := time.Date(2026, 11, 16, 1, 2, 3, 0, time.UTC)
	h.debug.now = func() time.Time { return now }
	h.debug.journal.started = now
	token, revision := debugWriter(t, h, "view", "writer_control", "text")
	intent := debugInputIntent{Kind: "follow_up", ID: "reply-1", Text: "forged-free body", ExpectedRevision: revision}
	if encoded, _ := json.Marshal(intent); bytes.Contains(encoded, []byte("actor")) || bytes.Contains(encoded, []byte(`"at"`)) {
		t.Fatal("intent type admits caller-supplied attribution")
	}
	record, err := h.debugInput(token, intent)
	debugWant(t, err, "")
	if record.Stage != "write_known" || record.Actor != "manager" || record.Role != "manager" || record.Sequence != 2 || !record.IssuedAt.Equal(now) || e.count() != 1 {
		t.Fatalf("record %+v effects=%d", record, e.count())
	}
	entries := debugEntries(t, h)
	if len(entries) != 3 || entries[1].Kind != "follow_up" || entries[1].Actor != "manager" || !entries[1].At.Equal(now) || entries[1].Sequence != 2 || entries[1].WriterRevision != revision || entries[1].Stage != "accepted" {
		t.Fatalf("attribution missing: %+v", entries)
	}
	body, _ := json.Marshal(intent)
	if !bytes.Equal(entries[1].Body, body) || entries[1].Digest != wireDigest(body) || record.Digest != entries[1].Digest {
		t.Fatal("journaled intent is not the whole typed intent")
	}
	// The receipt names the attributed accepted frame (actor plus ID) by its
	// sequence; it repeats no attribution members of its own.
	if entries[2].Kind != "receipt" || entries[2].Stage != "write_known" || entries[2].ID != "reply-1" || entries[2].Digest != record.Digest || entries[2].Ref != entries[1].Sequence || entries[2].Actor != "" || entries[2].Execution != "" {
		t.Fatalf("receipt frame %+v", entries[2])
	}
	if !bytes.Equal(e.frames[0], []byte(`{"method":"turn/start","params":{"text":"forged-free body"}}`+"\n")) {
		t.Fatalf("effect frame %q", e.frames[0])
	}
	if h.state.Debug.Inputs[0].Stage != "write_known" || strings.Contains(string(debugDisk(t, h)), "forged-free") {
		t.Fatal("snapshot lost the stage or copied the body")
	}
	for _, failure := range []string{"write", "sync", "commit", "stop", "notice"} {
		t.Run(failure, func(t *testing.T) {
			h, e := debugEffectFixture(t)
			token, revision := debugWriter(t, h, "view", "writer_control", "text", "stop")
			before := cloneState(h.state)
			seq := h.debug.journal.sequence
			j := h.debug.journal
			switch failure {
			case "write":
				j.file.Close()
			case "sync":
				reader, writer, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { reader.Close(); writer.Close() })
				j.file.Close()
				j.file = writer
			case "commit":
				if err := os.WriteFile(filepath.Join(h.store.dir, "state.pending"), []byte("fixture"), 0600); err != nil {
					t.Fatal(err)
				}
			case "stop", "notice":
				j.file.Close()
			}
			var err error
			switch failure {
			case "stop":
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				err = h.debugStop(ctx, token)
				if h.state.StopRequested {
					t.Fatal("unjournaled stop intent accepted")
				}
			case "notice":
				err = h.debugJournalLocked("notice", "queued", nil, false)
			default:
				_, err = h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "reply", Text: "hello", ExpectedRevision: revision})
			}
			if err == nil {
				t.Fatal("failed durability accepted input")
			}
			if failure == "commit" {
				if err.Error() != "host_storage_unavailable" || h.debug.journal.sequence != seq+1 {
					t.Fatalf("commit failure %v seq=%d", err, h.debug.journal.sequence)
				}
			} else if err.Error() != "input_storage_unavailable" || h.debug.journal.sequence != seq {
				t.Fatalf("storage failure %v", err)
			}
			if e.count() != 0 || len(h.state.Debug.Inputs) != 0 || !reflect.DeepEqual(before.Debug, h.state.Debug) {
				t.Fatal("failed durability produced an effect or accepted record")
			}
		})
	}
}

// JournalCapAndReserve (text): the exact admissible used+reserved frontier
// accepts one real encoded follow-up, one more byte refuses V16 before any
// effect or record, the receipt consumes exactly its reservation, an exact
// retry spends no second reservation or sequence, the worst-case receipt frame
// fits the fixed reserve, and a broken file is reported V17 rather than cured
// by reserve space.
func TestJournalCapAndReserveInertText(t *testing.T) {
	for _, delta := range []int64{0, 1} {
		h, e := debugEffectFixture(t)
		now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
		h.debug.now = func() time.Time { return now }
		h.debug.journal.started = now
		token, revision := debugWriter(t, h, "view", "writer_control", "text")
		intent := debugInputIntent{Kind: "follow_up", ID: "reply", Text: strings.Repeat("x", 100), ExpectedRevision: revision}
		body, _ := json.Marshal(intent)
		j := h.debug.journal
		probe := debugJournalEntry{Contract: debugContract, Sequence: j.sequence + 1, At: now, Actor: "manager", Role: "manager", Kind: "follow_up", ID: "reply", Execution: "fixture", Epoch: 1, GrantRevision: 1, WriterRevision: revision, Observation: InputIdle, Digest: wireDigest(body), Body: body, Stage: "accepted"}
		encoded, err := json.Marshal(probe)
		if err != nil {
			t.Fatal(err)
		}
		cost := int64(len(encoded) + 1 + debugReceiptReserve)
		j.used = debugJournalCap - debugControlReserve - j.reserved - cost + delta
		used, reserved, seq := j.used, j.reserved, j.sequence
		record, err := h.debugInput(token, intent)
		if delta > 0 {
			debugWant(t, err, "input_capacity_exceeded")
			if e.count() != 0 || len(h.state.Debug.Inputs) != 0 || j.used != used || j.reserved != reserved || j.sequence != seq {
				t.Fatal("overflow accepted, reserved or issued")
			}
			continue
		}
		debugWant(t, err, "")
		if record.Stage != "write_known" || e.count() != 1 || j.reserved != reserved || j.sequence != seq+2 {
			t.Fatalf("exact frontier: %+v reserved=%d seq=%d", record, j.reserved, j.sequence)
		}
		if j.used+j.reserved > debugJournalCap-debugControlReserve+debugReceiptReserve {
			t.Fatal("receipt exceeded its reservation")
		}
		retry, err := h.debugInput(token, intent)
		debugWant(t, err, "")
		if retry != record || e.count() != 1 || j.sequence != seq+2 || j.reserved != reserved {
			t.Fatal("retry spent a second reservation or effect")
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		g := h.debug.grants[token]
		g.rights = append(g.rights, "stop")
		h.debug.grants[token] = g
		debugWant(t, h.debugStop(ctx, token), "")
		if j.used+j.reserved > debugJournalCap {
			t.Fatal("stop obligation exceeded the total cap")
		}
	}
	worst := debugJournalEntry{Contract: debugContract, Sequence: ^uint64(0), At: time.Date(2026, 11, 16, 0, 0, 0, 123456789, time.UTC), Elapsed: 1<<63 - 1, Kind: "receipt", ID: strings.Repeat("i", 128), Digest: strings.Repeat("f", 64), Stage: "effect_unknown", Ref: ^uint64(0)}
	encoded, err := json.Marshal(worst)
	if err != nil || len(encoded)+1 > debugReceiptReserve {
		t.Fatalf("worst-case receipt %d exceeds reserve %d", len(encoded)+1, debugReceiptReserve)
	}
	h, e := debugEffectFixture(t)
	token, revision := debugWriter(t, h, "view", "writer_control", "text", "stop")
	h.debug.journal.file.Close()
	_, err = h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "reply", Text: "x", ExpectedRevision: revision})
	debugWant(t, err, "input_storage_unavailable")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	debugWant(t, h.debugStop(ctx, token), "input_storage_unavailable")
	if e.count() != 0 || h.state.StopRequested {
		t.Fatal("broken storage was cured by reserve")
	}
}

// TerminalReplyWithPendingDisposition (inert adapter): while a terminal fact
// blocks the queued automatic notice, the minimal view+writer_control+text
// grant claims and submits exactly one inert reply; the reply neither
// dispositions the fact nor reopens automatic delivery, its completion creates
// a second blocking fact, and only explicit dispositions of both facts plus an
// eligible resume reopen delivery. Busy, stale turn, stop, pending native
// interaction and missing text right cannot be bypassed.
func TestTerminalReplyWithPendingDispositionInert(t *testing.T) {
	h, e := debugEffectFixture(t)
	next := cloneState(h.state)
	debugWant(t, h.debugTerminalLocked(&next, "notice-1", "thread", "turn-1", "completed"), "")
	debugWant(t, h.persistLocked(next), "")
	if h.debugAutomaticLocked() {
		t.Fatal("terminal fact did not block delivery")
	}
	noText := debugView(t, h, "view", "writer_control")
	debugWant(t, h.debugControl(noText, "claim", 0), "")
	debugWant(t, debugText(h, noText, "reply", "may I continue?", h.state.Debug.WriterRevision, "turn-1"), "input_right_required")
	debugWant(t, h.debugControl(noText, "detach", h.state.Debug.WriterRevision), "")
	token, revision := debugWriter(t, h, "view", "writer_control", "text")
	debugWant(t, debugText(h, token, "reply", "stale", revision, "turn-0"), "input_observation_stale")
	h.state.InputState = InputBusy
	debugWant(t, debugText(h, token, "reply", "busy", revision, "turn-1"), "input_busy")
	h.state.InputState = InputIdle
	interaction, err := h.debugObserveInteraction("thread", "turn-1", 9, "item/tool/requestUserInput", []string{"continue", "stop"}, false)
	debugWant(t, err, "")
	debugWant(t, debugText(h, token, "reply", "native hold", revision, "turn-1"), "input_fenced")
	h.state.Debug.Interactions[0].State = "invalidated"
	debugWant(t, h.persistLocked(h.state), "")
	if e.count() != 0 {
		t.Fatal("refused reply issued an effect")
	}
	record, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "reply", Text: "yes, continue", ExpectedRevision: revision, ExpectedTurn: "turn-1"})
	debugWant(t, err, "")
	if record.Stage != "write_known" || e.count() != 1 || h.state.Debug.Results[0].Permitted || h.debugAutomaticLocked() || h.state.Debug.Interactions[0].ID != interaction {
		t.Fatal("reply dispositioned the fact or reopened automatic delivery")
	}
	debugWant(t, h.debugInputTerminal("manager", "reply", "thread", "turn-2", "completed"), "")
	if len(h.state.Debug.Results) != 2 || h.state.Debug.Results[1].Input != "reply" || h.state.Debug.Results[1].Permitted || h.state.Debug.Inputs[0].Stage != "turn_terminal" || h.debugAutomaticLocked() {
		t.Fatalf("reply completion did not create its own blocking fact: %+v", h.state.Debug)
	}
	debugWant(t, h.debugDisposition(token, h.state.Debug.Results[0].ID, 1, false, true), "")
	debugWant(t, h.debugControl(token, "yield_and_resume", h.state.Debug.WriterRevision), "input_fenced")
	debugWant(t, h.debugDisposition(token, h.state.Debug.Results[1].ID, 1, false, true), "")
	debugWant(t, h.debugControl(token, "yield_and_resume", h.state.Debug.WriterRevision), "")
	if !h.debugAutomaticLocked() {
		t.Fatal("explicit dispositions plus resume did not reopen delivery")
	}
	h.state.StopRequested = true
	debugWant(t, debugText(h, token, "late", "after stop", h.state.Debug.WriterRevision, "turn-2"), "input_stopped")
	if e.count() != 1 {
		t.Fatal("stopped target accepted input")
	}
}

// RetryConflictAndLostReply (host receipts): a lost reply is the specified V18
// for that submission while its unknown receipt is persisted, exactly one
// effect was issued and new effects stay closed; the same authenticated actor,
// ID and whole intent then reads the original receipt with no new sequence or
// effect, also after stop and under view authority; a changed body, target
// turn or kind is V15. A written reply is the nil-error positive control.
func TestRetryConflictAndLostReplyInert(t *testing.T) {
	control, written := debugEffectFixture(t)
	controlToken, controlRevision := debugWriter(t, control, "view", "writer_control", "text")
	if record, err := control.debugInput(controlToken, debugInputIntent{Kind: "follow_up", ID: "ok", Text: "written", ExpectedRevision: controlRevision}); err != nil || record.Stage != "write_known" || written.count() != 1 {
		t.Fatalf("written control: %+v %v", record, err)
	}
	h, e := debugEffectFixture(t)
	e.err = errors.New("reply_lost")
	token, revision := debugWriter(t, h, "view", "writer_control", "text", "stop")
	intent := debugInputIntent{Kind: "follow_up", ID: "reply", Text: "original", ExpectedRevision: revision}
	record, err := h.debugInput(token, intent)
	debugWant(t, err, "input_effect_unknown")
	if record.Stage != "effect_unknown" || e.count() != 1 || h.state.Debug.Inputs[0].Stage != "effect_unknown" {
		t.Fatalf("lost reply not retained as unknown: %+v", record)
	}
	if restored, exists, err := h.store.read(); err != nil || !exists || restored.Debug.Inputs[0].Stage != "effect_unknown" {
		t.Fatal("unknown receipt not durable")
	}
	seq := h.debug.journal.sequence
	debugWant(t, debugText(h, token, "next", "another", revision, ""), "input_fenced")
	retry, err := h.debugInput(token, intent)
	debugWant(t, err, "")
	if retry != record || e.count() != 1 || h.debug.journal.sequence != seq {
		t.Fatal("retry resubmitted or re-sequenced")
	}
	for name, changed := range map[string]debugInputIntent{
		"body": {Kind: "follow_up", ID: "reply", Text: "changed", ExpectedRevision: revision},
		"turn": {Kind: "follow_up", ID: "reply", Text: "original", ExpectedRevision: revision, ExpectedTurn: "other"},
		"kind": {Kind: "answer_interaction", ID: "reply", ExpectedRevision: revision, Interaction: "interaction-01", RequestID: 1, OptionsDigest: strings.Repeat("a", 64), Option: "yes"},
	} {
		if name == "kind" {
			g := h.debug.grants[token]
			g.rights = append(g.rights, "native_response")
			h.debug.grants[token] = g
		}
		_, err := h.debugInput(token, changed)
		debugWant(t, err, "input_conflict")
	}
	if e.count() != 1 || h.debug.journal.sequence != seq || len(h.state.Debug.Inputs) != 1 {
		t.Fatal("conflict produced an effect or record")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	debugWant(t, h.debugStop(ctx, token), "")
	retry, err = h.debugInput(token, intent)
	debugWant(t, err, "")
	receipt, err := h.debugReceipt(token, "reply")
	debugWant(t, err, "")
	if retry != record || receipt != record || e.count() != 1 {
		t.Fatal("resolved receipt unavailable after stop or replayed")
	}
	_, err = h.debugReceipt(token, "unknown")
	debugWant(t, err, "input_invalid")
	other := debugToken(t, h, "view")
	_, err = h.debugReceipt(other, "reply")
	debugWant(t, err, "attach_unauthorized")
	debugWant(t, h.debugRetire(false), "input_effect_unknown")
	debugWant(t, h.debugRetire(true), "")
	if in := h.state.Debug.Inputs[0]; in.Stage != "effect_unknown" || !in.Abandoned {
		t.Fatalf("abandoned effect rewritten: %+v", in)
	}
}

// RevokeMidAttach / StopDuringAttach (barriers): a revocation or stop that
// wins before issue leaves zero effects, while one that arrives after the
// issue marker reconciles exactly that one effect without a resend; the host
// state lock is free during the blocked effect call, and the late result
// revives no authority.
func TestRevokeAndStopAcrossIssueMarkerInert(t *testing.T) {
	for _, mode := range []string{"revoke-after", "stop-after", "revoke-before", "stop-before"} {
		t.Run(mode, func(t *testing.T) {
			h, e := debugEffectFixture(t)
			token, revision := debugWriter(t, h, "view", "writer_control", "text", "stop")
			intent := debugInputIntent{Kind: "follow_up", ID: "reply", Text: "hello", ExpectedRevision: revision}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if strings.HasSuffix(mode, "before") {
				if mode == "revoke-before" {
					debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", 1}, token), "")
					_, err := h.debugInput(token, intent)
					debugWant(t, err, "attach_grant_revoked")
				} else {
					debugWant(t, h.debugStop(ctx, token), "")
					_, err := h.debugInput(token, intent)
					debugWant(t, err, "input_stopped")
				}
				if e.count() != 0 || len(h.state.Debug.Inputs) != 0 {
					t.Fatal("authority loss before issue still issued")
				}
				return
			}
			e.block, e.started = make(chan struct{}), make(chan struct{}, 1)
			done := make(chan debugInputRecord, 1)
			go func() {
				record, err := h.debugInput(token, intent)
				if err != nil {
					record.Stage = "error:" + err.Error()
				}
				done <- record
			}()
			select {
			case <-e.started:
			case <-ctx.Done():
				t.Fatal("effect never started")
			}
			locked := make(chan struct{})
			go func() {
				h.mu.Lock()
				h.mu.Unlock()
				close(locked)
			}()
			select {
			case <-locked:
			case <-ctx.Done():
				t.Fatal("state lock held across the effect call")
			}
			if debugState(h).Debug.Inputs[0].Stage != "issued" {
				t.Fatal("issue marker not durable before the effect")
			}
			if mode == "revoke-after" {
				debugWant(t, h.debugRevoke(debugAuthority{"owner", "owner", 1}, token), "")
			} else {
				debugWant(t, h.debugStop(ctx, token), "")
			}
			close(e.block)
			record := <-done
			if record.Stage != "write_known" || e.count() != 1 || h.state.Debug.Inputs[0].Stage != "write_known" {
				t.Fatalf("issued effect not reconciled exactly once: %+v", record)
			}
			if mode == "revoke-after" {
				_, err := h.debugRead(token, 0)
				debugWant(t, err, "attach_grant_revoked")
				_, err = h.debugInput(token, intent)
				debugWant(t, err, "attach_grant_revoked")
			} else {
				_, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "later", Text: "x", ExpectedRevision: h.state.Debug.WriterRevision})
				debugWant(t, err, "input_stopped")
				if !h.state.StopRequested || h.state.Debug.Writer != "stopped" {
					t.Fatal("stop intent lost")
				}
			}
			if e.count() != 1 {
				t.Fatal("late result or retry caused a second effect")
			}
		})
	}
}

// TextWriterCannotAnswerNative / NativeAnswerBindingAndAuthority (inert): the
// current text writer answering is V23 before the answer CAS; the paired grant
// adding only native_response reserves exactly one offered inert answer, and
// the minimal no-text answer grant also works. Wrong request ID, options
// digest, turn, epoch, an already reserved interaction or one expired on the
// host clock are V21, a malformed or unoffered option V14, an unqualified
// class V20 and an owner-only class V22, each with zero answer CAS or effect;
// a request observed at the later clock stays answerable.
func TestNativeAnswerBindingAndAuthorityInert(t *testing.T) {
	h, e := debugEffectFixture(t)
	// Answers require the host's current native thread to be the request's.
	h.state.Protocol = &ProtocolState{View: ap.View{NativeSession: "thread"}}
	offered := []string{"allow", "deny"}
	digest := debugOptionsDigest(offered)
	interaction, err := h.debugObserveInteraction("thread", "turn-1", 11, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	answer := debugInputIntent{Kind: "answer_interaction", ID: "answer", ExpectedTurn: "turn-1", Interaction: interaction, RequestID: 11, OptionsDigest: digest, Option: "allow"}
	textOnly, revision := debugWriter(t, h, "view", "writer_control", "text")
	answer.ExpectedRevision = revision
	_, err = h.debugInput(textOnly, answer)
	debugWant(t, err, "input_right_required")
	if h.state.Debug.Interactions[0].State != "pending" || e.count() != 0 {
		t.Fatal("text-only writer reached the answer CAS")
	}
	debugWant(t, h.debugControl(textOnly, "detach", revision), "")
	paired, revision := debugWriter(t, h, "view", "writer_control", "text", "native_response")
	answer.ExpectedRevision = revision
	negatives := map[string]func(debugInputIntent) debugInputIntent{
		"request-id":     func(a debugInputIntent) debugInputIntent { a.RequestID = 12; return a },
		"options-digest": func(a debugInputIntent) debugInputIntent { a.OptionsDigest = strings.Repeat("d", 64); return a },
		"turn":           func(a debugInputIntent) debugInputIntent { a.ExpectedTurn = "turn-2"; return a },
		"unknown-handle": func(a debugInputIntent) debugInputIntent { a.Interaction = "interaction-99"; return a },
	}
	for name, change := range negatives {
		t.Run(name, func(t *testing.T) {
			bad := change(answer)
			bad.ID = "answer-" + name
			_, err := h.debugInput(paired, bad)
			debugWant(t, err, "interaction_stale")
		})
	}
	for name, bad := range map[string]debugInputIntent{
		"empty-option":   {Kind: "answer_interaction", ID: "bad-1", ExpectedRevision: revision, ExpectedTurn: "turn-1", Interaction: interaction, RequestID: 11, OptionsDigest: digest},
		"nul-option":     {Kind: "answer_interaction", ID: "bad-2", ExpectedRevision: revision, ExpectedTurn: "turn-1", Interaction: interaction, RequestID: 11, OptionsDigest: digest, Option: "a\x00b"},
		"text-in-answer": {Kind: "answer_interaction", ID: "bad-3", ExpectedRevision: revision, ExpectedTurn: "turn-1", Interaction: interaction, RequestID: 11, OptionsDigest: digest, Option: "allow", Text: "also text"},
		"unknown-kind":   {Kind: "approve", ID: "bad-4", ExpectedRevision: revision},
		// Well formed, correct digest, but never offered by the native request.
		"unoffered-option": {Kind: "answer_interaction", ID: "bad-5", ExpectedRevision: revision, ExpectedTurn: "turn-1", Interaction: interaction, RequestID: 11, OptionsDigest: digest, Option: "maybe"},
	} {
		_, err := h.debugInput(paired, bad)
		if err == nil || err.Error() != "input_invalid" {
			t.Fatalf("%s: %v", name, err)
		}
	}
	h.state.Debug.Interactions[0].Epoch = 2
	_, err = h.debugInput(paired, answer)
	debugWant(t, err, "interaction_stale")
	h.state.Debug.Interactions[0].Epoch = 1
	e.classes = nil
	_, err = h.debugInput(paired, answer)
	debugWant(t, err, "interaction_unsupported")
	e.classes = []string{"item/tool/requestUserInput"}
	h.state.Debug.Interactions[0].OwnerOnly = true
	_, err = h.debugInput(paired, answer)
	debugWant(t, err, "interaction_owner_required")
	h.state.Debug.Interactions[0].OwnerOnly = false
	if h.state.Debug.Interactions[0].State != "pending" || e.count() != 0 || len(h.state.Debug.Inputs) != 0 {
		t.Fatal("refused answer reached CAS or effect")
	}
	record, err := h.debugInput(paired, answer)
	debugWant(t, err, "")
	x := h.state.Debug.Interactions[0]
	if record.Stage != "write_known" || record.Interaction != interaction || x.State != "answer_reserved" || x.Input != "answer" || x.Revision != 2 || e.count() != 1 {
		t.Fatalf("qualified answer not reserved once: %+v %+v", record, x)
	}
	if !bytes.Equal(e.frames[0], []byte(`{"id":11,"result":{"answer":"allow"}}`+"\n")) {
		t.Fatalf("answer frame %q", e.frames[0])
	}
	second := answer
	second.ID, second.Option = "answer-2", "deny"
	_, err = h.debugInput(paired, second)
	debugWant(t, err, "interaction_stale")
	retry, err := h.debugInput(paired, answer)
	debugWant(t, err, "")
	if retry != record || e.count() != 1 {
		t.Fatal("second answer or retry produced another response")
	}
	debugWant(t, h.debugInteractionResolved(interaction), "")
	if h.state.Debug.Interactions[0].State != "resolved" || h.state.Debug.Inputs[0].Stage != "native_accepted" {
		t.Fatal("qualified resolution evidence not recorded")
	}
	debugWant(t, h.debugControl(paired, "detach", h.state.Debug.WriterRevision), "")
	// Minimal no-text answer grant at a fresh pending interaction.
	fresh, err := h.debugObserveInteraction("thread", "turn-1", 13, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	noText, revision := debugWriter(t, h, "view", "writer_control", "native_response")
	_, err = h.debugInput(noText, debugInputIntent{Kind: "answer_interaction", ID: "answer-3", ExpectedRevision: revision, ExpectedTurn: "turn-1", Interaction: fresh, RequestID: 13, OptionsDigest: digest, Option: "allow"})
	debugWant(t, err, "")
	if e.count() != 2 {
		t.Fatal("no-text answer grant failed")
	}
	// Expiry on the host clock: a request observed before the clock advances by
	// the interaction lifetime is V21 and recorded expired; one observed at the
	// advanced clock is the positive control at the same checkpoint.
	expiring, err := h.debugObserveInteraction("thread", "turn-1", 17, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	debugWant(t, h.debugControl(noText, "detach", h.state.Debug.WriterRevision), "")
	base := time.Now()
	h.debug.now = func() time.Time { return base.Add(debugInteractionLifetime) }
	late, revision := debugWriter(t, h, "view", "writer_control", "native_response")
	_, err = h.debugInput(late, debugInputIntent{Kind: "answer_interaction", ID: "answer-4", ExpectedRevision: revision, ExpectedTurn: "turn-1", Interaction: expiring, RequestID: 17, OptionsDigest: digest, Option: "allow"})
	debugWant(t, err, "interaction_stale")
	expired := h.state.Debug.Interactions[slices.IndexFunc(h.state.Debug.Interactions, func(x debugInteraction) bool { return x.ID == expiring })]
	if expired.State != "expired" || expired.Input != "" || e.count() != 2 {
		t.Fatalf("expired request answered or not recorded expired: %+v", expired)
	}
	current, err := h.debugObserveInteraction("thread", "turn-1", 19, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	_, err = h.debugInput(late, debugInputIntent{Kind: "answer_interaction", ID: "answer-5", ExpectedRevision: revision, ExpectedTurn: "turn-1", Interaction: current, RequestID: 19, OptionsDigest: digest, Option: "deny"})
	debugWant(t, err, "")
	if e.count() != 3 {
		t.Fatal("current request at the advanced clock was not answerable")
	}
}

// TextValidFormsAndEncodedBound: multiline Unicode, quotes, backslashes and
// shell metacharacters round-trip as data in the journaled intent and the
// issued frame; a text whose complete encoded frame including LF is exactly
// 4096 bytes is accepted and one more byte is V16; empty, NUL and invalid
// UTF-8 text are V14. Nothing is shell-evaluated.
func TestTextValidFormsAndEncodedBoundInert(t *testing.T) {
	h, e := debugEffectFixture(t)
	token, revision := debugWriter(t, h, "view", "writer_control", "text")
	forms := []string{"line one\nline two\n\ttabbed", "quotes \" and 'single' and \\backslash\\", "unicode ünïcødé 日本語 emoji 🚀", "$(touch /tmp/not-run) `uname` ; rm -rf / | cat && echo $HOME"}
	for i, text := range forms {
		id := "form-" + string(rune('a'+i))
		record, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: id, Text: text, ExpectedRevision: revision})
		debugWant(t, err, "")
		if record.Stage != "write_known" {
			t.Fatal(record)
		}
		var frame struct {
			Params struct {
				Text string `json:"text"`
			} `json:"params"`
		}
		if err := json.Unmarshal(e.frames[i], &frame); err != nil || frame.Params.Text != text {
			t.Fatalf("form %d not preserved as data: %q", i, e.frames[i])
		}
		entries := debugEntries(t, h)
		var journaled debugInputIntent
		if err := json.Unmarshal(entries[len(entries)-2].Body, &journaled); err != nil || journaled.Text != text {
			t.Fatal("journaled body differs from the submitted text")
		}
	}
	if _, err := os.Stat("/tmp/not-run"); err == nil {
		t.Fatal("shell metacharacters were evaluated")
	}
	overhead, err := e.debugEncode(debugInputIntent{Kind: "follow_up", Text: ""})
	if err != nil {
		t.Fatal(err)
	}
	exact := strings.Repeat("y", debugFrameLimit-len(overhead))
	record, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "exact", Text: exact, ExpectedRevision: revision})
	debugWant(t, err, "")
	if len(e.frames[len(e.frames)-1]) != debugFrameLimit || record.Stage != "write_known" {
		t.Fatalf("exact frame %d", len(e.frames[len(e.frames)-1]))
	}
	issued := e.count()
	_, err = h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "over", Text: exact + "y", ExpectedRevision: revision})
	debugWant(t, err, "input_capacity_exceeded")
	for name, text := range map[string]string{"empty": "", "nul": "a\x00b", "invalid-utf8": "a\xffb"} {
		_, err := h.debugInput(token, debugInputIntent{Kind: "follow_up", ID: "bad-" + name, Text: text, ExpectedRevision: revision})
		if err == nil || err.Error() != "input_invalid" {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if e.count() != issued || len(h.state.Debug.Inputs) != len(forms)+1 {
		t.Fatal("refused text issued or was recorded")
	}
}
