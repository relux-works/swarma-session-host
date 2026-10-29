package musepty

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Test-side envelope stamps (r12). The fold requires every envelope
// id and every sequence in the owned stream to be unique, so every
// record a test appends carries a fresh stamp from a space the
// scripted peer never uses: the peer stamps small sequences from a
// log scan, tests stamp from 1e9 up. Deliberate-alias tests override
// the stamp after museOwnedRecord returns.
var museTestEnvelopeSeq atomic.Int64

func museStampTestEnvelope(record map[string]any) {
	n := museTestEnvelopeSeq.Add(1) + 1000000000
	record["id"] = museTestUUID(fmt.Sprintf("test-%d", n))
	record["sequence"] = int(n)
}

// museRestampRecord assigns a fresh test envelope to a record that
// already carries one. Duplicate-shape tests use it on the second
// copy so the refusal is the duplicated lifecycle identity, not the
// reused envelope.
func museRestampRecord(record map[string]any) map[string]any {
	museStampTestEnvelope(record)
	return record
}

// museCloneRecord deep-copies a record map so duplicate-shape tests
// can restamp the copy without touching the first append.
func museCloneRecord(t *testing.T, record map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var clone map[string]any
	if err := json.Unmarshal(raw, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

// museResumeTarget returns the envelope identity of the last complete
// record in the host log right now. Call it BEFORE appending the
// reopen opened: the resume reference must cite a record of the
// prior epoch, and everything after the reopen opened is the current
// one. Any prior-epoch record resolves; the scan simply takes the
// latest one.
func museResumeTarget(t *testing.T, h *musePTYHost) (string, int) {
	t.Helper()
	data, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	var id string
	var seq int
	found := false
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var r struct {
			ID       string `json:"id"`
			Sequence int    `json:"sequence"`
		}
		if json.Unmarshal([]byte(line), &r) != nil || r.ID == "" {
			continue
		}
		id, seq, found = r.ID, r.Sequence, true
	}
	if !found {
		t.Fatal("no complete log record to resume from")
	}
	return id, seq
}

// museResumedRecord builds an owned resumed record citing the given
// prior-epoch target. Tests that resume through a reopen use it; the
// captured resumed_from_* fragment points outside any peer log and
// can no longer clear a reopen.
func museResumedRecord(t *testing.T, h *musePTYHost, targetID string, targetSeq int) map[string]any {
	t.Helper()
	r := museOwnedRecord(h, museCorpusRecord(t, "session.resumed", "", ""))
	rec := r["payload"].(map[string]any)["record"].(map[string]any)
	rec["resumed_from_record_id"] = targetID
	rec["resumed_from_sequence"] = targetSeq
	return r
}

func appendMuseRecord(t *testing.T, h *musePTYHost, record any) {
	t.Helper()
	b, e := json.Marshal(record)
	if e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(h.snapshot.LogPath, os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.Write(append(b, '\n'))
	closeErr := f.Close()
	if e != nil {
		t.Fatal(e)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}

// museOwnedRecord stamps a captured record as the owned session's own, so a
// test that appends it refuses (or admits) for the defect under test rather
// than for a foreign stream. Every session reference the record carries —
// the stream and any nested or direct payload session id — names the owned
// session. Foreign-session tests keep their own stamps.
func museOwnedRecord(h *musePTYHost, record map[string]any) map[string]any {
	museStampTestEnvelope(record)
	owned := h.NativeIdentity()
	if stream, ok := record["stream"].(map[string]any); ok {
		stream["id"] = owned
	}
	payload, ok := record["payload"].(map[string]any)
	if !ok {
		return record
	}
	// Every session reference names the owned session. Run and task
	// references keep their captured ids; tests emit matching runs and
	// tasks for them.
	if rec, ok := payload["record"].(map[string]any); ok {
		for _, key := range []string{"session_id", "root_session_id"} {
			if _, ok := rec[key]; ok {
				rec[key] = owned
			}
		}
		if ref, ok := rec["session_stream"].(map[string]any); ok {
			if _, ok := ref["id"]; ok {
				ref["id"] = owned
			}
		}
	}
	for _, key := range []string{"session_id", "source_session_id"} {
		if _, ok := payload[key]; ok {
			payload[key] = owned
		}
	}
	if ev, ok := payload["event"].(map[string]any); ok {
		if _, ok := ev["parent_session_id"]; ok {
			ev["parent_session_id"] = owned
		}
		if rec, ok := ev["record"].(map[string]any); ok {
			if owner, ok := rec["owner"].(map[string]any); ok {
				if _, ok := owner["session_id"]; ok {
					owner["session_id"] = owned
				}
			}
		}
	}
	return record
}

func assertMuseNoInput(t *testing.T, h *musePTYHost, home string) {
	t.Helper()
	if e := h.DeliverSessionNotice(context.Background(), "refuse unknown state"); e != ErrMusePTYDeferred {
		t.Fatalf("unknown state admitted: %v", e)
	}
	for _, name := range []string{"peer-paste", "peer-submit"} {
		if b, e := os.ReadFile(filepath.Join(home, name)); !os.IsNotExist(e) {
			t.Fatalf("unsafe input %s=%q: %v", name, b, e)
		}
	}
}
func TestMusePTYUnknownRunRecordsNeverPasteOrSubmit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record any
	}{
		{"null_event", map[string]any{"payload_type": "runtime.session", "payload": map[string]any{"kind": "run", "run_id": "unknown", "event": nil}}},
		{"empty_event", map[string]any{"payload_type": "runtime.session", "payload": map[string]any{"kind": "run", "run_id": "unknown", "event": map[string]any{}}}},
		{"omitted_event", map[string]any{"payload_type": "runtime.session", "payload": map[string]any{"kind": "run", "run_id": "unknown"}}},
		{"future_event", map[string]any{"payload_type": "runtime.session", "payload": map[string]any{"kind": "run", "run_id": "unknown", "event": map[string]any{"kind": "future_event"}}}},
		{"future_kind", map[string]any{"payload_type": "runtime.session", "payload": map[string]any{"kind": "future_kind"}}},
		{"null_payload", map[string]any{"payload_type": "runtime.session", "payload": nil}},
		{"future_record", map[string]any{"payload_type": "runtime.future", "payload": map[string]any{"kind": "future"}}},
		{"missing_run_id", map[string]any{"payload_type": "runtime.session", "payload": map[string]any{"kind": "run", "event": map[string]any{"kind": "started"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			appendMuseRecord(t, h, tc.record)
			assertMuseNoInput(t, h, home)
		})
	}
}
func TestMusePTYCapturedQueuedItemNeverSubmits(t *testing.T) {
	h, home := musePeer(t, "idle")
	// A completed run with one real undrained inbox item must remain non-idle.
	for _, kind := range []string{"started", "inbox_item_queued", "terminal"} {
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord(kind)))
	}
	assertMuseNoInput(t, h, home)
	// The captured drain targets a second run: delivery after the drain
	// needs that run observed, not just the forward reference.
	target := museFixtureRecord("inbox_item_drained")["payload"].(map[string]any)["run_id"].(string)
	museEmitRunBounded(t, h, "started", target)
	museEmitRunBounded(t, h, "terminal", target)
	drain := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
	appendMuseRecord(t, h, drain)
	if e := h.DeliverSessionNotice(context.Background(), "delivery after captured drain"); e != nil {
		t.Fatal(e)
	}
	if b, e := os.ReadFile(filepath.Join(home, "peer-submit")); e != nil || !strings.Contains(string(b), "delivery after captured drain") {
		t.Fatalf("after drain %q: %v", b, e)
	}
}

func TestMusePTYCompletedTranscriptKeywordsPermitNotice(t *testing.T) {
	h, home := musePeer(t, "transcript")
	if e := h.DeliverSessionNotice(context.Background(), "child reached review"); e != nil {
		t.Fatal(e)
	}
	if b, e := os.ReadFile(filepath.Join(home, "peer-submit")); e != nil || !strings.Contains(string(b), "child reached review") {
		t.Fatalf("notice %q: %v", b, e)
	}
}
func TestMusePTYUnboxedLiteralBorderRemainsText(t *testing.T) {
	h, home := musePeer(t, "unboxed")
	for _, text := range []string{"Echo this literal: │", "continued\n│ literal continuation │", "literal ─ in continuation\nlast │"} {
		museSettleScriptedIdle(t, h, home)
		if e := h.DeliverSessionNotice(context.Background(), text); e != nil {
			t.Fatal(e)
		}
		if b, e := os.ReadFile(filepath.Join(home, "peer-paste")); e != nil || string(b) != text {
			t.Fatalf("paste changed %q: %v", b, e)
		}
		if b, e := os.ReadFile(filepath.Join(home, "peer-submit")); e != nil || !strings.HasSuffix(string(b), ":"+text) {
			t.Fatalf("literal refused %q: %v", b, e)
		}
		museSettleScriptedIdle(t, h, home)
	}
}

// Compare every field/key/type of the actual peer log to the captured records.
// Dynamic identities, timestamps, process ids and text may change; field names,
// nesting, absent fields and nulls cannot silently drift to an invented schema.
func museRecordShape(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, item := range v {
			out[k] = museRecordShape(item)
		}
		return out
	case []any:
		out := []any{}
		for _, item := range v {
			out = append(out, museRecordShape(item))
		}
		return out
	case nil:
		return nil
	default:
		return reflect.TypeOf(value).String()
	}
}
func TestMusePTYPeerRecordsMatchCapturedWireShapes(t *testing.T) {
	h, _ := musePeer(t, "drained")
	if e := h.DeliverSessionNotice(context.Background(), "shape check"); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(h.snapshot.LogPath)
	if e != nil {
		t.Fatal(e)
	}
	_, e = museLogRecords(data)
	if e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	// Compare the peer's original wire bytes: the production boundary drops
	// additive members, which must not conceal drift in this fixture assay.
	for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		b := []byte(line)
		var r map[string]any
		if e = json.Unmarshal(b, &r); e != nil {
			t.Fatal(e)
		}
		key := r["payload_type"].(string)
		if key == "runtime.session" {
			key = r["payload"].(map[string]any)["event"].(map[string]any)["kind"].(string)
		}
		if !reflect.DeepEqual(museRecordShape(r), museRecordShape(museFixtureRecord(key))) {
			t.Fatalf("peer drifted from captured %s shape: %s", key, b)
		}
		seen[key] = true
	}
	for _, kind := range []string{"started", "inbox_item_queued", "inbox_item_drained", "assistant_message_committed", "terminal"} {
		if !seen[kind] {
			t.Errorf("peer did not emit %s", kind)
		}
	}
}

func museRealEchoHost(t *testing.T, delay string) (*musePTYHost, context.Context) {
	t.Helper()
	binary := os.Getenv("TASK_BOARD_MUSE_SMOKE_BIN")
	if binary == "" {
		t.Skip("pinned Echo binary not supplied; real regression unrun")
	}
	home := t.TempDir()
	if e := os.Symlink(binary, filepath.Join(home, "muse")); e != nil {
		t.Fatal(e)
	}
	req := agentic.LaunchRequest{System: "muse", Model: agentic.Model{ID: "echo"}, WorkDir: home, Env: []string{"HOME=" + home, "PATH=" + home + ":/usr/bin:/bin", "TERM=xterm-256color", "XDG_DATA_HOME=" + filepath.Join(home, "data"), "XDG_CONFIG_HOME=" + filepath.Join(home, "config"), "XDG_CACHE_HOME=" + filepath.Join(home, "cache"), "XDG_STATE_HOME=" + filepath.Join(home, "state")}, NativeArgs: []string{"--trust-workspace"}}
	if delay != "" {
		req.NativeArgs = append(req.NativeArgs, "--echo-delay-ms", delay)
	}
	plan, e := BuildMusePTYLaunchPlan(req)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	h, e := StartMusePTYHost(ctx, plan)
	if e != nil {
		cancel()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_ = h.Stop(context.Background())
		cancel()
		if dir := os.Getenv("TASK_BOARD_MUSE_EVIDENCE_DIR"); dir != "" {
			h.mu.Lock()
			replay := append([]byte(nil), h.replay...)
			screen := strings.Join(h.vt.lines(), "\n")
			if t.Failed() {
				t.Logf("unknown=%v pending=%q screen=%q raw=%q", h.vt.unknown, h.vt.pending, screen, replay)
			}
			h.mu.Unlock()
			if e := os.WriteFile(filepath.Join(dir, t.Name()+".vt"), replay, 0600); e != nil {
				t.Error(e)
			}
			if e := os.WriteFile(filepath.Join(dir, t.Name()+".screen.txt"), []byte(screen), 0600); e != nil {
				t.Error(e)
			}
			if b, e := os.ReadFile(h.snapshot.LogPath); e == nil {
				if e = os.WriteFile(filepath.Join(dir, t.Name()+".jsonl"), b, 0600); e != nil {
					t.Error(e)
				}
			}
		}
	})
	museSmokeBoundedWait(t, ctx, "real echo startup idle", func() bool { return h.idle("") })
	return h, ctx
}
func TestMusePTYRealCompletedTranscriptAndLiteralFollowup(t *testing.T) {
	h, ctx := museRealEchoHost(t, "")
	text := "Please confirm the receipt; permissions thinking do you trust [y/n] approve"
	r, e := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: text})
	if e != nil || r == nil || r.Status != sm.ProviderRunIdle || !strings.Contains(r.Output, text) {
		t.Fatalf("echo %+v: %v", r, e)
	}
	if e = h.awaitQuiet(ctx); e != nil {
		t.Fatal(e)
	}
	if e = h.DeliverSessionNotice(ctx, "child reached review"); e != nil {
		t.Fatal(e)
	}
	museSmokeBoundedWait(t, ctx, "real idle after notice", func() bool { return h.idle("") })
	r, e = h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: "Echo this literal: │"})
	if e != nil || r == nil || !strings.Contains(r.Output, "Echo this literal: │") {
		t.Fatalf("literal echo %+v: %v", r, e)
	}
}
func TestMusePTYRealQueueDrainRestoresNotice(t *testing.T) {
	h, ctx := museRealEchoHost(t, "2000")
	s, e := h.AttachTerminal(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Write([]byte("\x1b[200~first echo\x1b[201~\r")); e != nil {
		t.Fatal(e)
	}
	museSmokeBoundedWait(t, ctx, "real first run active", func() bool { f, e := h.fold(); return e == nil && f.activeRunCount() > 0 })
	if _, e = s.Write([]byte("\x1b[200~second echo\x1b[201~\r")); e != nil {
		t.Fatal(e)
	}
	museSmokeBoundedWait(t, ctx, "real queue observed", func() bool { f, e := h.fold(); return e == nil && len(f.Queued) > 0 })
	if e = h.DeliverSessionNotice(ctx, "refuse pending queue"); e != ErrMusePTYDeferred {
		t.Fatalf("queued input admitted: %v", e)
	}
	museSmokeBoundedWait(t, ctx, "real idle after drain", func() bool { return h.idle("") })
	f, e := h.fold()
	if e != nil || len(f.Terminals) != 2 {
		t.Fatalf("two turns not observed %+v: %v", f, e)
	}
	var text string
	for _, r := range f.Terminals {
		text += r.Text
	}
	if !strings.Contains(text, "first echo") || !strings.Contains(text, "second echo") {
		t.Fatalf("real output missing: %q", text)
	}
	if e = h.DeliverSessionNotice(ctx, "child completed after drain"); e != nil {
		t.Fatal(e)
	}
}
func TestMusePTYRealPermissionsModalNeverPastes(t *testing.T) {
	h, ctx := museRealEchoHost(t, "")
	s, e := h.AttachTerminal(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Write([]byte("\x1b[200~/permissions\x1b[201~\r")); e != nil {
		t.Fatal(e)
	}
	museSmokeBoundedWait(t, ctx, "real permissions modal visible", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return strings.Contains(strings.Join(h.vt.lines(), "\n"), "Selections are saved")
	})
	if e = h.awaitQuiet(ctx); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(h.snapshot.LogPath)
	if e != nil {
		t.Fatal(e)
	}
	fold, e := h.fold()
	if e != nil || !fold.idle() {
		t.Fatalf("modal test must isolate the UI gate with a known idle log: %+v %v", fold, e)
	}
	if e = h.DeliverSessionNotice(ctx, "must refuse actual modal"); e != ErrMusePTYDeferred {
		t.Fatalf("actual modal admitted: %v", e)
	}
	after, e := os.ReadFile(h.snapshot.LogPath)
	if e != nil {
		t.Fatal(e)
	}
	if string(before) != string(after) {
		t.Fatal("modal delivery changed durable log")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if strings.Contains(string(h.replay), "must refuse actual modal") {
		t.Fatal("notice appeared in modal")
	}
}

func TestMusePTYNoticeSettlesIdleRedrawAndRechecks(t *testing.T) {
	for _, mode := range []string{"idle", "settle-dialog"} {
		t.Run(mode, func(t *testing.T) {
			h, home := musePeer(t, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if mode == "idle" {
				s, e := h.AttachTerminal(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer s.Close()
				if _, e = s.Write([]byte{'\f'}); e != nil {
					t.Fatal(e)
				}
				museSettleScriptedIdle(t, h, home)
				if e = h.DeliverSessionNotice(ctx, "notice immediately after redraw"); e != nil {
					t.Fatal(e)
				}
				return
			}
			// Settle-then-recheck: the clock parks just inside the
			// quiet window so deliver waits the production settle,
			// and a late dialog frame lands while it waits (ordered
			// by the settle-entry signal, not by a sleep). The
			// post-settle recheck must defer before any paste.
			base := museLastOutput(h)
			museSetTestClockAt(t, h, base.Add(time.Millisecond))
			museDrainNotify(h.settleNotify)
			errCh := make(chan error, 1)
			go func() {
				errCh <- h.DeliverSessionNotice(ctx, "late dialog must defer")
			}()
			museWaitNotify(t, h.settleNotify, "deliver settle entry")
			h.mu.Lock()
			h.vt.observe([]byte(strings.Replace(museTestScreen(""), "  echo · ~ · Auto-review", "Permissions\r\n› 1 Auto-review\r\n  2 Ask", 1)))
			h.lastOutput = time.Now()
			h.mu.Unlock()
			if err := <-errCh; err != ErrMusePTYDeferred {
				t.Fatalf("late dialog during settle: got %v want %v", err, ErrMusePTYDeferred)
			}
			for _, name := range []string{"peer-paste", "peer-submit"} {
				if b, e := os.ReadFile(filepath.Join(home, name)); !os.IsNotExist(e) {
					t.Fatalf("unsafe input %s=%q", name, b)
				}
			}
		})
	}
}

// TestMusePTYQuietSettleAdmitsWhenQuietArrives is the admit side of
// settle-then-recheck: deliver waits the production settle inside
// the quiet window, the clock advances past the window while it
// waits (ordered by the settle-entry signal), and the recheck
// admits through paste and submit.
func TestMusePTYQuietSettleAdmitsWhenQuietArrives(t *testing.T) {
	h, home := musePeer(t, "idle")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base := museLastOutput(h)
	museSetTestClockAt(t, h, base.Add(time.Millisecond))
	museDrainNotify(h.settleNotify)
	errCh := make(chan error, 1)
	go func() {
		errCh <- h.DeliverSessionNotice(ctx, "quiet arriving during settle admits")
	}()
	museWaitNotify(t, h.settleNotify, "deliver settle entry")
	museAdvanceTestClockPastQuiet(h)
	if err := <-errCh; err != nil {
		t.Fatalf("settle then quiet must admit: %v", err)
	}
	if b, e := os.ReadFile(filepath.Join(home, "peer-submit")); e != nil || !strings.Contains(string(b), "quiet arriving during settle admits") {
		t.Fatalf("settle admit not submitted %q: %v", b, e)
	}
	museSettleScriptedIdle(t, h, home)
}

func TestMusePTYPartialRecognizedEventsNeverBecomeIdle(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload map[string]any
		kind    string
	}{
		{"partial_usage", map[string]any{"kind": "run", "run_id": museFixturePayload("started")["run_id"], "event": map[string]any{"kind": "resource_usage_sampled"}}, "runtime.session"},
		{"null_usage", map[string]any{"kind": "run", "run_id": museFixturePayload("started")["run_id"], "event": map[string]any{"kind": "resource_usage_sampled", "usage": nil}}, "runtime.session"},
		{"missing_metadata_record", map[string]any{"kind": "metadata"}, "runtime.session.metadata"},
		{"null_metadata_record", map[string]any{"kind": "metadata", "record": nil}, "runtime.session.metadata"},
		{"empty_metadata_record", map[string]any{"kind": "metadata", "record": map[string]any{}}, "runtime.session.metadata"},
		{"null_task_event", map[string]any{"task_id": "task", "event": nil}, "runtime.session.task"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("started")))
			appendMuseRecord(t, h, map[string]any{"payload_type": tc.kind, "payload": tc.payload})
			appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("terminal")))
			assertMuseNoInput(t, h, home)
		})
	}
}

func TestMusePTYResumeIgnoresSubagentLogGrammar(t *testing.T) {
	h, home := musePeer(t, "idle")
	snapshot, e := museTestSnapshot(h)
	if e != nil {
		t.Fatal(e)
	}
	id := h.NativeIdentity()
	_ = h.Stop(context.Background())
	// The real pinned binary writes nested subagent session.jsonl with a
	// different record grammar. It is not a primary TUI candidate, even if
	// it has an unknown event or claims the parent's process identity.
	nested := filepath.Join(filepath.Dir(h.snapshot.LogPath), "subagent", "foreign", "session.jsonl")
	if e = os.MkdirAll(filepath.Dir(nested), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(nested, []byte("{\"payload_type\":\"runtime.session.capture_selection\",\"payload\":{\"kind\":\"capture_selection\"}}\n"), 0600); e != nil {
		t.Fatal(e)
	}
	// Tier-1 fix-forward r3 (nested-foreign-primary-only): move the whole
	// session tree aside, outside the sessions root, before resume. The
	// resumed peer completes the staged tree with a pid-matching poison
	// log under a nested foreign dir and publishes it with one atomic
	// rename, so no traversal can list <session> without foreign/: clean
	// discovery skips both nested logs by depth, while a depth check
	// weakened to admit foreign-nested logs reads the poison before the
	// primary and must fail the resume fast with the typed prefix
	// refusal instead of admitting it.
	if e = os.WriteFile(filepath.Join(home, "peer-plant-foreign"), []byte("plant"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(filepath.Dir(h.snapshot.LogPath), filepath.Join(home, musePeerStagedSession)); e != nil {
		t.Fatal(e)
	}
	plan, e := BuildMusePTYLaunchPlan(agentic.LaunchRequest{System: "muse", Model: agentic.Model{ID: "echo"}, WorkDir: home, Env: h.command.Env, NativeArgs: []string{"resume", id, "--trust-workspace"}})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resumed, e := ResumeMusePTYHost(ctx, plan, sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(snapshot)})
	if e != nil {
		t.Fatal(e)
	}
	defer resumed.Stop(context.Background())
	if resumed.snapshot.LogPath != h.snapshot.LogPath {
		t.Fatalf("resume selected %q, want the primary %q", resumed.snapshot.LogPath, h.snapshot.LogPath)
	}
	if e = museRequirePlantComplete(h.snapshot.LogPath, resumed.snapshot.PID); e != nil {
		t.Fatalf("poison did not arm: %v", e)
	}
	museInjectTestSeams(t, resumed)
	museSettleScriptedIdle(t, resumed, home)
	if e = resumed.DeliverSessionNotice(ctx, "resumed primary ignores child grammar"); e != nil {
		t.Fatal(e)
	}
}

// museRequirePlantComplete is the post-resume arming proof for
// TestMusePTYResumeIgnoresSubagentLogGrammar: the nested foreign plant
// is three complete records — a route naming the resumed child second
// and the unknown shape third. A missing, partial or pid-mismatched
// plant fails loudly here instead of passing vacuously: only a
// complete pid-matching plant can kill the weakened gate.
func museRequirePlantComplete(primary string, pid int) error {
	data, err := os.ReadFile(filepath.Join(filepath.Dir(primary), "foreign", "session.jsonl"))
	if err != nil {
		return fmt.Errorf("published tree without plant file: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) != 4 || lines[3] != "" {
		return fmt.Errorf("plant has %d lines, want 3 complete", len(lines)-1)
	}
	var route struct {
		PayloadType string `json:"payload_type"`
		Payload     struct {
			Record struct {
				PID int64 `json:"pid"`
			} `json:"record"`
		} `json:"payload"`
	}
	if json.Unmarshal([]byte(lines[1]), &route) != nil || route.PayloadType != "runtime.session.route_facts" || route.Payload.Record.PID != int64(pid) {
		return fmt.Errorf("plant second record is not a route for pid %d", pid)
	}
	var third struct {
		PayloadType string `json:"payload_type"`
	}
	if json.Unmarshal([]byte(lines[2]), &third) != nil || third.PayloadType != "runtime.future" {
		return fmt.Errorf("plant third record is not the unknown shape")
	}
	return nil
}

func TestMusePTYUnknownTerminalRefusesNextNotice(t *testing.T) {
	h, home := musePeer(t, "idle")
	if e := os.WriteFile(filepath.Join(home, "peer-terminal"), []byte(`{"terminal":"futureTerminal","text":"unknown outcome"}`), 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, e := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: "unknown terminal result"})
	if e != nil || r == nil || r.Status != sm.ProviderRunFailed || r.ProviderStatus != "unknown_terminal:futureTerminal" || r.Recoverable {
		t.Fatalf("M2 unknown result %+v: %v", r, e)
	}
	for _, name := range []string{"peer-paste", "peer-submit"} {
		if e = os.Remove(filepath.Join(home, name)); e != nil {
			t.Fatal(e)
		}
	}
	museSyncPeerEmission(t, h, home, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.vt.composerMatches("")
	})
	assertMuseNoInput(t, h, home)
}
