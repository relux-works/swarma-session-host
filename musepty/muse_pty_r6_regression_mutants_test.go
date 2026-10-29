package musepty

import (
	"encoding/json"
	"strings"
	"testing"
)

// A non-printing rune lodged inside an otherwise content-shaped row must
// still poison the screen: the cell rule fires before classification. The
// full-region match would also refuse a lone control row, so this test
// isolates the cell rule for the del-only mutant.
func TestMusePTYR6ControlLodgedInContentMustRefuse(t *testing.T) {
	for _, tc := range []struct{ name, row string }{
		{"DEL", "  studi\x7fo detail"},
		{"C1", "  studi\u009b detail"},
		{"DEL_with_parent", "\u276f harvest line\r\n  studi\x7fo detail"},
		{"C1_with_parent", "\u276f harvest line\r\n  studi\u009b detail"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte("\x1b[1;1H\x1b[J" + tc.row + "\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
}

// Lossy JSON string decoding must not certify idle. encoding/json replaces
// an unpaired surrogate escape with U+FFFD; the strict scan refuses before
// decoding. Repeat of malformed-utf8 (rounds 4-5).
func TestMusePTYR6LossySurrogateMustRefuse(t *testing.T) {
	for _, tc := range []struct{ name, field, escape string }{
		{"high_credential", "credential_backend", `"\ud800"`},
		{"low_credential", "credential_backend", `"\udfff"`},
		{"high_cwd", "cwd", `"\ud800"`},
		{"high_then_nonlow", "credential_backend", `"\ud83d\u0041"`},
		{"lone_low", "credential_backend", `"\udc00"`},
		{"high_at_end", "credential_backend", `"tail\ud83d"`},
		{"truncated", "credential_backend", `"\ud83"`},
		{"non_hex", "credential_backend", `"\uzzzz"`},
		{"unknown_escape", "credential_backend", `"\q"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			kind := "session.opened.observed"
			if tc.field == "cwd" {
				kind = "runtime.session.route_facts"
			}
			targetID, targetSeq := museResumeTarget(t, h)
			record := museOwnedRecord(h, museFixtureRecord(kind))
			record["payload"].(map[string]any)["record"].(map[string]any)[tc.field] = "SENTINEL"
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			raw = []byte(strings.Replace(string(raw), `"SENTINEL"`, tc.escape, 1))
			museAppendRawLog(t, h, raw)
			// The second opened reopens the session, so the captured
			// resume context witnesses it; the lossy escape itself must
			// still refuse. (The cwd vehicle is a route restating the
			// owned epoch, which needs no resume.) The resumed
			// reference resolves so the surrogate-d800-only mutant,
			// which admits the escape, must deliver through it.
			if kind == "session.opened.observed" {
				appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
			}
			assertMuseNoInput(t, h, home)
		})
	}
}

// A task terminal discharges only the pending task with the same (run, task)
// key. Three panel orderings of the same cross-run mechanism.
func TestMusePTYR6CrossRunTaskTerminalMustRefuse(t *testing.T) {
	emitRun := func(t *testing.T, h *musePTYHost, kind, id string) {
		t.Helper()
		r := museOwnedRecord(h, museFixtureRecord(kind))
		r["payload"].(map[string]any)["run_id"] = id
		appendMuseRecord(t, h, r)
	}
	emitTask := func(t *testing.T, h *musePTYHost, event, run, task string) {
		t.Helper()
		r := museOwnedRecord(h, museCorpusTaskRecord(t, event))
		r["payload"].(map[string]any)["run_id"] = run
		r["payload"].(map[string]any)["task_id"] = task
		r["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, r)
	}
	taskID := func(t *testing.T) string {
		t.Helper()
		return museCorpusTaskRecord(t, "started")["payload"].(map[string]any)["task_id"].(string)
	}
	// r7 lifecycle states: each subtest runs the full captured planning
	// order before its start, so the refusal stays attributable to the
	// cross-run terminal rather than to a skipped planning edge.
	t.Run("panel_a_runs_settle_first", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		id := taskID(t)
		emitRun(t, h, "started", museTestUUID("run-A"))
		museEmitTaskPlanned(t, h, museTestUUID("run-A"), id)
		emitRun(t, h, "terminal", museTestUUID("run-A"))
		emitRun(t, h, "started", museTestUUID("run-B"))
		emitRun(t, h, "terminal", museTestUUID("run-B"))
		emitTask(t, h, "completed", museTestUUID("run-B"), id)
		assertMuseNoInput(t, h, home)
	})
	t.Run("panel_b_terminal_mid_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museOwnedRecord(h, museFixtureRecord("started"))
		runID := run["payload"].(map[string]any)["run_id"].(string)
		appendMuseRecord(t, h, run)
		// Fixed sentinel id: the task-cross-run-one-only mutant restores
		// task-id-only matching for exactly this task.
		id := museTestUUID("r6-cross-run-sentinel")
		museEmitTaskPlanned(t, h, runID, id)
		emitRun(t, h, "terminal", runID)
		emitRun(t, h, "started", museTestUUID("other-run"))
		emitTask(t, h, "completed", museTestUUID("other-run"), id)
		emitRun(t, h, "terminal", museTestUUID("other-run"))
		assertMuseNoInput(t, h, home)
	})
	t.Run("panel_d_mismatched_terminal_between_starts", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		id := taskID(t)
		emitRun(t, h, "started", museTestUUID("run-a"))
		emitRun(t, h, "started", museTestUUID("run-b"))
		museEmitTaskPlanned(t, h, museTestUUID("run-a"), id)
		emitTask(t, h, "completed", museTestUUID("run-b"), id)
		emitRun(t, h, "terminal", museTestUUID("run-a"))
		emitRun(t, h, "terminal", museTestUUID("run-b"))
		assertMuseNoInput(t, h, home)
	})
}
