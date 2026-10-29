package musepty

// Revision r7: close the round-6 identity/lifecycle/queue/mirror/screen
// gaps. Every round-6 panel reproduction is ported here as a named
// production-entry test through DeliverSessionNotice. Sources:
//   - panel A (regression): TestPanelR6* in
//     regression_probes_test.go
//   - panel B (regression): TestReviewR6* in
//     regression_probes.go
//   - delta (regression): TestIndependentR6QueueCorrelation in
//     regression_independent_test.go
// Ports use full captured lifecycles where the r7 transition table
// requires them, so each refusal stays attributable to the mechanism
// under test rather than to a skipped planning edge.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// museCorpusRecord returns a clone of the first outer corpus record with
// the given payload type, payload kind and event kind. Empty kind or
// event matches records without that level.
func museCorpusRecord(t *testing.T, payloadType, kind, event string) map[string]any {
	t.Helper()
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		if record["payload_type"] != payloadType {
			continue
		}
		payload, _ := record["payload"].(map[string]any)
		if payload == nil {
			continue
		}
		if kind != "" && payload["kind"] != kind {
			continue
		}
		if event != "" {
			ev, _ := payload["event"].(map[string]any)
			if ev == nil || ev["kind"] != event {
				continue
			}
		}
		out, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		var clone map[string]any
		if err := json.Unmarshal(out, &clone); err != nil {
			t.Fatal(err)
		}
		return clone
	}
	t.Fatalf("capture missing %s/%s/%s", payloadType, kind, event)
	return nil
}

// museCorpusMirrorEvent returns a clone of the captured session-scoped
// mirror record for one lifecycle event.
func museCorpusMirrorEvent(t *testing.T, event string) map[string]any {
	t.Helper()
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		if record["payload_type"] != "runtime.session.task" {
			continue
		}
		payload, _ := record["payload"].(map[string]any)
		ev, _ := payload["event"].(map[string]any)
		if ev == nil || ev["kind"] != event {
			continue
		}
		out, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		var clone map[string]any
		if err := json.Unmarshal(out, &clone); err != nil {
			t.Fatal(err)
		}
		return clone
	}
	t.Fatalf("capture missing mirror %s", event)
	return nil
}

// museEmitMirrorPlanned emits the captured mirror order for one task:
// proposed, accepted, scheduled, side_effect_intent, started.
func museEmitMirrorPlanned(t *testing.T, h *musePTYHost, taskID string) {
	t.Helper()
	for _, event := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started"} {
		r := museOwnedRecord(h, museCorpusMirrorEvent(t, event))
		r["payload"].(map[string]any)["task_id"] = taskID
		r["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = taskID
		appendMuseRecord(t, h, r)
	}
}

func museEmitMirrorTerminal(t *testing.T, h *musePTYHost, taskID, terminal string) {
	t.Helper()
	r := museOwnedRecord(h, museCorpusMirrorEvent(t, terminal))
	r["payload"].(map[string]any)["task_id"] = taskID
	r["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = taskID
	appendMuseRecord(t, h, r)
}

// museFrameRows renders rows above a valid idle composer.
func museFrameRows(rows ...string) string {
	frame := "\x1b[1;1H\x1b[J"
	if len(rows) > 0 {
		frame += strings.Join(rows, "\r\n") + "\r\n"
	}
	return frame + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")
}

func museShowFrame(t *testing.T, h *musePTYHost, rows ...string) {
	t.Helper()
	h.mu.Lock()
	h.vt.observe([]byte(museFrameRows(rows...)))
	h.mu.Unlock()
}

// A terminated task key may not start again. Ports panel B task_reuse
// with the full captured lifecycle isolating the reuse edge.
func TestMusePTYR7TaskReuseMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	run := museOwnedRecord(h, museFixtureRecord("started"))
	runID := run["payload"].(map[string]any)["run_id"].(string)
	appendMuseRecord(t, h, run)
	taskID := museTestUUID("r7-task-reuse")
	museEmitTaskPlanned(t, h, runID, taskID)
	end := museOwnedRecord(h, museCorpusTaskRecord(t, "completed"))
	end["payload"].(map[string]any)["run_id"] = runID
	end["payload"].(map[string]any)["task_id"] = taskID
	end["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = taskID
	appendMuseRecord(t, h, end)
	again := museOwnedRecord(h, museCorpusTaskRecord(t, "started"))
	again["payload"].(map[string]any)["run_id"] = runID
	again["payload"].(map[string]any)["task_id"] = taskID
	again["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = taskID
	appendMuseRecord(t, h, again)
	assertMuseNoInput(t, h, home)
}

// end followed by opened without the captured resume context is unknown
// state. Ports panel A TestPanelR6EndedReopenWithoutResumeMustRefuse
// (corpus end) and panel B end_reopen_without_resume (fixture end).
func TestMusePTYR7EndedReopenWithoutResumeMustRefuse(t *testing.T) {
	t.Run("panel_a_corpus_end", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		for _, raw := range museCorpus(t) {
			var r map[string]any
			if json.Unmarshal(raw, &r) != nil {
				t.Fatal("capture")
			}
			if r["payload_type"] == "session.end" {
				appendMuseRecord(t, h, museOwnedRecord(h, r))
				break
			}
		}
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		assertMuseNoInput(t, h, home)
	})
	t.Run("panel_b_fixture_end", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		assertMuseNoInput(t, h, home)
	})
}

// Session epoch edges outside the captured resume flow are unknown:
// end before open, a second end, and a second opened while the reopen
// still waits for its resume.
func TestMusePTYR7SessionEpochEdgesMustRefuse(t *testing.T) {
	t.Run("double_end", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
		assertMuseNoInput(t, h, home)
	})
	t.Run("opened_while_reopen_pending", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		assertMuseNoInput(t, h, home)
	})
	t.Run("end_before_open", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		base, err := os.ReadFile(h.snapshot.LogPath)
		if err != nil {
			t.Fatal(err)
		}
		var kept []string
		for _, line := range strings.Split(string(base), "\n") {
			if line == "" || strings.Contains(line, `"session.opened.observed"`) {
				continue
			}
			kept = append(kept, line)
		}
		end, err := json.Marshal(museOwnedRecord(h, museFixtureRecord("session.end")))
		if err != nil {
			t.Fatal(err)
		}
		kept = append(kept, string(end))
		if err := os.WriteFile(h.snapshot.LogPath, []byte(strings.Join(kept, "\n")+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		assertMuseNoInput(t, h, home)
	})
}

// A route record that changes the process identity inside one session
// epoch is unknown state. Ports panel B changed_route_pid.
func TestMusePTYR7ChangedRoutePIDMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	r := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
	r["payload"].(map[string]any)["record"].(map[string]any)["pid"] = 99
	appendMuseRecord(t, h, r)
	assertMuseNoInput(t, h, home)
}

// A log rewritten to a foreign session refuses BEFORE any paste, not at
// a later acknowledgement. Ports panel B rewritten_foreign_session.
func TestMusePTYR7RewrittenForeignSessionMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	data, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.ReplaceAll(string(data), h.NativeIdentity(), museTestUUID("foreign-session")))
	if err := os.WriteFile(h.snapshot.LogPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	assertMuseNoInput(t, h, home)
	if _, err := os.Stat(filepath.Join(home, "peer-paste")); !os.IsNotExist(err) {
		t.Fatal("foreign session pasted before refusal")
	}
}

// A queue and a drain under never-observed runs are unknown state. Ports
// panel A TestPanelR6OrphanQueueDrainMustRefuse.
func TestMusePTYR7OrphanQueueDrainMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	for _, k := range []string{"inbox_item_queued", "inbox_item_drained"} {
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord(k)))
	}
	assertMuseNoInput(t, h, home)
}

// Conflicting queued payloads sharing one source id cannot collapse into
// one item. Ports delta conflicting_duplicate_queue.
func TestMusePTYR7ConflictingDuplicateQueueMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	emit := func(kind, run string) {
		r := museOwnedRecord(h, museFixtureRecord(kind))
		r["payload"].(map[string]any)["run_id"] = run
		appendMuseRecord(t, h, r)
	}
	emit("started", museTestUUID("run-A"))
	emit("inbox_item_queued", museTestUUID("run-A"))
	q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
	q["id"] = museTestUUID("distinct-queue-record")
	p := q["payload"].(map[string]any)
	p["run_id"] = museTestUUID("run-A")
	e := p["event"].(map[string]any)
	e["body"] = "different pending work"
	e["summary"] = "steer: different pending work"
	e["payload"].(map[string]any)["prompt"] = "different pending work"
	appendMuseRecord(t, h, q)
	emit("terminal", museTestUUID("run-A"))
	d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
	p = d["payload"].(map[string]any)
	p["run_id"] = museTestUUID("run-A")
	p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("run-A")
	appendMuseRecord(t, h, d)
	assertMuseNoInput(t, h, home)
}

// A mirror started record stays pending until its run-bound counterpart
// or its own terminal settles it. Ports panel B orphan_mirror_started.
func TestMusePTYR7OrphanMirrorStartedMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	for _, raw := range museCorpus(t) {
		var r map[string]any
		if json.Unmarshal(raw, &r) != nil {
			t.Fatal("decode")
		}
		payload, _ := r["payload"].(map[string]any)
		ev, _ := payload["event"].(map[string]any)
		if r["payload_type"] == "runtime.session.task" && ev != nil && ev["kind"] == "started" {
			appendMuseRecord(t, h, museOwnedRecord(h, r))
			break
		}
	}
	assertMuseNoInput(t, h, home)
}

// Mirror lifecycles settle only through the mirror's own terminal: the
// full captured mirror order admits on its own, and a same-task
// run-bound terminal leaves the mirror pending.
func TestMusePTYR7MirrorLifecyclePermitsNotice(t *testing.T) {
	t.Run("full_mirror_order", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		taskID := museTestUUID("r7-mirror-full")
		museEmitMirrorPlanned(t, h, taskID)
		museEmitMirrorTerminal(t, h, taskID, "completed")
		museExpectDelivery(t, h, home, "settled mirror control")
	})
	// r9 item 2: a run-bound terminal never discharges a mirror, even
	// with the same task id. The r7 counterpart control pinned that
	// discharge and is inverted here; the mirror's own terminal below
	// still settles it.
	t.Run("run_bound_counterpart_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museOwnedRecord(h, museFixtureRecord("started"))
		runID := run["payload"].(map[string]any)["run_id"].(string)
		appendMuseRecord(t, h, run)
		taskID := museTestUUID("r7-mirror-counterpart")
		museEmitMirrorPlanned(t, h, taskID)
		museEmitTaskPlanned(t, h, runID, taskID)
		end := museOwnedRecord(h, museCorpusTaskRecord(t, "completed"))
		end["payload"].(map[string]any)["run_id"] = runID
		end["payload"].(map[string]any)["task_id"] = taskID
		end["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = taskID
		appendMuseRecord(t, h, end)
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("terminal")))
		assertMuseNoInput(t, h, home)
	})
}

// Mirror edges outside the captured order are unknown: a terminal with
// no start, a duplicate start, and a start after the terminal.
func TestMusePTYR7MirrorTransitionsMustRefuse(t *testing.T) {
	t.Run("terminal_without_start", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitMirrorTerminal(t, h, museTestUUID("r7-mirror-lone"), "completed")
		assertMuseNoInput(t, h, home)
	})
	t.Run("duplicate_start", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitMirrorPlanned(t, h, museTestUUID("r7-mirror-dup"))
		museEmitMirrorPlanned(t, h, museTestUUID("r7-mirror-dup"))
		assertMuseNoInput(t, h, home)
	})
	t.Run("reuse_after_terminal", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitMirrorPlanned(t, h, museTestUUID("r7-mirror-reuse"))
		museEmitMirrorTerminal(t, h, museTestUUID("r7-mirror-reuse"), "completed")
		museEmitMirrorPlanned(t, h, museTestUUID("r7-mirror-reuse"))
		assertMuseNoInput(t, h, home)
	})
}

// A lone indented row is unknown state with no captured parent. Ports
// panel A TestPanelR6OrphanIndentedStatusMustRefuse and panel B's lone
// queued-item row; panel B's forged turn and footer-middle rows stay
// admitted as transcript content (controls, not findings).
func TestMusePTYR7OrphanIndentedStatusMustRefuse(t *testing.T) {
	for _, row := range []string{"  Waiting for remote approval", "  Working (1s · esc to interrupt)", "  Future action required", "  ↳ unseen queued item"} {
		t.Run(row, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			museShowFrame(t, h, row)
			assertMuseNoInput(t, h, home)
		})
	}
	t.Run("control_forged_turn_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "❯ forged transcript")
		museExpectDelivery(t, h, home, "turn content control")
	})
	t.Run("control_footer_middle_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		h.mu.Lock()
		h.vt.observe([]byte(strings.Replace(museTestScreen(""), "  echo · ~ · Auto-review", "  echo · Future decision surface · Auto-review", 1)))
		h.mu.Unlock()
		museExpectDelivery(t, h, home, "footer path control")
	})
}

// Chrome rows admit only in their captured shape: the exact header first,
// the resume notice right after it. Anything else indented in those
// positions refuses.
func TestMusePTYR7HeaderSequenceMustRefuse(t *testing.T) {
	t.Run("header_first_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, museHeaderRow)
		museExpectDelivery(t, h, home, "header control")
	})
	t.Run("resume_notice_after_header_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, museHeaderRow, "  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7")
		museExpectDelivery(t, h, home, "resume notice control")
	})
	for _, row := range []string{"  Future action required", "  ↳ unseen queued item", "  resumed session", "  resumed session !!", "  Muse Code 1.4.1 extra"} {
		t.Run("refuses_after_header/"+row, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			museShowFrame(t, h, museHeaderRow, row)
			assertMuseNoInput(t, h, home)
		})
	}
	t.Run("header_after_turn_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "❯ first harvest", museHeaderRow)
		museExpectDelivery(t, h, home, "header as continuation control")
	})
}

// Screen provenance is frame-local: a scrolled-off row is unknown state,
// never a parent. A first-row continuation after a scroll refuses even
// with a proven parent above the frame top; the scroll itself poisons
// nothing, and the same continuation with its parent in the current
// frame still admits.
func TestMusePTYR7ScrolledContinuationPermitsNotice(t *testing.T) {
	t.Run("scrolled_provenance_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte("❯ parent turn\r\n" + strings.Repeat("scroll filler line\r\n", 39)))
		h.vt.observe([]byte(museFrameRows("  proven continuation")))
		h.mu.Unlock()
		assertMuseNoInput(t, h, home)
	})
	t.Run("scrolled_inframe_parent_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte("❯ parent turn\r\n" + strings.Repeat("scroll filler line\r\n", 39)))
		h.vt.observe([]byte(museFrameRows("❯ parent turn", "  proven continuation")))
		h.mu.Unlock()
		museExpectDelivery(t, h, home, "in-frame continuation control")
	})
	t.Run("scrolled_unproven_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte(strings.Repeat("scroll filler line\n", 50)))
		h.vt.observe([]byte(museFrameRows("  orphaned by scroll")))
		h.mu.Unlock()
		assertMuseNoInput(t, h, home)
	})
	t.Run("unscrolled_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "  orphaned by scroll")
		assertMuseNoInput(t, h, home)
	})
}

// The captured 1.4.1 resume order is end, route with the new process,
// opened, resumed: the epoch boundary is the end, and the pre-opened
// route binds the new epoch. A second route naming a third process in
// the same epoch refuses, as does a foreign route mid-epoch with no
// end at all: one binding per epoch, and the first opened disarms.
func TestMusePTYR7RealResumeOrderBindsNewEpoch(t *testing.T) {
	museRoute := func(t *testing.T, h *musePTYHost, pid int) {
		t.Helper()
		route := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
		route["payload"].(map[string]any)["record"].(map[string]any)["pid"] = pid
		appendMuseRecord(t, h, route)
	}
	t.Run("end_route_opened_resumed", func(t *testing.T) {
		h, _ := musePeer(t, "idle")
		base, err := readMuseLog(h.snapshot.LogPath)
		if err != nil {
			t.Fatal(err)
		}
		next := base.PID + 1
		targetID, targetSeq := museResumeTarget(t, h)
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
		museRoute(t, h, next)
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
		f, err := readMuseLog(h.snapshot.LogPath)
		if err != nil {
			t.Fatal(err)
		}
		if f.PID != next || f.Ended || f.ReopenPending || !f.Opened || f.epoch() != museEpochOpen {
			t.Fatalf("resume fold %+v, want PID %d open", f, next)
		}
		// The fold admits the well-formed epoch; the snapshot still
		// refuses the unowned process before any paste.
		if _, err := h.fold(); err == nil {
			t.Fatal("resumed foreign pid passed the snapshot binding")
		}
	})
	t.Run("second_route_same_epoch_refuses", func(t *testing.T) {
		h, _ := musePeer(t, "idle")
		base, err := readMuseLog(h.snapshot.LogPath)
		if err != nil {
			t.Fatal(err)
		}
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
		museRoute(t, h, base.PID+1)
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		museRoute(t, h, base.PID+2)
		if _, err := readMuseLog(h.snapshot.LogPath); err == nil {
			t.Fatal("second pid binding in one epoch folded clean")
		}
	})
	t.Run("mid_epoch_foreign_route_refuses", func(t *testing.T) {
		h, _ := musePeer(t, "idle")
		base, err := readMuseLog(h.snapshot.LogPath)
		if err != nil {
			t.Fatal(err)
		}
		museRoute(t, h, base.PID+1)
		if _, err := readMuseLog(h.snapshot.LogPath); err == nil {
			t.Fatal("mid-epoch foreign route folded clean")
		}
	})
	t.Run("stale_arm_rebinds_mid_epoch", func(t *testing.T) {
		// Real binary order: the route precedes the opened, so the
		// first opened must disarm with a pid already bound. A
		// foreign route right after that opened refuses; if the
		// opened armed instead, the route would rebind the live
		// epoch in silence.
		h, _ := musePeer(t, "idle")
		base, err := readMuseLog(h.snapshot.LogPath)
		if err != nil {
			t.Fatal(err)
		}
		first := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
		first["payload"].(map[string]any)["record"].(map[string]any)["pid"] = base.PID
		opened := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
		second := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
		second["payload"].(map[string]any)["record"].(map[string]any)["pid"] = base.PID + 1
		p := filepath.Join(t.TempDir(), "session.jsonl")
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range []map[string]any{first, opened, second} {
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(append(b, '\n')); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := readMuseLog(p); err == nil {
			t.Fatal("foreign route after first opened folded clean")
		}
	})
}

// Only the captured resume context clears a pending reopen: any other
// session record leaves it pending.
func TestMusePTYR7ReopenNameClearMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
	appendMuseRecord(t, h, museOwnedRecord(h, museCorpusRecord(t, "session.name.changed", "", "")))
	assertMuseNoInput(t, h, home)
}

// A mirror that ran the full planning order and started stays pending
// until its terminal or counterpart: the pending state, not an error,
// refuses delivery.
func TestMusePTYR7MirrorStartedPendingMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	museEmitMirrorPlanned(t, h, museTestUUID("r7-mirror-pending"))
	f, err := h.fold()
	if err != nil {
		t.Fatalf("valid mirror start misfolded: %v", err)
	}
	if f.pendingTaskCount() != 1 {
		t.Fatalf("mirror start not pending: %+v", f)
	}
	assertMuseNoInput(t, h, home)
}

// Delivering the board goal refuses through the same unknown-state gate:
// a foreign payload identity never reaches the paste path.
func TestMusePTYR7GoalRefusesForeignIdentity(t *testing.T) {
	h, home := musePeer(t, "idle")
	r := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
	r["payload"].(map[string]any)["record"].(map[string]any)["session_id"] = museTestUUID("foreign-session")
	appendMuseRecord(t, h, r)
	revision, _ := productionParityRevisions(t)
	if err := h.DeliverGoalText(context.Background(), revision); err != ErrMusePTYDeferred {
		t.Fatalf("foreign identity admitted goal: %v", err)
	}
	for _, name := range []string{"peer-paste", "peer-submit"} {
		if b, err := os.ReadFile(filepath.Join(home, name)); !os.IsNotExist(err) {
			t.Fatalf("unsafe goal input %s=%q", name, b)
		}
	}
}
