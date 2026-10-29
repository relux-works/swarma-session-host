package musepty

import (
	"testing"
)

// A terminated run id may not start again. Ports panel A
// TestPanelR6RunIDReuseMustRefuse and panel B run_reuse.
func TestMusePTYR7RunIDReuseMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	// Fixed sentinel id: the m2-run-reuse-one-only mutant restarts
	// exactly this run.
	for _, k := range []string{"started", "terminal", "started", "terminal"} {
		r := museOwnedRecord(h, museFixtureRecord(k))
		r["payload"].(map[string]any)["run_id"] = museTestUUID("r7-run-reuse")
		appendMuseRecord(t, h, r)
	}
	assertMuseNoInput(t, h, home)
}

// Planning order is the captured order: an accepted after the task
// started is unknown state. Ports panel B task_planning_regression.
func TestMusePTYR7TaskPlanningRegressionMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	run := museOwnedRecord(h, museFixtureRecord("started"))
	runID := run["payload"].(map[string]any)["run_id"].(string)
	appendMuseRecord(t, h, run)
	// Fixed sentinel id: the m2-task-order-one-only mutant admits the
	// accepted-after-started of exactly this task.
	taskID := museTestUUID("r7-task-order")
	museEmitTaskPlanned(t, h, runID, taskID)
	back := museOwnedRecord(h, museCorpusTaskRecord(t, "accepted"))
	back["payload"].(map[string]any)["run_id"] = runID
	back["payload"].(map[string]any)["task_id"] = taskID
	back["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = taskID
	appendMuseRecord(t, h, back)
	// Settles the run past the refusing record so the mutant that admits
	// the regression reaches idle instead of a pending run.
	end := museOwnedRecord(h, museFixtureRecord("terminal"))
	end["payload"].(map[string]any)["run_id"] = runID
	appendMuseRecord(t, h, end)
	assertMuseNoInput(t, h, home)
}

// A task may not start under a terminated run. Ports panel A
// TestPanelR6TaskStartAfterRunTerminalMustRefuse, both in its original
// shape and with the full lifecycle isolating the run rule.
func TestMusePTYR7TaskStartAfterRunTerminalMustRefuse(t *testing.T) {
	t.Run("panel_shape", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		for _, k := range []string{"started", "terminal"} {
			appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord(k)))
		}
		id := museFixtureRecord("started")["payload"].(map[string]any)["run_id"].(string)
		task := museCorpusTaskRecord(t, "started")["payload"].(map[string]any)["task_id"].(string)
		for _, k := range []string{"started", "completed"} {
			r := museOwnedRecord(h, museCorpusTaskRecord(t, k))
			p := r["payload"].(map[string]any)
			p["run_id"] = id
			p["task_id"] = task
			p["event"].(map[string]any)["task_id"] = task
			appendMuseRecord(t, h, r)
		}
		assertMuseNoInput(t, h, home)
	})
	t.Run("planning_under_terminated_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		// Fixed sentinel id: the m2-task-run-one-only mutant admits
		// tasks under exactly this terminated run.
		run := museOwnedRecord(h, museFixtureRecord("started"))
		run["payload"].(map[string]any)["run_id"] = museTestUUID("r7-task-run")
		appendMuseRecord(t, h, run)
		end := museOwnedRecord(h, museFixtureRecord("terminal"))
		end["payload"].(map[string]any)["run_id"] = museTestUUID("r7-task-run")
		appendMuseRecord(t, h, end)
		// The full lifecycle runs under the terminated run: unmutated,
		// the first record already refuses; the m2-task-run-one-only
		// mutant admits the whole lifecycle and must deliver.
		task := museCorpusTaskRecord(t, "proposed")["payload"].(map[string]any)["task_id"].(string)
		for _, event := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"} {
			appendMuseRecord(t, h, museR7TaskRecord(t, h, event, museTestUUID("r7-task-run"), task))
		}
		assertMuseNoInput(t, h, home)
	})
}

// An opened record whose payload session id is foreign is unknown state.
// Ports panel A TestPanelR6OpenedIdentityMismatchMustRefuse (filed as a
// note there) and panel B mismatched_opened_payload.
func TestMusePTYR7OpenedIdentityMismatchMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	targetID, targetSeq := museResumeTarget(t, h)
	r := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
	r["payload"].(map[string]any)["record"].(map[string]any)["session_id"] = museTestUUID("different-session")
	appendMuseRecord(t, h, r)
	// The second opened reopens the session, so the captured resume
	// context witnesses it; the foreign payload identity itself must
	// still refuse. The m1-opened-session-one-only mutant admits exactly
	// this session id and must deliver through the resume, so the
	// resumed reference resolves to the prior epoch.
	appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
	assertMuseNoInput(t, h, home)
}

// A second queue with the same source id is unknown state, even with a
// drain settling the pair. Ports panel A
// TestPanelR6DuplicateQueueMustRefuse and panel B duplicate_queue, whose
// drain positions differ.
func TestMusePTYR7DuplicateQueueMustRefuse(t *testing.T) {
	// The drain target run is observed so the m3-duplicate-queue-one-only
	// mutant discharges the admitted duplicate and must deliver.
	emitWithTarget := func(t *testing.T, h *musePTYHost, kinds []string) {
		t.Helper()
		for _, k := range kinds {
			if k == "inbox_item_drained" {
				target := museFixtureRecord("inbox_item_drained")["payload"].(map[string]any)["run_id"].(string)
				museEmitRunBounded(t, h, "started", target)
				museEmitRunBounded(t, h, "terminal", target)
			}
			appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord(k)))
		}
	}
	t.Run("panel_a_drain_after_terminal", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		emitWithTarget(t, h, []string{"started", "inbox_item_queued", "inbox_item_queued", "terminal", "inbox_item_drained"})
		assertMuseNoInput(t, h, home)
	})
	t.Run("panel_b_drain_before_terminal", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		emitWithTarget(t, h, []string{"started", "inbox_item_queued", "inbox_item_queued", "inbox_item_drained", "terminal"})
		assertMuseNoInput(t, h, home)
	})
}

// A drain whose target names another run than its payload run is unknown
// state. Ports delta mismatched_drain_target with its matched control.
// The queued forward identity matches the drain target in every case, so
// the refusal stays attributable to the target/run split rather than to
// the forward rule; the mismatched target run is observed so the pending
// rule cannot refuse ahead of it either.
func TestMusePTYR7DrainTargetMismatchMustRefuse(t *testing.T) {
	emit := func(t *testing.T, h *musePTYHost, kind, run string) {
		t.Helper()
		r := museOwnedRecord(h, museFixtureRecord(kind))
		r["payload"].(map[string]any)["run_id"] = run
		appendMuseRecord(t, h, r)
	}
	emitQueue := func(t *testing.T, h *musePTYHost, run, forward string) {
		t.Helper()
		r := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
		r["payload"].(map[string]any)["run_id"] = run
		r["causation_id"] = forward
		appendMuseRecord(t, h, r)
	}
	t.Run("matched_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		emit(t, h, "started", museTestUUID("run-A"))
		emitQueue(t, h, museTestUUID("run-A"), museTestUUID("run-A"))
		emit(t, h, "terminal", museTestUUID("run-A"))
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		p := d["payload"].(map[string]any)
		p["run_id"] = museTestUUID("run-A")
		p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("run-A")
		appendMuseRecord(t, h, d)
		museExpectDelivery(t, h, home, "valid queue control")
	})
	t.Run("mismatched_drain_target", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		emit(t, h, "started", museTestUUID("run-A"))
		// Fixed sentinel id: the m3-drain-target-one-only mutant admits
		// the target/run split for exactly this target.
		emitQueue(t, h, museTestUUID("run-A"), museTestUUID("unobserved-foreign-target"))
		emit(t, h, "terminal", museTestUUID("run-A"))
		emit(t, h, "started", museTestUUID("unobserved-foreign-target"))
		emit(t, h, "terminal", museTestUUID("unobserved-foreign-target"))
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		p := d["payload"].(map[string]any)
		p["run_id"] = museTestUUID("run-A")
		p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("unobserved-foreign-target")
		appendMuseRecord(t, h, d)
		assertMuseNoInput(t, h, home)
	})
}

// A drain whose snapshot differs from the stored queued content is
// unknown state; a drain into a run the owned stream never started keeps
// its wire shape but stays pending and never certifies idle.
func TestMusePTYR7DrainCorrelationMustRefuse(t *testing.T) {
	emit := func(t *testing.T, h *musePTYHost, kind, run string) {
		t.Helper()
		r := museOwnedRecord(h, museFixtureRecord(kind))
		r["payload"].(map[string]any)["run_id"] = run
		appendMuseRecord(t, h, r)
	}
	emitQueue := func(t *testing.T, h *musePTYHost, run, forward string) {
		t.Helper()
		r := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
		r["payload"].(map[string]any)["run_id"] = run
		r["causation_id"] = forward
		appendMuseRecord(t, h, r)
	}
	t.Run("snapshot_differs", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		emit(t, h, "started", museTestUUID("run-A"))
		emitQueue(t, h, museTestUUID("run-A"), museTestUUID("run-A"))
		emit(t, h, "terminal", museTestUUID("run-A"))
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		p := d["payload"].(map[string]any)
		p["run_id"] = museTestUUID("run-A")
		p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("run-A")
		p["event"].(map[string]any)["delivery_snapshot"].(map[string]any)["body"] = "rewritten work"
		appendMuseRecord(t, h, d)
		assertMuseNoInput(t, h, home)
	})
	t.Run("drain_run_unobserved", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		emit(t, h, "started", museTestUUID("run-A"))
		// Fixed sentinel id: the m3-drain-run-one-only mutant clears
		// the pending target for exactly this run.
		emitQueue(t, h, museTestUUID("run-A"), museTestUUID("never-observed-run"))
		emit(t, h, "terminal", museTestUUID("run-A"))
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		p := d["payload"].(map[string]any)
		p["run_id"] = museTestUUID("never-observed-run")
		p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("never-observed-run")
		appendMuseRecord(t, h, d)
		f, err := h.fold()
		if err != nil {
			t.Fatalf("forward-only drain misfolded: %v", err)
		}
		if len(f.DrainTargetPending) != 1 {
			t.Fatalf("forward drain target not pending: %+v", f)
		}
		assertMuseNoInput(t, h, home)
	})
	t.Run("queue_after_drain", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		emit(t, h, "started", museTestUUID("run-A"))
		emitQueue(t, h, museTestUUID("run-A"), museTestUUID("run-A"))
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		p := d["payload"].(map[string]any)
		p["run_id"] = museTestUUID("run-A")
		p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("run-A")
		appendMuseRecord(t, h, d)
		// The run is still active, so only the drained-item rule refuses.
		emit(t, h, "inbox_item_queued", museTestUUID("run-A"))
		assertMuseNoInput(t, h, home)
	})
	t.Run("drain_twice", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		emit(t, h, "started", museTestUUID("run-A"))
		emitQueue(t, h, museTestUUID("run-A"), museTestUUID("run-A"))
		emit(t, h, "terminal", museTestUUID("run-A"))
		for i := 0; i < 2; i++ {
			d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
			p := d["payload"].(map[string]any)
			p["run_id"] = museTestUUID("run-A")
			p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("run-A")
			appendMuseRecord(t, h, d)
		}
		assertMuseNoInput(t, h, home)
	})
}

// A task proposed without a run identity refuses. The proposed edge is
// the only admission from the absent state, so skipping the run gate
// for it reaches idle: the task-empty-run-one-only mutant proves the
// gate narrows to exactly the empty run.
func TestMusePTYR7TaskEmptyRunMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	// The full lifecycle runs under the empty run id: unmutated, the
	// first record already refuses; the task-empty-run-only mutant admits
	// the whole lifecycle and must deliver.
	for _, event := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"} {
		appendMuseRecord(t, h, museR7TaskRecord(t, h, event, "", museTestUUID("r7-empty-run-task")))
	}
	assertMuseNoInput(t, h, home)
}

// A resumed epoch that rebinds another process refuses at the snapshot,
// even though the epoch transition itself is well formed.
func TestMusePTYR7ResumedForeignPIDMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	targetID, targetSeq := museResumeTarget(t, h)
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
	route := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
	route["payload"].(map[string]any)["record"].(map[string]any)["pid"] = 99
	appendMuseRecord(t, h, route)
	// The resumed reference resolves: the m1-snapshot-pid-one-only
	// mutant admits pid 99 and must deliver through the resume.
	appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
	assertMuseNoInput(t, h, home)
}
