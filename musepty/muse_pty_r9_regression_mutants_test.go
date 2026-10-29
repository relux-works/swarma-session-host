package musepty

import (
	"testing"
)

// A mirror is discharged only by its own mirror terminal. A run-bound
// terminal with the same task id never discharges it. Ports delta
// TestR8PanelMirrorRequiresOwnTerminal and panel B
// TestPanelR8MirrorWithoutOwnTerminalMustRemainPending. The sentinel task
// is the r9-mirror-counterpart-only mutant target.
func TestMusePTYR9MirrorRequiresOwnTerminal(t *testing.T) {
	t.Run("own_terminal_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		task := museTestUUID("r9-mirror-own-terminal")
		museEmitMirrorPlanned(t, h, task)
		museEmitMirrorTerminal(t, h, task, "completed")
		museExpectDelivery(t, h, home, "complete mirror control")
	})
	t.Run("run_bound_terminal_is_not_mirror_terminal", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-mirror-run")
		task := museTestUUID("r9-mirror-sentinel")
		museEmitMirrorPlanned(t, h, task)
		museEmitRunBounded(t, h, "started", run)
		museEmitTaskPlanned(t, h, run, task)
		museR9ChildTerminal(t, h, run, task, "completed")
		museEmitRunBounded(t, h, "terminal", run)
		f, err := h.fold()
		if err != nil {
			t.Fatalf("mirror prefix misfolded: %v", err)
		}
		if f.pendingTaskCount() != 1 {
			t.Fatalf("mirror without own terminal not pending: %+v", f)
		}
		museR9Refuse(t, h, home)
	})
	t.Run("panel_b_prefix_plus_counterpart", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		task := museTestUUID("unfinished-mirror")
		for _, event := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started"} {
			appendMuseRecord(t, h, museR7MirrorRecord(t, h, event, task))
		}
		museEmitRunBounded(t, h, "started", museTestUUID("counterpart-run"))
		for _, event := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"} {
			appendMuseRecord(t, h, museR7TaskRecord(t, h, event, museTestUUID("counterpart-run"), task))
		}
		museEmitRunBounded(t, h, "terminal", museTestUUID("counterpart-run"))
		museR9Refuse(t, h, home)
	})
}

// A committed message referring to an observed model response matches
// that response in its own run. Ports panel A
// TestR141R8ResponseIdentityMustMatch (mismatch plus matched control),
// with the duplicate-created, message-replay and reasoning edges of the
// same join. A run that observed no response carries no witness to
// match: the sixth captured message commits without one and still
// admits. The sentinel run is the r9-response-mismatch-only and
// r9-response-duplicate-only mutant target.
func TestMusePTYR9ResponseIdentityMustMatch(t *testing.T) {
	emitCreated := func(t *testing.T, h *musePTYHost, run, response string) {
		t.Helper()
		created := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_response_created"))
		created["payload"].(map[string]any)["run_id"] = run
		created["payload"].(map[string]any)["event"].(map[string]any)["response_id"] = response
		appendMuseRecord(t, h, created)
	}
	emitMessage := func(t *testing.T, h *musePTYHost, run, response string) {
		t.Helper()
		message := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "assistant_message_committed"))
		p := message["payload"].(map[string]any)
		p["run_id"] = run
		p["event"].(map[string]any)["response_id"] = response
		appendMuseRecord(t, h, message)
	}
	t.Run("matched_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-response-control")
		museEmitRunBounded(t, h, "started", run)
		emitCreated(t, h, run, "response-a")
		emitMessage(t, h, run, "response-a")
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "matched response control")
	})
	t.Run("mismatched_response", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-response-sentinel")
		museEmitRunBounded(t, h, "started", run)
		emitCreated(t, h, run, "response-a")
		emitMessage(t, h, run, "response-b")
		museEmitRunBounded(t, h, "terminal", run)
		museR9Refuse(t, h, home)
	})
	t.Run("duplicate_response_created", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-response-sentinel")
		museEmitRunBounded(t, h, "started", run)
		emitCreated(t, h, run, "response-a")
		emitCreated(t, h, run, "response-a")
		museEmitRunBounded(t, h, "terminal", run)
		museR9Refuse(t, h, home)
	})
	t.Run("duplicate_message", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-message-dup-sentinel")
		museEmitRunBounded(t, h, "started", run)
		emitMessage(t, h, run, "response-a")
		emitMessage(t, h, run, "response-a")
		museEmitRunBounded(t, h, "terminal", run)
		museR9Refuse(t, h, home)
	})
	t.Run("message_without_created_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-response-unwitnessed")
		museEmitRunBounded(t, h, "started", run)
		emitMessage(t, h, run, "response-a")
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "unwitnessed message control")
	})
	t.Run("reasoning_matched_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-reasoning-control")
		museEmitRunBounded(t, h, "started", run)
		emitCreated(t, h, run, "response-a")
		reasoning := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "reasoning_committed"))
		reasoning["payload"].(map[string]any)["run_id"] = run
		reasoning["payload"].(map[string]any)["event"].(map[string]any)["response_id"] = "response-a"
		appendMuseRecord(t, h, reasoning)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "matched reasoning control")
	})
	t.Run("reasoning_mismatched_response", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-reasoning-sentinel")
		museEmitRunBounded(t, h, "started", run)
		emitCreated(t, h, run, "response-a")
		reasoning := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "reasoning_committed"))
		reasoning["payload"].(map[string]any)["run_id"] = run
		reasoning["payload"].(map[string]any)["event"].(map[string]any)["response_id"] = "response-b"
		appendMuseRecord(t, h, reasoning)
		museEmitRunBounded(t, h, "terminal", run)
		museR9Refuse(t, h, home)
	})
}

// A cleanup terminal reports the bound child's outcome: the child the
// start named must have terminated with exactly the claimed kind. Ports
// panel A TestR141R8CleanupChildOutcomeMustMatch plus its captured
// control, with the order edge (the child terminates before the cleanup
// reports it). The sentinel task is the r9-cleanup-child-only mutant
// target.
func TestMusePTYR9CleanupChildOutcomeMustMatch(t *testing.T) {
	setup := func(t *testing.T) (*musePTYHost, string, string, string, map[string]any, map[string]any) {
		t.Helper()
		h, home := musePeer(t, "idle")
		pair := museR8CleanupPairs(t)[0]
		start := museOwnedRecord(h, pair.Start)
		p := start["payload"].(map[string]any)
		run := p["run_id"].(string)
		task := museTestUUID("r9-cleanup-sentinel")
		p["record"].(map[string]any)["child_task"].(map[string]any)["task_id"] = task
		p["record"].(map[string]any)["child_task"].(map[string]any)["task_stream"].(map[string]any)["id"] = task
		terminal := museOwnedRecord(h, pair.Terminal)
		terminal["payload"].(map[string]any)["run_id"] = run
		if terminal["payload"].(map[string]any)["record"].(map[string]any)["outcome"].(map[string]any)["child_terminal"].(map[string]any)["kind"] != "cancelled" {
			t.Fatal("capture changed")
		}
		return h, home, run, task, start, terminal
	}
	t.Run("captured_order_control", func(t *testing.T) {
		h, home, run, task, start, terminal := setup(t)
		museEmitRunBounded(t, h, "started", run)
		museEmitTaskPlanned(t, h, run, task)
		appendMuseRecord(t, h, start)
		museR9ChildTerminal(t, h, run, task, "cancelled")
		appendMuseRecord(t, h, terminal)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "captured cleanup control")
	})
	t.Run("child_completed_claimed_cancelled", func(t *testing.T) {
		h, home, run, task, start, terminal := setup(t)
		museEmitRunBounded(t, h, "started", run)
		museEmitTaskPlanned(t, h, run, task)
		appendMuseRecord(t, h, start)
		museR9ChildTerminal(t, h, run, task, "completed")
		appendMuseRecord(t, h, terminal)
		museEmitRunBounded(t, h, "terminal", run)
		museR9Refuse(t, h, home)
	})
	t.Run("cleanup_terminal_before_child_terminal", func(t *testing.T) {
		h, home, run, task, start, terminal := setup(t)
		museEmitRunBounded(t, h, "started", run)
		museEmitTaskPlanned(t, h, run, task)
		appendMuseRecord(t, h, start)
		appendMuseRecord(t, h, terminal)
		museR9ChildTerminal(t, h, run, task, "cancelled")
		museEmitRunBounded(t, h, "terminal", run)
		museR9Refuse(t, h, home)
	})
}

// A cleanup terminal lands inside the parent run's life: a terminated
// parent is unknown state. Ports panel A
// TestR141R8CleanupAfterRunTerminalMustRefuse plus its order control.
// The sentinel run is the r9-cleanup-epoch-only mutant target.
func TestMusePTYR9CleanupTerminalRequiresActiveParent(t *testing.T) {
	setup := func(t *testing.T) (*musePTYHost, string, string, string, map[string]any, map[string]any) {
		t.Helper()
		h, home := musePeer(t, "idle")
		pair := museR8CleanupPairs(t)[0]
		start := museOwnedRecord(h, pair.Start)
		p := start["payload"].(map[string]any)
		capturedRun := p["run_id"].(string)
		run := museTestUUID("r9-cleanup-epoch-sentinel")
		p["run_id"] = run
		p["record"].(map[string]any)["parent_run_stream"].(map[string]any)["id"] = run
		task := p["record"].(map[string]any)["child_task"].(map[string]any)["task_id"].(string)
		terminal := museOwnedRecord(h, pair.Terminal)
		terminal["payload"].(map[string]any)["run_id"] = run
		_ = capturedRun
		return h, home, run, task, start, terminal
	}
	t.Run("terminal_before_parent_terminal_control", func(t *testing.T) {
		h, home, run, task, start, terminal := setup(t)
		museEmitRunBounded(t, h, "started", run)
		museEmitTaskPlanned(t, h, run, task)
		appendMuseRecord(t, h, start)
		museR9ChildTerminal(t, h, run, task, "cancelled")
		appendMuseRecord(t, h, terminal)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "captured cleanup order control")
	})
	t.Run("terminal_after_parent_terminal", func(t *testing.T) {
		h, home, run, task, start, terminal := setup(t)
		museEmitRunBounded(t, h, "started", run)
		museEmitTaskPlanned(t, h, run, task)
		appendMuseRecord(t, h, start)
		museR9ChildTerminal(t, h, run, task, "cancelled")
		museEmitRunBounded(t, h, "terminal", run)
		appendMuseRecord(t, h, terminal)
		museR9Refuse(t, h, home)
	})
}

// A materialized intent references the exact accepted envelope id and
// sequence. Ports panel B TestPanelR8EnvelopeIdentityMustRefuse (both
// envelope vectors plus the matched control). The sentinel intent is
// the r9-envelope-only mutant target.
func TestMusePTYR9IntentEnvelopeMustRefuse(t *testing.T) {
	for _, field := range []string{"matched_control", "envelope_record_id", "envelope_sequence"} {
		t.Run(field, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			a := museR9AcceptIntent(t, h, museTestUUID("r9-envelope-sentinel"))
			run := museTestUUID("r9-envelope-sentinel")
			started := museR9EmitRunStarted(t, h, run)
			m := museR9TopLevelOutcome(t, h, a, started)
			switch field {
			case "envelope_record_id":
				m["payload"].(map[string]any)["envelope_record_id"] = museTestUUID("foreign-envelope")
			case "envelope_sequence":
				m["payload"].(map[string]any)["envelope_sequence"] = 987654
			}
			appendMuseRecord(t, h, m)
			museEmitRunBounded(t, h, "terminal", run)
			if field == "matched_control" {
				museExpectDelivery(t, h, home, "matched envelope control")
				return
			}
			museR9Refuse(t, h, home)
		})
	}
}

// A top-level outcome resolves the exact started record it cites: the
// session record id and the source record id of the run's start. Ports
// panel B TestPanelR8EnvelopeIdentityMustRefuse (both started-record
// vectors). The sentinel intent is the r9-started-refs-only mutant
// target.
func TestMusePTYR9IntentStartedRefsMustRefuse(t *testing.T) {
	for _, field := range []string{"run_started_session_record_id", "run_started_source_record_id"} {
		t.Run(field, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			a := museR9AcceptIntent(t, h, museTestUUID("r9-started-refs-sentinel"))
			run := museTestUUID("r9-started-refs-sentinel")
			started := museR9EmitRunStarted(t, h, run)
			m := museR9TopLevelOutcome(t, h, a, started)
			m["payload"].(map[string]any)["outcome"].(map[string]any)[field] = museTestUUID("foreign-start")
			appendMuseRecord(t, h, m)
			museEmitRunBounded(t, h, "terminal", run)
			museR9Refuse(t, h, home)
		})
	}
	t.Run("matched_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		a := museR9AcceptIntent(t, h, museTestUUID("r9-started-refs-control"))
		run := museTestUUID("r9-started-refs-control")
		started := museR9EmitRunStarted(t, h, run)
		appendMuseRecord(t, h, museR9TopLevelOutcome(t, h, a, started))
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "matched started-refs control")
	})
}

// A top-level outcome names the run the intent produced: the intent's
// own run, started after the acceptance. A previously finished
// unrelated run never settles a new top-level intent. Ports panel B
// TestPanelR8TopLevelIntentMustNotBindPriorRun, with the order edge (a
// start before the acceptance is not the intent's run). The sentinel
// intents are the r9-top-level-run-only and r9-top-level-order-only
// mutant targets.
func TestMusePTYR9TopLevelIntentMustProduceRun(t *testing.T) {
	t.Run("prior_finished_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		prior := museR9EmitRunStarted(t, h, museTestUUID("prior-finished-run"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("prior-finished-run"))
		a := museR9AcceptIntent(t, h, museTestUUID("r9-top-level-sentinel"))
		m := museR9TopLevelOutcome(t, h, a, prior)
		m["payload"].(map[string]any)["outcome"].(map[string]any)["run_id"] = museTestUUID("prior-finished-run")
		appendMuseRecord(t, h, m)
		museR9Refuse(t, h, home)
	})
	t.Run("unrelated_run_started_after_acceptance", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		a := museR9AcceptIntent(t, h, museTestUUID("r9-top-level-sentinel"))
		prior := museR9EmitRunStarted(t, h, museTestUUID("prior-finished-run"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("prior-finished-run"))
		m := museR9TopLevelOutcome(t, h, a, prior)
		m["payload"].(map[string]any)["outcome"].(map[string]any)["run_id"] = museTestUUID("prior-finished-run")
		appendMuseRecord(t, h, m)
		museR9Refuse(t, h, home)
	})
	t.Run("run_started_before_acceptance", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-top-level-order-sentinel")
		started := museR9EmitRunStarted(t, h, run)
		a := museR9AcceptIntent(t, h, run)
		appendMuseRecord(t, h, museR9TopLevelOutcome(t, h, a, started))
		museEmitRunBounded(t, h, "terminal", run)
		museR9Refuse(t, h, home)
	})
	t.Run("matched_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		a := museR9AcceptIntent(t, h, museTestUUID("r9-top-level-control"))
		run := museTestUUID("r9-top-level-control")
		started := museR9EmitRunStarted(t, h, run)
		appendMuseRecord(t, h, museR9TopLevelOutcome(t, h, a, started))
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "matched top-level control")
	})
}

// The accepted intent lookup narrows to the stored intent id: a
// materialized outcome with every other reference aligned still refuses
// when its intent was never accepted. No other intent is pending, so
// only the lookup gate distinguishes this shape. The sentinel intent is
// the r8-intent-pairing-one-only mutant target.
func TestMusePTYR9IntentLookupMustRefuse(t *testing.T) {
	t.Run("wrong_id_with_aligned_refs", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("intent-B")
		started := museR9EmitRunStarted(t, h, run)
		m := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.materialized", "", ""))
		p := m["payload"].(map[string]any)
		p["intent_id"] = museTestUUID("intent-B")
		p["envelope_record_id"] = museTestUUID("r9-lookup-envelope")
		p["envelope_sequence"] = 4242
		o := p["outcome"].(map[string]any)
		o["run_id"] = run
		o["run_started_session_record_id"] = started["id"]
		o["run_started_source_record_id"] = started["payload"].(map[string]any)["source_run_record_id"]
		appendMuseRecord(t, h, m)
		museEmitRunBounded(t, h, "terminal", run)
		museR9Refuse(t, h, home)
	})
}

// An active-run delivery cites the queued record it delivers: without
// that queue record the outcome is unknown state. Ports panel B
// TestPanelR8ActiveDeliveryNeedsItsQueueRecord, with the matched
// control (queue, drain, accepted, delivery). The sentinel intent is
// the r9-delivery-queue-only mutant target.
func TestMusePTYR9ActiveDeliveryNeedsQueue(t *testing.T) {
	emitDelivery := func(t *testing.T, h *musePTYHost, intentID, run string, queue map[string]any) {
		t.Helper()
		a := museR9AcceptIntent(t, h, intentID)
		m := museOwnedRecord(h, museR9CorpusMaterialized(t, "active_run_delivery"))
		p := m["payload"].(map[string]any)
		p["intent_id"] = intentID
		p["envelope_record_id"] = a["id"]
		p["envelope_sequence"] = a["sequence"]
		o := p["outcome"].(map[string]any)
		o["run_id"] = run
		if queue != nil {
			o["delivery_record_id"] = queue["id"]
		}
		appendMuseRecord(t, h, m)
	}
	t.Run("missing_queue_record", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-delivery-run")
		museR9EmitRunStarted(t, h, run)
		emitDelivery(t, h, museTestUUID("r9-delivery-sentinel"), run, nil)
		// The corpus delivery record id names a queue record the log
		// never observed.
		museEmitRunBounded(t, h, "terminal", run)
		museR9Refuse(t, h, home)
	})
	t.Run("matched_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		run := museTestUUID("r9-delivery-run")
		// r10: the delivery cites the queue of its own intent
		// and run, queued after the acceptance. The intent is
		// the drain target run, as in the captured 1/1 shape.
		a := museR9AcceptIntent(t, h, museTestUUID("r9-delivery-target"))
		museR9EmitRunStarted(t, h, run)
		// The queued item drains before the delivery resolves it: the
		// delivery cites the queue observation, not a pending item.
		museR9EmitRunStarted(t, h, museTestUUID("r9-delivery-target"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("r9-delivery-target"))
		q := museR9QueueItem(t, h, run, museTestUUID("r9-delivery-target"))
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		p := d["payload"].(map[string]any)
		p["run_id"] = museTestUUID("r9-delivery-target")
		p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("r9-delivery-target")
		appendMuseRecord(t, h, d)
		m := museOwnedRecord(h, museR9CorpusMaterialized(t, "active_run_delivery"))
		mp := m["payload"].(map[string]any)
		mp["intent_id"] = museTestUUID("r9-delivery-target")
		mp["envelope_record_id"] = a["id"]
		mp["envelope_sequence"] = a["sequence"]
		mo := mp["outcome"].(map[string]any)
		mo["run_id"] = run
		mo["delivery_record_id"] = q["id"]
		appendMuseRecord(t, h, m)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "matched delivery control")
	})
}
