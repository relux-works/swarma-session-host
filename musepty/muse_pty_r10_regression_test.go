package musepty

// Revision r10: every decoded identity joined, and log-backed screen
// provenance. Every round-9 panel reproduction is ported here as a named
// production-entry test through DeliverSessionNotice. Sources:
//   - delta (regression): TestR141R9ActiveDeliveryIdentity in
//     independent_probes_test.go (task evidence archive)
//   - panel A (regression): TestR141R9* in
//     regression_probes_test.go
//   - panel B (regression): TestPanelR9* in
//     regression_probes_test.go (reconstruction in verdict)
//
// Two panel-B probe shapes were ported as STATED BOUNDS in r10. Both
// are removed in r11: the orphan resumed refuses, and a malformed
// frame digest refuses, while a well-formed but unverifiable digest
// stays a pinned Stop-The-Line admission (no derivation reproduces
// the captured digests; see the r11 regression file).

import (
	"encoding/json"
	"testing"
)

// museR10QueueAndDrain appends a queued inbox item under run with the
// given forward causation and drains it into target, returning the
// appended queue record. Causation, drain target and drain run must all
// name target (J16-J17); the caller aligns the intent separately.
func museR10QueueAndDrain(t *testing.T, h *musePTYHost, run, causation, target string) map[string]any {
	t.Helper()
	q := museR9QueueItem(t, h, run, causation)
	d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
	dp := d["payload"].(map[string]any)
	dp["run_id"] = target
	dp["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = target
	appendMuseRecord(t, h, d)
	return q
}

// museR10ActiveDelivery appends a materialized active-run delivery for
// the accepted intent record, citing the queue record and outcome run.
func museR10ActiveDelivery(t *testing.T, h *musePTYHost, accepted, queue map[string]any, intent, run string) {
	t.Helper()
	m := museOwnedRecord(h, museR9CorpusMaterialized(t, "active_run_delivery"))
	p := m["payload"].(map[string]any)
	p["intent_id"] = intent
	p["envelope_record_id"] = accepted["id"]
	p["envelope_sequence"] = accepted["sequence"]
	o := p["outcome"].(map[string]any)
	o["run_id"] = run
	o["delivery_record_id"] = queue["id"]
	appendMuseRecord(t, h, m)
}

// museR10EmitCreated appends a created-response record for the run.
func museR10EmitCreated(t *testing.T, h *musePTYHost, run, response string) {
	t.Helper()
	created := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_response_created"))
	created["payload"].(map[string]any)["run_id"] = run
	created["payload"].(map[string]any)["event"].(map[string]any)["response_id"] = response
	appendMuseRecord(t, h, created)
}

// museR10EmitMessage appends a committed message with the given
// response, message id and text for the run.
func museR10EmitMessage(t *testing.T, h *musePTYHost, run, response, msgID, text string) {
	t.Helper()
	m := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "assistant_message_committed"))
	p := m["payload"].(map[string]any)
	p["run_id"] = run
	ev := p["event"].(map[string]any)
	ev["response_id"] = response
	ev["message_id"] = msgID
	ev["text"] = text
	appendMuseRecord(t, h, m)
}

// An active-run delivery cites the queue of its own intent and run,
// observed after the acceptance: queue.causation_id = intent_id and
// queue.payload.run_id = outcome.run_id (captured 1/1), accepted
// before queue. Ports delta wrong_known_run + wrong_intent, panel A
// foreign_queue_run + foreign_queue_intent, and panel B wrong_queue_run
// + stale_queue_before_acceptance + wrong_intent.
func TestMusePTYR10ActiveDeliveryExactJoins(t *testing.T) {
	setup := func(t *testing.T) (*musePTYHost, string) {
		t.Helper()
		h, home := musePeer(t, "idle")
		museR9EmitRunStarted(t, h, museTestUUID("queue-owner"))
		museR9EmitRunStarted(t, h, museTestUUID("drain-target"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("drain-target"))
		return h, home
	}
	t.Run("matched_control", func(t *testing.T) {
		h, home := setup(t)
		a := museR9AcceptIntent(t, h, museTestUUID("drain-target"))
		q := museR10QueueAndDrain(t, h, museTestUUID("queue-owner"), museTestUUID("drain-target"), museTestUUID("drain-target"))
		museR10ActiveDelivery(t, h, a, q, museTestUUID("drain-target"), museTestUUID("queue-owner"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("queue-owner"))
		museExpectDelivery(t, h, home, "matched queue joins")
	})
	t.Run("wrong_queue_run", func(t *testing.T) {
		h, home := setup(t)
		museR9EmitRunStarted(t, h, museTestUUID("unrelated-run"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("unrelated-run"))
		a := museR9AcceptIntent(t, h, museTestUUID("drain-target"))
		q := museR10QueueAndDrain(t, h, museTestUUID("queue-owner"), museTestUUID("drain-target"), museTestUUID("drain-target"))
		// The outcome names a known run that never owned the queue.
		museR10ActiveDelivery(t, h, a, q, museTestUUID("drain-target"), museTestUUID("unrelated-run"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("queue-owner"))
		museR9Refuse(t, h, home)
	})
	t.Run("wrong_intent", func(t *testing.T) {
		h, home := setup(t)
		a := museR9AcceptIntent(t, h, museTestUUID("unrelated-intent"))
		q := museR10QueueAndDrain(t, h, museTestUUID("queue-owner"), museTestUUID("drain-target"), museTestUUID("drain-target"))
		// The queue drains for another intent than the accepted one.
		museR10ActiveDelivery(t, h, a, q, museTestUUID("unrelated-intent"), museTestUUID("queue-owner"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("queue-owner"))
		museR9Refuse(t, h, home)
	})
	t.Run("stale_queue_before_acceptance", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museR9EmitRunStarted(t, h, museTestUUID("queue-owner"))
		museR9EmitRunStarted(t, h, museTestUUID("r10-stale-sentinel"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("r10-stale-sentinel"))
		// The queue predates the acceptance it is cited for; every
		// other edge aligns so the refusal is the order gate.
		q := museR10QueueAndDrain(t, h, museTestUUID("queue-owner"), museTestUUID("r10-stale-sentinel"), museTestUUID("r10-stale-sentinel"))
		a := museR9AcceptIntent(t, h, museTestUUID("r10-stale-sentinel"))
		museR10ActiveDelivery(t, h, a, q, museTestUUID("r10-stale-sentinel"), museTestUUID("queue-owner"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("queue-owner"))
		museR9Refuse(t, h, home)
	})
	t.Run("queue_reuse_across_intents", func(t *testing.T) {
		h, home := setup(t)
		a := museR9AcceptIntent(t, h, museTestUUID("drain-target"))
		q := museR10QueueAndDrain(t, h, museTestUUID("queue-owner"), museTestUUID("drain-target"), museTestUUID("drain-target"))
		// The first intent settles with its correctly joined delivery.
		museR10ActiveDelivery(t, h, a, q, museTestUUID("drain-target"), museTestUUID("queue-owner"))
		other := museR9AcceptIntent(t, h, museTestUUID("other-intent"))
		// A second intent cannot reuse the same queue record.
		museR10ActiveDelivery(t, h, other, q, museTestUUID("other-intent"), museTestUUID("queue-owner"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("queue-owner"))
		museR9Refuse(t, h, home)
	})
}

// The scheduled effect and its side-effect intent carry the same
// idempotency identity (19/19 captured pairs, both scopes). Ports
// panel A foreign_side_effect_key and panel B run_mismatch +
// mirror_mismatch.
func TestMusePTYR10TaskIdempotencyMustMatch(t *testing.T) {
	lifecycle := func(t *testing.T, h *musePTYHost, scope, run, task, scheduledKey, intentKey string) {
		t.Helper()
		if scope == "run" {
			museEmitRunBounded(t, h, "started", run)
		}
		for _, kind := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"} {
			var r map[string]any
			if scope == "run" {
				r = museR7TaskRecord(t, h, kind, run, task)
			} else {
				r = museR7MirrorRecord(t, h, kind, task)
			}
			e := r["payload"].(map[string]any)["event"].(map[string]any)
			switch kind {
			case "scheduled":
				e["idempotency_key"] = scheduledKey
			case "side_effect_intent":
				e["idempotency_key"] = intentKey
			}
			appendMuseRecord(t, h, r)
		}
		if scope == "run" {
			museEmitRunBounded(t, h, "terminal", run)
		}
	}
	for _, scope := range []string{"run", "mirror"} {
		t.Run(scope+"_control", func(t *testing.T) {
			h, home := musePeer(t, "idle")
			lifecycle(t, h, scope, museTestUUID("idempotency-run"), museTestUUID("idempotency-task"), "reminder_child:skill-reminder:1", "reminder_child:skill-reminder:1")
			museExpectDelivery(t, h, home, "idempotency control")
		})
		t.Run(scope+"_mismatch", func(t *testing.T) {
			h, home := musePeer(t, "idle")
			lifecycle(t, h, scope, museTestUUID("idempotency-run"), museTestUUID("r10-idempotency-sentinel"), "reminder_child:skill-reminder:1", "reminder_child:verify-reminder:1")
			museR9Refuse(t, h, home)
		})
		t.Run(scope+"_empty_key", func(t *testing.T) {
			h, home := musePeer(t, "idle")
			// The closed grammar (r14) refuses an empty key at
			// decode. Every other edge aligns.
			lifecycle(t, h, scope, museTestUUID("idempotency-run"), museTestUUID("idempotency-task"), "", "")
			museR9Refuse(t, h, home)
		})
	}
	t.Run("missing_key_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("idempotency-run"))
		for _, kind := range []string{"proposed", "accepted", "scheduled"} {
			r := museR7TaskRecord(t, h, kind, museTestUUID("idempotency-run"), museTestUUID("idempotency-task"))
			if kind == "scheduled" {
				delete(r["payload"].(map[string]any)["event"].(map[string]any), "idempotency_key")
			}
			appendMuseRecord(t, h, r)
		}
		// The grammar requires the key, so the production entry
		// refuses before the fold runs.
		museR9Refuse(t, h, home)
	})
}

// Response identity is order-independent: a created record landing
// after the message binds it just the same. The captured
// message-without-created exception (r9) is kept: a run with no
// created response carries no witness. Ports panel A
// LateResponseMustNotEraseIdentity and panel B unmatched_late_created
// plus its matching control.
func TestMusePTYR10ResponseReconcilesRegardlessOfOrder(t *testing.T) {
	t.Run("matching_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("late-response-run"))
		museR10EmitMessage(t, h, museTestUUID("late-response-run"), "response-a", museTestUUID("msg-control"), "control text")
		museR10EmitCreated(t, h, museTestUUID("late-response-run"), "response-a")
		museEmitRunBounded(t, h, "terminal", museTestUUID("late-response-run"))
		museExpectDelivery(t, h, home, "late matching control")
	})
	t.Run("unmatched_late_created", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("r10-response-sentinel"))
		museR10EmitMessage(t, h, museTestUUID("r10-response-sentinel"), "response-a", museTestUUID("msg-reordered"), "reordered text")
		museR10EmitCreated(t, h, museTestUUID("r10-response-sentinel"), "response-b")
		museEmitRunBounded(t, h, "terminal", museTestUUID("r10-response-sentinel"))
		museR9Refuse(t, h, home)
	})
	t.Run("unmatched_late_created_reasoning", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("r10-reasoning-sentinel"))
		reasoning := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "reasoning_committed"))
		reasoning["payload"].(map[string]any)["run_id"] = museTestUUID("r10-reasoning-sentinel")
		reasoning["payload"].(map[string]any)["event"].(map[string]any)["response_id"] = "response-a"
		appendMuseRecord(t, h, reasoning)
		museR10EmitCreated(t, h, museTestUUID("r10-reasoning-sentinel"), "response-b")
		museEmitRunBounded(t, h, "terminal", museTestUUID("r10-reasoning-sentinel"))
		museR9Refuse(t, h, home)
	})
	// The inline arm pins the captured order: in every capture, a
	// message/reasoning entry names only an already-witnessed response
	// when its run witnessed any (5/5 + 2/2). Anticipating an
	// unwitnessed response refuses at commit time even when the
	// response is created later in the same run.
	t.Run("anticipated_response_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("r10-anticipation-run"))
		museR10EmitCreated(t, h, museTestUUID("r10-anticipation-run"), "response-a")
		museR10EmitMessage(t, h, museTestUUID("r10-anticipation-run"), "response-b", museTestUUID("msg-anticipation"), "anticipated text")
		museR10EmitCreated(t, h, museTestUUID("r10-anticipation-run"), "response-b")
		museEmitRunBounded(t, h, "terminal", museTestUUID("r10-anticipation-run"))
		museR9Refuse(t, h, home)
	})
}

// A retained permission frame carries exactly two children in order:
// the format declaration, then the profile commit (3/3). Any other
// child count, index or kind is unknown state and is never folded.
// Ports panel A duplicate_index + reverse_indices and panel B
// foreign_lifecycle_child + duplicate_child_index.
func TestMusePTYR10RetainedFrameMustBeClosed(t *testing.T) {
	frame := func(t *testing.T, h *musePTYHost) map[string]any {
		t.Helper()
		var found map[string]any
		for _, raw := range museCorpus(t) {
			var r map[string]any
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatal(err)
			}
			if r["retained_frame"] != nil {
				found = r
				break
			}
		}
		if found == nil {
			t.Fatal("no retained frame in corpus")
		}
		// Re-stamp the children as the owned session's own so the
		// refusal is the structure under test, not a foreign
		// stream. Child order and kinds stay captured.
		children := found["children"].([]any)
		for _, child := range children {
			c := child.(map[string]any)
			var r map[string]any
			if err := json.Unmarshal([]byte(c["record_json"].(string)), &r); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(museOwnedRecord(h, r))
			if err != nil {
				t.Fatal(err)
			}
			c["record_json"] = string(raw)
		}
		return found
	}
	t.Run("matched_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, frame(t, h))
		museExpectDelivery(t, h, home, "permission frame control")
	})
	t.Run("duplicate_child_index", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		f := frame(t, h)
		children := f["children"].([]any)
		children[1].(map[string]any)["child_index"] = children[0].(map[string]any)["child_index"]
		appendMuseRecord(t, h, f)
		museR9Refuse(t, h, home)
	})
	t.Run("reverse_indices", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		f := frame(t, h)
		children := f["children"].([]any)
		for i, child := range children {
			child.(map[string]any)["child_index"] = len(children) - 1 - i
		}
		appendMuseRecord(t, h, f)
		museR9Refuse(t, h, home)
	})
	t.Run("foreign_lifecycle_child", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("frame-injected-run"))
		f := frame(t, h)
		r := museOwnedRecord(h, museFixtureRecord("terminal"))
		r["payload"].(map[string]any)["run_id"] = museTestUUID("frame-injected-run")
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		f["children"].([]any)[1].(map[string]any)["record_json"] = string(raw)
		appendMuseRecord(t, h, f)
		// The smuggled terminal never folds, so the run stays
		// active and the notice defers. Under r9 the same frame
		// expanded and settled the run.
		museR9Refuse(t, h, home)
	})
	t.Run("swapped_child_kinds", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		f := frame(t, h)
		children := f["children"].([]any)
		first := children[0].(map[string]any)["record_json"]
		children[0].(map[string]any)["record_json"] = children[1].(map[string]any)["record_json"]
		children[1].(map[string]any)["record_json"] = first
		appendMuseRecord(t, h, f)
		museR9Refuse(t, h, home)
	})
	t.Run("wrong_child_count", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		f := frame(t, h)
		f["children"] = f["children"].([]any)[:1]
		appendMuseRecord(t, h, f)
		museR9Refuse(t, h, home)
	})
}

// r11: the two admitting bounds above are removed. A malformed frame
// digest refuses (TestMusePTYR11FrameDigestMalformedMustRefuse); a
// well-formed but unverifiable digest stays a pinned Stop-The-Line
// admission (TestMusePTYR11FrameDigestContentUnverifiedStopTheLine)
// because no derivation reproduces the captured digests and refusing
// real frames would brick real sessions. An orphan resumed refuses
// (TestMusePTYR11OrphanResumedMustRefuse).
