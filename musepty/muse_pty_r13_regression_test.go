package musepty

// Revision r13: session epoch admission, chrome row states, non-empty
// identities. Every round-12 panel reproduction is ported here as a named
// production-entry test through DeliverSessionNotice. Sources:
//   - panel A (regression): TestPanelR12* in the verdict appendix
//   - panel B (regression): TestPanelR12* in the verdict appendix
//   - delta (regression) accepted the r11 to r12 delta with no
//     findings, so there is no delta probe to port.
//
// Mechanical adaptations (no assertion, shape, or expectation touched):
//   - the panel-A epoch probe's run id is the r13-epoch-run-only mutant
//     target ("r13-epoch-sentinel"); the panel's verbatim "after-end-run"
//     id stays as the narrowness companion subtest.
//   - the panel-B blank-envelope probe's stamped sequence is fixed to
//     424243, the r13-empty-id-only mutant target (precedent:
//     r12-envelope-seq-only's fixed 424242); the stamped shape stays as
//     the narrowness companion subtest.
//   - panel probes assert through their own inline DeliverSessionNotice
//     scaffolding; ports use the committed museR9Refuse helper, which
//     asserts the same deferral with no peer input.
// Panel A's third probe (shared source ids across runs) is an explicit
// scope observation, not a blocking finding: it is ported as a
// characterization of the (run, source) join scoping, pinned as a
// documented bound.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A run recorded between a reopen and its resolved resume is unknown
// state too: the epoch opens only when the resume resolves, not when
// the reopen lands.
func TestMusePTYR13RunInsideReopenPendingMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	targetID, targetSeq := museResumeTarget(t, h)
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
	museEmitRunBounded(t, h, "started", museTestUUID("reopen-pending-run"))
	museEmitRunBounded(t, h, "terminal", museTestUUID("reopen-pending-run"))
	appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
	museR9Refuse(t, h, home)
}

// Every other gated runtime record kind refuses in a closed epoch. Each
// subtest builds its prerequisite state in the open epoch, ends the
// session, then appends one record of its kind; the epoch gate refuses
// before any lifecycle check. These pin the breadth of the epoch gate;
// the laundering reproduction is TestMusePTYR13RunInsideEndedEpochMustRefuse.
func TestMusePTYR13RuntimeKindsInsideEndedEpochMustRefuse(t *testing.T) {
	end := func(t *testing.T, h *musePTYHost) {
		t.Helper()
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
	}
	t.Run("run_terminal", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		museEmitRunBounded(t, h, "terminal", museTestUUID("epoch-kind-run"))
		museR9Refuse(t, h, home)
	})
	t.Run("task", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		museEmitTaskPlanned(t, h, museTestUUID("epoch-kind-run"), museTestUUID("epoch-kind-task"))
		end(t, h)
		done := museOwnedRecord(h, museCorpusTaskRecord(t, "completed"))
		done["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		done["payload"].(map[string]any)["task_id"] = museTestUUID("epoch-kind-task")
		done["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = museTestUUID("epoch-kind-task")
		appendMuseRecord(t, h, done)
		museR9Refuse(t, h, home)
	})
	t.Run("queue", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
		q["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, q)
		museR9Refuse(t, h, home)
	})
	t.Run("message", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		m := museOwnedRecord(h, museFixtureRecord("assistant_message_committed"))
		m["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, m)
		museR9Refuse(t, h, home)
	})
	t.Run("drain", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
		q["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, q)
		end(t, h)
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		d["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		d["payload"].(map[string]any)["event"].(map[string]any)["item_id"] = q["payload"].(map[string]any)["source_run_record_id"]
		appendMuseRecord(t, h, d)
		museR9Refuse(t, h, home)
	})
	t.Run("aux", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		a := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_completed"))
		a["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, a)
		museR9Refuse(t, h, home)
	})
	t.Run("configured", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		c := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
		c["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, c)
		museR9Refuse(t, h, home)
	})
	// r14: the remaining run-event kinds share the run epoch gate
	// (line 604, before the event switch); one refusal subtest each.
	t.Run("created", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		museR10EmitCreated(t, h, museTestUUID("epoch-kind-run"), "response-a")
		museR9Refuse(t, h, home)
	})
	t.Run("reasoning", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		r := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "reasoning_committed"))
		r["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, r)
		museR9Refuse(t, h, home)
	})
	t.Run("task_stream_linked", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		l := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "task_stream_linked"))
		l["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, l)
		museR9Refuse(t, h, home)
	})
	t.Run("reminder_linked", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		l := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "memory_reminder_child_session_linked"))
		l["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, l)
		museR9Refuse(t, h, home)
	})
	t.Run("trace", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		r := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_input_trace_recorded"))
		r["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, r)
		museR9Refuse(t, h, home)
	})
	t.Run("diagnostic", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		d := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "context_block_diagnostic"))
		d["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, d)
		museR9Refuse(t, h, home)
	})
	t.Run("mirror", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		end(t, h)
		museEmitMirrorPlanned(t, h, museTestUUID("epoch-kind-mirror"))
		museR9Refuse(t, h, home)
	})
	t.Run("retained", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("epoch-kind-run"))
		end(t, h)
		r := museOwnedRecord(h, museCorpusRecord(t, "runtime.retained_fact", "", ""))
		r["payload"].(map[string]any)["run_id"] = museTestUUID("epoch-kind-run")
		appendMuseRecord(t, h, r)
		museR9Refuse(t, h, home)
	})
	t.Run("intent_accepted", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		end(t, h)
		a := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", ""))
		a["payload"].(map[string]any)["intent_id"] = museTestUUID("epoch-kind-intent")
		appendMuseRecord(t, h, a)
		museR9Refuse(t, h, home)
	})
	t.Run("intent_materialized", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		a := museR9AcceptIntent(t, h, museTestUUID("epoch-kind-intent"))
		end(t, h)
		m := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.materialized", "", ""))
		p := m["payload"].(map[string]any)
		p["intent_id"] = museTestUUID("epoch-kind-intent")
		p["envelope_record_id"] = a["id"]
		p["envelope_sequence"] = a["sequence"]
		appendMuseRecord(t, h, m)
		museR9Refuse(t, h, home)
	})
	t.Run("intake_received", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		end(t, h)
		r := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.received", "command_intake", ""))
		r["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("epoch-kind-command")
		appendMuseRecord(t, h, r)
		museR9Refuse(t, h, home)
	})
	t.Run("intake_settled", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.received", "command_intake", ""))
		r["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("epoch-kind-command")
		appendMuseRecord(t, h, r)
		end(t, h)
		s := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
		s["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("epoch-kind-command")
		appendMuseRecord(t, h, s)
		museR9Refuse(t, h, home)
	})
	t.Run("cleanup_start", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		pair := museR8CleanupPairs(t)[0]
		start := museOwnedRecord(h, pair.Start)
		p := start["payload"].(map[string]any)
		run := p["run_id"].(string)
		p["record"].(map[string]any)["child_task"].(map[string]any)["task_id"] = museTestUUID("epoch-kind-task")
		p["record"].(map[string]any)["child_task"].(map[string]any)["task_stream"].(map[string]any)["id"] = museTestUUID("epoch-kind-task")
		museEmitRunBounded(t, h, "started", run)
		museEmitTaskPlanned(t, h, run, museTestUUID("epoch-kind-task"))
		end(t, h)
		appendMuseRecord(t, h, start)
		museR9Refuse(t, h, home)
	})
	t.Run("cleanup_terminal", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		pair := museR8CleanupPairs(t)[0]
		start := museOwnedRecord(h, pair.Start)
		p := start["payload"].(map[string]any)
		run := p["run_id"].(string)
		p["record"].(map[string]any)["child_task"].(map[string]any)["task_id"] = museTestUUID("epoch-kind-task")
		p["record"].(map[string]any)["child_task"].(map[string]any)["task_stream"].(map[string]any)["id"] = museTestUUID("epoch-kind-task")
		terminal := museOwnedRecord(h, pair.Terminal)
		terminal["payload"].(map[string]any)["run_id"] = run
		museEmitRunBounded(t, h, "started", run)
		museEmitTaskPlanned(t, h, run, museTestUUID("epoch-kind-task"))
		appendMuseRecord(t, h, start)
		museR9ChildTerminal(t, h, run, museTestUUID("epoch-kind-task"), "cancelled")
		end(t, h)
		appendMuseRecord(t, h, terminal)
		museR9Refuse(t, h, home)
	})
	t.Run("agent_tree", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		end(t, h)
		appendMuseRecord(t, h, museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "agent_tree_initialized", "")))
		museR9Refuse(t, h, home)
	})
}

// A runtime record before the first opened record is unknown state: no
// epoch has opened yet. Fold-level: a log without an opened record can
// never be idle, so delivery cannot distinguish the gate.
func TestMusePTYR13RunBeforeOpenedMustRefuse(t *testing.T) {
	write := func(t *testing.T, records ...map[string]any) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "session.jsonl")
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
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
		return p
	}
	t.Run("run_before_opened_refuses", func(t *testing.T) {
		h, _ := musePeer(t, "idle")
		route := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
		route["payload"].(map[string]any)["record"].(map[string]any)["pid"] = 4242
		start := museOwnedRecord(h, museFixtureRecord("started"))
		start["payload"].(map[string]any)["run_id"] = museTestUUID("before-opened-run")
		if _, err := readMuseLog(write(t, route, start)); err == nil || !strings.Contains(err.Error(), "outside an open session epoch") {
			t.Fatalf("pre-open runtime admitted: %v", err)
		}
	})
	t.Run("route_before_opened_permits", func(t *testing.T) {
		h, _ := musePeer(t, "idle")
		route := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
		route["payload"].(map[string]any)["record"].(map[string]any)["pid"] = 4242
		opened := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
		if _, err := readMuseLog(write(t, route, opened)); err != nil {
			t.Fatalf("captured pre-open order misfolded: %v", err)
		}
	})
}

// A reopened epoch admits runtime work once the resume resolves: the
// epoch gate must not over-refuse legitimate post-resume turns.
func TestMusePTYR13ReopenedEpochAdmitsRuntime(t *testing.T) {
	h, home := musePeer(t, "idle")
	targetID, targetSeq := museResumeTarget(t, h)
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
	appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
	museEmitRunBounded(t, h, "started", museTestUUID("reopened-epoch-run"))
	museEmitRunBounded(t, h, "terminal", museTestUUID("reopened-epoch-run"))
	museExpectDelivery(t, h, home, "reopened epoch control")
}

// Ports panel A TestPanelR12OpenEpochControlDelivers verbatim.
func TestMusePTYR13OpenEpochControlDelivers(t *testing.T) {
	h, home := musePeer(t, "idle")
	museEmitRunBounded(t, h, "started", museTestUUID("open-epoch-run"))
	museEmitRunBounded(t, h, "terminal", museTestUUID("open-epoch-run"))
	museExpectDelivery(t, h, home, "valid open epoch control")
}

// A transcript opener after the chrome notice delivers: the captured
// resume-frame shape (header, notice, user turn) must keep working.
func TestMusePTYR13ChromeNoticeFollowedByTurnDelivers(t *testing.T) {
	h, home := musePeer(t, "idle")
	museShowFrame(t, h, museHeaderRow, "  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7", "❯ first harvest line", "  second harvest line")
	museExpectDelivery(t, h, home, "chrome notice opener control")
}

// Ports panel A TestPanelR12ChromeNoticeControlDelivers verbatim.
func TestMusePTYR13ChromeNoticeControlDelivers(t *testing.T) {
	h, home := musePeer(t, "idle")
	museShowFrame(t, h, museHeaderRow, "  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7")
	museExpectDelivery(t, h, home, "valid chrome notice control")
}

// Panel A's third probe is a scope observation, not a blocking finding:
// distinct started envelopes may share a source run record id across
// different runs, because source references join as (run, source) in
// J36 and no global source-ID uniqueness contract exists. Ported as a
// characterization of that scoping: the shape delivers.
func TestMusePTYR13CrossRunSharedSourceBound(t *testing.T) {
	h, home := musePeer(t, "idle")
	for _, run := range []string{museTestUUID("source-A"), museTestUUID("source-B")} {
		a := museR9AcceptIntent(t, h, run)
		s := museOwnedRecord(h, museFixtureRecord("started"))
		p := s["payload"].(map[string]any)
		p["run_id"] = run
		p["source_run_record_id"] = museTestUUID("same-source-record")
		appendMuseRecord(t, h, s)
		appendMuseRecord(t, h, museR9TopLevelOutcome(t, h, a, s))
		museEmitRunBounded(t, h, "terminal", run)
	}
	museExpectDelivery(t, h, home, "shared source bound")
}

// The empty-envelope gate is not kind-specific: a blank started
// envelope refuses too.
func TestMusePTYR13EmptyEnvelopeOtherKindMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	s := museOwnedRecord(h, museFixtureRecord("started"))
	s["id"] = ""
	s["payload"].(map[string]any)["run_id"] = museTestUUID("blank-envelope-run")
	appendMuseRecord(t, h, s)
	museEmitRunBounded(t, h, "terminal", museTestUUID("blank-envelope-run"))
	museR9Refuse(t, h, home)
}

// An empty resume citation backed by a blank prior-envelope record is
// unknown state: the citation cannot resolve. Ports panel B
// TestPanelR12EmptyResumeReferenceMustRefuse.
func TestMusePTYR13EmptyResumeReferenceMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	r := museOwnedRecord(h, museCorpusRecord(t, "session.name.changed", "", ""))
	r["id"] = ""
	seq := r["sequence"].(int)
	appendMuseRecord(t, h, r)
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
	appendMuseRecord(t, h, museResumedRecord(t, h, "", seq))
	museR9Refuse(t, h, home)
}

// Ports panel B TestPanelR12NonemptyResumeControl verbatim.
func TestMusePTYR13NonemptyResumeControl(t *testing.T) {
	h, home := musePeer(t, "idle")
	id, seq := museResumeTarget(t, h)
	appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
	appendMuseRecord(t, h, museResumedRecord(t, h, id, seq))
	museExpectDelivery(t, h, home, "nonempty resume control")
}
