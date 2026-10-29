package musepty

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A chrome resume notice never parents an indented transcript row: the
// orphan below it is unknown state. Ports panel A
// TestPanelR12ChromeNoticeCannotParentOrphan. The orphan row is the
// r13-chrome-notice-only mutant target.
func TestMusePTYR13ChromeNoticeCannotParentOrphan(t *testing.T) {
	notice := "  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7"
	t.Run("queued_continuation_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, museHeaderRow, notice, "  ↳ unseen queued item")
		museR9Refuse(t, h, home)
	})
	t.Run("other_continuation_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, museHeaderRow, notice, "  Future action required")
		museR9Refuse(t, h, home)
	})
	// NOTE (r14, brief item 2 + panel B F2): a notice text after a turn
	// is quoted transcript by position, not chrome, so the former third
	// subtest here ([turn, notice, continuation] refuses) encoded the
	// quoted-chrome regression. Its shape moved to
	// TestMusePTYR14QuotedChromeDelivers as a positive control; the two
	// orphan-after-chrome negatives above are kept.
}

// An empty outer envelope id is unknown state and is never stored in an
// index. Ports panel B TestPanelR12EmptyEnvelopeMustRefuse; the closed
// grammar (r14) refuses it at decode, and re-targeted r13-empty-id-only
// (admits "" at every UUID site) is the mutant.
func TestMusePTYR13EmptyEnvelopeMustRefuse(t *testing.T) {
	blank := func(t *testing.T, h *musePTYHost, seq int) {
		t.Helper()
		r := museOwnedRecord(h, museCorpusRecord(t, "session.name.changed", "", ""))
		r["id"] = ""
		r["sequence"] = seq
		appendMuseRecord(t, h, r)
	}
	t.Run("blank_name_envelope_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		blank(t, h, 424243)
		museR9Refuse(t, h, home)
	})
	t.Run("blank_other_sequence_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museOwnedRecord(h, museCorpusRecord(t, "session.name.changed", "", ""))
		r["id"] = ""
		appendMuseRecord(t, h, r)
		museR9Refuse(t, h, home)
	})
}

// A non-positive outer sequence is unknown state: every captured
// sequence is a positive integer. The zero member is the
// r13-zero-seq-only mutant target.
func TestMusePTYR13ZeroSequenceMustRefuse(t *testing.T) {
	shape := func(t *testing.T, h *musePTYHost, seq int) {
		t.Helper()
		r := museOwnedRecord(h, museCorpusRecord(t, "session.name.changed", "", ""))
		r["sequence"] = seq
		appendMuseRecord(t, h, r)
	}
	t.Run("zero_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, 0)
		museR9Refuse(t, h, home)
	})
	t.Run("negative_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, -1)
		museR9Refuse(t, h, home)
	})
}

// An empty stream id is unknown state. The r13 fold gate moved to the
// closed grammar in r14 (stream ids are UUIDs): the refusal now fires
// at record decode, before any foreign check. Superseded mutant
// r13-empty-stream-only is covered by re-targeted r13-empty-id-only,
// which admits "" at every UUID site.
func TestMusePTYR13EmptyStreamMustRefuse(t *testing.T) {
	write := func(t *testing.T, h *musePTYHost, seq int) string {
		t.Helper()
		r := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
		r["stream"].(map[string]any)["id"] = ""
		r["sequence"] = seq
		p := filepath.Join(t.TempDir(), "session.jsonl")
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	t.Run("blank_stream_refuses", func(t *testing.T) {
		h, _ := musePeer(t, "idle")
		if _, err := readMuseLog(write(t, h, 424243)); err == nil || !strings.Contains(err.Error(), "unknown Muse record shape") {
			t.Fatalf("blank stream admitted: %v", err)
		}
	})
	t.Run("blank_stream_other_sequence_refuses", func(t *testing.T) {
		h, _ := musePeer(t, "idle")
		if _, err := readMuseLog(write(t, h, 424244)); err == nil || !strings.Contains(err.Error(), "unknown Muse record shape") {
			t.Fatalf("blank stream admitted: %v", err)
		}
	})
}

// An empty queue forward identity is unknown state and is never stored:
// every captured queue causation is a UUID. The r13 fold gate moved to
// the closed grammar in r14: the refusal now fires at record decode.
// Superseded mutant r13-empty-causation-only is covered by re-targeted
// r13-empty-id-only, which admits "" at every UUID site.
func TestMusePTYR13EmptyQueueCausationMustRefuse(t *testing.T) {
	shape := func(t *testing.T, h *musePTYHost, item string) {
		t.Helper()
		museEmitRunBounded(t, h, "started", museTestUUID("r13-causation-run"))
		q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
		q["causation_id"] = ""
		q["payload"].(map[string]any)["run_id"] = museTestUUID("r13-causation-run")
		q["payload"].(map[string]any)["source_run_record_id"] = item
		appendMuseRecord(t, h, q)
	}
	t.Run("sentinel_item_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-causation-sentinel"))
		if _, err := h.fold(); err == nil || !strings.Contains(err.Error(), "unknown Muse record shape") {
			t.Fatalf("blank causation admitted: %v", err)
		}
		museR9Refuse(t, h, home)
	})
	t.Run("other_item_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-causation-other"))
		if _, err := h.fold(); err == nil || !strings.Contains(err.Error(), "unknown Muse record shape") {
			t.Fatalf("blank causation admitted: %v", err)
		}
		museR9Refuse(t, h, home)
	})
}

// An empty started source identity is unknown state and is never
// stored: every captured started source is a UUID. The r13 fold gate
// moved to the closed grammar in r14; superseded mutant
// r13-empty-started-source-only is covered by re-targeted
// r13-empty-id-only.
func TestMusePTYR13EmptyStartedSourceMustRefuse(t *testing.T) {
	shape := func(t *testing.T, h *musePTYHost, run string) {
		t.Helper()
		s := museOwnedRecord(h, museFixtureRecord("started"))
		s["payload"].(map[string]any)["run_id"] = run
		s["payload"].(map[string]any)["source_run_record_id"] = ""
		appendMuseRecord(t, h, s)
		museEmitRunBounded(t, h, "terminal", run)
	}
	t.Run("sentinel_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-started-source-sentinel"))
		museR9Refuse(t, h, home)
	})
	t.Run("other_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-started-source-other"))
		museR9Refuse(t, h, home)
	})
}

// An empty configured source identity is unknown state and is never
// stored: every captured config source is a UUID. The r13 fold gate
// moved to the closed grammar in r14; superseded mutant
// r13-empty-config-source-only is covered by re-targeted
// r13-empty-id-only.
func TestMusePTYR13EmptyConfigSourceMustRefuse(t *testing.T) {
	shape := func(t *testing.T, h *musePTYHost, run string) {
		t.Helper()
		museEmitRunBounded(t, h, "started", run)
		c := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
		c["payload"].(map[string]any)["run_id"] = run
		c["payload"].(map[string]any)["source_run_record_id"] = ""
		appendMuseRecord(t, h, c)
		museEmitRunBounded(t, h, "terminal", run)
	}
	t.Run("sentinel_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-config-source-sentinel"))
		museR9Refuse(t, h, home)
	})
	t.Run("other_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-config-source-other"))
		museR9Refuse(t, h, home)
	})
}

// An empty configured context identity is unknown state and is never
// stored: all 132 captured context ids name the 11 blocks. The r13
// fold gate moved to the closed grammar in r14; re-targeted
// r13-empty-context-id-only (admits "" past the block matcher) is the
// mutant.
func TestMusePTYR13EmptyConfigContextMustRefuse(t *testing.T) {
	shape := func(t *testing.T, h *musePTYHost, run string) {
		t.Helper()
		museEmitRunBounded(t, h, "started", run)
		c := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
		c["payload"].(map[string]any)["run_id"] = run
		msgs := c["payload"].(map[string]any)["event"].(map[string]any)["run_context_messages"].([]any)
		msgs[0].(map[string]any)["id"] = ""
		appendMuseRecord(t, h, c)
		museEmitRunBounded(t, h, "terminal", run)
	}
	t.Run("sentinel_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-config-context-sentinel"))
		museR9Refuse(t, h, home)
	})
	t.Run("other_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-config-context-other"))
		museR9Refuse(t, h, home)
	})
}

// An empty configured roster identity is unknown state and is never
// stored: all 22 captured roster ids name the 2 agents. The r13 fold
// gate moved to the closed grammar in r14; re-targeted
// r13-empty-roster-id-only (admits "" past the agent matcher) is the
// mutant.
func TestMusePTYR13EmptyConfigRosterMustRefuse(t *testing.T) {
	shape := func(t *testing.T, h *musePTYHost, run string) {
		t.Helper()
		museEmitRunBounded(t, h, "started", run)
		c := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
		c["payload"].(map[string]any)["run_id"] = run
		agents := c["payload"].(map[string]any)["event"].(map[string]any)["reminder_roster"].(map[string]any)["agents"].([]any)
		agents[0].(map[string]any)["id"] = ""
		appendMuseRecord(t, h, c)
		museEmitRunBounded(t, h, "terminal", run)
	}
	t.Run("sentinel_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-config-roster-sentinel"))
		museR9Refuse(t, h, home)
	})
	t.Run("other_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("r13-config-roster-other"))
		museR9Refuse(t, h, home)
	})
}

// A non-positive cleanup effect identity is unknown state: all 6
// captured effect ids read 1. The zero member is the
// r13-zero-effect-only mutant target.
func TestMusePTYR13ZeroCleanupEffectMustRefuse(t *testing.T) {
	shape := func(t *testing.T, h *musePTYHost, effect int) {
		t.Helper()
		pair := museR8CleanupPairs(t)[0]
		start := museOwnedRecord(h, pair.Start)
		p := start["payload"].(map[string]any)
		run := p["run_id"].(string)
		p["record"].(map[string]any)["child_task"].(map[string]any)["task_id"] = museTestUUID("r13-effect-task")
		p["record"].(map[string]any)["child_task"].(map[string]any)["task_stream"].(map[string]any)["id"] = museTestUUID("r13-effect-task")
		p["record"].(map[string]any)["cleanup_effect_id"] = effect
		terminal := museOwnedRecord(h, pair.Terminal)
		terminal["payload"].(map[string]any)["run_id"] = run
		terminal["payload"].(map[string]any)["record"].(map[string]any)["cleanup_effect_id"] = effect
		museEmitRunBounded(t, h, "started", run)
		museEmitTaskPlanned(t, h, run, museTestUUID("r13-effect-task"))
		appendMuseRecord(t, h, start)
		museR9ChildTerminal(t, h, run, museTestUUID("r13-effect-task"), "cancelled")
		appendMuseRecord(t, h, terminal)
		museEmitRunBounded(t, h, "terminal", run)
	}
	t.Run("zero_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, 0)
		museR9Refuse(t, h, home)
	})
	t.Run("negative_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, -1)
		museR9Refuse(t, h, home)
	})
}

// A run recorded after the session end is unknown state even when a later
// reopen and a resolved resume would otherwise certify idle: runtime work
// in a closed epoch cannot become valid by reopening. Ports panel A
// TestPanelR12RunInsideEndedEpochMustRefuse. The resumed citation names
// the illegal post-end terminal, exactly as the panel wrote it.
func TestMusePTYR13RunInsideEndedEpochMustRefuse(t *testing.T) {
	shape := func(t *testing.T, h *musePTYHost, run string) {
		t.Helper()
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
		museEmitRunBounded(t, h, "started", run)
		museEmitRunBounded(t, h, "terminal", run)
		targetID, targetSeq := museResumeTarget(t, h)
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
	}
	t.Run("sentinel_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		// Fixed sentinel run: the r13-epoch-run-only mutant admits
		// exactly this run's records in closed epochs.
		shape(t, h, museTestUUID("r13-epoch-sentinel"))
		museR9Refuse(t, h, home)
	})
	t.Run("other_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		shape(t, h, museTestUUID("after-end-run"))
		museR9Refuse(t, h, home)
	})
}
