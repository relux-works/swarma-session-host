package musepty

// Revision r9: bind every remaining field, and make screen provenance
// frame-local. Every round-8 panel reproduction is ported here as a named
// production-entry test through DeliverSessionNotice. Sources:
//   - delta (regression): TestR8Panel* in
//     regression_probes_test.go
//   - panel A (regression): TestR141R8* in
//     regression_probes_test.go
//   - panel B (regression): TestPanelR8* in
//     regression_probes_test.go
// Ports use full captured lifecycles where the transition tables require
// them, so each refusal stays attributable to the mechanism under test.
// Panel B's blank-gap probes are ported as stated: the scrolled shape
// refuses under the frame-local rule, and the in-frame shape across a
// blank pins the blank-skipping admission as a bound (see the r9 field
// table).

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// museR9Refuse drives the production entry point and requires a deferral
// with no peer input.
func museR9Refuse(t *testing.T, h *musePTYHost, home string) {
	t.Helper()
	if err := h.DeliverSessionNotice(context.Background(), "refuse unknown state"); err != ErrMusePTYDeferred {
		t.Fatalf("unknown state admitted: %v", err)
	}
	for _, name := range []string{"peer-paste", "peer-submit"} {
		if b, e := os.ReadFile(filepath.Join(home, name)); !os.IsNotExist(e) {
			t.Fatalf("unsafe input %s=%q: %v", name, b, e)
		}
	}
}

// museR9EmitRunStarted emits a started record for run and returns the
// outer record identity an intent outcome must cite.
func museR9EmitRunStarted(t *testing.T, h *musePTYHost, run string) map[string]any {
	t.Helper()
	r := museOwnedRecord(h, museFixtureRecord("started"))
	r["payload"].(map[string]any)["run_id"] = run
	appendMuseRecord(t, h, r)
	return r
}

// museR9AcceptIntent appends an accepted intent record and returns the
// appended record with its envelope identity.
func museR9AcceptIntent(t *testing.T, h *musePTYHost, intentID string) map[string]any {
	t.Helper()
	a := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", ""))
	a["payload"].(map[string]any)["intent_id"] = intentID
	appendMuseRecord(t, h, a)
	return a
}

// museR9TopLevelOutcome builds (not appends) a materialized top-level
// outcome aligned to the accepted record and the started record of the
// intent's own run.
func museR9TopLevelOutcome(t *testing.T, h *musePTYHost, accepted, started map[string]any) map[string]any {
	t.Helper()
	m := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.materialized", "", ""))
	intent := accepted["payload"].(map[string]any)["intent_id"].(string)
	p := m["payload"].(map[string]any)
	p["intent_id"] = intent
	p["envelope_record_id"] = accepted["id"]
	p["envelope_sequence"] = accepted["sequence"]
	o := p["outcome"].(map[string]any)
	if o["kind"] != "top_level_turn_started" {
		t.Fatalf("first corpus outcome is %q, want top_level_turn_started", o["kind"])
	}
	o["run_id"] = intent
	o["run_started_session_record_id"] = started["id"]
	o["run_started_source_record_id"] = started["payload"].(map[string]any)["source_run_record_id"]
	return m
}

// museR9CorpusMaterialized returns a clone of the corpus materialized
// record with the given outcome kind.
func museR9CorpusMaterialized(t *testing.T, kind string) map[string]any {
	t.Helper()
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		if record["payload_type"] != "runtime.user_intent.materialized" {
			continue
		}
		if record["payload"].(map[string]any)["outcome"].(map[string]any)["kind"] != kind {
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
	t.Fatalf("capture missing materialized outcome %q", kind)
	return nil
}

// museR9QueueItem appends a queued inbox item under run and returns the
// appended record with its outer identity.
func museR9QueueItem(t *testing.T, h *musePTYHost, run, causation string) map[string]any {
	t.Helper()
	q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
	q["causation_id"] = causation
	q["payload"].(map[string]any)["run_id"] = run
	appendMuseRecord(t, h, q)
	return q
}

// museR9ChildTerminal appends a task terminal of the given kind for the
// exact (run, task) key.
func museR9ChildTerminal(t *testing.T, h *musePTYHost, run, task, kind string) {
	t.Helper()
	r := museOwnedRecord(h, museCorpusTaskRecord(t, kind))
	p := r["payload"].(map[string]any)
	p["run_id"] = run
	p["task_id"] = task
	p["event"].(map[string]any)["task_id"] = task
	appendMuseRecord(t, h, r)
}

// Screen provenance for off-frame parents is log-backed (r10; the r9
// frame-local rule is replaced): an indented row whose parent is not in
// the current frame admits only as a contiguous suffix of one
// terminated run's rendered committed output. A scrolled-off row is
// still never a parent by itself — even a proven turn or echo — and
// these foreign rows match no committed output, so every shape below
// still refuses. Ports delta TestR8PanelScrolledParentMustBeProven
// (both orphan variants plus the echo shape), panel A
// TestR141R8OrphanScrollParentMustRefuse, and panel B
// TestPanelR8OrphanProvenanceMustRefuse plus the scrolled blank-gap
// supporting probe.
func TestMusePTYR9ScreenFrameLocalMustRefuse(t *testing.T) {
	scrollThenRedraw := func(t *testing.T, h *musePTYHost, parent, first string) {
		t.Helper()
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte(parent + "\r\n" + strings.Repeat("scroll filler line\r\n", 39)))
		h.vt.observe([]byte(museFrameRows(first)))
		h.mu.Unlock()
	}
	for _, tc := range []struct{ name, parent, first string }{
		{"delta_orphan_indented_parent", "  Future action required", "  Waiting for remote approval"},
		{"delta_orphan_header_child_parent", "  ↳ unseen queued item", "  Waiting for remote approval"},
		{"delta_echo_parent_refuses", "◆ echo: harvest short line", "  Waiting for remote approval"},
		{"panel_a_orphan_scroll", "  Future action required", "  Waiting for remote approval"},
		{"panel_b_orphan_provenance", "  Waiting for remote approval", "  orphan continuation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			// The orphan refuses in-frame before scrolling, so the
			// refusal below is attributable to the scroll edge.
			if strings.HasPrefix(tc.parent, "  ") {
				// The idle peer fold carries no committed
				// output, so no off-frame row is backed.
				fold, err := h.fold()
				if err != nil {
					t.Fatal(err)
				}
				h.mu.Lock()
				h.vt.observe([]byte(museFrameRows(tc.parent)))
				safe := h.vt.safeComposer("", fold)
				h.mu.Unlock()
				if safe {
					t.Fatal("initial orphan frame unexpectedly safe")
				}
			}
			scrollThenRedraw(t, h, tc.parent, tc.first)
			museR9Refuse(t, h, home)
		})
	}
	t.Run("panel_b_blank_gap_scroll_parent", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte("❯ captured parent\r\n" + strings.Repeat("scroll filler line\r\n", 39)))
		h.vt.observe([]byte(museFrameRows("", "  Waiting for remote approval")))
		h.mu.Unlock()
		museR9Refuse(t, h, home)
	})
	t.Run("control_inframe_parent_after_scroll", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte("❯ old turn\r\n" + strings.Repeat("scroll filler line\r\n", 39)))
		h.vt.observe([]byte(museFrameRows("❯ first harvest line", "  wrapped tail")))
		h.mu.Unlock()
		museExpectDelivery(t, h, home, "in-frame parent control")
	})
	t.Run("control_unscrolled_parent", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "❯ first harvest line", "  wrapped tail")
		museExpectDelivery(t, h, home, "unscrolled parent control")
	})
}

// A reopen without an end still waits for the captured resume context:
// a reopen that only rebinds the PID is unknown state. Ports panel A
// TestR141R8OpenedWithoutResumeMustRefuse.
func TestMusePTYR9NoEndReopenRequiresResume(t *testing.T) {
	t.Run("opened_without_resume_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		f, err := h.fold()
		if err != nil {
			t.Fatalf("reopen misfolded: %v", err)
		}
		if !f.ReopenPending {
			t.Fatal("no-end reopen left no pending resume")
		}
		museR9Refuse(t, h, home)
	})
	t.Run("opened_with_resume_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		targetID, targetSeq := museResumeTarget(t, h)
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
		museExpectDelivery(t, h, home, "resumed reopen control")
	})
}
