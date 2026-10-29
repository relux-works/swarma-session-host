package musepty

import (
	"strings"
	"testing"
)

// A cleanup terminal discharges only the started effect with its exact
// (run, effect) key. Ports delta TestPanelR7CleanupIdentityMustRefuse
// (all four negatives plus the matched control), panel A
// TestR147CleanupTerminalWithoutStartMustRefuse and
// TestR147CleanupIdentityMismatchMustRefuse.
func TestMusePTYR8CleanupPairingMustRefuse(t *testing.T) {
	pairs := museR8CleanupPairs(t)
	for i, pair := range pairs {
		t.Run("captured_pair_"+string(rune('0'+i)), func(t *testing.T) {
			h, home := musePeer(t, "idle")
			start := museOwnedRecord(h, pair.Start)
			p := start["payload"].(map[string]any)
			run := p["run_id"].(string)
			task := p["record"].(map[string]any)["child_task"].(map[string]any)["task_id"].(string)
			museR8CleanupLifecycle(t, h, run, task)
			appendMuseRecord(t, h, start)
			museR8CleanupChildTerminal(t, h, run, task)
			terminal := museOwnedRecord(h, pair.Terminal)
			terminal["payload"].(map[string]any)["run_id"] = run
			appendMuseRecord(t, h, terminal)
			museEmitRunBounded(t, h, "terminal", run)
			museExpectDelivery(t, h, home, "captured cleanup control")
		})
	}
	first := pairs[0]
	setup := func(t *testing.T) (*musePTYHost, string, string, string) {
		t.Helper()
		h, home := musePeer(t, "idle")
		start := museOwnedRecord(h, first.Start)
		p := start["payload"].(map[string]any)
		run := p["run_id"].(string)
		task := p["record"].(map[string]any)["child_task"].(map[string]any)["task_id"].(string)
		museR8CleanupLifecycle(t, h, run, task)
		return h, home, run, task
	}
	t.Run("terminal_without_start", func(t *testing.T) {
		h, home, run, _ := setup(t)
		end := museOwnedRecord(h, first.Terminal)
		end["payload"].(map[string]any)["run_id"] = run
		appendMuseRecord(t, h, end)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	// Fixed sentinel id: the r8-cleanup-mismatch-one-only mutant admits
	// the terminal with effect 999 for exactly this pair.
	t.Run("mismatched_effect_id_999", func(t *testing.T) {
		h, home, run, task := setup(t)
		appendMuseRecord(t, h, museOwnedRecord(h, first.Start))
		museR8CleanupChildTerminal(t, h, run, task)
		end := museOwnedRecord(h, first.Terminal)
		end["payload"].(map[string]any)["run_id"] = run
		end["payload"].(map[string]any)["record"].(map[string]any)["cleanup_effect_id"] = 999
		appendMuseRecord(t, h, end)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("mismatched_effect_id_2", func(t *testing.T) {
		h, home, run, task := setup(t)
		appendMuseRecord(t, h, museOwnedRecord(h, first.Start))
		museR8CleanupChildTerminal(t, h, run, task)
		end := museOwnedRecord(h, first.Terminal)
		end["payload"].(map[string]any)["run_id"] = run
		end["payload"].(map[string]any)["record"].(map[string]any)["cleanup_effect_id"] = 2
		appendMuseRecord(t, h, end)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("duplicate_terminal", func(t *testing.T) {
		h, home, run, task := setup(t)
		appendMuseRecord(t, h, museOwnedRecord(h, first.Start))
		museR8CleanupChildTerminal(t, h, run, task)
		end := museOwnedRecord(h, first.Terminal)
		end["payload"].(map[string]any)["run_id"] = run
		appendMuseRecord(t, h, end)
		// A fresh envelope on the copy: the refusal is the repeated
		// terminal, not the reused envelope.
		appendMuseRecord(t, h, museRestampRecord(museCloneRecord(t, end)))
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("started_without_terminal", func(t *testing.T) {
		h, home, run, task := setup(t)
		appendMuseRecord(t, h, museOwnedRecord(h, first.Start))
		museR8CleanupChildTerminal(t, h, run, task)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("started_twice", func(t *testing.T) {
		h, home, run, _ := setup(t)
		appendMuseRecord(t, h, museOwnedRecord(h, first.Start))
		appendMuseRecord(t, h, museOwnedRecord(h, first.Start))
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
}

// A task link resolves to the exact (run, task) lifecycle it names. Ports
// panel A TestR147TaskLinkWrongRunMustRefuse and panel B
// TestPanelR7TaskLinkCrossRunMustRefuse.
func TestMusePTYR8TaskLinkExactRunMustRefuse(t *testing.T) {
	t.Run("panel_a_wrong_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		link := museCorpusRecord(t, "runtime.session", "run", "task_stream_linked")
		run := link["payload"].(map[string]any)["run_id"].(string)
		task := link["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		appendMuseRecord(t, h, museOwnedRecord(h, link))
		other := museTestUUID("r8-link-unrelated-run")
		museEmitRunBounded(t, h, "started", other)
		for _, event := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"} {
			appendMuseRecord(t, h, museR7TaskRecord(t, h, event, other, task))
		}
		museEmitRunBounded(t, h, "terminal", other)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("panel_b_cross_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("A"))
		link := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "task_stream_linked"))
		p := link["payload"].(map[string]any)
		p["run_id"] = museTestUUID("A")
		e := p["event"].(map[string]any)
		// Fixed sentinel id: the r8-link-cross-run-one-only mutant
		// resolves the link for exactly this task.
		e["task_id"] = museTestUUID("r8-link-sentinel")
		e["task_stream"].(map[string]any)["id"] = museTestUUID("r8-link-sentinel")
		appendMuseRecord(t, h, link)
		museEmitRunBounded(t, h, "terminal", museTestUUID("A"))
		museEmitRunBounded(t, h, "started", museTestUUID("B"))
		museEmitTaskPlanned(t, h, museTestUUID("B"), museTestUUID("r8-link-sentinel"))
		appendMuseRecord(t, h, museR7TaskRecord(t, h, "completed", museTestUUID("B"), museTestUUID("r8-link-sentinel")))
		museEmitRunBounded(t, h, "terminal", museTestUUID("B"))
		assertMuseNoInput(t, h, home)
	})
}

// A drain target must equal the stored forward identity even when the
// target run is known. Ports panel B
// TestPanelR7DrainKnownWrongRunMustRefuse.
func TestMusePTYR8DrainTargetMustMatchForwardReference(t *testing.T) {
	h, home := musePeer(t, "idle")
	// Fixed sentinel id: the r8-drain-causation-one-only mutant admits
	// the mismatched forward reference for exactly this target.
	museEmitRunBounded(t, h, "started", museTestUUID("r8-drain-causation"))
	museEmitRunBounded(t, h, "terminal", museTestUUID("r8-drain-causation"))
	museEmitRunBounded(t, h, "started", museTestUUID("A"))
	q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
	q["causation_id"] = museTestUUID("expected-C")
	q["payload"].(map[string]any)["run_id"] = museTestUUID("A")
	appendMuseRecord(t, h, q)
	museEmitRunBounded(t, h, "terminal", museTestUUID("A"))
	d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
	p := d["payload"].(map[string]any)
	p["run_id"] = museTestUUID("r8-drain-causation")
	p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("r8-drain-causation")
	appendMuseRecord(t, h, d)
	assertMuseNoInput(t, h, home)
}

// A first-row continuation after a scroll admits only with tracked
// provenance naming its exact parent. Ports delta
// TestPanelR7ScrolledFreshFrameMustRefuse, panel A
// TestR147ScrolledOrphanMustRefuse, and panel B
// TestPanelR7ScrolledUnknownMustRefuse.
func TestMusePTYR8ScrolledProvenanceMustRefuse(t *testing.T) {
	t.Run("delta_scroll_then_fresh_frame", func(t *testing.T) {
		for _, scroll := range []bool{false, true} {
			name := "no_scroll_control"
			if scroll {
				name = "scroll_then_fresh_frame"
			}
			t.Run(name, func(t *testing.T) {
				h, home := musePeer(t, "idle")
				h.mu.Lock()
				if scroll {
					h.vt.observe([]byte(strings.Repeat("\r\n", len(h.vt.cells)+2)))
				}
				frame := strings.Replace(museTestScreen(""), "── Voice", "  ↳ unseen queued item\r\n── Voice", 1)
				h.vt.observe([]byte(frame))
				h.mu.Unlock()
				assertMuseNoInput(t, h, home)
			})
		}
	})
	for _, rows := range [][]string{{"  Future action required"}, {museHeaderRow, "  ◈ Future action required"}} {
		t.Run("panel_a_"+strings.Join(rows, "/"), func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte(strings.Repeat("scroll filler line\n", 50)))
			h.vt.observe([]byte(museFrameRows(rows...)))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
	for _, row := range []string{"  Future action required", "  Waiting for remote approval", "  ↳ unseen queued item"} {
		t.Run("panel_b_"+row, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte(strings.Repeat("scroll filler line\n", 50)))
			h.vt.observe([]byte(museFrameRows(row)))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
	// Painted filler names an unproven parent: the last discarded row is
	// tracked (painted from column 0) but matches no content entry, so
	// the redrawn orphan still refuses. The
	// r8-provenance-unproven-one-only mutant admits exactly this
	// (parent, row) pair and must deliver.
	t.Run("painted_filler_parent", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte(strings.Repeat("scroll filler line\r\n", 50)))
		h.vt.observe([]byte(museFrameRows("  Future action required")))
		h.mu.Unlock()
		assertMuseNoInput(t, h, home)
	})
}
