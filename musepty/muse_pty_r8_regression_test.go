package musepty

// Revision r8: every join on a full key, every pair closed, screen
// provenance per frame. Every round-7 panel reproduction is ported here as
// a named production-entry test through DeliverSessionNotice. Sources:
//   - delta (regression): TestPanelR7* in
//     regression_hunt_test.go
//   - panel A (regression): TestR147* in
//     regression_probes.go
//   - panel B (regression): TestPanelR7* in
//     regression_probes_test.go
// Ports use full captured lifecycles where the transition tables require
// them, so each refusal stays attributable to the mechanism under test.
// Panel B's mirror-counterpart probe is NOT ported as a refusal: r8 item 6
// (revised) keeps the r7 mirror rule on pinned startup evidence, and the
// startup capture is pinned as a positive control below instead. Panel B's
// settled-intake probe is a noted non-finding (partial intake history,
// 5/8 in captures) and stays out of contract.

import (
	"encoding/json"
	"sort"

	"testing"
)

// museR8CleanupPairs groups the captured cleanup effects by their full
// (run, effect) key. The captures hold three correctly paired effects.
func museR8CleanupPairs(t *testing.T) []struct {
	Start, Terminal map[string]any
} {
	t.Helper()
	starts := map[museCleanupKey]map[string]any{}
	terminals := map[museCleanupKey]map[string]any{}
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		typ, _ := record["payload_type"].(string)
		if typ != "reminder.cleanup_effect.started" && typ != "reminder.cleanup_effect.terminal" {
			continue
		}
		payload := record["payload"].(map[string]any)
		rec := payload["record"].(map[string]any)
		key := museCleanupKey{Run: payload["run_id"].(string), Effect: int(rec["cleanup_effect_id"].(float64))}
		out, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		var clone map[string]any
		if err := json.Unmarshal(out, &clone); err != nil {
			t.Fatal(err)
		}
		if typ == "reminder.cleanup_effect.started" {
			starts[key] = clone
		} else {
			terminals[key] = clone
		}
	}
	if len(starts) != 3 || len(terminals) != 3 {
		t.Fatalf("captures hold %d cleanup starts and %d terminals, want 3 and 3", len(starts), len(terminals))
	}
	var pairs []struct {
		Start, Terminal map[string]any
	}
	for key, start := range starts {
		terminal, ok := terminals[key]
		if !ok {
			t.Fatalf("captured cleanup effect %+v lacks its terminal", key)
		}
		pairs = append(pairs, struct {
			Start, Terminal map[string]any
		}{start, terminal})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Start["sequence"].(float64) < pairs[j].Start["sequence"].(float64)
	})
	return pairs
}

// museR8CleanupLifecycle emits the run and child-task prefix one captured
// cleanup pair needs: the run starts and the child task runs its planned
// order, still live. The caller appends the cleanup start, then the
// child terminal (cancelled in all three captures), then the cleanup
// terminal: the captured start -> child terminal -> cleanup terminal
// order. (r11: the r8 helper emitted the child terminal before the
// start, which the cleanup order gate now refuses.)
func museR8CleanupLifecycle(t *testing.T, h *musePTYHost, run, task string) {
	t.Helper()
	museEmitRunBounded(t, h, "started", run)
	museEmitTaskPlanned(t, h, run, task)
}

// museR8CleanupChildTerminal appends the bound child's cancelled
// terminal, in the captured position between the cleanup start and the
// cleanup terminal.
func museR8CleanupChildTerminal(t *testing.T, h *musePTYHost, run, task string) {
	t.Helper()
	end := museOwnedRecord(h, museCorpusTaskRecord(t, "cancelled"))
	end["payload"].(map[string]any)["run_id"] = run
	end["payload"].(map[string]any)["task_id"] = task
	end["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
	appendMuseRecord(t, h, end)
}

// A drain into a run the owned stream never started keeps its wire shape
// but stays pending and never certifies an open host idle. Ports panel A
// TestR147DrainForwardRunNeverStartedMustRefuse (F4) and panel B
// TestPanelR7DrainForwardReferenceMustResolve.
func TestMusePTYR8DrainForwardReferenceMustResolve(t *testing.T) {
	t.Run("panel_a_forward_never_started", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
		run := q["payload"].(map[string]any)["run_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		appendMuseRecord(t, h, q)
		museEmitRunBounded(t, h, "terminal", run)
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		target := q["causation_id"].(string)
		d["payload"].(map[string]any)["run_id"] = target
		d["payload"].(map[string]any)["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = target
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
	t.Run("panel_b_forward_must_resolve", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("A"))
		q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
		q["causation_id"] = museTestUUID("never-started-B")
		q["payload"].(map[string]any)["run_id"] = museTestUUID("A")
		appendMuseRecord(t, h, q)
		museEmitRunBounded(t, h, "terminal", museTestUUID("A"))
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		p := d["payload"].(map[string]any)
		p["run_id"] = museTestUUID("never-started-B")
		p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("never-started-B")
		appendMuseRecord(t, h, d)
		assertMuseNoInput(t, h, home)
	})
	t.Run("observed_target_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("A"))
		q := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
		q["causation_id"] = museTestUUID("B")
		q["payload"].(map[string]any)["run_id"] = museTestUUID("A")
		appendMuseRecord(t, h, q)
		museEmitRunBounded(t, h, "terminal", museTestUUID("A"))
		museEmitRunBounded(t, h, "started", museTestUUID("B"))
		museEmitRunBounded(t, h, "terminal", museTestUUID("B"))
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		p := d["payload"].(map[string]any)
		p["run_id"] = museTestUUID("B")
		p["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("B")
		appendMuseRecord(t, h, d)
		museExpectDelivery(t, h, home, "observed drain control")
	})
}

// An accepted intent stays pending until its materialized outcome
// references the stored intent id. Ports panel B
// TestPanelR7AcceptedIntentMissingCounterpartMustRefuse and
// TestPanelR7MaterializedIntentWrongIDMustRefuse, plus the missing-start
// and duplicate edges of the pair table.
func TestMusePTYR8IntentPairingMustRefuse(t *testing.T) {
	t.Run("accepted_without_outcome", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", "")))
		f, err := h.fold()
		if err != nil {
			t.Fatalf("pending intent misfolded: %v", err)
		}
		if f.pendingIntentCount() != 1 {
			t.Fatalf("accepted intent not pending: %+v", f)
		}
		assertMuseNoInput(t, h, home)
	})
	t.Run("materialized_wrong_id", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		a := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", ""))
		a["payload"].(map[string]any)["intent_id"] = museTestUUID("intent-A")
		appendMuseRecord(t, h, a)
		m := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.materialized", "", ""))
		p := m["payload"].(map[string]any)
		p["intent_id"] = museTestUUID("intent-B")
		run := p["outcome"].(map[string]any)["run_id"].(string)
		appendMuseRecord(t, h, m)
		museEmitRunBounded(t, h, "started", run)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("materialized_without_accepted", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		m := museCorpusRecord(t, "runtime.user_intent.materialized", "", "")
		run := m["payload"].(map[string]any)["outcome"].(map[string]any)["run_id"].(string)
		appendMuseRecord(t, h, museOwnedRecord(h, m))
		museEmitRunBounded(t, h, "started", run)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("duplicate_accepted", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		a := museCorpusRecord(t, "runtime.user_intent.accepted", "", "")
		appendMuseRecord(t, h, museOwnedRecord(h, a))
		appendMuseRecord(t, h, museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", "")))
		assertMuseNoInput(t, h, home)
	})
	t.Run("duplicate_materialized", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		a := museCorpusRecord(t, "runtime.user_intent.accepted", "", "")
		intent := a["payload"].(map[string]any)["intent_id"].(string)
		appendMuseRecord(t, h, museOwnedRecord(h, a))
		m := museCorpusRecord(t, "runtime.user_intent.materialized", "", "")
		m["payload"].(map[string]any)["intent_id"] = intent
		run := m["payload"].(map[string]any)["outcome"].(map[string]any)["run_id"].(string)
		appendMuseRecord(t, h, museOwnedRecord(h, m))
		again := museCorpusRecord(t, "runtime.user_intent.materialized", "", "")
		again["payload"].(map[string]any)["intent_id"] = intent
		appendMuseRecord(t, h, museOwnedRecord(h, again))
		museEmitRunBounded(t, h, "started", run)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("matched_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		a := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", ""))
		intent := a["payload"].(map[string]any)["intent_id"].(string)
		appendMuseRecord(t, h, a)
		// Capture order: the acceptance precedes the run it
		// produced, and the outcome cites the accepted envelope
		// and the started record of the intent's own run.
		run := intent
		started := museOwnedRecord(h, museFixtureRecord("started"))
		started["payload"].(map[string]any)["run_id"] = run
		appendMuseRecord(t, h, started)
		m := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.materialized", "", ""))
		p := m["payload"].(map[string]any)
		p["intent_id"] = intent
		p["envelope_record_id"] = a["id"]
		p["envelope_sequence"] = a["sequence"]
		o := p["outcome"].(map[string]any)
		o["run_id"] = run
		o["run_started_session_record_id"] = started["id"]
		o["run_started_source_record_id"] = started["payload"].(map[string]any)["source_run_record_id"]
		appendMuseRecord(t, h, m)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "paired intent control")
	})
}

// Any task with a planning prefix and no terminal is pending work, whether
// or not it reached started and whether or not its parent run has ended.
// Ports panel B TestPanelR7RunTerminalWithProposedTaskMustRefuse and covers
// every planning prefix, run-bound and mirror alike.
func TestMusePTYR8PlanningPrefixPendingMustRefuse(t *testing.T) {
	for _, prefix := range [][]string{
		{"proposed"},
		{"proposed", "accepted"},
		{"proposed", "accepted", "scheduled"},
		{"proposed", "accepted", "scheduled", "side_effect_intent"},
		{"proposed", "accepted", "scheduled", "side_effect_intent", "started"},
	} {
		t.Run("run_bound_"+prefix[len(prefix)-1], func(t *testing.T) {
			h, home := musePeer(t, "idle")
			museEmitRunBounded(t, h, "started", museTestUUID("A"))
			for _, event := range prefix {
				appendMuseRecord(t, h, museR7TaskRecord(t, h, event, museTestUUID("A"), museTestUUID("unfinished-task")))
			}
			museEmitRunBounded(t, h, "terminal", museTestUUID("A"))
			f, err := h.fold()
			if err != nil {
				t.Fatalf("unfinished task misfolded: %v", err)
			}
			if f.pendingTaskCount() != 1 {
				t.Fatalf("planning prefix not pending: %+v", f)
			}
			assertMuseNoInput(t, h, home)
		})
	}
	for _, prefix := range [][]string{
		{"proposed"},
		{"proposed", "accepted", "scheduled", "side_effect_intent", "started"},
	} {
		t.Run("mirror_"+prefix[len(prefix)-1], func(t *testing.T) {
			h, home := musePeer(t, "idle")
			for _, event := range prefix {
				appendMuseRecord(t, h, museR7MirrorRecord(t, h, event, museTestUUID("unfinished-mirror")))
			}
			assertMuseNoInput(t, h, home)
		})
	}
}

// A complete captured mirror lifecycle discharges itself: the pinned TUI
// writes one at every startup before any run exists, so a
// counterpart-free mirror terminal is fresh-TUI idle. Each captured
// mirror task replays verbatim as a positive control; the run-free
// startup stream is named explicitly. (r8 item 6, revised: panel B's
// mirror-counterpart probe is disputed to tb-keeper on this evidence.)
func TestMusePTYR8StartupMirrorLifecyclePermitsNotice(t *testing.T) {
	byTask := map[string][]map[string]any{}
	streamHasRun := map[string]bool{}
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		typ, _ := record["payload_type"].(string)
		if typ == "" {
			continue
		}
		payload, _ := record["payload"].(map[string]any)
		if payload == nil {
			continue
		}
		stream, _ := record["stream"].(map[string]any)
		streamID, _ := stream["id"].(string)
		if typ == "runtime.session" && payload["kind"] == "run" {
			streamHasRun[streamID] = true
		}
		if typ != "runtime.session.task" {
			continue
		}
		task := payload["task_id"].(string)
		out, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		var clone map[string]any
		if err := json.Unmarshal(out, &clone); err != nil {
			t.Fatal(err)
		}
		byTask[task] = append(byTask[task], clone)
	}
	if len(byTask) != 3 {
		t.Fatalf("captures hold %d mirror tasks, want 3", len(byTask))
	}
	for task := range byTask {
		seq := byTask[task]
		sort.Slice(seq, func(i, j int) bool {
			return seq[i]["sequence"].(float64) < seq[j]["sequence"].(float64)
		})
		if len(seq) != 6 {
			t.Fatalf("captured mirror task %s holds %d records, want 6", task, len(seq))
		}
	}
	var tasks []string
	for task := range byTask {
		tasks = append(tasks, task)
	}
	sort.Strings(tasks)
	startup := 0
	for _, task := range tasks {
		name := "mirror_" + task[:8]
		stream := byTask[task][0]["stream"].(map[string]any)["id"].(string)
		if !streamHasRun[stream] {
			name = "startup_stream_" + task[:8]
			startup++
		}
		t.Run(name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			for _, r := range byTask[task] {
				appendMuseRecord(t, h, museOwnedRecord(h, r))
			}
			museExpectDelivery(t, h, home, "captured mirror control")
		})
	}
	if startup != 1 {
		t.Fatalf("captures hold %d run-free mirror streams, want 1", startup)
	}
}
