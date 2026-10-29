package musepty

// Revision r5: idle only on positive proof. Task records join the run fold,
// the screen boundary row is classified by the closed screen table, record
// bytes must be valid UTF-8, and only printable runes become VT cells.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func museCorpusTaskRecord(t *testing.T, event string) map[string]any {
	t.Helper()
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		payload, ok := record["payload"].(map[string]any)
		if !ok {
			continue
		}
		ev, ok := payload["event"].(map[string]any)
		if !ok {
			continue
		}
		if record["payload_type"] == "runtime.session" && payload["kind"] == "task" && ev["kind"] == event {
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
	}
	t.Fatalf("capture missing task %s", event)
	return nil
}

func museCorpusMirrorRecord(t *testing.T) map[string]any {
	t.Helper()
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		if record["payload_type"] == "runtime.session.task" {
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
	}
	t.Fatal("capture missing task mirror")
	return nil
}

// museEmitTaskPlanned emits the captured planning order for one (run, task)
// key: proposed, accepted, scheduled, side_effect_intent, started.
func museEmitTaskPlanned(t *testing.T, h *musePTYHost, runID, taskID string) {
	t.Helper()
	for _, event := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started"} {
		r := museOwnedRecord(h, museCorpusTaskRecord(t, event))
		r["payload"].(map[string]any)["run_id"] = runID
		r["payload"].(map[string]any)["task_id"] = taskID
		r["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = taskID
		appendMuseRecord(t, h, r)
	}
}

func TestMusePTYR5TaskLifecycle(t *testing.T) {
	for _, terminal := range []string{"completed", "failed", "cancelled"} {
		t.Run("terminal_"+terminal, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			run := museOwnedRecord(h, museFixtureRecord("started"))
			runID := run["payload"].(map[string]any)["run_id"].(string)
			appendMuseRecord(t, h, run)
			taskID := museCorpusTaskRecord(t, "started")["payload"].(map[string]any)["task_id"].(string)
			museEmitTaskPlanned(t, h, runID, taskID)
			assertMuseNoInput(t, h, home)
			end := museOwnedRecord(h, museCorpusTaskRecord(t, terminal))
			end["payload"].(map[string]any)["run_id"] = runID
			end["payload"].(map[string]any)["task_id"] = taskID
			end["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = taskID
			appendMuseRecord(t, h, end)
			appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("assistant_message_committed")))
			appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("terminal")))
			if err := h.DeliverSessionNotice(context.Background(), "delivery after captured task terminal"); err != nil {
				t.Fatal(err)
			}
			if b, err := os.ReadFile(filepath.Join(home, "peer-submit")); err != nil || !strings.Contains(string(b), "delivery after captured task terminal") {
				t.Fatalf("after task terminal %q: %v", b, err)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		apply func(t *testing.T, h *musePTYHost, runID string)
	}{
		{"task_id_mismatch", func(t *testing.T, h *musePTYHost, runID string) {
			task := museOwnedRecord(h, museCorpusTaskRecord(t, "started"))
			task["payload"].(map[string]any)["run_id"] = runID
			task["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = museTestUUID("other-task")
			appendMuseRecord(t, h, task)
		}},
		{"empty_task_id", func(t *testing.T, h *musePTYHost, runID string) {
			task := museOwnedRecord(h, museCorpusTaskRecord(t, "started"))
			task["payload"].(map[string]any)["run_id"] = runID
			task["payload"].(map[string]any)["task_id"] = ""
			task["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = ""
			appendMuseRecord(t, h, task)
		}},
		{"double_start", func(t *testing.T, h *musePTYHost, runID string) {
			task := museOwnedRecord(h, museCorpusTaskRecord(t, "started"))
			task["payload"].(map[string]any)["run_id"] = runID
			appendMuseRecord(t, h, task)
			// A fresh envelope on the copy: the refusal is the
			// uncaptured transition, not the reused envelope.
			appendMuseRecord(t, h, museRestampRecord(museCloneRecord(t, task)))
		}},
		{"terminal_without_start", func(t *testing.T, h *musePTYHost, runID string) {
			end := museOwnedRecord(h, museCorpusTaskRecord(t, "completed"))
			end["payload"].(map[string]any)["run_id"] = runID
			appendMuseRecord(t, h, end)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			run := museOwnedRecord(h, museFixtureRecord("started"))
			appendMuseRecord(t, h, run)
			tc.apply(t, h, run["payload"].(map[string]any)["run_id"].(string))
			assertMuseNoInput(t, h, home)
		})
	}
}

func TestMusePTYR5MirrorTaskIdentity(t *testing.T) {
	h, home := musePeer(t, "idle")
	// A settled mirror lifecycle proves identity only: any planning
	// prefix without its terminal stays pending work.
	mirror := museCorpusMirrorRecord(t)
	taskID := mirror["payload"].(map[string]any)["task_id"].(string)
	museEmitMirrorPlanned(t, h, taskID)
	museEmitMirrorTerminal(t, h, taskID, "completed")
	museExpectDelivery(t, h, home, "mirror proves identity only")
	for _, tc := range []struct {
		name  string
		apply func(m map[string]any)
	}{
		{"empty_task_id", func(m map[string]any) {
			m["payload"].(map[string]any)["task_id"] = ""
			m["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = ""
		}},
		{"task_id_mismatch", func(m map[string]any) {
			m["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = museTestUUID("other-task")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			bad := museOwnedRecord(h, museCorpusMirrorRecord(t))
			tc.apply(bad)
			appendMuseRecord(t, h, bad)
			assertMuseNoInput(t, h, home)
		})
	}
}

func TestMusePTYR5ProgressRowsMustRefuse(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []string
	}{
		{"bare_thinking", []string{"◈ Thinking", "◆ Thinking", "◇ Thinking"}},
		{"timed_thinking", []string{"◈ Thinking (0s · esc to interrupt)", "◆ Thinking (1s · esc to interrupt)", "◇ Thinking (12s · esc to interrupt)"}},
		{"timed_working", []string{"◈ Working (0s · esc to interrupt)", "◆ Working (2s · esc to interrupt)", "◇ Working (11s · esc to interrupt)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, row := range tc.rows {
				h, home := musePeer(t, "idle")
				h.mu.Lock()
				h.vt.observe([]byte("\x1b[1;1H\x1b[J" + row + "\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
				h.mu.Unlock()
				assertMuseNoInput(t, h, home)
			}
		})
	}
}

func TestMusePTYR5QueueStatusMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	h.mu.Lock()
	h.vt.observe([]byte("\x1b[1;1H\x1b[J• Queued input\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
	h.mu.Unlock()
	assertMuseNoInput(t, h, home)
}

func TestMusePTYR5QueuedItemRowPermitsNoticeWhenLogIdle(t *testing.T) {
	// r7 sequence grammar: a queued-item row is content only after its
	// captured parent. Past a transcript turn it reads as quoted
	// continuation; a lone row refuses (see the r7 orphan ports).
	h, home := musePeer(t, "idle")
	h.mu.Lock()
	h.vt.observe([]byte("\x1b[1;1H\x1b[J❯ first harvest\r\n  ↳ second harvest\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
	h.mu.Unlock()
	if err := h.DeliverSessionNotice(context.Background(), "stale queue item row with idle log"); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(home, "peer-submit")); err != nil || !strings.Contains(string(b), "stale queue item row with idle log") {
		t.Fatalf("notice %q: %v", b, err)
	}
	// The captured queue context refuses through the busy row, not the
	// sequence: status still visible means not idle.
	busy, busyHome := musePeer(t, "idle")
	busy.mu.Lock()
	busy.vt.observe([]byte("\x1b[1;1H\x1b[J❯ first harvest\r\n• Queued input\r\n  ↳ second echo\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
	busy.mu.Unlock()
	assertMuseNoInput(t, busy, busyHome)
}

func TestMusePTYR5BuriedControlMustRefuse(t *testing.T) {
	for _, tc := range []struct{ name, control string }{
		{"DEL", "\x7f"},
		{"C1_CSI", "\u009b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte("\x1b[1;1H\x1b[J" + tc.control + "\r\n◆ echo: stable content\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
}

func TestMusePTYScreenGrammarCaptureCompleteness(t *testing.T) {
	b, err := os.ReadFile("testdata/muse-1.4.1/screen_rows.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Screens []struct {
			Name string   `json:"name"`
			Rows []string `json:"rows"`
		} `json:"screens"`
		LiveRows []struct {
			Row    string `json:"row"`
			Source string `json:"source"`
		} `json:"live_rows"`
	}
	if err := json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Screens) == 0 || len(fixture.LiveRows) == 0 {
		t.Fatal("screen harvest is empty")
	}
	seen := map[string]bool{}
	check := func(row, where string) {
		t.Helper()
		matches := 0
		for _, entry := range museScreenRows {
			if entry.Pattern.MatchString(row) {
				matches++
				seen[entry.ID] = true
			}
		}
		if matches != 1 {
			t.Fatalf("%s matches %d screen entries: %q", where, matches, row)
		}
	}
	for _, s := range fixture.Screens {
		vt := newMuseVT(40, 120)
		for i, row := range s.Rows {
			j := 0
			for _, r := range row {
				if i < 40 && j < 120 {
					vt.cells[i][j] = r
				}
				j++
			}
		}
		_, begin, _, found := vt.composerRows()
		if !found {
			t.Fatalf("screen %s: no composer in real capture", s.Name)
		}
		// The gate matches the whole region above the composer, so every
		// nonblank harvested row must match exactly one entry, not only
		// the boundary row.
		for k := begin - 2; k >= 0; k-- {
			line := vt.lines()[k]
			if strings.TrimSpace(line) == "" {
				continue
			}
			check(line, "screen "+s.Name)
		}
	}
	for _, lr := range fixture.LiveRows {
		check(lr.Row, "live "+lr.Source)
	}
	for _, entry := range museScreenRows {
		if !seen[entry.ID] {
			t.Errorf("unwitnessed screen entry %s", entry.ID)
		}
	}
	t.Logf("every harvested screen row matches exactly one entry; %d entries witnessed", len(seen))
}

// Bounded deterministic negatives over the r5 closures: malformed record
// bytes, orphan task records, unknown boundary rows and injected non-printing
// runes. Fixed case lists, no sampling; every case refuses through the
// production DeliverSessionNotice entry point.
func TestMusePTYR5GeneratedNegatives(t *testing.T) {
	deadline := time.Now().Add(3 * time.Minute)
	guard := func() {
		t.Helper()
		if time.Now().After(deadline) {
			t.Fatal("bounded property deadline exceeded")
		}
	}
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	restore := func() {
		t.Helper()
		if err := os.WriteFile(h.snapshot.LogPath, base, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Malformed UTF-8 at fixed interior byte positions of real shapes.
	fixtures := []map[string]any{
		museFixtureRecord("session.opened.observed"),
		museFixtureRecord("started"),
		museCorpusTaskRecord(t, "started"),
	}
	for fi, record := range fixtures {
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		for _, pos := range []int{len(raw) / 4, len(raw) / 2, 3 * len(raw) / 4} {
			guard()
			mut := append([]byte(nil), raw...)
			mut[pos] = 0xff
			if err := validateMuseRecord(mut); err == nil {
				t.Fatalf("grammar admitted malformed bytes in fixture %d", fi)
			}
			if err := os.WriteFile(h.snapshot.LogPath, append(append([]byte(nil), base...), append(mut, '\n')...), 0600); err != nil {
				t.Fatal(err)
			}
			assertMuseNoInput(t, h, home)
			restore()
		}
	}
	// Orphan and uncorrelated task records over the captured started shape.
	run := museOwnedRecord(h, museFixtureRecord("started"))
	runID := run["payload"].(map[string]any)["run_id"].(string)
	for _, tc := range []struct {
		name  string
		apply func(m map[string]any)
	}{
		{"unobserved_run", func(m map[string]any) { m["payload"].(map[string]any)["run_id"] = museTestUUID("unobserved-run") }},
		{"empty_run", func(m map[string]any) { m["payload"].(map[string]any)["run_id"] = "" }},
		{"task_id_mismatch", func(m map[string]any) {
			m["payload"].(map[string]any)["run_id"] = runID
			m["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = museTestUUID("other-task")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			guard()
			if err := os.WriteFile(h.snapshot.LogPath, base, 0600); err != nil {
				t.Fatal(err)
			}
			appendMuseRecord(t, h, run)
			task := museOwnedRecord(h, museCorpusTaskRecord(t, "started"))
			tc.apply(task)
			appendMuseRecord(t, h, task)
			assertMuseNoInput(t, h, home)
			restore()
		})
	}
	// Unknown boundary rows above a valid composer.
	for _, row := range []string{
		"• Future queue state",
		"❯",
		"◆ echo",
		"◆ Working",
		"◇ Thinking (soon)",
		"◈ Working (2s · esc to interrupt) extra",
		"› 1 Decision surface",
		"↳ bare hook row",
		"To continue this session, run muse resume elsewhere",
		"Future action required",
	} {
		guard()
		peer, peerHome := musePeer(t, "idle")
		peer.mu.Lock()
		peer.vt.observe([]byte("\x1b[1;1H\x1b[J" + row + "\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
		peer.mu.Unlock()
		assertMuseNoInput(t, peer, peerHome)
	}
	// Injected non-printing runes: C0 sample, DEL, C1 sample, format and
	// separator runes. Each poisons the screen and refuses delivery.
	for _, control := range []string{"\x02", "\x07", "\x7f", "\u0080", "\u009f", "\u200b", "\ufeff", "\u2028"} {
		guard()
		peer, peerHome := musePeer(t, "idle")
		peer.mu.Lock()
		peer.vt.observe([]byte("\x1b[1;1H\x1b[J" + control + "\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
		peer.mu.Unlock()
		assertMuseNoInput(t, peer, peerHome)
	}
}
