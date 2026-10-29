package musepty

// Revision r7 completeness: every captured lifecycle edge is in the
// transition tables, and every table edge is witnessed by the captures.
// The r13 epoch arm binds the lifecycle to session epoch state: every
// captured stream walks the epoch machine, runtime records appear only
// in open epochs, and the single-epoch edges are witnessed in-corpus
// (the reopen edges are witnessed by named peer tests and the real
// resume smoke, since the captures hold single-epoch streams). The
// row-sequence table covers the indented edges the r7 grammar closes,
// split by r13 row state so chrome and transcript edges are witnessed
// separately.

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
)

type museR7Record struct {
	Type     string `json:"payload_type"`
	Sequence int    `json:"sequence"`
	Stream   struct {
		ID string `json:"id"`
	} `json:"stream"`
	Payload struct {
		Kind              string `json:"kind"`
		RunID             string `json:"run_id"`
		SourceRunRecordID string `json:"source_run_record_id"`
		TaskID            string `json:"task_id"`
		Event             struct {
			Kind     string `json:"kind"`
			TaskID   string `json:"task_id"`
			ItemID   string `json:"item_id"`
			Terminal string `json:"terminal"`
			Target   struct {
				ID string `json:"id"`
			} `json:"drain_target_run_stream"`
			Snapshot map[string]any `json:"delivery_snapshot"`
			Source   any            `json:"source"`
			Summary  string         `json:"summary"`
			Body     string         `json:"body"`
			Payload  any            `json:"payload"`
			Dispo    string         `json:"disposition"`
		} `json:"event"`
	} `json:"payload"`
}

func museR7ExpandedRecords(t *testing.T) []museR7Record {
	t.Helper()
	raw, err := os.ReadFile("testdata/muse-1.4.1/corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	var outer []json.RawMessage
	if err := json.Unmarshal(raw, &outer); err != nil {
		t.Fatal(err)
	}
	outer = append(outer, museVersionCorpus(t, "1.4.7")...)
	// META's additional auxiliaries use invented content in a complete
	// active-run lifecycle, rather than copying the private verification log.
	for _, r := range muse147SyntheticAuxLifecycle(t) {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		outer = append(outer, b)
	}
	var out []museR7Record
	for _, rec := range outer {
		var r map[string]any
		if err := json.Unmarshal(rec, &r); err != nil {
			t.Fatal(err)
		}
		if fr, ok := r["retained_frame"].(string); ok && fr != "" {
			for _, c := range r["children"].([]any) {
				var cr museR7Record
				if err := json.Unmarshal([]byte(c.(map[string]any)["record_json"].(string)), &cr); err != nil {
					t.Fatal(err)
				}
				out = append(out, cr)
			}
			continue
		}
		var cr museR7Record
		if err := json.Unmarshal(rec, &cr); err != nil {
			t.Fatal(err)
		}
		out = append(out, cr)
	}
	return out
}

// museR13RuntimePayload names the payload families the fold's epoch gate
// covers (r13, J49): run and task events, mirrors, retained facts,
// intents, intake, cleanup effects, and the agent tree record. Route
// facts and session-informational records keep their own epoch rules.
func museR13RuntimePayload(payloadType, kind string) bool {
	switch payloadType {
	case "runtime.session":
		return kind == "run" || kind == "task" || kind == "agent_tree_initialized" || kind == "security_mode"
	case "runtime.session.task", "runtime.retained_fact",
		"runtime.user_intent.accepted", "runtime.user_intent.materialized",
		"runtime.command_intake.received", "runtime.command_intake.session_name.received",
		"runtime.command_intake.settled",
		"reminder.cleanup_effect.started", "reminder.cleanup_effect.terminal",
		"tool_batch.effect.started", "tool_batch.effect.terminal":
		return true
	}
	return false
}

func TestMusePTYR7LifecycleCompleteness(t *testing.T) {
	records := museR7ExpandedRecords(t)
	byStream := map[string][]museR7Record{}
	for _, r := range records {
		byStream[r.Stream.ID] = append(byStream[r.Stream.ID], r)
	}

	// Capture -> table, run arm: every per-run adjacent pair is a table
	// edge, started opens, terminal closes, and drains ride the queue's
	// item identity rather than the run state.
	runAuxObserved := map[string]bool{}
	startedWitnessed := false
	terminalWitnessed := false
	messageWitnessed := false
	for sid, recs := range byStream {
		sort.Slice(recs, func(i, j int) bool { return recs[i].Sequence < recs[j].Sequence })
		byRun := map[string][]museR7Record{}
		for _, r := range recs {
			if r.Type == "runtime.session" && r.Payload.Kind == "run" {
				byRun[r.Payload.RunID] = append(byRun[r.Payload.RunID], r)
			}
		}
		for run, seq := range byRun {
			starts, terminals := 0, 0
			for i, r := range seq {
				kind := r.Payload.Event.Kind
				switch kind {
				case "started":
					starts++
					startedWitnessed = true
				case "terminal":
					terminals++
					terminalWitnessed = true
					if i != len(seq)-1 {
						t.Errorf("stream %s run %s: terminal is not last", sid, run)
					}
				case "assistant_message_committed":
					messageWitnessed = true
				case "inbox_item_queued", "inbox_item_drained":
				default:
					if !museRunAuxEvents[kind] {
						t.Errorf("stream %s run %s: aux %q outside the table", sid, run, kind)
					}
					runAuxObserved[kind] = true
				}
			}
			if starts > 1 || terminals > 1 {
				t.Errorf("stream %s run %s: %d starts %d terminals", sid, run, starts, terminals)
			}
		}
	}
	// Capture -> table, epoch arm (r13): every stream walks the epoch
	// machine in sequence order; runtime records appear only in open
	// epochs, and every session edge is a table edge. Table -> capture:
	// the witnessed edge set is exactly the single-epoch pair — a
	// future multi-epoch capture forces this table to grow. The reopen
	// edges (open/ended -> reopen-pending -> open) are witnessed by
	// named peer tests instead: TestMusePTYR7RealResumeOrderBindsNewEpoch,
	// TestMusePTYR9NoEndReopenRequiresResume,
	// TestMusePTYR13ReopenedEpochAdmitsRuntime, and the real resume
	// smoke. The lone resumed fragment is the known orphan the fold
	// refuses, pinned by TestMusePTYR7CorpusStreamsFoldClean.
	epochEdges := map[string]bool{}
	runtimeInOpen := 0
	runtimeOutsideOpen := 0
	for sid, recs := range byStream {
		sort.Slice(recs, func(i, j int) bool { return recs[i].Sequence < recs[j].Sequence })
		if len(recs) == 1 && recs[0].Type == "session.resumed" {
			continue
		}
		epoch := museEpochNone
		for _, r := range recs {
			switch r.Type {
			case "session.opened.observed":
				switch epoch {
				case museEpochNone:
					epochEdges["none>open"] = true
					epoch = museEpochOpen
				case museEpochEnded, museEpochOpen:
					epochEdges["reopen>pending"] = true
					epoch = museEpochReopenPending
				default:
					t.Errorf("stream %s: opened in epoch %d outside the table", sid, epoch)
				}
			case "session.end":
				if epoch != museEpochOpen {
					t.Errorf("stream %s: end in epoch %d outside the table", sid, epoch)
					continue
				}
				epochEdges["open>ended"] = true
				epoch = museEpochEnded
			case "session.resumed":
				if epoch != museEpochReopenPending {
					t.Errorf("stream %s: resumed in epoch %d outside the table", sid, epoch)
					continue
				}
				epochEdges["pending>open"] = true
				epoch = museEpochOpen
			default:
				if museR13RuntimePayload(r.Type, r.Payload.Kind) && epoch != museEpochOpen {
					t.Errorf("stream %s: %s/%s in epoch %d, want open", sid, r.Type, r.Payload.Kind, epoch)
					runtimeOutsideOpen++
					continue
				}
				if museR13RuntimePayload(r.Type, r.Payload.Kind) {
					runtimeInOpen++
				}
			}
		}
	}
	if len(epochEdges) != 2 || !epochEdges["none>open"] || !epochEdges["open>ended"] {
		t.Fatalf("epoch edges %v, want exactly none>open and open>ended", epochEdges)
	}
	if runtimeOutsideOpen != 0 || runtimeInOpen == 0 {
		t.Fatalf("runtime records outside open: %d, in open: %d", runtimeOutsideOpen, runtimeInOpen)
	}
	// Table -> capture, run arm: every table edge is witnessed, and the
	// aux set matches the captures exactly in both directions.
	if !startedWitnessed || !terminalWitnessed || !messageWitnessed {
		t.Fatal("run started/terminal/message edge unwitnessed")
	}
	if len(runAuxObserved) != len(museRunAuxEvents) {
		t.Fatalf("aux set mismatch: captured %d table %d", len(runAuxObserved), len(museRunAuxEvents))
	}
	for kind := range museRunAuxEvents {
		if !runAuxObserved[kind] {
			t.Errorf("aux table edge %q unwitnessed", kind)
		}
	}

	// Capture -> table, task and mirror arms: every adjacent pair is a
	// table edge. Table -> capture: every table edge is witnessed.
	eventState := map[string]museTaskState{
		"proposed": museTaskProposed, "accepted": museTaskAccepted,
		"scheduled": museTaskScheduled, "side_effect_intent": museTaskIntent,
		"started": museTaskStarted, "completed": museTaskTerminated,
		"failed": museTaskTerminated, "cancelled": museTaskTerminated,
	}
	edgeWitnessed := map[museTaskState]map[string]bool{}
	for from, edges := range museTaskTransitions {
		edgeWitnessed[from] = map[string]bool{}
		for event := range edges {
			edgeWitnessed[from][event] = false
		}
	}
	checkTaskSeq := func(sid, scope string, seq []museR7Record) {
		t.Helper()
		state := museTaskState(0)
		for _, r := range seq {
			event := r.Payload.Event.Kind
			next, ok := museTaskTransitions[state][event]
			if !ok {
				t.Errorf("stream %s %s: edge %q after state %d outside the table", sid, scope, event, state)
				return
			}
			edgeWitnessed[state][event] = true
			state = next
		}
		_ = eventState
	}
	for sid, recs := range byStream {
		byTask := map[string][]museR7Record{}
		mirrors := map[string][]museR7Record{}
		for _, r := range recs {
			switch {
			case r.Type == "runtime.session" && r.Payload.Kind == "task":
				byTask[r.Payload.RunID+"\x00"+r.Payload.TaskID] = append(byTask[r.Payload.RunID+"\x00"+r.Payload.TaskID], r)
			case r.Type == "runtime.session.task":
				mirrors[r.Payload.TaskID] = append(mirrors[r.Payload.TaskID], r)
			}
		}
		for key, seq := range byTask {
			checkTaskSeq(sid, "task "+key, seq)
		}
		for key, seq := range mirrors {
			checkTaskSeq(sid, "mirror "+key, seq)
		}
	}
	for from, edges := range edgeWitnessed {
		for event, seen := range edges {
			if !seen {
				t.Errorf("task table edge state %d + %q unwitnessed", from, event)
			}
		}
	}

	// Capture -> table, queue arm: each queued item drains exactly once,
	// the drain target equals its payload run, and the snapshot
	// reproduces the queued content field by field.
	queued := map[string]museR7Record{}
	drains := 0
	for _, r := range records {
		if r.Type != "runtime.session" || r.Payload.Kind != "run" {
			continue
		}
		switch r.Payload.Event.Kind {
		case "inbox_item_queued":
			if _, dup := queued[r.Payload.SourceRunRecordID]; dup {
				t.Fatalf("queued item %s twice in captures", r.Payload.SourceRunRecordID)
			}
			queued[r.Payload.SourceRunRecordID] = r
		case "inbox_item_drained":
			drains++
			q, ok := queued[r.Payload.Event.ItemID]
			if !ok {
				t.Fatalf("drain of never-queued item %s", r.Payload.Event.ItemID)
			}
			if r.Payload.Event.Target.ID != r.Payload.RunID {
				t.Errorf("captured drain target %q differs from run %q", r.Payload.Event.Target.ID, r.Payload.RunID)
			}
			snap := r.Payload.Event.Snapshot
			qe := q.Payload.Event
			raw := func(v any) string {
				b, _ := json.Marshal(v)
				return string(b)
			}
			if raw(snap["source"]) != raw(qe.Source) || snap["summary"] != qe.Summary || snap["body"] != qe.Body || raw(snap["payload"]) != raw(qe.Payload) || snap["disposition"] != qe.Dispo {
				t.Errorf("captured drain snapshot differs from queued content for %s", r.Payload.Event.ItemID)
			}
		}
	}
	if len(queued) != 2 || drains != 2 {
		t.Fatalf("captures hold %d queues and %d drains, want 2 and 2", len(queued), drains)
	}

	// Capture -> table, cleanup arm (r11): every captured pair follows
	// the start -> child terminal -> cleanup terminal order with
	// matching keys, and the cleanup terminal claims the bound child's
	// recorded kind. Table -> capture: all three pairs witness each
	// edge of that order.
	{
		raw, err := os.ReadFile("testdata/muse-1.4.1/corpus.json")
		if err != nil {
			t.Fatal(err)
		}
		var outer []map[string]any
		if err := json.Unmarshal(raw, &outer); err != nil {
			t.Fatal(err)
		}
		type cleanupMark struct {
			start, terminal int
			child           museTaskKey
			kind            string
		}
		marks := map[museCleanupKey]*cleanupMark{}
		childTerminal := map[museTaskKey]int{}
		childKind := map[museTaskKey]string{}
		at := func(m map[string]any) int { return int(m["sequence"].(float64)) }
		for _, r := range outer {
			typ, _ := r["payload_type"].(string)
			payload, _ := r["payload"].(map[string]any)
			switch typ {
			case "reminder.cleanup_effect.started":
				rec := payload["record"].(map[string]any)
				key := museCleanupKey{Run: payload["run_id"].(string), Effect: int(rec["cleanup_effect_id"].(float64))}
				m, ok := marks[key]
				if !ok {
					m = &cleanupMark{}
					marks[key] = m
				}
				m.start = at(r)
				m.child = museTaskKey{Run: payload["run_id"].(string), Task: rec["child_task"].(map[string]any)["task_id"].(string)}
			case "reminder.cleanup_effect.terminal":
				rec := payload["record"].(map[string]any)
				key := museCleanupKey{Run: payload["run_id"].(string), Effect: int(rec["cleanup_effect_id"].(float64))}
				m, ok := marks[key]
				if !ok {
					m = &cleanupMark{}
					marks[key] = m
				}
				m.terminal = at(r)
				m.kind = rec["outcome"].(map[string]any)["child_terminal"].(map[string]any)["kind"].(string)
			case "runtime.session":
				if payload["kind"] != "task" {
					continue
				}
				ev := payload["event"].(map[string]any)
				switch ev["kind"] {
				case "completed", "failed", "cancelled":
					k := museTaskKey{Run: payload["run_id"].(string), Task: payload["task_id"].(string)}
					childTerminal[k] = at(r)
					childKind[k] = ev["kind"].(string)
				}
			}
		}
		if len(marks) != 3 {
			t.Fatalf("captures hold %d cleanup pairs, want 3", len(marks))
		}
		for key, m := range marks {
			ct, ok := childTerminal[m.child]
			if !ok {
				t.Errorf("cleanup %+v: bound child never terminates", key)
				continue
			}
			if !(m.start < ct && ct < m.terminal) {
				t.Errorf("cleanup %+v: order %d/%d/%d outside the table", key, m.start, ct, m.terminal)
			}
			if childKind[m.child] != m.kind {
				t.Errorf("cleanup %+v: claimed %q, recorded %q", key, m.kind, childKind[m.child])
			}
		}
	}
}

func TestMusePTYR7ScreenSequenceCompleteness(t *testing.T) {
	// Direction 1 (capture -> table): every edge of every real capture
	// is admitted by the production transition check. The walk calls
	// the production classifier, the production positional states and
	// the production table check — it never re-implements the
	// conditional (round-13 panel A). Backing is off: no walked frame
	// starts mid-transcript (asserted below), so no walked edge can
	// need it.
	witnesses := map[string]map[string]bool{}
	firstEdges := map[string]bool{}
	walk := func(name string, lines []string) {
		t.Helper()
		prev := museRowStart
		at := -1
		// The 1.4.10 rules warning is a positional block judged on its
		// rejoined sentence, exactly as safeComposer reads it.
		var region []string
		for _, row := range lines {
			if strings.HasPrefix(row, "─") {
				break
			}
			if strings.TrimSpace(row) != "" {
				region = append(region, row)
			}
		}
		warningEnd := museRulesWarningEnd(region)
		for _, row := range lines {
			if strings.HasPrefix(row, "─") {
				// Composer rule: the transcript region ends
				// here. Region-finding itself is pinned by
				// the composer tests; the walk covers edges.
				break
			}
			if strings.TrimSpace(row) == "" {
				continue
			}
			class, matches := museClassifyScreenRow(row)
			if class != museScreenRowContent {
				continue
			}
			if matches != 1 {
				t.Fatalf("%s: row %q matches %d content entries", name, row, matches)
			}
			at++
			state := museScreenRowStateAt(row, at, prev)
			if at >= 1 && at <= warningEnd {
				state = museRowChromeWarning
			}
			if state == "" {
				t.Fatalf("%s: row %q has no content id", name, row)
			}
			if prev == museRowStart {
				firstEdges[prev+">"+state] = true
			}
			if !museRowTransitionOK(prev, state, row, false) {
				t.Errorf("%s: captured edge %s>%s refused by the table", name, prev, state)
			}
			if witnesses[prev] == nil {
				witnesses[prev] = map[string]bool{}
			}
			witnesses[prev][state] = true
			prev = state
		}
	}
	raw, err := os.ReadFile("testdata/muse-1.4.1/screen_rows.json")
	if err != nil {
		t.Fatal(err)
	}
	var harvest struct {
		Screens []struct {
			Name string   `json:"name"`
			Rows []string `json:"rows"`
		} `json:"screens"`
	}
	if err := json.Unmarshal(raw, &harvest); err != nil {
		t.Fatal(err)
	}
	var postTurn []string
	for _, screen := range harvest.Screens {
		walk("harvest/"+screen.Name, screen.Rows)
		if screen.Name == "post_turn" {
			postTurn = screen.Rows
		}
	}
	if postTurn == nil {
		t.Fatal("harvest lost its post_turn still")
	}
	for _, file := range []string{
		"TestMusePTYReal141EchoSmoke-resume.vt",
		"TestMusePTYReal141EchoSmoke-resume-variable-color.vt",
		"TestMusePTYRealCompletedTranscriptAndLiteralFollowup.vt",
		"TestMusePTYRealLiteralRuleRoundTrip.vt",
		"TestMusePTYRealPermissionsModalNeverPastes.vt",
		"TestMusePTYRealQueueDrainRestoresNotice.vt",
	} {
		b, err := os.ReadFile("testdata/muse-1.4.1/" + file)
		if err != nil {
			t.Fatal(err)
		}
		frame := newMuseVT(40, 120)
		frame.observe(b)
		walk("vt/"+file, frame.lines())
	}
	for _, file := range []string{"screen.txt", "screen-after-submitted-turn.txt", "screen-rules-warning-startup.txt"} {
		b, err := os.ReadFile("testdata/muse-1.4.10/" + file)
		if err != nil {
			t.Fatal(err)
		}
		walk("1.4.10/"+file, strings.Split(strings.TrimSuffix(string(b), "\n"), "\n"))
	}
	// Direction 2 (table -> capture): every table edge is witnessed.
	// The literal map holds exactly the 16 unconditional edges.
	count := 0
	derived := map[string]bool{
		museRowStart + ">" + museScreenRowUserTurn: true,
		museRowStart + ">" + museScreenRowEcho:     true,
	}
	for prev, currents := range museRowTransitions {
		for current := range currents {
			count++
			edge := prev + ">" + current
			if derived[edge] {
				continue
			}
			if !witnesses[prev][current] {
				t.Errorf("table edge %s unwitnessed by any capture", edge)
			}
		}
	}
	if count != 16 {
		t.Errorf("transition table holds %d unconditional edges, want 16", count)
	}
	// No walked capture starts mid-transcript: every observed first
	// edge is start>chrome_header. The headerless start edges cannot
	// come from a still and are witnessed below instead: start>turn
	// and start>echo by scroll derivation, start>indented by the
	// log-backed behavioral tests.
	for edge := range firstEdges {
		if edge != museRowStart+">"+museRowChromeHeader {
			t.Errorf("unexpected captured first edge %s", edge)
		}
	}
	// Scroll-derived witnesses for the headerless opener edges: the
	// captured post_turn still scrolled through the production
	// emulator. post_turn physical rows open ["", header, "", turn,
	// "", echo, ...]: two linefeeds discard "" + header (turn first),
	// four discard through the turn (echo first). Filler below the
	// still fills the 40-row screen so the top rows actually scroll;
	// filler matches no content entry and skips like any non-content
	// row.
	derive := func(name string, feeds int, wantFirst string) {
		t.Helper()
		vt := newMuseVT(40, 120)
		var b strings.Builder
		for _, row := range postTurn {
			b.WriteString(row + "\r\n")
		}
		for i := 0; i < 40-len(postTurn); i++ {
			b.WriteString("scroll filler line\r\n")
		}
		vt.observe([]byte(b.String()))
		vt.observe([]byte(strings.Repeat("\n", feeds)))
		prev := museRowStart
		at := -1
		first := ""
		for _, row := range vt.lines() {
			if strings.HasPrefix(row, "─") {
				break
			}
			if strings.TrimSpace(row) == "" {
				continue
			}
			class, matches := museClassifyScreenRow(row)
			if class != museScreenRowContent {
				continue
			}
			if matches != 1 {
				t.Fatalf("%s: derived row %q matches %d", name, row, matches)
			}
			at++
			state := museScreenRowStateAt(row, at, prev)
			if first == "" {
				first = prev + ">" + state
			}
			if !museRowTransitionOK(prev, state, row, false) {
				t.Errorf("%s: derived edge %s>%s refused", name, prev, state)
			}
			prev = state
		}
		if first != wantFirst {
			t.Errorf("%s: first edge = %s, want %s", name, first, wantFirst)
		}
	}
	derive("scroll/turn-first", 2, museRowStart+">"+museScreenRowUserTurn)
	derive("scroll/echo-first", 4, museRowStart+">"+museScreenRowEcho)
	// The conditional start edge: a first-row continuation admits only
	// with log backing. No checked-in capture starts mid-transcript,
	// so this edge is witnessed behaviorally rather than by a still:
	// TestMusePTYR10LogBackedOffFrameAdmits delivers the synthetic
	// mirror, TestMusePTYR10RealLongIdleTranscriptDelivers delivers a
	// real 57-row transcript on a 40-row screen (the header necessarily
	// scrolls off), and every scrolled-frame orphan below refuses.
	if !museRowTransitionOK(museRowStart, museScreenRowIndented, "  wrapped tail", true) ||
		museRowTransitionOK(museRowStart, museScreenRowIndented, "  wrapped tail", false) {
		t.Error("start>indented ignores its backing condition")
	}

	// Log-backed production edge (r10; the r9 frame-local rule is
	// replaced): a first-row continuation refuses even when its
	// would-be parent just scrolled off, because no committed output
	// backs it. Each case drives the production VT through a real
	// scroll, a redraw, and the safeComposer predicate with a nil
	// fold: a bare-VT frame carries no log, so no off-frame row is
	// backed. No cross-frame rows are retained.
	scrolledFrame := func(parent, first string, clear bool) *museVT {
		t.Helper()
		vt := newMuseVT(40, 120)
		// Forty linefeeds on a fresh screen discard exactly the first
		// row last: rows 0-38 advance, the fortieth scrolls row 0 off.
		// The carriage returns matter: a bare linefeed keeps the
		// column, and wrapped filler would scroll the parent off early.
		vt.observe([]byte(parent + "\r\n" + strings.Repeat("scroll filler line\r\n", 39)))
		if clear {
			vt.observe([]byte("\x1b[1;1H\x1b[2J"))
		}
		vt.observe([]byte(museFrameRows(first)))
		return vt
	}
	for _, tc := range []struct {
		name, parent, first string
	}{
		{"scrolled_turn", "❯ first harvest line", "  wrapped tail"},
		{"scrolled_echo", "◆ echo: harvest short line", "  wrapped tail"},
		{"scrolled_indented", "  quoted hook row", "  wrapped tail"},
		{"scrolled_header_notice", museHeaderRow, "  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7"},
		{"filler_parent", "scroll filler line", "  Future action required"},
		{"busy_parent", "◆ Working (2s · esc to interrupt)", "  wrapped tail"},
		{"header_non_notice", museHeaderRow, "  Future action required"},
	} {
		if scrolledFrame(tc.parent, tc.first, false).safeComposer("", nil) {
			t.Errorf("scrolled-off parent admitted a first-row continuation: %s", tc.name)
		}
	}
	// Blank scrolls prove no parent: forty-one bare linefeeds discard
	// only empty rows.
	blank := newMuseVT(40, 120)
	blank.observe([]byte(strings.Repeat("\r\n", 41)))
	blank.observe([]byte(museFrameRows("  ↳ unseen queued item")))
	if blank.safeComposer("", nil) {
		t.Error("blank scroll admitted a parentless row")
	}
	// Bare linefeeds without carriage returns paint mid-row fragments:
	// whatever the last discarded row holds, it never matches a content
	// entry, so the redrawn orphan still refuses.
	bare := newMuseVT(40, 120)
	bare.observe([]byte(strings.Repeat("scroll filler line\n", 50)))
	bare.observe([]byte(museFrameRows("  Future action required")))
	if bare.safeComposer("", nil) {
		t.Error("mid-row-painted scroll admitted a parentless row")
	}
	// A full clear starts a new view: the redrawn orphan still refuses.
	if scrolledFrame("❯ first harvest line", "  wrapped tail", true).safeComposer("", nil) {
		t.Error("redrawn orphan admitted after a full clear")
	}
	// A scrolled frame whose current view carries both the parent and
	// the continuation still admits: the scroll itself poisons nothing,
	// the missing in-frame parent is what refuses above.
	scrolledInFrame := newMuseVT(40, 120)
	scrolledInFrame.observe([]byte("❯ old turn\r\n" + strings.Repeat("scroll filler line\r\n", 39)))
	scrolledInFrame.observe([]byte(museFrameRows("❯ first harvest line", "  wrapped tail")))
	if !scrolledInFrame.safeComposer("", nil) {
		t.Error("in-frame parent after scroll refused")
	}
}
