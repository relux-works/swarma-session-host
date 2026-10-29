package musepty

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const muse147SubmittedLog = "submitted-tree/2026/10/14/01a136e3-960b-7b53-be21-9cfa1cd0ed9b/session.jsonl"

func muse147SyntheticShapes(t *testing.T) []json.RawMessage {
	t.Helper()
	var rows []json.RawMessage
	if err := json.Unmarshal(muse147Fixture(t, "synthetic-meta-shapes.json"), &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestMusePTY147SubmittedTurnFoldAndStartup(t *testing.T) {
	data := muse147Fixture(t, muse147SubmittedLog)
	if err := ValidateMuseStartupPrefix(data); err != nil {
		t.Fatal(err)
	}
	f, err := readMuseLog(filepath.Join("testdata/muse-1.4.7", muse147SubmittedLog))
	if err != nil {
		t.Fatal(err)
	}
	if !f.Opened || !f.StartupPrefixWitnessed || !f.idle() || len(f.Terminals) != 1 {
		t.Fatalf("submitted turn not settled: %+v", f)
	}
	for run, terminal := range f.Terminals {
		if terminal.Terminal != "completed" || f.Messages[run] != "echo: harvest probe line" {
			t.Fatalf("captured turn output=%q terminal=%+v", f.Messages[run], terminal)
		}
	}
	rows := strings.Split(strings.TrimRight(string(muse147Fixture(t, "screen-after-submitted-turn.txt")), "\n"), "\n")
	if !muse147Screen(t, rows).safeComposer("", f) {
		t.Fatal("captured post-turn composer not safe and empty")
	}
}

func TestMusePTY147DiscoveryWithSubagentFixtureTree(t *testing.T) {
	root := "testdata/muse-1.4.7/submitted-tree"
	owned := filepath.Join("testdata/muse-1.4.7", muse147SubmittedLog)
	f, err := readMuseLog(owned)
	if err != nil {
		t.Fatal(err)
	}
	children, err := filepath.Glob(filepath.Join(filepath.Dir(owned), "subagent/*/session.jsonl"))
	if err != nil || len(children) != 2 {
		t.Fatalf("missing captured subagents: %v %v", children, err)
	}
	got, err := findMuseLog(root, f.PID)
	if err != nil || got != owned {
		t.Fatalf("primary discovery=%q err=%v", got, err)
	}
	// An otherwise-valid nested log claiming the parent's PID is stronger
	// than an unparseable child: without the depth guard it creates ambiguity.
	scratch := t.TempDir()
	rel, err := filepath.Rel(root, owned)
	if err != nil {
		t.Fatal(err)
	}
	primary := filepath.Join(scratch, rel)
	for _, p := range []string{primary, filepath.Join(filepath.Dir(primary), "subagent/claims-parent/session.jsonl")} {
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, muse147Fixture(t, muse147SubmittedLog), 0600); err != nil {
			t.Fatal(err)
		}
	}
	got, err = findMuseLog(scratch, f.PID)
	if err != nil || got != primary {
		t.Fatalf("nested PID claim affected discovery: %q %v", got, err)
	}
	if err := os.Remove(primary); err != nil {
		t.Fatal(err)
	}
	if _, err = findMuseLog(scratch, f.PID); err == nil {
		t.Fatal("nested-only session admitted")
	}
}

func TestMusePTY147PlaceholderComposerPredicates(t *testing.T) {
	captured := muse147Fixture(t, "screen-after-submitted-turn.txt")
	if bytes.Contains(captured, []byte{27}) {
		t.Fatal("SGR evidence changed; reassess placeholder identity")
	}
	rows := strings.Split(strings.TrimRight(string(captured), "\n"), "\n")
	v := muse147Screen(t, rows)
	if !v.composerMatches("") || !v.safeComposer("", nil) {
		t.Fatal("hint blocks empty composer")
	}
	if v.composerMatches(museComposerPlaceholder) || v.safeComposer(museComposerPlaceholder, nil) {
		t.Fatal("hint read as user text")
	}
	composer, _, _, found := v.composerRows()
	if !found || len(composer) != 1 || composer[0] != "" {
		t.Fatalf("composer text=%q found=%v", composer, found)
	}
	for _, text := range []string{"human draft", museComposerPlaceholder + "!", " " + museComposerPlaceholder, museComposerPlaceholder + "\nmore text"} {
		t.Run(text, func(t *testing.T) {
			v := newMuseVT(40, 120)
			v.observe([]byte(museTestScreen(text)))
			if v.composerMatches("") || v.safeComposer("", nil) {
				t.Fatal("real draft counted as empty")
			}
			if !v.composerMatches(text) || !v.safeComposer(text, nil) {
				t.Fatal("real draft not read as typed")
			}
		})
	}
	// Stills and this VT have no attribute provenance: identical typed text
	// necessarily has the hint's verdict. This is an explicit stated bound.
	typed := newMuseVT(40, 120)
	typed.observe([]byte(museTestScreen(museComposerPlaceholder)))
	if !typed.composerMatches("") || typed.composerMatches(museComposerPlaceholder) {
		t.Fatal("attribute-free ambiguity changed")
	}
}

func TestMusePTY147PlaceholderDeliveryThroughHost(t *testing.T) {
	h, home := musePeer(t, "idle")
	rows := strings.Split(strings.TrimRight(string(muse147Fixture(t, "screen-after-submitted-turn.txt")), "\n"), "\n")
	h.mu.Lock()
	h.vt.observe([]byte("\x1b[1;1H\x1b[J" + strings.Join(rows, "\r\n")))
	h.mu.Unlock()
	if !h.idle("") {
		t.Fatal("placeholder blocks host idle")
	}
	ctx, cancel := context.WithTimeout(context.Background(), museScriptedWatchdog)
	defer cancel()
	const notice = "deliver after the 1.4.7 echo turn"
	if err := h.DeliverSessionNotice(ctx, notice); err != nil {
		t.Fatal(err)
	}
	museSettleScriptedIdle(t, h, home)
	submitted, err := os.ReadFile(filepath.Join(home, "peer-submit"))
	if err != nil || string(submitted) != "1:"+notice {
		t.Fatalf("submit=%q err=%v", submitted, err)
	}
}

func TestMusePTY147SyntheticMETAClosedShapes(t *testing.T) {
	for i, raw := range muse147SyntheticShapes(t) {
		r, err := decodeMuseLogRecord(raw)
		if err != nil {
			t.Fatalf("synthetic record %d: %v", i, err)
		}
		// New protocol kinds must reject future outer and nested members,
		// unsupported versions and discriminators. Historical additive kinds
		// retain their existing L3G contract and are tested by its controls.
		key := museGrammarKey(r.known)
		closed := true
		for _, shape := range museGrammar[key] {
			closed = closed && shape.Closed
		}
		if !closed {
			continue
		}
		changed := museCloneRecord(t, r.known)
		changed["future_outer"] = true
		b, _ := json.Marshal(changed)
		if _, err := decodeMuseLogRecord(b); err == nil {
			t.Fatalf("new kind admits unknown outer field: %s", key)
		}
		changed = museCloneRecord(t, r.known)
		changed["schema_version"] = float64(2)
		b, _ = json.Marshal(changed)
		if _, err := decodeMuseLogRecord(b); err == nil {
			t.Fatalf("new kind admits future version: %s", key)
		}
		if p, ok := changed["payload"].(map[string]any); ok {
			changed = museCloneRecord(t, r.known)
			p = changed["payload"].(map[string]any)
			p["future_payload"] = true
			b, _ = json.Marshal(changed)
			if _, err := decodeMuseLogRecord(b); err == nil {
				t.Fatalf("new kind admits unknown nested field: %s", key)
			}
		}
	}
}

// Construct a short public Echo startup plus synthetic tool/status lifecycle.
// Private META bytes are never loaded by persistent tests.
func muse147ToolLifecycle(t *testing.T) ([]map[string]any, map[string]int) {
	t.Helper()
	raw := bytes.Split(bytes.TrimSuffix(muse147Fixture(t, muse147SubmittedLog), []byte{'\n'}), []byte{'\n'})
	records := make([]map[string]any, 17)
	for i := range records {
		if err := json.Unmarshal(raw[i], &records[i]); err != nil {
			t.Fatal(err)
		}
	}
	stream := records[1]["stream"].(map[string]any)["id"].(string)
	run, task := museTestUUID("147-tool-run"), museTestUUID("147-tool-task")
	indexes := map[string]int{}
	add := func(key string, r map[string]any) {
		indexes[key] = len(records)
		r["id"], r["sequence"] = museTestUUID(key), float64(100+len(records))
		r["stream"].(map[string]any)["id"] = stream
		if p, ok := r["payload"].(map[string]any); ok {
			p["run_id"] = run
			if _, ok := p["task_id"]; ok {
				p["task_id"] = task
			}
			if e, ok := p["event"].(map[string]any); ok {
				if _, ok := e["run_id"]; ok {
					e["run_id"] = run
				}
				if _, ok := e["task_id"]; ok {
					e["task_id"] = task
				}
			}
			if e, ok := p["record"].(map[string]any); ok {
				e["task_id"] = task
				e["task_stream"].(map[string]any)["id"] = task
			}
		} else {
			delete(r, "id")
			delete(r, "sequence")
			r["position"].(map[string]any)["id"], r["position"].(map[string]any)["sequence"] = museTestUUID(key), float64(100+len(records))
		}
		records = append(records, r)
	}
	add("started", museFixtureRecord("started"))
	for _, i := range []int{38, 39, 40, 41, 43} {
		var r map[string]any
		if err := json.Unmarshal(raw[i], &r); err != nil {
			t.Fatal(err)
		}
		add("task-"+r["payload"].(map[string]any)["event"].(map[string]any)["kind"].(string), r)
	}
	for _, key := range []string{"runtime.session/task/status", "tool_batch.effect.started/tool_batch_effect/", "@marker/omitted_live_only", "tool_batch.effect.terminal/tool_batch_effect/", "runtime.session/task/output"} {
		for _, r := range muse147SyntheticShapes(t) {
			v, err := decodeMuseRecord(r)
			if err != nil {
				t.Fatal(err)
			}
			if museGrammarKey(v) == key {
				add(key, v.(map[string]any))
				break
			}
		}
		if _, ok := indexes[key]; !ok {
			t.Fatalf("missing synthetic shape %s", key)
		}
	}
	var completed map[string]any
	if err := json.Unmarshal(raw[71], &completed); err != nil {
		t.Fatal(err)
	}
	add("task-completed", completed)
	add("terminal", museFixtureRecord("terminal"))
	return records, indexes
}

func muse147FoldSynthetic(t *testing.T, records []map[string]any) (*museLogFold, error) {
	t.Helper()
	var b bytes.Buffer
	for _, r := range records {
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, b.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return readMuseLog(path)
}

func TestMusePTY147SyntheticToolAndDiagnosticFold(t *testing.T) {
	records, _ := muse147ToolLifecycle(t)
	f, err := muse147FoldSynthetic(t, records)
	if err != nil || !f.idle() {
		t.Fatalf("settled tool lifecycle: fold=%+v err=%v", f, err)
	}
	for _, tc := range []struct {
		name   string
		change func([]map[string]any, map[string]int) []map[string]any
		busy   bool
	}{
		{"status-before-start", func(r []map[string]any, i map[string]int) []map[string]any {
			a, b := i["task-started"], i["runtime.session/task/status"]
			r[a], r[b] = r[b], r[a]
			return r
		}, false},
		{"output-after-completion", func(r []map[string]any, i map[string]int) []map[string]any {
			a, b := i["task-completed"], i["runtime.session/task/output"]
			r[a], r[b] = r[b], r[a]
			return r
		}, false},
		{"tool-terminal-without-start", func(r []map[string]any, i map[string]int) []map[string]any {
			n := i["tool_batch.effect.started/tool_batch_effect/"]
			return append(r[:n], r[n+1:]...)
		}, false},
		{"tool-terminal-wrong-call", func(r []map[string]any, i map[string]int) []map[string]any {
			r[i["tool_batch.effect.terminal/tool_batch_effect/"]]["payload"].(map[string]any)["record"].(map[string]any)["call_id"] = "foreign"
			return r
		}, false},
		{"tool-foreign-task-stream", func(r []map[string]any, i map[string]int) []map[string]any {
			r[i["tool_batch.effect.started/tool_batch_effect/"]]["payload"].(map[string]any)["record"].(map[string]any)["task_stream"].(map[string]any)["id"] = museTestUUID("foreign-task")
			return r
		}, false},
		{"tool-without-terminal-stays-busy", func(r []map[string]any, i map[string]int) []map[string]any {
			n := i["tool_batch.effect.terminal/tool_batch_effect/"]
			return append(r[:n], r[n+1:]...)
		}, true},
		{"marker-foreign-stream", func(r []map[string]any, i map[string]int) []map[string]any {
			r[i["@marker/omitted_live_only"]]["stream"].(map[string]any)["id"] = museTestUUID("foreign-stream")
			return r
		}, false},
		{"marker-duplicate-sequence", func(r []map[string]any, i map[string]int) []map[string]any {
			r[i["@marker/omitted_live_only"]]["position"].(map[string]any)["sequence"] = float64(3)
			return r
		}, false},
		{"marker-unknown-omission-class", func(r []map[string]any, i map[string]int) []map[string]any {
			r[i["@marker/omitted_live_only"]]["omitted_record"].(map[string]any)["omission_class"] = "future"
			return r
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, i := muse147ToolLifecycle(t)
			f, err := muse147FoldSynthetic(t, tc.change(r, i))
			if tc.busy {
				if err != nil || f.idle() {
					t.Fatalf("unsettled effect must stay busy: %+v %v", f, err)
				}
			} else if err == nil {
				t.Fatal("invalid lifecycle admitted")
			}
		})
	}
}

func TestMusePTY147IdentifierNegativeControls(t *testing.T) {
	for _, id := range []string{"resp_" + strings.Repeat("a", 24), "muse-tui-echo"} {
		if !museWellFormedResponseTag(id) {
			t.Fatalf("response refused: %s", id)
		}
	}
	for _, id := range []string{"resp_", "resp_" + strings.Repeat("a", 23), "resp_" + strings.Repeat("A", 24), "future_response"} {
		if museWellFormedResponseTag(id) {
			t.Fatalf("response admitted: %s", id)
		}
	}
	if !museWellFormedIdempotencyKey("tool:call_" + strings.Repeat("a", 32)) {
		t.Fatal("tool key refused")
	}
	for _, id := range []string{"tool:call_" + strings.Repeat("a", 31), "tool:future_" + strings.Repeat("a", 32), "reminder_child:future-reminder:1"} {
		if museWellFormedIdempotencyKey(id) {
			t.Fatalf("key admitted: %s", id)
		}
	}
	if !museWellFormedContextBlock("rules_file") || museWellFormedContextBlock("future_rules_file") || !museWellFormedReminderAgent("goal-reminder") || museWellFormedReminderAgent("future-reminder") {
		t.Fatal("identifier enum widened or captured value refused")
	}
}

func muse147SyntheticAuxLifecycle(t *testing.T) []map[string]any {
	t.Helper()
	records := []map[string]any{museFixtureRecord("session.opened.observed"), museFixtureRecord("started")}
	for _, kind := range []string{"reasoning_summary_delta", "reasoning_summary_committed", "assistant_tool_calls_committed", "tool_result_batch_committed", "context_block_updated", "reminder_proposal", "skill_reminder_decision", "reminder_reconciler_outcome"} {
		found := false
		for _, raw := range muse147SyntheticShapes(t) {
			v, err := decodeMuseRecord(raw)
			if err != nil {
				t.Fatal(err)
			}
			if museGrammarKey(v) == "runtime.session/run/"+kind {
				records = append(records, v.(map[string]any))
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing synthetic auxiliary %s", kind)
		}
	}
	records = append(records, museFixtureRecord("terminal"))
	stream, run := museTestUUID("147-aux-session"), museTestUUID("147-aux-run")
	for i, r := range records {
		r["id"], r["sequence"] = museTestUUID("147-aux-envelope-"+string(rune('a'+i))), float64(i+1)
		r["stream"].(map[string]any)["id"] = stream
		p := r["payload"].(map[string]any)
		if i == 0 {
			p["record"].(map[string]any)["session_id"] = stream
			continue
		}
		p["run_id"] = run
		if e := p["event"].(map[string]any); e["run_id"] != nil {
			e["run_id"] = run
		}
	}
	return records
}

func TestMusePTY147SyntheticAuxiliaryLifecycle(t *testing.T) {
	records := muse147SyntheticAuxLifecycle(t)
	f, err := muse147FoldSynthetic(t, records)
	if err != nil || !f.idle() {
		t.Fatalf("aux lifecycle %+v: %v", f, err)
	}
	for i := 2; i < len(records)-1; i++ {
		kind := records[i]["payload"].(map[string]any)["event"].(map[string]any)["kind"].(string)
		t.Run(kind, func(t *testing.T) {
			r := muse147SyntheticAuxLifecycle(t)
			r[i], r[len(r)-1] = r[len(r)-1], r[i]
			if _, err := muse147FoldSynthetic(t, r); err == nil {
				t.Fatal("auxiliary after terminal admitted")
			}
			r = muse147SyntheticAuxLifecycle(t)
			r[i]["payload"].(map[string]any)["run_id"] = museTestUUID("unstarted-aux-run")
			if _, err := muse147FoldSynthetic(t, r); err == nil {
				t.Fatal("auxiliary under unstarted run admitted")
			}
		})
	}
}
