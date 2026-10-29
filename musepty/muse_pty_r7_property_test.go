package musepty

// Revision r7 generated properties: one per changed branch, built from the
// real captures. Each drops, duplicates, reorders and cross-keys the
// identity fields of its branch and must refuse; the matched control from
// the real captures still admits, including the captured cross-run
// queue-to-drain transitions. Bounded and deterministic per seed, with an
// explicit timeout.

import (
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const museR7PropertySeed = 0xC10

func museR7Deadline(t *testing.T, d time.Duration) func() {
	t.Helper()
	end := time.Now().Add(d)
	return func() {
		t.Helper()
		if time.Now().After(end) {
			t.Fatalf("property exceeded its explicit %s bound", d)
		}
	}
}

// museR7Case replaces the host log with the base plus the given records
// and clears the peer input files so each case is judged on its own.
func museR7Case(t *testing.T, h *musePTYHost, home string, base []byte, records []map[string]any) {
	t.Helper()
	os.Remove(filepath.Join(home, "peer-paste"))
	os.Remove(filepath.Join(home, "peer-submit"))
	data := append([]byte(nil), base...)
	for _, r := range records {
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, raw...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(h.snapshot.LogPath, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func museR7RunRecord(t *testing.T, h *musePTYHost, kind, run string) map[string]any {
	t.Helper()
	r := museOwnedRecord(h, museFixtureRecord(kind))
	r["payload"].(map[string]any)["run_id"] = run
	return r
}

func museR7TaskRecord(t *testing.T, h *musePTYHost, event, run, task string) map[string]any {
	t.Helper()
	r := museOwnedRecord(h, museCorpusTaskRecord(t, event))
	r["payload"].(map[string]any)["run_id"] = run
	r["payload"].(map[string]any)["task_id"] = task
	r["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
	return r
}

func museR7MirrorRecord(t *testing.T, h *musePTYHost, event, task string) map[string]any {
	t.Helper()
	r := museOwnedRecord(h, museCorpusMirrorEvent(t, event))
	r["payload"].(map[string]any)["task_id"] = task
	r["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
	return r
}

// M1: every session and process reference names the owned host. Each
// corrupted identity refuses; the fully owned block admits.
func TestMusePTYR7IdentityProperty(t *testing.T) {
	guard := museR7Deadline(t, 3*time.Minute)
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	goal := museCorpusRecord(t, "runtime.session", "run", "goal_usage_attribution")
	goalRun := goal["payload"].(map[string]any)["run_id"].(string)
	tree := museCorpusRecord(t, "runtime.session", "agent_tree_initialized", "")
	name := museCorpusRecord(t, "session.name.changed", "", "")
	intake := museCorpusRecord(t, "runtime.command_intake.received", "command_intake", "")
	// The intent produces its own run: the outcome names the intent's
	// run, the accepted envelope, and the started record the run cites,
	// and the acceptance precedes the start (capture order).
	intent := museCorpusRecord(t, "runtime.user_intent.accepted", "", "")
	run := museTestUUID("prop-intent-run")
	intent["payload"].(map[string]any)["intent_id"] = run
	outcome := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.materialized", "", ""))
	outcome["payload"].(map[string]any)["intent_id"] = run
	o := outcome["payload"].(map[string]any)["outcome"].(map[string]any)
	o["run_id"] = run
	started := museR7RunRecord(t, h, "started", run)
	o["run_started_session_record_id"] = started["id"]
	o["run_started_source_record_id"] = started["payload"].(map[string]any)["source_run_record_id"]
	ownedIntent := museOwnedRecord(h, intent)
	outcome["payload"].(map[string]any)["envelope_record_id"] = ownedIntent["id"]
	outcome["payload"].(map[string]any)["envelope_sequence"] = ownedIntent["sequence"]
	// Received intake stays pending until its settle (r12): the valid
	// control settles the received command. Appended last so the
	// refuse-case indices below do not shift.
	ownedIntake := museOwnedRecord(h, intake)
	settled := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
	settled["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = ownedIntake["payload"].(map[string]any)["record"].(map[string]any)["command_id"]
	valid := []map[string]any{
		ownedIntent,
		started,
		museR7RunRecord(t, h, "terminal", run),
		museR7RunRecord(t, h, "started", goalRun),
		museOwnedRecord(h, goal),
		museR7RunRecord(t, h, "terminal", goalRun),
		museOwnedRecord(h, tree),
		museOwnedRecord(h, name),
		ownedIntake,
		outcome,
		settled,
	}
	museR7Case(t, h, home, base, valid)
	museExpectDelivery(t, h, home, "owned identity control")
	count := 1
	refuse := func(records []map[string]any) {
		t.Helper()
		guard()
		museR7Case(t, h, home, base, records)
		assertMuseNoInput(t, h, home)
		count++
	}
	clone := func(records []map[string]any) []map[string]any {
		t.Helper()
		raw, err := json.Marshal(records)
		if err != nil {
			t.Fatal(err)
		}
		var out []map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	// Drop one session reference binding at a time: stream, nested
	// record, direct payload, intake ref, intent source, aux owner.
	foreign := clone(valid)
	foreign[1]["stream"].(map[string]any)["id"] = museTestUUID("foreign-session")
	refuse(foreign)
	for _, field := range []struct {
		idx int
		get func(map[string]any) map[string]any
		key string
	}{
		{6, func(r map[string]any) map[string]any { return r["payload"].(map[string]any)["record"].(map[string]any) }, "root_session_id"},
		{7, func(r map[string]any) map[string]any { return r["payload"].(map[string]any) }, "session_id"},
		{0, func(r map[string]any) map[string]any { return r["payload"].(map[string]any) }, "source_session_id"},
	} {
		bad := clone(valid)
		field.get(bad[field.idx])[field.key] = museTestUUID("foreign-session")
		refuse(bad)
	}
	bad := clone(valid)
	bad[8]["payload"].(map[string]any)["record"].(map[string]any)["session_stream"].(map[string]any)["id"] = museTestUUID("foreign-session")
	refuse(bad)
	bad = clone(valid)
	bad[4]["payload"].(map[string]any)["event"].(map[string]any)["record"].(map[string]any)["owner"].(map[string]any)["session_id"] = museTestUUID("foreign-session")
	refuse(bad)
	// A second opened with a foreign payload identity.
	bad = clone(valid)
	opened := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
	opened["payload"].(map[string]any)["record"].(map[string]any)["session_id"] = museTestUUID("foreign-session")
	refuse(append(bad, opened))
	// A route claiming another process in the live epoch.
	bad = clone(valid)
	route := museOwnedRecord(h, museFixtureRecord("runtime.session.route_facts"))
	route["payload"].(map[string]any)["record"].(map[string]any)["pid"] = 424242
	refuse(append(bad, route))
	// The whole log rewritten to a foreign session: every record is
	// self-consistent, but the snapshot binding refuses before any paste.
	museR7Case(t, h, home, base, valid)
	data, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	guard()
	rewritten := []byte(strings.ReplaceAll(string(data), h.NativeIdentity(), museTestUUID("foreign-session")))
	if err := os.WriteFile(h.snapshot.LogPath, rewritten, 0600); err != nil {
		t.Fatal(err)
	}
	assertMuseNoInput(t, h, home)
	count++
	t.Logf("identity cases: %d", count)
}

// M2 run arm: per-run lifecycle states with an absorbing terminal. Each
// dropped, duplicated, reordered or cross-keyed run record refuses; two
// sequential valid lifecycles admit.
func TestMusePTYR7RunLifecycleProperty(t *testing.T) {
	guard := museR7Deadline(t, 3*time.Minute)
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	a := museFixtureRecord("started")["payload"].(map[string]any)["run_id"].(string)
	b := museTestUUID("prop-run-b")
	aux := museCorpusRecord(t, "runtime.session", "run", "resource_usage_sampled")
	auxRun := aux["payload"].(map[string]any)["run_id"].(string)
	control := []map[string]any{
		museR7RunRecord(t, h, "started", a),
		museR7RunRecord(t, h, "assistant_message_committed", a),
		museR7RunRecord(t, h, "terminal", a),
		museR7RunRecord(t, h, "started", b),
		museR7RunRecord(t, h, "terminal", b),
	}
	museR7Case(t, h, home, base, control)
	museExpectDelivery(t, h, home, "run lifecycle control")
	// The captured aux event settles nothing and disturbs nothing.
	auxControl := []map[string]any{
		museR7RunRecord(t, h, "started", auxRun),
		museOwnedRecord(h, aux),
		museR7RunRecord(t, h, "terminal", auxRun),
	}
	museR7Case(t, h, home, base, auxControl)
	museExpectDelivery(t, h, home, "run aux control")
	count := 2
	refuse := func(records []map[string]any) {
		t.Helper()
		guard()
		museR7Case(t, h, home, base, records)
		assertMuseNoInput(t, h, home)
		count++
	}
	rng := rand.New(rand.NewSource(museR7PropertySeed + 1))
	shapes := []func() []map[string]any{
		// Duplicate start without terminal.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "started", a), museR7RunRecord(t, h, "started", a)}
		},
		// Restart after terminal.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "started", a), museR7RunRecord(t, h, "terminal", a), museR7RunRecord(t, h, "started", a)}
		},
		// Terminal without start.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "terminal", b)}
		},
		// Message without start.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "assistant_message_committed", b)}
		},
		// Message after terminal.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "started", a), museR7RunRecord(t, h, "terminal", a), museR7RunRecord(t, h, "assistant_message_committed", a)}
		},
		// Terminal twice.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "started", a), museR7RunRecord(t, h, "terminal", a), museR7RunRecord(t, h, "terminal", a)}
		},
		// Reordered terminal before start.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "terminal", a), museR7RunRecord(t, h, "started", a)}
		},
		// Cross-keyed message under an unstarted run.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "started", a), museR7RunRecord(t, h, "assistant_message_committed", b), museR7RunRecord(t, h, "terminal", a)}
		},
		// Aux under an unstarted run.
		func() []map[string]any {
			return []map[string]any{museOwnedRecord(h, aux)}
		},
		// Aux after terminal.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "started", auxRun), museR7RunRecord(t, h, "terminal", auxRun), museOwnedRecord(h, aux)}
		},
		// Dropped terminal leaves the run active, never idle.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "started", a)}
		},
		// Cross-keyed terminal under the wrong active run.
		func() []map[string]any {
			return []map[string]any{museR7RunRecord(t, h, "started", a), museR7RunRecord(t, h, "started", b), museR7RunRecord(t, h, "terminal", museTestUUID("prop-run-c"))}
		},
	}
	for i := 0; i < 36; i++ {
		refuse(shapes[rng.Intn(len(shapes))]())
	}
	t.Logf("run lifecycle cases: %d", count)
}

// M2 task arm: the captured planning order per (run, task) key with an
// absorbing terminal. Each dropped, duplicated, reordered or cross-keyed
// task record refuses; the full order under an active run admits.
func TestMusePTYR7TaskLifecycleProperty(t *testing.T) {
	guard := museR7Deadline(t, 3*time.Minute)
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	a := museTestUUID("prop-task-run-a")
	b := museTestUUID("prop-task-run-b")
	task := museTestUUID("prop-task")
	order := []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"}
	full := func(run, id string) []map[string]any {
		var out []map[string]any
		for _, event := range order {
			out = append(out, museR7TaskRecord(t, h, event, run, id))
		}
		return out
	}
	control := append([]map[string]any{museR7RunRecord(t, h, "started", a)}, full(a, task)...)
	control = append(control, museR7RunRecord(t, h, "terminal", a))
	museR7Case(t, h, home, base, control)
	museExpectDelivery(t, h, home, "task lifecycle control")
	count := 1
	refuse := func(records []map[string]any) {
		t.Helper()
		guard()
		museR7Case(t, h, home, base, records)
		assertMuseNoInput(t, h, home)
		count++
	}
	rng := rand.New(rand.NewSource(museR7PropertySeed + 2))
	started := func() []map[string]any {
		return []map[string]any{museR7RunRecord(t, h, "started", a), museR7RunRecord(t, h, "started", b)}
	}
	for i := 0; i < 40; i++ {
		switch rng.Intn(10) {
		case 0: // Drop one planning event.
			drop := rng.Intn(4)
			var seq []map[string]any
			for j, event := range order[:5] {
				if j == drop {
					continue
				}
				seq = append(seq, museR7TaskRecord(t, h, event, a, task))
			}
			seq = append(seq, museR7TaskRecord(t, h, "completed", a, task))
			refuse(append(started()[:1], seq...))
		case 1: // Duplicate one event.
			dup := order[rng.Intn(5)]
			var seq []map[string]any
			for _, event := range order[:5] {
				seq = append(seq, museR7TaskRecord(t, h, event, a, task))
				if event == dup {
					seq = append(seq, museR7TaskRecord(t, h, event, a, task))
				}
			}
			refuse(append(started()[:1], seq...))
		case 2: // Swap two adjacent events.
			at := rng.Intn(4)
			seq := append([]string(nil), order[:5]...)
			seq[at], seq[at+1] = seq[at+1], seq[at]
			var records []map[string]any
			for _, event := range seq {
				records = append(records, museR7TaskRecord(t, h, event, a, task))
			}
			refuse(append(started()[:1], records...))
		case 3: // Terminal without start.
			refuse(append(started()[:1], museR7TaskRecord(t, h, "completed", a, task)))
		case 4: // Terminal twice.
			refuse(append(append(started()[:1], full(a, task)...), museR7TaskRecord(t, h, "completed", a, task)))
		case 5: // Started twice.
			seq := full(a, task)[:5]
			seq = append(seq, museR7TaskRecord(t, h, "started", a, task))
			refuse(append(started()[:1], seq...))
		case 6: // Terminal under the wrong active run.
			seq := append(started(), full(a, task)[:5]...)
			seq = append(seq, museR7TaskRecord(t, h, "completed", b, task))
			refuse(seq)
		case 7: // Planning under a terminated run.
			seq := []map[string]any{museR7RunRecord(t, h, "started", a), museR7RunRecord(t, h, "terminal", a)}
			seq = append(seq, museR7TaskRecord(t, h, "proposed", a, task))
			refuse(seq)
		case 8: // Started under an unobserved run.
			seq := full(museTestUUID("prop-task-run-ghost"), task)[:5]
			refuse(seq)
		case 9: // Planning regression after started.
			back := order[rng.Intn(4)]
			seq := append(started()[:1], full(a, task)[:5]...)
			seq = append(seq, museR7TaskRecord(t, h, back, a, task))
			refuse(seq)
		}
	}
	t.Logf("task lifecycle cases: %d", count)
}

// M3 queue arm: the queued item's identity and content are stored. Each
// duplicate, unobserved-run or post-drain queue refuses; the captured
// cross-run pair and the same-run pair admit.
func TestMusePTYR7QueueProperty(t *testing.T) {
	guard := museR7Deadline(t, 3*time.Minute)
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	a := museFixtureRecord("started")["payload"].(map[string]any)["run_id"].(string)
	b := museFixtureRecord("inbox_item_drained")["payload"].(map[string]any)["run_id"].(string)
	captured := []map[string]any{
		museR7RunRecord(t, h, "started", a),
		museR7RunRecord(t, h, "inbox_item_queued", a),
		museR7RunRecord(t, h, "terminal", a),
		museR7RunRecord(t, h, "started", b),
		museR7RunRecord(t, h, "terminal", b),
		museOwnedRecord(h, museFixtureRecord("inbox_item_drained")),
	}
	museR7Case(t, h, home, base, captured)
	museExpectDelivery(t, h, home, "captured cross-run control")
	sameQueue := museR7RunRecord(t, h, "inbox_item_queued", a)
	sameQueue["causation_id"] = a
	same := []map[string]any{
		museR7RunRecord(t, h, "started", a),
		sameQueue,
		museR7RunRecord(t, h, "terminal", a),
	}
	d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
	d["payload"].(map[string]any)["run_id"] = a
	d["payload"].(map[string]any)["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = a
	same = append(same, d)
	museR7Case(t, h, home, base, same)
	museExpectDelivery(t, h, home, "same-run control")
	count := 2
	refuse := func(records []map[string]any) {
		t.Helper()
		guard()
		museR7Case(t, h, home, base, records)
		assertMuseNoInput(t, h, home)
		count++
	}
	rng := rand.New(rand.NewSource(museR7PropertySeed + 3))
	for i := 0; i < 28; i++ {
		switch rng.Intn(7) {
		case 0: // Duplicate queue with the same source id.
			refuse([]map[string]any{
				museR7RunRecord(t, h, "started", a),
				museR7RunRecord(t, h, "inbox_item_queued", a),
				museR7RunRecord(t, h, "inbox_item_queued", a),
			})
		case 1: // Queue under an unobserved run.
			q := museR7RunRecord(t, h, "inbox_item_queued", museTestUUID("prop-queue-ghost"))
			refuse([]map[string]any{museR7RunRecord(t, h, "started", a), q})
		case 2: // Queue under a terminated run.
			refuse([]map[string]any{
				museR7RunRecord(t, h, "started", a),
				museR7RunRecord(t, h, "terminal", a),
				museR7RunRecord(t, h, "inbox_item_queued", a),
			})
		case 3: // Queue reuses an item id after its drain.
			seq := append([]map[string]any(nil), same...)
			seq = append(seq, museR7RunRecord(t, h, "inbox_item_queued", a))
			refuse(seq)
		case 4: // Drain without any queue.
			seq := []map[string]any{
				museR7RunRecord(t, h, "started", a),
				museR7RunRecord(t, h, "terminal", a),
			}
			dd := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
			dd["payload"].(map[string]any)["run_id"] = a
			dd["payload"].(map[string]any)["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = a
			refuse(append(seq, dd))
		case 5: // Drain twice.
			dd := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
			dd["payload"].(map[string]any)["run_id"] = a
			dd["payload"].(map[string]any)["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = a
			seq := append([]map[string]any(nil), same...)
			refuse(append(seq, dd))
		case 6: // Queued item never drains: pending, never idle.
			refuse([]map[string]any{
				museR7RunRecord(t, h, "started", a),
				museR7RunRecord(t, h, "inbox_item_queued", a),
				museR7RunRecord(t, h, "terminal", a),
			})
		}
	}
	t.Logf("queue cases: %d", count)
}

// M3 drain arm: the drain matches the stored item, decodes its target
// run stream against its payload run the way the captures relate them,
// and both runs are observed in the owned stream.
func TestMusePTYR7DrainProperty(t *testing.T) {
	guard := museR7Deadline(t, 3*time.Minute)
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	a := museFixtureRecord("started")["payload"].(map[string]any)["run_id"].(string)
	b := museFixtureRecord("inbox_item_drained")["payload"].(map[string]any)["run_id"].(string)
	prefix := func(forward string) []map[string]any {
		q := museR7RunRecord(t, h, "inbox_item_queued", a)
		q["causation_id"] = forward
		return []map[string]any{
			museR7RunRecord(t, h, "started", a),
			q,
			museR7RunRecord(t, h, "terminal", a),
		}
	}
	drain := func(run, target string) map[string]any {
		d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
		d["payload"].(map[string]any)["run_id"] = run
		d["payload"].(map[string]any)["event"].(map[string]any)["drain_target_run_stream"].(map[string]any)["id"] = target
		return d
	}
	// The captured cross-run queue-to-drain transition admits with both
	// runs observed; the drain target equals the queued forward identity.
	// A forward-only drain into a never-started run stays pending: see
	// TestMusePTYR8DrainForwardReferenceMustResolve.
	cross := append(prefix(b), museR7RunRecord(t, h, "started", b), museR7RunRecord(t, h, "terminal", b))
	museR7Case(t, h, home, base, append(cross, drain(b, b)))
	museExpectDelivery(t, h, home, "captured cross-run control")
	// The same-run pair admits with both runs started.
	museR7Case(t, h, home, base, append(prefix(a), drain(a, a)))
	museExpectDelivery(t, h, home, "same-run control")
	count := 2
	refuse := func(records []map[string]any) {
		t.Helper()
		guard()
		museR7Case(t, h, home, base, records)
		assertMuseNoInput(t, h, home)
		count++
	}
	rng := rand.New(rand.NewSource(museR7PropertySeed + 4))
	snapshotFields := []string{"source", "summary", "body", "payload", "disposition"}
	for i := 0; i < 30; i++ {
		switch rng.Intn(5) {
		case 0: // Target names another run than the payload run.
			other := museTestUUID("prop-drain-foreign")
			if rng.Intn(2) == 0 {
				refuse(append(prefix(a), drain(a, other)))
			} else {
				refuse(append(prefix(a), drain(other, a)))
			}
		case 1: // Snapshot differs from the stored queued content.
			d := drain(a, a)
			snap := d["payload"].(map[string]any)["event"].(map[string]any)["delivery_snapshot"].(map[string]any)
			switch snapshotFields[rng.Intn(len(snapshotFields))] {
			case "source":
				snap["source"] = map[string]any{"source": "rewritten"}
			case "summary":
				snap["summary"] = "rewritten summary"
			case "body":
				snap["body"] = "rewritten body"
			case "payload":
				snap["payload"] = map[string]any{"prompt": "rewritten", "workflow_trigger_mode": "manual"}
			case "disposition":
				snap["disposition"] = "rewritten"
			}
			refuse(append(prefix(a), d))
		case 2: // Drain run neither started nor the queued forward reference.
			c := museTestUUID("prop-drain-ghost")
			refuse(append(prefix(a), drain(c, c)))
		case 3: // Drain for an item that was never queued.
			d := drain(a, a)
			d["payload"].(map[string]any)["event"].(map[string]any)["item_id"] = museTestUUID("prop-never-queued")
			seq := []map[string]any{
				museR7RunRecord(t, h, "started", a),
				museR7RunRecord(t, h, "terminal", a),
			}
			refuse(append(seq, d))
		case 4: // Queue content rewritten after the fact cannot match.
			q := museR7RunRecord(t, h, "inbox_item_queued", a)
			q["causation_id"] = a
			q["payload"].(map[string]any)["event"].(map[string]any)["body"] = "reworded pending work"
			seq := []map[string]any{
				museR7RunRecord(t, h, "started", a),
				q,
				museR7RunRecord(t, h, "terminal", a),
			}
			refuse(append(seq, drain(a, a)))
		}
	}
	t.Logf("drain cases: %d", count)
}

// M4 mirror arm: a mirror started record stays pending until its
// run-bound counterpart or its own terminal settles it. Each dangling,
// duplicated, reordered or reused mirror record refuses.
func TestMusePTYR7MirrorProperty(t *testing.T) {
	guard := museR7Deadline(t, 3*time.Minute)
	h, home := musePeer(t, "idle")
	base, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	task := museTestUUID("prop-mirror")
	order := []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"}
	full := func(id string) []map[string]any {
		var out []map[string]any
		for _, event := range order {
			out = append(out, museR7MirrorRecord(t, h, event, id))
		}
		return out
	}
	museR7Case(t, h, home, base, full(task))
	museExpectDelivery(t, h, home, "full mirror control")
	run := museTestUUID("prop-mirror-run")
	counterpart := []map[string]any{museR7RunRecord(t, h, "started", run)}
	for _, event := range order[:5] {
		counterpart = append(counterpart, museR7MirrorRecord(t, h, event, task))
	}
	for _, event := range order {
		counterpart = append(counterpart, museR7TaskRecord(t, h, event, run, task))
	}
	counterpart = append(counterpart, museR7RunRecord(t, h, "terminal", run))
	count := 1
	refuse := func(records []map[string]any) {
		t.Helper()
		guard()
		museR7Case(t, h, home, base, records)
		assertMuseNoInput(t, h, home)
		count++
	}
	// r9 item 2: a same-task run-bound terminal never discharges the
	// mirror; the mirror's own terminal is the only discharge.
	refuse(counterpart)
	rng := rand.New(rand.NewSource(museR7PropertySeed + 5))
	for i := 0; i < 28; i++ {
		switch rng.Intn(8) {
		case 0: // Started alone stays pending.
			refuse(full(task)[:5])
		case 1: // Terminal without start.
			refuse([]map[string]any{museR7MirrorRecord(t, h, "completed", task)})
		case 2: // Duplicate one event.
			dup := order[rng.Intn(5)]
			var seq []map[string]any
			for _, event := range order[:5] {
				seq = append(seq, museR7MirrorRecord(t, h, event, task))
				if event == dup {
					seq = append(seq, museR7MirrorRecord(t, h, event, task))
				}
			}
			refuse(seq)
		case 3: // Swap two adjacent events.
			at := rng.Intn(4)
			seq := append([]string(nil), order[:5]...)
			seq[at], seq[at+1] = seq[at+1], seq[at]
			var records []map[string]any
			for _, event := range seq {
				records = append(records, museR7MirrorRecord(t, h, event, task))
			}
			refuse(records)
		case 4: // Drop one planning event.
			drop := rng.Intn(4)
			var seq []map[string]any
			for j, event := range order[:5] {
				if j == drop {
					continue
				}
				seq = append(seq, museR7MirrorRecord(t, h, event, task))
			}
			seq = append(seq, museR7MirrorRecord(t, h, "completed", task))
			refuse(seq)
		case 5: // Reuse after terminal.
			refuse(append(full(task), museR7MirrorRecord(t, h, "proposed", task)))
		case 6: // Run-bound counterpart for another task settles nothing.
			seq := append([]map[string]any{museR7RunRecord(t, h, "started", run)}, full(task)[:5]...)
			for _, event := range order {
				seq = append(seq, museR7TaskRecord(t, h, event, run, museTestUUID("prop-other-mirror")))
			}
			seq = append(seq, museR7RunRecord(t, h, "terminal", run))
			refuse(seq)
		case 7: // A mirror failed outcome is not even a captured shape.
			bad := museR7MirrorRecord(t, h, "completed", task)
			bad["payload"].(map[string]any)["event"].(map[string]any)["kind"] = "failed"
			refuse([]map[string]any{bad})
		}
	}
	t.Logf("mirror cases: %d", count)
}

// M5 screen arm: an indented row is content only right after its captured
// parent. Each parentless indented row refuses; each captured sequence
// admits.
func TestMusePTYR7ScreenSequenceProperty(t *testing.T) {
	guard := museR7Deadline(t, 3*time.Minute)
	h, home := musePeer(t, "idle")
	rng := rand.New(rand.NewSource(museR7PropertySeed + 6))
	render := func(rows []string) {
		t.Helper()
		guard()
		os.Remove(filepath.Join(home, "peer-paste"))
		os.Remove(filepath.Join(home, "peer-submit"))
		h.mu.Lock()
		h.vt = newMuseVT(40, 120)
		h.vt.observe([]byte(museFrameRows(rows...)))
		h.mu.Unlock()
	}
	count := 0
	admit := func(rows []string) {
		t.Helper()
		guard()
		// A fresh peer per admission: a delivery's trailing output
		// must not land in the next case's frame.
		ah, ahome := musePeer(t, "idle")
		ah.mu.Lock()
		ah.vt.observe([]byte(museFrameRows(rows...)))
		ah.mu.Unlock()
		museExpectDelivery(t, ah, ahome, "sequence control")
		count++
	}
	// Captured sequences admit.
	admit(nil)
	admit([]string{museHeaderRow})
	admit([]string{museHeaderRow, "  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7"})
	admit([]string{museHeaderRow, "❯ first harvest line"})
	admit([]string{museHeaderRow, "❯ first harvest line", "  second harvest line"})
	admit([]string{"❯ first harvest line", "◆ echo: harvest short line", "  wrapped tail"})
	admit([]string{"❯ first harvest line", "  ↳ quoted hook row"})
	admit([]string{"❯ first harvest line", "  ◈ quoted glyph row"})
	orphans := []string{
		"  Future action required", "  Waiting for remote approval",
		"  Working (1s · esc to interrupt)", "  ↳ unseen queued item",
		"  ◈ Future action required", "  ◆ Working (2s · esc to interrupt)",
		"  ◇ Thinking (1s · esc to interrupt)", "  • Queued input",
		"  ❯ smuggled turn", "   deep orphan", "      code orphan",
		"  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7",
	}
	afterHeader := []string{
		"  Future action required", "  ↳ unseen queued item",
		"  resumed session", "  resumed session !!",
		"  ◈ Future action required", "  Muse Code 1.4.1 extra",
	}
	for i := 0; i < 36; i++ {
		guard()
		if rng.Intn(2) == 0 {
			render([]string{orphans[rng.Intn(len(orphans))]})
		} else {
			render([]string{museHeaderRow, afterHeader[rng.Intn(len(afterHeader))]})
		}
		assertMuseNoInput(t, h, home)
		count++
	}
	t.Logf("screen sequence cases: %d", count)
}
