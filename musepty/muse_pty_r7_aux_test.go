package musepty

// Revision r7, owned-identity bindings for the auxiliary and metadata
// record families (M1), plus the whole-capture positive control: every
// checked-in corpus stream folds without error under the strict rules.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every captured stream folds without error: the strict r7 transition
// tables, session bindings and correlation rules admit all real data,
// including both cross-run queue-to-drain pairs and the failed-run
// stream (which folds clean but never idle).
func TestMusePTYR7CorpusStreamsFoldClean(t *testing.T) {
	raw, err := os.ReadFile("testdata/muse-1.4.1/corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	var outer []json.RawMessage
	if err := json.Unmarshal(raw, &outer); err != nil {
		t.Fatal(err)
	}
	byStream := map[string][][]byte{}
	emit := func(sid string, v any) {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		byStream[sid] = append(byStream[sid], b)
	}
	for _, rec := range outer {
		var r map[string]any
		if err := json.Unmarshal(rec, &r); err != nil {
			t.Fatal(err)
		}
		if fr, ok := r["retained_frame"].(string); ok && fr != "" {
			for _, c := range r["children"].([]any) {
				s := c.(map[string]any)["record_json"].(string)
				var cr map[string]any
				if err := json.Unmarshal([]byte(s), &cr); err != nil {
					t.Fatal(err)
				}
				emit(cr["stream"].(map[string]any)["id"].(string), cr)
			}
			continue
		}
		emit(r["stream"].(map[string]any)["id"].(string), r)
	}
	if len(byStream) != 6 {
		t.Fatalf("captures hold %d streams, want 6", len(byStream))
	}
	for sid, recs := range byStream {
		p := filepath.Join(t.TempDir(), "session.jsonl")
		var data []byte
		for _, r := range recs {
			data = append(data, r...)
			data = append(data, '\n')
		}
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatal(err)
		}
		// r11: the lone resumed record is a single-record fragment
		// stream, not a foldable session: an orphan resumed is
		// unknown state. Every other captured stream folds clean.
		var probe struct {
			Type string `json:"payload_type"`
		}
		if len(recs) == 1 && json.Unmarshal(recs[0], &probe) == nil && probe.Type == "session.resumed" {
			if _, err := readMuseLog(p); err == nil || !strings.Contains(err.Error(), "without a pending reopen") {
				t.Errorf("stream %s: orphan resumed fragment admitted: %v", sid, err)
			}
			continue
		}
		fold, err := readMuseLog(p)
		if err != nil {
			t.Errorf("stream %s: %v", sid, err)
			continue
		}
		// r12: every captured received command settles in its own stream,
		// so no captured stream leaves intake pending either.
		if len(fold.Queued) != 0 || fold.pendingTaskCount() != 0 || fold.pendingIntentCount() != 0 || fold.pendingIntakeCount() != 0 {
			t.Errorf("stream %s: captured work left pending", sid)
		}
	}
}

func museEmitRunBounded(t *testing.T, h *musePTYHost, kind, runID string) {
	t.Helper()
	r := museOwnedRecord(h, museFixtureRecord(kind))
	r["payload"].(map[string]any)["run_id"] = runID
	appendMuseRecord(t, h, r)
}

// Session-family records whose session reference is foreign refuse; the
// owned shape is a no-op control.
func TestMusePTYR7SessionFamilyBindingsMustRefuse(t *testing.T) {
	cases := []struct {
		name string
		get  func(*testing.T) map[string]any
		swap func(map[string]any)
	}{
		{"agent_tree_root", func(t *testing.T) map[string]any {
			return museCorpusRecord(t, "runtime.session", "agent_tree_initialized", "")
		}, func(r map[string]any) {
			r["payload"].(map[string]any)["record"].(map[string]any)["root_session_id"] = museTestUUID("foreign-session")
		}},
		{"name_changed", func(t *testing.T) map[string]any {
			return museCorpusRecord(t, "session.name.changed", "", "")
		}, func(r map[string]any) {
			r["payload"].(map[string]any)["session_id"] = museTestUUID("foreign-session")
		}},
		{"command_invoked", func(t *testing.T) map[string]any {
			return museCorpusRecord(t, "command.invoked", "command_invoked", "")
		}, func(r map[string]any) {
			r["payload"].(map[string]any)["record"].(map[string]any)["session_id"] = museTestUUID("foreign-session")
		}},
		{"startup_phases", func(t *testing.T) map[string]any {
			return museCorpusRecord(t, "session.startup_phases.observed", "startup_phases", "")
		}, func(r map[string]any) {
			r["payload"].(map[string]any)["record"].(map[string]any)["session_id"] = museTestUUID("foreign-session")
		}},
		{"intake_session_stream", func(t *testing.T) map[string]any {
			return museCorpusRecord(t, "runtime.command_intake.received", "command_intake", "")
		}, func(r map[string]any) {
			r["payload"].(map[string]any)["record"].(map[string]any)["session_stream"].(map[string]any)["id"] = museTestUUID("foreign-session")
		}},
		{"intent_source", func(t *testing.T) map[string]any {
			return museCorpusRecord(t, "runtime.user_intent.accepted", "", "")
		}, func(r map[string]any) {
			r["payload"].(map[string]any)["source_session_id"] = museTestUUID("foreign-session")
		}},
		{"session_end", func(t *testing.T) map[string]any {
			return museFixtureRecord("session.end")
		}, func(r map[string]any) {
			r["payload"].(map[string]any)["record"].(map[string]any)["session_id"] = museTestUUID("foreign-session")
		}},
	}
	for _, c := range cases {
		t.Run(c.name+"/foreign_refuses", func(t *testing.T) {
			h, home := musePeer(t, "idle")
			r := museOwnedRecord(h, c.get(t))
			c.swap(r)
			appendMuseRecord(t, h, r)
			assertMuseNoInput(t, h, home)
		})
		t.Run(c.name+"/owned_permits", func(t *testing.T) {
			h, home := musePeer(t, "idle")
			base := c.get(t)
			appendMuseRecord(t, h, museOwnedRecord(h, base))
			// session.end is a state change, not a no-op: it ends the
			// session, so the permit check is a clean fold, not idle.
			if c.name == "session_end" {
				f, err := h.fold()
				if err != nil || !f.Ended {
					t.Fatalf("owned end misfolded: %v %+v", err, f)
				}
				assertMuseNoInput(t, h, home)
				return
			}
			// An accepted intent stays pending until its outcome
			// references it, so the owned control settles the pair
			// with the run the intent produced: the outcome cites
			// the accepted envelope and the started record.
			if c.name == "intent_source" {
				run := base["payload"].(map[string]any)["intent_id"].(string)
				started := museOwnedRecord(h, museFixtureRecord("started"))
				started["payload"].(map[string]any)["run_id"] = run
				appendMuseRecord(t, h, started)
				owned := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.materialized", "", ""))
				p := owned["payload"].(map[string]any)
				p["intent_id"] = run
				p["envelope_record_id"] = base["id"]
				p["envelope_sequence"] = base["sequence"]
				o := p["outcome"].(map[string]any)
				o["run_id"] = run
				o["run_started_session_record_id"] = started["id"]
				o["run_started_source_record_id"] = started["payload"].(map[string]any)["source_run_record_id"]
				appendMuseRecord(t, h, owned)
				museEmitRunBounded(t, h, "terminal", run)
			}
			// A received command stays pending until its settle
			// (r12), so the owned intake control settles the
			// received command: the case under test is the
			// session binding, not pending intake.
			if c.name == "intake_session_stream" {
				cmd := base["payload"].(map[string]any)["record"].(map[string]any)["command_id"].(string)
				settled := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
				settled["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = cmd
				appendMuseRecord(t, h, settled)
			}
			museExpectDelivery(t, h, home, "owned session control")
		})
	}
}

// Auxiliary run events name their session, run and task stream. Each
// binding is refused foreign and admitted owned.
func TestMusePTYR7AuxIdentityMustRefuse(t *testing.T) {
	// Corpus run and task ids stay captured; the tests start matching
	// runs and propose matching tasks for them.
	t.Run("goal_owner_session", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		g := museCorpusRecord(t, "runtime.session", "run", "goal_usage_attribution")
		run := g["payload"].(map[string]any)["run_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, g)
		r["payload"].(map[string]any)["event"].(map[string]any)["record"].(map[string]any)["owner"].(map[string]any)["session_id"] = museTestUUID("foreign-session")
		appendMuseRecord(t, h, r)
		assertMuseNoInput(t, h, home)
	})
	t.Run("goal_owner_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		g := museCorpusRecord(t, "runtime.session", "run", "goal_usage_attribution")
		run := g["payload"].(map[string]any)["run_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, g)
		r["payload"].(map[string]any)["event"].(map[string]any)["record"].(map[string]any)["owner"].(map[string]any)["run_id"] = museTestUUID("another-run")
		appendMuseRecord(t, h, r)
		assertMuseNoInput(t, h, home)
	})
	t.Run("goal_owned_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		g := museCorpusRecord(t, "runtime.session", "run", "goal_usage_attribution")
		run := g["payload"].(map[string]any)["run_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		appendMuseRecord(t, h, museOwnedRecord(h, g))
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "owned goal control")
	})
	t.Run("memory_parent_session", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		m := museCorpusRecord(t, "runtime.session", "run", "memory_reminder_child_session_linked")
		run := m["payload"].(map[string]any)["run_id"].(string)
		task := m["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, m)
		r["payload"].(map[string]any)["event"].(map[string]any)["parent_session_id"] = museTestUUID("foreign-session")
		appendMuseRecord(t, h, r)
		// The task link resolves so only the session binding refuses.
		first := museOwnedRecord(h, museCorpusTaskRecord(t, "proposed"))
		first["payload"].(map[string]any)["run_id"] = run
		first["payload"].(map[string]any)["task_id"] = task
		first["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, first)
		assertMuseNoInput(t, h, home)
	})
	t.Run("memory_parent_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		m := museCorpusRecord(t, "runtime.session", "run", "memory_reminder_child_session_linked")
		run := m["payload"].(map[string]any)["run_id"].(string)
		task := m["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, m)
		r["payload"].(map[string]any)["event"].(map[string]any)["parent_run_id"] = museTestUUID("another-run")
		appendMuseRecord(t, h, r)
		first := museOwnedRecord(h, museCorpusTaskRecord(t, "proposed"))
		first["payload"].(map[string]any)["run_id"] = run
		first["payload"].(map[string]any)["task_id"] = task
		first["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, first)
		assertMuseNoInput(t, h, home)
	})
	t.Run("memory_task_stream", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		m := museCorpusRecord(t, "runtime.session", "run", "memory_reminder_child_session_linked")
		run := m["payload"].(map[string]any)["run_id"].(string)
		task := m["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, m)
		r["payload"].(map[string]any)["event"].(map[string]any)["task_stream"].(map[string]any)["id"] = museTestUUID("another-task")
		appendMuseRecord(t, h, r)
		first := museOwnedRecord(h, museCorpusTaskRecord(t, "proposed"))
		first["payload"].(map[string]any)["run_id"] = run
		first["payload"].(map[string]any)["task_id"] = task
		first["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, first)
		assertMuseNoInput(t, h, home)
	})
	t.Run("memory_owned_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		m := museCorpusRecord(t, "runtime.session", "run", "memory_reminder_child_session_linked")
		run := m["payload"].(map[string]any)["run_id"].(string)
		task := m["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		// r11: the link's reminder agent resolves against the
		// run's configured roster (identical in every capture).
		cfg := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
		cfg["payload"].(map[string]any)["run_id"] = run
		appendMuseRecord(t, h, cfg)
		appendMuseRecord(t, h, museOwnedRecord(h, m))
		museEmitTaskPlanned(t, h, run, task)
		end := museOwnedRecord(h, museCorpusTaskRecord(t, "completed"))
		end["payload"].(map[string]any)["run_id"] = run
		end["payload"].(map[string]any)["task_id"] = task
		end["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, end)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "owned reminder control")
	})
	t.Run("linked_task_stream", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		l := museCorpusRecord(t, "runtime.session", "run", "task_stream_linked")
		run := l["payload"].(map[string]any)["run_id"].(string)
		task := l["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, l)
		r["payload"].(map[string]any)["event"].(map[string]any)["task_stream"].(map[string]any)["id"] = museTestUUID("another-task")
		appendMuseRecord(t, h, r)
		first := museOwnedRecord(h, museCorpusTaskRecord(t, "proposed"))
		first["payload"].(map[string]any)["run_id"] = run
		first["payload"].(map[string]any)["task_id"] = task
		first["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, first)
		assertMuseNoInput(t, h, home)
	})
	t.Run("linked_owned_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		l := museCorpusRecord(t, "runtime.session", "run", "task_stream_linked")
		run := l["payload"].(map[string]any)["run_id"].(string)
		task := l["payload"].(map[string]any)["event"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		appendMuseRecord(t, h, museOwnedRecord(h, l))
		museEmitTaskPlanned(t, h, run, task)
		end := museOwnedRecord(h, museCorpusTaskRecord(t, "completed"))
		end["payload"].(map[string]any)["run_id"] = run
		end["payload"].(map[string]any)["task_id"] = task
		end["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, end)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "owned link control")
	})
}

// Forward references resolve by end of fold: a retained fact, a task
// link, or a materialized outcome naming a run or task the log never
// observes is unknown state.
func TestMusePTYR7ForwardReferencesMustResolve(t *testing.T) {
	t.Run("retained_unknown_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museOwnedRecord(h, museCorpusRecord(t, "runtime.retained_fact", "retained_runtime_fact", ""))
		r["payload"].(map[string]any)["run_id"] = museTestUUID("never-observed-run")
		appendMuseRecord(t, h, r)
		assertMuseNoInput(t, h, home)
	})
	t.Run("retained_known_run_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museCorpusRecord(t, "runtime.retained_fact", "retained_runtime_fact", "")
		run := r["payload"].(map[string]any)["run_id"].(string)
		appendMuseRecord(t, h, museOwnedRecord(h, r))
		museEmitRunBounded(t, h, "started", run)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "owned retained control")
	})
	t.Run("linked_unknown_task_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		l := museCorpusRecord(t, "runtime.session", "run", "task_stream_linked")
		run := l["payload"].(map[string]any)["run_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		r := museOwnedRecord(h, l)
		r["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = museTestUUID("never-observed-task")
		r["payload"].(map[string]any)["event"].(map[string]any)["task_stream"].(map[string]any)["id"] = museTestUUID("never-observed-task")
		appendMuseRecord(t, h, r)
		museEmitRunBounded(t, h, "terminal", run)
		assertMuseNoInput(t, h, home)
	})
	t.Run("outcome_unknown_run_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museCorpusRecord(t, "runtime.user_intent.materialized", "", "")
		intent := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", ""))
		intent["payload"].(map[string]any)["intent_id"] = museTestUUID("never-observed-run")
		appendMuseRecord(t, h, intent)
		owned := museOwnedRecord(h, r)
		p := owned["payload"].(map[string]any)
		p["intent_id"] = museTestUUID("never-observed-run")
		p["envelope_record_id"] = intent["id"]
		p["envelope_sequence"] = intent["sequence"]
		o := p["outcome"].(map[string]any)
		o["run_id"] = museTestUUID("never-observed-run")
		o["run_started_session_record_id"] = museTestUUID("never-observed-start")
		o["run_started_source_record_id"] = museTestUUID("never-observed-source")
		appendMuseRecord(t, h, owned)
		assertMuseNoInput(t, h, home)
	})
	t.Run("outcome_known_run_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museCorpusRecord(t, "runtime.user_intent.materialized", "", "")
		intent := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", ""))
		intent["payload"].(map[string]any)["intent_id"] = r["payload"].(map[string]any)["intent_id"].(string)
		run := intent["payload"].(map[string]any)["intent_id"].(string)
		appendMuseRecord(t, h, intent)
		started := museOwnedRecord(h, museFixtureRecord("started"))
		started["payload"].(map[string]any)["run_id"] = run
		appendMuseRecord(t, h, started)
		owned := museOwnedRecord(h, r)
		p := owned["payload"].(map[string]any)
		p["envelope_record_id"] = intent["id"]
		p["envelope_sequence"] = intent["sequence"]
		o := p["outcome"].(map[string]any)
		o["run_id"] = run
		o["run_started_session_record_id"] = started["id"]
		o["run_started_source_record_id"] = started["payload"].(map[string]any)["source_run_record_id"]
		appendMuseRecord(t, h, owned)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "owned outcome control")
	})
}

// Cleanup effects correlate inline: the parent run and the child task
// are already observed when the effect lands in the captures.
func TestMusePTYR7CleanupCorrelationMustRefuse(t *testing.T) {
	setup := func(t *testing.T) (*musePTYHost, string, string, string) {
		t.Helper()
		h, home := musePeer(t, "idle")
		c := museCorpusRecord(t, "reminder.cleanup_effect.started", "reminder_cleanup", "")
		run := c["payload"].(map[string]any)["run_id"].(string)
		task := c["payload"].(map[string]any)["record"].(map[string]any)["child_task"].(map[string]any)["task_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		first := museOwnedRecord(h, museCorpusTaskRecord(t, "proposed"))
		first["payload"].(map[string]any)["run_id"] = run
		first["payload"].(map[string]any)["task_id"] = task
		first["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
		appendMuseRecord(t, h, first)
		return h, home, run, task
	}
	t.Run("owned_permits", func(t *testing.T) {
		h, home, run, task := setup(t)
		appendMuseRecord(t, h, museOwnedRecord(h, museCorpusRecord(t, "reminder.cleanup_effect.started", "reminder_cleanup", "")))
		// Captured order: the child cancels before the cleanup
		// reports cancellation, and the cleanup terminal lands
		// before the parent terminal.
		for _, event := range []string{"accepted", "scheduled", "side_effect_intent", "started", "cancelled"} {
			r := museOwnedRecord(h, museCorpusTaskRecord(t, event))
			r["payload"].(map[string]any)["run_id"] = run
			r["payload"].(map[string]any)["task_id"] = task
			r["payload"].(map[string]any)["event"].(map[string]any)["task_id"] = task
			appendMuseRecord(t, h, r)
		}
		end := museOwnedRecord(h, museCorpusRecord(t, "reminder.cleanup_effect.terminal", "reminder_cleanup", ""))
		end["payload"].(map[string]any)["run_id"] = run
		appendMuseRecord(t, h, end)
		museEmitRunBounded(t, h, "terminal", run)
		museExpectDelivery(t, h, home, "owned cleanup control")
	})
	t.Run("parent_run_mismatch", func(t *testing.T) {
		h, home, _, _ := setup(t)
		r := museOwnedRecord(h, museCorpusRecord(t, "reminder.cleanup_effect.started", "reminder_cleanup", ""))
		r["payload"].(map[string]any)["record"].(map[string]any)["parent_run_stream"].(map[string]any)["id"] = museTestUUID("another-run")
		appendMuseRecord(t, h, r)
		assertMuseNoInput(t, h, home)
	})
	t.Run("child_stream_mismatch", func(t *testing.T) {
		h, home, _, _ := setup(t)
		r := museOwnedRecord(h, museCorpusRecord(t, "reminder.cleanup_effect.started", "reminder_cleanup", ""))
		r["payload"].(map[string]any)["record"].(map[string]any)["child_task"].(map[string]any)["task_stream"].(map[string]any)["id"] = museTestUUID("another-task")
		appendMuseRecord(t, h, r)
		assertMuseNoInput(t, h, home)
	})
	t.Run("unknown_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museOwnedRecord(h, museCorpusRecord(t, "reminder.cleanup_effect.started", "reminder_cleanup", ""))
		r["payload"].(map[string]any)["run_id"] = museTestUUID("never-observed-run")
		r["payload"].(map[string]any)["record"].(map[string]any)["parent_run_stream"].(map[string]any)["id"] = museTestUUID("never-observed-run")
		appendMuseRecord(t, h, r)
		assertMuseNoInput(t, h, home)
	})
	t.Run("unknown_task", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museCorpusRecord(t, "reminder.cleanup_effect.started", "reminder_cleanup", "")
		run := r["payload"].(map[string]any)["run_id"].(string)
		museEmitRunBounded(t, h, "started", run)
		owned := museOwnedRecord(h, r)
		owned["payload"].(map[string]any)["record"].(map[string]any)["child_task"].(map[string]any)["task_id"] = museTestUUID("never-observed-task")
		owned["payload"].(map[string]any)["record"].(map[string]any)["child_task"].(map[string]any)["task_stream"].(map[string]any)["id"] = museTestUUID("never-observed-task")
		appendMuseRecord(t, h, owned)
		assertMuseNoInput(t, h, home)
	})
	t.Run("terminal_unknown_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		r := museOwnedRecord(h, museCorpusRecord(t, "reminder.cleanup_effect.terminal", "reminder_cleanup", ""))
		r["payload"].(map[string]any)["run_id"] = museTestUUID("never-observed-run")
		appendMuseRecord(t, h, r)
		assertMuseNoInput(t, h, home)
	})
}
