package musepty

import (
	"testing"
)

// Every joined enum identity refuses "" and a malformed exemplar at
// decode. Each subtest configures AND cites the same value, so a
// mutant admitting the exemplar past the format matcher resolves the
// join and delivers: r13-empty-context-id-only (""),
// r14-context-block-only ("never-configured-block"),
// r13-empty-roster-id-only (""),
// r14-reminder-agent-only ("never-configured-reminder"),
// r14-response-tag-only ("BAD TAG!"), r14-idempotency-key-only
// ("not-a-key!").
func TestMusePTYR14EnumFormatMustRefuse(t *testing.T) {
	t.Run("context_empty", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museR14ConfiguredCitation(t, h, "context", "", "")
		museR9Refuse(t, h, home)
	})
	t.Run("context_exemplar", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museR14ConfiguredCitation(t, h, "context", "never-configured-block", "never-configured-block")
		museR9Refuse(t, h, home)
	})
	t.Run("roster_empty", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museR14ConfiguredCitation(t, h, "roster", "", "")
		museR9Refuse(t, h, home)
	})
	t.Run("roster_exemplar", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museR14ConfiguredCitation(t, h, "roster", "never-configured-reminder", "never-configured-reminder")
		museR9Refuse(t, h, home)
	})
	for _, c := range []struct{ name, value string }{{"empty", ""}, {"exemplar", "BAD TAG!"}} {
		t.Run("response_"+c.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			run := museTestUUID("r14-format-response-run")
			museEmitRunBounded(t, h, "started", run)
			museR10EmitCreated(t, h, run, c.value)
			museR10EmitMessage(t, h, run, c.value, museTestUUID("r14-format-message"), "echo: response text")
			museEmitRunBounded(t, h, "terminal", run)
			museR9Refuse(t, h, home)
		})
	}
	for _, c := range []struct{ name, value string }{{"empty", ""}, {"exemplar", "not-a-key!"}} {
		t.Run("idempotency_"+c.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			run := museTestUUID("r14-format-idempotency-run")
			task := museTestUUID("r14-format-idempotency-task")
			museEmitRunBounded(t, h, "started", run)
			for _, kind := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"} {
				r := museR7TaskRecord(t, h, kind, run, task)
				if kind == "scheduled" || kind == "side_effect_intent" {
					r["payload"].(map[string]any)["event"].(map[string]any)["idempotency_key"] = c.value
				}
				appendMuseRecord(t, h, r)
			}
			museEmitRunBounded(t, h, "terminal", run)
			museR9Refuse(t, h, home)
		})
	}
}

// TestMusePTYR14TestUUIDMapping pins the deterministic name -> UUID
// mapping for every value baked into a narrowing-mutant replacement.
// Mutant replacements inject into production files, so they cannot call
// the test helper; they bake these literals instead. If this test fails,
// the helper changed and every baked literal must be recomputed.
func TestMusePTYR14TestUUIDMapping(t *testing.T) {
	want := map[string]string{
		"other":                       "02e9046e-9853-4225-9990-66468f634af8",
		"foreign-session":             "a6d56b99-1f61-487b-8886-188c7ae6053e",
		"different-session":           "7cddd7e6-3233-4b90-9647-5c645673d522",
		"r7-run-reuse":                "af7ed83a-a897-413b-ac7e-8290f5d25ea4",
		"r7-task-run":                 "decf9b5f-18bc-4a32-b54b-d217636b6c23",
		"r7-task-order":               "a6600299-cb99-4110-9a26-24fae7c3e89d",
		"r6-cross-run-sentinel":       "97d7799e-f999-4e86-872a-44063cb003ba",
		"peer-session":                "fc777faf-c959-4fcd-b2c7-c35044561e5b",
		"intent-B":                    "b939d73d-eb55-4b9c-9663-13b0109cffe9",
		"r8-link-sentinel":            "1597ad8e-20a9-4765-a06c-3ae848e88366",
		"r8-drain-causation":          "d7f0fed2-0789-4e4c-b1eb-92b6c5618e7d",
		"r9-response-sentinel":        "848d4966-8a65-4c70-9c54-4300267e22c3",
		"r9-message-dup-sentinel":     "47971786-2f58-40f8-a373-993f6ac5c873",
		"r9-reasoning-sentinel":       "82a6e63b-90aa-4000-94dd-6f0168ba65ec",
		"r9-mirror-sentinel":          "9ada03b3-7b8a-40e2-a468-2c0c0442d98e",
		"r9-cleanup-sentinel":         "dbae1e6d-3700-42da-a713-4d21012e1b8b",
		"r9-cleanup-epoch-sentinel":   "3fbe87c0-22b2-4d84-bd2a-737832d14a39",
		"r9-envelope-sentinel":        "153252fa-01fd-4df7-8e55-20bbfecd9c4f",
		"r9-started-refs-sentinel":    "51d5d0bf-6fe9-4f15-bb58-a3b79c424dfd",
		"r9-top-level-sentinel":       "601dcecc-dc18-4275-b0f5-268d08646079",
		"r9-top-level-order-sentinel": "4e5c551c-fbe6-4f27-8195-742d0614a9e1",
		"r9-delivery-sentinel":        "3f2f2569-457f-4149-8f43-ef7b247c5f59",
		"r9-delivery-target":          "c274951c-755f-4489-9b82-b120d6fd2a54",
		"r10-stale-sentinel":          "940dee65-9bb5-493c-a18a-79f4baf8a1df",
		"unrelated-run":               "345d1a60-808f-4d3a-910e-77b703c27c8b",
		"unrelated-intent":            "2ad3c02b-356f-4b0b-93b7-d844053d525e",
		"r10-idempotency-sentinel":    "864aae15-58b3-4dad-b916-c85c32f3f136",
		"r10-response-sentinel":       "99073490-7808-43a2-b632-c130539f0aa6",
		"active-run":                  "de89b9cd-f7dd-4f64-aa85-7857b921972a",
		"racing-run":                  "6e1140bb-c21f-449f-8c05-bce8c6d3df0f",
		"shared-start-envelope":       "46e38642-2027-46e0-9e34-a95d30c1b7f5",
		"r12-intake-pending-sentinel": "09c41ca3-0b9c-4388-b954-cdaf4f298122",
		"never-observed-prior-record": "80bff5f2-33fb-4224-8840-c615477a13d7",
		"r12-resume-epoch-sentinel":   "8f93330f-6d8a-46f5-b974-3e5104086d2a",
		"r13-epoch-sentinel":          "b7340aa1-dd64-4fc5-a245-7648700d5fac",
		"never-configured-request":    "d9b99aa3-eae0-4346-9e95-1463342131a7",
		"different-command":           "2a8b4630-1511-409e-9522-b3d5d42c7922",
		"r11-intake-settled-dup":      "9c2c2e85-593a-4dff-9b59-a615cfa5d0e8",
		"r11-intake-received-dup":     "fb1fceb2-249c-4e08-b55d-323d711ce67d",
		"unobserved-foreign-target":   "63f0a28d-25de-41b9-a4c5-0a42376dcc31",
		"never-observed-run":          "9841204f-3188-4ee3-8b07-bf17d2bc7345",
		"queue-run":                   "9b0f2bbd-592c-4121-ae6d-8810168ae219",
	}
	for name, id := range want {
		if got := museTestUUID(name); got != id {
			t.Errorf("museTestUUID(%q) = %s, want %s", name, got, id)
		}
		if !museWellFormedUUID(id) {
			t.Errorf("baked uuid %s for %q is malformed", id, name)
		}
	}
}

// Every joined UUID identity refuses "" (re-targeted r13-empty-id-only
// admits exactly ""), "   " (r14-uuid-blank-only) and a malformed
// value (r14-uuid-malformed-only admits exactly "not-a-uuid") at
// decode. Ports panel A TestPanelR13WhitespaceEnvelopeMustRefuse and
// TestPanelR13WhitespaceStartedSourceMustResolve verbatim.
func TestMusePTYR14IdentityFormatMustRefuse(t *testing.T) {
	envelope := func(t *testing.T, h *musePTYHost, id string, seq int) {
		t.Helper()
		// Informational record: under a mutant admitting the id the
		// log stays idle and delivers, so the subtest flips.
		s := museOwnedRecord(h, museCorpusRecord(t, "session.name.changed", "", ""))
		s["id"] = id
		s["sequence"] = seq
		appendMuseRecord(t, h, s)
	}
	startedSource := func(t *testing.T, h *musePTYHost, source string) {
		t.Helper()
		run := museTestUUID("r14-format-source-run")
		s := museOwnedRecord(h, museFixtureRecord("started"))
		s["payload"].(map[string]any)["run_id"] = run
		s["payload"].(map[string]any)["source_run_record_id"] = source
		appendMuseRecord(t, h, s)
		a := museR9AcceptIntent(t, h, run)
		m := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.materialized", "", ""))
		p := m["payload"].(map[string]any)
		p["intent_id"] = run
		p["envelope_record_id"] = a["id"]
		p["envelope_sequence"] = a["sequence"]
		out := p["outcome"].(map[string]any)
		out["run_id"] = run
		out["run_started_session_record_id"] = s["id"]
		out["run_started_source_record_id"] = source
		appendMuseRecord(t, h, m)
		museEmitRunBounded(t, h, "terminal", run)
		appendMuseRecord(t, h, museR9TopLevelOutcome(t, h, a, s))
	}
	cases := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"whitespace", "   "},
		{"malformed", "not-a-uuid"},
	}
	for _, c := range cases {
		t.Run("envelope_"+c.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			envelope(t, h, c.value, 424243)
			museR9Refuse(t, h, home)
		})
		t.Run("started_source_"+c.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			startedSource(t, h, c.value)
			museR9Refuse(t, h, home)
		})
	}
}
