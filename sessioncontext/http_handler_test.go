package sessioncontext

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func contextRequest(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func testContextHandler(t *testing.T) (*Service, http.Handler) {
	t.Helper()
	m := newMaterializationManager(t, testLayout(t, "http-context"), newFakeMaterializationAuthority())
	h, e := NewHTTPHandler(m)
	if e != nil {
		t.Fatal(e)
	}
	return m, h
}

// The original context routes retain their exact methods, public envelope and
// safe read shapes; no listener, provider, daemon or outer-auth claim is made.
func TestContextHTTPOriginalReadRoutesAndUnknownMethods(t *testing.T) {
	_, h := testContextHandler(t)
	for _, test := range []struct{ method, path, body, key string }{{"GET", "/v1/contexts/materializations", "", "materializations"}, {"POST", "/v1/contexts/economics", "{}", "contract_version"}, {"POST", "/v1/contexts/completeness", "{}", "contract_version"}, {"POST", "/v1/contexts/reclaim", `{"dry_run":true}`, "dry_run"}, {"POST", "/v1/contexts/sweep", `{"dry_run":true}`, "dry_run"}} {
		w := contextRequest(t, h, test.method, test.path, test.body)
		if w.Code != 200 || w.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("original read route %s: %d %s", test.path, w.Code, w.Body.String())
		}
		var out map[string]json.RawMessage
		if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil || out[test.key] == nil {
			t.Fatalf("original route result key %s missing: %s %v", test.key, w.Body.String(), e)
		}
	}
	for _, test := range []struct{ method, path string }{{"POST", "/v1/contexts/materializations"}, {"GET", "/v1/contexts/economics"}, {"POST", "/v1/contexts/retention/sweep"}, {"POST", "/v1/contexts/missing"}} {
		w := contextRequest(t, h, test.method, test.path, "{}")
		if w.Code != 404 || w.Body.String() != "{\"error\":{\"code\":\"not_found\",\"message\":\"control route was not found\"}}\n" {
			t.Fatalf("unknown route/method changed: %d %s", w.Code, w.Body.String())
		}
	}
}

// All request-bearing routes reject malformed, unknown, trailing and oversized
// input before service effects. A raw body owner/UID never becomes a principal.
func TestContextHTTPStrictInputBeforeServiceEffects(t *testing.T) {
	m, h := testContextHandler(t)
	routes := []string{"reclaim", "sweep", "economics", "completeness", "materializations/prepare", "materializations/complete", "materializations/fail", "builder-gateway/finalize"}
	for _, route := range routes {
		for _, body := range []string{"{", `{"owner":"privileged","uid":0}`, "{} {}", "{} " + strings.Repeat(" ", 1<<20)} {
			w := contextRequest(t, h, "POST", "/v1/contexts/"+route, body)
			if w.Code != 400 || !strings.Contains(w.Body.String(), `"code":"invalid_request"`) {
				t.Fatalf("strict input reached %s: %d %s", route, w.Code, w.Body.String())
			}
			if len(m.ListMaterializations()) != 0 {
				t.Fatal("invalid HTTP body changed catalog")
			}
		}
	}
}

// Operational errors keep original fixed route messages; private storage or
// authority details cannot be copied into a wire refusal.
func TestContextHTTPPrivateFailuresUseOriginalFixedRefusals(t *testing.T) {
	m, h := testContextHandler(t)
	a := m.materializationAuthority.(*fakeMaterializationAuthority)
	a.err = errors.New("private-token-and-provider-path")
	w := contextRequest(t, h, "POST", "/v1/contexts/reclaim", "{}")
	want := "{\"error\":{\"code\":\"context_reclamation_failed\",\"message\":\"board-owned context payload reclamation failed\"}}\n"
	if w.Code != 409 || w.Body.String() != want {
		t.Fatalf("private failure escaped original envelope: %d %s", w.Code, w.Body.String())
	}
	if _, e := NewHTTPHandler(nil); e == nil {
		t.Fatal("nil service became a supported HTTP runtime")
	}
}

// An authenticated outer transport must retain its request context all the
// way to the actual source authority; body fields supply no substitute.
type contextMarkerKey struct{}
type requestContextAuthority struct {
	*fakeMaterializationAuthority
	seen bool
}

// Authority-only confinement survives isolated copies in both directions; the
// canonical HTTP body cannot supply that runtime fact or change the catalog.
func TestContextAuthorityConfinementSurvivesCloneButNotWire(t *testing.T) {
	for _, required := range []bool{true, false} {
		r := materializationTestContext("CTX-260729-c04001", true)
		r.RequiresConfinement = required
		c := cloneContext(r)
		if c.RequiresConfinement != required {
			t.Fatalf("verified runtime confinement lost: want %v got %v", required, c.RequiresConfinement)
		}
		c.Binding.ProducerRole = "changed"
		if r.Binding.ProducerRole == "changed" {
			t.Fatal("clone aliases authority facts")
		}
	}
	m, h := testContextHandler(t)
	w := contextRequest(t, h, "POST", "/v1/contexts/materializations/prepare", `{"context":{"id":"CTX-260729-c04001","requires_confinement":true}}`)
	if w.Code != 400 || len(m.ListMaterializations()) != 0 {
		t.Fatalf("body supplied runtime confinement: %d %s", w.Code, w.Body.String())
	}
}
func (a *requestContextAuthority) Context(ctx context.Context, id string) (*sm.ContextRecord, error) {
	a.seen = ctx.Value(contextMarkerKey{}) == "actual-request"
	return a.fakeMaterializationAuthority.Context(ctx, id)
}

// Gateway finalization carries the real authenticated transport context to the
// source authority; no background context or body-created identity substitutes.
func TestContextHTTPFinalizeRetainsActualRequestContext(t *testing.T) {
	r := materializationTestContext("CTX-260729-c04002", true)
	r.SecretClass = "unrestricted"
	a := &requestContextAuthority{fakeMaterializationAuthority: newFakeMaterializationAuthority(r)}
	m := newMaterializationManager(t, testLayout(t, "request-context"), a)
	if _, e := m.CreateMaterialization(context.Background(), CreateMaterializationRequest{Context: r, RunProvider: "codex", TargetElementID: r.Binding.ElementID, TargetRole: "developer", Source: MaterializationSourceBuilt, ProviderHost: sm.ProviderHostRecord{Provider: "codex"}}); e != nil {
		t.Fatal(e)
	}
	a.seen = false
	h, e := NewHTTPHandler(m)
	if e != nil {
		t.Fatal(e)
	}
	req := httptest.NewRequest("POST", "/v1/contexts/builder-gateway/finalize", strings.NewReader(`{"ctx_id":"`+r.ID+`"}`)).WithContext(context.WithValue(context.Background(), contextMarkerKey{}, "actual-request"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || !a.seen {
		t.Fatalf("gateway discarded actual request context: status=%d seen=%v body=%s", w.Code, a.seen, w.Body.String())
	}
}

// Original canonical snake_case input traverses the actual durable service.
// Wrong holder/completion requests preserve the exact leased image; an admitted
// fail or observed transcript completion releases only that operation's child.
func TestContextHTTPMaterializationLifecycleAndExactLease(t *testing.T) {
	for _, finish := range []string{"complete", "fail"} {
		t.Run(finish, func(t *testing.T) {
			r := materializationTestContext("CTX-260729-c04301", true)
			r.SecretClass = "unrestricted"
			r.Lineage.ProducedByRunID = "RUN-http-child"
			a := newFakeMaterializationAuthority(r)
			a.runs[r.Lineage.ProducedByRunID] = &sm.RunEvidence{ID: r.Lineage.ProducedByRunID, ContextID: r.ID, Owner: sm.OwnerKey(r.Binding.ElementID), Status: "running"}
			m := newMaterializationManager(t, testLayout(t, "http-lifecycle"), a)
			h, e := NewHTTPHandler(m)
			if e != nil {
				t.Fatal(e)
			}
			q := PrepareRunMaterializationRequest{Context: r, TargetElementID: r.Binding.ElementID, TargetRole: "developer", RunID: r.Lineage.ProducedByRunID, Mode: "explicit"}
			body, e := json.Marshal(q)
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Contains(body, []byte(`"produced_by_run_id"`)) {
				t.Fatal("original canonical lineage key absent")
			}
			// The same input with inactive actual source facts refuses before writes.
			a.runs[r.Lineage.ProducedByRunID].Status = "failed"
			refused := contextRequest(t, h, "POST", "/v1/contexts/materializations/prepare", string(body))
			if refused.Code != 409 || len(m.ListMaterializations()) != 0 {
				t.Fatal("inactive source created materialization")
			}
			a.runs[r.Lineage.ProducedByRunID].Status = "running"
			w := contextRequest(t, h, "POST", "/v1/contexts/materializations/prepare", string(body))
			if w.Code != 200 {
				t.Fatalf("canonical prepare: %d %s", w.Code, w.Body.String())
			}
			before := catalogBytes(t, m.layout, r.ID)
			wrong := contextRequest(t, h, "POST", "/v1/contexts/materializations/"+finish, `{"ctx_id":"`+r.ID+`","run_id":"RUN-foreign"}`)
			if wrong.Code != 409 || !bytes.Equal(before, catalogBytes(t, m.layout, r.ID)) {
				t.Fatal("wrong holder changed durable lease")
			}
			view := contextRequest(t, h, "GET", "/v1/contexts/materializations", "")
			if view.Code != 200 || !strings.Contains(view.Body.String(), `"active_run_id":"RUN-http-child"`) || strings.Contains(view.Body.String(), "provider_host") {
				t.Fatal("safe materialization view lost lease or leaked snapshot")
			}
			if finish == "complete" {
				home := t.TempDir()
				dir := filepath.Join(home, "sessions")
				if e = os.MkdirAll(dir, 0700); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(filepath.Join(dir, "rollout-http-thread.jsonl"), []byte("{\"type\":\"response_item\",\"payload\":{}}\n"), 0600); e != nil {
					t.Fatal(e)
				}
				host := testMaterializationHost()
				host.Snapshot.Codex.CodexHome = home
				host.Snapshot.Codex.ThreadID = "http-thread"
				host.Snapshot.Codex.LastCompletedTurnID = "actual-test-observation"
				if e = m.RecordProviderHost(r.ID, r.Lineage.ProducedByRunID, host); e != nil {
					t.Fatal(e)
				}
			}
			result := contextRequest(t, h, "POST", "/v1/contexts/materializations/"+finish, `{"ctx_id":"`+r.ID+`","run_id":"RUN-http-child","published":false}`)
			if result.Code != 200 {
				t.Fatalf("admitted %s: %d %s", finish, result.Code, result.Body.String())
			}
			state := m.ListMaterializations()[0]
			if state.ActiveRunID != "" || state.State == MaterializationLeased || a.contexts[r.ID].State != "retired" {
				t.Fatal("settled HTTP operation retained writer or source child")
			}
		})
	}
}

func readContextFeedback(t *testing.T, h http.Handler, id string) (*ContextCompletenessFeedback, error) {
	t.Helper()
	w := contextRequest(t, h, "POST", "/v1/contexts/completeness", `{"ctx_id":"`+id+`"}`)
	if w.Code != 200 {
		t.Fatalf("completeness route: %d %s", w.Code, w.Body.String())
	}
	var out ContextCompletenessFeedback
	e := json.Unmarshal(w.Body.Bytes(), &out)
	return &out, e
}

// Original F2 route assertions are retained, with original measurement-shaped
// rows supplied at the actual durable telemetry log and the concrete handler.
// Native stream collection, private Client/auth/server wiring remain separate.
func TestTheCompletenessAggregateIsReachableFromTheControlSurface(t *testing.T) {
	m, h := testContextHandler(t)
	const telemetryBaseCtxID = "CTX-260728-a1b2c3"
	const telemetryOtherBaseCtxID = "CTX-260728-d4e5f6"
	for i := 0; i < 24; i++ {
		role := "developer"
		if i%2 == 1 {
			role = "reviewer"
		}
		row := feedbackRow(role, i < 10, 0, nil, m.now())
		if i < 10 {
			row.ExtraReads = 2
			row.OutsidePrefixes = []string{"internal"}
		}
		row.CtxID = telemetryBaseCtxID
		if e := m.forkTelemetry.append(row, m.now()); e != nil {
			t.Fatal(e)
		}
	}
	for i := 0; i < 2; i++ {
		row := unobservableRow(m.now())
		row.CtxID = telemetryBaseCtxID
		if e := m.forkTelemetry.append(row, m.now()); e != nil {
			t.Fatal(e)
		}
	}
	for i := 0; i < 3; i++ {
		row := feedbackRow("developer", false, 0, nil, m.now())
		row.CtxID = telemetryOtherBaseCtxID
		if e := m.forkTelemetry.append(row, m.now()); e != nil {
			t.Fatal(e)
		}
	}
	feedback, err := readContextFeedback(t, h, telemetryBaseCtxID)
	if err != nil {
		t.Fatal(err)
	}
	if feedback.ContractVersion != ContextCompletenessFeedbackContract {
		t.Fatalf("control-plane feedback contract = %q", feedback.ContractVersion)
	}
	if feedback.Blocking {
		t.Fatalf("the operator read presented completeness as blocking")
	}
	if feedback.ObservableForks != 24 || feedback.UnavailableForks != 2 {
		t.Fatalf("observable=%d unavailable=%d, want 24 and 2 over the wire",
			feedback.ObservableForks, feedback.UnavailableForks)
	}
	if !feedback.SampleSufficient {
		t.Fatalf("24 observable forks did not reach the warning threshold")
	}
	if len(feedback.Warnings) == 0 {
		t.Fatalf("the economic warning did not survive the control route: %+v", feedback)
	}
	if len(feedback.Widening) != 1 || feedback.Widening[0].Prefix != "internal" {
		t.Fatalf("widening recommendation over the wire = %+v", feedback.Widening)
	}
	if feedback.Widening[0].AutoApplied {
		t.Fatalf("a widening recommendation arrived marked auto-applied")
	}
	if feedback.CtxID != telemetryBaseCtxID {
		t.Fatalf("the route answered for %q", feedback.CtxID)
	}

	// The same surface, asked about the other base. Its rows are a different
	// population — a small, quiet sample — and it has to come back as itself
	// rather than as a share of the fold.
	other, err := readContextFeedback(t, h, telemetryOtherBaseCtxID)
	if err != nil {
		t.Fatalf("ContextCompleteness for the second base: %v", err)
	}
	if other.CtxID != telemetryOtherBaseCtxID {
		t.Fatalf("the route answered for %q when asked about the second base", other.CtxID)
	}
	if other.ObservableForks != 3 || other.UnavailableForks != 0 {
		t.Fatalf("second base observable=%d unavailable=%d, want 3 and 0",
			other.ObservableForks, other.UnavailableForks)
	}
	if other.SampleSufficient {
		t.Fatalf("three observable forks were reported as a sufficient sample")
	}
	if len(other.Warnings) != 0 || len(other.Widening) != 0 {
		t.Fatalf("the second base inherited the first one's findings: %+v", other)
	}
}

type httpRetentionAuthority struct {
	*fakeMaterializationAuthority
	id string
}

func (a httpRetentionAuthority) CollectContextRetention(context.Context, sm.RetentionPolicy) (*sm.RetentionCollection, error) {
	return &sm.RetentionCollection{Delete: []sm.ContextRetentionCollection{{CtxID: a.id, Provider: "codex", IncludeArchived: true}}}, nil
}

// Original reclamation route claim: actual HTTP -> service -> private live AND
// archived deletion, while an unrelated rollout survives. Core socket/auth and
// the old private CLI client are outside this handler fixture.
func TestReclamationIsReachableThroughTheControlPlane(t *testing.T) {
	m, a, r, _ := seededLease(t)
	home := t.TempDir()
	dir := filepath.Join(home, "sessions")
	archive := filepath.Join(home, "archived_sessions")
	for _, d := range []string{dir, archive} {
		if e := os.MkdirAll(d, 0700); e != nil {
			t.Fatal(e)
		}
	}
	live := filepath.Join(dir, "rollout-http-owned.jsonl")
	archived := filepath.Join(archive, "rollout-http-owned.jsonl")
	otherLive := filepath.Join(dir, "rollout-other-thread.jsonl")
	for _, p := range []string{live, archived, otherLive} {
		if e := os.WriteFile(p, []byte("payload"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	local := m.materializations[r.ID]
	local.State = MaterializationDetached
	local.ActiveRunID = ""
	local.LeaseAcquiredAt = time.Time{}
	local.ProviderHost.Snapshot.Codex.CodexHome = home
	local.ProviderHost.Snapshot.Codex.ThreadID = "http-owned"
	a.contexts[r.ID].State = "retired"
	m.materializationAuthority = httpRetentionAuthority{a, r.ID}
	h, e := NewHTTPHandler(m)
	if e != nil {
		t.Fatal(e)
	}
	w := contextRequest(t, h, "POST", "/v1/contexts/reclaim", "{}")
	if w.Code != 200 {
		t.Fatalf("ReclaimContexts over the control plane: %d %s", w.Code, w.Body.String())
	}
	var report ContextReclamationReport
	if e = json.Unmarshal(w.Body.Bytes(), &report); e != nil {
		t.Fatal(e)
	}
	if len(report.Outcomes) != 1 {
		t.Fatalf("missing actual reclamation outcome: %+v", report)
	}
	outcome := report.Outcomes[0]
	if outcome.Status != ContextReclamationDone || outcome.ArchivedFiles != 1 {
		t.Fatalf("the control-plane sweep did not reclaim the payload: %+v", outcome)
	}
	for _, p := range []string{live, archived} {
		if _, e = os.Stat(p); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("the rollout was not deleted through the operator route")
		}
	}
	if b, e := os.ReadFile(otherLive); e != nil || string(b) != "payload" {
		t.Fatal("an unrelated rollout changed")
	}
}
