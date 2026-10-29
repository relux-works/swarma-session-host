package sessionplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func (h *testHost) ValidateResume(context.Context, string) error { return nil }
func (h *testHost) Resume(_ context.Context, id string, _ Request) (HostSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.sessions[id]
	if !ok {
		return HostSession{}, Fail("session_resume_not_found")
	}
	h.starts++
	s.State = "unknown"
	h.sessions[id] = s
	return s, nil
}
func (h *testHost) Stop(_ context.Context, id, mode string) (HostSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.sessions[id]
	s.State = "down"
	h.sessions[id] = s
	return s, nil
}
func (h *testHost) StopNotBusy(_ context.Context, id, mode string) (HostSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.sessions[id]
	if s.State != "idle" {
		return HostSession{}, Fail("werkstatt_busy", map[string]string{"state": "busy"})
	}
	s.State = "down"
	h.sessions[id] = s
	return s, nil
}
func (h *testHost) Attach(_ context.Context, id string) (json.RawMessage, error) {
	return json.RawMessage(`{"native":"` + id + `"}`), nil
}
func resumeRequest(launch map[string]any, session string, n int) map[string]any {
	q := request(n)
	out := map[string]any{}
	for _, key := range []string{"operation_id", "submitted_at", "admission_key", "runtime", "profile", "credential_source", "doorbell", "detach"} {
		out[key] = q[key]
	}
	out["selector"] = map[string]string{"kind": "handle", "handle": session}
	out["scope"] = map[string]any{"cwd": launch["cwd"], "labels": launch["labels"]}
	return out
}

// Missing/scoped/profile refusals do not reserve or start; a valid resume reuses
// the handle/target and retains one list row while advancing its generation.
func TestResumeSelectionAndNoFreshFallback(t *testing.T) {
	e, h := testEngine(t)
	missing := request(1)
	expired := resumeRequest(missing, "SES-missing", 3)
	expired["submitted_at"] = testClock.Add(-8 * 24 * time.Hour).Format(time.RFC3339)
	rejected(t, e, "resume", expired, "session_operation_expired")
	rejected(t, e, "resume", resumeRequest(missing, "SES-missing", 2), "session_resume_not_found")
	if _, err := os.Lstat(filepath.Join(e.Dir, "ledger.json")); !os.IsNotExist(err) {
		t.Fatal("missing resume initialized a ledger")
	}
	q := request(1)
	q["cwd"] = t.TempDir()
	q["detach"] = false
	first := decode[Record](t, call(t, e, "launch", q))
	call(t, e, "stop", map[string]any{"session": first.Session, "expected_generation": 1, "mode": "force", "precondition": "none"})
	path := filepath.Join(e.Dir, "ledger.json")
	before, _ := os.ReadFile(path)
	for _, tc := range []struct {
		name, code string
		change     func(map[string]any)
	}{
		{"missing", "session_resume_not_found", func(q map[string]any) { q["selector"] = map[string]string{"kind": "handle", "handle": "SES-missing"} }},
		{"cwd", "session_resume_location_mismatch", func(q map[string]any) { q["scope"].(map[string]any)["cwd"] = "/elsewhere" }},
		{"profile", "session_resume_identity_mismatch", func(q map[string]any) { q["profile"].(map[string]any)["name"] = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := resumeRequest(q, first.Session, 2)
			tc.change(r)
			rejected(t, e, "resume", r, tc.code)
			after, _ := os.ReadFile(path)
			if string(after) != string(before) || h.starts != 1 {
				t.Fatal("refused resume changed ledger or started provider")
			}
		})
	}
	r := resumeRequest(q, first.Session, 2)
	r["detach"] = false
	next := decode[Record](t, call(t, e, "resume", r))
	if next.Session != first.Session || *next.Generation != 2 || h.starts != 2 {
		t.Fatal("resume did not reuse the admitted session")
	}
	call(t, e, "resume", r)
	if h.starts != 2 {
		t.Fatal("resume replay started twice")
	}
	r["detach"] = true
	rejected(t, e, "resume", r, "session_operation_conflict")
	page := call(t, e, "list", map[string]any{}).(map[string]any)
	if len(page["sessions"].([]Record)) != 1 {
		t.Fatal("resume duplicated list membership")
	}
	rejected(t, e, "stop", map[string]any{"session": first.Session, "expected_generation": 1, "mode": "force", "precondition": "none"}, "session_generation_mismatch")
}

// Busy/unknown not-busy refusals leave ledger and host intact; idle is the
// controlled observer positive case. The provider rechecks idle atomically.
func TestStopPreconditionNoEffects(t *testing.T) {
	e, h := testEngine(t)
	r := decode[Record](t, call(t, e, "launch", request(1)))
	for _, state := range []string{"busy", "unknown"} {
		h.mu.Lock()
		v := h.sessions["1"]
		v.State = state
		h.sessions["1"] = v
		h.mu.Unlock()
		before, _ := os.ReadFile(filepath.Join(e.Dir, "ledger.json"))
		rejected(t, e, "stop", map[string]any{"session": r.Session, "expected_generation": 1, "mode": "graceful", "precondition": "not-busy"}, "werkstatt_busy")
		after, _ := os.ReadFile(filepath.Join(e.Dir, "ledger.json"))
		if string(after) != string(before) || h.sessions["1"].State != state {
			t.Fatal("precondition refusal had effects")
		}
	}
	h.mu.Lock()
	v := h.sessions["1"]
	v.State = "idle"
	h.sessions["1"] = v
	h.mu.Unlock()
	stopped := decode[Record](t, call(t, e, "stop", map[string]any{"session": r.Session, "expected_generation": 1, "mode": "force", "precondition": "not-busy"}))
	if stopped.State != "down" {
		t.Fatal("idle control did not stop")
	}
}

// Attach checks generation and detached eligibility before asking the host;
// only an eligible current generation can obtain private attachment data.
func TestAttachGenerationAndDetached(t *testing.T) {
	for _, detached := range []bool{false, true} {
		e, _ := testEngine(t)
		q := request(1)
		q["detach"] = detached
		r := decode[Record](t, call(t, e, "launch", q))
		rejected(t, e, "attach", map[string]any{"session": r.Session, "expected_generation": 2}, "session_generation_mismatch")
		a := map[string]any{"session": r.Session, "expected_generation": 1}
		if detached {
			rejected(t, e, "attach", a, "session_host_unavailable")
			continue
		}
		b, _ := json.Marshal(a)
		name, data, err := e.Handle(context.Background(), "attach", b)
		if err != nil || name != "attachment" || len(data.(json.RawMessage)) == 0 {
			t.Fatalf("attach positive control: %s %v", name, err)
		}
	}
}

// Canonical cwd aliases and label subsets retain scope boundaries; latest uses
// the current generation timestamp, while a removed provider never falls back.
func TestResumeScopedLatestAndRetired(t *testing.T) {
	e, h := testEngine(t)
	cwd := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(cwd, alias); err != nil {
		t.Fatal(err)
	}
	var second Record
	for n := 1; n <= 2; n++ {
		e.Now = func() time.Time { return testClock.Add(time.Duration(n) * time.Second) }
		q := request(n)
		q["cwd"] = cwd
		q["labels"] = map[string]string{"group": "yes", "member": fmt.Sprint(n)}
		row := decode[Record](t, call(t, e, "launch", q))
		call(t, e, "stop", map[string]any{"session": row.Session, "expected_generation": 1, "mode": "force", "precondition": "none"})
		second = row
	}
	q := request(2)
	q["cwd"] = alias
	q["labels"] = map[string]string{"group": "yes"}
	r := resumeRequest(q, second.Session, 3)
	r["selector"] = map[string]string{"kind": "latest"}
	row := decode[Record](t, call(t, e, "resume", r))
	if row.Session != second.Session || *row.Generation != 2 {
		t.Fatal("latest crossed scope or ignored generation ordering")
	}
	call(t, e, "stop", map[string]any{"session": row.Session, "expected_generation": 2, "mode": "force", "precondition": "none"})
	h.mu.Lock()
	delete(h.sessions, "2")
	h.mu.Unlock()
	before, _ := os.ReadFile(filepath.Join(e.Dir, "ledger.json"))
	rejected(t, e, "resume", resumeRequest(q, row.Session, 4), "session_resume_unreadable")
	after, _ := os.ReadFile(filepath.Join(e.Dir, "ledger.json"))
	if string(before) != string(after) || h.starts != 3 {
		t.Fatal("retired resume wrote ledger or launched")
	}
}

type blockedResumeHost struct {
	*testHost
	entered, release chan struct{}
}

func (h *blockedResumeHost) Resume(ctx context.Context, id string, q Request) (HostSession, error) {
	row, err := h.testHost.Resume(ctx, id, q)
	if err != nil {
		return HostSession{}, err
	}
	close(h.entered)
	select {
	case <-h.release:
	case <-ctx.Done():
		return HostSession{}, Uncertain{}
	}
	return row, nil
}

// A physical conversation has one reserved generation even without an admission
// key. During native resume, a distinct start and stale attach/stop refuse;
// the published current generation remains controllable.
func TestResumeConcurrentWithoutAdmissionKey(t *testing.T) {
	e, h := testEngine(t)
	q := request(1)
	row := decode[Record](t, call(t, e, "launch", q))
	call(t, e, "stop", map[string]any{"session": row.Session, "expected_generation": 1, "mode": "force", "precondition": "none"})
	host := &blockedResumeHost{testHost: h, entered: make(chan struct{}), release: make(chan struct{})}
	e.Host = host
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	finished := make(chan struct{})
	defer func() {
		close(host.release)
		cancel()
		select {
		case <-finished:
		case <-time.After(60 * time.Second):
			t.Error("resume worker survived its guard")
		}
	}()
	raw, _ := json.Marshal(resumeRequest(q, row.Session, 2))
	go func() { defer close(finished); _, _, err := e.Handle(ctx, "resume", raw); done <- err }()
	select {
	case <-host.entered:
	case <-time.After(60 * time.Second):
		t.Fatal("resume did not enter provider")
	}
	rejected(t, e, "resume", resumeRequest(q, row.Session, 3), "werkstatt_already_up")
	for _, op := range []string{"stop", "attach"} {
		control := map[string]any{"session": row.Session, "expected_generation": 1}
		if op == "stop" {
			control["mode"] = "force"
			control["precondition"] = "none"
		}
		rejected(t, e, op, control, "session_generation_mismatch")
	}
	h.mu.Lock()
	state := h.sessions["1"].State
	h.mu.Unlock()
	if state != "unknown" {
		t.Fatal("old generation controlled the resumed provider")
	}
	// Release before waiting, and leave deferred close responsible for only one close.
	host.release <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("resume did not finish")
	}
	if h.starts != 2 {
		t.Fatal("concurrent resume started twice")
	}
	call(t, e, "stop", map[string]any{"session": row.Session, "expected_generation": 2, "mode": "force", "precondition": "none"})
	if h.sessions["1"].State != "down" {
		t.Fatal("current generation stop control failed")
	}
}

// A persisted resume reservation cannot replace its original admitted binding.
// The ordinary resume control succeeds before the corrupt-index counterexample.
func TestResumeBindingCannotBeRewritten(t *testing.T) {
	e, _ := testEngine(t)
	q := request(1)
	row := decode[Record](t, call(t, e, "launch", q))
	call(t, e, "stop", map[string]any{"session": row.Session, "expected_generation": 1, "mode": "force", "precondition": "none"})
	r := resumeRequest(q, row.Session, 2)
	call(t, e, "resume", r)
	if err := e.transaction(func(s *ledger) (bool, error) {
		s.Operations[r["operation_id"].(string)].Resume.Record.Generation = nil
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]any{"session": row.Session})
	_, _, err := e.Handle(context.Background(), "status", b)
	var refusal Refusal
	if !errors.As(err, &refusal) || refusal.Code != "session_host_unavailable" || refusal.Details["reason"] != "index_unreadable" {
		t.Fatalf("rewritten binding accepted: %v", err)
	}
}

type changedActivityHost struct {
	*testHost
	conditionalCalls int
}

func (h *changedActivityHost) StopNotBusy(context.Context, string, string) (HostSession, error) {
	h.conditionalCalls++
	return HostSession{}, Fail("werkstatt_busy", map[string]string{"state": "busy"})
}

// An idle projection cannot authorize stop if the provider's atomic recheck
// refuses. The ledger and provider stay intact; the nearby idle test accepts.
func TestStopUsesAtomicProviderCheck(t *testing.T) {
	e, base := testEngine(t)
	h := &changedActivityHost{testHost: base}
	e.Host = h
	row := decode[Record](t, call(t, e, "launch", request(1)))
	base.mu.Lock()
	s := base.sessions["1"]
	s.State = "idle"
	base.sessions["1"] = s
	base.mu.Unlock()
	before, _ := os.ReadFile(filepath.Join(e.Dir, "ledger.json"))
	rejected(t, e, "stop", map[string]any{"session": row.Session, "expected_generation": 1, "mode": "graceful", "precondition": "not-busy"}, "werkstatt_busy")
	after, _ := os.ReadFile(filepath.Join(e.Dir, "ledger.json"))
	if h.conditionalCalls != 1 || string(before) != string(after) || base.sessions["1"].State != "idle" {
		t.Fatal("stale projection bypassed atomic provider check")
	}
}
