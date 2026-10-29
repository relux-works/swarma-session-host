//go:build unix

package hostdefaults

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// These explicit in-process authority/native/budget fixtures qualify composition
// ordering, not a genuine external registrar, native provider or budget engine.
type goalRuntimeAuthority struct {
	facts  goal.VerifiedCaller
	before func() error
}

func (a *goalRuntimeAuthority) WithVerifiedCaller(_ context.Context, _ string, fn func(goal.VerifiedCaller) error) error {
	if a.before != nil {
		if err := a.before(); err != nil {
			return err
		}
	}
	return fn(a.facts)
}

type goalRuntimeBudget struct{}

func (goalRuntimeBudget) WithBudget(_ context.Context, _ core.ControllerLease, _ goal.Revision, fn func() error) error {
	return fn()
}

type goalRuntimeFactory struct {
	core.GoalBindingFactory
	binds int
}

func (f *goalRuntimeFactory) BindGoal(ctx context.Context, lease core.ControllerLease, provider string, writer core.HostInputWriter) (core.OwnerKey, core.GoalBinding, error) {
	f.binds++
	return f.GoalBindingFactory.BindGoal(ctx, lease, provider, writer)
}

type goalRuntimeCodec struct {
	*core.LegacyRecordCodec
	fail bool
}

var errGoalRuntimePersist = errors.New("isolated applied metadata persistence failure")

func (c *goalRuntimeCodec) EncodeMerge(ctx context.Context, base core.LegacyRecordInput, record core.RunnerRecord) ([]byte, error) {
	if c.fail && record.AppliedGoalRef != nil {
		return nil, errGoalRuntimePersist
	}
	return c.LegacyRecordCodec.EncodeMerge(ctx, base, record)
}

type goalRuntimeNative struct {
	snapshot core.RunnerSnapshot
	writes   int
}

func (n *goalRuntimeNative) State(context.Context) (core.ProviderHostState, error) {
	return core.ProviderHostRunning, nil
}
func (n *goalRuntimeNative) Snapshot() (core.RunnerSnapshot, error) { return n.snapshot, nil }
func (n *goalRuntimeNative) Stop(context.Context) error             { return nil }
func (n *goalRuntimeNative) SupportsNativeGoals() bool              { return true }
func (n *goalRuntimeNative) AcceptsInput(core.InputSubmission) bool { return true }
func (n *goalRuntimeNative) ObserveNativeGoal(context.Context) (core.NativeGoalObservation, error) {
	return core.NativeGoalObservation{Status: "completed", Sequence: 1}, nil
}
func (n *goalRuntimeNative) ExecuteInput(_ context.Context, in core.InputSubmission, receipt core.InputReceipt) (func(context.Context) (core.InputObservation, error), error) {
	n.writes++
	return func(context.Context) (core.InputObservation, error) {
		return core.InputObservation{Receipt: receipt, Kind: core.InputObservationKind(in.Expected.Kind), Correlation: in.Expected.Correlation}, nil
	}, nil
}

type goalRuntimeProvider struct{ native *goalRuntimeNative }

func (f goalRuntimeProvider) Provider() string { return "muse" }
func (f goalRuntimeProvider) Launch(context.Context, core.ProviderLaunchRequest) (core.RunnerHost, error) {
	return f.native, nil
}
func (f goalRuntimeProvider) Restore(context.Context, core.ProviderHostRecord) (core.RunnerHost, error) {
	return f.native, nil
}

type goalRuntimeFixture struct {
	host      *core.SessionHost
	service   *goal.Service
	authority *goalRuntimeAuthority
	factory   *goalRuntimeFactory
	codec     *goalRuntimeCodec
	native    *goalRuntimeNative
	runtime   goal.Protocol8Runtime
	id, root  string
	ref       core.RevisionRef
}

func newGoalRuntimeFixture(t *testing.T) *goalRuntimeFixture {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	a := &goalRuntimeAuthority{}
	s, err := goal.Open(filepath.Join(root, "goals"), a)
	if err != nil {
		t.Fatal(err)
	}
	c := &goalRuntimeCodec{LegacyRecordCodec: core.NewLegacyRecordCodec()}
	raw := materializationProviderFixture(t, "muse-pty")
	p, err := c.DecodeProviderHost(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	n := &goalRuntimeNative{snapshot: p.Snapshot}
	layout := core.Layout{Namespace: "fixture", StateDir: filepath.Join(root, "host"), SessionsDir: filepath.Join(root, "sessions"), LockFile: filepath.Join(root, "host", "manager.lock"), NoticesDir: filepath.Join(root, "notices")}
	notices, err := core.NewSessionNoticeStore(layout)
	if err != nil {
		t.Fatal(err)
	}
	f := &goalRuntimeFactory{GoalBindingFactory: goal.BindingFactory{Service: s, Budget: goalRuntimeBudget{}}}
	h, err := core.NewHost(core.HostOptions{Layout: layout, Config: FileConfig{Path: filepath.Join(root, "absent.json"), Optional: true, Version: "fixture", Namespace: layout.Namespace}, Codec: c, Notices: notices, Goals: s, GoalBindings: f, Factories: []core.ProviderHostFactory{goalRuntimeProvider{n}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	attachment, err := h.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if err != nil {
		t.Fatal(err)
	}
	id := attachment.Session.SessionID
	a.facts = goal.VerifiedCaller{SessionID: id, Principal: "launcher", Holder: "launcher", Generation: 1, Assignment: true}
	receipt, err := s.Handle(ctx, goal.Command{EnvelopeID: "source", SessionID: id, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "Preserve the session", Condition: "Holder explicitly accepts", Budget: goal.Budget{Tokens: 100}}})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewGoalRuntime(h, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &goalRuntimeFixture{h, s, a, f, c, n, runtime, id, root, receipt.Ref}
}

type observedGoalRuntime struct {
	goal.Protocol8Runtime
	err error
}

func (o *observedGoalRuntime) WithGoalSession(ctx context.Context, owner core.OwnerKey, fn func(goal.Protocol8GoalSession) error) error {
	return o.Protocol8Runtime.WithGoalSession(ctx, owner, func(s goal.Protocol8GoalSession) error {
		reconcile := s.Reconcile
		s.Reconcile = func() (core.SessionView, error) { v, e := reconcile(); o.err = e; return v, e }
		return fn(s)
	})
}
func runtimeHTTP(t *testing.T, f *goalRuntimeFixture, path string) *httptest.ResponseRecorder {
	t.Helper()
	handler, err := goal.NewProtocol8Handler(f.service, f.runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"owner":{"kind":"parent","id":"primary"}}`))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

// The real core scoped operation keeps Service's concrete receipt binding and
// commits applied metadata before the HTTP response is projected. Native done
// does not accept/clear the durable assignment.
func TestGoalRuntimeHTTPReconcileCommitsThenRefreshes(t *testing.T) {
	f := newGoalRuntimeFixture(t)
	observed := &observedGoalRuntime{Protocol8Runtime: f.runtime}
	f.runtime = observed
	w := runtimeHTTP(t, f, "/v1/owners/reconcile")
	view, _, live, err := f.host.OwnerSession(context.Background(), "parent/primary")
	head, headErr := f.service.Snapshot(f.id, goal.Assignment)
	if w.Code != 200 || err != nil || !live || view.AppliedGoalRef == nil || *view.AppliedGoalRef != f.ref || headErr != nil || head.Latest.State != goal.Active || head.Latest.Acceptance != nil || f.native.writes != 1 {
		t.Fatalf("real reconcile lost: %d %s %+v %v %+v %v writes=%d operation=%v", w.Code, w.Body, view, err, head, headErr, f.native.writes, observed.err)
	}
	var response struct {
		Session struct {
			Applied *core.RevisionRef `json:"applied_goal_ref"`
		} `json:"session"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Session.Applied == nil || *response.Session.Applied != f.ref {
		t.Fatalf("response preceded commit: %s %v", w.Body, err)
	}
}

// Missing/foreign/stale holder proof cannot bind, hydrate, write native input
// or alter journal/session bytes. The actual valid reconcile above is control.
func TestGoalRuntimeHTTPAuthorityBeforeBinding(t *testing.T) {
	for _, kind := range []string{"missing", "foreign", "old-holder", "no-grant"} {
		t.Run(kind, func(t *testing.T) {
			f := newGoalRuntimeFixture(t)
			switch kind {
			case "missing":
				f.authority.before = func() error { return goal.ErrAuthorityUnavailable }
			case "foreign":
				f.authority.facts.SessionID = "another"
			case "old-holder":
				f.authority.facts.Holder = "replacement"
				f.authority.facts.Generation = 2
			case "no-grant":
				f.authority.facts.Assignment = false
			}
			before, _ := os.ReadFile(filepath.Join(f.root, "goals", "journal.json"))
			recordBefore, _ := os.ReadFile(filepath.Join(f.root, "sessions", f.id+".json"))
			binds := f.factory.binds
			w := runtimeHTTP(t, f, "/v1/owners/reconcile")
			after, _ := os.ReadFile(filepath.Join(f.root, "goals", "journal.json"))
			recordAfter, _ := os.ReadFile(filepath.Join(f.root, "sessions", f.id+".json"))
			if w.Code != 409 || !strings.Contains(w.Body.String(), `"code":"reconcile_failed"`) || f.factory.binds != binds || f.native.writes != 0 || string(before) != string(after) || string(recordBefore) != string(recordAfter) {
				t.Fatalf("authority refusal had effects: %d %s binds=%d writes=%d", w.Code, w.Body, f.factory.binds-binds, f.native.writes)
			}
		})
	}
}

// A captured old physical slot cannot bind after Stop; escaped operations close
// on callback return. Neither body owner nor a retained operation grants access.
func TestGoalRuntimeCapturedLeaseAndScopeRefuse(t *testing.T) {
	f := newGoalRuntimeFixture(t)
	var escaped goal.Protocol8GoalSession
	binds := f.factory.binds
	err := f.runtime.WithGoalSession(context.Background(), "parent/primary", func(s goal.Protocol8GoalSession) error {
		escaped = s
		if _, err := f.host.Stop(context.Background(), f.id); err != nil {
			return err
		}
		if _, err := s.Reconcile(); !errors.Is(err, core.ErrProviderHostNotFound) {
			t.Fatalf("stale slot reached binding: %v", err)
		}
		return nil
	})
	if err != nil || f.factory.binds != binds || f.native.writes != 0 {
		t.Fatalf("stale effect: %v", err)
	}
	if _, err := escaped.Reconcile(); !errors.Is(err, core.ErrInputScopeClosed) {
		t.Fatalf("escaped operation remained open: %v", err)
	}
	if err := escaped.Rollback(nil); !errors.Is(err, core.ErrInputScopeClosed) {
		t.Fatalf("escaped rollback: %v", err)
	}
}

// A real codec persistence refusal after native acknowledgement cannot return
// successful HTTP projection or a fabricated applied host ref. Recovery remains
// explicit; the fixture does not simulate a provider or journal crash restart.
func TestGoalRuntimeHTTPPersistenceFailureIsNotSuccess(t *testing.T) {
	f := newGoalRuntimeFixture(t)
	f.codec.fail = true
	observed := &observedGoalRuntime{Protocol8Runtime: f.runtime}
	f.runtime = observed
	w := runtimeHTTP(t, f, "/v1/owners/reconcile")
	if w.Code != 409 || !strings.Contains(w.Body.String(), `"code":"reconcile_failed"`) || strings.Contains(w.Body.String(), `"reconciled":true`) || f.native.writes != 1 || !errors.Is(observed.err, errGoalRuntimePersist) {
		t.Fatalf("persistence refusal lost: %d %s writes=%d operation=%v", w.Code, w.Body, f.native.writes, observed.err)
	}
}

// Activation uses the same observed lease without binding/hydrating. Actual
// duplicate activation is rejected, and exact release permits another begin.
func TestGoalRuntimeActivationUsesCapturedLease(t *testing.T) {
	f := newGoalRuntimeFixture(t)
	before := f.factory.binds
	err := f.runtime.WithGoalSession(context.Background(), "parent/primary", func(s goal.Protocol8GoalSession) error {
		id, live, err := s.BeginActivation()
		if err != nil || !live || id == "" {
			t.Fatalf("begin: %s %v %v", id, live, err)
		}
		if _, _, err = s.BeginActivation(); err == nil {
			t.Fatal("duplicate activation succeeded")
		}
		if err = s.EndActivation(id); err != nil {
			return err
		}
		id, _, err = s.BeginActivation()
		if err != nil {
			return err
		}
		return s.EndActivation(id)
	})
	if err != nil || f.factory.binds != before || f.native.writes != 0 {
		t.Fatalf("activation bound input: %v", err)
	}
}
