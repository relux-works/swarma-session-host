//go:build unix

package hostdefaults

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/sessionclient"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// The source/provider below are explicitly controlled in-process boundaries.
// Real Host, legacy codec, controller journal, proxy, authenticated ControlServer
// and sessionclient run; no native process or production authority is qualified.
type legacyAttachSourceFixture struct {
	verify func(context.Context, LegacyAttachRequest, LegacyAttachTarget, func(context.Context, []string) error) error
	calls  atomic.Int32
}

func (s *legacyAttachSourceFixture) WithVerifiedAttach(ctx context.Context, q LegacyAttachRequest, target LegacyAttachTarget, fn func(context.Context, []string) error) error {
	s.calls.Add(1)
	return s.verify(ctx, q, target, fn)
}

type legacyAttachNativeFixture struct {
	snapshot core.CodexRunnerSnapshot
	stopped  atomic.Bool
}

func (n *legacyAttachNativeFixture) State(context.Context) (core.ProviderHostState, error) {
	if n.stopped.Load() {
		return core.ProviderHostStopped, nil
	}
	return core.ProviderHostRunning, nil
}
func (n *legacyAttachNativeFixture) Snapshot() (core.RunnerSnapshot, error) {
	copy := n.snapshot
	copy.Stopped = n.stopped.Load()
	return core.RunnerSnapshot{Kind: "codex", Codex: &copy}, nil
}
func (n *legacyAttachNativeFixture) Stop(context.Context) error { n.stopped.Store(true); return nil }
func (n *legacyAttachNativeFixture) SupportsNativeGoals() bool  { return false }

// Actual unsupported capability: this fixture supplies no native goal observation.
func (n *legacyAttachNativeFixture) ObserveNativeGoal(context.Context) (core.NativeGoalObservation, error) {
	return core.NativeGoalObservation{}, core.ErrHostCapabilityUnavailable
}
func (n *legacyAttachNativeFixture) AcceptsInput(core.InputSubmission) bool { return false }
func (n *legacyAttachNativeFixture) ExecuteInput(context.Context, core.InputSubmission, core.InputReceipt) (func(context.Context) (core.InputObservation, error), error) {
	return nil, errors.New("goal-less fixture cannot execute provider input")
}
func (n *legacyAttachNativeFixture) NativeIdentity() string { return n.snapshot.ThreadID }
func (n *legacyAttachNativeFixture) AttachmentTarget(context.Context) (string, error) {
	return n.snapshot.URL, nil
}
func (n *legacyAttachNativeFixture) ClientHome() string { return n.snapshot.CodexHome }
func (n *legacyAttachNativeFixture) ClientToolPolicy() core.ClientToolPolicy {
	return n.snapshot.ToolPolicy
}

type legacyAttachProviderFixture struct {
	target, root string
	mu           sync.Mutex
	requests     []core.ProviderLaunchRequest
	native       *legacyAttachNativeFixture
}

func (f *legacyAttachProviderFixture) Provider() string { return "codex" }
func (f *legacyAttachProviderFixture) Launch(_ context.Context, q core.ProviderLaunchRequest) (core.RunnerHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, q)
	f.native = &legacyAttachNativeFixture{snapshot: core.CodexRunnerSnapshot{Contract: "task-board.codex-provider-host-v1", PID: 1, URL: f.target, ThreadID: "fixture-native-thread", ProjectDir: f.root, CodexHome: filepath.Join(f.root, "client"), ToolPolicy: core.ClientToolPolicy{Sandbox: "read-only", Approval: "never"}}}
	return f.native, nil
}
func (f *legacyAttachProviderFixture) Restore(context.Context, core.ProviderHostRecord) (core.RunnerHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.native == nil {
		return nil, core.ErrProviderHostNotFound
	}
	return f.native, nil
}

type legacyAttachFixture struct {
	host     *core.SessionHost
	provider *legacyAttachProviderFixture
	layout   core.Layout
	request  LegacyAttachRequest
	ctx      context.Context
}

func newLegacyAttachFixture(t *testing.T) *legacyAttachFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	root := t.TempDir()
	layout, err := LayoutAt(root, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	short := os.Getenv("SESSIONHOST_TEST_SOCKET_ROOT")
	if short == "" {
		t.Fatal("short isolated socket fixture root required")
	}
	dir, err := os.MkdirTemp(short, "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	layout.SocketPath = filepath.Join(dir, "c.sock")
	layout.AttachmentsDir = filepath.Join(dir, "a")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var clients sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			clients.Add(1)
			go func() { defer clients.Done(); defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done; clients.Wait() })
	provider := &legacyAttachProviderFixture{target: "ws://" + listener.Addr().String(), root: root}
	notices, err := core.NewSessionNoticeStore(layout)
	if err != nil {
		t.Fatal(err)
	}
	host, err := core.NewHost(core.HostOptions{Layout: layout, Config: FileConfig{Path: filepath.Join(root, "absent.json"), Optional: true, Version: "fixture", Namespace: layout.Namespace}, Codec: core.NewLegacyRecordCodec(), Notices: notices, Factories: []core.ProviderHostFactory{provider}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(host.Close)
	plan := legacyPlan(t, "codex")
	plan.RequiredEnvNames = []string{"FIXTURE_LAUNCH"}
	plan.Sources = []core.LaunchPlanSource{{Kind: "fixture", Path: root, Scope: "fixture"}}
	launch, err := json.Marshal(LegacyCodexSessionLaunch{Contract: LegacyCodexSessionLaunchContract, Plan: plan, ChildEnv: map[string]string{"FIXTURE_OVERLAY": "literal"}, AutoContinue: core.CodexAutoContinueOptions{Enabled: true, DelaySeconds: 2, MaxNudgesPerTurn: 3, Prompt: "continue"}})
	if err != nil {
		t.Fatal(err)
	}
	request := LegacyAttachRequest{Owner: core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}, Provider: " CoDeX ", Launch: launch}
	return &legacyAttachFixture{host, provider, layout, request, ctx}
}
func (f *legacyAttachFixture) handler(t *testing.T, source LegacyAttachSource) *legacyAttachHandler {
	t.Helper()
	h, err := NewLegacyAttachHandler(f.host, source)
	if err != nil {
		t.Fatal(err)
	}
	return h.(*legacyAttachHandler)
}
func (f *legacyAttachFixture) authenticatedClient(t *testing.T, handler http.Handler) (*sessionclient.Client, *core.ControlServer) {
	t.Helper()
	server, err := core.NewControlServer(f.host, func(s *core.ControlServer) http.Handler {
		h, e := core.NewProtocol8Handler(f.host, s.Status, handler)
		if e != nil {
			t.Fatal(e)
		}
		return h
	}, core.WithControlServerVersion("fixture"))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- server.Serve(f.ctx) }()
	select {
	case <-server.Ready():
	case e := <-result:
		t.Fatalf("isolated control startup: %v", e)
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	t.Cleanup(func() {
		server.Drain()
		select {
		case e := <-result:
			if e != nil {
				t.Error(e)
			}
		case <-f.ctx.Done():
			t.Error("isolated control did not join", f.ctx.Err())
		}
	})
	// Dial consumes only the fresh isolated fixture's generated bearer file.
	// No live credential is read, printed, copied or used by this test.
	client, err := sessionclient.Dial(sessionclient.EndpointFiles{EndpointFile: f.layout.EndpointFile, TokenFile: f.layout.TokenFile, Namespace: string(f.layout.Namespace)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, server
}
func fixtureVerifiedAttach(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
	return fn(ctx, []string{"FIXTURE_LAUNCH=configured"})
}

// Actual authenticated HTTP preserves complete launch data and original client
// response projection; the endpoint really relays, Detach keeps it, and Stop
// removes it. This qualifies only the declared goal-less/provider fixture.
func TestLegacyAttachAuthenticatedOriginalWireAndProxy(t *testing.T) {
	f := newLegacyAttachFixture(t)
	source := &legacyAttachSourceFixture{verify: fixtureVerifiedAttach}
	client, _ := f.authenticatedClient(t, f.handler(t, source))
	var attachment LegacyAttachment
	if err := client.DoRequest(f.ctx, http.MethodPost, "/v1/sessions/attach", f.request, &attachment, false); err != nil {
		t.Fatal(err)
	}
	if attachment.AttachmentID == "" || attachment.ManagerEndpoint == "" || attachment.ClientHome != filepath.Join(f.provider.root, "client") || attachment.CodexThreadID != "fixture-native-thread" || attachment.ClientToolPolicy.Sandbox != "read-only" || attachment.Session.SessionID == "" || attachment.Session.Owner != f.request.Owner || source.calls.Load() != 1 || f.provider.launchCount() != 1 {
		t.Fatalf("original attachment projection lost: %+v source=%d launches=%d", attachment, source.calls.Load(), f.provider.launchCount())
	}
	parsed, err := DecodeLegacyAttachRequest(mustLegacyAttachJSON(t, f.request))
	if err != nil {
		t.Fatal(err)
	}
	launch := f.provider.launchRequests()[0].Launch
	if launch.Codex == nil || !reflect.DeepEqual(launch.Codex.Plan, parsed.Codex.Plan) || launch.Codex.AutoContinue != parsed.Codex.AutoContinue || !reflect.DeepEqual(launch.Codex.ChildEnv, parsed.Codex.ChildEnv) || !reflect.DeepEqual(launch.Environment, []string{"FIXTURE_LAUNCH=configured"}) {
		t.Fatal("actual full prepared launch or verified environment lost")
	}
	u, err := url.Parse(attachment.ManagerEndpoint)
	if err != nil || u.Scheme != "unix" {
		t.Fatalf("not actual proxy: %v", err)
	}
	c, err := net.Dial("unix", u.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = c.Write([]byte("actual")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 6)
	if _, err = io.ReadFull(c, b); err != nil || string(b) != "actual" {
		t.Fatal("proxy did not relay actual fixture bytes", err)
	}
	var detached core.Protocol8Session
	if err := client.DoRequest(f.ctx, http.MethodPost, "/v1/sessions/"+attachment.Session.SessionID+"/detach", map[string]string{"attachment_id": attachment.AttachmentID}, &detached, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(u.Path); err != nil {
		t.Fatal("detach removed live proxy", err)
	}
	var again LegacyAttachment
	if err := client.DoRequest(f.ctx, http.MethodPost, "/v1/sessions/attach", f.request, &again, false); err != nil || again.Session.SessionID != attachment.Session.SessionID || again.ManagerEndpoint != attachment.ManagerEndpoint || f.provider.launchCount() != 1 {
		t.Fatalf("reattach replaced original session: %+v %v", again, err)
	}
	if err := client.DoRequest(f.ctx, http.MethodPost, "/v1/sessions/"+attachment.Session.SessionID+"/stop", map[string]string{}, &detached, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(u.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("confirmed stop retained proxy: %v", err)
	}
}
func mustLegacyAttachJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

// Outer bearer refusal and original unknown-field parsing happen before the
// trusted source or any provider effects. A valid request then really attaches.
func TestLegacyAttachAuthenticationAndUnknownFieldBeforeEffects(t *testing.T) {
	f := newLegacyAttachFixture(t)
	source := &legacyAttachSourceFixture{verify: fixtureVerifiedAttach}
	client, _ := f.authenticatedClient(t, f.handler(t, source))
	foreign := sessionclient.New(sessionclient.Config{HTTPClient: client.HTTPClient, Token: "fixture-wrong", SocketPath: f.layout.SocketPath})
	if err := foreign.DoRequest(f.ctx, http.MethodPost, "/v1/sessions/attach", f.request, nil, false); err == nil || !strings.Contains(err.Error(), "unauthorized") || source.calls.Load() != 0 || f.provider.launchCount() != 0 {
		t.Fatalf("outer auth reached effects: %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(mustLegacyAttachJSON(t, f.request), &wire); err != nil {
		t.Fatal(err)
	}
	wire["unknown_original_field"] = true
	if err := client.DoRequest(f.ctx, http.MethodPost, "/v1/sessions/attach", wire, nil, false); !errors.Is(err, sessionclient.ErrManagerProtocolUpgradeRequired) || source.calls.Load() != 0 || f.provider.launchCount() != 0 {
		t.Fatalf("unknown field reached source/launch: %v", err)
	}
	var valid LegacyAttachment
	if err := client.DoRequest(f.ctx, http.MethodPost, "/v1/sessions/attach", f.request, &valid, false); err != nil || valid.AttachmentID == "" || f.provider.launchCount() != 1 {
		t.Fatalf("valid neighbor refused: %+v %v", valid, err)
	}
}

func (f *legacyAttachProviderFixture) launchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}
func (f *legacyAttachProviderFixture) launchRequests() []core.ProviderLaunchRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]core.ProviderLaunchRequest(nil), f.requests...)
}

// Missing/expired/denied source, absent configured environment and unsupported
// original consumers refuse before record, epoch, proxy or client publication.
// The authenticated valid launch above is the nearby reject-all control.
func TestLegacyAttachSourceRefusalsBeforeEffects(t *testing.T) {
	for _, kind := range []string{"missing", "typed-nil", "silent-source", "denied", "environment", "fenced-cancelled", "explicit-budget", "owner-runtime-control", "wire-lease"} {
		t.Run(kind, func(t *testing.T) {
			f := newLegacyAttachFixture(t)
			source := &legacyAttachSourceFixture{verify: fixtureVerifiedAttach}
			var supplier LegacyAttachSource = source
			switch kind {
			case "missing":
				supplier = nil
			case "typed-nil":
				var absent *legacyAttachSourceFixture
				supplier = absent
			case "silent-source":
				source.verify = func(context.Context, LegacyAttachRequest, LegacyAttachTarget, func(context.Context, []string) error) error {
					return nil
				}
			case "denied":
				source.verify = func(context.Context, LegacyAttachRequest, LegacyAttachTarget, func(context.Context, []string) error) error {
					return errors.Join(&os.PathError{Op: "read", Path: "/fixture/private-source", Err: os.ErrPermission}, &core.PublicError{Code: "must-not-leak", Message: "private-source"})
				}
			case "environment":
				source.verify = func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
					return fn(ctx, nil)
				}
			case "fenced-cancelled":
				source.verify = func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
					c, cancel := context.WithCancel(ctx)
					cancel()
					return fn(c, []string{"FIXTURE_LAUNCH=configured"})
				}
			case "explicit-budget", "owner-runtime-control":
				parsed, err := DecodeLegacyAttachRequest(mustLegacyAttachJSON(t, f.request))
				if err != nil {
					t.Fatal(err)
				}
				if kind == "explicit-budget" {
					value := int64(123)
					parsed.Codex.GoalTokenBudget = &value
				} else {
					parsed.Codex.OwnerRun = &LegacyOwnerRunContext{RunID: "RUN-fixture", TaskID: "TASK-fixture", ContextCtxID: "CTX-fixture", RuntimeControl: json.RawMessage(`{"untrusted":"data"}`)}
				}
				f.request.Launch = mustLegacyAttachJSON(t, parsed.Codex)
			}
			handler := f.handler(t, supplier)
			body := mustLegacyAttachJSON(t, f.request)
			if kind == "wire-lease" {
				var fields map[string]any
				if err := json.Unmarshal(body, &fields); err != nil {
					t.Fatal(err)
				}
				fields["expected_owner_lease"] = map[string]any{"SessionID": "claimed", "Epoch": 1}
				body = mustLegacyAttachJSON(t, fields)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/sessions/attach", strings.NewReader(string(body))))
			want := 409
			if kind == "wire-lease" {
				want = 400
			}
			if response.Code != want || strings.Contains(response.Body.String(), "private-source") || strings.Contains(response.Body.String(), "must-not-leak") || f.provider.launchCount() != 0 {
				t.Fatalf("source refusal wrong or leaked: %d %s launches=%d", response.Code, response.Body, f.provider.launchCount())
			}
			views, err := f.host.List(f.ctx)
			if err != nil || len(views) != 0 {
				t.Fatalf("source refusal published session: %+v %v", views, err)
			}
			entries, err := os.ReadDir(f.layout.SessionsDir)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatal("source refusal wrote record")
			}
			if _, err := os.Lstat(f.layout.AttachmentsDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("source refusal created proxy: %v", err)
			}
		})
	}
}

// A grant based on an empty/old observed slot cannot attach to a replacement.
// The competitor is an explicit fixture setup effect; the stale callback must
// create no additional launch/client and leave its durable record byte-exact.
func TestLegacyAttachPinsCapturedLeaseBeforeLaunch(t *testing.T) {
	for _, mode := range []string{"empty-became-held", "holder-replaced"} {
		t.Run(mode, func(t *testing.T) {
			f := newLegacyAttachFixture(t)
			prepared, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "holder-replaced" {
				if _, err = f.host.Attach(f.ctx, prepared); err != nil {
					t.Fatal(err)
				}
			}
			var competitor *core.Attachment
			var before []byte
			var recordPath string
			source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, target LegacyAttachTarget, fn func(context.Context, []string) error) error {
				if mode == "holder-replaced" {
					if !target.Live || target.Lease.SessionID == "" {
						t.Fatal("existing captured mapping absent")
					}
					if _, e := f.host.Stop(ctx, target.Session.SessionID); e != nil {
						return e
					}
				} else if target.Live || target.Lease != (core.ControllerLease{}) {
					t.Fatal("empty mapping not observed")
				}
				var e error
				competitor, e = f.host.Attach(ctx, prepared)
				if e != nil {
					return e
				}
				recordPath = filepath.Join(f.layout.SessionsDir, competitor.Session.SessionID, "record.json")
				before, e = os.ReadFile(recordPath)
				if e != nil {
					return e
				}
				return fn(ctx, []string{"FIXTURE_LAUNCH=configured"})
			}}
			_, err = f.handler(t, source).attach(f.ctx, f.request)
			if !errors.Is(err, core.ErrStaleInputController) || competitor == nil {
				t.Fatalf("stale mapping accepted/wrong refusal: %v", err)
			}
			want := 1
			if mode == "holder-replaced" {
				want = 2
			}
			if f.provider.launchCount() != want {
				t.Fatal("stale callback launched another provider")
			}
			after, e := os.ReadFile(recordPath)
			if e != nil || string(after) != string(before) {
				t.Fatalf("stale callback changed committed image: %v", e)
			}
			current, e := f.host.Session(f.ctx, competitor.Session.SessionID)
			if e != nil || current.AttachedClients != 1 {
				t.Fatalf("stale callback published client: %+v %v", current, e)
			}
		})
	}
}

// Source callbacks expire on return and cannot be invoked twice to issue a
// second client. A refusal after the first callback must leave no undisclosed
// client; returning the source violation alone does not undo that owned effect.
func TestLegacyAttachSourceCallbackExpiresAndCannotDuplicate(t *testing.T) {
	f := newLegacyAttachFixture(t)
	var escaped func(context.Context, []string) error
	source := &legacyAttachSourceFixture{verify: func(_ context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		escaped = fn
		return nil
	}}
	if _, err := f.handler(t, source).attach(f.ctx, f.request); !errors.Is(err, ErrLegacyAttachSourceUnavailable) {
		t.Fatalf("source without live callback accepted: %v", err)
	}
	if err := escaped(f.ctx, []string{"FIXTURE_LAUNCH=configured"}); !errors.Is(err, core.ErrInputScopeClosed) || f.provider.launchCount() != 0 {
		t.Fatalf("escaped source retained grant: %v", err)
	}
	source.verify = func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if e := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); e != nil {
			return e
		}
		if e := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); !errors.Is(e, core.ErrInputScopeClosed) {
			t.Fatalf("duplicate callback not fenced: %v", e)
		}
		return nil
	}
	if _, err := f.handler(t, source).attach(f.ctx, f.request); !errors.Is(err, core.ErrInputScopeClosed) {
		t.Fatalf("source violation became success: %v", err)
	}
	views, err := f.host.List(f.ctx)
	if err != nil || len(views) != 1 || views[0].AttachedClients != 0 || f.provider.launchCount() != 1 {
		t.Fatalf("source duplicate issued extra effects: %+v %v", views, err)
	}
}

// The actual standalone default mount reaches the trusted source boundary while
// configured launch rights are absent; it does not fall back to raw core input.
// This constructor-only test does not qualify a native provider or registrar.
func TestStandaloneOriginalAttachMountUsesVerifiedSource(t *testing.T) {
	o := standaloneOptions(t)
	source := &legacyAttachSourceFixture{verify: func(context.Context, LegacyAttachRequest, LegacyAttachTarget, func(context.Context, []string) error) error {
		return errors.New("isolated missing grant")
	}}
	o.AttachSource = source
	s, err := OpenStandalone(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	handler, err := s.controlHandler(s.Control.Status)
	if err != nil {
		t.Fatal(err)
	}
	request := LegacyAttachRequest{Owner: core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}, Provider: "codex"}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/sessions/attach", strings.NewReader(string(mustLegacyAttachJSON(t, request)))))
	if w.Code != 409 || !strings.Contains(w.Body.String(), `"code":"attach_failed"`) || source.calls.Load() != 1 {
		t.Fatalf("default skipped actual source: %d %s calls=%d", w.Code, w.Body, source.calls.Load())
	}
	views, e := s.Host.List(context.Background())
	if e != nil || len(views) != 0 {
		t.Fatalf("source refusal created runtime: %+v %v", views, e)
	}
}
