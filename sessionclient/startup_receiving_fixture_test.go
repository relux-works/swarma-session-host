//go:build linux

package sessionclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	defaults "github.com/relux-works/swarma-session-host/hostdefaults"
	client "github.com/relux-works/swarma-session-host/sessionclient"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// These fixtures replace only original board-coupled setup/provider processes.
// Host, durable codec/notices, source-fenced full parser, controller journal,
// authenticated ControlServer, HTTP routes and session proxy are production7da.
// No in-process Source below is a production authority or SessionStart proof.
type receivingLayout struct {
	core.Layout
	startup *client.StartupReceivingFixture
	project string
}

func receivingTestLayout(t *testing.T, name string) receivingLayout {
	t.Helper()
	startup := client.NewStartupReceivingFixture(t, name)
	l := startup.Layout()
	root := filepath.Dir(l.EndpointFile)
	layout, err := defaults.LayoutAt(root, core.NamespaceKey(l.Namespace))
	if err != nil {
		t.Fatal(err)
	}
	layout.EndpointFile, layout.TokenFile, layout.LockFile, layout.SocketPath = l.EndpointFile, l.TokenFile, l.LockFile, l.SocketPath
	return receivingLayout{Layout: layout, startup: startup, project: t.TempDir()}
}

type receivingNative struct {
	mu              sync.Mutex
	id              string
	state           core.ProviderHostState
	target, project string
	stops           int
}

func (n *receivingNative) State(context.Context) (core.ProviderHostState, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, nil
}
func (n *receivingNative) Snapshot() (core.RunnerSnapshot, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return core.RunnerSnapshot{Kind: "codex", Codex: &core.CodexRunnerSnapshot{Contract: "task-board.codex-provider-host-v1", PID: os.Getpid(), URL: n.target, ThreadID: "019f8e50-1234-7abc-8def-0123456789ab", ProjectDir: n.project, CodexHome: filepath.Join(n.project, "client"), Stopped: n.state == core.ProviderHostStopped}}, nil
}
func (n *receivingNative) Stop(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stops++
	n.state = core.ProviderHostStopped
	return nil
}
func (n *receivingNative) AttachmentTarget(context.Context) (string, error) { return n.target, nil }
func (n *receivingNative) NativeIdentity() string                           { return "019f8e50-1234-7abc-8def-0123456789ab" }
func (n *receivingNative) ClientHome() string                               { return filepath.Join(n.project, "client") }

// Exact goal-less boundary of7da hostdefaults/legacy_attach_test.go:57-66.
// The actual Host installs its durable controller around this structural port;
// no provider input or successful native observation is supplied by this fixture.
func (n *receivingNative) SupportsNativeGoals() bool { return false }
func (n *receivingNative) ObserveNativeGoal(context.Context) (core.NativeGoalObservation, error) {
	return core.NativeGoalObservation{}, core.ErrHostCapabilityUnavailable
}
func (n *receivingNative) AcceptsInput(core.InputSubmission) bool { return false }
func (n *receivingNative) ExecuteInput(context.Context, core.InputSubmission, core.InputReceipt) (func(context.Context) (core.InputObservation, error), error) {
	return nil, errors.New("goal-less fixture cannot execute provider input")
}

type receivingFactory struct {
	mu       sync.Mutex
	native   *receivingNative
	plan     core.PrimarySessionLaunchPlan
	envelope defaults.LegacyCodexSessionLaunch
	requests []core.ProviderLaunchRequest
	restores int
}

func newReceivingFactory(l receivingLayout, target string) *receivingFactory {
	plan := core.PrimarySessionLaunchPlan{Contract: core.PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Producer: core.LaunchPlanProducer{Version: "fixture", Commit: "5721-to-7da"}, Provider: "codex", ProjectDir: l.project, Executable: filepath.Join(l.project, "codex"), LaunchVariants: core.LaunchVariants{ManagedHost: core.LaunchArgv{Kind: "codex-app-server", Argv: []string{"app-server", "--listen", "ws://127.0.0.1:0"}}}, RequiredEnvNames: []string{"FIXTURE_LAUNCH"}, Sources: []core.LaunchPlanSource{{Kind: "fixture", Path: l.project, Scope: "isolated"}}}
	envelope := defaults.LegacyCodexSessionLaunch{Contract: defaults.LegacyCodexSessionLaunchContract, Plan: plan, ChildEnv: map[string]string{"FIXTURE_LABEL": "receiving"}, AutoContinue: core.CodexAutoContinueOptions{Enabled: true, DelaySeconds: 2, MaxNudgesPerTurn: 3, Prompt: "continue"}}
	return &receivingFactory{native: &receivingNative{state: core.ProviderHostRunning, target: target, project: l.project}, plan: plan, envelope: envelope}
}
func (f *receivingFactory) Provider() string { return "codex" }
func (f *receivingFactory) Launch(_ context.Context, q core.ProviderLaunchRequest) (core.RunnerHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if q.Owner != "parent/primary" || q.SessionID == "" || q.Launch.Codex == nil || !reflect.DeepEqual(q.Launch.Codex.Plan, f.envelope.Plan) || !reflect.DeepEqual(q.Launch.Codex.ChildEnv, f.envelope.ChildEnv) || q.Launch.Codex.AutoContinue != f.envelope.AutoContinue || !reflect.DeepEqual(q.Launch.Environment, []string{"FIXTURE_LAUNCH=configured"}) || q.Launch.Executable != f.plan.Executable || !reflect.DeepEqual(q.Launch.Variants, f.plan.LaunchVariants) {
		return nil, errors.New("complete configured original launch did not reach provider boundary")
	}
	f.requests = append(f.requests, q)
	f.native.mu.Lock()
	f.native.id = q.SessionID
	f.native.state = core.ProviderHostRunning
	f.native.mu.Unlock()
	return f.native, nil
}
func (f *receivingFactory) Restore(_ context.Context, q core.ProviderHostRecord) (core.RunnerHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	expected, _ := f.native.Snapshot()
	if q.Provider != "codex" || q.Snapshot.Kind != "codex" || q.Snapshot.Codex == nil || !reflect.DeepEqual(q.Snapshot.Codex, expected.Codex) {
		return nil, core.ErrProviderHostNotFound
	}
	f.restores++
	return f.native, nil
}
func (f *receivingFactory) launchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}
func (f *receivingFactory) request(adopt bool) defaults.LegacyAttachRequest {
	b, err := json.Marshal(f.envelope)
	if err != nil {
		panic(err)
	}
	return defaults.LegacyAttachRequest{Owner: core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}, Provider: f.Provider(), Launch: b, Adopt: adopt}
}

type receivingVerifiedSource struct {
	manager      *receivingManager
	mu           sync.Mutex
	calls        int
	denied       bool
	adoptAllowed bool
	leases       []core.ControllerLease
}

func (s *receivingVerifiedSource) WithVerifiedAttach(ctx context.Context, q defaults.LegacyAttachRequest, target defaults.LegacyAttachTarget, fn func(context.Context, []string) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if err := ctx.Err(); err != nil {
		return err
	}
	expected := s.manager.factory.request(s.adoptAllowed)
	if s.denied || !reflect.DeepEqual(q, expected) {
		return errors.New("request not in configured isolated receiving authority")
	}
	// This is an actual fresh owner-slot read from this exact controlled Host;
	// body owner/PID/native identity are not the authority for the callback.
	current, lease, live, err := s.manager.host.OwnerSession(ctx, "parent/primary")
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(target.Session, current) || target.Lease != lease || target.Live != live || live && (lease.SessionID != current.SessionID || lease.Epoch == 0) {
		return core.ErrStaleInputController
	}
	s.leases = append(s.leases, lease)
	return fn(ctx, []string{"FIXTURE_LAUNCH=configured"})
}

type receivingManager struct {
	t             *testing.T
	layout        receivingLayout
	factory       *receivingFactory
	host          *core.SessionHost
	source        *receivingVerifiedSource
	server        *core.ControlServer
	client        *client.Client
	cancel        context.CancelFunc
	done          chan error
	joined        bool
	missingSource bool
	version       string
}

func newReceivingManager(t *testing.T, l receivingLayout, f *receivingFactory) *receivingManager {
	t.Helper()
	m := &receivingManager{t: t, layout: l, factory: f, version: "0.22.2-48"}
	m.source = &receivingVerifiedSource{manager: m}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m
}
func (m *receivingManager) LoadAndReconcile(ctx context.Context) error {
	if m.host != nil {
		return errors.New("receiving manager already started")
	}
	notices, err := core.NewSessionNoticeStore(m.layout.Layout)
	if err != nil {
		return err
	}
	m.host, err = core.NewHost(core.HostOptions{Layout: m.layout.Layout, Config: defaults.FileConfig{Path: filepath.Join(m.layout.StateDir, "absent.json"), Optional: true, Version: m.version, Namespace: m.layout.Namespace}, Codec: core.NewLegacyRecordCodec(), Notices: notices, Factories: []core.ProviderHostFactory{m.factory}})
	if err != nil {
		return err
	}
	var source defaults.LegacyAttachSource = m.source
	if m.missingSource {
		source = nil
	}
	attach, err := defaults.NewLegacyAttachHandler(m.host, source)
	if err != nil {
		return err
	}
	m.server, err = core.NewControlServer(m.host, func(s *core.ControlServer) http.Handler {
		h, e := core.NewProtocol8Handler(m.host, s.Status, attach)
		if e != nil {
			panic(e)
		}
		return h
	}, core.WithControlServerVersion(m.version))
	if err != nil {
		return err
	}
	life, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	m.cancel = cancel
	m.done = make(chan error, 1)
	go func() { m.done <- m.server.Serve(life) }()
	select {
	case <-m.server.Ready():
	case err := <-m.done:
		m.joined = true
		cancel()
		return err
	case <-ctx.Done():
		cancel()
		return ctx.Err()
	}
	m.client, err = client.Dial(m.layout.startup.Layout().EndpointFiles)
	return err
}
func (m *receivingManager) Attach(ctx context.Context, q defaults.LegacyAttachRequest) (*defaults.LegacyAttachment, error) {
	var out defaults.LegacyAttachment
	err := m.client.DoRequest(ctx, http.MethodPost, "/v1/sessions/attach", q, &out, true)
	if err != nil {
		return nil, err
	}
	return &out, nil
}
func (m *receivingManager) Detach(id, attachment string) (core.Protocol8Session, error) {
	var out core.Protocol8Session
	err := m.client.DoRequest(context.Background(), http.MethodPost, "/v1/sessions/"+id+"/detach", map[string]string{"attachment_id": attachment}, &out, true)
	return out, err
}
func (m *receivingManager) Stop(ctx context.Context, id string) (core.Protocol8Session, error) {
	var out core.Protocol8Session
	err := m.client.DoRequest(ctx, http.MethodPost, "/v1/sessions/"+id+"/stop", map[string]string{}, &out, true)
	return out, err
}
func (m *receivingManager) Close() error {
	if m.client != nil {
		m.client.Close()
	}
	if m.done == nil {
		if m.host != nil {
			m.host.Close()
		}
		return nil
	}
	if m.joined {
		return nil
	}
	m.server.Drain()
	m.cancel()
	select {
	case err := <-m.done:
		m.joined = true
		return err
	case <-time.After(6 * time.Second):
		return errors.New("receiving ControlServer failed to join")
	}
}
func startReceivingControl(t *testing.T, m *receivingManager) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := m.LoadAndReconcile(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	once := sync.Once{}
	stop := func() { once.Do(func() { done <- m.Close() }) }
	return stop, done
}
func receivingEcho(t *testing.T) (string, net.Listener) {
	t.Helper()
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
	return "ws://" + listener.Addr().String(), listener
}
func providerProxySocketPath(t *testing.T, endpoint string) string {
	t.Helper()
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "unix" || parsed.Host != "" || parsed.Path == "" {
		t.Fatalf("provider proxy endpoint = %q", endpoint)
	}
	return parsed.Path
}

func assertProviderProxyRoundTrip(t *testing.T, socketPath, message string) {
	t.Helper()
	connection, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.Write([]byte(message)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(message))
	if _, err := io.ReadFull(connection, buffer); err != nil {
		t.Fatal(err)
	}
	if string(buffer) != message {
		t.Fatalf("proxy echo = %q, want %q", buffer, message)
	}
}

func connectReceivingAndAttach(ctx context.Context, l receivingLayout, launch func() error, request defaults.LegacyAttachRequest) (*client.Client, *defaults.LegacyAttachment, error) {
	var attachment defaults.LegacyAttachment
	c, err := client.ConnectOrStartAndAttach(ctx, l.startup.Layout(), l.startup.Options(launch), request, &attachment)
	if err != nil {
		return nil, nil, err
	}
	return c, &attachment, nil
}
func declareOutsideShell(t *testing.T) {
	t.Helper()
	t.Setenv("TASK_BOARD_MANAGED_SESSION_ID", "")
	t.Setenv("TASK_BOARD_RUN_ID", "")
}
