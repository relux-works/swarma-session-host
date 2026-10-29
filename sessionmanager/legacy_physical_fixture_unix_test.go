//go:build unix

package sessionmanager

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	client "github.com/relux-works/swarma-session-host/sessionclient"
)

// All host records use the real legacy codec and durable notice store. Only
// the native provider boundary is a fixture; it launches no external provider.
func legacyPhysicalPrepareDirectories(t *testing.T, layout Layout) error {
	// Preparation is now the real neutral constructor, not a copied directory algorithm.
	h, err := legacyPhysicalNewHost(t, layout)
	if err == nil {
		h.Close()
	}
	return err
}

type legacyPhysicalRunner struct {
	mu       sync.Mutex
	state    ProviderHostState
	stateErr error
	snapshot RunnerSnapshot
	target   string
	messages []string
}

func (r *legacyPhysicalRunner) State(context.Context) (ProviderHostState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.stateErr
}
func (r *legacyPhysicalRunner) Snapshot() (RunnerSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return copyRunnerSnapshot(r.snapshot), nil
}
func (r *legacyPhysicalRunner) Stop(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = ProviderHostStopped
	return nil
}

type legacyPhysicalProxyRunner struct{ *legacyPhysicalRunner }

func (r *legacyPhysicalProxyRunner) AttachmentTarget(context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.target, nil
}
func (r *legacyPhysicalRunner) failState(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stateErr = err
}
func (r *legacyPhysicalRunner) DeliverSessionNotice(_ context.Context, text string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, text)
	return nil
}
func (r *legacyPhysicalRunner) delivered() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.messages...)
}
func legacyPhysicalSnapshot(t *testing.T, provider, id string) RunnerSnapshot {
	t.Helper()
	if provider == "claude" {
		plan := PrimarySessionLaunchPlan{Contract: PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "claude", ProjectDir: t.TempDir(), Executable: "/bin/false", LaunchVariants: LaunchVariants{ManagedHost: LaunchArgv{Kind: "claude-pty"}}}
		native := projectClaudeSnapshot(legacyClaudeSnapshot{Contract: "task-board.claude-provider-host-v1", SessionUUID: id, PID: os.Getpid(), Plan: plan})
		return RunnerSnapshot{Kind: "claude", Claude: &native}
	}
	return RunnerSnapshot{Kind: "codex", Codex: &CodexRunnerSnapshot{Contract: "task-board.codex-provider-host-v1", PID: os.Getpid(), URL: "ws://127.0.0.1:1/physical-fixture", ThreadID: id, ProjectDir: t.TempDir(), CodexHome: t.TempDir()}}
}

type legacyPhysicalFactory struct {
	t           *testing.T
	provider    string
	mu          sync.Mutex
	hosts       map[string]*legacyPhysicalRunner
	launches    int
	snapshotFor func(string) RunnerSnapshot
}

func legacyPhysicalNewFactory(t *testing.T, provider string) *legacyPhysicalFactory {
	return &legacyPhysicalFactory{t: t, provider: provider, hosts: map[string]*legacyPhysicalRunner{}}
}
func (f *legacyPhysicalFactory) Provider() string { return f.provider }
func (f *legacyPhysicalFactory) Launch(_ context.Context, q ProviderLaunchRequest) (RunnerHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	snapshot := legacyPhysicalSnapshot(f.t, f.provider, q.SessionID)
	if f.snapshotFor != nil {
		snapshot = f.snapshotFor(q.SessionID)
	}
	if q.Prior != nil {
		snapshot = copyRunnerSnapshot(q.Prior.Snapshot)
	}
	r := &legacyPhysicalRunner{state: ProviderHostRunning, snapshot: snapshot}
	f.hosts[q.SessionID] = r
	f.launches++
	return r, nil
}
func (f *legacyPhysicalFactory) Restore(_ context.Context, q ProviderHostRecord) (RunnerHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.hosts {
		snapshot, _ := r.Snapshot()
		a, _ := json.Marshal(snapshot)
		b, _ := json.Marshal(q.Snapshot)
		if string(a) == string(b) {
			return r, nil
		}
	}
	return nil, ErrProviderHostNotFound
}
func (f *legacyPhysicalFactory) launchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.launches
}
func (f *legacyPhysicalFactory) host(id string) *legacyPhysicalRunner {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hosts[id]
}
func (f *legacyPhysicalFactory) only(t *testing.T) *legacyPhysicalRunner {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.hosts) != 1 {
		t.Fatalf("native hosts=%d, want1", len(f.hosts))
	}
	for _, r := range f.hosts {
		return r
	}
	panic("unreachable")
}

type legacyPhysicalProxyFactory struct {
	t    *testing.T
	host *legacyPhysicalRunner
}

func (f *legacyPhysicalProxyFactory) Provider() string { return "codex" }
func (f *legacyPhysicalProxyFactory) Launch(_ context.Context, q ProviderLaunchRequest) (RunnerHost, error) {
	f.host.mu.Lock()
	defer f.host.mu.Unlock()
	f.host.state = ProviderHostRunning
	f.host.snapshot = legacyPhysicalSnapshot(f.t, "codex", q.SessionID)
	return &legacyPhysicalProxyRunner{f.host}, nil
}
func (f *legacyPhysicalProxyFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	return &legacyPhysicalProxyRunner{f.host}, nil
}

// These two helpers preserve the original real listener and endpoint assertions.
func legacyPhysicalServeProxyEcho(listener net.Listener) {
	for {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		go func() { defer connection.Close(); _, _ = io.Copy(connection, connection) }()
	}
}
func legacyPhysicalProxySocketPath(t *testing.T, endpoint string) string {
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
func legacyPhysicalLeaveUnixSocketOnClose(t *testing.T, l net.Listener) {
	t.Helper()
	u, ok := l.(*net.UnixListener)
	if !ok {
		t.Fatal("expected Unix listener")
	}
	u.SetUnlinkOnClose(false)
}
func legacyPhysicalNewServer(h *SessionHost) (*ControlServer, error) {
	return NewControlServer(h, func(s *ControlServer) http.Handler {
		handler, err := NewProtocol8Handler(h, s.Status, nil)
		if err != nil {
			panic(err)
		}
		return handler
	}, WithControlServerVersion("physical-fixture"))
}
func legacyPhysicalStartServer(t *testing.T, s *ControlServer) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	select {
	case <-s.Ready():
		return cancel, done
	case err := <-done:
		cancel()
		t.Fatalf("control server startup: %v", err)
	case <-ctx.Done():
		cancel()
		err := <-done
		t.Fatalf("control server readiness: %v / %v", ctx.Err(), err)
	}
	panic("unreachable")
}

type legacyPhysicalClient struct{ *client.Client }

func legacyPhysicalDial(l Layout) (*legacyPhysicalClient, error) {
	c, e := client.Dial(client.EndpointFiles{EndpointFile: l.EndpointFile, TokenFile: l.TokenFile, Namespace: string(l.Namespace)})
	if e != nil {
		return nil, e
	}
	return &legacyPhysicalClient{c}, nil
}
func (c *legacyPhysicalClient) Doctor(ctx context.Context) (DoctorReport, error) {
	var out DoctorReport
	e := c.DoRequest(ctx, "POST", "/v1/doctor", nil, &out, true)
	return out, e
}

var _ ProviderAttachmentTarget = (*legacyPhysicalProxyRunner)(nil)
