//go:build linux

package sessionclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const wedgedHolderToken = "wedged-holder-token"

var errStartupFixtureAlreadyRunning = errors.New("fixture singleton already running")

type startupTestLayout struct {
	StartupLayout
	OperatorLogFile string
	source          *startupFixtureSource
}
type startupFixtureSource struct {
	layout       StartupLayout
	sessions     string
	mu           sync.Mutex
	children     map[int]*wedgedHolderProcess
	events       []fixtureEvent
	launches     int
	unknown      bool
	refusal      error
	foreignProof bool
}
type fixtureEvent struct {
	Name   string
	Fields map[string]any
}

func staleTakeoverTestLayout(t *testing.T, name string) startupTestLayout {
	t.Helper()
	root, e := os.MkdirTemp("/tmp", "sh1-start-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	hash := sha256.Sum256([]byte(name + root))
	l := startupTestLayout{StartupLayout: StartupLayout{EndpointFiles: EndpointFiles{EndpointFile: filepath.Join(root, "endpoint.json"), TokenFile: filepath.Join(root, "token"), Namespace: fmt.Sprintf("%x", hash)}, LockFile: filepath.Join(root, "manager.lock"), SocketPath: filepath.Join(root, "control.sock")}, OperatorLogFile: filepath.Join(root, "events.jsonl")}
	sessions := filepath.Join(root, "hosted.json")
	if e = os.WriteFile(sessions, []byte("[]"), 0600); e != nil {
		t.Fatal(e)
	}
	l.source = &startupFixtureSource{layout: l.StartupLayout, sessions: sessions, children: map[int]*wedgedHolderProcess{}}
	return l
}
func testLayout(t *testing.T, name string) startupTestLayout { return staleTakeoverTestLayout(t, name) }
func declareOutsideShell(t *testing.T) {
	t.Helper()
	t.Setenv("TASK_BOARD_MANAGED_SESSION_ID", "")
	t.Setenv("TASK_BOARD_RUN_ID", "")
}

func (s *startupFixtureSource) LockHeld(ctx context.Context, l StartupLayout) (bool, error) {
	if e := ctx.Err(); e != nil {
		return false, e
	}
	if l != s.layout {
		return false, ErrStartupAuthorityUnavailable
	}
	f, e := os.OpenFile(l.LockFile, os.O_RDWR, 0)
	if errors.Is(e, fs.ErrNotExist) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	defer f.Close()
	e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(e, syscall.EWOULDBLOCK) {
		return true, nil
	}
	if e != nil {
		return false, e
	}
	return false, syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
func processBirth(pid int) (string, error) {
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if e != nil {
		return "", e
	}
	p := strings.LastIndexByte(string(b), ')')
	if p < 0 {
		return "", errors.New("invalid proc stat")
	}
	fields := strings.Fields(string(b[p+1:]))
	if len(fields) < 20 {
		return "", errors.New("short proc stat")
	}
	return fields[19], nil
}
func fixtureProcessAlive(pid int) (bool, error) {
	_, e := processBirth(pid)
	if errors.Is(e, fs.ErrNotExist) {
		return false, nil
	}
	return e == nil, e
}
func (s *startupFixtureSource) Process(ctx context.Context, ep Endpoint) (StartupProcessState, error) {
	if e := ctx.Err(); e != nil {
		return StartupProcessUnknown, e
	}
	s.mu.Lock()
	child := s.children[ep.PID]
	unknown := s.unknown
	s.mu.Unlock()
	if unknown || child == nil {
		return StartupProcessUnknown, ErrStartupObservation
	}
	birth, e := processBirth(ep.PID)
	if errors.Is(e, fs.ErrNotExist) {
		return StartupProcessAbsent, nil
	}
	if e != nil || birth != child.birth {
		return StartupProcessUnknown, ErrStartupObservation
	}
	return StartupProcessPresent, nil
}
func (s *startupFixtureSource) HostedSessions(_ context.Context, l StartupLayout) ([]HostedSessionSummary, error) {
	if l != s.layout {
		return nil, ErrStartupAuthorityUnavailable
	}
	b, e := os.ReadFile(s.sessions)
	if e != nil {
		return nil, e
	}
	var rows []HostedSessionSummary
	if e = json.Unmarshal(b, &rows); e != nil || rows == nil {
		return nil, ErrStartupObservation
	}
	return rows, nil
}
func (s *startupFixtureSource) Caller(ctx context.Context, rows []HostedSessionSummary) (*HostedCallerEvidence, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if id := os.Getenv("TASK_BOARD_MANAGED_SESSION_ID"); id != "" {
		return &HostedCallerEvidence{Reason: HostedCallerManagedSessionEnv, SessionID: id}, nil
	}
	for _, row := range rows {
		if row.PID == os.Getpid() {
			return &HostedCallerEvidence{Reason: HostedCallerProviderDescendant, SessionID: row.SessionID, ProviderPID: row.PID}, nil
		}
	}
	return nil, nil
}
func (s *startupFixtureSource) Launch(context.Context, StartupLayout) error {
	return ErrStartupAuthorityUnavailable
}
func (s *startupFixtureSource) Replacement(ctx context.Context, l StartupLayout, ep Endpoint, _ ManagerReplacementReason) (StartupReplacement, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if l != s.layout {
		return nil, ErrStartupAuthorityUnavailable
	}
	s.mu.Lock()
	child := s.children[ep.PID]
	refusal := s.refusal
	foreignProof := s.foreignProof
	s.mu.Unlock()
	if refusal != nil {
		return nil, refusal
	}
	if child == nil {
		return nil, ErrStartupAuthorityUnavailable
	}
	state, e := s.Process(ctx, ep)
	if e != nil || state != StartupProcessPresent {
		return nil, ErrStartupAuthorityUnavailable
	}
	current, e := startupEndpoint(l)
	if e != nil || !sameEndpointInstance(current, ep) {
		return nil, ErrStartupEndpointChanged
	}
	if foreignProof {
		ep.InstanceID = "MGR-foreign-fixture"
	}
	return &ownedFixtureReplacement{s: s, child: child, endpoint: ep}, nil
}

type ownedFixtureReplacement struct {
	s        *startupFixtureSource
	child    *wedgedHolderProcess
	endpoint Endpoint
}

func (c *ownedFixtureReplacement) Endpoint() Endpoint { return c.endpoint }
func (c *ownedFixtureReplacement) Terminate(ctx context.Context, force bool) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	birth, e := processBirth(c.child.pid)
	if e != nil || birth != c.child.birth {
		return ErrStartupObservation
	}
	current, e := startupEndpoint(c.s.layout)
	if e != nil || !sameEndpointInstance(current, c.endpoint) {
		return ErrStartupEndpointChanged
	}
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	return c.child.command.Process.Signal(sig)
}

type fixtureLaunchSource struct {
	*startupFixtureSource
	launch func() error
}

func (s *fixtureLaunchSource) Launch(ctx context.Context, l StartupLayout) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if l != s.layout || s.launch == nil {
		return ErrStartupAuthorityUnavailable
	}
	if e := validateStartupLayout(l); e != nil {
		return e
	}
	held, e := s.LockHeld(ctx, l)
	if e != nil {
		return e
	}
	if held {
		return ErrStartupAuthorityUnavailable
	}
	s.mu.Lock()
	s.launches++
	s.mu.Unlock()
	return s.launch()
}
func (s *startupFixtureSource) record(name string, fields map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, fixtureEvent{name, fields})
}
func connectOriginalStartup(ctx context.Context, l startupTestLayout, launch func() error) (*Client, error) {
	return ConnectOrStart(ctx, l.StartupLayout, StartupOptions{Source: &fixtureLaunchSource{l.source, launch}, RecordEvent: l.source.record})
}
func readOperatorLogEntries(t *testing.T, path string) ([]fixtureEvent, string) {
	t.Helper()
	// Events originate in the actual startup callbacks; no predetermined success
	// event is fabricated. The caller's layout owns this exact root.
	l, ok := fixtureLayouts.Load(path)
	if !ok {
		t.Fatal("fixture layout missing")
	}
	s := l.(*startupFixtureSource)
	s.mu.Lock()
	defer s.mu.Unlock()
	b, e := json.Marshal(s.events)
	if e != nil {
		t.Fatal(e)
	}
	return append([]fixtureEvent(nil), s.events...), string(b)
}
func operatorLogContains(rows []fixtureEvent, name string, want map[string]string) bool {
	for _, row := range rows {
		if row.Name != name {
			continue
		}
		match := true
		for k, v := range want {
			if fmt.Sprint(row.Fields[k]) != v {
				match = false
			}
		}
		if match {
			return true
		}
	}
	return false
}

var fixtureLayouts sync.Map

// startupTestDaemon is an owned HTTP/socket/lock fixture, not a second Manager
// implementation. It publishes the original identity/token wire and no provider
// attach success, native identity or authorization endpoint.
type startupTestDaemon struct {
	layout            startupTestLayout
	version, instance string
	ready             chan struct{}
	server            *http.Server
}

func newStartupTestDaemon(l startupTestLayout, version string) (*startupTestDaemon, error) {
	if e := validateStartupLayout(l.StartupLayout); e != nil {
		return nil, e
	}
	fixtureLayouts.Store(l.OperatorLogFile, l.source)
	return &startupTestDaemon{layout: l, version: version, instance: fmt.Sprintf("MGR-new-%d", time.Now().UnixNano()), ready: make(chan struct{})}, nil
}
func (d *startupTestDaemon) Ready() <-chan struct{} { return d.ready }
func (d *startupTestDaemon) Serve(ctx context.Context) error {
	l := d.layout
	lock, e := os.OpenFile(l.LockFile, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return errStartupFixtureAlreadyRunning
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	_ = os.Remove(l.SocketPath) // Exact owned fixture, authorized after lock acquisition.
	listener, e := net.Listen("unix", l.SocketPath)
	if e != nil {
		return e
	}
	defer listener.Close()
	if e = os.Chmod(l.SocketPath, 0600); e != nil {
		return e
	}
	ep := Endpoint{Protocol: ProtocolIdentifier(ProtocolVersion), ProtocolVersion: ProtocolVersion, DaemonVersion: d.version, Namespace: l.Namespace, PID: os.Getpid(), InstanceID: d.instance, SocketPath: l.SocketPath, StartedAt: time.Now().UTC()}
	data, e := json.Marshal(ep)
	if e != nil {
		return e
	}
	token := "replacement-fixture-token"
	if e = os.WriteFile(l.TokenFile, []byte(token), 0600); e != nil {
		return e
	}
	if e = os.WriteFile(l.EndpointFile, data, 0600); e != nil {
		return e
	}
	d.server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/v1/status" {
			http.Error(w, "fixture has no production route", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(StartupStatus{ProtocolStatus: ProtocolStatus{Protocol: ep.Protocol, ProtocolVersion: ep.ProtocolVersion, DaemonVersion: ep.DaemonVersion, StartupHealthy: true}, Namespace: ep.Namespace, PID: ep.PID, InstanceID: ep.InstanceID, StartedAt: ep.StartedAt})
	}), ReadHeaderTimeout: time.Second}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = d.server.Close()
		case <-done:
		}
	}()
	close(d.ready)
	e = d.server.Serve(listener)
	close(done)
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
func startTestDaemon(t *testing.T, d *startupTestDaemon) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Serve(ctx) }()
	select {
	case <-d.Ready():
	case e := <-done:
		cancel()
		t.Fatalf("fixture startup: %v", e)
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("fixture startup timed out")
	}
	return cancel, done
}

type wedgedHolderProcess struct {
	command           *exec.Cmd
	done              chan struct{}
	waitErr           error
	stderr            bytes.Buffer
	pid               int
	birth, instanceID string
}

func startWedgedHolderProcess(t *testing.T, l startupTestLayout) *wedgedHolderProcess {
	return startSessionManagerHolderProcess(t, l, "wedged")
}
func startSessionManagerHolderProcess(t *testing.T, l startupTestLayout, mode string) *wedgedHolderProcess {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestWedgedSessionManagerHolderHelper$")
	raw, e := json.Marshal(l.StartupLayout)
	if e != nil {
		t.Fatal(e)
	}
	command.Env = append(os.Environ(), "SH1_STARTUP_OWNED_HELPER=1", "SH1_STARTUP_LAYOUT="+string(raw), "SH1_STARTUP_MODE="+mode)
	h := &wedgedHolderProcess{command: command, done: make(chan struct{}), instanceID: "MGR-wedged-holder"}
	command.Stderr = &h.stderr
	if e = command.Start(); e != nil {
		t.Fatal(e)
	}
	h.pid = command.Process.Pid
	h.birth, e = processBirth(h.pid)
	if e != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(e)
	}
	t.Logf("owned helper pid=%d birth=%s", h.pid, h.birth)
	l.source.mu.Lock()
	l.source.children[h.pid] = h
	l.source.mu.Unlock()
	go func() { h.waitErr = command.Wait(); close(h.done) }()
	t.Cleanup(func() {
		select {
		case <-h.done:
			return
		default:
		}
		birth, e := processBirth(h.pid)
		if e == nil && birth == h.birth {
			_ = command.Process.Kill()
		}
		select {
		case <-h.done:
			t.Logf("owned helper reaped pid=%d birth=%s", h.pid, h.birth)
		case <-time.After(2 * time.Second):
			t.Errorf("owned helper did not join pid%d birth%s", h.pid, h.birth)
		}
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ep, e := startupEndpoint(l.StartupLayout)
		if e == nil && ep.PID == h.pid && ep.InstanceID == h.instanceID {
			return h
		}
		select {
		case <-h.done:
			t.Fatalf("holder died before ready: %v %s", h.waitErr, h.stderr.String())
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("holder readiness timed out: %s", h.stderr.String())
	return nil
}
func (h *wedgedHolderProcess) waitForExit(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-h.done:
		if h.waitErr == nil {
			t.Fatal("wedged holder exited successfully; expected takeover termination")
		}
	case <-time.After(timeout):
		t.Fatalf("wedged holder pid %d still running after takeover", h.pid)
	}
}

// Actual original helper modes use only an owned test binary/Unix endpoint and
// flock. It never starts a provider, installed daemon, model or native session.
func TestWedgedSessionManagerHolderHelper(t *testing.T) {
	if os.Getenv("SH1_STARTUP_OWNED_HELPER") != "1" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	var l StartupLayout
	if e := json.Unmarshal([]byte(os.Getenv("SH1_STARTUP_LAYOUT")), &l); e != nil {
		t.Fatal(e)
	}
	lock, e := os.OpenFile(l.LockFile, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		t.Fatal(e)
	}
	listener, e := net.Listen("unix", l.SocketPath)
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	if e = os.Chmod(l.SocketPath, 0600); e != nil {
		t.Fatal(e)
	}
	mode := os.Getenv("SH1_STARTUP_MODE")
	ep := Endpoint{Protocol: ProtocolIdentifier(ProtocolVersion), ProtocolVersion: ProtocolVersion, Namespace: l.Namespace, PID: os.Getpid(), InstanceID: "MGR-wedged-holder", SocketPath: l.SocketPath, StartedAt: time.Now().UTC().Add(-time.Hour)}
	if mode == "outdated-protocol" {
		ep.Protocol = ProtocolIdentifier(ProtocolVersion - 1)
		ep.ProtocolVersion = ProtocolVersion - 1
		ep.DaemonVersion = "0.22.2-47"
	}
	b, e := json.Marshal(ep)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(l.TokenFile, []byte(wedgedHolderToken), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(l.EndpointFile, b, 0600); e != nil {
		t.Fatal(e)
	}
	if mode == "wedged" {
		var conns []net.Conn
		for {
			c, e := listener.Accept()
			if e != nil {
				t.Fatal(e)
			}
			conns = append(conns, c)
		}
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+wedgedHolderToken {
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.URL.Path == "/v1/status" {
			_ = json.NewEncoder(w).Encode(StartupStatus{ProtocolStatus: ProtocolStatus{Protocol: ep.Protocol, ProtocolVersion: ep.ProtocolVersion, DaemonVersion: ep.DaemonVersion, StartupHealthy: mode != "startup-unhealthy"}, Namespace: ep.Namespace, PID: ep.PID, InstanceID: ep.InstanceID, StartedAt: ep.StartedAt})
			return
		}
		if r.URL.Path == "/v1/sessions/attach" && mode == "unknown-field" {
			http.Error(w, "json: unknown field adopt", 400)
			return
		}
		http.Error(w, "fixture route unavailable", 404)
	}), ReadHeaderTimeout: time.Second}
	if e = server.Serve(listener); e != nil && !errors.Is(e, http.ErrServerClosed) {
		t.Fatal(e)
	}
}
