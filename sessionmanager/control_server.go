//go:build unix

package sessionmanager

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const controlProtocolName = "task-board-session-manager"
const controlProtocolVersion = 8

func ControlProtocolVersion() int { return controlProtocolVersion }
func controlProtocolIdentifier(version int) string {
	return fmt.Sprintf("%s-v%d", controlProtocolName, version)
}

// ControlStatus preserves the original control inventory and validator vocabulary.
type ControlStatus struct {
	QuarantinedCount                int                     `json:"quarantined_count"`
	MaterializationCount            int                     `json:"materialization_count"`
	QuarantinedMaterializationCount int                     `json:"quarantined_materialization_count"`
	GoalReflectionContract          *GoalReflectionContract `json:"goal_reflection_contract"`
	Running                         bool                    `json:"running"`
	StartupHealthy                  bool                    `json:"startup_healthy"`
	Protocol                        string                  `json:"protocol,omitempty"`
	ProtocolVersion                 int                     `json:"protocol_version,omitempty"`
	DaemonVersion                   string                  `json:"daemon_version,omitempty"`
	InstanceID                      string                  `json:"instance_id,omitempty"`
	NamespaceFingerprint            string                  `json:"board_fingerprint,omitempty"`
	PID                             int                     `json:"pid,omitempty"`
	StartedAt                       time.Time               `json:"started_at,omitempty"`
	SessionCount                    int                     `json:"session_count"`
}

type endpointMetadata struct {
	Protocol             string    `json:"protocol"`
	ProtocolVersion      int       `json:"protocol_version"`
	DaemonVersion        string    `json:"daemon_version"`
	NamespaceFingerprint string    `json:"board_fingerprint"`
	InstanceID           string    `json:"instance_id"`
	SocketPath           string    `json:"socket_path"`
	PID                  int       `json:"pid"`
	StartedAt            time.Time `json:"started_at"`
}

// ControlServer owns the authenticated Unix listener, endpoint publication,
// startup self-check and joined HTTP shutdown. SessionHost owns the lifetime
// lock and provider/controller/storage runtime. A handler converts the existing
// wire requests to typed host calls; it does not own a server or session loop.
type ControlServer struct {
	details func() ControlStatusDetails
	stateMu sync.Mutex

	host     *SessionHost
	handler  func(*ControlServer) http.Handler
	ready    chan struct{}
	healthy  atomic.Bool
	served   atomic.Bool
	instance string
	started  time.Time
	version  string
	signals  *ShutdownSignalWatch
	cancel   context.CancelFunc
}
type ControlServerOption func(*ControlServer)

func WithControlStatusDetails(details func() ControlStatusDetails) ControlServerOption {
	return func(s *ControlServer) { s.details = details }
}
func WithControlServerVersion(version string) ControlServerOption {
	return func(s *ControlServer) { s.version = version }
}
func WithControlServerSignals(watch *ShutdownSignalWatch) ControlServerOption {
	return func(s *ControlServer) { s.signals = watch }
}
func NewControlServer(host *SessionHost, handler func(*ControlServer) http.Handler, options ...ControlServerOption) (*ControlServer, error) {
	if host == nil || handler == nil {
		return nil, fmt.Errorf("host and control handler are required")
	}
	l := host.options.Layout
	for _, path := range []string{l.SocketPath, l.EndpointFile, l.TokenFile, l.OperatorLogFile} {
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("absolute control runtime paths required")
		}
	}
	id, err := opaqueID("MGR")
	if err != nil {
		return nil, err
	}
	s := &ControlServer{host: host, handler: handler, ready: make(chan struct{}), instance: id, version: host.settings.DaemonVersion}
	for _, option := range options {
		if option != nil {
			option(s)
		}
	}
	if strings.TrimSpace(s.version) == "" {
		return nil, fmt.Errorf("daemon version is required")
	}
	return s, nil
}
func (s *ControlServer) Ready() <-chan struct{} { return s.ready }
func (s *ControlServer) Status() ControlStatus {
	s.stateMu.Lock()
	started := s.started
	s.stateMu.Unlock()
	s.host.mu.Lock()
	count := len(s.host.sessions)
	quarantined := s.host.quarantinedCount
	s.host.mu.Unlock()
	details := ControlStatusDetails{}
	if s.details != nil {
		details = s.details()
	}
	return ControlStatus{QuarantinedCount: quarantined, MaterializationCount: details.MaterializationCount, QuarantinedMaterializationCount: details.QuarantinedMaterializationCount, GoalReflectionContract: copyGoalReflectionContract(details.GoalReflectionContract), Running: true, StartupHealthy: s.healthy.Load(), Protocol: controlProtocolIdentifier(controlProtocolVersion), ProtocolVersion: controlProtocolVersion, DaemonVersion: s.version, InstanceID: s.instance, NamespaceFingerprint: string(s.host.options.Layout.Namespace), PID: os.Getpid(), StartedAt: started, SessionCount: count}
}
func (s *ControlServer) Drain() {
	s.healthy.Store(false)
	s.stateMu.Lock()
	cancel := s.cancel
	s.stateMu.Unlock()
	if cancel != nil {
		cancel()
	}
}
func controlJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func (s *ControlServer) authenticated(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		header := r.Header.Get("Authorization")
		provided := strings.TrimSpace(strings.TrimPrefix(header, prefix))
		if !strings.HasPrefix(header, prefix) || len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			controlJSON(w, http.StatusUnauthorized, controlUnauthorizedBody(r.URL.Path))
			return
		}
		if r.URL.Path == "/v1/daemon/drain" && r.Method == http.MethodPost {
			controlJSON(w, http.StatusAccepted, map[string]bool{"draining": true})
			go s.Drain()
			return
		}
		next.ServeHTTP(w, r)
	})
}
func randomToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating manager bearer token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *ControlServer) Serve(ctx context.Context) (retErr error) {
	if !s.served.CompareAndSwap(false, true) {
		return fmt.Errorf("control server already served")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	serveCtx, cancel := context.WithCancel(ctx)
	s.stateMu.Lock()
	s.cancel = cancel
	s.stateMu.Unlock()
	defer cancel()
	defer s.host.Close()
	defer s.healthy.Store(false)
	l := s.host.options.Layout
	if err := checkBoardlessSocketBeforeSetup(l.SocketPath); err != nil {
		return err
	}
	logger, err := newOperatorLogger(l)
	if err != nil {
		return err
	}
	phase := "initializing"
	defer func() {
		fields := map[string]any{"version": s.version, "pid": os.Getpid(), "phase": phase}
		if observed, ok := s.signals.Observed(); ok {
			for k, v := range observed.operatorLogFields() {
				fields[k] = v
			}
		}
		if retErr != nil {
			logger.Error("daemon_error", retErr, fields)
		}
		logger.Info("daemon_stopped", fields)
	}()
	logger.Info("daemon_starting", map[string]any{"version": s.version, "pid": os.Getpid()})
	phase = "reconciling"
	if err := s.host.Recover(serveCtx); err != nil {
		return s.startupFailure(ctx, "reconciling", err)
	}
	if _, err := s.host.RepairStartup(serveCtx); err != nil {
		return s.startupFailure(ctx, "reconciling", err)
	}
	for _, dir := range []string{filepath.Dir(l.SocketPath), filepath.Dir(l.EndpointFile), filepath.Dir(l.TokenFile)} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err := os.Chmod(dir, 0700); err != nil {
			return err
		}
	}
	if err := checkBoardlessSocketDirectory(filepath.Dir(l.SocketPath)); err != nil {
		return err
	}
	// A foreign file or a reachable listener is not stale runtime metadata. Only
	// an owned socket with the kernel's explicit refused-connection result may go.
	if info, err := os.Lstat(l.SocketPath); err == nil {
		owner, ok := info.Sys().(*syscall.Stat_t)
		if !ok || owner.Uid != uint32(os.Getuid()) || info.Mode()&os.ModeSocket == 0 {
			return boardlessSocketRefusal("existing path is not an owned socket")
		}
		e := probeUnixSocket(l.SocketPath, 50*time.Millisecond)
		if e == nil {
			return boardlessSocketRefusal("existing listener is reachable")
		}
		if !unixSocketKnownAbsent(e) {
			return boardlessSocketRefusal("existing socket liveness is unknown")
		}
		removed, err := removeSameOwnedSocket(l.SocketPath, info)
		if err != nil {
			return err
		}
		if !removed {
			return boardlessSocketRefusal("existing socket changed before cleanup")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	phase = "listening"
	listener, err := net.Listen("unix", l.SocketPath)
	if err != nil {
		return fmt.Errorf("listening on session manager socket: %w", err)
	}
	// UnixListener.Close otherwise unlinks by pathname, even after another
	// listener has replaced our entry. Retain custody of this inode instead.
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	socketInfo, err := os.Lstat(l.SocketPath)
	defer func() {
		e := listener.Close()
		if errors.Is(e, net.ErrClosed) {
			e = nil
		}
		retErr = errors.Join(retErr, e)
		_, e = removeSameOwnedSocket(l.SocketPath, socketInfo)
		retErr = errors.Join(retErr, e)
	}()
	if err != nil {
		return err
	}
	if err := os.Chmod(l.SocketPath, 0600); err != nil {
		return err
	}
	token, err := randomToken()
	if err != nil {
		return err
	}
	s.stateMu.Lock()
	s.started = time.Now().UTC()
	started := s.started
	s.stateMu.Unlock()
	metadata := endpointMetadata{Protocol: controlProtocolIdentifier(controlProtocolVersion), ProtocolVersion: controlProtocolVersion, DaemonVersion: s.version, NamespaceFingerprint: string(l.Namespace), InstanceID: s.instance, SocketPath: l.SocketPath, PID: os.Getpid(), StartedAt: started}
	raw, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	if err := writePrivateFile(l.TokenFile, []byte(token+"\n")); err != nil {
		return err
	}
	defer func() { _ = os.Remove(l.EndpointFile); _ = os.Remove(l.TokenFile) }()
	if err := writePrivateFile(l.EndpointFile, append(raw, '\n')); err != nil {
		return err
	}
	handler := s.handler(s)
	if handler == nil {
		return fmt.Errorf("control handler is required")
	}
	server := &http.Server{Handler: s.authenticated(token, handler), ReadHeaderTimeout: 5 * time.Second}
	phase = "serving"
	logger.Info("daemon_started", map[string]any{"version": s.version, "pid": os.Getpid(), "instance": s.instance})
	serveErr := make(chan error, 1)
	go func() {
		e := server.Serve(listener)
		if errors.Is(e, http.ErrServerClosed) {
			e = nil
		}
		serveErr <- e
	}()
	shutdown := func() error {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		return errors.Join(server.Shutdown(shutdownCtx), <-serveErr)
	}
	phase = "self_checking"
	if err := s.selfCheck(serveCtx, token); err != nil {
		return errors.Join(fmt.Errorf("session manager startup self-check failed: %w", err), shutdown())
	}
	stopNotices := s.host.StartSessionNoticeDelivery(serveCtx)
	defer stopNotices()
	s.healthy.Store(true)
	logger.Info("startup_healthy", map[string]any{"version": s.version, "pid": os.Getpid(), "instance": s.instance})
	phase = "serving"
	close(s.ready)
	select {
	case <-serveCtx.Done():
		phase = "stopping"
		return shutdown()
	case err := <-serveErr:
		phase = "serve_error"
		return err
	}
}
func (s *ControlServer) selfCheck(ctx context.Context, token string) error {
	checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	transport := &http.Transport{DialContext: func(c context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(c, "unix", s.host.options.Layout.SocketPath)
	}}
	defer transport.CloseIdleConnections()
	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, "http://session-manager/v1/status", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("control status self-check returned HTTP %d", response.StatusCode)
	}
	var got ControlStatus
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		return err
	}
	want := s.Status()
	if !got.Running || got.Protocol != want.Protocol || got.ProtocolVersion != want.ProtocolVersion || got.DaemonVersion != want.DaemonVersion || got.InstanceID != want.InstanceID || got.NamespaceFingerprint != want.NamespaceFingerprint || got.PID != want.PID {
		return fmt.Errorf("control endpoint returned a different daemon identity")
	}
	return nil
}
func (s *ControlServer) startupFailure(ctx context.Context, phase string, cause error) error {
	if ctx.Err() == nil {
		return cause
	}
	result := &StartupCancelledError{Phase: phase, Err: cause}
	if observed, ok := s.signals.Observed(); ok {
		result.Signal = &observed
	}
	return result
}

// Authentication failures preserve both existing response envelopes. Recognizing
// the reserved hosted prefix grants no capability and does not open its gate.
func controlUnauthorizedBody(path string) any {
	if strings.HasPrefix(path, "/v1/hosted/") {
		// Field order is part of the existing refusal wire image.
		return map[string]any{"error": struct {
			Schema        string   `json:"schema"`
			SchemaVersion string   `json:"schema_version"`
			Type          string   `json:"type"`
			Code          string   `json:"code"`
			Message       string   `json:"message"`
			Details       struct{} `json:"details"`
		}{"urn:relux:task-board:session-launch-status", "1.0.0", "refusal", "session_host_unavailable", "Session host is unavailable.", struct{}{}}}
	}
	return map[string]any{"error": map[string]string{"code": "unauthorized", "message": "valid manager bearer token is required"}}
}
