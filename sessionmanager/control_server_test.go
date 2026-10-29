//go:build unix

package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func controlFixture(t *testing.T) (*SessionHost, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	// A separate short task-local root avoids the platform sun_path bound when
	// the command's mandatory TMPDIR is long. Ordinary checkouts need no override.
	root := os.Getenv("SESSIONHOST_TEST_SOCKET_ROOT")
	if root == "" {
		root = os.TempDir()
	}
	socketDir, err := os.MkdirTemp(root, "s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(socketDir); err != nil {
			t.Error(err)
		}
	})
	if len(filepath.Join(socketDir, "c.sock")) >= 104 {
		t.Fatal("use a shorter SESSIONHOST_TEST_SOCKET_ROOT for Unix socket fixtures")
	}
	dir := t.TempDir()
	l := Layout{Namespace: "fixture-namespace", StateDir: dir, SessionsDir: filepath.Join(dir, "sessions"), LockFile: filepath.Join(dir, "manager.lock"), SocketPath: filepath.Join(socketDir, "c.sock"), AttachmentsDir: filepath.Join(socketDir, "a"), EndpointFile: filepath.Join(dir, "endpoint.json"), TokenFile: filepath.Join(dir, "token"), OperatorLogFile: filepath.Join(dir, "logs", "sessiond.log")}
	h, err := NewHost(HostOptions{Layout: l, Config: hostCommitProbeConfig{}, Codec: &hostCommitProbeCodec{}, Notices: hostCommitProbeNotices{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h, ctx
}
func fixtureControlClient(t *testing.T, path string) *http.Client {
	t.Helper()
	transport := &http.Transport{DialContext: func(c context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(c, "unix", path)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport}
}
func fixtureControlRequest(t *testing.T, ctx context.Context, c *http.Client, method, path, token string) (int, []byte) {
	t.Helper()
	r, err := http.NewRequestWithContext(ctx, method, "http://session-manager"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := c.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}
func fixtureControlJoined(t *testing.T, ctx context.Context, result <-chan error) error {
	t.Helper()
	select {
	case e := <-result:
		return e
	case <-ctx.Done():
		t.Fatal("fixture server did not join")
		return ctx.Err()
	}
}
func assertControlUnpublished(t *testing.T, h *SessionHost) {
	t.Helper()
	for _, path := range []string{h.options.Layout.SocketPath, h.options.Layout.EndpointFile, h.options.Layout.TokenFile} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("control runtime path was retained: %s: %v", filepath.Base(path), err)
		}
	}
	if !h.closeReleased {
		t.Fatal("control owner did not release its lifetime lock")
	}
}

// Actual isolated Unix HTTP and the real host lifetime/lock run here. Auth
// failures never reach the converter; a matching request and acknowledged drain
// do. This does not qualify a product executable or a provider's process.
func TestControlServerIdentityAuthenticationAndJoinedDrain(t *testing.T) {
	h, ctx := controlFixture(t)
	var calls atomic.Int32
	server, err := NewControlServer(h, func(s *ControlServer) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); controlJSON(w, http.StatusOK, s.Status()) })
	}, WithControlServerVersion("fixture-version"))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- server.Serve(ctx) }()
	joined := false
	t.Cleanup(func() {
		server.Drain()
		if !joined {
			_ = fixtureControlJoined(t, ctx, result)
		}
	})
	select {
	case <-server.Ready():
	case e := <-result:
		t.Fatalf("startup: %v", e)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer server.Drain()
	l := h.options.Layout
	client := fixtureControlClient(t, l.SocketPath)
	// This token is created solely by this isolated fixture, never real auth data.
	raw, err := os.ReadFile(l.TokenFile)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		t.Fatal("fixture token absent")
	}
	metadata, err := os.ReadFile(l.EndpointFile)
	if err != nil {
		t.Fatal(err)
	}
	var endpoint endpointMetadata
	if err := json.Unmarshal(metadata, &endpoint); err != nil {
		t.Fatal(err)
	}
	status := server.Status()
	if !status.StartupHealthy || endpoint.InstanceID != status.InstanceID || endpoint.NamespaceFingerprint != string(l.Namespace) || endpoint.ProtocolVersion != 8 || endpoint.SocketPath != l.SocketPath {
		t.Fatal("published identity does not describe its actual server")
	}
	for _, p := range []string{l.TokenFile, l.EndpointFile, l.SocketPath} {
		info, err := os.Stat(p)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("published control path is not private")
		}
	}
	before := calls.Load()
	for _, row := range []struct{ path, token, code string }{{"/v1/status", "", "unauthorized"}, {"/v1/status", "foreign", "unauthorized"}, {"/v1/hosted/launch-plan", "foreign", "session_host_unavailable"}} {
		code, body := fixtureControlRequest(t, ctx, client, http.MethodGet, row.path, row.token)
		var envelope struct {
			Error struct {
				Code   string `json:"code"`
				Schema string `json:"schema"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		if code != http.StatusUnauthorized || envelope.Error.Code != row.code || calls.Load() != before {
			t.Fatalf("unauthorized request changed wire refusal or reached converter: status=%d code=%q calls=%d before=%d", code, envelope.Error.Code, calls.Load(), before)
		}
		if strings.HasPrefix(row.path, "/v1/hosted/") && envelope.Error.Schema != "urn:relux:task-board:session-launch-status" {
			t.Fatal("reserved refusal envelope changed")
		}
	}
	code, _ := fixtureControlRequest(t, ctx, client, http.MethodGet, "/v1/status", token)
	if code != http.StatusOK || calls.Load() != before+1 {
		t.Fatal("authorized positive control did not reach converter")
	}
	code, body := fixtureControlRequest(t, ctx, client, http.MethodPost, "/v1/daemon/drain", token)
	if code != http.StatusAccepted || !strings.Contains(string(body), `"draining":true`) {
		t.Fatal("drain acknowledgement missing")
	}
	err = fixtureControlJoined(t, ctx, result)
	joined = true
	if err != nil {
		t.Fatal(err)
	}
	assertControlUnpublished(t, h)
	log, err := os.ReadFile(l.OperatorLogFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{"daemon_starting", "daemon_started", "startup_healthy", "daemon_stopped"} {
		if !strings.Contains(string(log), `"event":"`+event+`"`) {
			t.Fatalf("missing actual lifecycle event %s", event)
		}
	}
}

// A converter that returns a foreign identity cannot make startup healthy.
// Failed self-check joins serving, removes only its own publications and closes
// the host. No native child or live daemon is involved.
func TestControlServerForeignIdentityNeverBecomesReady(t *testing.T) {
	h, ctx := controlFixture(t)
	server, err := NewControlServer(h, func(s *ControlServer) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			status := s.Status()
			status.NamespaceFingerprint = "foreign"
			controlJSON(w, http.StatusOK, status)
		})
	}, WithControlServerVersion("fixture"))
	if err != nil {
		t.Fatal(err)
	}
	err = server.Serve(ctx)
	if err == nil || !strings.Contains(err.Error(), "different daemon identity") {
		t.Fatalf("foreign identity: %v", err)
	}
	select {
	case <-server.Ready():
		t.Fatal("foreign identity became ready")
	default:
	}
	assertControlUnpublished(t, h)
}

// A foreign non-socket path is never unlinked, and startup cancellation records
// the actual parent error without manufacturing an observed signal.
func TestControlServerRefusesForeignPathAndInventedSignal(t *testing.T) {
	t.Run("foreign-file", func(t *testing.T) {
		h, ctx := controlFixture(t)
		path := h.options.Layout.SocketPath
		if err := os.WriteFile(path, []byte("preserve"), 0600); err != nil {
			t.Fatal(err)
		}
		var calls int
		server, err := NewControlServer(h, func(*ControlServer) http.Handler { calls++; return http.NotFoundHandler() }, WithControlServerVersion("fixture"))
		if err != nil {
			t.Fatal(err)
		}
		err = server.Serve(ctx)
		var refusal *PublicError
		if !errors.As(err, &refusal) || refusal.Code != ErrorBoardlessSocketUnavailable || calls != 0 {
			t.Fatalf("foreign file: %v", err)
		}
		raw, e := os.ReadFile(path)
		if e != nil || string(raw) != "preserve" {
			t.Fatal("foreign path was changed")
		}
		for _, p := range []string{h.options.Layout.EndpointFile, h.options.Layout.TokenFile} {
			if _, e := os.Stat(p); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("failed startup published control credentials")
			}
		}
	})
	t.Run("cancelled-parent", func(t *testing.T) {
		h, _ := controlFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		server, err := NewControlServer(h, func(*ControlServer) http.Handler { return http.NotFoundHandler() }, WithControlServerVersion("fixture"))
		if err != nil {
			t.Fatal(err)
		}
		err = server.Serve(ctx)
		var cancelled *StartupCancelledError
		if !errors.Is(err, context.Canceled) || !errors.As(err, &cancelled) || cancelled.Signal != nil {
			t.Fatalf("cancellation attribution: %v", err)
		}
		assertControlUnpublished(t, h)
	})
}

// The two original unauthorized response images retain their exact field order,
// fixed vocabulary and newline. Auth rejects before the converter; the same
// wrapper still admits its matching synthetic bearer. No live credential exists.
func TestControlAuthenticationRefusalPreservesWireImage(t *testing.T) {
	for _, row := range []struct{ name, path, want string }{
		{"legacy", "/v1/status", "{\"error\":{\"code\":\"unauthorized\",\"message\":\"valid manager bearer token is required\"}}\n"},
		{"reserved", "/v1/hosted/launch-plan", "{\"error\":{\"schema\":\"urn:relux:task-board:session-launch-status\",\"schema_version\":\"1.0.0\",\"type\":\"refusal\",\"code\":\"session_host_unavailable\",\"message\":\"Session host is unavailable.\",\"details\":{}}}\n"},
	} {
		t.Run(row.name, func(t *testing.T) {
			calls := 0
			server := &ControlServer{}
			handler := server.authenticated("fixture-bearer", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("accepted"))
			}))
			request := httptest.NewRequest(http.MethodGet, row.path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized || response.Body.String() != row.want || calls != 0 {
				t.Fatalf("unauthorized wire image changed: status=%d calls=%d body=%q want=%q", response.Code, calls, response.Body.String(), row.want)
			}
			request.Header.Set("Authorization", "Bearer fixture-bearer")
			response = httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusAccepted || response.Body.String() != "accepted" || calls != 1 {
				t.Fatal("matching bearer positive control was refused")
			}
		})
	}
}
