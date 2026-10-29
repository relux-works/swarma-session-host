package sessionclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Socket fixtures reuse the repository's bounded path owner so task-local
// TMPDIR length cannot turn a wire assertion into an unrelated bind failure.
func compatibilitySocketDir(t *testing.T) string {
	t.Helper()
	return filepath.Dir(ShortSocketPath(t))
}

// Both ordinary and deliberately long TMPDIRs bind a private fixture socket;
// no daemon/provider is started and the shared helper owns directory cleanup.
func TestClientCompatibilitySocketDirIgnoresLongTMPDIR(t *testing.T) {
	for _, suffix := range []string{"short", strings.Repeat("long", 40)} {
		t.Run(suffix[:5], func(t *testing.T) {
			root := filepath.Join(t.TempDir(), suffix)
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("TMPDIR", root)
			path := filepath.Join(compatibilitySocketDir(t), "actual.sock")
			if len(path) >= 104 {
				t.Fatalf("fixture socket exceeds portable bound: %d", len(path))
			}
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func compatibilityEndpoint(t *testing.T, dir string) (EndpointFiles, Endpoint) {
	t.Helper()
	layout := EndpointFiles{Namespace: "fixture-namespace", EndpointFile: filepath.Join(dir, "endpoint.json"), TokenFile: filepath.Join(dir, "token")}
	meta := Endpoint{Namespace: "fixture-namespace", SocketPath: filepath.Join(dir, "actual.sock")}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(layout.EndpointFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(layout.TokenFile, []byte("  public-fixture-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return layout, meta
}

// Dial selects the recorded socket (not the caller's stale hint), trims the
// private token and sends a real authenticated HTTP request over Unix. Close
// releases its actual idle socket and may be called again safely. The
// server is an isolated control fixture, not a daemon/provider launch.
func TestClientCompatibilityDialUsesRecordedSocketAndPrivateToken(t *testing.T) {
	dir := compatibilitySocketDir(t)
	layout, meta := compatibilityEndpoint(t, dir)
	// Recorded endpoint is the only socket source.
	listener, err := net.Listen("unix", meta.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan string, 1)
	idle, closed := make(chan struct{}, 1), make(chan struct{}, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- r.Header.Get("Authorization")
		if r.Method != "GET" || r.URL.Path != "/v1/status" {
			http.Error(w, "wrong route", 400)
			return
		}
		_, _ = io.WriteString(w, `{"pid":23}`)
	}), ConnState: func(_ net.Conn, state http.ConnState) {
		if state == http.StateIdle {
			idle <- struct{}{}
		}
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	})
	c, err := Dial(layout)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var status struct {
		PID int `json:"pid"`
	}
	err = c.DoRequest(ctx, "GET", "/v1/status", nil, &status, true)
	if err != nil || status.PID != 23 {
		t.Fatalf("recorded Unix endpoint failed: %+v / %v", status, err)
	}
	if got := <-observed; got != "Bearer public-fixture-token" {
		t.Fatal("private token was not trimmed into exact bearer authentication")
	}
	select {
	case <-idle:
	case <-ctx.Done():
		t.Fatal("positive request did not leave an idle socket")
	}
	c.Close()
	c.Close()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("client Close left owned idle socket open")
	}
}

// Each malformed local authority artifact is refused before a control client
// can issue any request; the real 0600/socket test above is its positive pair.
func TestClientCompatibilityDialRejectsWrongNamespaceAndPrivateModes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*EndpointFiles, *Endpoint)
		want    string
		missing bool
	}{
		{name: "foreign_namespace", mutate: func(_ *EndpointFiles, m *Endpoint) { m.Namespace = "foreign" }, want: "different namespace"},
		{name: "empty_socket", mutate: func(_ *EndpointFiles, m *Endpoint) { m.SocketPath = " " }, want: "socket is missing"},
		{name: "endpoint_permissions", mutate: func(l *EndpointFiles, _ *Endpoint) {
			if err := os.Chmod(l.EndpointFile, 0644); err != nil {
				t.Fatal(err)
			}
		}, want: "mode 0600"},
		{name: "token_permissions", mutate: func(l *EndpointFiles, _ *Endpoint) {
			if err := os.Chmod(l.TokenFile, 0644); err != nil {
				t.Fatal(err)
			}
		}, want: "mode 0600"},
		{name: "empty_token", mutate: func(l *EndpointFiles, _ *Endpoint) {
			if err := os.WriteFile(l.TokenFile, []byte(" \n"), 0600); err != nil {
				t.Fatal(err)
			}
		}, want: "token is empty"},
		{name: "missing_token", mutate: func(l *EndpointFiles, _ *Endpoint) {
			if err := os.Remove(l.TokenFile); err != nil {
				t.Fatal(err)
			}
		}, missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := compatibilitySocketDir(t)
			layout, meta := compatibilityEndpoint(t, dir)
			tc.mutate(&layout, &meta)
			if tc.name == "foreign_namespace" || tc.name == "empty_socket" {
				data, _ := json.Marshal(meta)
				if err := os.WriteFile(layout.EndpointFile, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			c, err := Dial(layout)
			if c != nil || err == nil {
				t.Fatalf("invalid local endpoint admitted: client=%v error=%v", c, err)
			}
			if tc.missing {
				if !errors.Is(err, ErrManagerNotRunning) {
					t.Fatalf("missing token changed sentinel: %v", err)
				}
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wrong endpoint rejection: %v want=%s", err, tc.want)
			}
		})
	}
}

// The dedicated terminal connection preserves exact HTTP/body/auth, verifies
// the pinned preamble, retains already-buffered provider bytes, and supports
// raw writes, read deadlines and Close. Bad preamble and typed refusal must
// never return a usable stream. All sockets are owned isolated fixtures.
func TestClientCompatibilityTerminalHandshakeAndRefusal(t *testing.T) {
	for _, mode := range []string{"valid", "wrong_preamble", "typed_refusal"} {
		t.Run(mode, func(t *testing.T) {
			dir := compatibilitySocketDir(t)
			path := filepath.Join(dir, "terminal.sock")
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				r, err := http.ReadRequest(bufio.NewReader(conn))
				if err != nil {
					done <- err
					return
				}
				data, err := io.ReadAll(r.Body)
				_ = r.Body.Close()
				if err != nil {
					done <- err
					return
				}
				var body map[string]any
				err = json.Unmarshal(data, &body)
				if err != nil || r.Method != "POST" || r.URL.Path != "/v1/sessions/s/terminal" || r.Header.Get("Authorization") != "Bearer public-fixture-token" || r.Header.Get("Content-Type") != "application/json" || body["attachment_id"] != "a" || body["rows"] != float64(24) || body["cols"] != float64(80) {
					done <- fmt.Errorf("terminal wire changed: %s %s %s", r.Method, r.URL, data)
					return
				}
				if mode == "typed_refusal" {
					_, err = io.WriteString(conn, "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"+`{"error":{"code":"attachment_missing","message":"fixture refusal"}}`)
					done <- err
					return
				}
				preamble := terminalStreamPreamble
				if mode == "wrong_preamble" {
					preamble = strings.Repeat("x", len(preamble))
				}
				_, err = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n"+preamble+"buffered-output")
				if err != nil {
					done <- err
					return
				}
				if mode != "valid" {
					done <- nil
					return
				}
				input := make([]byte, 3)
				_, err = io.ReadFull(conn, input)
				if err == nil && string(input) != "key" {
					err = fmt.Errorf("raw terminal input changed: %q", input)
				}
				if err == nil {
					_, err = io.WriteString(conn, "tail")
				}
				done <- err
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			c := &Client{SocketPath: path, Token: "public-fixture-token"}
			stream, err := c.OpenTerminal(ctx, "s", "a", 24, 80)
			if stream != nil {
				t.Cleanup(func() { _ = stream.Close() })
			}
			if mode == "valid" {
				if err != nil || stream == nil {
					t.Fatalf("valid terminal refused: %v", err)
				}
				if err = stream.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				data := make([]byte, len("buffered-output"))
				if _, err = io.ReadFull(stream, data); err != nil || string(data) != "buffered-output" {
					t.Fatalf("buffered terminal bytes lost: %q / %v", data, err)
				}
				if _, err = stream.Write([]byte("key")); err != nil {
					t.Fatal(err)
				}
				tail := make([]byte, 4)
				if _, err = io.ReadFull(stream, tail); err != nil || string(tail) != "tail" {
					t.Fatalf("raw terminal tail lost: %q / %v", tail, err)
				}
				if err = stream.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if stream != nil || err == nil {
					t.Fatalf("invalid terminal gained stream: %v / %v", stream, err)
				}
				if mode == "typed_refusal" {
					var e *PublicError
					if !errors.As(err, &e) || e.Code != "attachment_missing" || e.StatusCode != 409 {
						t.Fatalf("terminal public refusal changed: %v", err)
					}
				} else if !strings.Contains(err.Error(), "preamble mismatch") {
					t.Fatalf("terminal preamble rejection changed: %v", err)
				}
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(60 * time.Second):
				t.Fatal("owned terminal fixture did not join")
			}
		})
	}
	var absent *Client
	if stream, err := absent.OpenTerminal(context.Background(), "s", "a", 24, 80); stream != nil || !errors.Is(err, ErrManagerNotRunning) {
		t.Fatalf("absent terminal transport changed refusal: %v / %v", stream, err)
	}
}
