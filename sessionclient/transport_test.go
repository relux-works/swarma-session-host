package sessionclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Ordinary requests retain exact route/auth/body bytes, response cleanup and
// short versus caller-owned deadlines. Raw payload fields remain opaque.
func TestTransportWireAndDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name              string
		bounded, deadline bool
	}{
		{"default", true, false}, {"long", false, false}, {"explicit_short", true, true}, {"explicit_long", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var cancel context.CancelFunc
			if tc.deadline {
				ctx, cancel = context.WithTimeout(ctx, time.Minute)
				defer cancel()
			}
			calls := 0
			body := &wireBody{Reader: strings.NewReader(`{"opaque":{"nested":true}}`)}
			c := New(Config{Token: "fixture", HTTPClient: &http.Client{Transport: wireRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != "POST" || r.URL.String() != "http://session-manager/v1/sessions/attach" || r.Header.Get("Authorization") != "Bearer fixture" || r.Header.Get("Content-Type") != "application/json" {
					t.Fatalf("wire changed: %s %s", r.Method, r.URL)
				}
				data, e := io.ReadAll(r.Body)
				if e != nil || string(data) != `{"opaque":{"unknown_field":42}}` {
					t.Fatalf("opaque payload changed: %s %v", data, e)
				}
				d, ok := r.Context().Deadline()
				if tc.deadline {
					want, _ := ctx.Deadline()
					if !ok || d != want {
						t.Fatal("caller deadline replaced")
					}
				} else if tc.bounded {
					if !ok || time.Until(d) < 9*time.Second || time.Until(d) > 11*time.Second {
						t.Fatal("default deadline lost")
					}
				} else if ok {
					t.Fatal("long request gained deadline")
				}
				return &http.Response{StatusCode: 200, Body: body, Request: r}, nil
			})}})
			var out json.RawMessage
			if err := c.DoRequest(ctx, "POST", "/v1/sessions/attach", json.RawMessage(`{"opaque":{"unknown_field":42}}`), &out, tc.bounded); err != nil {
				t.Fatal(err)
			}
			if string(out) != `{"opaque":{"nested":true}}` || calls != 1 || !body.closed {
				t.Fatalf("response lost/duplicated/leaked: %s %d %t", out, calls, body.closed)
			}
		})
	}
}

// The raw response keeps the default context live through decoding and closes
// it only when its codec owner closes the body. JSON/refusal errors close too.
func TestTransportResponseOwnershipAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		want    string
		upgrade bool
	}{
		{"valid", 200, `{"pid":2}`, "", false},
		{"decoder", 200, `{`, "decoding manager response", false},
		{"typed", 409, `{"error":{"code":"busy","message":"safe refusal"}}`, "busy (409): safe refusal", false},
		{"unknown_field", 400, `unknown JSON field PRIVATE-FIXTURE`, "protocol upgrade is required", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &wireBody{Reader: strings.NewReader(tc.body)}
			c := New(Config{HTTPClient: &http.Client{Transport: wireRoundTripper(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Body: body, Request: r}, nil
			})}})
			var out any
			err := c.DoRequest(nil, "GET", "/v1/status", nil, &out, true)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wrong error: %v", err)
			}
			if tc.upgrade && (!errors.Is(err, ErrManagerProtocolUpgradeRequired) || strings.Contains(err.Error(), "PRIVATE-FIXTURE")) {
				t.Fatal("decoder diagnostic escaped")
			}
			if tc.name == "typed" {
				var e *PublicError
				if !errors.As(err, &e) || e.StatusCode != 409 || e.Code != "busy" {
					t.Fatal("typed error lost")
				}
			}
			if !body.closed {
				t.Fatal("response body leaked")
			}
		})
	}
	var observed context.Context
	body := &wireBody{Reader: strings.NewReader(`{}`)}
	c := New(Config{HTTPClient: &http.Client{Transport: wireRoundTripper(func(r *http.Request) (*http.Response, error) {
		observed = r.Context()
		return &http.Response{StatusCode: 200, Body: body, Request: r}, nil
	})}})
	response, err := c.RoundTrip(nil, "POST", "/v1/fixture", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Err() != nil {
		t.Fatal("response context cancelled before codec read")
	}
	if err = response.Body.Close(); err != nil || !body.closed || observed.Err() != context.Canceled {
		t.Fatalf("body ownership lost: %v %v", err, observed.Err())
	}
	var absent *Client
	if err = absent.DoRequest(nil, "GET", "/v1/status", nil, nil, true); !errors.Is(err, ErrManagerNotRunning) {
		t.Fatal(err)
	}
}

// Version vectors independently pin protocol8, identifier fallback, build
// identity and the rule that newer peers never become replacement targets.
func TestTransportProtocolCompatibility(t *testing.T) {
	if ProtocolVersion != 8 || ProtocolIdentifier(8) != "task-board-session-manager-v8" {
		t.Fatal("published protocol bytes changed")
	}
	for _, tc := range []struct {
		name        string
		status      ProtocolStatus
		wrapper     string
		ok, replace bool
	}{
		{"same", ProtocolStatus{ProtocolVersion: 8, DaemonVersion: "a"}, "a", true, false},
		{"identifier", ProtocolStatus{Protocol: "task-board-session-manager-v8"}, "", true, false},
		{"older", ProtocolStatus{ProtocolVersion: 7}, "", false, true},
		{"missing", ProtocolStatus{}, "", false, true},
		{"newer", ProtocolStatus{ProtocolVersion: 9}, "", false, false},
		{"foreign_identifier", ProtocolStatus{ProtocolVersion: 8, Protocol: "other-v8"}, "", false, false},
		{"skew", ProtocolStatus{ProtocolVersion: 8, DaemonVersion: "a"}, "b", false, true},
		{"unknown_wrapper", ProtocolStatus{ProtocolVersion: 8, DaemonVersion: "a"}, "", true, false},
		{"development", ProtocolStatus{ProtocolVersion: 8, DaemonVersion: "dev"}, "b", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckCompatibility(tc.status, tc.wrapper)
			if (err == nil) != tc.ok {
				t.Fatalf("compatibility: %v", err)
			}
			var upgrade *ProtocolUpgradeError
			replacement := errors.As(err, &upgrade) && upgrade.RequiresReplacement()
			if replacement != tc.replace {
				t.Fatalf("replacement: %v", err)
			}
		})
	}
	for _, s := range []string{"other-v8", "task-board-session-manager-v0", "task-board-session-manager-vno"} {
		if ProtocolVersionFromIdentifier(s) != 0 {
			t.Fatal("invalid identifier accepted")
		}
	}
}

// An existing Unix fixture performs the actual no-launch handshake. Wrong
// protocol, unhealthy startup and cancelled callers return no usable client.
func TestTransportConnectExistingEndpoint(t *testing.T) {
	for _, mode := range []string{"healthy", "unhealthy", "older", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			dir := compatibilitySocketDir(t)
			files, meta := compatibilityEndpoint(t, dir)
			listener, err := net.Listen("unix", meta.SocketPath)
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/status" || r.Header.Get("Authorization") != "Bearer public-fixture-token" {
					t.Error("handshake route/auth changed")
				}
				version := 8
				if mode == "older" {
					version = 7
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"protocol_version": version, "startup_healthy": mode != "unhealthy"})
			})}
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			defer func() {
				_ = server.Close()
				if e := <-done; !errors.Is(e, http.ErrServerClosed) {
					t.Error(e)
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			client, err := Connect(ctx, files, "")
			if mode == "healthy" {
				if err != nil || client == nil {
					t.Fatal(err)
				}
				client.Close()
			} else {
				if err == nil || client != nil {
					t.Fatalf("invalid endpoint admitted: %v", err)
				}
				if mode == "unhealthy" && !errors.Is(err, ErrManagerStartupUnhealthy) {
					t.Fatal(err)
				}
			}
		})
	}
	files := EndpointFiles{EndpointFile: filepath.Join(t.TempDir(), "absent")}
	if c, e := Connect(nil, files, ""); c != nil || !errors.Is(e, ErrManagerNotRunning) {
		t.Fatalf("missing endpoint changed: %v", e)
	}
}

// Resize and detach keep their exact wire payloads. A cancelled terminal dial
// yields no stream; established stream cancellation remains caller-owned via
// SetReadDeadline/Close, as exercised by the moved handshake test.
func TestTransportGeometryDetachAndCancelledDial(t *testing.T) {
	var paths []string
	c := New(Config{Token: "fixture", HTTPClient: &http.Client{Transport: wireRoundTripper(func(r *http.Request) (*http.Response, error) {
		data, _ := io.ReadAll(r.Body)
		paths = append(paths, r.URL.Path)
		want := `{"attachment_id":"a"}`
		if strings.HasSuffix(r.URL.Path, "/resize") {
			want = `{"attachment_id":"a","cols":80,"rows":24}`
		}
		if string(data) != want {
			t.Fatalf("geometry/detach payload changed: %s", data)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
	})}})
	if err := c.ResizeTerminal(context.Background(), "s", "a", 24, 80); err != nil {
		t.Fatal(err)
	}
	if err := c.DoRequest(context.Background(), "POST", "/v1/sessions/s/detach", map[string]any{"attachment_id": "a"}, nil, true); err != nil {
		t.Fatal(err)
	}
	if strings.Join(paths, ",") != "/v1/sessions/s/resize,/v1/sessions/s/detach" {
		t.Fatal(paths)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.SocketPath = filepath.Join(compatibilitySocketDir(t), "absent.sock")
	if stream, err := c.OpenTerminal(ctx, "s", "a", 24, 80); stream != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled dial admitted: %v", err)
	}
}

// Missing/malformed endpoint metadata refuses before Dial can read credentials
// or connect, while the moved private-mode test supplies the valid pair.
func TestTransportEndpointMalformedAndMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "endpoint.json")
	files := EndpointFiles{EndpointFile: path, Namespace: "fixture"}
	if _, err := ReadEndpoint(files); !errors.Is(err, ErrManagerNotRunning) {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if c, err := Dial(files); c != nil || err == nil || !strings.Contains(err.Error(), "decoding manager endpoint metadata") {
		t.Fatalf("malformed metadata admitted: %v", err)
	}
}
