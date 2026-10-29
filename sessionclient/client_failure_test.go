package sessionclient

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Ordinary non-JSON errors preserve the legacy status/text fallback and close
// the response body. Valid HTTP responses are paired in the literal wire table.
func TestClientCompatibilityUnstructuredHTTPError(t *testing.T) {
	body := &wireBody{Reader: strings.NewReader(" \nupstream fixture refused\t ")}
	calls := 0
	c := &Client{HTTPClient: &http.Client{Transport: wireRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 503, Body: body, Header: make(http.Header), Request: r}, nil
	})}}
	err := c.DoRequest(context.Background(), "GET", "/v1/status", nil, nil, true)
	var public *PublicError
	if err == nil || err.Error() != "manager control request failed (503): upstream fixture refused" || errors.As(err, &public) || calls != 1 || !body.closed {
		t.Fatalf("unstructured HTTP failure lost legacy fallback or cleanup: err=%v calls=%d closed=%t", err, calls, body.closed)
	}
}

// A peer that refuses the remaining request bytes forces request.Write itself
// to fail. After OpenTerminal returns, peer writes must see the client's close;
// disabling GC prevents a finalizer from disguising a missing explicit close.
// The existing terminal handshake test is the adjacent valid control.
func TestClientCompatibilityTerminalRequestWriteFailureClosesSocket(t *testing.T) {
	previousGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGC)
	path := filepath.Join(compatibilitySocketDir(t), "write-failure.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	returned := make(chan struct{})
	served := make(chan error, 1)
	go func() {
		conn, e := listener.Accept()
		if e != nil {
			served <- e
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, e = io.ReadFull(conn, make([]byte, 1)); e != nil {
			served <- e
			return
		}
		if e = conn.(*net.UnixConn).CloseRead(); e != nil {
			served <- e
			return
		}
		// Drain bytes already queued before SHUT_RD. Otherwise a writer that
		// filled the Unix send buffer can remain asleep despite the half-close.
		// This EOF is local shutdown evidence, not the client-close assertion.
		if _, e = io.Copy(io.Discard, conn); e != nil {
			served <- e
			return
		}
		select {
		case <-returned:
		case <-time.After(60 * time.Second):
			served <- errors.New("terminal request did not return after peer read shutdown")
			return
		}
		_, e = conn.Write([]byte("closed-client witness"))
		if !errors.Is(e, syscall.EPIPE) && !errors.Is(e, syscall.ECONNRESET) {
			served <- errors.New("terminal send failure left dedicated socket open")
			return
		}
		served <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Exceed the Unix send buffer, so the peer's half-close happens during Write.
	stream, err := (&Client{SocketPath: path, Token: "fixture"}).OpenTerminal(ctx, "s", strings.Repeat("a", 8<<20), 24, 80)
	close(returned)
	if stream != nil {
		_ = stream.Close()
		t.Error("terminal send failure returned stream")
	}
	// net/http's request writer may render the nested write error as text.
	if err == nil || !strings.HasPrefix(err.Error(), "sending terminal request: ") || !strings.Contains(err.Error(), syscall.EPIPE.Error()) {
		t.Errorf("terminal request did not fail at the owned write: %v", err)
	}
	select {
	case e := <-served:
		if e != nil {
			t.Error(e)
		}
	case <-time.After(60 * time.Second):
		t.Error("terminal send failure fixture did not join")
	}
}
