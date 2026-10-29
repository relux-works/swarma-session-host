package sessionclient

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// A silent established terminal observes its read deadline without stopping
// the stream: resetting the deadline restores I/O. Close produces peer EOF and
// never invents an HTTP stop/detach request. This is an owned socket fixture.
func TestTransportTerminalReadDeadlineAndClose(t *testing.T) {
	path := filepath.Join(compatibilitySocketDir(t), "deadline.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		conn, e := listener.Accept()
		if e != nil {
			done <- e
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		request, e := http.ReadRequest(bufio.NewReader(conn))
		if e != nil {
			done <- e
			return
		}
		_, e = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		if e != nil {
			done <- e
			return
		}
		if _, e = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n"+terminalStreamPreamble); e != nil {
			done <- e
			return
		}
		key := make([]byte, 3)
		if _, e = io.ReadFull(conn, key); e != nil {
			done <- e
			return
		}
		if string(key) != "key" {
			done <- errors.New("terminal input changed after deadline")
			return
		}
		if _, e = io.WriteString(conn, "tail"); e != nil {
			done <- e
			return
		}
		var b [1]byte
		n, e := conn.Read(b[:])
		if n != 0 || !errors.Is(e, io.EOF) {
			done <- errors.New("terminal Close did not produce peer EOF")
			return
		}
		done <- nil
	}()
	calls := 0
	c := New(Config{SocketPath: path, Token: "fixture", HTTPClient: &http.Client{Transport: wireRoundTripper(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("unexpected stop or detach")
	})}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := c.OpenTerminal(ctx, "s", "a", 24, 80)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err = stream.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	n, err := stream.Read(b[:])
	var timeout net.Error
	if n != 0 || !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("silent terminal did not time out: %d %v", n, err)
	}
	if err = stream.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Write([]byte("key")); err != nil {
		t.Fatal(err)
	}
	tail := make([]byte, 4)
	if _, err = io.ReadFull(stream, tail); err != nil || string(tail) != "tail" {
		t.Fatalf("deadline stopped terminal: %q %v", tail, err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("terminal Close sent %d unsolicited control calls", calls)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("owned terminal peer did not join")
	}
}
