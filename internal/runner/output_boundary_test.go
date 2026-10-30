package runner

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type gatedInput struct {
	childInput
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (g *gatedInput) Write(p []byte) (int, error) {
	g.once.Do(func() { close(g.entered) })
	<-g.resume
	return g.childInput.Write(p)
}

func boundaryFile(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
		t.Fatal(err)
	}
}
func awaitOutputEnd(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		ended := s.outputEnded
		s.mu.Unlock()
		if ended {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("output cancellation did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

// A deterministic gate pauses after durable reservation but before native I/O.
// EOF must cancel that unissued payload, preserving unknown across a controller
// replacement. Without EOF, the same payload reaches the real child once.
func TestOutputEndCancelsReservedWrite(t *testing.T) {
	for _, ended := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "ended"}[ended], func(t *testing.T) {
			cfg := testConfig(t, "gated-output-end")
			cfg.IOTimeout = 2 * time.Second
			dir := cfg.Plan.Cwd
			s, err := startTestRunner(t, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			gate := &gatedInput{childInput: s.stdin, entered: make(chan struct{}), resume: make(chan struct{})}
			s.mu.Lock()
			s.stdin = gate
			s.mu.Unlock()
			defer func() {
				select {
				case <-gate.resume:
				default:
					close(gate.resume)
				}
			}()
			owner := testClient(t, cfg)
			call(t, owner, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
			data := []byte("reserved-payload\n")
			result := make(chan Response, 1)
			errs := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				r, e := owner.Call(ctx, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: data})
				result <- r
				errs <- e
			}()
			select {
			case <-gate.entered:
			case <-time.After(time.Second):
				t.Fatal("reservation barrier not reached")
			}
			s.mu.Lock()
			reserved := s.writes[1].code
			s.mu.Unlock()
			if reserved != "write_unknown" {
				t.Fatal("barrier preceded reservation")
			}
			if ended {
				boundaryFile(t, dir, "end-output")
				awaitOutputEnd(t, s)
			}
			close(gate.resume)
			var r Response
			if ended {
				select {
				case r = <-result:
				case <-time.After(250 * time.Millisecond):
					t.Fatal("output end did not cancel pending stdin I/O")
				}
			} else {
				r = <-result
			}
			if e := <-errs; e != nil {
				t.Fatal(e)
			}
			want := "written"
			if ended {
				want = "write_unknown"
			}
			if r.Code != want {
				t.Fatalf("reserved effect after termination: got %s want %s", r.Code, want)
			}
			call(t, owner, Request{Op: "release", Execution: cfg.Execution, Epoch: 1}, "released")
			replacement := testClient(t, cfg)
			call(t, replacement, Request{Op: "claim", Execution: cfg.Execution, Epoch: 2}, "ok")
			req := receiptRequest(cfg, data)
			req.Epoch = 2
			for range 2 {
				if q := call(t, replacement, req, want); q.Writes != 1 {
					t.Fatal("receipt lookup repeated effect")
				}
			}
			if ended {
				call(t, replacement, Request{Op: "write", Execution: cfg.Execution, Epoch: 2, Sequence: 2, Data: []byte("forbidden\n")}, "write_refused")
			}
			boundaryFile(t, dir, "drain-input")
			call(t, replacement, Request{Op: "stop", Execution: cfg.Execution, Epoch: 2}, "stopped")
			received, err := os.ReadFile(filepath.Join(dir, "received-input"))
			if err != nil {
				t.Fatal(err)
			}
			expected := data
			if ended {
				expected = nil
			}
			if !bytes.Equal(received, expected) {
				t.Fatal("unissued payload reached child after EOF")
			}
		})
	}
}

// Fill the real owned pipe without a reader, then reserve a production write.
// EOF must cancel the backpressured write before the child starts draining;
// only the explicitly prefilled prefix may remain. Live output permits the
// pending payload when draining begins. This checks cancellation, not just flags.
func TestOutputEndCancelsBackpressuredWrite(t *testing.T) {
	for _, ended := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "ended"}[ended], func(t *testing.T) {
			cfg := testConfig(t, "gated-output-end")
			cfg.IOTimeout = 2 * time.Second
			dir := cfg.Plan.Cwd
			s, err := startTestRunner(t, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			prefix := []byte{}
			chunk := bytes.Repeat([]byte("p"), 4096)
			full := false
			for range 128 {
				s.stdin.SetWriteDeadline(time.Now().Add(5 * time.Millisecond))
				n, e := s.stdin.Write(chunk)
				prefix = append(prefix, chunk[:n]...)
				if e != nil {
					full = true
					break
				}
			}
			if !full {
				t.Fatal("bounded fixture could not establish backpressure")
			}
			gate := &gatedInput{childInput: s.stdin, entered: make(chan struct{}), resume: make(chan struct{})}
			close(gate.resume)
			s.mu.Lock()
			s.stdin = gate
			s.mu.Unlock()
			owner := testClient(t, cfg)
			call(t, owner, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
			data := bytes.Repeat([]byte("x"), 4096)
			result := make(chan Response, 1)
			errs := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				r, e := owner.Call(ctx, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: data})
				result <- r
				errs <- e
			}()
			select {
			case <-gate.entered:
			case <-time.After(time.Second):
				t.Fatal("write did not enter")
			}
			select {
			case <-result:
				t.Fatal("fixture did not retain backpressure")
			case <-time.After(30 * time.Millisecond):
			}
			if ended {
				boundaryFile(t, dir, "end-output")
				awaitOutputEnd(t, s)
			} else {
				boundaryFile(t, dir, "drain-input")
			}
			var r Response
			if ended {
				select {
				case r = <-result:
				case <-time.After(250 * time.Millisecond):
					t.Fatal("output end did not cancel pending stdin I/O")
				}
			} else {
				r = <-result
			}
			if e := <-errs; e != nil {
				t.Fatal(e)
			}
			want := "written"
			if ended {
				want = "write_unknown"
			}
			if r.Code != want {
				t.Fatalf("backpressure receipt: got %s want %s", r.Code, want)
			}
			call(t, owner, Request{Op: "release", Execution: cfg.Execution, Epoch: 1}, "released")
			replacement := testClient(t, cfg)
			call(t, replacement, Request{Op: "claim", Execution: cfg.Execution, Epoch: 2}, "ok")
			req := receiptRequest(cfg, data)
			req.Epoch = 2
			call(t, replacement, req, want)
			if ended {
				boundaryFile(t, dir, "drain-input")
				call(t, replacement, Request{Op: "write", Execution: cfg.Execution, Epoch: 2, Sequence: 2, Data: []byte("forbidden\n")}, "write_refused")
			}
			call(t, replacement, Request{Op: "stop", Execution: cfg.Execution, Epoch: 2}, "stopped")
			received, err := os.ReadFile(filepath.Join(dir, "received-input"))
			if err != nil {
				t.Fatal(err)
			}
			expected := append([]byte(nil), prefix...)
			if !ended {
				expected = append(expected, data...)
			}
			if !bytes.Equal(received, expected) {
				t.Fatalf("post-boundary bytes: got %d want %d", len(received), len(expected))
			}
		})
	}
}
