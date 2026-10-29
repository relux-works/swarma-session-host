package runner

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// A real accepted connection paused before registration cannot escape shutdown,
// even with a claim already queued and its client left open. The live variant
// claims normally; the closed variant also refuses direct controller admission.
func TestShutdownRejectsLateRegistration(t *testing.T) {
	for _, closing := range []bool{false, true} {
		t.Run(fmt.Sprint(closing), func(t *testing.T) {
			cfg := testConfig(t, "echo")
			s, err := Start(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			listener, err := localipc.Listen(filepath.Join(filepath.Dir(cfg.Socket), "late.sock"))
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			client, err := net.DialUnix("unix", nil, listener.Addr().(*net.UnixAddr))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			accepted, err := listener.AcceptUnix()
			if err != nil {
				t.Fatal(err)
			}
			defer accepted.Close()
			claim := Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}
			if err := localipc.Write(client, claim); err != nil {
				t.Fatal(err)
			}
			resume, registered := make(chan struct{}), make(chan bool, 1)
			// Model the accept loop's existing WaitGroup member while paused
			// between AcceptUnix and register, before any handler is counted.
			s.wg.Add(1)
			defer func() {
				select {
				case <-resume:
				default:
					close(resume)
				}
			}()
			go func() {
				defer s.wg.Done()
				<-resume
				ok := s.register(accepted)
				registered <- ok
				if ok {
					go s.handle(accepted)
				}
			}()
			closed := make(chan error, 1)
			if closing {
				go func() { closed <- s.Close() }()
				select {
				case <-s.closed:
				case <-time.After(time.Second):
					t.Fatal("shutdown did not start")
				}
			}
			close(resume)
			select {
			case ok := <-registered:
				if ok == closing {
					t.Fatalf("registration=%v during closed=%v", ok, closing)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("registration did not return")
			}
			if !closing {
				client.SetReadDeadline(time.Now().Add(time.Second))
				var response Response
				if err := localipc.Read(bufio.NewReader(client), &response); err != nil || response.Code != "ok" {
					t.Fatalf("live admission: %+v %v", response, err)
				}
				go func() { closed <- s.Close() }()
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown hung with client open")
			}
			if response := s.process(accepted, claim); response.Code != "runner_closed" {
				t.Fatalf("post-close claim: %+v", response)
			}
		})
	}
}

func TestFixtureChild(t *testing.T) {
	switch os.Getenv("SESSION_HOST_FIXTURE") {
	case "echo":
		io.Copy(os.Stdout, os.Stdin)
		os.Exit(0)
	case "eof-live":
		os.Stdout.Close()
		io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	case "flood":
		os.Stdout.Write(bytes.Repeat([]byte("x"), MaxOutput+4096))
		io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	case "ignore-eof":
		io.Copy(io.Discard, os.Stdin)
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
}

// A fixture that ignores stdin EOF takes the bounded forced-stop path. Repeated
// stop after reap is inert and cannot affect another independently owned child.
// This exercises the serialized lifecycle; it does not force kernel PID reuse.
func TestForcedStopAndReapedHandle(t *testing.T) {
	cfg := testConfig(t, "ignore-eof")
	cfg.IOTimeout = 100 * time.Millisecond
	server, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	otherCfg := testConfig(t, "echo")
	other, err := Start(otherCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { other.Close() })
	client := testClient(t, cfg)
	call(t, client, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	first := call(t, client, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
	second := call(t, client, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
	if !first.Exited || first.ExitCode != -1 || second.PID != first.PID || !second.Exited {
		t.Fatalf("forced/repeated stop: %+v %+v", first, second)
	}
	otherClient := testClient(t, otherCfg)
	status := call(t, otherClient, Request{Op: "status", Execution: otherCfg.Execution}, "ok")
	if status.Exited || status.PID == first.PID {
		t.Fatal("reaped stop affected another child")
	}
}

func testConfig(t *testing.T, fixture string) Config {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("/tmp", "session-runner-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return Config{Socket: filepath.Join(dir, "r.sock"), Execution: "fixture-execution", IOTimeout: time.Second,
		Plan: Plan{Binary: binary, Args: []string{"-test.run=^TestFixtureChild$"}, Cwd: dir,
			Env: []string{"SESSION_HOST_FIXTURE=" + fixture, "GORACE=atexit_sleep_ms=0"}}}
}

func testClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := Dial(ctx, cfg.Socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func call(t *testing.T, client *Client, req Request, code string) Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := client.Call(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != code {
		t.Fatalf("%s: got %s; expected %s", req.Op, response.Code, code)
	}
	return response
}

// A live fixture controller can write once; competing/stale/wrong-execution
// requests and changed-data retries do not add bytes to this child's output.
func TestControllerFenceAndDeduplication(t *testing.T) {
	cfg := testConfig(t, "echo")
	server, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	owner, competitor := testClient(t, cfg), testClient(t, cfg)
	claim := Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}
	first := call(t, owner, claim, "ok")
	claim.Epoch = 500
	call(t, competitor, claim, "controller_conflict")
	write := Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("one\n")}
	call(t, competitor, write, "stale_controller")
	write.Execution = "other"
	call(t, owner, write, "wrong_execution")
	write.Execution, write.Epoch = cfg.Execution, 0
	call(t, owner, write, "stale_controller")
	write.Epoch = 1
	call(t, owner, write, "written")
	call(t, owner, write, "written")
	write.Data = []byte("forbidden\n")
	call(t, owner, write, "sequence_conflict")
	call(t, owner, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
	read := call(t, owner, Request{Op: "read", Execution: cfg.Execution, Epoch: 1}, "ok")
	if string(read.Data) != "one\n" || !read.Exited || read.ExitCode != 0 || first.PID != read.PID {
		t.Fatalf("unexpected fixture result: %+v", read)
	}
}

// Closing a host connection leaves the same runner child alive; the next
// controller claims it without spawning a replacement and old epochs refuse.
func TestControllerReplacementPreservesChild(t *testing.T) {
	cfg := testConfig(t, "echo")
	server, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	owner := testClient(t, cfg)
	first := call(t, owner, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	owner.Close()
	next := testClient(t, cfg)
	// The previous reader has a bounded scheduling handoff, not an assumption
	// that close and server-side ownership release are an atomic operation.
	deadline := time.Now().Add(time.Second)
	var response Response
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		response, err = next.Call(ctx, Request{Op: "claim", Execution: cfg.Execution, Epoch: 2})
		cancel()
		if err != nil || response.Code == "ok" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil || response.Code != "ok" || response.PID != first.PID || response.Instance != first.Instance || response.Exited {
		t.Fatalf("re-adoption failed: %+v %v", response, err)
	}
	call(t, next, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("stale")}, "stale_controller")
	call(t, next, Request{Op: "write", Execution: cfg.Execution, Epoch: 2, Sequence: 1, Data: []byte("next\n")}, "written")
	call(t, next, Request{Op: "stop", Execution: cfg.Execution, Epoch: 2}, "stopped")
	got := call(t, next, Request{Op: "read", Execution: cfg.Execution, Epoch: 2}, "ok")
	if string(got.Data) != "next\n" {
		t.Fatalf("stale write escaped fence: %q", got.Data)
	}
}

// Output overflow closes writes and returns an explicit unavailable result;
// retained output stays bounded and status/owned stop remain reachable.
func TestOutputPressurePreservesControl(t *testing.T) {
	cfg := testConfig(t, "flood")
	server, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	client := testClient(t, cfg)
	call(t, client, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	deadline := time.Now().Add(time.Second)
	for {
		server.mu.Lock()
		overflow, changed := server.overflow, server.changed
		server.mu.Unlock()
		if overflow {
			break
		}
		select {
		case <-changed:
		case <-time.After(time.Until(deadline)):
			t.Fatal("fixture did not overflow")
		}
	}
	read := call(t, client, Request{Op: "read", Execution: cfg.Execution, Epoch: 1}, "output_unavailable")
	if !read.Overflow || read.Offset > MaxOutput || len(read.Data) != 0 {
		t.Fatalf("overflow misreported: %+v", read)
	}
	call(t, client, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("x")}, "write_refused")
	call(t, client, Request{Op: "status", Execution: cfg.Execution}, "ok")
	call(t, client, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
}

// Invalid plans and occupied endpoints refuse before a second child starts;
// the original endpoint/child remains usable as the positive control.
func TestStartRefusalsPreserveOriginal(t *testing.T) {
	cfg := testConfig(t, "echo")
	bad := cfg
	bad.Plan.Binary = "relative-binary"
	if server, err := Start(bad); err == nil || server != nil {
		t.Fatal("relative binary accepted")
	}
	server, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	if duplicate, err := Start(cfg); err == nil || duplicate != nil {
		t.Fatal("occupied socket accepted")
	}
	client := testClient(t, cfg)
	call(t, client, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	got := call(t, client, Request{Op: "status", Execution: cfg.Execution}, "ok")
	if got.Exited || !strings.HasSuffix(cfg.Socket, "r.sock") {
		t.Fatal("original runner was disturbed")
	}
}

// Output closure is independently observable while the child still owns live
// stdin. Reads wake at the empty ended tail; new writes have no reserved effect.
// The ordinary echo child supplies a valid output/write control.
func TestOutputEndClosesWritesBeforeProcessExit(t *testing.T) {
	for _, ended := range []bool{false, true} {
		t.Run(fmt.Sprint(ended), func(t *testing.T) {
			mode := "echo"
			if ended {
				mode = "eof-live"
			}
			cfg := testConfig(t, mode)
			s, err := Start(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			client := testClient(t, cfg)
			call(t, client, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
			if ended {
				deadline := time.Now().Add(time.Second)
				for {
					status := call(t, client, Request{Op: "status", Execution: cfg.Execution}, "ok")
					if status.OutputEnded {
						if status.Exited {
							t.Fatal("child exited before output end test")
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("output end not observable")
					}
					time.Sleep(time.Millisecond)
				}
				read := call(t, client, Request{Op: "read", Execution: cfg.Execution, Epoch: 1}, "ok")
				if !read.OutputEnded || read.Exited || len(read.Data) != 0 {
					t.Fatal("empty ended read lost EOF")
				}
				refused := call(t, client, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("forbidden\n")}, "write_refused")
				if refused.Writes != 0 {
					t.Fatal("output end reserved a write")
				}
			} else {
				status := call(t, client, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("valid\n")}, "written")
				if status.OutputEnded {
					t.Fatal("live output falsely ended")
				}
				read := call(t, client, Request{Op: "read", Execution: cfg.Execution, Epoch: 1}, "ok")
				if string(read.Data) != "valid\n" || read.OutputEnded {
					t.Fatal("live output control failed")
				}
			}
			call(t, client, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
		})
	}
}

type failedOutput struct{}

func (failedOutput) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (failedOutput) Close() error             { return nil }

// A collector read failure has the same closed observation/write invariant as
// EOF; no fabricated process-exit claim is needed to unblock a tail read.
func TestCollectorReadFailureClosesTransport(t *testing.T) {
	s := &Server{changed: make(chan struct{}), outputDone: make(chan struct{}), closed: make(chan struct{}), writes: make(map[uint64]writeRecord)}
	s.cfg.IOTimeout = time.Second
	s.wg.Add(1)
	s.collect(failedOutput{})
	r := s.read(Request{})
	if !r.OutputEnded || r.Exited || r.Code != "ok" || !s.blocked {
		t.Fatal("read failure left transport eligible")
	}
}
