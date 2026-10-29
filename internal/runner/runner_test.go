package runner

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFixtureChild(t *testing.T) {
	switch os.Getenv("SESSION_HOST_FIXTURE") {
	case "echo":
		io.Copy(os.Stdout, os.Stdin)
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
