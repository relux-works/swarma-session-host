//go:build darwin && cgo

package runner

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// Optional qualification uses an explicitly supplied retained pre-identify
// binary. Ordinary tests require no historical checkout. This test starts only
// owned synthetic processes and proves unavailable plus continued protocol3
// control. Optional accept tracing qualifies no extra connection against an
// explicitly instrumented historical build; it never upgrades a hosted runner.
func TestIdentifyOldRunnerCompatibility(t *testing.T) {
	binary := os.Getenv("SESSION_HOST_OLD_RUNNER")
	if binary == "" {
		t.Skip("retained old binary supplied by separate compatibility qualification lane")
	}
	cfg := testConfig(t, "echo")
	plan, err := json.Marshal(cfg.Plan)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-socket", cfg.Socket, "-execution", cfg.Execution, "-io-timeout", "1s")
	cmd.Env = []string{}
	trace := ""
	if os.Getenv("SESSION_HOST_OLD_RUNNER_COUNT_ACCEPTS") == "1" {
		trace = filepath.Join(filepath.Dir(cfg.Socket), "accepts.log")
		if err := os.WriteFile(trace, nil, 0600); err != nil {
			t.Fatal(err)
		}
		cmd.Env = append(cmd.Env, "SESSION_HOST_OLD_RUNNER_ACCEPT_TRACE="+trace)
	}
	cmd.Stdin = bytes.NewReader(plan)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	resourceReceipt(t, "child-start", cmd.Process.Pid, "")
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	childPID := 0
	t.Cleanup(func() {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			<-done
			t.Error("IDENTIFY_OLD_RUNNER_CLEANUP")
		}
		for _, pid := range []int{cmd.Process.Pid, childPID} {
			if pid <= 0 {
				continue
			}
			limit := time.Now().Add(2 * time.Second)
			for syscall.Kill(pid, 0) != syscall.ESRCH && time.Now().Before(limit) {
				time.Sleep(time.Millisecond)
			}
			if err := syscall.Kill(pid, 0); err != syscall.ESRCH {
				t.Errorf("OWNED_CHILD_LEFTOVER: oldfixture%d %v", pid, err)
			}
			resourceReceipt(t, "child-end", pid, "")
		}
	})
	type ready struct {
		Code      string "json:\"code\""
		RunnerPID int    "json:\"runner_pid\""
		ChildPID  int    "json:\"child_pid\""
		Execution string "json:\"execution\""
	}
	readyResult := make(chan struct {
		r   ready
		err error
	}, 1)
	go func() {
		var r ready
		err := localipc.Read(bufio.NewReader(stdout), &r)
		readyResult <- struct {
			r   ready
			err error
		}{r, err}
	}()
	select {
	case result := <-readyResult:
		if result.err != nil || result.r.Code != "runner_ready" || result.r.RunnerPID != cmd.Process.Pid || result.r.ChildPID <= 0 {
			t.Fatalf("IDENTIFY_OLD_READY: %+v", result)
		}
		childPID = result.r.ChildPID
		resourceReceipt(t, "child-start", childPID, "")
	case <-time.After(3 * time.Second):
		t.Fatal("IDENTIFY_OLD_READY_TIMEOUT")
	}
	countAccepts := func() int {
		b, err := os.ReadFile(trace)
		if err != nil || !bytes.Equal(b, bytes.Repeat([]byte("accept\n"), bytes.Count(b, []byte("accept\n")))) {
			t.Fatalf("IDENTIFY_OLD_COUNTER_SETUP: %q %v", b, err)
		}
		return bytes.Count(b, []byte("accept\n"))
	}
	if trace != "" {
		// A real served connection proves the external trace is active before
		// relying on its absence to reject fallback traffic.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		probe, err := Dial(ctx, cfg.Socket)
		if err != nil {
			t.Fatal(err)
		}
		call(t, probe, Request{Op: "status", Execution: cfg.Execution}, "ok")
		probe.Close()
		if n := countAccepts(); n != 1 {
			t.Fatalf("IDENTIFY_OLD_COUNTER_SETUP: accepts=%d", n)
		}
	}
	ctx, cancel := identifyContext(t)
	defer cancel()
	req := identifyRequest{Schema: identifyRequestSchema, Execution: cfg.Execution, ExpectedInstance: identifyTestInstance}
	if _, err := identifyForQualification(ctx, cfg.Socket, cmd.Process.Pid, uint32(os.Getuid()), req); !errors.Is(err, errIdentifyUnavailable) {
		t.Fatalf("IDENTIFY_OLD_UNAVAILABLE: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	c, err := Dial(ctx2, cfg.Socket)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	// This served connection fences the single accept loop: all synchronous
	// connections from the completed identify attempt were accepted before it.
	if trace != "" {
		if n := countAccepts(); n != 3 {
			t.Fatalf("IDENTIFY_OLD_SINGLE_OFFER: accepts=%d want=3 (probe, identify, controller)", n)
		}
	}
	call(t, c, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("old-compatible")}, "written")
	r := call(t, c, Request{Op: "read", Execution: cfg.Execution, Epoch: 1}, "ok")
	if string(r.Data) != "old-compatible" {
		t.Fatal("IDENTIFY_OLD_CONTROLLER_UNCHANGED")
	}
}
