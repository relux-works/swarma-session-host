//go:build darwin && cgo

package admission

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Two kernel observations identify this owned test process with its real birth,
// effective UID, boot UUID and code image. This is no PID-reuse/exec-race proof.
func TestOwnedKernelIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := ObserveProcess(ctx, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	second, err := ObserveProcess(ctx, os.Getpid())
	if err != nil || first != second || first.UID != uint32(os.Geteuid()) || !validProcess(first) {
		t.Fatalf("unstable owned identity: %v", err)
	}
	for _, pid := range []int{0, -1, 1 << 40} {
		if _, err := ObserveProcess(ctx, pid); err == nil {
			t.Fatalf("invalid pid %d observed", pid)
		}
	}
	expired, cancel2 := context.WithCancel(ctx)
	cancel2()
	if _, err := ObserveProcess(expired, os.Getpid()); err == nil {
		t.Fatal("expired observation succeeded")
	}
}

// This owned child stays live until stdin closes. No Wait runs until after the
// explicit zombie assertion, so the test never mistakes reaped PID reuse for it.
func TestOwnedKernelChildProcess(t *testing.T) {
	if os.Getenv("ADMISSION_KERNEL_CHILD") != "1" {
		return
	}
	os.Stdout.Write([]byte("ready\n"))
	io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

// Real live-child reads agree with the admitted tuple and refuse altered expected
// UID/birth/image. Actual zombie and reaped states both become unavailable. These
// are real-reader controls, separate from injected mock tuple comparisons.
func TestOwnedKernelChildLifecycle(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestOwnedKernelChildProcess$")
	cmd.Env = []string{"ADMISSION_KERNEL_CHILD=1", "GORACE=atexit_sleep_ms=0"}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	reaped := false
	defer func() {
		input.Close()
		if !reaped {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	ready := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(output).ReadString('\n')
		if err == nil && line != "ready\n" {
			err = errors.New("bad child barrier")
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("child readiness timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	parent, err := ObserveProcess(ctx, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	child, err := ObserveProcess(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"valid", "birth", "uid", "image"} {
		t.Run(field, func(t *testing.T) {
			s := fixture()
			s.Record.Binding.Runner = parent
			s.Record.Binding.Harness = child
			switch field {
			case "birth":
				at, _ := time.Parse(ProcessTime, child.StartedAt)
				s.Record.Binding.Harness.StartedAt = at.Add(time.Microsecond).Format(ProcessTime)
			case "uid":
				s.Record.Binding.Harness.UID++
			case "image":
				s.Record.Binding.Harness.CDHash = strings.Repeat("f", 40)
			}
			s.Lease.PID = pid
			s.Lease.StartedAt = s.Record.Binding.Harness.StartedAt
			c := config(s)
			c.Process = ObserveProcess
			_, err := verify(t, c, nil)
			if field == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != "admission_process_conflict" {
				t.Fatalf("altered %s matched live child: %v", field, err)
			}
		})
	}
	input.Close()
	// ps observes state without consuming wait status. Only this explicitly
	// spawned, unreaped PID is inspected; no process discovery or pattern kill.
	deadline := time.Now().Add(3 * time.Second)
	for {
		state, err := exec.Command("/bin/ps", "-p", strconv.Itoa(pid), "-o", "stat=").Output()
		if err != nil {
			t.Fatalf("owned unreaped child state: %v", err)
		}
		if strings.HasPrefix(strings.TrimSpace(string(state)), "Z") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned child never reached zombie barrier")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := ObserveProcess(ctx, pid); err == nil || err.Error() != "process_unavailable" {
		t.Fatalf("zombie produced live evidence: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	reaped = true
	if _, err := ObserveProcess(ctx, pid); err == nil || err.Error() != "process_unavailable" {
		t.Fatalf("reaped child produced evidence: %v", err)
	}
}

// Apple Silicon requires signed executable pages, so an unsigned arm64 image
// cannot provide a live negative control. An invalid csops operation exercises
// the actual syscall-failure branch, with valid CDHASH reads on both sides.
func TestCSOpsFailureUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	before, err := ObserveProcess(ctx, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observeProcess(ctx, os.Getpid(), ^uint32(0)); err == nil || err.Error() != "process_unavailable" {
		t.Fatalf("failed csops admitted: %v", err)
	}
	after, err := ObserveProcess(ctx, os.Getpid())
	if err != nil || after != before {
		t.Fatalf("valid image control failed: %v", err)
	}
}
