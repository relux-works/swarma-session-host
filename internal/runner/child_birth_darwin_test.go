//go:build darwin && cgo

package runner

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/admission"
)

// Two independent existing/new kernel readers agree to the microsecond for the
// directly owned live child. Stopping/reaping never changes the captured tuple.
func TestOriginalChildKernelBirth(t *testing.T) {
	cfg := testConfig(t, "echo")
	s, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if s.original.err != nil {
		t.Fatal(s.original.err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	observed, err := admission.ObserveProcess(ctx, s.pid)
	if err != nil {
		t.Fatal(err)
	}
	started, err := time.Parse(admission.ProcessTime, observed.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	want := childBirth{sec: uint64(started.Unix()), usec: uint64(started.Nanosecond() / 1000)}
	before := s.original
	if before.pid != observed.PID || before.birth != want {
		t.Fatalf("KERNEL_BIRTH_MATCH: original=%+v observed=%+v", before, observed)
	}
	c := testClient(t, cfg)
	call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	call(t, c, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
	s.processMu.Lock()
	s.mu.Lock()
	if s.childState != childReaped || s.ownsChildLocked() {
		t.Error("KERNEL_REAP_PUBLICATION")
	}
	s.mu.Unlock()
	s.processMu.Unlock()
	if s.original != before {
		t.Fatal("KERNEL_BIRTH_IMMUTABLE")
	}
}

func awaitOwnedZombie(pid int) error {
	deadline := time.Now().Add(3 * time.Second)
	for {
		data, err := exec.Command("/bin/ps", "-p", strconv.Itoa(pid), "-o", "stat=").Output()
		if err != nil {
			return fmt.Errorf("owned state: %w", err)
		}
		if strings.HasPrefix(strings.TrimSpace(string(data)), "Z") {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("owned zombie deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

// The naturally exiting child's zombie is observable inside capture, before any
// reaper starts. The ordinary reaper later consumes it; no test calls Wait4.
func TestOriginalZombieCaptureBeforeReaper(t *testing.T) {
	cfg := testConfig(t, "exit")
	var witness error
	s, err := startWithBirthCapture(cfg, func(pid int) (childBirth, error) {
		witness = awaitOwnedZombie(pid)
		if witness != nil {
			return childBirth{}, witness
		}
		return captureChildBirth(pid)
	})
	if err != nil {
		t.Fatal(err)
	}
	trackTestRunner(t, s)
	if witness != nil || s.original.err != nil {
		t.Fatalf("BIRTH_BEFORE_REAP: witness=%v capture=%v", witness, s.original.err)
	}
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("BIRTH_NORMAL_REAP")
	}
	s.processMu.Lock()
	s.mu.Lock()
	if s.childState != childReaped || s.ownsChildLocked() {
		t.Error("BIRTH_ZOMBIE_REAP_STATE")
	}
	s.mu.Unlock()
	s.processMu.Unlock()
}

// Holding processMu preserves the exited-unreaped ownership state until the
// ordinary reaper can publish reaped; shutdown then leaves no cached authority.
func TestChildReapSerialization(t *testing.T) {
	cfg := testConfig(t, "echo")
	s, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.processMu.Lock()
	s.stdin.Close()
	witness := awaitOwnedZombie(s.pid)
	s.mu.Lock()
	owned := s.ownsChildLocked()
	state := s.childState
	s.mu.Unlock()
	s.processMu.Unlock()
	if witness != nil || !owned || state != childOwned {
		t.Fatalf("WAIT_SERIALIZED_OWNED: witness=%v owned=%v state=%v", witness, owned, state)
	}
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("WAIT_SERIALIZED_REAP")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s.processMu.Lock()
	s.mu.Lock()
	after, owned := s.childState, s.ownsChildLocked()
	s.mu.Unlock()
	s.processMu.Unlock()
	if after != childReaped || owned {
		t.Fatalf("WAIT_SERIALIZED_TERMINAL: %v %v", after, owned)
	}
}
