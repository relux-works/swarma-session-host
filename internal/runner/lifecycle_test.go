package runner

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
)

// These isolated transition cases do not consume or abandon a real child's wait
// status. Pending/EINTR remain owned; only the exact consumed PID is reaped.
func TestChildWaitClassification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pid      int
		err      error
		state    childOwnership
		terminal bool
	}{
		{"pending", 0, nil, childOwned, false},
		{"interrupted", -1, syscall.EINTR, childOwned, false},
		{"reaped", 123, nil, childReaped, true},
		{"foreign-result", 124, nil, childWaitUnknown, true},
		{"negative-result", -1, nil, childWaitUnknown, true},
		{"lost-child", -1, syscall.ECHILD, childWaitUnknown, true},
		{"wait-error", 0, syscall.EIO, childWaitUnknown, true},
		{"pid-and-error", 123, syscall.EIO, childWaitUnknown, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, terminal := classifyChildWait(123, tc.pid, tc.err)
			if state != tc.state || terminal != tc.terminal {
				t.Fatalf("WAIT_CLASS_%s: got %v/%v want %v/%v", tc.name, state, terminal, tc.state, tc.terminal)
			}
		})
	}
}

// The same kernel tuple is preserved exactly; malformed size/PID/range evidence
// cannot be published as an original child birth. This is decoder sensitivity.
func TestChildBirthValidation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		pid, observed int
		size          bool
		sec, usec     uint64
		valid         bool
	}{
		{"valid", 123, 123, true, 1800000000, 123456, true},
		{"limits", 123, 123, true, (1 << 53) - 1, 999999, true},
		{"zero-time", 123, 123, true, 0, 0, true},
		{"size", 123, 123, false, 1, 0, false},
		{"pid", 123, 124, true, 1, 0, false},
		{"zero-pid", 0, 0, true, 1, 0, false},
		{"negative-pid", -1, -1, true, 1, 0, false},
		{"large-pid", 1 << 31, 1 << 31, true, 1, 0, false},
		{"seconds", 123, 123, true, 1 << 53, 0, false},
		{"microseconds", 123, 123, true, 1, 1000000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := validateChildBirth(tc.pid, tc.observed, tc.size, tc.sec, tc.usec)
			if (err == nil) != tc.valid || tc.valid && (b.sec != tc.sec || b.usec != tc.usec) {
				t.Fatalf("BIRTH_VALID_%s: birth=%+v err=%v", tc.name, b, err)
			}
		})
	}
}

// No OS process is constructed or signalled here: ownership/handle consistency
// itself is checked before a cached PID can authorize proof or owned signalling.
func TestChildOwnershipConsistency(t *testing.T) {
	for _, tc := range []struct {
		name          string
		state         childOwnership
		handle        *os.Process
		pid, original int
		want          bool
	}{
		{"owned", childOwned, &os.Process{Pid: 123}, 123, 123, true},
		{"reaped", childReaped, &os.Process{Pid: 123}, 123, 123, false},
		{"unknown", childWaitUnknown, &os.Process{Pid: 123}, 123, 123, false},
		{"missing-handle", childOwned, nil, 123, 123, false},
		{"changed-handle", childOwned, &os.Process{Pid: 124}, 123, 123, false},
		{"changed-original", childOwned, &os.Process{Pid: 123}, 123, 124, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{childState: tc.state, child: tc.handle, pid: tc.pid, original: originalChild{pid: tc.original}}
			s.processMu.Lock()
			s.mu.Lock()
			got := s.ownsChildLocked()
			s.mu.Unlock()
			s.processMu.Unlock()
			if got != tc.want {
				t.Fatalf("OWNERSHIP_%s: got %v want %v", tc.name, got, tc.want)
			}
		})
	}
}

// Capture failure is immutable and nonfatal to legacy control: one spawn/capture
// only, then a normal claim/write/stop, with no attempt to recover birth by PID.
func TestOriginalBirthCaptureFailurePreservesControl(t *testing.T) {
	cfg := testConfig(t, "echo")
	calls := 0
	s, err := startWithBirthCapture(cfg, func(pid int) (childBirth, error) {
		calls++
		if pid <= 0 {
			t.Fatal("invalid capture PID")
		}
		return childBirth{}, errors.New("fixture capture failure")
	})
	if err != nil {
		t.Fatal(err)
	}
	trackTestRunner(t, s)
	if calls != 1 || s.original.pid != s.pid || s.original.err == nil {
		t.Fatalf("BIRTH_CAPTURE_FAILURE: calls=%d original=%+v", calls, s.original)
	}
	c := testClient(t, cfg)
	call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	call(t, c, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("valid\n")}, "written")
	call(t, c, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
	if calls != 1 || s.original.err == nil {
		t.Fatalf("BIRTH_RECAPTURE: calls=%d original=%+v", calls, s.original)
	}
	s.processMu.Lock()
	s.mu.Lock()
	owned, state := s.ownsChildLocked(), s.childState
	s.mu.Unlock()
	s.processMu.Unlock()
	if owned || state != childReaped {
		t.Fatalf("WAIT_PUBLICATION: owned=%v state=%v", owned, state)
	}
}

// A successful per-instance capture is retained exactly, without global syscall
// hooks. The Darwin zombie test separately witnesses capture-before-reap ordering.
func TestOriginalBirthCaptureOnce(t *testing.T) {
	cfg := testConfig(t, "echo")
	calls := 0
	s, err := startWithBirthCapture(cfg, func(pid int) (childBirth, error) {
		calls++
		return childBirth{sec: 123, usec: 456}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	trackTestRunner(t, s)
	if calls != 1 || s.original.birth != (childBirth{sec: 123, usec: 456}) || s.original.err != nil {
		t.Fatalf("BIRTH_ONCE: calls=%d original=%+v", calls, s.original)
	}
}

// Terminal publication exposes the same verdict to stop and later ownership
// readers, fencing unknown results. It does not exercise a real wait failure.
func TestChildWaitPublication(t *testing.T) {
	for _, state := range []childOwnership{childReaped, childWaitUnknown} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			s := &Server{childState: childOwned, changed: make(chan struct{}), done: make(chan struct{}), exitCode: -1}
			s.processMu.Lock()
			s.mu.Lock()
			s.publishChildWaitLocked(state, syscall.WaitStatus(7<<8))
			if s.childState != state || s.exited != (state == childReaped) || s.waitUnknown != (state == childWaitUnknown) || s.blocked != (state == childWaitUnknown) {
				t.Errorf("WAIT_STATE_PUBLISHED: state=%v exited=%v unknown=%v blocked=%v", s.childState, s.exited, s.waitUnknown, s.blocked)
			}
			select {
			case <-s.done:
			default:
				t.Error("WAIT_DONE_PUBLICATION")
			}
			if state == childReaped && s.exitCode != 7 {
				t.Errorf("WAIT_EXIT_CODE: %d", s.exitCode)
			}
			s.mu.Unlock()
			s.processMu.Unlock()
		})
	}
}

// A failed or malformed capture returns no usable birth and is never retried.
func TestOriginalBirthFailureImmutable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		birth childBirth
		err   error
	}{
		{"error", childBirth{sec: 1, usec: 2}, errChildBirthUnavailable},
		{"unsupported", childBirth{}, errChildBirthUnsupported},
		{"range", childBirth{sec: 1, usec: 1000000}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			original := captureOriginalChild(123, func(int) (childBirth, error) { calls++; return tc.birth, tc.err })
			if calls != 1 || original.err == nil || original.birth != (childBirth{}) {
				t.Fatalf("BIRTH_FAILURE_%s: calls=%d original=%+v", tc.name, calls, original)
			}
			if tc.err == errChildBirthUnsupported && !errors.Is(original.err, errChildBirthUnsupported) {
				t.Fatal("BIRTH_UNSUPPORTED_REASON")
			}
		})
	}
}

// Normal close finishes owned stop/reap, retains the immutable original tuple,
// and leaves neither closed-server control nor a cached ownership authority.
func TestChildClosedLifecycle(t *testing.T) {
	cfg := testConfig(t, "echo")
	s, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	before := s.original
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s.processMu.Lock()
	s.mu.Lock()
	closed, owned, state := s.isClosedLocked(), s.ownsChildLocked(), s.childState
	s.mu.Unlock()
	s.processMu.Unlock()
	if !closed || owned || state != childReaped || s.original != before {
		t.Fatalf("CLOSED_LIFECYCLE: closed=%v owned=%v state=%v original=%+v", closed, owned, state, s.original)
	}
	if r := s.process(nil, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}); r.Code != "runner_closed" {
		t.Fatalf("CLOSED_CONTROL: %+v", r)
	}
}

// At every real Wait4 invocation the reaper itself holds processMu: this isolated
// fixture has no competing signal/lock holder at the observation point. Removing
// reaper serialization is detected even if a sampled zombie state looks normal.
func TestChildKernelWaitSerialization(t *testing.T) {
	cfg := testConfig(t, "echo")
	var unserialized atomic.Bool
	var calls atomic.Int32
	s, err := startWithChildLifecycle(cfg, captureChildBirth, func(s *Server, status *syscall.WaitStatus) (int, error) {
		if s.processMu.TryLock() {
			s.processMu.Unlock()
			unserialized.Store(true)
		}
		calls.Add(1)
		return waitOriginalChild(s, status)
	})
	if err != nil {
		t.Fatal(err)
	}
	trackTestRunner(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() == 0 || unserialized.Load() {
		t.Fatalf("WAIT_SERIALIZED_CALL: calls=%d unprotected=%v", calls.Load(), unserialized.Load())
	}
}
