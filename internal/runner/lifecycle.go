package runner

import (
	"errors"
	"os"
	"syscall"
)

type childOwnership uint8

const (
	// The zero value is uncertainty, never permission to use a remembered PID.
	childWaitUnknown childOwnership = iota
	childOwned
	childReaped
)

type childBirth struct{ sec, usec uint64 }
type originalChild struct {
	pid   int
	birth childBirth
	err   error
}

var (
	errChildBirthUnavailable = errors.New("child_birth_unavailable")
	errChildBirthUnsupported = errors.New("child_birth_unsupported")
)

func validateChildBirth(pid, observedPID int, sizeOK bool, sec, usec uint64) (childBirth, error) {
	if !sizeOK || pid <= 0 || pid > 2147483647 || observedPID != pid || sec > 9007199254740991 || usec >= 1000000 {
		return childBirth{}, errChildBirthUnavailable
	}
	return childBirth{sec: sec, usec: usec}, nil
}

func captureOriginalChild(pid int, capture func(int) (childBirth, error)) originalChild {
	birth, err := capture(pid)
	if err == nil {
		birth, err = validateChildBirth(pid, pid, true, birth.sec, birth.usec)
	}
	// Failed capture is permanent for this original child; never repair it by PID.
	if err != nil {
		birth = childBirth{}
	}
	return originalChild{pid: pid, birth: birth, err: err}
}

func classifyChildWait(ownedPID, resultPID int, err error) (childOwnership, bool) {
	if err == syscall.EINTR {
		return childOwned, false
	}
	if err != nil {
		return childWaitUnknown, true
	}
	if resultPID == 0 {
		return childOwned, false
	}
	if ownedPID > 0 && resultPID == ownedPID {
		return childReaped, true
	}
	return childWaitUnknown, true
}

// ownsChildLocked requires processMu then mu. It is an in-memory authority check,
// not a process lookup or liveness claim; birth availability is checked separately.
func (s *Server) ownsChildLocked() bool {
	return s.childState == childOwned && s.child != nil && s.pid > 0 &&
		s.child.Pid == s.pid && s.original.pid == s.pid
}

// Delivery is per call so inert qualification can inspect the authority seam
// without constructing an OS signal target. Production always uses Process.Signal.
type childSignaler func(*os.Process, os.Signal) error

func (s *Server) signalOriginalChild(signal os.Signal, deliver childSignaler) (bool, error) {
	s.processMu.Lock()
	defer s.processMu.Unlock()
	s.mu.Lock()
	owned := s.ownsChildLocked() && !s.exited && !s.waitUnknown
	s.mu.Unlock()
	if !owned {
		return false, nil
	}
	// Reap cannot consume this child's wait status while processMu is held.
	// No PID lookup, adoption, process group or replacement handle is used.
	return true, deliver(s.child, signal)
}

// publishChildWaitLocked requires processMu then mu and a terminal wait result.
// It publishes ownership loss before done wakes stop/controller waiters.
func (s *Server) publishChildWaitLocked(state childOwnership, status syscall.WaitStatus) {
	s.childState = state
	s.waitUnknown = state == childWaitUnknown
	s.exited = state == childReaped
	if s.exited && status.Exited() {
		s.exitCode = status.ExitStatus()
	}
	if s.waitUnknown {
		s.blocked = true
	}
	s.changedLocked()
	close(s.done)
}

// The public start path always supplies this waiter. Per-instance qualification
// can observe the serialization at the real syscall without replacing globals.
type childWaiter func(*Server, *syscall.WaitStatus) (int, error)

func waitOriginalChild(s *Server, status *syscall.WaitStatus) (int, error) {
	return syscall.Wait4(s.pid, status, syscall.WNOHANG, nil)
}
