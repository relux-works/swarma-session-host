package runner

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// IdentifyV2TrustProfile is the fixed deployment limit of this component.
// It grants no per-agent isolation, custody or production admission. Future
// O/S/C composition must supply its own UID policy without changing the wire.
const IdentifyV2TrustProfile = "single-trust-unit"

// linuxOriginal is captured at the sole spawn. release is owned exclusively by
// the serialized terminal reaper, including permanently failed capture.
type linuxOriginal struct {
	identity LinuxPIDFSIdentity
	err      error
	release  func()
	started  OwnedProcessEvidence
	process  func(context.Context) (OwnedProcessEvidence, error)
}

// OwnedProcessEvidence is private to the module's retained-owner seam. It is
// raw kernel evidence, not a session-manager wire type or harness admission.
// Linux clock-frequency/schema projection belongs to the negotiated consumer.
// Darwin seconds/microseconds are never recovered from a formatted date.
type OwnedProcessEvidence struct {
	Execution             string
	PID                   int
	RealUID, EffectiveUID uint32
	BootID, Platform      string
	StartTicks, StartSec  string
	StartUsec             uint32
	Executable            string
}

// ProcessEvidence freshly checks the originally captured live child while
// serializing with its sole reaper. It neither opens a process handle by PID
// nor uses identify's cached ownership snapshot as liveness authority.
func (s *Server) ProcessEvidence(ctx context.Context) (OwnedProcessEvidence, error) {
	if s == nil || ctx == nil || ctx.Err() != nil {
		return OwnedProcessEvidence{}, errChildBirthUnavailable
	}
	if _, ok := ctx.Deadline(); !ok || !lockProcessObservation(ctx, &s.processMu) {
		return OwnedProcessEvidence{}, errChildBirthUnavailable
	}
	defer s.processMu.Unlock()
	if !lockProcessObservation(ctx, &s.mu) {
		return OwnedProcessEvidence{}, errChildBirthUnavailable
	}
	if s.isClosedLocked() || !s.ownsChildLocked() || s.exited || s.waitUnknown ||
		s.linux.process == nil || s.linux.started.PID != s.original.pid {
		s.mu.Unlock()
		return OwnedProcessEvidence{}, errChildBirthUnavailable
	}
	process, started, execution := s.linux.process, s.linux.started, s.cfg.Execution
	// processMu alone excludes reaping, so the kernel reread runs without
	// s.mu: holding the runner's main mutex across /proc reads would stall
	// output collection and every runner client for the whole read.
	s.mu.Unlock()
	fresh, err := process(ctx)
	if err != nil || ctx.Err() != nil || fresh != started {
		return OwnedProcessEvidence{}, errChildBirthUnavailable
	}
	// Re-check ownership after the unlocked read before returning evidence.
	if !lockProcessObservation(ctx, &s.mu) {
		return OwnedProcessEvidence{}, errChildBirthUnavailable
	}
	defer s.mu.Unlock()
	if s.isClosedLocked() || !s.ownsChildLocked() || s.exited || s.waitUnknown ||
		s.linux.process == nil || s.linux.started != started || ctx.Err() != nil {
		return OwnedProcessEvidence{}, errChildBirthUnavailable
	}
	fresh.Execution = execution
	return fresh, nil
}

// Acquiring existing ownership is deadline-bounded; it never retries capture,
// starts a child, or adopts a handle. This keeps normal reaper polling from
// making the first live observation depend on scheduler coincidence.
func lockProcessObservation(ctx context.Context, mu *sync.Mutex) bool {
	for ctx.Err() == nil {
		if mu.TryLock() {
			return true
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return false
		}
	}
	return false
}

func identifyV2Refusal(req IdentifyV2Request, result, reason string) IdentifyV2Response {
	return IdentifyV2Response{Schema: identifyResponseSchemaV2, Execution: req.Execution, Result: result, Reason: &reason}
}
func (s *Server) identifyV2Snapshot(req IdentifyV2Request) (IdentifyV2Response, func()) {
	if req.Execution != s.cfg.Execution {
		return identifyV2Refusal(req, "conflict", "execution_mismatch"), nil
	}
	if req.ExpectedInstance != s.instance {
		return identifyV2Refusal(req, "conflict", "instance_mismatch"), nil
	}
	if !s.processMu.TryLock() {
		return identifyV2Refusal(req, "unavailable", "busy"), nil
	}
	if !s.mu.TryLock() {
		s.processMu.Unlock()
		return identifyV2Refusal(req, "unavailable", "busy"), nil
	}
	defer s.processMu.Unlock()
	defer s.mu.Unlock()
	gate := s.identifyBeforeReply
	reason := ""
	switch {
	case s.isClosedLocked():
		reason = "runner_closed"
	case s.childState == childWaitUnknown || s.waitUnknown:
		reason = "wait_unknown"
	case s.childState == childReaped:
		reason = "child_reaped"
	case !s.ownsChildLocked():
		reason = "wait_unknown"
	case errors.Is(s.linux.err, errChildBirthUnsupported):
		reason = "unsupported_platform"
	case s.linux.err != nil || !validLinuxIdentity(s.linux.identity) || !identifyExecution(s.cfg.Execution):
		reason = "birth_unavailable"
	}
	if reason != "" {
		return identifyV2Refusal(req, "unavailable", reason), gate
	}
	instance := s.instance
	return IdentifyV2Response{Schema: identifyResponseSchemaV2, Execution: req.Execution, Instance: &instance, Result: "verified", Child: &IdentifyV2Child{PID: s.original.pid, Identity: s.linux.identity}}, gate
}
func (s *Server) handleIdentifyV2(conn *net.UnixConn, frame []byte, deadline time.Time) {
	req, err := decodeIdentifyV2Request(frame)
	if err != nil {
		return
	}
	r, gate := s.identifyV2Snapshot(req)
	if gate != nil {
		gate()
	}
	if !time.Now().Before(deadline) {
		return
	}
	b, err := encodeIdentifyV2(r)
	if err != nil {
		return
	}
	if !time.Now().Before(deadline) {
		return
	}
	_, _ = conn.Write(b)
}
