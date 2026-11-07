package runner

import (
	"errors"
	"net"
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
