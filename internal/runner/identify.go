package runner

import (
	"encoding/json"
	"errors"
	"net"
	"time"
)

const identifyBudget = time.Second

// Only a first-frame top-level schema selects the separate decoder. Legacy
// requests retain their existing decoder, frame size and controller behavior.
func offersIdentify(frame []byte) bool {
	var members map[string]json.RawMessage
	if json.Unmarshal(frame, &members) != nil {
		return false
	}
	_, ok := members["schema"]
	return ok
}
func identifyDeadline(accepted time.Time, configured time.Duration) time.Time {
	if configured > identifyBudget {
		configured = identifyBudget
	}
	return accepted.Add(configured)
}
func identifyRefusal(req identifyRequest, result, reason string) identifyResponse {
	return identifyResponse{Schema: identifyResponseSchema, Execution: req.Execution, Result: result, Reason: &reason}
}

// identifySnapshot only copies bounded memory. It never queries a PID, waits,
// signals, reads output, reserves receipts, takes writeMu or changes authority.
func (s *Server) identifySnapshot(req identifyRequest) (identifyResponse, func()) {
	if req.Execution != s.cfg.Execution {
		return identifyRefusal(req, "conflict", "execution_mismatch"), nil
	}
	if req.ExpectedInstance != s.instance {
		return identifyRefusal(req, "conflict", "instance_mismatch"), nil
	}
	if !s.processMu.TryLock() {
		return identifyRefusal(req, "unavailable", "busy"), nil
	}
	if !s.mu.TryLock() {
		s.processMu.Unlock()
		return identifyRefusal(req, "unavailable", "busy"), nil
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
	case errors.Is(s.original.err, errChildBirthUnsupported):
		reason = "unsupported_platform"
	case s.original.err != nil || !identifyExecution(s.cfg.Execution):
		reason = "birth_unavailable"
	}
	if reason != "" {
		return identifyRefusal(req, "unavailable", reason), gate
	}
	instance := s.instance
	return identifyResponse{Schema: identifyResponseSchema, Execution: req.Execution, Instance: &instance, Result: "verified",
		Child: &identifyChild{PID: s.original.pid, Birth: identifyBirth{Sec: s.original.birth.sec, Usec: s.original.birth.usec}}}, gate
}
func (s *Server) handleIdentify(conn *net.UnixConn, frame []byte, accepted time.Time) {
	select {
	case s.identifySlots <- struct{}{}:
		defer func() { <-s.identifySlots }()
	default:
		return
	}
	deadline := identifyDeadline(accepted, s.cfg.IOTimeout)
	if !time.Now().Before(deadline) {
		return
	}
	if conn.SetDeadline(deadline) != nil {
		return
	}
	req, err := decodeIdentifyRequest(frame)
	if err != nil {
		return
	}
	response, gate := s.identifySnapshot(req)
	if gate != nil {
		gate()
	} // Test-only per-instance barrier, always outside locks.
	if !time.Now().Before(deadline) {
		return
	}
	data, err := encodeIdentify(response)
	if err != nil {
		return
	}
	if !time.Now().Before(deadline) {
		return
	}
	_, _ = conn.Write(data)
}
