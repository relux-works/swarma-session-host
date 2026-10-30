package runner

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
)

func identifyMemoryFixture() *Server {
	return &Server{cfg: Config{Execution: "exec-a"}, instance: identifyTestInstance,
		child: &os.Process{Pid: 123}, pid: 123, childState: childOwned,
		original: originalChild{pid: 123, birth: childBirth{sec: 1800000000, usec: 123456}}, closed: make(chan struct{})}
}
func identifyMatchingRequest(s *Server) identifyRequest {
	return identifyRequest{Schema: identifyRequestSchema, Execution: s.cfg.Execution, ExpectedInstance: s.instance}
}
func requireIdentifyResult(t *testing.T, r identifyResponse, result, reason string) {
	t.Helper()
	if r.Result != result {
		t.Fatalf("IDENTIFY_RESULT: got%+v want%s/%s", r, result, reason)
	}
	if result == "verified" {
		if r.Reason != nil || r.Instance == nil || r.Child == nil {
			t.Fatal("IDENTIFY_SUCCESS_SHAPE")
		}
	} else if r.Reason == nil || *r.Reason != reason || r.Instance != nil || r.Child != nil {
		t.Fatalf("IDENTIFY_REFUSAL_%s: %+v", reason, r)
	}
}

// Pure state fixtures qualify exact refusal precedence/disclosure. No fake
// terminal wait is injected into a real owned-child reaper.
func TestIdentifySnapshotStates(t *testing.T) {
	for _, tc := range []struct {
		name, result, reason string
		change               func(*Server, *identifyRequest)
	}{
		{"live", "verified", "", func(*Server, *identifyRequest) {}},
		{"execution", "conflict", "execution_mismatch", func(s *Server, r *identifyRequest) { r.Execution = "other" }},
		{"instance", "conflict", "instance_mismatch", func(s *Server, r *identifyRequest) { r.ExpectedInstance = "ffffffffffffffffffffffffffffffff" }},
		{"closed", "unavailable", "runner_closed", func(s *Server, _ *identifyRequest) { close(s.closed) }},
		{"unknown", "unavailable", "wait_unknown", func(s *Server, _ *identifyRequest) { s.childState = childWaitUnknown }},
		{"handle", "unavailable", "wait_unknown", func(s *Server, _ *identifyRequest) { s.child = nil }},
		{"handlePID", "unavailable", "wait_unknown", func(s *Server, _ *identifyRequest) { s.child = &os.Process{Pid: 124} }},
		{"originalPID", "unavailable", "wait_unknown", func(s *Server, _ *identifyRequest) { s.original.pid = 124 }},
		{"reaped", "unavailable", "child_reaped", func(s *Server, _ *identifyRequest) { s.childState = childReaped }},
		{"birth", "unavailable", "birth_unavailable", func(s *Server, _ *identifyRequest) { s.original.err = errChildBirthUnavailable }},
		{"unsupported", "unavailable", "unsupported_platform", func(s *Server, _ *identifyRequest) { s.original.err = errChildBirthUnsupported }},
		{"closedUnknown", "unavailable", "runner_closed", func(s *Server, _ *identifyRequest) { s.childState = childWaitUnknown; close(s.closed) }},
		{"unknownBirth", "unavailable", "wait_unknown", func(s *Server, _ *identifyRequest) {
			s.childState = childWaitUnknown
			s.original.err = errChildBirthUnsupported
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := identifyMemoryFixture()
			req := identifyMatchingRequest(s)
			tc.change(s, &req)
			original := s.original
			r, _ := s.identifySnapshot(req)
			requireIdentifyResult(t, r, tc.result, tc.reason)
			if r.Execution != req.Execution || s.original != original {
				t.Fatal("IDENTIFY_ECHO_IMMUTABLE")
			}
			if !s.processMu.TryLock() {
				t.Fatal("IDENTIFY_PROCESS_UNLOCK")
			}
			s.processMu.Unlock()
			if !s.mu.TryLock() {
				t.Fatal("IDENTIFY_MU_UNLOCK")
			}
			s.mu.Unlock()
		})
	}
}

// Holding each lock separately yields busy immediately; failure of the second
// try releases processMu. A free writeMu is never required for this read path.
func TestIdentifySnapshotContention(t *testing.T) {
	for _, which := range []string{"process", "mu", "write"} {
		t.Run(which, func(t *testing.T) {
			s := identifyMemoryFixture()
			switch which {
			case "process":
				s.processMu.Lock()
				defer s.processMu.Unlock()
			case "mu":
				s.mu.Lock()
				defer s.mu.Unlock()
			case "write":
				s.writeMu.Lock()
				defer s.writeMu.Unlock()
			}
			done := make(chan identifyResponse, 1)
			go func() { r, _ := s.identifySnapshot(identifyMatchingRequest(s)); done <- r }()
			select {
			case r := <-done:
				if which == "write" {
					requireIdentifyResult(t, r, "verified", "")
				} else {
					requireIdentifyResult(t, r, "unavailable", "busy")
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatal("IDENTIFY_TRY_LOCK_PROGRESS")
			}
			if which == "mu" {
				if !s.processMu.TryLock() {
					t.Fatal("IDENTIFY_SECOND_LOCK_RELEASE")
				}
				s.processMu.Unlock()
			}
		})
	}
}

// The memory path preserves controller/receipt/output/child/fence state. Real
// controller follow-up and actual kernel lifecycle are tested separately.
func TestIdentifyMemoryReadOnly(t *testing.T) {
	s := identifyMemoryFixture()
	s.epoch = 9
	s.seq = 2
	s.writes = map[uint64]writeRecord{1: {code: "written"}, 2: {code: "write_unknown"}}
	s.output = []byte("retained")
	s.blocked = true
	s.overflow = true
	s.outputEnded = true
	before := struct {
		epoch, seq               uint64
		writes                   map[uint64]writeRecord
		out                      string
		blocked, overflow, ended bool
		original                 originalChild
		state                    childOwnership
	}{
		s.epoch, s.seq, map[uint64]writeRecord{1: s.writes[1], 2: s.writes[2]}, string(s.output), s.blocked, s.overflow, s.outputEnded, s.original, s.childState}
	r, _ := s.identifySnapshot(identifyMatchingRequest(s))
	requireIdentifyResult(t, r, "verified", "")
	after := struct {
		epoch, seq               uint64
		writes                   map[uint64]writeRecord
		out                      string
		blocked, overflow, ended bool
		original                 originalChild
		state                    childOwnership
	}{
		s.epoch, s.seq, s.writes, string(s.output), s.blocked, s.overflow, s.outputEnded, s.original, s.childState}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("IDENTIFY_READ_ONLY_STATE")
	}
}

// Unsupported peer evidence is unavailable; a positively wrong connected UID
// or PID is conflict. This pure seam does not claim actual cross-UID OS coverage.
func TestIdentifyPeerVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name          string
		uid           uint32
		pid           int
		readErr, want error
	}{
		{"valid", 42, 123, nil, nil}, {"uid", 43, 123, nil, errIdentifyConflict}, {"pid", 42, 124, nil, errIdentifyConflict},
		{"missing", 0, 0, errors.New("missing"), errIdentifyUnavailable},
		{"readErrorWithValues", 42, 123, errors.New("missing"), errIdentifyUnavailable},
		{"zeroPID", 42, 0, nil, errIdentifyUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := identifyPeer(tc.uid, tc.pid, tc.readErr, 42, 123); !errors.Is(err, tc.want) {
				t.Fatalf("IDENTIFY_PEER_%s: %v", tc.name, err)
			}
		})
	}
}
