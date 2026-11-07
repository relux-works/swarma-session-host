package runner

import (
	"errors"
	"os"
	"testing"
)

func TestIdentifyV2SelectionBeforeListen(t *testing.T) {
	for _, failure := range []error{errChildBirthUnsupported, errChildBirthUnavailable} {
		cfg := testConfig(t, "echo")
		cfg.IdentifyV2 = true
		// A preflight refusal wins over even invalid endpoint validation: Listen
		// must not be reached. The workload executable is deliberately absent too.
		cfg.Socket = "relative-refused.sock"
		cfg.Plan.Binary = "/absent-oi1-workload"
		calls := 0
		s, err := startWithPreflight(cfg, captureChildBirth, waitOriginalChild, nil, func() error { calls++; return failure })
		if s != nil || err == nil || err.Error() != "child_start_failed" || calls != 1 {
			t.Fatalf("%v %v calls=%d", s, err, calls)
		}
	}
	cfg := testConfig(t, "echo")
	cfg.IdentifyV2 = true
	// Failed preflight cannot unlink an occupied path, even a regular file.
	if err := os.WriteFile(cfg.Socket, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := startWithPreflight(cfg, captureChildBirth, waitOriginalChild, nil, func() error { return errChildBirthUnavailable })
	data, readErr := os.ReadFile(cfg.Socket)
	if s != nil || err == nil || err.Error() != "child_start_failed" || readErr != nil || string(data) != "preserve" {
		t.Fatal(err, readErr, string(data))
	}
}

func TestIdentifyV2FalseSkipsPreflightAndRetainsController(t *testing.T) {
	cfg := testConfig(t, "echo")
	if cfg.IdentifyV2 {
		t.Fatal("selection must default false")
	}
	s, err := startWithPreflight(cfg, captureChildBirth, waitOriginalChild, nil, func() error { t.Error("nonselected preflight"); return errors.New("blocked facility") })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	trackTestRunner(t, s)
	c := testClient(t, cfg)
	call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	r, _ := s.identifyV2Snapshot(IdentifyV2Request{identifyRequestSchemaV2, cfg.Execution, s.instance})
	if r.Reason == nil || *r.Reason != "unsupported_platform" || r.Child != nil || s.linux.release != nil {
		t.Fatalf("unexpected nonselected evidence: %+v", r)
	}
	// A request cannot enable selection or change the ordinary lifetime.
	if s.cfg.IdentifyV2 {
		t.Fatal("request changed selection")
	}
	s.mu.Lock()
	s.childState = childWaitUnknown
	s.mu.Unlock()
	r, _ = s.identifyV2Snapshot(IdentifyV2Request{identifyRequestSchemaV2, cfg.Execution, s.instance})
	if r.Reason == nil || *r.Reason != "wait_unknown" {
		t.Fatal("lost lifecycle precedence", r)
	}
	s.mu.Lock()
	s.childState = childOwned
	s.mu.Unlock()
}

func TestIdentifyV2ValidationPrecedesPreflight(t *testing.T) {
	cfg := testConfig(t, "echo")
	cfg.IdentifyV2 = true
	cfg.Diagnostics = "invalid"
	calls := 0
	s, err := startWithPreflight(cfg, captureChildBirth, waitOriginalChild, nil, func() error { calls++; return nil })
	if s != nil || err == nil || err.Error() != "invalid_runner_plan" || calls != 0 {
		t.Fatal(s, err, calls)
	}
}
