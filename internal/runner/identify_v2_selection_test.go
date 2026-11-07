package runner

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// V2 is a recognized wire schema even when Linux identity was not selected.
// Refuse proof explicitly on both Linux and Darwin without dispatching a
// pipelined controller operation or disclosing instance/child identity.
func TestIdentifyV2NonselectedWireOneShot(t *testing.T) {
	cfg := testConfig(t, "echo")
	// Keep periodic observation from turning the expected refusal into busy.
	release := make(chan struct{})
	s, err := startWithReaperGate(cfg, captureChildBirth, waitOriginalChild, func() { <-release })
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	defer func() { close(release); _ = s.Close() }()
	trackTestRunner(t, s)
	b, err := encodeIdentifyV2(IdentifyV2Request{identifyRequestSchemaV2, cfg.Execution, s.instance})
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: cfg.Socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	pipeline := append(bytes.Clone(b), []byte("{\"op\":\"stop\",\"execution\":\""+cfg.Execution+"\",\"epoch\":1}\n")...)
	if _, err := c.Write(pipeline); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(c)
	wire, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	r, err := decodeIdentifyV2Response(wire)
	if err != nil || r.Schema != identifyResponseSchemaV2 || r.Execution != cfg.Execution || r.Result != "unavailable" ||
		r.Reason == nil || *r.Reason != "unsupported_platform" || r.Instance != nil || r.Child != nil {
		t.Fatalf("nonselected v2 response: %+v %v", r, err)
	}
	if rest, err := reader.ReadBytes('\n'); !identifyV2PeerClosed(rest, err) {
		t.Fatalf("v2 one-shot: %q %v", rest, err)
	}
	s.mu.Lock()
	inert := s.owner == nil && s.epoch == 0 && s.seq == 0 && !s.blocked && len(s.writes) == 0 && !s.isClosedLocked() && !s.cfg.IdentifyV2
	s.mu.Unlock()
	if !inert {
		t.Fatal("nonselected v2 changed controller state")
	}
	controller := testClient(t, cfg)
	call(t, controller, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	call(t, controller, Request{Op: "write", Execution: cfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("after")}, "written")
}

func identifyV2PeerClosed(rest []byte, err error) bool {
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return false
	}
	return err == io.EOF && len(rest) == 0
}

func TestIdentifyV2PeerClosureControls(t *testing.T) {
	for _, closed := range []bool{true, false} {
		name := "still-open-timeout"
		if closed {
			name = "closed-peer"
		}
		t.Run(name, func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			if err := client.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if closed {
				if err := peer.Close(); err != nil {
					t.Fatal(err)
				}
			}
			rest, err := bufio.NewReader(client).ReadBytes('\n')
			if closed {
				if err != io.EOF || len(rest) != 0 {
					t.Fatalf("closed peer: %q %v", rest, err)
				}
			} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() || len(rest) != 0 {
				t.Fatalf("still-open peer: %q %v", rest, err)
			}
			if got := identifyV2PeerClosed(rest, err); got != closed {
				t.Fatalf("peer closed = %t, want %t: %q %v", got, closed, rest, err)
			}
		})
	}
	for _, err := range []error{nil, errors.New("unrelated read failure"), io.ErrUnexpectedEOF} {
		if identifyV2PeerClosed(nil, err) {
			t.Fatalf("accepted non-EOF error: %v", err)
		}
	}
	if identifyV2PeerClosed([]byte("extra"), io.EOF) {
		t.Fatal("accepted remaining bytes at EOF")
	}
}

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
