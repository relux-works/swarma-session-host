//go:build darwin && cgo

package runner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

// Positive wire fixtures gate the ordinary reaper before its first lock, so a
// periodic poll cannot turn the intended success control into allowed busy.
func startIdentifyTestRunner(t *testing.T, cfg Config) (*Server, func(), error) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	s, err := startWithReaperGate(cfg, captureChildBirth, waitOriginalChild, func() { <-release })
	if err != nil {
		open()
		return nil, open, err
	}
	trackTestRunner(t, s)
	t.Cleanup(open)
	return s, open, nil
}

type identifyObservedInput struct {
	childInput
	writes, closes atomic.Int32
}

func (in *identifyObservedInput) Write(b []byte) (int, error) {
	in.writes.Add(1)
	return in.childInput.Write(b)
}
func (in *identifyObservedInput) Close() error { in.closes.Add(1); return in.childInput.Close() }

func identifyContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), identifyBudget)
}
func identifyRaw(t *testing.T, cfg Config, frame []byte) ([]byte, error) {
	t.Helper()
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: cfg.Socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write(frame); err != nil {
		return nil, err
	}
	return bufio.NewReaderSize(c, localipc.MaxFrame).ReadBytes('\n')
}
func identifyRequestFor(s *Server) []byte {
	b, _ := encodeIdentify(identifyMatchingRequest(s))
	return b
}
func identifyCall(t *testing.T, s *Server) (identifyResponse, error) {
	t.Helper()
	ctx, cancel := identifyContext(t)
	defer cancel()
	return identifyForQualification(ctx, s.cfg.Socket, os.Getpid(), uint32(os.Getuid()), identifyMatchingRequest(s))
}
func setIdentifyGate(s *Server, gate func()) {
	s.mu.Lock()
	s.identifyBeforeReply = gate
	s.mu.Unlock()
}
func identifyBarrier(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	return release, open
}
func awaitIdentifyEvent(t *testing.T, event <-chan struct{}, claim string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(2 * time.Second):
		t.Fatal(claim)
	}
}

// A real one-shot exchange leaves an existing controller, written receipt,
// output, input fence and original child unchanged, then permits the same
// controller's next write at the same epoch. Reaper is gated before its locks.
func TestIdentifyControllerReadOnly(t *testing.T) {
	cfg := testConfig(t, "echo")
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	s, err := startWithReaperGate(cfg, captureChildBirth, waitOriginalChild, func() { close(entered); <-release })
	if err != nil {
		open()
		t.Fatal(err)
	}
	trackTestRunner(t, s)
	t.Cleanup(open)
	awaitIdentifyEvent(t, entered, "IDENTIFY_REAPER_GATE")
	c := testClient(t, cfg)
	call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 7}, "ok")
	call(t, c, Request{Op: "write", Execution: cfg.Execution, Epoch: 7, Sequence: 1, Data: []byte("before")}, "written")
	r := call(t, c, Request{Op: "read", Execution: cfg.Execution, Epoch: 7}, "ok")
	if string(r.Data) != "before" {
		t.Fatal("IDENTIFY_OUTPUT_SETUP")
	}
	s.mu.Lock()
	input := &identifyObservedInput{childInput: s.stdin}
	s.stdin = input
	owner, epoch, seq, record, out, blocked, handle, original, state := s.owner, s.epoch, s.seq, s.writes[1], bytes.Clone(s.output), s.blocked, s.child, s.original, s.childState
	s.mu.Unlock()
	for i := 0; i < 3; i++ {
		reply, err := identifyCall(t, s)
		if err != nil {
			t.Fatal(err)
		}
		requireIdentifyResult(t, reply, "verified", "")
		if reply.Child.PID != s.pid || reply.Child.Birth.Sec != original.birth.sec || reply.Child.Birth.Usec != original.birth.usec {
			t.Fatal("IDENTIFY_ORIGINAL_TUPLE")
		}
	}
	s.mu.Lock()
	unchanged := owner == s.owner && epoch == s.epoch && seq == s.seq && record == s.writes[1] && len(s.writes) == 1 && bytes.Equal(out, s.output) && blocked == s.blocked && handle == s.child && original == s.original && state == s.childState
	s.mu.Unlock()
	if input.writes.Load() != 0 || input.closes.Load() != 0 {
		t.Fatal("IDENTIFY_NO_INPUT_EFFECT")
	}
	if !unchanged {
		t.Fatal("IDENTIFY_CONTROLLER_READ_ONLY")
	}
	call(t, c, Request{Op: "write", Execution: cfg.Execution, Epoch: 7, Sequence: 2, Data: []byte("after")}, "written")
	if input.writes.Load() != 1 {
		t.Fatal("IDENTIFY_INPUT_COUNTER_CONTROL")
	}
	open()
}

// Schema-bearing invalid/hybrid frames close without proof or control effects.
// A complete first offer followed by stop never dispatches the second frame.
func TestIdentifyDispatchAndOneShot(t *testing.T) {
	cfg := testConfig(t, "echo")
	s, openReap, err := startIdentifyTestRunner(t, cfg)
	_ = openReap
	if err != nil {
		t.Fatal(err)
	}
	good := identifyRequestFor(s)
	cases := map[string][]byte{
		"nonobject": []byte("null\n"), "array": []byte("[]\n"),
		"hybrid":    bytes.Replace(good, []byte("{"), []byte("{\"op\":\"claim\",\"epoch\":5,"), 1),
		"version":   bytes.Replace(good, []byte("request-v1"), []byte("request-v2"), 1),
		"duplicate": bytes.Replace(good, []byte("{"), []byte("{\"execution\":\"other\","), 1),
		"oversize":  append(bytes.Repeat([]byte(" "), 1025-len(good)), good...),
		"missing":   []byte("{\"schema\":\"runner-identify-request-v1\"}\n"),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			reply, err := identifyRaw(t, cfg, b)
			if err == nil || len(reply) != 0 {
				t.Fatalf("IDENTIFY_DISPATCH_REJECT_%s: %q %v", name, reply, err)
			}
		})
	}
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: cfg.Socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	pipeline := append(bytes.Clone(good), []byte("{\"op\":\"stop\",\"execution\":\""+cfg.Execution+"\",\"epoch\":1}\n")...)
	if _, err = c.Write(pipeline); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(c)
	reply, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeIdentifyResponse(reply); err != nil {
		t.Fatal(err)
	}
	if rest, err := reader.ReadBytes('\n'); err == nil || len(rest) != 0 {
		t.Fatalf("IDENTIFY_ONE_SHOT: %q %v", rest, err)
	}
	s.mu.Lock()
	inert := s.owner == nil && s.epoch == 0 && s.seq == 0 && !s.blocked && len(s.writes) == 0
	s.mu.Unlock()
	if !inert {
		t.Fatal("IDENTIFY_PIPELINE_EFFECT")
	}
}

// Matching escaped bytes succeed; different valid case/space/normalization
// bytes produce exact execution conflict without disclosing instance/child.
func TestIdentifyCorrelation(t *testing.T) {
	cfg := testConfig(t, "echo")
	cfg.Execution = "exec-é"
	s, openReap, err := startIdentifyTestRunner(t, cfg)
	_ = openReap
	if err != nil {
		t.Fatal(err)
	}
	good := identifyRequestFor(s)
	if b, err := identifyRaw(t, cfg, bytes.Replace(good, []byte("é"), []byte("\\u00e9"), 1)); err != nil {
		t.Fatal(err)
	} else if r, err := decodeIdentifyResponse(b); err != nil || r.Result != "verified" {
		t.Fatalf("IDENTIFY_ESCAPED_EQUAL: %v %+v", err, r)
	}
	for _, e := range []string{"exec-e\u0301", "exec-É", " exec-é", "operation-not-execution", "session-not-execution"} {
		req := identifyMatchingRequest(s)
		req.Execution = e
		b, _ := encodeIdentify(req)
		wire, err := identifyRaw(t, cfg, b)
		if err != nil {
			t.Fatal(err)
		}
		r, err := decodeIdentifyResponse(wire)
		if err != nil {
			t.Fatal(err)
		}
		requireIdentifyResult(t, r, "conflict", "execution_mismatch")
		if r.Execution != e {
			t.Fatal("IDENTIFY_REQUEST_ECHO")
		}
	}
	req := identifyMatchingRequest(s)
	req.ExpectedInstance = "ffffffffffffffffffffffffffffffff"
	b, _ := encodeIdentify(req)
	wire, err := identifyRaw(t, cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	r, err := decodeIdentifyResponse(wire)
	if err != nil {
		t.Fatal(err)
	}
	requireIdentifyResult(t, r, "conflict", "instance_mismatch")
}

// Real failed capture remains unavailable and legacy control remains usable;
// normal owned stop/reap then yields child_reaped without a cached PID proof.
func TestIdentifyLifecycleWire(t *testing.T) {
	for _, captureFail := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "failedBirth"}[captureFail], func(t *testing.T) {
			cfg := testConfig(t, "echo")
			var captures atomic.Int32
			release, openReap := identifyBarrier(t)
			s, err := startWithReaperGate(cfg, func(pid int) (childBirth, error) {
				captures.Add(1)
				if captureFail {
					return childBirth{}, errChildBirthUnavailable
				}
				return captureChildBirth(pid)
			}, waitOriginalChild, func() { <-release })
			if err != nil {
				t.Fatal(err)
			}
			trackTestRunner(t, s)
			t.Cleanup(openReap)
			r, err := identifyCall(t, s)
			if captureFail {
				if !errors.Is(err, errIdentifyUnavailable) {
					t.Fatal("IDENTIFY_BIRTH_ERROR")
				}
				requireIdentifyResult(t, r, "unavailable", "birth_unavailable")
			} else {
				if err != nil {
					t.Fatal(err)
				}
				requireIdentifyResult(t, r, "verified", "")
			}
			c := testClient(t, cfg)
			call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
			openReap()
			call(t, c, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
			r, err = identifyCall(t, s)
			if !errors.Is(err, errIdentifyUnavailable) {
				t.Fatal("IDENTIFY_REAP_ERROR")
			}
			requireIdentifyResult(t, r, "unavailable", "child_reaped")
			if captures.Load() != 1 {
				t.Fatal("IDENTIFY_NO_RECAPTURE")
			}
		})
	}
}

// Two gated handlers occupy the fixed slots; excess closes, then both slots
// return after delivery. Gates run outside lifecycle locks; stop/reap progress
// while both replies are held. The small reply is not claimed to fill a socket.
func TestIdentifySlotsAndWireLocks(t *testing.T) {
	cfg := testConfig(t, "echo")
	s, openReap, err := startIdentifyTestRunner(t, cfg)
	_ = openReap
	if err != nil {
		t.Fatal(err)
	}
	release, open := identifyBarrier(t)
	entered := make(chan struct{}, 2)
	setIdentifyGate(s, func() { entered <- struct{}{}; <-release })

	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := identifyCall(t, s); results <- err }()
		// Admit the next snapshot only after the prior one has released its
		// locks. Both replies remain in flight; allowed snapshot busy is not
		// a failure of the independent handler-slot bound.
		awaitIdentifyEvent(t, entered, "IDENTIFY_TWO_SLOTS")
	}
	if len(s.identifySlots) != 2 {
		t.Fatal("IDENTIFY_SLOT_BOUND_SETUP")
	}
	b, err := identifyRaw(t, cfg, identifyRequestFor(s))
	var timeout net.Error
	if err == nil || len(b) != 0 || (errors.As(err, &timeout) && timeout.Timeout()) {
		t.Fatal("IDENTIFY_THIRD_SLOT")
	}
	progress := make(chan struct{})
	go func() { s.processMu.Lock(); s.mu.Lock(); s.mu.Unlock(); s.processMu.Unlock(); close(progress) }()
	awaitIdentifyEvent(t, progress, "IDENTIFY_WIRE_LOCKS_RELEASED")
	c := testClient(t, cfg)
	call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	openReap()
	call(t, c, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
	open()
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	setIdentifyGate(s, nil)
	deadline := time.Now().Add(time.Second)
	for len(s.identifySlots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(s.identifySlots) != 0 {
		t.Fatal("IDENTIFY_SLOT_RELEASE")
	}
	for i := 0; i < 2; i++ {
		r, err := identifyCall(t, s)
		if !errors.Is(err, errIdentifyUnavailable) {
			t.Fatal("IDENTIFY_REUSED_SLOT")
		}
		requireIdentifyResult(t, r, "unavailable", "child_reaped")
	}
}

// Endpoint guards use real owned files/modes/symlink. Actual peer PID comes from
// the socket: substituting this runner's distinct owned child PID conflicts.
func TestIdentifyClientCustodyAndPeer(t *testing.T) {
	cfg := testConfig(t, "echo")
	s, openReap, err := startIdentifyTestRunner(t, cfg)
	_ = openReap
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identifyCall(t, s); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := identifyContext(t)
	defer cancel()
	if _, err := identifyForQualification(ctx, cfg.Socket, s.pid, uint32(os.Getuid()), identifyMatchingRequest(s)); !errors.Is(err, errIdentifyConflict) {
		t.Fatalf("IDENTIFY_ACTUAL_WRONG_PEER: %v", err)
	}
	link := filepath.Join(filepath.Dir(cfg.Socket), "alias.sock")
	if err := os.Symlink(cfg.Socket, link); err != nil {
		t.Fatal(err)
	}
	if err := identifyEndpoint(link, uint32(os.Getuid())); !errors.Is(err, errIdentifyConflict) {
		t.Fatal("IDENTIFY_FINAL_SYMLINK")
	}
	if err := os.Chmod(filepath.Dir(cfg.Socket), 0755); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(filepath.Dir(cfg.Socket), 0700)
	if err := identifyEndpoint(cfg.Socket, uint32(os.Getuid())); !errors.Is(err, errIdentifyConflict) {
		t.Fatal("IDENTIFY_PARENT_MODE")
	}
	os.Chmod(filepath.Dir(cfg.Socket), 0700)
	if err := os.Chmod(cfg.Socket, 0666); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(cfg.Socket, 0600)
	if err := identifyEndpoint(cfg.Socket, uint32(os.Getuid())); !errors.Is(err, errIdentifyConflict) {
		t.Fatal("IDENTIFY_SOCKET_MODE")
	}
	if _, err := identifyForQualification(context.Background(), cfg.Socket, os.Getpid(), uint32(os.Getuid()), identifyMatchingRequest(s)); !errors.Is(err, errIdentifyUnavailable) {
		t.Fatal("IDENTIFY_CALLER_DEADLINE")
	}
}

// Directly inspect legacy limits through their retained decoder; identify cap
// does not constrain an ordinary padded status frame above1024 and below64KiB.
func TestIdentifyLegacyFrameBound(t *testing.T) {
	cfg := testConfig(t, "echo")
	_, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	frame := append(bytes.Repeat([]byte(" "), 2048), []byte("{\"op\":\"status\",\"execution\":\""+cfg.Execution+"\",\"epoch\":0}\n")...)
	b, err := identifyRaw(t, cfg, frame)
	if err != nil {
		t.Fatalf("IDENTIFY_LEGACY_FRAME: %v", err)
	}
	var r Response
	if err := localipc.Decode(bytes.NewReader(b), &r); err != nil || r.Code != "ok" {
		t.Fatalf("IDENTIFY_LEGACY_FRAME: %v %+v", err, r)
	}
	if len(frame) <= identifyMaxFrame || localipc.MaxFrame != 64<<10 {
		t.Fatal("IDENTIFY_LEGACY_BOUND_SETUP")
	}
}

type identifyStatFixture struct {
	os.FileInfo
	stat any
}

func (f identifyStatFixture) Sys() any { return f.stat }

// Stat-owner/read failures use copied owned metadata only. No actual file
// ownership or user account changes are made.
func TestIdentifyEndpointMetadata(t *testing.T) {
	cfg := testConfig(t, "echo")
	_, _, err := startIdentifyTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := os.Lstat(filepath.Dir(cfg.Socket))
	if err != nil {
		t.Fatal(err)
	}
	socket, err := os.Lstat(cfg.Socket)
	if err != nil {
		t.Fatal(err)
	}
	uid := uint32(os.Getuid())
	if err := identifyEndpointMetadata(parent, socket, uid); err != nil {
		t.Fatal(err)
	}
	for _, which := range []string{"parent", "socket", "missing"} {
		t.Run(which, func(t *testing.T) {
			p, s := parent, socket
			want := errIdentifyConflict
			switch which {
			case "parent":
				stat := *(parent.Sys().(*syscall.Stat_t))
				stat.Uid++
				p = identifyStatFixture{parent, &stat}
			case "socket":
				stat := *(socket.Sys().(*syscall.Stat_t))
				stat.Uid++
				s = identifyStatFixture{socket, &stat}
			case "missing":
				s = identifyStatFixture{socket, nil}
				want = errIdentifyUnavailable
			}
			if err := identifyEndpointMetadata(p, s, uid); !errors.Is(err, want) {
				t.Fatalf("IDENTIFY_ENDPOINT_OWNER_%s: %v", which, err)
			}
		})
	}
}

// With exactly one gated delivery and the reaper paused before its first lock,
// both lifecycle locks must be immediately free. Normal owned stop/reap then
// completes while reply delivery is still held, with no acknowledgement.
func TestIdentifyWireLockSeparation(t *testing.T) {
	cfg := testConfig(t, "echo")
	s, openReap, err := startIdentifyTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	release, open := identifyBarrier(t)
	entered := make(chan struct{}, 1)
	setIdentifyGate(s, func() { entered <- struct{}{}; <-release })
	done := make(chan error, 1)
	go func() { _, err := identifyCall(t, s); done <- err }()
	awaitIdentifyEvent(t, entered, "IDENTIFY_SINGLE_WIRE_GATE")
	if !s.processMu.TryLock() {
		t.Fatal("IDENTIFY_WIRE_LOCKS_RELEASED")
	}
	muFree := s.mu.TryLock()
	if muFree {
		s.mu.Unlock()
	}
	s.processMu.Unlock()
	if !muFree {
		t.Fatal("IDENTIFY_WIRE_LOCKS_RELEASED")
	}
	c := testClient(t, cfg)
	call(t, c, Request{Op: "claim", Execution: cfg.Execution, Epoch: 1}, "ok")
	openReap()
	call(t, c, Request{Op: "stop", Execution: cfg.Execution, Epoch: 1}, "stopped")
	open()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
