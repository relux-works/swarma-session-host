// Package runner owns the fixture child independently from host connections.
package runner

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

const MaxOutput = 32 << 10
const MaxWrites = 128

const ProtocolVersion = 3
const WriteReceiptVersion = 1

type Plan struct {
	Binary string   `json:"binary"`
	Args   []string `json:"args"`
	Cwd    string   `json:"cwd"`
	Env    []string `json:"env"`
}

type Config struct {
	Socket     string
	Execution  string
	Plan       Plan
	IOTimeout  time.Duration
	StopSignal string
}

type Request struct {
	Op        string `json:"op"`
	Execution string `json:"execution"`
	Epoch     uint64 `json:"epoch"`
	Sequence  uint64 `json:"sequence,omitempty"`
	Data      []byte `json:"data,omitempty"`
	Offset    int    `json:"offset,omitempty"`
	Digest    string `json:"digest,omitempty"`
}

type Response struct {
	ProtocolVersion     int    `json:"protocol_version"`
	WriteReceiptVersion int    `json:"write_receipt_version"`
	Code                string `json:"code"`
	Instance            string `json:"instance"`
	Epoch               uint64 `json:"epoch"`
	PID                 int    `json:"pid"`
	Exited              bool   `json:"exited"`
	OutputEnded         bool   `json:"output_ended"`
	ExitCode            int    `json:"exit_code"`
	Data                []byte `json:"data,omitempty"`
	Offset              int    `json:"offset"`
	Overflow            bool   `json:"overflow"`
	Writes              int    `json:"writes"`
}

type childInput interface {
	Write([]byte) (int, error)
	Close() error
	SetWriteDeadline(time.Time) error
}

type writeRecord struct {
	digest [32]byte
	code   string
}

type Server struct {
	cfg         Config
	listener    *net.UnixListener
	child       *os.Process
	processMu   sync.Mutex
	pid         int
	original    originalChild
	childState  childOwnership
	stdin       childInput
	stdout      io.ReadCloser
	instance    string
	done        chan struct{}
	outputDone  chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
	closeErr    error
	wg          sync.WaitGroup
	mu          sync.Mutex
	writeMu     sync.Mutex
	owner       *net.UnixConn
	conns       map[*net.UnixConn]bool
	epoch       uint64
	seq         uint64
	writes      map[uint64]writeRecord
	output      []byte
	changed     chan struct{}
	blocked     bool
	overflow    bool
	outputEnded bool
	exited      bool
	waitUnknown bool
	exitCode    int

	cooperativeStopOnce sync.Once
	cooperativeStopCode string

	identifySlots       chan struct{}
	identifyBeforeReply func() // guarded by mu; qualification barrier, nil in production
}

func (s *Server) ChildPID() int { return s.pid }

func Start(cfg Config) (*Server, error) {
	return startWithBirthCapture(cfg, captureChildBirth)
}

// The capture argument is per-instance so failure fixtures never alter global
// syscalls. The public path always uses the platform kernel capture.
func startWithBirthCapture(cfg Config, capture func(int) (childBirth, error)) (*Server, error) {
	return startWithChildLifecycle(cfg, capture, waitOriginalChild)
}

func startWithChildLifecycle(cfg Config, capture func(int) (childBirth, error), wait childWaiter) (*Server, error) {
	return startWithReaperGate(cfg, capture, wait, nil)
}

// A qualification-only gate runs before the first lifecycle lock acquisition;
// ordinary start always passes nil and ownership/reap semantics stay unchanged.
func startWithReaperGate(cfg Config, capture func(int) (childBirth, error), wait childWaiter, beforeReap func()) (*Server, error) {
	if cfg.Execution == "" || len(cfg.Execution) > 128 || cfg.IOTimeout <= 0 || cfg.IOTimeout > time.Minute ||
		!filepath.IsAbs(cfg.Plan.Binary) || !filepath.IsAbs(cfg.Plan.Cwd) || cfg.Plan.Env == nil || len(cfg.Plan.Args) > 32 ||
		(cfg.StopSignal != "" && cfg.StopSignal != "none" && cfg.StopSignal != "term") {
		return nil, errors.New("invalid_runner_plan")
	}
	// Bind before child creation: an occupied endpoint must have no spawn effect.
	listener, err := localipc.Listen(cfg.Socket)
	if err != nil {
		return nil, err
	}
	instance, err := randomInstance()
	if err != nil {
		listener.Close()
		return nil, err
	}
	childIn, stdin, err := os.Pipe()
	if err != nil {
		listener.Close()
		return nil, errors.New("child_pipe_failed")
	}
	defer childIn.Close()
	stdout, childOut, err := os.Pipe()
	if err != nil {
		stdin.Close()
		listener.Close()
		return nil, errors.New("child_pipe_failed")
	}
	defer childOut.Close()
	stderr, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		stdin.Close()
		stdout.Close()
		listener.Close()
		return nil, errors.New("child_pipe_failed")
	}
	defer stderr.Close()
	process, err := os.StartProcess(cfg.Plan.Binary, append([]string{cfg.Plan.Binary}, cfg.Plan.Args...),
		&os.ProcAttr{Dir: cfg.Plan.Cwd, Env: cfg.Plan.Env, Files: []*os.File{childIn, childOut, stderr}})
	if err != nil {
		stdin.Close()
		stdout.Close()
		listener.Close()
		return nil, errors.New("child_start_failed")
	}
	original := captureOriginalChild(process.Pid, capture)
	s := &Server{cfg: cfg, listener: listener, child: process, pid: process.Pid, original: original, childState: childOwned, stdin: stdin, stdout: stdout,
		instance: instance, identifySlots: make(chan struct{}, 2), done: make(chan struct{}), outputDone: make(chan struct{}), closed: make(chan struct{}),
		conns: make(map[*net.UnixConn]bool), writes: make(map[uint64]writeRecord), changed: make(chan struct{}), exitCode: -1}
	s.wg.Add(3)
	go s.collect(stdout)
	go func() {
		if beforeReap != nil {
			beforeReap()
		}
		s.reap(wait)
	}()
	go s.accept()
	return s, nil
}

func randomInstance() (string, error) {
	// Public incarnation identifier, not an authentication credential.
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", errors.New("instance_generation_failed")
	}
	return hex.EncodeToString(value), nil
}

func (s *Server) changedLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Server) collect(stdout io.ReadCloser) {
	defer s.wg.Done()
	defer close(s.outputDone)
	buffer := make([]byte, 4096)
	for {
		n, err := stdout.Read(buffer)
		if n > 0 {
			s.mu.Lock()
			if len(s.output)+n > MaxOutput {
				s.overflow, s.blocked = true, true
			} else if !s.overflow {
				s.output = append(s.output, buffer[:n]...)
			}
			s.changedLocked()
			s.mu.Unlock()
		}
		if err != nil {
			break
		}
	}
	s.mu.Lock()
	// Closing the actual pipe cancels a reserved writer that has not entered
	// native I/O and wakes a backpressured write. Publish termination only once
	// that cancellation boundary has completed; do not wait on writeMu, which
	// the canceled writer itself owns. Its reserved receipt stays uncertain.
	s.blocked = true
	s.stdin.Close()
	s.outputEnded = true
	s.changedLocked()
	s.mu.Unlock()
	stdout.Close()
}

func (s *Server) reap(wait childWaiter) {
	defer s.wg.Done()
	// Nonblocking wait and signal share processMu. Until this code reaps the
	// child its PID remains allocated, even after exit. This avoids Darwin's
	// concurrent os.Process.Wait/Signal PID-reuse window without PID adoption.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.processMu.Lock()
		s.mu.Lock()
		owns := s.ownsChildLocked()
		s.mu.Unlock()
		var status syscall.WaitStatus
		pid, err := -1, error(syscall.ECHILD)
		if owns {
			pid, err = wait(s, &status)
		}
		if err == syscall.EINTR {
			s.processMu.Unlock()
			continue
		}
		state, terminal := classifyChildWait(s.pid, pid, err)
		if terminal {
			s.mu.Lock()
			s.publishChildWaitLocked(state, status)
			s.mu.Unlock()
			if s.child != nil {
				s.child.Release()
			}
			s.processMu.Unlock()
			return
		}
		s.processMu.Unlock()
		<-ticker.C
	}
}

func (s *Server) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			return
		}
		accepted := time.Now()
		if s.register(conn) {
			go s.handle(conn, accepted)
		}
	}
}

// register and shutdown enumerate connections under the same lock. The accept
// goroutine remains counted until every accepted connection is rejected or added.
func (s *Server) register(conn *net.UnixConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isClosedLocked() || len(s.conns) >= 8 {
		conn.Close()
		return false
	}
	s.conns[conn] = true
	s.wg.Add(1)
	return true
}

func (s *Server) isClosedLocked() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *Server) handle(conn *net.UnixConn, accepted time.Time) {
	s.handleWithPeerUID(conn, accepted, localipc.PeerUID)
}

// Per-connection credential seam is used only by qualification fixtures.
func (s *Server) handleWithPeerUID(conn *net.UnixConn, accepted time.Time, peerUID func(*net.UnixConn) (uint32, error)) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		if s.owner == conn {
			s.owner = nil
		}
		s.mu.Unlock()
		conn.Close()
	}()
	uid, err := peerUID(conn)
	if err != nil || uid != uint32(os.Getuid()) {
		return
	}
	reader := bufio.NewReaderSize(conn, localipc.MaxFrame)
	first := true
	for {
		owner := false
		if !first {
			s.mu.Lock()
			owner = s.owner == conn
			s.mu.Unlock()
		}
		deadline := time.Now().Add(s.cfg.IOTimeout)
		if owner {
			deadline = time.Time{}
		}
		conn.SetReadDeadline(deadline)
		frame, err := reader.ReadSlice('\n')
		if err != nil {
			return
		}
		if first && !bytes.HasPrefix(bytes.TrimSpace(frame), []byte("{")) {
			return
		}
		if first && offersIdentify(frame) {
			s.handleIdentify(conn, frame, accepted)
			return
		}
		first = false
		var req Request
		if localipc.Decode(bytes.NewReader(frame), &req) != nil {
			return
		}
		response := s.process(conn, req)
		conn.SetWriteDeadline(time.Now().Add(s.cfg.IOTimeout))
		if localipc.Write(conn, response) != nil {
			return
		}
	}
}

func (s *Server) snapshotLocked(code string) Response {
	return Response{ProtocolVersion: ProtocolVersion, WriteReceiptVersion: WriteReceiptVersion, Code: code, Instance: s.instance, Epoch: s.epoch, PID: s.pid,
		Exited: s.exited, OutputEnded: s.outputEnded, ExitCode: s.exitCode, Offset: len(s.output), Overflow: s.overflow, Writes: len(s.writes)}
}

func (s *Server) process(conn *net.UnixConn, req Request) Response {
	if req.Op == "claim" {
		// Claim and effects use the same serialization lock. Replacing a
		// controller cannot overtake an already admitted pipe write or stop.
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.isClosedLocked() {
			return s.snapshotLocked("runner_closed")
		}
		if req.Execution != s.cfg.Execution {
			return s.snapshotLocked("wrong_execution")
		}
		if s.owner != nil || req.Epoch <= s.epoch || req.Sequence != 0 || len(req.Data) != 0 || req.Offset != 0 || req.Digest != "" {
			return s.snapshotLocked("controller_conflict")
		}
		s.owner, s.epoch = conn, req.Epoch
		return s.snapshotLocked("ok")
	}
	s.mu.Lock()
	if s.isClosedLocked() {
		defer s.mu.Unlock()
		return s.snapshotLocked("runner_closed")
	}
	if req.Execution != s.cfg.Execution {
		defer s.mu.Unlock()
		return s.snapshotLocked("wrong_execution")
	}
	if req.Op == "status" {
		defer s.mu.Unlock()
		return s.snapshotLocked("ok")
	}
	if s.owner != conn || req.Epoch != s.epoch {
		defer s.mu.Unlock()
		return s.snapshotLocked("stale_controller")
	}
	s.mu.Unlock()
	switch req.Op {
	case "write":
		return s.write(conn, req)
	case "write-receipt":
		return s.receipt(conn, req)
	case "read":
		return s.read(req)
	case "stop":
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		s.mu.Lock()
		if s.owner != conn || req.Epoch != s.epoch {
			response := s.snapshotLocked("stale_controller")
			s.mu.Unlock()
			return response
		}
		s.blocked = true
		s.mu.Unlock()
		return s.stop()
	case "release":
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.owner != conn || req.Epoch != s.epoch {
			return s.snapshotLocked("stale_controller")
		}
		s.owner = nil
		return s.snapshotLocked("released")
	default:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.snapshotLocked("unsupported_operation")
	}
}

// receipt observes a reserved effect without reserving or repeating any write.
// It can observe write_unknown while a pipe write is still in flight.
func (s *Server) receipt(conn *net.UnixConn, req Request) Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owner != conn || req.Epoch != s.epoch {
		return s.snapshotLocked("stale_controller")
	}
	digest, err := hex.DecodeString(req.Digest)
	if err != nil || len(digest) != sha256.Size || req.Sequence == 0 || req.Sequence > MaxWrites || len(req.Data) != 0 || req.Offset != 0 {
		return s.snapshotLocked("invalid_receipt")
	}
	prior, ok := s.writes[req.Sequence]
	if !ok {
		return s.snapshotLocked("write_missing")
	}
	if !bytes.Equal(prior.digest[:], digest) {
		return s.snapshotLocked("sequence_conflict")
	}
	return s.snapshotLocked(prior.code)
}

func (s *Server) write(conn *net.UnixConn, req Request) Response {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if s.owner != conn || req.Epoch != s.epoch {
		defer s.mu.Unlock()
		return s.snapshotLocked("stale_controller")
	}
	digest := sha256.Sum256(req.Data)
	if prior, ok := s.writes[req.Sequence]; ok {
		defer s.mu.Unlock()
		if prior.digest != digest {
			return s.snapshotLocked("sequence_conflict")
		}
		return s.snapshotLocked(prior.code)
	}
	if s.blocked || s.exited || req.Sequence != s.seq+1 || len(req.Data) == 0 || len(req.Data) > 4096 || len(s.writes) >= MaxWrites {
		defer s.mu.Unlock()
		return s.snapshotLocked("write_refused")
	}
	s.seq = req.Sequence
	// Reserve the unknown effect before touching stdin.
	s.writes[req.Sequence] = writeRecord{digest: digest, code: "write_unknown"}
	s.mu.Unlock()
	s.stdin.SetWriteDeadline(time.Now().Add(s.cfg.IOTimeout))
	n, err := s.stdin.Write(req.Data)
	s.mu.Lock()
	defer s.mu.Unlock()
	code := "written"
	if err != nil || n != len(req.Data) {
		code, s.blocked = "write_unknown", true
	}
	s.writes[req.Sequence] = writeRecord{digest: digest, code: code}
	return s.snapshotLocked(code)
}

func (s *Server) read(req Request) Response {
	deadline := time.NewTimer(s.cfg.IOTimeout)
	defer deadline.Stop()
	for {
		s.mu.Lock()
		if req.Offset < 0 || req.Offset > len(s.output) || s.overflow {
			response := s.snapshotLocked("output_unavailable")
			s.mu.Unlock()
			return response
		}
		if req.Offset < len(s.output) || s.exited || s.outputEnded {
			response := s.snapshotLocked("ok")
			response.Data = bytes.Clone(s.output[req.Offset:])
			s.mu.Unlock()
			return response
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-deadline.C:
			s.mu.Lock()
			response := s.snapshotLocked("read_timeout")
			s.mu.Unlock()
			return response
		case <-s.closed:
			return Response{Code: "runner_closed"}
		}
	}
}

func (s *Server) stop() Response {
	if s.cfg.StopSignal != "term" {
		return s.stopDirect()
	}
	// One attempt owns all three phase budgets. Concurrent calls join it;
	// retries cannot restart grace or turn a forced cutoff into success.
	s.cooperativeStopOnce.Do(func() {
		s.cooperativeStopCode = s.stopCooperatively((*os.Process).Signal)
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked(s.cooperativeStopCode)
}

// The legacy path deliberately retains its EOF/forced-direct-child semantics.
func (s *Server) stopDirect() Response {
	s.stdin.Close()
	timer := time.NewTimer(s.cfg.IOTimeout)
	defer timer.Stop()
	select {
	case <-s.done:
	case <-timer.C:
		s.processMu.Lock()
		s.mu.Lock()
		maySignal := s.ownsChildLocked() && !s.exited && !s.waitUnknown
		s.mu.Unlock()
		if maySignal {
			// Only this unreaped child; never a PID reconstructed from state.
			s.child.Kill()
		}
		s.processMu.Unlock()
		select {
		case <-s.done:
		case <-time.After(s.cfg.IOTimeout):
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.snapshotLocked("stop_unknown")
		}
	}
	select {
	case <-s.outputDone:
	case <-time.After(s.cfg.IOTimeout):
		s.stdout.Close()
		s.mu.Lock()
		s.blocked, s.overflow = true, true
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exited || s.waitUnknown {
		return s.snapshotLocked("stop_unknown")
	}
	return s.snapshotLocked("stopped")
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		close(s.closed)
		s.listener.Close()
		for conn := range s.conns {
			conn.Close()
		}
		s.mu.Unlock()
		if result := s.stop(); result.Code != "stopped" {
			s.stdout.Close()
			s.closeErr = errors.New("runner_stop_unknown")
			// A cooperative deadline remains unknown after forced root reap,
			// but its completed reaper and closed transports still need joining.
			// Never wait indefinitely for a child whose wait is unresolved.
			select {
			case <-s.done:
			default:
				return
			}
		}
		s.wg.Wait()
	})
	return s.closeErr
}

type Client struct {
	conn   *net.UnixConn
	reader *bufio.Reader
	mu     sync.Mutex
}

func Dial(ctx context.Context, socket string) (*Client, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, err
	}
	unix := conn.(*net.UnixConn)
	uid, err := localipc.PeerUID(unix)
	if err != nil || uid != uint32(os.Getuid()) {
		conn.Close()
		return nil, errors.New("runner_peer_refused")
	}
	return &Client{conn: unix, reader: bufio.NewReaderSize(conn, localipc.MaxFrame)}, nil
}

func (c *Client) Call(ctx context.Context, req Request) (Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	deadline, ok := ctx.Deadline()
	if !ok {
		return Response{}, errors.New("deadline_required")
	}
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	stop := context.AfterFunc(ctx, func() { c.conn.Close() })
	defer stop()
	c.conn.SetDeadline(deadline)
	if err := localipc.Write(c.conn, req); err != nil {
		return Response{}, err
	}
	var response Response
	if err := localipc.Read(c.reader, &response); err != nil {
		return Response{}, err
	}
	return response, nil
}

func (c *Client) Close() error { return c.conn.Close() }
