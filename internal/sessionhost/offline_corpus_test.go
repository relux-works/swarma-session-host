package sessionhost_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
	"github.com/relux-works/swarma-session-host/internal/testsupport/adaptercorpus"
)

const offlineOutputSentinel = "fabricated-offline-output-only-canary"

func offlineExists(path string) bool { _, err := os.Stat(path); return err == nil }
func offlineWaitFile(path string, done <-chan struct{}) bool {
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if offlineExists(path) {
			return true
		}
		select {
		case <-done:
			return false
		case <-timer.C:
			return false
		case <-ticker.C:
		}
	}
}

// This is a synthetic Go child, not a native harness. Its input counter and
// independently gated output let the parent witness actual pipe effects.
func TestOfflineRPCChild(t *testing.T) {
	if os.Getenv("SESSION_HOST_OFFLINE_RPC") != "1" {
		return
	}
	var output sync.Mutex
	write := func(v any) {
		output.Lock()
		defer output.Unlock()
		if json.NewEncoder(os.Stdout).Encode(v) != nil {
			os.Exit(10)
		}
	}
	done := make(chan struct{})
	injected := make(chan struct{})
	go func() {
		defer close(injected)
		if !offlineWaitFile("inject-output", done) {
			return
		}
		mode, err := os.ReadFile("inject-output")
		if err != nil {
			os.Exit(11)
		}
		output.Lock()
		switch string(mode) {
		case "overflow", "overflow-eof":
			_, err = os.Stdout.Write(bytes.Repeat([]byte("x"), runner.MaxOutput+1))
			if err == nil && string(mode) == "overflow-eof" {
				_, err = io.WriteString(os.Stdout, "{\"method\":\"future/notification\",\"params\":{\"text\":\""+offlineOutputSentinel)
				if err == nil {
					err = os.Stdout.Close()
				}
			}
		case "eof":
			_, err = io.WriteString(os.Stdout, "{\"method\":\"future/notification\",\"params\":{\"text\":\""+offlineOutputSentinel)
			if err == nil {
				err = os.Stdout.Close()
			}
		case "normal":
			_, err = io.WriteString(os.Stdout, "{\"method\":\"future/notification\",\"params\":{\"text\":\""+offlineOutputSentinel+"\"}}\n")
		default:
			os.Exit(20) // an incomplete/unknown trigger is never normal output
		}
		output.Unlock()
		if err != nil {
			os.Exit(12)
		}
		// Publish the selected mode atomically in both directions: existence
		// means all acknowledgement bytes are available to the parent.
		if os.WriteFile("injected-output.pending", mode, 0600) != nil {
			os.Exit(13)
		}
		if os.Rename("injected-output.pending", "injected-output") != nil {
			os.Exit(13)
		}
	}()
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var req struct {
			ID     uint64
			Method string
			Params map[string]any
		}
		if json.Unmarshal(scanner.Bytes(), &req) != nil {
			os.Exit(14)
		}
		f, err := os.OpenFile("input-methods", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(15)
		}
		if _, err = f.WriteString(req.Method + "\n"); err != nil {
			os.Exit(16)
		}
		if f.Close() != nil {
			os.Exit(17)
		}
		switch req.Method {
		case "initialize":
			write(map[string]any{"id": req.ID, "result": map[string]any{"userAgent": "synthetic", "platformFamily": "unix", "platformOs": "macos"}})
		case "initialized":
		case "thread/start":
			write(map[string]any{"id": req.ID, "result": map[string]any{"thread": map[string]any{"id": "offline-thread"}, "model": req.Params["model"], "modelProvider": req.Params["modelProvider"]}})
		case "turn/start":
			turn := fmt.Sprintf("turn-%d", req.ID)
			write(map[string]any{"id": req.ID, "result": map[string]any{"turn": map[string]any{"id": turn, "status": "inProgress"}}})
			if !offlineWaitFile("complete-"+turn, done) {
				os.Exit(18)
			}
			write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "offline-thread", "turn": map[string]any{"id": turn, "status": "completed"}}})
		default:
			os.Exit(19)
		}
	}
	close(done)
	<-injected
	os.Exit(0)
}

type offlineDriver struct {
	ap.Driver
	c            adaptercorpus.Case
	crash        bool
	active       bool
	mu           sync.Mutex
	observed     bool
	observeErr   string
	cycles       uint64
	cycleChanged chan struct{}
}

func (d *offlineDriver) Prepare(in ap.Input) (ap.Transition, error) {
	x, err := d.Driver.Prepare(in)
	if d.crash && in.ID == "one" && err == nil && len(x.Actions) == 1 {
		crashBarrier("offline-prepare") // real transition, before host persists it
	}
	return x, err
}
func (d *offlineDriver) Observe(chunk ap.Chunk) (ap.Transition, error) {
	defer d.finishCycle()
	if !d.active || len(chunk.Bytes) == 0 {
		return d.Driver.Observe(chunk)
	}
	d.mu.Lock()
	first := !d.observed
	d.observed = true
	d.mu.Unlock()
	if !first {
		return d.Driver.Observe(chunk)
	}
	var result ap.Transition
	var actions []ap.Action
	var facts []ap.Fact
	parts := adaptercorpus.Parts(chunk.Bytes, d.c.C)
	at := chunk.Offset
	for i, p := range parts {
		offset := at
		if i == 0 {
			if d.c.O == 1 {
				offset++
			}
			if d.c.O == 2 {
				offset--
			}
		}
		x, err := d.Driver.Observe(ap.Chunk{Offset: offset, Bytes: p, AtTail: i == len(parts)-1 && chunk.AtTail, EOF: i == len(parts)-1 && chunk.EOF})
		actions = append(actions, x.Actions...)
		facts = append(facts, x.Facts...)
		result = x
		at += uint64(len(p))
		if err != nil {
			d.mu.Lock()
			d.observeErr = err.Error()
			d.mu.Unlock()
			result.Actions, result.Facts = actions, facts
			return result, err
		}
	}
	result.Actions, result.Facts = actions, facts
	return result, nil
}

// A single dispatcher calls Observe once per successful poll. Two completions
// after enqueue prove the first poll's later dispatch decision finished while
// this host remained live. This synchronizes on actual work, never a sleep.
func (d *offlineDriver) finishCycle() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cycles++
	if d.cycleChanged != nil {
		close(d.cycleChanged)
		d.cycleChanged = make(chan struct{})
	}
}
func (d *offlineDriver) awaitSuccessorCycle(t *testing.T) uint64 {
	t.Helper()
	d.mu.Lock()
	start := d.cycles
	d.mu.Unlock()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		d.mu.Lock()
		finished, changed := d.cycles-start, d.cycleChanged
		d.mu.Unlock()
		if finished >= 2 {
			return finished
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatal("OFFLINE_INPUT_FENCE: successor worker cycle not reached")
		}
	}
}

func (d *offlineDriver) observation() (bool, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.observed, d.observeErr
}

func offlineConfig(dir, socket string) sessionhost.Config {
	cfg := sessionhost.Config{StateDir: filepath.Join(dir, "state"), Socket: socket, Execution: "offline-execution", Selection: sessionhost.Selection{Vendor: "offline-provider", Model: "offline-model", Reasoning: "offline-effort"}, ProtocolName: "codex", PollInterval: time.Millisecond, IOTimeout: time.Second}
	cfg.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
		return codexadapter.OpenDriver(ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create, Selection: ap.Selection{Provider: cfg.Selection.Vendor, Model: cfg.Selection.Model, Effort: cfg.Selection.Reasoning}, Workspace: dir}, saved)
	}
	return cfg
}

// The parent supplies only explicit synthetic paths and kills this owned helper
// after the real requested frontier; no runtime or storage implementation is replaced.
func TestOfflineHostProcess(t *testing.T) {
	if os.Getenv("SESSION_HOST_OFFLINE_HOST") != "1" {
		return
	}
	dir, socket := os.Getenv("OFFLINE_DIR"), os.Getenv("OFFLINE_SOCKET")
	cfg := offlineConfig(dir, socket)
	factory := cfg.ProtocolFactory
	cfg.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
		d, off, err := factory(saved)
		if err != nil {
			return nil, 0, err
		}
		return &offlineDriver{Driver: d, crash: os.Getenv("OFFLINE_PREPARE_BARRIER") == "1"}, off, nil
	}
	h, err := sessionhost.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	awaitProtocol(t, h, "offline helper idle", func(s sessionhost.State) bool { return s.InputState == sessionhost.InputIdle })
	if _, err = h.Enqueue(sessionhost.Notice{ID: "one", Reference: "synthetic/one"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(12 * time.Second):
		t.Fatal("OFFLINE_CRASH_NOT_REACHED")
	}
}

type offlineRequestTrace struct {
	Recovery bool
	Op       string
	Sequence uint64
	Digest   string
}
type offlineProxy struct {
	listener         *net.UnixListener
	upstream         string
	barrier, release chan struct{}
	done             chan struct{}
	wg               sync.WaitGroup
	mu               sync.Mutex
	conns            []*net.UnixConn
	trace            []offlineRequestTrace
	recovering       bool
	phase            int
	errors           []error
	releaseOnce      sync.Once
}

func offlineProxyStart(t *testing.T, dir, upstream string, phase int) *offlineProxy {
	t.Helper()
	l, err := localipc.Listen(filepath.Join(dir, "p.sock"))
	if err != nil {
		t.Fatal(err)
	}
	p := &offlineProxy{listener: l, upstream: upstream, phase: phase, barrier: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		for {
			c, err := l.AcceptUnix()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.conns = append(p.conns, c)
			p.mu.Unlock()
			p.wg.Add(1)
			go p.handle(c)
		}
	}()
	t.Cleanup(func() {
		p.finishBarrier()
		l.Close()
		p.mu.Lock()
		for _, c := range p.conns {
			c.Close()
		}
		p.mu.Unlock()
		<-p.done
		p.wg.Wait()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, err := range p.errors {
			t.Error("OFFLINE_PROXY", err)
		}
	})
	offlineResource(t, "socket", 0, l.Addr().String())
	return p
}
func (p *offlineProxy) problem(err error) {
	p.mu.Lock()
	p.errors = append(p.errors, err)
	p.mu.Unlock()
}
func (p *offlineProxy) finishBarrier() { p.releaseOnce.Do(func() { close(p.release) }) }
func (p *offlineProxy) handle(c *net.UnixConn) {
	defer p.wg.Done()
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	up, err := runner.Dial(ctx, p.upstream)
	if err != nil {
		p.problem(err)
		return
	}
	defer up.Close()
	reader := bufio.NewReader(c)
	for {
		c.SetReadDeadline(time.Now().Add(10 * time.Second))
		var req runner.Request
		if err := localipc.Read(reader, &req); err != nil {
			return
		}
		p.mu.Lock()
		recovery := p.recovering
		digest := req.Digest
		if req.Op == "write" {
			sum := sha256.Sum256(req.Data)
			digest = hex.EncodeToString(sum[:])
		}
		p.trace = append(p.trace, offlineRequestTrace{Recovery: recovery, Op: req.Op, Sequence: req.Sequence, Digest: digest})
		p.mu.Unlock()
		target := !recovery && req.Op == "write" && req.Sequence == 4
		hold := func() {
			close(p.barrier)
			select {
			case <-p.release:
			case <-ctx.Done():
				p.problem(errors.New("actual proxy barrier expired"))
			}
		}
		if target && p.phase == 2 {
			hold()
			return
		}
		callCtx, finish := context.WithTimeout(ctx, time.Second)
		response, err := up.Call(callCtx, req)
		finish()
		if err != nil {
			return
		}
		if target && p.phase == 3 {
			if response.Code != "written" {
				p.problem(fmt.Errorf("target not written: %s", response.Code))
				return
			}
			hold()
			return
		}
		c.SetWriteDeadline(time.Now().Add(time.Second))
		if err := localipc.Write(c, response); err != nil {
			return
		}
	}
}
func (p *offlineProxy) recoveringNow() { p.mu.Lock(); p.recovering = true; p.mu.Unlock() }
func (p *offlineProxy) recoveryWrites() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, r := range p.trace {
		if r.Recovery && r.Op == "write" {
			n++
		}
	}
	return n
}
func (p *offlineProxy) recoveryRequests() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, r := range p.trace {
		if r.Recovery {
			n++
		}
	}
	return n
}

func offlineResource(t *testing.T, kind string, pid int, path string) {
	t.Helper()
	b, err := json.Marshal(struct {
		Kind string "json:\"kind\""
		PID  int    "json:\"pid,omitempty\""
		Path string "json:\"path,omitempty\""
	}{kind, pid, path})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("owned_resource %s", b)
	if ledger := os.Getenv("SESSION_HOST_TEST_RESOURCES"); ledger != "" {
		f, err := os.OpenFile(ledger, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(append(b, '\n'))
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
	}
}
func offlineAwait(t *testing.T, claim string, predicate func() bool) {
	t.Helper()
	limit := time.Now().Add(5 * time.Second)
	for !predicate() {
		if time.Now().After(limit) {
			t.Fatal("OFFLINE_FRONTIER_NOT_REACHED", claim)
		}
		time.Sleep(time.Millisecond)
	}
}
func offlineTurns(t *testing.T, dir string) int {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "input-methods"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return strings.Count(string(b), "turn/start\n")
}
func offlineDisk(t *testing.T, dir string) (sessionhost.State, []byte) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "state", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var s sessionhost.State
	if json.Unmarshal(b, &s) != nil {
		t.Fatal("OFFLINE_STATE_SETUP")
	}
	return s, b
}
func offlineDiskMetadata(t *testing.T, b []byte, dir, instance string, corruption int) {
	t.Helper()
	values := adaptercorpus.MetadataValues{Model: "offline-model", Provider: "offline-provider", Effort: "offline-effort", Workspace: dir, Thread: "offline-thread", RunnerInstance: instance, Turns: []string{"turn-3", "turn-4"}, Inputs: []string{"one", "two"}}
	if err := adaptercorpus.JournalMetadata(b, values, corruption); err != nil {
		t.Fatal(err)
	}
}

// The 71 host rows exercise real recovery frontiers and selected factor pairs.
// Earlier checkpoint/overflow refusals are recorded as such; they are never
// credited as execution of a later Observe guard. No higher-order claim is made.
func TestOfflineHostCorpus(t *testing.T) {
	for _, tc := range adaptercorpus.Generate() {
		if tc.Array != "host" {
			continue
		}
		t.Run(tc.ID, func(t *testing.T) {
			status := map[string]string{"C": "not-reached", "O": "not-reached", "V": "not-reached", "E": "not-reached", "K": "not-reached", "B": "not-reached"}
			var actualProxy *offlineProxy
			receipt := struct {
				Case                  adaptercorpus.Case
				Status                map[string]string
				Frontier              string
				HelperPID             int
				HelperSignal          string
				ChildPID              int
				RunnerInstance        string
				BarrierWrites         int
				TargetInputsAtBarrier int
				RestoredWrites        int
				RestoredTargetInputs  int
				SavedSHA256           string
				SuppliedSHA256        string
				SuccessorWindow       string
				SuccessorCycles       uint64
				Trace                 []offlineRequestTrace
			}{Case: tc, Status: status}
			defer func() {
				if actualProxy != nil {
					actualProxy.mu.Lock()
					receipt.Trace = append([]offlineRequestTrace(nil), actualProxy.trace...)
					actualProxy.mu.Unlock()
				}
				b, _ := json.Marshal(receipt)
				t.Log("OFFLINE_CASE " + string(b))
			}()
			dir, err := os.MkdirTemp("/tmp", "session-offline-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(dir); err != nil {
					t.Error(err)
				}
			})
			offlineResource(t, "tempdir", 0, dir)
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			rc := runner.Config{Socket: filepath.Join(dir, "r.sock"), Execution: "offline-execution", IOTimeout: 200 * time.Millisecond, Plan: runner.Plan{Binary: binary, Args: []string{"-test.run=^TestOfflineRPCChild$"}, Cwd: dir, Env: []string{"SESSION_HOST_OFFLINE_RPC=1", "GORACE=atexit_sleep_ms=0"}}}
			s, err := runner.Start(rc)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			direct := offlineConfig(dir, rc.Socket)
			original := inspectRunner(t, direct)
			childPID := original.PID
			receipt.ChildPID, receipt.RunnerInstance = childPID, original.Instance
			offlineResource(t, "child-start", childPID, "")
			offlineResource(t, "socket", 0, rc.Socket)
			t.Cleanup(func() {
				for _, name := range []string{"complete-turn-3", "complete-turn-4"} {
					os.WriteFile(filepath.Join(dir, name), nil, 0600)
				}
				if err := s.Close(); err != nil {
					t.Error("OFFLINE_CHILD_CLEANUP", err)
				}
				if err := syscall.Kill(childPID, 0); err != syscall.ESRCH {
					t.Errorf("OWNED_CHILD_LEFTOVER: %d %v", childPID, err)
				}
				offlineResource(t, "child-end", childPID, "")
				if _, err := os.Lstat(rc.Socket); !os.IsNotExist(err) {
					t.Error("OWNED_SOCKET_LEFTOVER", err)
				}
			})
			proxy := offlineProxyStart(t, dir, rc.Socket, tc.B)
			actualProxy = proxy
			cfg := offlineConfig(dir, proxy.listener.Addr().String())
			wantWrites, wantTurns := 4, 1
			if tc.B == 1 || tc.B == 2 {
				wantWrites, wantTurns = 3, 0
			}
			if tc.B == 0 {
				h, err := sessionhost.Open(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { h.Close() })
				awaitProtocol(t, h, "offline initial idle", func(st sessionhost.State) bool { return st.InputState == sessionhost.InputIdle })
				if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "synthetic/one"}); err != nil {
					t.Fatal(err)
				}
				offlineAwait(t, "first actual target input", func() bool { return offlineTurns(t, dir) == 1 })
				if err := os.WriteFile(filepath.Join(dir, "complete-turn-3"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				await(t, h, "one", "applied")
				if err := h.Close(); err != nil {
					t.Fatal(err)
				}
				status["B"] = "reached-normal-replacement"
				receipt.Frontier = "completed first notice;normal host Close"
				receipt.BarrierWrites = inspectRunner(t, direct).Writes
				receipt.TargetInputsAtBarrier = offlineTurns(t, dir)
			} else {
				cmd := exec.Command(binary, "-test.run=^TestOfflineHostProcess$", "-test.v")
				env := []string{"SESSION_HOST_OFFLINE_HOST=1", "OFFLINE_DIR=" + dir, "OFFLINE_SOCKET=" + cfg.Socket, "GORACE=atexit_sleep_ms=0"}
				if tc.B == 1 {
					env = append(env, "OFFLINE_PREPARE_BARRIER=1")
				}
				cmd.Env = env
				input, err := cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				output, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				offlineResource(t, "child-start", cmd.Process.Pid, "")
				receipt.HelperPID = cmd.Process.Pid
				done := make(chan error, 1)
				observed := make(chan struct{}, 1)
				scanDone := make(chan struct{})
				go func() {
					defer close(scanDone)
					scanner := bufio.NewScanner(output)
					for scanner.Scan() {
						if scanner.Text() == "offline-prepare" {
							observed <- struct{}{}
						}
					}
				}()
				go func() { done <- cmd.Wait() }()
				waited := false
				t.Cleanup(func() {
					if !waited {
						cmd.Process.Kill()
						select {
						case <-done:
						case <-time.After(3 * time.Second):
							t.Error("OFFLINE_HELPER_CLEANUP")
						}
					}
					input.Close()
					<-scanDone
					if err := syscall.Kill(cmd.Process.Pid, 0); err != syscall.ESRCH {
						t.Errorf("OWNED_CHILD_LEFTOVER: helper %d %v", cmd.Process.Pid, err)
					}
					offlineResource(t, "child-end", cmd.Process.Pid, "")
				})
				var barrier <-chan struct{} = proxy.barrier
				if tc.B == 1 {
					barrier = observed
				}
				select {
				case <-barrier:
				case <-time.After(5 * time.Second):
					t.Fatal("OFFLINE_CRASH_NOT_REACHED")
				}
				atBarrier, _ := offlineDisk(t, dir)
				if len(atBarrier.Records) != 1 || atBarrier.Records[0].State != "unknown" {
					t.Fatal("OFFLINE_CRASH_FRONTIER: unknown not durable")
				}
				if tc.B == 1 && len(atBarrier.Protocol.Pending) != 0 {
					t.Fatal("OFFLINE_CRASH_FRONTIER: Prepare already committed")
				}
				if tc.B > 1 && (len(atBarrier.Protocol.Pending) != 1 || atBarrier.Protocol.Pending[0].Sequence != 4) {
					t.Fatal("OFFLINE_CRASH_FRONTIER: outbox not durable")
				}
				atRunner := inspectRunner(t, direct)
				receipt.BarrierWrites = atRunner.Writes
				receipt.Frontier = fmt.Sprintf("B%d unknown=%s pending=%d", tc.B, atBarrier.Records[0].State, len(atBarrier.Protocol.Pending))
				if atRunner.Writes != wantWrites {
					t.Fatalf("OFFLINE_CRASH_FRONTIER: writes=%d want=%d", atRunner.Writes, wantWrites)
				}
				offlineAwait(t, "actual target pipe count at barrier", func() bool { return offlineTurns(t, dir) == wantTurns })
				receipt.TargetInputsAtBarrier = offlineTurns(t, dir)
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					waited = true
					if err == nil {
						t.Fatal("OFFLINE_CRASH_EXIT")
					}
				case <-time.After(3 * time.Second):
					t.Fatal("OFFLINE_CRASH_WAIT")
				}
				exit, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
				if !ok || !exit.Signaled() || exit.Signal() != syscall.SIGKILL {
					t.Fatal("OFFLINE_CRASH_EXIT", cmd.ProcessState)
				}
				input.Close()
				proxy.finishBarrier()
				status["B"] = "reached-real-SIGKILL"
				receipt.HelperSignal = "SIGKILL"
			}
			before, raw := offlineDisk(t, dir)
			savedSum := sha256.Sum256(raw)
			receipt.SavedSHA256 = hex.EncodeToString(savedSum[:])
			offlineDiskMetadata(t, raw, dir, original.Instance, 0)
			before.Protocol.Checkpoint, err = adaptercorpus.Corrupt(before.Protocol.Checkpoint, tc.K)
			if err != nil {
				t.Fatal(err)
			}
			if tc.K != 0 {
				raw, err = json.Marshal(before)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(cfg.StateDir, "state.json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			suppliedSum := sha256.Sum256(raw)
			receipt.SuppliedSHA256 = hex.EncodeToString(suppliedSum[:])
			proxy.recoveringNow()
			factory := cfg.ProtocolFactory
			var wrapped *offlineDriver
			cfg.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
				d, offset, err := factory(saved)
				if err != nil {
					return nil, 0, err
				}
				wrapped = &offlineDriver{Driver: d, c: tc, active: true, cycleChanged: make(chan struct{})}
				return wrapped, offset, nil
			}
			next, err := sessionhost.Open(cfg)
			status["K"] = "reached"
			if tc.K != 0 {
				if err == nil || next != nil || err.Error() != "protocol_restore_refused" {
					t.Fatalf("OFFLINE_CHECKPOINT_REFUSAL: %v", err)
				}
				_, after := offlineDisk(t, dir)
				offlineDiskMetadata(t, after, dir, original.Instance, tc.K)
				if !bytes.Equal(raw, after) || proxy.recoveryRequests() != 0 {
					t.Fatal("OFFLINE_CHECKPOINT_NO_EFFECT: rejected restore changed disk/contacted runner")
				}
				receipt.RestoredTargetInputs, receipt.RestoredWrites = offlineTurns(t, dir), inspectRunner(t, direct).Writes
				if offlineTurns(t, dir) != wantTurns || inspectRunner(t, direct).Writes != wantWrites {
					t.Fatal("OFFLINE_NO_REPLAY: refused checkpoint wrote")
				}
				for _, k := range []string{"C", "O", "V", "E"} {
					status[k] = "not-reached-checkpoint-refusal"
				}
				return
			}
			if err != nil {
				t.Fatal("OFFLINE_RESTORE_SETUP", err)
			}
			t.Cleanup(func() { next.Close() })
			mode := "normal"
			if tc.E != 0 {
				mode = "eof"
			}
			if tc.V != 0 {
				mode = "overflow"
				if tc.E != 0 {
					mode = "overflow-eof"
				}
			}
			// Publish bytes before existence: the child must never observe a
			// newly created empty trigger as a different fault selection.
			trigger := filepath.Join(dir, "inject-output")
			if err := os.WriteFile(trigger+".pending", []byte(mode), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(trigger+".pending", trigger); err != nil {
				t.Fatal(err)
			}
			offlineAwait(t, "owned output injection", func() bool { return offlineExists(filepath.Join(dir, "injected-output")) })
			consumed, err := os.ReadFile(filepath.Join(dir, "injected-output"))
			if err != nil || string(consumed) != mode {
				t.Fatalf("OFFLINE_FAULT_SELECTION: want=%s got=%q err=%v", mode, consumed, err)
			}
			if tc.V != 0 {
				var lastRunner runner.Response
				var lastState sessionhost.State
				defer func() {
					t.Logf("OFFLINE_TERMINAL_WITNESS overflow=%t offset=%d ended=%t fence=%s input=%s", lastRunner.Overflow, lastRunner.Offset, lastRunner.OutputEnded, lastState.Fence, lastState.InputState)
				}()
				offlineAwait(t, "actual runner overflow and host fence", func() bool {
					lastRunner = inspectRunner(t, direct)
					lastState = next.Snapshot()
					return lastRunner.Overflow && lastState.Fence == "unknown"
				})
				status["V"] = "reached-real-runner-overflow"
				if tc.E != 0 {
					offlineAwait(t, "actual EOF after overflow", func() bool {
						return inspectRunner(t, direct).OutputEnded && next.Snapshot().InputState == sessionhost.InputUnknown
					})
					status["E"] = "reached-real-EOF;parser-path-dominated-by-overflow"
				}
			} else if tc.E != 0 {
				offlineAwait(t, "actual output EOF and host closed input", func() bool {
					st := inspectRunner(t, direct)
					return st.OutputEnded && next.Snapshot().InputState == sessionhost.InputUnknown
				})
				status["E"] = "reached-real-output-EOF"
			} else {
				offlineAwait(t, "replacement observed output", func() bool { ok, _ := wrapped.observation(); return ok })
			}
			observed, observeErr := wrapped.observation()
			if observed {
				status["C"] = "reached"
				status["O"] = "reached"
			} else {
				status["C"] = "not-reached-runner-refusal"
				status["O"] = "not-reached-runner-refusal"
			}
			if tc.O != 0 && tc.V == 0 {
				offlineAwait(t, "offset refusal fence", func() bool { return next.Snapshot().Fence == "unknown" })
				_, observeErr = wrapped.observation()
				if observeErr != "stream_offset_or_bound" {
					t.Fatalf("OFFLINE_OFFSET: %q", observeErr)
				}
			}
			if tc.O == 0 && tc.V == 0 && tc.E == 0 {
				switch tc.B {
				case 0, 1:
					awaitProtocol(t, next, "ordinary restored tail", func(st sessionhost.State) bool { return st.InputState == sessionhost.InputIdle })
				case 2:
					offlineAwait(t, "read-only missing receipt", func() bool {
						st := next.Snapshot()
						return proxy.recoveryWrites() != 0 || (len(st.Protocol.Pending) == 1 && st.Protocol.Pending[0].Receipt == "write_missing")
					})
				case 3:
					if err := os.WriteFile(filepath.Join(dir, "complete-turn-3"), nil, 0600); err != nil {
						t.Fatal(err)
					}
					offlineAwait(t, "written receipt/native completion", func() bool {
						st := next.Snapshot()
						return proxy.recoveryWrites() != 0 || (len(st.Protocol.Pending) == 0 && st.Records[0].State == "applied")
					})
				}
			}
			receipt.RestoredTargetInputs, receipt.RestoredWrites = offlineTurns(t, dir), inspectRunner(t, direct).Writes
			if proxy.recoveryWrites() != 0 {
				t.Fatal("OFFLINE_NO_REPLAY: replacement issued write")
			}
			if offlineTurns(t, dir) != wantTurns {
				t.Fatal("OFFLINE_NO_REPLAY: replacement changed target input count")
			}
			if st := inspectRunner(t, direct); st.Writes != wantWrites || st.PID != original.PID || st.Instance != original.Instance {
				t.Fatalf("OFFLINE_NO_REPLAY: original runner/child/effects changed: %+v", st)
			}
			_, raw = offlineDisk(t, dir)
			offlineDiskMetadata(t, raw, dir, original.Instance, 0)
			originalNotice := next.Snapshot().Records[0]
			same, err := next.Enqueue(sessionhost.Notice{ID: "one", Reference: "synthetic/one"})
			if err != nil || same.Notice != originalNotice.Notice || same.Sequence != originalNotice.Sequence {
				t.Fatal("OFFLINE_NO_REPLAY: exact retry changed intent", err)
			}
			if _, err := next.Enqueue(sessionhost.Notice{ID: "one", Reference: "synthetic/changed"}); err == nil || err.Error() != "request_conflict" {
				t.Fatal("OFFLINE_NO_REPLAY: changed intent accepted", err)
			}
			eligible := tc.B == 0 && tc.O == 0 && tc.V == 0 && tc.E == 0
			if !eligible {
				st := next.Snapshot()
				if st.Fence != "unknown" {
					t.Fatalf("OFFLINE_INPUT_FENCE: %s", st.Fence)
				}
				if err := next.Fence(true); err == nil || err.Error() != "reconciliation_required" {
					t.Fatalf("OFFLINE_INPUT_FENCE: want reconciliation_required got %v", err)
				}
				if _, err := next.Enqueue(sessionhost.Notice{ID: "two", Reference: "synthetic/two"}); err != nil {
					t.Fatal(err)
				}
				if tc.O == 0 && tc.V == 0 && tc.E == 0 {
					receipt.SuccessorCycles = wrapped.awaitSuccessorCycle(t)
					receipt.SuccessorWindow = "actual worker cycle completed under unknown fence"
				} else {
					// A preceding protocol failure dominates dispatch here; do not
					// pretend that its later queue decision was exercised.
					receipt.SuccessorWindow = "dominated by preceding protocol refusal"
				}
				afterCycle := next.Snapshot()
				if len(afterCycle.Records) != 2 || afterCycle.Records[1].State != "queued" || afterCycle.Fence != "unknown" {
					t.Fatal("OFFLINE_INPUT_FENCE: successor crossed the unknown delivery fence")
				}
				next.Close()
				if proxy.recoveryWrites() != 0 || offlineTurns(t, dir) != wantTurns || inspectRunner(t, direct).Writes != wantWrites {
					t.Fatal("OFFLINE_INPUT_FENCE: queued successor wrote")
				}
			} else {
				// A valid normal replacement proves this fixture does not merely
				// reject all input; a second distinct turn really reaches stdin.
				proxy.mu.Lock()
				proxy.recovering = false
				proxy.mu.Unlock()
				if _, err := next.Enqueue(sessionhost.Notice{ID: "two", Reference: "synthetic/two"}); err != nil {
					t.Fatal(err)
				}
				offlineAwait(t, "second real input", func() bool { return offlineTurns(t, dir) == 2 })
				if err := os.WriteFile(filepath.Join(dir, "complete-turn-4"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				await(t, next, "two", "applied")
				next.Close()
				if inspectRunner(t, direct).Writes != 5 {
					t.Fatal("OFFLINE_VALID_SUCCESSOR: wrong real write count")
				}
			}
			_, raw = offlineDisk(t, dir)
			offlineDiskMetadata(t, raw, dir, original.Instance, 0)
		})
	}
}
