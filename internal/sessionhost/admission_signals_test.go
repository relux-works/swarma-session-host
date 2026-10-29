//go:build darwin && cgo

package sessionhost_test

import (
	"bufio"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// signalFixture is a test runner peer with one actual owned, unreaped child.
// Its only signal site records the target before Kill. This qualifies host
// routing/effect counts; the production runner retains its separate stop tests.
type signalFixture struct {
	mu      sync.Mutex
	child   *exec.Cmd
	epoch   uint64
	targets []int
	writes  int
	waited  bool
	conns   map[*net.UnixConn]bool
}

func (f *signalFixture) serve(conn *net.UnixConn) {
	defer conn.Close()
	defer func() { f.mu.Lock(); delete(f.conns, conn); f.mu.Unlock() }()
	reader := bufio.NewReaderSize(conn, localipc.MaxFrame)
	for {
		var q runner.Request
		if localipc.Read(reader, &q) != nil {
			return
		}
		f.mu.Lock()
		r := runner.Response{Code: "ok", Instance: "11111111111111111111111111111111", PID: f.child.Process.Pid, Epoch: f.epoch, Exited: f.waited, Writes: f.writes}
		switch q.Op {
		case "claim":
			f.epoch = q.Epoch
			r.Epoch = q.Epoch
		case "status", "release":
		case "write":
			f.writes++
			r.Code = "unexpected_write"
		case "stop":
			if q.Epoch != f.epoch {
				r.Code = "stale_controller"
				break
			}
			if !f.waited {
				f.targets = append(f.targets, f.child.Process.Pid)
				if err := f.child.Process.Kill(); err != nil {
					r.Code = "stop_unknown"
					break
				}
				// A signal termination is expected; Wait reaps only this child.
				f.child.Wait()
				f.waited = true
				status, ok := f.child.ProcessState.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					r.Code = "stop_unknown"
					break
				}
			}
			r.Code = "stopped"
			r.Exited = true
		default:
			r.Code = "unsupported_operation"
		}
		f.mu.Unlock()
		if localipc.Write(conn, r) != nil {
			return
		}
	}
}
func signalRunner(t *testing.T) (sessionhost.Config, *signalFixture) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "admission-signals-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(binary, "-test.run=^TestFixtureProcess$")
	child.Env = []string{"SESSION_HOST_PROTOCOL=json", "GORACE=atexit_sleep_ms=0"}
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	f := &signalFixture{child: child, conns: make(map[*net.UnixConn]bool)}
	socket := filepath.Join(dir, "r.sock")
	listener, err := localipc.Listen(socket)
	if err != nil {
		child.Process.Kill()
		child.Wait()
		input.Close()
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.AcceptUnix()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns[conn] = true
			f.mu.Unlock()
			wg.Add(1)
			go func() { defer wg.Done(); f.serve(conn) }()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		f.mu.Lock()
		for c := range f.conns {
			c.Close()
		}
		f.mu.Unlock()
		wg.Wait()
		input.Close()
		f.mu.Lock()
		if !f.waited {
			child.Process.Kill()
			child.Wait()
			f.waited = true
		}
		f.mu.Unlock()
	})
	return sessionhost.Config{StateDir: filepath.Join(dir, "state"), Socket: socket, Execution: "test-execution", Adapter: fixtureadapters.JSON{}, IOTimeout: time.Second, Selection: sessionhost.Selection{Vendor: "future-vendor", Model: "not-a-compiled-model", Reasoning: "new-reasoning"}}, f
}

// Failed admission never signals or writes through the fixture runner; explicit
// owned stop makes exactly one Kill attempt, targeting only its unreaped child.
// Status/idle/fence observations and refused reopen add no signals or writes.
func TestAdmissionOwnedStopSignalCounts(t *testing.T) {
	cfg, f := signalRunner(t)
	cfg, p := admitted(t, cfg)
	h := openHost(t, cfg)
	p.set("lease")
	if _, err := h.Enqueue(sessionhost.Notice{ID: "queued", Reference: "fixture/queued"}); err != nil {
		t.Fatal(err)
	}
	awaitAdmissionFailure(t, h)
	if err := h.ObserveInput(); err != nil {
		t.Fatal(err)
	}
	if err := h.Fence(true); err == nil || err.Error() != "reconciliation_required" {
		t.Fatalf("release: %v", err)
	}
	f.mu.Lock()
	writes, signals := f.writes, len(f.targets)
	f.mu.Unlock()
	if writes != 0 || signals != 0 {
		t.Fatalf("loss performed effects: writes=%d signals=%d", writes, signals)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := h.Stop(ctx)
	if err != nil || response.Code != "stopped" {
		t.Fatalf("owned stop: %v %s", err, response.Code)
	}
	h.Close()
	rejected, err := sessionhost.Open(cfg)
	if err == nil {
		rejected.Close()
		t.Fatal("failed admission reopened")
	}
	if err.Error() != "admission_reconciliation_required" {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writes != 0 || len(f.targets) != 1 || f.targets[0] != p.proof.Record.Binding.Harness.PID || !f.waited {
		t.Fatal("only owned stop may signal the retained child")
	}
}
