package sessionhost_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// These fault barriers exist only in the test binary. The ordinary adapter's
// real encode/ack methods still run; no host storage or delivery hook is replaced.
type crashAdapter struct {
	sessionhost.Adapter
	phase   string
	encodes atomic.Int32
}

func crashBarrier(phase string) {
	fmt.Fprintln(os.Stdout, phase)
	// Parent owns this stdin pipe and kills this exact process while blocked.
	io.Copy(io.Discard, os.Stdin)
	os.Exit(3) // Closing the pipe is not an intended crash-control success.
}

func (a *crashAdapter) Encode(n sessionhost.Notice, s sessionhost.Selection) ([]byte, error) {
	if a.encodes.Add(1) == 2 && a.phase == "inflight" {
		crashBarrier(a.phase) // Dispatch Encode follows the durable unknown marker.
	}
	return a.Adapter.Encode(n, s)
}

func (a *crashAdapter) Applied(b []byte, n sessionhost.Notice, s sessionhost.Selection) (bool, error) {
	applied, err := a.Adapter.Applied(b, n, s)
	if applied && err == nil && a.phase == "ack" {
		crashBarrier(a.phase)
	}
	return applied, err
}

func fixtureAdapter(protocol string) sessionhost.Adapter {
	if protocol == "line" {
		return fixtureadapters.Lines{}
	}
	return fixtureadapters.JSON{}
}

// Subprocess-only helper runs the actual host against an independently owned
// runner. All environment entries are explicit synthetic fixture metadata.
func TestCrashHostProcess(t *testing.T) {
	if os.Getenv("SESSIONHOST_CRASH_HELPER") != "1" {
		return
	}
	phase := os.Getenv("SESSIONHOST_CRASH_PHASE")
	cfg := sessionhost.Config{
		StateDir: os.Getenv("SESSIONHOST_CRASH_STATE"), Socket: os.Getenv("SESSIONHOST_CRASH_SOCKET"),
		Execution: "test-execution", IOTimeout: 10 * time.Second,
		Selection: sessionhost.Selection{Vendor: "future-vendor", Model: "not-a-compiled-model", Reasoning: "new-reasoning"},
		Adapter:   &crashAdapter{Adapter: fixtureAdapter(os.Getenv("SESSIONHOST_CRASH_PROTOCOL")), phase: phase},
	}
	h, err := sessionhost.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err := h.Enqueue(sessionhost.Notice{ID: "crash-one", Reference: "fixture/crash-one"}); err != nil {
		t.Fatal(err)
	}
	if phase == "applied" {
		await(t, h, "crash-one", "applied")
		crashBarrier(phase)
	}
	// Other phases block inside an adapter or on the proxy response. This bound
	// distinguishes failure to reach the requested barrier from a passing control.
	select {
	case <-time.After(15 * time.Second):
		t.Fatal("crash barrier not reached")
	}
}

// replyBarrier relays the real claim/write to the real runner but withholds the
// successful write reply. The barrier is reached only for actual accepted bytes.
// Closing it disconnects the upstream controller before replacement admission.
func replyBarrier(t *testing.T, cfg sessionhost.Config) (string, <-chan struct{}, func()) {
	t.Helper()
	socket := filepath.Join(filepath.Dir(cfg.StateDir), "proxy.sock")
	listener, err := localipc.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reached, done := make(chan struct{}), make(chan error, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		stop := context.AfterFunc(ctx, func() { conn.Close() })
		defer stop()
		callCtx, finish := context.WithTimeout(ctx, 12*time.Second)
		defer finish()
		upstream, err := runner.Dial(callCtx, cfg.Socket)
		if err != nil {
			done <- err
			return
		}
		defer upstream.Close()
		reader := bufio.NewReader(conn)
		for {
			var request runner.Request
			if err := localipc.Read(reader, &request); err != nil {
				done <- err
				return
			}
			response, err := upstream.Call(callCtx, request)
			if err != nil {
				done <- err
				return
			}
			if request.Op == "write" {
				if response.Code != "written" || response.Writes != 1 {
					done <- fmt.Errorf("write not accepted: %+v", response)
					return
				}
				close(reached)
				<-ctx.Done()
				done <- nil
				return
			}
			if err := localipc.Write(conn, response); err != nil {
				done <- err
				return
			}
		}
	}()
	var stopped atomic.Bool
	finish := func() {
		if stopped.Swap(true) {
			return
		}
		cancel()
		listener.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("reply barrier: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("reply barrier did not close")
		}
	}
	t.Cleanup(finish)
	return socket, reached, finish
}

func replacementAfterCrash(t *testing.T, cfg sessionhost.Config) *sessionhost.Host {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		h, err := sessionhost.Open(cfg)
		if err == nil {
			t.Cleanup(func() { h.Close() })
			return h
		}
		if err.Error() != "runner_claim_refused" || time.Now().After(deadline) {
			t.Fatalf("replacement admission: %v", err)
		}
		time.Sleep(time.Millisecond) // Bounded scheduling handoff of the dead connection.
	}
}

// Actual SIGKILL interrupts each named dispatch boundary. Same runner/child,
// exact write count, durable uncertainty and refusal of replay/release are
// asserted after replacement. The applied path proves successful delivery and
// continuation. Both encodings run independently; no real harness is launched.
func TestDispatchCrashBoundaries(t *testing.T) {
	for _, protocol := range []string{"json", "line"} {
		for _, phase := range []string{"inflight", "runner-reply", "ack", "applied"} {
			t.Run(protocol+"/"+phase, func(t *testing.T) {
				cfg, _ := setup(t, fixtureAdapter(protocol), false)
				before := inspectRunner(t, cfg)
				socket := cfg.Socket
				var proxyReached <-chan struct{}
				finishProxy := func() {}
				if phase == "runner-reply" {
					socket, proxyReached, finishProxy = replyBarrier(t, cfg)
				}
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(binary, "-test.run=^TestCrashHostProcess$", "-test.v")
				cmd.Env = []string{
					"GORACE=atexit_sleep_ms=0", "SESSIONHOST_CRASH_HELPER=1", "SESSIONHOST_CRASH_PHASE=" + phase,
					"SESSIONHOST_CRASH_STATE=" + cfg.StateDir, "SESSIONHOST_CRASH_SOCKET=" + socket, "SESSIONHOST_CRASH_PROTOCOL=" + protocol,
				}
				cmd.Dir = filepath.Dir(cfg.StateDir)
				input, err := cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				defer input.Close()
				output, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				pid := cmd.Process.Pid
				done, observed := make(chan error, 1), make(chan string, 1)
				go func() {
					scanner := bufio.NewScanner(output)
					for scanner.Scan() {
						if scanner.Text() == phase {
							observed <- phase
							break
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
							t.Errorf("owned host PID %d did not exit", pid)
						}
					}
				})
				if phase == "runner-reply" {
					select {
					case <-proxyReached:
					case <-time.After(12 * time.Second):
						t.Fatal("runner acceptance barrier not reached")
					}
				} else {
					select {
					case <-observed:
					case <-time.After(12 * time.Second):
						t.Fatal("requested adapter/receipt barrier not reached")
					}
				}
				atBarrier := inspectRunner(t, cfg)
				wantWrites := 1
				if phase == "inflight" {
					wantWrites = 0
				}
				if atBarrier.Writes != wantWrites || atBarrier.Exited {
					t.Fatalf("wrong crash window: %+v", atBarrier)
				}
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					waited = true
					if err == nil {
						t.Fatal("host was not killed")
					}
				case <-time.After(3 * time.Second):
					t.Fatal("owned host kill unconfirmed")
				}
				status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatalf("wrong host exit: %v", cmd.ProcessState)
				}
				finishProxy()
				replacement := replacementAfterCrash(t, cfg)
				notice := sessionhost.Notice{ID: "crash-one", Reference: "fixture/crash-one"}
				record, err := replacement.Enqueue(notice)
				if err != nil {
					t.Fatal(err)
				}
				if phase == "applied" {
					if record.State != "applied" {
						t.Fatalf("applied receipt lost: %+v", record)
					}
					if err := replacement.Fence(true); err != nil {
						t.Fatal(err)
					}
					if _, err := replacement.Enqueue(sessionhost.Notice{ID: "two", Reference: "fixture/two"}); err != nil {
						t.Fatal(err)
					}
					await(t, replacement, "two", "applied")
					wantWrites = 2
				} else {
					if record.State != "unknown" || replacement.Snapshot().Fence != "unknown" {
						t.Fatalf("uncertainty lost: %+v", replacement.Snapshot())
					}
					if err := replacement.Fence(true); err == nil || err.Error() != "reconciliation_required" {
						t.Fatalf("unknown release: %v", err)
					}
					if _, err := replacement.Enqueue(sessionhost.Notice{ID: "two", Reference: "fixture/two"}); err != nil {
						t.Fatal(err)
					}
				}
				replacement.Close() // Drain dispatch opportunities before counting effects.
				after := inspectRunner(t, cfg)
				if after.Instance != before.Instance || after.PID != before.PID || after.Exited || after.Writes != wantWrites {
					t.Fatalf("crash/retry effect: before=%+v after=%+v wantWrites=%d", before, after, wantWrites)
				}
				t.Logf("phase=%s owned_host_pid=%d killed=SIGKILL runner_pid=%d instance=%s writes=%d", phase, pid, after.PID, after.Instance, after.Writes)
			})
		}
	}
}
