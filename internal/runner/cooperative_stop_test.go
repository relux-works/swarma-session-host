package runner

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// This selected subprocess is inert and owns no descendants. Its readiness
// marker follows signal registration, so a successful stop proves real TERM
// delivery rather than merely killing an uninitialized fixture.
func TestCooperativeStopFixture(t *testing.T) {
	mode := os.Getenv("SESSION_HOST_FIXTURE")
	if mode != "term" && mode != "ignore-term" {
		return
	}
	if mode == "ignore-term" {
		signal.Ignore(syscall.SIGTERM)
		fmt.Println("stop-fixture-ready")
		io.Copy(io.Discard, os.Stdin)
		time.Sleep(30 * time.Second)
		os.Exit(20)
	}
	received := make(chan os.Signal, 1)
	signal.Notify(received, syscall.SIGTERM)
	fmt.Println("stop-fixture-ready")
	select {
	case <-received:
		if os.WriteFile("term-received", []byte("SIGTERM\n"), 0600) != nil {
			os.Exit(21)
		}
		os.Exit(0)
	case <-time.After(30 * time.Second):
		os.Exit(22)
	}
}

func cooperativeConfig(t *testing.T, mode string) Config {
	t.Helper()
	cfg := testConfig(t, mode)
	cfg.StopSignal = "term"
	cfg.Plan.Args = []string{"-test.run=^TestCooperativeStopFixture$"}
	return cfg
}

func waitStopFixtureReady(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		s.mu.Lock()
		ready := bytes.Contains(s.output, []byte("stop-fixture-ready\n"))
		changed := s.changed
		s.mu.Unlock()
		if ready {
			return
		}
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatal("cooperative fixture did not register its signal handler")
		}
	}
}

// Concurrent callers share one bounded stop and a completed retry is inert.
// The real fixture proves TERM delivery and exit0; it does not count kernel
// signal coalescing or prove any native-wrapper/descendant behavior.
func TestCooperativeStopConcurrentAndRetry(t *testing.T) {
	cfg := cooperativeConfig(t, "term")
	s, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	waitStopFixtureReady(t, s)
	var wg sync.WaitGroup
	results := make(chan Response, 8)
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- s.stop()
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.Code != "stopped" || !result.Exited || result.ExitCode != 0 || result.PID != s.pid {
			t.Fatalf("cooperative stop: %+v", result)
		}
	}
	got, err := os.ReadFile(filepath.Join(cfg.Plan.Cwd, "term-received"))
	if err != nil || string(got) != "SIGTERM\n" {
		t.Fatalf("required graceful signal not observed: %q %v", got, err)
	}
	if got := s.stop(); got.Code != "stopped" || got.ExitCode != 0 {
		t.Fatalf("completed retry: %+v", got)
	}
}

// An explicitly noncooperative child is reaped within the bounded phases but
// cannot become a cooperative success on retry. Another owned child stays live.
// This witnesses original-child cleanup, not forced kernel PID reuse.
func TestCooperativeStopForcedReapRemainsUnknown(t *testing.T) {
	cfg := cooperativeConfig(t, "ignore-term")
	cfg.IOTimeout = 200 * time.Millisecond
	s, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	resourceReceipt(t, "child-start", s.pid, "")
	t.Cleanup(func() {
		if err := s.Close(); err == nil || err.Error() != "runner_stop_unknown" {
			t.Errorf("forced-stop uncertainty lost on close: %v", err)
		}
		if err := syscall.Kill(s.pid, 0); err != syscall.ESRCH {
			t.Errorf("owned forced child remains: %v", err)
		}
		resourceReceipt(t, "child-end", s.pid, "")
		if _, err := os.Lstat(cfg.Socket); !os.IsNotExist(err) {
			t.Errorf("owned socket remains: %v", err)
		}
	})
	waitStopFixtureReady(t, s)
	otherCfg := testConfig(t, "echo")
	other, err := startTestRunner(t, otherCfg)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	first := s.stop()
	second := s.stop()
	if time.Since(started) > 3*time.Second || first.Code != "stop_unknown" || second.Code != "stop_unknown" ||
		!first.Exited || first.ExitCode != -1 || second.PID != first.PID {
		t.Fatalf("forced stop/retry: %+v %+v", first, second)
	}
	client := testClient(t, otherCfg)
	status := call(t, client, Request{Op: "status", Execution: otherCfg.Execution}, "ok")
	if status.Exited || status.PID != other.pid || status.PID == first.PID {
		t.Fatalf("unrelated owned fixture affected: %+v", status)
	}
	call(t, client, Request{Op: "claim", Execution: otherCfg.Execution, Epoch: 1}, "ok")
	call(t, client, Request{Op: "write", Execution: otherCfg.Execution, Epoch: 1, Sequence: 1, Data: []byte("still-live\n")}, "written")
}

// A child that completed naturally before stop remains a successful direct-child
// stop. Guard tests below separately assert no signal after reaped publication.
func TestCooperativeStopAlreadyReaped(t *testing.T) {
	cfg := testConfig(t, "exit")
	cfg.StopSignal = "term"
	s, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("ordinary fixture did not exit")
	}
	if result := s.stop(); result.Code != "stopped" || !result.Exited || result.ExitCode != 0 {
		t.Fatalf("ordinary completion: %+v", result)
	}
}

// Invalid selectors fail before binding/spawn/capture, alongside a live valid
// default control. This checks the intended rejection, not a setup failure.
func TestCooperativeStopSelectorBeforeEffects(t *testing.T) {
	cfg := testConfig(t, "echo")
	cfg.StopSignal = "future"
	captures := 0
	s, err := startWithBirthCapture(cfg, func(int) (childBirth, error) {
		captures++
		return childBirth{}, nil
	})
	if s != nil || err == nil || err.Error() != "invalid_runner_plan" || captures != 0 {
		t.Fatalf("unknown selector: server=%v err=%v captures=%d", s, err, captures)
	}
	if _, err := os.Lstat(cfg.Socket); !os.IsNotExist(err) {
		t.Fatalf("invalid selector bound endpoint: %v", err)
	}
	cfg.StopSignal = "none"
	valid, err := startTestRunner(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result := valid.stop(); result.Code != "stopped" || result.ExitCode != 0 {
		t.Fatalf("default EOF control: %+v", result)
	}
}

// Per-call inert delivery observes the real authority/serialization seam without
// signalling fabricated PIDs. It does not simulate OS PID reuse or native work.
func TestOriginalChildSignalGuards(t *testing.T) {
	for _, tc := range []struct {
		name             string
		state            childOwnership
		handle, original int
		exited, unknown  bool
		want             bool
	}{
		{"owned", childOwned, 123, 123, false, false, true},
		{"reaped", childReaped, 123, 123, true, false, false},
		{"unknown", childWaitUnknown, 123, 123, false, true, false},
		{"changed-handle", childOwned, 124, 123, false, false, false},
		{"changed-original", childOwned, 123, 124, false, false, false},
		{"exited", childOwned, 123, 123, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{child: &os.Process{Pid: tc.handle}, pid: 123, original: originalChild{pid: tc.original},
				childState: tc.state, exited: tc.exited, waitUnknown: tc.unknown}
			calls := 0
			failure := errors.New("fixture signal refusal")
			attempted, err := s.signalOriginalChild(syscall.SIGTERM, func(process *os.Process, sig os.Signal) error {
				calls++
				if process != s.child || sig != syscall.SIGTERM {
					t.Error("changed original handle or signal")
				}
				if s.processMu.TryLock() {
					s.processMu.Unlock()
					t.Error("signal not serialized against reap")
				}
				return failure
			})
			if attempted != tc.want || (calls == 1) != tc.want || (err == failure) != tc.want {
				t.Fatalf("signal guard: attempted=%v calls=%d err=%v", attempted, calls, err)
			}
		})
	}
}

func inertStopServer(t *testing.T) *Server {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { read.Close(); write.Close() })
	return &Server{cfg: Config{StopSignal: "term", IOTimeout: 20 * time.Millisecond},
		stdin: write, stdout: read, pid: 123, original: originalChild{pid: 123},
		done: make(chan struct{}), outputDone: make(chan struct{}), changed: make(chan struct{})}
}

// Controlled original-wait completion isolates required-signal failure from
// successful completion. The callback never signals the fabricated handle.
func TestCooperativeStopRequiredSignalFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			s := inertStopServer(t)
			s.child, s.childState = &os.Process{Pid: 123}, childOwned
			close(s.outputDone)
			calls := 0
			got := s.stopCooperatively(func(_ *os.Process, sig os.Signal) error {
				calls++
				if sig != syscall.SIGTERM {
					t.Error("unexpected forced signal")
				}
				s.mu.Lock()
				s.publishChildWaitLocked(childReaped, 0)
				s.mu.Unlock()
				if failed {
					return errors.New("fixture signal refusal")
				}
				return nil
			})
			want := "stopped"
			if failed {
				want = "stop_unknown"
			}
			if got != want || calls != 1 || !s.exited {
				t.Fatalf("required signal: code=%s calls=%d exited=%v", got, calls, s.exited)
			}
		})
	}
}

// A missing output-drain completion forces unknown and closes the owned pipe;
// later completion cannot upgrade a retry. A completed drain is a valid control.
// No descendant or real signal is involved in this phase fixture.
func TestCooperativeStopOutputDrainDeadline(t *testing.T) {
	for _, drained := range []bool{false, true} {
		t.Run(fmt.Sprint(drained), func(t *testing.T) {
			s := inertStopServer(t)
			s.childState, s.exited = childReaped, true
			close(s.done)
			if drained {
				close(s.outputDone)
			}
			got := s.stop()
			want := "stopped"
			if !drained {
				want = "stop_unknown"
				if _, err := s.stdout.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("drain cutoff did not close owned pipe: %v", err)
				}
				close(s.outputDone)
			}
			if got.Code != want || s.stop().Code != want || s.overflow != !drained || s.blocked != !drained {
				t.Fatalf("drain/retry: %+v overflow=%v blocked=%v", got, s.overflow, s.blocked)
			}
		})
	}
}
