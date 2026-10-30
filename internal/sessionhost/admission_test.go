//go:build darwin && cgo

package sessionhost_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/admission"
	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

type evidenceProvider struct {
	mu    sync.Mutex
	proof admission.Snapshot
	mode  string
}

func (p *evidenceProvider) observe(ctx context.Context) (admission.Record, admission.Lease, error) {
	p.mu.Lock()
	s, mode := p.proof, p.mode
	p.mu.Unlock()
	switch mode {
	case "lease":
		s.Lease.Disposition = "released"
	case "registration":
		s.Record.Binding.Registration = "other"
		s.Lease.Registration = "other"
	case "rollback":
		s.Record.Revision--
	case "unavailable":
		return s.Record, s.Lease, errors.New("synthetic observer unavailable")
	case "timeout":
		<-ctx.Done()
		return s.Record, s.Lease, nil
	}
	return s.Record, s.Lease, nil
}
func (p *evidenceProvider) set(mode string) { p.mu.Lock(); defer p.mu.Unlock(); p.mode = mode }
func admitted(t *testing.T, cfg sessionhost.Config) (sessionhost.Config, *evidenceProvider) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status := inspectRunner(t, cfg)
	parent, err := admission.ObserveProcess(ctx, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	child, err := admission.ObserveProcess(ctx, status.PID)
	if err != nil {
		t.Fatal(err)
	}
	b := admission.Binding{Execution: cfg.Execution, Registration: "synthetic-manager", Operation: "synthetic-operation", Session: "synthetic-session", Generation: 1, PlanDigest: strings.Repeat("a", 64), Profile: "synthetic", Environment: "synthetic-env", ProfileFragment: strings.Repeat("c", 64), Transport: "pipe", Permissions: "synthetic", ProfileLock: strings.Repeat("b", 64), Workspace: filepath.Dir(cfg.StateDir), Home: filepath.Dir(cfg.StateDir), Runtime: cfg.Adapter.Name(), Vendor: cfg.Selection.Vendor, Model: cfg.Selection.Model, Effort: cfg.Selection.Reasoning, NativeRelease: "synthetic", NativeSession: "synthetic-native", RunnerInstance: status.Instance, RunnerSocket: cfg.Socket, Runner: parent, Harness: child}
	s := admission.Snapshot{Record: admission.Record{Version: admission.Version, Revision: 2, Lifecycle: "started", Binding: b}, Lease: admission.Lease{Version: admission.Version, Revision: 2, ID: "synthetic-lease", AcquireID: "synthetic-acquire", Disposition: "bound", Registration: b.Registration, Session: b.Session, Generation: b.Generation, Home: b.Home, Profile: b.Profile, Environment: b.Environment, ProfileLock: b.ProfileLock, PID: child.PID, StartedAt: child.StartedAt}}
	p := &evidenceProvider{proof: s}
	cfg.Admission = &admission.Config{Expected: s, Observe: p.observe, Process: admission.ObserveProcess}
	return cfg, p
}
func awaitAdmissionFailure(t *testing.T, h *sessionhost.Host) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s := h.Snapshot()
		if s.Admission != nil && s.Admission.Failed && s.Fence == "unknown" {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("admission loss did not durably fence delivery")
}

// admissionWindowAdapter has per-test barriers only. Applied holds the first
// real acknowledgement before dispatch can begin the next intent. Encode holds
// the second intent's dispatch preparation, after its durable unknown marker;
// Enqueue's separate validation call is counted and never blocked.
type admissionWindowAdapter struct {
	sessionhost.Adapter
	window        string
	entered       chan struct{}
	release       chan struct{}
	releaseOnce   sync.Once
	mu            sync.Mutex
	secondEncodes int
}

func (a *admissionWindowAdapter) unblock() { a.releaseOnce.Do(func() { close(a.release) }) }
func (a *admissionWindowAdapter) Encode(n sessionhost.Notice, s sessionhost.Selection) ([]byte, error) {
	b, err := a.Adapter.Encode(n, s)
	if n.ID == "two" && a.window == "after-prepare" && err == nil {
		a.mu.Lock()
		a.secondEncodes++
		call := a.secondEncodes
		a.mu.Unlock()
		if call == 2 {
			close(a.entered)
			<-a.release
		}
	}
	return b, err
}
func (a *admissionWindowAdapter) Applied(b []byte, n sessionhost.Notice, s sessionhost.Selection) (bool, error) {
	applied, err := a.Adapter.Applied(b, n, s)
	if a.window == "before-dispatch" && n.ID == "one" && applied && err == nil {
		close(a.entered)
		<-a.release
	}
	return applied, err
}
func awaitAdmissionWindow(t *testing.T, a *admissionWindowAdapter) {
	t.Helper()
	select {
	case <-a.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("AT1_WINDOW_NOT_REACHED", a.window)
	}
}

// Each window has its own exact durable state: before-dispatch loss retains a
// queued intent; after-prepare loss retains unknown. Both fence before the
// second write, preserve intent on disk, refuse release/downgrade/recovery and
// retain owned stop. Valid controls in both windows really perform two writes.
func TestAdmissionLossAndRecovery(t *testing.T) {
	for _, window := range []string{"before-dispatch", "after-prepare"} {
		t.Run(window, func(t *testing.T) {
			for _, mode := range []string{"valid", "lease", "registration", "rollback", "unavailable", "timeout", "process"} {
				t.Run(mode, func(t *testing.T) {
					gate := &admissionWindowAdapter{Adapter: fixtureadapters.JSON{}, window: window, entered: make(chan struct{}), release: make(chan struct{})}
					cfg, _ := setup(t, gate, false)
					cfg, p := admitted(t, cfg)
					cfg.IOTimeout = 500 * time.Millisecond
					if mode == "process" {
						base := cfg.Admission.Process
						cfg.Admission.Process = func(ctx context.Context, pid int) (admission.Process, error) {
							got, err := base(ctx, pid)
							p.mu.Lock()
							bad := p.mode == "process"
							p.mu.Unlock()
							if bad {
								got.UID++
							}
							return got, err
						}
					}
					h := openHost(t, cfg)
					// Cleanup releases the test-owned barrier before openHost's Close.
					t.Cleanup(gate.unblock)
					if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
						t.Fatal(err)
					}
					if window == "before-dispatch" {
						awaitAdmissionWindow(t, gate)
					} else {
						first := await(t, h, "one", "applied")
						if first.Version != 2 || first.Admission == nil || first.Admission.Failed {
							t.Fatal("valid admission not persisted")
						}
					}
					original := *cfg.Admission
					cfg.Admission.Expected.Record.Binding.Model = "caller-mutated"
					if window == "before-dispatch" {
						p.set(mode)
					}
					if _, err := h.Enqueue(sessionhost.Notice{ID: "two", Reference: "fixture/two"}); err != nil {
						t.Fatal(err)
					}
					wantAtBarrier := "queued"
					if window == "after-prepare" {
						awaitAdmissionWindow(t, gate)
						wantAtBarrier = "unknown"
					}
					atBarrier := h.Snapshot()
					if len(atBarrier.Records) != 2 || atBarrier.Records[1].State != wantAtBarrier {
						t.Fatalf("AT1_WINDOW_STATE: %s want=%s got=%+v", window, wantAtBarrier, atBarrier.Records)
					}
					if got := inspectRunner(t, cfg); got.Writes != 1 || got.PID != p.proof.Record.Binding.Harness.PID {
						t.Fatal("AT1_BARRIER_EFFECT: unexpected write or child")
					}
					if window == "after-prepare" {
						p.set(mode)
					}
					gate.unblock()
					if mode == "valid" {
						await(t, h, "two", "applied")
						if got := inspectRunner(t, cfg); got.Writes != 2 {
							t.Fatal("AT1_VALID_CONTROL: valid admission suppressed writes")
						}
						cfg.Admission = &original
						return
					}
					awaitAdmissionFailure(t, h)
					if got := inspectRunner(t, cfg); got.Writes != 1 || got.PID != p.proof.Record.Binding.Harness.PID {
						t.Fatal("AT1_NO_SECOND_EFFECT: admission loss allowed an effect or replaced child")
					}
					if err := h.Fence(true); err == nil || err.Error() != "reconciliation_required" {
						t.Fatalf("AT1_RELEASE_REFUSAL: %v", err)
					}
					data, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
					if err != nil {
						t.Fatal(err)
					}
					var disk sessionhost.State
					if json.Unmarshal(data, &disk) != nil || disk.Version != 2 || disk.Admission == nil || !disk.Admission.Failed || disk.Fence != "unknown" || disk.InputState != sessionhost.InputUnknown || len(disk.Records) != 2 {
						t.Fatal("AT1_DURABLE_FENCE: failed gate or intent lost")
					}
					if disk.Records[0].State != "applied" {
						t.Fatal("AT1_FIRST_INTENT: first acknowledgement lost")
					}
					marker := "AT1_BEFORE_DISPATCH_STATE"
					if window == "after-prepare" {
						marker = "AT1_AFTER_PREPARE_STATE"
					}
					if disk.Records[1].State != wantAtBarrier {
						t.Fatalf("%s: want=%s got=%s", marker, wantAtBarrier, disk.Records[1].State)
					}
					if disk.Records[1].Notice != (sessionhost.Notice{ID: "two", Reference: "fixture/two"}) || disk.Records[1].Sequence != 2 {
						t.Fatal("AT1_RETAINED_INTENT: intent changed")
					}
					// Provider recovery never clears the persisted failed latch.
					p.set("valid")
					cfg.Admission = &original
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					stopped, err := h.Stop(ctx)
					if err != nil || stopped.Code != "stopped" || !stopped.Exited || stopped.Writes != 1 || stopped.PID != p.proof.Record.Binding.Harness.PID {
						t.Fatalf("AT1_OWNED_STOP: code=%s exited=%t writes=%d err=%v", stopped.Code, stopped.Exited, stopped.Writes, err)
					}
					h.Close()
					closedDisk, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
					if err != nil {
						t.Fatal(err)
					}
					if recovered, err := sessionhost.Open(cfg); err == nil || recovered != nil || err.Error() != "admission_reconciliation_required" {
						if recovered != nil {
							recovered.Close()
						}
						t.Fatalf("AT1_RECOVERY_REFUSAL: want reconciliation required got %v", err)
					}
					cfg.Admission = nil
					if recovered, err := sessionhost.Open(cfg); err == nil || recovered != nil || err.Error() != "admission_reconciliation_required" {
						if recovered != nil {
							recovered.Close()
						}
						t.Fatalf("AT1_DOWNGRADE_REFUSAL: want reconciliation required got %v", err)
					}
					finalDisk, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
					if err != nil || !bytes.Equal(closedDisk, finalDisk) {
						t.Fatalf("AT1_REFUSAL_NO_EFFECT: failed restore changed disk: %v", err)
					}
					t.Logf("AT1_WINDOW_RECEIPT window=%s mode=%s second=%s writes=1 release=refused recovery=refused downgrade=refused owned-stop=usable", window, mode, wantAtBarrier)
				})
			}
		})
	}
}

// Invalid initial evidence has zero writes, retains the failed metadata marker,
// and never becomes an initialized session by omitting the admission option.
func TestAdmissionInitialRefusal(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	cfg, p := admitted(t, cfg)
	p.set("lease")
	h, err := sessionhost.Open(cfg)
	if err == nil {
		h.Close()
		t.Fatal("released lease admitted")
	}
	if err.Error() != "admission_evidence_invalid" {
		t.Fatal(err)
	}
	if got := inspectRunner(t, cfg); got.Writes != 0 {
		t.Fatal("refused admission wrote")
	}
	cfg.Admission = nil
	if h, err := sessionhost.Open(cfg); err == nil {
		h.Close()
		t.Fatal("failed initialization downgraded")
	}
}

// Subprocess helper has only explicit non-secret synthetic record/state paths.
func TestAdmissionHostProcess(t *testing.T) {
	if os.Getenv("ADMISSION_HOST_HELPER") != "1" {
		return
	}
	data, err := os.ReadFile(os.Getenv("ADMISSION_PROOF_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	var proof admission.Snapshot
	if json.Unmarshal(data, &proof) != nil {
		t.Fatal("bad synthetic proof")
	}
	b := proof.Record.Binding
	phase := os.Getenv("ADMISSION_PHASE")
	cfg := sessionhost.Config{StateDir: os.Getenv("ADMISSION_STATE"), Socket: b.RunnerSocket, Execution: b.Execution, IOTimeout: time.Second, Adapter: &crashAdapter{Adapter: fixtureadapters.JSON{}, phase: phase}, Selection: sessionhost.Selection{Vendor: b.Vendor, Model: b.Model, Reasoning: b.Effort}, Admission: &admission.Config{Expected: proof, Observe: func(context.Context) (admission.Record, admission.Lease, error) {
		if phase == "admission" {
			crashBarrier(phase)
		}
		return proof.Record, proof.Lease, nil
	}, Process: admission.ObserveProcess}}
	h := openHost(t, cfg)
	if _, err := h.Enqueue(sessionhost.Notice{ID: "crash-one", Reference: "fixture/crash-one"}); err != nil {
		t.Fatal(err)
	}
	if phase == "applied" {
		await(t, h, "crash-one", "applied")
		crashBarrier(phase)
	}
	<-time.After(12 * time.Second)
	t.Fatal("admitted crash barrier not reached")
}

// Actual owned-host SIGKILL preserves a verified execution and applied receipt;
// in-flight/ack crashes cannot replay. A pre-verification crash retains the
// unverified marker, refuses recovery, and keeps the same child with zero writes.
func TestAdmittedHostSIGKILL(t *testing.T) {
	for _, phase := range []string{"admission", "applied", "inflight", "ack"} {
		t.Run(phase, func(t *testing.T) {
			cfg, _ := setup(t, fixtureadapters.JSON{}, false)
			cfg, p := admitted(t, cfg)
			proofFile := filepath.Join(filepath.Dir(cfg.StateDir), "proof.json")
			data, _ := json.Marshal(p.proof)
			if err := os.WriteFile(proofFile, data, 0600); err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "-test.run=^TestAdmissionHostProcess$", "-test.v")
			cmd.Env = []string{"ADMISSION_HOST_HELPER=1", "ADMISSION_PROOF_FILE=" + proofFile, "ADMISSION_STATE=" + cfg.StateDir, "ADMISSION_PHASE=" + phase, "GORACE=atexit_sleep_ms=0"}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = io.Discard
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			pid := cmd.Process.Pid
			t.Logf("owned host pid=%d phase=%s", pid, phase)
			reaped := false
			defer func() {
				stdin.Close()
				if !reaped {
					cmd.Process.Kill()
					cmd.Wait()
				}
			}()
			reached := make(chan bool, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					if scanner.Text() == phase {
						reached <- true
						return
					}
				}
				reached <- false
			}()
			select {
			case ok := <-reached:
				if !ok {
					t.Fatal("host exited before barrier")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("host barrier timeout")
			}
			before := inspectRunner(t, cfg)
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			reaped = true
			status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
			if err == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("not actual SIGKILL: %v", err)
			}
			if phase == "admission" {
				rejected, err := sessionhost.Open(cfg)
				if err == nil {
					rejected.Close()
					t.Fatal("unverified admission crash recovered")
				}
				if err.Error() != "admission_reconciliation_required" {
					t.Fatal(err)
				}
				data, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
				if err != nil {
					t.Fatal(err)
				}
				var saved sessionhost.State
				if json.Unmarshal(data, &saved) != nil || saved.Admission == nil || !saved.Admission.Failed || saved.InputState != sessionhost.InputUnknown || len(saved.Records) != 0 {
					t.Fatal("unverified admission marker was lost")
				}
				after := inspectRunner(t, cfg)
				if after.Instance != before.Instance || after.PID != before.PID || after.Writes != 0 || after.Exited {
					t.Fatal("unverified crash changed execution or wrote")
				}
				return
			}
			h := replacementAfterCrash(t, cfg)
			s := h.Snapshot()
			if s.Admission == nil || s.Admission.Failed || s.Admission.Record.Binding != p.proof.Record.Binding || len(s.Records) != 1 {
				t.Fatal("replacement lost binding/receipt")
			}
			if phase == "applied" {
				if s.Records[0].State != "applied" {
					t.Fatal("applied receipt lost")
				}
				if err := h.Fence(true); err != nil {
					t.Fatal(err)
				}
				if _, err := h.Enqueue(sessionhost.Notice{ID: "two", Reference: "fixture/two"}); err != nil {
					t.Fatal(err)
				}
				await(t, h, "two", "applied")
			} else {
				if s.Records[0].State != "unknown" || s.Fence != "unknown" {
					t.Fatal("crash uncertainty lost")
				}
				if _, err := h.Enqueue(sessionhost.Notice{ID: "crash-one", Reference: "fixture/crash-one"}); err != nil {
					t.Fatal(err)
				}
				if err := h.Fence(true); err == nil {
					t.Fatal("uncertain effect released")
				}
			}
			after := inspectRunner(t, cfg)
			want := 1
			if phase == "inflight" {
				want = 0
			}
			if phase == "applied" {
				want = 2
			}
			if after.Instance != before.Instance || after.PID != before.PID || after.Writes != want || after.Exited {
				t.Fatalf("same execution/replay invariant failed: writes=%d want=%d", after.Writes, want)
			}
		})
	}
}

// Revocation during the real adapter's second Encode is observed immediately
// before pipe I/O: the reserved notice remains unknown and zero bytes are sent.
type revokingAdapter struct {
	sessionhost.Adapter
	count  int
	revoke func()
}

func (a *revokingAdapter) Encode(n sessionhost.Notice, s sessionhost.Selection) ([]byte, error) {
	a.count++
	if a.count == 2 {
		a.revoke()
	}
	return a.Adapter.Encode(n, s)
}
func TestAdmissionRevocationAfterEncode(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	cfg, p := admitted(t, cfg)
	cfg.Adapter = &revokingAdapter{Adapter: fixtureadapters.JSON{}, revoke: func() { p.set("lease") }}
	h := openHost(t, cfg)
	if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
		t.Fatal(err)
	}
	awaitAdmissionFailure(t, h)
	if got := inspectRunner(t, cfg); got.Writes != 0 {
		t.Fatalf("revoked lease allowed prepared write: %d", got.Writes)
	}
	assertFailedUnknown(t, cfg, h)
	if err := h.ObserveInput(); err != nil {
		t.Fatal(err)
	}
	assertFailedUnknown(t, cfg, h)
	if err := h.Fence(true); err == nil || err.Error() != "reconciliation_required" {
		t.Fatalf("failed release: %v", err)
	}
	assertFailedUnknown(t, cfg, h)
	s := h.Snapshot()
	if len(s.Records) != 1 || s.Records[0].State != "unknown" {
		t.Fatal("reserved uncertainty not retained")
	}
}

// The protocol path revalidates bound admission before wire writes. A valid
// control completes a turn; revoked lease retains exactly initialization's three
// writes, and a mismatched native session never enables turn preparation.
func TestAdmissionProtocolGate(t *testing.T) {
	for _, mode := range []string{"valid", "lease", "native"} {
		t.Run(mode, func(t *testing.T) {
			fixtureMode := "normal"
			if mode == "native" {
				fixtureMode = "native-gated"
			}
			cfg, _, dir := setupProtocol(t, fixtureMode)
			// Reuse only synthetic metadata construction, not an alternative transport.
			base := cfg
			base.Adapter = fixtureadapters.JSON{}
			_, p := admitted(t, base)
			b := &p.proof.Record.Binding
			b.Runtime = "codex"
			b.NativeRelease = "0.164.0"
			b.NativeSession = "synthetic-thread"
			if mode == "native" {
				b.NativeSession = "other-native"
			}
			cfg.Admission = &admission.Config{Expected: p.proof, Observe: p.observe, Process: admission.ObserveProcess}
			h := openHost(t, cfg)
			if mode == "native" {
				if _, err := h.Enqueue(sessionhost.Notice{ID: "retained", Reference: "fixture/retained"}); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "allow-thread-start"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				awaitProtocol(t, h, "native conflict closes fence", func(s sessionhost.State) bool { return s.Fence == "unknown" })
				assertFailedUnknown(t, cfg, h)
				if got := inspectRunner(t, cfg); got.Writes != 3 || got.PID != b.Harness.PID || got.Instance != b.RunnerInstance {
					t.Fatalf("native conflict changed execution/effects: %+v", got)
				}
				if records := h.Snapshot().Records; len(records) != 1 || records[0].State != "queued" {
					t.Fatal("native conflict lost queued intent")
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if _, err := h.Stop(ctx); err != nil {
					t.Fatalf("owned stop: %v", err)
				}
				h.Close()
				rejected, err := sessionhost.Open(cfg)
				if err == nil {
					rejected.Close()
					t.Fatal("native conflict reopened verified admission")
				}
				if err.Error() != "admission_reconciliation_required" {
					t.Fatal(err)
				}
				disk := readAdmissionDisk(t, cfg)
				if len(disk.Records) != 1 || disk.Records[0].Notice.ID != "retained" || disk.Records[0].State != "queued" {
					t.Fatal("native conflict lost durable intent")
				}
				if got := inspectRunner(t, cfg); got.Writes != 3 {
					t.Fatal("native conflict replayed on recovery")
				}
				return
			}
			awaitProtocol(t, h, "admitted protocol idle", func(s sessionhost.State) bool {
				return s.InputState == sessionhost.InputIdle && len(s.Protocol.Pending) == 0
			})
			p.set(mode)
			if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
				t.Fatal(err)
			}
			want := 4
			if mode == "valid" {
				await(t, h, "one", "applied")
			} else {
				want = 3
				awaitAdmissionFailure(t, h)
			}
			if got := inspectRunner(t, cfg); got.Writes != want {
				t.Fatalf("protocol admission writes=%d want=%d", got.Writes, want)
			}
		})
	}
}

// Corrupt/new-version/lost admission metadata is rejected before controller
// contact or mutation; restoring the exact bytes supplies the valid control.
func TestAdmissionJournalRefusal(t *testing.T) {
	for _, mode := range []string{"version", "missing", "binding", "unknown-field", "permissions", "lost-state", "pending"} {
		t.Run(mode, func(t *testing.T) {
			cfg, _ := setup(t, fixtureadapters.JSON{}, false)
			cfg, _ = admitted(t, cfg)
			h := openHost(t, cfg)
			if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
				t.Fatal(err)
			}
			originalState := await(t, h, "one", "applied")
			h.Close()
			file := filepath.Join(cfg.StateDir, "state.json")
			original, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var obj map[string]json.RawMessage
			if json.Unmarshal(original, &obj) != nil {
				t.Fatal("fixture decode")
			}
			switch mode {
			case "version":
				obj["version"] = json.RawMessage("99")
			case "missing":
				delete(obj, "admission")
			case "binding":
				var proof admission.Snapshot
				json.Unmarshal(obj["admission"], &proof)
				proof.Record.Binding.Home = "/changed"
				obj["admission"], _ = json.Marshal(proof)
			case "unknown-field":
				obj["unknown_evidence"] = json.RawMessage("true")
			}
			changed, _ := json.Marshal(obj)
			if err := os.WriteFile(file, changed, 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "permissions" {
				if err := os.Chmod(file, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "lost-state" {
				if err := os.Remove(file); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "pending" {
				if err := os.WriteFile(filepath.Join(cfg.StateDir, "state.pending"), []byte("update-in-progress\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			rejected, err := sessionhost.Open(cfg)
			if err == nil {
				rejected.Close()
				t.Fatal("corrupt admission journal opened")
			}
			if err.Error() != "state_corrupt" && !(mode == "permissions" && err.Error() == "state_not_private") && !(mode == "lost-state" && err.Error() == "state_missing") && !(mode == "pending" && err.Error() == "state_update_unknown") {
				t.Fatalf("unexpected refusal: %v", err)
			}
			after, err := os.ReadFile(file)
			if (mode == "lost-state" && !os.IsNotExist(err)) || (mode != "lost-state" && (err != nil || string(after) != string(changed))) {
				t.Fatal("refusal changed journal")
			}
			if got := inspectRunner(t, cfg); got.Writes != 1 || got.Epoch != originalState.Epoch {
				t.Fatal("corrupt journal contacted runner")
			}
			if mode == "pending" {
				if err := os.Remove(filepath.Join(cfg.StateDir, "state.pending")); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "lost-state" {
				if err := os.Chmod(file, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(file, original, 0600); err != nil {
				t.Fatal(err)
			}
			valid := openHost(t, cfg)
			if valid.Snapshot().Admission == nil || valid.Snapshot().Admission.Failed {
				t.Fatal("restored positive control failed")
			}
		})
	}
}

func readAdmissionDisk(t *testing.T, cfg sessionhost.Config) sessionhost.State {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var s sessionhost.State
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	return s
}
func assertFailedUnknown(t *testing.T, cfg sessionhost.Config, h *sessionhost.Host) {
	t.Helper()
	for _, s := range []sessionhost.State{h.Snapshot(), readAdmissionDisk(t, cfg)} {
		if s.Admission == nil || !s.Admission.Failed || s.Fence != "unknown" || s.InputState != sessionhost.InputUnknown {
			t.Fatalf("failed admission lost unknown input/fence: input=%s fence=%s", s.InputState, s.Fence)
		}
	}
}

// Admission loss keeps input unknown through both idle and security-stop
// observations; the independent security-stop latch survives subsequent idle.
func TestAdmissionFailureSecurityStop(t *testing.T) {
	a := &observedFixture{}
	a.input.Store(sessionhost.InputIdle)
	cfg, _ := setup(t, a, false)
	cfg, p := admitted(t, cfg)
	h := openHost(t, cfg)
	p.set("lease")
	if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
		t.Fatal(err)
	}
	awaitAdmissionFailure(t, h)
	for _, state := range []sessionhost.InputState{sessionhost.InputSecurityStop, sessionhost.InputIdle} {
		a.input.Store(state)
		if err := h.ObserveInput(); err != nil {
			t.Fatal(err)
		}
		assertFailedUnknown(t, cfg, h)
		if !h.Snapshot().SecurityStopped || !readAdmissionDisk(t, cfg).SecurityStopped {
			t.Fatal("security stop was not retained")
		}
	}
}
