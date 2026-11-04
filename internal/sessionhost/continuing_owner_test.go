package sessionhost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

func continuingOwnerConfig(t *testing.T) (Config, *runner.Server) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "continuing-owner-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "runner.sock")
	s, err := runner.Start(runner.Config{Socket: socket, Execution: "execution-1", IOTimeout: time.Second,
		Plan: runner.Plan{Binary: binary, Args: []string{"-test.run=^TestNotificationJournalChild$"}, Cwd: dir,
			Env: []string{"SESSION_HOST_NOTIFICATION_CHILD=1", "GORACE=atexit_sleep_ms=0"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("owned child-start pid=%d", s.ChildPID())
	t.Cleanup(func() { _ = s.Close(); t.Logf("owned child-end pid=%d", s.ChildPID()) })
	cfg := Config{StateDir: filepath.Join(dir, "state"), Socket: socket, Execution: "execution-1",
		NoticeMode: WorkReference, ProtocolName: "codex", IOTimeout: time.Second, PollInterval: time.Second,
		Selection:       Selection{Vendor: "provider", Model: "model", Reasoning: "effort"},
		ProtocolFactory: func([]byte) (ap.Driver, uint64, error) { return &journalDriver{}, 0, nil }}
	return cfg, s
}

func continuingOwnerOpen(t *testing.T, parent context.Context, expires time.Time, cfg Config) *continuingHostOwner {
	t.Helper()
	o, err := openContinuingHostOwner(parent, expires, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := o.close(ctx); err != nil {
			t.Error("owner cleanup", err)
		}
	})
	return o
}

func continuingOwnerDone(t *testing.T, o *continuingHostOwner) {
	t.Helper()
	select {
	case <-o.done:
	case <-time.After(2 * time.Second):
		t.Fatal("owned controller was not joined")
	}
}

func continuingOwnerRunner(t *testing.T, cfg Config) runner.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := runner.Dial(ctx, cfg.Socket)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := c.Call(ctx, runner.Request{Op: "status", Execution: cfg.Execution})
	if err != nil || r.Code != "ok" {
		t.Fatalf("runner status %+v: %v", r, err)
	}
	return r
}

// A real locally opened Host outlives a canceled status request. Its advisory
// copy cannot replace the retained object. An overlapping Open is refused by
// the actual state lock without disturbing the original controller. Inert Go
// children establish resource behavior, not native/Source/member admission.
func TestContinuingOwnerRequestLifetime(t *testing.T) {
	cfg, s := continuingOwnerConfig(t)
	o := continuingOwnerOpen(t, context.Background(), time.Now().Add(10*time.Second), cfg)
	request, cancel := context.WithCancel(context.Background())
	before, err := o.snapshot(request)
	if err != nil || before.RunnerInstance == "" || before.Epoch != 1 {
		t.Fatalf("actual owner snapshot: %+v %v", before, err)
	}
	cancel()
	if _, err := o.snapshot(request); !errors.Is(err, errHostOwnerUnavailable) {
		t.Fatal("canceled request was observed", err)
	}
	if err := o.checkCurrentHost(o.host); err != nil {
		t.Fatal("request cancellation ended continuing custody", err)
	}
	before.RunnerInstance = "copied-other-runner"
	for _, candidate := range []*Host{nil, {state: before}, {state: o.host.Snapshot()}} {
		if err := o.checkCurrentHost(candidate); !errors.Is(err, errHostOwnerUnavailable) {
			t.Fatal("foreign object was admitted", err)
		}
	}
	other, err := openContinuingHostOwner(context.Background(), time.Now().Add(time.Second), cfg)
	if other != nil || err == nil || err.Error() != "host_already_active" {
		t.Fatalf("overlapping owner: %v %v", other, err)
	}
	after, err := o.snapshot(context.Background())
	if err != nil || after.Epoch != 1 || after.RunnerInstance != o.runner {
		t.Fatalf("original owner disturbed: %+v %v", after, err)
	}
	r := continuingOwnerRunner(t, cfg)
	if r.Exited || r.PID != s.ChildPID() || r.Writes != 0 {
		t.Fatalf("original child disturbed: %+v", r)
	}
	if o.host.notificationManager != nil || o.host.notificationStream != nil {
		t.Fatal("resource ownership created notification authority")
	}
	if _, admitted := any(o).(notificationManagerCustody); admitted {
		t.Fatal("resource owner substituted for member admission")
	}
}

// Invalid lifetime is rejected before Open creates state or contacts a runner;
// a nearby actual finite owner is exercised by RequestLifetime above.
func TestContinuingOwnerLifetimeAdmission(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, tc := range map[string]struct {
		parent  context.Context
		expires time.Time
	}{
		"nil-parent":      {nil, time.Now().Add(time.Second)},
		"canceled-parent": {canceled, time.Now().Add(time.Second)},
		"absent-bound":    {context.Background(), time.Time{}},
		"expired-bound":   {context.Background(), time.Now().Add(-time.Second)},
	} {
		t.Run(name, func(t *testing.T) {
			state := filepath.Join(t.TempDir(), "untouched")
			o, err := openContinuingHostOwner(tc.parent, tc.expires, Config{StateDir: state})
			if o != nil || !errors.Is(err, errHostOwnerUnavailable) {
				t.Fatalf("invalid lifetime accepted: %v %v", o, err)
			}
			if _, err := os.Lstat(state); !os.IsNotExist(err) {
				t.Fatal("invalid lifetime reached Host.Open", err)
			}
		})
	}
}

// Owner cancellation/expiry/explicit Host close join the controller, retain the
// exact runner child with zero writes, and forbid late current-object access.
// Expiry is an actual finite deadline; other cases use explicit cancellation.
func TestContinuingOwnerTerminal(t *testing.T) {
	for _, how := range []string{"parent-cancel", "expiry", "host-close", "concurrent-close"} {
		t.Run(how, func(t *testing.T) {
			cfg, s := continuingOwnerConfig(t)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			expires := time.Now().Add(10 * time.Second)
			if how == "expiry" {
				expires = time.Now().Add(200 * time.Millisecond)
			}
			o := continuingOwnerOpen(t, parent, expires, cfg)
			switch how {
			case "parent-cancel":
				cancel()
			case "host-close":
				_ = o.host.Close()
			case "concurrent-close":
				var wg sync.WaitGroup
				for range 8 {
					wg.Add(1)
					go func() { defer wg.Done(); _ = o.close(context.Background()) }()
				}
				wg.Wait()
			}
			continuingOwnerDone(t, o)
			if err := o.checkCurrentHost(o.host); !errors.Is(err, errHostOwnerUnavailable) {
				t.Fatal("terminal owner became current", err)
			}
			if _, err := o.snapshot(context.Background()); !errors.Is(err, errHostOwnerUnavailable) {
				t.Fatal("terminal owner exposed a success snapshot", err)
			}
			r := continuingOwnerRunner(t, cfg)
			if r.Exited || r.PID != s.ChildPID() || r.Writes != 0 {
				t.Fatalf("owner loss stopped or wrote to child: %+v", r)
			}
			// The completed join has really released this controller's lock.
			store, err := openStore(cfg.StateDir)
			if err != nil {
				t.Fatal("owner done preceded store release", err)
			}
			store.close()
		})
	}
}

// Actual identity/state changes refuse current ownership; restoring the original
// metadata after cleanup cannot revive it. These bounded in-memory
// mutations model a transition and do not establish external record admission.
func TestContinuingOwnerCurrentIdentity(t *testing.T) {
	for _, field := range []string{"execution", "runner", "epoch", "stop", "storage"} {
		t.Run(field, func(t *testing.T) {
			cfg, _ := continuingOwnerConfig(t)
			o := continuingOwnerOpen(t, context.Background(), time.Now().Add(10*time.Second), cfg)
			if err := o.checkCurrentHost(o.host); err != nil {
				t.Fatal(err)
			}
			o.host.mu.Lock()
			original := cloneState(o.host.state)
			switch field {
			case "execution":
				o.host.state.Execution = "other"
			case "runner":
				o.host.state.RunnerInstance = "other"
			case "epoch":
				o.host.state.Epoch++
			case "stop":
				o.host.state.StopRequested = true
			case "storage":
				o.host.err = errors.New("inert storage failure")
			}
			o.host.mu.Unlock()
			if err := o.checkCurrentHost(o.host); !errors.Is(err, errHostOwnerUnavailable) {
				t.Fatal("changed actual identity remained current", err)
			}
			continuingOwnerDone(t, o)
			o.host.mu.Lock()
			o.host.state, o.host.err = original, nil
			o.host.mu.Unlock()
			if err := o.checkCurrentHost(o.host); !errors.Is(err, errHostOwnerUnavailable) {
				t.Fatal("restored metadata resurrected terminal owner", err)
			}
		})
	}
}

// An absent/zero owner has no resources, and none of its operations supplies a
// success or hangs waiting for nonexistent cleanup.
func TestContinuingOwnerAbsent(t *testing.T) {
	for _, o := range []*continuingHostOwner{nil, {}} {
		if err := o.checkCurrentHost(nil); !errors.Is(err, errHostOwnerUnavailable) {
			t.Fatal("absent owner was current", err)
		}
		if _, err := o.snapshot(context.Background()); !errors.Is(err, errHostOwnerUnavailable) {
			t.Fatal("absent owner returned metadata", err)
		}
		if err := o.close(context.Background()); !errors.Is(err, errHostOwnerUnavailable) {
			t.Fatal("absent owner close succeeded", err)
		}
	}
}

// A bounded waiter may return canceled while Host.Close is held at its actual
// state barrier. Cleanup continues and done closes only after the barrier and
// controller release, without requiring another request or close call.
func TestContinuingOwnerCanceledCloseWaiter(t *testing.T) {
	cfg, _ := continuingOwnerConfig(t)
	o := continuingOwnerOpen(t, context.Background(), time.Now().Add(10*time.Second), cfg)
	o.host.mu.Lock()
	wait, cancel := context.WithCancel(context.Background())
	cancel()
	err := o.close(wait)
	select {
	case <-o.done:
		o.host.mu.Unlock()
		t.Fatal("done preceded Host.Close admission barrier")
	default:
	}
	o.host.mu.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Fatal("wrong waiter result", err)
	}
	continuingOwnerDone(t, o)
	if err := o.checkCurrentHost(o.host); !errors.Is(err, errHostOwnerUnavailable) {
		t.Fatal("canceled waiter abandoned cleanup", err)
	}
}
