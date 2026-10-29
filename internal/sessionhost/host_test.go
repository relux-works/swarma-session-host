package sessionhost_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/fixtureengine"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

func TestFixtureProcess(t *testing.T) {
	protocol := os.Getenv("SESSION_HOST_PROTOCOL")
	if protocol == "" {
		return
	}
	var output io.Writer = os.Stdout
	if os.Getenv("SESSION_HOST_DROP_ACK") == "1" {
		output = io.Discard
	}
	if fixtureengine.Run(protocol, os.Stdin, output) != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func setup(t *testing.T, adapter sessionhost.Adapter, drop bool) (sessionhost.Config, *runner.Server) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "session-host-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	protocol := "json"
	if adapter.Name() == "line-fixture" {
		protocol = "line"
	}
	env := []string{"SESSION_HOST_PROTOCOL=" + protocol, "GORACE=atexit_sleep_ms=0"}
	if drop {
		env = append(env, "SESSION_HOST_DROP_ACK=1")
	}
	runnerCfg := runner.Config{Socket: filepath.Join(dir, "r.sock"), Execution: "test-execution", IOTimeout: 100 * time.Millisecond,
		Plan: runner.Plan{Binary: binary, Args: []string{"-test.run=^TestFixtureProcess$"}, Cwd: dir, Env: env}}
	s, err := runner.Start(runnerCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return sessionhost.Config{StateDir: filepath.Join(dir, "state"), Socket: runnerCfg.Socket,
		Execution: runnerCfg.Execution, Adapter: adapter, IOTimeout: time.Second,
		Selection: sessionhost.Selection{Vendor: "future-vendor", Model: "not-a-compiled-model", Reasoning: "new-reasoning"}}, s
}

func openHost(t *testing.T, cfg sessionhost.Config) *sessionhost.Host {
	t.Helper()
	host, err := sessionhost.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { host.Close() })
	return host
}

func await(t *testing.T, host *sessionhost.Host, id, state string) sessionhost.State {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snapshot := host.Snapshot()
		for _, record := range snapshot.Records {
			if record.Notice.ID == id && record.State == state {
				return snapshot
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("notice %s did not reach %s: %+v", id, state, host.Snapshot())
	return sessionhost.State{}
}

func inspectRunner(t *testing.T, cfg sessionhost.Config) runner.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := runner.Dial(ctx, cfg.Socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	status, err := client.Call(ctx, runner.Request{Op: "status", Execution: cfg.Execution})
	if err != nil || status.Code != "ok" {
		t.Fatalf("runner status: %+v %v", status, err)
	}
	return status
}

// Both independently encoded fixtures acknowledge two unfamiliar selection
// tuples without core changes; a host replacement retains the same child and
// exact retries do not write again. This is not real harness qualification.
func TestPluginsAndHostReplacement(t *testing.T) {
	for _, adapter := range []sessionhost.Adapter{fixtureadapters.JSON{}, fixtureadapters.Lines{}} {
		for _, selection := range []sessionhost.Selection{
			{Vendor: "unlisted/vendor", Model: "next-model@1", Reasoning: "ultra-new"},
			{Vendor: "different-vendor", Model: "model:two", Reasoning: "reasoning+future"},
		} {
			t.Run(adapter.Name()+"/"+selection.Vendor, func(t *testing.T) {
				cfg, _ := setup(t, adapter, false)
				cfg.Selection = selection
				host := openHost(t, cfg)
				notice := sessionhost.Notice{ID: "one", Reference: "fixture/one"}
				if _, err := host.Enqueue(notice); err != nil {
					t.Fatal(err)
				}
				before := await(t, host, "one", "applied")
				first := inspectRunner(t, cfg)
				if _, err := host.Enqueue(notice); err != nil {
					t.Fatal(err)
				}
				notice.Reference = "forbidden-change"
				if _, err := host.Enqueue(notice); err == nil || err.Error() != "request_conflict" {
					t.Fatalf("changed intent accepted: %v", err)
				}
				host.Close()
				replacement := openHost(t, cfg)
				if replacement.Snapshot().Epoch != before.Epoch+1 || replacement.Snapshot().Selection != selection {
					t.Fatal("restart changed selection or epoch")
				}
				if _, err := replacement.Enqueue(sessionhost.Notice{ID: "two", Reference: "fixture/two"}); err != nil {
					t.Fatal(err)
				}
				await(t, replacement, "two", "applied")
				after := inspectRunner(t, cfg)
				if after.Instance != first.Instance || after.PID != first.PID || after.Writes != 2 {
					t.Fatalf("duplicate/replaced child: before=%+v after=%+v", first, after)
				}
			})
		}
	}
}

// A closed fence retains a bounded queue without writes; overflow and invalid
// input leave state unchanged, and releasing the fence supplies a valid control.
func TestQueueFenceAndCapacity(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	host := openHost(t, cfg)
	if err := host.Fence(false); err != nil {
		t.Fatal(err)
	}
	for i := range sessionhost.MaxRecords {
		if _, err := host.Enqueue(sessionhost.Notice{ID: fmt.Sprintf("n%d", i), Reference: "fixture/reference"}); err != nil {
			t.Fatal(err)
		}
	}
	before := host.Snapshot()
	for _, notice := range []sessionhost.Notice{{ID: "excess", Reference: "fixture"}, {ID: "bad\n", Reference: "fixture"}} {
		if _, err := host.Enqueue(notice); err == nil {
			t.Fatal("invalid or excessive notice accepted")
		}
	}
	if got := inspectRunner(t, cfg); got.Writes != 0 {
		t.Fatal("closed fence wrote to runner")
	}
	if len(host.Snapshot().Records) != len(before.Records) || host.Snapshot().Sequence != before.Sequence {
		t.Fatal("rejection changed queue")
	}
	if err := host.Fence(true); err != nil {
		t.Fatal(err)
	}
	await(t, host, "n31", "applied")
	if got := inspectRunner(t, cfg); got.Writes != sessionhost.MaxRecords {
		t.Fatal("valid queued notices did not apply exactly once")
	}
}

// An accepted write with no acknowledgement remains unknown across restart;
// neither an exact retry nor fence release can replay it or reopen delivery.
func TestLostAcknowledgementRemainsUnknown(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, true)
	host := openHost(t, cfg)
	notice := sessionhost.Notice{ID: "lost-ack", Reference: "fixture/reference"}
	if _, err := host.Enqueue(notice); err != nil {
		t.Fatal(err)
	}
	// The in-flight record is also unknown; wait for the distinct closed fence.
	deadline := time.Now().Add(2 * time.Second)
	for host.Snapshot().Fence != "unknown" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if host.Snapshot().Fence != "unknown" {
		t.Fatal("lost acknowledgement did not close fence")
	}
	host.Close()
	replacement := openHost(t, cfg)
	if replacement.Snapshot().Fence != "unknown" {
		t.Fatal("restart reopened unknown delivery")
	}
	if _, err := replacement.Enqueue(notice); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Fence(true); err == nil || err.Error() != "reconciliation_required" {
		t.Fatalf("unknown release: %v", err)
	}
	if got := inspectRunner(t, cfg); got.Writes != 1 {
		t.Fatalf("unknown effect replayed: %+v", got)
	}
}

// Duplicate hosts, changed stored plans and corrupt/truncated state fail closed
// without changing the runner's write count; a valid first host is the control.
func TestStateAndSingletonRefusals(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	host := openHost(t, cfg)
	if duplicate, err := sessionhost.Open(cfg); err == nil {
		duplicate.Close()
		t.Fatal("duplicate host admitted")
	}
	host.Close()
	changed := cfg
	changed.Selection.Model = "different-model"
	if other, err := sessionhost.Open(changed); err == nil || err.Error() != "stored_plan_conflict" {
		if other != nil {
			other.Close()
		}
		t.Fatalf("changed stored selection admitted: %v", err)
	}
	statePath := filepath.Join(cfg.StateDir, "state.json")
	valid, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range [][]byte{[]byte("{"), []byte(`{"version":999}`), append(bytes.Clone(valid), []byte("{}")...)} {
		if err := os.WriteFile(statePath, corrupt, 0600); err != nil {
			t.Fatal(err)
		}
		if other, err := sessionhost.Open(cfg); err == nil || err.Error() != "state_corrupt" {
			if other != nil {
				other.Close()
			}
			t.Fatalf("corrupt state admitted: %v", err)
		}
	}
	if err := os.WriteFile(statePath, valid, 0600); err != nil {
		t.Fatal(err)
	}
	replacement := openHost(t, cfg)
	if got := inspectRunner(t, cfg); got.Writes != 0 {
		t.Fatal("refusal caused an input effect")
	}
	if replacement.Snapshot().Fence != "open" {
		t.Fatal("valid state failed to recover")
	}
}

// Explicit stop persists before signalling its retained fixture process handle;
// future enqueue/release and restart cannot resume that stopped session.
func TestStopStaysStopped(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.Lines{}, false)
	host := openHost(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := host.Stop(ctx)
	if err != nil || response.Code != "stopped" || !response.Exited {
		t.Fatalf("stop failed: %+v %v", response, err)
	}
	if err := host.Fence(true); err == nil {
		t.Fatal("stopped fence reopened")
	}
	if _, err := host.Enqueue(sessionhost.Notice{ID: "after-stop", Reference: "fixture"}); err == nil {
		t.Fatal("stopped session accepted input")
	}
	host.Close()
	replacement := openHost(t, cfg)
	if !replacement.Snapshot().StopRequested {
		t.Fatal("stop intent lost on restart")
	}
	if err := replacement.Fence(true); err == nil {
		t.Fatal("restart reopened stopped fence")
	}
}

type diagnosticAdapter struct{ fixtureadapters.JSON }

func (diagnosticAdapter) Encode(n sessionhost.Notice, s sessionhost.Selection) ([]byte, error) {
	data, err := (fixtureadapters.JSON{}).Encode(n, s)
	return bytes.Replace(data, []byte(`"kind":"notice"`), []byte(`"kind":"fixture-canary-not-a-credential"`), 1), err
}

// Durable snapshots contain typed references/selection metadata, not encoded
// adapter payload. A fabricated noncredential marker exercises that boundary;
// this is not a general secret detector or a real-credential test.
func TestPersistedMetadataIsClosed(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	cfg.Adapter = diagnosticAdapter{}
	host := openHost(t, cfg)
	if _, err := host.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/reference"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for host.Snapshot().Fence != "unknown" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if host.Snapshot().Fence != "unknown" {
		t.Fatal("invalid diagnostic wire was not rejected")
	}
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("fixture-canary-not-a-credential")) {
		t.Fatal("adapter wire payload persisted")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"env", "argv", "binary", "prompt", "stdin", "stdout", "credentials"} {
		if _, found := fields[forbidden]; found {
			t.Fatalf("forbidden durable field %s", forbidden)
		}
	}
	info, err := os.Stat(filepath.Join(cfg.StateDir, "state.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("state not private")
	}
}

// Losing previously initialized state is not a fresh host. Refusal does not
// write a replacement snapshot or send input to the retained runner.
func TestMissingStateRefusesWithoutReplacement(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	host := openHost(t, cfg)
	host.Close()
	path := filepath.Join(cfg.StateDir, "state.json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if replacement, err := sessionhost.Open(cfg); err == nil || err.Error() != "state_missing" {
		if replacement != nil {
			replacement.Close()
		}
		t.Fatalf("missing state became fresh admission: %v", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("replacement state written after evidence loss")
	}
	if status := inspectRunner(t, cfg); status.Writes != 0 || status.Exited {
		t.Fatal("state refusal affected child")
	}
}

// An interrupted snapshot transaction is unknown even when its JSON is valid;
// startup cannot replay queued effects until explicit reconciliation exists.
func TestPendingStateUpdateKeepsDeliveryClosed(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	host := openHost(t, cfg)
	if err := host.Fence(false); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Enqueue(sessionhost.Notice{ID: "queued", Reference: "fixture"}); err != nil {
		t.Fatal(err)
	}
	host.Close()
	if err := os.WriteFile(filepath.Join(cfg.StateDir, "state.pending"), []byte("update-in-progress\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if replacement, err := sessionhost.Open(cfg); err == nil || err.Error() != "state_update_unknown" {
		if replacement != nil {
			replacement.Close()
		}
		t.Fatalf("pending transaction admitted: %v", err)
	}
	if status := inspectRunner(t, cfg); status.Writes != 0 || status.Exited {
		t.Fatal("pending snapshot replayed or stopped child")
	}
}
