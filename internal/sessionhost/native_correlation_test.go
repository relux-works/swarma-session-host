package sessionhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

// An inert Go test child answers synthetic initialize/thread/start RPCs. Its
// bytes travel through the actual retained runner and Host parser. No provider,
// credentials, real native launcher, ordinary policy or Source broker is used.
func nativeCorrelationFixture(t *testing.T) (*ordinaryWireOperation, *runner.Server, *continuingHostOwner) {
	t.Helper()
	pre, q := preChildFixture(t)
	dir, err := os.MkdirTemp("/tmp", "t2-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := []byte("#!/bin/sh\nSESSION_HOST_RPC_FIXTURE=plain GORACE=atexit_sleep_ms=0 exec '" + strings.ReplaceAll(binary, "'", "'\\''") + "' -test.run='^TestProtocolProcess$'\n")
	launcher := filepath.Join(dir, "curator-run")
	if err := os.WriteFile(launcher, script, 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(script)
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	m, err := openOrdinaryWireManager(context.Background(), time.Now().Add(time.Minute), pre.records.path,
		ordinaryLauncherConfig{binary: launcher, sha256: hex.EncodeToString(sum[:]), environment: []string{"HOME=" + u.HomeDir, "PATH=/usr/bin:/bin"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.close(context.Background()) })
	op, err := m.dispatch(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	rc, err := op.takeRunnerConfig(context.Background(), m, filepath.Join(dir, "r.sock"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	s, err := runner.Start(rc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ready := make(chan struct{}, 1)
	cfg := Config{StateDir: filepath.Join(dir, "state"), Socket: rc.Socket, Execution: rc.Execution,
		ProtocolName: "codex", NoticeMode: SyntheticNotice, IOTimeout: time.Second, PollInterval: 10 * time.Millisecond,
		Selection: Selection{Vendor: "unfamiliar-provider", Model: "unfamiliar-model", Reasoning: "unfamiliar-effort"}}
	cfg.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
		d, off, err := codexadapter.OpenDriver(ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create,
			Selection: ap.Selection{Provider: cfg.Selection.Vendor, Model: cfg.Selection.Model, Effort: cfg.Selection.Reasoning}, Workspace: rc.Plan.Cwd}, saved)
		// Host invokes this restore only after persisting actual observed bytes.
		if err == nil && len(saved) > 0 && d.Snapshot().View.NativeSession != "" {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
		return d, off, err
	}
	o := continuingOwnerOpen(t, context.Background(), time.Now().Add(time.Minute), cfg)
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("synthetic thread not observed", o.host.Snapshot())
	}
	return op, s, o
}

func adoptNativeFixture(t *testing.T) (*nativeCorrelation, *ordinaryWireOperation, *runner.Server, *continuingHostOwner) {
	t.Helper()
	op, s, o := nativeCorrelationFixture(t)
	c, err := retainNativeCorrelation(op, s, o)
	if err != nil {
		t.Fatal("retain actual resources", err)
	}
	if err := c.check(context.Background()); err == nil {
		t.Fatal("unacknowledged candidate became current")
	}
	if err := c.acknowledge(op, s, o); err != nil {
		t.Fatal("handoff", err)
	}
	t.Cleanup(func() {
		if err := c.close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return c, op, s, o
}

func TestNativeCorrelationTransfer(t *testing.T) {
	c, op, s, o := adoptNativeFixture(t)
	if err := c.check(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan := runner.Plan{Binary: op.manager.launcher.path, Args: op.claim.invocation.args, Cwd: op.claim.invocation.workspace, Env: op.manager.environment.entries}
	if !s.RetainsController(o.execution, o.runner, o.epoch, plan) {
		t.Fatal("actual controller refused")
	}
	if s.RetainsController(o.execution, o.runner, o.epoch+1, plan) {
		t.Fatal("replayed generation admitted")
	}
	plan.Binary = "/wrong-launcher"
	if s.RetainsController(o.execution, o.runner, o.epoch, plan) {
		t.Fatal("foreign launch plan admitted")
	}
	if c.thread != "synthetic-thread" {
		t.Fatal("wrong parser thread", c.thread)
	}
	if _, ok := any(c).(notificationManagerCustody); ok {
		t.Fatal("resource correlation became admission")
	}
	if _, err := retainNativeCorrelation(op, s, o); err == nil {
		t.Fatal("second retained transfer")
	}
	if err := c.acknowledge(op, s, o); err == nil {
		t.Fatal("second acknowledgment")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.check(canceled); err == nil {
		t.Fatal("canceled observation")
	}
	if err := c.check(context.Background()); err != nil {
		t.Fatal("request cancellation ended custody", err)
	}
}

func TestNativeCorrelationForeignResources(t *testing.T) {
	op, s, o := nativeCorrelationFixture(t)
	foreign, otherS, otherO := nativeCorrelationFixture(t)
	if _, err := retainNativeCorrelation(op, otherS, otherO); err == nil {
		t.Fatal("foreign execution borrowed operation")
	}
	// Failed evidence consumes this operation's handoff, never a retry license.
	if _, err := retainNativeCorrelation(op, s, o); err == nil {
		t.Fatal("failed handoff recycled")
	}
	c, err := retainNativeCorrelation(foreign, otherS, otherO)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.acknowledge(op, otherS, otherO); err == nil {
		t.Fatal("foreign acknowledgment")
	}
	if err := c.acknowledge(foreign, otherS, otherO); err != nil {
		t.Fatal("foreign query disturbed owner", err)
	}
	if err := c.close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCorrelationTerminalLoss(t *testing.T) {
	for _, how := range []string{"wrong-thread", "generation", "unknown", "revoked", "restored-driver", "prechild-loss", "runner-loss", "physical-record-loss"} {
		t.Run(how, func(t *testing.T) {
			c, _, s, o := adoptNativeFixture(t)
			h := o.host
			h.mu.Lock()
			original := cloneState(h.state)
			driver := h.driver
			switch how {
			case "wrong-thread":
				h.state.Protocol.View.NativeSession = "wrong-thread"
			case "generation":
				h.state.Epoch++
			case "unknown":
				h.state.Fence = "unknown"
			case "revoked":
				h.state.SecurityStopped = true
			case "restored-driver":
				d, _, err := codexadapter.OpenDriver(ap.Config{NativeRelease: codexadapter.ObservedRelease, Lifecycle: ap.Create,
					Selection: ap.Selection{Provider: h.cfg.Selection.Vendor, Model: h.cfg.Selection.Model, Effort: h.cfg.Selection.Reasoning}, Workspace: c.op.claim.invocation.workspace}, h.state.Protocol.Checkpoint)
				if err != nil {
					h.mu.Unlock()
					t.Fatal(err)
				}
				h.driver = d
			}
			h.mu.Unlock()
			if how == "prechild-loss" {
				c.op.manager.owner.cancel()
			}
			if how == "runner-loss" {
				_ = s.Close()
			}
			if how == "physical-record-loss" {
				path := filepath.Join(c.op.manager.owner.records.path, "member-1.json")
				if err := os.Rename(path, path+".held"); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.check(context.Background()); err == nil {
				t.Fatal("loss remained current")
			}
			select {
			case <-c.done:
			case <-time.After(5 * time.Second):
				t.Fatal("terminal loss did not join")
			}
			h.mu.Lock()
			h.state = original
			h.driver = driver
			h.mu.Unlock()
			if err := c.check(context.Background()); err == nil {
				t.Fatal("metadata restored terminal proof")
			}
		})
	}
}

func TestNativeCorrelationCanceledCloseAndRace(t *testing.T) {
	c, op, s, o := adoptNativeFixture(t)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.check(context.Background()) }()
	}
	wg.Wait()
	// The Host barrier holds cleanup while a canceled waiter leaves. No second
	// request is needed to finish joining the exact transferred resources.
	o.host.mu.Lock()
	wait, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.close(wait)
	select {
	case <-c.done:
		o.host.mu.Unlock()
		t.Fatal("cleanup passed held Host barrier")
	default:
	}
	o.host.mu.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup abandoned")
	}
	if err := c.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !op.manager.closed || s.RetainsController(o.execution, o.runner, o.epoch, runner.Plan{}) {
		t.Fatal("transferred resource survived cleanup")
	}
	if err := c.check(context.Background()); err == nil {
		t.Fatal("closed correlation reopened")
	}
}

func TestNativeCorrelationOwnerLossJoins(t *testing.T) {
	c, _, _, o := adoptNativeFixture(t)
	o.cancel()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatal("owner loss did not join transfer")
	}
	if err := c.check(context.Background()); err == nil {
		t.Fatal("lost owner current")
	}
}

func TestNativeCorrelationConcurrentAcknowledgment(t *testing.T) {
	op, s, o := nativeCorrelationFixture(t)
	c, err := retainNativeCorrelation(op, s, o)
	if err != nil {
		t.Fatal(err)
	}
	wins := make(chan bool, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); wins <- c.acknowledge(op, s, o) == nil }()
	}
	wg.Wait()
	close(wins)
	count := 0
	for win := range wins {
		if win {
			count++
		}
	}
	if count != 1 {
		t.Fatal("handoff acknowledgments", count)
	}
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.check(context.Background()) }()
	}
	if err := c.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestNativeCorrelationResumeRefusal(t *testing.T) {
	op, s, o := nativeCorrelationFixture(t)
	op.manager.owner.mu.Lock()
	op.claim.kind = "resume"
	op.manager.owner.mu.Unlock()
	if c, err := retainNativeCorrelation(op, s, o); err == nil || c != nil {
		t.Fatal("resume correlated", err)
	}
}
