package sessionhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/runner"
)

type inertCreatePolicy struct {
	operation *ordinaryWireOperation
	calls     int
	denyAt    int
	change    func()
}

func (*inertCreatePolicy) close() {}

func (p *inertCreatePolicy) checkCreate(ctx context.Context, op *ordinaryWireOperation) error {
	p.calls++
	if ctx.Err() != nil || op == nil || op.claim.retained == nil || op.manager.operation != op {
		return errOrdinaryWireUnavailable
	}
	if p.operation == nil {
		p.operation = op
	}
	if p.operation != op || p.calls == p.denyAt {
		return errOrdinaryWireUnavailable
	}
	if p.change != nil && p.calls == 1 {
		p.change()
	}
	return nil
}

type inertCreateChild struct {
	closes int
	err    error
}

func (c *inertCreateChild) Close() error { c.closes++; return c.err }
func createFixture(t *testing.T) (*ordinaryCreateHandler, []byte, *inertCreateChild, *int) {
	t.Helper()
	m, q := ordinaryWireFixture(t)
	c, calls := &inertCreateChild{}, new(int)
	h := &ordinaryCreateHandler{wire: m, socket: "/tmp/synthetic-create.sock", timeout: time.Millisecond,
		policy: &inertCreatePolicy{}, done: make(chan struct{})}
	h.start = func(cfg runner.Config) (createRunner, error) {
		*calls++
		if !m.operation.claim.attempted || cfg.Execution != m.operation.claim.id || cfg.Plan.Cwd != m.operation.claim.invocation.workspace || len(cfg.Plan.Args) != 19 {
			t.Error("start lost actual once-owned configuration")
		}
		return c, nil
	}
	go h.watch()
	t.Cleanup(func() { _ = h.close(context.Background()) })
	return h, memberOperationBytes(t, q), c, calls
}
func TestCreateOnceAndCleanup(t *testing.T) {
	h, body, c, starts := createFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	got, err := h.create(ctx, "launch", body)
	if err != nil || got != createRunnerOwned || *starts != 1 {
		t.Fatal(got, err, *starts)
	}
	cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if state, err := h.create(context.Background(), "launch", body); err != nil || state != createRunnerOwned {
				t.Error(state, err)
			}
		}()
	}
	wg.Wait()
	if *starts != 1 {
		t.Fatal("duplicate effect")
	}
	if _, err := h.create(context.Background(), "launch", append(body, ' ')); err == nil {
		t.Fatal("conflicting bytes reconciled")
	}
	if _, err := h.custody(); err == nil {
		t.Fatal("fake child became T2 authority")
	}
	if err := h.close(context.Background()); err != nil || c.closes != 1 {
		t.Fatal(err, c.closes)
	}
}
func TestCreateRefusalsNeverRetry(t *testing.T) {
	for _, name := range []string{"production-policy", "nil-policy", "final-policy", "physical-loss", "canceled", "failed-start", "ambiguous-child", "owner-loss"} {
		t.Run(name, func(t *testing.T) {
			h, b, c, starts := createFixture(t)
			ctx := context.Background()
			switch name {
			case "production-policy":
				h.policy = unselectedCreatePolicy{}
			case "nil-policy":
				h.policy = nil
			case "final-policy":
				h.policy = &inertCreatePolicy{denyAt: 2}
			case "physical-loss":
				h.policy = &inertCreatePolicy{change: func() {
					if err := os.WriteFile(h.wire.launcher.path, []byte("changed"), 0700); err != nil {
						t.Fatal(err)
					}
				}}
			case "canceled":
				x, cancel := context.WithCancel(ctx)
				cancel()
				ctx = x
			case "owner-loss":
				h.wire.owner.cancel()
			case "failed-start":
				h.start = func(runner.Config) (createRunner, error) { *starts++; return nil, errors.New("synthetic") }
			case "ambiguous-child":
				h.start = func(runner.Config) (createRunner, error) { *starts++; return c, errors.New("synthetic") }
			}
			if _, err := h.create(ctx, "launch", b); err == nil {
				t.Fatal("refusal accepted")
			}
			if name != "canceled" {
				_, _ = h.create(context.Background(), "launch", b)
			}
			want := 0
			if name == "failed-start" || name == "ambiguous-child" {
				want = 1
			}
			if *starts != want {
				t.Fatal("wrong start count", *starts, want)
			}
			_ = h.close(context.Background())
			if name == "ambiguous-child" && c.closes != 1 {
				t.Fatal("possible child abandoned")
			}
		})
	}
}
func TestCreateCancellationDuringStartAndUnknownCleanup(t *testing.T) {
	h, b, c, starts := createFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	original := h.start
	h.start = func(cfg runner.Config) (createRunner, error) { close(entered); <-release; return original(cfg) }
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() { defer close(returned); _, _ = h.create(ctx, "launch", b) }()
	<-entered
	cancel()
	waiter, stop := context.WithCancel(context.Background())
	stop()
	_ = h.close(waiter)
	c.err = errors.New("synthetic cleanup unknown")
	close(release)
	<-returned
	if err := h.close(context.Background()); err == nil || *starts != 1 || c.closes != 1 {
		t.Fatal(err, *starts, c.closes)
	}
	if _, err := h.create(context.Background(), "launch", b); err == nil {
		t.Fatal("closed replay")
	}
}

// Real runner with a bounded inert shell child: no launcher, native runtime,
// provider, credentials or network. Policy remains test-only and object-bound.
func TestCreateRealInertRunnerCustody(t *testing.T) {
	m, q := ordinaryWireFixture(t)
	script := []byte("#!/bin/sh\nread ignored\n")
	if err := os.WriteFile(m.launcher.path, script, 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(script)
	dir, err := os.MkdirTemp("", "t1-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	cfg := ordinaryLauncherConfig{binary: m.launcher.path, sha256: hex.EncodeToString(sum[:]), environment: m.environment.entries}
	h, err := openOrdinaryCreateHandler(context.Background(), time.Now().Add(10*time.Second), m.owner.records.path, cfg, filepath.Join(dir, "runner.sock"), 100*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.close(context.Background())
	h.policy = &inertCreatePolicy{}
	b := memberOperationBytes(t, q)
	if state, err := h.create(context.Background(), "launch", b); err != nil || state != createRunnerOwned {
		t.Fatal(state, err)
	}
	receipt, err := h.custody()
	if err != nil || receipt.server == nil || receipt.operation != h.operation || receipt.claim != h.operation.claim || receipt.retained != h.operation.claim.retained || receipt.source != h.wire.owner.records {
		t.Fatal("lost actual handles", err)
	}
	if err := h.close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
