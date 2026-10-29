package sessionhost_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// Real Unix control calls reject stale execution/epoch and unsupported operations
// before any runner write. A current admitted fixture request supplies the control.
func TestControlEndpointFencesMutations(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	host := openHost(t, cfg)
	endpoint := filepath.Join(filepath.Dir(cfg.StateDir), "host.sock")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sessionhost.Serve(ctx, endpoint, host, time.Second) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	deadline := time.Now().Add(time.Second)
	for {
		callCtx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err := sessionhost.Call(callCtx, endpoint, sessionhost.Request{Op: "status"})
		stop()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("control endpoint not ready")
		}
		time.Sleep(time.Millisecond)
	}
	request := sessionhost.Request{Op: "enqueue", Execution: cfg.Execution, Epoch: 0,
		Notice: sessionhost.Notice{ID: "one", Reference: "fixture/one"}}
	for _, tc := range []struct {
		execution string
		epoch     uint64
		op, code  string
	}{
		{cfg.Execution, 0, "enqueue", "stale_session"},
		{"other", 1, "enqueue", "stale_session"},
		{cfg.Execution, 1, "unsupported", "unsupported_operation"},
		{cfg.Execution, 1, "enqueue", "ok"},
	} {
		request.Execution, request.Epoch, request.Op = tc.execution, tc.epoch, tc.op
		callCtx, stop := context.WithTimeout(context.Background(), time.Second)
		response, err := sessionhost.Call(callCtx, endpoint, request)
		stop()
		if err != nil || response.Code != tc.code {
			t.Fatalf("control result: %+v %v", response, err)
		}
	}
	await(t, host, "one", "applied")
	if got := inspectRunner(t, cfg); got.Writes != 1 {
		t.Fatal("forbidden control wrote to child")
	}
}

// A closed controller cannot change disk or stop the retained runner after a
// replacement host takes its state lock. This guards stale post-close effects.
func TestClosedHostCannotMutateReplacement(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	old := openHost(t, cfg)
	old.Close()
	replacement := openHost(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := old.Stop(ctx); err == nil || err.Error() != "host_unavailable" {
		t.Fatalf("closed host stop: %v", err)
	}
	if err := old.Fence(false); err == nil {
		t.Fatal("closed host changed fence")
	}
	if _, err := old.Enqueue(sessionhost.Notice{ID: "forbidden", Reference: "fixture"}); err == nil {
		t.Fatal("closed host enqueued")
	}
	if replacement.Snapshot().StopRequested || replacement.Snapshot().Fence != "open" {
		t.Fatal("replacement state changed")
	}
	if got := inspectRunner(t, cfg); got.Exited || got.Writes != 0 {
		t.Fatal("stale host affected child")
	}
}
