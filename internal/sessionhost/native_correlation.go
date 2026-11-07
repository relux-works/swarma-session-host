package sessionhost

import (
	"context"
	"errors"
	"sync"

	"github.com/relux-works/swarma-session-host/internal/codexadapter"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

var errNativeCorrelation = errors.New("native_correlation_unavailable")

// nativeCorrelation retains physical resources, never a manager admission grant.
// Private composition calls this boundary. T1 must acknowledge the
// returned owner before relinquishing its own cleanup; this is not a DTO dock.
type nativeCorrelation struct {
	mu        sync.Mutex
	op        *ordinaryWireOperation
	server    *runner.Server
	owner     *continuingHostOwner
	driver    *codexadapter.Driver
	thread    string
	lost      bool
	done      chan struct{}
	closeOnce sync.Once
	adopted   bool
	closeErr  error
}

func retainNativeCorrelation(op *ordinaryWireOperation, server *runner.Server, owner *continuingHostOwner) (*nativeCorrelation, error) {
	if op == nil || op.manager == nil || op.manager.owner == nil || server == nil || owner == nil || owner.host == nil || owner.ctx == nil {
		return nil, errNativeCorrelation
	}
	m := op.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.operation != op || op.t2 != nil || !op.ready || op.claim == nil || op.claim.owner != m.owner {
		return nil, errNativeCorrelation
	}
	m.owner.mu.Lock()
	defer m.owner.mu.Unlock()
	if !op.claim.attempted || op.claim.kind != "launch" || m.owner.operation != op.claim {
		return nil, errNativeCorrelation
	}
	h := owner.host
	h.mu.Lock()
	defer h.mu.Unlock()
	d, ok := h.driver.(*codexadapter.Driver)
	if !ok || h.recovering || h.client == nil || h.state.Execution != op.claim.id || !owner.currentLocked(h) {
		return nil, errNativeCorrelation
	}
	c := &nativeCorrelation{op: op, server: server, owner: owner, driver: d, done: make(chan struct{})}
	// Consume the handoff slot even if its current evidence is refused.
	op.t2 = c
	if !c.currentLocked() {
		return nil, errNativeCorrelation
	}
	return c, nil
}

// Locks: correlation, wire manager, pre-child owner, Host, then driver/runner.
// Physical file checks run under custody locks; no runner RPC or cleanup does.
func (c *nativeCorrelation) currentLocked() bool {
	m, h := c.op.manager, c.owner.host
	thread, fresh := c.driver.FreshThread()
	good := !c.lost && !m.closed && m.owner.ctx.Err() == nil && !m.owner.closed &&
		m.operation == c.op && m.owner.operation == c.op.claim && c.op.claim.owner == m.owner &&
		c.op.claim.kind == "launch" && c.op.claim.attempted && c.op.claim.retained != nil && c.op.claim.retained.recheckPhysical(m.owner.records, "launch", c.op.claim.data) == nil &&
		c.owner.currentLocked(h) && h.driver == c.driver && !h.recovering && !h.protocolFailed && !h.writeBlocked &&
		h.state.Fence != "unknown" && !h.state.SecurityStopped && h.state.Protocol != nil && h.state.Protocol.Refusal == "" && len(h.state.Protocol.Pending) == 0 && h.outputOffset != 0 &&
		fresh && h.state.Protocol.View.NativeSession == thread && (c.thread == "" || c.thread == thread) &&
		c.server.RetainsController(c.owner.execution, c.owner.runner, c.owner.epoch, runner.Plan{Binary: m.launcher.path, Args: c.op.claim.invocation.args, Cwd: c.op.claim.invocation.workspace, Env: m.environment.entries})
	if !good {
		c.lost = true
		return false
	}
	c.thread = thread
	return true
}

func (c *nativeCorrelation) check(request context.Context) error {
	if c == nil || request == nil || request.Err() != nil {
		return errNativeCorrelation
	}
	terminal := false
	defer func() {
		if terminal {
			go func() { _ = c.close(context.Background()) }()
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.op.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	m.owner.mu.Lock()
	defer m.owner.mu.Unlock()
	c.owner.host.mu.Lock()
	defer c.owner.host.mu.Unlock()
	if request.Err() != nil {
		return errNativeCorrelation
	}
	if !c.adopted {
		return errNativeCorrelation
	}
	if !c.currentLocked() {
		terminal = true
		return errNativeCorrelation
	}
	return nil
}

// acknowledge is the one-time transfer acknowledgment. The T1 caller must hold
// its own create/close serialization and retain cleanup until this returns nil.
// It passes retained objects, never a success flag or decoded identity. No
// public caller is installed; T1's release must follow this return.
func (c *nativeCorrelation) acknowledge(op *ordinaryWireOperation, server *runner.Server, owner *continuingHostOwner) error {
	if c == nil || c.op != op || c.server != server || c.owner != owner {
		return errNativeCorrelation
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.op.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	m.owner.mu.Lock()
	defer m.owner.mu.Unlock()
	c.owner.host.mu.Lock()
	defer c.owner.host.mu.Unlock()
	if c.adopted || !c.currentLocked() {
		return errNativeCorrelation
	}
	c.adopted = true
	// Retain the original finite pre-child lifetime. Releasing T1's cleanup duty
	// must not cancel it: expiry/revocation still ends all transferred resources.
	go func() {
		select {
		case <-m.owner.ctx.Done():
		case <-owner.ctx.Done():
		case <-owner.host.ctx.Done():
		case <-c.done:
			return
		}
		_ = c.close(context.Background())
	}()
	return nil
}

// Cleanup continues if the caller abandons its waiter. Only retained objects
// are closed; no PID/socket reconstruction or replacement controller is used.
func (c *nativeCorrelation) close(wait context.Context) error {
	if c == nil || wait == nil || c.done == nil {
		return errNativeCorrelation
	}
	c.mu.Lock()
	adopted := c.adopted
	c.mu.Unlock()
	if !adopted {
		return errNativeCorrelation
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.lost = true
		c.mu.Unlock()
		go func() {
			defer close(c.done)
			c.closeErr = joinOwnedCleanup(
				func() error { return c.owner.close(context.Background()) },
				c.server.Close,
				func() error { return c.op.manager.close(context.Background()) },
			)
		}()
	})
	select {
	case <-c.done:
		return c.closeErr
	case <-wait.Done():
		return wait.Err()
	}
}
