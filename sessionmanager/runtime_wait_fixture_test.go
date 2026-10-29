package sessionmanager

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Wait callbacks report evaluation only, preserving the actual parent context.
// In cross-process fixtures Done is evaluated after failed real flock; in
// WaitExit it is evaluated after the live-process predicate was checked.
type nativeFixtureWaitContext struct {
	context.Context
	onDone func()
}

func (c *nativeFixtureWaitContext) Done() <-chan struct{} {
	if c.onDone != nil {
		c.onDone()
	}
	return c.Context.Done()
}

// A callback must not substitute cancellation or evidence for the parent.
func TestNativeFixtureWaitContextPreservesParent(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := 0
	ctx := &nativeFixtureWaitContext{Context: parent, onDone: func() { observed++ }}
	if ctx.Done() != parent.Done() || observed != 1 || ctx.Err() != nil {
		t.Fatal("native wait observer replaced context evidence")
	}
	cancel()
	if ctx.Done() != parent.Done() || ctx.Err() != context.Canceled {
		t.Fatal("native wait observer replaced cancellation")
	}
}

// museTestAckDeadline is one fake goal-ack wait: it never fires unless
// the test fires it, and firing reports context.DeadlineExceeded like
// the real GoalAckTimeout. Answered reads hold one and never fire it.
// Parent cancellation still aborts the wait (the child derives from the
// parent), so context timeouts and Stop keep working under the hook.
type museTestAckDeadline struct {
	parent context.Context
	child  context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	fired  bool
}

func (d *museTestAckDeadline) Deadline() (time.Time, bool) { return time.Time{}, false }

func (d *museTestAckDeadline) Done() <-chan struct{} { return d.child.Done() }

func (d *museTestAckDeadline) Err() error {
	d.mu.Lock()
	fired := d.fired
	d.mu.Unlock()
	if fired {
		return context.DeadlineExceeded
	}
	return d.parent.Err()
}

func (d *museTestAckDeadline) Value(key any) any { return d.parent.Value(key) }

func (d *museTestAckDeadline) fire() {
	d.mu.Lock()
	d.fired = true
	d.mu.Unlock()
	d.cancel()
}

// museTestAckClock is the r42 fake ack clock. hook replaces the host's
// per-wait GoalAckTimeout contexts; fireLatest fires the most recently
// created wait. Applies hold goalMu and create waits sequentially with
// no worker in flight after the settle barrier, so the latest wait is
// always the current phase's wait.
type museTestAckClock struct {
	mu     sync.Mutex
	latest *museTestAckDeadline
	count  int
}

func (c *museTestAckClock) hook(parent context.Context) (context.Context, context.CancelFunc) {
	child, cancel := context.WithCancel(parent)
	d := &museTestAckDeadline{parent: parent, child: child, cancel: cancel}
	c.mu.Lock()
	c.latest = d
	c.count++
	c.mu.Unlock()
	return d, cancel
}

// fireLatest fires the host's current ack wait, simulating a lost
// response. Call it after reading the request frame the wait bounds;
// the host is blocked in that wait and observes DeadlineExceeded.
func (c *museTestAckClock) fireLatest(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	d := c.latest
	c.mu.Unlock()
	if d == nil {
		t.Fatal("no ack deadline to fire (host has not created one)")
	}
	d.fire()
}
