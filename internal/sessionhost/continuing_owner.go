package sessionhost

import (
	"context"
	"errors"
	"time"
)

var errHostOwnerUnavailable = errors.New("host_owner_unavailable")

// continuingHostOwner retains the actual controller across individual manager
// requests. Only its own Open creates the retained Host; a decoded state or an
// arbitrary Host pointer cannot replace it. This is resource ownership, not
// member admission: it deliberately does not implement notificationManagerCustody.
// The selected manager must independently admit the record, assignment, profile,
// workflow and operation before it may create any broker attachment.
type continuingHostOwner struct {
	host      *Host
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	execution string
	runner    string
	epoch     uint64
}

// The caller supplies the continuing composition's context and explicit finite
// custody bound, never an individual launch/status request's context. Open keeps
// its existing bounded runner I/O and state-lock semantics. Cancellation while
// opening cleans up the actual opened Host rather than publishing a late owner.
func openContinuingHostOwner(parent context.Context, expires time.Time, cfg Config) (*continuingHostOwner, error) {
	if parent == nil || parent.Err() != nil || !time.Now().Before(expires) {
		return nil, errHostOwnerUnavailable
	}
	ctx, cancel := context.WithDeadline(parent, expires)
	h, err := Open(cfg)
	if err != nil {
		cancel()
		return nil, err
	}
	if ctx.Err() != nil {
		cancel()
		_ = h.Close()
		return nil, errHostOwnerUnavailable
	}
	h.mu.Lock()
	o := &continuingHostOwner{host: h, ctx: ctx, cancel: cancel, done: make(chan struct{}),
		execution: h.state.Execution, runner: h.state.RunnerInstance, epoch: h.state.Epoch}
	h.mu.Unlock()
	go o.watch()
	return o, nil
}

func (o *continuingHostOwner) watch() {
	defer close(o.done)
	select {
	case <-o.ctx.Done():
	case <-o.host.ctx.Done():
	}
	o.cancel()
	// Close joins Host services and releases only this controller. The runner
	// and durable notification journal retain their original ownership.
	_ = o.host.Close()
}

// currentLocked is only the physical-owner conjunct of future admission. It
// neither consults a public projection nor asserts membership or Source origin.
// The caller holds o.host.mu, including when producing an advisory snapshot.
func (o *continuingHostOwner) currentLocked(expected *Host) bool {
	h := o.host
	if expected != h {
		return false // A foreign-object query must not disturb the real owner.
	}
	current := o.ctx.Err() == nil && h.ctx.Err() == nil && h.err == nil &&
		!h.state.StopRequested && h.state.Execution == o.execution &&
		h.state.RunnerInstance == o.runner && h.state.Epoch == o.epoch
	if !current {
		o.cancel() // Observed loss is terminal, even if metadata is later restored.
	}
	return current
}

func (o *continuingHostOwner) checkCurrentHost(expected *Host) error {
	if o == nil || o.host == nil || o.ctx == nil {
		return errHostOwnerUnavailable
	}
	o.host.mu.Lock()
	defer o.host.mu.Unlock()
	if !o.currentLocked(expected) {
		return errHostOwnerUnavailable
	}
	return nil
}

// snapshot's request context only controls this observation. Its cancellation
// never closes the continuing owner. Returned metadata is a copy, not authority.
func (o *continuingHostOwner) snapshot(request context.Context) (State, error) {
	if o == nil || o.host == nil || o.ctx == nil || request == nil || request.Err() != nil {
		return State{}, errHostOwnerUnavailable
	}
	o.host.mu.Lock()
	defer o.host.mu.Unlock()
	if request.Err() != nil || !o.currentLocked(o.host) {
		return State{}, errHostOwnerUnavailable
	}
	return cloneState(o.host.state), nil
}

// A canceled close waiter does not abandon cleanup or reclaim custody. The
// watcher keeps joining the owned Host; callers may observe done separately.
func (o *continuingHostOwner) close(wait context.Context) error {
	if o == nil || o.cancel == nil || o.done == nil || wait == nil {
		return errHostOwnerUnavailable
	}
	o.cancel()
	select {
	case <-o.done:
		return nil
	default:
	}
	select {
	case <-o.done:
		return nil
	case <-wait.Done():
		return wait.Err()
	}
}
