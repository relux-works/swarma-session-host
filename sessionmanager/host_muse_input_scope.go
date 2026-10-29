package sessionmanager

import (
	"context"
	"sync/atomic"
)

// An ordinary native input owns its initial admission, then re-enters the same
// arbiter for every later physical frame. Pure response waits own no writer
// resource. This private context capability never crosses a public host port.
type hostMuseInputPhase struct {
	host           *sessionInput
	lease          ControllerLease
	initial        bool
	closed         atomic.Bool
	releaseInitial func()
}
type hostMuseInputPhaseKey struct{}

func (h *sessionInput) beginMuseOrdinaryWaitScope(ctx context.Context, lease ControllerLease, releaseInitial func()) (context.Context, func()) {
	backend, ok := h.backend.(interface{ ResolveMusePending(context.Context) error })
	if !ok {
		return ctx, func() {}
	}
	phase := &hostMuseInputPhase{host: h, lease: lease, initial: true, releaseInitial: releaseInitial}
	scope := &hostMuseWaitScope{host: h, lease: lease, requests: make(chan hostMusePendingCall), observations: make(chan hostMuseObservationPublication), done: make(chan struct{}), resolve: backend.ResolveMusePending, ordinary: phase}
	h.mu.Lock()
	h.museWaiting = append(h.museWaiting, scope)
	h.notifyMuseWaitLocked()
	h.mu.Unlock()
	ctx = context.WithValue(ctx, hostMuseInputPhaseKey{}, phase)
	ctx = context.WithValue(ctx, hostMuseWaitScopeKey{}, scope)
	return ctx, func() {
		phase.closed.Store(true)
		h.mu.Lock()
		for i, item := range h.museWaiting {
			if item == scope {
				h.museWaiting = append(h.museWaiting[:i], h.museWaiting[i+1:]...)
				break
			}
		}
		close(scope.done)
		h.notifyMuseWaitLocked()
		h.mu.Unlock()
		// Also handles validation/cancellation before the first physical write.
		// Both captured releases are the initial admission's one-shot releases.
		releaseInitial()
	}
}

// Later attempts join the existing FIFO arbiter; they cannot borrow another
// input's admission. Every failure releases only resources acquired here.
func (s *hostMuseInputPhase) acquire(ctx context.Context) (func(), error) {
	if s.closed.Load() {
		return nil, ErrInputScopeClosed
	}
	if err := s.host.lock(ctx); err != nil {
		return nil, err
	}
	if err := s.host.requireLease(ctx, s.lease); err != nil {
		s.host.unlock()
		return nil, err
	}
	releaseIO, err := s.host.acquireIO(ctx, s.lease)
	if err != nil {
		s.host.unlock()
		return nil, err
	}
	release := func() { releaseIO(); s.host.unlock() }
	if s.closed.Load() {
		release()
		return nil, ErrInputScopeClosed
	}
	if err := s.host.requireLease(ctx, s.lease); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (s *hostMuseInputPhase) write(ctx context.Context, write func(context.Context) error) error {
	if s.closed.Load() {
		return ErrInputScopeClosed
	}
	release := s.releaseInitial
	if s.initial {
		s.initial = false
	} else {
		var err error
		release, err = s.acquire(ctx)
		if err != nil {
			return err
		}
	}
	defer release()
	if err := s.host.requireLease(ctx, s.lease); err != nil {
		return err
	}
	// A cancelled pipe write may still be draining. Join that exact write before
	// releasing its arbiter admission, even if its caller stopped waiting.
	var drain mspWriteDrain
	defer drain.wait()
	return write(drain.bind(ctx))
}
