package sessionmanager

import "context"

// A native response wait may need a pending-request pass before the peer can
// acknowledge the original command. The same host worker serves that pass
// synchronously, retaining a D1 admission or acquiring its own exclusive
// fence/durable I/O admission during an ordinary response-only wait.
// Only the closed native resolver can be queued; no caller callback or RPC
// selector crosses this private scope.
type hostMuseWaitScope struct {
	host         *sessionInput
	lease        ControllerLease
	requests     chan hostMusePendingCall
	observations chan hostMuseObservationPublication
	done         chan struct{}
	resolve      func(context.Context) error
	ordinary     *hostMuseInputPhase
}

type hostMusePendingCall struct {
	ctx    context.Context
	result chan error
}

type hostMuseWaitScopeKey struct{}

// Called only after the exact lease and durable I/O have been admitted under
// the actual writer fence. finish precedes releaseIO and fence release. A pass
// accepted by the unbuffered queue runs on that worker before finish can run.
func (h *sessionInput) beginMuseWaitScope(ctx context.Context, lease ControllerLease) (context.Context, func()) {
	backend, ok := h.backend.(interface{ ResolveMusePending(context.Context) error })
	if !ok {
		return ctx, func() {}
	}
	scope := &hostMuseWaitScope{host: h, lease: lease, requests: make(chan hostMusePendingCall), observations: make(chan hostMuseObservationPublication), done: make(chan struct{}), resolve: backend.ResolveMusePending}
	h.mu.Lock()
	h.musePending = scope
	h.notifyMuseWaitLocked()
	h.mu.Unlock()
	return context.WithValue(ctx, hostMuseWaitScopeKey{}, scope), func() {
		h.mu.Lock()
		if h.musePending == scope {
			h.musePending = nil
		}
		close(scope.done)
		h.notifyMuseWaitLocked()
		h.mu.Unlock()
	}
}

// A waiter notices scopes created after it began waiting as well as the
// physical fence becoming available. Nil means this caller owns that fence.
func (h *sessionInput) waitMuseFence(ctx context.Context, lease ControllerLease) (*hostMuseWaitScope, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h.mu.Lock()
		scope := h.musePending
		if scope == nil && len(h.museWaiting) != 0 {
			scope = h.museWaiting[0]
		}
		if h.museWaitChanged == nil {
			h.museWaitChanged = make(chan struct{})
		}
		changed := h.museWaitChanged
		h.mu.Unlock()
		if scope != nil && scope.lease == lease {
			return scope, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
			continue
		case <-h.fence:
			if err := ctx.Err(); err != nil {
				h.unlock()
				return nil, err
			}
			return nil, nil
		}
	}
}
func (h *sessionInput) notifyMuseWaitLocked() {
	if h.museWaitChanged != nil {
		close(h.museWaitChanged)
	}
	h.museWaitChanged = make(chan struct{})
}

func (h *sessionInput) joinMusePendingScope(ctx context.Context, lease ControllerLease) (error, bool) {
	for {
		scope, err := h.waitMuseFence(ctx, lease)
		if err != nil {
			return err, true
		}
		if scope == nil {
			return nil, false
		} // actual fence transferred to this caller
		call := hostMusePendingCall{ctx: ctx, result: make(chan error, 1)}
		select {
		case scope.requests <- call:
			// The accepted pass joins its native writes before replying.
			return <-call.result, true
		case <-scope.done:
			continue
		case <-ctx.Done():
			return ctx.Err(), true
		}
	}
}

func (s *hostMuseWaitScope) serve(parent context.Context, call hostMusePendingCall) {
	ctx, cancel := bindInputContext(parent, call.ctx)
	defer cancel()
	// A pass is one indivisible native inventory/response operation: another
	// pass cannot nest in its own RPC waits, even if more callers are queued.
	ctx = context.WithValue(ctx, hostMuseWaitScopeKey{}, false)
	ctx = context.WithValue(ctx, hostMuseInputPhaseKey{}, false)
	if s.ordinary != nil {
		release, err := s.ordinary.acquire(ctx)
		if err != nil {
			call.result <- err
			return
		}
		defer release()
	}
	if err := s.host.requireLease(ctx, s.lease); err != nil {
		call.result <- err
		return
	}
	err := s.resolve(ctx)
	if err == nil {
		err = s.host.requireLease(ctx, s.lease)
	}
	call.result <- err
}

type hostMuseObservationPublication struct {
	ctx    context.Context
	feed   *hostMuseObservationFeed
	value  MuseObservation
	result chan hostMuseObservationPublished
}
type hostMuseObservationPublished struct {
	value MuseObservation
	err   error
	retry bool
}

// Read-only metadata publication must also pass the actual writer fence. The
// worker publishes the native value, then the fork can interpret it and queue
// its separate pending pass; no canonical event handler runs in this worker.
func (s *hostMuseWaitScope) publish(parent context.Context, call hostMuseObservationPublication) {
	ctx, cancel := bindInputContext(parent, call.ctx)
	defer cancel()
	var value MuseObservation
	var err error
	if s.ordinary != nil {
		var release func()
		release, err = s.ordinary.acquire(ctx)
		if err == nil {
			defer release()
		}
	}
	if err == nil {
		value, err = call.feed.publishMuseValueUnderFence(ctx, call.value)
	}
	retry := err != nil && ctx.Err() != nil && call.ctx.Err() == nil
	call.result <- hostMuseObservationPublished{value: value, err: err, retry: retry}
}

func (f *hostMuseObservationFeed) publishMuseValue(ctx context.Context, value MuseObservation) (MuseObservation, error) {
	for {
		scope, err := f.host.waitMuseFence(ctx, f.lease)
		if err != nil {
			return MuseObservation{}, err
		}
		if scope != nil {
			call := hostMuseObservationPublication{ctx: ctx, feed: f, value: value, result: make(chan hostMuseObservationPublished, 1)}
			select {
			case scope.observations <- call:
				result := <-call.result
				if result.retry {
					continue
				}
				return result.value, result.err
			case <-scope.done:
				continue
			case <-ctx.Done():
				return MuseObservation{}, ctx.Err()
			}
		}
		// No active worker accepted the value. This reader owns the ordinary
		// physical fence and admits its own durable I/O before publishing.
		defer f.host.unlock()
		release, err := f.host.acquireIO(ctx, f.lease)
		if err != nil {
			return MuseObservation{}, err
		}
		defer release()
		return f.publishMuseValueUnderFence(ctx, value)
	}
}

func (f *hostMuseObservationFeed) publishMuseValueUnderFence(ctx context.Context, value MuseObservation) (MuseObservation, error) {
	if err := f.host.requireMuseObservationLease(ctx, f.lease); err != nil {
		return MuseObservation{}, err
	}
	if f.ctx.Err() != nil {
		return MuseObservation{}, ErrMuseObservationClosed
	}
	owned, err := f.validate(value)
	if err != nil {
		f.Close()
		return MuseObservation{}, err
	}
	f.last = owned.Sequence
	f.pending = nil
	owned.Lease = f.lease
	owned.ObservedAt = f.host.now()
	return owned, nil
}

func (s *hostMuseWaitScope) pendingCalls() <-chan hostMusePendingCall { return s.requests }
func (s *hostMuseWaitScope) nativePublications() <-chan hostMuseObservationPublication {
	return s.observations
}
