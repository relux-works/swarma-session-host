package sessionbudget

import (
	"context"
	"errors"
	"sync"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// AccountingSource hydrates the actual admitted session's retained native
// accounting through Store.Restore before binding. The runtime/codec owner
// supplies it from the real snapshot, never an empty-record success default.
type AccountingSource interface {
	HydrateBudget(context.Context, core.ControllerLease, core.NativeGoalObserver, *Store) error
}

type accountingPreparer interface {
	PrepareBudget(context.Context, core.ControllerLease, goal.Revision, *Store) error
}
type accountingCommitter interface {
	CommitBudget(context.Context, core.ControllerLease, *Store) error
}

// Factory binds the real goal service to an exact host writer. Its lifetime
// belongs to the host composition; Close joins only its own observation workers.
type Factory struct {
	service  *goal.Service
	store    *Store
	source   AccountingSource
	life     context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool
	bindings map[core.ControllerLease]*binding
	wg       sync.WaitGroup
}

var _ core.GoalBindingFactory = (*Factory)(nil)

func NewFactory(life context.Context, service *goal.Service, store *Store, source AccountingSource) (*Factory, error) {
	if life == nil || service == nil || store == nil {
		return nil, ErrAccountingUnavailable
	}
	ctx, cancel := context.WithCancel(life)
	return &Factory{service: service, store: store, source: source, life: ctx, cancel: cancel, bindings: map[core.ControllerLease]*binding{}}, nil
}
func (f *Factory) Close(ctx context.Context) error {
	f.mu.Lock()
	if !f.closed {
		f.closed = true
		f.cancel()
		for _, b := range f.bindings {
			b.stop()
		}
	}
	f.mu.Unlock()
	done := make(chan struct{})
	go func() { f.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (f *Factory) BindGoal(ctx context.Context, l core.ControllerLease, provider string, w core.HostInputWriter) (core.OwnerKey, core.GoalBinding, error) {
	if l.SessionID == "" || l.Epoch == 0 || missing(w) {
		return "", nil, ErrAccountingUnavailable
	}
	if provider != "muse" && provider != "codex" && provider != "claude" {
		return "", nil, core.ErrHostCapabilityUnavailable
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.life.Err() != nil {
		return "", nil, core.ErrHostClosed
	}
	if provider == "muse" {
		if capability, ok := w.(nativeGoalCapability); ok && !capability.SupportsNativeGoals() {
			return f.nonNativeMuseBinding(ctx, l, w)
		}
	}
	b := f.bindings[l]
	if b == nil {
		b = &binding{store: f.store, lease: l, provider: provider, writer: w, trace: &deliveryTrace{lease: l}}
		if provider == "muse" {
			if missing(f.source) {
				return "", nil, ErrAccountingUnavailable
			}
			observer, ok := w.(core.NativeGoalObserver)
			if !ok {
				return "", nil, core.ErrHostCapabilityUnavailable
			}
			b.observer = observer
			if p, ok := f.source.(accountingPreparer); ok {
				b.prepareBudget = p.PrepareBudget
			}
			if p, ok := f.source.(accountingCommitter); ok {
				b.commitBudget = p.CommitBudget
			}
			source, ok := w.(core.MuseObservationSource)
			if !ok {
				return "", nil, core.ErrHostCapabilityUnavailable
			}
			if e := f.source.HydrateBudget(ctx, l, observer, f.store); e != nil {
				return "", nil, e
			}
			if e := b.requireAccount(ctx); e != nil {
				return "", nil, e
			}
			watch, cancel := context.WithCancel(f.life)
			feed, e := source.SubscribeMuse(watch, l)
			if e != nil {
				cancel()
				return "", nil, e
			}
			if feed == nil {
				cancel()
				return "", nil, ErrAccountingUnavailable
			}
			b.stop = func() { cancel(); feed.Close() }
			b.done = make(chan struct{})
			// Retire only this factory's prior lease worker. The core owns the writer
			// epoch and any revocation/drain; this supplier cannot change it.
			for old, prior := range f.bindings {
				if old.SessionID == l.SessionID && old != l {
					prior.stop()
					delete(f.bindings, old)
				}
			}
			f.wg.Add(1)
			go func() { defer close(b.done); defer f.wg.Done(); defer feed.Close(); b.watch(watch, feed) }()
		} else {
			b.stop = func() {}
		}
		f.bindings[l] = b
	} else if b.retiring {
		return "", nil, core.ErrStaleInputController
	} else if b.provider != provider {
		return "", nil, ErrAccountingUnavailable
	}
	tracked := traceWriter(w, b.trace)
	out, e := f.service.NewBinding(ctx, l, goal.Assignment, provider, tracked, b)
	return goal.Owner(l.SessionID, goal.Assignment), out, e
}

type binding struct {
	retiring      bool          // protected by Factory.mu; actual observer lease tombstone
	done          chan struct{} // closed only when that native worker and feed have joined
	textOnly      bool
	life          context.Context
	mu            sync.Mutex
	store         *Store
	lease         core.ControllerLease
	provider      string
	writer        core.HostInputWriter
	observer      core.NativeGoalObserver
	trace         *deliveryTrace
	stop          func()
	prepareBudget func(context.Context, core.ControllerLease, goal.Revision, *Store) error
	commitBudget  func(context.Context, core.ControllerLease, *Store) error
}

var _ goal.BudgetBinding = (*binding)(nil)

func (b *binding) WithBudget(ctx context.Context, l core.ControllerLease, r goal.Revision, deliver func() error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if l != b.lease || r.SessionID != l.SessionID || r.Namespace != goal.Assignment || deliver == nil {
		return core.ErrStaleInputController
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	if b.textOnly && (b.life == nil || b.life.Err() != nil) {
		return core.ErrHostClosed
	}
	if r.State != goal.Active && r.State != goal.Cleared && r.State != goal.Accepted {
		return goal.ErrInvalidCommand
	}
	budget, e := SelectBudget(&r.Content.Budget, 0)
	if e != nil && r.State == goal.Active {
		return e
	}
	t := b.trace
	t.mu.Lock()
	t.input = nil
	t.receipt = nil
	t.ack = nil
	t.native = nil
	t.mu.Unlock()
	if b.provider == "muse" && !b.textOnly {
		if e = b.requireAccount(ctx); e != nil {
			return e
		}
		if b.prepareBudget != nil {
			if err := b.prepareBudget(ctx, l, r, b.store); err != nil {
				return err
			}
		}
		t.onObservation = func(o core.NativeGoalObservation) error { return b.prepare(ctx, r, budget, o) }
		t.beforeSubmit = func(i core.InputSubmission) error { return b.recordInput(ctx, r, i) }
		t.afterReceipt = func(receipt core.InputReceipt) error { return b.recordReceipt(ctx, receipt) }
	} else {
		t.onObservation = nil
		t.beforeSubmit = nil
		t.afterReceipt = nil
	}
	effectErr := deliver()
	// A real provider can stage a combined owner directive without offering any
	// input. Preserve its exact error tree; no receipt/accounting was applied.
	// Once Submit was offered, even a deferred label cannot certify no effect.
	if b.provider == "codex" && !t.possibleEffect() && errors.Is(effectErr, goal.ErrNativeDeferred) {
		return effectErr
	}
	ack, ackErr := t.checked()
	if b.provider != "muse" || b.textOnly {
		return errors.Join(effectErr, ackErr)
	}
	// An error from deliver is not a no-effect certificate. Persist the actual
	// receipt/ACK disposition even when a later journal operation failed.
	finishErr := b.finish(ctx, r, budget, ack, ackErr, effectErr, t.possibleEffect())
	if finishErr == nil && ackErr == nil {
		finishErr = b.check(ctx)
	}
	if ackErr == nil && b.commitBudget != nil {
		finishErr = errors.Join(finishErr, b.commitBudget(ctx, l, b.store))
	}
	return errors.Join(effectErr, ackErr, finishErr)
}
func (b *binding) watch(ctx context.Context, feed core.MuseObservationFeed) {
	for {
		v, e := feed.Next(ctx)
		if e != nil {
			if ctx.Err() == nil {
				_ = b.markUncertain(context.WithoutCancel(ctx))
			}
			return
		}
		if v.Lease != b.lease || v.Gap != nil {
			_ = b.markUncertain(ctx)
			return
		}
		if e = b.Check(ctx); e != nil && (errors.Is(e, core.ErrStaleInputController) || errors.Is(e, ErrAccountingUncertain) || errors.Is(e, ErrCounterRegression)) {
			return
		}
	}
}
