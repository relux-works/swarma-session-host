package sessionrun

import (
	"context"
	"errors"
	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"io"
	"sync"
	"time"
)

// Selected existing durable-budget test fixtures: typed scoped native I/O only.
// Production goal service, receipt checker and budget binding remain real.
type authority struct{ facts goal.VerifiedCaller }

func (a *authority) WithVerifiedCaller(_ context.Context, _ string, fn func(goal.VerifiedCaller) error) error {
	return fn(a.facts)
}

type accountingSource struct{ nativeID string }

func (s accountingSource) HydrateBudget(ctx context.Context, l core.ControllerLease, o core.NativeGoalObserver, store *budget.Store) error {
	return store.Restore(ctx, l, s.nativeID, o)
}

type feed struct {
	closed chan struct{}
	once   sync.Once
}

func (f *feed) Next(ctx context.Context) (core.MuseObservation, error) {
	select {
	case <-ctx.Done():
		return core.MuseObservation{}, ctx.Err()
	case <-f.closed:
		return core.MuseObservation{}, io.EOF
	}
}
func (f *feed) Close() { f.once.Do(func() { close(f.closed) }) }

type nativeFixture struct {
	mu               sync.Mutex
	lease            core.ControllerLease
	native           core.NativeGoalObservation
	inputs           map[string]core.InputSubmission
	acks             map[core.InputReceipt]core.InputObservation
	writes           int
	pauses           int
	scope            bool
	wrongAck         bool
	ackError         bool
	clearError       bool
	failReceiptCheck bool
	awaits           map[core.InputReceipt]int
}
type scopedFixture struct{ f *nativeFixture }

func newNative(l core.ControllerLease) *nativeFixture {
	return &nativeFixture{lease: l, native: core.NativeGoalObservation{SessionID: l.SessionID, Epoch: l.Epoch, Sequence: 1, ObservedAt: time.Unix(1, 0), Cumulative: &core.MuseRestoreUsage{TotalTokens: 100, PromptTokens: 50, OutputTokens: 50}}, inputs: map[string]core.InputSubmission{}, acks: map[core.InputReceipt]core.InputObservation{}, awaits: map[core.InputReceipt]int{}}
}
func (f *nativeFixture) WithNativeGoal(_ context.Context, l core.ControllerLease, fn func(core.NativeGoalObservation, core.HostInputWriter) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l != f.lease {
		return core.ErrStaleInputController
	}
	if f.scope {
		return errors.New("nested writer fence")
	}
	f.scope = true
	defer func() { f.scope = false }()
	o := f.native
	if o.Cumulative != nil {
		copy := *o.Cumulative
		o.Cumulative = &copy
	}
	return fn(o, scopedFixture{f})
}
func (f *nativeFixture) Submit(ctx context.Context, i core.InputSubmission) (core.InputReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.submit(i)
}
func (f *nativeFixture) submit(i core.InputSubmission) (core.InputReceipt, error) {
	if i.Lease != f.lease {
		return core.InputReceipt{}, core.ErrStaleInputController
	}
	r := core.InputReceipt{SessionID: i.Lease.SessionID, Epoch: i.Lease.Epoch, ID: i.ID}
	if _, ok := f.inputs[i.ID]; ok {
		return r, nil
	}
	c := i.Payload.MuseGoal
	if c == nil {
		return core.InputReceipt{}, core.ErrInvalidInput
	}
	if c.Clear != nil && f.clearError {
		return core.InputReceipt{}, errors.New("native clear rejected")
	}
	f.inputs[i.ID] = i
	f.writes++
	switch {
	case c.Set != nil:
		f.native.Objective = c.Set.Objective
		f.native.Status = "active"
	case c.Edit != nil:
		f.native.Objective = c.Edit.Objective
		f.native.Status = "active"
	case c.Clear != nil:
		f.native.Objective = ""
		f.native.Status = ""
	case c.Pause != nil:
		if c.Pause.Sequence != f.native.Sequence {
			return r, core.ErrNativeGoalChanged
		}
		f.pauses++
		f.native.Status = "paused"
	}
	f.native.Sequence++
	f.native.ObservedAt = f.native.ObservedAt.Add(time.Second)
	f.acks[r] = core.InputObservation{Receipt: r, Kind: core.InputObservationKind(i.Expected.Kind), Correlation: i.Expected.Correlation, ObservedAt: f.native.ObservedAt}
	return r, nil
}
func (f *nativeFixture) Await(_ context.Context, r core.InputReceipt) (core.InputObservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.await(r)
}
func (f *nativeFixture) await(r core.InputReceipt) (core.InputObservation, error) {
	f.awaits[r]++
	if f.failReceiptCheck && f.awaits[r] > 1 {
		return core.InputObservation{}, errors.New("receipt check interrupted after matched ACK")
	}
	if f.ackError {
		return core.InputObservation{}, errors.New("ACK lost")
	}
	a, ok := f.acks[r]
	if !ok {
		return a, core.ErrInputReceiptUnknown
	}
	if f.wrongAck {
		a.Correlation = "foreign"
	}
	return a, nil
}
func (*nativeFixture) Cancel(context.Context, core.InputReceipt) error { return nil }
func (f *nativeFixture) SubscribeMuse(context.Context, core.ControllerLease) (core.MuseObservationFeed, error) {
	return &feed{closed: make(chan struct{})}, nil
}
func (w scopedFixture) Submit(_ context.Context, i core.InputSubmission) (core.InputReceipt, error) {
	return w.f.submit(i)
}
func (w scopedFixture) Await(_ context.Context, r core.InputReceipt) (core.InputObservation, error) {
	return w.f.await(r)
}
func (w scopedFixture) Cancel(context.Context, core.InputReceipt) error { return nil }
func (f *nativeFixture) usage(total int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.native.Cumulative = &core.MuseRestoreUsage{TotalTokens: total, PromptTokens: total / 2, OutputTokens: total - total/2}
	f.native.Sequence++
	f.native.ObservedAt = f.native.ObservedAt.Add(time.Second)
}
