package sessionbudget

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type authority struct{ facts goal.VerifiedCaller }

func (a *authority) WithVerifiedCaller(_ context.Context, _ string, fn func(goal.VerifiedCaller) error) error {
	return fn(a.facts)
}

type accountingSource struct{ nativeID string }

func (s accountingSource) HydrateBudget(ctx context.Context, l core.ControllerLease, o core.NativeGoalObserver, store *Store) error {
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

type testHost struct {
	s       *goal.Service
	store   *Store
	factory *Factory
	native  *nativeFixture
	lease   core.ControllerLease
	root    string
	a       *authority
}

func setup(t *testing.T) *testHost {
	t.Helper()
	root := t.TempDir()
	a := &authority{goal.VerifiedCaller{SessionID: "s", Principal: "holder", Holder: "holder", Generation: 1, Assignment: true, UnlimitedBudget: true, Operator: true}}
	s, e := goal.Open(filepath.Join(root, "goals"), a)
	if e != nil {
		t.Fatal(e)
	}
	store, e := Open(filepath.Join(root, "budget"))
	if e != nil {
		t.Fatal(e)
	}
	l := core.ControllerLease{SessionID: "s", Epoch: 1}
	n := newNative(l)
	f, e := NewFactory(context.Background(), s, store, accountingSource{"native-s"})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := f.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	return &testHost{s, store, f, n, l, root, a}
}
func (h *testHost) binding(t *testing.T) core.GoalBinding {
	t.Helper()
	_, b, e := h.factory.BindGoal(context.Background(), h.lease, "muse", h.native)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func (h *testHost) set(t *testing.T, id string, previous int, budget goal.Budget) error {
	t.Helper()
	b := h.binding(t)
	_, e := h.s.Execute(context.Background(), goal.Command{EnvelopeID: id, SessionID: "s", Namespace: goal.Assignment, Action: goal.Set, ExpectedRevision: previous, Content: &goal.Content{Objective: "Work", Condition: "Holder accepts", Budget: budget}}, b)
	return e
}
func (h *testHost) check() error { return h.factory.bindings[h.lease].Check(context.Background()) }
func (h *testHost) account(t *testing.T) *account {
	t.Helper()
	var got *account
	e := h.store.transaction(context.Background(), false, func(j *journal) error { got = j.Sessions["s"]; return nil })
	if e != nil {
		t.Fatal(e)
	}
	return got
}

// The real goal service and core receipt checker drive the sink on a typed
// isolated observer/writer. Below-threshold and explicit-none neighbors forbid
// pause; the finite ceiling issues one matched pause and preserves its latch.
func TestBudgetFiniteUnlimitedAndThresholdPause(t *testing.T) {
	for _, unlimited := range []bool{false, true} {
		t.Run(map[bool]string{false: "finite", true: "explicit_none"}[unlimited], func(t *testing.T) {
			h := setup(t)
			budget := goal.Budget{Tokens: 10}
			if unlimited {
				budget = goal.Budget{Unlimited: true}
			}
			if e := h.set(t, "first", 0, budget); e != nil {
				t.Fatal(e)
			}
			if a := h.account(t); a.Current == nil || a.Current.Baseline != 100 {
				t.Fatalf("missing matched accounting: %+v", a)
			}
			h.native.usage(109)
			if e := h.check(); e != nil {
				t.Fatal(e)
			}
			if h.native.pauses != 0 {
				t.Fatal("budget paused below the configured ceiling")
			}
			h.native.usage(110)
			if e := h.check(); e != nil {
				t.Fatal(e)
			}
			if e := h.check(); e != nil {
				t.Fatal(e)
			}
			want := 1
			if unlimited {
				want = 0
			}
			if h.native.pauses != want {
				t.Fatalf("threshold pause count=%d want%d", h.native.pauses, want)
			}
			if !unlimited && !h.account(t).Current.BudgetPaused {
				t.Fatal("matched pause did not latch")
			}
		})
	}
}

// Wrong/lost acknowledgements retain the attempted identity and never reset
// the old baseline or advertise an applied unlimited ceiling.
func TestBudgetWrongAckNeverResetsOrApplies(t *testing.T) {
	for _, mode := range []string{"wrong_ack", "lost_ack"} {
		t.Run(mode, func(t *testing.T) {
			h := setup(t)
			if e := h.set(t, "first", 0, goal.Budget{Tokens: 10}); e != nil {
				t.Fatal(e)
			}
			before := *h.account(t).Current
			h.native.usage(105)
			h.native.wrongAck = mode == "wrong_ack"
			h.native.ackError = mode == "lost_ack"
			e := h.set(t, "second", 1, goal.Budget{Unlimited: true})
			if e == nil {
				t.Fatal("unacknowledged revision succeeded")
			}
			a := h.account(t)
			if a.Current == nil || *a.Current != before || a.Pending == nil {
				t.Fatalf("unmatched effect changed accounting or lost obligation: %+v", a)
			}
		})
	}
}

// A missing/negative/regressing cumulative observation refuses before goal
// delivery. The positive neighbor is exercised by the finite-threshold root.
func TestBudgetUnknownAndRegressedCountersHaveNoNativeEffect(t *testing.T) {
	for _, mode := range []string{"unknown", "negative", "regressed"} {
		t.Run(mode, func(t *testing.T) {
			h := setup(t)
			if e := h.set(t, "first", 0, goal.Budget{Tokens: 10}); e != nil {
				t.Fatal(e)
			}
			writes := h.native.writes
			switch mode {
			case "unknown":
				h.native.native.Cumulative = nil
			case "negative":
				h.native.usage(-1)
			case "regressed":
				h.native.usage(99)
			}
			e := h.set(t, "second", 1, goal.Budget{Tokens: 20})
			if e == nil || h.native.writes != writes {
				t.Fatalf("uncertain usage admitted input: %v writes%d/%d", e, h.native.writes, writes)
			}
			if h.account(t).Current.Baseline != 100 {
				t.Fatal("unknown usage reset accounting")
			}
		})
	}
}

// A revision bump keeps the goal's existing spend; a matched clear removes
// native accounting, while a rejected clear preserves the prior ceiling.
func TestBudgetRevisionClearAndReenable(t *testing.T) {
	h := setup(t)
	if e := h.set(t, "first", 0, goal.Budget{Tokens: 20}); e != nil {
		t.Fatal(e)
	}
	h.native.usage(108)
	if e := h.set(t, "bump", 1, goal.Budget{Tokens: 9}); e != nil {
		t.Fatal(e)
	}
	if h.account(t).Current.Baseline != 100 {
		t.Fatal("revision bump forgave spend")
	}
	h.native.clearError = true
	b := h.binding(t)
	_, e := h.s.Execute(context.Background(), goal.Command{EnvelopeID: "clear", SessionID: "s", Namespace: goal.Assignment, Action: goal.Clear, ExpectedRevision: 2}, b)
	if e == nil || h.account(t).Current == nil || h.account(t).Current.Limit != 9 {
		t.Fatalf("failed clear forgot budget: %v", e)
	}
	h.native.clearError = false
	// Reconciliation reuses the same retained exact clear attempt, never a
	// second goal command guessed from the accounting journal.
	if _, e = h.s.Reconcile(goal.Owner("s", goal.Assignment), b); e != nil {
		t.Fatal(e)
	}
	if h.account(t).Current != nil {
		t.Fatal("matched clear retained active accounting")
	}
	if e = h.set(t, "reenable", 3, goal.Budget{Tokens: 10}); e != nil {
		t.Fatal(e)
	}
	if a := h.account(t); a.Current == nil || a.Current.Baseline != 108 {
		t.Fatalf("re-enable reused forgiven spend: %+v", a)
	}
}

// Restart keeps real durable spend and pause facts; a fresh admitted epoch may
// rebind only a resolved account. Old leases and native-ID substitution write
// no input. Unresolved ACKs cannot be erased by reopening another Store.
func TestBudgetRestartAndControllerFences(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "resolved", true: "pending_ack"}[pending], func(t *testing.T) {
			h := setup(t)
			if e := h.set(t, "first", 0, goal.Budget{Tokens: 20}); e != nil {
				t.Fatal(e)
			}
			h.native.usage(105)
			if e := h.check(); e != nil {
				t.Fatal(e)
			}
			if pending {
				h.native.ackError = true
				if e := h.set(t, "second", 1, goal.Budget{Tokens: 30}); e == nil {
					t.Fatal("lost ACK passed")
				}
			}
			other, e := Open(filepath.Join(h.root, "budget"))
			if e != nil {
				t.Fatal(e)
			}
			old := h.lease
			h.native.lease.Epoch++
			h.native.native.Epoch = h.native.lease.Epoch
			writes := h.native.writes
			e = other.Restore(context.Background(), h.native.lease, "native-s", h.native)
			if pending {
				if !errors.Is(e, ErrAccountingUncertain) {
					t.Fatalf("unresolved restore = %v", e)
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				if e = other.transaction(context.Background(), false, func(j *journal) error {
					if j.Sessions["s"].Current.Baseline != 100 {
						return errors.New("baseline reset")
					}
					return nil
				}); e != nil {
					t.Fatal(e)
				}
			}
			if e = other.Restore(context.Background(), old, "native-s", h.native); !errors.Is(e, core.ErrStaleInputController) {
				t.Fatalf("old lease admitted: %v", e)
			}
			if e = other.Restore(context.Background(), h.native.lease, "other-native", h.native); e == nil {
				t.Fatal("foreign native identity admitted")
			}
			if h.native.writes != writes {
				t.Fatal("restore/fence issued native input")
			}
		})
	}
}

// Admission still owns holder/unlimited authority. Refused holder, generation
// and unlimited requests must reach neither the budget ledger nor native input.
func TestBudgetCurrentHolderAndUnlimitedAdmission(t *testing.T) {
	for _, mode := range []string{"wrong_holder", "old_holder", "unlimited_not_entitled"} {
		t.Run(mode, func(t *testing.T) {
			h := setup(t)
			b := h.binding(t)
			h.a.facts.Operator = false
			budget := goal.Budget{Tokens: 10}
			switch mode {
			case "wrong_holder":
				h.a.facts.Principal = "other"
			case "old_holder":
				h.a.facts.Holder = "new-holder"
				h.a.facts.Generation = 2
			case "unlimited_not_entitled":
				budget = goal.Budget{Unlimited: true}
				h.a.facts.UnlimitedBudget = false
			}
			_, e := h.s.Execute(context.Background(), goal.Command{EnvelopeID: "invalid", SessionID: "s", Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "Work", Condition: "Holder accepts", Budget: budget}}, b)
			if !errors.Is(e, goal.ErrForbidden) || h.native.writes != 0 || h.account(t).Current != nil {
				t.Fatalf("forbidden admission reached sink: %v", e)
			}
		})
	}
}

// The actual current fence observes a native resume and re-arms a spent goal;
// a displaced objective and an old epoch must never receive a pause.
func TestBudgetResumeRearmsAndDisplacementRefusesPause(t *testing.T) {
	h := setup(t)
	if e := h.set(t, "first", 0, goal.Budget{Tokens: 10}); e != nil {
		t.Fatal(e)
	}
	h.native.usage(110)
	if e := h.check(); e != nil {
		t.Fatal(e)
	}
	h.native.native.Status = "active"
	h.native.native.Sequence++
	if e := h.check(); e != nil {
		t.Fatal(e)
	}
	if h.native.pauses != 2 {
		t.Fatal("observed resume failed to re-arm budget pause")
	}
	h.native.native.Status = "active"
	h.native.native.Objective = "foreign objective"
	h.native.native.Sequence++
	writes := h.native.writes
	if e := h.check(); !errors.Is(e, ErrAccountingUncertain) {
		t.Fatalf("displaced objective = %v", e)
	}
	if h.native.writes != writes {
		t.Fatal("displaced objective received budget pause")
	}
	h.native.lease.Epoch++
	h.native.native.Epoch = h.native.lease.Epoch
	if e := h.check(); !errors.Is(e, core.ErrStaleInputController) {
		t.Fatalf("old lease = %v", e)
	}
	if h.native.writes != writes {
		t.Fatal("old lease issued native input")
	}
}

// Missing concrete runtime observation/hydration never becomes an empty-store
// success default. Existing native work cannot be adopted as fresh accounting.
func TestBudgetMissingHydrationAndExistingGoalFailClosed(t *testing.T) {
	for _, mode := range []string{"missing_source", "existing_native_goal", "unknown_usage"} {
		t.Run(mode, func(t *testing.T) {
			h := setup(t)
			if mode == "missing_source" {
				h.factory.source = nil
			}
			if mode == "existing_native_goal" {
				h.native.native.Objective = "already running"
				h.native.native.Status = "active"
			}
			if mode == "unknown_usage" {
				h.native.native.Cumulative = nil
			}
			_, _, e := h.factory.BindGoal(context.Background(), h.lease, "muse", h.native)
			if e == nil || h.native.writes != 0 {
				t.Fatalf("missing accounting adopted: %v", e)
			}
		})
	}
}

// A matched native effect followed by a failed core re-check can lead to an
// acknowledged but unresolved rollback. Keep its actual receipt and restored
// ceiling; neither any error nor an issued receipt implies a budget reset.
func TestBudgetAcknowledgedButUnresolvedRollbackPreservesFacts(t *testing.T) {
	h := setup(t)
	if e := h.set(t, "first", 0, goal.Budget{Tokens: 20}); e != nil {
		t.Fatal(e)
	}
	h.native.usage(105)
	h.native.failReceiptCheck = true
	if e := h.set(t, "second", 1, goal.Budget{Unlimited: true}); e == nil {
		t.Fatal("interrupted core check passed")
	}
	a := h.account(t)
	if a.Recovery == nil || a.Recovery.Acknowledged == nil || a.Current == nil || a.Current.Baseline != 100 || a.Current.Limit != 0 {
		t.Fatalf("matched native effect was not retained as unresolved: current=%+v recovery=%+v", a.Current, a.Recovery)
	}
	head, e := h.s.Snapshot("s", goal.Assignment)
	if e != nil {
		t.Fatal(e)
	}
	if head.Applied == nil || *head.Applied == head.Latest.Ref {
		t.Fatal("unresolved goal was marked applied or prior receipt lost")
	}
	// This kind of failed core re-check is not the goal service's acknowledged
	// journal-error branch, so rollback must be driven explicitly, not assumed.
	b := h.binding(t)
	if e = b.Rollback(&core.GoalEnvelope{Owner: goal.Owner("s", goal.Assignment), Ref: head.Applied}); e == nil {
		t.Fatal("interrupted rollback core check passed")
	}
	a = h.account(t)
	if a.Recovery == nil || a.Recovery.Acknowledged == nil || a.Current == nil || a.Current.Baseline != 100 || a.Current.Limit != 20 {
		t.Fatalf("actual acknowledged rollback facts lost: current=%+v recovery=%+v", a.Current, a.Recovery)
	}
}

// An unacknowledged pause is not a budget-pause attestation. A later paused
// native observation is provenance only, and must not erase the pending input.
func TestBudgetPauseWrongAckDoesNotLatchOrForget(t *testing.T) {
	h := setup(t)
	if e := h.set(t, "first", 0, goal.Budget{Tokens: 10}); e != nil {
		t.Fatal(e)
	}
	h.native.wrongAck = true
	h.native.usage(110)
	if e := h.check(); e == nil {
		t.Fatal("wrong pause ACK passed")
	}
	a := h.account(t)
	if a.Current.BudgetPaused || a.Pending == nil || a.Pending.Kind != "pause" {
		t.Fatal("wrong pause ACK latched or was forgotten")
	}
	if e := h.check(); e != nil {
		t.Fatal(e)
	}
	a = h.account(t)
	if a.Current.BudgetPaused || !a.Current.ExternalPause || a.Pending == nil || h.native.pauses != 1 {
		t.Fatal("observed pause forged host attestation or duplicated effect")
	}
}

// Concurrent core binding requests reuse one exact-lease supplier and worker;
// closing it joins that worker and permanently refuses later bindings.
func TestBudgetConcurrentBindingAndClose(t *testing.T) {
	h := setup(t)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, e := h.factory.BindGoal(context.Background(), h.lease, "muse", h.native)
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if len(h.factory.bindings) != 1 {
		t.Fatal("duplicate lease workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := h.factory.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if _, _, e := h.factory.BindGoal(context.Background(), h.lease, "muse", h.native); !errors.Is(e, core.ErrHostClosed) {
		t.Fatalf("closed budget supplier rebound: %v", e)
	}
	if h.native.writes != 0 {
		t.Fatal("binding/close emitted native input")
	}
}

// A counter reset taints durable accounting even when the refused operation
// itself writes no input. Later catch-up and a new controller cannot forgive
// the lost counter epoch and silently undercharge that goal.
func TestBudgetCounterRegressionCannotRecoverByCatchup(t *testing.T) {
	h := setup(t)
	if e := h.set(t, "first", 0, goal.Budget{Tokens: 20}); e != nil {
		t.Fatal(e)
	}
	h.native.usage(99)
	writes := h.native.writes
	if e := h.check(); !errors.Is(e, ErrCounterRegression) {
		t.Fatalf("regression = %v", e)
	}
	if !h.account(t).Uncertain {
		t.Fatal("counter regression taint was not durably retained")
	}
	h.native.usage(125)
	if e := h.check(); !errors.Is(e, ErrAccountingUncertain) {
		t.Fatalf("counter catch-up resumed accounting: %v", e)
	}
	other, e := Open(filepath.Join(h.root, "budget"))
	if e != nil {
		t.Fatal(e)
	}
	h.native.lease.Epoch++
	h.native.native.Epoch = h.native.lease.Epoch
	if e = other.Restore(context.Background(), h.native.lease, "native-s", h.native); !errors.Is(e, ErrAccountingUncertain) {
		t.Fatalf("restart forgave counter reset: %v", e)
	}
	if h.native.writes != writes {
		t.Fatal("tainted counter epoch issued a native effect")
	}
}
