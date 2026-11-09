package sessionhost

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/runner"
)

// Refusals and possible Starts remain terminal observations, including for
// err-only callers. Repairing policy or changing Start cannot replay them.
func TestCreateTerminalTypedObservation(t *testing.T) {
	for _, mode := range []string{"policy-refused", "failed-start", "possible-child"} {
		t.Run(mode, func(t *testing.T) {
			h, body, child, starts := createFixture(t)
			want, wantStarts := createRefused, 0
			if mode == "policy-refused" {
				h.policy = unselectedCreatePolicy{}
			} else {
				want, wantStarts = createStartUnknown, 1
				h.start = func(runner.Config) (createRunner, error) {
					*starts++
					if mode == "possible-child" {
						return child, errors.New("synthetic possible child")
					}
					return nil, errors.New("synthetic lost start result")
				}
			}
			check := func() {
				got, err := h.create(context.Background(), "launch", body)
				var observation *createObservationError
				if got != want || !errors.Is(err, errOrdinaryWireUnavailable) || !errors.As(err, &observation) || observation.outcome != want {
					t.Errorf("terminal observation = %v, %v", got, err)
				}
			}
			check()
			h.policy = &inertCreatePolicy{} // a repair is not permission to retry
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(check)
			}
			wg.Wait()
			if *starts != wantStarts {
				t.Fatal("terminal observation replayed Start", *starts)
			}
			if mode == "possible-child" {
				child.err = errors.New("synthetic cleanup remains unknown")
			}
			closeErr := h.close(context.Background())
			got, err := h.create(context.Background(), "launch", body)
			wantClosed := createClosed
			if mode == "possible-child" {
				wantClosed = createCleanupUnknown
				if closeErr == nil || child.closes != 1 {
					t.Fatal("possible child cleanup lost", closeErr, child.closes)
				}
			} else if closeErr != nil || child.closes != 0 {
				t.Fatal("refusal fabricated child cleanup", closeErr, child.closes)
			}
			var observation *createObservationError
			if got != wantClosed || !errors.As(err, &observation) || observation.outcome != wantClosed {
				t.Fatal("terminal cleanup observation", got, err)
			}
		})
	}
}

func cleanupAwait(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal(what)
	}
}

// Every caller has a bounded observation. None may report success while the
// deliberately context-ignoring provider still owns its unresolved work.
func cleanupBoundedCloses(t *testing.T, closeFn func(context.Context) error) {
	t.Helper()
	results := make(chan error, 8)
	for i := range 8 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			if i%2 == 0 {
				cancel()
			}
			results <- closeFn(ctx)
		}()
	}
	for range 8 {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unfinished cleanup reported %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Close ignored caller context")
		}
	}
}

// Prepare ignores ctx until the test releases it. The manager must preserve
// the late prepared result, join it, and never retry that failed operation.
func TestManagerCloseIgnoringPrepare(t *testing.T) {
	h, owner, body, selected, fragment := t4CreateFixture(t)
	entered, release, cleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	releaseProvider := func() { unblock.Do(func() { close(release) }) }
	var calls atomic.Int32
	broker := bridgeBrokerFixture{prepare: func(_ context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
		calls.Add(1)
		close(entered)
		<-release
		return &bridgePreparedFixture{origin: origin, close: func() error { close(cleaned); return nil }}, nil
	}}
	m := t4Manager(t, h, owner, selected, fragment, broker)
	t.Cleanup(releaseProvider)
	prepareResult := make(chan error, 1)
	go func() { _, err := m.Prepare(context.Background(), "launch", body); prepareResult <- err }()
	cleanupAwait(t, entered, "Prepare never entered")
	cleanupBoundedCloses(t, m.Close)
	// T2/T1 and the actual runner complete independently of this provider.
	cleanupAwait(t, m.createCorrelation.done, "provider blocked retained runner cleanup")
	cleanupAwait(t, h.done, "T1 watcher did not join actual runner")
	select {
	case <-m.closeDone:
		t.Fatal("manager claimed joined cleanup before late Prepare")
	default:
	}
	releaseProvider()
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	cleanupAwait(t, cleaned, "late prepared result was abandoned")
	if err := <-prepareResult; !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("late Prepare became success", err)
	}
	if a, err := m.Prepare(context.Background(), "launch", body); a != nil || err == nil || calls.Load() != 1 {
		t.Fatal("closed/refused operation repeated provider work", err, calls.Load())
	}
}

// Bootstrap has already consumed the endpoint, then ignores cancellation.
// Releasing Bootstrap is insufficient: its late completion must actually join.
func TestManagerCloseIgnoringBootstrapAndCompletion(t *testing.T) {
	h, owner, body, selected, fragment := t4CreateFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	closeEntered, closeRelease := make(chan struct{}), make(chan struct{})
	var releaseOnce, closeReleaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }); closeReleaseOnce.Do(func() { close(closeRelease) }) }
	var completion *heldCleanupCompletion
	var bootstraps, preparedCloses atomic.Int32
	broker := bridgeBrokerFixture{prepare: func(_ context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
		return &bridgePreparedFixture{origin: origin, bootstrap: func(ctx context.Context, b NotificationBootstrap) (NotificationCompletion, error) {
			bootstraps.Add(1)
			cctx, cancel := context.WithCancel(context.Background())
			completion = &heldCleanupCompletion{bridgeCompletionFixture: bridgeCompletionFixture{ctx: cctx, cancel: cancel, endpoint: b.MethodEndpoint}, entered: closeEntered, release: closeRelease}
			close(entered)
			<-release
			return completion, nil // even this late "success" must be refused
		}, close: func() error { preparedCloses.Add(1); return nil }}, nil
	}}
	m := t4Manager(t, h, owner, selected, fragment, broker)
	t.Cleanup(unblock)
	a, err := m.Prepare(context.Background(), "launch", body)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapResult := make(chan error, 1)
	go func() { bootstrapResult <- a.Bootstrap(context.Background()) }()
	cleanupAwait(t, entered, "Bootstrap never entered")
	cleanupBoundedCloses(t, m.Close)
	cleanupAwait(t, h.done, "provider Bootstrap blocked actual runner cleanup")
	select {
	case <-m.closeDone:
		t.Fatal("manager claimed Bootstrap joined")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	cleanupAwait(t, closeEntered, "late completion cleanup never ran")
	cleanupBoundedCloses(t, m.Close)
	select {
	case <-m.closeDone:
		t.Fatal("manager abandoned context-ignoring completion")
	default:
	}
	unblock()
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-bootstrapResult; !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal("late Bootstrap became success", err)
	}
	cleanupAwait(t, completion.Done(), "consumed endpoint completion not closed")
	if err := a.Bootstrap(context.Background()); err == nil || bootstraps.Load() != 1 || preparedCloses.Load() != 1 {
		t.Fatal("terminal Bootstrap repeated transfer", err, bootstraps.Load(), preparedCloses.Load())
	}
}

type heldCleanupCompletion struct {
	bridgeCompletionFixture
	entered, release chan struct{}
	holdOnce         sync.Once
}

func (c *heldCleanupCompletion) Close() error {
	c.holdOnce.Do(func() { close(c.entered); <-c.release })
	return c.bridgeCompletionFixture.Close()
}

// The real Host close is held behind its own mutex. T2 must still start the
// independently owned runner/wire cleanup and retain its own unfinished join.
func TestNativeCorrelationCloseStuckHost(t *testing.T) {
	h, owner, _, _, _ := t4CreateFixture(t)
	c, err := h.transferTo(owner)
	if err != nil {
		t.Fatal(err)
	}
	owner.host.mu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(owner.host.mu.Unlock) }
	t.Cleanup(unlock)
	cleanupBoundedCloses(t, c.close)
	cleanupAwait(t, h.wire.done, "stuck Host blocked wire cleanup")
	runnerDone := make(chan struct{})
	go func() {
		if err := c.server.Close(); err != nil {
			t.Error("actual runner did not join", err)
		}
		close(runnerDone)
	}()
	cleanupAwait(t, runnerDone, "stuck Host blocked actual runner join")
	select {
	case <-c.done:
		t.Fatal("T2 claimed Host joined while Host was stuck")
	default:
	}
	unlock()
	if err := c.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	cleanupAwait(t, h.done, "T1 watcher abandoned transferred cleanup")
}

// Cleanup failure is retained even if an attachment retired before manager
// Close, or the provider was acquired by a failed unpublished preparation.
func TestManagerCloseRetainsCleanupError(t *testing.T) {
	for _, mode := range []string{"unpublished", "retired-attachment"} {
		t.Run(mode, func(t *testing.T) {
			m, q, _ := managerBridgeFixture(t)
			unknown := errors.New("synthetic provider cleanup unknown")
			var closes atomic.Int32
			m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
				p := &bridgePreparedFixture{origin: origin, close: func() error { closes.Add(1); return unknown }}
				if mode == "unpublished" {
					return p, errors.New("synthetic refusal after acquisition")
				}
				return p, nil
			}}
			a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
			if mode == "unpublished" {
				if a != nil || err == nil {
					t.Fatal("unpublished refusal became success")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err = a.Close(); !errors.Is(err, unknown) {
					t.Fatal("attachment hid cleanup error", err)
				}
			}
			for range 2 {
				if err = m.Close(context.Background()); !errors.Is(err, unknown) || closes.Load() != 1 {
					t.Fatal("manager hid cleanup error or repeated cleanup", err, closes.Load())
				}
			}
		})
	}
}

type heldCreateCleanup struct {
	entered, release chan struct{}
	closes           atomic.Int32
}

func (c *heldCreateCleanup) Close() error {
	c.closes.Add(1)
	close(c.entered)
	<-c.release
	return nil
}

// No terminal 'closed' observation may precede the actual join. A stuck child
// close also cannot block context-bounded closes or create-status observations.
func TestCreateCleanupPendingObservation(t *testing.T) {
	h, body, _, starts := createFixture(t)
	child := &heldCreateCleanup{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(child.release) }) }
	t.Cleanup(unblock)
	h.start = func(runner.Config) (createRunner, error) { *starts++; return child, nil }
	if got, err := h.create(context.Background(), "launch", body); got != createRunnerOwned || err != nil {
		t.Fatal(got, err)
	}
	cleanupBoundedCloses(t, h.close)
	cleanupAwait(t, child.entered, "child cleanup never started")
	observationDone := make(chan struct{})
	go func() {
		got, err := h.create(context.Background(), "launch", body)
		var observation *createObservationError
		if got != createCleanupPending || !errors.As(err, &observation) || observation.outcome != got {
			t.Error("unfinished cleanup reported success", got, err)
		}
		close(observationDone)
	}()
	cleanupAwait(t, observationDone, "pending observation blocked on child cleanup")
	select {
	case <-h.done:
		t.Fatal("create claimed cleanup joined")
	default:
	}
	unblock()
	if err := h.close(context.Background()); err != nil || child.closes.Load() != 1 || *starts != 1 {
		t.Fatal("actual cleanup not joined once", err, child.closes.Load(), *starts)
	}
}

// A provider returning from Close without closing Done has not certified
// completion. The retained manager join must await that actual terminal signal.
func TestManagerCloseWaitsCompletionDone(t *testing.T) {
	m, q, _ := managerBridgeFixture(t)
	m.owner.host.mu.Lock()
	m.owner.host.state.Protocol.ObservedRelease = "0.167.0"
	m.owner.host.mu.Unlock()
	called, preparedClosed := make(chan struct{}), make(chan struct{})
	var completion *signalCleanupCompletion
	m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
		return &bridgePreparedFixture{origin: origin, bootstrap: func(ctx context.Context, b NotificationBootstrap) (NotificationCompletion, error) {
			cctx, cancel := context.WithCancel(context.Background())
			completion = &signalCleanupCompletion{bridgeCompletionFixture: bridgeCompletionFixture{ctx: cctx, cancel: cancel, endpoint: b.MethodEndpoint}, called: called}
			binding := b.Binding
			binding.ConsumerGeneration, binding.EnrollmentRef = "fixture-generation", "fixture-enrollment"
			return completion, b.OnEnrolled(ctx, bridgeEnrollmentFixture{binding, b.AttachmentReference, cctx, time.Now().Add(10 * time.Second)})
		}, close: func() error { close(preparedClosed); return nil }}, nil
	}}
	a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = completion.bridgeCompletionFixture.Close() })
	cleanupBoundedCloses(t, m.Close)
	cleanupAwait(t, called, "completion Close never started")
	cleanupAwait(t, preparedClosed, "completion wait blocked Prepared.Close")
	select {
	case <-m.closeDone:
		t.Fatal("Close return substituted for completion Done")
	default:
	}
	_ = completion.bridgeCompletionFixture.Close()
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type signalCleanupCompletion struct {
	bridgeCompletionFixture
	called chan struct{}
	once   sync.Once
}

func (c *signalCleanupCompletion) Close() error {
	c.once.Do(func() { close(c.called) })
	return nil
}
