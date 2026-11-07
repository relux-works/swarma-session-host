package sessionhost

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/relux-works/swarma-session-host/internal/runner"
)

// Private composition, deliberately not installed by any public entry point.
// Policy selection belongs to composition; absent policy always refuses.
// Neither expected wire fields nor executable identity is policy.
type createPolicy interface {
	checkCreate(context.Context, *ordinaryWireOperation) error
	close()
}
type unselectedCreatePolicy struct{}

func (unselectedCreatePolicy) checkCreate(context.Context, *ordinaryWireOperation) error {
	return errOrdinaryWireUnavailable
}
func (unselectedCreatePolicy) close() {}

// createOutcome is a private observation of retained custody, never a wire
// result, native success, or permission to repeat an effect.
type createOutcome string

const (
	createRefused         createOutcome = "refused"
	createStartingUnknown createOutcome = "starting_unknown"
	createStartUnknown    createOutcome = "start_unknown"
	createRunnerOwned     createOutcome = "runner_owned"
	createContinuingOwned createOutcome = "continuing_owned"
	createCleanupPending  createOutcome = "cleanup_pending"
	createClosed          createOutcome = "closed"
	createCleanupUnknown  createOutcome = "cleanup_unknown"
)

// Err-only callers must also fail closed on duplicate terminal observations.
// Unwrap preserves the existing unavailable classification; no wire code is added.
type createObservationError struct{ outcome createOutcome }

func (e *createObservationError) Error() string {
	return "private create observation: " + string(e.outcome)
}
func (e *createObservationError) Unwrap() error { return errOrdinaryWireUnavailable }

func (o createOutcome) observation() (createOutcome, error) {
	switch o {
	case createRunnerOwned, createContinuingOwned:
		return o, nil
	default:
		return o, &createObservationError{outcome: o}
	}
}

type createRunner interface{ Close() error }
type ordinaryCreateHandler struct {
	mu          sync.Mutex
	wire        *ordinaryWireManager
	socket      string
	timeout     time.Duration
	policy      createPolicy
	start       func(runner.Config) (createRunner, error) // private qualification seam
	operation   *ordinaryWireOperation
	child       createRunner
	outcome     createOutcome
	closed      bool
	transferred *nativeCorrelation // cleanup duty moves only after acknowledgment
	done        chan struct{}
}

func openOrdinaryCreateHandler(parent context.Context, expires time.Time, records string, config ordinaryLauncherConfig, socket string, timeout time.Duration, policy createPolicy) (*ordinaryCreateHandler, error) {
	wire, err := openOrdinaryWireManager(parent, expires, records, config)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		policy = unselectedCreatePolicy{}
	}
	h := &ordinaryCreateHandler{wire: wire, socket: socket, timeout: timeout,
		policy: policy, start: startCreateRunner, done: make(chan struct{})}
	go h.watch()
	return h, nil
}

func startCreateRunner(cfg runner.Config) (createRunner, error) {
	child, err := runner.Start(cfg)
	if child == nil {
		return nil, err
	}
	return child, err
}

// create returns a private in-memory observation, never native success or an
// admission receipt. The same exact request observes pending/terminal state;
// no failed or ambiguous effect is replayed. All calls serialize with cleanup.
func (h *ordinaryCreateHandler) create(ctx context.Context, method string, body []byte) (createOutcome, error) {
	if h == nil || ctx == nil || ctx.Err() != nil {
		return createRefused.observation()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return h.outcome, &createObservationError{outcome: h.outcome}
	}
	if prior := h.operation; prior != nil {
		if h.wire.owner.ctx.Err() != nil {
			return createCleanupPending.observation()
		}
		if prior.claim.kind != method || !bytes.Equal(prior.claim.data, body) {
			return createRefused.observation()
		}
		return h.outcome.observation()
	}
	op, err := h.wire.dispatch(ctx, method, body)
	if err != nil {
		return createRefused, err
	}
	h.operation, h.outcome = op, createRefused
	// No fallback on nil or unknown. Retain the terminal operation even when
	// admission refuses; policy configuration never comes from this request.
	if method != "launch" || h.policy == nil || h.policy.checkCreate(ctx, op) != nil {
		return h.outcome.observation()
	}
	cfg, err := op.takeRunnerConfig(ctx, h.wire, h.socket, h.timeout)
	if err != nil {
		return h.outcome.observation()
	}
	// Consume first, then observe all current retained physical conjuncts and
	// policy again. Holding these locks excludes local cleanup, not FS mutation.
	m := h.wire
	m.mu.Lock()
	m.owner.mu.Lock()
	valid := !m.closed && !m.owner.closed && m.owner.ctx.Err() == nil && ctx.Err() == nil &&
		op.claim.retained.recheckPhysical(m.owner.records, op.claim.kind, op.claim.data) == nil &&
		m.launcher.recheck(ctx) == nil
	m.owner.mu.Unlock()
	m.mu.Unlock()
	if !valid || h.policy.checkCreate(ctx, op) != nil || ctx.Err() != nil || m.owner.ctx.Err() != nil {
		return h.outcome.observation()
	}
	// This linearization precedes the only possible effect. Request loss after
	// this point cannot discard a possible child. The owner watcher joins Start.
	h.outcome = createStartingUnknown
	child, err := h.start(cfg)
	h.child = child // retain even an unusual nonnil child plus error
	if err != nil || child == nil {
		h.outcome = createStartUnknown
		return h.outcome.observation()
	}
	h.outcome = createRunnerOwned
	return h.outcome.observation()
}

// These are the exact retained handles borrowed under T1 custody before the
// private T2 transfer. Returning them grants nothing and never releases duty.
type createCustody struct {
	operation *ordinaryWireOperation
	claim     *preChildOperation
	retained  *retainedMemberOperation
	source    *memberRecordSource
	server    *runner.Server
}

func (h *ordinaryCreateHandler) custody() (createCustody, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.outcome != createRunnerOwned || h.wire.owner.ctx.Err() != nil {
		return createCustody{}, errOrdinaryWireUnavailable
	}
	server, ok := h.child.(*runner.Server)
	if !ok || server == nil {
		return createCustody{}, errOrdinaryWireUnavailable
	}
	op := h.operation
	return createCustody{op, op.claim, op.claim.retained, h.wire.owner.records, server}, nil
}

func (h *ordinaryCreateHandler) watch() {
	<-h.wire.owner.ctx.Done()
	h.mu.Lock()
	h.closed, h.outcome = true, createCleanupPending
	if c := h.transferred; c != nil {
		h.mu.Unlock()
		<-c.done
		h.mu.Lock()
		h.closed = true
		h.outcome = createClosed
		if c.closeErr != nil {
			h.outcome = createCleanupUnknown
		}
		h.mu.Unlock()
		close(h.done)
		return
	}
	child, policy := h.child, h.policy
	h.mu.Unlock()
	cleanupUnknown := child != nil && child.Close() != nil
	if policy != nil {
		policy.close()
	}
	cleanupUnknown = h.wire.close(context.Background()) != nil || cleanupUnknown
	h.mu.Lock()
	h.outcome = createClosed
	if cleanupUnknown {
		h.outcome = createCleanupUnknown
	}
	h.mu.Unlock()
	close(h.done)
}

// A canceled waiter never cancels or abandons cleanup of the actual runner.
func (h *ordinaryCreateHandler) close(ctx context.Context) error {
	if h == nil || ctx == nil {
		return errOrdinaryWireUnavailable
	}
	// Cancellation must not wait for create's lock: Start can still be in
	// flight. The serialized watcher chooses T1 cleanup or joins acknowledged
	// T2 cleanup after Start/transfer releases that lock.
	h.wire.owner.cancel()
	select {
	case <-h.done:
		return h.closeResult()
	default:
	}
	select {
	case <-h.done:
		return h.closeResult()
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (h *ordinaryCreateHandler) closeResult() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.outcome == createCleanupUnknown {
		return errOrdinaryWireUnavailable
	}
	return nil
}
