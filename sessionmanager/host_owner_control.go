package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

const ownerActivationLeaseTTL = 90 * time.Second

type hostOwnerActivation struct {
	leaseID string
	expires time.Time
	done    chan struct{}
}
type hostOwnerOperation struct{ done chan struct{} }

// OwnerGoalBinding is actual host session/controller evidence plus the existing
// receipt-bound service binding. It is callback-scoped, not caller authorization.
type OwnerGoalBinding struct {
	reconcile func() (*GoalActivation, error)
	Session   SessionView
	Lease     ControllerLease
	Owner     OwnerKey
	Binding   GoalBinding
}

// Reconcile retains the real service binding's concrete identity while the
// callback scope joins/invalidates it. No unwrapped binding escapes to callers.
func (b OwnerGoalBinding) Reconcile() (*GoalActivation, error) {
	if b.reconcile == nil {
		return nil, ErrHostCapabilityUnavailable
	}
	return b.reconcile()
}
func (h *SessionHost) beginOwnerOperationLocked(owner OwnerKey) *hostOwnerOperation {
	if h.ownerOperations == nil {
		h.ownerOperations = map[OwnerKey]*hostOwnerOperation{}
	}
	op := &hostOwnerOperation{done: make(chan struct{})}
	h.ownerOperations[owner] = op
	return op
}
func (h *SessionHost) endOwnerOperationLocked(owner OwnerKey, op *hostOwnerOperation) {
	if h.ownerOperations[owner] == op {
		delete(h.ownerOperations, owner)
		close(op.done)
	}
}
func (h *SessionHost) expireOwnerActivationLocked(owner OwnerKey, a *hostOwnerActivation) {
	if h.ownerActivations[owner] == a {
		delete(h.ownerActivations, owner)
		close(a.done)
	}
}

// Returns with the map lock held on success, unlocked on cancellation. Waiting
// is owner-scoped; unrelated owners do not inherit a long native acknowledgement.
func (h *SessionHost) lockOwnerOperation(ctx context.Context, owner OwnerKey, activation bool) error {
	for {
		if e := h.lockContext(ctx); e != nil {
			return e
		}
		var done <-chan struct{}
		var timer *time.Timer
		if op := h.ownerOperations[owner]; op != nil {
			done = op.done
		} else if a := h.ownerActivations[owner]; activation && a != nil {
			wait := time.Until(a.expires)
			if wait <= 0 {
				h.expireOwnerActivationLocked(owner, a)
				return nil
			}
			done = a.done
			timer = time.NewTimer(wait)
		} else {
			return nil
		}
		h.mu.Unlock()
		if timer == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-h.life.Done():
				return ErrHostClosed
			case <-done:
			}
		} else {
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-h.life.Done():
				timer.Stop()
				return ErrHostClosed
			case <-done:
				timer.Stop()
			case <-timer.C:
			}
		}
	}
}

func (h *SessionHost) liveOwnerLocked(ctx context.Context, owner OwnerKey) (RunnerRecord, RunnerHost, bool, error) {
	var found RunnerRecord
	for _, r := range h.sessions {
		if r.Owner == owner && r.State != SessionStopped {
			if found.SessionID != "" {
				return RunnerRecord{}, nil, false, ErrLegacyCommitUncertain
			}
			found = r
		}
	}
	if found.SessionID == "" {
		return RunnerRecord{}, nil, false, nil
	}
	if found.ConditionalStop != nil || found.GracefulRetirement || h.records.uncertain[found.SessionID] {
		return RunnerRecord{}, nil, false, ErrLegacyCommitUncertain
	}
	runner := h.runners[found.SessionID]
	if absentHostPort(runner) {
		return found, nil, false, nil
	}
	state, e := runner.State(ctx)
	if e != nil {
		return RunnerRecord{}, nil, false, e
	}
	return found, runner, state == ProviderHostRunning, nil
}

// BeginOwnerActivation retains the original physical90s decision boundary.
// The trusted caller supplies an already verified routing key; this method does
// not create a grant from a body owner string, PID or shared OS identity.
func (h *SessionHost) BeginOwnerActivation(ctx context.Context, owner OwnerKey, expected ControllerLease) (string, bool, error) {
	if strings.TrimSpace(string(owner)) == "" {
		return "", false, ErrInvalidInput
	}
	if e := h.lockOwnerActivation(ctx, owner); e != nil {
		return "", false, e
	}
	defer h.mu.Unlock()
	if a := h.ownerActivations[owner]; a != nil {
		if time.Now().Before(a.expires) {
			return "", false, &PublicError{Code: "owner_activation_in_progress", Message: "another goal activation is in progress for this owner; retry shortly"}
		}
		h.expireOwnerActivationLocked(owner, a)
	}
	if h.attachmentReserved(owner, "") {
		return "", false, ErrLegacyCommitUncertain
	}
	record, _, live, e := h.liveOwnerLocked(ctx, owner)
	if e != nil {
		return "", false, e
	}
	if live {
		input := h.inputs[record.SessionID]
		if input == nil {
			return "", false, ErrStaleInputController
		}
		current, err := input.CurrentLease(ctx, record.SessionID)
		if err != nil {
			return "", false, err
		}
		if current != expected {
			return "", false, ErrStaleInputController
		}
	} else if expected != (ControllerLease{}) {
		return "", false, ErrStaleInputController
	}
	id, e := opaqueID("ACT")
	if e != nil {
		return "", false, e
	}
	if h.ownerActivations == nil {
		h.ownerActivations = map[OwnerKey]*hostOwnerActivation{}
	}
	h.ownerActivations[owner] = &hostOwnerActivation{leaseID: id, expires: time.Now().Add(ownerActivationLeaseTTL), done: make(chan struct{})}
	return id, live, nil
}
func (h *SessionHost) EndOwnerActivation(owner OwnerKey, leaseID string) error {
	if h == nil {
		return ErrHostClosed
	}
	if strings.TrimSpace(string(owner)) == "" {
		return ErrInvalidInput
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if a := h.ownerActivations[owner]; a != nil && a.leaseID == leaseID {
		h.expireOwnerActivationLocked(owner, a)
	}
	return nil
}

// ReconcileOwner preserves the original owner-scoped wait and calls the actual
// configured goal service/binding, never a fabricated board callback.
func (h *SessionHost) ReconcileOwner(ctx context.Context, owner OwnerKey) (SessionView, bool, error) {
	if e := h.lockOwnerOperation(ctx, owner, true); e != nil {
		return SessionView{}, false, e
	}
	defer h.mu.Unlock()
	r, _, live, e := h.liveOwnerLocked(ctx, owner)
	if e != nil || !live {
		return SessionView{}, false, e
	}
	if h.attachmentReserved(owner, r.SessionID) {
		return SessionView{}, false, ErrLegacyCommitUncertain
	}
	if absentHostPort(h.options.Goals) || absentHostPort(h.options.GoalBindings) {
		return SessionView{}, false, ErrHostCapabilityUnavailable
	}
	if e = h.reconcileHostGoalLocked(ctx, r.SessionID); e != nil {
		return SessionView{}, false, e
	}
	return h.view(h.sessions[r.SessionID]), true, nil
}

type scopedOwnerBinding struct {
	mu      sync.Mutex
	open    bool
	binding GoalBinding
}

func (b *scopedOwnerBinding) invoke(env *GoalEnvelope, rollback bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return ErrInputScopeClosed
	}
	if rollback {
		return b.binding.Rollback(env)
	}
	return b.binding.Apply(env)
}
func (b *scopedOwnerBinding) Apply(e *GoalEnvelope) error    { return b.invoke(e, false) }
func (b *scopedOwnerBinding) Rollback(e *GoalEnvelope) error { return b.invoke(e, true) }
func (b *scopedOwnerBinding) reconcile(source GoalSource, owner OwnerKey) (*GoalActivation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return nil, ErrInputScopeClosed
	}
	return source.Reconcile(owner, b.binding)
}
func (b *scopedOwnerBinding) close() { b.mu.Lock(); b.open = false; b.mu.Unlock() }

// WithOwnerGoalBinding allows the owning service to reflect an already-stored
// revision under the actual current writer. An open activation intentionally
// does not block its own apply/rollback phase; launch/reconcile wait for it.
// The caller must supply verified session/holder admission independently.
func (h *SessionHost) WithOwnerGoalBinding(ctx context.Context, owner OwnerKey, expected ControllerLease, fn func(OwnerGoalBinding) error) error {
	if fn == nil {
		return ErrInvalidInput
	}
	if e := h.lockOwnerOperation(ctx, owner, false); e != nil {
		return e
	}
	if absentHostPort(h.options.Goals) || absentHostPort(h.options.GoalBindings) {
		h.mu.Unlock()
		return ErrHostCapabilityUnavailable
	}
	record, _, live, e := h.liveOwnerLocked(ctx, owner)
	if e != nil || !live {
		h.mu.Unlock()
		if e != nil {
			return e
		}
		return ErrProviderHostNotFound
	}
	id := record.SessionID
	if h.attachmentReserved(owner, id) {
		h.mu.Unlock()
		return ErrLegacyCommitUncertain
	}
	input := h.inputs[id]
	if input == nil {
		h.mu.Unlock()
		return ErrHostCapabilityUnavailable
	}
	lease, e := input.CurrentLease(ctx, id)
	if e != nil {
		h.mu.Unlock()
		return e
	}
	if lease != expected {
		h.mu.Unlock()
		return ErrStaleInputController
	}
	base, ok := h.records.bases[id]
	if !ok {
		h.mu.Unlock()
		return ErrLegacyViewStale
	}
	path, e := h.records.path(id)
	if e != nil {
		h.mu.Unlock()
		return e
	}
	if e = h.records.matchesCurrent(path, base); e != nil {
		h.mu.Unlock()
		return e
	}
	view := h.view(record)
	operation := h.beginOwnerOperationLocked(owner)
	if h.goalReconciling == nil {
		h.goalReconciling = map[string]bool{}
	}
	h.goalReconciling[id] = true
	h.attachmentOps.Add(1)
	h.mu.Unlock()
	defer h.attachmentOps.Done()

	// Always expire the service binding before releasing its owner reservation.
	var scope *scopedOwnerBinding
	cleanupOwned := true
	defer func() {
		if scope != nil {
			scope.close()
		}
		if cleanupOwned {
			h.mu.Lock()
			delete(h.goalReconciling, id)
			h.endOwnerOperationLocked(owner, operation)
			h.mu.Unlock()
		}
	}()
	var read *GoalRead
	bound, binding, callErr := h.options.GoalBindings.BindGoal(ctx, lease, record.Provider, input)
	if callErr == nil && (bound == "" || absentHostPort(binding)) {
		callErr = ErrHostCapabilityUnavailable
	}
	if callErr == nil && !absentHostPort(h.options.Gate) {
		var disabled string
		disabled, callErr = h.options.Gate.DisabledGoalPlane(bound)
		if callErr == nil && disabled != "" {
			callErr = fmt.Errorf("%w: goal plane is disabled", ErrHostCapabilityUnavailable)
		}
	}
	if callErr == nil {
		scope = &scopedOwnerBinding{open: true, binding: binding}
		callErr = fn(OwnerGoalBinding{Session: view, Lease: lease, Owner: bound, Binding: scope, reconcile: func() (*GoalActivation, error) { return scope.reconcile(h.options.Goals, bound) }})
		scope.close()
	}
	if callErr == nil {
		read, callErr = h.options.Goals.Get(bound)
		if callErr == nil && read == nil {
			callErr = ErrHostCapabilityUnavailable
		}
	}

	h.mu.Lock()
	cleanupOwned = false
	defer h.mu.Unlock()
	defer h.endOwnerOperationLocked(owner, operation)
	defer delete(h.goalReconciling, id)
	current, leaseErr := input.CurrentLease(ctx, id)
	if h.closed || h.inputs[id] != input || leaseErr != nil || current != lease || h.sessions[id].State == SessionStopped {
		return errors.Join(callErr, leaseErr, ErrStaleInputController)
	}
	if callErr != nil {
		return callErr
	}
	next := copyRunnerRecord(h.sessions[id])
	next.AppliedGoalRef = nil
	if read.Applied != nil {
		ref := *read.Applied
		next.AppliedGoalRef = &ref
	}
	if reflect.DeepEqual(next, h.sessions[id]) {
		return nil
	}
	next.UpdatedAt = time.Now().UTC()
	if e = h.updateRecord(ctx, next); e != nil {
		return e
	}
	h.sessions[id] = next
	return nil
}

// LookupOwnerMapping observes the original recorded owner slot before native
// liveness/admission. The bool means occupied, not live; Session.Liveness is the
// retained observation. It never calls the runner, changes a record, or creates
// an epoch/authority grant. Attach must still validate the captured lease and
// all current admission requirements inside the verified caller's fence.
func (h *SessionHost) LookupOwnerMapping(ctx context.Context, owner OwnerKey) (SessionView, ControllerLease, bool, error) {
	if strings.TrimSpace(string(owner)) == "" {
		return SessionView{}, ControllerLease{}, false, ErrInvalidInput
	}
	if err := h.lockContext(ctx); err != nil {
		return SessionView{}, ControllerLease{}, false, err
	}
	defer h.mu.Unlock()
	if h.attachmentReserved(owner, "") {
		return SessionView{}, ControllerLease{}, false, ErrLegacyCommitUncertain
	}
	for _, pending := range h.pendingInitial {
		if pending.record.Owner == owner {
			return SessionView{}, ControllerLease{}, false, ErrLegacyCommitUncertain
		}
	}
	var record RunnerRecord
	for _, candidate := range h.sessions {
		if candidate.Owner == owner && candidate.State != SessionStopped {
			if record.SessionID != "" {
				return SessionView{}, ControllerLease{}, false, ErrLegacyCommitUncertain
			}
			record = candidate
		}
	}
	if record.SessionID == "" {
		return SessionView{}, ControllerLease{}, false, nil
	}
	if record.ConditionalStop != nil || record.GracefulRetirement || h.records.uncertain[record.SessionID] {
		return SessionView{}, ControllerLease{}, false, ErrLegacyCommitUncertain
	}
	base, ok := h.records.bases[record.SessionID]
	if !ok {
		return SessionView{}, ControllerLease{}, false, ErrLegacyViewStale
	}
	path, err := h.records.path(record.SessionID)
	if err != nil {
		return SessionView{}, ControllerLease{}, false, err
	}
	if err = h.records.matchesCurrent(path, base); err != nil {
		return SessionView{}, ControllerLease{}, false, err
	}
	input := h.inputs[record.SessionID]
	if input == nil {
		return SessionView{}, ControllerLease{}, false, ErrStaleInputController
	}
	lease, err := input.CurrentLease(ctx, record.SessionID)
	if err != nil {
		return SessionView{}, ControllerLease{}, false, err
	}
	if lease != h.attachmentControllers[record.SessionID] {
		return SessionView{}, ControllerLease{}, false, ErrStaleInputController
	}
	return h.view(record), lease, true, nil
}

// OwnerSession is a read-only mapping observation for external verified-caller
// admission. The subsequent physical operation must present the same lease;
// the observation alone grants no authority and makes no persistent write.
func (h *SessionHost) OwnerSession(ctx context.Context, owner OwnerKey) (SessionView, ControllerLease, bool, error) {
	if e := h.lockContext(ctx); e != nil {
		return SessionView{}, ControllerLease{}, false, e
	}
	defer h.mu.Unlock()
	record, _, live, e := h.liveOwnerLocked(ctx, owner)
	if e != nil || !live {
		return SessionView{}, ControllerLease{}, false, e
	}
	input := h.inputs[record.SessionID]
	if input == nil {
		return SessionView{}, ControllerLease{}, false, ErrStaleInputController
	}
	lease, e := input.CurrentLease(ctx, record.SessionID)
	if e != nil {
		return SessionView{}, ControllerLease{}, false, e
	}
	return h.view(record), lease, true, nil
}
