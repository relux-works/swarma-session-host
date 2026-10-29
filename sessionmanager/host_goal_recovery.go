package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type goalRecoveryContextKey struct{}
type goalRecoveryScope struct {
	lease   ControllerLease
	input   *sessionInput
	native  RunnerHost
	owner   OwnerKey
	settled bool
}

// A failed attachment retains its actual runner. Retry may install only a new
// controller for that same acknowledged image; it cannot launch or stop native
// work or turn an unknown/dead native into a live capability.
func (h *SessionHost) restoreRetainedInputLocked(ctx context.Context, id string) error {
	record := h.sessions[id]
	runner := h.runners[id]
	if record.SessionID == "" || record.State == SessionStopped || record.GracefulRetirement || record.ConditionalStop != nil || absentHostPort(runner) || h.inputs[id] != nil {
		return ErrHostCapabilityUnavailable
	}
	base, ok := h.records.bases[id]
	if !ok {
		return ErrLegacyViewStale
	}
	path, err := h.records.path(id)
	if err != nil {
		return err
	}
	if err = h.records.matchesCurrent(path, base); err != nil {
		return err
	}
	if err := h.validateNativeAccountingRestoreLocked(ctx, id); err != nil {
		return err
	}
	if h.records.uncertain[id] {
		return ErrLegacyCommitUncertain
	}
	state, err := runner.State(ctx)
	if err != nil {
		return err
	}
	if state != ProviderHostRunning {
		return fmt.Errorf("%w: retained native is not running", ErrHostCapabilityUnavailable)
	}
	backend, ok := runner.(hostInputBackend)
	if !ok {
		return ErrHostCapabilityUnavailable
	}
	epoch, err := h.epochs.reserveContext(ctx, id)
	if err != nil {
		return err
	}
	input, err := h.installInput(ctx, id, epoch, backend)
	if err != nil {
		return err
	}
	h.inputs[id] = input
	return nil
}

// Only the actual in-flight binding callback can read a recovering image. The
// completed hydration lease, native/input identities and owner reservation all
// remain current; a raw Recovering record or an escaped context grants nothing.
func (h *SessionHost) goalRecoveryAdmittedLocked(ctx context.Context, id string) bool {
	scope := h.goalRecoveries[id]
	return scope != nil && ctx.Value(goalRecoveryContextKey{}) == scope && h.goalReconciling[id] && h.ownerOperations[scope.owner] != nil && h.nativeAccountingReady[id] == scope.lease && h.inputs[id] == scope.input && sameProxyRunner(h.runners[id], scope.native)
}

// Called only after the real goal reconciler has passed its error/deferred and
// controller revalidation branches. Ordinary calls carry no recovery scope.
func (h *SessionHost) goalRecoverySettledLocked(ctx context.Context, id string) error {
	scope, _ := ctx.Value(goalRecoveryContextKey{}).(*goalRecoveryScope)
	if scope == nil {
		return nil
	}
	if h.goalRecoveries[id] != scope || h.inputs[id] != scope.input || !sameProxyRunner(h.runners[id], scope.native) || h.nativeAccountingReady[id] != scope.lease {
		return ErrStaleInputController
	}
	lease, err := scope.input.CurrentLease(ctx, id)
	if err != nil || lease != scope.lease {
		return errors.Join(err, ErrStaleInputController)
	}
	scope.settled = true
	return nil
}

// This transaction begins only after actual full accounting hydration. It owns
// a distinct callback scope, never extends the earlier restore scope, and only
// publishes Running after real goal settlement and an acknowledged final write.
func (h *SessionHost) reconcileInstalledHostGoalLocked(ctx context.Context, id string) (outcomeErr error) {
	record := h.sessions[id]
	if record.State != SessionRecovering || absentHostPort(h.options.Goals) || h.nativeAccountingRestorer(record) == nil {
		return h.reconcileHostGoalLocked(ctx, id)
	}
	input := h.inputs[id]
	if input == nil || h.goalRecoveries[id] != nil {
		return ErrHostCapabilityUnavailable
	}
	lease, err := input.CurrentLease(ctx, id)
	if err != nil {
		return err
	}
	if h.nativeAccountingReady[id] != lease {
		return ErrHostCapabilityUnavailable
	}
	if h.records.uncertain[id] {
		return ErrLegacyCommitUncertain
	}
	base, ok := h.records.bases[id]
	if !ok {
		return ErrLegacyViewStale
	}
	path, err := h.records.path(id)
	if err != nil {
		return err
	}
	if err = h.records.matchesCurrent(path, base); err != nil {
		return err
	}
	scope := &goalRecoveryScope{lease: lease, input: input, native: h.runners[id], owner: record.Owner}
	if h.goalRecoveries == nil {
		h.goalRecoveries = map[string]*goalRecoveryScope{}
	}
	h.goalRecoveries[id] = scope
	defer func() {
		delete(h.goalRecoveries, id)
		if outcomeErr != nil && h.inputs[id] == input && sameProxyRunner(h.runners[id], scope.native) {
			clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			outcomeErr = errors.Join(outcomeErr, input.revokeAndDrain(clean))
			delete(h.inputs, id)
		}
	}()
	h.attachmentOps.Add(1)
	defer h.attachmentOps.Done()
	call := context.WithValue(ctx, goalRecoveryContextKey{}, scope)
	if err = h.reconcileHostGoalLocked(call, id); err != nil {
		return err
	}
	current, leaseErr := input.CurrentLease(ctx, id)
	if h.closed || h.inputs[id] != input || current != lease || leaseErr != nil || !sameProxyRunner(h.runners[id], scope.native) || h.sessions[id].State == SessionStopped {
		return errors.Join(leaseErr, ErrStaleInputController)
	}
	if !scope.settled {
		return fmt.Errorf("%w: recovery goal remains unsettled", ErrHostCapabilityUnavailable)
	}
	next := copyRunnerRecord(h.sessions[id])
	if next.State == SessionRunning {
		return nil
	} // The reconciler already acknowledged this transition.
	next.State = SessionRunning
	if next.OwnerRunError == "native accounting restoration failed" || next.OwnerRunError == "goal reconciliation failed" {
		next.OwnerRunError = ""
	}
	next.UpdatedAt = time.Now().UTC()
	if err = h.updateRecord(ctx, next); err != nil {
		return err
	}
	h.sessions[id] = next
	return nil
}
