package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// GoalObserverRetirer joins only the actual selected factory's observer for one
// exact controller. False means registry-observed absence, not a native ACK or
// healthy accounting. Retirement never clears uncertainty, pending effects or
// ledger facts and never stops a provider or closes the shared factory.
type GoalObserverRetirer interface {
	RetireGoalObserver(context.Context, ControllerLease) (joined bool, err error)
}

func nativeGoalObserverRecord(record RunnerRecord) bool {
	return record.Provider == "muse" && record.State != SessionStopped && record.ProviderHost.Snapshot.MuseMSP != nil && record.ProviderHost.Snapshot.MuseMSP.GoalTransport == "native"
}
func (h *SessionHost) goalObserverRetirer() GoalObserverRetirer {
	if port, ok := h.options.GoalBindings.(GoalObserverRetirer); ok && !absentHostPort(port) {
		return port
	}
	if absentHostPort(h.options.GoalBindings) {
		if port, ok := h.options.OwnerRuns.(GoalObserverRetirer); ok && !absentHostPort(port) {
			return port
		}
	}
	return nil
}
func (h *SessionHost) requiresGoalObserverRetirer() bool {
	codec, ok := h.options.Codec.(*LegacyRecordCodec)
	return ok && !absentHostPort(codec.museAccounting)
}
func (h *SessionHost) recordedObserverLeaseLocked(ctx context.Context, id string) (ControllerLease, error) {
	lease := h.attachmentControllers[id]
	if input := h.inputs[id]; input != nil {
		actual, e := input.CurrentLease(ctx, id)
		if e != nil {
			return ControllerLease{}, e
		}
		if actual != lease {
			return ControllerLease{}, ErrStaleInputController
		}
	}
	if lease.SessionID == "" && lease.Epoch == 0 {
		return ControllerLease{}, nil
	}
	if lease.SessionID != id || lease.Epoch == 0 {
		return ControllerLease{}, ErrStaleInputController
	}
	release, e := h.epochs.beginIO(ctx, lease)
	if e != nil {
		return ControllerLease{}, e
	}
	release()
	return lease, nil
}

// Caller holds mu. Cancellation/join runs without mu or a durable writer fence,
// with the original owner/attachment reservation held until exact revalidation.
func (h *SessionHost) retireGoalObserverBeforeRevokeLocked(ctx context.Context, id string) error {
	record := h.sessions[id]
	if !nativeGoalObserverRecord(record) {
		return nil
	}
	lease, e := h.recordedObserverLeaseLocked(ctx, id)
	if e != nil {
		return e
	}
	if lease.SessionID == "" {
		return nil
	}
	port := h.goalObserverRetirer()
	if port == nil {
		if h.requiresGoalObserverRetirer() {
			return fmt.Errorf("%w: native observer retirement source", ErrHostCapabilityUnavailable)
		}
		return nil
	}
	base, ok := h.records.bases[id]
	if !ok || h.records.uncertain[id] {
		return ErrLegacyCommitUncertain
	}
	path, e := h.records.path(id)
	if e != nil {
		return e
	}
	if e = h.records.matchesCurrent(path, base); e != nil {
		return e
	}
	input, native := h.inputs[id], h.runners[id]
	_, scope, e := h.beginNativeAccountingScopeLocked(ctx, id)
	if e != nil {
		return e
	}
	defer h.endNativeAccountingScopeLocked(id, scope)
	e = h.callNativeAccountingUnlocked(func() error { _, e := port.RetireGoalObserver(ctx, lease); return e })
	if h.closed {
		return errors.Join(e, ErrHostClosed)
	}
	if h.inputs[id] != input || !sameProxyRunner(h.runners[id], native) || h.attachmentControllers[id] != lease || h.sessions[id].Owner != record.Owner || h.sessions[id].State != record.State {
		return errors.Join(e, ErrStaleInputController)
	}
	if _, leaseErr := h.recordedObserverLeaseLocked(ctx, id); leaseErr != nil {
		return errors.Join(e, leaseErr)
	}
	current, ok := h.records.bases[id]
	if !ok || current.SHA256 != base.SHA256 {
		return errors.Join(e, ErrLegacyViewStale)
	}
	return errors.Join(e, h.records.matchesCurrent(path, base))
}

// Close has closed public host admission, but deliberately has not canceled the
// input lifetime yet. Existing real observers can join without a fabricated
// feed-loss uncertainty. The shared factory itself remains caller-owned.
// A later retry uses only the host's already-recorded issued controller leases.
func (h *SessionHost) retireGoalObserversBeforeCloseLocked(ctx context.Context) error {
	port := h.goalObserverRetirer()
	ids := make([]string, 0)
	for id, record := range h.sessions {
		if nativeGoalObserverRecord(record) && h.attachmentControllers[id].SessionID == id {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	if port == nil {
		if h.requiresGoalObserverRetirer() {
			return fmt.Errorf("%w: native observer retirement source", ErrHostCapabilityUnavailable)
		}
		return nil
	}
	sort.Strings(ids)
	leases := make([]ControllerLease, 0, len(ids))
	for _, id := range ids {
		lease := h.attachmentControllers[id]
		if lease.Epoch == 0 {
			return ErrStaleInputController
		}
		leases = append(leases, lease)
	}
	return h.callNativeAccountingUnlocked(func() error {
		var result error
		for _, lease := range leases {
			_, e := port.RetireGoalObserver(ctx, lease)
			result = errors.Join(result, e)
		}
		return result
	})
}
