package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type nativeAccountingScopeKey struct{}
type nativeAccountingScope struct {
	owner     OwnerKey
	operation *hostOwnerOperation
}

func (h *SessionHost) nativeAccountingAdmitted(ctx context.Context, id string) bool {
	s := h.nativeAccountingRestores[id]
	return s == nil || ctx.Value(nativeAccountingScopeKey{}) == s
}
func (h *SessionHost) nativeAccountingRestorer(record RunnerRecord) MuseAccountingRestorer {
	if record.Provider != "muse" || record.ProviderHost.Snapshot.MuseMSP == nil || record.ProviderHost.Snapshot.MuseMSP.GoalTransport != "native" || record.GracefulRetirement && record.State != SessionStopped {
		return nil
	}
	consumer, _ := h.options.OwnerRuns.(MuseAccountingRestorer)
	if absentHostPort(consumer) {
		return nil
	}
	return consumer
}
func (h *SessionHost) beginNativeAccountingScopeLocked(ctx context.Context, id string) (context.Context, *nativeAccountingScope, error) {
	r := h.sessions[id]
	if h.nativeAccountingRestores[id] != nil || h.ownerOperations[r.Owner] != nil {
		return nil, nil, ErrLegacyCommitUncertain
	}
	s := &nativeAccountingScope{owner: r.Owner, operation: h.beginOwnerOperationLocked(r.Owner)}
	if h.nativeAccountingRestores == nil {
		h.nativeAccountingRestores = map[string]*nativeAccountingScope{}
	}
	h.nativeAccountingRestores[id] = s
	h.attachmentOps.Add(1)
	return context.WithValue(ctx, nativeAccountingScopeKey{}, s), s, nil
}
func (h *SessionHost) endNativeAccountingScopeLocked(id string, s *nativeAccountingScope) {
	if h.nativeAccountingRestores[id] == s {
		delete(h.nativeAccountingRestores, id)
		h.endOwnerOperationLocked(s.owner, s.operation)
		h.attachmentOps.Done()
	}
}

// Called with mu held and returns with it held, including a callback panic.
// Only the actual local consumer receives the private reservation context.
func (h *SessionHost) callNativeAccountingUnlocked(fn func() error) (err error) {
	h.mu.Unlock()
	defer h.mu.Lock()
	return fn()
}

// Validation precedes physical Restore/managed resume. It reads the exact
// acknowledged record, never an attempted image or a caller's raw snapshot.
func (h *SessionHost) validateNativeAccountingRestoreLocked(ctx context.Context, id string) error {
	record := h.sessions[id]
	consumer := h.nativeAccountingRestorer(record)
	if consumer == nil {
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
	call, s, e := h.beginNativeAccountingScopeLocked(ctx, id)
	if e != nil {
		return e
	}
	defer h.endNativeAccountingScopeLocked(id, s)
	e = h.callNativeAccountingUnlocked(func() error {
		validation, e := nativeAccountingPreflightRecord(call, record, base)
		if e != nil {
			return e
		}
		return consumer.ValidateMuseAccountingRestore(call, validation, copyLegacyRecordImage(base))
	})
	if h.closed {
		return errors.Join(e, ErrHostClosed)
	}
	if ctx.Err() != nil {
		return errors.Join(e, ctx.Err())
	}
	current, ok := h.records.bases[id]
	if !ok || current.SHA256 != base.SHA256 {
		return errors.Join(e, ErrLegacyViewStale)
	}
	if diskErr := h.records.matchesCurrent(path, base); diskErr != nil {
		return errors.Join(e, diskErr)
	}
	return e
}

// Hydration is after the real controller/native runner exists, before any
// attachment or goal operation is published. No epoch fence spans the callback:
// its real Read/CAS and native observation scopes own their individual fences.
func (h *SessionHost) restoreNativeAccountingInstalledLocked(ctx context.Context, id string) error {
	record := h.sessions[id]
	// Stopped records are validated BEFORE resume, but have no live writer to
	// hydrate until a new physical host and Running image are installed.
	if record.State == SessionStopped || record.GracefulRetirement {
		return nil
	}
	consumer := h.nativeAccountingRestorer(record)
	if consumer == nil {
		return nil
	}
	input := h.inputs[id]
	if input == nil {
		return ErrHostCapabilityUnavailable
	}
	lease, e := input.CurrentLease(ctx, id)
	if e != nil {
		return e
	}
	if h.nativeAccountingReady[id] == lease {
		return nil
	}
	call, s, e := h.beginNativeAccountingScopeLocked(ctx, id)
	if e != nil {
		return e
	}
	defer h.endNativeAccountingScopeLocked(id, s)
	cut, e := h.ownerRunStateLocked(call, lease)
	if e != nil {
		return e
	}
	e = h.callNativeAccountingUnlocked(func() error {
		validation, e := nativeAccountingPreflightRecord(call, cut.Record, cut.Committed)
		if e != nil {
			return e
		}
		if e := consumer.ValidateMuseAccountingRestore(call, validation, copyLegacyRecordImage(cut.Committed)); e != nil {
			return e
		}
		cut, e = h.bootstrapObservedEmptyMuseAccounting(call, cut)
		if e != nil {
			return e
		}
		return consumer.RestoreMuseAccounting(call, cut)
	})
	current, leaseErr := input.CurrentLease(call, id)
	if h.closed || h.inputs[id] != input || h.runners[id] != cut.Native || leaseErr != nil || current != lease {
		e = errors.Join(e, leaseErr, ErrStaleInputController)
	}
	if e == nil {
		_, e = h.ownerRunStateLocked(call, lease)
	}
	if e == nil {
		if h.nativeAccountingReady == nil {
			h.nativeAccountingReady = map[string]ControllerLease{}
		}
		h.nativeAccountingReady[id] = lease
		return nil
	}
	delete(h.nativeAccountingReady, id)
	// Preserve the actual runner and latest committed accounting for recovery;
	// failed hydration grants neither input nor a replacement launch/force path.
	clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	revokeErr := input.revokeAndDrain(clean)
	delete(h.inputs, id)
	next := copyRunnerRecord(h.sessions[id])
	next.State = SessionRecovering
	next.OwnerRunError = "native accounting restoration failed"
	next.UpdatedAt = time.Now().UTC()
	commitErr := h.updateRecord(clean, next)
	if commitErr == nil {
		h.sessions[id] = next
	}
	return fmt.Errorf("restoring native accounting: %w", errors.Join(e, revokeErr, commitErr))
}
