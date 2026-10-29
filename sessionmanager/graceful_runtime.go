package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var _ GracefulWorkspaceSessionManager = (*SessionHost)(nil)

// StopGraceful owns both record transactions. Native capability is checked
// before retirement; only the sole writer's observed ACK licenses RequestExit.
// Canonical authorization remains in the owning fork, not in a provider port.
func (h *SessionHost) StopGraceful(ctx context.Context, id string) (SessionView, error) {
	if err := h.lockContext(ctx); err != nil {
		return SessionView{}, err
	}
	defer h.mu.Unlock()
	id = strings.TrimSpace(id)
	if h.attachmentReserved("", id) {
		return SessionView{}, ErrLegacyCommitUncertain
	}
	if _, pending := h.pendingInitial[id]; pending || h.records.uncertain[id] {
		return SessionView{}, ErrLegacyCommitUncertain
	}
	record, ok := h.sessions[id]
	if !ok {
		return SessionView{}, ErrProviderHostNotFound
	}
	if record.ConditionalStop != nil {
		return SessionView{}, ErrLegacyCommitUncertain
	}
	base, ok := h.records.bases[id]
	if !ok {
		return SessionView{}, ErrLegacyViewStale
	}
	path, err := h.records.path(id)
	if err != nil {
		return SessionView{}, err
	}
	if err = h.records.matchesCurrent(path, base); err != nil {
		return SessionView{}, err
	}
	if record.State == SessionStopped {
		if h.gracefulJoins[id] {
			if err := h.joinStoppedRunner(ctx, id); err != nil {
				return SessionView{}, err
			}
			if err := h.forgetStoppedRunner(ctx, id); err != nil {
				return SessionView{}, err
			}
		}
		return h.view(record), nil
	}
	if lease, ok := h.attachmentControllers[id]; ok && !h.epochs.admitsContext(ctx, id, lease.Epoch) {
		return SessionView{}, ErrStaleInputController
	}
	runner := h.runners[id]
	if runner == nil {
		factory, ok := h.factory(record.Provider).(RetiringHostFactory)
		if !ok || absentHostPort(factory) {
			return SessionView{}, fmt.Errorf("%w: retirement observer", ErrHostCapabilityUnavailable)
		}
		// Loading is read admission only, not a receipt or permission to signal.
		committed, e := h.records.load(ctx, id)
		if e != nil {
			return SessionView{}, e
		}
		runner, err = factory.RestoreRetiring(ctx, ProviderHostRecord{Provider: record.Provider, Snapshot: copyRunnerSnapshot(committed.ProviderHost.Snapshot)})
		if err != nil {
			return SessionView{}, err
		}
	}
	if absentHostPort(runner) {
		return SessionView{}, ErrHostCapabilityUnavailable
	}
	graceful, ok := runner.(ProviderGracefulExit)
	if !ok || absentHostPort(graceful) {
		return SessionView{}, fmt.Errorf("%w: graceful exit", ErrHostCapabilityUnavailable)
	}
	// Revoke and drain actual previously issued inputs before intent. Keep the
	// observed runner on errors: recovery must never manufacture a replacement.
	h.runners[id] = runner
	if h.gracefulJoins == nil {
		h.gracefulJoins = map[string]bool{}
	}
	h.gracefulJoins[id] = true
	epoch, err := h.epochs.reserveContext(ctx, id)
	if err != nil {
		return SessionView{}, err
	}
	h.rememberAttachmentController(id, epoch)
	// Retain the existing physical-I/O generation fences across both writer
	// transactions and the native effect; replacement cannot license old work.
	release, err := h.epochs.beginIO(ctx, ControllerLease{SessionID: id, Epoch: epoch})
	if err != nil {
		return SessionView{}, err
	}
	defer release()
	delete(h.inputs, id)
	delete(h.metadataLeases, id)
	snapshot, err := runner.Snapshot()
	if err != nil {
		return SessionView{}, err
	}
	record.ProviderHost = ProviderHostRecord{Provider: record.Provider, Snapshot: copyRunnerSnapshot(snapshot)}
	if !record.GracefulRetirement {
		record.GracefulRetirement = true
		record.UpdatedAt = time.Now().UTC()
	}
	// Even a pending retry obtains its own exact image acknowledgement. The
	// retirement bit is reused; no receipt from an earlier operation is borrowed.
	if err = h.updateRecord(ctx, record); err != nil {
		return SessionView{}, err
	}
	h.sessions[id] = record
	if err = graceful.RequestExit(ctx); err != nil {
		return SessionView{}, err
	}
	// Original graceful.go:117 protocol budget; caller cancellation/shorter
	// deadline wins. This is not an observation delay or a test success timer.
	wait, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	gone, err := graceful.WaitExit(wait)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return SessionView{}, err
	}
	if !gone {
		return h.view(record), nil
	}
	// As in the original path, a failed optional snapshot does not erase actual
	// exit evidence. The final sole-writer ACK still precedes cleanup/success.
	if snapshot, e := runner.Snapshot(); e == nil {
		record.ProviderHost = ProviderHostRecord{Provider: record.Provider, Snapshot: copyRunnerSnapshot(snapshot)}
	}
	record.State = SessionStopped
	record.Liveness = SessionLivenessDead
	record.LivenessDetail = "provider exited after graceful request; mode=graceful"
	record.UpdatedAt = time.Now().UTC()
	if err = h.updateRecord(ctx, record); err != nil {
		return SessionView{}, err
	}
	h.sessions[id] = record
	if err = h.joinStoppedRunner(ctx, id); err != nil {
		return SessionView{}, err
	}
	if err = h.forgetStoppedRunner(ctx, id); err != nil {
		return SessionView{}, err
	}
	return h.view(record), nil
}

// A stopped ACK or checked committed recovery settles an actual retirement
// observer, including one left by failed force without a graceful wait. The
// ordinary graceful flag also retains its original join/retry obligation.
// Direct successful force keeps its existing cleanup; optional observation
// closing here neither signals nor manufactures a graceful wait obligation.
func (h *SessionHost) joinStoppedRunner(ctx context.Context, id string) error {
	if !h.gracefulJoins[id] && !h.sessions[id].GracefulRetirement {
		return nil
	}
	if closer, ok := h.runners[id].(ProviderObservationCloser); ok {
		if err := closer.CloseObservation(ctx); err != nil {
			return err
		}
	}
	delete(h.gracefulJoins, id)
	return nil
}
