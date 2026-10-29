package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var _ WorkspaceSessionManager = (*SessionHost)(nil)

func (h *SessionHost) Stop(ctx context.Context, id string) (SessionView, error) {
	return h.StopWithDetail(ctx, id, "")
}

// StopWithDetail revokes input before publishing retirement intent. Each
// transaction passes through the sole writer's observed acknowledgement; a
// failed intent never licenses force, and a failed final commit never licenses
// success or another force before committed-image recovery.
func (h *SessionHost) StopWithDetail(ctx context.Context, id, detail string) (SessionView, error) {
	if err := h.lockContext(ctx); err != nil {
		return SessionView{}, err
	}
	defer h.mu.Unlock()
	return h.stopWithDetailLocked(ctx, id, detail)
}

func (h *SessionHost) stopWithDetailLocked(ctx context.Context, id, detail string) (SessionView, error) {
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
	if err := h.records.matchesCurrent(path, base); err != nil {
		return SessionView{}, err
	}
	if record.State == SessionStopped {
		if err := h.forgetStoppedRunner(ctx, id); err != nil {
			return SessionView{}, err
		}
		return h.view(record), nil
	}
	if err := h.retireGoalObserverBeforeRevokeLocked(ctx, id); err != nil {
		return SessionView{}, err
	}
	record = h.sessions[id]
	// Burn a durable generation and drain all actual issued writers. A cancelled
	// drain or failed epoch commit must return before any retirement signal.
	if _, err := h.epochs.reserveContext(ctx, id); err != nil {
		return SessionView{}, err
	}
	delete(h.inputs, id)
	runner := h.runners[id]
	if runner != nil {
		// The fork may have captured its joined canonical accounting cut just
		// before Stop. Commit the corresponding typed projection with intent,
		// rather than overwriting it with an older host snapshot.
		snapshot, err := runner.Snapshot()
		if err != nil {
			return SessionView{}, err
		}
		snapshot, err = snapshotWithCommittedAccounting(record, snapshot)
		if err != nil {
			return SessionView{}, err
		}
		record.ProviderHost = ProviderHostRecord{Provider: record.Provider, Snapshot: copyRunnerSnapshot(snapshot)}
	}
	record.GracefulRetirement = true
	record.UpdatedAt = time.Now().UTC()
	if err := h.updateRecord(ctx, record); err != nil {
		return SessionView{}, err
	}
	h.sessions[id] = record

	if runner == nil {
		factory, ok := h.factory(record.Provider).(RetiringHostFactory)
		if !ok {
			return SessionView{}, fmt.Errorf("%w: retirement observer", ErrHostCapabilityUnavailable)
		}
		// Supply a fresh actual committed read to the fork factory, after the
		// intent acknowledgement. Candidate decode alone grants no force view.
		committed, err := h.records.load(ctx, id)
		if err != nil {
			return SessionView{}, err
		}
		runner, err = factory.RestoreRetiring(ctx, ProviderHostRecord{Provider: record.Provider, Snapshot: copyRunnerSnapshot(committed.ProviderHost.Snapshot)})
		if err != nil && !errors.Is(err, ErrProviderHostNotFound) {
			return SessionView{}, err
		}
		if err == nil && absentHostPort(runner) {
			return SessionView{}, fmt.Errorf("nil retirement runner")
		}
		if err != nil {
			runner = nil
		} else {
			h.runners[id] = runner
		}
	}
	if err := ctx.Err(); err != nil {
		return SessionView{}, err
	}
	livenessDetail := stoppedHostAbsentLivenessDetail
	if runner != nil {
		if err := runner.Stop(ctx); err != nil {
			record.Liveness = SessionLivenessUnknown
			record.LivenessDetail = err.Error()
			record.UpdatedAt = time.Now().UTC()
			commitErr := h.updateRecord(ctx, record)
			if commitErr == nil {
				h.sessions[id] = record
			}
			return SessionView{}, errors.Join(err, commitErr)
		}
		snapshot, err := runner.Snapshot()
		if err != nil {
			return SessionView{}, err
		}
		snapshot, err = snapshotWithCommittedAccounting(record, snapshot)
		if err != nil {
			return SessionView{}, err
		}
		record.ProviderHost = ProviderHostRecord{Provider: record.Provider, Snapshot: copyRunnerSnapshot(snapshot)}
		livenessDetail = stoppedHostTerminatedLivenessDetail
	}
	record.State = SessionStopped
	record.Liveness = SessionLivenessDead
	record.LivenessDetail = livenessDetail
	if detail = strings.TrimSpace(detail); detail != "" {
		record.LivenessDetail += "; " + detail
	}
	record.UpdatedAt = time.Now().UTC()
	if err := h.updateRecord(ctx, record); err != nil {
		return SessionView{}, err
	}
	h.sessions[id] = record
	if err := h.forgetStoppedRunner(ctx, id); err != nil {
		return SessionView{}, err
	}
	return h.view(record), nil
}

// A stopped committed image cannot retain an old provider/input capability.
// Explicit resume may launch from its durable native snapshot afterwards.
func (h *SessionHost) forgetStoppedRunner(ctx context.Context, id string) error {
	if err := h.closeProviderProxyLocked(id); err != nil {
		return err
	}
	if input := h.inputs[id]; input != nil {
		if err := input.revokeAndDrain(ctx); err != nil {
			return err
		}
	}
	delete(h.gracefulJoins, id)
	delete(h.runners, id)
	delete(h.inputs, id)
	for attachment, session := range h.attachments {
		if session == id {
			delete(h.attachments, attachment)
		}
	}
	return nil
}
