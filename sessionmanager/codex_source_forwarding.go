package sessionmanager

import (
	"context"
	"errors"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

var _ CodexOwnerRunSource = (*sessionInput)(nil)
var _ CodexOwnerRunSource = (*attachmentWriter)(nil)
var _ CodexOwnerRunSource = (*goalInvocationWriter)(nil)
var _ CodexOwnerRunSource = (*scopedInputWriter)(nil)

// readCodexSource observes only the sole writer's acknowledged current image.
// It never loads, recovers or acknowledges a record. The caller holds the actual
// short controller fence; no host map lock is acquired by this read path.
func (s *codecRecords) readCodexSource(ctx context.Context, lease ControllerLease, input *sessionInput, native RunnerHost) (CodexRunnerSnapshot, error) {
	if err := historyguard.LockMutex(ctx, &s.mu); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	defer s.mu.Unlock()
	id := lease.SessionID
	if s.uncertain[id] {
		return CodexRunnerSnapshot{}, ErrLegacyCommitUncertain
	}
	if _, pending := s.pendingInitial[id]; pending {
		return CodexRunnerSnapshot{}, ErrLegacyCommitUncertain
	}
	if _, pending := s.pendingReplacement[id]; pending {
		return CodexRunnerSnapshot{}, ErrLegacyCommitUncertain
	}
	base, ok := s.bases[id]
	if !ok {
		return CodexRunnerSnapshot{}, ErrLegacyViewStale
	}
	path, err := s.path(id)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	if err = s.matchesCurrent(path, base); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	record, err := s.decode(ctx, base)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	if record.State != SessionRunning || record.Provider != "codex" || record.GracefulRetirement || record.ConditionalStop != nil || record.ProviderHostPendingFork {
		return CodexRunnerSnapshot{}, ErrHostCapabilityUnavailable
	}
	cut := CapturedOwnerRun{Lease: lease, Input: input, Native: native, Record: record, Committed: copyLegacyRecordImage(base)}
	snapshot, err := ReadCapturedCodexOwnerRun(ctx, cut)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	if err = s.matchesCurrent(path, base); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	return snapshot, nil
}

// This is source data, not holder/goal/raising authority. Native and committed
// facts must agree under this input's real current controller; no caller-supplied
// snapshot or host-map reentry can replace that admission.
func (h *sessionInput) CodexOwnerRunSnapshot(ctx context.Context, lease ControllerLease) (CodexRunnerSnapshot, error) {
	if err := h.requireLease(ctx, lease); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	h.mu.Lock()
	read := h.codexSource
	life := h.epochCtx
	h.mu.Unlock()
	if read == nil {
		return CodexRunnerSnapshot{}, ErrHostCapabilityUnavailable
	}
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(life, cancel)
	defer stop()
	if life.Err() != nil {
		cancel()
	}
	release, err := h.acquireIO(call, lease)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	defer release()
	snapshot, readErr := read(call, lease)
	if err := h.requireLease(ctx, lease); err != nil {
		return CodexRunnerSnapshot{}, errors.Join(readErr, err)
	}
	if readErr != nil {
		return CodexRunnerSnapshot{}, readErr
	}
	if err := call.Err(); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	return snapshot, nil
}

func forwardCodexSource(ctx context.Context, parent HostInputWriter, lease ControllerLease) (CodexRunnerSnapshot, error) {
	source, ok := parent.(CodexOwnerRunSource)
	if !ok || absentHostPort(source) {
		return CodexRunnerSnapshot{}, ErrHostCapabilityUnavailable
	}
	snapshot, err := source.CodexOwnerRunSnapshot(ctx, lease)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	return snapshot, nil
}

func (w *attachmentWriter) CodexOwnerRunSnapshot(ctx context.Context, lease ControllerLease) (CodexRunnerSnapshot, error) {
	call, end, err := w.enter(ctx)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	defer end()
	return forwardCodexSource(call, w.parent, lease)
}
func (w *goalInvocationWriter) CodexOwnerRunSnapshot(ctx context.Context, lease ControllerLease) (CodexRunnerSnapshot, error) {
	call, end, err := w.scope.begin(ctx)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	defer end()
	return forwardCodexSource(call, w.parent, lease)
}
func (w *scopedInputWriter) CodexOwnerRunSnapshot(ctx context.Context, lease ControllerLease) (CodexRunnerSnapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.active {
		return CodexRunnerSnapshot{}, ErrInputScopeClosed
	}
	if lease.SessionID != w.observation.SessionID || lease.Epoch != w.observation.Epoch {
		return CodexRunnerSnapshot{}, ErrStaleInputController
	}
	call, cancel := bindInputContext(w.ctx, ctx)
	defer cancel()
	return w.host.CodexOwnerRunSnapshot(call, lease)
}

// The recovery error path also publishes uncertainty under the sole writer's
// mutex, so a concurrent read cannot observe an unsynchronized map mutation.
func (s *codecRecords) markUncertain(id string) { s.mu.Lock(); s.uncertain[id] = true; s.mu.Unlock() }
