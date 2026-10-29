package sessionmanager

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"time"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

// CodexRetirementRunner observes only the original process. Construction never
// resumes a thread, starts a child, dials RPC, or starts an auto-continue worker.
// Fork-private launch and owner state remain in the fork's committed record.
type CodexRetirementRunner struct {
	operation                               sync.Mutex
	mu                                      sync.Mutex
	snapshot                                CodexRunnerSnapshot
	exited                                  <-chan struct{}
	signalled, gracefulRequested, confirmed bool
}

var _ RunnerHost = (*CodexRetirementRunner)(nil)
var _ ProviderGracefulExit = (*CodexRetirementRunner)(nil)

func NewCodexRetirementRunner(snapshot CodexRunnerSnapshot) (*CodexRetirementRunner, error) {
	if snapshot.Contract != codexHostSnapshotContract {
		return nil, ErrLegacyCodecMismatch
	}
	owned := copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &snapshot})
	return &CodexRetirementRunner{snapshot: *owned.Codex}, nil
}
func (h *CodexRetirementRunner) Snapshot() (RunnerSnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &h.snapshot}), nil
}
func (h *CodexRetirementRunner) processGone() bool {
	h.mu.Lock()
	pid, exited := h.snapshot.PID, h.exited
	h.mu.Unlock()
	if pid <= 1 {
		return false
	}
	if exited != nil {
		select {
		case <-exited:
		default:
			return false
		}
	}
	_, pidErr := syscall.Getpgid(pid)
	groupErr := syscall.Kill(-pid, 0)
	return errors.Is(pidErr, syscall.ESRCH) && errors.Is(groupErr, syscall.ESRCH)
}
func (h *CodexRetirementRunner) State(ctx context.Context) (ProviderHostState, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if h.processGone() {
		h.mu.Lock()
		h.snapshot.Stopped = true
		h.confirmed = true
		h.mu.Unlock()
		return ProviderHostStopped, nil
	}
	return ProviderHostRunning, nil
}
func (h *CodexRetirementRunner) signal(signal syscall.Signal) error {
	h.mu.Lock()
	pid := h.snapshot.PID
	var identity *providerProcessIdentity
	if h.snapshot.ProcessIdentity != nil {
		p := h.snapshot.ProcessIdentity
		identity = &providerProcessIdentity{PID: p.PID, UID: p.UID, Start: p.Start, PGID: p.PGID}
	}
	h.mu.Unlock()
	return signalPersistedProviderGroup(pid, identity, signal)
}
func (h *CodexRetirementRunner) Stop(ctx context.Context) error {
	if err := historyguard.LockMutex(ctx, &h.operation); err != nil {
		return err
	}
	defer h.operation.Unlock()
	h.mu.Lock()
	signalled, confirmed := h.signalled, h.confirmed
	h.mu.Unlock()
	if confirmed {
		return nil
	}
	if !signalled {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := h.signal(syscall.SIGKILL); err != nil {
			return err
		}
		h.mu.Lock()
		h.signalled = true
		h.mu.Unlock()
	}
	_, err := h.WaitExit(ctx)
	return err
}
func (h *CodexRetirementRunner) RequestExit(ctx context.Context) error {
	if err := historyguard.LockMutex(ctx, &h.operation); err != nil {
		return err
	}
	defer h.operation.Unlock()
	h.mu.Lock()
	requested := h.gracefulRequested
	h.mu.Unlock()
	if requested {
		return nil
	}
	if !h.processGone() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := h.signal(syscall.SIGTERM); err != nil {
			return err
		}
	}
	h.mu.Lock()
	h.gracefulRequested = true
	h.mu.Unlock()
	return nil
}
func (h *CodexRetirementRunner) WaitExit(ctx context.Context) (bool, error) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if h.processGone() {
			h.mu.Lock()
			h.snapshot.Stopped = true
			h.confirmed = true
			h.mu.Unlock()
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-tick.C:
		}
	}
}
func (h *CodexRetirementRunner) UnsignalledAfterRestart() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.signalled && !h.gracefulRequested
}
