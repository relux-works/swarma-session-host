package sessionmanager

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"syscall"
	"time"
)

// Caller holds the host lock. Canonical owner/activation exclusion belongs to
// the fork; the host owns physical identity, attachments and durable release.
// No restore, native RPC, signal or factory preparation is needed to observe.
func (h *SessionHost) releaseDeadAdoptionIncumbent(ctx context.Context, record RunnerRecord) (bool, error) {
	if h.view(record).AttachedClients != 0 || record.GracefulRetirement || record.ConditionalStop != nil || record.ProviderHostPendingFork {
		return false, nil
	}
	observed, err := observeDeadAdoptionIncumbent(ctx, record, h.runners[record.SessionID])
	if !observed {
		return false, err
	}
	stopped := copyRunnerRecord(record)
	stopped.ProviderHost.Snapshot.Codex.Stopped = true
	stopped.State = SessionStopped
	stopped.Liveness = SessionLivenessDead
	stopped.LivenessDetail = "recorded native process, process group and socket are absent"
	stopped.UpdatedAt = time.Now().UTC()
	// Never expose the free owner slot until the sole writer acknowledges it.
	if err := h.updateRecord(ctx, stopped); err != nil {
		return false, err
	}
	h.sessions[record.SessionID] = stopped
	delete(h.metadataLeases, record.SessionID)
	if input := h.inputs[record.SessionID]; input != nil {
		if err := input.revokeAndDrain(ctx); err != nil {
			return false, err
		}
		delete(h.inputs, record.SessionID)
	}
	return true, nil
}

// Physical absence is shared by ordinary and staged adoption. It carries no
// host state or writer; staging reserves/revalidates around this unlocked read.
func observeDeadAdoptionIncumbent(ctx context.Context, record RunnerRecord, runner RunnerHost) (bool, error) {
	native := record.ProviderHost.Snapshot
	if record.Provider != "codex" || native.Kind != "codex" || native.Codex == nil {
		return false, nil
	}
	codex := native.Codex
	identity := codex.ProcessIdentity
	// A legacy PID without birth/group provenance is unverifiable. A reused PID
	// or surviving group/socket also refuses, even if the persisted bit is stopped.
	if identity == nil || codex.PID <= 1 || identity.PID != codex.PID || identity.PGID != codex.PID || identity.Start == "" {
		return false, nil
	}
	parsed, err := url.Parse(codex.URL)
	if err != nil || (parsed.Scheme != "ws" && parsed.Scheme != "wss") || parsed.Host == "" {
		return false, nil
	}
	if runner != nil {
		state, err := runner.State(ctx)
		if err != nil || state != ProviderHostStopped {
			return false, ctx.Err()
		}
	}
	_, pidErr := syscall.Getpgid(codex.PID)
	groupErr := syscall.Kill(-codex.PID, 0)
	if !errors.Is(pidErr, syscall.ESRCH) || !errors.Is(groupErr, syscall.ESRCH) {
		return false, nil
	}
	probe, cancel := context.WithTimeout(ctx, transportProbeTimeout)
	defer cancel()
	connection, err := (&net.Dialer{}).DialContext(probe, "tcp", parsed.Host)
	if err == nil {
		_ = connection.Close()
		return false, nil
	}
	if !errors.Is(err, syscall.ECONNREFUSED) && !errors.Is(err, os.ErrNotExist) {
		return false, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}
