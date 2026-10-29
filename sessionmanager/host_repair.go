//go:build unix

package sessionmanager

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Doctor uses the same real sweep as startup. The host map lock stabilizes
// record/process ownership; no public response includes private process evidence.
func (h *SessionHost) Doctor(ctx context.Context) (DoctorReport, error) {
	current, err := h.repair(ctx, "operator")
	h.mu.Lock()
	startup := h.startupRepair
	h.mu.Unlock()
	return DoctorReport{Startup: startup, Current: current}, err
}
func (h *SessionHost) RepairStartup(ctx context.Context) (RepairReport, error) {
	report, err := h.repair(ctx, "startup")
	if err == nil {
		h.mu.Lock()
		h.startupRepair = report
		h.mu.Unlock()
	}
	return report, err
}
func (h *SessionHost) repair(ctx context.Context, trigger string) (RepairReport, error) {
	if e := h.lockContext(ctx); e != nil {
		return RepairReport{}, e
	}
	defer h.mu.Unlock()
	l := h.options.Layout
	for _, path := range []string{l.StateDir, l.SocketPath, l.AttachmentsDir, l.EndpointFile, l.TokenFile} {
		if !filepath.IsAbs(path) {
			return RepairReport{}, fmt.Errorf("%w: explicit repair layout required", ErrHostCapabilityUnavailable)
		}
	}
	report, err := sweepStaleTransports(ctx, l)
	if err == nil {
		var processes RepairReport
		processes, err = h.sweepOrphanProviderProcessesLocked(ctx)
		report.merge(processes)
	}
	if filepath.IsAbs(l.OperatorLogFile) {
		fields := map[string]any{"trigger": trigger, "repairs": report.Repairs(), "orphan_processes_terminated": report.OrphanProcessesTerminated, "live_processes_preserved": report.LiveProcessesPreserved, "unsafe_processes_preserved": report.UnsafeProcessesPreserved, "stale_sockets_removed": report.StaleSocketsRemoved, "live_sockets_preserved": report.LiveSocketsPreserved, "stale_metadata_removed": report.StaleEndpointsRemoved, "live_metadata_preserved": report.LiveEndpointsPreserved, "stale_attachment_dirs_removed": report.StaleAttachmentDirsRemoved, "live_attachment_dirs_preserved": report.LiveAttachmentDirsPreserved}
		if err != nil {
			_ = RecordOperatorError(l, "session_repair_failed", err, fields)
		} else {
			_ = RecordOperatorInfo(l, "session_repair_finished", fields)
		}
	}
	return report, err
}

type recordedProviderProcess struct {
	provider                        string
	pid                             int
	executable, identity, sessionID string
	record                          *RunnerRecord
	markStopped                     func() error
}

func recordedProcess(record *RunnerRecord) (recordedProviderProcess, bool, error) {
	if record == nil {
		return recordedProviderProcess{}, false, nil
	}
	var pid int
	var executable, identity string
	var mark func() error
	switch record.Provider {
	case "codex":
		s := record.ProviderHost.Snapshot.Codex
		if s == nil {
			return recordedProviderProcess{}, false, fmt.Errorf("decoding Codex provider snapshot")
		}
		pid = s.PID
		executable = s.Executable
		identity = s.URL
		mark = func() error { s.Stopped = true; return nil }
	case "claude":
		s := record.ProviderHost.Snapshot.Claude
		if s == nil {
			return recordedProviderProcess{}, false, fmt.Errorf("decoding Claude provider snapshot")
		}
		pid = s.PID
		executable = s.Plan.Executable
		identity = s.SessionUUID
		mark = func() error { s.Stopped = true; return nil }
	default:
		return recordedProviderProcess{}, false, nil
	}
	if pid <= 0 {
		return recordedProviderProcess{}, false, nil
	}
	return recordedProviderProcess{provider: record.Provider, pid: pid, executable: executable, identity: identity, sessionID: record.SessionID, record: record, markStopped: mark}, true, nil
}
func (p recordedProviderProcess) matches(ctx context.Context) (bool, error) {
	switch p.provider {
	case "codex":
		return codexProcessMatches(ctx, p.pid, p.executable, p.identity)
	case "claude":
		return claudeRecordedProcessMatches(ctx, p.pid, p.identity)
	default:
		return false, nil
	}
}
func claudeRecordedProcessMatches(ctx context.Context, pid int, id string) (bool, error) {
	alive, e := processAlive(pid)
	if e != nil || !alive {
		return false, e
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return false, nil
	}
	line, e := processCommandLine(ctx, pid)
	if e != nil {
		return false, e
	}
	return bytes.Contains(line, []byte(id)), nil
}
func (h *SessionHost) sweepOrphanProviderProcessesLocked(
	ctx context.Context,
) (RepairReport, error) {
	var report RepairReport
	livePIDs := make(map[int]string)
	sessionIDs := make([]string, 0, len(h.sessions))
	for sessionID := range h.sessions {
		sessionIDs = append(sessionIDs, sessionID)
	}
	sort.Strings(sessionIDs)

	for _, sessionID := range sessionIDs {
		record := copyRunnerRecord(h.sessions[sessionID])
		if record.State != SessionRunning || h.runners[sessionID] == nil {
			continue
		}
		process, ok, err := recordedProcess(&record)
		if err != nil {
			return report, fmt.Errorf("reading live provider process for session %s: %w", sessionID, err)
		}
		if ok {
			livePIDs[process.pid] = sessionID
			if alive, aliveErr := processAlive(process.pid); aliveErr != nil {
				return report, fmt.Errorf("checking live provider pid %d: %w", process.pid, aliveErr)
			} else if alive {
				report.LiveProcessesPreserved++
			}
		}
	}

	for _, sessionID := range sessionIDs {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		record := copyRunnerRecord(h.sessions[sessionID])
		if refuseRetiredOwner(record.Owner) != nil || (record.State == SessionRunning && h.runners[sessionID] != nil) {
			continue
		}
		process, ok, err := recordedProcess(&record)
		if err != nil {
			h.repairLogError(
				"orphan_provider_process_sweep_failed",
				err,
				repairSessionFields(record),
			)
			return report, fmt.Errorf("reading orphan provider process for session %s: %w", sessionID, err)
		}
		if !ok {
			continue
		}
		if record.Liveness == SessionLivenessUnknown {
			report.UnsafeProcessesPreserved++
			fields := repairSessionFields(record)
			fields["pid"] = process.pid
			fields["reason"] = "provider_liveness_unknown"
			fields["liveness_detail"] = record.LivenessDetail
			h.repairLogInfo("orphan_provider_process_preserved", fields)
			continue
		}
		if record.Liveness == SessionLivenessLive {
			alive, aliveErr := processAlive(process.pid)
			if aliveErr != nil {
				return report, fmt.Errorf("checking preserved provider pid %d: %w", process.pid, aliveErr)
			}
			if alive {
				report.LiveProcessesPreserved++
				continue
			}
		}
		if liveSessionID := livePIDs[process.pid]; liveSessionID != "" {
			fields := repairSessionFields(record)
			fields["pid"] = process.pid
			fields["reason"] = "pid_owned_by_live_session"
			fields["live_session_id"] = liveSessionID
			h.repairLogInfo("orphan_provider_process_preserved", fields)
			continue
		}
		alive, err := processAlive(process.pid)
		if err != nil {
			return report, fmt.Errorf("checking provider pid %d: %w", process.pid, err)
		}
		if !alive {
			continue
		}
		matched, err := process.matches(ctx)
		if err != nil {
			return report, err
		}
		if !matched || process.pid <= 1 || process.pid == os.Getpid() {
			report.UnsafeProcessesPreserved++
			fields := repairSessionFields(record)
			fields["pid"] = process.pid
			fields["reason"] = "process_identity_not_proven"
			h.repairLogInfo("orphan_provider_process_preserved", fields)
			continue
		}
		if err := process.terminate(ctx); err != nil {
			fields := repairSessionFields(record)
			fields["pid"] = process.pid
			h.repairLogError("orphan_provider_process_sweep_failed", err, fields)
			return report, err
		}
		if err := process.markStopped(); err != nil {
			return report, err
		}
		record.UpdatedAt = time.Now().UTC()
		if err := h.updateRecord(ctx, record); err != nil {
			return report, fmt.Errorf("saving swept provider record %s: %w", sessionID, err)
		}
		h.sessions[sessionID] = record
		report.OrphanProcessesTerminated++
		fields := repairSessionFields(record)
		fields["pid"] = process.pid
		fields["result"] = "terminated"
		h.repairLogInfo("orphan_provider_process_swept", fields)
	}
	return report, nil
}

func (p recordedProviderProcess) terminate(ctx context.Context) error {
	matched, err := p.matches(ctx)
	if err != nil {
		return err
	}
	if !matched {
		// The recorded process exited or the PID was recycled after the sweep
		// classified it. Treat the original orphan as gone and never signal the
		// replacement.
		return nil
	}
	if err := requestProcessTermination(p.pid); err != nil {
		alive, aliveErr := processAlive(p.pid)
		if aliveErr != nil {
			return aliveErr
		}
		if !alive {
			return nil
		}
		return fmt.Errorf("terminating orphan provider pid %d: %w", p.pid, err)
	}
	exited, err := waitForProcessExit(ctx, p.pid, providerProcessStopGrace)
	if err != nil {
		return err
	}
	if exited {
		return nil
	}
	matched, err = p.matches(ctx)
	if err != nil {
		return err
	}
	if !matched {
		return nil
	}
	if err := forceProcessTermination(p.pid); err != nil {
		alive, aliveErr := processAlive(p.pid)
		if aliveErr != nil {
			return aliveErr
		}
		if !alive {
			return nil
		}
		return fmt.Errorf("force-terminating orphan provider pid %d: %w", p.pid, err)
	}
	exited, err = waitForProcessExit(ctx, p.pid, providerProcessForceGrace)
	if err != nil {
		return err
	}
	if !exited {
		return fmt.Errorf("orphan provider pid %d remained alive after forced termination", p.pid)
	}
	return nil
}

func repairSessionFields(record RunnerRecord) map[string]any {
	return map[string]any{"session_id": record.SessionID, "provider": record.Provider}
}
func (h *SessionHost) repairLogInfo(event string, fields map[string]any) {
	if filepath.IsAbs(h.options.Layout.OperatorLogFile) {
		_ = RecordOperatorInfo(h.options.Layout, event, fields)
	}
}
func (h *SessionHost) repairLogError(event string, cause error, fields map[string]any) {
	if filepath.IsAbs(h.options.Layout.OperatorLogFile) {
		_ = RecordOperatorError(h.options.Layout, event, cause, fields)
	}
}
