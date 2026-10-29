package sessionclient

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

var (
	staleInstanceHealthCheckTimeout = 750 * time.Millisecond
	startupHealthyDeadline          = 3 * time.Second
	staleInstanceTerminateGrace     = 6 * time.Second
	staleInstanceForceGrace         = 2 * time.Second
	staleInstancePollInterval       = 25 * time.Millisecond
)

type staleInstanceTakeover struct {
	old Endpoint
}

func (r *startupRuntime) dialStartupStatus(
	ctx context.Context,
	layout StartupLayout,
) (*Client, StartupStatus, error) {
	client, err := r.dial(layout)
	if err != nil {
		return nil, StartupStatus{}, err
	}
	healthCtx, cancel := context.WithTimeout(ctx, staleInstanceHealthCheckTimeout)
	defer cancel()
	status, err := r.status(healthCtx, client, layout)
	if err != nil {
		client.Close()
		return nil, StartupStatus{}, err
	}
	return client, status, nil
}

func (r *startupRuntime) dialHealthyManager(
	ctx context.Context,
	layout StartupLayout,
) (*Client, StartupStatus, error) {
	client, status, err := r.dialStartupStatus(ctx, layout)
	if err != nil {
		return nil, StartupStatus{}, err
	}
	if err := CheckCompatibility(status.ProtocolStatus, r.options.WrapperVersion); err != nil {
		client.Close()
		return nil, status, err
	}
	if !status.StartupHealthy {
		client.Close()
		return nil, status, ErrManagerStartupUnhealthy
	}
	return client, status, nil
}

type protocolUpgradeTakeover struct {
	old     Endpoint
	drained bool
}

// takeOverOutdatedManager replaces a responsive but protocol-incompatible
// holder. It first asks a daemon that already knows the drain endpoint to
// relinquish its lock without stopping provider hosts. A pre-drain daemon is
// fenced with the same endpoint/PID/lock checks as an unresponsive holder.
func (r *startupRuntime) takeOverOutdatedManager(
	ctx context.Context,
	layout StartupLayout,
	upgrade *ProtocolUpgradeError,
) (*protocolUpgradeTakeover, error) {
	if !upgrade.RequiresReplacement() {
		return nil, fmt.Errorf("refusing protocol takeover without an older or divergent daemon build")
	}
	return r.takeOverManagerForProtocolUpgrade(ctx, layout, false)
}

// takeOverManagerForProtocolUpgrade is entered only after either negotiated
// older protocol metadata or a typed unknown-field decoder rejection. The
// latter is a compatibility proof even when a malformed legacy status claims
// the current protocol, so forceAfterRejection permits the same fenced restart.
func (r *startupRuntime) takeOverManagerForProtocolUpgrade(
	ctx context.Context,
	layout StartupLayout,
	forceAfterRejection bool,
) (*protocolUpgradeTakeover, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("checking outdated session manager: %w", err)
	}
	lockHeld, err := r.lockHeld(ctx, layout)
	if err != nil {
		return nil, err
	}
	if !lockHeld {
		return nil, nil
	}
	recorded, err := startupEndpoint(layout)
	if err != nil {
		return nil, fmt.Errorf("reading outdated session manager metadata: %w", err)
	}
	if recorded.PID <= 1 || recorded.PID == os.Getpid() {
		return nil, fmt.Errorf("refusing protocol takeover for recorded pid %d", recorded.PID)
	}
	alive, err := r.processAlive(ctx, recorded)
	if err != nil {
		return nil, fmt.Errorf("checking outdated session manager pid %d: %w", recorded.PID, err)
	}
	if !alive {
		return nil, fmt.Errorf("outdated session manager pid %d is not alive", recorded.PID)
	}

	client, status, err := r.dialStartupStatus(ctx, layout)
	if err != nil {
		return nil, fmt.Errorf("checking outdated session manager status: %w", err)
	}
	defer client.Close()
	if !status.StartupHealthy ||
		status.PID != recorded.PID ||
		status.InstanceID != recorded.InstanceID ||
		status.Namespace != recorded.Namespace {
		return nil, nil
	}
	if compatibilityErr := CheckCompatibility(status.ProtocolStatus, r.options.WrapperVersion); compatibilityErr != nil {
		var current *ProtocolUpgradeError
		if !errors.As(compatibilityErr, &current) || !current.RequiresReplacement() {
			return nil, compatibilityErr
		}

	} else if !forceAfterRejection {
		return nil, nil
	}

	// The replacement gate sits here, above BOTH exits: the graceful drain and
	// the fenced SIGTERM. Placing it on the signal alone would have been
	// theatre — a drained daemon still exits, and a Claude host's PTY master
	// lives in that process, so draining kills the hosted sessions exactly as
	// terminating does.
	if refusal := r.guardManagerReplacement(
		ctx, layout, recorded, r.options.WrapperVersion, r.protocolUpgradeReason(status),
	); refusal != nil {
		var typed *ManagerReplacementRefusedError
		if errors.As(refusal, &typed) {
			r.logManagerReplacementRefusal(layout, typed)
		}
		return nil, refusal
	}

	// v2+ daemons can drain themselves. The v1 protocol that introduced the
	// regression has no such route, so a 404/other control rejection falls
	// through to the existing fenced SIGTERM/SIGKILL takeover.
	if _, err := r.replacement(ctx, layout, recorded); err != nil {
		return nil, err
	}
	if err := client.DoRequest(ctx, "POST", "/v1/drain", nil, nil, true); err == nil {
		released, waitErr := r.waitForInstanceLockRelease(ctx, layout, staleInstanceTerminateGrace)
		if waitErr != nil {
			return nil, waitErr
		}
		if released {
			return &protocolUpgradeTakeover{old: recorded, drained: true}, nil
		}
	}
	current, err := startupEndpoint(layout)
	if err != nil {
		return nil, fmt.Errorf("re-reading outdated session manager metadata: %w", err)
	}
	if !sameEndpointInstance(recorded, current) {
		return nil, nil
	}
	lockHeld, err = r.lockHeld(ctx, layout)
	if err != nil {
		return nil, err
	}
	if !lockHeld {
		return nil, nil
	}
	if err := r.terminateStaleInstance(ctx, layout, recorded); err != nil {
		return nil, err
	}
	return &protocolUpgradeTakeover{old: recorded}, nil
}

func (r *startupRuntime) takeOverUnresponsiveManager(
	ctx context.Context,
	layout StartupLayout,
) (*Client, *staleInstanceTakeover, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("health-checking session manager lock holder: %w", err)
	}
	lockHeld, err := r.lockHeld(ctx, layout)
	if err != nil {
		return nil, nil, err
	}
	if !lockHeld {
		return nil, nil, nil
	}

	recorded, err := startupEndpoint(layout)
	if errors.Is(err, ErrManagerNotRunning) {
		// A new daemon may still be reconciling before publishing its endpoint.
		// Let ordinary launch arbitration and polling discover that instance.
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("inspecting lock-holding session manager: %w", err)
	}
	if recorded.PID <= 1 {
		return nil, nil, fmt.Errorf(
			"refusing to terminate invalid recorded session manager pid %d",
			recorded.PID,
		)
	}
	if recorded.PID == os.Getpid() {
		return nil, nil, fmt.Errorf(
			"refusing to terminate the current process for unresponsive session manager pid %d",
			recorded.PID,
		)
	}
	alive, err := r.processAlive(ctx, recorded)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"checking recorded session manager pid %d: %w",
			recorded.PID,
			err,
		)
	}
	if !alive {
		return nil, nil, fmt.Errorf(
			"session manager lock is held but recorded pid %d is not alive; refusing an unsafe takeover",
			recorded.PID,
		)
	}

	// The first wrapper connection may have raced a transient startup phase.
	// Require a second bounded endpoint round-trip before signaling a live PID.
	if client, status, healthErr := r.dialStartupStatus(ctx, layout); healthErr == nil {
		if status.StartupHealthy {
			if err := CheckCompatibility(status.ProtocolStatus, r.options.WrapperVersion); err != nil {
				client.Close()
				return nil, nil, err
			}
			return client, nil, nil
		}
		client.Close()
		if !recorded.StartedAt.IsZero() &&
			time.Now().UTC().Before(recorded.StartedAt.Add(startupHealthyDeadline)) {
			return nil, nil, nil
		}
	} else if startupFatal(healthErr) {
		return nil, nil, healthErr
	}
	current, err := startupEndpoint(layout)
	if err != nil {
		return nil, nil, fmt.Errorf("re-reading lock holder metadata: %w", err)
	}
	if !sameEndpointInstance(recorded, current) {
		// Another wrapper replaced or refreshed the daemon between checks.
		return nil, nil, nil
	}
	lockHeld, err = r.lockHeld(ctx, layout)
	if err != nil {
		return nil, nil, err
	}
	if !lockHeld {
		return nil, nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("health-checking session manager lock holder: %w", err)
	}
	alive, err = r.processAlive(ctx, recorded)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"rechecking recorded session manager pid %d before takeover: %w",
			recorded.PID,
			err,
		)
	}
	if !alive {
		return nil, nil, fmt.Errorf(
			"session manager pid %d exited while its singleton lock remained held",
			recorded.PID,
		)
	}
	// The same gate as the protocol path. An unresponsive control endpoint says
	// nothing about the PTYs the process still holds: the daemon in the
	// 2026-09-06 incident was answering /v1/status right up to the SIGTERM that
	// hung up sixteen sessions, and one that had stopped answering would have
	// held them just as hard.
	if refusal := r.guardManagerReplacement(
		ctx, layout, recorded, r.options.WrapperVersion, ManagerReplacementUnresponsiveHolder,
	); refusal != nil {
		var typed *ManagerReplacementRefusedError
		if errors.As(refusal, &typed) {
			r.logManagerReplacementRefusal(layout, typed)
		}
		return nil, nil, refusal
	}
	if err := r.terminateStaleInstance(ctx, layout, recorded); err != nil {
		return nil, nil, err
	}
	return nil, &staleInstanceTakeover{old: recorded}, nil
}

func (r *startupRuntime) terminateStaleInstance(
	ctx context.Context,
	layout StartupLayout,
	recorded Endpoint,
) error {
	capability, err := r.replacement(ctx, layout, recorded)
	if err != nil {
		return err
	}
	if err := capability.Terminate(ctx, false); err != nil {
		return fmt.Errorf(
			"terminating unresponsive session manager pid %d: %w",
			recorded.PID,
			&startupSourceError{cause: err},
		)
	}
	released, err := r.waitForInstanceLockRelease(ctx, layout, staleInstanceTerminateGrace)
	if err != nil {
		return err
	}
	if released {
		return nil
	}

	current, err := startupEndpoint(layout)
	if err != nil {
		return fmt.Errorf("verifying stale session manager before forced termination: %w", err)
	}
	if !sameEndpointInstance(recorded, current) {
		return fmt.Errorf(
			"session manager holder changed while waiting for pid %d to terminate; refusing a forced signal",
			recorded.PID,
		)
	}
	alive, err := r.processAlive(ctx, recorded)
	if err != nil {
		return fmt.Errorf(
			"rechecking recorded session manager pid %d: %w",
			recorded.PID,
			err,
		)
	}
	if !alive {
		return fmt.Errorf(
			"recorded session manager pid %d exited but its singleton lock remains held",
			recorded.PID,
		)
	}
	if err := r.validateReplacement(ctx, layout, recorded, capability); err != nil {
		return err
	}
	if err := capability.Terminate(ctx, true); err != nil {
		return fmt.Errorf(
			"force-terminating unresponsive session manager pid %d: %w",
			recorded.PID,
			&startupSourceError{cause: err},
		)
	}
	released, err = r.waitForInstanceLockRelease(ctx, layout, staleInstanceForceGrace)
	if err != nil {
		return err
	}
	if !released {
		return fmt.Errorf(
			"session manager pid %d did not release its singleton lock after forced termination",
			recorded.PID,
		)
	}
	return nil
}

func (r *startupRuntime) waitForInstanceLockRelease(
	ctx context.Context,
	layout StartupLayout,
	timeout time.Duration,
) (bool, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(staleInstancePollInterval)
	defer ticker.Stop()
	for {
		held, err := r.lockHeld(ctx, layout)
		if err != nil {
			return false, err
		}
		if !held {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf(
				"waiting for stale session manager lock release: %w",
				ctx.Err(),
			)
		case <-timer.C:
			return false, nil
		case <-ticker.C:
		}
	}
}

func sameEndpointInstance(left, right Endpoint) bool {
	return left.Protocol == right.Protocol &&
		left.ProtocolVersion == right.ProtocolVersion &&
		left.DaemonVersion == right.DaemonVersion &&
		left.Namespace == right.Namespace &&
		left.InstanceID == right.InstanceID &&
		left.SocketPath == right.SocketPath &&
		left.PID == right.PID &&
		left.StartedAt.Equal(right.StartedAt)
}

func (r *startupRuntime) logStaleInstanceTakeover(
	layout StartupLayout,
	old Endpoint,
	replacement StartupStatus,
) {
	logger := r.options.RecordEvent
	if logger == nil {
		return
	}
	logger("stale_instance_takeover", map[string]any{
		"reason":       "control_endpoint_unresponsive",
		"old_pid":      old.PID,
		"new_pid":      replacement.PID,
		"old_instance": old.InstanceID,
		"new_instance": replacement.InstanceID,
	})
}

func (r *startupRuntime) logProtocolUpgrade(
	layout StartupLayout,
	takeover *protocolUpgradeTakeover,
	replacement StartupStatus,
) {
	if takeover == nil {
		return
	}
	logger := r.options.RecordEvent
	if logger == nil {
		return
	}
	logger("protocol_upgrade_takeover", map[string]any{
		"old_pid":              takeover.old.PID,
		"new_pid":              replacement.PID,
		"old_protocol_version": endpointProtocolVersion(takeover.old),
		"new_protocol_version": replacement.ProtocolVersion,
		"old_daemon_version":   takeover.old.DaemonVersion,
		"new_daemon_version":   replacement.DaemonVersion,
		"graceful_drain":       takeover.drained,
	})
}

// protocolUpgradeReason distinguishes the two shapes of "this daemon must be
// replaced to converge" at the point the takeover was about to happen.
func (r *startupRuntime) protocolUpgradeReason(status StartupStatus) ManagerReplacementReason {
	if BuildSkew(status.DaemonVersion, r.options.WrapperVersion) != nil {
		return ManagerReplacementBuildSkew
	}
	return ManagerReplacementProtocolSkew
}

// logManagerBuildSkew records the handshake fact an equal protocol version
// hides. Without it the operator log shows a takeover with no reason: both
// sides report v7 and the only difference is the build string, which is exactly
// the recovery case surfaced as an unexplained acknowledgement
// timeout.
func (r *startupRuntime) logManagerBuildSkew(layout StartupLayout, upgrade *ProtocolUpgradeError) {
	if upgrade == nil {
		return
	}
	logger := r.options.RecordEvent
	if logger == nil {
		return
	}
	logger("manager_build_skew_detected", map[string]any{
		"old_daemon_version": upgrade.DaemonVersion,
		"new_daemon_version": upgrade.WrapperVersion,
		"protocol_version":   upgrade.ActualVersion,
	})
}

func endpointProtocolVersion(metadata Endpoint) int {
	if metadata.ProtocolVersion > 0 {
		return metadata.ProtocolVersion
	}
	return ProtocolVersionFromIdentifier(metadata.Protocol)
}
