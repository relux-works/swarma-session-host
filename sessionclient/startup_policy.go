package sessionclient

import (
	"context"
	"errors"
)

// guardManagerReplacement retains the original fixed ordering: hosted caller
// first, then live/unproven sessions. Real observations come from the source;
// missing/unreadable input never becomes an empty inventory or permission.
func (r *startupRuntime) guardManagerReplacement(ctx context.Context, l StartupLayout, recorded Endpoint, version string, reason ManagerReplacementReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.options.Source == nil {
		return ErrStartupAuthorityUnavailable
	}
	sessions, readErr := r.options.Source.HostedSessions(ctx, l)
	caller, callerErr := r.options.Source.Caller(ctx, sessions)
	if callerErr != nil {
		return ErrStartupObservation
	}
	if readErr != nil {
		sessions = []HostedSessionSummary{{SessionID: "unknown", Provider: "unknown", Detail: "session records could not be read"}}
	}
	refusal := &ManagerReplacementRefusedError{Reason: reason, HostedCaller: caller, LiveSessions: sessions, DaemonPID: recorded.PID, OldDaemonVersion: recorded.DaemonVersion, NewDaemonVersion: version, OldProtocolVersion: endpointProtocolVersion(recorded), NewProtocolVersion: ProtocolVersion}
	if caller != nil {
		refusal.Code = ManagerReplacementRefusedHostedCaller
		return refusal
	}
	if len(sessions) > 0 {
		refusal.Code = ManagerReplacementRefusedLiveHostedSessions
		return refusal
	}
	// Deliberately no raw context bool/operator flag here: a real verified source
	// must first perform its authorized hosted stop, then reobserve actual absence.
	r.reason = reason
	return nil
}
func (r *startupRuntime) guardManagerReplacementForUpgrade(ctx context.Context, l StartupLayout, upgrade *ProtocolUpgradeError) error {
	reason := ManagerReplacementProtocolSkew
	if upgrade != nil && upgrade.BuildSkew {
		reason = ManagerReplacementBuildSkew
	}
	recorded, err := startupEndpoint(l)
	if err != nil {
		if startupFatal(err) {
			return err
		}
		recorded = Endpoint{}
		if upgrade != nil {
			recorded.DaemonVersion = upgrade.DaemonVersion
			recorded.ProtocolVersion = upgrade.ActualVersion
		}
	}
	refusal := r.guardManagerReplacement(ctx, l, recorded, r.options.WrapperVersion, reason)
	var typed *ManagerReplacementRefusedError
	if errors.As(refusal, &typed) {
		r.logManagerReplacementRefusal(l, typed)
	}
	return refusal
}
func (r *startupRuntime) logManagerReplacementRefusal(_ StartupLayout, e *ManagerReplacementRefusedError) {
	if r.options.RecordEvent != nil && e != nil {
		r.options.RecordEvent("manager_replacement_refused", map[string]any{"code": e.Code, "reason": e.Reason, "daemon_pid": e.DaemonPID, "old_daemon_version": e.OldDaemonVersion, "new_daemon_version": e.NewDaemonVersion})
	}
}

// ConnectOrStartAndAttach retains the original one-upgrade retry after a typed
// unknown-field rejection. Request/target are the consumer's unchanged wire
// DTOs; transporting them grants no owner or launch authority.
func ConnectOrStartAndAttach(ctx context.Context, l StartupLayout, o StartupOptions, request, target any) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultConnectOrStartTimeout)
		defer cancel()
	}
	client, err := ConnectOrStart(ctx, l, o)
	if err != nil {
		return nil, err
	}
	err = client.DoRequest(ctx, "POST", "/v1/sessions/attach", request, target, true)
	if !errors.Is(err, ErrManagerProtocolUpgradeRequired) {
		if err != nil {
			client.Close()
			return nil, err
		}
		return client, nil
	}
	client.Close()
	r := &startupRuntime{options: o}
	if err = r.guardManagerReplacementForUpgrade(ctx, l, &ProtocolUpgradeError{}); err != nil {
		return nil, err
	}
	takeover, err := r.takeOverManagerForProtocolUpgrade(ctx, l, true)
	if err != nil {
		return nil, err
	}
	if takeover == nil {
		return nil, ErrStartupEndpointChanged
	}
	client, err = r.connectAfterManagerUpgrade(ctx, l, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if o.Source == nil {
			return ErrStartupAuthorityUnavailable
		}
		if err := o.Source.Launch(ctx, l); err != nil {
			return &startupSourceError{cause: err}
		}
		return nil
	}, takeover)
	if err != nil {
		return nil, err
	}
	if err = client.DoRequest(ctx, "POST", "/v1/sessions/attach", request, target, true); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}
