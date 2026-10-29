package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"syscall"
)

type sessionLivenessObservation struct {
	State  SessionLiveness
	Detail string
}

var probeProviderProcess = processAlive
var dialProviderTransport = func(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{Timeout: transportProbeTimeout}).DialContext(ctx, network, address)
}

// Original physical liveness source; a failed read is unknown, never consent to
// stop. Snapshot structural validity remains with the committed-record codec.
func observeHostProviderLiveness(ctx context.Context, r RunnerRecord) (sessionLivenessObservation, bool) {
	if r.Provider != "codex" && r.Provider != "claude" {
		return sessionLivenessObservation{}, false
	}
	if e := validateRunnerSnapshot(r.ProviderHost); e != nil {
		return sessionLivenessObservation{}, false
	}
	pid := 0
	switch {
	case r.Provider == "codex" && r.ProviderHost.Snapshot.Codex != nil:
		pid = r.ProviderHost.Snapshot.Codex.PID
	case r.Provider == "claude" && r.ProviderHost.Snapshot.Claude != nil:
		pid = r.ProviderHost.Snapshot.Claude.PID
	default:
		return sessionLivenessObservation{}, false
	}
	state, detail := SessionLivenessDead, "provider process is absent"
	if pid > 0 {
		alive, e := probeProviderProcess(pid)
		if e != nil {
			state = SessionLivenessUnknown
			detail = fmt.Sprintf("provider process pid %d could not be read: %v", pid, e)
		} else if alive {
			return sessionLivenessObservation{SessionLivenessLive, fmt.Sprintf("provider process pid %d is live", pid)}, true
		}
	}
	transport, transportDetail := observeHostProviderTransport(ctx, r)
	if transport == SessionLivenessLive {
		return sessionLivenessObservation{transport, transportDetail}, true
	}
	if state == SessionLivenessUnknown || transport == SessionLivenessUnknown {
		var parts []string
		if state == SessionLivenessUnknown {
			parts = append(parts, detail)
		}
		if transport == SessionLivenessUnknown {
			parts = append(parts, transportDetail)
		}
		return sessionLivenessObservation{SessionLivenessUnknown, strings.Join(parts, "; ")}, true
	}
	return sessionLivenessObservation{SessionLivenessDead, detail + "; " + transportDetail}, true
}
func observeHostProviderTransport(ctx context.Context, r RunnerRecord) (SessionLiveness, string) {
	if r.Provider == "claude" {
		return SessionLivenessDead, "provider has no independently reconnectable socket"
	}
	if r.ProviderHost.Snapshot.Codex == nil {
		return SessionLivenessUnknown, "provider socket metadata could not be read"
	}
	address := r.ProviderHost.Snapshot.Codex.URL
	parsed, e := url.Parse(strings.TrimSpace(address))
	if e != nil || parsed.Host == "" {
		return SessionLivenessUnknown, fmt.Sprintf("provider socket address could not be read: %q", address)
	}
	connection, e := dialProviderTransport(ctx, "tcp", parsed.Host)
	if e == nil {
		_ = connection.Close()
		return SessionLivenessLive, "provider socket is live at " + parsed.Host
	}
	if errors.Is(e, syscall.ECONNREFUSED) || errors.Is(e, os.ErrNotExist) {
		return SessionLivenessDead, "provider socket is absent at " + parsed.Host
	}
	return SessionLivenessUnknown, fmt.Sprintf("provider socket %s could not be read: %v", parsed.Host, e)
}
