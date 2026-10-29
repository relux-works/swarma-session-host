package sessionclient

import (
	"errors"
	"fmt"
	"strings"
)

var ErrManagerNotRunning = errors.New("session manager is not running")

// ErrManagerProtocolUpgradeRequired marks a daemon that cannot safely accept
// this wrapper's control requests. Callers must never render an older daemon's
// raw JSON-decoder diagnostics to an operator.
var ErrManagerProtocolUpgradeRequired = errors.New("session manager protocol upgrade is required")

// ProtocolUpgradeError is deliberately secret- and decoder-free. An
// old daemon may reject a newly added field before its status can advertise a
// comparable version; ActualVersion is therefore zero in that fallback case.
//
// BuildSkew widens it to the second shape of the same fact: a daemon on the
// CURRENT protocol version built from a different tree. That skew used to be
// undetectable at the handshake, so a wrapper negotiated cleanly and only found
// out when a route behaved differently — at best an acknowledgement timeout,
// at worst silently. It is routed through this type, and therefore through the
// same replacement gate, rather than through a second one.
type ProtocolUpgradeError struct {
	ActualVersion int
	DaemonVersion string
	// BuildSkew marks an equal-protocol, unequal-build daemon.
	BuildSkew bool
	// WrapperVersion is this build's own version string, and is empty when this
	// build does not know it.
	WrapperVersion string
}

// RequiresReplacement reports whether converging on this wrapper needs the
// running daemon replaced. A daemon that is merely NEWER than this wrapper does
// not: replacing it would be a downgrade, and the caller refuses instead.
func (e *ProtocolUpgradeError) RequiresReplacement() bool {
	if e == nil {
		return false
	}
	return e.BuildSkew || e.ActualVersion < ProtocolVersion
}

func (e *ProtocolUpgradeError) Error() string {
	if e == nil {
		return "Session Manager protocol upgrade is required; rerun the wrapper so it can restart tb-sessiond, then retry"
	}
	if e.BuildSkew {
		return fmt.Sprintf(
			"Session Manager %s and this wrapper %s share control protocol v%d but are different builds; restart tb-sessiond from an outside shell, then retry",
			e.DaemonVersion,
			e.WrapperVersion,
			ProtocolVersion,
		)
	}
	if e.ActualVersion <= 0 {
		return "Session Manager protocol upgrade is required; rerun the wrapper so it can restart tb-sessiond, then retry"
	}
	if strings.TrimSpace(e.DaemonVersion) == "" {
		return fmt.Sprintf(
			"Session Manager protocol v%d is older than required v%d; rerun the wrapper to restart tb-sessiond, then retry",
			e.ActualVersion,
			ProtocolVersion,
		)
	}
	return fmt.Sprintf(
		"Session Manager %s uses protocol v%d but this wrapper requires v%d; rerun the wrapper to restart tb-sessiond, then retry",
		e.DaemonVersion,
		e.ActualVersion,
		ProtocolVersion,
	)
}

func (e *ProtocolUpgradeError) Unwrap() error {
	return ErrManagerProtocolUpgradeRequired
}

// PublicError is the only typed manager failure shape the control plane may
// expose to clients. Manager operations and adapters return it for capability
// and policy failures whose code and message are safe outside the manager
// boundary. The control client reconstructs the same type from non-2xx error
// envelopes so wrappers retain the actionable cause instead of raw JSON.
type PublicError struct {
	Code       string
	Message    string
	StatusCode int
}

func (e *PublicError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("%s (%d): %s", e.Code, e.StatusCode, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}
