package sessionclient

import (
	"fmt"
	"strings"
)

// The two refusal codes this gate can return.
//
// They are separate because the remedies are different, not because the danger
// is. A hosted caller cannot restart the manager at all — the restart kills the
// shell issuing it — while an outside caller merely has to decide what happens
// to the sessions it is about to end.
const (
	// ManagerReplacementRefusedHostedCaller marks a replacement requested from
	// inside a session the daemon itself hosts.
	ManagerReplacementRefusedHostedCaller = "session_manager_skew_hosted_caller"
	// ManagerReplacementRefusedLiveHostedSessions marks a replacement requested
	// from an outside shell while the daemon still holds live provider PTYs.
	ManagerReplacementRefusedLiveHostedSessions = "session_manager_skew_live_hosted_sessions"
)

// ManagerReplacementReason names why a replacement was about to happen.
type ManagerReplacementReason string

const (
	// ManagerReplacementProtocolSkew: the daemon speaks an older control
	// protocol than this wrapper requires.
	ManagerReplacementProtocolSkew ManagerReplacementReason = "protocol_skew"
	// ManagerReplacementBuildSkew: equal protocol, different build string.
	ManagerReplacementBuildSkew ManagerReplacementReason = "build_skew"
	// ManagerReplacementUnresponsiveHolder: the lock holder stopped answering.
	ManagerReplacementUnresponsiveHolder ManagerReplacementReason = "unresponsive_holder"
	// ManagerReplacementOperatorRestart: an explicit `session restart`.
	ManagerReplacementOperatorRestart ManagerReplacementReason = "operator_restart"
)

// HostedCallerReason names the evidence that put the caller inside a hosted
// session. Each value is a fact read from the environment or the process table,
// never a caller assertion.
type HostedCallerReason string

const (
	// HostedCallerManagedSessionEnv: the process carries the marker a hosted
	// PRIMARY Claude launch exports into its PTY.
	HostedCallerManagedSessionEnv HostedCallerReason = "managed_session_env"
	// HostedCallerHostedRunOwner: the process carries a RUN id the daemon
	// records as a live hosted owner session.
	HostedCallerHostedRunOwner HostedCallerReason = "hosted_run_owner"
	// HostedCallerProviderDescendant: the process is a descendant of a provider
	// process the daemon launched, which is the shape a tool shell has.
	HostedCallerProviderDescendant HostedCallerReason = "hosted_provider_descendant"
)

// HostedCallerEvidence is why this process is considered to be running inside
// a manager-hosted session.
type HostedCallerEvidence struct {
	Reason      HostedCallerReason
	SessionID   string
	OwnerKind   string
	OwnerID     string
	ProviderPID int
}

// HostedSessionSummary is one session whose provider host would die with the
// daemon. It carries no provider handle: the identity, the owner and the
// process are all an operator needs to decide what to end.
//
// Membership IS the liveness claim. A session appears here unless its process
// was PROVEN absent, so the list carries no live flag: an unreadable record or
// an unreadable process table keeps the session, because a failure to read is
// not evidence that nothing is there. The reason it could not be read goes in
// Detail, which is how an operator tells "alive" from "unproven".
type HostedSessionSummary struct {
	SessionID string
	Provider  string
	OwnerKind string
	OwnerID   string
	PID       int
	// Detail explains an unproven observation, and is empty for a plain
	// process-is-alive reading.
	Detail string
}

// ManagerReplacementRefusedError is the typed refusal returned instead of
// replacing a running Session Manager daemon.
//
// The incident it exists for: a hosted Claude session ran `make
// install-frontend` and its very next CLI call performed the automatic
// protocol-upgrade takeover. The old daemon holds the PTY master of every
// hosted Claude session in its own process, so SIGTERM to it SIGHUPed the
// caller and sixteen hosted run owners at once. The caller was killed by its
// own command. Nothing about that is recoverable by retrying, so this refusal
// stages nothing, signals nothing, and leaves the old daemon exactly as it was.
type ManagerReplacementRefusedError struct {
	Code string
	// Reason names WHY the daemon would have been replaced. Without it the
	// message has to guess from the version numbers, and an unresponsive
	// holder — which is neither a protocol nor a build disagreement — reads as
	// a build skew between two empty strings.
	Reason             ManagerReplacementReason
	HostedCaller       *HostedCallerEvidence
	LiveSessions       []HostedSessionSummary
	DaemonPID          int
	OldDaemonVersion   string
	NewDaemonVersion   string
	OldProtocolVersion int
	NewProtocolVersion int
}

// ManagerReplacementRefusalCode satisfies the interface consumers outside this
// package match on. It exists so a classifier does not have to import the
// session-manager package to recognise the refusal.
func (e *ManagerReplacementRefusedError) ManagerReplacementRefusalCode() string {
	if e == nil {
		return ""
	}
	return e.Code
}

// AutonomousRecovery reports whether a runtime may retry this by itself. It is
// always false: the remedy is a human ending or detaching sessions, and a
// successor run would meet the identical refusal from the identical shell.
func (e *ManagerReplacementRefusedError) AutonomousRecovery() bool { return false }

func (e *ManagerReplacementRefusedError) Error() string {
	if e == nil {
		return ""
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s: ", e.Code)
	builder.WriteString(e.skewSentence())
	if e.HostedCaller != nil {
		fmt.Fprintf(
			&builder,
			" This process runs inside manager-hosted session %s (%s); replacing tb-sessiond pid %d closes the provider PTY of every hosted Claude session, including this one.",
			e.hostedCallerSessionLabel(),
			e.HostedCaller.Reason,
			e.DaemonPID,
		)
	} else {
		fmt.Fprintf(
			&builder,
			" tb-sessiond pid %d still hosts %d live provider session(s) whose PTY dies with it: %s.",
			e.DaemonPID,
			len(e.LiveSessions),
			e.liveSessionLabels(),
		)
	}
	builder.WriteString(" ")
	builder.WriteString(e.Remediation())
	return builder.String()
}

// Remediation is the operator route out, and it is different per branch on
// purpose. A hosted caller has no route that does not start with leaving the
// session.
func (e *ManagerReplacementRefusedError) Remediation() string {
	if e == nil {
		return ""
	}
	if e.HostedCaller != nil {
		return "End or detach this session and restart the manager from an outside shell: " +
			"`task-board session restart` (add --stop-hosted to end the remaining hosted sessions first). " +
			"Never run `curator global upgrade` from inside a managed session."
	}
	return "End the listed sessions first, or restart from this outside shell with " +
		"`task-board session restart --stop-hosted`, which stops them before replacing the daemon."
}

func (e *ManagerReplacementRefusedError) skewSentence() string {
	switch e.Reason {
	case ManagerReplacementUnresponsiveHolder:
		return fmt.Sprintf(
			"the running Session Manager build %q is holding this board's manager lock without answering its control endpoint, so it would have been replaced",
			e.OldDaemonVersion,
		)
	case ManagerReplacementBuildSkew:
		return fmt.Sprintf(
			"the running Session Manager build %q differs from this wrapper's build %q on the same control protocol v%d, so it must be replaced to converge",
			e.OldDaemonVersion,
			e.NewDaemonVersion,
			e.NewProtocolVersion,
		)
	default:
		return fmt.Sprintf(
			"the running Session Manager build %q speaks control protocol v%d but this wrapper build %q requires v%d, so it must be replaced to converge",
			e.OldDaemonVersion,
			e.OldProtocolVersion,
			e.NewDaemonVersion,
			e.NewProtocolVersion,
		)
	}
}

func (e *ManagerReplacementRefusedError) hostedCallerSessionLabel() string {
	if e.HostedCaller == nil {
		return "unknown"
	}
	label := strings.TrimSpace(e.HostedCaller.SessionID)
	if label == "" {
		label = "unknown"
	}
	if e.HostedCaller.OwnerKind != "" && e.HostedCaller.OwnerID != "" {
		label += fmt.Sprintf(" owner=%s/%s", e.HostedCaller.OwnerKind, e.HostedCaller.OwnerID)
	}
	return label
}

func (e *ManagerReplacementRefusedError) liveSessionLabels() string {
	labels := make([]string, 0, len(e.LiveSessions))
	for _, session := range e.LiveSessions {
		label := fmt.Sprintf("%s (%s", session.SessionID, session.Provider)
		if session.OwnerKind != "" && session.OwnerID != "" {
			label += fmt.Sprintf(" owner=%s/%s", session.OwnerKind, session.OwnerID)
		}
		if session.PID > 0 {
			label += fmt.Sprintf(" pid=%d", session.PID)
		}
		if detail := strings.TrimSpace(session.Detail); detail != "" {
			label += ", " + detail
		}
		labels = append(labels, label+")")
	}
	if len(labels) == 0 {
		return "none readable"
	}
	return strings.Join(labels, "; ")
}
