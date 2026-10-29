package sessionclient

import (
	"context"
	"errors"
	"time"
)

var ErrStartupAuthorityUnavailable = errors.New("session manager startup authority is unavailable")

// StartupLayout identifies the original endpoint/token/namespace and lock.
// Namespace, PID and endpoint bytes are comparison data, never grants.
type StartupLayout struct {
	EndpointFiles
	LockFile   string
	SocketPath string
}

type StartupStatus struct {
	ProtocolStatus
	Namespace  string    `json:"board_fingerprint"`
	InstanceID string    `json:"instance_id"`
	PID        int       `json:"pid"`
	StartedAt  time.Time `json:"started_at"`
}

type StartupProcessState string

const (
	StartupProcessUnknown StartupProcessState = "unknown"
	StartupProcessPresent StartupProcessState = "present"
	StartupProcessAbsent  StartupProcessState = "absent"
)

// StartupSource is the original external-fact/effect closure. Composition must
// supply actual verified authority, not infer it from owner text, PID, executable,
// native SID or the shared Unix user. Observation methods must be read-only.
// HostedSessions retains unreadable/unproven sessions; Caller performs actual
// environment/ancestry observation. Launch preserves raising-side SessionStart
// and a2a identity binding. A missing supplier never means empty or authorized.
type StartupSource interface {
	LockHeld(context.Context, StartupLayout) (bool, error)
	Process(context.Context, Endpoint) (StartupProcessState, error)
	HostedSessions(context.Context, StartupLayout) ([]HostedSessionSummary, error)
	Caller(context.Context, []HostedSessionSummary) (*HostedCallerEvidence, error)
	Launch(context.Context, StartupLayout) error
	Replacement(context.Context, StartupLayout, Endpoint, ManagerReplacementReason) (StartupReplacement, error)
}

// StartupReplacement is an already-authorized exact-holder operation. Each
// effect revalidates retained process birth/current-holder proof; it is not a
// raw-PID signal helper. Only an explicitly owned/authorized PID may be signaled.
type StartupReplacement interface {
	Endpoint() Endpoint
	Terminate(context.Context, bool) error
}

type StartupOptions struct {
	WrapperVersion string
	Source         StartupSource
	// RecordEvent receives the original operator event name/fields.
	RecordEvent func(string, map[string]any)
}
