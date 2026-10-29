package sessionmanager

import (
	"context"
	"time"
)

type SessionState string

type CodexAutoContinueOptions struct {
	Enabled          bool   `json:"enabled"`
	DelaySeconds     int    `json:"delay_seconds"`
	MaxNudgesPerTurn int    `json:"max_nudges_per_turn"`
	Prompt           string `json:"prompt"`
}

type SessionLiveness string

type CodexCuratorHomeCopy struct {
	Profile        string   `json:"profile"`
	FragmentSHA256 string   `json:"fragment_sha256"`
	LockSHA256     string   `json:"lock_sha256"`
	Absent         []string `json:"absent,omitempty"`
}

type SessionEvictionNotice struct {
	ByProvider string    `json:"by_provider"`
	At         time.Time `json:"at"`
	Reason     string    `json:"reason,omitempty"`
}

type MaterializationSource string

type ProviderGracefulExit interface {
	RequestExit(context.Context) error
	WaitExit(context.Context) (bool, error)
}

type LaunchPlanSource struct {
	Kind           string   `json:"kind"`
	Path           string   `json:"path"`
	Scope          string   `json:"scope,omitempty"`
	EnabledServers []string `json:"enabled_servers,omitempty"`
}

type CuratorProvenanceState string

type ClientToolPolicy struct {
	Sandbox  string `json:"sandbox,omitempty"`
	Approval string `json:"approval,omitempty"`
}

type LaunchVariants struct {
	Interactive   LaunchArgv `json:"interactive"`
	ManagedHost   LaunchArgv `json:"managed_host"`
	ManagedClient LaunchArgv `json:"managed_client"`
}

type ProviderHostState string

type BuilderGatewayEndpoint struct {
	// Command and Args spawn the shim.
	Command string
	Args    []string
	// SocketPath is the manager-owned socket the shim connects to. It is reported
	// separately so a launch boundary can grant access to exactly this path.
	SocketPath string
	// Token authenticates the shim to the manager. A gateway that served an
	// unauthenticated caller would let any process on the machine write events
	// into a run's trace.
	Token string
}

type BuilderConfinementResult struct {
	// Mechanism identifies the confinement that was installed, for the audit
	// trail and the signed claim. An empty mechanism is a refusal.
	Mechanism string
	// EnforcedRoots are the repo-relative paths the launched process may read.
	EnforcedRoots []string
}

type MaterializationState string

type LaunchArgv struct {
	Kind string   `json:"kind,omitempty"`
	Argv []string `json:"argv"`
}
