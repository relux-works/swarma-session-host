package sessionmanager

import (
	"context"
	"time"
)

// Accepted neutral host construction and lifecycle ports. Compatibility wire
// declarations retain their legacy names, fields and JSON tags separately.
type AttachRequest struct {
	// ExpectedOwnerLease pins a trusted caller's prior owner-slot observation.
	// Nil retains internal unpinned admission; a pointer to zero asserts absence.
	// A nonzero pin must match the current exact controller before any launch.
	// This carries no caller, provider-launch or materialization authority.
	ExpectedOwnerLease   *ControllerLease
	Owner                OwnerKey
	Provider             string
	Launch               LaunchSpec
	ProjectDir           string
	NamespaceFingerprint string
	Resume               bool
	ResumeSessionID      string
	Adopt                bool
	MaterializationCtxID string
}

type Attachment struct {
	AttachmentID     string
	ManagerEndpoint  string
	ClientHome       string
	CodexThreadID    string
	ClientToolPolicy ClientToolPolicy
	Session          SessionView
}

type BuilderConfinementRequest struct {
	CtxID              string
	RunID              string
	Owner              OwnerKey
	Provider           ContextProvider
	Allowlist          ContextBuilderAllowlist
	RequestedPathSpecs []string
	Gateway            BuilderGatewayEndpoint
	IsolationConfigDir string
}

type BuilderIngestionConfiner interface {
	ConfineBuilderIngestion(context.Context, RunnerHost, BuilderConfinementRequest) (BuilderConfinementResult, error)
}

type ContextPayloadMode string

type FrozenReceipt struct {
	Build  string
	SHA256 string
	Path   string
}

type HostConfig interface {
	Snapshot(context.Context) (HostSettings, error)
}

type HostOptions struct {
	BuilderGateways BuilderGatewayLifecycle
	OwnerRuns       CapturedOwnerRunner
	Layout          Layout
	Config          HostConfig
	Codec           ForkRunnerCodec
	Goals           GoalSource
	Binding         GoalBinding
	GoalBindings    GoalBindingFactory
	Gate            GoalGate
	Contexts        ContextAuthority
	Notices         NoticeStore
	Workspace       WorkspaceResolver
	Factories       []ProviderHostFactory
}

type HostSettings struct {
	DaemonVersion  string
	GoalAckTimeout time.Duration
	LocalPolicy    LocalPolicy
	LaunchDefaults LaunchDefaults
}

type LastGood struct {
	Schema     int
	Sequence   uint64
	Frozen     FrozenReceipt
	AttemptID  string
	Surface    string
	By         string
	ReadyAt    string
	AdmittedAt string
}

type LaunchDefaults struct {
	ToolPolicy       ClientToolPolicy
	RequiredEnvNames []string
}

type Layout struct {
	Namespace                 NamespaceKey
	StateDir                  string
	SessionsDir               string
	QuarantineDir             string
	MaterializationsDir       string
	MaterializationQuarantine string
	LogsDir                   string
	OperatorLogFile           string
	AttachmentsDir            string
	EndpointFile              string
	TokenFile                 string
	LockFile                  string
	SocketPath                string
	EnforcementKeyFile        string
	BuilderEnforcementDir     string
	NoticesDir                string
}

type LocalPolicy struct {
	Clearance          map[string][]ContextSecretClass
	BuilderAllowlist   ContextBuilderAllowlist
	PayloadMode        ContextPayloadMode
	EnforcementKeyFile string
	Namespace          NamespaceKey
}

type Notice struct {
	// Payload is the original durable notice envelope. ID is its queue receipt,
	// not RunID; delivery formats complete payloads together, never Text fallback.
	Payload   *SessionNotice
	ID        string
	SessionID string
	Text      string
	CreatedAt time.Time
}

type NoticeStore interface {
	Enqueue(context.Context, string, Notice) error
	Pending(context.Context, string) ([]Notice, error)
	Acknowledge(context.Context, string, string) error
}

type ProviderHostFactory interface {
	Provider() string
	Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error)
	Restore(context.Context, ProviderHostRecord) (RunnerHost, error)
}

type ProviderLaunchRequest struct {
	SessionID          string
	Owner              OwnerKey
	Launch             LaunchSpec
	Prior              *ProviderHostRecord
	PriorMustFork      bool
	BuilderConfinement *BuilderConfinementRequest
}

type RunnerHost interface {
	State(context.Context) (ProviderHostState, error)
	Snapshot() (RunnerSnapshot, error)
	Stop(context.Context) error
}

type RetiringHostFactory interface {
	RestoreRetiring(context.Context, ProviderHostRecord) (RunnerHost, error)
}

type SessionView struct {
	BindingScope          ContextBindingScope
	BindingElementID      string
	TargetElementID       string
	Retiring              bool
	CuratorProvenance     CuratorProvenanceState
	SessionID             string
	Owner                 OwnerKey
	Provider              string
	State                 SessionState
	Liveness              SessionLiveness
	LivenessDetail        string
	OwnerRunError         string
	AttachedClients       int
	AppliedGoalRef        *RevisionRef
	CreatedAt             time.Time
	UpdatedAt             time.Time
	QuarantineReason      string
	QuarantinedAt         *time.Time
	Eviction              *SessionEvictionNotice
	CtxID                 string
	MaterializationState  MaterializationState
	MaterializationSource MaterializationSource
	ActiveRunID           string
}

type WorkspaceResolver interface {
	Resolve(context.Context, string) (WorkspaceSessionManager, error)
}

type WorkspaceSessionManager interface {
	Close()
	Attach(context.Context, AttachRequest) (*Attachment, error)
	Session(context.Context, string) (SessionView, error)
	Detach(context.Context, string, string) (SessionView, error)
	Stop(context.Context, string) (SessionView, error)
}
