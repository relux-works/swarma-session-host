package sessionmanager

import (
	"context"
	"time"
)

// Typed runner and record closure. Retained launch facts describe provider
// state; canonical goal authority and legacy wire records remain separate.
type ClaudeRunnerSnapshot struct {
	Contract                  string
	SessionUUID               string
	PID                       int
	StartedAt                 time.Time
	Plan                      LaunchSpec
	Term                      string
	Rows                      uint16
	Cols                      uint16
	Adopted                   bool
	Stopped                   bool
	ContextCtxID              string
	ContextPrimaryCtxID       string
	ContextProjectSlug        string
	ContextResumeSessionUUID  string
	ContextBaseSHA256         string
	ContextMaterializedSHA256 string
	ContextInjectedCtxIDs     []string
	ContextSplicedCtxIDs      []string
	ContextForkVerifiedAt     time.Time
}

type CodexRolloutPrefix struct {
	Bytes  int64
	SHA256 string
}

type CodexRunnerSnapshot struct {
	// OwnerRun is the retained original launch fact, not current authority.
	OwnerRun *OwnerRunContext
	// GoalTokenBudget is carried data; nil and an explicit zero remain distinct.
	GoalTokenBudget     *int64
	ProcessIdentity     *ProviderProcessIdentity
	Contract            string
	PID                 int
	URL                 string
	ThreadID            string
	Executable          string
	ProjectDir          string
	CodexHome           string
	HostArgv            []string
	ManagedClient       []string
	LogPath             string
	AutoContinue        CodexAutoContinueOptions
	ForkedFromID        string
	ProviderVersion     string
	BaseRolloutSHA256   string
	InheritedLastTurnID string
	InheritedRollout    CodexRolloutPrefix
	ManagedTurnID       string
	LastCompletedTurnID string
	Adopted             bool
	Stopped             bool
	ToolPolicy          ClientToolPolicy
	Sources             []LaunchPlanSource
	CuratorHomeCopy     *CodexCuratorHomeCopy
}

type LaunchSpec struct {
	Claude *ClaudePreparedLaunch

	Codex *CodexPreparedLaunch

	// Muse carries the complete rebuild payload; the Muse factory validates and seals it.
	Muse *MuseSessionLaunch

	Executable  string
	Cwd         string
	Variants    LaunchVariants
	Environment []string
	Sources     []LaunchPlanSource
	ToolPolicy  ClientToolPolicy
}

type MuseDeferralWarning struct {
	Code    string
	State   string
	Target  MuseFrozenTuple
	Newest  MuseFrozenTuple
	Attempt string
	Mode    string
	Consent bool
}

type MuseFallbackWarning struct {
	Code             string
	State            string
	Newest           MuseFrozenTuple
	FailureAttempt   string
	FailureStage     string
	FailureCode      string
	LastGood         MuseFrozenTuple
	LastGoodSequence uint64
	Mode             string
	Consent          bool
}

type MuseFrozenTuple struct {
	Build  string
	SHA256 string
	Path   string
}

type MusePTYDeferralWarning struct {
	Code    string
	State   string
	Target  MusePTYFrozenTuple
	Newest  MusePTYFrozenTuple
	Attempt string
	Mode    string
	Consent bool
}

type MusePTYFallbackWarning struct {
	Code             string
	State            string
	Newest           MusePTYFrozenTuple
	FailureAttempt   string
	FailureStage     string
	FailureCode      string
	LastGood         MusePTYFrozenTuple
	LastGoodSequence uint64
	Mode             string
	Consent          bool
}

type MusePTYFrozenTuple struct {
	Build  string
	SHA256 string
	Path   string
}

type MusePTYProvenanceSnapshot struct {
	Base           MusePTYSnapshot
	Profile        string
	ProfileLock    string
	ManagedHome    string
	FragmentSHA256 string
}

type MusePTYSnapshot struct {
	Contract             string
	SessionID            string
	LogPath              string
	PID                  int
	VersionFallback      bool
	VersionFallbackFrom  string
	VersionFallbackEvent *MusePTYFallbackWarning
	VersionDeferralEvent *MusePTYDeferralWarning
}

type MuseRunnerSnapshot struct {
	// NativeAccounting is the full original committed native field group.
	// It carries data, never caller authority or permission to restore.
	NativeAccounting          *LegacyMuseNativeAccounting
	GoalTransport             string
	Provider                  string
	State                     ProviderHostState
	SessionID                 string
	WorkspaceRoot             string
	ModelID                   string
	ApprovalMode              string
	ServerVersion             string
	ServerVersionReported     bool
	SchemaFingerprint         string
	SchemaFingerprintReported bool
	VersionUnqualified        bool
	VersionFallback           bool
	VersionFallbackFrom       string
	VersionFallbackEvent      *MuseFallbackWarning
	VersionDeferralEvent      *MuseDeferralWarning
	TokenUsageTotal           int64
	ExternalPause             bool
	ActiveTurnID              string
	ObservedApprovalMode      string
	ObservedModelID           string
	ModelRouteUnserved        bool
}

type ProviderHostRecord struct {
	Provider string
	Snapshot RunnerSnapshot
}

type ProviderProcessIdentity struct {
	PID   int
	UID   uint32
	Start string
	PGID  int
}

type RunnerSnapshot struct {
	CodexFork *CodexContextForkPlan

	ClaudeFork *ClaudeContextForkPlan

	Kind              SnapshotKind
	Claude            *ClaudeRunnerSnapshot
	Codex             *CodexRunnerSnapshot
	MuseMSP           *MuseRunnerSnapshot
	MusePTY           *MusePTYSnapshot
	MusePTYProvenance *MusePTYProvenanceSnapshot
}

type SnapshotKind string

type ForkRunnerCodec interface {
	DecodeLegacy(context.Context, LegacyRecordInput) (RunnerRecord, error)
	EncodeInitial(context.Context, RunnerRecord) ([]byte, error)
	EncodeMerge(context.Context, LegacyRecordInput, RunnerRecord) ([]byte, error)
	AcknowledgeCommitted(context.Context, LegacyRecordInput) error
}

type LegacyRecordInput struct {
	SessionID string
	SHA256    string
	Bytes     []byte
}

type RunnerRecord struct {
	ConditionalStop         *ConditionalStopRecord
	GracefulRetirement      bool
	Contract                string
	SessionID               string
	Owner                   OwnerKey
	Provider                string
	ProjectDir              string
	NamespaceFingerprint    string
	State                   SessionState
	Liveness                SessionLiveness
	LivenessDetail          string
	OwnerRunError           string
	Eviction                *SessionEvictionNotice
	ProviderHost            ProviderHostRecord
	MaterializationCtxID    string
	ProviderHostPendingFork bool
	AppliedGoalRef          *RevisionRef
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type ConditionalStopRecord struct {
	Precondition string
	Phase        string
	Signalled    bool
	Previous     *RunnerRecord
}
