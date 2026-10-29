package sessionmanager

import (
	"context"
	"errors"
	"time"
)

// Context authority carries resource facts and fork decisions from the frozen
// contract. Canonical lifecycle, ownership and policy remain in the fork.
type ContextAuthority interface {
	AuthorizeContextUse(context.Context, ContextUse) (*ContextPermit, error)
	Context(context.Context, string) (*ContextRecord, error)
	ContextCoverageManifest(context.Context, string) (*CoverageManifest, error)
	Element(context.Context, string) (*OwnerFact, error)
	RunEvidence(context.Context, string) (*RunEvidence, error)
	RetireContext(context.Context, string, int, StateReason, time.Time) (*ContextRecord, error)
	PublishContext(context.Context, PublicationRequest) (*ContextRecord, error)
	RecoverUnusableContext(context.Context, string, int, StateReason, WorkspaceFingerprint, time.Time) (*UnusableRecovery, error)
	ReportContextRetention(context.Context, RetentionPolicy) (*RetentionReport, error)
	CollectContextRetention(context.Context, RetentionPolicy) (*RetentionCollection, error)
	SweepContextRetention(context.Context, RetentionSweepRequest) (*RetentionSweepResult, error)
	ConfirmContextReclamation(context.Context, string, ProviderReclamation) (*ContextRecord, error)
}

type ContextBuilderAllowlist struct {
	Paths []string
}

type ContextBuilderEnforcement struct {
	ContractVersion string                `json:"contract_version,omitempty"`
	CtxID           string                `json:"ctx_id,omitempty"`
	Revision        int                   `json:"revision,omitempty"`
	RunID           string                `json:"produced_by_run_id,omitempty"`
	ManagerID       string                `json:"manager_id,omitempty"`
	AllowlistDigest string                `json:"allowlist_digest,omitempty"`
	AllowlistSize   int                   `json:"allowlist_size,omitempty"`
	ProducerRole    string                `json:"producer_role,omitempty"`
	BuilderProfile  ContextBuilderProfile `json:"builder_profile,omitempty"`
	PathSpecsDigest string                `json:"path_specs_digest,omitempty"`
	Mechanism       string                `json:"mechanism,omitempty"`
	EnforcedScope   string                `json:"enforced_scope_digest,omitempty"`
	EnforcedBy      string                `json:"enforced_by,omitempty"`
	EnforcedAt      time.Time             `json:"enforced_at,omitempty"`
	Signature       string                `json:"signature,omitempty"`
}

type ContextBuilderProfile string

type ContextClassificationEvidence struct {
	ContractVersion        string                       `json:"contract_version,omitempty"`
	ClassifierVersion      string                       `json:"classifier_version,omitempty"`
	Source                 ContextIntegritySource       `json:"source,omitempty"`
	Verdict                ContextClassificationVerdict `json:"verdict,omitempty"`
	AssignedClass          ContextSecretClass           `json:"assigned_class,omitempty"`
	RuleIDs                []string                     `json:"rule_ids,omitempty"`
	InconclusiveReasons    []string                     `json:"inconclusive_reasons,omitempty"`
	IdentityScannerVersion string                       `json:"identity_scanner_version,omitempty"`
	IdentityDigest         string                       `json:"identity_digest,omitempty"`
	TranscriptSHA256       string                       `json:"transcript_sha256,omitempty"`
	TranscriptBytes        int64                        `json:"transcript_bytes,omitempty"`
	ScannedRecords         int                          `json:"scanned_records,omitempty"`
	EvidenceDigest         string                       `json:"evidence_digest,omitempty"`
	AssignmentDigest       string                       `json:"assignment_digest,omitempty"`
	ClassifiedAt           time.Time                    `json:"classified_at,omitempty"`
	AttestedBy             string                       `json:"attested_by,omitempty"`
}

type ContextClassificationVerdict string

type ContextConsumerOperation string

type ContextCoverageAccess string

type ContextCoverageAssertionDependency struct {
	ClaimID        string   `json:"claim_id"`
	SourceRefs     []string `json:"source_refs"`
	DependentPaths []string `json:"dependent_paths"`
}

type ContextCoverageBoundaryState string

type ContextCoverageDenial struct {
	PathSpec string `json:"path_spec,omitempty"`
	Reason   string `json:"reason"`
	Count    int    `json:"count"`
}

type ContextCoverageEnumeratedNotRead struct {
	Path          string `json:"path"`
	SourceEventID string `json:"source_event_id"`
}

type ContextCoverageEnumeration struct {
	PathSpec      string   `json:"path_spec"`
	Query         string   `json:"query,omitempty"`
	Entries       []string `json:"entries"`
	Complete      bool     `json:"complete"`
	Digest        string   `json:"digest"`
	SourceEventID string   `json:"source_event_id"`
	Sequence      int      `json:"sequence"`
}

type ContextCoverageExclusion struct {
	PathSpec string                         `json:"path_spec"`
	Reason   ContextCoverageExclusionReason `json:"reason"`
}

type ContextCoverageExclusionReason string

type ContextCoverageFailure struct {
	Path       string `json:"path"`
	ReasonCode string `json:"reason_code"`
}

type ContextCoverageObservation struct {
	Path             string                 `json:"path"`
	Access           ContextCoverageAccess  `json:"access"`
	ContentSHA256    string                 `json:"content_sha256"`
	ByteOffset       int64                  `json:"byte_offset,omitempty"`
	ByteLength       int64                  `json:"byte_length,omitempty"`
	TotalBytes       int64                  `json:"total_bytes,omitempty"`
	Ranges           []ContextCoverageRange `json:"ranges,omitempty"`
	Query            string                 `json:"query,omitempty"`
	FirstToolEventID string                 `json:"first_tool_event_id"`
	Sequence         int                    `json:"sequence"`
}

type ContextCoverageRange struct {
	Access        ContextCoverageAccess `json:"access"`
	ContentSHA256 string                `json:"content_sha256"`
	ByteOffset    int64                 `json:"byte_offset"`
	ByteLength    int64                 `json:"byte_length"`
	TotalBytes    int64                 `json:"total_bytes"`
	ToolEventID   string                `json:"tool_event_id"`
	Sequence      int                   `json:"sequence"`
}

type ContextCoverageTarget struct {
	PathSpec string `json:"path_spec"`
	Purpose  string `json:"purpose,omitempty"`
}

type ContextFreshness struct {
	CommitSHA                    string    `json:"commit_sha,omitempty"`
	PathSpecs                    []string  `json:"path_specs,omitempty"`
	IngestedPaths                []string  `json:"ingested_paths,omitempty"`
	IngestedScopes               []string  `json:"ingested_scopes,omitempty"`
	ResolvedPathsDigest          string    `json:"resolved_paths_digest,omitempty"`
	BuilderContractDigest        string    `json:"builder_contract_digest,omitempty"`
	BaseLiveTokens               int64     `json:"base_live_tokens,omitempty"`
	ScoutIngestionBaselineTokens int64     `json:"scout_ingestion_baseline_tokens,omitempty"`
	VerifiedCommitSHA            string    `json:"verified_commit_sha,omitempty"`
	CheckedAt                    time.Time `json:"checked_at,omitempty"`
}

type ContextIntegrityEvidence struct {
	Source           ContextIntegritySource `json:"source,omitempty"`
	TranscriptSHA256 string                 `json:"transcript_sha256,omitempty"`
	TranscriptBytes  int64                  `json:"transcript_bytes,omitempty"`
	ScannedRecords   int                    `json:"scanned_records,omitempty"`
	VerifiedAt       time.Time              `json:"verified_at,omitempty"`
	AttestedBy       string                 `json:"attested_by,omitempty"`
}

type ContextIntegritySource string

type ContextLeaseVisibility string

type ContextLineage struct {
	PrimaryCtxID       string   `json:"primary_ctx_id,omitempty"`
	SupplementalCtxIDs []string `json:"supplemental_ctx_ids,omitempty"`
	SupersedesCtxID    string   `json:"supersedes_ctx_id,omitempty"`
	ProducedByRunID    string   `json:"produced_by_run_id,omitempty"`
}

type ContextPayload struct {
	State       ContextPayloadState      `json:"state"`
	Contract    string                   `json:"contract,omitempty"`
	SHA256      string                   `json:"sha256,omitempty"`
	SizeBytes   int64                    `json:"size_bytes,omitempty"`
	ResourceRef string                   `json:"resource_ref,omitempty"`
	Withdrawal  ContextPayloadWithdrawal `json:"withdrawal,omitempty"`
}

type ContextPayloadExpiry struct {
	CtxID            string `json:"ctx_id"`
	ExpectedRevision int    `json:"expected_revision"`
	Rationale        string `json:"rationale"`
}

type ContextPayloadState string

type ContextPayloadWithdrawal struct {
	State       ContextPayloadWithdrawalState   `json:"state,omitempty"`
	Reason      StateReason                     `json:"reason,omitempty"`
	Attempts    int                             `json:"attempts,omitempty"`
	Failure     ContextPayloadWithdrawalFailure `json:"failure,omitempty"`
	OrderedAt   time.Time                       `json:"ordered_at,omitempty"`
	CompletedAt time.Time                       `json:"completed_at,omitempty"`
}

type ContextPayloadWithdrawalFailure string

type ContextPayloadWithdrawalState string

type ContextPermit struct {
	Allowed   bool
	ContextID string
	Code      string
	Reason    string
	Remedy    string
	RetryNext bool
	Checks    []ContextSecurityCheck
	DecidedAt time.Time
}

type ContextProvider string

type ContextReadSnapshotter interface {
	MaterializationReadSnapshot(context.Context) (ContextAuthority, error)
}

// ContextBinding is the existing read-only binding fact. This package carries
// its exact fields; it does not own or mutate the external business ledger.
type ContextBinding struct {
	Scope          ContextBindingScope `json:"scope"`
	ElementID      string              `json:"element_id,omitempty"`
	ProducerRole   string              `json:"producer_role"`
	AncestryDigest string              `json:"ancestry_digest,omitempty"`
}
type ContextBindingScope string

const (
	ContextBindingScopeBoard   ContextBindingScope = "board"
	ContextBindingScopeElement ContextBindingScope = "element"
)

// A missing record is distinguishable from an unavailable authority read.
var ErrMaterializationEvidenceNotFound = errors.New("materialization evidence not found")

type ContextRecord struct {
	Validation                 ContextValidationEvidence     `json:"validation,omitempty"`
	ValidationAudit            ContextValidationAudit        `json:"validation_audit,omitempty"`
	Binding                    ContextBinding                `json:"binding"`
	ContractVersion            string                        `json:"contract_version"`
	ID                         string                        `json:"id"`
	Revision                   int                           `json:"revision"`
	Provider                   ContextProvider               `json:"provider"`
	Lineage                    ContextLineage                `json:"lineage"`
	State                      ContextState                  `json:"state"`
	StateReason                StateReason                   `json:"state_reason,omitempty"`
	CreatedAt                  time.Time                     `json:"created_at"`
	ReadyAt                    time.Time                     `json:"ready_at,omitempty"`
	UpdatedAt                  time.Time                     `json:"updated_at"`
	ExpiresAt                  time.Time                     `json:"expires_at,omitempty"`
	LastSelectedAt             time.Time                     `json:"last_selected_at,omitempty"`
	Freshness                  ContextFreshness              `json:"freshness"`
	CoverageBoundaryState      ContextCoverageBoundaryState  `json:"coverage_boundary_state"`
	CoverageBoundaryVoidedAt   time.Time                     `json:"coverage_boundary_voided_at,omitempty"`
	CoverageBoundaryVoidReason StateReason                   `json:"coverage_boundary_void_reason,omitempty"`
	CompactedAt                time.Time                     `json:"compacted_at,omitempty"`
	CompactionMarker           string                        `json:"compaction_marker,omitempty"`
	Integrity                  ContextIntegrityEvidence      `json:"integrity,omitempty"`
	BuilderProfile             ContextBuilderProfile         `json:"builder_profile"`
	SecretClass                ContextSecretClass            `json:"secret_class"`
	Classification             ContextClassificationEvidence `json:"classification,omitempty"`
	BuilderEnforcement         ContextBuilderEnforcement     `json:"builder_enforcement,omitempty"`
	Payload                    ContextPayload                `json:"payload"`
	Reclamation                ProviderReclamation           `json:"reclamation,omitempty"`
	Pinned                     bool                          `json:"pinned,omitempty"`
	RequiresConfinement        bool                          `json:"-"`
	Kind                       ContextKind                   `json:"kind"`
	Actualization              ContextActualizationEvidence  `json:"actualization,omitempty"`
}

type ContextResolutionMode string

type ContextRetentionAction string

type ContextRetentionCollection struct {
	CtxID            string                 `json:"ctx_id"`
	Provider         ContextProvider        `json:"provider"`
	Action           ContextRetentionAction `json:"action"`
	Reason           StateReason            `json:"reason,omitempty"`
	ExpectedRevision int                    `json:"expected_revision"`
	IncludeArchived  bool                   `json:"include_archived"`
	Rationale        string                 `json:"rationale"`
}

type ContextRetentionHold struct {
	CtxID  string                     `json:"ctx_id"`
	Rule   ContextRetentionRule       `json:"rule"`
	Reason ContextRetentionHoldReason `json:"reason"`
	Detail string                     `json:"detail"`
}

type ContextRetentionHoldReason string

type ContextRetentionOutlook struct {
	CtxID     string               `json:"ctx_id"`
	Provider  ContextProvider      `json:"provider"`
	State     ContextState         `json:"state"`
	Risk      ContextRetentionRisk `json:"risk"`
	Anchor    time.Time            `json:"anchor,omitempty"`
	Horizon   time.Time            `json:"horizon,omitempty"`
	Remaining time.Duration        `json:"remaining_seconds,omitempty"`
	Advice    string               `json:"advice,omitempty"`
	Kind      ContextKind          `json:"kind"`
}

type ContextRetentionRisk string

type ContextRetentionRule string

type ContextRetentionSettings struct {
	ReadyTTL                      time.Duration `json:"ready_ttl"`
	StaleTTL                      time.Duration `json:"stale_ttl"`
	BuildingTTL                   time.Duration `json:"building_ttl"`
	RetiredTTL                    time.Duration `json:"retired_ttl"`
	UnpublishedMaterializationTTL time.Duration `json:"unpublished_materialization_ttl"`
	MaterializationIdleTTL        time.Duration `json:"materialization_idle_ttl"`
	PayloadTTL                    time.Duration `json:"payload_ttl"`
	MaxReadyPerBinding            int           `json:"max_ready_per_binding"`
	MaxDescendantsPerBase         int           `json:"max_descendants_per_base"`
}

type ContextRetentionSkip struct {
	CtxID  string `json:"ctx_id"`
	Reason string `json:"reason"`
}

type ContextRetentionSweep struct {
	Retire         []ContextRetirementPlan  `json:"retire"`
	Held           []ContextRetentionHold   `json:"held,omitempty"`
	ExpirePayloads []ContextPayloadExpiry   `json:"expire_payloads,omitempty"`
	Settings       ContextRetentionSettings `json:"settings"`
	Refusal        string                   `json:"refusal,omitempty"`
	EvaluatedAt    time.Time                `json:"evaluated_at"`
}

type ContextRetirementPlan struct {
	CtxID            string               `json:"ctx_id"`
	Provider         ContextProvider      `json:"provider"`
	Rule             ContextRetentionRule `json:"rule"`
	Reason           StateReason          `json:"reason"`
	ExpectedRevision int                  `json:"expected_revision"`
	Rationale        string               `json:"rationale"`
	Group            string               `json:"group,omitempty"`
}

type ContextSecretClass string

type ContextSecurityCheck struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
}

type ContextState string

type ContextUse struct {
	Owner     OwnerKey
	ContextID string
	Role      string
	Provider  ContextProvider
	Operation ContextConsumerOperation
	Mode      ContextResolutionMode
	Now       time.Time
}

type CoverageManifest struct {
	ContractVersion       string                               `json:"contract_version"`
	CtxID                 string                               `json:"ctx_id"`
	Provider              ContextProvider                      `json:"provider"`
	RepoFingerprint       string                               `json:"repo_fingerprint"`
	AnchorCommit          string                               `json:"anchor_commit"`
	WorkspaceDirtyDigest  string                               `json:"workspace_dirty_digest,omitempty"`
	BuilderPolicyDigest   string                               `json:"builder_policy_digest"`
	CreatedFromRunID      string                               `json:"created_from_run_id"`
	TraceAnchor           string                               `json:"trace_anchor"`
	TraceDigest           string                               `json:"trace_digest"`
	EventCount            int                                  `json:"event_count"`
	DeclaredTargets       []ContextCoverageTarget              `json:"declared_targets"`
	Observed              []ContextCoverageObservation         `json:"observed"`
	Enumerations          []ContextCoverageEnumeration         `json:"enumerations"`
	EnumeratedNotRead     []ContextCoverageEnumeratedNotRead   `json:"enumerated_not_read"`
	Excluded              []ContextCoverageExclusion           `json:"excluded"`
	DeniedAccesses        []ContextCoverageDenial              `json:"denied_accesses"`
	FailedAccesses        []ContextCoverageFailure             `json:"failed_accesses"`
	AssertionDependencies []ContextCoverageAssertionDependency `json:"assertion_dependencies"`
	UnknownRemainder      bool                                 `json:"unknown_remainder"`
	ManifestSHA256        string                               `json:"manifest_sha256"`
	Namespace             NamespaceKey                         `json:"-"`
	ProjectID             string                               `json:"project_id"`
}

type NamespaceKey string

type OwnerFact struct {
	ParentID string
	Status   string
	Closed   bool
	ID       OwnerKey
	Eligible bool
}

type OwnerFacts interface {
	Element(context.Context, string) (*OwnerFact, error)
	RunEvidence(context.Context, string) (*RunEvidence, error)
}

type ProviderReclamation struct {
	ReclaimedAt      time.Time `json:"reclaimed_at,omitempty"`
	ArchivedIncluded bool      `json:"archived_included,omitempty"`
	Files            int       `json:"files,omitempty"`
}

type PublicationRequest struct {
	CtxID              string
	ExpectedRevision   int
	TranscriptPath     string
	BuilderAllowlist   ContextBuilderAllowlist
	BuilderEnforcement *ContextBuilderEnforcement
	Now                time.Time
}

type RetentionCollection struct {
	Retired     []*ContextRecord             `json:"retired"`
	Delete      []ContextRetentionCollection `json:"delete"`
	Skipped     []ContextRetentionSkip       `json:"skipped,omitempty"`
	Expiring    []ContextRetentionOutlook    `json:"expiring"`
	EvaluatedAt time.Time                    `json:"evaluated_at"`
}

type RetentionPolicy struct {
	WarnWindow    time.Duration
	Now           time.Time
	IncludePinned bool
}

type RetentionReport struct {
	Expiring    []ContextRetentionOutlook    `json:"expiring"`
	Collect     []ContextRetentionCollection `json:"collect"`
	EvaluatedAt time.Time                    `json:"evaluated_at"`
}

type RetentionSweepRequest struct {
	Settings        ContextRetentionSettings
	Now             time.Time
	LeaseVisibility ContextLeaseVisibility
	Leased          map[string]struct{}
	IncludePinned   bool
	DryRun          bool
}

type RetentionSweepResult struct {
	Plan              ContextRetentionSweep  `json:"plan"`
	Retired           []*ContextRecord       `json:"retired"`
	PayloadsWithdrawn []*ContextRecord       `json:"payloads_withdrawn,omitempty"`
	Skipped           []ContextRetentionSkip `json:"skipped,omitempty"`
	DryRun            bool                   `json:"dry_run,omitempty"`
}

// RunEvidenceStatus preserves the original durable run projection. Empty or
// unknown status is unknown custody, never proof of completion/reclamation.
type RunEvidenceStatus string

const (
	RunEvidenceQueued    RunEvidenceStatus = "queued"
	RunEvidenceRunning   RunEvidenceStatus = "running"
	RunEvidenceCompleted RunEvidenceStatus = "completed"
	RunEvidenceFailed    RunEvidenceStatus = "failed"
	RunEvidenceCancelled RunEvidenceStatus = "cancelled"
)

type RunEvidence struct {
	Status        RunEvidenceStatus
	ID            string
	ContextID     string
	Owner         OwnerKey
	Active        bool
	Published     bool
	PayloadSHA256 string
}

type StateReason string

type UnusableRecovery struct {
	Contexts      []*ContextRecord
	ReplacementID string
	ColdRebuild   bool
	Reason        StateReason
}

type WorkspaceFingerprint struct {
	CommitSHA             string
	ResolvedPathsDigest   func(pathSpecs []string) (string, error)
	BuilderContractDigest string
}
