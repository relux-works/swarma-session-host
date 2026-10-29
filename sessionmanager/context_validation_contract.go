package sessionmanager

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Read-only validation facts retain the original field types and wire names.
// Carrying them is neither a passed preflight nor a replacement for the actual
// evidence/coverage authority. No registry store or validation engine lives here.

type ContextClaimGateCounts struct {
	Claims           int `json:"claims"`
	SourceRefs       int `json:"source_refs"`
	PointersResolved int `json:"pointers_resolved"`
	PredicateChecked int `json:"predicate_checked"`
	PredicateCorrect int `json:"predicate_correct"`
	SemanticChecked  int `json:"semantic_checked"`
	SemanticEntailed int `json:"semantic_entailed"`
	SemanticRefused  int `json:"semantic_refused"`
}

type ContextValidationEvidence struct {
	ContractVersion string          `json:"contract_version,omitempty"`
	CtxID           string          `json:"ctx_id,omitempty"`
	Provider        ContextProvider `json:"provider,omitempty"`
	ContextKind     ContextKind     `json:"context_kind,omitempty"`

	ProjectID       string `json:"project_id,omitempty"`
	RepoFingerprint string `json:"repo_fingerprint,omitempty"`
	AnchorCommit    string `json:"anchor_commit,omitempty"`

	BuilderProfile       ContextBuilderProfile `json:"builder_profile,omitempty"`
	BuilderRole          string                `json:"builder_role,omitempty"`
	BuilderPolicyDigest  string                `json:"builder_policy_digest,omitempty"`
	SourceRunID          string                `json:"source_run_id,omitempty"`
	ProviderCompletionID string                `json:"provider_completion_id,omitempty"`
	ManagerID            string                `json:"manager_id,omitempty"`

	CoverageManifestSHA256 string `json:"coverage_manifest_sha256,omitempty"`
	AssertionLedgerSHA256  string `json:"assertion_ledger_sha256,omitempty"`
	PayloadSHA256          string `json:"payload_sha256,omitempty"`
	TraceDigest            string `json:"trace_digest,omitempty"`

	ValidatorVersion       string `json:"validator_version,omitempty"`
	ExtractorVersion       string `json:"extractor_version,omitempty"`
	ValidatorModel         string `json:"validator_model,omitempty"`
	QuestionSeedCommitment string `json:"question_seed_commitment,omitempty"`

	AssayNonce           string `json:"assay_nonce,omitempty"`
	AssayCatalogSize     int    `json:"assay_catalog_size,omitempty"`
	AssayCatalogSHA256   string `json:"assay_catalog_sha256,omitempty"`
	AssaySelectionSHA256 string `json:"assay_selection_sha256,omitempty"`

	Assay              *ContextAssayScore     `json:"assay,omitempty"`
	ClaimGates         ContextClaimGateCounts `json:"claim_gates,omitempty"`
	ContaminationClean bool                   `json:"contamination_clean,omitempty"`
	ContaminationSHA   string                 `json:"contamination_digest,omitempty"`

	TranscriptSHA256     string `json:"transcript_sha256,omitempty"`
	ClassificationSHA256 string `json:"classification_digest,omitempty"`

	IdentityScannerVersion string `json:"identity_scanner_version,omitempty"`
	IdentityDigest         string `json:"identity_digest,omitempty"`

	Usage ContextValidationUsage `json:"usage,omitempty"`

	BaseBuildCostUSD float64 `json:"base_build_cost_usd,omitempty"`
	CostCeilingUSD   float64 `json:"cost_ceiling_usd,omitempty"`
	ModelCalls       int     `json:"model_calls,omitempty"`

	Failure           *ContextValidationFailure `json:"failure,omitempty"`
	ValidatedAt       time.Time                 `json:"validated_at,omitempty"`
	AttestationSHA256 string                    `json:"attestation_sha256,omitempty"`
}

type ContextValidationFailure struct {
	Code     ContextErrorCode `json:"code"`
	Message  string           `json:"message"`
	FailedAt time.Time        `json:"failed_at"`
}

type ContextValidationUsage struct {
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	CostUSD      float64 `json:"cost_usd,omitempty"`
}

type ContextValidationPolicy struct {
	ValidatorVersion string `json:"validator_version"`
	ExtractorVersion string `json:"extractor_version"`
}

type ContextValidationAudit struct {
	ContractVersion  string    `json:"contract_version,omitempty"`
	LastCheapAuditAt time.Time `json:"last_cheap_audit_at,omitempty"`
	LastDeepAuditAt  time.Time `json:"last_deep_audit_at,omitempty"`
	DeepAuditDigest  string    `json:"deep_audit_digest,omitempty"`
	ValidatorVersion string    `json:"validator_version,omitempty"`
	ExtractorVersion string    `json:"extractor_version,omitempty"`
	DeepAuditCount   int       `json:"deep_audit_count,omitempty"`
	CheapAuditCount  int       `json:"cheap_audit_count,omitempty"`
}

type ContextValidationBinding struct {
	PayloadSHA256          string
	CoverageManifestSHA256 string
	AnchorCommit           string
}

type ContextForkFallback string

type ContextForkPreflight struct {
	Record   *ContextRecord
	Mode     ContextResolutionMode
	Policy   ContextValidationPolicy
	Observed ContextValidationBinding
	Now      time.Time
}

type ContextForkPreflightDecision struct {
	ContractVersion      string                       `json:"contract_version"`
	CtxID                string                       `json:"ctx_id"`
	Mode                 ContextResolutionMode        `json:"mode"`
	State                ContextState                 `json:"state"`
	Admitted             bool                         `json:"admitted"`
	Skipped              bool                         `json:"skipped"`
	Fallback             ContextForkFallback          `json:"fallback"`
	Reasons              []ContextValidationReason    `json:"reasons,omitempty"`
	Consequence          ContextValidationConsequence `json:"consequence"`
	ValidationAgeSeconds int64                        `json:"validation_age_seconds"`
	ModelAssayRequired   bool                         `json:"model_assay_required"`
}

type ContextBoundaryBanner struct {
	ContractVersion      string `json:"contract_version"`
	CtxID                string `json:"ctx_id"`
	ManifestRef          string `json:"manifest_ref"`
	AnchorCommit         string `json:"anchor_commit,omitempty"`
	ValidationAge        string `json:"validation_age"`
	ValidationAgeSeconds int64  `json:"validation_age_seconds"`

	CoveredReads          []string `json:"covered_reads,omitempty"`
	CoveredReadsRemainder int      `json:"covered_reads_remainder,omitempty"`
	CoveredReadCount      int      `json:"covered_read_count"`

	EnumeratedNotRead          []string `json:"enumerated_not_read,omitempty"`
	EnumeratedNotReadRemainder int      `json:"enumerated_not_read_remainder,omitempty"`
	EnumeratedNotReadCount     int      `json:"enumerated_not_read_count"`

	Excluded          []string `json:"excluded,omitempty"`
	ExcludedRemainder int      `json:"excluded_remainder,omitempty"`
	ExcludedCount     int      `json:"excluded_count"`

	UnknownRemainder bool `json:"unknown_remainder"`
}

type ContextAssayContradiction struct {
	FactID string            `json:"fact_id"`
	Kind   ContextFactKind   `json:"kind"`
	Family ContextFactFamily `json:"family"`
}

type ContextAssayScore struct {
	Scope                  ContextAssayScope           `json:"scope"`
	SampledCount           int                         `json:"sampled_count"`
	CorrectCount           int                         `json:"correct_count"`
	ContradictedCount      int                         `json:"contradicted_count"`
	AbstainedCount         int                         `json:"abstained_count"`
	AnsweredCount          int                         `json:"answered_count"`
	CorrectnessApplicable  bool                        `json:"correctness_applicable"`
	CorrectnessScore       float64                     `json:"correctness_score"`
	AnswerRate             float64                     `json:"answer_rate"`
	AbstainRate            float64                     `json:"abstain_rate"`
	RealizedDetectionPower float64                     `json:"realized_detection_power_at_6pct"`
	Contradictions         []ContextAssayContradiction `json:"contradictions,omitempty"`
}

type ContextKind string
type ContextErrorCode string
type ContextAssayScope string
type ContextFactKind string
type ContextFactFamily string
type ContextValidationReason string
type ContextValidationConsequence string

// ContextForkValidationSource supplies the original validation decision and
// coverage-derived boundary. It is separate from AuthorizeContextUse: a security
// permit is not evidence of validation. Implementations must use the actual
// current policy/evidence and manifest, never infer a boundary from a digest.
type ContextForkValidationSource interface {
	ActiveContextValidationPolicy(context.Context) (ContextValidationPolicy, error)
	PreflightContextFork(context.Context, ContextForkPreflight) (*ContextForkPreflightDecision, error)
	BuildContextBoundaryBanner(context.Context, *ContextRecord, *CoverageManifest, string, time.Time) (*ContextBoundaryBanner, error)
}

func (b *ContextBoundaryBanner) Render() string {
	if b == nil {
		return ""
	}
	var out strings.Builder
	fmt.Fprintf(&out, "CONTEXT BOUNDARY %s (%s)\n", b.CtxID, b.ContractVersion)
	if b.AnchorCommit != "" {
		fmt.Fprintf(&out, "anchor: %s | validated: %s ago | manifest: %s\n",
			b.AnchorCommit, b.ValidationAge, b.ManifestRef)
	} else {
		fmt.Fprintf(&out, "validated: %s ago | manifest: %s\n", b.ValidationAge, b.ManifestRef)
	}
	writeList := func(label string, values []string, remainder, total int) {
		fmt.Fprintf(&out, "%s (%d):", label, total)
		if len(values) == 0 {
			out.WriteString(" none\n")
			return
		}
		out.WriteString(" " + strings.Join(values, ", "))
		if remainder > 0 {
			fmt.Fprintf(&out, ", +%d more", remainder)
		}
		out.WriteString("\n")
	}
	writeList("read", b.CoveredReads, b.CoveredReadsRemainder, b.CoveredReadCount)
	writeList("seen but not read", b.EnumeratedNotRead, b.EnumeratedNotReadRemainder, b.EnumeratedNotReadCount)
	writeList("excluded", b.Excluded, b.ExcludedRemainder, b.ExcludedCount)
	fmt.Fprintf(&out,
		"everything not named above is UNKNOWN to this base: it was never looked at, "+
			"so its silence is not evidence of absence. Read resource %s for the full "+
			"boundary before relying on a gap.\n",
		b.ManifestRef,
	)
	return out.String()
}
