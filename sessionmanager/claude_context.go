package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	claudeContextForkPlanContract = "task-board.claude-context-fork-plan-v1"
	claudeContextAssayContract    = "task-board.claude-context-assay-attestation-v1"

	// Provider-specific public failure codes. They are deliberately separate
	// from the registry taxonomy: each one names a Claude mechanism an operator
	// can act on, and none of them is a policy decision the registry makes.
	claudeContextBaseNotForkable  = "claude_context_base_not_forkable"
	claudeContextTranscriptDrift  = "claude_context_transcript_format_drift"
	claudeContextSlugUnverified   = "claude_context_cwd_slug_unverified"
	claudeContextWriterBusy       = "claude_context_writer_lease_unavailable"
	claudeContextMaterializeError = "claude_context_materialization_failed"
	// claudeContextForkUnverified is reported when the launch could not prove the
	// provider inherited the materialization. It is deliberately not the same code
	// as a missing resume target: the session started, so what failed is the
	// evidence, and an operator's next step is different in each case.
	claudeContextForkUnverified = "claude_context_fork_unverified"
	// claudeContextForkPlanInvalid is the fail-closed refusal when a launch that
	// must fork an immutable base is handed something that is not a fork plan.
	// Falling back to a bare resume there would append the run's turns straight
	// into a shared registry base, so the launch is refused before provider I/O.
	claudeContextForkPlanInvalid = "claude_context_fork_plan_invalid"
	// claudeContextSourceMutated reports that a fork source — the immutable base or
	// the materialized copy taken from it — changed while the provider held it. It
	// is separate from a failed resume because the session did start and succeed;
	// what is wrong is that a shared artifact no longer matches what the registry
	// described.
	claudeContextSourceMutated = "claude_context_fork_source_mutated"
)

type claudeContextForkPlan = ClaudeContextForkPlan

// requirePrimaryPayloadPresent probes the machine-local transcript store before
// the base is opened.
//
// Claude is the provider this check exists for. It deletes session transcripts
// about 30 days after their last write, on a timer the board neither controls
// nor is told about, so a base disappearing is the expected end of its life
// rather than an anomaly — and the old path reported it as "no longer has a
// readable base transcript", which reads like corruption and sends an operator
// looking for a file to repair.
func requireClaudePrimaryPayloadPresent(p *claudeContextForkPlan, transcriptPath string, at time.Time) error {
	if p == nil {
		return fmt.Errorf("a Claude fork plan is required to verify payload presence")
	}
	return requireClaudePayloadPresence(p.PrimaryCtxID, transcriptPath, p.PrimaryReadyAt, p.PrimaryMode, at)
}

// ClaudeContextAssayRequest is the provider adapter half of CV-2.
//
// The request has no field for a repository path, a precondition, an external
// resource, or an injected item, and that is the contract: the assay measures
// what the base believes, so any evidence reaching the fork from outside the
// inherited transcript would answer a different question.
type ClaudeContextAssayRequest struct {
	BaseCtxID    string          `json:"base_ctx_id"`
	Prompt       string          `json:"prompt"`
	OutputSchema json.RawMessage `json:"output_schema"`
	// BaseReadyAt is manager-private retention evidence. The public control
	// request cannot claim it; RunClaudeContextAssay copies it from the layer-1
	// record after the mirror and security gates succeed, so the adapter can say
	// whether an absent transcript crossed the provider's documented horizon.
	BaseReadyAt time.Time `json:"-"`
	// Model is the validator's model choice. Empty leaves the provider default
	// in place: the cheap-tier decision belongs to the validation policy, not to
	// this adapter.
	Model string `json:"model,omitempty"`
}

// ClaudeContextAssayAttestation binds the enforced isolation policy to the
// provider identity that ran under it.
//
// EnforcedTools/MCPServers/SlashCommands are the provider's own report of what
// it loaded, read back from the session init event. They are the reason this is
// an attestation rather than a claim: the adapter does not merely pass deny
// flags, it reads back what the provider says it has and refuses when that
// report is absent or non-empty.
type ClaudeContextAssayAttestation struct {
	Contract        string `json:"contract"`
	PolicyDigest    string `json:"policy_digest"`
	ProviderVersion string `json:"provider_version"`
	// BaseSessionUUID is the base identity; AssaySessionUUID is the disposable
	// fork. The fork is named by this adapter rather than discovered — the provider
	// honours --session-id alongside --resume --fork-session — and the identity the
	// provider reported is required to be the one that was named, so this field
	// binds the attestation to a session the manager chose.
	BaseSessionUUID  string `json:"base_session_uuid"`
	AssaySessionUUID string `json:"assay_session_uuid"`
	// MaterializedSHA256 is the digest of the transcript the fork inherited.
	MaterializedSHA256 string `json:"materialized_sha256"`
	// InheritedRecords is how many of the materialized copy's record identities
	// were found, in order, in the disposable fork's own transcript.
	//
	// It is the positive half of the lineage evidence and it is why a matching
	// session identity is not the whole check. Identity says which session
	// answered; it does not say what that session inherited. A provider that
	// ignored --resume --fork-session and started cold under the requested
	// --session-id would report the expected identity and leave both fork sources
	// untouched, so neither of those facts can distinguish a real fork from a cold
	// start. Record containment can: a fork copies the inherited records through
	// with their uuid values intact, so a child holding all of them in order
	// descends from this copy rather than merely resembling it. A zero here is
	// never attested — the run is refused before scoring.
	InheritedRecords int `json:"inherited_records"`
	// RepositoryFree records that the assay working directory contained nothing:
	// the deny policy is enforced by the launch, and this is the second,
	// structural half of it.
	RepositoryFree bool `json:"repository_free"`
	// SchemaEnforcement says who constrained the answer's shape and how far. On
	// Claude it is always this adapter, because the provider implements
	// schema-constrained output as a tool and the assay denies every tool. There is
	// exactly one value, adapter-json-schema: a schema is mandatory and every
	// keyword in it is evaluated against the answer, or the assay is refused as a
	// capability failure. SchemaDigest identifies the exact schema that was
	// enforced. Stating both keeps a consumer from reading the attestation as a
	// provider-side guarantee it is not.
	SchemaEnforcement  string    `json:"schema_enforcement"`
	SchemaDigest       string    `json:"schema_digest"`
	EnforcedTools      int       `json:"enforced_tools"`
	EnforcedMCPServers int       `json:"enforced_mcp_servers"`
	EnforcedSlashCmds  int       `json:"enforced_slash_commands"`
	ToolEventCount     int       `json:"tool_event_count"`
	CompletedAt        time.Time `json:"completed_at"`
}

type ClaudeContextAssayResult struct {
	Output      json.RawMessage               `json:"output"`
	Attestation ClaudeContextAssayAttestation `json:"attestation"`
	Usage       ProviderUsage                 `json:"usage,omitempty"`
}

// claudeContextAssayer is implemented by the Claude adapter. The manager reaches
// it only through the registered factory, so an adapter that cannot enforce the
// contract simply does not implement it and the manager reports the capability
// failure instead of scoring an unisolated answer.
type claudeContextAssayer interface {
	RunContextAssay(
		context.Context,
		ProviderHostRecord,
		ClaudeContextAssayRequest,
	) (*ClaudeContextAssayResult, error)
}

// normalizeClaudeSupplemental validates one lineage input and applies the
// raw-splice guard.
func normalizeClaudeSupplemental(
	input ClaudeSupplementalContext,
	baseFormatVersion string,
) (ClaudeSupplementalContext, error) {
	id := normalizeClaudeContextKey(input.CtxID)
	if err := validateClaudeContextKey(id); err != nil {
		return ClaudeSupplementalContext{}, err
	}
	if !input.Raw {
		handoff := strings.TrimSpace(input.Handoff)
		if handoff == "" {
			return ClaudeSupplementalContext{}, fmt.Errorf(
				"supplemental context %s requires a distilled handoff", id)
		}
		return ClaudeSupplementalContext{CtxID: id, Handoff: handoff}, nil
	}
	if err := requireSupportedClaudeSpliceFormat(id, baseFormatVersion); err != nil {
		return ClaudeSupplementalContext{}, err
	}
	sessionUUID, err := NormalizeClaudeSessionUUID(input.RawSessionUUID)
	if err != nil {
		return ClaudeSupplementalContext{}, err
	}
	dir := strings.TrimSpace(input.RawProjectDir)
	if !filepath.IsAbs(dir) {
		return ClaudeSupplementalContext{}, &PublicError{
			Code: claudeContextMaterializeError,
			Message: fmt.Sprintf(
				"raw-history supplemental %s requires the absolute working directory its session lives in", id,
			),
		}
	}
	return ClaudeSupplementalContext{
		CtxID:          id,
		Handoff:        strings.TrimSpace(input.Handoff),
		Raw:            true,
		RawSessionUUID: sessionUUID,
		RawProjectDir:  filepath.Clean(dir),
	}, nil
}

// claudeInspectBase reads a base transcript and applies every fork gate.
func claudeInspectBase(ctxID, sessionUUID, projectDir string) (claudeTranscriptFacts, error) {
	home, err := claudeProviderHome()
	if err != nil {
		return claudeTranscriptFacts{}, err
	}
	path, err := claudeSessionTranscriptPath(home, projectDir, sessionUUID)
	if err != nil {
		return claudeTranscriptFacts{}, err
	}
	_, facts, err := inspectClaudeTranscriptFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return claudeTranscriptFacts{}, &PublicError{
				Code: claudeContextBaseNotForkable,
				Message: fmt.Sprintf(
					"Claude context %s has no transcript in the project directory its session belongs to; "+
						"it cannot be forked from this machine",
					ctxID,
				),
			}
		}
		return claudeTranscriptFacts{}, &PublicError{
			Code: claudeContextBaseNotForkable,
			Message: fmt.Sprintf(
				"Claude context %s has an unreadable transcript, so neither its turn boundary nor its "+
					"compaction state can be established: %v",
				ctxID, err,
			),
		}
	}
	if err := verifyClaudeSlugRule(path, facts.RecordedCWD); err != nil {
		return claudeTranscriptFacts{}, err
	}
	if err := requireForkableClaudeBase(ctxID, facts); err != nil {
		return claudeTranscriptFacts{}, err
	}
	return facts, nil
}

// claudeBaseTranscriptRef resolves the session identity and directory of a
// Claude materialization's provider handle.
//
// A ready materialization carries the writable child's host snapshot; a
// prepared-but-unlaunched one still carries its fork plan. Both are legitimate
// fork sources, so both are accepted, and anything else is refused rather than
// guessed at.
func claudeBaseTranscriptRef(record ProviderHostRecord) (string, string, error) {
	if record.Provider != "claude" {
		return "", "", ErrProviderHostNotFound
	}
	if snapshot, err := decodeClaudeHostSnapshot(record.Snapshot); err == nil {
		dir := strings.TrimSpace(snapshot.Plan.Cwd)
		if dir == "" {
			return "", "", fmt.Errorf("Claude host snapshot records no project directory")
		}
		return snapshot.SessionUUID, filepath.Clean(dir), nil
	}
	plan, planErr := decodeClaudeForkPlan(record)
	if planErr != nil {
		return "", "", planErr
	}
	return plan.BaseSessionUUID, plan.BaseProjectDir, nil
}

// claudeForkPlanFailure is the refusal a launch gets when the manager required a
// fork and the private catalog handed over something else.
//
// It names the recovery an operator can act on — rematerialize the child — rather
// than degrading to a resume, because the degraded path writes into a base other
// runs are still forking from.
func claudeForkPlanFailure(cause error) error {
	return &PublicError{
		Code: claudeContextForkPlanInvalid,
		Message: fmt.Sprintf(
			"materialized Claude child requires a private fork plan; refusing immutable-base resume fallback: "+
				"%v; rematerialize the child from its ready registry base",
			cause,
		),
	}
}

func decodeClaudeForkPlan(record ProviderHostRecord) (*claudeContextForkPlan, error) {
	s := record.Snapshot
	if record.Provider != "claude" || s.Kind != "claude-fork" || s.ClaudeFork == nil || s.Claude != nil || s.Codex != nil || s.CodexFork != nil || s.MuseMSP != nil || s.MusePTY != nil || s.MusePTYProvenance != nil {
		return nil, fmt.Errorf("Claude fork plan is unavailable")
	}
	plan := copyRunnerSnapshot(s).ClaudeFork
	if plan.Contract != claudeContextForkPlanContract {
		return nil, fmt.Errorf("unsupported Claude fork plan contract %q", plan.Contract)
	}
	if err := validateClaudeContextKey(plan.PrimaryCtxID); err != nil {
		return nil, err
	}
	if err := validateClaudeContextKey(plan.ChildCtxID); err != nil {
		return nil, err
	}
	if _, err := NormalizeClaudeSessionUUID(plan.BaseSessionUUID); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(plan.BaseProjectDir) || !filepath.IsAbs(plan.TargetProjectDir) {
		return nil, fmt.Errorf("Claude fork plan requires absolute project directories")
	}
	return plan, nil
}

// claudeProviderHome resolves the directory Claude Code keeps its transcripts
// under, honouring the same CLAUDE_CONFIG_DIR the launched child will see.
func claudeProviderHome() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("CLAUDE_CONFIG_DIR")); configured != "" {
		if !filepath.IsAbs(configured) {
			return "", fmt.Errorf("CLAUDE_CONFIG_DIR %q must be an absolute path", configured)
		}
		return filepath.Clean(configured), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving Claude provider home: %w", err)
	}
	return filepath.Join(home, ".claude"), nil
}

func claudeSameContextIDs(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if normalizeClaudeContextKey(left[index]) !=
			normalizeClaudeContextKey(right[index]) {
			return false
		}
	}
	return true
}
