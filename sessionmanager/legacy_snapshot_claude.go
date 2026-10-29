package sessionmanager

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Full legacy plan and owner context remain private to the legacy wire view.
type legacyClaudeSnapshot struct {
	Contract    string                   `json:"contract"`
	SessionUUID string                   `json:"session_uuid"`
	PID         int                      `json:"pid"`
	StartedAt   time.Time                `json:"started_at"`
	Plan        PrimarySessionLaunchPlan `json:"primary_session_plan"`
	Term        string                   `json:"term,omitempty"`
	Rows        uint16                   `json:"rows,omitempty"`
	Cols        uint16                   `json:"cols,omitempty"`
	OwnerRun    json.RawMessage          `json:"owner_run,omitempty"`
	// Adopted records that SessionUUID was adopted from a pre-existing
	// provider session via explicit `--resume` intent rather than minted by
	// the manager; recovery relaunches keep resuming the adopted identity.
	Adopted bool `json:"adopted,omitempty"`
	Stopped bool `json:"stopped,omitempty"`

	// Context* record what this session inherited when it was launched from a
	// materialized registry context. They are manager-private provenance: the
	// project slug and the digests are enough to reconcile a restart against the
	// layer-2 catalog, and none of them is projected in a SessionView.
	//
	// SessionUUID above is the writable fork, never the copy it came from.
	// ContextResumeSessionUUID records that copy separately so a restart can tell
	// the two apart: the fork is resumed, the copy is only ever forked again.
	ContextCtxID              string    `json:"context_ctx_id,omitempty"`
	ContextPrimaryCtxID       string    `json:"context_primary_ctx_id,omitempty"`
	ContextProjectSlug        string    `json:"context_project_slug,omitempty"`
	ContextResumeSessionUUID  string    `json:"context_resume_session_uuid,omitempty"`
	ContextBaseSHA256         string    `json:"context_base_sha256,omitempty"`
	ContextMaterializedSHA256 string    `json:"context_materialized_sha256,omitempty"`
	ContextInjectedCtxIDs     []string  `json:"context_injected_ctx_ids,omitempty"`
	ContextSplicedCtxIDs      []string  `json:"context_spliced_ctx_ids,omitempty"`
	ContextForkVerifiedAt     time.Time `json:"context_fork_verified_at,omitempty"`
}

func validateLegacyClaude(src legacyClaudeSnapshot) error {
	if src.Contract != "task-board.claude-provider-host-v1" || strings.TrimSpace(src.SessionUUID) == "" || src.PID <= 0 {
		return fmt.Errorf("invalid Claude provider-host snapshot")
	}
	return nil
}
func projectClaudeSnapshot(src legacyClaudeSnapshot) ClaudeRunnerSnapshot {
	// Legacy snapshots did not persist environment values or an applied Claude
	// tool policy. Their record projection has no reconstructed ambient values.
	launch := projectLegacyClaudePlan(src)
	return ClaudeRunnerSnapshot{Contract: src.Contract, SessionUUID: src.SessionUUID, PID: src.PID, StartedAt: src.StartedAt, Plan: launch, Term: src.Term, Rows: src.Rows, Cols: src.Cols, Adopted: src.Adopted, Stopped: src.Stopped, ContextCtxID: src.ContextCtxID, ContextPrimaryCtxID: src.ContextPrimaryCtxID, ContextProjectSlug: src.ContextProjectSlug, ContextResumeSessionUUID: src.ContextResumeSessionUUID, ContextBaseSHA256: src.ContextBaseSHA256, ContextMaterializedSHA256: src.ContextMaterializedSHA256, ContextInjectedCtxIDs: copyCodecStrings(src.ContextInjectedCtxIDs), ContextSplicedCtxIDs: copyCodecStrings(src.ContextSplicedCtxIDs), ContextForkVerifiedAt: src.ContextForkVerifiedAt}
}
func mergeClaudeSnapshot(base legacyClaudeSnapshot, src ClaudeRunnerSnapshot) (legacyClaudeSnapshot, error) {
	if src.Plan.Environment != nil || src.Plan.ToolPolicy != (ClientToolPolicy{}) {
		return legacyClaudeSnapshot{}, ErrLegacyCodecMismatch
	}
	out := base
	out.Contract = src.Contract
	out.SessionUUID = src.SessionUUID
	out.PID = src.PID
	out.StartedAt = src.StartedAt
	if src.Plan.Claude == nil {
		return legacyClaudeSnapshot{}, fmt.Errorf("%w: complete admitted primary plan required", ErrHostCapabilityUnavailable)
	}
	out.Plan = copyPrimaryPlan(src.Plan.Claude.Plan)
	out.Term = src.Term
	out.Rows = src.Rows
	out.Cols = src.Cols
	out.Adopted = src.Adopted
	out.Stopped = src.Stopped
	out.ContextCtxID = src.ContextCtxID
	out.ContextPrimaryCtxID = src.ContextPrimaryCtxID
	out.ContextProjectSlug = src.ContextProjectSlug
	out.ContextResumeSessionUUID = src.ContextResumeSessionUUID
	out.ContextBaseSHA256 = src.ContextBaseSHA256
	out.ContextMaterializedSHA256 = src.ContextMaterializedSHA256
	out.ContextInjectedCtxIDs = copyCodecStrings(src.ContextInjectedCtxIDs)
	out.ContextSplicedCtxIDs = copyCodecStrings(src.ContextSplicedCtxIDs)
	out.ContextForkVerifiedAt = src.ContextForkVerifiedAt
	return out, nil
}
