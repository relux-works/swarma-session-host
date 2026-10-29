//go:build unix

package hostdefaults

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	schema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
	"github.com/relux-works/swarma-session-host/musepty"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

const LegacyCodexSessionLaunchContract = "task-board.codex-session-launch-v1"

// LegacyOwnerRunContext preserves the original launch envelope as DATA. The
// runtime-control snapshot and legacy board/goal locations are not host rights.
// Their real raising-side validation and environment projection are required
// separately; this module does not load a board or copy its authority engine.
type LegacyOwnerRunContext struct {
	RunID          string          `json:"run_id"`
	TaskID         string          `json:"task_id"`
	BoardDir       string          `json:"board_dir,omitempty"`
	ContextCtxID   string          `json:"context_ctx_id,omitempty"`
	DeliveryGoalID string          `json:"delivery_goal_id"`
	RuntimeControl json.RawMessage `json:"runtime_control,omitempty"`
}
type LegacyCodexSessionLaunch struct {
	ChildEnv        map[string]string             `json:"child_env,omitempty"`
	GoalTokenBudget *int64                        `json:"goal_token_budget,omitempty"`
	Contract        string                        `json:"contract"`
	Plan            core.PrimarySessionLaunchPlan `json:"primary_session_plan"`
	AutoContinue    core.CodexAutoContinueOptions `json:"auto_continue"`
	OwnerRun        *LegacyOwnerRunContext        `json:"owner_run,omitempty"`
	ResumeThreadID  string                        `json:"resume_thread_id,omitempty"`
}
type LegacyClaudeSessionLaunch struct {
	Contract          string                        `json:"contract"`
	Plan              core.PrimarySessionLaunchPlan `json:"primary_session_plan"`
	Term              string                        `json:"term,omitempty"`
	Rows              uint16                        `json:"rows,omitempty"`
	Cols              uint16                        `json:"cols,omitempty"`
	OwnerRun          *LegacyOwnerRunContext        `json:"owner_run,omitempty"`
	ResumeSessionUUID string                        `json:"resume_session_uuid,omitempty"`
}

func decodeLegacyLaunch(raw []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	return d.Decode(value) // original single-value envelope decoder
}
func validateLegacyOwnerShape(owner *LegacyOwnerRunContext) error {
	if owner == nil {
		return nil
	}
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(owner.RunID)), "RUN-") {
		return fmt.Errorf("owner_run.run_id must be a RUN-* identifier")
	}
	if strings.TrimSpace(owner.TaskID) == "" {
		return fmt.Errorf("owner_run.task_id is required")
	}
	ctx := strings.TrimSpace(owner.ContextCtxID)
	if ctx != "" && !strings.HasPrefix(strings.ToUpper(ctx), "CTX-") {
		return fmt.Errorf("owner_run.context_ctx_id must be a CTX-* identifier")
	}
	if strings.TrimSpace(owner.DeliveryGoalID) == "" && ctx == "" {
		return fmt.Errorf("owner_run requires an owning plane: either delivery_goal_id or context_ctx_id")
	}
	if len(owner.RuntimeControl) > 0 && string(owner.RuntimeControl) != "null" {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(owner.RuntimeControl, &object); err != nil || object == nil {
			return fmt.Errorf("owner_run.runtime_control must be an object")
		}
	}
	return nil
}

var legacyThreadID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// DecodeLegacyCodexSessionLaunch relocates the original pure plan, overlay,
// auto-continue, selector and owner-shape checks. RuntimeControl is retained
// intact but its authority is not inferred or validated by this shape parser.
func DecodeLegacyCodexSessionLaunch(raw []byte) (*LegacyCodexSessionLaunch, error) {
	var launch LegacyCodexSessionLaunch
	if err := decodeLegacyLaunch(raw, &launch); err != nil {
		return nil, fmt.Errorf("decoding Codex session launch: %w", err)
	}
	if len(launch.ChildEnv) > 0 {
		b, _ := json.Marshal(launch.ChildEnv)
		if schema.Validate("child-env.schema.json", b) != nil {
			return nil, errors.New("invalid non-secret child environment")
		}
		for name := range launch.ChildEnv {
			if schema.ReservedChildName(name) {
				return nil, errors.New("reserved child environment name")
			}
		}
	}
	if launch.Contract != LegacyCodexSessionLaunchContract {
		return nil, fmt.Errorf("unsupported Codex session launch contract %q", launch.Contract)
	}
	if err := core.ValidatePrimarySessionLaunchPlan(&launch.Plan, "codex"); err != nil {
		return nil, err
	}
	if launch.AutoContinue.DelaySeconds < 0 || launch.AutoContinue.MaxNudgesPerTurn < 0 {
		return nil, fmt.Errorf("Codex auto-continue values cannot be negative")
	}
	if strings.TrimSpace(launch.ResumeThreadID) != "" {
		normalized := strings.ToLower(strings.TrimSpace(launch.ResumeThreadID))
		if !legacyThreadID.MatchString(normalized) {
			return nil, &core.PublicError{Code: "codex_thread_adoption_invalid", Message: fmt.Sprintf("%q is not a Codex thread ID (expected the 8-4-4-4-12 hex form shown by `codex resume --all`)", strings.TrimSpace(launch.ResumeThreadID))}
		}
		launch.ResumeThreadID = normalized
	}
	if err := validateLegacyOwnerShape(launch.OwnerRun); err != nil {
		return nil, err
	}
	return &launch, nil
}
func DecodeLegacyClaudeSessionLaunch(raw []byte) (*LegacyClaudeSessionLaunch, error) {
	var launch LegacyClaudeSessionLaunch
	if err := decodeLegacyLaunch(raw, &launch); err != nil {
		return nil, fmt.Errorf("decoding Claude session launch: %w", err)
	}
	if launch.Contract != core.ClaudeSessionLaunchContract {
		return nil, fmt.Errorf("unsupported Claude session launch contract %q", launch.Contract)
	}
	if err := core.ValidatePrimarySessionLaunchPlan(&launch.Plan, "claude"); err != nil {
		return nil, err
	}
	if err := validateLegacyOwnerShape(launch.OwnerRun); err != nil {
		return nil, err
	}
	if strings.TrimSpace(launch.ResumeSessionUUID) != "" {
		normalized, err := core.NormalizeClaudeSessionUUID(launch.ResumeSessionUUID)
		if err != nil {
			return nil, err
		}
		launch.ResumeSessionUUID = normalized
	}
	return &launch, nil
}

// LegacyAttachRequest is the exact original protocol8 request. Launch remains
// the complete original provider envelope, never decoded as a guessed LaunchSpec.
type LegacyAttachRequest struct {
	Owner                core.Protocol8SessionOwner `json:"owner"`
	Provider             string                     `json:"provider"`
	Launch               json.RawMessage            `json:"launch,omitempty"`
	ProjectDir           string                     `json:"project_dir,omitempty"`
	BoardFingerprint     string                     `json:"board_fingerprint,omitempty"`
	Resume               bool                       `json:"resume,omitempty"`
	ResumeSessionID      string                     `json:"resume_session_id,omitempty"`
	Adopt                bool                       `json:"adopt,omitempty"`
	MaterializationCtxID string                     `json:"materialization_ctx_id,omitempty"`
}
type LegacyAttachLaunch struct {
	Request LegacyAttachRequest
	Owner   core.OwnerKey
	Codex   *LegacyCodexSessionLaunch
	Claude  *LegacyClaudeSessionLaunch
	Muse    *core.MuseSessionLaunch
}

// DecodeLegacyAttachRequest performs syntax/provider decoding only. It reads no
// files, grants no caller rights and never starts a provider. Optional Launch is
// retained for an existing-session attach; creation still needs a real payload.
func DecodeLegacyAttachRequest(raw []byte) (LegacyAttachLaunch, error) {
	var result LegacyAttachLaunch
	if len(raw) > 1<<20 {
		return result, fmt.Errorf("decoding request: body exceeds 1MiB")
	}
	if err := decodeLegacyLaunch(raw, &result.Request); err != nil {
		return result, fmt.Errorf("decoding request: %w", err)
	}
	owner, err := core.DecodeProtocol8Owner(result.Request.Owner)
	if err != nil {
		return result, err
	}
	result.Owner = owner
	switch strings.ToLower(strings.TrimSpace(result.Request.Provider)) {
	case "codex", "claude", "muse":
	default:
		return result, core.ErrInvalidInput
	}
	if len(result.Request.Launch) == 0 || string(result.Request.Launch) == "null" {
		return result, nil
	}
	switch strings.ToLower(strings.TrimSpace(result.Request.Provider)) {
	case "codex":
		result.Codex, err = DecodeLegacyCodexSessionLaunch(result.Request.Launch)
	case "claude":
		result.Claude, err = DecodeLegacyClaudeSessionLaunch(result.Request.Launch)
	case "muse":
		result.Muse, err = musepty.DecodeMuseSessionLaunch(result.Request.Launch)
	}
	return result, err
}
