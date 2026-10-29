package sessionmanager

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
)

// Legacy serialization stays codec-local. The optional budget is carried as
// data; owner/directive state stays raw and confers no runtime authority.
type legacyProcessIdentity struct {
	PID   int    `json:"pid"`
	UID   uint32 `json:"uid"`
	Start string `json:"start"`
	PGID  int    `json:"pgid"`
}
type legacyRolloutPrefix struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type legacyCodexSnapshot struct {
	ProcessIdentity      *legacyProcessIdentity   `json:"process_identity,omitempty"`
	GoalTokenBudget      *int64                   `json:"goal_token_budget,omitempty"`
	Contract             string                   `json:"contract"`
	PID                  int                      `json:"pid"`
	URL                  string                   `json:"url"`
	ThreadID             string                   `json:"thread_id"`
	Executable           string                   `json:"executable"`
	ProjectDir           string                   `json:"project_dir"`
	CodexHome            string                   `json:"codex_home"`
	HostArgv             []string                 `json:"host_argv"`
	ManagedClient        []string                 `json:"managed_client_argv"`
	LogPath              string                   `json:"log_path"`
	AutoContinue         CodexAutoContinueOptions `json:"auto_continue"`
	OwnerRun             json.RawMessage          `json:"owner_run,omitempty"`
	ForkedFromID         string                   `json:"forked_from_id,omitempty"`
	ProviderVersion      string                   `json:"provider_version,omitempty"`
	BaseRolloutSHA256    string                   `json:"base_rollout_sha256,omitempty"`
	InheritedLastTurnID  string                   `json:"inherited_last_turn_id,omitempty"`
	InheritedRollout     legacyRolloutPrefix      `json:"inherited_rollout,omitempty"`
	ManagedTurnID        string                   `json:"managed_turn_id,omitempty"`
	PendingGoalDirective string                   `json:"pending_goal_directive,omitempty"`
	LastCompletedTurnID  string                   `json:"last_completed_turn_id,omitempty"`
	Adopted              bool                     `json:"adopted,omitempty"`
	Stopped              bool                     `json:"stopped,omitempty"`
	ToolPolicy           ClientToolPolicy         `json:"tool_policy,omitzero"`
	Sources              []LaunchPlanSource       `json:"sources,omitempty"`
	CuratorHomeCopy      *CodexCuratorHomeCopy    `json:"curator_home_copy,omitempty"`
}

func validateLegacyCodex(src legacyCodexSnapshot) error {
	if src.Contract != "task-board.codex-provider-host-v1" || src.PID <= 0 || strings.TrimSpace(src.URL) == "" || strings.TrimSpace(src.ThreadID) == "" || !filepath.IsAbs(src.ProjectDir) || !filepath.IsAbs(src.CodexHome) {
		return fmt.Errorf("invalid Codex provider-host snapshot")
	}
	return nil
}
func copyCodecStrings(src []string) []string {
	if src == nil {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}
func copyCodecSources(src []LaunchPlanSource) []LaunchPlanSource {
	if src == nil {
		return nil
	}
	out := make([]LaunchPlanSource, len(src))
	for i, value := range src {
		out[i] = LaunchPlanSource{Kind: value.Kind, Path: value.Path, Scope: value.Scope, EnabledServers: copyCodecStrings(value.EnabledServers)}
	}
	return out
}
func copyCodecHome(src *CodexCuratorHomeCopy) *CodexCuratorHomeCopy {
	if src == nil {
		return nil
	}
	return &CodexCuratorHomeCopy{Profile: src.Profile, FragmentSHA256: src.FragmentSHA256, LockSHA256: src.LockSHA256, Absent: copyCodecStrings(src.Absent)}
}
func projectCodexSnapshot(src legacyCodexSnapshot) CodexRunnerSnapshot {
	out := CodexRunnerSnapshot{Contract: src.Contract, PID: src.PID, URL: src.URL, ThreadID: src.ThreadID, Executable: src.Executable, ProjectDir: src.ProjectDir, CodexHome: src.CodexHome, HostArgv: copyCodecStrings(src.HostArgv), ManagedClient: copyCodecStrings(src.ManagedClient), LogPath: src.LogPath, AutoContinue: CodexAutoContinueOptions{Enabled: src.AutoContinue.Enabled, DelaySeconds: src.AutoContinue.DelaySeconds, MaxNudgesPerTurn: src.AutoContinue.MaxNudgesPerTurn, Prompt: src.AutoContinue.Prompt}, ForkedFromID: src.ForkedFromID, ProviderVersion: src.ProviderVersion, BaseRolloutSHA256: src.BaseRolloutSHA256, InheritedLastTurnID: src.InheritedLastTurnID, InheritedRollout: CodexRolloutPrefix{Bytes: src.InheritedRollout.Bytes, SHA256: src.InheritedRollout.SHA256}, ManagedTurnID: src.ManagedTurnID, LastCompletedTurnID: src.LastCompletedTurnID, Adopted: src.Adopted, Stopped: src.Stopped, ToolPolicy: ClientToolPolicy{Sandbox: src.ToolPolicy.Sandbox, Approval: src.ToolPolicy.Approval}, Sources: copyCodecSources(src.Sources), CuratorHomeCopy: copyCodecHome(src.CuratorHomeCopy)}
	out.GoalTokenBudget = copyCodexGoalTokenBudget(src.GoalTokenBudget)
	if src.ProcessIdentity != nil {
		v := src.ProcessIdentity
		out.ProcessIdentity = &ProviderProcessIdentity{PID: v.PID, UID: v.UID, Start: v.Start, PGID: v.PGID}
	}
	return out
}
func mergeCodexSnapshot(prior legacyCodexSnapshot, src CodexRunnerSnapshot) legacyCodexSnapshot {
	// The decoded private base is local to this call. Only declared execution
	// members change here; live private-state integration awaits the D2 ruling.
	out := prior
	out.GoalTokenBudget = copyCodexGoalTokenBudget(src.GoalTokenBudget)
	out.Contract = src.Contract
	out.PID = src.PID
	out.URL = src.URL
	out.ThreadID = src.ThreadID
	out.Executable = src.Executable
	out.ProjectDir = src.ProjectDir
	out.CodexHome = src.CodexHome
	out.HostArgv = copyCodecStrings(src.HostArgv)
	out.ManagedClient = copyCodecStrings(src.ManagedClient)
	out.LogPath = src.LogPath
	out.AutoContinue = CodexAutoContinueOptions{Enabled: src.AutoContinue.Enabled, DelaySeconds: src.AutoContinue.DelaySeconds, MaxNudgesPerTurn: src.AutoContinue.MaxNudgesPerTurn, Prompt: src.AutoContinue.Prompt}
	out.ForkedFromID = src.ForkedFromID
	out.ProviderVersion = src.ProviderVersion
	out.BaseRolloutSHA256 = src.BaseRolloutSHA256
	out.InheritedLastTurnID = src.InheritedLastTurnID
	out.InheritedRollout = legacyRolloutPrefix{Bytes: src.InheritedRollout.Bytes, SHA256: src.InheritedRollout.SHA256}
	out.ManagedTurnID = src.ManagedTurnID
	out.LastCompletedTurnID = src.LastCompletedTurnID
	out.Adopted = src.Adopted
	out.Stopped = src.Stopped
	out.ToolPolicy = ClientToolPolicy{Sandbox: src.ToolPolicy.Sandbox, Approval: src.ToolPolicy.Approval}
	out.Sources = copyCodecSources(src.Sources)
	out.CuratorHomeCopy = copyCodecHome(src.CuratorHomeCopy)
	out.ProcessIdentity = nil
	if src.ProcessIdentity != nil {
		v := src.ProcessIdentity
		out.ProcessIdentity = &legacyProcessIdentity{PID: v.PID, UID: v.UID, Start: v.Start, PGID: v.PGID}
	}
	return out
}

// Checked metadata projection is separate from the existing execution-only
// projection. Archive fields are not admitted as a live owner capability.
func projectCodexSnapshotWithOwner(src legacyCodexSnapshot) (CodexRunnerSnapshot, error) {
	out := projectCodexSnapshot(src)
	owner, err := decodeCodexOwnerRun(src.OwnerRun)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	out.OwnerRun = copyCodexOwnerRun(owner)
	return out, nil
}

func mergeCodexSnapshotWithOwner(prior legacyCodexSnapshot, src CodexRunnerSnapshot) (legacyCodexSnapshot, error) {
	out := mergeCodexSnapshot(prior, src)
	// Like the original retained adapter, a nil new selection does not erase
	// an existing owner fact. Fresh construction selects nil independently.
	if src.OwnerRun == nil {
		out.OwnerRun = append(prior.OwnerRun[:0:0], prior.OwnerRun...)
		return out, nil
	}
	before, err := decodeCodexOwnerRun(prior.OwnerRun)
	if err != nil {
		return legacyCodexSnapshot{}, err
	}
	if before == nil {
		out.OwnerRun, err = json.Marshal(src.OwnerRun)
	} else {
		out.OwnerRun, err = encodeLegacySnapshot(prior.OwnerRun, before, src.OwnerRun)
	}
	if err != nil {
		return legacyCodexSnapshot{}, err
	}
	return out, nil
}

// An archived selection cannot be erased to admit a relaunch while the actual
// live budget consumer is unavailable. Zero is an explicit selection too.
// This reads admitted prior data and grants no launch or goal capability.
func validateCodexBudgetRelaunch(prior ProviderHostRecord) error {
	if prior.Provider != "codex" {
		return nil
	}
	snapshot := prior.Snapshot.Codex
	if prior.Snapshot.CodexFork != nil {
		snapshot = prior.Snapshot.CodexFork.Primary.Snapshot.Codex
	}
	if snapshot != nil && snapshot.GoalTokenBudget != nil {
		return legacyNativeStateUnavailable()
	}
	return nil
}

// Canonical record state, rather than the provider's Stopped flag, determines
// whether a merge stays archival. Pure archive edits may remove a selection;
// a live/recovering transition must retain the existing unsupported-state gate.
func validateCodexBudgetTransition(prior *legacyProviderImage, next RunnerRecord) error {
	if prior == nil || prior.Provider != "codex" || next.State == SessionStopped {
		return nil
	}
	var archived legacyCodexSnapshot
	if err := decodeLegacyJSON(prior.Opaque, &archived); err != nil {
		return err
	}
	if archived.GoalTokenBudget != nil {
		return legacyNativeStateUnavailable()
	}
	return nil
}
