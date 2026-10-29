package sessionmanager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CodexHostFactory executes the admitted, fully prepared native payload.
// Canonical owners, private goals and launch policy do not enter native state.
type CodexHostFactory struct {
	contextPolicy        CodexContextPolicy
	layout               Layout
	assaySource          func(context.Context, *CodexRunnerSnapshot) (*codexRPCClient, func(), error)
	assayStabilityWindow time.Duration
}

// NewCodexHostFactory optionally binds the host's context service.
// Ordinary goal-less launches and restore do not require that capability.
func NewCodexHostFactory(layout Layout, policy ...CodexContextPolicy) *CodexHostFactory {
	f := &CodexHostFactory{layout: layout}
	if len(policy) > 0 {
		f.contextPolicy = policy[0]
	}
	return f
}
func (*CodexHostFactory) Provider() string { return "codex" }
func (f *CodexHostFactory) Launch(ctx context.Context, q ProviderLaunchRequest) (RunnerHost, error) {
	if err := validateLaunchPayload("codex", q.Launch); err != nil {
		return nil, err
	}
	if q.Launch.Codex == nil {
		return nil, ErrHostCapabilityUnavailable
	}
	native := copyLaunchSpec(q.Launch)
	prepared := native.Codex
	var forkPlan *CodexContextForkPlan
	var recovered *CodexRunnerSnapshot
	if q.Prior != nil {
		var err error
		if q.Prior.Snapshot.Kind == "codex-fork" {
			forkPlan, err = decodeCodexForkPlan(*q.Prior)
			if err != nil {
				return nil, codexForkPlanFailure(err)
			}
			if f.contextPolicy == nil {
				return nil, ErrHostCapabilityUnavailable
			}
		} else {
			if q.PriorMustFork {
				return nil, codexForkPlanFailure(ErrInvalidInput)
			}
			recovered, err = decodeCodexHostSnapshot(*q.Prior)
			if err != nil {
				return nil, codexCapabilityFailure("provider-host recovery", "", err)
			}
		}
	} else if q.PriorMustFork {
		return nil, codexForkPlanFailure(ErrInvalidInput)
	}
	if q.Prior != nil {
		if err := validateCodexBudgetRelaunch(*q.Prior); err != nil {
			return nil, err
		}
	}
	preparedSnapshot, err := prepareCodexLaunchSnapshot(prepared)
	if err != nil {
		return nil, err
	}
	preparedSnapshot.OwnerRun, err = prepareCodexOwnerRun(prepared, recovered)
	if err != nil {
		return nil, err
	}
	// Metadata alone cannot enable the missing raising-side owner consumer.
	// Refuse before plan/environment/files/native effects, as the live codec does.
	if preparedSnapshot.OwnerRun != nil {
		return nil, legacyNativeStateUnavailable()
	}
	snapshot := RunnerSnapshot{Kind: "codex", Codex: &preparedSnapshot}
	if err := ValidatePrimarySessionLaunchPlan(&prepared.Plan, "codex"); err != nil {
		return nil, err
	}
	if err := RefuseLockedCodexLaunch(&prepared.Plan); err != nil {
		return nil, err
	}
	for _, name := range prepared.Plan.RequiredEnvNames {
		if _, ok := os.LookupEnv(name); !ok {
			return nil, fmt.Errorf("required launch environment %s is not set", name)
		}
	}
	if q.SessionID == "" || filepath.Base(q.SessionID) != q.SessionID || q.SessionID == "." || q.SessionID == ".." {
		return nil, ErrInvalidInput
	}
	if native.Cwd != prepared.Plan.ProjectDir || native.Executable != prepared.Plan.Executable {
		return nil, ErrInvalidInput
	}
	sessionDir := filepath.Join(f.layout.SessionsDir, q.SessionID)
	if err := os.MkdirAll(sessionDir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(sessionDir, 0700); err != nil {
		return nil, err
	}
	native.Variants = prepared.Plan.LaunchVariants
	native.Sources = copyLaunchSources(prepared.Plan.Sources)
	policy := codexAppliedToolPolicy(&prepared.Plan, q.BuilderConfinement == nil)
	home, homeCopy, err := prepareCuratorCodexHome(ctx, sessionDir, &prepared.Plan, q.BuilderConfinement, policy)
	if err != nil {
		return nil, err
	}
	snapshot.Codex.Contract = codexHostSnapshotContract
	snapshot.Codex.PID = 0
	snapshot.Codex.URL = ""
	snapshot.Codex.ProcessIdentity = nil
	snapshot.Codex.Stopped = false
	snapshot.Codex.Executable = native.Executable
	snapshot.Codex.ProjectDir = native.Cwd
	snapshot.Codex.CodexHome = home
	snapshot.Codex.LogPath = filepath.Join(sessionDir, "codex-app-server.log")
	snapshot.Codex.AutoContinue = prepared.AutoContinue
	snapshot.Codex.CuratorHomeCopy = homeCopy
	snapshot.Codex.Sources = copyLaunchSources(prepared.Plan.Sources)
	if recovered != nil {
		snapshot.Codex.ThreadID = recovered.ThreadID
		snapshot.Codex.ForkedFromID = recovered.ForkedFromID
		snapshot.Codex.ProviderVersion = recovered.ProviderVersion
		snapshot.Codex.BaseRolloutSHA256 = recovered.BaseRolloutSHA256
		snapshot.Codex.InheritedLastTurnID = recovered.InheritedLastTurnID
		snapshot.Codex.InheritedRollout = recovered.InheritedRollout
		snapshot.Codex.ManagedTurnID = recovered.ManagedTurnID
		snapshot.Codex.LastCompletedTurnID = recovered.LastCompletedTurnID
	}
	if forkPlan != nil {
		forked, err := f.materializeContextFork(ctx, forkPlan, home, native.Cwd)
		if err != nil {
			return nil, err
		}
		snapshot.Codex.ThreadID = forked.ThreadID
		snapshot.Codex.ForkedFromID = forked.ForkedFromID
		snapshot.Codex.ProviderVersion = forked.ProviderVersion
		snapshot.Codex.BaseRolloutSHA256 = forked.BaseRolloutSHA256
		snapshot.Codex.InheritedLastTurnID = forked.LastTurnID
		snapshot.Codex.InheritedRollout = forked.InheritedRollout
	}
	if prepared.ResumeThreadID != "" && q.Prior == nil {
		if strings.TrimSpace(snapshot.Codex.ThreadID) != "" && snapshot.Codex.ThreadID != prepared.ResumeThreadID {
			return nil, ErrInvalidInput
		}
		snapshot.Codex.ThreadID = prepared.ResumeThreadID
		snapshot.Codex.Adopted = true
		if err := prepareCodexAdoptionStore(home, prepared.ResumeThreadID); err != nil {
			return nil, err
		}
	}
	native.ToolPolicy = policy
	for key, value := range prepared.ChildEnv {
		native.Environment = replaceEnvironmentValue(native.Environment, key, value)
	}
	model := prepared.Model
	if model == "" {
		model = codexResolvedString(prepared.Plan.Resolved.Model)
	}
	return startCodexRunner(ctx, native, *snapshot.Codex, model, q.BuilderConfinement)
}

// Preparation selects the launch budget, including an absent selection, before
// any native or filesystem effects. Carrying it does not apply a goal budget.
func prepareCodexLaunchSnapshot(prepared *CodexPreparedLaunch) (CodexRunnerSnapshot, error) {
	if prepared == nil || prepared.GoalTokenBudget != nil && *prepared.GoalTokenBudget < 0 {
		return CodexRunnerSnapshot{}, ErrHostCapabilityUnavailable
	}
	owner, err := prepareCodexOwnerRun(prepared, nil)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	snapshot := copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &prepared.Snapshot})
	snapshot.Codex.OwnerRun = owner
	snapshot.Codex.GoalTokenBudget = copyCodexGoalTokenBudget(prepared.GoalTokenBudget)
	return *snapshot.Codex, nil
}

func (*CodexHostFactory) Restore(ctx context.Context, q ProviderHostRecord) (RunnerHost, error) {
	if q.Provider != "codex" || q.Snapshot.Kind != "codex" || q.Snapshot.Codex == nil {
		return nil, ErrInvalidInput
	}
	if err := validateRunnerSnapshot(q); err != nil {
		return nil, ErrInvalidInput
	}
	owned := copyRunnerSnapshot(q.Snapshot)
	return RestoreCodexRunner(ctx, *owned.Codex)
}
func (*CodexHostFactory) RestoreRetiring(ctx context.Context, q ProviderHostRecord) (RunnerHost, error) {
	if q.Provider != "codex" || q.Snapshot.Kind != "codex" || q.Snapshot.Codex == nil {
		return nil, ErrInvalidInput
	}
	if err := validateRunnerSnapshot(q); err != nil {
		return nil, ErrInvalidInput
	}
	owned := copyRunnerSnapshot(q.Snapshot)
	return NewCodexRetirementRunner(*owned.Codex)
}

var _ ProviderHostFactory = (*CodexHostFactory)(nil)
var _ RetiringHostFactory = (*CodexHostFactory)(nil)

const (
	CodexErrorThreadNotFound     = "codex_thread_not_found"
	CodexErrorThreadResumeFailed = "codex_thread_resume_failed"
)

func codexAppliedToolPolicy(
	plan *PrimarySessionLaunchPlan,
	includeOrdinaryToolPolicy bool,
) ClientToolPolicy {
	if plan == nil || !includeOrdinaryToolPolicy {
		return ClientToolPolicy{}
	}
	return ClientToolPolicy{
		Sandbox:  codexResolvedString(plan.Resolved.Sandbox),
		Approval: codexResolvedString(plan.Resolved.Approval),
	}
}

func codexResolvedString(value ResolvedLaunchValue) string {
	if text, ok := value.Value.(string); ok {
		return strings.TrimSpace(text)
	}
	return ""
}
