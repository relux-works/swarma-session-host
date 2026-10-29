package sessionmanager

import "fmt"

// CodexPreparedLaunch is the existing StartCodexRunner launch state and model.
// Canonical goal/owner validation prepares it privately before the factory call.
type CodexPreparedLaunch struct {
	// OwnerRun is the actual selected structured launch metadata.
	OwnerRun *OwnerRunContext
	// GoalTokenBudget retains the optional selected value, not an applied goal.
	GoalTokenBudget *int64
	Plan            PrimarySessionLaunchPlan
	AutoContinue    CodexAutoContinueOptions
	ResumeThreadID  string
	ChildEnv        map[string]string

	Snapshot CodexRunnerSnapshot
	Model    string
}

// ClaudePreparedLaunch retains the admitted primary plan, geometry and explicit
// adoption intent. Canonical owner-run environment is prepared before this port.
type ClaudePreparedLaunch struct {
	Contract          string
	Plan              PrimarySessionLaunchPlan
	Term              string
	Rows              uint16
	Cols              uint16
	ResumeSessionUUID string
}

// Existing common launch plans may omit a provider payload. A concrete payload
// cannot be silently ignored by another provider or compete with another variant.
func validateLaunchPayload(provider string, launch LaunchSpec) error {
	count := 0
	selected := ""
	if launch.Claude != nil {
		count++
		selected = "claude"
	}
	if launch.Codex != nil {
		count++
		selected = "codex"
	}
	if launch.Muse != nil {
		count++
		selected = "muse"
	}
	if count > 1 || count == 1 && selected != provider {
		return fmt.Errorf("%w: ambiguous or foreign typed launch payload", ErrInvalidInput)
	}
	return nil
}
