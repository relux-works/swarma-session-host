//go:build unix

package hostdefaults

import (
	"encoding/json"
	"errors"
	"strings"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

var ErrLegacyOwnerEnvironmentRequired = errors.New("verified legacy owner runtime-control and environment supplier required")
var ErrLegacyCodexBudgetConsumerRequired = errors.New("legacy Codex token-budget consumer required")

// PrepareLegacyAttach relocates the real raw-envelope to typed-provider
// conversion. It is pure DATA preparation, not caller admission. environment
// must come from the actual verified raising-side source before an effectful
// caller uses the result; this function never reads ambient process credentials.
// Missing consumers refuse rather than dropping an original field.
func PrepareLegacyAttach(request LegacyAttachRequest, environment []string) (core.AttachRequest, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return core.AttachRequest{}, err
	}
	parsed, err := DecodeLegacyAttachRequest(encoded)
	if err != nil {
		return core.AttachRequest{}, err
	}
	q := core.AttachRequest{Owner: parsed.Owner, Provider: strings.ToLower(strings.TrimSpace(parsed.Request.Provider)), ProjectDir: request.ProjectDir, NamespaceFingerprint: request.BoardFingerprint, Resume: request.Resume, ResumeSessionID: request.ResumeSessionID, Adopt: request.Adopt, MaterializationCtxID: request.MaterializationCtxID}
	switch {
	case parsed.Codex != nil:
		c := parsed.Codex
		if c.OwnerRun != nil {
			return core.AttachRequest{}, ErrLegacyOwnerEnvironmentRequired
		}
		if c.GoalTokenBudget != nil {
			return core.AttachRequest{}, ErrLegacyCodexBudgetConsumerRequired
		}
		if c.ResumeThreadID != "" && parsed.Owner != "parent/primary" {
			return core.AttachRequest{}, &core.PublicError{Code: "codex_thread_adoption_invalid", Message: "Codex thread adoption is supported only for the parent/primary owner"}
		}
		if err := core.RefuseLockedCodexLaunch(&c.Plan); err != nil {
			return core.AttachRequest{}, err
		}
		q.Launch = core.LaunchSpec{Executable: c.Plan.Executable, Cwd: c.Plan.ProjectDir, Variants: c.Plan.LaunchVariants, Environment: append([]string(nil), environment...), Sources: c.Plan.Sources, Codex: &core.CodexPreparedLaunch{Plan: c.Plan, AutoContinue: c.AutoContinue, ResumeThreadID: c.ResumeThreadID, ChildEnv: c.ChildEnv}}
	case parsed.Claude != nil:
		c := parsed.Claude
		if c.OwnerRun != nil {
			return core.AttachRequest{}, ErrLegacyOwnerEnvironmentRequired
		}
		q.Launch = core.LaunchSpec{Executable: c.Plan.Executable, Cwd: c.Plan.ProjectDir, Variants: c.Plan.LaunchVariants, Environment: append([]string(nil), environment...), Sources: c.Plan.Sources, Claude: &core.ClaudePreparedLaunch{Contract: c.Contract, Plan: c.Plan, Term: c.Term, Rows: c.Rows, Cols: c.Cols, ResumeSessionUUID: c.ResumeSessionUUID}}
	case parsed.Muse != nil:
		m := parsed.Muse
		q.Launch = core.LaunchSpec{Cwd: m.WorkDir, Environment: append([]string(nil), m.Env...), Muse: m}
	}
	// The original provider checks remain authoritative; preparation merely
	// preserves required environment names without choosing their values.
	var required []string
	if q.Launch.Codex != nil {
		required = q.Launch.Codex.Plan.RequiredEnvNames
	}
	if q.Launch.Claude != nil {
		required = q.Launch.Claude.Plan.RequiredEnvNames
	}
	for _, name := range required {
		present := false
		for _, entry := range q.Launch.Environment {
			if strings.HasPrefix(entry, name+"=") {
				present = true
				break
			}
		}
		if !present {
			return core.AttachRequest{}, errors.New("verified launch environment is missing a required name")
		}
	}
	return q, nil
}
