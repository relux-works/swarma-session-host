package sessionmanager

import (
	"encoding/json"
	"fmt"
)

// cloneRunnerRecord owns every mutable member passed to/from the injected
// codec. The recovery chain is checked first so an invalid cyclic input cannot
// recurse indefinitely or hand the codec an alias into live host state.
func cloneRunnerRecord(record RunnerRecord) (RunnerRecord, error) {
	seen := map[*RunnerRecord]bool{}
	for previous := record.ConditionalStop; previous != nil; {
		if previous.Previous == nil || seen[previous.Previous] {
			return RunnerRecord{}, fmt.Errorf("invalid conditional-stop recovery chain")
		}
		seen[previous.Previous] = true
		if previous.Previous.SessionID != record.SessionID || previous.Previous.Owner != record.Owner || previous.Previous.Provider != record.Provider {
			return RunnerRecord{}, fmt.Errorf("conditional-stop recovery identity mismatch")
		}
		previous = previous.Previous.ConditionalStop
	}
	return copyRunnerRecord(record), nil
}

func copyRunnerRecord(src RunnerRecord) RunnerRecord {
	out := src
	out.ProviderHost.Snapshot = copyRunnerSnapshot(src.ProviderHost.Snapshot)
	if src.Eviction != nil {
		value := *src.Eviction
		out.Eviction = &value
	}
	if src.AppliedGoalRef != nil {
		value := *src.AppliedGoalRef
		out.AppliedGoalRef = &value
	}
	if src.ConditionalStop != nil {
		value := *src.ConditionalStop
		previous := copyRunnerRecord(*src.ConditionalStop.Previous)
		value.Previous = &previous
		out.ConditionalStop = &value
	}
	return out
}

func copyLaunchSources(src []LaunchPlanSource) []LaunchPlanSource {
	if src == nil {
		return nil
	}
	out := make([]LaunchPlanSource, len(src))
	for i, value := range src {
		out[i] = value
		out[i].EnabledServers = copyRunnerStrings(value.EnabledServers)
	}
	return out
}

func copyRunnerStrings(src []string) []string {
	if src == nil {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

func copyLaunchSpec(src LaunchSpec) LaunchSpec {
	out := src
	if src.Muse != nil {
		m := *src.Muse
		m.Env = copyRunnerStrings(m.Env)
		m.OperatorArgs = copyRunnerStrings(m.OperatorArgs)
		if m.Fragment != nil {
			m.Fragment = append(m.Fragment[:0:0], m.Fragment...)
		}
		out.Muse = &m
	}
	if src.Codex != nil {
		c := *src.Codex
		c.OwnerRun = copyCodexOwnerRun(c.OwnerRun)
		c.GoalTokenBudget = copyCodexGoalTokenBudget(c.GoalTokenBudget)
		c.Plan = copyPrimaryPlan(c.Plan)
		if c.ChildEnv != nil {
			c.ChildEnv = make(map[string]string, len(src.Codex.ChildEnv))
			for k, v := range src.Codex.ChildEnv {
				c.ChildEnv[k] = v
			}
		}
		c.Snapshot = *copyRunnerSnapshot(RunnerSnapshot{Codex: &c.Snapshot}).Codex
		out.Codex = &c
	}
	if src.Claude != nil {
		c := *src.Claude
		c.Plan = copyPrimaryPlan(c.Plan)
		out.Claude = &c
	}
	out.Environment = copyRunnerStrings(src.Environment)
	out.Sources = copyLaunchSources(src.Sources)
	out.Variants.Interactive.Argv = copyRunnerStrings(src.Variants.Interactive.Argv)
	out.Variants.ManagedHost.Argv = copyRunnerStrings(src.Variants.ManagedHost.Argv)
	out.Variants.ManagedClient.Argv = copyRunnerStrings(src.Variants.ManagedClient.Argv)
	return out
}

func copyMusePTYSnapshot(src MusePTYSnapshot) MusePTYSnapshot {
	out := src
	if src.VersionFallbackEvent != nil {
		value := *src.VersionFallbackEvent
		out.VersionFallbackEvent = &value
	}
	if src.VersionDeferralEvent != nil {
		value := *src.VersionDeferralEvent
		out.VersionDeferralEvent = &value
	}
	return out
}

func copyRunnerSnapshot(src RunnerSnapshot) RunnerSnapshot {
	out := src
	if src.CodexFork != nil {
		f := *src.CodexFork
		f.Primary.Snapshot = copyRunnerSnapshot(f.Primary.Snapshot)
		if f.Supplemental != nil {
			f.Supplemental = append(f.Supplemental[:0:0], f.Supplemental...)
		}
		out.CodexFork = &f
	}
	if src.ClaudeFork != nil {
		f := *src.ClaudeFork
		if f.Supplemental != nil {
			f.Supplemental = append(f.Supplemental[:0:0], f.Supplemental...)
		}
		out.ClaudeFork = &f
	}
	if src.Claude != nil {
		value := *src.Claude
		value.Plan = copyLaunchSpec(value.Plan)
		value.ContextInjectedCtxIDs = copyRunnerStrings(value.ContextInjectedCtxIDs)
		value.ContextSplicedCtxIDs = copyRunnerStrings(value.ContextSplicedCtxIDs)
		out.Claude = &value
	}
	if src.Codex != nil {
		value := *src.Codex
		value.OwnerRun = copyCodexOwnerRun(value.OwnerRun)
		value.GoalTokenBudget = copyCodexGoalTokenBudget(value.GoalTokenBudget)
		value.HostArgv = copyRunnerStrings(value.HostArgv)
		value.ManagedClient = copyRunnerStrings(value.ManagedClient)
		value.Sources = copyLaunchSources(value.Sources)
		if value.ProcessIdentity != nil {
			process := *value.ProcessIdentity
			value.ProcessIdentity = &process
		}
		if value.CuratorHomeCopy != nil {
			home := *value.CuratorHomeCopy
			home.Absent = copyRunnerStrings(home.Absent)
			value.CuratorHomeCopy = &home
		}
		out.Codex = &value
	}
	if src.MuseMSP != nil {
		value := *src.MuseMSP
		if value.NativeAccounting != nil {
			group := copyLegacyMuseNativeAccounting(*value.NativeAccounting)
			value.NativeAccounting = &group
		}
		if value.VersionFallbackEvent != nil {
			event := *value.VersionFallbackEvent
			value.VersionFallbackEvent = &event
		}
		if value.VersionDeferralEvent != nil {
			event := *value.VersionDeferralEvent
			value.VersionDeferralEvent = &event
		}
		out.MuseMSP = &value
	}
	if src.MusePTY != nil {
		value := copyMusePTYSnapshot(*src.MusePTY)
		out.MusePTY = &value
	}
	if src.MusePTYProvenance != nil {
		value := *src.MusePTYProvenance
		value.Base = copyMusePTYSnapshot(value.Base)
		out.MusePTYProvenance = &value
	}
	return out
}

func copyCodexGoalTokenBudget(src *int64) *int64 {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}

// validateRunnerSnapshot checks the closed union, without interpreting fork
// policy or using a caller-selected field as a backend discriminator.
func validateRunnerSnapshot(record ProviderHostRecord) error {
	s := record.Snapshot
	count := 0
	for _, present := range []bool{s.CodexFork != nil, s.ClaudeFork != nil, s.Claude != nil, s.Codex != nil, s.MuseMSP != nil, s.MusePTY != nil, s.MusePTYProvenance != nil} {
		if present {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("runner snapshot requires exactly one typed backend")
	}
	valid := false
	switch s.Kind {
	case "claude":
		valid = record.Provider == "claude" && s.Claude != nil
	case "codex":
		valid = record.Provider == "codex" && s.Codex != nil
	case "muse-msp":
		valid = record.Provider == "muse" && s.MuseMSP != nil
	case "muse-pty":
		valid = record.Provider == "muse" && s.MusePTY != nil
	case "muse-pty-provenance":
		valid = record.Provider == "muse" && s.MusePTYProvenance != nil
	}
	if !valid {
		return fmt.Errorf("runner snapshot kind does not match registered provider")
	}
	return nil
}

// Values originate from the strict primary-plan JSON decoder. Preserve their
// concrete JSON shapes while detaching nested maps, arrays and raw MCP receipts.
func copyPlanJSONValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		if v == nil {
			return v
		}
		out := make(map[string]any, len(v))
		for k, x := range v {
			out[k] = copyPlanJSONValue(x)
		}
		return out
	case []any:
		if v == nil {
			return v
		}
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = copyPlanJSONValue(x)
		}
		return out
	case json.RawMessage:
		if v == nil {
			return v
		}
		return append(v[:0:0], v...)
	default:
		return value
	}
}
func copyPrimaryPlan(src PrimarySessionLaunchPlan) PrimarySessionLaunchPlan {
	out := src
	out.RequiredEnvNames = copyRunnerStrings(src.RequiredEnvNames)
	out.Sources = copyLaunchSources(src.Sources)
	out.LaunchVariants.Interactive.Argv = copyRunnerStrings(src.LaunchVariants.Interactive.Argv)
	out.LaunchVariants.ManagedHost.Argv = copyRunnerStrings(src.LaunchVariants.ManagedHost.Argv)
	out.LaunchVariants.ManagedClient.Argv = copyRunnerStrings(src.LaunchVariants.ManagedClient.Argv)
	out.Resolved.Model.Value = copyPlanJSONValue(src.Resolved.Model.Value)
	out.Resolved.Reasoning.Value = copyPlanJSONValue(src.Resolved.Reasoning.Value)
	out.Resolved.Yolo.Value = copyPlanJSONValue(src.Resolved.Yolo.Value)
	out.Resolved.Sandbox.Value = copyPlanJSONValue(src.Resolved.Sandbox.Value)
	out.Resolved.Profile.Value = copyPlanJSONValue(src.Resolved.Profile.Value)
	out.Resolved.Approval.Value = copyPlanJSONValue(src.Resolved.Approval.Value)
	if src.Resolved.PiCompatibility != nil {
		v := *src.Resolved.PiCompatibility
		v.Value = copyPlanJSONValue(v.Value)
		out.Resolved.PiCompatibility = &v
	}
	out.Resolved.MCP.Sources = copyRunnerStrings(src.Resolved.MCP.Sources)
	if src.Resolved.MCP.Servers != nil {
		out.Resolved.MCP.Servers = make([]json.RawMessage, len(src.Resolved.MCP.Servers))
		for i, r := range src.Resolved.MCP.Servers {
			out.Resolved.MCP.Servers[i] = append(r[:0:0], r...)
		}
	}
	return out
}
