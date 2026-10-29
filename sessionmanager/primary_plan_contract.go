package sessionmanager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

const PrimarySessionLaunchPlanContract = "agents-infra.primary-session-launch-plan"

type ResolvedLaunchValue struct {
	Value  any    `json:"value"`
	Source string `json:"source"`
}

type ResolvedMCP struct {
	Servers []json.RawMessage `json:"servers"`
	Sources []string          `json:"sources,omitempty"`
}

type ResolvedPrimarySession struct {
	Model     ResolvedLaunchValue `json:"model"`
	Reasoning ResolvedLaunchValue `json:"reasoning"`
	Yolo      ResolvedLaunchValue `json:"yolo"`
	Sandbox   ResolvedLaunchValue `json:"sandbox"`
	Profile   ResolvedLaunchValue `json:"profile"`
	Approval  ResolvedLaunchValue `json:"approval"`
	MCP       ResolvedMCP         `json:"mcp"`
	// PiCompatibility is emitted by agents-infra once it knows the `pi`
	// provider. It carries no meaning for a Codex or Claude managed session,
	// but the plan decoder rejects unknown fields, so an older CLI paired with
	// a newer composer refuses every spawn. Accepting and ignoring it keeps the
	// additive half of the contract forward-compatible.
	PiCompatibility *ResolvedLaunchValue `json:"pi_compatibility,omitempty"`
}

type LaunchPlanProducer struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
}

type PrimarySessionLaunchPlan struct {
	Contract         string                 `json:"contract"`
	SchemaVersion    int                    `json:"schema_version"`
	Status           string                 `json:"status"`
	Producer         LaunchPlanProducer     `json:"producer"`
	Provider         string                 `json:"provider"`
	ProjectDir       string                 `json:"project_dir"`
	Executable       string                 `json:"executable"`
	LaunchVariants   LaunchVariants         `json:"launch_variants"`
	Resolved         ResolvedPrimarySession `json:"resolved"`
	RequiredEnvNames []string               `json:"required_env_names"`
	Sources          []LaunchPlanSource     `json:"sources"`
}

func DecodePrimarySessionLaunchPlan(data []byte) (*PrimarySessionLaunchPlan, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var plan PrimarySessionLaunchPlan
	if err := decoder.Decode(&plan); err != nil {
		return nil, fmt.Errorf("decoding primary-session launch plan: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decoding primary-session launch plan: trailing JSON content")
	}
	if err := ValidatePrimarySessionLaunchPlan(&plan, ""); err != nil {
		return nil, err
	}
	return &plan, nil
}

func ValidatePrimarySessionLaunchPlan(plan *PrimarySessionLaunchPlan, provider string) error {
	if plan == nil {
		return fmt.Errorf("primary-session launch plan is required")
	}
	if plan.Contract != PrimarySessionLaunchPlanContract || plan.SchemaVersion != 1 || plan.Status != "ok" {
		return fmt.Errorf(
			"unsupported primary-session launch plan contract=%q schema=%d status=%q",
			plan.Contract,
			plan.SchemaVersion,
			plan.Status,
		)
	}
	normalized, err := normalizeProvider(plan.Provider)
	if err != nil {
		return err
	}
	if normalized != plan.Provider {
		return fmt.Errorf("launch plan provider must be normalized")
	}
	if provider != "" && normalized != provider {
		return fmt.Errorf("launch plan provider %q does not match %q", normalized, provider)
	}
	if strings.TrimSpace(plan.ProjectDir) == "" || !filepath.IsAbs(plan.ProjectDir) {
		return fmt.Errorf("launch plan project_dir must be absolute")
	}
	if strings.TrimSpace(plan.Executable) == "" || !filepath.IsAbs(plan.Executable) {
		return fmt.Errorf("launch plan executable must be absolute")
	}
	if plan.LaunchVariants.ManagedHost.Kind == "" {
		return fmt.Errorf("launch plan managed_host variant is required")
	}
	if normalized == "codex" {
		if plan.LaunchVariants.ManagedHost.Kind != "codex-app-server" {
			return fmt.Errorf("Codex launch plan managed_host.kind must be codex-app-server")
		}
		if len(plan.LaunchVariants.ManagedHost.Argv) == 0 {
			return fmt.Errorf("Codex launch plan managed_host.argv is required")
		}
	}
	// The Claude managed host runs the full interactive composition under the
	// manager PTY; an empty argv is valid when no policy flags were composed.
	if normalized == "claude" && plan.LaunchVariants.ManagedHost.Kind != "claude-pty" {
		return fmt.Errorf("Claude launch plan managed_host.kind must be claude-pty")
	}
	for _, name := range plan.RequiredEnvNames {
		if strings.TrimSpace(name) == "" || strings.Contains(name, "=") {
			return fmt.Errorf("launch plan required_env_names contains an invalid name")
		}
	}
	return nil
}
