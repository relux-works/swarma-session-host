package sessionmanager

import (
	"fmt"
	"path/filepath"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/codex"
)

// composedCodexSystem retains the executable already resolved by composition.
// All argument, environment, validation and sealing behavior stays in the module.
type composedCodexSystem struct {
	*codex.System
	executable string
}

func (s composedCodexSystem) ResolveBinary(agentic.LaunchRequest) (string, error) {
	if !filepath.IsAbs(s.executable) {
		return "", fmt.Errorf("composed Codex executable must be absolute")
	}
	return s.executable, nil
}

// BuildManagedCodexPlan transports semantic inputs to the module while retaining
// the composer's executable identity. It does not execute a provider process.
func BuildManagedCodexPlan(executable string, req agentic.LaunchRequest) (agentic.Plan, error) {
	registry := agentic.NewRegistry()
	system := composedCodexSystem{System: codex.New(), executable: executable}
	if err := registry.Register(system); err != nil {
		return agentic.Plan{}, err
	}
	req.System = system.ID()
	return agentic.BuildPlan(registry, req, agentic.LaunchModeInteractive)
}
