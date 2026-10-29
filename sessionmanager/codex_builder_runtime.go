package sessionmanager

import (
	"fmt"
	"path/filepath"
)

func codexBuilderRuntimeReadRoots(codexHome string) []string {
	roots := []string{codexHome, filepath.Join(filepath.Dir(codexHome), "builder-runtime")}
	if sourceHome, err := codexSourceHome(); err == nil {
		roots = append(roots, filepath.Join(sourceHome, "auth.json"))
	}
	return roots
}

func (h *codexRunner) BuilderIngestionConfinement() (BuilderConfinementResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.builderConfinement == nil {
		return BuilderConfinementResult{}, fmt.Errorf(
			"Codex host was not launched under context-builder confinement",
		)
	}
	return BuilderConfinementResult{
		Mechanism:     h.builderConfinement.Mechanism,
		EnforcedRoots: append([]string(nil), h.builderConfinement.EnforcedRoots...),
	}, nil
}

// BuilderToolIsolation reports the tool surface this launch was actually
// composed with, on the same terms as the Claude adapter: unavailable unless the
// process was started under it.
func (h *codexRunner) BuilderToolIsolation() (BuilderToolIsolationResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.builderToolIsolation == nil {
		return BuilderToolIsolationResult{}, fmt.Errorf(
			"Codex host was not launched under context-builder tool isolation",
		)
	}
	return BuilderToolIsolationResult{
		Mechanism:      h.builderToolIsolation.Mechanism,
		DeniedSurfaces: append([]string(nil), h.builderToolIsolation.DeniedSurfaces...),
		GatewayTools:   append([]string(nil), h.builderToolIsolation.GatewayTools...),
	}, nil
}
