package sessionmanager

import (
	"fmt"
	contextpolicy "github.com/relux-works/swarma-session-host/sessionmanager/resource"
	"sort"
	"strings"
)

// Validate the observed runtime report shape; canonical publication stays with the caller.
func requireBuilderToolIsolation(
	ctxID string,
	isolation BuilderToolIsolationResult,
) (BuilderToolIsolationResult, error) {
	normalized := contextpolicy.NormalizeContextID(ctxID)
	mechanism := strings.TrimSpace(isolation.Mechanism)
	if mechanism == "" {
		return BuilderToolIsolationResult{}, &PublicError{
			Code: "context_builder_tool_isolation_unavailable",
			Message: fmt.Sprintf(
				"the provider launch boundary reported no tool-isolation mechanism for %s, so nothing "+
					"establishes that the repository-evidence gateway was the builder's only read surface",
				normalized,
			),
		}
	}
	denied := normalizeBuilderSurfaces(isolation.DeniedSurfaces)
	if missing := missingBuilderSurfaces(denied); len(missing) > 0 {
		return BuilderToolIsolationResult{}, &PublicError{
			Code: "context_builder_tool_isolation_unavailable",
			Message: fmt.Sprintf(
				"the provider launch boundary for %s left %d repository surface(s) reachable beside the "+
					"evidence gateway: %s",
				normalized, len(missing), strings.Join(missing, ", "),
			),
		}
	}
	tools := normalizeBuilderSurfaces(isolation.GatewayTools)
	if len(tools) == 0 {
		return BuilderToolIsolationResult{}, &PublicError{
			Code: "context_builder_tool_isolation_unavailable",
			Message: fmt.Sprintf(
				"the provider launch boundary for %s reported no reachable gateway tools, so the builder "+
					"had no instrumented way to read the repository at all",
				normalized,
			),
		}
	}
	if extra := toolsOutsideGateway(tools); len(extra) > 0 {
		return BuilderToolIsolationResult{}, &PublicError{
			Code: "context_builder_tool_isolation_unavailable",
			Message: fmt.Sprintf(
				"the provider launch boundary for %s left %d tool(s) outside the evidence gateway "+
					"reachable: %s",
				normalized, len(extra), strings.Join(extra, ", "),
			),
		}
	}
	return BuilderToolIsolationResult{
		Mechanism:      mechanism,
		DeniedSurfaces: denied,
		GatewayTools:   tools,
	}, nil
}

func normalizeBuilderSurfaces(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		clean := strings.ToLower(strings.TrimSpace(value))
		if clean == "" {
			continue
		}
		if _, duplicate := seen[clean]; duplicate {
			continue
		}
		seen[clean] = struct{}{}
		normalized = append(normalized, clean)
	}
	sort.Strings(normalized)
	return normalized
}

func missingBuilderSurfaces(denied []string) []string {
	present := make(map[string]struct{}, len(denied))
	for _, surface := range denied {
		present[surface] = struct{}{}
	}
	missing := make([]string, 0, len(builderRequiredDeniedSurfaces))
	for _, required := range builderRequiredDeniedSurfaces {
		if _, ok := present[required]; !ok {
			missing = append(missing, required)
		}
	}
	return missing
}

func toolsOutsideGateway(tools []string) []string {
	allowed := make(map[string]struct{}, len(builderGatewayToolNames))
	for _, tool := range builderGatewayToolNames {
		allowed[tool] = struct{}{}
	}
	extra := make([]string, 0, len(tools))
	for _, tool := range tools {
		if _, ok := allowed[tool]; !ok {
			extra = append(extra, tool)
		}
	}
	return extra
}
