package sessionmanager

import (
	"fmt"
	"strings"

	contextpolicy "github.com/relux-works/swarma-session-host/sessionmanager/resource"
)

// ValidateBuilderLaunchReports is the original post-Launch confinement and
// isolation check. Reports come from the returned native host, never a caller
// publication body. This validates source reports; it issues no signed claim.
func ValidateBuilderLaunchReports(host RunnerHost, request BuilderConfinementRequest) (BuilderConfinementResult, BuilderToolIsolationResult, error) {
	refuse := func(code, message string, cause error) (BuilderConfinementResult, BuilderToolIsolationResult, error) {
		return BuilderConfinementResult{}, BuilderToolIsolationResult{}, &BuilderTraceError{Code: code, Message: message, Cause: cause}
	}
	reporter, ok := host.(interface {
		BuilderIngestionConfinement() (BuilderConfinementResult, error)
	})
	if !ok {
		return refuse("context_builder_confinement_unavailable", "launched provider cannot report actual builder confinement", nil)
	}
	confinement, err := reporter.BuilderIngestionConfinement()
	if err != nil {
		return refuse("context_builder_confinement_unavailable", "launched provider confinement unavailable", err)
	}
	if strings.TrimSpace(confinement.Mechanism) == "" {
		return refuse("context_builder_confinement_unavailable", "launched provider reported no confinement mechanism", nil)
	}
	roots, err := normalizeConfinedRoots(confinement.EnforcedRoots)
	if err != nil {
		return refuse("context_builder_confinement_unavailable", "launched provider reported unusable read scope", err)
	}
	for _, root := range roots {
		if !request.Allowlist.Allows(root) {
			return refuse(ContextErrorBuilderAllowlistDenied, fmt.Sprintf("launched provider read scope %s exceeds builder allowlist", root), nil)
		}
	}
	scope := NewContextBuilderAllowlist(roots)
	for _, spec := range request.RequestedPathSpecs {
		clean := contextpolicy.NormalizeContextPathSpec(spec)
		if clean != "" && !scope.Allows(clean) {
			return refuse(ContextErrorBuilderAllowlistDenied, fmt.Sprintf("launched provider read scope does not cover %s", clean), nil)
		}
	}
	isolationReporter, ok := host.(BuilderToolIsolationReporter)
	if !ok {
		return refuse(ContextErrorBuilderToolIsolationUnavailable, "launched provider cannot report its tool isolation", nil)
	}
	isolation, err := isolationReporter.BuilderToolIsolation()
	if err != nil {
		return refuse(ContextErrorBuilderToolIsolationUnavailable, "launched provider tool isolation unavailable", err)
	}
	isolation, err = requireBuilderToolIsolation(request.CtxID, isolation)
	if err != nil {
		return BuilderConfinementResult{}, BuilderToolIsolationResult{}, err
	}
	confinement.Mechanism = strings.TrimSpace(confinement.Mechanism)
	confinement.EnforcedRoots = roots
	return confinement, isolation, nil
}
