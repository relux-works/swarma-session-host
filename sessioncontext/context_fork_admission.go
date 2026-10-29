package sessioncontext

import (
	"context"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

type contextForkAdmission struct {
	Record   *sm.ContextRecord
	Decision *sm.ContextForkPreflightDecision
	Banner   *sm.ContextBoundaryBanner
	Rendered string
	Manifest *sm.CoverageManifest
}

// Security authorization is not validation. Only the separately injected actual
// current-policy/evidence supplier can admit a fork and construct its boundary.
func (m *Service) admitContextFork(ctx context.Context, id string, p sm.ContextProvider, role, target string, mode sm.ContextResolutionMode) (*contextForkAdmission, error) {
	if m.materializationAuthority == nil {
		return nil, fmt.Errorf("context_fork_preflight_failed: context authority unavailable")
	}
	id = normalizeContextID(id)
	r, err := m.materializationAuthority.Context(ctx, id)
	if err != nil {
		return nil, err
	}
	if r == nil || r.ID != id {
		return nil, fmt.Errorf("context_fork_preflight_failed: matching base required")
	}
	if err = m.gateMaterializationLocked(ctx, r, p, role, target, MaterializationSourceForked, "provider_fork", mode); err != nil {
		return nil, err
	}
	if m.validation == nil {
		return nil, &ContextError{Code: "context_validation_unavailable", Message: "original fork validation preflight and rendered boundary supplier unavailable"}
	}
	if mode == "" {
		mode = "explicit"
	}
	if mode != "explicit" && mode != "auto" {
		return nil, fmt.Errorf("invalid context resolution mode")
	}
	policy, err := m.validation.ActiveContextValidationPolicy(ctx)
	if err != nil {
		return nil, err
	}
	decision, err := m.validation.PreflightContextFork(ctx, sm.ContextForkPreflight{Record: cloneContext(r), Mode: mode, Policy: policy, Now: m.now()})
	if err != nil {
		return nil, err
	}
	if decision == nil || decision.ContractVersion != "context-validation-lifecycle-v1" || decision.CtxID != id || decision.Mode != mode || decision.State != r.State || decision.ModelAssayRequired {
		return nil, &ContextError{Code: "context_validation_unavailable", Message: "matching original fork validation decision required"}
	}
	if !decision.Admitted {
		return nil, contextForkRefused(decision)
	}
	if r.State != "ready" || decision.Skipped || decision.Fallback != "none" {
		return nil, &ContextError{Code: "context_validation_unavailable", Message: "fork decision contradicts current base admission"}
	}
	manifest, err := m.materializationAuthority.ContextCoverageManifest(ctx, id)
	if err != nil {
		return nil, err
	}
	if manifest == nil || manifest.CtxID != id || manifest.Provider != p || manifest.ManifestSHA256 == "" {
		return nil, fmt.Errorf("context_fork_preflight_failed: matching coverage manifest required")
	}
	// This is the exact original resource name, not a digest-derived statement.
	ref := id + "_coverage-manifest.json"
	banner, err := m.validation.BuildContextBoundaryBanner(ctx, cloneContext(r), manifest, ref, m.now())
	if err != nil {
		return nil, err
	}
	if banner == nil || banner.ContractVersion != "context-boundary-banner-v1" || banner.CtxID != id || banner.ManifestRef != ref {
		return nil, &ContextError{Code: "context_validation_unavailable", Message: "matching source-derived context boundary required"}
	}
	return &contextForkAdmission{Record: cloneContext(r), Decision: decision, Banner: banner, Rendered: banner.Render(), Manifest: manifest}, nil
}

// Preserve the original source reason-to-refusal mapping; a skipped auto
// candidate remains an error here, before a provider can copy any transcript.
func contextForkRefused(d *sm.ContextForkPreflightDecision) error {
	code := "context_validation_unavailable"
	has := func(want string) bool {
		for _, r := range d.Reasons {
			if string(r) == want {
				return true
			}
		}
		return false
	}
	switch {
	case d.State == "stale":
		code = "context_stale"
	case has("validation_context_not_ready"):
		code = "context_not_ready"
	case has("validation_evidence_unreadable") || has("validation_evidence_unbound"):
		code = "context_integrity_unverified"
	case has("validation_boundary_drift") || has("validation_payload_drift") || has("validation_manifest_drift") || has("validation_deep_audit_overdue"):
		code = "context_stale"
	}
	reasons := make([]string, 0, len(d.Reasons))
	for _, r := range d.Reasons {
		reasons = append(reasons, string(r))
	}
	if len(reasons) == 0 {
		reasons = []string{"validation_missing"}
	}
	return &ContextError{Code: code, Message: fmt.Sprintf("context %s did not pass the fork preflight (%s); fallback %s", d.CtxID, strings.Join(reasons, ", "), d.Fallback), RetryNextCandidate: d.Skipped}
}
