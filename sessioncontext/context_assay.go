package sessioncontext

import (
	"context"
	"encoding/json"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
	"time"
)

// RunCodexContextAssay invokes the real registered provider's original optional
// assay capability. Registry/security checks precede native I/O; the provider
// owns isolation/attestation, while observed loss returns through recovery here.
func (m *Service) RunCodexContextAssay(ctx context.Context, q sm.CodexContextAssayRequest) (*sm.CodexContextAssayResult, error) {
	if strings.TrimSpace(q.Prompt) == "" {
		return nil, fmt.Errorf("Codex context assay prompt is required")
	}
	if !json.Valid(q.OutputSchema) {
		return nil, fmt.Errorf("Codex context assay output schema must be valid JSON")
	}
	q.BaseCtxID = normalizeContextID(q.BaseCtxID)
	m.mu.Lock()
	source, _, err := m.contextAssaySourceLocked(ctx, q.BaseCtxID, "codex")
	factory, ok := m.providers["codex"].(interface {
		RunContextAssay(context.Context, sm.ProviderHostRecord, sm.CodexContextAssayRequest) (*sm.CodexContextAssayResult, error)
	})
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &ContextError{Code: "context_assay_isolation_unavailable", Message: "the registered Codex adapter cannot enforce and observe the context assay isolation contract"}
	}
	result, err := factory.RunContextAssay(ctx, source, q)
	return result, m.RecoverUnusable(ctx, err)
}

// RunClaudeContextAssay carries authoritative ReadyAt into the real adapter;
// a request cannot forge retention evidence or bypass automatic recovery.
func (m *Service) RunClaudeContextAssay(ctx context.Context, q sm.ClaudeContextAssayRequest) (*sm.ClaudeContextAssayResult, error) {
	if strings.TrimSpace(q.Prompt) == "" {
		return nil, fmt.Errorf("Claude context assay prompt is required")
	}
	if !json.Valid(q.OutputSchema) {
		return nil, fmt.Errorf("Claude context assay output schema must be valid JSON")
	}
	q.BaseCtxID = normalizeContextID(q.BaseCtxID)
	m.mu.Lock()
	source, ready, err := m.contextAssaySourceLocked(ctx, q.BaseCtxID, "claude")
	factory, ok := m.providers["claude"].(interface {
		RunContextAssay(context.Context, sm.ProviderHostRecord, sm.ClaudeContextAssayRequest) (*sm.ClaudeContextAssayResult, error)
	})
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &ContextError{Code: "context_assay_isolation_unavailable", Message: "the registered Claude adapter cannot enforce and observe the context assay isolation contract"}
	}
	q.BaseReadyAt = ready
	result, err := factory.RunContextAssay(ctx, source, q)
	return result, m.RecoverUnusable(ctx, ClaudePolicyError(err))
}

func (m *Service) contextAssaySourceLocked(ctx context.Context, id string, p sm.ContextProvider) (sm.ProviderHostRecord, time.Time, error) {
	fail := func(e error) (sm.ProviderHostRecord, time.Time, error) {
		return sm.ProviderHostRecord{}, time.Time{}, e
	}
	local := m.materializations[id]
	if local == nil {
		return fail(fmt.Errorf("materialization %s was not found", id))
	}
	if local.Provider != p {
		return fail(&ContextError{Code: "context_provider_mismatch", Message: fmt.Sprintf("context assay base %s is not %s", id, p)})
	}
	if local.State != MaterializationReady {
		return fail(&ContextError{Code: "context_not_ready", Message: fmt.Sprintf("context assay base %s is %s, not ready", id, local.State)})
	}
	if m.materializationAuthority == nil {
		return fail(fmt.Errorf("context authority unavailable"))
	}
	r, err := m.materializationAuthority.Context(ctx, id)
	if err != nil {
		return fail(err)
	}
	if err = validateMaterializationMirror(local, r); err != nil {
		return fail(err)
	}
	if r.State != "ready" {
		return fail(&ContextError{Code: "context_not_ready", Message: "the authoritative assay base is not ready"})
	}
	if err = m.gateMaterializationLocked(ctx, r, p, local.TargetRole, local.TargetElementID, local.Source, "provider_fork", local.ResolutionMode); err != nil {
		return fail(err)
	}
	if !hostPresent(local.ProviderHost) {
		return fail(fmt.Errorf("matching actual provider snapshot required"))
	}
	return cloneHostRecord(local.ProviderHost), r.ReadyAt, nil
}
