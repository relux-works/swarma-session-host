package sessioncontext

import (
	"context"
	"errors"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// RecoverUnusable preserves the original typed refusal and adds the actual
// source outcome to its Message. Unverifiable probes never invalidate a base.
func (m *Service) RecoverUnusable(ctx context.Context, e error) error {
	var u *contextPayloadUnusableError
	if !errors.As(e, &u) || u == nil {
		return e
	}
	var typed *ContextError
	if !errors.As(e, &typed) || typed == nil {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recoverUnusableLocked(ctx, e)
}
func (m *Service) recoverUnusableLocked(ctx context.Context, e error) error {
	var u *contextPayloadUnusableError
	var typed *ContextError
	if !errors.As(e, &u) || u == nil || !errors.As(e, &typed) || typed == nil {
		return e
	}
	recovered, recoveryErr := m.applyContextRecoveryLocked(ctx, u)
	annotated := *typed
	if recoveryErr != nil {
		annotated.Message = fmt.Sprintf("%s. The registry could not be updated to match: %v — %s is still recorded as ready, so mark it stale with reason %s before another consumer selects it", typed.Message, recoveryErr, u.CtxID, u.Reason)
		m.logOperatorError("context_recovery_failed", recoveryErr, map[string]any{"ctx_id": u.CtxID, "reason": u.Reason})
	} else {
		summary := "was already invalidated by an earlier consumer, so no new refresh plan was computed"
		if recovered != nil && recovered.ColdRebuild {
			summary = "must be rebuilt from cold; no ancestor could be proven still usable on this machine"
		} else if recovered != nil && recovered.ReplacementID != "" {
			summary = fmt.Sprintf("can be rebuilt by forking its nearest usable ancestor %s", recovered.ReplacementID)
		}
		annotated.Message = fmt.Sprintf("%s. %s was marked stale with reason %s and %s", typed.Message, u.CtxID, u.Reason, summary)
		m.logOperatorInfo("context_recovered", map[string]any{"ctx_id": u.CtxID, "reason": u.Reason, "cold_rebuild": recovered != nil && recovered.ColdRebuild})
	}
	return &annotated
}
func (m *Service) applyContextRecoveryLocked(ctx context.Context, u *contextPayloadUnusableError) (*sm.UnusableRecovery, error) {
	if m.materializationAuthority == nil {
		return nil, fmt.Errorf("no layer-1 authority is configured for context recovery")
	}
	r, err := m.materializationAuthority.Context(ctx, u.CtxID)
	if err != nil {
		return nil, err
	}
	if r == nil || r.ID != u.CtxID {
		return nil, fmt.Errorf("context recovery facts unavailable")
	}
	if r.State == "stale" || r.State == "retired" {
		return nil, nil
	}
	// The host owns no workspace fingerprint; empty facts request a cold rebuild
	// rather than inventing a usable ancestor from a repository it did not read.
	return m.materializationAuthority.RecoverUnusableContext(ctx, r.ID, r.Revision, u.Reason, sm.WorkspaceFingerprint{}, m.now())
}
