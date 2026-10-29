package sessioncontext

import (
	"context"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

// Every service transition that makes provider content consumable asks the
// injected current authority; a caller's raw target name grants nothing.
func (m *Service) gateMaterializationLocked(ctx context.Context, r *sm.ContextRecord, p sm.ContextProvider, role, target string, source MaterializationSource, op sm.ContextConsumerOperation, mode sm.ContextResolutionMode) error {
	if m.materializationAuthority == nil {
		return fmt.Errorf("context authority unavailable")
	}
	if r == nil || p == "" || p != r.Provider {
		return &ContextError{Code: "context_provider_mismatch", Message: "consumer provider does not match context"}
	}
	if strings.TrimSpace(role) == "" {
		return fmt.Errorf("target role required")
	}
	fact, err := m.materializationAuthority.Element(ctx, normalizeElementID(target))
	if err != nil {
		return err
	}
	if fact == nil || normalizeElementID(fact.ID) != normalizeElementID(target) || !fact.Eligible || fact.Closed || fact.Status == "closed" {
		return fmt.Errorf("materialization target is inactive or unavailable")
	}

	policy, err := m.localPolicySnapshot(ctx)
	if err != nil {
		return err
	}
	authorize := func(record *sm.ContextRecord, operation sm.ContextConsumerOperation) error {
		if record == nil || record.Provider != p {
			return &ContextError{Code: "context_provider_mismatch", Message: "lineage consumer provider does not match"}
		}
		if operation == "payload_fetch" && policy.PayloadMode != "unrestricted" {
			return &ContextError{Code: "context_payload_capability_required", Message: "payload transport is off"}
		}
		if record.State == "ready" {
			allowed := record.SecretClass == "unrestricted"
			if classes := policy.Clearance[normalizeRole(role)]; len(classes) > 0 {
				allowed = false
				for _, class := range classes {
					if class == record.SecretClass {
						allowed = true
					}
				}
			}
			if !allowed {
				return &ContextError{Code: "context_secret_class_denied", Message: fmt.Sprintf("role %s is not locally cleared for %s context", role, record.SecretClass)}
			}
		}
		permit, err := m.materializationAuthority.AuthorizeContextUse(ctx, sm.ContextUse{Owner: fact.ID, ContextID: record.ID, Role: role, Provider: p, Operation: operation, Mode: normalizeContextResolutionMode(mode), Now: m.now()})
		if err != nil {
			return err
		}
		if permit == nil || !permit.Allowed || permit.ContextID != record.ID {
			code, msg := "context_authorization_unavailable", "current context use was not authorized"
			retry := false
			if permit != nil {
				if permit.Code != "" {
					code = permit.Code
				}
				if permit.Reason != "" {
					msg = permit.Reason
				}
				retry = permit.RetryNext
			}
			return &ContextError{Code: code, Message: msg, RetryNextCandidate: retry}
		}
		return nil
	}
	if source == MaterializationSourceFetched {
		if err := authorize(r, "payload_fetch"); err != nil {
			return err
		}
	}
	if err := authorize(r, op); err != nil {
		return err
	}
	if err := m.requireBuilderConfinementCapabilityLocked(ctx, r); err != nil {
		return err
	}
	parents := append([]string(nil), r.Lineage.SupplementalCtxIDs...)
	if r.Lineage.PrimaryCtxID != "" {
		parents = append([]string{r.Lineage.PrimaryCtxID}, parents...)
	}
	for i, id := range parents {
		parent, e := m.materializationAuthority.Context(ctx, normalizeContextID(id))
		if e != nil {
			return e
		}
		operation := sm.ContextConsumerOperation("context_injection")
		if p == "claude" {
			operation = "transcript_splice"
		}
		if i == 0 && r.Lineage.PrimaryCtxID != "" {
			operation = "provider_fork"
			if op == "provider_resume" {
				operation = op
			}
		}
		if e = authorize(parent, operation); e != nil {
			return e
		}
	}
	return nil
}
