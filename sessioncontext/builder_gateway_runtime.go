package sessioncontext

import (
	"context"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

// finalizeOwnedBuilderGatewayLocked closes the host-owned surface before loading
// evidence or querying mutable authority. Neither an absent record nor a failing
// authority may leave an actual owned gateway open. Issuance remains separate.
func (m *Service) finalizeOwnedBuilderGatewayLocked(ctx context.Context, id string) (BuilderGatewayFinalization, error) {
	result, err := m.builderGateways.FinalizeBuilderGateway(ctx, id)
	if err != nil {
		return BuilderGatewayFinalization{}, err
	}
	r := m.materializations[id]
	if r == nil {
		return BuilderGatewayFinalization{}, fmt.Errorf("materialization unavailable")
	}
	if m.materializationAuthority == nil {
		return BuilderGatewayFinalization{}, fmt.Errorf("context authority unavailable")
	}
	c, err := m.materializationAuthority.Context(ctx, id)
	if err != nil {
		return BuilderGatewayFinalization{}, err
	}
	if c == nil {
		return BuilderGatewayFinalization{}, fmt.Errorf("context facts unavailable")
	}
	if result.Served || c.RequiresConfinement {
		_, attestation, err := m.builderGateways.BuilderAccessTrace(id)
		if err != nil {
			return BuilderGatewayFinalization{}, err
		}
		run := r.ActiveRunID
		if run == "" {
			run = c.Lineage.ProducedByRunID
		}
		if attestation.CtxID != id || attestation.Provider != r.Provider || attestation.RunID != strings.ToUpper(strings.TrimSpace(run)) {
			return BuilderGatewayFinalization{}, &sm.PublicError{Code: sm.ContextErrorBuilderTraceUnavailable, Message: "gateway finalization returned a foreign trace receipt"}
		}
		result.Attestation = attestation
	}
	return result, nil
}
