package sessionmanager

import (
	"io"
	"strings"
	"testing"
)

// Claim: through the actual runtime close path, a native observation that reports a
// tool call outside the gateway refuses with ContextErrorBuilderToolIsolationUnavailable,
// while the identical clean run without a foreign call reconciles (positive control).
// Limit: in-process fixture observer and real gateway/socket/shim, no live provider.
func TestRev5BatchPForeignToolCallRefusesClose(t *testing.T) {
	for _, tc := range []struct {
		ctx     string
		foreign int
	}{{"CTX-260731-rv5f00", 0}, {"CTX-260731-rv5f01", 1}} {
		t.Run(tc.ctx, func(t *testing.T) {
			fixture := newGatewaySessionFixture(t, tc.ctx)
			script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` +
				`{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
			if err := RunBuilderGatewayShim(fixture.client, strings.NewReader(script), io.Discard); err != nil {
				t.Fatalf("driving the builder: %v", err)
			}
			fixture.observer.observation = &BuilderToolObservation{Reported: true, GatewayCalls: 1, ForeignToolCalls: tc.foreign}
			attestation, err := fixture.manager.CloseBuilderGateway(fixture.ctxID)
			if tc.foreign == 0 {
				if err != nil || attestation.EventCount != 1 {
					t.Fatalf("clean control refused: %v %+v", err, attestation)
				}
				return
			}
			if !builderIsContextErrorCode(err, ContextErrorBuilderToolIsolationUnavailable) || attestation.ContractVersion != "" {
				t.Fatalf("foreign native tool call reconciled: %v %+v", err, attestation)
			}
		})
	}
}
