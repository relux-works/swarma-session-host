package sessionmanager

import "testing"

// TestRequireBuilderTraceCompletenessSplitsItsTwoFailures checks the
// reconciliation and, just as importantly, which code each failure carries: a
// consumer branches on it to know whether to fix the launch or the machine.
func TestRequireBuilderTraceCompletenessSplitsItsTwoFailures(t *testing.T) {
	attestation := func(count int) ContextRepositoryAccessAttestation {
		return ContextRepositoryAccessAttestation{
			ContractVersion: ContextRepositoryAccessTraceContract,
			EventCount:      count,
		}
	}
	tests := []struct {
		name        string
		observation BuilderToolObservation
		attestation ContextRepositoryAccessAttestation
		wantCode    string
	}{
		{
			name:        "stream never observed",
			observation: BuilderToolObservation{},
			attestation: attestation(0),
			wantCode:    ContextErrorBuilderToolIsolationUnavailable,
		},
		{
			name:        "a tool call outside the gateway",
			observation: BuilderToolObservation{Reported: true, ForeignToolCalls: 1},
			attestation: attestation(0),
			wantCode:    ContextErrorBuilderToolIsolationUnavailable,
		},
		{
			name:        "provider disconnected mid-run",
			observation: BuilderToolObservation{Reported: true, Disconnected: true},
			attestation: attestation(0),
			wantCode:    ContextErrorBuilderTraceUnavailable,
		},
		{
			name:        "unclassifiable stream record",
			observation: BuilderToolObservation{Reported: true, UnknownShapes: 1},
			attestation: attestation(0),
			wantCode:    ContextErrorBuilderTraceUnavailable,
		},
		{
			name:        "a gateway call the trace never captured",
			observation: BuilderToolObservation{Reported: true, GatewayCalls: 3},
			attestation: attestation(2),
			wantCode:    ContextErrorBuilderTraceUnavailable,
		},
		{
			name:        "no attestation at all",
			observation: BuilderToolObservation{Reported: true},
			attestation: ContextRepositoryAccessAttestation{},
			wantCode:    ContextErrorBuilderTraceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := requireBuilderTraceCompleteness(
				testGatewayCtxID, test.observation, test.attestation)
			if !builderIsContextErrorCode(err, test.wantCode) {
				t.Fatalf("reconciliation = %v, want %s", err, test.wantCode)
			}
		})
	}

	// A run whose calls and events agree is publishable. Without this control the
	// cases above could all pass on a function that refused everything.
	if err := requireBuilderTraceCompleteness(
		testGatewayCtxID,
		BuilderToolObservation{Reported: true, GatewayCalls: 2},
		attestation(2),
	); err != nil {
		t.Fatalf("a reconciled run was refused: %v", err)
	}
}
