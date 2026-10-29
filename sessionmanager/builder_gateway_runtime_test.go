package sessionmanager

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
)

// The tests here exercise the reconciliation through the manager's own close
// path rather than by calling requireBuilderTraceCompleteness directly.
//
// A check that only ever runs from its unit test is not a gate. The standing bar
// on this story is that the check has to be reached from a real call path, so
// each case here drives a gateway, a live socket and the shim, then asks the
// manager to close it.

// gatewaySessionFixture is a manager holding one live builder gateway, wired the
// way a launch wires it.
type gatewaySessionFixture struct {
	manager  *BuilderGatewayRuntime
	ctxID    string
	gateway  *BuilderEvidenceGateway
	service  *BuilderGatewayService
	client   *BuilderGatewayClient
	observer *testBuilderToolObserver
}

type testBuilderToolObserver struct {
	gateway     *BuilderEvidenceGateway
	observation *BuilderToolObservation
	err         error
}

func (o *testBuilderToolObserver) BuilderToolObservation(
	context.Context,
) (BuilderToolObservation, error) {
	if o.err != nil {
		return BuilderToolObservation{}, o.err
	}
	if o.observation != nil {
		return *o.observation, nil
	}
	return BuilderToolObservation{
		Reported:     true,
		GatewayCalls: o.gateway.RecordedEvents(),
	}, nil
}

func newGatewaySessionFixture(t *testing.T, ctxID string) *gatewaySessionFixture {
	t.Helper()
	root := gatewayRepo(t)
	gateway, err := NewBuilderEvidenceGateway(BuilderEvidenceGatewayConfig{
		CtxID:          ctxID,
		RunID:          "RUN-260731-abc123",
		Provider:       "claude",
		RepositoryRoot: root,
		Allowlist:      NewContextBuilderAllowlist([]string{"docs"}),
		TracePath:      filepath.Join(t.TempDir(), "trace.jsonl"),
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := StartBuilderGatewayService(gateway, shortSocketPath(t))
	if err != nil {
		t.Fatal(err)
	}
	observer := &testBuilderToolObserver{gateway: gateway}
	manager := &BuilderGatewayRuntime{
		builderGateways: map[string]*builderGatewaySession{
			builderNormalizeContextID(ctxID): {
				ctxID:    builderNormalizeContextID(ctxID),
				gateway:  gateway,
				service:  service,
				observer: observer,
			},
		},
	}
	t.Cleanup(func() { _ = service.Stop(); _ = manager.Close() })
	client := NewBuilderGatewayClient(service.SocketPath(), service.Endpoint("", nil).Token)
	t.Cleanup(func() { _ = client.Disconnect() })
	return &gatewaySessionFixture{
		manager:  manager,
		ctxID:    ctxID,
		gateway:  gateway,
		service:  service,
		client:   client,
		observer: observer,
	}
}

// TestManagerCloseReconcilesACleanBuilderRun is the control. Without it the
// refusals below could all be produced by a close path that refused everything.
func TestManagerCloseReconcilesACleanBuilderRun(t *testing.T) {
	fixture := newGatewaySessionFixture(t, "CTX-260731-mg0001")

	script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` +
		`{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
	if err := RunBuilderGatewayShim(
		fixture.client, strings.NewReader(script), io.Discard,
	); err != nil {
		t.Fatalf("driving the builder: %v", err)
	}

	attestation, err := fixture.manager.CloseBuilderGateway(fixture.ctxID)
	if err != nil {
		t.Fatalf("closing a clean builder run: %v", err)
	}
	if attestation.EventCount != 1 {
		t.Fatalf("attestation binds %d event(s), want 1", attestation.EventCount)
	}
}

// TestManagerCloseRefusesAGatewayTheProviderNeverStarted covers the AC's
// disabled-gateway case. A run that never opened the read surface is
// indistinguishable from a run that chose to read nothing if you look only at
// the trace, and only one of those is a base worth inheriting.
func TestManagerCloseRefusesAGatewayTheProviderNeverStarted(t *testing.T) {
	fixture := newGatewaySessionFixture(t, "CTX-260731-mg0002")

	_, err := fixture.manager.CloseBuilderGateway(fixture.ctxID)
	if !builderIsContextErrorCode(
		err, ContextErrorBuilderToolIsolationUnavailable,
	) {
		t.Fatalf("closing an unstarted gateway = %v, want %s",
			err, ContextErrorBuilderToolIsolationUnavailable)
	}
}

// TestManagerCloseRefusesADisconnectedProvider covers the AC's disconnect case:
// the shim announced itself and never signed off, so the tail of the stream is
// unobserved.
func TestManagerCloseRefusesADisconnectedProvider(t *testing.T) {
	fixture := newGatewaySessionFixture(t, "CTX-260731-mg0003")

	// Connect and make a call, then vanish without the closing frame — exactly
	// what a killed provider leaves behind.
	if err := fixture.client.Connect(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.client.Call(BuilderGatewayRequest{
		Operation: "read", Path: "docs/guide.md", ProviderEventID: "evt-1",
	}); err != nil {
		t.Fatal(err)
	}
	fixture.observer.observation = &BuilderToolObservation{
		Reported: true, GatewayCalls: 1, Disconnected: true,
	}

	_, err := fixture.manager.CloseBuilderGateway(fixture.ctxID)
	if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("closing after a provider disconnect = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

// TestManagerCloseRefusesAnUnclassifiableStream covers the AC's unknown-shape
// case. A frame the shim could not read is not evidence that no tool call was
// in it.
func TestManagerCloseRefusesAnUnclassifiableStream(t *testing.T) {
	fixture := newGatewaySessionFixture(t, "CTX-260731-mg0004")

	if err := RunBuilderGatewayShim(
		fixture.client, strings.NewReader("this is not json\n"), io.Discard,
	); err != nil {
		t.Fatalf("driving the builder: %v", err)
	}

	_, err := fixture.manager.CloseBuilderGateway(fixture.ctxID)
	if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("closing after an unreadable stream record = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

// TestManagerCloseRefusesAMissingEvent covers the AC's missing-event case: the
// shim handled a call the trace does not contain. It is the failure the chain
// digest cannot see, because an event that was never written breaks nothing.
func TestManagerCloseRefusesAMissingEvent(t *testing.T) {
	fixture := newGatewaySessionFixture(t, "CTX-260731-mg0005")

	if err := fixture.client.Connect(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.client.Call(BuilderGatewayRequest{
		Operation: "read", Path: "docs/guide.md", ProviderEventID: "evt-1",
	}); err != nil {
		t.Fatal(err)
	}
	// Sign off claiming one more call than the gateway served, which is what a
	// call the manager dropped on the floor would look like from the shim's side.
	fixture.client.calls++
	if err := fixture.client.Close(0); err != nil {
		t.Fatal(err)
	}
	fixture.observer.observation = &BuilderToolObservation{Reported: true, GatewayCalls: 2}

	_, err := fixture.manager.CloseBuilderGateway(fixture.ctxID)
	if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("closing with an uncaptured call = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

// TestRequireBuilderCoverageEvidenceRefusesAMissingTrace is the publication-path
// gate. A machine that sealed a claim owns the record of what the run read, so a
// claim with no trace behind it must not reach the board.
func TestRequireBuilderCoverageEvidenceRefusesAMissingTrace(t *testing.T) {
	layout := gateTestLayout(t, "builder-coverage-evidence")
	err := RequireBuilderCoverageEvidence(layout, "CTX-260731-mg0006")
	if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("publication without a trace = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

// TestManagerRefusesASecondGatewayForOneContext keeps one context to one trace.
// A second gateway would open a second chain under the same anchor, and neither
// attestation would describe the run.
func TestManagerRefusesASecondGatewayForOneContext(t *testing.T) {
	fixture := newGatewaySessionFixture(t, "CTX-260731-mg0007")
	defer fixture.service.Stop()

	record := &ContextRecord{
		ID:       fixture.ctxID,
		Provider: "claude",
	}
	_, err := fixture.manager.StartBuilderGateway(context.Background(), record, gatewayRepo(t), NewContextBuilderAllowlist([]string{"docs"}))
	if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("opening a second gateway = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

// TestManagerCloseRefusesEveryUnclassifiableShape closes the holes the previous
// revision left open.
//
// Only invalid JSON was counted before. An unknown JSON-RPC method returned
// "method not found" and incremented nothing; an unknown tools/call name was
// forwarded and rewritten into a denied stat event, which the gateway then
// recorded — so the shim's tally and the attested event count agreed, the run
// reconciled as complete, and publication proceeded. Each case below is a shape
// nobody classified, and each must now make the trace unavailable.
func TestManagerCloseRefusesEveryUnclassifiableShape(t *testing.T) {
	cases := []struct {
		name   string
		ctxID  string
		script string
	}{
		{
			name:   "unknown JSON-RPC method",
			ctxID:  "CTX-260731-mg0010",
			script: `{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{}}`,
		},
		{
			name:  "unknown tool name",
			ctxID: "CTX-260731-mg0011",
			script: `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` +
				`{"name":"exfiltrate","arguments":{"path":"docs/guide.md"}}}`,
		},
		{
			name:  "malformed tool call parameters",
			ctxID: "CTX-260731-mg0012",
			script: `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` +
				`{"name":42,"arguments":"not-an-object"}}`,
		},
		{
			name:   "unreadable frame",
			ctxID:  "CTX-260731-mg0013",
			script: `{"jsonrpc":"2.0","id":1,`,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGatewaySessionFixture(t, test.ctxID)
			// A legitimate call runs alongside the bad frame, so the refusal cannot be
			// explained by the run having done nothing at all.
			script := `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":` +
				`{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n" +
				test.script + "\n"
			if err := RunBuilderGatewayShim(
				fixture.client, strings.NewReader(script), io.Discard,
			); err != nil {
				t.Fatalf("driving the builder: %v", err)
			}
			_, err := fixture.manager.CloseBuilderGateway(fixture.ctxID)
			if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
				t.Fatalf("closing after %s = %v, want %s",
					test.name, err, ContextErrorBuilderTraceUnavailable)
			}
		})
	}
}

// TestManagerCloseRefusesAnUnmodelledGatewayOperation attacks the socket rather
// than the shim.
//
// The shim now refuses an unknown tool name before it forwards, so the gateway's
// own unclassifiable branch would be unreachable from that direction — and a
// branch reachable only from its unit test is not a gate. The socket is a second
// surface: anything holding the token can send an operation the contract does
// not model, and that call must not reconcile either.
func TestManagerCloseRefusesAnUnmodelledGatewayOperation(t *testing.T) {
	fixture := newGatewaySessionFixture(t, "CTX-260731-mg0014")

	if err := fixture.client.Connect(); err != nil {
		t.Fatal(err)
	}
	response, err := fixture.client.Call(BuilderGatewayRequest{
		Operation: "exfiltrate", Path: "docs/guide.md", ProviderEventID: "evt-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Outcome != string(ContextRepositoryAccessDenied) {
		t.Fatalf("an unmodelled operation was served: %+v", response)
	}
	if err := fixture.client.Close(0); err != nil {
		t.Fatal(err)
	}

	_, err = fixture.manager.CloseBuilderGateway(fixture.ctxID)
	if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("closing after an unmodelled operation = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

// TestShimRefusesAnUnknownToolWithoutMintingATraceEvent isolates the shim's own
// gate.
//
// The reconciliation refusal above does not cover it: with the shim's check
// removed the call is forwarded, the gateway records it as unclassifiable, and
// the run is refused anyway — so that test passes either way and proves nothing
// about the shim. What the shim uniquely prevents is the trace acquiring an
// event for a call whose meaning nobody established, described in terms the
// builder never used. The previous revision minted exactly that: an unknown tool
// name became a denied `stat` event against the requested path.
func TestShimRefusesAnUnknownToolWithoutMintingATraceEvent(t *testing.T) {
	fixture := newGatewaySessionFixture(t, "CTX-260731-mg0015")

	script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` +
		`{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n" +
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":` +
		`{"name":"exfiltrate","arguments":{"path":"docs/guide.md"}}}` + "\n"
	if err := RunBuilderGatewayShim(
		fixture.client, strings.NewReader(script), io.Discard,
	); err != nil {
		t.Fatalf("driving the builder: %v", err)
	}

	events := fixture.gateway.RecordedEvents()
	if events != 1 {
		t.Fatalf(
			"the trace recorded %d event(s) for one legitimate call and one unmodelled tool name, want 1",
			events,
		)
	}
}

// A live, exercised shim is insufficient without the independent native runner
// observer. Refusal remains a refusal on retry, never a zero-count success.
func TestBuilderGatewayRuntimeRequiresNativeObserver(t *testing.T) {
	f := newGatewaySessionFixture(t, "CTX-260731-ob0001")
	f.manager.builderGateways[f.ctxID].observer = nil
	var unsupported struct{ RunnerHost }
	if err := f.manager.BindBuilderGatewayObserver(f.ctxID, unsupported); !builderIsContextErrorCode(err, ContextErrorBuilderToolIsolationUnavailable) {
		t.Fatalf("missing native observer was accepted: %v", err)
	}
	script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
	if err := RunBuilderGatewayShim(f.client, strings.NewReader(script), io.Discard); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		result, err := f.manager.FinalizeBuilderGateway(context.Background(), f.ctxID)
		if !builderIsContextErrorCode(err, ContextErrorBuilderToolIsolationUnavailable) || result.Attestation.ContractVersion != "" {
			t.Fatalf("missing native observer produced usable evidence on attempt%d: %+v %v", attempt, result, err)
		}
	}
}

// An actual Stop error after closing the fixture listener cannot be swallowed
// merely because a terminal trace was written. The same owner refuses retry.
func TestBuilderGatewayFailedSocketCloseCannotBecomeSuccessfulRetry(t *testing.T) {
	f := newGatewaySessionFixture(t, "CTX-260731-st0001")
	script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
	if err := RunBuilderGatewayShim(f.client, strings.NewReader(script), io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := f.service.listener.Close(); err != nil {
		t.Fatal(err)
	}
	<-f.service.done
	cause := net.ErrClosed
	for attempt := 0; attempt < 2; attempt++ {
		result, err := f.manager.FinalizeBuilderGateway(context.Background(), f.ctxID)
		if !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) || !errors.Is(err, cause) || result.Attestation.ContractVersion != "" {
			t.Fatalf("failed socket close produced usable evidence on attempt%d: %+v %v", attempt, result, err)
		}
	}
}
