package sessioncontext

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Original lifecycle names/assertions now traverse the concrete standalone
// runtime and Service/HTTP receiver. "Publishes" in the original name means
// the closed-trace eligibility check, not signed board publication. These use
// declared native observation fixtures, never a real provider/model process.
type lifecycleNativeObserver struct {
	sm.RunnerHost
	observation sm.BuilderToolObservation
	err         error
}

func (o *lifecycleNativeObserver) BuilderToolObservation(context.Context) (sm.BuilderToolObservation, error) {
	return o.observation, o.err
}

type lifecycleGatewayFixture struct {
	service   *Service
	runtime   *sm.BuilderGatewayRuntime
	layout    sm.Layout
	record    *sm.ContextRecord
	authority *fakeMaterializationAuthority
	client    *sm.BuilderGatewayClient
	observer  *lifecycleNativeObserver
	endpoint  sm.BuilderGatewayEndpoint
}

func newLifecycleGatewayManager(t *testing.T, id string) *lifecycleGatewayFixture {
	t.Helper()
	layout := testLayout(t, "gateway-lifecycle")
	dir, err := os.MkdirTemp("/tmp", "build-lc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	layout.SocketPath = filepath.Join(dir, "control.sock")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := sm.NewBuilderGatewayRuntime(sm.BuilderGatewayRuntimeOptions{Layout: layout, ShimCommand: executable, ShimArgs: []string{"session", "context-gateway-shim"}, Now: func() time.Time { return materializationTestClock }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	record := materializationTestContext(id, false)
	record.Lineage.ProducedByRunID = "RUN-260731-lc0001"
	record.RequiresConfinement = true
	authority := newFakeMaterializationAuthority(record)
	manager, err := Open(context.Background(), Options{Layout: layout, Authority: authority, References: references{known: true}, BuilderGateways: runtime, Now: func() time.Time { return materializationTestClock }})
	if err != nil {
		t.Fatal(err)
	}
	seedMaterialization(manager, id, MaterializationLeased, materializationTestClock, func(r *materializationRecord) {
		r.ActiveRunID = record.Lineage.ProducedByRunID
		r.LeaseAcquiredAt = materializationTestClock
		r.Source = MaterializationSourceBuilt
	})
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs/guide.md"), []byte("original lifecycle content\n"), 0600); err != nil {
		t.Fatal(err)
	}
	endpoint, err := runtime.StartBuilderGateway(context.Background(), record, root, sm.NewContextBuilderAllowlist([]string{"docs"}))
	if err != nil {
		t.Fatal(err)
	}
	observer := &lifecycleNativeObserver{observation: sm.BuilderToolObservation{Reported: true, GatewayCalls: 1}}
	if err := runtime.BindBuilderGatewayObserver(id, observer); err != nil {
		t.Fatal(err)
	}
	client := sm.NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
	t.Cleanup(func() { _ = client.Disconnect() })
	return &lifecycleGatewayFixture{service: manager, runtime: runtime, layout: layout, record: record, authority: authority, client: client, observer: observer, endpoint: endpoint}
}
func driveOneBuilderRead(t *testing.T, client *sm.BuilderGatewayClient) {
	t.Helper()
	script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
	if err := sm.RunBuilderGatewayShim(client, strings.NewReader(script), io.Discard); err != nil {
		t.Fatalf("driving the builder: %v", err)
	}
}
func requireTrace(t *testing.T, f *lifecycleGatewayFixture) {
	t.Helper()
	if err := sm.RequireBuilderCoverageEvidence(f.layout, f.record.ID); err != nil {
		t.Fatalf("a finalized builder run was refused publication: %v", err)
	}
}
func assertSocketClosed(t *testing.T, f *lifecycleGatewayFixture) {
	t.Helper()
	if _, err := os.Lstat(f.endpoint.SocketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completion left the builder gateway open: %v", err)
	}
}
func TestSuccessfulBuilderRunFinalizesAndPublishes(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0001")
	driveOneBuilderRead(t, f.client)
	if err := sm.RequireBuilderCoverageEvidence(f.layout, f.record.ID); err == nil {
		t.Fatal("an unfinalized builder run was accepted for publication")
	}
	finalization, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID)
	if err != nil {
		t.Fatalf("finalizing a clean builder run: %v", err)
	}
	if !finalization.Served {
		t.Fatal("the manager reported it held no gateway for a run it was serving")
	}
	if finalization.Attestation.EventCount != 1 {
		t.Fatalf("attestation binds %d event(s), want 1", finalization.Attestation.EventCount)
	}
	assertSocketClosed(t, f)
	requireTrace(t, f)
	repeat, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID)
	if err != nil {
		t.Fatalf("finalizing twice: %v", err)
	}
	if repeat.Served {
		t.Fatal("the manager claimed to hold a gateway it had already closed")
	}
	requireTrace(t, f)
}
func TestFinalizeAndRequireCoverageClosesTheGatewayItReads(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0002")
	driveOneBuilderRead(t, f.client)
	connect := func(context.Context, sm.Layout) (sm.BuilderGatewayFinalizationClient, error) {
		return nil, sm.ErrBuilderGatewayNotRunning
	}
	if err := sm.FinalizeAndRequireBuilderCoverageEvidence(context.Background(), f.layout, f.record.ID, connect); err == nil {
		t.Fatal("a live gateway with no reachable manager was accepted for publication")
	}
	if _, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID); err != nil {
		t.Fatalf("finalizing: %v", err)
	}
	if err := sm.FinalizeAndRequireBuilderCoverageEvidence(context.Background(), f.layout, f.record.ID, connect); err != nil {
		t.Fatalf("a finalized run was refused with no manager running: %v", err)
	}
}
func TestFinalizeAndRequireCoverageRefusesAnUnreachableManager(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0003")
	driveOneBuilderRead(t, f.client)
	if _, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID); err != nil {
		t.Fatal(err)
	}
	requireTrace(t, f)
	err := sm.FinalizeAndRequireBuilderCoverageEvidence(context.Background(), f.layout, f.record.ID, func(context.Context, sm.Layout) (sm.BuilderGatewayFinalizationClient, error) {
		return nil, io.ErrUnexpectedEOF
	})
	var typed *sm.PublicError
	if !errors.As(err, &typed) || typed.Code != sm.ContextErrorBuilderTraceUnavailable || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("unreachable manager = %v, want %s and original cause", err, sm.ContextErrorBuilderTraceUnavailable)
	}
}

type lifecycleControlClient struct {
	base   string
	client *http.Client
	closed bool
}

func (c *lifecycleControlClient) Close() error { c.closed = true; return nil }
func (c *lifecycleControlClient) FinalizeBuilderGateway(ctx context.Context, id string) (sm.BuilderGatewayFinalization, error) {
	b, _ := json.Marshal(map[string]string{"ctx_id": id})
	req, err := http.NewRequestWithContext(ctx, "POST", c.base+"/v1/contexts/builder-gateway/finalize", bytes.NewReader(b))
	if err != nil {
		return sm.BuilderGatewayFinalization{}, err
	}
	response, err := c.client.Do(req)
	if err != nil {
		return sm.BuilderGatewayFinalization{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		var failure struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if err := json.NewDecoder(response.Body).Decode(&failure); err != nil {
			return sm.BuilderGatewayFinalization{}, err
		}
		return sm.BuilderGatewayFinalization{}, &sm.PublicError{Code: failure.Code, Message: failure.Message}
	}
	var result sm.BuilderGatewayFinalization
	err = json.NewDecoder(response.Body).Decode(&result)
	return result, err
}
func startLifecycleControlServer(t *testing.T, f *lifecycleGatewayFixture) *lifecycleControlClient {
	t.Helper()
	handler, err := NewHTTPHandler(f.service)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &lifecycleControlClient{base: server.URL, client: server.Client()}
}
func TestFinalizeAndRequireCoverageActuallyCallsTheManager(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0008")
	driveOneBuilderRead(t, f.client)
	client := startLifecycleControlServer(t, f)
	if err := sm.FinalizeAndRequireBuilderCoverageEvidence(context.Background(), f.layout, f.record.ID, func(context.Context, sm.Layout) (sm.BuilderGatewayFinalizationClient, error) { return client, nil }); err != nil {
		t.Fatalf("publishing a live builder run: %v", err)
	}
	assertSocketClosed(t, f)
	requireTrace(t, f)
	if !client.closed {
		t.Fatal("publication did not release its non-starting control connection")
	}
}
func TestCompleteMaterializationFinalizesTheBuilderGateway(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0004")
	driveOneBuilderRead(t, f.client)
	// This terminal-step counterpart starts with a valid durable existing lease;
	// pre-launch confinement/claim issuance remains a separate canonical-source
	// bound, not a disabled requirement or a permissive confiner. Keep the actual
	// RequiresConfinement fact so completion has to verify the owned closed trace.
	if !f.authority.contexts[f.record.ID].RequiresConfinement {
		t.Fatal("fixture lost required confinement fact")
	}
	lease := f.service.materializations[f.record.ID]
	lease.Contract = MaterializationContractVersion
	lease.ProviderHost = testMaterializationHost()
	lease.BindingScope = f.record.Binding.Scope
	lease.BindingElementID = f.record.Binding.ElementID
	lease.TargetElementID = "TASK-260729-cccccc"
	lease.TargetRole = "context-builder"
	if err := f.service.saveMaterializationLocked(lease); err != nil {
		t.Fatalf("persisting declared existing lease: %v", err)
	}
	if _, err := f.service.CompleteMaterialization(context.Background(), CompleteMaterializationRequest{CtxID: f.record.ID, RunID: f.record.Lineage.ProducedByRunID, Published: false, PayloadSHA256: strings.Repeat("a", 64), ProviderHost: testMaterializationHost()}); err != nil {
		t.Fatalf("complete materialization: %v", err)
	}
	assertSocketClosed(t, f)
	requireTrace(t, f)
	if r := f.service.materializations[f.record.ID]; r.State != MaterializationDetached || r.ActiveRunID != "" || r.DetachReason != MaterializationDetachUnpublished {
		t.Fatalf("completion custody = %+v", r)
	}
}
func TestBuilderGatewayFinalizeControlRouteIsReachable(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0005")
	driveOneBuilderRead(t, f.client)
	client := startLifecycleControlServer(t, f)
	finalization, err := client.FinalizeBuilderGateway(context.Background(), f.record.ID)
	if err != nil {
		t.Fatalf("finalizing over the control route: %v", err)
	}
	if !finalization.Served || finalization.Attestation.EventCount != 1 {
		t.Fatalf("control-route finalization = %+v", finalization)
	}
	assertSocketClosed(t, f)
	requireTrace(t, f)
}
func TestBuilderGatewayFinalizeControlRouteReportsRefusal(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0006")
	client := startLifecycleControlServer(t, f)
	if result, err := client.FinalizeBuilderGateway(context.Background(), f.record.ID); err == nil || result.Attestation.ContractVersion != "" {
		t.Fatal("the control route reported success for an unstarted gateway")
	}
	assertSocketClosed(t, f)
}
func TestGatewayLifecycleTracePathIsInsideLayout(t *testing.T) {
	layout := testLayout(t, "gateway-lifecycle-path")
	path := sm.BuilderAccessTracePath(layout, "CTX-260731-lc0007")
	if !strings.HasPrefix(path, filepath.Clean(layout.StateDir)) {
		t.Fatalf("trace path %q is outside the manager state directory %q", path, layout.StateDir)
	}
}

// These changed receiver boundaries preserve failclosed custody after a real
// closure failure or cancellation. No arbitrary failure/panic counts as success.
func TestBuilderGatewayFailedCloseCannotSucceedOnRetry(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0009")
	driveOneBuilderRead(t, f.client)
	cause := errors.New("fixture native observation failed")
	f.observer.err = cause
	for attempt := 0; attempt < 2; attempt++ {
		result, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID)
		var typed *sm.PublicError
		if !errors.As(err, &typed) || typed.Code != sm.ContextErrorBuilderTraceUnavailable || !errors.Is(err, cause) || result.Attestation.ContractVersion != "" {
			t.Fatalf("failed close retry %d returned usable evidence: %+v %v", attempt, result, err)
		}
	}
	assertSocketClosed(t, f)
	if _, _, err := f.runtime.BuilderAccessTrace(f.record.ID); !errors.Is(err, cause) {
		t.Fatalf("failed owner returned usable trace on retry: %v", err)
	}
	if len(f.authority.publications) != 0 {
		t.Fatal("failed finalization published context")
	}
}
func TestBuilderGatewayCancellationClosesOwnedSurfaceWithoutEvidence(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0010")
	driveOneBuilderRead(t, f.client)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := f.service.FinalizeBuilderGateway(ctx, f.record.ID)
	if !errors.Is(err, context.Canceled) || result.Attestation.ContractVersion != "" {
		t.Fatalf("canceled finalization returned evidence: %+v %v", result, err)
	}
	assertSocketClosed(t, f)
	if len(f.authority.publications) != 0 {
		t.Fatal("canceled finalization published context")
	}
	// Healthy closure survives a canceled caller and is available to a later retry.
	if _, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID); err != nil {
		t.Fatalf("healthy closed trace lost after caller cancellation: %v", err)
	}
	requireTrace(t, f)
}
func TestBuilderGatewayFinalizationRejectsForeignTrace(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0011")
	driveOneBuilderRead(t, f.client)
	if _, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID); err != nil {
		t.Fatal(err)
	}
	other := newLifecycleGatewayManager(t, "CTX-260731-lc0012")
	driveOneBuilderRead(t, other.client)
	if _, err := other.service.FinalizeBuilderGateway(context.Background(), other.record.ID); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(sm.BuilderAccessTracePath(other.layout, other.record.ID))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sm.BuilderAccessTracePath(f.layout, f.record.ID), raw, 0600); err != nil {
		t.Fatal(err)
	}
	result, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID)
	var typed *sm.PublicError
	if !errors.As(err, &typed) || typed.Code != sm.ContextErrorBuilderTraceUnavailable || !strings.Contains(err.Error(), "another context") || result.Attestation.ContractVersion != "" {
		t.Fatalf("foreign valid trace accepted: %+v %v", result, err)
	}
	if len(f.authority.publications) != 0 {
		t.Fatal("foreign trace publication effects")
	}
}

// A genuine signed claim's missing-trace publication test remains in original
// custody until the canonical station signer/carrier is admitted. This receiver
// counterpart tests its actual host-side missing/unterminated trace requirement,
// without creating fake signed claims or calling a private board facade.
func TestBuilderPublicationPreparationRefusesMissingAccessTrace(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0013")
	driveOneBuilderRead(t, f.client)
	if _, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID); err != nil {
		t.Fatal(err)
	}
	requireTrace(t, f)
	if err := os.Remove(sm.BuilderAccessTracePath(f.layout, f.record.ID)); err != nil {
		t.Fatal(err)
	}
	result, serviceErr := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID)
	var serviceFailure *sm.PublicError
	if !errors.As(serviceErr, &serviceFailure) || serviceFailure.Code != sm.ContextErrorBuilderTraceUnavailable || result.Attestation.ContractVersion != "" {
		t.Fatalf("required trace missing but Service finalized: %+v %v", result, serviceErr)
	}
	err := sm.FinalizeAndRequireBuilderCoverageEvidence(context.Background(), f.layout, f.record.ID, func(context.Context, sm.Layout) (sm.BuilderGatewayFinalizationClient, error) {
		return nil, sm.ErrBuilderGatewayNotRunning
	})
	var typed *sm.PublicError
	if !errors.As(err, &typed) || typed.Code != sm.ContextErrorBuilderTraceUnavailable || len(f.authority.publications) != 0 {
		t.Fatalf("missing access trace was accepted or published: %v", err)
	}
}

// An unknown/failing authority must not leave an actual owned gateway serving;
// closing it is cleanup, while usable finalization/publication remains refused.
func TestBuilderGatewayClosesBeforeAuthorityReadFailure(t *testing.T) {
	f := newLifecycleGatewayManager(t, "CTX-260731-lc0014")
	driveOneBuilderRead(t, f.client)
	cause := errors.New("fixture authority unavailable")
	f.authority.err = cause
	before := cloneMaterialization(f.service.materializations[f.record.ID])
	result, err := f.service.FinalizeBuilderGateway(context.Background(), f.record.ID)
	if !errors.Is(err, cause) || result.Attestation.ContractVersion != "" {
		t.Fatalf("failed authority finalization = %+v %v", result, err)
	}
	assertSocketClosed(t, f)
	if !reflect.DeepEqual(f.service.materializations[f.record.ID], before) || len(f.authority.publications) != 0 {
		t.Fatal("authority failure changed catalog/publication custody")
	}
}
