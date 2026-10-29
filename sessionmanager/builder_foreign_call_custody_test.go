package sessionmanager

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Claim: the actual runtime Start/Bind/Finalize path refuses one foreign native
// call with the isolation code and no finalization, closes the socket, preserves
// the original trace and served file, and retains refusal on same-owner retry/read.
// The identical clean neighbor returns its one-event finalization.
// Limit: native observation is a declared fixture, not a live provider. An
// attested disk trace still precedes reconciliation; restart durability is unproven.
func TestBuilderGatewayForeignCallFinalizationPreservesCustody(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ctxID   string
		foreign int
	}{
		{"clean", "CTX-260731-fc0000", 0},
		{"one_foreign_call", "CTX-260731-fc0001", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			layout := gateTestLayout(t, tc.name)
			runtime, err := NewBuilderGatewayRuntime(BuilderGatewayRuntimeOptions{Layout: layout, ShimCommand: "/declared-fixture-shim"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = runtime.Close() })
			root := gatewayRepo(t)
			guidePath := filepath.Join(root, "docs/guide.md")
			before, err := os.ReadFile(guidePath)
			if err != nil {
				t.Fatal(err)
			}
			record := &ContextRecord{ID: tc.ctxID, Provider: "claude"}
			record.Lineage.ProducedByRunID = "RUN-260731-abc123"
			endpoint, err := runtime.StartBuilderGateway(ctx, record, root, NewContextBuilderAllowlist([]string{"docs"}))
			if err != nil {
				t.Fatal(err)
			}
			observer := &testBuilderToolObserver{observation: &BuilderToolObservation{Reported: true, GatewayCalls: 1, ForeignToolCalls: tc.foreign}}
			var host = struct {
				RunnerHost
				*testBuilderToolObserver
			}{testBuilderToolObserver: observer}
			if err := runtime.BindBuilderGatewayObserver(tc.ctxID, host); err != nil {
				t.Fatal(err)
			}
			client := NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
			t.Cleanup(func() { _ = client.Disconnect() })
			script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
			if err := RunBuilderGatewayShim(client, strings.NewReader(script), io.Discard); err != nil {
				t.Fatalf("driving the builder: %v", err)
			}

			result, closeErr := runtime.FinalizeBuilderGateway(ctx, tc.ctxID)
			if tc.foreign == 0 {
				if closeErr != nil || !result.Served || result.CtxID != tc.ctxID || result.Attestation.EventCount != 1 {
					t.Fatalf("clean runtime control refused: %v %+v", closeErr, result)
				}
			} else if !builderIsContextErrorCode(closeErr, ContextErrorBuilderToolIsolationUnavailable) || result != (BuilderGatewayFinalization{}) {
				t.Fatalf("foreign native call produced usable finalization: %v %+v", closeErr, result)
			}

			if _, err := os.Lstat(endpoint.SocketPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("close left a socket path: %v", err)
			}
			if conn, err := net.DialTimeout("unix", endpoint.SocketPath, time.Second); err == nil {
				_ = conn.Close()
				t.Fatal("closed gateway still accepts connections")
			}
			tracePath := BuilderAccessTracePath(layout, tc.ctxID)
			events, attestation, err := LoadBuilderAccessTrace(tracePath)
			if err != nil {
				t.Fatalf("original internally attested trace lost: %v", err)
			}
			if len(events) != 1 || attestation.EventCount != 1 || attestation.CtxID != tc.ctxID || attestation.RunID != strings.ToUpper(record.Lineage.ProducedByRunID) || attestation.Provider != record.Provider {
				t.Fatalf("trace custody changed: %+v %+v", events, attestation)
			}
			event := events[0]
			if event.Operation != ContextRepositoryAccessRead || event.Outcome != ContextRepositoryAccessGranted || event.Path != "docs/guide.md" || event.ProviderEventID == "" || event.CtxID != tc.ctxID || event.RunID != attestation.RunID || event.Provider != attestation.Provider || event.ContentDigest != ContextRepositoryAccessContentDigest(attestation.Anchor, before) {
				t.Fatalf("original read evidence changed or foreign event synthesized: %+v", event)
			}
			traceBefore, err := os.ReadFile(tracePath)
			if err != nil {
				t.Fatal(err)
			}
			if tc.foreign == 0 {
				_, readAttestation, err := runtime.BuilderAccessTrace(tc.ctxID)
				if err != nil || readAttestation != result.Attestation || readAttestation != attestation {
					t.Fatalf("clean finalization/read disagree: %v %+v", err, readAttestation)
				}
			} else {
				for attempt := 0; attempt < 2; attempt++ {
					retry, err := runtime.FinalizeBuilderGateway(ctx, tc.ctxID)
					if !builderIsContextErrorCode(err, ContextErrorBuilderToolIsolationUnavailable) || retry != (BuilderGatewayFinalization{}) {
						t.Fatalf("foreign refusal lost on retry %d: %v %+v", attempt, err, retry)
					}
				}
				readEvents, readAttestation, err := runtime.BuilderAccessTrace(tc.ctxID)
				if !builderIsContextErrorCode(err, ContextErrorBuilderToolIsolationUnavailable) || len(readEvents) != 0 || readAttestation != (ContextRepositoryAccessAttestation{}) {
					t.Fatalf("failed runtime exposed usable trace: %v %+v %+v", err, readEvents, readAttestation)
				}
			}
			traceAfter, err := os.ReadFile(tracePath)
			if err != nil || !bytes.Equal(traceBefore, traceAfter) {
				t.Fatalf("retry/read changed original trace: %v", err)
			}
			after, err := os.ReadFile(guidePath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("gateway changed repository content: %v", err)
			}
		})
	}
}
