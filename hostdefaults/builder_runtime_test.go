//go:build unix

package hostdefaults

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Explicit selected shim metadata reaches the real gateway; the same instance
// is reached through Service finalization or explicit Standalone.Close. Missing
// native observation refuses usable evidence while joining the owned socket.
// This does not qualify provider Start/Bind or ControlServer pre-close ordering.
func TestStandaloneBuilderRuntimeBindsServiceAndExplicitClose(t *testing.T) {
	for _, path := range []string{"service-finalize", "explicit-close"} {
		t.Run(path, func(t *testing.T) {
			o := standaloneOptions(t)
			binary := os.Getenv("SESSIONHOST_TEST_BINARY")
			if !filepath.IsAbs(binary) {
				t.Fatal("actual built shim binary required")
			}
			o.BuilderShimCommand, o.BuilderShimArgs = core.BuilderGatewayShimUsage(binary)
			short := os.Getenv("SESSIONHOST_TEST_SOCKET_ROOT")
			if short == "" {
				t.Fatal("short owned socket root required")
			}
			dir, err := os.MkdirTemp(short, "builder")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			o.Layout.SocketPath = filepath.Join(dir, "control.sock")
			s, err := OpenStandalone(context.Background(), o)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close(context.Background()) })
			if s.BuilderGateways == nil {
				t.Fatal("selected real builder runtime absent")
			}
			repo := t.TempDir()
			if err := os.Mkdir(filepath.Join(repo, "docs"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(repo, "docs", "guide.md"), []byte("controlled context"), 0600); err != nil {
				t.Fatal(err)
			}
			id := "CTX-261029-gw0001"
			record := &core.ContextRecord{ID: id, Provider: "claude", Lineage: core.ContextLineage{ProducedByRunID: "RUN-261029-builder01"}}
			endpoint, err := s.BuilderGateways.StartBuilderGateway(context.Background(), record, repo, core.NewContextBuilderAllowlist([]string{"docs"}))
			if err != nil {
				t.Fatal(err)
			}
			if endpoint.Command != binary || !reflect.DeepEqual(endpoint.Args, o.BuilderShimArgs) {
				t.Fatal("selected original shim command/argv lost")
			}
			client := core.NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
			script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
			if err := core.RunBuilderGatewayShim(client, strings.NewReader(script), io.Discard); err != nil {
				t.Fatal(err)
			}
			if path == "service-finalize" {
				result, e := s.Contexts.FinalizeBuilderGateway(context.Background(), id)
				err = e
				if result.Attestation.ContractVersion != "" {
					t.Fatal("missing native observer produced usable service evidence")
				}
			} else {
				err = s.Close(context.Background())
			}
			var public *core.PublicError
			if !errors.As(err, &public) || public.Code != core.ContextErrorBuilderToolIsolationUnavailable {
				t.Fatalf("wrong missing native observer refusal: %v", err)
			}
			if _, err := os.Lstat(endpoint.SocketPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("owned gateway remained after actual close: %v", err)
			}
			result, err := s.BuilderGateways.FinalizeBuilderGateway(context.Background(), id)
			if !errors.As(err, &public) || public.Code != core.ContextErrorBuilderToolIsolationUnavailable || result.Attestation.ContractVersion != "" {
				t.Fatalf("retry converted missing proof into success: %v", err)
			}
		})
	}
}

// Invalid selected shim paths and orphan argv refuse before allocating durable
// runtime state. Optional absence leaves the genuine source unavailable rather
// than guessing that the caller's executable implements the helper protocol.
func TestStandaloneBuilderShimSelectionRefusesBeforeState(t *testing.T) {
	for _, mode := range []string{"relative", "orphan-args", "unselected"} {
		t.Run(mode, func(t *testing.T) {
			o := standaloneOptions(t)
			if mode == "relative" {
				o.BuilderShimCommand = "relative-shim"
			} else if mode == "orphan-args" {
				o.BuilderShimArgs = []string{"session", "context-gateway-shim"}
			}
			s, err := OpenStandalone(context.Background(), o)
			if mode == "unselected" {
				if err != nil || s == nil || s.BuilderGateways != nil {
					t.Fatalf("unselected shim invented: %v", err)
				}
				_ = s.Close(context.Background())
				return
			}
			if err == nil || s != nil || !strings.Contains(err.Error(), "absolute builder shim") {
				t.Fatalf("invalid selection accepted/wrong refusal: %v", err)
			}
			for _, p := range []string{o.Layout.StateDir, o.GoalDirectory, o.BudgetDirectory} {
				if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("invalid selection allocated state: %v", err)
				}
			}
		})
	}
}
