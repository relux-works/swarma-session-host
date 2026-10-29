//go:build unix

package hostdefaults

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Actual default construction binds its selected gateway runtime into core.
// ControlServer drain must retain missing-observer failure after its deferred
// Host.Close; an empty gateway runtime is the valid neighboring case. This does
// not qualify a native observer, launch authority or builder confinement.
func TestStandaloneBuilderGatewayCloseFollowsControlLifetime(t *testing.T) {
	for _, active := range []bool{false, true} {
		name := "empty"
		if active {
			name = "missing-observer"
		}
		t.Run(name, func(t *testing.T) {
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
			dir, err := os.MkdirTemp(short, "close")
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
			if !errors.Is(s.Host.BuilderGatewayCloseError(), core.ErrHostCapabilityUnavailable) {
				t.Fatal("selected gateway lifecycle was not bound to core before serving")
			}
			var endpoint core.BuilderGatewayEndpoint
			if active {
				repo := t.TempDir()
				if err := os.Mkdir(filepath.Join(repo, "docs"), 0700); err != nil {
					t.Fatal(err)
				}
				record := &core.ContextRecord{ID: "CTX-261029-gw0002", Provider: "claude", Lineage: core.ContextLineage{ProducedByRunID: "RUN-261029-builder02"}}
				endpoint, err = s.BuilderGateways.StartBuilderGateway(context.Background(), record, repo, core.NewContextBuilderAllowlist([]string{"docs"}))
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			exited := make(chan error, 1)
			go func() { exited <- s.Serve(ctx) }()
			select {
			case <-s.Control.Ready():
			case err := <-exited:
				t.Fatalf("control startup: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			s.Control.Drain()
			select {
			case err = <-exited:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if !active {
				if err != nil || s.Host.BuilderGatewayCloseError() != nil || s.Close(context.Background()) != nil {
					t.Fatalf("empty gateway close failed: %v", err)
				}
				return
			}
			for _, result := range []error{err, s.Host.BuilderGatewayCloseError(), s.Close(context.Background())} {
				var public *core.PublicError
				if !errors.As(result, &public) || public.Code != core.ContextErrorBuilderToolIsolationUnavailable {
					t.Fatalf("control close lost original missing-observer refusal: %v", result)
				}
			}
			if _, err := os.Lstat(endpoint.SocketPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("gateway socket survived control drain: %v", err)
			}
		})
	}
}
