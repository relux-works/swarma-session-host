package sessionmanager

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type builderLifetimeRunner struct {
	*hostCommitProbeRunner
	host             *SessionHost
	t                *testing.T
	observeError     error
	observed, closed int
}

func (r *builderLifetimeRunner) BuilderToolObservation(ctx context.Context) (BuilderToolObservation, error) {
	r.observed++
	if r.closed != 0 {
		r.t.Fatal("builder evidence read after native observation close")
	}
	if _, e := r.host.List(ctx); !errors.Is(e, ErrHostClosed) {
		r.t.Fatal("gateway close retained map lock or open admission", e)
	}
	if r.observeError != nil {
		return BuilderToolObservation{}, r.observeError
	}
	// This independent native boundary reports the one known fixture tool call;
	// the actual gateway/trace independently records the read below.
	return BuilderToolObservation{Reported: true, GatewayCalls: 1}, nil
}
func (r *builderLifetimeRunner) CloseObservation(context.Context) error {
	if r.observed != 1 {
		r.t.Fatal("builder observation did not precede observation close")
	}
	r.closed++
	return nil
}

type builderLifetimeFactory struct{ runner *builderLifetimeRunner }

func (f builderLifetimeFactory) Provider() string { return "muse" }
func (f builderLifetimeFactory) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	return f.runner, nil
}
func (f builderLifetimeFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	return f.runner, nil
}

// Real configured gateway/runtime/shim/trace must finalize before the actual
// native observation closer. A failed native observation keeps the lifetime
// lock and its original failure on retry, never a new healthy absence result.
// Configured private logging must retain the actual failed Close cause without
// panicking when the production call supplies nil fields.
// Only provider observation is controlled; no live provider/signer is qualified.
func TestHostBuilderGatewayClosesBeforeProviderObservation(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "observed", true: "failed-observation"}[failed], func(t *testing.T) {
			ctx := context.Background()
			h, _, factory := attachResultHost(t)
			h.options.Layout.OperatorLogFile = filepath.Join(t.TempDir(), "logs", "sessiond.log")
			runner := &builderLifetimeRunner{hostCommitProbeRunner: &hostCommitProbeRunner{inputTestBackend: &inputTestBackend{}, snapshot: factory.snapshot}, host: h, t: t}
			fault := errors.New("native builder observation unavailable")
			if failed {
				// Teardown owns this isolated fixture lock; no healthy handoff is claimed.
				t.Cleanup(func() { _ = h.lock.Close() })
				runner.observeError = fault
			}
			h.options.Factories = []ProviderHostFactory{builderLifetimeFactory{runner}}
			if _, e := h.Attach(ctx, AttachRequest{Owner: "parent/primary", Provider: "muse"}); e != nil {
				t.Fatal(e)
			}
			root := os.Getenv("SESSIONHOST_TEST_SOCKET_ROOT")
			if root == "" {
				t.Fatal("short isolated socket root required")
			}
			short, e := os.MkdirTemp(root, "builder-")
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = os.RemoveAll(short) })
			layout := h.options.Layout
			layout.SocketPath = filepath.Join(short, "control.sock")
			runtime, e := NewBuilderGatewayRuntime(BuilderGatewayRuntimeOptions{Layout: layout, ShimCommand: "/fixture/unused-shim"})
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = runtime.Close() })
			repo := t.TempDir()
			if e = os.MkdirAll(filepath.Join(repo, "docs"), 0700); e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(repo, "docs", "guide.md"), []byte("fixture evidence"), 0600); e != nil {
				t.Fatal(e)
			}
			id := "CTX-000000-hc0001"
			endpoint, e := runtime.StartBuilderGateway(ctx, &ContextRecord{ID: id, Provider: "muse"}, repo, NewContextBuilderAllowlist([]string{"docs"}))
			if e != nil {
				t.Fatal(e)
			}
			if e = runtime.BindBuilderGatewayObserver(id, runner); e != nil {
				t.Fatal(e)
			}
			client := NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
			t.Cleanup(func() { _ = client.Disconnect() })
			script := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read","arguments":{"path":"docs/guide.md"}}}` + "\n"
			if e = RunBuilderGatewayShim(client, strings.NewReader(script), io.Discard); e != nil {
				t.Fatal(e)
			}
			h.options.BuilderGateways = runtime
			if e = h.BuilderGatewayCloseError(); !errors.Is(e, ErrHostCapabilityUnavailable) {
				t.Fatal("unstarted builder close became observed success", e)
			}
			h.Close()
			if failed {
				if e = h.BuilderGatewayCloseError(); !errors.Is(e, fault) {
					t.Fatal("executable lost actual builder failure", e)
				}
				if runner.closed != 0 || h.closeReleased || h.builderClose == nil || !errors.Is(h.builderClose.err, fault) {
					t.Fatal("failed builder observation became healthy Close")
				}
				entries, raw := readOperatorLogEntries(t, h.options.Layout.OperatorLogFile)
				if !operatorLogContains(entries, "builder_gateway_close_failed", map[string]string{"cause": h.BuilderGatewayCloseError().Error()}) {
					t.Fatalf("actual builder Close cause missing from private log: %s", raw)
				}
				h.Close()
				if runner.closed != 0 || h.closeReleased || runner.observed != 1 {
					t.Fatal("retry forgot actual failed builder close")
				}
			} else {
				if e = h.BuilderGatewayCloseError(); e != nil {
					t.Fatal("executable could not read actual joined result", e)
				}
				if !h.closeReleased || runner.observed != 1 || runner.closed != 1 {
					t.Fatal("actual builder/native close order incomplete")
				}
				_, att, e := runtime.BuilderAccessTrace(id)
				if e != nil || att.EventCount != 1 {
					t.Fatal("actual gateway trace not finalized", e)
				}
			}
		})
	}
}
