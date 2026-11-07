package sessionhost

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

func TestSelectedRegistryExit6(t *testing.T) {
	for _, code := range []string{"session_host_capability_missing", "session_host_provider_unsupported"} {
		t.Run(code, func(t *testing.T) {
			p, o, b, q := selectedFixture(t)
			refuse := func() {
				o.available = code != "session_host_capability_missing"
				if o.available {
					o.admitHook = func(context.Context, string, []byte) error {
						return ordinaryManagerRefusal{code: code}
					}
				}
			}
			refuse()
			original := memberOperationBytes(t, q)
			check := func(t *testing.T, data []byte, exit int) {
				t.Helper()
				selectedCode(t, data, exit, code)
				t.Logf("exit=%d want=6 starts=%d preparations=%d", exit, o.starts.Load(), b.prepares.Load())
				if exit != 6 {
					t.Errorf("registry exit for %s = %d, want 6", code, exit)
				}
				if o.starts.Load() != 0 || b.prepares.Load() != 0 {
					t.Fatal("refusal caused ordinary start or provider preparation")
				}
			}
			var first []byte
			t.Run("first-refusal", func(t *testing.T) {
				data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", original)
				first = data
				check(t, data, exit)
			})
			entry, exists, err := p.daemon.journal.read(q["operation_id"].(string))
			if err != nil || !exists || !bytes.Equal(entry.Refusal, first) {
				t.Fatal("first refusal was not durably stored", err)
			}
			// Remove the refusal cause: replay must use the saved refusal, even
			// across daemon restart, rather than re-enter ordinary execution.
			o.available = true
			o.admitHook = nil
			t.Run("durable-replay", func(t *testing.T) {
				data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", original)
				check(t, data, exit)
				if !bytes.Equal(data, first) {
					t.Fatal("replay changed refusal bytes")
				}
			})
			directory := p.daemon.journal.store.dir
			closeSelected(t, p)
			p, err = registerHostPlugin(context.Background(), time.Now().Add(20*time.Second), directory, o)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeSelected(t, p) })
			o.plugin = p
			t.Run("restart-replay", func(t *testing.T) {
				data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", original)
				check(t, data, exit)
				if !bytes.Equal(data, first) {
					t.Fatal("restart changed refusal bytes")
				}
			})
			dir, err := os.MkdirTemp("/tmp", "registry-exit-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			socket := filepath.Join(dir, "manager.sock")
			listener, err := localipc.Listen(socket)
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan error, 1)
			go func() { served <- p.serveSelectedListener(listener) }()
			defer func() {
				closeSelected(t, p)
				select {
				case err := <-served:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("selected RPC listener cleanup exceeded bound")
				}
			}()
			for _, phase := range []string{"rpc-replay", "rpc-first-refusal"} {
				t.Run(phase, func(t *testing.T) {
					request := original
					if phase == "rpc-first-refusal" {
						refuse()
						q["operation_id"] = "55555555-5555-4555-8555-555555555555"
						request = memberOperationBytes(t, q)
					}
					var output bytes.Buffer
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					exit := SelectedManagerRPC(ctx, selectedManagerProtocol, "launch", socket, bytes.NewReader(request), &output)
					check(t, output.Bytes(), exit)
					if phase == "rpc-replay" && !bytes.Equal(bytes.TrimSpace(output.Bytes()), first) {
						t.Fatal("RPC replay changed refusal bytes")
					}
				})
			}
		})
	}
}
