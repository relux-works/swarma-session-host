//go:build unix

package sessionmanager

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/sessionproxy"
)

// The recovery helper recreates the real endpoint without Attach, retains its
// controller/record, and refuses foreign or unknown occupants without effects.
// The external original lifecycle separately proves the actual Recover callsite.
func TestHostProxyRestoreCustodyAndRetry(t *testing.T) {
	for _, mode := range []string{"absent", "stale", "live", "unknown", "file", "symlink", "nonprivate", "invalid-target"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h, _, runner, request := coreProxyFixture(t)
			attached, err := h.Attach(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			id := attached.Session.SessionID
			validTarget := runner.target
			path := sessionproxy.SocketPathFor(h.options.Layout.AttachmentsDir, id)
			h.mu.Lock()
			defer h.mu.Unlock()
			if err := h.closeProviderProxyLocked(id); err != nil {
				t.Fatal(err)
			}
			root := filepath.Dir(path)
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			var closeOccupant func()
			switch mode {
			case "stale", "live":
				listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if err != nil {
					t.Fatal(err)
				}
				listener.SetUnlinkOnClose(false)
				closeOccupant = func() { _ = listener.Close() }
				if mode == "stale" {
					closeOccupant()
				}
			case "unknown":
				// Stream-to-datagram EPROTOTYPE is unknown, never known absent.
				listener, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
				if err != nil {
					t.Fatal(err)
				}
				closeOccupant = func() { _ = listener.Close() }
				if probe := probeUnixSocket(path, transportProbeTimeout); probe == nil || unixSocketKnownAbsent(probe) {
					t.Fatalf("unknown fixture did not materialize: %v", probe)
				}
			case "file", "invalid-target":
				if err := os.WriteFile(path, []byte("foreign fixture"), 0600); err != nil {
					t.Fatal(err)
				}
				if mode == "invalid-target" {
					runner.target = "ws://foreign.example:1"
				}
			case "symlink":
				if err := os.Symlink("untouched-missing-target", path); err != nil {
					t.Fatal(err)
				}
			case "nonprivate":
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
			}
			if closeOccupant != nil {
				defer closeOccupant()
			}
			before, statErr := os.Lstat(path)
			if mode != "absent" && mode != "nonprivate" && statErr != nil {
				t.Fatal("occupant fixture absent", statErr)
			}
			record := copyRunnerRecord(h.sessions[id])
			lease := h.attachmentControllers[id]
			input := h.inputs[id]
			err = h.restoreProviderProxyLocked(ctx, id)
			valid := mode == "absent" || mode == "stale"
			if valid {
				if err != nil || h.proxies[id] == nil || h.proxies[id].proxy.Endpoint() != attached.ManagerEndpoint {
					t.Fatalf("valid restored proxy absent: %v", err)
				}
				connection, err := net.DialTimeout("unix", path, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				_ = connection.SetDeadline(time.Now().Add(time.Second))
				if _, err := connection.Write([]byte("echo")); err != nil {
					t.Fatal(err)
				}
				body := make([]byte, 4)
				_, err = io.ReadFull(connection, body)
				_ = connection.Close()
				if err != nil || string(body) != "echo" {
					t.Fatalf("restored bytes missing: %q %v", body, err)
				}
				owned := h.proxies[id]
				if err := h.restoreProviderProxyLocked(ctx, id); err != nil || h.proxies[id] != owned {
					t.Fatalf("retry replaced current proxy: %v", err)
				}
			} else {
				if err == nil || h.proxies[id] != nil {
					t.Fatalf("refusal published proxy: %v", err)
				}
				if mode == "nonprivate" {
					if !errors.Is(err, sessionproxy.ErrPrivatePath) {
						t.Fatalf("wrong private-root rejection: %v", err)
					}
				} else if mode != "invalid-target" && !errors.Is(err, sessionproxy.ErrPathOccupied) {
					t.Fatalf("wrong occupant rejection: %v", err)
				}
				if before != nil {
					after, err := os.Lstat(path)
					if err != nil || !os.SameFile(before, after) {
						t.Fatalf("refusal changed occupant: %v", err)
					}
				}
				if mode == "file" || mode == "invalid-target" {
					data, err := os.ReadFile(path)
					if err != nil || string(data) != "foreign fixture" {
						t.Fatalf("refusal changed foreign bytes: %q %v", data, err)
					}
				}
				if mode == "symlink" {
					target, err := os.Readlink(path)
					if err != nil || target != "untouched-missing-target" {
						t.Fatalf("refusal changed symlink: %q %v", target, err)
					}
				}
				// Only the fixture owner removes its deliberately blocking path.
				// Retry must use the retained real runner/controller, without Attach.
				if closeOccupant != nil {
					closeOccupant()
				}
				if before != nil {
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Chmod(root, 0700); err != nil {
					t.Fatal(err)
				}
				runner.target = validTarget
				if err := h.restoreProviderProxyLocked(ctx, id); err != nil || h.proxies[id] == nil || h.proxies[id].proxy.Endpoint() != attached.ManagerEndpoint {
					t.Fatalf("retry after owned fixture removal failed: %v", err)
				}
			}
			if !reflect.DeepEqual(record, h.sessions[id]) || h.runners[id] != runner || h.inputs[id] != input || h.attachmentControllers[id] != lease || runner.forces != 0 {
				t.Fatal("restore changed committed state, controller, input or native runner")
			}
			if err := h.closeProviderProxyLocked(id); err != nil {
				t.Fatal(err)
			}
		})
	}
}
