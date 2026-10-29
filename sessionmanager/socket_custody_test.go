//go:build unix

package sessionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Only explicit refused/absent kernel results authorize stale classification;
// permission, transport mismatch and timeout remain unknown. This table tests
// classification, not actual foreign-path permission mutation.
func TestSocketProbeAbsenceRequiresKnownKernelResult(t *testing.T) {
	for _, row := range []struct {
		name   string
		err    error
		absent bool
	}{
		{"refused", syscall.ECONNREFUSED, true},
		{"missing", os.ErrNotExist, true},
		{"wrapped-refused", fmt.Errorf("probe: %w", syscall.ECONNREFUSED), true},
		{"permission", syscall.EACCES, false},
		{"operation", syscall.EPERM, false},
		{"transport", syscall.EPROTOTYPE, false},
		{"timeout", syscall.ETIMEDOUT, false},
		{"other", errors.New("unknown probe"), false},
		{"reachable", nil, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := unixSocketKnownAbsent(row.err); got != row.absent {
				t.Fatalf("probe classified absent=%v, want %v", got, row.absent)
			}
		})
	}
}

// The same actual startup sweep preserves unknown datagram endpoint/token/socket
// images, preserves live stream neighbors, and still removes known stale stream
// endpoints and sockets. Counts do not relabel unknown observations as live.
func TestSocketRepairPreservesUnknownAndLiveButRemovesKnownStale(t *testing.T) {
	for _, mode := range []string{"unknown", "live", "stale"} {
		t.Run(mode, func(t *testing.T) {
			layout := legacyPhysicalLayout(t, "socket-repair-neighbor")
			host, err := legacyPhysicalNewHost(t, layout)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(filepath.Dir(layout.SocketPath), "neighbor.sock")
			if mode == "unknown" {
				conn, e := net.ListenUnixgram("unixgram", &net.UnixAddr{Net: "unixgram", Name: path})
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { _ = conn.Close() })
			} else {
				listener, e := net.Listen("unix", path)
				if e != nil {
					t.Fatal(e)
				}
				listener.(*net.UnixListener).SetUnlinkOnClose(false)
				t.Cleanup(func() { _ = listener.Close() })
				if mode == "stale" {
					if e = listener.Close(); e != nil {
						t.Fatal(e)
					}
				}
			}
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			name := strings.Repeat("a", 64)
			dir := filepath.Join(filepath.Dir(layout.StateDir), name)
			if err = os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			endpoint := filepath.Join(dir, endpointFileName)
			token := filepath.Join(dir, tokenFileName)
			raw, err := json.Marshal(map[string]string{"board_fingerprint": name, "socket_path": path})
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(endpoint, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(token, []byte("fixture-only"), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			report, err := host.RepairStartup(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "stale" {
				if report.StaleSocketsRemoved != 1 || report.StaleEndpointsRemoved != 1 {
					t.Fatalf("known stale not repaired: %+v", report)
				}
				for _, p := range []string{path, endpoint, token} {
					if _, e := os.Lstat(p); !errors.Is(e, os.ErrNotExist) {
						t.Fatalf("known stale retained: %s %v", filepath.Base(p), e)
					}
				}
				return
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("retained transport changed: %v", err)
			}
			if b, e := os.ReadFile(endpoint); e != nil || string(b) != string(raw) {
				t.Fatalf("retained endpoint changed: %v", e)
			}
			if b, e := os.ReadFile(token); e != nil || string(b) != "fixture-only" {
				t.Fatalf("retained token changed: %v", e)
			}
			if report.StaleSocketsRemoved != 0 || report.StaleEndpointsRemoved != 0 {
				t.Fatalf("retained entry reported stale: %+v", report)
			}
			if mode == "live" && (report.LiveSocketsPreserved < 1 || report.LiveEndpointsPreserved != 1) {
				t.Fatalf("live neighbor missing: %+v", report)
			}
			if mode == "unknown" && (report.LiveSocketsPreserved != 0 || report.LiveEndpointsPreserved != 0) {
				t.Fatalf("unknown falsely reported live: %+v", report)
			}
		})
	}
}

// The shared cleanup primitive preserves a changed inode and removes its own
// unchanged socket. This deterministic check does not claim atomic unlink
// against a rename between the final Lstat and Remove.
func TestSocketCustodyRemovalRequiresSameOwnedInode(t *testing.T) {
	layout := legacyPhysicalLayout(t, "socket-inode")
	path := layout.SocketPath
	original, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	original.(*net.UnixListener).SetUnlinkOnClose(false)
	defer original.Close()
	observed, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	replacement.(*net.UnixListener).SetUnlinkOnClose(false)
	defer replacement.Close()
	current, err := os.Lstat(path)
	if err != nil || os.SameFile(observed, current) {
		t.Fatalf("replacement did not materialize: %v", err)
	}
	if removed, e := removeSameOwnedSocket(path, observed); e != nil || removed {
		t.Fatalf("changed socket removed=%v error=%v", removed, e)
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(current, after) {
		t.Fatalf("changed socket lost: %v", err)
	}
	if removed, e := removeSameOwnedSocket(path, current); e != nil || !removed {
		t.Fatalf("own unchanged socket not removed=%v error=%v", removed, e)
	}
	if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("own unchanged path retained: %v", err)
	}
}
