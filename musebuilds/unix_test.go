//go:build darwin || linux

package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// writeFifo creates a FIFO with no writer: any blocking open of it would
// hang forever, so reaching a typed refusal proves the open did not block.
// The caller uses a readiness channel and a deadline watchdog: a regressed
// blocking open must produce a named test failure, not a hung suite.
func writeFifo(t *testing.T, path string) {
	t.Helper()
	if err := syscall.Mkfifo(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func resolveFifo(t *testing.T, env []string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := ResolveNewest(ctx, env); result <- err }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		t.Fatal("discovery blocked on FIFO instead of reaching its regular-file gate")
		return ctx.Err()
	}
}

// TestDiscoveryFifoCandidateRefusesWithoutBlocking proves a selected newest
// that is a FIFO refuses at the source gates instead of hanging the open:
// no writer ever arrives, so a blocking open would never return.
func TestDiscoveryFifoCandidateRefusesWithoutBlocking(t *testing.T) {
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	writeFifo(t, filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1"))
	err := resolveFifo(t, install.env())
	refusal := requireCode(t, err, CodeSelectionUnusable)
	if refusal.Stage != StageSource {
		t.Fatalf("stage = %s, want %s", refusal.Stage, StageSource)
	}
}

// TestDiscoveryFifoSidecarRefusesWithoutBlocking proves a sidecar that is a
// FIFO refuses as unreadable instead of hanging the open, while a regular
// sidecar on the same install shape succeeds as a control.
func TestDiscoveryFifoSidecarRefusesWithoutBlocking(t *testing.T) {
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	install.writeCandidate("1.4.4-R5419.1")
	writeFifo(t, filepath.Join(install.dir, sidecarName))
	err := resolveFifo(t, install.env())
	refusal := requireCode(t, err, CodeSidecarUnreadable)
	if refusal.Stage != StageSidecar {
		t.Fatalf("stage = %s, want %s", refusal.Stage, StageSidecar)
	}

	t.Run("regular sidecar succeeds as a control", func(t *testing.T) {
		control := newInstall(t)
		control.writeLauncher(plainScript)
		control.writeCandidate("1.4.3-R5000")
		control.writeCandidate("1.4.4-R5419.1")
		control.writeSidecar("1.4.4-R5419.1\n")
		discovery := resolveSuccess(t, control.env())
		if discovery.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v, want the installed maximum", discovery.Identity)
		}
	})
}

// TestDiscoveryForeignOwnedSourceRefuses proves a selected newest owned by
// another user refuses. Fixtures cannot fabricate ownership without
// privileges, so the test flips the expected owner through the EUID seam
// and drives the public entry; the restored seam succeeds as a control.
func TestDiscoveryForeignOwnedSourceRefuses(t *testing.T) {
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	install.writeCandidate("1.4.4-R5419.1")
	other := os.Geteuid() + 1
	previous := currentEUID
	currentEUID = func() int { return other }
	t.Cleanup(func() { currentEUID = previous })
	_, err := ResolveNewest(context.Background(), install.env())
	refusal := requireCode(t, err, CodeSelectionUnusable)
	if refusal.Stage != StageSource {
		t.Fatalf("stage = %s, want %s", refusal.Stage, StageSource)
	}
	currentEUID = previous
	discovery := resolveSuccess(t, install.env())
	if discovery.Identity.Build != "1.4.4-R5419.1" {
		t.Fatalf("selected %+v after restoring the seam", discovery.Identity)
	}
}
