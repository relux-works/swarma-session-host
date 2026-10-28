package musebuilds

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// armInterpose arms the deterministic interposition hook for one test and
// guarantees it is disarmed afterwards, so no hook leaks into another test.
// Discovery calls the hook synchronously on the test's own goroutine.
func armInterpose(t *testing.T, hook func(point interposePoint)) {
	t.Helper()
	if interposeForTest != nil {
		t.Fatal("interposition hook already armed")
	}
	interposeForTest = hook
	t.Cleanup(func() { interposeForTest = nil })
}

// digestOfFile hashes the current bytes at path.
func digestOfFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// TestDiscoveryRestartsOnceOnNewerInstall proves a newer candidate that
// lands between selection and the recheck restarts discovery onto the new
// maximum: the first attempt disagrees and the second linearizes on the
// install as it now stands.
func TestDiscoveryRestartsOnceOnNewerInstall(t *testing.T) {
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	install.writeCandidate("1.4.4-R5419.1")
	rechecks := 0
	armInterpose(t, func(point interposePoint) {
		if point != interposeBeforeRecheck {
			return
		}
		rechecks++
		if rechecks == 1 {
			install.writeCandidate("1.4.5-R5500")
		}
	})
	discovery, err := ResolveNewest(context.Background(), install.env())
	discovery = requireSuccess(t, discovery, err)
	if discovery.Identity.Build != "1.4.5-R5500" {
		t.Fatalf("selected %s, want the newer build that landed mid-discovery", discovery.Identity.Build)
	}
	if rechecks != 2 {
		t.Fatalf("recheck ran %d times, want 2 (disagree, restart, agree)", rechecks)
	}
	if discovery.Provenance != ProvenanceSidecarMissing {
		t.Fatalf("Provenance = %q, want %q", discovery.Provenance, ProvenanceSidecarMissing)
	}
}

// TestDiscoveryRepeatedChangeRefuses proves an install that keeps changing
// under discovery refuses after exactly one restart: each attempt observes
// a newer candidate than it selected, and the second disagreement is
// terminal.
func TestDiscoveryRepeatedChangeRefuses(t *testing.T) {
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	install.writeCandidate("1.4.4-R5419.1")
	rechecks := 0
	armInterpose(t, func(point interposePoint) {
		if point != interposeBeforeRecheck {
			return
		}
		rechecks++
		install.writeCandidate(fmt.Sprintf("1.4.%d-R%d", 4+rechecks, 5419+rechecks))
	})
	_, err := ResolveNewest(context.Background(), install.env())
	refusal := requireCode(t, err, CodeDiscoveryChanged)
	if refusal.Stage != StageSelect {
		t.Fatalf("stage = %s, want %s", refusal.Stage, StageSelect)
	}
	if rechecks != 2 {
		t.Fatalf("recheck ran %d times, want exactly 2 (one restart, then refuse)", rechecks)
	}
}

// TestDiscoveryRestartsOnInPlaceSourceRewrite proves a selected source
// rewritten in place — the same inode with new bytes, which no identity
// comparison can observe — restarts discovery through the digest check and
// succeeds with the rewritten bytes.
func TestDiscoveryRestartsOnInPlaceSourceRewrite(t *testing.T) {
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	selected := install.writeCandidate("1.4.4-R5419.1")
	before := digestOfFile(t, selected)
	var hookErr error
	rechecks := 0
	armInterpose(t, func(point interposePoint) {
		if point != interposeBeforeRecheck {
			return
		}
		rechecks++
		if rechecks == 1 {
			if err := os.WriteFile(selected, []byte("#!/bin/sh\necho rebuilt-in-place\n"), 0o755); err != nil {
				hookErr = err
			}
		}
	})
	discovery := resolveSuccess(t, install.env())
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if discovery.Identity.Build != "1.4.4-R5419.1" {
		t.Fatalf("selected %+v, want the rewritten maximum", discovery.Identity)
	}
	if discovery.Identity.SHA256 == before {
		t.Fatalf("digest %q survived an in-place byte rewrite", discovery.Identity.SHA256)
	}
	if discovery.Identity.SHA256 != digestOfFile(t, selected) {
		t.Fatalf("digest %q does not match the rewritten bytes", discovery.Identity.SHA256)
	}
	if rechecks != 2 {
		t.Fatalf("recheck ran %d times, want 2 (disagree, restart, agree)", rechecks)
	}
}

// TestDiscoverySourceSwapBetweenValidateAndHash proves a swap of the
// selected source after it passed validation cannot lend unvalidated bytes
// to the selection: the digest comes from the pinned descriptor, the
// recheck observes the swap, and the restart reports the swapped install
// truthfully instead of stepping down to an older build.
func TestDiscoverySourceSwapBetweenValidateAndHash(t *testing.T) {
	const newest, older = "1.4.4-R5419.1", "1.4.3-R5000"

	t.Run("non-executable swap restarts onto a refusal", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate(older)
		selected := install.writeCandidate(newest)
		validations := 0
		attempts := 0
		armInterpose(t, func(point interposePoint) {
			if point == interposeAttemptEntry {
				attempts++
			}
			if point != interposeAfterValidate {
				return
			}
			validations++
			if validations == 1 {
				install.writeFileSync(selected, []byte("swapped bytes"), 0o644)
			}
		})
		_, err := ResolveNewest(context.Background(), install.env())
		refusal := requireCode(t, err, CodeSelectionUnusable)
		if refusal.Stage != StageSource {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StageSource)
		}
		if validations != 1 || attempts != 2 {
			t.Fatalf("validations=%d attempts=%d, want 1 validation and 2 attempts", validations, attempts)
		}
	})

	t.Run("valid swap restarts and succeeds with the new bytes", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate(older)
		selected := install.writeCandidate(newest)
		before := digestOfFile(t, selected)
		validations := 0
		armInterpose(t, func(point interposePoint) {
			if point != interposeAfterValidate {
				return
			}
			validations++
			if validations == 1 {
				install.writeFileSync(selected, []byte("#!/bin/sh\necho swapped-valid\n"), 0o755)
			}
		})
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != newest {
			t.Fatalf("selected %+v, want the swapped maximum", discovery.Identity)
		}
		if discovery.Identity.SHA256 == before {
			t.Fatalf("digest %q survived a source swap", discovery.Identity.SHA256)
		}
		if discovery.Identity.SHA256 != digestOfFile(t, selected) {
			t.Fatalf("digest %q does not match the swapped bytes", discovery.Identity.SHA256)
		}
		if validations != 2 {
			t.Fatalf("validation ran %d times, want 2 (disagree, restart, agree)", validations)
		}
	})
}

// TestDiscoveryRestartsOnSidecarChange proves a sidecar that moves between
// selection and the recheck restarts discovery: the second attempt reads
// the moved selector and its provenance reflects the install as it now
// stands.
func TestDiscoveryRestartsOnSidecarChange(t *testing.T) {
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	install.writeCandidate("1.4.4-R5419.1")
	install.writeSidecar("1.4.3-R5000\n")
	rechecks := 0
	armInterpose(t, func(point interposePoint) {
		if point != interposeBeforeRecheck {
			return
		}
		rechecks++
		if rechecks == 1 {
			install.writeSidecar("1.4.4-R5419.1\n")
		}
	})
	discovery := resolveSuccess(t, install.env())
	if rechecks != 2 {
		t.Fatalf("recheck ran %d times, want 2 (disagree, restart, agree)", rechecks)
	}
	if discovery.SidecarTarget != "1.4.4-R5419.1" || discovery.Provenance != "" {
		t.Fatalf("unexpected selector after restart: %+v", discovery)
	}
}

// TestDiscoveryRestartsOnLauncherBoundaryChange proves a launcher relinked
// onto another install between selection and the recheck restarts discovery
// onto the new boundary: the same PATH entry resolves to a new canonical
// install, and the selection comes from there.
func TestDiscoveryRestartsOnLauncherBoundaryChange(t *testing.T) {
	first := newInstall(t)
	first.writeLauncher(plainScript)
	first.writeCandidate("1.4.4-R5419.1")
	second := newInstall(t)
	second.writeLauncher(plainScript)
	second.writeCandidate("1.4.5-R5500")
	var hookErr error
	rechecks := 0
	armInterpose(t, func(point interposePoint) {
		if point != interposeBeforeRecheck {
			return
		}
		rechecks++
		if rechecks == 1 {
			link := filepath.Join(first.dir, launcherName)
			if err := os.Remove(link); err != nil {
				hookErr = err
				return
			}
			if err := os.Symlink(filepath.Join(second.dir, launcherName), link); err != nil {
				hookErr = err
			}
		}
	})
	discovery := resolveSuccess(t, first.env())
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if discovery.Identity.Build != "1.4.5-R5500" {
		t.Fatalf("selected %+v, want the maximum of the relinked install", discovery.Identity)
	}
	if discovery.InstallDir != second.dir {
		t.Fatalf("InstallDir = %s, want %s", discovery.InstallDir, second.dir)
	}
	if rechecks != 2 {
		t.Fatalf("recheck ran %d times, want 2 (disagree, restart, agree)", rechecks)
	}
}

// TestDiscoveryEmptiedInstallRefuses proves candidates deleted between
// selection and the recheck do not produce an empty success: the first
// attempt disagrees and the restart reports the emptied install with its
// own refusal.
func TestDiscoveryEmptiedInstallRefuses(t *testing.T) {
	install := newInstall(t)
	install.writeLauncher(plainScript)
	selected := install.writeCandidate("1.4.4-R5419.1")
	var hookErr error
	rechecks := 0
	armInterpose(t, func(point interposePoint) {
		if point != interposeBeforeRecheck {
			return
		}
		rechecks++
		if rechecks == 1 {
			if err := os.Remove(selected); err != nil {
				hookErr = err
			}
		}
	})
	_, err := ResolveNewest(context.Background(), install.env())
	refusal := requireCode(t, err, CodeNoInstalledBuild)
	if refusal.Stage != StageScan {
		t.Fatalf("stage = %s, want %s", refusal.Stage, StageScan)
	}
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if rechecks != 1 {
		t.Fatalf("recheck ran %d times, want 1 (the restart refuses before rechecking)", rechecks)
	}
}

// TestDiscoveryUnreadableSwapKeepsValidatedFD distinguishes reading the
// validated descriptor from opening the pathname again after validation.
// The first attempt MUST reach the recheck; only the restart may refuse.
func TestDiscoveryUnreadableSwapKeepsValidatedFD(t *testing.T) {
	skipIfRoot(t)
	install := validScanWithNewest(t)
	path := filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1")
	attempts, validations, rechecks := 0, 0, 0
	armInterpose(t, func(point interposePoint) {
		switch point {
		case interposeAttemptEntry:
			attempts++
		case interposeAfterValidate:
			validations++
			if validations == 1 {
				install.writeFileSync(path, []byte("unreadable replacement"), 0000)
			}
		case interposeBeforeRecheck:
			rechecks++
		}
	})
	_, err := ResolveNewest(context.Background(), install.env())
	requireCode(t, err, CodeSelectionUnusable)
	if attempts != 2 || validations != 1 || rechecks != 1 {
		t.Fatalf("attempts=%d validations=%d rechecks=%d, want 2/1/1", attempts, validations, rechecks)
	}
}

// TestDiscoverySourceTruncationRestartsRead changes the source DURING the
// first digest, not only before the acceptance sweep. A short read is a
// consistency disagreement and a stable retry hashes the new whole source.
func TestDiscoverySourceTruncationRestartsRead(t *testing.T) {
	install := validScanWithNewest(t)
	path := filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1")
	install.writeFileSync(path, bytes.Repeat([]byte("x"), 3*hashReadChunk), 0755)
	attempts, reads := 0, 0
	armInterpose(t, func(point interposePoint) {
		if point == interposeAttemptEntry {
			attempts++
		}
		if point == interposeInitialRead {
			reads++
			if reads == 1 {
				if err := os.Truncate(path, 16); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
	d := resolveSuccess(t, install.env())
	if attempts != 2 || d.Identity.SHA256 != digestOfFile(t, path) {
		t.Fatalf("attempts=%d discovery=%+v", attempts, d)
	}
}
