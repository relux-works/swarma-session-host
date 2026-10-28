package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDiscoveryDirectoryReplacementKeepsLeases is the F2 repeated-class
// regression. Replacement is performed only after all discovery observations
// exist, and every original handle must still be live through that point.
func TestDiscoveryDirectoryReplacementKeepsLeases(t *testing.T) {
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.4-R5419.1")
	var leases []*os.File
	attempts, rechecks := 0, 0
	snapshotObservedForTest = func(s *discoverySnapshot) {
		if s.inventoryFile == nil {
			t.Fatal("inventory handle was never reached")
		}
		for _, lease := range s.files {
			if _, err := lease.file.Stat(); err != nil {
				t.Fatalf("descriptor released before acceptance: %s: %v", lease.path, err)
			}
			leases = append(leases, lease.file)
		}
	}
	t.Cleanup(func() { snapshotObservedForTest = nil })
	armInterpose(t, func(point interposePoint) {
		if point == interposeAttemptEntry {
			attempts++
		}
		if point != interposeBeforeRecheck {
			return
		}
		rechecks++
		if rechecks != 1 {
			return
		}
		if err := os.Rename(install.dir, install.dir+"-old"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(install.dir, 0755); err != nil {
			t.Fatal(err)
		}
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.5-R5500")
		// The renamed objects remain observable by their original handles.
		for _, file := range leases {
			if _, err := file.Stat(); err != nil {
				t.Fatalf("lost held FD after rename: %v", err)
			}
		}
	})
	d, err := ResolveNewest(context.Background(), install.env())
	d = requireSuccess(t, d, err)
	if d.Identity.Build != "1.4.5-R5500" || attempts != 2 || rechecks != 2 {
		t.Fatalf("directory replacement: discovery=%+v attempts=%d rechecks=%d", d, attempts, rechecks)
	}
	for _, file := range leases {
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("descriptor escaped the attempt: %v", err)
		}
	}
}

// TestDiscoveryRewriteWithRestoredMtimeRestarts keeps bytes, size and mtime
// equal across the observation interval. ctime must still expose a rewrite.
func TestDiscoveryRewriteWithRestoredMtimeRestarts(t *testing.T) {
	install := validScanWithNewest(t)
	path := filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// A deliberately old mtime makes restoration independent of clock granularity.
	if err := os.Chtimes(path, info.ModTime(), time.Unix(1234567890, 0)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rechecks := 0
	armInterpose(t, func(point interposePoint) {
		if point != interposeBeforeRecheck {
			return
		}
		rechecks++
		if rechecks != 1 {
			return
		}
		if err := os.WriteFile(path, raw, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, info.ModTime(), time.Unix(1234567890, 0)); err != nil {
			t.Fatal(err)
		}
	})
	d := resolveSuccess(t, install.env())
	if rechecks != 2 || d.Identity.SHA256 != digestOfFile(t, path) {
		t.Fatalf("rewrite was not re-observed: rechecks=%d discovery=%+v", rechecks, d)
	}
}

// TestDiscoveryLauncherResolutionRefusal reaches canonical resolution after
// a successful PATH lookup. A preexisting loop would test only StagePath.
func TestDiscoveryLauncherResolutionRefusal(t *testing.T) {
	install := validScanWithNewest(t)
	lookups, validations := 0, 0
	armInterpose(t, func(point interposePoint) {
		if point == interposeAfterValidate {
			validations++
		}
		if point != interposeAfterPath {
			return
		}
		lookups++
		launcher := filepath.Join(install.dir, launcherName)
		if err := os.Remove(launcher); err != nil {
			t.Fatal(err)
		}
	})
	_, err := ResolveNewest(context.Background(), install.env())
	requireCode(t, err, CodeLauncherResolution)
	if lookups != 1 || validations != 0 {
		t.Fatalf("lookups=%d source validations=%d, want 1/0", lookups, validations)
	}
}

// TestDiscoveryPhysicalDotDotInLinkTarget proves relative link traversal
// resolves physical parents before applying .. (the launcher's shell semantics).
func TestDiscoveryPhysicalDotDotInLinkTarget(t *testing.T) {
	install := validScanWithNewest(t)
	inner := filepath.Join(install.dir, "inner")
	if err := os.Mkdir(inner, 0755); err != nil {
		t.Fatal(err)
	}
	outer := newInstall(t)
	if err := os.Symlink(inner, filepath.Join(outer.dir, "indirection")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("indirection/../muse", filepath.Join(outer.dir, "muse")); err != nil {
		t.Fatal(err)
	}
	d := resolveSuccess(t, outer.env())
	if d.InstallDir != install.dir || strings.Contains(d.LauncherPath, "indirection") {
		t.Fatalf("physical resolution = %+v", d)
	}
}

// TestDiscoveryAncestorSiblingCreateDoesNotRestart proves an unrelated entry
// created in an ancestor directory between the snapshot and the recheck is
// not a consistency disagreement: directory stamps are not part of a held
// directory's identity, so discovery accepts on its FIRST attempt with no
// restart. Shared ancestors (the test TMPDIR root under parallel tests,
// $HOME and ~/.local in production) change all the time; comparing their
// stamps refused healthy discoveries with muse_discovery_changed.
func TestDiscoveryAncestorSiblingCreateDoesNotRestart(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	installDir := filepath.Join(root, "install")
	if err := os.Mkdir(installDir, 0o755); err != nil {
		t.Fatal(err)
	}
	install := &installFixture{t: t, dir: installDir}
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	install.writeCandidate("1.4.4-R5419.1")
	attempts, rechecks := 0, 0
	armInterpose(t, func(point interposePoint) {
		if point == interposeAttemptEntry {
			attempts++
		}
		if point != interposeBeforeRecheck {
			return
		}
		rechecks++
		// A file (mtime/ctime/size) and a subdirectory (nlink) land in the
		// ancestor on EVERY attempt, so a directory-stamp comparison
		// refuses outright instead of merely restarting once.
		sibling := filepath.Join(root, fmt.Sprintf("unrelated-%d", rechecks))
		if err := os.WriteFile(sibling, []byte("unrelated\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("unrelated-dir-%d", rechecks)), 0o755); err != nil {
			t.Fatal(err)
		}
	})
	discovery := resolveSuccess(t, install.env())
	if discovery.Identity.Build != "1.4.4-R5419.1" {
		t.Fatalf("selected %s, want the installed maximum", discovery.Identity.Build)
	}
	if attempts != 1 || rechecks != 1 {
		t.Fatalf("attempts=%d rechecks=%d, want 1/1 (no consistency restart)", attempts, rechecks)
	}
}
