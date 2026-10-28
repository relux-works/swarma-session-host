package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// validScanWithNewest arms an install whose scan succeeds on its own: two
// usable builds with the newest selectable. Sidecar-failure cells reuse it
// to prove the failure refuses instead of falling back to this scan.
func validScanWithNewest(t *testing.T) *installFixture {
	t.Helper()
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	install.writeCandidate("1.4.4-R5419.1")
	return install
}

// TestCorruptSidecarDoesNotBecomeAbsence (M08) proves a malformed,
// unreadable or over-bound sidecar refuses with its own code: corruption is
// never absence, and never permission to scan instead.
func TestCorruptSidecarDoesNotBecomeAbsence(t *testing.T) {
	t.Run("malformed sidecar refuses", func(t *testing.T) {
		install := validScanWithNewest(t)
		install.writeSidecar("not-a-build\n")
		_, err := ResolveNewest(context.Background(), install.env())
		refusal := requireCode(t, err, CodeSidecarMalformed)
		if refusal.Stage != StageSidecar {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StageSidecar)
		}
	})

	t.Run("unreadable sidecar refuses", func(t *testing.T) {
		skipIfRoot(t)
		install := validScanWithNewest(t)
		install.writeSidecar("1.4.4-R5419.1\n")
		if err := os.Chmod(filepath.Join(install.dir, sidecarName), 0); err != nil {
			t.Fatal(err)
		}
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSidecarUnreadable)
	})

	t.Run("over-bound sidecar refuses", func(t *testing.T) {
		install := validScanWithNewest(t)
		install.writeSidecar(strings.Repeat("x", sidecarMaxBytes+1))
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSidecarUnreadable)
	})

	t.Run("unsearchable install directory refuses at PATH lookup", func(t *testing.T) {
		skipIfRoot(t)
		install := validScanWithNewest(t)
		install.writeSidecar("1.4.4-R5419.1\n")
		if err := os.Chmod(install.dir, 0); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.Chmod(install.dir, 0o755); err != nil {
				t.Fatal(err)
			}
		}()
		_, err := ResolveNewest(context.Background(), install.env())
		refusal := requireCode(t, err, CodeLauncherUnusable)
		if refusal.Stage != StagePath {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StagePath)
		}
	})

	t.Run("valid sidecar succeeds", func(t *testing.T) {
		install := validScanWithNewest(t)
		install.writeSidecar("1.4.4-R5419.1\n")
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != "1.4.4-R5419.1" || discovery.Provenance != "" {
			t.Fatalf("unexpected selection %+v", discovery)
		}
	})

	t.Run("missing sidecar scans", func(t *testing.T) {
		install := validScanWithNewest(t)
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v, want the installed maximum", discovery.Identity)
		}
		if discovery.Provenance != ProvenanceSidecarMissing {
			t.Fatalf("Provenance = %q, want %q", discovery.Provenance, ProvenanceSidecarMissing)
		}
		if discovery.SidecarTarget != "" {
			t.Fatalf("SidecarTarget = %q, want empty", discovery.SidecarTarget)
		}
	})
}

// TestSidecarGrammarCells proves the sidecar accepts exactly one bare build
// with trailing LFs: leading whitespace, CR bytes, trailing spaces and
// embedded newlines refuse, while stacked trailing LFs read like the shell
// substitution.
func TestSidecarGrammarCells(t *testing.T) {
	cells := []struct {
		name    string
		sidecar string
		wantErr bool
	}{
		{"trailing LF", "1.4.4-R5419.1\n", false},
		{"stacked trailing LFs", "1.4.4-R5419.1\n\n\n", false},
		{"no trailing LF", "1.4.4-R5419.1", false},
		{"CR before LF", "1.4.4-R5419.1\r\n", true},
		{"leading spaces", "  1.4.4-R5419.1\n", true},
		{"trailing space", "1.4.4-R5419.1 \n", true},
		{"embedded newline", "1.4.4-R5419.1\n1.4.3-R5000\n", true},
		{"empty", "", true},
		{"only newlines", "\n\n", true},
		{"bare triple", "1.4.4\n", true},
		{"answer shape", "Muse Code 1.4.4 (1.4.4-R5419.1)\n", true},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			install := newInstall(t)
			install.writeLauncher(plainScript)
			install.writeCandidate("1.4.3-R5000")
			install.writeCandidate("1.4.4-R5419.1")
			install.writeSidecar(cell.sidecar)
			discovery, err := ResolveNewest(context.Background(), install.env())
			if cell.wantErr {
				requireCode(t, err, CodeSidecarMalformed)
				return
			}
			discovery = requireSuccess(t, discovery, err)
			if discovery.Identity.Build != "1.4.4-R5419.1" || discovery.Provenance != "" {
				t.Fatalf("unexpected selection %+v", discovery)
			}
			if discovery.SidecarTarget != "1.4.4-R5419.1" {
				t.Fatalf("SidecarTarget = %q", discovery.SidecarTarget)
			}
		})
	}
}

// TestDanglingSidecarTargetRefuses proves a sidecar whose named install
// target is missing or fails the static gates refuses: a rejected target is
// never a reason to look for another build.
func TestDanglingSidecarTargetRefuses(t *testing.T) {
	t.Run("missing target refuses", func(t *testing.T) {
		install := validScanWithNewest(t)
		install.writeSidecar("1.4.5-R5500\n")
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSidecarTargetRejected)
	})

	t.Run("symlink target refuses", func(t *testing.T) {
		install := validScanWithNewest(t)
		target := filepath.Join(install.dir, pinnedNamePrefix+"1.4.5-R5500")
		if err := os.Symlink(filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1"), target); err != nil {
			t.Fatal(err)
		}
		install.writeSidecar("1.4.5-R5500\n")
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSidecarTargetRejected)
	})

	t.Run("directory target refuses", func(t *testing.T) {
		install := validScanWithNewest(t)
		if err := os.Mkdir(filepath.Join(install.dir, pinnedNamePrefix+"1.4.5-R5500"), 0o755); err != nil {
			t.Fatal(err)
		}
		install.writeSidecar("1.4.5-R5500\n")
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSidecarTargetRejected)
	})

	t.Run("non-executable target refuses", func(t *testing.T) {
		install := validScanWithNewest(t)
		install.writeFileSync(filepath.Join(install.dir, pinnedNamePrefix+"1.4.5-R5500"), []byte("bytes"), 0o644)
		install.writeSidecar("1.4.5-R5500\n")
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSidecarTargetRejected)
	})

	t.Run("older valid target selects the newer scan maximum", func(t *testing.T) {
		install := validScanWithNewest(t)
		install.writeSidecar("1.4.3-R5000\n")
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v, want the installed maximum", discovery.Identity)
		}
		if discovery.Provenance != ProvenanceSidecarBehindInstalled {
			t.Fatalf("Provenance = %q, want %q", discovery.Provenance, ProvenanceSidecarBehindInstalled)
		}
		if discovery.SidecarTarget != "1.4.3-R5000" {
			t.Fatalf("SidecarTarget = %q", discovery.SidecarTarget)
		}
	})
}

// TestSymlinkedSidecarRefusesWithoutFollowing proves a sidecar that is a
// symlink refuses as unreadable even when its target names a valid build:
// discovery never follows the selector, and a dangling selector is
// unreadable rather than absent.
func TestSymlinkedSidecarRefusesWithoutFollowing(t *testing.T) {
	t.Run("symlink to a valid build refuses", func(t *testing.T) {
		install := validScanWithNewest(t)
		target := filepath.Join(install.dir, "version-target")
		install.writeFileSync(target, []byte("1.4.4-R5419.1\n"), 0o644)
		if err := os.Symlink(target, filepath.Join(install.dir, sidecarName)); err != nil {
			t.Fatal(err)
		}
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSidecarUnreadable)
	})

	t.Run("dangling symlink refuses instead of scanning", func(t *testing.T) {
		install := validScanWithNewest(t)
		if err := os.Symlink(filepath.Join(install.dir, "no-such-target"), filepath.Join(install.dir, sidecarName)); err != nil {
			t.Fatal(err)
		}
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSidecarUnreadable)
	})
}
