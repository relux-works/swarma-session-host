package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestUnrecognizedVersionedNameRefuses proves a name in the versioned
// namespace that the shared grammar does not parse makes the installed set
// unknown and refuses, while the exact legacy bare name and unrelated files
// stay ordinary non-candidates.
func TestUnrecognizedVersionedNameRefuses(t *testing.T) {
	t.Run("junk versioned name refuses", func(t *testing.T) {
		install := validScanWithNewest(t)
		install.writeSidecar("1.4.4-R5419.1\n")
		install.writeFileSync(filepath.Join(install.dir, pinnedNamePrefix+"tomorrow"), []byte("bytes"), 0o755)
		_, err := ResolveNewest(context.Background(), install.env())
		refusal := requireCode(t, err, CodeInstallNameUnrecognized)
		if refusal.Stage != StageScan {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StageScan)
		}
	})

	t.Run("dotless revision still parses, so only true junk refuses", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.5-R5500")
		install.writeFileSync(filepath.Join(install.dir, pinnedNamePrefix+"1.4.5"), []byte("bytes"), 0o755)
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeInstallNameUnrecognized)
	})

	t.Run("legacy bare name is ignored", func(t *testing.T) {
		install := validScanWithNewest(t)
		install.writeSidecar("1.4.4-R5419.1\n")
		install.writeFileSync(filepath.Join(install.dir, legacyBareBinaryName), []byte("bytes"), 0o755)
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v, want the installed maximum", discovery.Identity)
		}
	})

	t.Run("launcher and support files are ignored", func(t *testing.T) {
		install := validScanWithNewest(t)
		install.writeSidecar("1.4.4-R5419.1\n")
		install.writeFileSync(filepath.Join(install.dir, ".muse-release-info.json"), []byte("{}"), 0o644)
		install.writeFileSync(filepath.Join(install.dir, "muse-update-log"), []byte("log"), 0o644)
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v, want the installed maximum", discovery.Identity)
		}
	})
}

// TestInstallScanReadRefusal proves a searchable but unreadable install
// directory refuses at the scan: launcher and sidecar lookup succeed
// through search permission, while directory enumeration fails. The refusal
// is CodeInstallScanFailed, never a sidecar error and never an empty
// install.
func TestInstallScanReadRefusal(t *testing.T) {
	skipIfRoot(t)
	install := newInstall(t)
	install.writeLauncher(plainScript)
	install.writeCandidate("1.4.3-R5000")
	install.writeCandidate("1.4.4-R5419.1")
	install.writeSidecar("1.4.4-R5419.1\n")
	if err := os.Chmod(install.dir, 0o111); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chmod(install.dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}()
	_, err := ResolveNewest(context.Background(), install.env())
	refusal := requireCode(t, err, CodeInstallScanFailed)
	if refusal.Stage != StageScan {
		t.Fatalf("stage = %s, want %s", refusal.Stage, StageScan)
	}
}
