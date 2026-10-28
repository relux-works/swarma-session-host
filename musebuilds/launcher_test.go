package musebuilds

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLauncherSymlinkInstallDir (M07) proves the install directory is the
// launcher's physical parent: the launcher's own link chain and every
// symlinked parent are followed, and the reported roots are canonical.
func TestLauncherSymlinkInstallDir(t *testing.T) {
	t.Run("direct layout reports its own directory", func(t *testing.T) {
		install := newInstall(t)
		launcher := install.writeLauncher(plainScript)
		install.writeCandidate("1.4.3-R5000")
		install.writeCandidate("1.4.4-R5419.1")
		install.writeSidecar("1.4.3-R5000\n")
		discovery := resolveSuccess(t, install.env())
		if discovery.InstallDir != install.dir {
			t.Fatalf("InstallDir = %s, want %s", discovery.InstallDir, install.dir)
		}
		if discovery.LauncherPath != launcher {
			t.Fatalf("LauncherPath = %s, want %s", discovery.LauncherPath, launcher)
		}
		if discovery.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v, want the installed maximum", discovery.Identity)
		}
		if discovery.Provenance != ProvenanceSidecarBehindInstalled {
			t.Fatalf("Provenance = %q, want %q", discovery.Provenance, ProvenanceSidecarBehindInstalled)
		}
	})

	t.Run("symlinked parent resolves to the physical directory", func(t *testing.T) {
		parent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		real := filepath.Join(parent, "real")
		if err := os.Mkdir(real, 0o755); err != nil {
			t.Fatal(err)
		}
		install := &installFixture{t: t, dir: real}
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.3-R5000")
		install.writeCandidate("1.4.4-R5419.1")
		install.writeSidecar("1.4.3-R5000\n")
		link := filepath.Join(parent, "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		discovery := resolveSuccess(t, []string{"PATH=" + link})
		if discovery.InstallDir != real {
			t.Fatalf("InstallDir = %s, want physical %s", discovery.InstallDir, real)
		}
		if discovery.LauncherPath != filepath.Join(real, launcherName) {
			t.Fatalf("LauncherPath = %s, want the physical launcher", discovery.LauncherPath)
		}
		if discovery.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v, want the installed maximum", discovery.Identity)
		}
	})

	t.Run("symlinked launcher file resolves to its target", func(t *testing.T) {
		install := newInstall(t)
		target := filepath.Join(install.dir, "muse-real")
		install.writeFileSync(target, []byte(plainScript), 0o755)
		if err := os.Symlink(target, filepath.Join(install.dir, launcherName)); err != nil {
			t.Fatal(err)
		}
		install.writeCandidate("1.4.4-R5419.1")
		install.writeSidecar("1.4.4-R5419.1\n")
		discovery := resolveSuccess(t, install.env())
		if discovery.InstallDir != install.dir {
			t.Fatalf("InstallDir = %s, want %s", discovery.InstallDir, install.dir)
		}
		if discovery.LauncherPath != target {
			t.Fatalf("LauncherPath = %s, want %s", discovery.LauncherPath, target)
		}
	})
}
