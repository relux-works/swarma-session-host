package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestDiscoveryNeverBorrowsAmbientPath proves resolution reads only the
// supplied environment: without a PATH there it refuses, however usable the
// ambient PATH is, and with one there the ambient value is ignored.
func TestDiscoveryNeverBorrowsAmbientPath(t *testing.T) {
	ambient := newInstall(t)
	ambient.writeLauncher(plainScript)
	ambient.writeCandidate("1.4.4-R5419.1")
	ambient.writeSidecar("1.4.4-R5419.1\n")
	t.Setenv("PATH", ambient.dir)

	t.Run("missing PATH refuses despite usable ambient", func(t *testing.T) {
		_, err := ResolveNewest(context.Background(), []string{"HOME=/nowhere"})
		requireCode(t, err, CodeNoPath)
	})

	t.Run("bare PATH key refuses despite usable ambient", func(t *testing.T) {
		_, err := ResolveNewest(context.Background(), []string{"PATH"})
		requireCode(t, err, CodeNoPath)
	})

	t.Run("supplied PATH wins over ambient garbage", func(t *testing.T) {
		own := newInstall(t)
		own.writeLauncher(plainScript)
		own.writeCandidate("1.4.4-R5419.1")
		own.writeSidecar("1.4.4-R5419.1\n")
		discovery := resolveSuccess(t, own.env())
		if discovery.Identity.Build != "1.4.4-R5419.1" || discovery.InstallDir != own.dir {
			t.Fatalf("supplied PATH lost: %+v", discovery)
		}
	})

	t.Run("last PATH wins", func(t *testing.T) {
		own := newInstall(t)
		own.writeLauncher(plainScript)
		own.writeCandidate("1.4.4-R5419.1")
		own.writeSidecar("1.4.4-R5419.1\n")
		env := []string{"PATH=/nonexistent-first", "PATH=" + own.dir}
		discovery := resolveSuccess(t, env)
		if discovery.InstallDir != own.dir {
			t.Fatalf("first PATH won: %+v", discovery)
		}
	})
}

// TestDiscoveryPathErrorsAreDistinct proves missing PATH, not-found and
// unusable launcher candidates refuse with three distinct codes, while an
// unusable entry earlier on PATH never blocks a usable one behind it.
func TestDiscoveryPathErrorsAreDistinct(t *testing.T) {
	t.Run("missing PATH", func(t *testing.T) {
		_, err := ResolveNewest(context.Background(), nil)
		refusal := requireCode(t, err, CodeNoPath)
		if refusal.Stage != StagePath {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StagePath)
		}
	})

	t.Run("not found", func(t *testing.T) {
		empty := newInstall(t)
		_, err := ResolveNewest(context.Background(), empty.env())
		requireCode(t, err, CodeLauncherNotFound)
	})

	t.Run("directory named muse is unusable", func(t *testing.T) {
		busy := newInstall(t)
		if err := os.Mkdir(filepath.Join(busy.dir, launcherName), 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := ResolveNewest(context.Background(), busy.env())
		requireCode(t, err, CodeLauncherUnusable)
	})

	t.Run("non-executable muse is unusable", func(t *testing.T) {
		busy := newInstall(t)
		busy.writeFileSync(filepath.Join(busy.dir, launcherName), []byte(plainScript), 0o644)
		_, err := ResolveNewest(context.Background(), busy.env())
		requireCode(t, err, CodeLauncherUnusable)
	})

	t.Run("symlink loop is unusable", func(t *testing.T) {
		busy := newInstall(t)
		a := filepath.Join(busy.dir, "loop-a")
		b := filepath.Join(busy.dir, "loop-b")
		if err := os.Symlink(b, a); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(a, b); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(a, filepath.Join(busy.dir, launcherName)); err != nil {
			t.Fatal(err)
		}
		_, err := ResolveNewest(context.Background(), busy.env())
		requireCode(t, err, CodeLauncherUnusable)
	})

	t.Run("unusable entry does not block a usable one behind it", func(t *testing.T) {
		busy := newInstall(t)
		busy.writeFileSync(filepath.Join(busy.dir, launcherName), []byte(plainScript), 0o644)
		good := newInstall(t)
		good.writeLauncher(plainScript)
		good.writeCandidate("1.4.4-R5419.1")
		good.writeSidecar("1.4.4-R5419.1\n")
		env := []string{"PATH=" + busy.dir + string(os.PathListSeparator) + good.dir}
		discovery := resolveSuccess(t, env)
		if discovery.InstallDir != good.dir {
			t.Fatalf("resolved %+v, want install %s", discovery, good.dir)
		}
	})
}

// chdirTo moves the test into dir, restoring the previous working directory
// afterwards. Callers never run in parallel with it.
func chdirTo(t *testing.T, dir string) {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})
}

// TestDiscoveryRelativePathEntriesResolveAbsolute proves relative PATH
// entries resolve against the process working directory and still report
// canonical absolute roots: a relative subdirectory, a dot entry and an
// empty element all select with absolute launcher, install and source
// paths.
func TestDiscoveryRelativePathEntriesResolveAbsolute(t *testing.T) {
	seed := func(t *testing.T, dir string) {
		t.Helper()
		install := &installFixture{t: t, dir: dir}
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.3-R5000")
		install.writeCandidate("1.4.4-R5419.1")
		install.writeSidecar("1.4.4-R5419.1\n")
	}
	requireAbsolute := func(t *testing.T, discovery Discovery, wantDir string) {
		t.Helper()
		if discovery.InstallDir != wantDir {
			t.Fatalf("InstallDir = %s, want %s", discovery.InstallDir, wantDir)
		}
		for name, path := range map[string]string{
			"LauncherPath":  discovery.LauncherPath,
			"InstallDir":    discovery.InstallDir,
			"Identity.Path": discovery.Identity.Path,
		} {
			if !filepath.IsAbs(path) {
				t.Fatalf("%s = %s, want an absolute path", name, path)
			}
		}
		if discovery.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v, want the installed maximum", discovery.Identity)
		}
		if discovery.Identity.Path != filepath.Join(wantDir, pinnedNamePrefix+"1.4.4-R5419.1") {
			t.Fatalf("Identity.Path = %s, want the canonical source", discovery.Identity.Path)
		}
		if discovery.LauncherPath != filepath.Join(wantDir, launcherName) {
			t.Fatalf("LauncherPath = %s, want the canonical launcher", discovery.LauncherPath)
		}
	}

	t.Run("relative subdirectory", func(t *testing.T) {
		parent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		sub := filepath.Join(parent, "relkeep")
		if err := os.Mkdir(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		seed(t, sub)
		chdirTo(t, parent)
		discovery := resolveSuccess(t, []string{"PATH=relkeep"})
		requireAbsolute(t, discovery, sub)
	})

	t.Run("dot entry", func(t *testing.T) {
		parent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		seed(t, parent)
		chdirTo(t, parent)
		discovery := resolveSuccess(t, []string{"PATH=."})
		requireAbsolute(t, discovery, parent)
	})

	t.Run("empty element", func(t *testing.T) {
		parent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		seed(t, parent)
		chdirTo(t, parent)
		discovery := resolveSuccess(t, []string{"PATH=" + string(os.PathListSeparator) + filepath.Join(parent, "absent")})
		requireAbsolute(t, discovery, parent)
	})
}
