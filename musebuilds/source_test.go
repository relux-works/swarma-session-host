package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	muse "github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

// TestSelectedNewestUnusableRefuses (M01/M02 selection half) proves a newest
// that fails the source gates refuses instead of stepping down to an older
// usable build, in every sidecar provenance. When the sidecar itself names
// the unusable newest, its own target gate owns the refusal.
func TestSelectedNewestUnusableRefuses(t *testing.T) {
	const newest, older = "1.4.4-R5419.1", "1.4.3-R5000"

	plantDirectory := func(t *testing.T, install *installFixture) {
		t.Helper()
		if err := os.Mkdir(filepath.Join(install.dir, pinnedNamePrefix+newest), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	plantSymlink := func(t *testing.T, install *installFixture) {
		t.Helper()
		usable := filepath.Join(install.dir, pinnedNamePrefix+older)
		if err := os.Symlink(usable, filepath.Join(install.dir, pinnedNamePrefix+newest)); err != nil {
			t.Fatal(err)
		}
	}
	plantNonexec := func(t *testing.T, install *installFixture) {
		t.Helper()
		install.writeFileSync(filepath.Join(install.dir, pinnedNamePrefix+newest), []byte("bytes"), 0o644)
	}
	plantHardlink := func(t *testing.T, install *installFixture) {
		t.Helper()
		original := install.writeCandidate(newest)
		if err := os.Link(original, filepath.Join(install.dir, "spare-link")); err != nil {
			t.Fatal(err)
		}
	}
	shapes := []struct {
		name  string
		plant func(t *testing.T, install *installFixture)
		// sidecarNamesNewest reports whether the sidecar target gate owns
		// the refusal when the sidecar names the unusable newest: the
		// target gate checks all static source gates, including link count.
		sidecarOwns bool
	}{
		{"directory", plantDirectory, true},
		{"symlink", plantSymlink, true},
		{"non-executable", plantNonexec, true},
		{"hardlink", plantHardlink, true},
	}

	t.Run("sidecar names the unusable newest", func(t *testing.T) {
		for _, shape := range shapes {
			t.Run(shape.name, func(t *testing.T) {
				install := newInstall(t)
				install.writeLauncher(plainScript)
				install.writeCandidate(older)
				shape.plant(t, install)
				install.writeSidecar(newest + "\n")
				_, err := ResolveNewest(context.Background(), install.env())
				if shape.sidecarOwns {
					requireCode(t, err, CodeSidecarTargetRejected)
				} else {
					requireCode(t, err, CodeSelectionUnusable)
				}
			})
		}
	})

	t.Run("older sidecar with unusable scan maximum", func(t *testing.T) {
		for _, shape := range shapes {
			t.Run(shape.name, func(t *testing.T) {
				install := newInstall(t)
				install.writeLauncher(plainScript)
				install.writeCandidate(older)
				shape.plant(t, install)
				install.writeSidecar(older + "\n")
				_, err := ResolveNewest(context.Background(), install.env())
				requireCode(t, err, CodeSelectionUnusable)
			})
		}
	})

	t.Run("missing sidecar with unusable scan maximum", func(t *testing.T) {
		for _, shape := range shapes {
			t.Run(shape.name, func(t *testing.T) {
				install := newInstall(t)
				install.writeLauncher(plainScript)
				install.writeCandidate(older)
				shape.plant(t, install)
				_, err := ResolveNewest(context.Background(), install.env())
				refusal := requireCode(t, err, CodeSelectionUnusable)
				if refusal.Stage != StageSource {
					t.Fatalf("stage = %s, want %s", refusal.Stage, StageSource)
				}
			})
		}
	})

	t.Run("single unusable newest refuses", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		plantDirectory(t, install)
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSelectionUnusable)
	})

	t.Run("usable set keeps succeeding", func(t *testing.T) {
		discovery := selectBuilds(t, []string{older, newest}, newest)
		if discovery.SidecarTarget != "" {
			t.Fatalf("SidecarTarget = %q, want empty", discovery.SidecarTarget)
		}
	})
}

// TestSymlinkedSourceRefuses proves a selected newest that is a symlink
// refuses at the pinned open: discovery hashes the named file itself, never
// whatever a link points at.
func TestSymlinkedSourceRefuses(t *testing.T) {
	t.Run("symlink newest refuses", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		usable := install.writeCandidate("1.4.3-R5000")
		if err := os.Symlink(usable, filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1")); err != nil {
			t.Fatal(err)
		}
		_, err := ResolveNewest(context.Background(), install.env())
		refusal := requireCode(t, err, CodeSelectionUnusable)
		if refusal.Stage != StageSource {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StageSource)
		}
	})

	t.Run("hardlink newest still refuses", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.3-R5000")
		original := install.writeCandidate("1.4.4-R5419.1")
		if err := os.Link(original, filepath.Join(install.dir, "spare-link")); err != nil {
			t.Fatal(err)
		}
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSelectionUnusable)
	})

	t.Run("regular newest keeps succeeding", func(t *testing.T) {
		selectBuilds(t, []string{"1.4.3-R5000", "1.4.4-R5419.1"}, "1.4.4-R5419.1")
	})
}

// TestHardlinkedSourceRefuses proves a doubly-linked newest refuses: a hard
// link planted on the selected name reaches bytes elsewhere while passing
// every symlink check.
func TestHardlinkedSourceRefuses(t *testing.T) {
	linkNewest := func(t *testing.T, spares int) *installFixture {
		t.Helper()
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.3-R5000")
		original := install.writeCandidate("1.4.4-R5419.1")
		for i := 0; i < spares; i++ {
			spare := filepath.Join(install.dir, "spare-link")
			if i > 0 {
				spare = filepath.Join(install.dir, "spare-link-extra")
			}
			if err := os.Link(original, spare); err != nil {
				t.Fatal(err)
			}
		}
		return install
	}

	t.Run("doubly-linked newest refuses", func(t *testing.T) {
		install := linkNewest(t, 1)
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSelectionUnusable)
	})

	t.Run("triply-linked newest refuses", func(t *testing.T) {
		install := linkNewest(t, 2)
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSelectionUnusable)
	})

	t.Run("singly-linked newest keeps succeeding", func(t *testing.T) {
		selectBuilds(t, []string{"1.4.3-R5000", "1.4.4-R5419.1"}, "1.4.4-R5419.1")
	})
}

// TestNonexecutableSourceRefuses proves a selected newest without execute
// permission refuses even when its bytes read fine.
func TestNonexecutableSourceRefuses(t *testing.T) {
	t.Run("readable but non-executable newest refuses", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.3-R5000")
		install.writeFileSync(filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1"), []byte("readable bytes"), 0o644)
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSelectionUnusable)
	})

	t.Run("owner-only newest refuses", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.3-R5000")
		install.writeFileSync(filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1"), []byte("readable bytes"), 0o600)
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSelectionUnusable)
	})

	t.Run("executable newest keeps succeeding", func(t *testing.T) {
		selectBuilds(t, []string{"1.4.3-R5000", "1.4.4-R5419.1"}, "1.4.4-R5419.1")
	})
}

// TestEmptyInstallDirRefuses proves an install with a launcher but no
// versioned candidates refuses instead of succeeding with a zero selection.
func TestEmptyInstallDirRefuses(t *testing.T) {
	t.Run("no candidates refuses", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		_, err := ResolveNewest(context.Background(), install.env())
		refusal := requireCode(t, err, CodeNoInstalledBuild)
		if refusal.Stage != StageScan {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StageScan)
		}
	})

	t.Run("sidecar without candidates refuses at the sidecar", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeSidecar("1.4.4-R5419.1\n")
		_, err := ResolveNewest(context.Background(), install.env())
		requireCode(t, err, CodeSidecarTargetRejected)
	})

	t.Run("single candidate succeeds", func(t *testing.T) {
		selectBuilds(t, []string{"1.4.4-R5419.1"}, "1.4.4-R5419.1")
	})
}

// TestSelectedSourceNameMustMatchIdentity proves the source gate binds the
// file name to the identity it validates: a matching pair passes, and any
// mismatch refuses before a byte is read.
func TestSelectedSourceNameMustMatchIdentity(t *testing.T) {
	// The public parser cannot manufacture a name/ID mismatch. Exercise the
	// defensive invariant directly, without claiming another public AC row.
	open := func(t *testing.T, path string, id muse.BuildIdentity) (*pinnedSource, error) {
		t.Helper()
		s := &discoverySnapshot{dirs: make(map[string]*fileLease)}
		t.Cleanup(func() { _ = s.close() })
		dir, _, _, err := s.lookup(context.Background(), path, StageSource)
		if err != nil {
			t.Fatal(err)
		}
		return openValidateAndHashSelectedSource(context.Background(), s, dir, path, id)
	}
	parse := func(t *testing.T, base string) muse.BuildIdentity {
		t.Helper()
		id, err := muse.ParsePinnedBinaryName(base)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	t.Run("matching pair passes", func(t *testing.T) {
		install := newInstall(t)
		path := install.writeCandidate("1.4.4-R5419.1")
		pinned, err := open(t, path, parse(t, pinnedNamePrefix+"1.4.4-R5419.1"))
		if err != nil {
			t.Fatalf("matching pair refused: %v", err)
		}
		if len(pinned.digest) != 64 {
			t.Fatalf("digest = %q, want 64 hex digits", pinned.digest)
		}
	})

	t.Run("mismatched build refuses", func(t *testing.T) {
		install := newInstall(t)
		path := install.writeCandidate("1.4.4-R5419.1")
		_, err := open(t, path, parse(t, pinnedNamePrefix+"9.9.9-R9999"))
		refusal := requireCode(t, err, CodeSelectionUnusable)
		if refusal.Stage != StageSource {
			t.Fatalf("stage = %s, want %s", refusal.Stage, StageSource)
		}
	})

	t.Run("other mismatch refuses", func(t *testing.T) {
		install := newInstall(t)
		path := install.writeCandidate("1.4.4-R5419.1")
		_, err := open(t, path, parse(t, pinnedNamePrefix+"1.4.3-R5000"))
		requireCode(t, err, CodeSelectionUnusable)
	})
}
