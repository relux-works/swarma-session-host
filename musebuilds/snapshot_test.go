package musebuilds

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestDiscoverySnapshotStable proves the snapshot linearizes the selection:
// repeated discovery over an unchanged install returns the identical
// discovery, and any install change — set, selector or bytes — moves the ID.
func TestDiscoverySnapshotStable(t *testing.T) {
	seed := func(t *testing.T) *installFixture {
		t.Helper()
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.3-R5000")
		install.writeCandidate("1.4.4-R5419.1")
		return install
	}

	t.Run("repeated discovery is identical", func(t *testing.T) {
		install := seed(t)
		first := resolveSuccess(t, install.env())
		second := resolveSuccess(t, install.env())
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("unstable discovery:\nfirst  %+v\nsecond %+v", first, second)
		}
		if len(first.SnapshotID) != 64 {
			t.Fatalf("SnapshotID = %q, want 64 hex digits", first.SnapshotID)
		}
		if _, err := hex.DecodeString(first.SnapshotID); err != nil {
			t.Fatalf("SnapshotID = %q, want lowercase hex: %v", first.SnapshotID, err)
		}
	})

	t.Run("added older build moves the snapshot", func(t *testing.T) {
		install := seed(t)
		before := resolveSuccess(t, install.env())
		install.writeCandidate("1.4.2-R4000")
		after := resolveSuccess(t, install.env())
		if after.SnapshotID == before.SnapshotID {
			t.Fatalf("snapshot %q survived a set change", after.SnapshotID)
		}
		if after.Identity.Build != "1.4.4-R5419.1" {
			t.Fatalf("selected %+v, want the unchanged maximum", after.Identity)
		}
	})

	t.Run("selector change moves the snapshot", func(t *testing.T) {
		install := seed(t)
		before := resolveSuccess(t, install.env())
		install.writeSidecar("1.4.4-R5419.1\n")
		after := resolveSuccess(t, install.env())
		if after.SnapshotID == before.SnapshotID {
			t.Fatalf("snapshot %q survived a selector change", after.SnapshotID)
		}
		if after.Provenance != "" || before.Provenance != ProvenanceSidecarMissing {
			t.Fatalf("provenance moved %q -> %q", before.Provenance, after.Provenance)
		}
	})

	t.Run("source byte change moves snapshot and digest", func(t *testing.T) {
		install := seed(t)
		before := resolveSuccess(t, install.env())
		selected := filepath.Join(install.dir, pinnedNamePrefix+"1.4.4-R5419.1")
		install.writeFileSync(selected, []byte("#!/bin/sh\necho rebuilt\n"), 0o755)
		after := resolveSuccess(t, install.env())
		if after.SnapshotID == before.SnapshotID {
			t.Fatalf("snapshot %q survived a byte change", after.SnapshotID)
		}
		if after.Identity.SHA256 == before.Identity.SHA256 {
			t.Fatalf("digest %q survived a byte change", after.Identity.SHA256)
		}
		if after.Identity.Build != before.Identity.Build {
			t.Fatalf("selection moved %+v -> %+v", before.Identity, after.Identity)
		}
	})

	t.Run("unrelated file leaves the snapshot alone", func(t *testing.T) {
		install := seed(t)
		before := resolveSuccess(t, install.env())
		if err := os.WriteFile(filepath.Join(install.dir, "notes.txt"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		after := resolveSuccess(t, install.env())
		if after.SnapshotID != before.SnapshotID {
			t.Fatalf("snapshot moved %q -> %q on an unrelated file", before.SnapshotID, after.SnapshotID)
		}
	})
}
