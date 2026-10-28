package musebuilds

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// selectBuilds runs one ordering cell: an install holding exactly builds
// with no sidecar, asserting the numerical maximum wins with its full frozen
// identity (exact build text, canonical path, content digest) and missing
// sidecar provenance.
func selectBuilds(t *testing.T, builds []string, want string) Discovery {
	t.Helper()
	install := newInstall(t)
	install.writeLauncher(plainScript)
	for _, build := range builds {
		install.writeCandidate(build)
	}
	discovery := resolveSuccess(t, install.env())
	if discovery.Identity.Build != want {
		t.Fatalf("selected %s, want %s (set %v)", discovery.Identity.Build, want, builds)
	}
	wantPath := filepath.Join(install.dir, pinnedNamePrefix+want)
	if discovery.Identity.Path != wantPath {
		t.Fatalf("Path = %s, want %s", discovery.Identity.Path, wantPath)
	}
	raw, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if discovery.Identity.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 = %s, want the content digest", discovery.Identity.SHA256)
	}
	if discovery.Provenance != ProvenanceSidecarMissing {
		t.Fatalf("Provenance = %q, want %q", discovery.Provenance, ProvenanceSidecarMissing)
	}
	return discovery
}

// TestNewestNumericOrderingAllComponents (M09) proves selection is the
// numerical maximum on every component: each width cell defeats lexical
// order, while same-width cells prove the gate still orders where the two
// orders agree.
func TestNewestNumericOrderingAllComponents(t *testing.T) {
	cells := []struct {
		name   string
		builds []string
		want   string
	}{
		{"patch width", []string{"1.4.9-R100", "1.4.10-R100"}, "1.4.10-R100"},
		{"minor width", []string{"1.9.0-R100", "1.10.0-R100"}, "1.10.0-R100"},
		{"major width", []string{"9.0.0-R100", "10.0.0-R100"}, "10.0.0-R100"},
		{"revision width", []string{"1.4.4-R999", "1.4.4-R1000"}, "1.4.4-R1000"},
		{"subrevision width", []string{"1.4.4-R100.9", "1.4.4-R100.10"}, "1.4.4-R100.10"},
		{"absent subrevision sorts before .0", []string{"1.4.4-R100", "1.4.4-R100.0"}, "1.4.4-R100.0"},
		{"absent subrevision sorts before .1", []string{"1.4.4-R100", "1.4.4-R100.1"}, "1.4.4-R100.1"},
		{"undotted revision loses to dotted triple", []string{"1.4.4-R9999", "1.4.5-R1"}, "1.4.5-R1"},
		{"same-width revision orders", []string{"1.4.4-R100", "1.4.4-R101"}, "1.4.4-R101"},
		{"same-width triple orders", []string{"1.4.3-R5000", "1.4.4-R5419.1"}, "1.4.4-R5419.1"},
		{"three-way maximum", []string{"1.4.3-R5000", "1.4.10-R100", "1.4.4-R5419.1"}, "1.4.10-R100"},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			selectBuilds(t, cell.builds, cell.want)
		})
	}
}

// TestLauncherValidBuildWithoutTrailingRevisionEndToEnd (M10) proves a
// launcher-valid build without a dotted subrevision resolves end to end:
// through the sidecar selector, through the install scan, and through both
// together.
func TestLauncherValidBuildWithoutTrailingRevisionEndToEnd(t *testing.T) {
	t.Run("sidecar and scan agree on the undotted build", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.5-R5500")
		install.writeSidecar("1.4.5-R5500\n")
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != "1.4.5-R5500" || discovery.Provenance != "" {
			t.Fatalf("unexpected selection %+v", discovery)
		}
	})

	t.Run("scan alone resolves the undotted build", func(t *testing.T) {
		discovery := selectBuilds(t, []string{"1.4.5-R5500"}, "1.4.5-R5500")
		if discovery.SidecarTarget != "" {
			t.Fatalf("SidecarTarget = %q, want empty", discovery.SidecarTarget)
		}
	})

	t.Run("dotted builds keep resolving", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.4-R5419.1")
		install.writeSidecar("1.4.4-R5419.1\n")
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != "1.4.4-R5419.1" || discovery.Provenance != "" {
			t.Fatalf("unexpected selection %+v", discovery)
		}
	})

	t.Run("undotted scan maximum beats an older dotted sidecar", func(t *testing.T) {
		install := newInstall(t)
		install.writeLauncher(plainScript)
		install.writeCandidate("1.4.4-R5419.1")
		install.writeCandidate("1.4.5-R5500")
		install.writeSidecar("1.4.4-R5419.1\n")
		discovery := resolveSuccess(t, install.env())
		if discovery.Identity.Build != "1.4.5-R5500" {
			t.Fatalf("selected %+v, want the undotted maximum", discovery.Identity)
		}
		if discovery.Provenance != ProvenanceSidecarBehindInstalled {
			t.Fatalf("Provenance = %q, want %q", discovery.Provenance, ProvenanceSidecarBehindInstalled)
		}
	})
}

// TestAmbiguousNumericIdentityRefuses (M11) proves numerically equal raw
// identities with different text refuse instead of letting one win
// arbitrarily, even when a clear higher build exists beside them.
func TestAmbiguousNumericIdentityRefuses(t *testing.T) {
	aliasSets := [][]string{
		{"1.4.2-R100", "01.4.2-R100"},
		{"1.4.2-R100", "1.4.2-R0100"},
		{"1.4.2-R100.1", "1.4.2-R100.01"},
		{"1.4.2-R100", "01.4.2-R100", "1.4.3-R1"},
	}
	for _, set := range aliasSets {
		t.Run("aliases refuse: "+set[0]+" vs "+set[1], func(t *testing.T) {
			install := newInstall(t)
			install.writeLauncher(plainScript)
			for _, build := range set {
				install.writeCandidate(build)
			}
			_, err := ResolveNewest(context.Background(), install.env())
			refusal := requireCode(t, err, CodeSelectionAmbiguous)
			if refusal.Stage != StageSelect {
				t.Fatalf("stage = %s, want %s", refusal.Stage, StageSelect)
			}
		})
	}

	t.Run("distinct builds keep ordering", func(t *testing.T) {
		selectBuilds(t, []string{"1.4.2-R100", "1.4.2-R101"}, "1.4.2-R101")
	})
}
