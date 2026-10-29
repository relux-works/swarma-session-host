package sessionmanager

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClaudeAssayRetirementRetainsBytesAndRecordsPendingGC(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	workingDir := filepath.Join(fixture.layout.StateDir, claudeAssayDirName, "assay-retained")
	projectDir := filepath.Join(fixture.home, "projects", "-assay-retained")
	if err := os.MkdirAll(workingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(projectDir, 0o700); err != nil {
		t.Fatal(err)
	}
	materialized := filepath.Join(projectDir, "11111111-1111-4111-8111-111111111111.jsonl")
	fork := filepath.Join(projectDir, "22222222-2222-4222-8222-222222222222.jsonl")
	for _, path := range []string{materialized, fork} {
		if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	artifacts := &claudeAssayArtifacts{
		Layout:                  fixture.layout,
		ArtifactSetID:           "33333333-3333-4333-8333-333333333333",
		BaseCtxID:               "CTX-260730-c00001",
		WorkingDir:              workingDir,
		SlugDir:                 projectDir,
		SlugDirOwned:            true,
		MaterializedPath:        materialized,
		MaterializedSessionUUID: "11111111-1111-4111-8111-111111111111",
		ForkPath:                fork,
		ForkSessionUUID:         "22222222-2222-4222-8222-222222222222",
	}
	if err := retireClaudeAssayArtifacts(artifacts); err != nil {
		t.Fatal(err)
	}
	for _, retained := range []string{workingDir, projectDir, materialized, fork} {
		if _, err := os.Stat(retained); err != nil {
			t.Fatalf("adapter deleted retained artifact %s: %v", retained, err)
		}
	}
	events, err := loadClaudeReclamationEvents(fixture.layout, artifacts.ArtifactSetID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].State != claudeReclamationPendingGC {
		t.Fatalf("reclamation events = %+v", events)
	}
	if events[0].WorkingDir != workingDir || events[0].ProjectDir != projectDir ||
		len(events[0].Artifacts) != 2 {
		t.Fatalf("reclamation event lost exact private paths: %+v", events[0])
	}
}

func TestClaudeReclamationCatalogIsPrivateAppendOnlyAndNotAMaterialization(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	artifactSetID := "44444444-4444-4444-8444-444444444444"
	at := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	for index, state := range []claudeReclamationState{
		claudeReclamationActive,
		claudeReclamationPendingGC,
	} {
		if err := recordClaudeReclamationEvent(fixture.layout, claudeReclamationEvent{
			ArtifactSetID: artifactSetID,
			Kind:          claudeReclamationFork,
			State:         state,
			Reason:        string(state),
			BaseCtxID:     "CTX-260730-c00001",
			ChildCtxID:    "CTX-260730-c00002",
			ProjectDir:    filepath.Join(fixture.home, "projects", "-retained"),
			Artifacts: []claudeReclamationArtifact{{
				Role:        "writable_fork",
				Path:        filepath.Join(fixture.home, "projects", "-retained", "fork.jsonl"),
				SessionUUID: artifactSetID,
			}},
		}, func() time.Time { return at.Add(time.Duration(index) * time.Second) }); err != nil {
			t.Fatal(err)
		}
	}
	events, err := loadClaudeReclamationEvents(fixture.layout, artifactSetID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].State != claudeReclamationActive ||
		events[1].State != claudeReclamationPendingGC {
		t.Fatalf("append-only lifecycle = %+v", events)
	}
	root := filepath.Join(fixture.layout.StateDir, claudeReclamationDirName)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("catalog event count = %d, want 2", len(entries))
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("reclamation event %s mode = %o", entry.Name(), info.Mode().Perm())
		}
	}
	if _, err := os.Stat(filepath.Join(
		fixture.layout.MaterializationsDir, "CTX-260730-c00002", "record.json", // Original catalog filename, pinned independently of its private store.
	)); !os.IsNotExist(err) {
		t.Fatalf("reclamation event became a selectable materialization: %v", err)
	}
}
