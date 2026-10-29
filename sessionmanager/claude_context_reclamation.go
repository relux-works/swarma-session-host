package sessionmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	claudeReclamationContract = "task-board-claude-reclamation-v1"
	claudeReclamationDirName  = "claude-reclamation"
)

type claudeReclamationKind string

const (
	claudeReclamationFork  claudeReclamationKind = "context_fork"
	claudeReclamationAssay claudeReclamationKind = "context_assay"
)

type claudeReclamationState string

const (
	// active means at least one artifact is still owned by a live or recoverable
	// context. Retention/GC must join this record with the materialization/session
	// catalogs before reclaiming anything.
	claudeReclamationActive claudeReclamationState = "active"
	// pendingGC means the adapter no longer needs the artifact set. The adapter
	// deliberately leaves the bytes in place; lease-aware reclamation owns deletion.
	claudeReclamationPendingGC claudeReclamationState = "pending_gc"
)

type claudeReclamationArtifact struct {
	Role        string `json:"role"`
	Path        string `json:"path"`
	SessionUUID string `json:"session_uuid,omitempty"`
}

// claudeReclamationEvent is an append-only, manager-private handoff to the
// retention/GC owner. Provider paths and handles never enter the board tree.
//
// Events are append-only rather than rewritten so a crash cannot turn a valid
// active record into a torn state transition. A later pending_gc event for the
// same ArtifactSetID supersedes active for eligibility; missing terminal events
// remain conservatively active until retention policy can reconcile them.
type claudeReclamationEvent struct {
	Contract      string                      `json:"contract"`
	EventID       string                      `json:"event_id"`
	ArtifactSetID string                      `json:"artifact_set_id"`
	Provider      string                      `json:"provider"`
	Kind          claudeReclamationKind       `json:"kind"`
	State         claudeReclamationState      `json:"state"`
	Reason        string                      `json:"reason"`
	BaseCtxID     string                      `json:"base_ctx_id,omitempty"`
	ChildCtxID    string                      `json:"child_ctx_id,omitempty"`
	WorkingDir    string                      `json:"working_dir,omitempty"`
	ProjectDir    string                      `json:"project_dir,omitempty"`
	Artifacts     []claudeReclamationArtifact `json:"artifacts,omitempty"`
	RecordedAt    time.Time                   `json:"recorded_at"`
}

func recordClaudeReclamationEvent(
	layout Layout,
	event claudeReclamationEvent,
	now func() time.Time,
) error {
	if now == nil {
		now = time.Now
	}
	if strings.TrimSpace(event.ArtifactSetID) == "" {
		return fmt.Errorf("Claude reclamation artifact-set identity is required")
	}
	if event.Kind != claudeReclamationFork && event.Kind != claudeReclamationAssay {
		return fmt.Errorf("unsupported Claude reclamation kind %q", event.Kind)
	}
	if event.State != claudeReclamationActive && event.State != claudeReclamationPendingGC {
		return fmt.Errorf("unsupported Claude reclamation state %q", event.State)
	}
	eventID, err := newClaudeSessionUUID()
	if err != nil {
		return err
	}
	event.Contract = claudeReclamationContract
	event.EventID = eventID
	event.Provider = "claude"
	event.RecordedAt = now().UTC()
	event.Artifacts = append([]claudeReclamationArtifact(nil), event.Artifacts...)

	root := filepath.Join(layout.StateDir, claudeReclamationDirName)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("creating Claude reclamation catalog: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return fmt.Errorf("securing Claude reclamation catalog: %w", err)
	}
	data, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding Claude reclamation event: %w", err)
	}
	path := filepath.Join(root, event.ArtifactSetID+"-"+eventID+".json")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("creating Claude reclamation event: %w", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		// Deliberately no cleanup: even a partial private record is evidence for
		// the retention owner to quarantine; this adapter deletes no path.
		return fmt.Errorf("writing Claude reclamation event: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("syncing Claude reclamation event: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing Claude reclamation event: %w", err)
	}
	return syncDirectory(root)
}

func loadClaudeReclamationEvents(layout Layout, artifactSetID string) ([]claudeReclamationEvent, error) {
	root := filepath.Join(layout.StateDir, claudeReclamationDirName)
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading Claude reclamation catalog: %w", err)
	}
	prefix := artifactSetID + "-"
	var events []claudeReclamationEvent
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") ||
			(artifactSetID != "" && !strings.HasPrefix(entry.Name(), prefix)) {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(root, entry.Name()))
		if readErr != nil {
			return nil, fmt.Errorf("reading Claude reclamation event %s: %w", entry.Name(), readErr)
		}
		var event claudeReclamationEvent
		if decodeErr := json.Unmarshal(data, &event); decodeErr != nil {
			return nil, fmt.Errorf("decoding Claude reclamation event %s: %w", entry.Name(), decodeErr)
		}
		if event.Contract != claudeReclamationContract ||
			strings.TrimSpace(event.ArtifactSetID) == "" ||
			(artifactSetID != "" && event.ArtifactSetID != artifactSetID) ||
			event.Provider != "claude" {
			return nil, fmt.Errorf("invalid Claude reclamation event %s", entry.Name())
		}
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].RecordedAt.Equal(events[j].RecordedAt) {
			return events[i].EventID < events[j].EventID
		}
		return events[i].RecordedAt.Before(events[j].RecordedAt)
	})
	return events, nil
}
