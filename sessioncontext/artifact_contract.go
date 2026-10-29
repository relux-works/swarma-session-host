package sessioncontext

import "time"

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
	// deliberately leaves the bytes in place for the retention owner to reclaim.
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
