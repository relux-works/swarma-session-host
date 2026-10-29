package sessionmanager

const (
	SessionRunning     SessionState = "running"
	SessionRecovering  SessionState = "recovering"
	SessionStopped     SessionState = "stopped"
	SessionQuarantined SessionState = "quarantined"
)

const (
	SessionLivenessLive    SessionLiveness = "live"
	SessionLivenessDead    SessionLiveness = "dead"
	SessionLivenessUnknown SessionLiveness = "unknown"
)

const (
	MaterializationMaterializing MaterializationState = "materializing"
	MaterializationReady         MaterializationState = "ready"
	MaterializationLeased        MaterializationState = "leased"
	MaterializationDetached      MaterializationState = "detached"
	MaterializationOrphaned      MaterializationState = "orphaned"
	MaterializationQuarantined   MaterializationState = "quarantined"
)
