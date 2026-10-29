package sessionmanager

import (
	"encoding/json"
	"time"
)

// These are codec-local legacy JSON declarations, in original field order.
// They never cross the neutral host port. Conversion below is field-by-field;
// JSON is used only at the explicitly accepted legacy persistence boundary.
type legacyRunnerRecord struct {
	ConditionalStop         *legacyConditionalStop `json:"conditional_stop,omitempty"`
	GracefulRetirement      bool                   `json:"graceful_retirement,omitempty"`
	Contract                string                 `json:"contract"`
	SessionID               string                 `json:"session_id"`
	Owner                   legacySessionOwner     `json:"owner"`
	Provider                string                 `json:"provider"`
	ProjectDir              string                 `json:"project_dir,omitempty"`
	BoardFingerprint        string                 `json:"board_fingerprint,omitempty"`
	State                   SessionState           `json:"state"`
	Liveness                SessionLiveness        `json:"liveness,omitempty"`
	LivenessDetail          string                 `json:"liveness_detail,omitempty"`
	OwnerRunError           string                 `json:"owner_run_error,omitempty"`
	Eviction                *SessionEvictionNotice `json:"eviction,omitempty"`
	ProviderHost            legacyProviderImage    `json:"provider_host"`
	MaterializationCtxID    string                 `json:"materialization_ctx_id,omitempty"`
	ProviderHostPendingFork bool                   `json:"provider_host_pending_fork,omitempty"`
	AppliedGoalRef          *legacyGoalRef         `json:"applied_goal_ref,omitempty"`
	CreatedAt               time.Time              `json:"created_at"`
	UpdatedAt               time.Time              `json:"updated_at"`
}
type legacyConditionalStop struct {
	Precondition string          `json:"precondition"`
	Phase        string          `json:"phase,omitempty"`
	Signalled    bool            `json:"signalled"`
	Previous     json.RawMessage `json:"previous"`
}
type legacyProviderImage struct {
	Provider string          `json:"provider"`
	Opaque   json.RawMessage `json:"opaque,omitempty"`
}
type legacyPTYTuple struct {
	Build  string `json:"build"`
	SHA256 string `json:"sha256"`
	Path   string `json:"path"`
}
type legacyPTYFallback struct {
	Code             string         `json:"code"`
	State            string         `json:"state"`
	Newest           legacyPTYTuple `json:"newest"`
	FailureAttempt   string         `json:"failureAttempt"`
	FailureStage     string         `json:"failureStage"`
	FailureCode      string         `json:"failureCode"`
	LastGood         legacyPTYTuple `json:"lastGood"`
	LastGoodSequence uint64         `json:"lastGoodSequence"`
	Mode             string         `json:"mode"`
	Consent          bool           `json:"consent"`
}
type legacyPTYDeferral struct {
	Code    string         `json:"code"`
	State   string         `json:"state"`
	Target  legacyPTYTuple `json:"target"`
	Newest  legacyPTYTuple `json:"newest"`
	Attempt string         `json:"attempt"`
	Mode    string         `json:"mode"`
	Consent bool           `json:"consent"`
}
type legacyPTYSnapshot struct {
	Contract             string             `json:"contract"`
	SessionID            string             `json:"session_id"`
	LogPath              string             `json:"log_path"`
	PID                  int                `json:"pid"`
	VersionFallback      bool               `json:"versionFallback,omitempty"`
	VersionFallbackFrom  string             `json:"versionFallbackFrom,omitempty"`
	VersionFallbackEvent *legacyPTYFallback `json:"versionFallbackEvent,omitempty"`
	VersionDeferralEvent *legacyPTYDeferral `json:"versionDeferralEvent,omitempty"`
}
type legacyPTYProvenance struct {
	legacyPTYSnapshot
	Profile        string `json:"profile"`
	ProfileLock    string `json:"profile_lock_sha256"`
	ManagedHome    string `json:"managed_home"`
	FragmentSHA256 string `json:"fragment_sha256"`
}

func projectPTYTuple(src legacyPTYTuple) MusePTYFrozenTuple {
	return MusePTYFrozenTuple{Build: src.Build, SHA256: src.SHA256, Path: src.Path}
}
func mergePTYTuple(src MusePTYFrozenTuple) legacyPTYTuple {
	return legacyPTYTuple{Build: src.Build, SHA256: src.SHA256, Path: src.Path}
}
func projectPTYSnapshot(src legacyPTYSnapshot) MusePTYSnapshot {
	out := MusePTYSnapshot{Contract: src.Contract, SessionID: src.SessionID, LogPath: src.LogPath, PID: src.PID, VersionFallback: src.VersionFallback, VersionFallbackFrom: src.VersionFallbackFrom}
	if src.VersionFallbackEvent != nil {
		v := src.VersionFallbackEvent
		out.VersionFallbackEvent = &MusePTYFallbackWarning{Code: v.Code, State: v.State, Newest: projectPTYTuple(v.Newest), FailureAttempt: v.FailureAttempt, FailureStage: v.FailureStage, FailureCode: v.FailureCode, LastGood: projectPTYTuple(v.LastGood), LastGoodSequence: v.LastGoodSequence, Mode: v.Mode, Consent: v.Consent}
	}
	if src.VersionDeferralEvent != nil {
		v := src.VersionDeferralEvent
		out.VersionDeferralEvent = &MusePTYDeferralWarning{Code: v.Code, State: v.State, Target: projectPTYTuple(v.Target), Newest: projectPTYTuple(v.Newest), Attempt: v.Attempt, Mode: v.Mode, Consent: v.Consent}
	}
	return out
}
func mergePTYSnapshot(src MusePTYSnapshot) legacyPTYSnapshot {
	out := legacyPTYSnapshot{Contract: src.Contract, SessionID: src.SessionID, LogPath: src.LogPath, PID: src.PID, VersionFallback: src.VersionFallback, VersionFallbackFrom: src.VersionFallbackFrom}
	if src.VersionFallbackEvent != nil {
		v := src.VersionFallbackEvent
		out.VersionFallbackEvent = &legacyPTYFallback{Code: v.Code, State: v.State, Newest: mergePTYTuple(v.Newest), FailureAttempt: v.FailureAttempt, FailureStage: v.FailureStage, FailureCode: v.FailureCode, LastGood: mergePTYTuple(v.LastGood), LastGoodSequence: v.LastGoodSequence, Mode: v.Mode, Consent: v.Consent}
	}
	if src.VersionDeferralEvent != nil {
		v := src.VersionDeferralEvent
		out.VersionDeferralEvent = &legacyPTYDeferral{Code: v.Code, State: v.State, Target: mergePTYTuple(v.Target), Newest: mergePTYTuple(v.Newest), Attempt: v.Attempt, Mode: v.Mode, Consent: v.Consent}
	}
	return out
}
