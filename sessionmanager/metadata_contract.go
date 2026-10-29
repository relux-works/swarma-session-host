package sessionmanager

import "context"

// D12 is optional: existing host and workspace method sets remain unchanged.
// Commit is synchronous; only the sole writer's observed bytes yield a receipt.
type RecordMetadataCommitter interface {
	CommitRecordMetadata(context.Context, RecordMetadataCommit) (RecordMetadataReceipt, error)
}

// Identity and exact committed-base admission precede every metadata effect.
type RecordMetadataCommit struct {
	Lease      ControllerLease
	ID         string
	Owner      OwnerKey
	Provider   string
	BaseSHA256 string
	Patch      RecordMetadataPatch
}

// Nil groups mean unchanged; a nonnil wrapper can explicitly clear its value.
// There is no arbitrary RunnerRecord, native snapshot or retirement field.
type RecordMetadataPatch struct {
	AppliedGoalRef  *AppliedGoalMetadata
	ConditionalStop *ConditionalStopMetadata
	OwnerRunError   *OwnerRunMetadata
	Materialization *MaterializationMetadata
	Eviction        *EvictionMetadata
}

type AppliedGoalMetadata struct{ Value *RevisionRef }
type OwnerRunMetadata struct{ Value string }
type MaterializationMetadata struct {
	CtxID                   string
	ProviderHostPendingFork bool
}
type EvictionMetadata struct{ Value *SessionEvictionNotice }

// The host captures Previous at intent and derives Signalled, legacy phase and
// GracefulRetirement from this transition; the caller cannot supply them.
type ConditionalStopMetadata struct {
	ExpectedPhase ConditionalStopPhase
	NextPhase     ConditionalStopPhase
}
type ConditionalStopPhase string

const (
	ConditionalStopPhaseNone       ConditionalStopPhase = "none"
	ConditionalStopPhaseIntent     ConditionalStopPhase = "intent"
	ConditionalStopPhaseAttempt    ConditionalStopPhase = "signal-attempt"
	ConditionalStopPhaseSent       ConditionalStopPhase = "signal-sent"
	ConditionalStopPhaseRollback   ConditionalStopPhase = "rollback"
	ConditionalStopPhaseRetirement ConditionalStopPhase = "retirement"
)

// A receipt is evidence for this one fenced operation/base, never authority for
// a new attempt. The fork also requires its D9 private candidate acknowledgement.
type RecordMetadataReceipt struct {
	Lease           ControllerLease
	ID              string
	Owner           OwnerKey
	Provider        string
	BaseSHA256      string
	CommittedSHA256 string
}
