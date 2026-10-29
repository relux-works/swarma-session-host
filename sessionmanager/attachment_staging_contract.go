package sessionmanager

import "context"

// D13 is optional; existing Attach/WorkspaceSessionManager signatures stay fixed.
// The host owns token identity and fresh/reused classification. Preparation
// publishes no client; the fork binds canonical goals before CommitAttach.
type StagedAttachmentHost interface {
	PrepareAttach(context.Context, AttachRequest) (AttachmentPreparation, error)
	CommitAttach(context.Context, AttachmentCommit) (*Attachment, error)
	AbortAttach(context.Context, AttachmentAbort) (AttachmentAbortReceipt, error)
}
type AttachmentPreparationToken string
type AttachmentPreparation struct {
	Token      AttachmentPreparationToken
	Session    SessionView
	Lease      ControllerLease
	Writer     HostInputWriter
	BaseSHA256 string
}
type AttachmentCommit struct {
	Token      AttachmentPreparationToken
	Lease      ControllerLease
	BaseSHA256 string
}
type AttachmentAbort struct {
	Token      AttachmentPreparationToken
	Lease      ControllerLease
	BaseSHA256 string
}
type AttachmentAbortDisposition string

const (
	AttachmentAbortDiscardedFresh      AttachmentAbortDisposition = "discarded-fresh"
	AttachmentAbortPreservedRecovering AttachmentAbortDisposition = "preserved-recovering"
)

type AttachmentAbortReceipt struct {
	Token           AttachmentPreparationToken
	Lease           ControllerLease
	BaseSHA256      string
	Disposition     AttachmentAbortDisposition
	CommittedSHA256 string
	DiscardedSHA256 string
}

// No path or writer enters this optional codec acknowledgement. The host alone
// removes its checked fresh state, observes absence, then acknowledges the exact
// last removed image. A refusal retains uncertainty/reservation, never success.
type LegacyRecordDiscardAcknowledger interface {
	AcknowledgeDiscarded(context.Context, LegacyRecordDiscard) error
}
type LegacyRecordDiscard struct {
	NamespaceFingerprint string
	SessionID            string
	DiscardedSHA256      string
}
