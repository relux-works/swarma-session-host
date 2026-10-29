package sessionmanager

import (
	"context"
	"time"
)

// D16 is optional and precedes strict native snapshot projection. Classification
// is pure: neither filesystem paths nor a writer/callback enter this port. An
// error refuses admission and effects; it is never a quarantine/discard vote.
type LegacyBootstrapClassifier interface {
	ClassifyBootstrap(context.Context, LegacyBootstrapRequest) (LegacyBootstrapClassification, error)
}

// The host supplies its clock sample so metadata encoding is deterministic.
// Input remains the original hash-bound image; the classifier never edits it.
type LegacyBootstrapRequest struct {
	Input         LegacyRecordInput
	QuarantinedAt time.Time
}

type LegacyBootstrapDisposition string

const (
	LegacyBootstrapAdmit                 LegacyBootstrapDisposition = "admit"
	LegacyBootstrapDiscardEmptyPrelaunch LegacyBootstrapDisposition = "discard-empty-prelaunch"
	LegacyBootstrapQuarantine            LegacyBootstrapDisposition = "quarantine"
)

// Record is non-nil only for admit. QuarantineMetadata is the sole D16 raw-byte
// exception: original legacy quarantine JSON, never a host-interpreted owner,
// provider snapshot, path, executable payload or general extension envelope.
type LegacyBootstrapClassification struct {
	SessionID          string
	BaseSHA256         string
	Disposition        LegacyBootstrapDisposition
	Record             *RunnerRecord
	QuarantineMetadata []byte
}

// The host acknowledges only observed retained bytes or durable absence after
// its sole writer has rechecked the original image under the lifetime lock.
// A refusal preserves recovery responsibility; no path or write authority is
// returned to the fork. Existing codec methods and HostOptions stay unchanged.
type LegacyBootstrapAcknowledger interface {
	AcknowledgeBootstrap(context.Context, LegacyBootstrapReceipt) error
}

type LegacyBootstrapReceipt struct {
	NamespaceFingerprint     string
	SessionID                string
	BaseSHA256               string
	Disposition              LegacyBootstrapDisposition
	RetainedSHA256           string
	QuarantineMetadataSHA256 string
}
