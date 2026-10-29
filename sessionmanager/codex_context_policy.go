package sessionmanager

import (
	"fmt"
	"io"
	"time"
)

type CodexPresenceReport struct {
	CtxID     string
	Presence  string
	Detail    string
	CheckedAt time.Time
}
type CodexResumeAttempt struct {
	CtxID           string
	Provider        string
	RecordedVersion string
	ActiveVersion   string
	Operation       string
	Cause           error
}
type CodexTranscriptCompaction struct{ Marker string }

// CodexContextPolicy is the context-service decision port. File/process/RPC
// operations stay with the provider; the host context service supplies the existing decisions.
type CodexContextPolicy interface {
	RequirePayloadPresent(CodexPresenceReport, time.Time, ContextResolutionMode) error
	ResumeUnusable(CodexResumeAttempt) error
	InspectTranscript(io.Reader) (string, string, error)
}

// CodexContextError transports the original provider refusal facts for typed
// consumer translation; it does not implement retention or recovery policy.
type CodexContextError struct {
	Code    string
	Message string
	Cause   error
}

func (e *CodexContextError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}
func (e *CodexContextError) Unwrap() error { return e.Cause }
