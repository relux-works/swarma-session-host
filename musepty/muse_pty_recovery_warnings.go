package musepty

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	musebuilds "github.com/relux-works/swarma-session-host/musebuilds"
)

// This file is the L3P PTY recovery warnings (M50 leg): the same
// three channels L2M emits for MSP — run/host snapshot state,
// stderr, and the pending task note — over the PTY host. Wording
// matches the MSP record shape field for field; only the stage
// names differ.

// musePTYRecoveryWarningWriter carries fallback and deferral
// warnings; it defaults to stderr (the version-warning precedent)
// and tests swap it to capture the emission.
var musePTYRecoveryWarningWriter io.Writer = os.Stderr

// musePTYFrozenTupleJSON is one frozen tuple as the host snapshot
// records it. The shape is musepty-owned so resume never depends on
// the musebuilds store schema.
type musePTYFrozenTupleJSON struct {
	Build  string `json:"build"`
	SHA256 string `json:"sha256"`
	Path   string `json:"path"`
}

// musePTYFallbackWarning is the typed record of a completed fallback
// leg: the failed newest, its failure evidence, the exact last-good
// that ran, and the mode/consent behind it.
type musePTYFallbackWarning struct {
	Code             string                 `json:"code"`
	State            string                 `json:"state"`
	Newest           musePTYFrozenTupleJSON `json:"newest"`
	FailureAttempt   string                 `json:"failureAttempt"`
	FailureStage     string                 `json:"failureStage"`
	FailureCode      string                 `json:"failureCode"`
	LastGood         musePTYFrozenTupleJSON `json:"lastGood"`
	LastGoodSequence uint64                 `json:"lastGoodSequence"`
	Mode             string                 `json:"mode"`
	Consent          bool                   `json:"consent"`
}

// musePTYDeferralWarning is the typed record of a completed host-cap
// deferral leg: the installed newest that could not freeze and the
// newest already-frozen build that ran instead.
type musePTYDeferralWarning struct {
	Code    string                 `json:"code"`
	State   string                 `json:"state"`
	Target  musePTYFrozenTupleJSON `json:"target"`
	Newest  musePTYFrozenTupleJSON `json:"newest"`
	Attempt string                 `json:"attempt"`
	Mode    string                 `json:"mode"`
	Consent bool                   `json:"consent"`
}

func musePTYTupleJSON(build, sha, path string) musePTYFrozenTupleJSON {
	return musePTYFrozenTupleJSON{Build: build, SHA256: sha, Path: path}
}

func musePTYFallbackWarningFromEvent(event musebuilds.RecoveryEvent) musePTYFallbackWarning {
	return musePTYFallbackWarning{
		Code:             event.Code,
		State:            event.State,
		Newest:           musePTYTupleJSON(event.Newest.Build, event.Newest.SHA256, event.Newest.Path),
		FailureAttempt:   event.FailureAttempt,
		FailureStage:     event.FailureStage,
		FailureCode:      event.FailureCode,
		LastGood:         musePTYTupleJSON(event.LastGood.Frozen.Build, event.LastGood.Frozen.SHA256, event.LastGood.Frozen.Path),
		LastGoodSequence: event.LastGood.Sequence,
		Mode:             string(event.Mode),
		Consent:          event.Consent,
	}
}

func musePTYDeferralWarningFromStart(start musebuilds.DeferralStart) musePTYDeferralWarning {
	return musePTYDeferralWarning{
		Code:    start.Warning,
		Target:  musePTYTupleJSON(start.Target.Build, start.Target.SHA256, start.Target.Path),
		Newest:  musePTYTupleJSON(start.Newest.Build, start.Newest.SHA256, start.Newest.Path),
		Attempt: start.AttemptID,
		Mode:    string(start.Mode),
		Consent: start.Consented,
	}
}

func musePTYConsentSuffix(consented bool) string {
	if consented {
		return ", consented"
	}
	return ""
}

// musePTYFallbackNoteText composes the single fallback wording shared
// by the stderr emission and the pending note. The event passed in
// carries its terminal state.
func musePTYFallbackNoteText(event musebuilds.RecoveryEvent) string {
	return fmt.Sprintf("%s: newest %s failed at %s (%s); last-good %s %s (receipt %d, mode %s%s)",
		event.Code, event.Newest.Build, event.FailureStage, event.FailureCode,
		event.LastGood.Frozen.Build, event.State, event.LastGood.Sequence,
		string(event.Mode), musePTYConsentSuffix(event.Consent))
}

// musePTYDeferralNoteText composes the single deferral wording shared
// by the stderr emission and the pending note.
func musePTYDeferralNoteText(start musebuilds.DeferralStart) string {
	return fmt.Sprintf("%s: newest %s could not freeze (host cap); running newest already-frozen %s (mode %s%s)",
		start.Warning, start.Newest.Build, start.Target.Build,
		string(start.Mode), musePTYConsentSuffix(start.Consented))
}

// musePTYDeferralAttemptingText composes the pre-start attempting line
// from the deferral authorization: the installed newest that could
// not freeze and the already-frozen target about to run. Consent is
// unknown before the interactive offer, so the mode stands alone.
func musePTYDeferralAttemptingText(auth *musebuilds.DeferralAuthorization) string {
	return fmt.Sprintf("%s: attempting deferral of newest %s onto newest already-frozen %s (mode %s)",
		auth.Warning(), auth.Newest().Build, auth.Target().Build, string(auth.Mode()))
}

// musePTYDeferralFailedText composes the failed-deferral follow-up:
// the host-cap warning still fires when the authorized target leg
// fails, naming the target that was attempted and the failure.
func musePTYDeferralFailedText(auth *musebuilds.DeferralAuthorization, err error) string {
	return fmt.Sprintf("%s: deferral of newest %s onto newest already-frozen %s failed: %v (mode %s)",
		auth.Warning(), auth.Newest().Build, auth.Target().Build, err, string(auth.Mode()))
}

// musePTYDeferralDeclinedText composes the declined-deferral
// follow-up: the attempting warning already fired before the offer,
// so the decline resolves it instead of leaving it dangling. Nothing
// ran.
func musePTYDeferralDeclinedText(auth *musebuilds.DeferralAuthorization) string {
	return fmt.Sprintf("%s: deferral of newest %s onto newest already-frozen %s declined (mode %s)",
		auth.Warning(), auth.Newest().Build, auth.Target().Build, string(auth.Mode()))
}

// MusePTYRecoveryWarnings holds one invocation's warning snapshot and
// pending notes, including when startup fails before a host exists. A
// zero value is ready to use. Callers may supply one through
// MusePTYBuildsBinding to observe startup; use a fresh instance for
// each invocation. Notes are pending for L4 delivery, never an
// assertion that a task note was delivered.
type MusePTYRecoveryWarnings struct {
	mu       sync.Mutex
	snapshot musePTYRecoverySnapshot
	host     *musePTYHost
}

type musePTYRecoverySnapshot struct {
	VersionFallback      bool                    `json:"versionFallback,omitempty"`
	VersionFallbackFrom  string                  `json:"versionFallbackFrom,omitempty"`
	VersionFallbackEvent *musePTYFallbackWarning `json:"versionFallbackEvent,omitempty"`
	VersionDeferralEvent *musePTYDeferralWarning `json:"versionDeferralEvent,omitempty"`
	FallbackNote         string                  `json:"fallbackNote,omitempty"`
	DeferralNote         string                  `json:"deferralNote,omitempty"`
}

// Snapshot serializes the current warning and its pending note
// before, during, or after startup. The host receives the same
// warning when it is admitted.
func (w *MusePTYRecoveryWarnings) Snapshot() ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return json.Marshal(w.snapshot)
}

func (w *MusePTYRecoveryWarnings) FallbackNote() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshot.FallbackNote
}

func (w *MusePTYRecoveryWarnings) DeferralNote() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshot.DeferralNote
}

// MusePTYRecoveryError preserves all recovery channels when no host
// is returned. Unwrap retains the original typed refusals and
// primary-first error order.
type MusePTYRecoveryError struct {
	Cause    error
	Warnings *MusePTYRecoveryWarnings
}

func (e *MusePTYRecoveryError) Error() string { return e.Cause.Error() }
func (e *MusePTYRecoveryError) Unwrap() error { return e.Cause }

type musePTYRecoveryRecord struct {
	fallback *musePTYFallbackWarning
	deferral *musePTYDeferralWarning
	note     string
}

func musePTYFallbackRecord(event musebuilds.RecoveryEvent) musePTYRecoveryRecord {
	warning := musePTYFallbackWarningFromEvent(event)
	return musePTYRecoveryRecord{fallback: &warning, note: musePTYFallbackNoteText(event)}
}

func musePTYDeferralRecord(start musebuilds.DeferralStart, state, note string) musePTYRecoveryRecord {
	warning := musePTYDeferralWarningFromStart(start)
	warning.State = state
	return musePTYRecoveryRecord{deferral: &warning, note: note}
}

func (w *MusePTYRecoveryWarnings) recordSnapshot(record musePTYRecoveryRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snapshot.VersionFallback = record.fallback != nil
	w.snapshot.VersionFallbackFrom = ""
	if record.fallback != nil {
		w.snapshot.VersionFallbackFrom = record.fallback.Newest.Path
	}
	w.snapshot.VersionFallbackEvent = record.fallback
	w.snapshot.VersionDeferralEvent = record.deferral
	if w.host != nil {
		w.host.mu.Lock()
		w.projectSnapshotLocked()
		w.host.mu.Unlock()
	}
}

func (w *MusePTYRecoveryWarnings) recordNote(record musePTYRecoveryRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snapshot.FallbackNote, w.snapshot.DeferralNote = "", ""
	if record.fallback != nil {
		w.snapshot.FallbackNote = record.note
	}
	if record.deferral != nil {
		w.snapshot.DeferralNote = record.note
	}
	if w.host != nil {
		w.host.mu.Lock()
		w.host.fallbackNote, w.host.deferralNote = w.snapshot.FallbackNote, w.snapshot.DeferralNote
		w.host.mu.Unlock()
	}
}

func (w *MusePTYRecoveryWarnings) writeStderr(record musePTYRecoveryRecord) {
	if record.note != "" {
		fmt.Fprintf(musePTYRecoveryWarningWriter, "warning: %s\n", record.note)
	}
}

// Both w.mu and the host's mu are held by the caller.
func (w *MusePTYRecoveryWarnings) projectSnapshotLocked() {
	w.host.snapshot.VersionFallback = w.snapshot.VersionFallback
	w.host.snapshot.VersionFallbackFrom = w.snapshot.VersionFallbackFrom
	w.host.snapshot.VersionFallbackEvent = w.snapshot.VersionFallbackEvent
	w.host.snapshot.VersionDeferralEvent = w.snapshot.VersionDeferralEvent
}

func (w *MusePTYRecoveryWarnings) attach(host *musePTYHost) {
	if host == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.host = host
	host.mu.Lock()
	defer host.mu.Unlock()
	w.projectSnapshotLocked()
	host.fallbackNote, host.deferralNote = w.snapshot.FallbackNote, w.snapshot.DeferralNote
}

// musePTYRunRecovery is the single emission seam for both routes and
// every outcome. The three attempting channels are populated before
// start can invoke any child. The terminal channels replace them even
// when start returns an error and no host. Channel updates complete
// synchronously.
func musePTYRunRecovery(warnings *MusePTYRecoveryWarnings, attempt musePTYRecoveryRecord, start func() (*musePTYHost, musePTYRecoveryRecord, error)) (*musePTYHost, error) {
	if warnings == nil {
		warnings = &MusePTYRecoveryWarnings{}
	}
	warnings.recordSnapshot(attempt)
	warnings.recordNote(attempt)
	warnings.writeStderr(attempt)
	host, terminal, err := start()
	warnings.attach(host)
	warnings.recordSnapshot(terminal)
	warnings.recordNote(terminal)
	warnings.writeStderr(terminal)
	if err != nil {
		return nil, &MusePTYRecoveryError{Cause: err, Warnings: warnings}
	}
	return host, nil
}
