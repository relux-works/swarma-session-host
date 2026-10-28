package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

// Probe stage. Every refusal names its stage so a caller can tell where
// a typed reason came from without matching prose.
const StageProbe = "probe"

// Probe refusal codes. A failed, partial or malformed read is never
// a legitimate absence: each code below refuses a distinct fact.
const (
	// CodeProbeCanceled refuses a probe-state operation whose context ended.
	CodeProbeCanceled = "muse_probe_canceled"
	// CodeProbeInvalid refuses a malformed probe input: an unusable
	// frozen reference or an observation that contradicts itself.
	CodeProbeInvalid = "muse_probe_invalid"
	// CodeProbeUnsupportedPlatform refuses probe state where the host
	// cannot report file identity or hold advisory locks.
	CodeProbeUnsupportedPlatform = "muse_probe_unsupported_platform"
	// CodeProbeUnprepared refuses an observation for a build with no
	// COPIED state to record against. Freeze first.
	CodeProbeUnprepared = "muse_probe_unprepared"
	// CodeProbeStateAbsent reports a probe-state read for a build
	// that carries no STATE document. Absence is typed, never a
	// zero value that could be mistaken for COPIED.
	CodeProbeStateAbsent = "muse_probe_state_absent"
	// CodeProbeStateInconsistent refuses corrupt, unreadable or
	// disagreeing probe metadata. It is not absence and never
	// authorizes a fresh verdict over the old one.
	CodeProbeStateInconsistent = "muse_probe_state_inconsistent"
	// Contract mismatch reuses CodeProbeContractMismatch from freeze.go:
	// the same typed reason, refused here at the probe stage.
	// CodeProbeRecordedBad returns the recorded deterministic verdict
	// for this exact build instead of recording over it. The error
	// carries the verdict for the section-5 controller.
	CodeProbeRecordedBad = "muse_recorded_bad"
	// CodeProbeSelectionMismatch refuses a first-probe tuple that
	// differs from this invocation's accepted newest discovery. A
	// non-newest execution runs only as the exact last-good under
	// the recorded failure permit consumed by Recover; the probe
	// entry never admits it directly (design section 5).
	CodeProbeSelectionMismatch = "muse_probe_selection_mismatch"
	// CodeProbeLockFailed refuses when the maintenance lease or the
	// metadata lock cannot be acquired. The cause tells which one.
	CodeProbeLockFailed = "muse_probe_lock_failed"
	// CodeProbePublicationFailed refuses when a probe verdict cannot
	// be persisted. A persisted attempt receipt travels with the
	// error so the controller retains the observed failure.
	CodeProbePublicationFailed = "muse_probe_publication_failed"
	// CodeProbePublicationUnknown reports a rename that succeeded
	// followed by a directory sync that did not: durability is
	// UNKNOWN, not a claim that the old record is intact.
	CodeProbePublicationUnknown = "muse_probe_publication_unknown"
	// CodeProbeEvidenceConflict refuses contradictory terminal
	// evidence for identical bytes: PROBE_OK recorded but a
	// deterministic BAD observed, or BAD recorded but success
	// observed. Neither writer confers truth; reviewed recovery owns it.
	CodeProbeEvidenceConflict = "muse_probe_evidence_conflict"
	// CodeProbeDuplicateAttempt refuses an attempt identity reused
	// for a conflicting observation. An identical retry is idempotent
	// and returns the recorded receipt instead.
	CodeProbeDuplicateAttempt = "muse_probe_duplicate_attempt"
	// CodeProbeReceiptAbsent reports an attempt-receipt read for an
	// attempt that recorded nothing.
	CodeProbeReceiptAbsent = "muse_probe_receipt_absent"
	// CodeFailureRecordUnavailable refuses recovery when neither the
	// verdict nor its attempt receipt could be persisted.
	CodeFailureRecordUnavailable = "muse_failure_record_unavailable"
)

// Deterministic BAD verdict codes. Only a clean first-probe identity
// contradiction mints one, never a timeout or a transient failure.
const (
	// VerdictProbeIdentityUnparseable marks a clean exit-0 whose
	// bounded output is structurally invalid: no well-formed build
	// can be attested from these bytes under this probe contract.
	VerdictProbeIdentityUnparseable = "probe_identity_unparseable"
	// VerdictProbeIdentityMismatch marks a clean exit-0 that attests
	// a well-formed build different from the selected frozen build.
	VerdictProbeIdentityMismatch = "probe_identity_mismatch"
)

// ProbeOutcome classifies one observed first-probe result. ProbeRunner owns
// execution; the recording entry remains available to trusted host adapters.
type ProbeOutcome string

const (
	// ProbeExitOKMatch observed a clean exit-0 whose full bounded
	// output matches the selected build.
	ProbeExitOKMatch ProbeOutcome = "exit_ok_match"
	// ProbeExitOKUnparseable observed a clean exit-0 whose full
	// bounded output is structurally invalid, with healthy reads
	// and an unchanged frozen hash.
	ProbeExitOKUnparseable ProbeOutcome = "exit_ok_unparseable"
	// ProbeExitOKMismatch observed a clean exit-0 with a well-formed
	// full build different from the selected one.
	ProbeExitOKMismatch ProbeOutcome = "exit_ok_mismatch"
	// ProbeTimeout observed a probe timeout, including repeated and
	// concurrent ones. Timeouts never mint a byte verdict.
	ProbeTimeout ProbeOutcome = "timeout"
	// ProbeExecFailed observed a fork/exec failure for an attempted probe.
	ProbeExecFailed ProbeOutcome = "exec_failed"
	// ProbeExitNonzero observed a nonzero exit.
	ProbeExitNonzero ProbeOutcome = "exit_nonzero"
	// ProbeSignaled observed termination by signal.
	ProbeSignaled ProbeOutcome = "signaled"
	// ProbeOutputLimit observed output past the during-read cap.
	ProbeOutputLimit ProbeOutcome = "output_limit"
	// ProbeDrainFailed observed a pipe/drain error during the probe.
	ProbeDrainFailed ProbeOutcome = "drain_failed"
)

// CleanupResult reports whether bounded teardown and reap were
// established for the observed attempt. ProbeRunner refuses another child
// and recovery after unconfirmed teardown.
type CleanupResult string

const (
	// CleanupConfirmed records established bounded teardown and reap.
	CleanupConfirmed CleanupResult = "confirmed"
	// CleanupIncomplete records teardown that could not be confirmed.
	CleanupIncomplete CleanupResult = "incomplete"
)

// ProbeObservation is one L2/L3-observed first-probe result. Identity
// comes from the frozen build the observation is recorded against,
// never from these fields: ObservedBuild is evidence, and only clean
// exit-0 rows may carry any.
type ProbeObservation struct {
	Execution ProbeExecution
	// AttemptID names this attempt. It must be unique per attempt.
	AttemptID string
	// SelectionID binds the discovery snapshot this attempt runs under.
	SelectionID string
	// Outcome classifies what the consumer observed.
	Outcome ProbeOutcome
	// ObservedBuild carries the attested full build, and only for
	// clean exit-0 rows: the match row may echo the selected build,
	// the mismatch row must name a different well-formed build, and
	// every other row must leave it empty.
	ObservedBuild string
	// Detail carries bounded diagnostic evidence: exit, signal,
	// deadline or cap facts. It never carries credentials.
	Detail string
	// Cleanup reports whether bounded teardown was established.
	Cleanup CleanupResult
	// OutputDigest is the lowercase hex SHA-256 of the bounded
	// captured output, when the consumer captured any.
	OutputDigest string
	// OutputBytes is the captured output size in bytes.
	OutputBytes int64
}

// AttemptReceipt is the durable per-attempt record: the observation
// plus the frozen identity it was recorded against and its admission
// timestamp. Transient receipts authorize only the current
// invocation's one fallback; the next invocation selects newest and
// tries it again.
type AttemptReceipt struct {
	Execution     ProbeExecution `json:"execution"`
	Schema        int            `json:"schema"`
	AttemptID     string         `json:"attempt_id"`
	SelectionID   string         `json:"selection_id"`
	Build         string         `json:"build"`
	SHA256        string         `json:"sha256"`
	Outcome       ProbeOutcome   `json:"outcome"`
	ObservedBuild string         `json:"observed_build,omitempty"`
	Detail        string         `json:"detail,omitempty"`
	Cleanup       CleanupResult  `json:"cleanup"`
	OutputDigest  string         `json:"output_digest,omitempty"`
	OutputBytes   int64          `json:"output_bytes"`
	RecordedAt    string         `json:"recorded_at"`
}

// ProbeRecord is what one recording produced: the current authoritative
// state plus, for failure observations, the independently persisted
// attempt receipt the controller can use even when the verdict write
// itself failed.
type ProbeRecord struct {
	State   BuildState
	Receipt *AttemptReceipt
}

// ProbeError is a typed probe-state refusal: the stage that refused
// and the machine-readable reason, with the underlying cause chained.
// Bad carries the recorded verdict and is set only with
// CodeProbeRecordedBad.
type ProbeError struct {
	Stage string
	Code  string
	Cause error
	Bad   *BadVerdict
}

// Error renders the refusal with its stage and code.
func (e *ProbeError) Error() string {
	if e == nil {
		return "muse probe error"
	}
	if e.Cause != nil {
		return "muse probe " + e.Stage + " " + e.Code + ": " + e.Cause.Error()
	}
	return "muse probe " + e.Stage + " " + e.Code
}

// Unwrap returns the underlying cause.
func (e *ProbeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// AsProbeError matches a ProbeError in err's chain for code and stage
// assertions.
func AsProbeError(err error) (*ProbeError, bool) {
	var refusal *ProbeError
	if errors.As(err, &refusal) {
		return refusal, true
	}
	return nil, false
}

const (
	attemptReceiptSchema   = 1
	attemptReceiptMaxBytes = 16 * 1024
	probeDetailMaxBytes    = 8 * 1024
	attemptsDirName        = "attempts"
)

var (
	errDuplicateAttempt     = errors.New("attempt identity reused for a conflicting observation")
	errUnknownProbeOutcome  = errors.New("unknown probe outcome")
	errUnknownCleanupResult = errors.New("unknown cleanup result")
	errNotStartup           = errors.New("no observed startup readiness")
)

// verdictClass is the recording decision for one observation: success
// transitions COPIED to PROBE_OK, a deterministic class transitions
// COPIED to BAD, and transient records a receipt without touching the
// lifecycle state.
type verdictClass int

const (
	verdictSuccess verdictClass = iota
	verdictTransient
	verdictBadUnparseable
	verdictBadMismatch
)

func probeRefuseIfCanceled(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return &ProbeError{Stage: StageProbe, Code: CodeProbeCanceled, Cause: err}
	}
	return nil
}

func probeRefusal(ctx context.Context, code string, cause error) error {
	if err := probeRefuseIfCanceled(ctx); err != nil {
		return err
	}
	if existing, ok := AsProbeError(cause); ok && existing.Code == CodeProbeCanceled {
		return cause
	}
	return &ProbeError{Stage: StageProbe, Code: code, Cause: cause}
}

// validateStoreRoot refuses a frozen root this package cannot honestly
// bind to bytes. Probe and last-good operations use the frozen store
// and the configured clock only; the global locker, ledger and ramp
// bindings belong to freeze admission and are never consulted here.
func validateStoreRoot(root string) error {
	if root == "" || !filepath.IsAbs(root) {
		return errInvalidFrozenRoot
	}
	return nil
}

// validateFrozenReference binds a frozen identity to the exact copy
// the store must hold: a parseable build, a full digest, and the
// canonical frozen path for that tuple. Callers cannot supply path
// and expected hash from different legs.
func validateFrozenReference(root string, id FrozenIdentity) error {
	if _, err := muse.ParseBuildID(id.Build); err != nil {
		return fmt.Errorf("frozen build does not parse: %w", err)
	}
	if !isLowerHex64(id.SHA256) {
		return errors.New("frozen digest is not 64 lowercase hex")
	}
	key := freezeKey(id.Build, id.SHA256)
	if expected := frozenBinaryPath(root, key, id.Build); id.Path != expected {
		return fmt.Errorf("frozen path %q is not the %s copy", id.Path, key)
	}
	return nil
}

// validAttemptToken admits receipt and selection identities: nonempty,
// bounded, without path separators or parent traversal, so the token
// plus ".json" is always a valid pinned child name. The charset loop
// already excludes every separator on every platform, so no further
// child check is needed here.
func validAttemptToken(token string) bool {
	if token == "" || token == "." || token == ".." || len(token) > 128 {
		return false
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		letter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !letter && !(c >= '0' && c <= '9') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// validateProbeObservation refuses a malformed observation before any
// store work. Identity-evidence consistency belongs to the
// classifier, which sees the frozen build the observation binds to.
func validateProbeObservation(obs ProbeObservation) error {
	if !validAttemptToken(obs.AttemptID) {
		return errors.New("attempt identity is not a plain token")
	}
	if !validAttemptToken(obs.SelectionID) {
		return errors.New("selection identity is not a plain token")
	}
	switch obs.Outcome {
	case ProbeExitOKMatch, ProbeExitOKUnparseable, ProbeExitOKMismatch,
		ProbeTimeout, ProbeExecFailed, ProbeExitNonzero, ProbeSignaled,
		ProbeOutputLimit, ProbeDrainFailed:
	default:
		return errUnknownProbeOutcome
	}
	switch obs.Cleanup {
	case CleanupConfirmed, CleanupIncomplete:
	default:
		return errUnknownCleanupResult
	}
	if len(obs.Detail) > probeDetailMaxBytes {
		return fmt.Errorf("detail is %d bytes, past the %d bound", len(obs.Detail), probeDetailMaxBytes)
	}
	if obs.OutputDigest != "" && !isLowerHex64(obs.OutputDigest) {
		return errors.New("output digest is not 64 lowercase hex")
	}
	if obs.OutputBytes < 0 {
		return errors.New("output size is negative")
	}
	return nil
}

// classifyProbeOutcome maps one validated observation to its recording
// decision and enforces the identity-evidence rule: only clean exit-0
// rows may carry observed builds, the mismatch row must name a
// different well-formed build, and over-cap or failed rows are never
// parsed for identity.
func classifyProbeOutcome(id FrozenIdentity, obs ProbeObservation) (verdictClass, error) {
	switch obs.Outcome {
	case ProbeExitOKMatch:
		if obs.ObservedBuild != "" && obs.ObservedBuild != id.Build {
			return verdictTransient, errors.New("match row names another build")
		}
		return verdictSuccess, nil
	case ProbeExitOKUnparseable:
		if obs.ObservedBuild != "" {
			return verdictTransient, errors.New("unparseable row carries an observed build")
		}
		return verdictBadUnparseable, nil
	case ProbeExitOKMismatch:
		if obs.ObservedBuild == "" || obs.ObservedBuild == id.Build {
			return verdictTransient, errors.New("mismatch row names no other build")
		}
		if _, err := muse.ParseBuildID(obs.ObservedBuild); err != nil {
			return verdictTransient, fmt.Errorf("observed build does not parse: %w", err)
		}
		return verdictBadMismatch, nil
	case ProbeTimeout, ProbeExecFailed, ProbeExitNonzero, ProbeSignaled, ProbeOutputLimit, ProbeDrainFailed:
		if obs.ObservedBuild != "" {
			return verdictTransient, errors.New("failed row carries an observed build")
		}
		return verdictTransient, nil
	default:
		return verdictTransient, errUnknownProbeOutcome
	}
}

// RecordProbeOutcome records one L2/L3-observed first-probe result
// against the frozen build's state: success transitions COPIED to
// PROBE_OK, a deterministic identity contradiction transitions COPIED
// to BAD, and every other observation records a transient attempt
// receipt without touching the lifecycle state. It never executes
// anything; the consumer observed the child and reports the outcome.
//
// Recording holds the shared maintenance lease across the publication
// and the metadata lock across the state read-modify-write, in that
// order. The attempt receipt is persisted before the verdict, so a
// failed verdict write still leaves the independently persisted
// receipt the controller can use for the current invocation. Nothing
// is retried and nothing is deleted.
func RecordProbeOutcome(ctx context.Context, cfg HostConfig, id FrozenIdentity, obs ProbeObservation) (ProbeRecord, error) {
	if err := probeRefuseIfCanceled(ctx); err != nil {
		return ProbeRecord{}, err
	}
	if !supportedPlatform {
		return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbeUnsupportedPlatform}
	}
	if err := validateStoreRoot(cfg.FrozenRoot); err != nil {
		return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid, Cause: err}
	}
	if err := validateFrozenReference(cfg.FrozenRoot, id); err != nil {
		return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid, Cause: err}
	}
	if err := validateProbeObservation(obs); err != nil {
		return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid, Cause: err}
	}
	class, err := classifyProbeOutcome(id, obs)
	if err != nil {
		return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid, Cause: err}
	}
	store := cfg.store
	if store == nil {
		store, err = openFrozenStore(ctx, cfg.FrozenRoot)
		if err != nil {
			return ProbeRecord{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
		}
		defer store.close()
	}
	cfg.store = store
	releaseLease, err := acquireMaintenanceLeaseAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok && ferr.Code == CodeFreezeCanceled {
			return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbeCanceled, Cause: err}
		}
		return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbeLockFailed, Cause: err}
	}
	defer func() { _ = releaseLease() }()
	releaseState, err := acquireProbeStateLock(ctx, cfg)
	if err != nil {
		return ProbeRecord{}, err
	}
	defer func() { _ = releaseState() }()
	key := freezeKey(id.Build, id.SHA256)
	state, present, err := readBuildStateAt(ctx, cfg.store.meta, key)
	if err != nil {
		if errors.Is(err, errProbeContractMismatch) {
			return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbeContractMismatch, Cause: err}
		}
		return ProbeRecord{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	if !present {
		return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbeUnprepared, Cause: fmt.Errorf("build %s carries no probe state", key)}
	}
	if err := state.agreesWith(key, id); err != nil {
		return ProbeRecord{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	if class == verdictSuccess || class == verdictBadUnparseable || class == verdictBadMismatch {
		if _, err := verifyFrozenTuple(ctx, cfg.store, key, id); err != nil {
			return ProbeRecord{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
		}
	}
	now := cfg.clock()().UTC().Format(time.RFC3339)
	switch class {
	case verdictSuccess:
		return commitProbeSuccess(ctx, cfg, key, state)
	case verdictBadUnparseable, verdictBadMismatch:
		return commitProbeBad(ctx, cfg, key, id, obs, state, now, class)
	default:
		return commitTransientReceipt(ctx, cfg, key, id, obs, state, now)
	}
}

// acquireProbeStateLock serializes probe-state publication through the
// attempt's held root. One bounded wait, no retry: contention refuses
// so the post-probe path can never wait indefinitely.
func acquireProbeStateLock(ctx context.Context, cfg HostConfig) (func() error, error) {
	release, err := acquireStateLockAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok && ferr.Code == CodeFreezeCanceled {
			return nil, &ProbeError{Stage: StageProbe, Code: CodeProbeCanceled, Cause: err}
		}
		return nil, &ProbeError{Stage: StageProbe, Code: CodeProbeLockFailed, Cause: err}
	}
	return release, nil
}

// commitProbeSuccess transitions COPIED to PROBE_OK. Re-recording
// success over PROBE_OK is idempotent; success over a recorded BAD is
// contradictory terminal evidence and refuses without conferring
// truth either way. Success writes no attempt receipt and never
// writes last-good: the PROBE_OK state is the evidence.
func commitProbeSuccess(ctx context.Context, cfg HostConfig, key string, state BuildState) (ProbeRecord, error) {
	switch state.State {
	case StateProbeOK:
		return ProbeRecord{State: state}, nil
	case StateBAD:
		return ProbeRecord{State: state}, &ProbeError{Stage: StageProbe, Code: CodeProbeEvidenceConflict, Cause: fmt.Errorf("success observed for recorded %s", state.Verdict.Code)}
	}
	state.State = StateProbeOK
	if err := writeBuildStateAt(ctx, cfg.store.meta, key, state); err != nil {
		if errors.Is(err, errPublicationUnknown) {
			return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeProbePublicationUnknown, Cause: err}
		}
		return ProbeRecord{}, probeRefusal(ctx, CodeProbePublicationFailed, err)
	}
	return ProbeRecord{State: state}, nil
}

// commitProbeBad records a deterministic identity contradiction: the
// attempt receipt first, then the COPIED to BAD transition. A BAD
// observation over PROBE_OK persists its receipt and then refuses as
// contradictory evidence; over an already-recorded BAD the first
// verdict wins and travels with the refusal.
func commitProbeBad(ctx context.Context, cfg HostConfig, key string, id FrozenIdentity, obs ProbeObservation, state BuildState, now string, class verdictClass) (ProbeRecord, error) {
	if state.State == StateProbeOK {
		receipt, receiptErr := writeAttemptReceiptAt(ctx, cfg.store.meta, key, id, obs, now)
		if receiptErr != nil {
			return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeFailureRecordUnavailable, Cause: receiptErr}
		}
		return ProbeRecord{State: state, Receipt: &receipt}, &ProbeError{Stage: StageProbe, Code: CodeProbeEvidenceConflict, Cause: fmt.Errorf("deterministic BAD observed over PROBE_OK")}
	}
	if state.State == StateBAD {
		receipt, receiptErr := writeAttemptReceiptAt(ctx, cfg.store.meta, key, id, obs, now)
		if receiptErr != nil {
			return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeFailureRecordUnavailable, Cause: receiptErr}
		}
		verdict := state.Verdict
		return ProbeRecord{State: state, Receipt: &receipt}, &ProbeError{Stage: StageProbe, Code: CodeProbeRecordedBad, Cause: fmt.Errorf("build %s is recorded %s", id.Build, verdict.Code), Bad: verdict}
	}
	code := VerdictProbeIdentityUnparseable
	if class == verdictBadMismatch {
		code = VerdictProbeIdentityMismatch
	}
	receipt, err := writeAttemptReceiptAt(ctx, cfg.store.meta, key, id, obs, now)
	if err != nil {
		return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeFailureRecordUnavailable, Cause: err}
	}
	persisted := state
	state.State = StateBAD
	state.Verdict = &BadVerdict{Code: code, Detail: obs.Detail, RecordedAt: now}
	if err := writeBuildStateAt(ctx, cfg.store.meta, key, state); err != nil {
		if errors.Is(err, errPublicationUnknown) {
			return ProbeRecord{State: persisted, Receipt: &receipt}, &ProbeError{Stage: StageProbe, Code: CodeProbePublicationUnknown, Cause: err}
		}
		return ProbeRecord{State: persisted, Receipt: &receipt}, probeRefusal(ctx, CodeProbePublicationFailed, err)
	}
	return ProbeRecord{State: state, Receipt: &receipt}, nil
}

// commitTransientReceipt records one failed attempt without touching
// the lifecycle state: COPIED stays COPIED through every timeout, and
// a transient over a terminal state records its receipt while the
// terminal verdict stands. Failure recording never modifies
// last-good.
func commitTransientReceipt(ctx context.Context, cfg HostConfig, key string, id FrozenIdentity, obs ProbeObservation, state BuildState, now string) (ProbeRecord, error) {
	receipt, err := writeAttemptReceiptAt(ctx, cfg.store.meta, key, id, obs, now)
	if err != nil {
		return ProbeRecord{}, &ProbeError{Stage: StageProbe, Code: CodeFailureRecordUnavailable, Cause: err}
	}
	return ProbeRecord{State: state, Receipt: &receipt}, nil
}

// writeAttemptReceiptAt persists one attempt receipt under the build's
// metadata directory. Re-recording the identical observation is
// idempotent and returns the recorded receipt, so a crash between the
// receipt and the verdict write retries cleanly; reusing the attempt
// identity for a conflicting observation refuses.
func writeAttemptReceiptAt(ctx context.Context, meta *pinnedDir, key string, id FrozenIdentity, obs ProbeObservation, now string) (AttemptReceipt, error) {
	if err := ctx.Err(); err != nil {
		return AttemptReceipt{}, err
	}
	receipt := AttemptReceipt{
		Schema: attemptReceiptSchema, AttemptID: obs.AttemptID, SelectionID: obs.SelectionID,
		Build: id.Build, SHA256: id.SHA256, Outcome: obs.Outcome, ObservedBuild: obs.ObservedBuild,
		Detail: obs.Detail, Cleanup: obs.Cleanup, OutputDigest: obs.OutputDigest,
		OutputBytes: obs.OutputBytes, RecordedAt: now,
	}
	receipt.Execution = obs.Execution
	raw, err := attemptDocument.encode(receipt)
	if err != nil {
		return AttemptReceipt{}, err
	}
	if _, err := meta.mkdirChild(key); err != nil {
		return AttemptReceipt{}, err
	}
	dir, err := meta.openChildDir(key)
	if err != nil {
		return AttemptReceipt{}, err
	}
	defer dir.close()
	if err := requireDirMode(dir, 0o700); err != nil {
		return AttemptReceipt{}, err
	}
	if _, err := dir.mkdirChild(attemptsDirName); err != nil {
		return AttemptReceipt{}, err
	}
	attempts, err := dir.openChildDir(attemptsDirName)
	if err != nil {
		return AttemptReceipt{}, err
	}
	defer attempts.close()
	if err := requireDirMode(attempts, 0o700); err != nil {
		return AttemptReceipt{}, err
	}
	fileName := obs.AttemptID + ".json"
	exists, err := attempts.childExists(fileName)
	if err != nil {
		return AttemptReceipt{}, err
	}
	if exists {
		existing, present, err := readAttemptReceiptAt(ctx, attempts, obs.AttemptID)
		if err != nil {
			return AttemptReceipt{}, err
		}
		if !present {
			return AttemptReceipt{}, fmt.Errorf("attempt %s vanished after discovery", obs.AttemptID)
		}
		if !existing.equivalent(receipt) {
			return AttemptReceipt{}, errDuplicateAttempt
		}
		return existing, nil
	}
	if err := atomicDocumentAt(ctx, attempts, fileName, ".attempt-", raw); err != nil {
		return AttemptReceipt{}, err
	}
	return receipt, nil
}

// equivalent reports whether two receipts record the same observation.
// The admission timestamp is excluded: it names when the record
// landed, not what was observed.
func (r AttemptReceipt) equivalent(other AttemptReceipt) bool {
	return r.Schema == other.Schema &&
		r.Execution == other.Execution &&
		r.AttemptID == other.AttemptID &&
		r.SelectionID == other.SelectionID &&
		r.Build == other.Build &&
		r.SHA256 == other.SHA256 &&
		r.Outcome == other.Outcome &&
		r.ObservedBuild == other.ObservedBuild &&
		r.Detail == other.Detail &&
		r.Cleanup == other.Cleanup &&
		r.OutputDigest == other.OutputDigest &&
		r.OutputBytes == other.OutputBytes
}

func validateAttemptReceipt(receipt AttemptReceipt) error {
	if receipt.Schema != attemptReceiptSchema {
		return fmt.Errorf("schema is %d, want %d", receipt.Schema, attemptReceiptSchema)
	}
	if !validAttemptToken(receipt.AttemptID) {
		return errors.New("attempt identity is not a plain token")
	}
	if !validAttemptToken(receipt.SelectionID) {
		return errors.New("selection identity is not a plain token")
	}
	if _, err := muse.ParseBuildID(receipt.Build); err != nil {
		return fmt.Errorf("build %q does not parse: %w", receipt.Build, err)
	}
	if !isLowerHex64(receipt.SHA256) {
		return errors.New("sha256 is not 64 lowercase hex")
	}
	switch receipt.Outcome {
	case ProbeExitOKMatch, ProbeExitOKUnparseable, ProbeExitOKMismatch,
		ProbeTimeout, ProbeExecFailed, ProbeExitNonzero, ProbeSignaled,
		ProbeOutputLimit, ProbeDrainFailed:
	default:
		return errUnknownProbeOutcome
	}
	switch receipt.Cleanup {
	case CleanupConfirmed, CleanupIncomplete:
	default:
		return errUnknownCleanupResult
	}
	if len(receipt.Detail) > probeDetailMaxBytes {
		return fmt.Errorf("detail is %d bytes, past the %d bound", len(receipt.Detail), probeDetailMaxBytes)
	}
	if receipt.OutputDigest != "" && !isLowerHex64(receipt.OutputDigest) {
		return errors.New("output digest is not 64 lowercase hex")
	}
	if receipt.OutputBytes < 0 {
		return errors.New("output size is negative")
	}
	if _, err := time.Parse(time.RFC3339, receipt.RecordedAt); err != nil {
		return fmt.Errorf("recorded_at does not parse: %w", err)
	}
	selected := FrozenIdentity{Build: receipt.Build, SHA256: receipt.SHA256}
	observed := ProbeObservation{Outcome: receipt.Outcome, ObservedBuild: receipt.ObservedBuild}
	if _, err := classifyProbeOutcome(selected, observed); err != nil {
		return err
	}
	return nil
}

// readAttemptReceiptAt reads one attempt receipt through the held
// attempts directory. Absence is (zero, false, nil); every other
// failure is an error, never absence.
func readAttemptReceiptAt(ctx context.Context, attempts *pinnedDir, attemptID string) (AttemptReceipt, bool, error) {
	if err := ctx.Err(); err != nil {
		return AttemptReceipt{}, false, err
	}
	file, err := attempts.openChildFile(attemptID + ".json")
	if os.IsNotExist(err) {
		return AttemptReceipt{}, false, nil
	}
	if err != nil {
		return AttemptReceipt{}, false, err
	}
	defer file.Close()
	raw, info, _, err := readMetadataFD(ctx, file, attemptReceiptMaxBytes, true)
	if err != nil {
		return AttemptReceipt{}, false, err
	}
	if info.Mode().Perm() != 0o600 {
		return AttemptReceipt{}, false, fmt.Errorf("attempt receipt mode is %s", info.Mode())
	}
	receipt, err := attemptDocument.parse(raw)
	return receipt, err == nil, err
}

// ReadBuildState reads the one authoritative STATE document for a
// frozen build: schema, identity, publish transaction, probe contract
// and lifecycle state. Absence is typed; corruption is an error,
// never absence and never a silent default.
func ReadBuildState(ctx context.Context, cfg HostConfig, id FrozenIdentity) (BuildState, error) {
	if err := probeRefuseIfCanceled(ctx); err != nil {
		return BuildState{}, err
	}
	if !supportedPlatform {
		return BuildState{}, &ProbeError{Stage: StageProbe, Code: CodeProbeUnsupportedPlatform}
	}
	if err := validateStoreRoot(cfg.FrozenRoot); err != nil {
		return BuildState{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid, Cause: err}
	}
	if err := validateFrozenReference(cfg.FrozenRoot, id); err != nil {
		return BuildState{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid, Cause: err}
	}
	store, err := openFrozenStore(ctx, cfg.FrozenRoot)
	if err != nil {
		return BuildState{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	defer store.close()
	key := freezeKey(id.Build, id.SHA256)
	state, present, err := readBuildStateAt(ctx, store.meta, key)
	if err != nil {
		if errors.Is(err, errProbeContractMismatch) {
			return BuildState{}, &ProbeError{Stage: StageProbe, Code: CodeProbeContractMismatch, Cause: err}
		}
		return BuildState{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	if !present {
		return BuildState{}, &ProbeError{Stage: StageProbe, Code: CodeProbeStateAbsent, Cause: fmt.Errorf("build %s carries no probe state", key)}
	}
	if err := state.agreesWith(key, id); err != nil {
		return BuildState{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	return state, nil
}

// BadRecord returns the recorded deterministic verdict for a frozen
// build. Absence of state, or state that is not BAD, is (zero, false,
// nil); corruption is an error, never a silent false.
func BadRecord(ctx context.Context, cfg HostConfig, id FrozenIdentity) (BadVerdict, bool, error) {
	state, err := ReadBuildState(ctx, cfg, id)
	if err != nil {
		if refusal, ok := AsProbeError(err); ok && refusal.Code == CodeProbeStateAbsent {
			return BadVerdict{}, false, nil
		}
		return BadVerdict{}, false, err
	}
	if state.State != StateBAD || state.Verdict == nil {
		return BadVerdict{}, false, nil
	}
	return *state.Verdict, true, nil
}

// ReadAttemptReceipt reads one recorded attempt receipt for a frozen
// build. The receipt names its build and digest, which must agree
// with the frozen identity it is read under.
func ReadAttemptReceipt(ctx context.Context, cfg HostConfig, id FrozenIdentity, attemptID string) (AttemptReceipt, error) {
	if err := probeRefuseIfCanceled(ctx); err != nil {
		return AttemptReceipt{}, err
	}
	if !supportedPlatform {
		return AttemptReceipt{}, &ProbeError{Stage: StageProbe, Code: CodeProbeUnsupportedPlatform}
	}
	if err := validateStoreRoot(cfg.FrozenRoot); err != nil {
		return AttemptReceipt{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid, Cause: err}
	}
	if err := validateFrozenReference(cfg.FrozenRoot, id); err != nil {
		return AttemptReceipt{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid, Cause: err}
	}
	if !validAttemptToken(attemptID) {
		return AttemptReceipt{}, &ProbeError{Stage: StageProbe, Code: CodeProbeInvalid, Cause: errors.New("attempt identity is not a plain token")}
	}
	store, err := openFrozenStore(ctx, cfg.FrozenRoot)
	if err != nil {
		return AttemptReceipt{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	defer store.close()
	key := freezeKey(id.Build, id.SHA256)
	dir, err := store.meta.openChildDir(key)
	if os.IsNotExist(err) {
		return AttemptReceipt{}, &ProbeError{Stage: StageProbe, Code: CodeProbeReceiptAbsent, Cause: fmt.Errorf("build %s carries no attempts", key)}
	}
	if err != nil {
		return AttemptReceipt{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	defer dir.close()
	attempts, err := dir.openChildDir(attemptsDirName)
	if os.IsNotExist(err) {
		return AttemptReceipt{}, &ProbeError{Stage: StageProbe, Code: CodeProbeReceiptAbsent, Cause: fmt.Errorf("attempt %s recorded nothing", attemptID)}
	}
	if err != nil {
		return AttemptReceipt{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	defer attempts.close()
	receipt, present, err := readAttemptReceiptAt(ctx, attempts, attemptID)
	if err != nil {
		return AttemptReceipt{}, probeRefusal(ctx, CodeProbeStateInconsistent, err)
	}
	if !present {
		return AttemptReceipt{}, &ProbeError{Stage: StageProbe, Code: CodeProbeReceiptAbsent, Cause: fmt.Errorf("attempt %s recorded nothing", attemptID)}
	}
	if receipt.Build != id.Build || receipt.SHA256 != id.SHA256 {
		return AttemptReceipt{}, probeRefusal(ctx, CodeProbeStateInconsistent, fmt.Errorf("receipt names %s/%s, want the %s tuple", receipt.Build, receipt.SHA256, key))
	}
	return receipt, nil
}
