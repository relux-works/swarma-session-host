package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

// Last-good stage. Every refusal names its stage so a caller can tell
// where a typed reason came from without matching prose.
const StageLastGood = "lastgood"

// Last-good refusal codes. A failed, partial or malformed read is
// never a legitimate absence: each code below refuses a distinct fact.
const (
	// CodeLastGoodCanceled refuses a last-good operation whose context ended.
	CodeLastGoodCanceled = "muse_last_good_canceled"
	// CodeLastGoodInvalid refuses a malformed last-good input: an
	// unusable store binding or a success value that does not parse.
	CodeLastGoodInvalid = "muse_last_good_invalid"
	// CodeLastGoodUnsupportedPlatform refuses last-good receipts where
	// the host cannot report file identity or hold advisory locks.
	CodeLastGoodUnsupportedPlatform = "muse_last_good_unsupported_platform"
	// CodeLastGoodAbsent reports a last-good read with no receipt on
	// disk. Only a missing file reads absent.
	CodeLastGoodAbsent = "muse_last_good_absent"
	// CodeLastGoodUnreadable refuses a corrupt, unreadable or
	// disagreeing last-good receipt. It never resets to absent and
	// never invents a target.
	CodeLastGoodUnreadable = "muse_last_good_unreadable"
	// CodeLastGoodNotStartup refuses a receipt admission without
	// observed startup readiness. A probe result alone, however
	// successful, is not a successful start.
	CodeLastGoodNotStartup = "muse_last_good_not_startup"
	// CodeLastGoodUnverified refuses a receipt admission for bytes
	// that do not verify now: missing or disagreeing probe state,
	// state that is not PROBE_OK, or frozen bytes that disagree.
	CodeLastGoodUnverified = "muse_last_good_unverified"
	// CodeLastGoodLockFailed refuses when the maintenance lease or
	// the metadata lock cannot be acquired. The cause tells which one.
	CodeLastGoodLockFailed = "muse_last_good_lock_failed"
	// CodeLastGoodPublicationFailed refuses when a success receipt
	// cannot be persisted. The host does not acknowledge startup and
	// the prior durable receipt stays where it was.
	CodeLastGoodPublicationFailed = "muse_last_good_publication_failed"
	// CodeLastGoodPublicationUnknown reports a rename that succeeded
	// followed by a directory sync that did not: durability is
	// UNKNOWN, not a claim that the old record is intact.
	CodeLastGoodPublicationUnknown = "muse_last_good_publication_unknown"
)

// Startup surfaces. A success names the surface that observed
// readiness; the receipt says the build last started on the recorded
// surface, not that it is proven on every surface.
const (
	// SurfaceMSP names a managed-server-protocol startup.
	SurfaceMSP = "msp"
	// SurfacePTY names a terminal-session startup.
	SurfacePTY = "pty"
)

// StartSuccess is one observed successful startup, created only by the
// host's startup acceptance path with its attempt identity and observed
// evidence, never from a supplied success bool. MSP acceptance is the
// initialize handshake plus session establishment; PTY acceptance is
// the PID-bound log plus the validated startup prefix. No --version or
// --help result alone counts.
type StartSuccess struct {
	// Frozen is the exact tuple that started.
	Frozen FrozenIdentity
	// AttemptID names the startup attempt.
	AttemptID string
	// Surface names the observing surface: msp or pty.
	Surface string
	// ReadyAt is the observed readiness timestamp in RFC3339. Empty
	// means no startup was observed, however the probe went.
	ReadyAt string
	// By names the admitting path.
	By string
}

// FrozenReceipt is the frozen tuple named by a last-good receipt.
type FrozenReceipt struct {
	Build  string `json:"build"`
	SHA256 string `json:"sha256"`
	Path   string `json:"path"`
}

// LastGood is the durable last-good receipt: the latest durably
// admitted successful startup, ordered by its admission sequence. The
// host records ReadyAt when it observes readiness; while holding the
// bounded metadata lock it allocates the next sequence, records
// AdmittedAt and atomically publishes before returning startup
// success. An earlier ready child whose receipt is admitted later is
// explicitly the later admitted success.
type LastGood struct {
	Schema     int           `json:"schema"`
	Sequence   uint64        `json:"sequence"`
	Frozen     FrozenReceipt `json:"frozen"`
	AttemptID  string        `json:"attempt_id"`
	Surface    string        `json:"surface"`
	By         string        `json:"by"`
	ReadyAt    string        `json:"ready_at"`
	AdmittedAt string        `json:"admitted_at"`
}

// LastGoodError is a typed last-good refusal: the stage that refused
// and the machine-readable reason, with the underlying cause chained.
type LastGoodError struct {
	Stage string
	Code  string
	Cause error
}

// Error renders the refusal with its stage and code.
func (e *LastGoodError) Error() string {
	if e == nil {
		return "muse last-good error"
	}
	if e.Cause != nil {
		return "muse last-good " + e.Stage + " " + e.Code + ": " + e.Cause.Error()
	}
	return "muse last-good " + e.Stage + " " + e.Code
}

// Unwrap returns the underlying cause.
func (e *LastGoodError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// AsLastGoodError matches a LastGoodError in err's chain for code and
// stage assertions.
func AsLastGoodError(err error) (*LastGoodError, bool) {
	var refusal *LastGoodError
	if errors.As(err, &refusal) {
		return refusal, true
	}
	return nil, false
}

const (
	lastGoodFileName = "last-good.json"
	lastGoodSchema   = 1
	lastGoodMaxBytes = 64 * 1024
)

func lastGoodRefuseIfCanceled(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodCanceled, Cause: err}
	}
	return nil
}

func lastGoodRefusal(ctx context.Context, code string, cause error) error {
	if err := lastGoodRefuseIfCanceled(ctx); err != nil {
		return err
	}
	if existing, ok := AsLastGoodError(cause); ok && existing.Code == CodeLastGoodCanceled {
		return cause
	}
	return &LastGoodError{Stage: StageLastGood, Code: code, Cause: cause}
}

// validateStartSuccess refuses a success value admission cannot bind
// to an observed startup. Readiness is the admission key: without an
// observed timestamp there was no successful start, only a probe.
func validateStartSuccess(root string, success StartSuccess) error {
	if err := validateFrozenReference(root, success.Frozen); err != nil {
		return err
	}
	if !validAttemptToken(success.AttemptID) {
		return errors.New("attempt identity is not a plain token")
	}
	if success.Surface != SurfaceMSP && success.Surface != SurfacePTY {
		return fmt.Errorf("surface %q is not msp or pty", success.Surface)
	}
	if _, err := time.Parse(time.RFC3339, success.ReadyAt); err != nil {
		return errNotStartup
	}
	if success.By == "" {
		return errors.New("admitting path is empty")
	}
	return nil
}

// validateLastGoodDocument refuses a receipt that does not describe
// one admitted successful startup.
func validateLastGoodDocument(receipt LastGood) error {
	if receipt.Schema != lastGoodSchema {
		return fmt.Errorf("schema is %d, want %d", receipt.Schema, lastGoodSchema)
	}
	if receipt.Sequence == 0 {
		return errors.New("sequence is zero")
	}
	if _, err := muse.ParseBuildID(receipt.Frozen.Build); err != nil {
		return fmt.Errorf("build %q does not parse: %w", receipt.Frozen.Build, err)
	}
	if !isLowerHex64(receipt.Frozen.SHA256) {
		return errors.New("sha256 is not 64 lowercase hex")
	}
	if receipt.Frozen.Path == "" {
		return errors.New("frozen path is empty")
	}
	if !validAttemptToken(receipt.AttemptID) {
		return errors.New("attempt identity is not a plain token")
	}
	if receipt.Surface != SurfaceMSP && receipt.Surface != SurfacePTY {
		return fmt.Errorf("surface %q is not msp or pty", receipt.Surface)
	}
	if receipt.By == "" {
		return errors.New("admitting path is empty")
	}
	if _, err := time.Parse(time.RFC3339, receipt.ReadyAt); err != nil {
		return fmt.Errorf("ready_at does not parse: %w", err)
	}
	if _, err := time.Parse(time.RFC3339, receipt.AdmittedAt); err != nil {
		return fmt.Errorf("admitted_at does not parse: %w", err)
	}
	return nil
}

// readLastGoodDocument reads the last-good receipt through the held
// root. Absence of the file is (zero, false, nil); every other
// failure is an error, never absence. The read is a no-follow,
// nonblocking, size-bounded, owner-checked descriptor read.
func readLastGoodDocument(ctx context.Context, root *pinnedDir) (LastGood, bool, error) {
	if err := ctx.Err(); err != nil {
		return LastGood{}, false, err
	}
	file, err := root.openChildFile(lastGoodFileName)
	if os.IsNotExist(err) {
		return LastGood{}, false, nil
	}
	if err != nil {
		return LastGood{}, false, err
	}
	defer file.Close()
	raw, info, _, err := readMetadataFD(ctx, file, lastGoodMaxBytes, true)
	if err != nil {
		return LastGood{}, false, err
	}
	if info.Mode().Perm() != 0o600 {
		return LastGood{}, false, fmt.Errorf("last-good mode is %s", info.Mode())
	}
	receipt, err := lastGoodDocument.parse(raw)
	return receipt, err == nil, err
}

// acquireLastGoodStateLock serializes last-good admission through the
// attempt's held root. One bounded wait, no retry: contention refuses
// so the admission path can never wait indefinitely.
func acquireLastGoodStateLock(ctx context.Context, cfg HostConfig) (func() error, error) {
	release, err := acquireStateLockAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok && ferr.Code == CodeFreezeCanceled {
			return nil, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodCanceled, Cause: err}
		}
		return nil, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodLockFailed, Cause: err}
	}
	return release, nil
}

// WriteLastGood durably admits one observed successful startup as the
// new last-good receipt. Admission holds the shared maintenance lease
// while it verifies the frozen bytes and the metadata lock across the
// sequence allocation and atomic publication, in that order. Only an
// actual launch success writes: probe results, unverified bytes and
// failures never reach the receipt. Any failure preserves the prior
// receipt; no host failure clears it.
func WriteLastGood(ctx context.Context, cfg HostConfig, success StartSuccess) (LastGood, error) {
	if err := lastGoodRefuseIfCanceled(ctx); err != nil {
		return LastGood{}, err
	}
	if !supportedPlatform {
		return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodUnsupportedPlatform}
	}
	if err := validateStoreRoot(cfg.FrozenRoot); err != nil {
		return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodInvalid, Cause: err}
	}
	if err := validateStartSuccess(cfg.FrozenRoot, success); err != nil {
		if errors.Is(err, errNotStartup) {
			return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodNotStartup, Cause: fmt.Errorf("startup %s carries no observed readiness", success.AttemptID)}
		}
		return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodInvalid, Cause: err}
	}
	store, err := openFrozenStore(ctx, cfg.FrozenRoot)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnverified, err)
	}
	defer store.close()
	cfg.store = store
	releaseLease, err := acquireMaintenanceLeaseAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok && ferr.Code == CodeFreezeCanceled {
			return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodCanceled, Cause: err}
		}
		return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodLockFailed, Cause: err}
	}
	defer func() { _ = releaseLease() }()
	key := freezeKey(success.Frozen.Build, success.Frozen.SHA256)
	state, present, err := readBuildStateAt(ctx, cfg.store.meta, key)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnverified, err)
	}
	if !present {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnverified, fmt.Errorf("build %s carries no probe state", key))
	}
	if err := state.agreesWith(key, success.Frozen); err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnverified, err)
	}
	if state.State != StateProbeOK {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnverified, fmt.Errorf("build %s is %s, not PROBE_OK", key, state.State))
	}
	if _, err := verifyFrozenTuple(ctx, cfg.store, key, success.Frozen); err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnverified, err)
	}
	releaseState, err := acquireLastGoodStateLock(ctx, cfg)
	if err != nil {
		return LastGood{}, err
	}
	defer func() { _ = releaseState() }()
	current, present, err := readLastGoodDocument(ctx, cfg.store.root)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, err)
	}
	sequence := uint64(1)
	if present {
		existingKey := freezeKey(current.Frozen.Build, current.Frozen.SHA256)
		if expected := frozenBinaryPath(cfg.FrozenRoot, existingKey, current.Frozen.Build); current.Frozen.Path != expected {
			return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, fmt.Errorf("existing receipt path %q is not the %s copy", current.Frozen.Path, existingKey))
		}
		sequence = current.Sequence + 1
	}
	receipt := LastGood{
		Schema:   lastGoodSchema,
		Sequence: sequence,
		Frozen: FrozenReceipt{
			Build:  success.Frozen.Build,
			SHA256: success.Frozen.SHA256,
			Path:   success.Frozen.Path,
		},
		AttemptID:  success.AttemptID,
		Surface:    success.Surface,
		By:         success.By,
		ReadyAt:    success.ReadyAt,
		AdmittedAt: cfg.clock()().UTC().Format(time.RFC3339),
	}
	raw, err := lastGoodDocument.encode(receipt)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodInvalid, err)
	}
	if err := atomicDocumentAt(ctx, cfg.store.root, lastGoodFileName, ".lastgood-", raw); err != nil {
		if errors.Is(err, errPublicationUnknown) {
			return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodPublicationUnknown, Cause: err}
		}
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodPublicationFailed, err)
	}
	readBack, present, err := readLastGoodDocument(ctx, cfg.store.root)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodPublicationFailed, err)
	}
	if !present {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodPublicationFailed, errors.New("published receipt vanished before read-back"))
	}
	if readBack != receipt {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodPublicationFailed, errors.New("published receipt does not match the admitted startup"))
	}
	return receipt, nil
}

// ReadLastGood reads and validates the durable last-good receipt: the
// document itself, its frozen-root containment, and the frozen bytes
// with their PROBE_OK state under the shared maintenance lease. Only
// a missing file reads absent; every other failure is unreadable,
// never a silent reset and never an invented target.
func ReadLastGood(ctx context.Context, cfg HostConfig) (LastGood, error) {
	if err := lastGoodRefuseIfCanceled(ctx); err != nil {
		return LastGood{}, err
	}
	if !supportedPlatform {
		return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodUnsupportedPlatform}
	}
	if err := validateStoreRoot(cfg.FrozenRoot); err != nil {
		return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodInvalid, Cause: err}
	}
	store, err := openFrozenStore(ctx, cfg.FrozenRoot)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, err)
	}
	defer store.close()
	cfg.store = store
	receipt, present, err := readLastGoodDocument(ctx, cfg.store.root)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, err)
	}
	if !present {
		return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodAbsent, Cause: fmt.Errorf("no last-good receipt recorded")}
	}
	key := freezeKey(receipt.Frozen.Build, receipt.Frozen.SHA256)
	if expected := frozenBinaryPath(cfg.FrozenRoot, key, receipt.Frozen.Build); receipt.Frozen.Path != expected {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, fmt.Errorf("receipt path %q is not the %s copy", receipt.Frozen.Path, key))
	}
	releaseLease, err := acquireMaintenanceLeaseAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok && ferr.Code == CodeFreezeCanceled {
			return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodCanceled, Cause: err}
		}
		return LastGood{}, &LastGoodError{Stage: StageLastGood, Code: CodeLastGoodLockFailed, Cause: err}
	}
	defer func() { _ = releaseLease() }()
	state, present, err := readBuildStateAt(ctx, cfg.store.meta, key)
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, err)
	}
	if !present {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, fmt.Errorf("receipt %s carries no probe state", key))
	}
	if state.SHA256 != receipt.Frozen.SHA256 {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, fmt.Errorf("last-good digest disagrees with probe state"))
	}
	if err := state.agreesWith(key, FrozenIdentity{Build: receipt.Frozen.Build, SHA256: receipt.Frozen.SHA256}); err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, err)
	}
	if state.State != StateProbeOK {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, fmt.Errorf("receipt %s is %s, not PROBE_OK", key, state.State))
	}
	frozen, err := verifyFrozenTuple(ctx, cfg.store, key, FrozenIdentity{Build: receipt.Frozen.Build, SHA256: receipt.Frozen.SHA256})
	if err != nil {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, err)
	}
	if frozen.Path != receipt.Frozen.Path {
		return LastGood{}, lastGoodRefusal(ctx, CodeLastGoodUnreadable, fmt.Errorf("verified path %q disagrees with receipt %q", frozen.Path, receipt.Frozen.Path))
	}
	return receipt, nil
}
