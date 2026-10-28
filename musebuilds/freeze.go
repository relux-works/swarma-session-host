package musebuilds

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Freeze stages. Every refusal names its stage so a caller can tell
// where a typed reason came from without matching prose.
const (
	StageAdmit   = "admit"
	StageDedupe  = "dedupe"
	StageCopy    = "copy"
	StagePublish = "publish"
)

// Freeze refusal codes. A failed, partial or malformed read is never
// a legitimate absence: each code below refuses a distinct fact.
const (
	// CodeFreezeCanceled refuses a freeze whose context ended.
	CodeFreezeCanceled = "muse_freeze_canceled"
	// CodeFreezeInvalid refuses a HostConfig or Discovery value
	// freeze cannot honestly bind to bytes.
	CodeFreezeInvalid = "muse_freeze_invalid_config"
	// CodeFreezeUnsupportedPlatform refuses freeze where the host
	// cannot report file identity or hold advisory locks.
	CodeFreezeUnsupportedPlatform = "muse_freeze_unsupported_platform"
	// CodeFreezeLockFailed refuses when the global admission lock
	// cannot be acquired, is lost mid-section, or cannot be
	// released.
	CodeFreezeLockFailed = "muse_freeze_lock_failed"
	// CodeFreezeLeaseFailed refuses when the per-user maintenance
	// lease or metadata lock cannot be acquired. The stage tells
	// which one.
	CodeFreezeLeaseFailed = "muse_freeze_lease_failed"
	// CodeRampHold refuses a freeze while the host ramp reads HOLD.
	CodeRampHold = "muse_ramp_hold"
	// CodeRampUnknown refuses a freeze the ramp cannot vouch for:
	// a missing, unreadable, malformed or stale signal. Unknown is
	// not CLEAR.
	CodeRampUnknown = "muse_ramp_unknown"
	// CodeFreezeUnavailable refuses when no new freeze is possible
	// and no deferral target exists: an unprovisioned, unreadable
	// or corrupt ledger, a backward clock, a spent lifetime tuple
	// reservation, or an unexpected store shape. Nothing runs.
	CodeFreezeUnavailable = "muse_freeze_unavailable"
	// CodeDeferralHostCap is the Amendment 2 runaway-guard
	// warning: the newest build cannot freeze under today's cap,
	// so the already-frozen newest runs instead. The error carries
	// that frozen identity; nothing unfrozen runs.
	CodeDeferralHostCap = "muse_newest_deferred_host_cap"
	// CodeSourceQuarantined refuses a source carrying a Gatekeeper
	// quarantine xattr (G2 A1). Early refusal reserves nothing; a
	// copy-stage refusal retains its spent reservation and staging.
	CodeSourceQuarantined = "muse_source_quarantined"
	// CodeSourceUnusable refuses a source that fails the install
	// gates or disagrees with the discovery digest.
	CodeSourceUnusable = "muse_source_unusable"
	// CodeFrozenCollision refuses mismatching content at a frozen
	// key. It is never overwritten, deleted or retried in a loop.
	CodeFrozenCollision = "muse_frozen_collision"
	// CodeStateInconsistent refuses corrupt, unreadable or
	// disagreeing frozen metadata. It is not absence and never
	// authorizes a fresh copy over the old one.
	CodeStateInconsistent = "muse_state_inconsistent"
	// CodeProbeContractMismatch refuses metadata written under a
	// probe contract this binary does not speak. Migration is an
	// explicit reviewed operation, not a launch-time reinterpretation.
	CodeProbeContractMismatch = "muse_probe_contract_mismatch"
	// CodeRecordedBad returns the recorded deterministic verdict
	// for this exact build instead of a runnable copy. The error
	// carries the verdict for the section-5 controller.
	CodeRecordedBad = "muse_recorded_bad"
	// CodeFreezeCopyFailed refuses when the copy cannot be
	// completed and verified. The reservation outcome is recorded
	// as failed and the staging is retained, never deleted.
	CodeFreezeCopyFailed = "muse_freeze_copy_failed"
	// CodeFreezePublicationFailed refuses when verified bytes
	// cannot be published with their metadata. The prior record
	// stays intact.
	CodeFreezePublicationFailed = "muse_freeze_publication_failed"
	// CodeFreezePublicationUnknown reports a rename that succeeded
	// followed by a directory sync that did not: durability is
	// UNKNOWN, not a claim that the old record is intact.
	CodeFreezePublicationUnknown = "muse_freeze_publication_unknown"
)

// Freeze envelope and lock caps from design section 3.4: the whole
// selection-to-publication attempt runs under the caller's context
// capped at the envelope, and each local lock waits at most its cap.
const (
	freezeEnvelope   = 300 * time.Second
	leaseWaitCap     = 2 * time.Second
	stateLockWaitCap = 2 * time.Second
)

var (
	errInvalidFrozenRoot      = errors.New("frozen root must be an absolute path")
	errInvalidLedgerDir       = errors.New("ledger directory must be an absolute path")
	errInvalidRampState       = errors.New("ramp state path must be an absolute path")
	errInvalidRampLog         = errors.New("ramp log path must be an absolute path")
	errInvalidDayCap          = errors.New("the daily cap must not be negative")
	errInvalidHostName        = errors.New("the ledger host name must not be empty")
	errInvalidUserName        = errors.New("the ledger user name must not be empty")
	errMissingLocker          = errors.New("the global admission lock is required")
	errInvalidDiscoveryBuild  = errors.New("the discovery build does not parse")
	errInvalidDiscoveryDigest = errors.New("the discovery digest is not 64 lowercase hex")
	errInvalidDiscoveryPath   = errors.New("the discovery source path must be absolute")
	errNoAccountName          = errors.New("no account name for the ledger reservation")
)

// FreezeError is a typed freeze refusal: the stage that refused and
// the machine-readable reason, with the underlying cause chained.
// Frozen carries the deferral target and is set only with
// CodeDeferralHostCap; Bad carries the recorded verdict and is set
// only with CodeRecordedBad.
type FreezeError struct {
	Stage  string
	Code   string
	Cause  error
	Frozen *FrozenIdentity
	Bad    *BadVerdict
}

// Error renders the refusal with its stage and code.
func (e *FreezeError) Error() string {
	if e == nil {
		return "muse freeze error"
	}
	if e.Cause != nil {
		return "muse freeze " + e.Stage + " " + e.Code + ": " + e.Cause.Error()
	}
	return "muse freeze " + e.Stage + " " + e.Code
}

// Unwrap returns the underlying cause.
func (e *FreezeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// AsFreezeError matches a FreezeError in err's chain for code and
// stage assertions.
func AsFreezeError(err error) (*FreezeError, bool) {
	var refusal *FreezeError
	if errors.As(err, &refusal) {
		return refusal, true
	}
	return nil, false
}

// CopyOutcome describes how PublishFrozen produced its frozen copy.
// Only OutcomeReused is runnable; every other outcome leaves the
// first probe to pass 2b.
type CopyOutcome string

const (
	// OutcomeReused returns a deduplicated PROBE_OK copy. No
	// admission was consumed.
	OutcomeReused CopyOutcome = "reused"
	// OutcomeResumed returns an already-published COPIED copy. The
	// probe resumes in pass 2b without another copy or reservation.
	OutcomeResumed CopyOutcome = "resumed"
	// OutcomeAdopted publishes the missing COPIED state for
	// complete verified bytes left by a crash. Nothing was copied
	// or billed again.
	OutcomeAdopted CopyOutcome = "adopted"
	// OutcomePublished copies and publishes a new frozen copy as
	// COPIED. The first probe runs in pass 2b.
	OutcomePublished CopyOutcome = "published"
)

// FrozenCopy is one frozen build plus how it was produced.
type FrozenCopy struct {
	Frozen  FrozenIdentity
	Outcome CopyOutcome
}

// Runnable reports whether the copy may run: only a deduplicated
// PROBE_OK copy. A hash match alone never returns runnable.
func (c FrozenCopy) Runnable() bool {
	return c.Outcome == OutcomeReused
}

// Freeze hook points. Hooks are synchronous, nil in production, and
// reset by test cleanup. They coordinate real filesystem changes,
// never substitute freeze results.
type freezeInterposePoint int

const (
	freezeAfterDedupe freezeInterposePoint = iota
	freezeAfterLock
	freezeAfterRamp
	freezeAfterReserve
	freezeBeforeCopy
	freezeAfterCopyStream
	freezeAfterXattrCopy
	freezeBeforePublish
	freezeAfterPublish
	freezeLeaseAcquired
	freezeCopyChunk
)

// FreezeHook identifies the freeze attempt a hook fires for. Lease
// hooks fire without an attempt and carry the zero value.
type FreezeHook struct {
	closedWriter *os.File // the original writer is closed before publication
	stagedFile   *os.File // observation only; nil outside the publication hook
	Build        string
	SHA256       string
	Transaction  string
}

// freezeInterposeForTest is the freeze coordination hook. It is nil
// in production; tests arm it to flip the ramp, corrupt staging or
// observe lock and copy ordering.
var freezeInterposeForTest func(freezeInterposePoint, FreezeHook)

func freezeInterpose(point freezeInterposePoint, hook FreezeHook) {
	if freezeInterposeForTest != nil {
		freezeInterposeForTest(point, hook)
	}
}

func hookFor(d Discovery, txn string) FreezeHook {
	return FreezeHook{Build: d.Identity.Build, SHA256: d.Identity.SHA256, Transaction: txn}
}

func freezeRefuseIfCanceled(ctx context.Context, stage string) error {
	if err := ctx.Err(); err != nil {
		return &FreezeError{Stage: stage, Code: CodeFreezeCanceled, Cause: err}
	}
	return nil
}

func freezeRefusal(ctx context.Context, stage, code string, cause error) error {
	if err := freezeRefuseIfCanceled(ctx, stage); err != nil {
		return err
	}
	if existing, ok := AsFreezeError(cause); ok && existing.Code == CodeFreezeCanceled {
		return cause
	}
	return &FreezeError{Stage: stage, Code: code, Cause: cause}
}

// PublishFrozen publishes the discovery's frozen copy: dedupe first,
// then host admission (global lock, ramp CLEAR under the lock, durable
// day reservation), then a verified copy published as COPIED with its
// authoritative state. It never executes the copy — the first probe
// belongs to pass 2b — and it never deletes anything.
//
// The global lock is always acquired before the per-user maintenance
// lease; reviewed cleanup (L5) must take the same order, never the
// reverse. Nothing is retried: one attempt runs under the caller's
// context capped at the freeze envelope, and cancellation ends the
// attempt without starting another step.
func PublishFrozen(ctx context.Context, cfg HostConfig, d Discovery) (FrozenCopy, error) {
	ctx, cancel := context.WithTimeout(ctx, freezeEnvelope)
	defer cancel()
	if err := freezeRefuseIfCanceled(ctx, StageAdmit); err != nil {
		return FrozenCopy{}, err
	}
	if err := validateHostConfig(cfg); err != nil {
		return FrozenCopy{}, err
	}
	if err := validateDiscoveryInput(d); err != nil {
		return FrozenCopy{}, err
	}
	if !supportedPlatform {
		return FrozenCopy{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeUnsupportedPlatform}
	}
	store, err := openFrozenStore(ctx, cfg.FrozenRoot)
	if err != nil {
		return FrozenCopy{}, freezeRefusal(ctx, StageDedupe, CodeFreezeUnavailable, err)
	}
	defer store.close()
	cfg.store = store
	dedupe, err := dedupeFrozen(ctx, cfg, d)
	if err != nil {
		return FrozenCopy{}, err
	}
	if dedupe != nil {
		return *dedupe, nil
	}
	freezeInterpose(freezeAfterDedupe, hookFor(d, ""))
	if err := freezeRefuseIfCanceled(ctx, StageAdmit); err != nil {
		return FrozenCopy{}, err
	}
	quarantined, err := cfg.quarantineCheck(d.Identity.Path)
	if err != nil {
		return FrozenCopy{}, freezeRefusal(ctx, StageCopy, CodeSourceUnusable, err)
	}
	if quarantined {
		return FrozenCopy{}, &FreezeError{Stage: StageCopy, Code: CodeSourceQuarantined, Cause: fmt.Errorf("%s carries a quarantine xattr", d.Identity.Path)}
	}
	if err := checkInstallSource(d.Identity.Path); err != nil {
		return FrozenCopy{}, freezeRefusal(ctx, StageCopy, CodeSourceUnusable, err)
	}
	return admitAndCopy(ctx, cfg, d)
}

// dedupeFrozen returns a non-nil copy when the frozen store already
// answers this discovery, or nil when a new copy is needed. It reads
// valid state first and validates the full frozen tuple; a hash
// match alone never returns a runnable build, and BAD returns the
// recorded verdict on every path, not only when no bytes exist.
func dedupeFrozen(ctx context.Context, cfg HostConfig, d Discovery) (*FrozenCopy, error) {
	if err := freezeRefuseIfCanceled(ctx, StageDedupe); err != nil {
		return nil, err
	}
	key := freezeKey(d.Identity.Build, d.Identity.SHA256)
	state, present, err := readBuildStateAt(ctx, cfg.store.meta, key)
	if err != nil {
		if errors.Is(err, errProbeContractMismatch) {
			return nil, &FreezeError{Stage: StageDedupe, Code: CodeProbeContractMismatch, Cause: err}
		}
		return nil, freezeRefusal(ctx, StageDedupe, CodeStateInconsistent, err)
	}
	if present {
		if err := state.agreesWith(key, d.Identity); err != nil {
			return nil, freezeRefusal(ctx, StageDedupe, CodeStateInconsistent, err)
		}
		if state.State == StateBAD {
			verdict := state.Verdict
			return nil, &FreezeError{Stage: StageDedupe, Code: CodeRecordedBad, Cause: fmt.Errorf("build %s is recorded %s", d.Identity.Build, verdict.Code), Bad: verdict}
		}
		// The shared maintenance lease precedes every frozen-byte
		// verification (design 3.5), on the reuse/resume path as on
		// every other: reviewed cleanup cannot overlap this read.
		releaseLease, err := acquireMaintenanceLeaseAt(ctx, cfg.store.root)
		if err != nil {
			if ferr, ok := AsFreezeError(err); ok {
				return nil, ferr
			}
			return nil, freezeRefusal(ctx, StageDedupe, CodeFreezeLeaseFailed, err)
		}
		defer func() { _ = releaseLease() }()
		frozen, err := verifyFrozenTuple(ctx, cfg.store, key, d.Identity)
		if err != nil {
			return nil, freezeRefusal(ctx, StageDedupe, CodeStateInconsistent, err)
		}
		if state.State == StateProbeOK {
			return &FrozenCopy{Frozen: frozen, Outcome: OutcomeReused}, nil
		}
		return &FrozenCopy{Frozen: frozen, Outcome: OutcomeResumed}, nil
	}
	// The absent-state byte lookup holds the lease too: adoption
	// runs under it, and the state lock below serializes the
	// publication.
	releaseLease, err := acquireMaintenanceLeaseAt(ctx, cfg.store.root)
	if err != nil {
		if ferr, ok := AsFreezeError(err); ok {
			return nil, ferr
		}
		return nil, freezeRefusal(ctx, StageDedupe, CodeFreezeLeaseFailed, err)
	}
	defer func() { _ = releaseLease() }()
	bytesPresent, err := frozenBytesPresent(ctx, cfg.store, key, d.Identity)
	if err != nil {
		return nil, freezeRefusal(ctx, StageDedupe, CodeFrozenCollision, err)
	}
	if !bytesPresent {
		return nil, nil
	}
	adopted, err := adoptPublishedCopy(ctx, cfg, d, key)
	if err != nil {
		return nil, err
	}
	return adopted, nil
}

// admitAndCopy runs host admission and the new copy under the global
// lock: ramp CLEAR, durable reservation before the first byte, a
// second ramp read immediately before copying, then the verified
// copy and its publication. The reservation is created before any
// byte is copied; it counts even when the copy later fails.
func admitAndCopy(ctx context.Context, cfg HostConfig, d Discovery) (FrozenCopy, error) {
	held, err := cfg.Locker.Acquire(ctx)
	if err != nil {
		if cerr := freezeRefuseIfCanceled(ctx, StageAdmit); cerr != nil {
			return FrozenCopy{}, cerr
		}
		return FrozenCopy{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeLockFailed, Cause: err}
	}
	copyResult, err := admitAndCopyHeld(ctx, cfg, d, held)
	releaseErr := held.Release()
	if releaseErr != nil {
		return FrozenCopy{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeLockFailed, Cause: releaseErr}
	}
	if err != nil {
		return FrozenCopy{}, err
	}
	return copyResult, nil
}

func admitAndCopyHeld(ctx context.Context, cfg HostConfig, d Discovery, held HostLockHeld) (FrozenCopy, error) {
	freezeInterpose(freezeAfterLock, hookFor(d, ""))
	if err := held.Check(); err != nil {
		return FrozenCopy{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeLockFailed, Cause: err}
	}
	if err := requireRampClear(ctx, cfg); err != nil {
		return FrozenCopy{}, err
	}
	freezeInterpose(freezeAfterRamp, hookFor(d, ""))
	if err := held.Check(); err != nil {
		return FrozenCopy{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeLockFailed, Cause: err}
	}
	// A waiter that observed absence before the lock re-checks under
	// it: the winner may have published while this waiter queued. A
	// hit returns the existing copy without another reservation or
	// copy; a miss proceeds to reserve.
	if deduped, err := dedupeFrozen(ctx, cfg, d); err != nil {
		return FrozenCopy{}, err
	} else if deduped != nil {
		return *deduped, nil
	}
	reservation, deflected, err := reserveFreezeSlot(ctx, cfg, d)
	if err != nil {
		return FrozenCopy{}, err
	}
	if deflected {
		return deferToFrozen(ctx, cfg)
	}
	freezeInterpose(freezeAfterReserve, hookFor(d, reservation.Transaction))
	if err := held.Check(); err != nil {
		_ = recordReservationOutcome(cfg, reservation, outcomeFailed, "lock lost before copy")
		return FrozenCopy{}, &FreezeError{Stage: StageAdmit, Code: CodeFreezeLockFailed, Cause: err}
	}
	if err := requireRampClear(ctx, cfg); err != nil {
		_ = recordReservationOutcome(cfg, reservation, outcomeFailed, "ramp refused before copy")
		return FrozenCopy{}, err
	}
	return copyAndPublishHeld(ctx, cfg, d, held, reservation)
}

// reserveFreezeSlot inventories the ledger under the global lock and
// durably reserves this build's slot before the first byte copy. It
// reports deflected=true when the runaway guard trips and the caller
// must defer to the already-frozen newest instead of copying.
func reserveFreezeSlot(ctx context.Context, cfg HostConfig, d Discovery) (Reservation, bool, error) {
	if err := freezeRefuseIfCanceled(ctx, StageAdmit); err != nil {
		return Reservation{}, false, err
	}
	now := cfg.clock()().UTC()
	today := now.Format("2006-01-02")
	dayCount, existing, err := inventoryLedger(ctx, cfg.LedgerDir, today, d.Identity.Build, d.Identity.SHA256, cfg.Host)
	if err != nil {
		return Reservation{}, false, freezeRefusal(ctx, StageAdmit, CodeFreezeUnavailable, err)
	}
	if existing != nil {
		return *existing, false, existingReservationOutcome(*existing)
	}
	if cfg.MaxFreezesPerDay > 0 && dayCount >= cfg.MaxFreezesPerDay {
		return Reservation{}, true, nil
	}
	txn, err := newTransactionID()
	if err != nil {
		return Reservation{}, false, freezeRefusal(ctx, StageAdmit, CodeFreezeUnavailable, err)
	}
	reservation := Reservation{
		SchemaVersion: reservationSchema,
		Host:          cfg.Host,
		User:          cfg.User,
		Build:         d.Identity.Build,
		SHA256:        d.Identity.SHA256,
		Transaction:   txn,
		Day:           today,
		ReservedAt:    now.Format(time.RFC3339),
		Outcome:       outcomePending,
	}
	created, winner, err := createReservation(ctx, cfg.LedgerDir, reservation)
	if err != nil {
		return Reservation{}, false, freezeRefusal(ctx, StageAdmit, CodeFreezeUnavailable, err)
	}
	if !created {
		return winner, false, existingReservationOutcome(winner)
	}
	return reservation, false, nil
}

// existingReservationOutcome handles a reservation that already names
// this build today: no new copy may start under the spent slot. Bytes
// can only be adopted through the crash path, which dedupe owns; by
// the time admission sees the key, dedupe has already ruled adoption
// out, so every outcome here refuses without copying.
func existingReservationOutcome(existing Reservation) error {
	if existing.Outcome == outcomeFailed {
		return &FreezeError{Stage: StageAdmit, Code: CodeFreezeUnavailable, Cause: fmt.Errorf("reservation %s already failed: %s", existing.Transaction, existing.OutcomeDetail)}
	}
	return &FreezeError{Stage: StageAdmit, Code: CodeFreezeUnavailable, Cause: fmt.Errorf("reservation %s already holds this build", existing.Transaction)}
}

// deferToFrozen applies the Amendment 2 V-behaviour: the newest
// already-frozen build plus the typed host-cap warning. The global
// lock is released first — deferral reserves nothing — and the
// frozen bytes are read back under the shared maintenance lease,
// like every other executable-byte read.
func deferToFrozen(ctx context.Context, cfg HostConfig) (FrozenCopy, error) {
	target, err := selectDeferralTarget(ctx, cfg)
	if err != nil {
		return FrozenCopy{}, err
	}
	return FrozenCopy{}, &FreezeError{
		Stage:  StageAdmit,
		Code:   CodeDeferralHostCap,
		Cause:  fmt.Errorf("today's freeze cap is spent; newest already-frozen build %s stands in", target.Build),
		Frozen: &target,
	}
}

func newTransactionID() (string, error) {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return fmt.Sprintf("txn-%d-%d-%s", os.Getpid(), time.Now().UnixNano(), hex.EncodeToString(nonce)), nil
}

// fsyncForTest overrides every durability sync in freeze, ledger and
// state writes. It is nil in production; tests fail selected calls
// to prove sync errors are never claimed as durable.
var fsyncForTest func(f *os.File) error

func fsyncFile(f *os.File) error {
	if fsyncForTest != nil {
		return fsyncForTest(f)
	}
	return f.Sync()
}

func fsyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return fsyncFile(dir)
}

// frozenBinaryName is the versioned binary leaf inside a frozen key
// directory. The name grammar stays module-owned; only the prefix
// constant is shared with discovery.
func frozenBinaryName(build string) string {
	return pinnedNamePrefix + build
}

// freezeKey derives the content-addressed frozen directory key:
// the exact full build plus the full digest. Prefixes never address
// a key.
func freezeKey(build, sha256 string) string {
	return build + "-" + sha256
}

func buildsDir(root string) string         { return filepath.Join(root, "builds") }
func stagingDir(root string) string        { return filepath.Join(root, "staging") }
func metaDir(root string) string           { return filepath.Join(root, "meta") }
func frozenKeyDir(root, key string) string { return filepath.Join(buildsDir(root), key) }
func frozenBinaryPath(root, key, build string) string {
	return filepath.Join(frozenKeyDir(root, key), frozenBinaryName(build))
}
func statePath(root, key string) string {
	return filepath.Join(metaDir(root), key, "STATE.json")
}
