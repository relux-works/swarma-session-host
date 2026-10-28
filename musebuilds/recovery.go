package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

const (
	CodeFallbackFailed        = "muse_fallback_failed"
	CodeFallbackTargetChanged = "muse_fallback_target_changed"
	CodeFallbackWarning       = "muse_version_fallback_last_good"
)

// RecoveryMode is classified once by the consumer, never inferred here from
// stdin. An interactive decline, EOF or error cannot become unattended.
type RecoveryMode string

const (
	RecoveryInteractive RecoveryMode = "interactive"
	RecoveryUnattended  RecoveryMode = "unattended"
)

// RecoveryEvent is the durable, one-use warning/permit for this invocation.
// The consumer uses it for stderr/run-state/task-note delivery (L2/L3/L4).
type RecoveryEvent struct {
	Code           string        `json:"code"`
	State          string        `json:"state"`
	Newest         FrozenReceipt `json:"newest"`
	FailureAttempt string        `json:"failure_attempt"`
	FailureStage   string        `json:"failure_stage"`
	FailureCode    string        `json:"failure_code"`
	LastGood       LastGood      `json:"last_good"`
	Mode           RecoveryMode  `json:"mode"`
	Consent        bool          `json:"consent"`
}

// StartOutcome is the consumer's actual MSP/PTY startup observation. On
// failure Success is empty, Err is non-nil, and Cleanup reports real teardown.
type StartOutcome struct {
	Success StartSuccess
	Err     error
	Cleanup CleanupResult
}

// StartOutcomeReporter is the surface boundary, not an alternate selector.
// Offer must return explicit consent and honor its 60-second context. Start
// must honor its 45-second startup plus 5-second teardown context, use the
// supplied verified descriptor/tuple for all probes and the single startup,
// and return only after observing startup or teardown. It cannot select another
// build. Surface protocol parsing and production wiring belong to L2/L3.
type StartOutcomeReporter interface {
	Offer(context.Context, RecoveryEvent) (bool, error)
	Start(context.Context, RecoveryEvent, *os.File) StartOutcome
}

func validateRecoveryEvent(e RecoveryEvent) error {
	if e.Code != CodeFallbackWarning || !validAttemptToken(e.FailureAttempt) {
		return errors.New("invalid recovery evidence")
	}
	if e.Mode != RecoveryInteractive && e.Mode != RecoveryUnattended {
		return errors.New("invalid recovery mode")
	}
	if e.Mode == RecoveryInteractive && !e.Consent {
		return errors.New("interactive recovery has no consent")
	}
	if e.State != "fallback_attempting" && e.State != "fallback_started" && e.State != "fallback_failed" {
		return errors.New("invalid recovery state")
	}
	if e.Newest.Build == e.LastGood.Frozen.Build && e.Newest.SHA256 == e.LastGood.Frozen.SHA256 {
		return errors.New("recovery would repeat newest")
	}
	return validateLastGoodDocument(e.LastGood)
}

var recoveryDocument = documentCodec[RecoveryEvent]{128 * 1024, validateRecoveryEvent}

// Recover offers/automatically attempts the exact last-good once, only after
// this runner recorded a failed first probe and confirmed cleanup. The offered
// tuple is revalidated under the bounded state lock before durable consumption.
// A failed fallback returns primary then fallback errors and never clears the
// successful receipt. Consumers must keep using this runner after a refusal.
func (r *ProbeRunner) Recover(ctx context.Context, mode RecoveryMode, reporter StartOutcomeReporter) (RecoveryEvent, error) {
	if err := r.enter(ctx); err != nil {
		return RecoveryEvent{}, err
	}
	defer r.leave()
	f := r.failure
	if f == nil || r.recoveryUsed || reporter == nil || (mode != RecoveryInteractive && mode != RecoveryUnattended) {
		return RecoveryEvent{}, &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable}
	}
	receipt, err := ReadAttemptReceipt(ctx, f.cfg, f.result.Frozen, f.result.Observation.AttemptID)
	if err != nil || f.result.Record.Receipt == nil || receipt != *f.result.Record.Receipt || receipt.Cleanup != CleanupConfirmed {
		return RecoveryEvent{}, &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
	}
	good, err := ReadLastGood(ctx, f.cfg)
	if err != nil {
		return RecoveryEvent{}, &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
	}
	event := RecoveryEvent{Code: CodeFallbackWarning, State: "fallback_attempting", Newest: FrozenReceipt{Build: f.result.Frozen.Build, SHA256: f.result.Frozen.SHA256, Path: f.result.Frozen.Path}, FailureAttempt: receipt.AttemptID, FailureStage: StageProbeExecute, FailureCode: string(receipt.Outcome), LastGood: good, Mode: mode}
	if good.Frozen.Build == receipt.Build && good.Frozen.SHA256 == receipt.SHA256 {
		return event, &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable}
	}
	if mode == RecoveryInteractive {
		offerCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		consent, offerErr := reporter.Offer(offerCtx, event)
		expired := offerCtx.Err()
		cancel()
		if offerErr != nil || expired != nil || !consent {
			return event, &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: errors.Join(offerErr, expired)}
		}
		event.Consent = true
	}
	if err := probeRefuseIfCanceled(ctx); err != nil {
		return event, err
	}
	store, err := openFrozenStore(ctx, f.cfg.FrozenRoot)
	if err != nil {
		return event, probeRefusal(ctx, CodeRecoveryUnavailable, err)
	}
	defer store.close()
	release, err := acquireMaintenanceLeaseAt(ctx, store.root)
	if err != nil {
		return event, probeRefusal(ctx, CodeProbeLockFailed, err)
	}
	defer func() { _ = release() }()
	unlock, err := acquireStateLockAt(ctx, store.root)
	if err != nil {
		return event, probeRefusal(ctx, CodeProbeLockFailed, err)
	}
	// No metadata lock is held over an offer or startup. The use lease spans
	// verification, startup and successful last-good admission.
	consumed, file, leaf, err := consumeRecovery(ctx, store, f, event)
	_ = unlock()
	if err != nil {
		return event, err
	}
	event = consumed
	defer file.Close()
	defer leaf.close()
	r.recoveryUsed = true
	if err := probeRefuseIfCanceled(ctx); err != nil {
		return event, err
	}
	startCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
	outcome := reporter.Start(startCtx, event, file)
	deadlineErr := startCtx.Err()
	cancel()
	if outcome.Cleanup != CleanupConfirmed {
		r.mu.Lock()
		r.blocked = true
		r.mu.Unlock()
	}
	if deadlineErr != nil {
		outcome.Err = errors.Join(outcome.Err, deadlineErr)
	}
	if outcome.Cleanup != CleanupConfirmed {
		outcome.Err = errors.Join(outcome.Err, errors.New("fallback cleanup unconfirmed"))
	}
	if outcome.Err == nil {
		if outcome.Success.Frozen.Build != good.Frozen.Build || outcome.Success.Frozen.SHA256 != good.Frozen.SHA256 || outcome.Success.Frozen.Path != good.Frozen.Path {
			outcome.Err = errors.New("fallback success disagrees with the consented tuple")
		} else {
			_, outcome.Err = WriteLastGood(ctx, f.cfg, outcome.Success)
		}
	}
	if outcome.Err != nil {
		event.State = "fallback_failed"
	} else {
		event.State = "fallback_started"
	}
	publicationErr := publishRecovery(ctx, store.root, event, false)
	if outcome.Err != nil {
		return event, errors.Join(f.cause, &ProbeError{Stage: StageRecovery, Code: CodeFallbackFailed, Cause: outcome.Err}, publicationErr)
	}
	return event, publicationErr
}

func consumeRecovery(ctx context.Context, store *frozenStore, f *probeFailure, event RecoveryEvent) (RecoveryEvent, *os.File, *pinnedDir, error) {
	refuse := func(code string, err error) (RecoveryEvent, *os.File, *pinnedDir, error) {
		return event, nil, nil, &ProbeError{Stage: StageRecovery, Code: code, Cause: err}
	}
	current, present, err := readLastGoodDocument(ctx, store.root)
	if err != nil || !present {
		return refuse(CodeRecoveryUnavailable, err)
	}
	if current.Frozen != event.LastGood.Frozen || current.Sequence < event.LastGood.Sequence {
		return refuse(CodeFallbackTargetChanged, nil)
	}
	event.LastGood = current
	id := FrozenIdentity{Build: current.Frozen.Build, SHA256: current.Frozen.SHA256, Path: current.Frozen.Path}
	if err := validateFrozenReference(f.cfg.FrozenRoot, id); err != nil {
		return refuse(CodeRecoveryUnavailable, err)
	}
	key := freezeKey(id.Build, id.SHA256)
	state, present, err := readBuildStateAt(ctx, store.meta, key)
	if err != nil || !present || state.State != StateProbeOK || state.agreesWith(key, id) != nil {
		return refuse(CodeRecoveryUnavailable, err)
	}
	leaf, file, err := openFrozenBytes(store, key, id.Build, true)
	if err != nil {
		return refuse(CodeRecoveryUnavailable, err)
	}
	digest, err := hashFileDescriptor(ctx, StageRecovery, file)
	if err == nil && digest != id.SHA256 {
		err = errors.New("fallback bytes disagree with last-good")
	}
	if err == nil {
		err = publishRecovery(ctx, store.root, event, true)
	}
	if err != nil {
		_ = file.Close()
		_ = leaf.close()
		return refuse(CodeRecoveryUnavailable, err)
	}
	return event, file, leaf, nil
}

func publishRecovery(ctx context.Context, root *pinnedDir, event RecoveryEvent, first bool) error {
	name := "fallback-" + event.FailureAttempt + ".json"
	if first {
		exists, err := root.childExists(name)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("recovery attempt %s already consumed", event.FailureAttempt)
		}
	}
	raw, err := recoveryDocument.encode(event)
	if err != nil {
		return err
	}
	return atomicDocumentAt(ctx, root, name, ".fallback-", raw)
}
