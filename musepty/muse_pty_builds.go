package musepty

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	musebuilds "github.com/relux-works/swarma-session-host/musebuilds"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// This file is the L3P PTY integration: the newest-build controller
// path for StartMusePTYHost and ResumeMusePTYHost. The flow selects
// its binary only through musebuilds — newest selection, freeze,
// first probe, start — verifies the selected tuple immediately
// before every exec, reports the real start outcome, and runs an
// older build only through the L2C recovery permit (route i) or the
// host-cap deferral (route ii), with the L2M-style warning triple.
// Direct Start/Resume without bindings keep the legacy behavior the
// pre-L3P tests pin.

// MusePTYComposeLeg names one plan-composition leg for the outer
// composer: the authorized tuple the composed plan must serve.
type MusePTYComposeLeg string

const (
	// MusePTYComposeNewest composes the prepared newest leg.
	MusePTYComposeNewest MusePTYComposeLeg = "newest"
	// MusePTYComposeFallback composes the single permitted
	// last-good leg.
	MusePTYComposeFallback MusePTYComposeLeg = "fallback"
	// MusePTYComposeDeferred composes the authorized already-frozen
	// deferral leg.
	MusePTYComposeDeferred MusePTYComposeLeg = "deferred"
)

// MusePTYBuildsBinding carries the L2C controller bindings for one
// PTY launch or resume. Builds is the frozen-store host
// configuration, Runner the invocation probe runner (nil selects the
// production owned-group launcher), Env the explicit parent
// environment discovery resolves the launcher from (never the daemon
// ambient PATH), Mode the once-classified launch mode, and AttemptID
// the invocation attempt token (empty generates one). Offer presents
// the interactive fallback offer and DeferredOffer the interactive
// deferral offer; both are L4 surfaces the tests fake. Interactive
// launches require both. Compose builds one fresh-leg request per
// authorized tuple and Recompose one resume-leg seal plus process;
// the flow injects the tuple and authenticates the seal itself, so
// neither callback can select another build.
type MusePTYBuildsBinding struct {
	Builds        musebuilds.HostConfig
	Runner        *musebuilds.ProbeRunner
	Env           []string
	Mode          musebuilds.RecoveryMode
	AttemptID     string
	Offer         func(context.Context, musebuilds.RecoveryEvent) (bool, error)
	DeferredOffer func(context.Context, musebuilds.DeferralStart) (bool, error)
	// Warnings optionally exposes invocation state before a host exists.
	// Failures also return it through MusePTYRecoveryError; nil allocates one.
	Warnings  *MusePTYRecoveryWarnings
	Compose   func(context.Context, MusePTYComposeLeg, musebuilds.FrozenIdentity) (agentic.LaunchRequest, error)
	Recompose func(context.Context, musebuilds.FrozenIdentity, string) (agentic.Seal, agentic.Plan, error)
}

// Consumer stages and failure codes for newest-start reports.
const (
	musePTYStageLaunch   = "pty-launch"
	musePTYStageResume   = "pty-resume"
	musePTYStageSeal     = "seal-reprobe"
	musePTYStartupCode   = "pty_startup_error"
	musePTYSpawnCode     = "pty_spawn_error"
	musePTYSealProbeCode = "seal_probe_error"
	musePTYDetailCap     = 2048
)

// musePTYAttemptID mints one invocation attempt token: plain
// [a-z0-9-] text the controller accepts.
func musePTYAttemptID() string {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "pty-attempt"
	}
	return "pty-" + hex.EncodeToString(raw[:])
}

// musePTYFailureDetail bounds the diagnostic text a newest-leg report
// carries. The receipt codec caps detail well above this; truncation
// only bounds pathological transports.
func musePTYFailureDetail(err error) string {
	text := err.Error()
	if len(text) > musePTYDetailCap {
		text = text[:musePTYDetailCap]
	}
	return text
}

// musePTYLegOutcome is one PTY-leg attempt: the host on success, or
// the error with whether a child started and whether teardown was
// confirmed. Started distinguishes fork/exec failures from startup
// failures for the failure taxonomy.
type musePTYLegOutcome struct {
	Host    *musePTYHost
	Err     error
	Started bool
	Cleanup musebuilds.CleanupResult
}

// musePTYStopRetainedHost stops and reaps a host a post-start
// controller failure retained: a last-good admission or recovery
// publication that failed after the leg started. Stop reaps the
// child; without it the live host leaks past the returned error. A
// nil host stops nothing.
func musePTYStopRetainedHost(host *musePTYHost) error {
	if host == nil {
		return nil
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return host.Stop(stopCtx)
}

// musePTYComposeFreshLeg composes one fresh leg plan for the
// authorized tuple: the caller request must carry no conflicting
// tuple (a supplied tuple is an assertion, never a selection), the
// flow injects the authorized tuple itself, and the store binds for
// the pre-exec gate. A composition refusal (a non-Muse system, a
// trust or module-planning failure) is typed pre-attempt: no child
// ran, so the flow must report nothing and fall back nowhere — the
// same strict rule as L2M's pre-attempt refusal.
func musePTYComposeFreshLeg(req agentic.LaunchRequest, tuple musebuilds.FrozenIdentity, store musebuilds.HostConfig) (*MusePTYLaunchPlan, error) {
	supplied := musebuilds.FrozenIdentity{Build: req.FrozenToolBuild, SHA256: req.FrozenToolSHA256, Path: req.FrozenToolBinary}
	if supplied != (musebuilds.FrozenIdentity{}) && supplied != tuple {
		return nil, &MusePTYExecError{Leg: string(musePTYLegLaunch), Code: musePTYCodeInvalidConfig, Cause: fmt.Errorf("supplied frozen tuple %q is not the authorized %q", supplied.Path, tuple.Path)}
	}
	req.FrozenToolBinary, req.FrozenToolBuild, req.FrozenToolSHA256 = tuple.Path, tuple.Build, tuple.SHA256
	plan, err := BuildMusePTYLaunchPlan(req)
	if err != nil {
		return nil, &MusePTYExecError{Leg: string(musePTYLegLaunch), Code: musePTYCodeInvalidConfig, Cause: err}
	}
	plan.frozenStore = store
	return plan, nil
}

// musePTYTeardownOutcome stops a started leg host after a failed
// start and reports whether teardown was confirmed: Stop returns
// only after the child is reaped, so a return is confirmation and a
// caller cancellation first is not.
func musePTYTeardownOutcome(ctx context.Context, host *musePTYHost, err error) musePTYLegOutcome {
	cleanup := musebuilds.CleanupConfirmed
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- host.Stop(stopCtx) }()
	select {
	case stopErr := <-done:
		if stopErr != nil {
			cleanup = musebuilds.CleanupIncomplete
			err = errors.Join(err, stopErr)
		}
	case <-ctx.Done():
		cleanup = musebuilds.CleanupIncomplete
		err = errors.Join(err, ctx.Err())
	}
	return musePTYLegOutcome{Err: err, Started: true, Cleanup: cleanup}
}

// musePTYStartFreshLeg starts one authorized fresh leg and observes
// its real startup. A (nil, err) starter result is a fork/exec
// failure without a child; anything else started.
func musePTYStartFreshLeg(ctx context.Context, req agentic.LaunchRequest, tuple musebuilds.FrozenIdentity, store musebuilds.HostConfig) musePTYLegOutcome {
	plan, err := musePTYComposeFreshLeg(req, tuple, store)
	if err != nil {
		return musePTYLegOutcome{Err: err, Cleanup: musebuilds.CleanupConfirmed}
	}
	host, err := StartMusePTYHost(ctx, plan)
	if err != nil {
		if host == nil {
			return musePTYLegOutcome{Err: err, Cleanup: musebuilds.CleanupConfirmed}
		}
		return musePTYTeardownOutcome(ctx, host, err)
	}
	return musePTYLegOutcome{Host: host, Started: true, Cleanup: musebuilds.CleanupConfirmed}
}

// musePTYStartResumeLeg imports one authorized resume plan and starts
// it. The seal authenticates through FromImport against the
// authorized tuple; a stale seal refuses for outer recomposition.
func musePTYStartResumeLeg(ctx context.Context, seal agentic.Seal, process agentic.Plan, record sm.ProviderHostRecord, tuple musebuilds.FrozenIdentity, store musebuilds.HostConfig, keys []agentic.SealCommitmentKey) musePTYLegOutcome {
	plan, err := BuildMusePTYLaunchPlanFromImport(seal, process, tuple, keys...)
	if err != nil {
		return musePTYLegOutcome{Err: err, Cleanup: musebuilds.CleanupConfirmed}
	}
	plan.frozenStore = store
	host, err := ResumeMusePTYHost(ctx, plan, record)
	if err != nil {
		if host == nil {
			return musePTYLegOutcome{Err: err, Cleanup: musebuilds.CleanupConfirmed}
		}
		return musePTYTeardownOutcome(ctx, host, err)
	}
	return musePTYLegOutcome{Host: host, Started: true, Cleanup: musebuilds.CleanupConfirmed}
}

// musePTYResumeSessionID reads the durable native session identity
// the fallback recomposer resumes. It accepts the exact v1 bytes;
// anything else refuses before any permit issues.
func musePTYResumeSessionID(record sm.ProviderHostRecord) (string, error) {
	if record.Provider != "muse" || !museHasSnapshot(record.Snapshot) {
		return "", &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeInvalidConfig, Cause: fmt.Errorf("Muse resume record is required")}
	}
	prior, err := museRecordSnapshot(record)
	if err != nil {
		return "", &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeInvalidConfig, Cause: err}
	}
	if prior.Contract != "task-board.muse-pty-host-v1" || prior.SessionID == "" {
		return "", &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeInvalidConfig, Cause: fmt.Errorf("Muse provider snapshot carries no resumable native session")}
	}
	return prior.SessionID, nil
}

// musePTYFallbackReporter is the musepty StartOutcomeReporter: it
// presents the L4 fallback offer and runs the single exact-last-good
// leg. It cannot select another build: the tuple comes from the
// revalidated permit event, and fresh legs compose (with the
// caller-tuple assertion) while resume legs re-import (with the
// selection binding) under it.
type musePTYFallbackReporter struct {
	binding   *MusePTYBuildsBinding
	resume    bool
	sessionID string
	record    sm.ProviderHostRecord
	keys      []agentic.SealCommitmentKey

	startedHost *musePTYHost
}

var _ musebuilds.StartOutcomeReporter = (*musePTYFallbackReporter)(nil)

// Offer presents the interactive fallback offer through the L4
// surface. A missing surface declines fail-closed; the launch entry
// refuses interactive launches without one before selection.
func (r *musePTYFallbackReporter) Offer(ctx context.Context, event musebuilds.RecoveryEvent) (bool, error) {
	if r == nil || r.binding == nil || r.binding.Offer == nil {
		return false, errors.New("muse PTY newest-build launch carries no interactive offer surface")
	}
	return r.binding.Offer(ctx, event)
}

// Start runs the single permitted fallback leg and observes its real
// startup. The held descriptor must name the permitted tuple path;
// the tuple itself is verified again immediately before exec inside
// the shared leg starter.
func (r *musePTYFallbackReporter) Start(ctx context.Context, event musebuilds.RecoveryEvent, file *os.File) musebuilds.StartOutcome {
	fail := func(err error) musebuilds.StartOutcome {
		return musebuilds.StartOutcome{Err: err, Cleanup: musebuilds.CleanupConfirmed}
	}
	if file == nil {
		return fail(errors.New("muse PTY fallback startup lacks its verified descriptor"))
	}
	target := musebuilds.FrozenIdentity{Build: event.LastGood.Frozen.Build, SHA256: event.LastGood.Frozen.SHA256, Path: event.LastGood.Frozen.Path}
	if file.Name() != target.Path {
		return fail(fmt.Errorf("muse PTY fallback descriptor %q is not the permitted %q", file.Name(), target.Path))
	}
	outcome := r.startLeg(ctx, MusePTYComposeFallback, target)
	if outcome.Err != nil {
		return musebuilds.StartOutcome{Err: outcome.Err, Cleanup: outcome.Cleanup}
	}
	r.startedHost = outcome.Host
	return musebuilds.StartOutcome{
		Success: musebuilds.StartSuccess{Frozen: target, AttemptID: event.FailureAttempt, Surface: musebuilds.SurfacePTY, ReadyAt: time.Now().UTC().Format(time.RFC3339), By: "pty-fallback"},
		Cleanup: musebuilds.CleanupConfirmed,
	}
}

// startLeg composes and starts one permitted leg: fresh legs through
// the binding composer, resume legs through the binding recomposer
// plus a verified import.
func (r *musePTYFallbackReporter) startLeg(ctx context.Context, leg MusePTYComposeLeg, target musebuilds.FrozenIdentity) musePTYLegOutcome {
	if r.resume {
		if r.binding.Recompose == nil {
			return musePTYLegOutcome{Err: &MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeInvalidConfig, Cause: errors.New("muse PTY fallback resume carries no recomposer")}, Cleanup: musebuilds.CleanupConfirmed}
		}
		seal, process, err := r.binding.Recompose(ctx, target, r.sessionID)
		if err != nil {
			return musePTYLegOutcome{Err: err, Cleanup: musebuilds.CleanupConfirmed}
		}
		return musePTYStartResumeLeg(ctx, seal, process, r.record, target, r.binding.Builds, r.keys)
	}
	if r.binding.Compose == nil {
		return musePTYLegOutcome{Err: &MusePTYExecError{Leg: string(musePTYLegLaunch), Code: musePTYCodeInvalidConfig, Cause: errors.New("muse PTY fallback launch carries no composer")}, Cleanup: musebuilds.CleanupConfirmed}
	}
	req, err := r.binding.Compose(ctx, leg, target)
	if err != nil {
		return musePTYLegOutcome{Err: err, Cleanup: musebuilds.CleanupConfirmed}
	}
	return musePTYStartFreshLeg(ctx, req, target, r.binding.Builds)
}

// musePTYDeferredReporter is the musepty DeferredReporter: it
// presents the L4 deferral offer and runs the single already-frozen
// target leg. A failed leg arms no failure.
type musePTYDeferredReporter struct {
	binding   *MusePTYBuildsBinding
	resume    bool
	sessionID string
	record    sm.ProviderHostRecord
	keys      []agentic.SealCommitmentKey

	startedHost *musePTYHost
	started     musebuilds.DeferralStart
}

var _ musebuilds.DeferredReporter = (*musePTYDeferredReporter)(nil)

// Offer presents the interactive deferral offer through the L4
// surface. Unattended legs never call it.
func (r *musePTYDeferredReporter) Offer(ctx context.Context, start musebuilds.DeferralStart) (bool, error) {
	if r == nil || r.binding == nil || r.binding.DeferredOffer == nil {
		return false, errors.New("muse PTY newest-build launch carries no interactive deferral surface")
	}
	return r.binding.DeferredOffer(ctx, start)
}

// Start runs the single deferred-target leg and observes its real
// startup.
func (r *musePTYDeferredReporter) Start(ctx context.Context, start musebuilds.DeferralStart, file *os.File) musebuilds.StartOutcome {
	r.started = start
	fail := func(err error) musebuilds.StartOutcome {
		return musebuilds.StartOutcome{Err: err, Cleanup: musebuilds.CleanupConfirmed}
	}
	if file == nil {
		return fail(errors.New("muse PTY deferred startup lacks its verified descriptor"))
	}
	if file.Name() != start.Target.Path {
		return fail(fmt.Errorf("muse PTY deferred descriptor %q is not the authorized %q", file.Name(), start.Target.Path))
	}
	var outcome musePTYLegOutcome
	if r.resume {
		if r.binding.Recompose == nil {
			return fail(&MusePTYExecError{Leg: string(musePTYLegResume), Code: musePTYCodeInvalidConfig, Cause: errors.New("muse PTY deferred resume carries no recomposer")})
		}
		seal, process, err := r.binding.Recompose(ctx, start.Target, r.sessionID)
		if err != nil {
			return fail(err)
		}
		outcome = musePTYStartResumeLeg(ctx, seal, process, r.record, start.Target, r.binding.Builds, r.keys)
	} else {
		if r.binding.Compose == nil {
			return fail(&MusePTYExecError{Leg: string(musePTYLegLaunch), Code: musePTYCodeInvalidConfig, Cause: errors.New("muse PTY deferred launch carries no composer")})
		}
		req, err := r.binding.Compose(ctx, MusePTYComposeDeferred, start.Target)
		if err != nil {
			return fail(err)
		}
		outcome = musePTYStartFreshLeg(ctx, req, start.Target, r.binding.Builds)
	}
	if outcome.Err != nil {
		return musebuilds.StartOutcome{Err: outcome.Err, Cleanup: outcome.Cleanup}
	}
	r.startedHost = outcome.Host
	return musebuilds.StartOutcome{
		Success: musebuilds.StartSuccess{Frozen: start.Target, AttemptID: start.AttemptID, Surface: musebuilds.SurfacePTY, ReadyAt: time.Now().UTC().Format(time.RFC3339), By: "pty-deferred"},
		Cleanup: musebuilds.CleanupConfirmed,
	}
}

// musePTYInvocation is one validated newest-build invocation: the
// binding, the runner, the attempt token and the entry stage.
type musePTYInvocation struct {
	binding *MusePTYBuildsBinding
	runner  *musebuilds.ProbeRunner
	attempt string
	stage   string
	resume  bool
}

// musePTYPrepareInvocation validates one invocation before any
// selection: the binding, the mode surfaces, the runner and the
// attempt token.
func musePTYPrepareInvocation(binding *MusePTYBuildsBinding, resume bool) (*musePTYInvocation, error) {
	if binding == nil {
		return nil, &MusePTYExecError{Leg: string(musePTYLegLaunch), Code: musePTYCodeInvalidConfig, Cause: errors.New("newest-build launch requires builds bindings")}
	}
	leg := musePTYLegLaunch
	stage := musePTYStageLaunch
	if resume {
		leg = musePTYLegResume
		stage = musePTYStageResume
	}
	if binding.Mode == musebuilds.RecoveryInteractive && (binding.Offer == nil || binding.DeferredOffer == nil) {
		return nil, &MusePTYExecError{Leg: string(leg), Code: musePTYCodeInvalidConfig, Cause: errors.New("interactive newest-build launch requires the fallback and deferral offer surfaces")}
	}
	runner := binding.Runner
	if runner == nil {
		runner = musebuilds.NewProbeRunner(nil)
	}
	attempt := binding.AttemptID
	if attempt == "" {
		attempt = musePTYAttemptID()
	}
	return &musePTYInvocation{binding: binding, runner: runner, attempt: attempt, stage: stage, resume: resume}, nil
}

// musePTYPrepareSelection runs the controller selection for one
// invocation: newest selection, freeze and first probe on the
// entry's route.
func musePTYPrepareSelection(ctx context.Context, inv *musePTYInvocation) (*musebuilds.PreparedLaunch, error) {
	route := musebuilds.RouteLaunch
	if inv.resume {
		route = musebuilds.RouteResume
	}
	return musebuilds.PrepareNewestLaunch(ctx, inv.binding.Builds, inv.runner, musebuilds.LaunchRequest{
		Route: route, Surface: musebuilds.SurfacePTY, Mode: inv.binding.Mode, Env: inv.binding.Env,
		AttemptID: inv.attempt,
	})
}

// musePTYPreattemptError reports whether err is a pre-attempt refusal
// — no exec was attempted, so nothing is reported and no fallback
// can follow — or an attempted failure the controller must record.
// Every MusePTYExecError is pre-attempt EXCEPT a seal verification
// that failed inside a module probe execution: a timed-out or
// output-limited seal re-probe is attempt evidence (design section
// 4), and only the module's own ProbeExecutionError carries that
// fact. Static seal errors never share its wrapper.
func musePTYPreattemptError(err error) (preattempt bool, probe *agentic.ProbeExecutionError) {
	var refusal *MusePTYExecError
	if !errors.As(err, &refusal) {
		return false, nil
	}
	var execution *agentic.ProbeExecutionError
	if errors.As(err, &execution) {
		return false, execution
	}
	return true, nil
}

// musePTYReportStartFailure records one attempted newest-leg failure
// and routes the single fallback. Probe-execution failures report
// their own attempt facts; startup failures report the observed
// child start.
func musePTYReportStartFailure(ctx context.Context, inv *musePTYInvocation, prep *musebuilds.PreparedLaunch, newest musebuilds.FrozenIdentity, outcome musePTYLegOutcome, primary error) error {
	code := musePTYStartupCode
	started := outcome.Started
	if !started {
		code = musePTYSpawnCode
	}
	stage := inv.stage
	if _, probe := musePTYPreattemptError(primary); probe != nil {
		stage = musePTYStageSeal
		code = musePTYSealProbeCode
		started = probe.ChildStarted
		if probe.TeardownIncomplete {
			outcome.Cleanup = musebuilds.CleanupIncomplete
		}
	}
	failure := musebuilds.NewestFailure{
		AttemptID: inv.attempt, Newest: newest, Stage: stage, Code: code,
		Attempted: true, Started: started, Cleanup: outcome.Cleanup,
		Detail: musePTYFailureDetail(primary), Err: primary,
	}
	return prep.ReportNewestStartFailure(ctx, failure)
}

// StartMusePTYHostViaBuilds drives one fresh PTY launch through the
// newest-build controller: selection, freeze and first probe, then
// the newest leg, then at most one permitted fallback or one
// authorized deferral. The request must carry no conflicting frozen
// tuple; the flow injects the authorized tuple itself.
// It bypasses the fragment lock check (no fragment input); it has no production callers and must not be used in production paths.
func StartMusePTYHostViaBuilds(ctx context.Context, binding *MusePTYBuildsBinding, req agentic.LaunchRequest) (*musePTYHost, error) {
	inv, err := musePTYPrepareInvocation(binding, false)
	if err != nil {
		return nil, err
	}
	prep, prepErr := musePTYPrepareSelection(ctx, inv)
	if prepErr != nil && prep == nil {
		return nil, prepErr
	}
	if prepErr != nil {
		return musePTYRecoverFailedNewest(ctx, inv, prep, prepErr, "", sm.ProviderHostRecord{}, nil)
	}
	if prep.Outcome() == musebuilds.OutcomeRunDeferred {
		return musePTYRunDeferredNewest(ctx, inv, prep, "", sm.ProviderHostRecord{}, nil)
	}
	return musePTYStartNewestFreshLeg(ctx, inv, prep, req)
}

// ResumeMusePTYHostViaBuilds drives one PTY resume through the
// newest-build controller. The seal and process arrive from the
// durable record; the flow imports them verified and binds them to
// the current selection — a stale seal refuses for outer
// recomposition — then resumes, falling back or deferring exactly
// like the fresh leg. Keys travel to the import for keyed guards.
// It bypasses the fragment lock check (no fragment input); it has no production callers and must not be used in production paths.
func ResumeMusePTYHostViaBuilds(ctx context.Context, binding *MusePTYBuildsBinding, seal agentic.Seal, process agentic.Plan, record sm.ProviderHostRecord, keys ...agentic.SealCommitmentKey) (*musePTYHost, error) {
	inv, err := musePTYPrepareInvocation(binding, true)
	if err != nil {
		return nil, err
	}
	sessionID, err := musePTYResumeSessionID(record)
	if err != nil {
		return nil, err
	}
	prep, prepErr := musePTYPrepareSelection(ctx, inv)
	if prepErr != nil && prep == nil {
		return nil, prepErr
	}
	if prepErr != nil {
		return musePTYRecoverFailedNewest(ctx, inv, prep, prepErr, sessionID, record, keys)
	}
	if prep.Outcome() == musebuilds.OutcomeRunDeferred {
		return musePTYRunDeferredNewest(ctx, inv, prep, sessionID, record, keys)
	}
	return musePTYStartNewestResumeLeg(ctx, inv, prep, seal, process, record, sessionID, keys)
}

// musePTYRecoverFailedNewest runs the route-(i) fallback for a newest
// that already failed with durable failure evidence: one permit
// request, the attempting warning, then the single exact-last-good
// leg. No newest child runs here. resumeSessionID names the native
// session the resume route recomposes; empty runs the fresh leg. A
// controller failure after the leg started stops and reaps the
// retained host before the error returns, so no live host leaks
// past it.
func musePTYRecoverFailedNewest(ctx context.Context, inv *musePTYInvocation, prep *musebuilds.PreparedLaunch, primary error, resumeSessionID string, record sm.ProviderHostRecord, keys []agentic.SealCommitmentKey) (*musePTYHost, error) {
	reporter := &musePTYFallbackReporter{binding: inv.binding, resume: inv.resume, sessionID: resumeSessionID, record: record, keys: keys}
	grant, err := prep.RequestPermit(ctx, reporter)
	if err != nil {
		return nil, errors.Join(primary, err)
	}
	attemptEvent := grant.Permit.WarningEvent()
	attempt := musePTYFallbackRecord(attemptEvent)
	return musePTYRunRecovery(inv.binding.Warnings, attempt, func() (*musePTYHost, musePTYRecoveryRecord, error) {
		result, err := grant.Permit.StartFallback(ctx, reporter)
		event := result.Event
		if event.Code == "" {
			event = attemptEvent
		}
		if err == nil && reporter.startedHost == nil {
			err = errors.New("muse PTY fallback leg succeeded without starting its host")
		}
		if err != nil {
			stopErr := musePTYStopRetainedHost(reporter.startedHost)
			event.State = "fallback_failed"
			return nil, musePTYFallbackRecord(event), errors.Join(err, stopErr)
		}
		terminal := musePTYFallbackRecord(event)
		return reporter.startedHost, terminal, nil
	})
}

// musePTYRunDeferredNewest runs the route-(ii) leg: the newest
// already-frozen build with its host-cap warning. A failed leg arms
// no failure and offers no fallback, but it still warns: the
// attempting warning fires before the leg and the follow-up resolves
// it after, and a retained host is stopped before the error returns.
func musePTYRunDeferredNewest(ctx context.Context, inv *musePTYInvocation, prep *musebuilds.PreparedLaunch, resumeSessionID string, record sm.ProviderHostRecord, keys []agentic.SealCommitmentKey) (*musePTYHost, error) {
	auth, ok := prep.Deferral()
	if !ok {
		return nil, errors.New("muse PTY newest-build launch deferred without its authorization")
	}
	reporter := &musePTYDeferredReporter{binding: inv.binding, resume: inv.resume, sessionID: resumeSessionID, record: record, keys: keys, started: auth.WarningStart()}
	attempt := musePTYDeferralRecord(reporter.started, "deferral_attempting", musePTYDeferralAttemptingText(auth))
	return musePTYRunRecovery(inv.binding.Warnings, attempt, func() (*musePTYHost, musePTYRecoveryRecord, error) {
		_, err := auth.Start(ctx, reporter)
		if err == nil && reporter.startedHost == nil {
			err = errors.New("muse PTY deferred leg succeeded without starting its host")
		}
		if err != nil {
			stopErr := musePTYStopRetainedHost(reporter.startedHost)
			state, note := "deferral_failed", musePTYDeferralFailedText(auth, err)
			if refusal, ok := musebuilds.AsLaunchError(err); ok && refusal.Code == musebuilds.CodeLaunchDeferralDeclined {
				state, note = "deferral_declined", musePTYDeferralDeclinedText(auth)
			}
			return nil, musePTYDeferralRecord(reporter.started, state, note), errors.Join(err, stopErr)
		}
		return reporter.startedHost, musePTYDeferralRecord(reporter.started, "deferral_started", musePTYDeferralNoteText(reporter.started)), nil
	})
}

// musePTYStartNewestFreshLeg starts the prepared newest fresh leg and
// reports its real outcome: success admits last-good, an attempted
// failure arms the one fallback, and a pre-attempt refusal
// terminates without reporting anything. A controller failure after
// the leg started stops and reaps the retained host before the error
// returns.
func musePTYStartNewestFreshLeg(ctx context.Context, inv *musePTYInvocation, prep *musebuilds.PreparedLaunch, req agentic.LaunchRequest) (*musePTYHost, error) {
	newest := prep.Newest()
	outcome := musePTYStartFreshLeg(ctx, req, newest, inv.binding.Builds)
	if outcome.Err == nil {
		success := musebuilds.StartSuccess{Frozen: newest, AttemptID: inv.attempt, Surface: musebuilds.SurfacePTY, ReadyAt: time.Now().UTC().Format(time.RFC3339), By: inv.stage}
		if _, rerr := prep.ReportNewestStartSuccess(ctx, success); rerr != nil {
			return nil, errors.Join(rerr, musePTYStopRetainedHost(outcome.Host))
		}
		return outcome.Host, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// Pre-attempt refusals return directly: no exec was attempted, so
	// nothing is reported, no failure is armed, and no fallback to an
	// older build can follow — not even a pinned last-good. Seal
	// probe executions are the one exception: they attempted a
	// start at the verification stage and route the same way as a
	// startup failure.
	if preattempt, _ := musePTYPreattemptError(outcome.Err); preattempt {
		return nil, outcome.Err
	}
	if rerr := musePTYReportStartFailure(ctx, inv, prep, newest, outcome, outcome.Err); rerr != nil {
		return nil, errors.Join(outcome.Err, rerr)
	}
	return musePTYRecoverFailedNewest(ctx, inv, prep, outcome.Err, "", sm.ProviderHostRecord{}, nil)
}

// musePTYStartNewestResumeLeg imports the recorded seal against the
// prepared newest and resumes it, reporting the real outcome exactly
// like the fresh leg. A stale seal refuses for outer recomposition
// without reporting anything.
func musePTYStartNewestResumeLeg(ctx context.Context, inv *musePTYInvocation, prep *musebuilds.PreparedLaunch, seal agentic.Seal, process agentic.Plan, record sm.ProviderHostRecord, sessionID string, keys []agentic.SealCommitmentKey) (*musePTYHost, error) {
	newest := prep.Newest()
	outcome := musePTYStartResumeLeg(ctx, seal, process, record, newest, inv.binding.Builds, keys)
	if outcome.Err == nil {
		success := musebuilds.StartSuccess{Frozen: newest, AttemptID: inv.attempt, Surface: musebuilds.SurfacePTY, ReadyAt: time.Now().UTC().Format(time.RFC3339), By: inv.stage}
		if _, rerr := prep.ReportNewestStartSuccess(ctx, success); rerr != nil {
			return nil, errors.Join(rerr, musePTYStopRetainedHost(outcome.Host))
		}
		return outcome.Host, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if preattempt, _ := musePTYPreattemptError(outcome.Err); preattempt {
		return nil, outcome.Err
	}
	if rerr := musePTYReportStartFailure(ctx, inv, prep, newest, outcome, outcome.Err); rerr != nil {
		return nil, errors.Join(outcome.Err, rerr)
	}
	return musePTYRecoverFailedNewest(ctx, inv, prep, outcome.Err, sessionID, record, keys)
}
