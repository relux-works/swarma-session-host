package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

// Launch stage. Every L2C refusal names its stage so a caller can tell
// where a typed reason came from without matching prose. Underlying
// discovery, freeze, probe and last-good errors keep their own stages
// and codes and are returned unwrapped, so the exact reason survives.
const StageLaunch = "launch"

// Launch refusal codes. Each refuses a distinct controller fact; a
// failed, partial or malformed read is never a legitimate absence.
const (
	// CodeLaunchCanceled refuses a launch whose context ended.
	CodeLaunchCanceled = "muse_launch_canceled"
	// CodeLaunchInvalid refuses a malformed launch request or a
	// miswired call: an unknown route, an unclassified mode, an
	// unknown surface, a non-token attempt, a nil runner or a
	// nil reporter, or a report for a leg that never started.
	CodeLaunchInvalid = "muse_launch_invalid"
	// CodeLaunchInvalidConfig refuses an explicit last-good
	// assertion that differs from the durable receipt, or that
	// names a path while no receipt exists. There is no fallback
	// to the asserted path.
	CodeLaunchInvalidConfig = "muse_launch_invalid_config"
	// CodeLaunchRecoveryUnavailable refuses a fallback permit the
	// invocation did not earn: no recorded newest failure, an
	// absent or unusable last-good receipt, a receipt that names
	// the newest tuple itself, a pre-attempt report, unconfirmed
	// cleanup, or an allowance that is already spent.
	CodeLaunchRecoveryUnavailable = "muse_launch_recovery_unavailable"
	// CodeLaunchPermitConsumed refuses a second session leg or a
	// probe after the leg on one permit. One permit starts one
	// fallback leg, never a retry loop.
	CodeLaunchPermitConsumed = "muse_launch_permit_consumed"
	// CodeLaunchTupleMismatch refuses a reported or probed tuple
	// that differs from the bound tuple: a stale primary path, a
	// foreign failure attribution, a probe of a build the permit
	// does not name, or a deferral target superseded by a newer
	// frozen build.
	CodeLaunchTupleMismatch = "muse_launch_tuple_mismatch"
	// CodeLaunchDeferredUnprobed refuses a deferred start whose
	// target never completed its first probe. The target is
	// verified frozen bytes but not yet runnable; nothing runs
	// until a reviewed re-probe completes it.
	CodeLaunchDeferredUnprobed = "muse_launch_deferred_unprobed"
	// CodeLaunchProbeOutstanding refuses a fallback leg while a
	// fallback probe is still outstanding under the permit. The
	// consumer releases each probe lease before starting the leg,
	// so consumption can never overlap a running probe.
	CodeLaunchProbeOutstanding = "muse_launch_probe_outstanding"
	// CodeLaunchDeferralDeclined refuses an interactive deferral
	// leg without explicit consent: a declined, timed-out or
	// failed offer. Nothing starts and no failure arms.
	CodeLaunchDeferralDeclined = "muse_launch_deferral_declined"
)

// LaunchError is a typed launch-controller refusal: the stage that
// refused and the machine-readable reason, with the cause chained.
type LaunchError struct {
	Stage string
	Code  string
	Cause error
}

// Error renders the refusal with its stage and code.
func (e *LaunchError) Error() string {
	if e == nil {
		return "muse launch error"
	}
	if e.Cause != nil {
		return "muse launch " + e.Stage + " " + e.Code + ": " + e.Cause.Error()
	}
	return "muse launch " + e.Stage + " " + e.Code
}

// Unwrap returns the underlying cause.
func (e *LaunchError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// AsLaunchError matches a LaunchError in err's chain for code and
// stage assertions.
func AsLaunchError(err error) (*LaunchError, bool) {
	var refusal *LaunchError
	if errors.As(err, &refusal) {
		return refusal, true
	}
	return nil, false
}

func launchRefuseIfCanceled(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return &LaunchError{Stage: StageLaunch, Code: CodeLaunchCanceled, Cause: err}
	}
	return nil
}

func launchRefusal(code string, cause error) error {
	return &LaunchError{Stage: StageLaunch, Code: code, Cause: cause}
}

// LaunchRoute is the consumer entry point. Every route selects the
// newest installed build first; no route carries its own binary
// selection, and a restore snapshot or imported plan is provenance,
// never selection authority.
type LaunchRoute string

// The exhaustive section-5 entry inventory at controller level:
// fresh starts, restores with a prior snapshot, and resumes with a
// sealed plan. Unknown routes refuse; they never alias to launch.
const (
	// RouteLaunch serves fresh MSP/PTY/task-muse starts and managed
	// spawns. Detached and managed runs arrive already classified
	// as unattended; the controller never reclassifies.
	RouteLaunch LaunchRoute = "launch"
	// RouteRestore serves MSP restore/restart with a prior
	// snapshot. The snapshot preserves session identity; the
	// binary is always the current newest.
	RouteRestore LaunchRoute = "restore"
	// RouteResume serves PTY resume and imported sealed plans. A
	// stale plan refuses for outer recomposition; selection still
	// binds the current newest first.
	RouteResume LaunchRoute = "resume"
)

// LaunchRequest is one invocation's launch input. Mode is classified
// once by the outer L4 surface and carried here, never inferred from
// stdin. AttemptID names the invocation and must be unique per
// invocation; the first probe, the permit document and the receipts
// all bind to it.
type LaunchRequest struct {
	Route   LaunchRoute
	Surface string
	Mode    RecoveryMode
	Env     []string
	// AttemptID is the invocation's launch-attempt token.
	AttemptID string
	// SnapshotProvenance carries a restore snapshot's or imported
	// plan's previously used binary path for provenance only. It
	// is recorded, never selected, on every route.
	SnapshotProvenance string
	// ExplicitLastGood asserts the durable receipt must name this
	// frozen path when falling back. Empty means no assertion.
	ExplicitLastGood string
}

// LaunchOutcome is the typed Prepare result for the L4 layer.
type LaunchOutcome string

// Prepare outcomes. RunNewest and RunDeferred authorize their leg;
// FailedNewest arms the permit path; Refused runs nothing.
const (
	// OutcomeRunNewest authorizes the prepared verified newest.
	OutcomeRunNewest LaunchOutcome = "run-newest"
	// OutcomeRunDeferred authorizes the already-frozen deferral
	// target with the host-cap warning: the Amendment 2 route-(ii)
	// leg, taken only when the host cap is configured and tripped.
	// It is not a failure and arms no permit.
	OutcomeRunDeferred LaunchOutcome = "run-deferred"
	// OutcomeFailedNewest records a newest start failure with
	// durable evidence. The permit path is open exactly once.
	OutcomeFailedNewest LaunchOutcome = "failed-newest"
	// OutcomeNewestStarted records a reported newest startup,
	// admitted or admission-failed. The attempt state is spent.
	OutcomeNewestStarted LaunchOutcome = "newest-started"
	// OutcomeRefused runs nothing: a typed pre-attempt refusal.
	OutcomeRefused LaunchOutcome = "refused"
)

// PermitOutcome is the typed RequestPermit result for the L4 layer.
type PermitOutcome string

// Permit outcomes. Both grants persist the fallback warning before
// any fallback probe or exec; Declined terminates without a permit.
const (
	// PermitAutoGranted issues the permit automatically on an
	// unattended invocation, with the fallback warning.
	PermitAutoGranted PermitOutcome = "auto-granted"
	// PermitOfferGranted issues the permit after explicit
	// interactive consent, with the fallback warning.
	PermitOfferGranted PermitOutcome = "offer-granted"
	// PermitDeclined records an interactive decline, timeout, EOF
	// or offer error. No permit issues and none can be requested
	// again on this invocation.
	PermitDeclined PermitOutcome = "declined"
)

// LaunchExecKind names one traced execution leg.
type LaunchExecKind string

// Traced legs. Every recorded exec is either the frozen
// selected-newest tuple or the single permitted last-good tuple.
const (
	// LaunchExecNewestStart records a reported newest child start.
	LaunchExecNewestStart LaunchExecKind = "newest-start"
	// LaunchExecFallbackProbe records one permit-authorized
	// fallback probe.
	LaunchExecFallbackProbe LaunchExecKind = "fallback-probe"
	// LaunchExecFallbackStart records the single fallback leg.
	LaunchExecFallbackStart LaunchExecKind = "fallback-start"
	// LaunchExecDeferredStart records a deferred-target leg: the
	// route-(ii) newest already-frozen PROBE_OK target with the
	// host-cap warning. Permitted stays false because no fallback
	// permit authorizes it; the kind itself is the route record.
	LaunchExecDeferredStart LaunchExecKind = "deferred-start"
)

// LaunchTraceEntry is one authorized execution record: its leg, the
// exact tuple it ran, the selection it ran under, and whether a live
// fallback permit authorized it. Newest legs carry Permitted false
// and must name the prepared newest tuple.
type LaunchTraceEntry struct {
	Kind        LaunchExecKind
	Tuple       FrozenIdentity
	SelectionID string
	Permitted   bool
}

// launchTrace is the invocation's shared exec record. The prepared
// launch, its permit and its deferral authorization share one trace
// so the M69 property covers every leg from every handle.
type launchTrace struct {
	mu      sync.Mutex
	entries []LaunchTraceEntry
}

func (t *launchTrace) append(entry LaunchTraceEntry) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries = append(t.entries, entry)
}

func (t *launchTrace) snapshot() []LaunchTraceEntry {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]LaunchTraceEntry(nil), t.entries...)
}

// launchFailureKind names the recorded failure source. Exactly one
// arms the permit path per invocation.
type launchFailureKind int

const (
	failureNone launchFailureKind = iota
	// failureLiveProbe is this invocation's failed first probe with
	// its persisted attempt receipt.
	failureLiveProbe
	// failureRecordedBAD is the durable BAD state for the selected
	// newest. No probe runs; the verdict is the evidence.
	failureRecordedBAD
	// failureConsumerReport is a validated consumer-reported newest
	// start failure: an attempted exec with real evidence and
	// confirmed cleanup.
	failureConsumerReport
)

// launchFailure is the validated failure behind one permit. The
// attempt, stage and code bind the durable permit document; the
// cause travels with fallback errors as the primary failure. The
// receipt is the durable attempt record the permit re-reads and
// verifies before issuing: the runner's own receipt for live-probe
// failures, the persisted consumer observation for reported ones.
// Recorded BAD failures carry no attempt receipt; their durable
// evidence is the BAD verdict itself.
type launchFailure struct {
	kind      launchFailureKind
	attemptID string
	stage     string
	code      string
	cause     error
	newest    FrozenIdentity
	receipt   AttemptReceipt
	hasProof  bool
}

// NewestFailure is a consumer-reported newest start failure from MSP
// handshake/session establishment, PTY startup, or a module
// verification probe. Only attempted starts with confirmed cleanup
// arm the permit path; pre-attempt and static failures refuse.
type NewestFailure struct {
	// AttemptID names the failed startup attempt.
	AttemptID string
	// Newest must equal the prepared newest tuple exactly. Foreign
	// attributions refuse.
	Newest FrozenIdentity
	// Stage names the consumer stage: msp-handshake,
	// msp-session, pty-startup, seal-reprobe, or the equivalent
	// L2M/L3 surface stage.
	Stage string
	// Code is the consumer's machine-readable failure code.
	Code string
	// Attempted reports an exec was attempted: fork/exec tried or
	// a child started. Unattempted reports are not failures.
	Attempted bool
	// Started reports the child actually started. An attempted
	// fork/exec failure without a child still qualifies.
	Started bool
	// Cleanup must be confirmed. Unconfirmed teardown refuses a
	// second child on every route.
	Cleanup CleanupResult
	// Detail carries bounded diagnostic evidence, never
	// credentials.
	Detail string
	// Err chains the underlying cause.
	Err error
}

// PreparedLaunch is the controller's accepted invocation: the newest
// selection with its freeze/probe outcome, the armed failure if any,
// and the shared exec trace. Consumers report the newest leg through
// it and request at most one fallback permit from it. The zero value
// authorizes nothing; only PrepareNewestLaunch issues one.
type PreparedLaunch struct {
	mu            sync.Mutex
	runner        *ProbeRunner
	cfg           HostConfig
	req           LaunchRequest
	discovery     Discovery
	newest        FrozenIdentity
	outcome       LaunchOutcome
	failure       launchFailure
	deferred      *FrozenIdentity
	snapshotBuild string
	permitDecided bool
	trace         *launchTrace
}

// Newest returns the prepared newest tuple: the installed selection
// with its frozen-copy path after a successful freeze and probe.
func (p *PreparedLaunch) Newest() FrozenIdentity {
	if p == nil {
		return FrozenIdentity{}
	}
	return p.newest
}

// Discovery returns the accepted pass-1 selection behind this launch.
func (p *PreparedLaunch) Discovery() Discovery {
	if p == nil {
		return Discovery{}
	}
	return p.discovery
}

// Outcome returns the typed Prepare result.
func (p *PreparedLaunch) Outcome() LaunchOutcome {
	if p == nil {
		return OutcomeRefused
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.outcome
}

// SnapshotBuild returns the build parsed from the restore/resume
// provenance reference, or empty when none was supplied or it did
// not parse. Selection never follows it.
func (p *PreparedLaunch) SnapshotBuild() string {
	if p == nil {
		return ""
	}
	return p.snapshotBuild
}

// FailureRecorded reports whether a newest failure with durable
// evidence arms the permit path.
func (p *PreparedLaunch) FailureRecorded() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.failure.kind != failureNone
}

// Trace returns the invocation's exec record: every reported newest
// start, every authorized fallback probe, and every fallback or
// deferred leg, in authorization order.
func (p *PreparedLaunch) Trace() []LaunchTraceEntry {
	if p == nil {
		return nil
	}
	return p.trace.snapshot()
}

// Deferral returns the Amendment 2 deferral authorization when this
// launch deferred under the host cap. The second result is false on
// every other outcome.
func (p *PreparedLaunch) Deferral() (*DeferralAuthorization, bool) {
	if p == nil {
		return nil, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.outcome != OutcomeRunDeferred || p.deferred == nil {
		return nil, false
	}
	return &DeferralAuthorization{
		valid:       true,
		target:      *p.deferred,
		newest:      p.newest,
		selectionID: p.discovery.SnapshotID,
		warning:     CodeDeferralHostCap,
		attemptID:   p.req.AttemptID,
		mode:        p.req.Mode,
		cfg:         p.cfg,
		runner:      p.runner,
		trace:       p.trace,
	}, true
}

// permitState is the one fallback allowance behind an issued permit.
// Copies of the permit share it, so a second handle cannot start a
// second leg. Authorization and consumption coordinate on this one
// mutex: the consumed check, the tuple check and the probe count
// move under a single hold, and the leg refuses while any probe is
// outstanding. The zero value has no state and authorizes nothing.
type permitState struct {
	mu       sync.Mutex
	attempts int
	probes   int
	active   int
}

// FallbackPermit is the opaque one-use fallback authorization for
// this invocation. It binds the failed-newest tuple, the issue-time
// last-good tuple and sequence, the surface, the mode and consent,
// and the single session attempt. Consumers cannot mint or edit one:
// the zero value authorizes nothing, and only RequestPermit issues.
type FallbackPermit struct {
	issued       bool
	selectionID  string
	newest       FrozenIdentity
	lastGood     LastGood
	surface      string
	mode         RecoveryMode
	consent      bool
	attemptID    string
	failureStage string
	failureCode  string
	cause        error
	runner       *ProbeRunner
	cfg          HostConfig
	state        *permitState
	trace        *launchTrace
}

// Issued reports whether the controller issued this permit.
func (p *FallbackPermit) Issued() bool {
	return p != nil && p.issued && p.state != nil
}

// Newest returns the failed-newest tuple the permit answers.
func (p *FallbackPermit) Newest() FrozenIdentity {
	if p == nil {
		return FrozenIdentity{}
	}
	return p.newest
}

// LastGood returns the issue-time last-good receipt. The fallback
// leg revalidates it under the state lock before starting; the
// receipt may have been superseded since issuance.
func (p *FallbackPermit) LastGood() LastGood {
	if p == nil {
		return LastGood{}
	}
	return p.lastGood
}

// Surface returns the launch surface the permit binds.
func (p *FallbackPermit) Surface() string {
	if p == nil {
		return ""
	}
	return p.surface
}

// Mode returns the once-classified launch mode.
func (p *FallbackPermit) Mode() RecoveryMode {
	if p == nil {
		return ""
	}
	return p.mode
}

// Consented reports explicit interactive consent. Unattended permits
// never carry consent; their warning is automatic.
func (p *FallbackPermit) Consented() bool {
	return p != nil && p.consent
}

// Attempts returns the session legs started under this permit: 0 or
// 1. Shared across every copy of the permit.
func (p *FallbackPermit) Attempts() int {
	if p == nil || p.state == nil {
		return 0
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	return p.state.attempts
}

// Probes returns the fallback probes authorized under this permit.
func (p *FallbackPermit) Probes() int {
	if p == nil || p.state == nil {
		return 0
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	return p.state.probes
}

// PermitGrant is RequestPermit's typed result for the L4 layer: the
// permit when granted, the outcome, and the warning code L4 must
// emit before any fallback probe or exec.
type PermitGrant struct {
	Permit  FallbackPermit
	Outcome PermitOutcome
	Warning string
}

// DeferralAuthorization is the opaque Amendment 2 deferral: the
// newest already-frozen target plus the host-cap warning. It runs
// through its own Start, never through a fallback permit, and its
// failure arms no failure. Interactive legs offer the target first
// and run only on explicit consent; unattended legs run on the
// warning alone. The zero value authorizes nothing.
type DeferralAuthorization struct {
	valid       bool
	target      FrozenIdentity
	newest      FrozenIdentity
	selectionID string
	warning     string
	attemptID   string
	mode        RecoveryMode
	cfg         HostConfig
	runner      *ProbeRunner
	trace       *launchTrace
}

// Valid reports whether the controller issued this authorization.
func (a *DeferralAuthorization) Valid() bool {
	return a != nil && a.valid
}

// Target returns the already-frozen build to run.
func (a *DeferralAuthorization) Target() FrozenIdentity {
	if a == nil {
		return FrozenIdentity{}
	}
	return a.target
}

// Newest returns the installed newest that could not freeze.
func (a *DeferralAuthorization) Newest() FrozenIdentity {
	if a == nil {
		return FrozenIdentity{}
	}
	return a.newest
}

// Warning returns the host-cap warning code L4 must emit.
func (a *DeferralAuthorization) Warning() string {
	if a == nil {
		return ""
	}
	return a.warning
}

// Mode returns the once-classified launch mode: interactive legs are
// offered, unattended legs run on the warning alone.
func (a *DeferralAuthorization) Mode() RecoveryMode {
	if a == nil {
		return ""
	}
	return a.mode
}

// DeferralStart is the verified deferred leg handed to the surface
// reporter: the target, the installed newest it stands in for, the
// warning, the invocation attempt and mode. Consented reports the
// interactive offer granted it; unattended legs never carry consent.
type DeferralStart struct {
	Target    FrozenIdentity
	Newest    FrozenIdentity
	Warning   string
	AttemptID string
	Mode      RecoveryMode
	Consented bool
}

// DeferredReporter is the surface boundary for a deferred leg. Offer
// presents the target on interactive legs and must honor its bounded
// context; only an explicit grant authorizes the start, and
// unattended legs never call it. Start must honor its
// startup/teardown context, use the supplied held file for the
// single startup, and return only after observing startup or
// teardown. It cannot select another build.
type DeferredReporter interface {
	Offer(context.Context, DeferralStart) (bool, error)
	Start(context.Context, DeferralStart, *os.File) StartOutcome
}

// FallbackResult is the terminal fallback leg: the durable event and
// the admitted receipt when the fallback actually started.
type FallbackResult struct {
	Event   RecoveryEvent
	Receipt LastGood
}

var (
	errNilRunner          = errors.New("the probe runner is required")
	errNilReporter        = errors.New("the surface reporter is required")
	errUnknownRoute       = errors.New("unknown launch route")
	errUnclassifiedMode   = errors.New("the launch mode is not classified")
	errUnknownSurface     = errors.New("the launch surface is not msp or pty")
	errBadAttemptToken    = errors.New("attempt identity is not a plain token")
	errNoFailureArmed     = errors.New("no recorded newest failure arms a permit")
	errPermitDecided      = errors.New("the invocation already decided its one permit")
	errAllowanceSpent     = errors.New("the invocation fallback allowance is spent")
	errSameTuple          = errors.New("the receipt names the newest tuple itself")
	errNoLegStarted       = errors.New("no newest leg started")
	errLegAlreadyReported = errors.New("the newest leg already reported")
	errProbeOutstanding   = errors.New("a fallback probe is still outstanding")
)

// validateLaunchRequest refuses a malformed invocation before any
// selection. Unknown routes refuse; they never alias to launch.
func validateLaunchRequest(req LaunchRequest) error {
	switch req.Route {
	case RouteLaunch, RouteRestore, RouteResume:
	default:
		return launchRefusal(CodeLaunchInvalid, errUnknownRoute)
	}
	if req.Mode != RecoveryInteractive && req.Mode != RecoveryUnattended {
		return launchRefusal(CodeLaunchInvalid, errUnclassifiedMode)
	}
	if req.Surface != SurfaceMSP && req.Surface != SurfacePTY {
		return launchRefusal(CodeLaunchInvalid, errUnknownSurface)
	}
	if !validAttemptToken(req.AttemptID) {
		return launchRefusal(CodeLaunchInvalid, errBadAttemptToken)
	}
	return nil
}

// snapshotProvenanceBuild parses the build named by a restore
// snapshot or imported plan reference, for provenance only. Empty
// or unparseable references yield empty provenance; selection never
// follows them on any route.
func snapshotProvenanceBuild(reference string) string {
	base := filepath.Base(reference)
	if reference == "" || base == "" || base == "." || base == string(filepath.Separator) {
		return ""
	}
	if !strings.HasPrefix(base, pinnedNamePrefix) {
		return ""
	}
	id, err := muse.ParsePinnedBinaryName(base)
	if err != nil {
		return ""
	}
	return id.Build
}

// PrepareNewestLaunch selects the newest installed build on every
// route, freezes and first-probes it through the invocation runner,
// and classifies the outcome for the consumer:
//
//   - (prep, nil) with OutcomeRunNewest: the verified newest is ready
//     to start. The consumer reports the leg through Report calls.
//   - (prep, nil) with OutcomeRunDeferred: the host cap deferred to
//     the already-frozen target with its warning. It is not a
//     failure and arms no permit.
//   - (prep, failureErr) with OutcomeFailedNewest: the newest failed
//     with durable evidence, and the permit path is open exactly
//     once. The error is the typed primary failure.
//   - (nil, err): a typed pre-attempt refusal. Nothing runs, no
//     permit can issue, and no bare PATH launch exists on any route.
//
// The runner carries the invocation's probe and fallback latches and
// must be the same runner the consumer uses until the launch ends.
// Every route funnels through ResolveNewest: restore snapshots and
// imported plans are provenance, never selection authority.
func PrepareNewestLaunch(ctx context.Context, cfg HostConfig, runner *ProbeRunner, req LaunchRequest) (*PreparedLaunch, error) {
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return nil, err
	}
	if runner == nil {
		return nil, launchRefusal(CodeLaunchInvalid, errNilRunner)
	}
	if err := validateLaunchRequest(req); err != nil {
		return nil, err
	}
	d, err := ResolveNewest(ctx, req.Env)
	if err != nil {
		return nil, err
	}
	// Every route selects the resolved newest. Snapshot provenance
	// is recorded for the trace; it is never selection authority.
	snapshotBuild := snapshotProvenanceBuild(req.SnapshotProvenance)
	selected := d
	prep := &PreparedLaunch{
		runner: runner, cfg: cfg, req: req,
		discovery: selected, newest: selected.Identity,
		outcome: OutcomeRefused, snapshotBuild: snapshotBuild,
		trace: &launchTrace{},
	}
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return nil, err
	}
	result, freezeErr := runner.Freeze(ctx, cfg, selected, ProbeRequest{AttemptID: req.AttemptID, SelectionID: selected.SnapshotID})
	if freezeErr == nil {
		prep.newest = result.Frozen
		prep.outcome = OutcomeRunNewest
		return prep, nil
	}
	var ferr *FreezeError
	if errors.As(freezeErr, &ferr) {
		switch ferr.Code {
		case CodeRecordedBad:
			prep.outcome = OutcomeFailedNewest
			prep.failure = recordedFailure(cfg, selected, req.AttemptID, freezeErr, badCode(ferr.Bad))
			prep.newest = prep.failure.newest
			return prep, freezeErr
		case CodeDeferralHostCap:
			if ferr.Frozen == nil {
				return nil, launchRefusal(CodeLaunchInvalid, freezeErr)
			}
			prep.outcome = OutcomeRunDeferred
			target := *ferr.Frozen
			prep.deferred = &target
			return prep, nil
		default:
			return nil, freezeErr
		}
	}
	var perr *ProbeError
	if errors.As(freezeErr, &perr) {
		switch perr.Code {
		case CodeProbeAttemptFailed:
			if result.Record.Receipt == nil || !receiptUsable(perr.Cause) {
				return nil, freezeErr
			}
			prep.outcome = OutcomeFailedNewest
			prep.newest = result.Frozen
			prep.failure = launchFailure{
				kind: failureLiveProbe, attemptID: result.Record.Receipt.AttemptID,
				stage: StageProbeExecute, code: string(result.Record.Receipt.Outcome),
				cause: freezeErr, newest: result.Frozen,
				receipt: *result.Record.Receipt, hasProof: true,
			}
			return prep, freezeErr
		case CodeProbeRecordedBad:
			prep.outcome = OutcomeFailedNewest
			prep.failure = recordedFailure(cfg, selected, req.AttemptID, freezeErr, badCode(perr.Bad))
			prep.newest = prep.failure.newest
			return prep, freezeErr
		default:
			return nil, freezeErr
		}
	}
	return nil, freezeErr
}

// badCode names the recorded verdict for the permit document. The
// verdict always travels with a recorded-bad refusal; a missing one
// degrades to the refusal code, never to an empty code.
func badCode(verdict *BadVerdict) string {
	if verdict == nil || verdict.Code == "" {
		return CodeRecordedBad
	}
	return verdict.Code
}

// recordedFailure binds a recorded BAD verdict to the invocation.
// The newest tuple names the frozen copy the verdict constrains: the
// absolute published path, not the installed source path.
func recordedFailure(cfg HostConfig, selected Discovery, attemptID string, cause error, code string) launchFailure {
	key := freezeKey(selected.Identity.Build, selected.Identity.SHA256)
	frozen := FrozenIdentity{
		Build: selected.Identity.Build, SHA256: selected.Identity.SHA256,
		Path: frozenBinaryPath(cfg.FrozenRoot, key, selected.Identity.Build),
	}
	return launchFailure{
		kind: failureRecordedBAD, attemptID: attemptID,
		stage: StageProbe, code: code, cause: cause, newest: frozen,
	}
}

// receiptUsable mirrors the probe runner's recovery rule: a clean
// record, or a publication failure beside an independently persisted
// receipt, keeps the failure usable. Integrity conflicts and missing
// evidence never convert into a permit.
func receiptUsable(recordErr error) bool {
	if recordErr == nil {
		return true
	}
	refusal, ok := AsProbeError(recordErr)
	if !ok {
		return false
	}
	return refusal.Code == CodeProbePublicationFailed || refusal.Code == CodeProbePublicationUnknown
}

// failureEligible reports whether a recorded failure source arms the
// permit path. Every attempted-start class qualifies: the live first
// probe, the recorded BAD verdict, and validated consumer reports.
// There is no other source, and pre-attempt refusals never qualify.
func failureEligible(kind launchFailureKind) bool {
	switch kind {
	case failureLiveProbe, failureRecordedBAD, failureConsumerReport:
		return true
	default:
		return false
	}
}

// ReportNewestStartSuccess records the consumer's observed newest
// startup and admits the last-good receipt for it. The success tuple
// must equal the prepared newest exactly: a stale primary path
// refuses without a receipt. Admission follows the last-good rules;
// a publication failure is typed and admits nothing.
func (p *PreparedLaunch) ReportNewestStartSuccess(ctx context.Context, success StartSuccess) (LastGood, error) {
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return LastGood{}, err
	}
	if p == nil {
		return LastGood{}, launchRefusal(CodeLaunchInvalid, errNoLegStarted)
	}
	p.mu.Lock()
	if p.outcome != OutcomeRunNewest {
		p.mu.Unlock()
		return LastGood{}, launchRefusal(CodeLaunchInvalid, errLegAlreadyReported)
	}
	newest, cfg, selectionID, trace := p.newest, p.cfg, p.discovery.SnapshotID, p.trace
	p.mu.Unlock()
	if success.Frozen != newest {
		return LastGood{}, launchRefusal(CodeLaunchTupleMismatch, fmt.Errorf("startup names %s, want the prepared %s", success.Frozen.Build, newest.Build))
	}
	trace.append(LaunchTraceEntry{Kind: LaunchExecNewestStart, Tuple: newest, SelectionID: selectionID})
	receipt, err := WriteLastGood(ctx, cfg, success)
	p.mu.Lock()
	p.outcome = OutcomeNewestStarted
	p.mu.Unlock()
	return receipt, err
}

// consumerProbeObservation maps one validated consumer newest-start
// failure to the transient attempt receipt that durably arms it. The
// probe-outcome vocabulary is the store's only failure taxonomy, so
// the mapping is fixed by Attempted/Started: an attempted fork/exec
// without a child records exec_failed, and a started child that
// never established startup records exit_nonzero. The exact consumer
// stage, code and detail travel in Execution and Detail, so the
// durable record preserves the real facts behind the coarse outcome.
// No probe contract ran, so Execution carries no contract identity.
func consumerProbeObservation(failure NewestFailure, selectionID string) ProbeObservation {
	outcome := ProbeExitNonzero
	if !failure.Started {
		outcome = ProbeExecFailed
	}
	detail := failure.Stage + " " + failure.Code
	if failure.Detail != "" {
		detail += ": " + failure.Detail
	}
	return ProbeObservation{
		Execution:   ProbeExecution{Stage: failure.Stage, Attempted: failure.Attempted, Started: failure.Started},
		AttemptID:   failure.AttemptID,
		SelectionID: selectionID,
		Outcome:     outcome,
		Detail:      detail,
		Cleanup:     failure.Cleanup,
	}
}

// ReportNewestStartFailure records the consumer's observed newest
// start failure and arms the permit path for it. Only attempted
// starts with confirmed cleanup qualify: pre-attempt reports,
// unconfirmed teardown, foreign tuples and malformed reports refuse
// without arming anything. A qualifying report is persisted as a
// transient attempt receipt before it arms anything, and the exact
// store error propagates unwrapped when that publication fails, so
// a permit can never issue on an in-memory report alone.
func (p *PreparedLaunch) ReportNewestStartFailure(ctx context.Context, failure NewestFailure) error {
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return err
	}
	if p == nil {
		return launchRefusal(CodeLaunchInvalid, errNoLegStarted)
	}
	p.mu.Lock()
	if p.outcome != OutcomeRunNewest {
		p.mu.Unlock()
		return launchRefusal(CodeLaunchInvalid, errLegAlreadyReported)
	}
	newest, selectionID, trace, cfg := p.newest, p.discovery.SnapshotID, p.trace, p.cfg
	p.mu.Unlock()
	if failure.Newest != newest {
		return launchRefusal(CodeLaunchTupleMismatch, fmt.Errorf("failure names %s, want the prepared %s", failure.Newest.Build, newest.Build))
	}
	if !failure.Attempted {
		return launchRefusal(CodeLaunchRecoveryUnavailable, fmt.Errorf("stage %s never attempted a start", failure.Stage))
	}
	if failure.Cleanup != CleanupConfirmed {
		return launchRefusal(CodeLaunchRecoveryUnavailable, fmt.Errorf("stage %s left cleanup unconfirmed", failure.Stage))
	}
	if !validAttemptToken(failure.AttemptID) {
		return launchRefusal(CodeLaunchInvalid, errBadAttemptToken)
	}
	if failure.Stage == "" || failure.Code == "" {
		return launchRefusal(CodeLaunchInvalid, fmt.Errorf("failure carries no stage or code"))
	}
	obs := consumerProbeObservation(failure, selectionID)
	record, err := RecordProbeOutcome(ctx, cfg, newest, obs)
	if err != nil {
		return err
	}
	if record.Receipt == nil {
		// Fail closed on contract drift: a transient recording
		// without its receipt arms nothing. The receipt check is
		// also load-bearing for the narrowing mutant that swallows
		// the publication error above: without it the mutant would
		// dereference nil instead of failing the killer.
		return launchRefusal(CodeLaunchRecoveryUnavailable, errors.New("consumer failure recorded no receipt"))
	}
	trace.append(LaunchTraceEntry{Kind: LaunchExecNewestStart, Tuple: newest, SelectionID: selectionID, Permitted: false})
	p.mu.Lock()
	p.failure = launchFailure{
		kind: failureConsumerReport, attemptID: failure.AttemptID,
		stage: failure.Stage, code: failure.Code,
		cause: failureCause(failure), newest: newest,
		receipt: *record.Receipt, hasProof: true,
	}
	p.outcome = OutcomeFailedNewest
	p.mu.Unlock()
	return nil
}

// failureCause chains the consumer's reported failure as the primary
// error the fallback errors travel with.
func failureCause(failure NewestFailure) error {
	detail := failure.Detail
	if detail == "" {
		detail = "consumer reported a newest start failure"
	}
	cause := fmt.Errorf("newest %s failed at %s %s (attempted=%v started=%v): %s", failure.Newest.Build, failure.Stage, failure.Code, failure.Attempted, failure.Started, detail)
	if failure.Err == nil {
		return cause
	}
	return fmt.Errorf("%s: %w", cause.Error(), failure.Err)
}

// verifyFailureEvidence binds permit issuance to the durable
// failure record behind the armed failure. Live-probe and consumer
// failures re-read their attempt receipt and require it to agree
// with the armed record; recorded BAD failures re-read the verdict
// and require the same code. A vanished, corrupt or disagreeing
// record refuses with the typed launch code and the exact store
// error chained, so the permit never issues on memory alone.
func (p *PreparedLaunch) verifyFailureEvidence(ctx context.Context, failure launchFailure, cfg HostConfig) error {
	switch failure.kind {
	case failureLiveProbe, failureConsumerReport:
		if !failure.hasProof {
			return launchRefusal(CodeLaunchRecoveryUnavailable, errNoFailureArmed)
		}
		receipt, err := ReadAttemptReceipt(ctx, cfg, failure.newest, failure.attemptID)
		if err != nil {
			return launchRefusal(CodeLaunchRecoveryUnavailable, err)
		}
		if !receipt.equivalent(failure.receipt) {
			return launchRefusal(CodeLaunchRecoveryUnavailable, fmt.Errorf("attempt %s no longer records the armed failure", failure.attemptID))
		}
		return nil
	case failureRecordedBAD:
		verdict, present, err := BadRecord(ctx, cfg, failure.newest)
		if err != nil {
			return launchRefusal(CodeLaunchRecoveryUnavailable, err)
		}
		if !present {
			return launchRefusal(CodeLaunchRecoveryUnavailable, fmt.Errorf("newest %s no longer carries its recorded verdict", failure.newest.Build))
		}
		if verdict.Code != failure.code {
			return launchRefusal(CodeLaunchRecoveryUnavailable, fmt.Errorf("recorded %s is not the armed %s", verdict.Code, failure.code))
		}
		return nil
	default:
		return launchRefusal(CodeLaunchRecoveryUnavailable, errNoFailureArmed)
	}
}

// RequestPermit issues the invocation's one fallback permit from its
// recorded newest failure and the validated last-good receipt. The
// armed failure is re-verified against its durable record before
// anything else: a permit without failure evidence is not offered.
// The explicit assertion, if any, must match the receipt; interactive
// invocations get exactly one offer and unattended invocations fall
// back automatically. The fallback warning is persisted before the
// grant returns, so every later probe and the leg run under a
// durable permit. Declines, like grants, spend the allowance.
func (p *PreparedLaunch) RequestPermit(ctx context.Context, reporter StartOutcomeReporter) (PermitGrant, error) {
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return PermitGrant{}, err
	}
	if p == nil || p.runner == nil {
		return PermitGrant{}, launchRefusal(CodeLaunchInvalid, errNoLegStarted)
	}
	if reporter == nil {
		return PermitGrant{}, launchRefusal(CodeLaunchInvalid, errNilReporter)
	}
	if err := p.runner.enter(ctx); err != nil {
		return PermitGrant{}, err
	}
	defer p.runner.leave()
	p.mu.Lock()
	if !failureEligible(p.failure.kind) {
		p.mu.Unlock()
		return PermitGrant{}, launchRefusal(CodeLaunchRecoveryUnavailable, errNoFailureArmed)
	}
	if p.permitDecided {
		p.mu.Unlock()
		return PermitGrant{}, launchRefusal(CodeLaunchRecoveryUnavailable, errPermitDecided)
	}
	p.runner.mu.Lock()
	spent := p.runner.recoveryUsed
	p.runner.mu.Unlock()
	if spent {
		p.mu.Unlock()
		return PermitGrant{}, launchRefusal(CodeLaunchRecoveryUnavailable, errAllowanceSpent)
	}
	failure, cfg, req, selectionID := p.failure, p.cfg, p.req, p.discovery.SnapshotID
	p.mu.Unlock()
	if err := p.verifyFailureEvidence(ctx, failure, cfg); err != nil {
		return PermitGrant{}, err
	}
	// The receipt error propagates unwrapped: absent and unusable
	// are distinct facts and the L4 surface shows the exact one.
	// An explicit assertion without a recorded target is the caller's
	// config error, not a missing receipt: section 5.1 names typed
	// invalid_config for a different or unrecorded assertion, with
	// the underlying absence chained.
	good, err := ReadLastGood(ctx, cfg)
	if err != nil {
		if req.ExplicitLastGood != "" {
			if lastGoodErr, ok := AsLastGoodError(err); ok && lastGoodErr.Code == CodeLastGoodAbsent {
				return PermitGrant{}, launchRefusal(CodeLaunchInvalidConfig, err)
			}
		}
		return PermitGrant{}, err
	}
	if good.Frozen.Build == failure.newest.Build && good.Frozen.SHA256 == failure.newest.SHA256 {
		return PermitGrant{}, launchRefusal(CodeLaunchRecoveryUnavailable, errSameTuple)
	}
	if req.ExplicitLastGood != "" && req.ExplicitLastGood != good.Frozen.Path {
		return PermitGrant{}, launchRefusal(CodeLaunchInvalidConfig, fmt.Errorf("explicit last-good %q is not the receipt %q", req.ExplicitLastGood, good.Frozen.Path))
	}
	event := RecoveryEvent{
		Code: CodeFallbackWarning, State: "fallback_attempting",
		Newest:         FrozenReceipt{Build: failure.newest.Build, SHA256: failure.newest.SHA256, Path: failure.newest.Path},
		FailureAttempt: failure.attemptID, FailureStage: failure.stage, FailureCode: failure.code,
		LastGood: good, Mode: req.Mode,
	}
	if req.Mode == RecoveryInteractive {
		offerCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		consent, offerErr := reporter.Offer(offerCtx, event)
		expired := offerCtx.Err()
		cancel()
		if offerErr != nil || expired != nil || !consent {
			p.spendAllowance()
			return PermitGrant{Outcome: PermitDeclined}, launchRefusal(CodeLaunchRecoveryUnavailable, errors.Join(offerErr, expired))
		}
		event.Consent = true
	}
	if err := issuePermitDocument(ctx, cfg, event); err != nil {
		return PermitGrant{}, err
	}
	permit := FallbackPermit{
		issued: true, selectionID: selectionID, newest: failure.newest,
		lastGood: good, surface: req.Surface, mode: req.Mode, consent: event.Consent,
		attemptID: failure.attemptID, failureStage: failure.stage, failureCode: failure.code,
		cause: failure.cause, runner: p.runner, cfg: cfg,
		state: &permitState{}, trace: p.trace,
	}
	p.mu.Lock()
	p.permitDecided = true
	p.mu.Unlock()
	p.spendAllowance()
	outcome := PermitAutoGranted
	if req.Mode == RecoveryInteractive {
		outcome = PermitOfferGranted
	}
	return PermitGrant{Permit: permit, Outcome: outcome, Warning: CodeFallbackWarning}, nil
}

// spendAllowance marks the invocation's single fallback allowance
// spent on the shared runner, so a direct Recover beside this
// controller cannot offer or start a second fallback leg.
func (p *PreparedLaunch) spendAllowance() {
	p.runner.mu.Lock()
	p.runner.recoveryUsed = true
	p.runner.mu.Unlock()
}

// issuePermitDocument durably publishes the fallback-attempting
// warning before any fallback probe or exec. The first publication
// wins; a second permit for the same attempt refuses without
// overwriting. No lock is held across the offer: this runs either
// before any offer on the unattended path after eligibility, or
// after consent on the interactive path.
func issuePermitDocument(ctx context.Context, cfg HostConfig, event RecoveryEvent) error {
	store, err := openFrozenStore(ctx, cfg.FrozenRoot)
	if err != nil {
		return &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
	}
	defer store.close()
	release, err := acquireMaintenanceLeaseAt(ctx, store.root)
	if err != nil {
		return &ProbeError{Stage: StageRecovery, Code: CodeProbeLockFailed, Cause: err}
	}
	defer func() { _ = release() }()
	unlock, err := acquireStateLockAt(ctx, store.root)
	if err != nil {
		return &ProbeError{Stage: StageRecovery, Code: CodeProbeLockFailed, Cause: err}
	}
	defer func() { _ = unlock() }()
	return publishRecovery(ctx, store.root, event, true)
}

// ProbeLease is one outstanding fallback probe under a live permit:
// the exact tuple AuthorizeProbe admitted. The consumer runs the
// probe, then releases the lease; the fallback leg refuses while any
// lease is outstanding, so consumption can never overlap a running
// probe. The outstanding count lives on the shared permit state, so
// a lease from one copy gates a leg from another. The zero value
// admits nothing; only AuthorizeProbe issues a lease.
type ProbeLease struct {
	mu       sync.Mutex
	permit   *FallbackPermit
	tuple    FrozenIdentity
	released bool
}

// Tuple returns the exact tuple this lease admits.
func (l *ProbeLease) Tuple() FrozenIdentity {
	if l == nil {
		return FrozenIdentity{}
	}
	return l.tuple
}

// Release ends the outstanding probe. It is idempotent: a second
// release is a no-op, so deferred releases never corrupt the shared
// count. A release after the leg is harmless but authorizes nothing
// further.
func (l *ProbeLease) Release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	already := l.released
	l.released = true
	l.mu.Unlock()
	if already {
		return
	}
	if l.permit == nil || l.permit.state == nil {
		return
	}
	l.permit.state.mu.Lock()
	if l.permit.state.active > 0 {
		l.permit.state.active--
	}
	l.permit.state.mu.Unlock()
}

// AuthorizeProbe authorizes one fallback probe of the exact
// permit-bound tuple and returns its lease: the --version or --help
// executions the L2M/L3 consumers run while composing the fallback
// leg. It requires a live issued permit for this invocation, before
// the single leg is consumed and for the exact tuple. The consumed
// check, the tuple check and the probe count move under one hold on
// the shared state, so a leg from any copy cannot slip between the
// check and the count. Issuance already binds consent, so probes
// under a live permit always run post-consent. Unissued, consumed
// and foreign-tuple probes refuse, and refused probes record
// nothing.
func (p *FallbackPermit) AuthorizeProbe(ctx context.Context, tuple FrozenIdentity) (*ProbeLease, error) {
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return nil, err
	}
	if p == nil || !p.issued || p.state == nil {
		return nil, launchRefusal(CodeLaunchRecoveryUnavailable, errNoFailureArmed)
	}
	// Consent binds at issuance: an interactive permit exists only
	// after an explicit grant, so every probe under it runs
	// post-consent by construction. Unissued probes refuse above.
	p.state.mu.Lock()
	if p.state.attempts > 0 {
		p.state.mu.Unlock()
		return nil, launchRefusal(CodeLaunchPermitConsumed, errPermitDecided)
	}
	if tuple.Build != p.lastGood.Frozen.Build || tuple.SHA256 != p.lastGood.Frozen.SHA256 || tuple.Path != p.lastGood.Frozen.Path {
		p.state.mu.Unlock()
		return nil, launchRefusal(CodeLaunchTupleMismatch, fmt.Errorf("probe names %s, want the permitted %s", tuple.Build, p.lastGood.Frozen.Build))
	}
	p.state.probes++
	p.state.active++
	p.state.mu.Unlock()
	p.trace.append(LaunchTraceEntry{Kind: LaunchExecFallbackProbe, Tuple: tuple, SelectionID: p.selectionID, Permitted: true})
	return &ProbeLease{permit: p, tuple: tuple}, nil
}

// StartFallback starts the exact last-good once under the permit and
// admits its receipt when it actually starts. The durable receipt is
// re-read under the state lock before starting: a changed target
// refuses, in both modes, and a same-tuple newer sequence is adopted
// with updated provenance. The fallback target applies as one full
// tuple from the revalidated receipt. Consumption moves under the
// same hold as probe authorization, and refuses while any probe
// lease is outstanding. A second call on any copy of the permit
// refuses; a failed leg returns the primary error first and the
// fallback error second, and never clears last-good.
func (p *FallbackPermit) StartFallback(ctx context.Context, reporter StartOutcomeReporter) (FallbackResult, error) {
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return FallbackResult{}, err
	}
	if p == nil || !p.issued || p.state == nil || p.runner == nil {
		return FallbackResult{}, launchRefusal(CodeLaunchRecoveryUnavailable, errNoFailureArmed)
	}
	if reporter == nil {
		return FallbackResult{}, launchRefusal(CodeLaunchInvalid, errNilReporter)
	}
	if err := p.runner.enter(ctx); err != nil {
		return FallbackResult{}, err
	}
	defer p.runner.leave()
	p.state.mu.Lock()
	if p.state.attempts > 0 {
		p.state.mu.Unlock()
		return FallbackResult{}, launchRefusal(CodeLaunchPermitConsumed, errPermitDecided)
	}
	if p.state.active > 0 {
		p.state.mu.Unlock()
		return FallbackResult{}, launchRefusal(CodeLaunchProbeOutstanding, errProbeOutstanding)
	}
	p.state.attempts++
	p.state.mu.Unlock()
	event := RecoveryEvent{
		Code: CodeFallbackWarning, State: "fallback_attempting",
		Newest:         FrozenReceipt{Build: p.newest.Build, SHA256: p.newest.SHA256, Path: p.newest.Path},
		FailureAttempt: p.attemptID, FailureStage: p.failureStage, FailureCode: p.failureCode,
		LastGood: p.lastGood, Mode: p.mode, Consent: p.consent,
	}
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return FallbackResult{Event: event}, err
	}
	store, err := openFrozenStore(ctx, p.cfg.FrozenRoot)
	if err != nil {
		return FallbackResult{Event: event}, &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
	}
	defer store.close()
	release, err := acquireMaintenanceLeaseAt(ctx, store.root)
	if err != nil {
		return FallbackResult{Event: event}, &ProbeError{Stage: StageRecovery, Code: CodeProbeLockFailed, Cause: err}
	}
	defer func() { _ = release() }()
	// No metadata lock is held over the fallback startup. The use
	// lease spans verification, startup and successful admission.
	unlock, err := acquireStateLockAt(ctx, store.root)
	if err != nil {
		return FallbackResult{Event: event}, &ProbeError{Stage: StageRecovery, Code: CodeProbeLockFailed, Cause: err}
	}
	consumed, target, file, leaf, consumeErr := consumePermitLeg(ctx, store, p.cfg, event)
	_ = unlock()
	if consumeErr != nil {
		return FallbackResult{Event: event}, consumeErr
	}
	event = consumed
	defer file.Close()
	defer leaf.close()
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return FallbackResult{Event: event}, err
	}
	p.trace.append(LaunchTraceEntry{Kind: LaunchExecFallbackStart, Tuple: target, SelectionID: p.selectionID, Permitted: true})
	startCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
	outcome := reporter.Start(startCtx, event, file)
	deadlineErr := startCtx.Err()
	cancel()
	if outcome.Cleanup != CleanupConfirmed {
		p.runner.mu.Lock()
		p.runner.blocked = true
		p.runner.mu.Unlock()
	}
	if deadlineErr != nil {
		outcome.Err = errors.Join(outcome.Err, deadlineErr)
	}
	if outcome.Cleanup != CleanupConfirmed {
		outcome.Err = errors.Join(outcome.Err, errors.New("fallback cleanup unconfirmed"))
	}
	if outcome.Err == nil {
		if outcome.Success.Frozen != target {
			outcome.Err = errors.New("fallback success disagrees with the started tuple")
		} else {
			_, outcome.Err = WriteLastGood(ctx, p.cfg, outcome.Success)
		}
	}
	if outcome.Err != nil {
		event.State = "fallback_failed"
	} else {
		event.State = "fallback_started"
	}
	publicationErr := publishRecovery(ctx, store.root, event, false)
	if outcome.Err != nil {
		return FallbackResult{Event: event}, errors.Join(p.cause, &ProbeError{Stage: StageRecovery, Code: CodeFallbackFailed, Cause: outcome.Err}, publicationErr)
	}
	receipt, err := ReadLastGood(ctx, p.cfg)
	if err != nil {
		return FallbackResult{Event: event}, errors.Join(publicationErr, err)
	}
	return FallbackResult{Event: event, Receipt: receipt}, publicationErr
}

// consumePermitLeg revalidates the permit under the state lock and
// opens the verified fallback bytes. The durable receipt is re-read:
// a differing tuple refuses with the target-changed reason and a
// same-tuple newer sequence is adopted. The fallback target applies
// as one full tuple from the revalidated receipt, and only PROBE_OK
// bytes whose digest matches reach the leg.
func consumePermitLeg(ctx context.Context, store *frozenStore, cfg HostConfig, event RecoveryEvent) (RecoveryEvent, FrozenIdentity, *os.File, *pinnedDir, error) {
	refuse := func(code string, err error) (RecoveryEvent, FrozenIdentity, *os.File, *pinnedDir, error) {
		return event, FrozenIdentity{}, nil, nil, &ProbeError{Stage: StageRecovery, Code: code, Cause: err}
	}
	current, present, err := readLastGoodDocument(ctx, store.root)
	if err != nil || !present {
		return refuse(CodeRecoveryUnavailable, err)
	}
	if current.Frozen != event.LastGood.Frozen || current.Sequence < event.LastGood.Sequence {
		return refuse(CodeFallbackTargetChanged, nil)
	}
	event.LastGood = current
	target := FrozenIdentity{Build: current.Frozen.Build, SHA256: current.Frozen.SHA256, Path: current.Frozen.Path}
	id := target
	if err := validateFrozenReference(cfg.FrozenRoot, id); err != nil {
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
	if err != nil {
		_ = file.Close()
		_ = leaf.close()
		return refuse(CodeRecoveryUnavailable, err)
	}
	return event, target, file, leaf, nil
}

// Start runs the already-frozen deferral target once with its
// host-cap warning: the Amendment 2 route-(ii) leg. The target must
// be verified PROBE_OK bytes and still the newest frozen build; an
// unprobed or superseded target refuses without starting anything.
// Interactive legs offer the target first and run only on explicit
// consent, while unattended legs run on the warning alone. The
// target's state binds before the offer — an unrunnable target is
// never offered — and its bytes bind again before the start, so a
// concurrent newer freeze or state change cannot slip between the
// offer and the leg. No lock is held across the offer itself. A
// successful start admits the last-good receipt for the target. A
// failed leg returns its own error and arms no failure: deferral is
// not a newest start failure, so no fallback permit can follow it.
func (a *DeferralAuthorization) Start(ctx context.Context, reporter DeferredReporter) (LastGood, error) {
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return LastGood{}, err
	}
	if a == nil || !a.valid || a.runner == nil {
		return LastGood{}, launchRefusal(CodeLaunchInvalid, errNoLegStarted)
	}
	if reporter == nil {
		return LastGood{}, launchRefusal(CodeLaunchInvalid, errNilReporter)
	}
	if err := a.runner.enter(ctx); err != nil {
		return LastGood{}, err
	}
	defer a.runner.leave()
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return LastGood{}, err
	}
	store, err := openFrozenStore(ctx, a.cfg.FrozenRoot)
	if err != nil {
		return LastGood{}, &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
	}
	defer store.close()
	if err := a.checkDeferralOfferable(ctx, store); err != nil {
		return LastGood{}, err
	}
	start := DeferralStart{Target: a.target, Newest: a.newest, Warning: a.warning, AttemptID: a.attemptID, Mode: a.mode}
	if a.mode == RecoveryInteractive {
		offerCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		granted, offerErr := reporter.Offer(offerCtx, start)
		expired := offerCtx.Err()
		cancel()
		if offerErr != nil || expired != nil || !granted {
			return LastGood{}, launchRefusal(CodeLaunchDeferralDeclined, errors.Join(offerErr, expired))
		}
		start.Consented = true
	}
	release, err := acquireMaintenanceLeaseAt(ctx, store.root)
	if err != nil {
		return LastGood{}, &ProbeError{Stage: StageRecovery, Code: CodeProbeLockFailed, Cause: err}
	}
	defer func() { _ = release() }()
	// No metadata lock is held over the deferred startup. The use
	// lease spans verification, startup and successful admission.
	unlock, err := acquireStateLockAt(ctx, store.root)
	if err != nil {
		return LastGood{}, &ProbeError{Stage: StageRecovery, Code: CodeProbeLockFailed, Cause: err}
	}
	file, leaf, verifyErr := verifyDeferredTarget(ctx, store, a.cfg, a.target)
	if verifyErr == nil {
		verifyErr = requireNewestFrozenTarget(ctx, store, a.target)
	}
	_ = unlock()
	if verifyErr != nil {
		if file != nil {
			_ = file.Close()
		}
		if leaf != nil {
			_ = leaf.close()
		}
		return LastGood{}, verifyErr
	}
	defer file.Close()
	defer leaf.close()
	if err := launchRefuseIfCanceled(ctx); err != nil {
		return LastGood{}, err
	}
	a.trace.append(LaunchTraceEntry{Kind: LaunchExecDeferredStart, Tuple: a.target, SelectionID: a.selectionID})
	startCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
	outcome := reporter.Start(startCtx, start, file)
	deadlineErr := startCtx.Err()
	cancel()
	if outcome.Cleanup != CleanupConfirmed {
		a.runner.mu.Lock()
		a.runner.blocked = true
		a.runner.mu.Unlock()
	}
	if deadlineErr != nil {
		outcome.Err = errors.Join(outcome.Err, deadlineErr)
	}
	if outcome.Cleanup != CleanupConfirmed {
		outcome.Err = errors.Join(outcome.Err, errors.New("deferred cleanup unconfirmed"))
	}
	if outcome.Err == nil {
		if outcome.Success.Frozen != a.target {
			outcome.Err = errors.New("deferred success disagrees with the started tuple")
		} else {
			_, outcome.Err = WriteLastGood(ctx, a.cfg, outcome.Success)
		}
	}
	if outcome.Err != nil {
		return LastGood{}, outcome.Err
	}
	return ReadLastGood(ctx, a.cfg)
}

// verifyDeferredTarget opens the deferral target's verified PROBE_OK
// bytes under the state lock. Anything else refuses: missing or
// disagreeing state, bytes that disagree, or a target that never
// completed its first probe.
func verifyDeferredTarget(ctx context.Context, store *frozenStore, cfg HostConfig, target FrozenIdentity) (*os.File, *pinnedDir, error) {
	refuse := func(err error) (*os.File, *pinnedDir, error) {
		return nil, nil, err
	}
	if err := validateFrozenReference(cfg.FrozenRoot, target); err != nil {
		return refuse(&ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err})
	}
	key := freezeKey(target.Build, target.SHA256)
	state, present, err := readBuildStateAt(ctx, store.meta, key)
	if err != nil || !present || state.agreesWith(key, target) != nil {
		return refuse(&ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err})
	}
	if state.State != StateProbeOK {
		return refuse(launchRefusal(CodeLaunchDeferredUnprobed, fmt.Errorf("deferred %s is %s, not PROBE_OK", key, state.State)))
	}
	leaf, file, err := openFrozenBytes(store, key, target.Build, true)
	if err != nil {
		return refuse(&ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err})
	}
	digest, err := hashFileDescriptor(ctx, StageRecovery, file)
	if err == nil && digest != target.SHA256 {
		err = errors.New("deferred bytes disagree with the frozen target")
	}
	if err != nil {
		_ = file.Close()
		_ = leaf.close()
		return refuse(&ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err})
	}
	return file, leaf, nil
}

// checkDeferralOfferable binds the deferral target's state before any
// offer: PROBE_OK state agreeing with the target, and still the
// newest frozen build. An unrunnable target is never offered. Bytes
// bind later, before the start; the state lock is released before
// return so no lock spans the offer.
func (a *DeferralAuthorization) checkDeferralOfferable(ctx context.Context, store *frozenStore) error {
	unlock, err := acquireStateLockAt(ctx, store.root)
	if err != nil {
		return &ProbeError{Stage: StageRecovery, Code: CodeProbeLockFailed, Cause: err}
	}
	defer func() { _ = unlock() }()
	key := freezeKey(a.target.Build, a.target.SHA256)
	state, present, err := readBuildStateAt(ctx, store.meta, key)
	if err != nil || !present || state.agreesWith(key, a.target) != nil {
		return &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
	}
	if state.State != StateProbeOK {
		return launchRefusal(CodeLaunchDeferredUnprobed, fmt.Errorf("deferred %s is %s, not PROBE_OK", key, state.State))
	}
	return requireNewestFrozenTarget(ctx, store, a.target)
}

// requireNewestFrozenTarget refuses unless target still names the
// newest frozen build key: no strictly newer build may exist in the
// held store. A corrupt or unreadable inventory refuses instead of
// guessing. Callers hold the state lock, so a sequential superseding
// freeze is observed deterministically; a publication racing the
// inventory read itself is the same documented bound as the M70
// receipt re-read.
func requireNewestFrozenTarget(ctx context.Context, store *frozenStore, target FrozenIdentity) error {
	entries, err := store.builds.entries()
	if err != nil {
		return &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: fmt.Errorf("frozen builds unreadable: %w", err)}
	}
	wanted, err := muse.ParseBuildID(target.Build)
	if err != nil {
		return &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
	}
	for _, entry := range entries {
		if err := launchRefuseIfCanceled(ctx); err != nil {
			return err
		}
		name := entry.Name()
		if !entry.IsDir() {
			return &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: fmt.Errorf("frozen builds hold file %q", name)}
		}
		build, _, err := parseFrozenKey(name)
		if err != nil {
			return &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
		}
		id, err := muse.ParseBuildID(build)
		if err != nil {
			return &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
		}
		order, err := muse.CompareBuildIDs(id, wanted)
		if err != nil {
			return &ProbeError{Stage: StageRecovery, Code: CodeRecoveryUnavailable, Cause: err}
		}
		if order > 0 {
			return launchRefusal(CodeLaunchTupleMismatch, fmt.Errorf("deferred %s is superseded by frozen %s", target.Build, build))
		}
	}
	return nil
}
