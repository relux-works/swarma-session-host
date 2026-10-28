//go:build darwin || linux

package musebuilds

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/relux-works/swarma-session-host/musebuilds/probeprocess"
)

// execRecord observes an attempted process or an ordered recovery event.
// Permit identity is the shared allowance, not a nonempty token or tuple.
type execRecord struct {
	stage  string
	tuple  FrozenIdentity
	permit *permitState
	failed bool
}

// execRecorder is installed before preparation. Every fake execution boundary
// uses it, including Start calls that fail. Production Trace is not an oracle.
// Route-(ii) deferral legs record here too: deferral names the authorized
// newest-frozen target, and deferred-start attempts assert against it with
// no fallback permit and no exemption.
type execRecorder struct {
	t        *testing.T
	cfg      HostConfig
	prep     *PreparedLaunch
	good     LastGood
	attempt  string
	decision string
	permit   *FallbackPermit
	deferral FrozenIdentity
	records  []execRecord
}

func (r *execRecorder) add(stage string, tuple FrozenIdentity, permit *permitState, failed bool) {
	r.records = append(r.records, execRecord{stage, tuple, permit, failed})
}

func (r *execRecorder) require(want ...execRecord) {
	r.t.Helper()
	if !reflect.DeepEqual(r.records, want) {
		r.t.Fatalf("attempt/event stream\n got: %+v\nwant: %+v", r.records, want)
	}
}

func (r *execRecorder) proof() {
	r.t.Helper()
	newest := r.prep.Newest()
	if r.prep.failure.kind == failureRecordedBAD {
		bad, present, err := BadRecord(context.Background(), r.cfg, newest)
		if err != nil || !present || bad.Code != r.prep.failure.code {
			r.t.Fatalf("recorded failure not durable: %+v present=%v err=%v", bad, present, err)
		}
		return
	}
	proof, err := ReadAttemptReceipt(context.Background(), r.cfg, newest, r.attempt)
	if err != nil || proof.Build != newest.Build || proof.SHA256 != newest.SHA256 ||
		proof.SelectionID != r.prep.Discovery().SnapshotID || proof.AttemptID != r.attempt ||
		proof.Cleanup != CleanupConfirmed || !proof.Execution.Attempted || proof.Outcome == ProbeExitOKMatch {
		r.t.Fatalf("newest failure not durable before recovery: %+v err=%v", proof, err)
	}
}

func (r *execRecorder) warning() RecoveryEvent {
	r.t.Helper()
	raw, err := os.ReadFile(filepath.Join(r.cfg.FrozenRoot, "fallback-"+r.attempt+".json"))
	if err != nil {
		r.t.Fatalf("fallback warning not durable before execution: %v", err)
	}
	event, err := recoveryDocument.parse(raw)
	n := r.prep.Newest()
	if err != nil || event.State != "fallback_attempting" || event.Code != CodeFallbackWarning ||
		event.Newest != (FrozenReceipt{Build: n.Build, SHA256: n.SHA256, Path: n.Path}) ||
		event.LastGood != r.good || event.FailureAttempt != r.attempt ||
		event.FailureStage != r.prep.failure.stage || event.FailureCode != r.prep.failure.code ||
		event.Mode != r.prep.req.Mode || event.Consent != (r.prep.req.Mode == RecoveryInteractive) {
		r.t.Fatalf("fallback warning binding: %+v err=%v", event, err)
	}
	return event
}

// observe reads the held executable at the actual fake Start boundary. The
// attempted call is captured before checking authority, so an unauthorized
// attempt cannot disappear merely because a fake refuses to execute it.
func (r *execRecorder) observe(ctx context.Context, stage string, file *os.File, lease *ProbeLease, failed bool) {
	r.t.Helper()
	digest, err := hashFileDescriptor(ctx, StageLaunch, file)
	if err != nil {
		r.t.Fatal(err)
	}
	tuple := FrozenIdentity{Build: strings.TrimPrefix(filepath.Base(file.Name()), pinnedNamePrefix), SHA256: digest, Path: file.Name()}
	var identity *permitState
	if r.permit != nil {
		identity = r.permit.state
	}
	r.add(stage, tuple, identity, failed)
	if stage == "deferred-start" {
		if r.permit != nil {
			r.t.Fatalf("deferral attempted under a fallback permit: %+v", r.records)
		}
		if tuple != r.deferral {
			r.t.Fatalf("deferral tuple %+v is not the authorized %+v", tuple, r.deferral)
		}
		if r.prep == nil || tuple == r.prep.Newest() {
			r.t.Fatalf("deferral did not leave the installed newest: %+v", r.records)
		}
		if !strings.HasPrefix(tuple.Path, r.cfg.FrozenRoot+"/") {
			r.t.Fatalf("deferral target is not frozen-root-contained: %+v", tuple)
		}
		state, err := ReadBuildState(ctx, r.cfg, tuple)
		if err != nil || state.State != StateProbeOK || state.agreesWith(freezeKey(tuple.Build, tuple.SHA256), tuple) != nil {
			r.t.Fatalf("deferral target is not verified PROBE_OK: %+v %v", state, err)
		}
		if r.prep.req.Mode == RecoveryInteractive && r.decision != "accept" {
			r.t.Fatalf("deferral execution preceded explicit consent: %+v", r.records)
		}
		requireNoFallbackDocument(r.t, r.cfg.FrozenRoot, r.attempt)
		return
	}
	if !strings.HasPrefix(stage, "fallback-") {
		if identity != nil || (r.prep != nil && tuple != r.prep.Newest()) {
			r.t.Fatalf("primary execution is not the selected newest: %+v", r.records)
		}
		return
	}
	if r.permit == nil || !r.permit.Issued() || identity == nil {
		r.t.Fatalf("fallback attempted without a permit: %+v", r.records)
	}
	p := r.permit
	if p.Newest() != r.prep.Newest() || p.LastGood() != r.good || tuple != receiptTuple(r.good) ||
		p.selectionID != r.prep.Discovery().SnapshotID || p.attemptID != r.attempt || p.Surface() != r.prep.req.Surface || p.Mode() != r.prep.req.Mode {
		r.t.Fatalf("execution disagrees with invocation/permit tuple: %+v", r.records)
	}
	if p.Mode() == RecoveryInteractive && (!p.Consented() || r.decision != "accept") {
		r.t.Fatalf("fallback execution preceded explicit consent: %+v", r.records)
	}
	p.state.mu.Lock()
	attempts, active := p.state.attempts, p.state.active
	p.state.mu.Unlock()
	if stage == "fallback-start" {
		if attempts != 1 || active != 0 {
			r.t.Fatalf("session lacks sole consumed allowance: attempts=%d probes=%d", attempts, active)
		}
	} else {
		if lease == nil {
			r.t.Fatalf("fallback probe attempted without a lease: %+v", r.records)
		}
		lease.mu.Lock()
		live := !lease.released && lease.permit != nil && lease.permit.state == identity && lease.tuple == tuple
		lease.mu.Unlock()
		if !live || attempts != 0 || active != 1 {
			r.t.Fatalf("probe lacks a live exact-tuple lease: live=%v attempts=%d active=%d", live, attempts, active)
		}
	}
	r.proof()
	r.warning()
}

func receiptTuple(good LastGood) FrozenIdentity {
	return FrozenIdentity{Build: good.Frozen.Build, SHA256: good.Frozen.SHA256, Path: good.Frozen.Path}
}

// recordingProbeLauncher is shared by the first-probe runner and the consumer
// probe fake. Failed fork attempts are recorded before returning their error.
type recordingProbeLauncher struct {
	recorder *execRecorder
	stage    string
	lease    *ProbeLease
	fail     error
}

func (l *recordingProbeLauncher) Start(ctx context.Context, file *os.File, _ []string) (probeprocess.Process, error) {
	l.recorder.observe(ctx, l.stage, file, l.lease, l.fail != nil)
	if l.fail != nil {
		return nil, l.fail
	}
	build := strings.TrimPrefix(filepath.Base(file.Name()), pinnedNamePrefix)
	release := strings.Split(build, "-R")[0]
	return finishedProbe("Muse Code " + release + " (" + build + ")\n"), nil
}

type recordingReporter struct {
	recorder *execRecorder
	consent  bool
	fail     error
}

// newest is the fake consumer's primary session/probe boundary. Consumers
// report this observed outcome through PreparedLaunch; they do not pre-arm it.
func (s *recordingReporter) newest(ctx context.Context, stage string, file *os.File, fail bool) StartOutcome {
	r := s.recorder
	r.observe(ctx, stage, file, nil, fail)
	if fail {
		return StartOutcome{Err: errors.New("newest startup failed"), Cleanup: CleanupConfirmed}
	}
	success := genuineSuccess(r.prep.Newest(), "trace-newest-start")
	success.Surface = r.prep.req.Surface
	return StartOutcome{Success: success, Cleanup: CleanupConfirmed}
}

func (s *recordingReporter) Offer(_ context.Context, event RecoveryEvent) (bool, error) {
	r := s.recorder
	r.proof()
	requireNoFallbackDocument(r.t, r.cfg.FrozenRoot, r.attempt)
	if event.LastGood != r.good || event.FailureAttempt != r.attempt || event.Mode != RecoveryInteractive || event.Consent {
		r.t.Fatalf("offer not bound to the failed newest/receipt: %+v", event)
	}
	r.add("offer", receiptTuple(r.good), nil, false)
	r.decision = "decline"
	if s.consent {
		r.decision = "accept"
	}
	r.add(r.decision, receiptTuple(r.good), nil, false)
	return s.consent, nil
}

func (s *recordingReporter) Start(ctx context.Context, event RecoveryEvent, file *os.File) StartOutcome {
	r := s.recorder
	r.observe(ctx, "fallback-start", file, nil, s.fail != nil)
	if event != r.warning() {
		r.t.Fatal("session event differs from durable warning")
	}
	if s.fail != nil {
		return StartOutcome{Err: s.fail, Cleanup: CleanupConfirmed}
	}
	success := genuineSuccess(receiptTuple(r.good), "trace-fallback-start")
	success.Surface = r.prep.req.Surface
	return StartOutcome{Success: success, Cleanup: CleanupConfirmed}
}

type launchTraceFixture struct {
	f        *freezeFixture
	r        *execRecorder
	reporter *recordingReporter
}

// source selects the failed attempted execution, or an already-recorded BAD.
// All instrumentation is installed before the public preparation call.
func prepareLaunchTrace(t *testing.T, route LaunchRoute, surface string, mode RecoveryMode, source string) *launchTraceFixture {
	t.Helper()
	f := newFreeze(t)
	_, good := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-trace")
	install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.3-R5000", "1.4.4-R5419.1")
	if source == "recorded-bad" || source == "recorded-bad-scan" {
		frozen, d := probeBuild(t, f, "1.4.4-R5419.1")
		markBADForLaunch(t, f.config(), frozen, d.SnapshotID)
		if source == "recorded-bad-scan" {
			if err := os.Remove(filepath.Join(install.dir, sidecarName)); err != nil {
				t.Fatal(err)
			}
		}
	}
	r := &execRecorder{t: t, cfg: f.config(), good: good, attempt: "l2c-trace"}
	reporter := &recordingReporter{recorder: r}
	launcher := &recordingProbeLauncher{recorder: r, stage: "first-probe"}
	if source == "first-fork" {
		launcher.fail = errors.New("first fork failed")
	}
	runner := NewProbeRunner(launcher)
	req := launchRequest(route, surface, mode, install.env(), r.attempt)
	if route != RouteLaunch {
		req.SnapshotProvenance = filepath.Join(install.dir, pinnedNamePrefix+"1.4.3-R5000")
	}
	prep, err := PrepareNewestLaunch(context.Background(), r.cfg, runner, req)
	if strings.HasPrefix(source, "recorded-bad") {
		requireFreezeCode(t, err, CodeRecordedBad, StageDedupe)
	} else if source == "first-fork" {
		requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
	} else if err != nil {
		t.Fatalf("newest preparation: %v", err)
	}
	if prep == nil || prep.Newest().Build != "1.4.4-R5419.1" {
		t.Fatal("trace fixture did not reach newest selection")
	}
	r.prep = prep
	if strings.HasPrefix(source, "consumer-") {
		r.attempt = "l2c-trace-consumer-failure"
		file, err := os.Open(prep.Newest().Path)
		if err != nil {
			t.Fatal(err)
		}
		observed := reporter.newest(context.Background(), source, file, true)
		_ = file.Close()
		failure := NewestFailure{AttemptID: r.attempt, Newest: prep.Newest(), Stage: source, Code: "startup_failed", Attempted: true, Started: true, Cleanup: observed.Cleanup, Err: observed.Err}
		if err := prep.ReportNewestStartFailure(context.Background(), failure); err != nil {
			t.Fatalf("consumer failure publication: %v", err)
		}
	}
	if source != "newest-success" {
		if prep.Outcome() != OutcomeFailedNewest || !prep.FailureRecorded() {
			t.Fatal("fixture never reached recorded failure")
		}
		r.proof()
		r.add("failure-durable", prep.Newest(), nil, false)
	}
	return &launchTraceFixture{f: f, r: r, reporter: reporter}
}

func (f *launchTraceFixture) prefix(source string) []execRecord {
	n := f.r.prep.Newest()
	var want []execRecord
	if !strings.HasPrefix(source, "recorded-bad") {
		want = append(want, execRecord{stage: "first-probe", tuple: n, failed: source == "first-fork"})
	}
	if strings.HasPrefix(source, "consumer-") {
		want = append(want, execRecord{stage: source, tuple: n, failed: true})
	}
	if source != "newest-success" {
		want = append(want, execRecord{stage: "failure-durable", tuple: n})
	}
	return want
}

func (f *launchTraceFixture) recover(consent, failSession bool) {
	r, t := f.r, f.r.t
	t.Helper()
	want := append([]execRecord(nil), r.records...)
	f.reporter.consent = consent
	if failSession {
		f.reporter.fail = errors.New("fallback startup failed")
	}
	grant, err := r.prep.RequestPermit(context.Background(), f.reporter)
	g := receiptTuple(r.good)
	if r.prep.req.Mode == RecoveryInteractive {
		decision := "decline"
		if consent {
			decision = "accept"
		}
		want = append(want, execRecord{stage: "offer", tuple: g}, execRecord{stage: decision, tuple: g})
		if !consent {
			requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
			if grant.Outcome != PermitDeclined || grant.Permit.Issued() {
				t.Fatalf("decline authorized fallback: %+v", grant)
			}
			requireNoFallbackDocument(t, f.f.root, r.attempt)
			lease, err := grant.Permit.AuthorizeProbe(context.Background(), g)
			requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
			if lease != nil {
				t.Fatal("decline issued a probe lease")
			}
			_, err = grant.Permit.StartFallback(context.Background(), f.reporter)
			requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
			r.require(want...)
			return
		}
	}
	if err != nil || !grant.Permit.Issued() || grant.Warning != CodeFallbackWarning {
		t.Fatalf("permit issuance: %+v %v", grant, err)
	}
	r.permit = &grant.Permit
	r.warning()
	r.add("warning-durable", g, r.permit.state, false)
	want = append(want, execRecord{stage: "warning-durable", tuple: g, permit: r.permit.state})
	for _, stage := range []string{"fallback-version", "fallback-help"} {
		lease, err := r.permit.AuthorizeProbe(context.Background(), g)
		if err != nil || lease == nil {
			t.Fatalf("%s authorization: %v", stage, err)
		}
		file, err := os.Open(lease.Tuple().Path)
		if err != nil {
			t.Fatal(err)
		}
		launcher := &recordingProbeLauncher{recorder: r, stage: stage, lease: lease}
		process, err := launcher.Start(context.Background(), file, nil)
		_ = file.Close()
		if err != nil {
			t.Fatalf("%s attempted: %v", stage, err)
		}
		_ = process.Stdout().Close()
		_ = process.Stderr().Close()
		<-process.Done()
		lease.Release()
		want = append(want, execRecord{stage: stage, tuple: g, permit: r.permit.state})
	}
	result, err := r.permit.StartFallback(context.Background(), f.reporter)
	if failSession {
		if result.Event.State != "fallback_failed" || !errors.Is(err, f.reporter.fail) {
			t.Fatalf("fallback failure: %+v %v", result, err)
		}
		current, err := ReadLastGood(context.Background(), r.cfg)
		if err != nil || current != r.good {
			t.Fatalf("failed fallback changed last-good: %+v %v", current, err)
		}
	} else if err != nil || result.Event.State != "fallback_started" {
		t.Fatalf("fallback startup: %+v %v", result, err)
	}
	want = append(want, execRecord{stage: "fallback-start", tuple: g, permit: r.permit.state, failed: failSession})
	r.require(want...)
}

// TestLaunchTraceIncludesFailedProbeAndInteractiveConsent retains the entire
// failed-first-probe -> offer -> decision -> fallback stream. A failed session
// must remain in that stream, and decline must leave no fallback execution.
func TestLaunchTraceIncludesFailedProbeAndInteractiveConsent(t *testing.T) {
	for _, consent := range []bool{true, false} {
		name := "decline"
		if consent {
			name = "accept then failed session"
		}
		t.Run(name, func(t *testing.T) {
			f := prepareLaunchTrace(t, RouteResume, SurfacePTY, RecoveryInteractive, "first-fork")
			f.r.require(f.prefix("first-fork")...)
			f.recover(consent, true)
		})
	}
}

// TestDeferredNewestCannotOfferRecovery reaches a genuine host-cap deferral
// with a valid durable last-good. Neither eligibility nor evidence checking may
// offer recovery when the selected newest was never attempted.
func TestDeferredNewestCannotOfferRecovery(t *testing.T) {
	f := newFreeze(t)
	_, seeded := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-trace-defer")
	install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
	cfg := f.config()
	cfg.MaxFreezesPerDay = 1
	runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
	prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryInteractive, install.env(), "trace-defer"))
	if err != nil || prep == nil || prep.Outcome() != OutcomeRunDeferred || prep.FailureRecorded() {
		t.Fatalf("did not reach an unattempted host-cap deferral: %+v %v", prep, err)
	}
	authorization, ok := prep.Deferral()
	if !ok || authorization.Target() != receiptTuple(seeded) || authorization.Warning() != CodeDeferralHostCap {
		t.Fatal("deferral does not bind the already-frozen target")
	}
	reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP, consent: true}
	grant, err := prep.RequestPermit(context.Background(), reporter)
	// Check the actual forbidden side effect before the refusal code: an
	// unauthorized offer followed by a refusal is still a contract breach.
	if reporter.offers != 0 || reporter.starts != 0 || grant.Permit.Issued() {
		t.Fatalf("unattempted deferral reached recovery: offers=%d starts=%d grant=%+v", reporter.offers, reporter.starts, grant)
	}
	requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
	requireNoFallbackDocument(t, f.root, "trace-defer")
	if launcher.calls != 0 {
		t.Fatalf("unattempted deferral executed %d probes", launcher.calls)
	}
}

// recordingDeferredReporter is the route-(ii) surface boundary for the
// trace property. Offer and Start both record through the shared
// recorder, including failed legs; the offer binds the authorized
// target, newest, warning, attempt and mode before any decision.
type recordingDeferredReporter struct {
	recorder *execRecorder
	consent  bool
	fail     error
}

func (s *recordingDeferredReporter) Offer(_ context.Context, start DeferralStart) (bool, error) {
	r := s.recorder
	if start.Warning != CodeDeferralHostCap || start.Target != r.deferral || start.Newest != r.prep.Newest() ||
		start.Mode != RecoveryInteractive || start.AttemptID != r.attempt || start.Consented {
		r.t.Fatalf("deferral offer not bound to the capped newest/target: %+v", start)
	}
	requireNoFallbackDocument(r.t, r.cfg.FrozenRoot, r.attempt)
	r.add("deferral-offer", r.deferral, nil, false)
	r.decision = "decline"
	if s.consent {
		r.decision = "accept"
	}
	r.add("deferral-"+r.decision, r.deferral, nil, false)
	return s.consent, nil
}

func (s *recordingDeferredReporter) Start(ctx context.Context, start DeferralStart, file *os.File) StartOutcome {
	r := s.recorder
	r.observe(ctx, "deferred-start", file, nil, s.fail != nil)
	if start.Target != r.deferral || start.Warning != CodeDeferralHostCap || start.Newest != r.prep.Newest() ||
		start.Mode != r.prep.req.Mode || start.Consented != (r.prep.req.Mode == RecoveryInteractive) {
		r.t.Fatal("deferred session differs from the authorized target")
	}
	if s.fail != nil {
		return StartOutcome{Err: s.fail, Cleanup: CleanupConfirmed}
	}
	success := genuineSuccess(r.deferral, "trace-deferral-start")
	success.Surface = r.prep.req.Surface
	return StartOutcome{Success: success, Cleanup: CleanupConfirmed}
}

type deferralTraceFixture struct {
	f        *freezeFixture
	r        *execRecorder
	reporter *recordingDeferredReporter
	authz    *DeferralAuthorization
	launcher *fakeProbeLauncher
}

// prepareDeferralTrace reaches a genuine host-cap deferral with the
// recorder installed before preparation. The first probe never runs:
// admission deflects before any copy, and the authorized target is
// the seeded newest frozen build.
func prepareDeferralTrace(t *testing.T, mode RecoveryMode) *deferralTraceFixture {
	t.Helper()
	f := newFreeze(t)
	good, seeded := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-trace-defer")
	install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
	cfg := f.config()
	cfg.MaxFreezesPerDay = 1
	r := &execRecorder{t: t, cfg: cfg, good: seeded, attempt: "l2c-trace-defer", deferral: good}
	reporter := &recordingDeferredReporter{recorder: r}
	runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
	prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, mode, install.env(), r.attempt))
	if err != nil || prep == nil || prep.Outcome() != OutcomeRunDeferred || prep.FailureRecorded() {
		t.Fatalf("did not reach an unattempted host-cap deferral: %+v %v", prep, err)
	}
	if launcher.calls != 0 {
		t.Fatalf("unattempted deferral executed %d probes", launcher.calls)
	}
	authz, ok := prep.Deferral()
	if !ok || authz.Target() != good || authz.Warning() != CodeDeferralHostCap || authz.Mode() != mode {
		t.Fatal("deferral does not bind the already-frozen target")
	}
	if authz.Newest().Build != "1.4.4-R5419.1" {
		t.Fatalf("deferral newest = %+v, want the installed 1.4.4-R5419.1", authz.Newest())
	}
	r.prep = prep
	return &deferralTraceFixture{f: f, r: r, reporter: reporter, authz: authz, launcher: launcher}
}
