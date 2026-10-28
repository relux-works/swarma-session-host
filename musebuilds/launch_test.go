//go:build darwin || linux

package musebuilds

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/musebuilds/probeprocess"
)

// requireLaunchCode fails unless err chains to a launch-controller
// refusal with the exact code and stage. Refusal identity is the
// code, never message prose.
func requireLaunchCode(t *testing.T, err error, code, stage string) *LaunchError {
	t.Helper()
	if err == nil {
		t.Fatalf("want refusal %s, got success", code)
	}
	refusal, ok := AsLaunchError(err)
	if !ok {
		t.Fatalf("want refusal %s, got untyped %v", code, err)
	}
	if refusal.Code != code {
		t.Fatalf("want refusal %s, got %s (%v)", code, refusal.Code, err)
	}
	if refusal.Stage != stage {
		t.Fatalf("want stage %s for %s, got %s (%v)", stage, code, refusal.Stage, err)
	}
	return refusal
}

// launchInstall builds one install serving the given builds with the
// given sidecar text.
func launchInstall(t *testing.T, sidecar string, builds ...string) *installFixture {
	t.Helper()
	install := newInstall(t)
	install.writeLauncher(plainScript)
	for _, build := range builds {
		install.writeCandidate(build)
	}
	install.writeSidecar(sidecar)
	return install
}

// launchRequest bodies one invocation input for the given route,
// surface, mode, environment and attempt token.
func launchRequest(route LaunchRoute, surface string, mode RecoveryMode, env []string, attempt string) LaunchRequest {
	return LaunchRequest{Route: route, Surface: surface, Mode: mode, Env: env, AttemptID: attempt}
}

// okProbeRunner returns a runner whose launcher answers a clean
// matching --version for the named build and release.
func okProbeRunner(build, release string) (*ProbeRunner, *fakeProbeLauncher) {
	l := &fakeProbeLauncher{}
	answer := "Muse Code " + release + " (" + build + ")\n"
	l.start = func(context.Context, *os.File, []string) (probeprocess.Process, error) {
		return finishedProbe(answer), nil
	}
	return NewProbeRunner(l), l
}

// execFailedRunner returns a runner whose launcher fails to start,
// for attempted-but-unstarted first probes.
func execFailedRunner() (*ProbeRunner, *fakeProbeLauncher) {
	l := &fakeProbeLauncher{}
	l.start = func(context.Context, *os.File, []string) (probeprocess.Process, error) {
		return nil, errors.New("fork failed")
	}
	return NewProbeRunner(l), l
}

// exitRunner returns a runner whose launcher reaps the given exit
// with the given stdout. An empty signal means a plain exit code.
func exitRunner(code int, signal, output string) (*ProbeRunner, *fakeProbeLauncher) {
	l := &fakeProbeLauncher{}
	l.start = func(context.Context, *os.File, []string) (probeprocess.Process, error) {
		p := finishedProbe(output)
		<-p.done
		p.done <- probeprocess.Exit{Code: code, Signal: signal}
		return p, nil
	}
	return NewProbeRunner(l), l
}

// errReadCloser is a pipe that fails every read, for drain-failure
// first probes.
type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) { return 0, errors.New("drain failed") }
func (errReadCloser) Close() error             { return nil }

// drainFailedRunner returns a runner whose stdout pipe fails while
// the child itself reaps cleanly.
func drainFailedRunner() (*ProbeRunner, *fakeProbeLauncher) {
	l := &fakeProbeLauncher{}
	l.start = func(context.Context, *os.File, []string) (probeprocess.Process, error) {
		p := finishedProbe("")
		p.out = errReadCloser{}
		return p, nil
	}
	return NewProbeRunner(l), l
}

// markBADForLaunch records a deterministic mismatch verdict for one
// frozen build, for recorded-newest-failure launches.
func markBADForLaunch(t *testing.T, cfg HostConfig, frozen FrozenIdentity, selectionID string) {
	t.Helper()
	obs := ProbeObservation{
		AttemptID: "attempt-launch-bad-1", SelectionID: selectionID,
		Outcome: ProbeExitOKMismatch, ObservedBuild: "1.4.2-R4000",
		Detail: "clean exit attesting another build", Cleanup: CleanupConfirmed,
		OutputDigest: strings.Repeat("23", 32), OutputBytes: 48,
	}
	record, err := RecordProbeOutcome(context.Background(), cfg, frozen, obs)
	if err != nil {
		t.Fatal(err)
	}
	if record.State.State != StateBAD {
		t.Fatalf("state = %s, want %s", record.State.State, StateBAD)
	}
}

// seedReceiptForLaunch admits one genuine last-good receipt for the
// named build and returns it with its frozen tuple.
func seedReceiptForLaunch(t *testing.T, f *freezeFixture, build, attempt string) (FrozenIdentity, LastGood) {
	t.Helper()
	frozen := probeOKBuild(t, f, build)
	receipt, err := WriteLastGood(context.Background(), f.config(), genuineSuccess(frozen, attempt))
	if err != nil {
		t.Fatal(err)
	}
	return frozen, receipt
}

// requireNoFallbackDocument fails when the invocation minted a
// fallback permit document. Deferrals and refusals mint none.
func requireNoFallbackDocument(t *testing.T, root, attempt string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(root, "fallback-"+attempt+".json")); !os.IsNotExist(err) {
		t.Fatalf("fallback document exists: %v", err)
	}
}

// requireTracePermitted fails unless every traced exec is either the
// prepared newest tuple or the permitted last-good tuple. Deferred
// legs name the deferral target without a permit.
func requireTracePermitted(t *testing.T, trace []LaunchTraceEntry, newest, lastGood, deferred FrozenIdentity) {
	t.Helper()
	for i, entry := range trace {
		switch entry.Kind {
		case LaunchExecNewestStart:
			if entry.Permitted || entry.Tuple != newest {
				t.Fatalf("trace[%d] = %+v, want the unpermitted newest %+v", i, entry, newest)
			}
		case LaunchExecFallbackProbe, LaunchExecFallbackStart:
			if !entry.Permitted || entry.Tuple != lastGood {
				t.Fatalf("trace[%d] = %+v, want the permitted %+v", i, entry, lastGood)
			}
		case LaunchExecDeferredStart:
			if entry.Permitted || entry.Tuple != deferred {
				t.Fatalf("trace[%d] = %+v, want the deferred %+v", i, entry, deferred)
			}
		default:
			t.Fatalf("trace[%d] names unknown leg %+v", i, entry)
		}
		if entry.SelectionID == "" {
			t.Fatalf("trace[%d] carries no selection", i)
		}
	}
}

// adoptionReporter is a fallback reporter that tolerates an adopted
// same-tuple newer receipt: the durable issue-time document keeps
// the old sequence while the leg starts the revalidated one.
type adoptionReporter struct {
	t            *testing.T
	starts       int
	seen         RecoveryEvent
	startedTuple FrozenIdentity
}

func (s *adoptionReporter) Offer(context.Context, RecoveryEvent) (bool, error) { return true, nil }

func (s *adoptionReporter) Start(_ context.Context, e RecoveryEvent, _ *os.File) StartOutcome {
	s.t.Helper()
	s.starts++
	s.seen = e
	s.startedTuple = FrozenIdentity{Build: e.LastGood.Frozen.Build, SHA256: e.LastGood.Frozen.SHA256, Path: e.LastGood.Frozen.Path}
	success := genuineSuccess(s.startedTuple, "adoption-start")
	return StartOutcome{Success: success, Cleanup: CleanupConfirmed}
}

// deferredReporter is a scripted deferred-leg reporter: it checks
// the warning, the mode/consent binding and the held bytes, then
// succeeds or fails on demand. Offer grants only when consent is
// set; unattended legs must never call it.
type deferredReporter struct {
	t            *testing.T
	cfg          HostConfig
	starts       int
	seen         DeferralStart
	fail         error
	startedTuple FrozenIdentity
	offers       int
	consent      bool
	offerSeen    DeferralStart
}

func (s *deferredReporter) Offer(_ context.Context, start DeferralStart) (bool, error) {
	s.t.Helper()
	s.offers++
	s.offerSeen = start
	if start.Warning != CodeDeferralHostCap {
		s.t.Fatalf("deferred offer warns %q, want %q", start.Warning, CodeDeferralHostCap)
	}
	if start.Mode != RecoveryInteractive {
		s.t.Fatalf("deferred offer mode = %q, want interactive", start.Mode)
	}
	if start.Consented {
		s.t.Fatal("deferred offer arrives pre-consented")
	}
	return s.consent, nil
}

func (s *deferredReporter) Start(ctx context.Context, start DeferralStart, file *os.File) StartOutcome {
	s.t.Helper()
	s.starts++
	s.seen = start
	s.startedTuple = start.Target
	if start.Warning != CodeDeferralHostCap {
		s.t.Fatalf("deferred leg warns %q, want %q", start.Warning, CodeDeferralHostCap)
	}
	if start.Mode != RecoveryInteractive && start.Mode != RecoveryUnattended {
		s.t.Fatalf("deferred leg mode = %q", start.Mode)
	}
	if start.Consented != (start.Mode == RecoveryInteractive) {
		s.t.Fatalf("deferred leg consent = %v for mode %q", start.Consented, start.Mode)
	}
	digest, err := hashFileDescriptor(ctx, StageRecovery, file)
	if err != nil || digest != start.Target.SHA256 {
		s.t.Fatalf("deferred reporter received wrong held bytes: %s %v", digest, err)
	}
	if s.fail != nil {
		return StartOutcome{Err: s.fail, Cleanup: CleanupConfirmed}
	}
	success := genuineSuccess(start.Target, "deferred-start")
	return StartOutcome{Success: success, Cleanup: CleanupConfirmed}
}

// TestNewestDeferredDoesNotPretendStartFailure proves the host-cap
// deferral runs the already-frozen newest with its typed warning and
// mints no failure: no permit issues, no recovery offer runs, no
// fallback document appears, and the first probe never starts. The
// deferred leg itself starts verified PROBE_OK bytes and admits its
// receipt, while an unprobed target refuses and a failed leg still
// arms no permit. Interactive legs offer the deferral target first:
// an accept starts with consent, a decline runs nothing, an
// unrunnable target is never offered, and a target superseded by a
// newer freeze refuses.
func TestNewestDeferredDoesNotPretendStartFailure(t *testing.T) {
	prepareDeflected := func(t *testing.T, f *freezeFixture, mode RecoveryMode) (*PreparedLaunch, *fakeProbeLauncher, HostConfig, FrozenIdentity, LastGood) {
		t.Helper()
		good, seeded := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-defer")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		cfg := f.config()
		cfg.MaxFreezesPerDay = 1
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, mode, install.env(), "l2c-defer-1"))
		if err != nil {
			t.Fatalf("deferral prepares: %v", err)
		}
		if prep.Outcome() != OutcomeRunDeferred {
			t.Fatalf("outcome = %s, want %s", prep.Outcome(), OutcomeRunDeferred)
		}
		if prep.FailureRecorded() {
			t.Fatal("deferral armed a failure")
		}
		return prep, launcher, cfg, good, seeded
	}

	t.Run("no permit follows a deferral", func(t *testing.T) {
		f := newFreeze(t)
		prep, launcher, cfg, good, _ := prepareDeflected(t, f, RecoveryUnattended)
		authz, ok := prep.Deferral()
		if !ok || !authz.Valid() {
			t.Fatal("deferral carries no authorization")
		}
		if authz.Target() != good || authz.Warning() != CodeDeferralHostCap {
			t.Fatalf("deferral = %+v %q, want %+v with the host-cap warning", authz.Target(), authz.Warning(), good)
		}
		if authz.Newest().Build != "1.4.4-R5419.1" {
			t.Fatalf("deferral newest = %+v, want the installed 1.4.4-R5419.1", authz.Newest())
		}
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP, consent: true}
		_, err := prep.RequestPermit(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
		if reporter.offers != 0 {
			t.Fatalf("offers = %d, want none without a failure", reporter.offers)
		}
		requireNoFallbackDocument(t, f.root, "l2c-defer-1")
		if launcher.calls != 0 {
			t.Fatalf("probe calls = %d, want none before any start", launcher.calls)
		}
	})

	t.Run("deferred verified bytes start and admit", func(t *testing.T) {
		f := newFreeze(t)
		prep, _, cfg, good, seeded := prepareDeflected(t, f, RecoveryUnattended)
		authz, ok := prep.Deferral()
		if !ok {
			t.Fatal("deferral carries no authorization")
		}
		reporter := &deferredReporter{t: t, cfg: cfg}
		receipt, err := authz.Start(context.Background(), reporter)
		if err != nil {
			t.Fatalf("deferred start: %v", err)
		}
		if receipt.Frozen.Build != good.Build || receipt.Frozen.SHA256 != good.SHA256 || receipt.Sequence != seeded.Sequence+1 {
			t.Fatalf("receipt = %+v, want the deferred tuple at sequence %d", receipt, seeded.Sequence+1)
		}
		if reporter.starts != 1 || reporter.startedTuple != good {
			t.Fatalf("starts = %d tuple = %+v, want one start of %+v", reporter.starts, reporter.startedTuple, good)
		}
		if reporter.offers != 0 {
			t.Fatalf("unattended deferral offered %d times, want the warning only", reporter.offers)
		}
		requireTracePermitted(t, prep.Trace(), prep.Newest(), FrozenIdentity{}, good)
		if len(prep.Trace()) != 1 || prep.Trace()[0].Kind != LaunchExecDeferredStart {
			t.Fatalf("trace = %+v, want the single deferred leg", prep.Trace())
		}
	})

	t.Run("interactive accept starts with consent", func(t *testing.T) {
		f := newFreeze(t)
		prep, _, cfg, good, seeded := prepareDeflected(t, f, RecoveryInteractive)
		authz, ok := prep.Deferral()
		if !ok || authz.Mode() != RecoveryInteractive {
			t.Fatal("deferral carries no interactive authorization")
		}
		reporter := &deferredReporter{t: t, cfg: cfg, consent: true}
		receipt, err := authz.Start(context.Background(), reporter)
		if err != nil {
			t.Fatalf("accepted deferral starts: %v", err)
		}
		if reporter.offers != 1 || reporter.starts != 1 {
			t.Fatalf("offers = %d starts = %d, want one offer and one start", reporter.offers, reporter.starts)
		}
		offer := reporter.offerSeen
		if offer.Target != good || offer.Newest.Build != "1.4.4-R5419.1" || offer.Warning != CodeDeferralHostCap ||
			offer.AttemptID != "l2c-defer-1" || offer.Mode != RecoveryInteractive {
			t.Fatalf("offer = %+v, want the bound target/newest/warning/attempt/mode", offer)
		}
		if !reporter.seen.Consented {
			t.Fatal("accepted leg started unconsented")
		}
		if receipt.Frozen.Build != good.Build || receipt.Frozen.SHA256 != good.SHA256 || receipt.Sequence != seeded.Sequence+1 {
			t.Fatalf("receipt = %+v, want the deferred tuple at sequence %d", receipt, seeded.Sequence+1)
		}
		requireTracePermitted(t, prep.Trace(), prep.Newest(), FrozenIdentity{}, good)
	})

	t.Run("interactive decline runs nothing", func(t *testing.T) {
		f := newFreeze(t)
		prep, launcher, cfg, _, seeded := prepareDeflected(t, f, RecoveryInteractive)
		authz, ok := prep.Deferral()
		if !ok {
			t.Fatal("deferral carries no authorization")
		}
		reporter := &deferredReporter{t: t, cfg: cfg, consent: false}
		_, err := authz.Start(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchDeferralDeclined, StageLaunch)
		if reporter.offers != 1 || reporter.starts != 0 {
			t.Fatalf("offers = %d starts = %d, want one declined offer and no start", reporter.offers, reporter.starts)
		}
		if len(prep.Trace()) != 0 {
			t.Fatalf("trace = %+v, want no execution", prep.Trace())
		}
		if launcher.calls != 0 {
			t.Fatal("declined deferral probed")
		}
		current, err := ReadLastGood(context.Background(), cfg)
		if err != nil || current != seeded {
			t.Fatalf("declined deferral kept %+v, want %+v", current, seeded)
		}
		requireNoFallbackDocument(t, f.root, "l2c-defer-1")
	})

	t.Run("unprobed target refuses before starting", func(t *testing.T) {
		f := newFreeze(t)
		published := f.publishFor("1.4.3-R5000")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		cfg := f.config()
		cfg.MaxFreezesPerDay = 1
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-defer-copied"))
		if err != nil {
			t.Fatalf("deferral prepares: %v", err)
		}
		authz, ok := prep.Deferral()
		if !ok || authz.Target() != published.Frozen {
			t.Fatalf("deferral target = %+v, want %+v", authz.Target(), published.Frozen)
		}
		reporter := &deferredReporter{t: t, cfg: cfg}
		_, err = authz.Start(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchDeferredUnprobed, StageLaunch)
		if reporter.starts != 0 {
			t.Fatal("unprobed target started")
		}
		if reporter.offers != 0 {
			t.Fatal("unprobed target was offered")
		}
	})

	t.Run("interactive unprobed target refuses without an offer", func(t *testing.T) {
		f := newFreeze(t)
		f.publishFor("1.4.3-R5000")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		cfg := f.config()
		cfg.MaxFreezesPerDay = 1
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryInteractive, install.env(), "l2c-defer-copied-offer"))
		if err != nil {
			t.Fatalf("deferral prepares: %v", err)
		}
		authz, ok := prep.Deferral()
		if !ok {
			t.Fatal("deferral carries no authorization")
		}
		reporter := &deferredReporter{t: t, cfg: cfg, consent: true}
		_, err = authz.Start(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchDeferredUnprobed, StageLaunch)
		if reporter.offers != 0 || reporter.starts != 0 {
			t.Fatalf("offers = %d starts = %d, want an unoffered refusal", reporter.offers, reporter.starts)
		}
	})

	t.Run("superseding freeze refuses the stale deferral", func(t *testing.T) {
		f := newFreeze(t)
		prep, _, cfg, good, _ := prepareDeflected(t, f, RecoveryUnattended)
		authz, ok := prep.Deferral()
		if !ok || authz.Target() != good {
			t.Fatal("deferral carries no authorization for the older target")
		}
		newer := probeOKBuild(t, f, "1.4.5-R5500")
		if newer.Build != "1.4.5-R5500" {
			t.Fatalf("superseding build = %+v", newer)
		}
		reporter := &deferredReporter{t: t, cfg: cfg}
		_, err := authz.Start(context.Background(), reporter)
		refusal := requireLaunchCode(t, err, CodeLaunchTupleMismatch, StageLaunch)
		if refusal.Cause == nil || !strings.Contains(refusal.Cause.Error(), "superseded") {
			t.Fatalf("refusal cause = %v, want the supersede stage", refusal.Cause)
		}
		if reporter.starts != 0 {
			t.Fatal("superseded deferral started")
		}
		if len(prep.Trace()) != 0 {
			t.Fatalf("trace = %+v, want no execution", prep.Trace())
		}
	})

	t.Run("failed deferred leg arms no permit", func(t *testing.T) {
		f := newFreeze(t)
		prep, _, cfg, _, _ := prepareDeflected(t, f, RecoveryUnattended)
		authz, ok := prep.Deferral()
		if !ok {
			t.Fatal("deferral carries no authorization")
		}
		legErr := errors.New("deferred child failed")
		reporter := &deferredReporter{t: t, cfg: cfg, fail: legErr}
		_, err := authz.Start(context.Background(), reporter)
		if !errors.Is(err, legErr) {
			t.Fatalf("deferred failure = %v, want the leg error", err)
		}
		if reporter.offers != 0 {
			t.Fatal("unattended deferred failure was offered")
		}
		if prep.FailureRecorded() {
			t.Fatal("deferred failure armed a newest failure")
		}
		permitReporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP, consent: true}
		_, err = prep.RequestPermit(context.Background(), permitReporter)
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
		requireNoFallbackDocument(t, f.root, "l2c-defer-1")
	})
}

// TestHostCapUnsetNeverDefers proves route (ii) is unreachable under
// the product-default unset cap: the newest freezes and runs
// normally, no deferral authorization issues, and the older frozen
// build never runs. It is the named killer for the narrowing that
// drops the configured-cap check.
func TestHostCapUnsetNeverDefers(t *testing.T) {
	f := newFreeze(t)
	good, seeded := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-unset-cap")
	install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
	cfg := f.config()
	if cfg.MaxFreezesPerDay != 0 {
		t.Fatalf("fixture cap = %d, want the unset product default", cfg.MaxFreezesPerDay)
	}
	runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
	prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-unset-cap"))
	if err != nil {
		t.Fatalf("unset-cap newest prepares: %v", err)
	}
	if prep.Outcome() != OutcomeRunNewest {
		t.Fatalf("outcome = %s, want %s", prep.Outcome(), OutcomeRunNewest)
	}
	if prep.FailureRecorded() {
		t.Fatal("unset-cap preparation armed a failure")
	}
	if _, ok := prep.Deferral(); ok {
		t.Fatal("unset cap issued a deferral authorization")
	}
	if prep.Newest().Build != "1.4.4-R5419.1" {
		t.Fatalf("selected %+v, want the installed newest", prep.Newest())
	}
	if launcher.calls != 1 {
		t.Fatalf("probe calls = %d, want the single newest first probe", launcher.calls)
	}
	receipt, err := prep.ReportNewestStartSuccess(context.Background(), genuineSuccess(prep.Newest(), "l2c-unset-cap-start"))
	if err != nil {
		t.Fatalf("newest success admits: %v", err)
	}
	if receipt.Frozen.Build != prep.Newest().Build || receipt.Frozen.SHA256 != prep.Newest().SHA256 || receipt.Sequence != seeded.Sequence+1 {
		t.Fatalf("receipt = %+v, want the newest tuple at sequence %d", receipt, seeded.Sequence+1)
	}
	if receipt.Frozen.Build == good.Build && receipt.Frozen.SHA256 == good.SHA256 {
		t.Fatal("unset cap admitted the older build")
	}
	trace := prep.Trace()
	if len(trace) != 1 || trace[0].Kind != LaunchExecNewestStart || trace[0].Tuple != prep.Newest() {
		t.Fatalf("trace = %+v, want the single newest leg", trace)
	}
	requireNoFallbackDocument(t, f.root, "l2c-unset-cap")
}

// TestDeferralNeverSelectsUnfrozen proves route (ii) runs only frozen
// bytes: with no frozen build at all the tripped cap refuses typed
// and runs nothing, and with a frozen older build the target is the
// frozen-root-contained newest frozen tuple, never the installed
// unfrozen newest.
func TestDeferralNeverSelectsUnfrozen(t *testing.T) {
	t.Run("empty store refuses without running", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		cfg := f.config()
		cfg.MaxFreezesPerDay = 1
		f.plantReservation("2026-10-10", "1.4.0-R1000", strings.Repeat("ab", 32), outcomePublished)
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-unfrozen-empty"))
		if prep != nil {
			t.Fatalf("empty-store deferral prepared %+v", prep.Outcome())
		}
		requireFreezeCode(t, err, CodeFreezeUnavailable, StageAdmit)
		if launcher.calls != 0 {
			t.Fatal("empty-store deferral probed")
		}
	})

	t.Run("target is the frozen newest, never the installed newest", func(t *testing.T) {
		f := newFreeze(t)
		good, _ := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-unfrozen")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		cfg := f.config()
		cfg.MaxFreezesPerDay = 1
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-unfrozen-target"))
		if err != nil || prep.Outcome() != OutcomeRunDeferred {
			t.Fatalf("deferral prepares: %+v %v", prep, err)
		}
		authz, ok := prep.Deferral()
		if !ok {
			t.Fatal("deferral carries no authorization")
		}
		target := authz.Target()
		if target != good {
			t.Fatalf("target = %+v, want the frozen %+v", target, good)
		}
		if !strings.HasPrefix(target.Path, cfg.FrozenRoot+"/") {
			t.Fatalf("target path %q is not frozen-root-contained", target.Path)
		}
		if target.Build == "1.4.4-R5419.1" {
			t.Fatal("deferral selected the unfrozen installed newest")
		}
		reporter := &deferredReporter{t: t, cfg: cfg}
		receipt, err := authz.Start(context.Background(), reporter)
		if err != nil {
			t.Fatalf("deferred start: %v", err)
		}
		if receipt.Frozen.Build != good.Build || receipt.Frozen.SHA256 != good.SHA256 || receipt.Frozen.Path != good.Path {
			t.Fatalf("receipt tuple = %+v, want the frozen %+v", receipt.Frozen, good)
		}
		if launcher.calls != 0 {
			t.Fatalf("deferral probed %d times", launcher.calls)
		}
	})
}

// TestFreezeUnavailableLaunchesNothing proves every freeze-subsystem
// refusal launches nothing: the lock, the ramp, the ledger and the
// source gate each refuse typed with zero probe calls and no
// prepared launch that could issue a permit.
func TestFreezeUnavailableLaunchesNothing(t *testing.T) {
	setup := func(t *testing.T) (*freezeFixture, HostConfig, []string) {
		t.Helper()
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		return f, f.config(), install.env()
	}
	prepare := func(t *testing.T, cfg HostConfig, env []string) (*fakeProbeLauncher, error) {
		t.Helper()
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, env, "l2c-unavail"))
		if prep != nil {
			t.Fatalf("refusal prepared %+v", prep.Outcome())
		}
		return launcher, err
	}

	t.Run("global lock failure", func(t *testing.T) {
		_, cfg, env := setup(t)
		cfg.Locker = &fakeLock{acquireErr: errors.New("lock down")}
		launcher, err := prepare(t, cfg, env)
		requireFreezeCode(t, err, CodeFreezeLockFailed, StageAdmit)
		if launcher.calls != 0 {
			t.Fatal("locked-out freeze probed")
		}
	})

	t.Run("ramp hold", func(t *testing.T) {
		f, cfg, env := setup(t)
		f.writeRamp("ramp_down=1\n")
		launcher, err := prepare(t, cfg, env)
		requireFreezeCode(t, err, CodeRampHold, StageAdmit)
		if launcher.calls != 0 {
			t.Fatal("held freeze probed")
		}
	})

	t.Run("unprovisioned ledger", func(t *testing.T) {
		f, cfg, env := setup(t)
		cfg.LedgerDir = filepath.Join(f.root, "no-ledger")
		launcher, err := prepare(t, cfg, env)
		requireFreezeCode(t, err, CodeFreezeUnavailable, StageAdmit)
		if launcher.calls != 0 {
			t.Fatal("ledgerless freeze probed")
		}
	})

	t.Run("quarantined source", func(t *testing.T) {
		_, cfg, env := setup(t)
		cfg.Quarantine = func(string) (bool, error) { return true, nil }
		launcher, err := prepare(t, cfg, env)
		requireFreezeCode(t, err, CodeSourceQuarantined, StageCopy)
		if launcher.calls != 0 {
			t.Fatal("quarantined freeze probed")
		}
	})
}

// TestFallbackRequiresActualLastGoodReceipt proves a PROBE_OK copy is
// never a fallback target by itself: the permit needs the validated
// durable receipt, refuses absent, corrupt and same-tuple receipts
// before any offer, and revalidates state at the leg.
func TestFallbackRequiresActualLastGoodReceipt(t *testing.T) {
	prepareFailed := func(t *testing.T, f *freezeFixture, mode RecoveryMode, attempt string) (*PreparedLaunch, HostConfig) {
		t.Helper()
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, launcher := execFailedRunner()
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, mode, install.env(), attempt))
		requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
		if prep == nil || prep.Outcome() != OutcomeFailedNewest || !prep.FailureRecorded() {
			t.Fatal("exec failure never reached the armed stage")
		}
		if launcher.calls != 1 {
			t.Fatalf("probe calls = %d, want the single failed attempt", launcher.calls)
		}
		return prep, f.config()
	}

	t.Run("absent receipt refuses before any offer", func(t *testing.T) {
		f := newFreeze(t)
		prep, cfg := prepareFailed(t, f, RecoveryInteractive, "l2c-m04-absent")
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP, consent: true}
		_, err := prep.RequestPermit(context.Background(), reporter)
		if reporter.offers != 0 {
			t.Fatal("absent receipt was offered")
		}
		requireLastGoodCode(t, err, CodeLastGoodAbsent, StageLastGood)
	})

	t.Run("corrupt receipt refuses before any offer", func(t *testing.T) {
		f := newFreeze(t)
		f.plantLastGood(t, []byte("{corrupt"))
		prep, cfg := prepareFailed(t, f, RecoveryInteractive, "l2c-m04-corrupt")
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP, consent: true}
		_, err := prep.RequestPermit(context.Background(), reporter)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
		if reporter.offers != 0 {
			t.Fatal("corrupt receipt was offered")
		}
	})

	t.Run("same-tuple receipt refuses without a second attempt", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m04-same"))
		if err != nil || prep.Outcome() != OutcomeRunNewest {
			t.Fatalf("newest prepares: %+v %v", prep, err)
		}
		if _, err := prep.ReportNewestStartSuccess(context.Background(), genuineSuccess(prep.Newest(), "l2c-m04-same-start")); err != nil {
			t.Fatalf("newest success admits: %v", err)
		}
		install2 := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner2, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep2, err := PrepareNewestLaunch(context.Background(), f.config(), runner2, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install2.env(), "l2c-m04-same-2"))
		if err != nil || prep2.Outcome() != OutcomeRunNewest {
			t.Fatalf("second newest prepares: %+v %v", prep2, err)
		}
		failure := NewestFailure{
			AttemptID: "l2c-m04-same-fail", Newest: prep2.Newest(),
			Stage: "msp-handshake", Code: "handshake_refused",
			Attempted: true, Started: true, Cleanup: CleanupConfirmed,
		}
		if err := prep2.ReportNewestStartFailure(context.Background(), failure); err != nil {
			t.Fatalf("consumer failure arms: %v", err)
		}
		reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP}
		_, err = prep2.RequestPermit(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
	})

	t.Run("state lost before the leg refuses at the leg", func(t *testing.T) {
		f := newFreeze(t)
		good, _ := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m04")
		prep, cfg := prepareFailed(t, f, RecoveryUnattended, "l2c-m04-lost")
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP}
		grant, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil || !grant.Permit.Issued() {
			t.Fatalf("permit issues: %+v %v", grant, err)
		}
		key := freezeKey(good.Build, good.SHA256)
		state, err := ReadBuildState(context.Background(), cfg, good)
		if err != nil {
			t.Fatal(err)
		}
		state.State = StateCopied
		f.plantState(key, state)
		_, err = grant.Permit.StartFallback(context.Background(), reporter)
		requireProbeCode(t, err, CodeRecoveryUnavailable, StageRecovery)
		if reporter.starts != 0 {
			t.Fatal("unverified leg started")
		}
	})

	t.Run("valid receipt issues", func(t *testing.T) {
		f := newFreeze(t)
		_, seeded := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m04-ok")
		prep, cfg := prepareFailed(t, f, RecoveryUnattended, "l2c-m04-ok")
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP}
		grant, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil {
			t.Fatalf("permit issues: %v", err)
		}
		if grant.Outcome != PermitAutoGranted || grant.Warning != CodeFallbackWarning || !grant.Permit.Issued() {
			t.Fatalf("grant = %+v, want the automatic permitted grant", grant)
		}
		if grant.Permit.LastGood() != seeded {
			t.Fatalf("permit receipt = %+v, want %+v", grant.Permit.LastGood(), seeded)
		}
	})
}

// TestExplicitLKGRequiresMatchingReceipt proves the explicit
// last-good assertion is checked against the durable receipt: a
// mismatch or a missing receipt refuses invalid_config before any
// offer, with the underlying absence chained, while a matching
// assertion and no assertion both proceed.
func TestExplicitLKGRequiresMatchingReceipt(t *testing.T) {
	const foreignPath = "/var/empty/muse-lkg/muse-bin-1.4.2-R4000"
	prepareFailed := func(t *testing.T, f *freezeFixture, explicit string) (*PreparedLaunch, HostConfig) {
		t.Helper()
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := execFailedRunner()
		req := launchRequest(RouteLaunch, SurfaceMSP, RecoveryInteractive, install.env(), "l2c-m05")
		req.ExplicitLastGood = explicit
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, req)
		requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
		if prep == nil || !prep.FailureRecorded() {
			t.Fatal("exec failure never reached the armed stage")
		}
		return prep, f.config()
	}

	t.Run("matching assertion proceeds", func(t *testing.T) {
		f := newFreeze(t)
		good, _ := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m05")
		prep, cfg := prepareFailed(t, f, good.Path)
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP, consent: true}
		grant, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil || grant.Outcome != PermitOfferGranted {
			t.Fatalf("matching assertion grants: %+v %v", grant, err)
		}
	})

	t.Run("mismatching assertion refuses before any offer", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m05")
		prep, cfg := prepareFailed(t, f, foreignPath)
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP, consent: true}
		_, err := prep.RequestPermit(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchInvalidConfig, StageLaunch)
		if reporter.offers != 0 {
			t.Fatal("mismatching assertion was offered")
		}
	})

	t.Run("assertion without a receipt refuses as invalid config", func(t *testing.T) {
		f := newFreeze(t)
		prep, cfg := prepareFailed(t, f, foreignPath)
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP, consent: true}
		_, err := prep.RequestPermit(context.Background(), reporter)
		refusal := requireLaunchCode(t, err, CodeLaunchInvalidConfig, StageLaunch)
		absent, ok := AsLastGoodError(refusal.Cause)
		if !ok || absent.Code != CodeLastGoodAbsent {
			t.Fatalf("invalid-config cause = %v, want the chained last-good absence", refusal.Cause)
		}
		if reporter.offers != 0 {
			t.Fatal("unrecorded assertion was offered")
		}
		requireNoFallbackDocument(t, f.root, "l2c-m05")
	})

	t.Run("no assertion proceeds", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m05")
		prep, cfg := prepareFailed(t, f, "")
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP, consent: true}
		grant, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil || !grant.Permit.Issued() {
			t.Fatalf("unasserted request grants: %+v %v", grant, err)
		}
	})
}

// TestRestoreRediscoversNewestBeforeOldPlan proves restore and resume
// routes reselect the current newest: the snapshot or plan reference
// is recorded provenance and never selection authority, on every
// route including plain launch.
func TestRestoreRediscoversNewestBeforeOldPlan(t *testing.T) {
	routes := []struct {
		route LaunchRoute
		name  string
	}{
		{RouteLaunch, "launch ignores provenance"},
		{RouteRestore, "restore reselects newest"},
		{RouteResume, "resume reselects newest"},
	}
	for _, cell := range routes {
		t.Run(cell.name, func(t *testing.T) {
			f := newFreeze(t)
			install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.3-R5000", "1.4.4-R5419.1")
			runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
			req := launchRequest(cell.route, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m06")
			req.SnapshotProvenance = filepath.Join(install.dir, pinnedNamePrefix+"1.4.3-R5000")
			prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, req)
			if err != nil {
				t.Fatalf("restore prepares: %v", err)
			}
			if prep.Newest().Build != "1.4.4-R5419.1" {
				t.Fatalf("selected %+v, want the installed newest", prep.Newest())
			}
			if prep.SnapshotBuild() != "1.4.3-R5000" {
				t.Fatalf("provenance = %q, want the recorded older snapshot", prep.SnapshotBuild())
			}
			if prep.Outcome() != OutcomeRunNewest {
				t.Fatalf("outcome = %s, want %s", prep.Outcome(), OutcomeRunNewest)
			}
		})
	}
}

// TestFallbackSwapsPathBuildAndDigestTogether proves the fallback
// leg applies the receipt as one full tuple: the permit, the probe
// authorization, the started bytes and the admitted receipt all name
// the exact path, build and digest together.
func TestFallbackSwapsPathBuildAndDigestTogether(t *testing.T) {
	f := newFreeze(t)
	good, seeded := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m32")
	install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
	runner, _ := execFailedRunner()
	prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m32"))
	requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
	reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP}
	grant, err := prep.RequestPermit(context.Background(), reporter)
	if err != nil || !grant.Permit.Issued() {
		t.Fatalf("permit issues: %+v %v", grant, err)
	}
	if grant.Permit.LastGood() != seeded {
		t.Fatalf("permit receipt = %+v, want %+v", grant.Permit.LastGood(), seeded)
	}
	lease, err := grant.Permit.AuthorizeProbe(context.Background(), good)
	if err != nil {
		t.Fatalf("exact-tuple probe authorizes: %v", err)
	}
	if lease == nil || lease.Tuple() != good {
		t.Fatalf("lease admits %+v, want %+v", lease, good)
	}
	lease.Release()
	result, err := grant.Permit.StartFallback(context.Background(), reporter)
	if err != nil {
		t.Fatalf("fallback starts: %v", err)
	}
	if result.Receipt.Frozen.Build != good.Build || result.Receipt.Frozen.SHA256 != good.SHA256 || result.Receipt.Frozen.Path != good.Path {
		t.Fatalf("receipt tuple = %+v, want %+v", result.Receipt.Frozen, good)
	}
	if result.Receipt.Sequence != seeded.Sequence+1 {
		t.Fatalf("receipt sequence = %d, want %d", result.Receipt.Sequence, seeded.Sequence+1)
	}
	if result.Event.State != "fallback_started" {
		t.Fatalf("event state = %q, want fallback_started", result.Event.State)
	}
	trace := prep.Trace()
	if len(trace) != 2 || trace[0].Kind != LaunchExecFallbackProbe || trace[1].Kind != LaunchExecFallbackStart {
		t.Fatalf("trace = %+v, want one probe and one leg", trace)
	}
	if trace[1].Tuple != good {
		t.Fatalf("leg tuple = %+v, want %+v", trace[1].Tuple, good)
	}
	requireTracePermitted(t, trace, prep.Newest(), good, FrozenIdentity{})
}

// TestMSPFallbackTriggerPerClass proves every attempted-start
// failure class opens the permit path exactly once: every first
// probe class, the recorded verdict, and every consumer surface
// stage. Pre-attempt reports and unconfirmed teardown never arm.
func TestMSPFallbackTriggerPerClass(t *testing.T) {
	probeCells := []struct {
		name   string
		runner func() (*ProbeRunner, *fakeProbeLauncher)
	}{
		{"probe timeout", func() (*ProbeRunner, *fakeProbeLauncher) {
			r, l := fakeTimeoutRunner(t)
			return r, l
		}},
		{"probe exec failed", execFailedRunner},
		{"probe nonzero exit", func() (*ProbeRunner, *fakeProbeLauncher) { return exitRunner(1, "", "") }},
		{"probe signaled", func() (*ProbeRunner, *fakeProbeLauncher) { return exitRunner(-1, "killed", "") }},
		{"probe output limit", func() (*ProbeRunner, *fakeProbeLauncher) {
			return exitRunner(0, "", strings.Repeat("x", 70*1024))
		}},
		{"probe drain failed", drainFailedRunner},
	}
	for _, cell := range probeCells {
		t.Run(cell.name, func(t *testing.T) {
			f := newFreeze(t)
			seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m52")
			install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
			runner, launcher := cell.runner()
			prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m52-probe"))
			requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
			if prep == nil || !prep.FailureRecorded() {
				t.Fatal("probe failure never reached the armed stage")
			}
			if launcher.calls != 1 {
				t.Fatalf("probe calls = %d, want the single failed attempt", launcher.calls)
			}
			reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP}
			grant, err := prep.RequestPermit(context.Background(), reporter)
			if err != nil || !grant.Permit.Issued() {
				t.Fatalf("probe class grants: %+v %v", grant, err)
			}
		})
	}

	t.Run("recorded verdict", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m52")
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		markBADForLaunch(t, f.config(), frozen, discovery.SnapshotID)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m52-bad"))
		requireFreezeCode(t, err, CodeRecordedBad, StageDedupe)
		if prep == nil || !prep.FailureRecorded() {
			t.Fatal("recorded verdict never reached the armed stage")
		}
		if launcher.calls != 0 {
			t.Fatal("recorded verdict was re-probed")
		}
		reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP}
		grant, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil || !grant.Permit.Issued() {
			t.Fatalf("recorded class grants: %+v %v", grant, err)
		}
	})

	consumerCells := []struct {
		name    string
		surface string
		stage   string
		started bool
	}{
		{"msp handshake refused", SurfaceMSP, "msp-handshake", true},
		{"msp session exec failed", SurfaceMSP, "msp-session", false},
		{"pty startup absent", SurfacePTY, "pty-startup", true},
		{"seal reprobe timeout", SurfacePTY, "seal-reprobe", true},
	}
	for _, cell := range consumerCells {
		t.Run(cell.name, func(t *testing.T) {
			f := newFreeze(t)
			seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m52")
			install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
			runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
			prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, cell.surface, RecoveryUnattended, install.env(), "l2c-m52-consumer"))
			if err != nil || prep.Outcome() != OutcomeRunNewest {
				t.Fatalf("newest prepares: %+v %v", prep, err)
			}
			failure := NewestFailure{
				AttemptID: "l2c-m52-fail", Newest: prep.Newest(),
				Stage: cell.stage, Code: "start_refused",
				Attempted: true, Started: cell.started, Cleanup: CleanupConfirmed,
			}
			if err := prep.ReportNewestStartFailure(context.Background(), failure); err != nil {
				t.Fatalf("consumer failure arms: %v", err)
			}
			reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: cell.surface}
			grant, err := prep.RequestPermit(context.Background(), reporter)
			if err != nil || !grant.Permit.Issued() {
				t.Fatalf("consumer class grants: %+v %v", grant, err)
			}
			if grant.Permit.Surface() != cell.surface {
				t.Fatalf("permit surface = %q, want %q", grant.Permit.Surface(), cell.surface)
			}
		})
	}

	t.Run("pre-attempt report never arms", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m52")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m52-pre"))
		if err != nil {
			t.Fatalf("newest prepares: %v", err)
		}
		failure := NewestFailure{
			AttemptID: "l2c-m52-pre-fail", Newest: prep.Newest(),
			Stage: "msp-config", Code: "static_refused",
			Attempted: false, Started: false, Cleanup: CleanupConfirmed,
		}
		requireLaunchCode(t, prep.ReportNewestStartFailure(context.Background(), failure), CodeLaunchRecoveryUnavailable, StageLaunch)
		if prep.FailureRecorded() {
			t.Fatal("pre-attempt report armed a failure")
		}
		reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP}
		_, err = prep.RequestPermit(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
	})

	t.Run("unconfirmed teardown never arms", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m52")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m52-teardown"))
		if err != nil {
			t.Fatalf("newest prepares: %v", err)
		}
		failure := NewestFailure{
			AttemptID: "l2c-m52-teardown-fail", Newest: prep.Newest(),
			Stage: "pty-startup", Code: "teardown_unknown",
			Attempted: true, Started: true, Cleanup: CleanupIncomplete,
		}
		requireLaunchCode(t, prep.ReportNewestStartFailure(context.Background(), failure), CodeLaunchRecoveryUnavailable, StageLaunch)
		if prep.FailureRecorded() {
			t.Fatal("unconfirmed teardown armed a failure")
		}
	})
}

// TestMSPStillOnceNeverLoop proves the retry cap: one permit issues
// per invocation, one leg starts per permit on every copy, and a
// failed leg never clears the receipt or opens another leg through
// any handle, including a direct Recover beside the controller.
func TestMSPStillOnceNeverLoop(t *testing.T) {
	f := newFreeze(t)
	_, seeded := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m52once")
	install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
	runner, _ := execFailedRunner()
	prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m52once"))
	requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
	legErr := errors.New("fallback child failed")
	reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP, fail: legErr}
	grant, err := prep.RequestPermit(context.Background(), reporter)
	if err != nil || !grant.Permit.Issued() {
		t.Fatalf("permit issues: %+v %v", grant, err)
	}
	result, err := grant.Permit.StartFallback(context.Background(), reporter)
	if !errors.Is(err, legErr) || result.Event.State != "fallback_failed" {
		t.Fatalf("failed leg reports: %+v %v", result, err)
	}
	current, err := ReadLastGood(context.Background(), f.config())
	if err != nil || current != seeded {
		t.Fatalf("failed leg kept %+v, want %+v", current, seeded)
	}
	again, err := grant.Permit.StartFallback(context.Background(), reporter)
	requireLaunchCode(t, err, CodeLaunchPermitConsumed, StageLaunch)
	if again.Event.State != "" {
		t.Fatalf("second leg returned %+v", again)
	}
	permitCopy := grant.Permit
	_, err = permitCopy.StartFallback(context.Background(), reporter)
	requireLaunchCode(t, err, CodeLaunchPermitConsumed, StageLaunch)
	if reporter.starts != 1 {
		t.Fatalf("starts = %d, want the single failed leg", reporter.starts)
	}
	_, err = prep.RequestPermit(context.Background(), reporter)
	requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
	_, err = runner.Recover(context.Background(), RecoveryUnattended, reporter)
	requireProbeCode(t, err, CodeRecoveryUnavailable, StageRecovery)
	if reporter.starts != 1 {
		t.Fatal("direct Recover started a second leg")
	}
}

// TestCompositionFailureRefuses proves every composition failure
// refuses without preparing a launch: discovery failures, freeze
// failures and malformed requests each refuse typed with zero probe
// calls and no bare PATH fallback on any route.
func TestCompositionFailureRefuses(t *testing.T) {
	t.Run("missing PATH", func(t *testing.T) {
		f := newFreeze(t)
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, []string{"HOME=/nowhere"}, "l2c-m61-nopath"))
		if prep != nil {
			t.Fatalf("refusal prepared %+v", prep.Outcome())
		}
		requireCode(t, err, CodeNoPath)
		if launcher.calls != 0 {
			t.Fatal("pathless composition probed")
		}
	})

	t.Run("unusable launcher", func(t *testing.T) {
		f := newFreeze(t)
		install := newInstall(t)
		launcherPath := filepath.Join(install.dir, launcherName)
		install.writeFileSync(launcherPath, []byte("not a script"), 0o644)
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m61-launcher"))
		if prep != nil {
			t.Fatalf("refusal prepared %+v", prep.Outcome())
		}
		requireCode(t, err, CodeLauncherUnusable)
		if launcher.calls != 0 {
			t.Fatal("launcherless composition probed")
		}
	})

	t.Run("corrupt sidecar", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "not a build\n", "1.4.4-R5419.1")
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m61-sidecar"))
		if prep != nil {
			t.Fatalf("refusal prepared %+v", prep.Outcome())
		}
		requireCode(t, err, CodeSidecarMalformed)
		if launcher.calls != 0 {
			t.Fatal("sidecarless composition probed")
		}
	})

	t.Run("freeze lock failure", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		cfg := f.config()
		cfg.Locker = &fakeLock{acquireErr: errors.New("lock down")}
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), cfg, runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m61-lock"))
		if prep != nil {
			t.Fatalf("refusal prepared %+v", prep.Outcome())
		}
		requireFreezeCode(t, err, CodeFreezeLockFailed, StageAdmit)
		if launcher.calls != 0 {
			t.Fatal("locked-out composition probed")
		}
	})

	t.Run("malformed requests", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		good := launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-m61-req")
		bad := []LaunchRequest{
			{Route: "sideways", Surface: good.Surface, Mode: good.Mode, Env: good.Env, AttemptID: good.AttemptID},
			{Route: good.Route, Surface: "teletype", Mode: good.Mode, Env: good.Env, AttemptID: good.AttemptID},
			{Route: good.Route, Surface: good.Surface, Mode: "maybe", Env: good.Env, AttemptID: good.AttemptID},
			{Route: good.Route, Surface: good.Surface, Mode: good.Mode, Env: good.Env, AttemptID: "has space"},
		}
		for i, req := range bad {
			prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, req)
			if prep != nil {
				t.Fatalf("request %d prepared %+v", i, prep.Outcome())
			}
			requireLaunchCode(t, err, CodeLaunchInvalid, StageLaunch)
		}
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), nil, good)
		if prep != nil {
			t.Fatalf("nil runner prepared %+v", prep.Outcome())
		}
		requireLaunchCode(t, err, CodeLaunchInvalid, StageLaunch)
		if launcher.calls != 0 {
			t.Fatal("malformed composition probed")
		}
	})
}

// recvTimeout receives one choreography signal with a 60-second hang
// guard. Coordination is event-triggered; the ceiling only converts a
// stuck peer into a named failure instead of a wedged suite.
func recvTimeout(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestEveryMuseExecHasNewestOrLastGoodPermit checks the independent execution
// stream from before preparation through all consumer route/mode decisions.
// Both successful and failed attempts are included; no controller trace entry
// substitutes for an observed fake launcher or session call. Route-(ii)
// deferral legs are in the same stream with their offer/decision ordering
// and their newest-frozen PROBE_OK binding, with no exemption.
func TestEveryMuseExecHasNewestOrLastGoodPermit(t *testing.T) {
	t.Run("newest leg traces unpermitted newest", func(t *testing.T) {
		f := prepareLaunchTrace(t, RouteLaunch, SurfaceMSP, RecoveryUnattended, "newest-success")
		r := f.r
		r.require(f.prefix("newest-success")...)
		file, err := os.Open(r.prep.Newest().Path)
		if err != nil {
			t.Fatal(err)
		}
		observed := f.reporter.newest(context.Background(), "newest-start", file, false)
		_ = file.Close()
		receipt, err := r.prep.ReportNewestStartSuccess(context.Background(), observed.Success)
		if err != nil || receipt.Sequence != r.good.Sequence+1 {
			t.Fatalf("newest success admits: %+v %v", receipt, err)
		}
		r.require(execRecord{stage: "first-probe", tuple: r.prep.Newest()}, execRecord{stage: "newest-start", tuple: r.prep.Newest()})
		requireNoFallbackDocument(t, f.f.root, r.attempt)
	})

	t.Run("fallback probes and leg trace permitted last-good", func(t *testing.T) {
		// The normative §5 route/surface inventory, not the production table.
		routes := []struct {
			route   LaunchRoute
			surface string
		}{
			{RouteLaunch, SurfaceMSP}, {RouteLaunch, SurfacePTY},
			{RouteRestore, SurfaceMSP}, {RouteRestore, SurfacePTY},
			{RouteResume, SurfaceMSP}, {RouteResume, SurfacePTY},
		}
		decisions := []struct {
			name    string
			mode    RecoveryMode
			consent bool
		}{
			{"automatic", RecoveryUnattended, false},
			{"accepted", RecoveryInteractive, true},
			{"declined", RecoveryInteractive, false},
		}
		for _, route := range routes {
			for _, decision := range decisions {
				t.Run(string(route.route)+"/"+route.surface+"/"+decision.name, func(t *testing.T) {
					f := prepareLaunchTrace(t, route.route, route.surface, decision.mode, "first-fork")
					f.r.require(f.prefix("first-fork")...)
					f.recover(decision.consent, false)
				})
			}
		}
	})

	t.Run("consumer failed attempts are in the same stream", func(t *testing.T) {
		for _, cell := range []struct{ stage, surface string }{
			{"consumer-plan-version", SurfacePTY},
			{"consumer-preexec-help", SurfacePTY},
			{"consumer-import-version", SurfacePTY},
			{"consumer-msp-handshake", SurfaceMSP},
			{"consumer-pty-startup", SurfacePTY},
		} {
			t.Run(cell.stage, func(t *testing.T) {
				f := prepareLaunchTrace(t, RouteLaunch, cell.surface, RecoveryInteractive, cell.stage)
				f.r.require(f.prefix(cell.stage)...)
				f.recover(true, true)
			})
		}
	})

	t.Run("recorded BAD routes have no fictitious first probe", func(t *testing.T) {
		for _, source := range []string{"recorded-bad", "recorded-bad-scan"} {
			t.Run(source, func(t *testing.T) {
				f := prepareLaunchTrace(t, RouteLaunch, SurfaceMSP, RecoveryUnattended, source)
				f.r.require(f.prefix(source)...)
				f.recover(false, false)
			})
		}
	})

	t.Run("unissued probes refuse and record nothing", func(t *testing.T) {
		f := prepareLaunchTrace(t, RouteLaunch, SurfaceMSP, RecoveryInteractive, "first-fork")
		var zero FallbackPermit
		lease, err := zero.AuthorizeProbe(context.Background(), receiptTuple(f.r.good))
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
		if lease != nil || zero.Probes() != 0 {
			t.Fatal("unissued permit authorized a probe")
		}
		f.r.require(f.prefix("first-fork")...)
	})

	t.Run("foreign-tuple probes refuse on a live permit", func(t *testing.T) {
		f := prepareLaunchTrace(t, RouteLaunch, SurfaceMSP, RecoveryUnattended, "first-fork")
		grant, err := f.r.prep.RequestPermit(context.Background(), f.reporter)
		if err != nil || !grant.Permit.Issued() {
			t.Fatalf("permit issues: %+v %v", grant, err)
		}
		foreign := FrozenIdentity{Build: "1.4.2-R4000", SHA256: strings.Repeat("ef", 32), Path: "/var/empty/muse-lkg/muse-bin-1.4.2-R4000"}
		for _, tuple := range []FrozenIdentity{foreign, f.r.prep.Newest()} {
			lease, err := grant.Permit.AuthorizeProbe(context.Background(), tuple)
			requireLaunchCode(t, err, CodeLaunchTupleMismatch, StageLaunch)
			if lease != nil {
				t.Fatal("foreign-tuple authorization issued a lease")
			}
		}
		if grant.Permit.Probes() != 0 {
			t.Fatal("refused probes counted")
		}
		f.r.require(f.prefix("first-fork")...)
		lease, err := grant.Permit.AuthorizeProbe(context.Background(), receiptTuple(f.r.good))
		if err != nil || lease == nil {
			t.Fatalf("exact-tuple authorization: %v", err)
		}
		lease.Release()
	})

	t.Run("probes after the leg refuse", func(t *testing.T) {
		f := prepareLaunchTrace(t, RouteLaunch, SurfaceMSP, RecoveryUnattended, "first-fork")
		f.r.require(f.prefix("first-fork")...)
		f.recover(false, false)
		before := append([]execRecord(nil), f.r.records...)
		lease, err := f.r.permit.AuthorizeProbe(context.Background(), receiptTuple(f.r.good))
		requireLaunchCode(t, err, CodeLaunchPermitConsumed, StageLaunch)
		if lease != nil || f.r.permit.Probes() != 2 {
			t.Fatal("consumed permit authorized a probe")
		}
		f.r.require(before...)
	})

	t.Run("probe without a lease executes nothing", func(t *testing.T) {
		f := prepareLaunchTrace(t, RouteLaunch, SurfaceMSP, RecoveryInteractive, "first-fork")
		// Exercise production authorization with an omitted permit. The fake
		// launcher is reached only through a returned lease, never directly.
		var missing *FallbackPermit
		lease, err := missing.AuthorizeProbe(context.Background(), receiptTuple(f.r.good))
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
		if lease != nil {
			t.Fatal("omitted authorization issued a lease")
		}
		f.r.require(f.prefix("first-fork")...)
	})

	t.Run("missing failure evidence authorizes nothing", func(t *testing.T) {
		f := prepareLaunchTrace(t, RouteLaunch, SurfaceMSP, RecoveryInteractive, "consumer-msp-handshake")
		r := f.r
		r.require(f.prefix("consumer-msp-handshake")...)
		newest := r.prep.Newest()
		if err := os.Remove(filepath.Join(f.f.root, "meta", freezeKey(newest.Build, newest.SHA256), "attempts", r.attempt+".json")); err != nil {
			t.Fatal(err)
		}
		grant, err := r.prep.RequestPermit(context.Background(), f.reporter)
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
		if grant.Permit.Issued() {
			t.Fatal("evidenceless failure issued a permit")
		}
		requireNoFallbackDocument(t, f.f.root, r.attempt)
		r.require(f.prefix("consumer-msp-handshake")...)
	})

	t.Run("deferral legs trace the newest frozen target", func(t *testing.T) {
		t.Run("unattended", func(t *testing.T) {
			f := prepareDeferralTrace(t, RecoveryUnattended)
			receipt, err := f.authz.Start(context.Background(), f.reporter)
			if err != nil {
				t.Fatalf("deferred start: %v", err)
			}
			if receipt.Frozen.Build != f.r.deferral.Build || receipt.Frozen.SHA256 != f.r.deferral.SHA256 {
				t.Fatalf("receipt tuple = %+v, want %+v", receipt.Frozen, f.r.deferral)
			}
			f.r.require(execRecord{stage: "deferred-start", tuple: f.r.deferral})
			requireTracePermitted(t, f.r.prep.Trace(), f.r.prep.Newest(), FrozenIdentity{}, f.r.deferral)
			if f.launcher.calls != 0 {
				t.Fatal("deferral probed the newest")
			}
		})

		t.Run("interactive accept", func(t *testing.T) {
			f := prepareDeferralTrace(t, RecoveryInteractive)
			f.reporter.consent = true
			receipt, err := f.authz.Start(context.Background(), f.reporter)
			if err != nil {
				t.Fatalf("accepted deferral starts: %v", err)
			}
			if receipt.Frozen.Build != f.r.deferral.Build || receipt.Frozen.SHA256 != f.r.deferral.SHA256 {
				t.Fatalf("receipt tuple = %+v, want %+v", receipt.Frozen, f.r.deferral)
			}
			g := f.r.deferral
			f.r.require(
				execRecord{stage: "deferral-offer", tuple: g},
				execRecord{stage: "deferral-accept", tuple: g},
				execRecord{stage: "deferred-start", tuple: g},
			)
			requireTracePermitted(t, f.r.prep.Trace(), f.r.prep.Newest(), FrozenIdentity{}, g)
		})

		t.Run("interactive decline", func(t *testing.T) {
			f := prepareDeferralTrace(t, RecoveryInteractive)
			f.reporter.consent = false
			_, err := f.authz.Start(context.Background(), f.reporter)
			requireLaunchCode(t, err, CodeLaunchDeferralDeclined, StageLaunch)
			g := f.r.deferral
			f.r.require(
				execRecord{stage: "deferral-offer", tuple: g},
				execRecord{stage: "deferral-decline", tuple: g},
			)
			if len(f.r.prep.Trace()) != 0 {
				t.Fatalf("trace = %+v, want no execution", f.r.prep.Trace())
			}
			current, err := ReadLastGood(context.Background(), f.r.cfg)
			if err != nil || current != f.r.good {
				t.Fatalf("declined deferral kept %+v, want %+v", current, f.r.good)
			}
			if f.launcher.calls != 0 {
				t.Fatal("declined deferral probed")
			}
		})

		t.Run("interactive accept then failed leg", func(t *testing.T) {
			f := prepareDeferralTrace(t, RecoveryInteractive)
			f.reporter.consent = true
			legErr := errors.New("deferred child failed")
			f.reporter.fail = legErr
			_, err := f.authz.Start(context.Background(), f.reporter)
			if !errors.Is(err, legErr) {
				t.Fatalf("deferred failure = %v, want the leg error", err)
			}
			g := f.r.deferral
			f.r.require(
				execRecord{stage: "deferral-offer", tuple: g},
				execRecord{stage: "deferral-accept", tuple: g},
				execRecord{stage: "deferred-start", tuple: g, failed: true},
			)
			current, err := ReadLastGood(context.Background(), f.r.cfg)
			if err != nil || current != f.r.good {
				t.Fatalf("failed deferral kept %+v, want %+v", current, f.r.good)
			}
			requireNoFallbackDocument(t, f.f.root, f.r.attempt)
		})
	})
}

// TestChangedLastGoodReceiptCannotReuseConsent proves the permit
// re-reads the durable receipt under the state lock before starting,
// in both modes: a superseding different tuple refuses with the
// target-changed reason and starts nothing, while a same-tuple newer
// sequence is adopted with updated provenance.
func TestChangedLastGoodReceiptCannotReuseConsent(t *testing.T) {
	prepareGranted := func(t *testing.T, mode RecoveryMode, attempt string) (*freezeFixture, HostConfig, PermitGrant, *fakeStartReporter, FrozenIdentity) {
		t.Helper()
		f := newFreeze(t)
		good, _ := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-m70")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := execFailedRunner()
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, mode, install.env(), attempt))
		requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
		reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP, consent: true}
		grant, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil || !grant.Permit.Issued() {
			t.Fatalf("permit issues: %+v %v", grant, err)
		}
		return f, f.config(), grant, reporter, good
	}

	t.Run("interactive consent cannot run a superseded target", func(t *testing.T) {
		f, cfg, grant, reporter, _ := prepareGranted(t, RecoveryInteractive, "l2c-m70")
		if reporter.offers != 1 {
			t.Fatalf("offers = %d, want the single granted offer", reporter.offers)
		}
		newer, err := WriteLastGood(context.Background(), cfg, genuineSuccess(probeOKBuild(t, f, "1.4.2-R4000"), "supersede-m70"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = grant.Permit.StartFallback(context.Background(), reporter)
		requireProbeCode(t, err, CodeFallbackTargetChanged, StageRecovery)
		if reporter.starts != 0 {
			t.Fatal("superseded consent started")
		}
		current, err := ReadLastGood(context.Background(), cfg)
		if err != nil || current != newer {
			t.Fatalf("receipt = %+v, want the superseding %+v", current, newer)
		}
	})

	t.Run("unattended grant cannot run a superseded target", func(t *testing.T) {
		f, cfg, grant, reporter, _ := prepareGranted(t, RecoveryUnattended, "l2c-m70-auto")
		newer, err := WriteLastGood(context.Background(), cfg, genuineSuccess(probeOKBuild(t, f, "1.4.2-R4000"), "supersede-m70-auto"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = grant.Permit.StartFallback(context.Background(), reporter)
		requireProbeCode(t, err, CodeFallbackTargetChanged, StageRecovery)
		if reporter.starts != 0 {
			t.Fatal("superseded grant started")
		}
		current, err := ReadLastGood(context.Background(), cfg)
		if err != nil || current != newer {
			t.Fatalf("receipt = %+v, want the superseding %+v", current, newer)
		}
	})

	t.Run("same-tuple newer sequence is adopted", func(t *testing.T) {
		_, cfg, grant, _, good := prepareGranted(t, RecoveryInteractive, "l2c-m70-adopt")
		second, err := WriteLastGood(context.Background(), cfg, genuineSuccess(good, "resequence-m70"))
		if err != nil {
			t.Fatal(err)
		}
		reporter := &adoptionReporter{t: t}
		result, err := grant.Permit.StartFallback(context.Background(), reporter)
		if err != nil {
			t.Fatalf("adopted leg starts: %v", err)
		}
		if reporter.starts != 1 || reporter.startedTuple != good {
			t.Fatalf("starts = %d tuple = %+v, want one start of %+v", reporter.starts, reporter.startedTuple, good)
		}
		if result.Event.LastGood.Sequence != second.Sequence {
			t.Fatalf("event sequence = %d, want the adopted %d", result.Event.LastGood.Sequence, second.Sequence)
		}
		if result.Receipt.Sequence != second.Sequence+1 {
			t.Fatalf("receipt sequence = %d, want %d", result.Receipt.Sequence, second.Sequence+1)
		}
	})
}

// TestLaunchDeclinedOfferSpendsAllowance proves an interactive
// decline terminates the invocation's fallback: the decision is
// typed, the allowance is spent, and neither a second request nor a
// direct Recover beside the controller can re-offer.
func TestLaunchDeclinedOfferSpendsAllowance(t *testing.T) {
	f := newFreeze(t)
	seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-decline")
	install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
	runner, _ := execFailedRunner()
	prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryInteractive, install.env(), "l2c-decline"))
	requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
	reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP, consent: false}
	grant, err := prep.RequestPermit(context.Background(), reporter)
	requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
	if grant.Outcome != PermitDeclined || grant.Permit.Issued() {
		t.Fatalf("decline grants: %+v", grant)
	}
	if reporter.offers != 1 {
		t.Fatalf("offers = %d, want the single declined offer", reporter.offers)
	}
	requireNoFallbackDocument(t, f.root, "l2c-decline")
	_, err = prep.RequestPermit(context.Background(), reporter)
	requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
	if reporter.offers != 1 {
		t.Fatal("decline was re-offered")
	}
	_, err = runner.Recover(context.Background(), RecoveryInteractive, reporter)
	requireProbeCode(t, err, CodeRecoveryUnavailable, StageRecovery)
	if reporter.offers != 1 {
		t.Fatal("direct Recover re-offered after a decline")
	}
}

// TestLaunchRouteTableCoversSection5 drives the section-5 route
// inventory through the controller entry: every route selects
// newest first, every failure class routes to its outcome, stale
// primaries refuse, and unknown routes and canceled contexts refuse
// before any selection.
func TestLaunchRouteTableCoversSection5(t *testing.T) {
	t.Run("fresh launch admits newest success", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfacePTY, RecoveryInteractive, install.env(), "l2c-route-fresh"))
		if err != nil || prep.Outcome() != OutcomeRunNewest {
			t.Fatalf("fresh prepares: %+v %v", prep, err)
		}
		success := genuineSuccess(prep.Newest(), "l2c-route-fresh-start")
		success.Surface = SurfacePTY
		receipt, err := prep.ReportNewestStartSuccess(context.Background(), success)
		if err != nil || receipt.Surface != SurfacePTY || receipt.Sequence != 1 {
			t.Fatalf("pty success admits: %+v %v", receipt, err)
		}
	})

	t.Run("resume reselects before an old plan", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.3-R5000", "1.4.4-R5419.1")
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		req := launchRequest(RouteResume, SurfacePTY, RecoveryInteractive, install.env(), "l2c-route-resume")
		req.SnapshotProvenance = "/frozen/1.4.3-R5000/muse-bin-1.4.3-R5000"
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, req)
		if err != nil {
			t.Fatalf("resume prepares: %v", err)
		}
		if prep.Newest().Build != "1.4.4-R5419.1" || prep.SnapshotBuild() != "1.4.3-R5000" {
			t.Fatalf("selected %+v provenance %q, want newest with recorded plan", prep.Newest(), prep.SnapshotBuild())
		}
	})

	t.Run("provenance on launch never becomes a pin", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.3-R5000", "1.4.4-R5419.1")
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		req := launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-route-pin")
		req.SnapshotProvenance = filepath.Join(install.dir, pinnedNamePrefix+"1.4.3-R5000")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, req)
		if err != nil || prep.Newest().Build != "1.4.4-R5419.1" {
			t.Fatalf("launch selects: %+v %v", prep, err)
		}
	})

	t.Run("recorded verdict routes without a probe", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-route")
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		markBADForLaunch(t, f.config(), frozen, discovery.SnapshotID)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-route-bad"))
		requireFreezeCode(t, err, CodeRecordedBad, StageDedupe)
		if prep == nil || prep.Outcome() != OutcomeFailedNewest || !prep.FailureRecorded() {
			t.Fatal("recorded verdict never reached the armed stage")
		}
		if launcher.calls != 0 {
			t.Fatal("recorded verdict was re-probed")
		}
		reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP}
		grant, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil || grant.Outcome != PermitAutoGranted {
			t.Fatalf("recorded verdict grants automatically: %+v %v", grant, err)
		}
		if grant.Warning != CodeFallbackWarning {
			t.Fatalf("warning = %q, want %q", grant.Warning, CodeFallbackWarning)
		}
	})

	t.Run("consumer seal failure arms and grants", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-route")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfacePTY, RecoveryUnattended, install.env(), "l2c-route-seal"))
		if err != nil {
			t.Fatalf("newest prepares: %v", err)
		}
		failure := NewestFailure{
			AttemptID: "l2c-route-seal-fail", Newest: prep.Newest(),
			Stage: "seal-reprobe", Code: "reprobe_timeout",
			Attempted: true, Started: true, Cleanup: CleanupConfirmed,
		}
		if err := prep.ReportNewestStartFailure(context.Background(), failure); err != nil {
			t.Fatalf("seal failure arms: %v", err)
		}
		reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfacePTY}
		grant, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil || !grant.Permit.Issued() {
			t.Fatalf("seal failure grants: %+v %v", grant, err)
		}
	})

	t.Run("stale primary path refuses without a receipt", func(t *testing.T) {
		f := newFreeze(t)
		good, _ := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-route")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-route-stale"))
		if err != nil {
			t.Fatalf("newest prepares: %v", err)
		}
		stale := genuineSuccess(good, "l2c-route-stale-start")
		_, err = prep.ReportNewestStartSuccess(context.Background(), stale)
		requireLaunchCode(t, err, CodeLaunchTupleMismatch, StageLaunch)
		current, err := ReadLastGood(context.Background(), f.config())
		if err != nil {
			t.Fatal(err)
		}
		if current.AttemptID == "l2c-route-stale-start" {
			t.Fatalf("stale primary admitted %+v", current)
		}
		for _, entry := range prep.Trace() {
			t.Fatalf("refused stale report traced %+v", entry)
		}
	})

	t.Run("unknown route refuses", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		req := launchRequest("bogus-route", SurfaceMSP, RecoveryUnattended, install.env(), "l2c-route-unknown")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, req)
		if prep != nil {
			t.Fatalf("unknown route prepared %+v", prep.Outcome())
		}
		requireLaunchCode(t, err, CodeLaunchInvalid, StageLaunch)
		if launcher.calls != 0 {
			t.Fatal("unknown route probed")
		}
	})

	t.Run("canceled context refuses before selection", func(t *testing.T) {
		f := newFreeze(t)
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, launcher := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		prep, err := PrepareNewestLaunch(ctx, f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), "l2c-route-canceled"))
		if prep != nil {
			t.Fatalf("canceled launch prepared %+v", prep.Outcome())
		}
		requireLaunchCode(t, err, CodeLaunchCanceled, StageLaunch)
		if launcher.calls != 0 {
			t.Fatal("canceled launch probed")
		}
	})
}

// TestConsumerFailureRequiresDurableEvidence proves a consumer
// newest-start failure arms recovery only through its durable attempt
// receipt: the report persists a readable receipt before arming, a
// conflicting publication arms nothing, and a deleted or disagreeing
// receipt prevents issuance. Refused reports touch no state.
func TestConsumerFailureRequiresDurableEvidence(t *testing.T) {
	prepareNewest := func(t *testing.T, f *freezeFixture, attempt string) (*PreparedLaunch, HostConfig) {
		t.Helper()
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := okProbeRunner("1.4.4-R5419.1", "1.4.4")
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), attempt))
		if err != nil || prep.Outcome() != OutcomeRunNewest {
			t.Fatalf("newest prepares: %+v %v", prep, err)
		}
		return prep, f.config()
	}
	report := func(newest FrozenIdentity, attempt string) NewestFailure {
		return NewestFailure{
			AttemptID: attempt, Newest: newest,
			Stage: "msp-handshake", Code: "handshake_refused",
			Attempted: true, Started: true, Cleanup: CleanupConfirmed,
			Detail: "handshake timed out",
		}
	}
	receiptPath := func(f *freezeFixture, newest FrozenIdentity, attempt string) string {
		return filepath.Join(f.root, "meta", freezeKey(newest.Build, newest.SHA256), "attempts", attempt+".json")
	}

	t.Run("report persists a readable receipt before arming", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-evidence")
		prep, cfg := prepareNewest(t, f, "l2c-evidence-ok")
		failure := report(prep.Newest(), "l2c-evidence-ok-fail")
		if _, err := os.Stat(receiptPath(f, prep.Newest(), failure.AttemptID)); !os.IsNotExist(err) {
			t.Fatalf("receipt exists before the report: %v", err)
		}
		if err := prep.ReportNewestStartFailure(context.Background(), failure); err != nil {
			t.Fatalf("consumer failure arms: %v", err)
		}
		if !prep.FailureRecorded() || prep.Outcome() != OutcomeFailedNewest {
			t.Fatal("report never reached the armed stage")
		}
		proof, err := ReadAttemptReceipt(context.Background(), cfg, prep.Newest(), failure.AttemptID)
		if err != nil {
			t.Fatalf("armed receipt reads back: %v", err)
		}
		if proof.Build != prep.Newest().Build || proof.SHA256 != prep.Newest().SHA256 ||
			proof.SelectionID != prep.Discovery().SnapshotID || proof.Cleanup != CleanupConfirmed ||
			proof.Outcome != ProbeExitNonzero || proof.Execution.Stage != failure.Stage ||
			!proof.Execution.Attempted || !proof.Execution.Started {
			t.Fatalf("receipt = %+v, want the exact reported failure", proof)
		}
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP}
		grant, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil || !grant.Permit.Issued() {
			t.Fatalf("evidenced failure grants: %+v %v", grant, err)
		}
	})

	t.Run("unstarted report persists the fork-failure outcome", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-evidence")
		prep, cfg := prepareNewest(t, f, "l2c-evidence-unstarted")
		failure := report(prep.Newest(), "l2c-evidence-unstarted-fail")
		failure.Started = false
		failure.Stage = "msp-session"
		failure.Code = "session_exec_failed"
		if err := prep.ReportNewestStartFailure(context.Background(), failure); err != nil {
			t.Fatalf("consumer failure arms: %v", err)
		}
		proof, err := ReadAttemptReceipt(context.Background(), cfg, prep.Newest(), failure.AttemptID)
		if err != nil || proof.Outcome != ProbeExecFailed || proof.Execution.Started {
			t.Fatalf("receipt = %+v %v, want the exec-failed outcome", proof, err)
		}
	})

	t.Run("unstarted conflict still refuses", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-evidence")
		prep, cfg := prepareNewest(t, f, "l2c-evidence-unstarted-conflict")
		newest := prep.Newest()
		prior := ProbeObservation{
			AttemptID: "l2c-evidence-unstarted-conflict-fail", SelectionID: prep.Discovery().SnapshotID,
			Outcome: ProbeTimeout, Detail: "an earlier attempt", Cleanup: CleanupConfirmed,
		}
		if _, err := RecordProbeOutcome(context.Background(), cfg, newest, prior); err != nil {
			t.Fatalf("prior receipt records: %v", err)
		}
		failure := report(newest, "l2c-evidence-unstarted-conflict-fail")
		failure.Started = false
		failure.Stage = "msp-session"
		failure.Code = "session_exec_failed"
		requireProbeCode(t, prep.ReportNewestStartFailure(context.Background(), failure), CodeFailureRecordUnavailable, StageProbe)
		if prep.FailureRecorded() {
			t.Fatal("conflicting report armed a failure")
		}
	})

	t.Run("conflicting publication prevents arming", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-evidence")
		prep, cfg := prepareNewest(t, f, "l2c-evidence-conflict")
		newest := prep.Newest()
		prior := ProbeObservation{
			AttemptID: "l2c-evidence-conflict-fail", SelectionID: prep.Discovery().SnapshotID,
			Outcome: ProbeTimeout, Detail: "an earlier attempt", Cleanup: CleanupConfirmed,
		}
		if _, err := RecordProbeOutcome(context.Background(), cfg, newest, prior); err != nil {
			t.Fatalf("prior receipt records: %v", err)
		}
		failure := report(newest, "l2c-evidence-conflict-fail")
		requireProbeCode(t, prep.ReportNewestStartFailure(context.Background(), failure), CodeFailureRecordUnavailable, StageProbe)
		if prep.FailureRecorded() || prep.Outcome() != OutcomeRunNewest {
			t.Fatal("conflicting report armed a failure")
		}
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP}
		_, err := prep.RequestPermit(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
		if reporter.offers != 0 {
			t.Fatal("conflicting report was offered")
		}
		requireNoFallbackDocument(t, f.root, failure.AttemptID)
	})

	t.Run("deleted receipt prevents issuance", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-evidence")
		prep, cfg := prepareNewest(t, f, "l2c-evidence-missing-launch")
		failure := report(prep.Newest(), "l2c-evidence-missing")
		if err := prep.ReportNewestStartFailure(context.Background(), failure); err != nil {
			t.Fatalf("consumer failure arms: %v", err)
		}
		if err := os.Remove(receiptPath(f, prep.Newest(), failure.AttemptID)); err != nil {
			t.Fatal(err)
		}
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP}
		_, err := prep.RequestPermit(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
		if reporter.offers != 0 {
			t.Fatal("evidenceless failure was offered")
		}
		requireNoFallbackDocument(t, f.root, failure.AttemptID)
	})

	t.Run("disagreeing receipt prevents issuance", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-evidence")
		prep, cfg := prepareNewest(t, f, "l2c-evidence-drift")
		failure := report(prep.Newest(), "l2c-evidence-drift-fail")
		if err := prep.ReportNewestStartFailure(context.Background(), failure); err != nil {
			t.Fatalf("consumer failure arms: %v", err)
		}
		path := receiptPath(f, prep.Newest(), failure.AttemptID)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		proof, err := attemptDocument.parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		proof.SelectionID = "drifted-selection"
		raw, err = attemptDocument.encode(proof)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP}
		_, err = prep.RequestPermit(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchRecoveryUnavailable, StageLaunch)
		if reporter.offers != 0 {
			t.Fatal("drifted failure was offered")
		}
	})

	t.Run("oversize detail refuses without arming", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-evidence")
		prep, _ := prepareNewest(t, f, "l2c-evidence-detail")
		failure := report(prep.Newest(), "l2c-evidence-detail-fail")
		failure.Detail = strings.Repeat("d", 9*1024)
		requireProbeCode(t, prep.ReportNewestStartFailure(context.Background(), failure), CodeProbeInvalid, StageProbe)
		if prep.FailureRecorded() {
			t.Fatal("oversize report armed a failure")
		}
		if _, err := os.Stat(receiptPath(f, prep.Newest(), failure.AttemptID)); !os.IsNotExist(err) {
			t.Fatalf("refused report persisted a receipt: %v", err)
		}
	})

	t.Run("refused reports touch no state", func(t *testing.T) {
		f := newFreeze(t)
		seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-evidence")
		prep, _ := prepareNewest(t, f, "l2c-evidence-refused")
		unattempted := report(prep.Newest(), "l2c-evidence-refused-pre")
		unattempted.Attempted = false
		requireLaunchCode(t, prep.ReportNewestStartFailure(context.Background(), unattempted), CodeLaunchRecoveryUnavailable, StageLaunch)
		if _, err := os.Stat(receiptPath(f, prep.Newest(), unattempted.AttemptID)); !os.IsNotExist(err) {
			t.Fatalf("pre-attempt report persisted a receipt: %v", err)
		}
		untidy := report(prep.Newest(), "l2c-evidence-refused-teardown")
		untidy.Cleanup = CleanupIncomplete
		requireLaunchCode(t, prep.ReportNewestStartFailure(context.Background(), untidy), CodeLaunchRecoveryUnavailable, StageLaunch)
		if _, err := os.Stat(receiptPath(f, prep.Newest(), untidy.AttemptID)); !os.IsNotExist(err) {
			t.Fatalf("unconfirmed report persisted a receipt: %v", err)
		}
		if prep.FailureRecorded() {
			t.Fatal("refused reports armed a failure")
		}
	})
}

// TestPermitProbeAndLegCoordinateOnSharedState proves probe
// authorization and session consumption coordinate on the shared
// permit state across copies: the leg refuses while a probe lease is
// outstanding and proceeds after its release, authorization after
// consumption refuses on every copy, concurrent authorizations count
// exactly once each, and a repeated release never corrupts the count.
func TestPermitProbeAndLegCoordinateOnSharedState(t *testing.T) {
	issue := func(t *testing.T, f *freezeFixture, attempt string) (PermitGrant, HostConfig, FrozenIdentity) {
		t.Helper()
		good, _ := seedReceiptForLaunch(t, f, "1.4.3-R5000", "seed-coord")
		install := launchInstall(t, "1.4.4-R5419.1\n", "1.4.4-R5419.1")
		runner, _ := execFailedRunner()
		prep, err := PrepareNewestLaunch(context.Background(), f.config(), runner, launchRequest(RouteLaunch, SurfaceMSP, RecoveryUnattended, install.env(), attempt))
		requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
		reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP}
		issued, err := prep.RequestPermit(context.Background(), reporter)
		if err != nil || !issued.Permit.Issued() {
			t.Fatalf("permit issues: %+v %v", issued, err)
		}
		return issued, f.config(), good
	}

	t.Run("leg refuses while a probe is outstanding", func(t *testing.T) {
		f := newFreeze(t)
		granted, cfg, good := issue(t, f, "l2c-coord-outstanding")
		authorized := make(chan *ProbeLease, 1)
		releaseIt := make(chan struct{})
		released := make(chan struct{})
		done := make(chan struct{})
		defer close(done)
		probeErr := make(chan error, 1)
		go func() {
			lease, err := granted.Permit.AuthorizeProbe(context.Background(), good)
			if err != nil {
				probeErr <- err
				return
			}
			authorized <- lease
			select {
			case <-releaseIt:
				lease.Release()
				close(released)
			case <-done:
			}
		}()
		var lease *ProbeLease
		select {
		case lease = <-authorized:
		case err := <-probeErr:
			t.Fatalf("probe authorizes: %v", err)
		case <-time.After(60 * time.Second):
			t.Fatal("probe never authorized")
		}
		if lease == nil || lease.Tuple() != good {
			t.Fatalf("lease admits %+v, want %+v", lease, good)
		}
		permitCopy := granted.Permit
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP}
		_, err := permitCopy.StartFallback(context.Background(), reporter)
		requireLaunchCode(t, err, CodeLaunchProbeOutstanding, StageLaunch)
		if reporter.starts != 0 {
			t.Fatal("leg started under an outstanding probe")
		}
		close(releaseIt)
		recvTimeout(t, released, "the probe release")
		result, err := permitCopy.StartFallback(context.Background(), reporter)
		if err != nil {
			t.Fatalf("leg starts after the release: %v", err)
		}
		if result.Event.State != "fallback_started" || reporter.starts != 1 {
			t.Fatalf("leg started once: %+v starts=%d", result.Event, reporter.starts)
		}
	})

	t.Run("authorization after consumption refuses on every copy", func(t *testing.T) {
		f := newFreeze(t)
		granted, cfg, good := issue(t, f, "l2c-coord-consumed")
		authorizer := granted.Permit
		consumer := granted.Permit
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP}
		if _, err := consumer.StartFallback(context.Background(), reporter); err != nil {
			t.Fatalf("leg starts: %v", err)
		}
		lease, err := authorizer.AuthorizeProbe(context.Background(), good)
		requireLaunchCode(t, err, CodeLaunchPermitConsumed, StageLaunch)
		if lease != nil {
			t.Fatalf("consumed permit issued %+v", lease)
		}
	})

	t.Run("concurrent authorizations count exactly", func(t *testing.T) {
		f := newFreeze(t)
		granted, _, good := issue(t, f, "l2c-coord-race")
		const probers = 8
		gate := make(chan struct{})
		errs := make(chan error, probers)
		leases := make(chan *ProbeLease, probers)
		for i := 0; i < probers; i++ {
			handle := granted.Permit
			go func() {
				<-gate
				lease, err := handle.AuthorizeProbe(context.Background(), good)
				if err != nil {
					errs <- err
					return
				}
				leases <- lease
			}()
		}
		close(gate)
		for i := 0; i < probers; i++ {
			select {
			case err := <-errs:
				t.Fatalf("concurrent probe authorizes: %v", err)
			case lease := <-leases:
				lease.Release()
			case <-time.After(60 * time.Second):
				t.Fatal("concurrent probes never returned")
			}
		}
		if granted.Permit.Probes() != probers {
			t.Fatalf("probes = %d, want %d", granted.Permit.Probes(), probers)
		}
	})

	t.Run("repeated release keeps the leg open", func(t *testing.T) {
		f := newFreeze(t)
		granted, cfg, good := issue(t, f, "l2c-coord-release")
		lease, err := granted.Permit.AuthorizeProbe(context.Background(), good)
		if err != nil {
			t.Fatalf("probe authorizes: %v", err)
		}
		lease.Release()
		lease.Release()
		reporter := &fakeStartReporter{t: t, cfg: cfg, surface: SurfaceMSP}
		if _, err := granted.Permit.StartFallback(context.Background(), reporter); err != nil {
			t.Fatalf("leg starts after a repeated release: %v", err)
		}
	})
}
