//go:build darwin || linux

package musebuilds

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/musebuilds/probeprocess"
)

type fakeProbeProcess struct {
	out, stderr io.ReadCloser
	done        chan probeprocess.Exit
	kill        func()
	gone        bool
}

func (p *fakeProbeProcess) Stdout() io.ReadCloser          { return p.out }
func (p *fakeProbeProcess) Stderr() io.ReadCloser          { return p.stderr }
func (p *fakeProbeProcess) Done() <-chan probeprocess.Exit { return p.done }
func (p *fakeProbeProcess) Kill() error {
	if p.kill != nil {
		p.kill()
	}
	return nil
}
func (p *fakeProbeProcess) GroupGone() bool { return p.gone }

type fakeProbeLauncher struct {
	calls int
	start func(context.Context, *os.File, []string) (probeprocess.Process, error)
}

func (l *fakeProbeLauncher) Start(ctx context.Context, f *os.File, env []string) (probeprocess.Process, error) {
	l.calls++
	return l.start(ctx, f, env)
}

func finishedProbe(output string) *fakeProbeProcess {
	p := &fakeProbeProcess{out: io.NopCloser(strings.NewReader(output)), stderr: io.NopCloser(strings.NewReader("")), done: make(chan probeprocess.Exit, 1), gone: true}
	p.done <- probeprocess.Exit{Code: 0}
	return p
}

func fakeTimeoutRunner(t *testing.T) (*ProbeRunner, *fakeProbeLauncher) {
	t.Helper()
	var expire context.CancelFunc
	l := &fakeProbeLauncher{}
	l.start = func(ctx context.Context, file *os.File, env []string) (probeprocess.Process, error) {
		p := finishedProbe("")
		<-p.done // the direct child has not exited until the group kill
		var once sync.Once
		p.kill = func() { once.Do(func() { p.done <- probeprocess.Exit{Code: -1, Signal: "killed"} }) }
		expire() // readiness-triggered stage expiry; outer context stays live
		return p, nil
	}
	r := NewProbeRunner(l)
	r.budget = func(ctx context.Context, cap time.Duration) (context.Context, context.CancelFunc) {
		if cap == firstProbeCap {
			c, cancel := context.WithCancel(ctx)
			expire = cancel
			return c, cancel
		}
		return context.WithTimeout(ctx, cap)
	}
	return r, l
}

type fakeStartReporter struct {
	t              *testing.T
	cfg            HostConfig
	surface        string
	offers, starts int
	consent        bool
	offer          func(RecoveryEvent)
	fail           error
	seen           RecoveryEvent
}

func (s *fakeStartReporter) Offer(ctx context.Context, e RecoveryEvent) (bool, error) {
	s.offers++
	s.seen = e
	if s.offer != nil {
		s.offer(e)
	}
	return s.consent, nil
}
func (s *fakeStartReporter) Start(ctx context.Context, e RecoveryEvent, file *os.File) StartOutcome {
	s.starts++
	s.seen = e
	if e.State != "fallback_attempting" || e.Code != CodeFallbackWarning {
		s.t.Fatalf("startup lacks durable attempting warning: %+v", e)
	}
	raw, err := os.ReadFile(filepath.Join(s.cfg.FrozenRoot, "fallback-"+e.FailureAttempt+".json"))
	if err != nil {
		s.t.Fatal(err)
	}
	durable, err := recoveryDocument.parse(raw)
	if err != nil || durable != e {
		s.t.Fatalf("permit was not durable before startup: %+v %v", durable, err)
	}
	digest, err := hashFileDescriptor(ctx, StageRecovery, file)
	if err != nil || digest != e.LastGood.Frozen.SHA256 {
		s.t.Fatalf("reporter received wrong held bytes: %s %v", digest, err)
	}
	if s.fail != nil {
		return StartOutcome{Err: s.fail, Cleanup: CleanupConfirmed}
	}
	success := genuineSuccess(FrozenIdentity{Build: e.LastGood.Frozen.Build, SHA256: e.LastGood.Frozen.SHA256, Path: e.LastGood.Frozen.Path}, "fallback-start")
	success.Surface = s.surface
	return StartOutcome{Success: success, Cleanup: CleanupConfirmed}
}

// exerciseTimeoutRecovery composes the first-probe executor, persisted failure,
// offered/automatic recovery and the fake consumer's actual startup outcome.
func exerciseTimeoutRecovery(t *testing.T, surface string, mode RecoveryMode, fail bool) {
	t.Helper()
	f := newFreeze(t)
	good := probeOKBuild(t, f, "1.4.3-R5000")
	seeded, err := WriteLastGood(context.Background(), f.config(), genuineSuccess(good, "seed-start"))
	if err != nil {
		t.Fatal(err)
	}
	prior, err := os.ReadFile(filepath.Join(f.root, lastGoodFileName))
	if err != nil {
		t.Fatal(err)
	}
	id, d := probeBuild(t, f, "1.4.4-R5419.1")
	r, launcher := fakeTimeoutRunner(t)
	result, err := r.ProbeFrozen(context.Background(), f.config(), d, id, ProbeRequest{AttemptID: "first-timeout", SelectionID: d.SnapshotID})
	requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
	if result.Observation.Outcome != ProbeTimeout || result.Record.Receipt == nil || !result.Observation.Execution.Reaped {
		t.Fatalf("first timeout never reached the reaped recording stage: %+v", result)
	}
	requireCopied(t, f.config(), id)
	reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: surface, consent: true}
	if fail {
		reporter.fail = errors.New("surface startup failed")
	}
	event, recoveryErr := r.Recover(context.Background(), mode, reporter)
	if fail {
		if !errors.Is(recoveryErr, reporter.fail) || event.State != "fallback_failed" {
			t.Fatalf("fallback failure stage missing: %+v %v", event, recoveryErr)
		}
		after, err := os.ReadFile(filepath.Join(f.root, lastGoodFileName))
		if err != nil || string(after) != string(prior) {
			t.Fatalf("failed fallback changed last-good: %v", err)
		}
		current, err := ReadLastGood(context.Background(), f.config())
		if err != nil || current != seeded {
			t.Fatalf("failed fallback lost receipt: %+v %v", current, err)
		}
	} else {
		if recoveryErr != nil || event.State != "fallback_started" {
			t.Fatalf("recovery failed: %+v %v", event, recoveryErr)
		}
		current, err := ReadLastGood(context.Background(), f.config())
		if err != nil || current.Surface != surface || current.Sequence != seeded.Sequence+1 {
			t.Fatalf("recovery was not admitted on %s: %+v %v", surface, current, err)
		}
	}
	wantOffers := 0
	if mode == RecoveryInteractive {
		wantOffers = 1
	}
	if reporter.offers != wantOffers || reporter.starts != 1 || launcher.calls != 1 || event.LastGood.Frozen != seeded.Frozen {
		t.Fatalf("wrong recovery route: offers=%d starts=%d probes=%d %+v", reporter.offers, reporter.starts, launcher.calls, event)
	}
	_, err = r.Recover(context.Background(), mode, reporter)
	requireProbeCode(t, err, CodeRecoveryUnavailable, StageRecovery)
	if reporter.starts != 1 {
		t.Fatal("second fallback was started")
	}
}

// TestFirstProbeRecoveryReachesBothSurfaces supplements the retained M22 name
// with actual consumer-entry stages, including automatic first-timeout recovery.
func TestFirstProbeRecoveryReachesBothSurfaces(t *testing.T) {
	for _, surface := range []string{SurfaceMSP, SurfacePTY} {
		for _, mode := range []RecoveryMode{RecoveryInteractive, RecoveryUnattended} {
			t.Run(surface+"/"+string(mode), func(t *testing.T) { exerciseTimeoutRecovery(t, surface, mode, false) })
		}
	}
}

// TestFailedFallbackPreservesLastGood reaches the failed LAST-GOOD leg on both
// surface abstractions, rather than merely recording a first-probe failure.
func TestFailedFallbackPreservesLastGood(t *testing.T) {
	for _, surface := range []string{SurfaceMSP, SurfacePTY} {
		t.Run(surface, func(t *testing.T) { exerciseTimeoutRecovery(t, surface, RecoveryUnattended, true) })
	}
}

type heldProbePipe struct {
	started, closed, returned chan struct{}
	readOnce, closeOnce       sync.Once
}

func newHeldProbePipe() *heldProbePipe {
	return &heldProbePipe{started: make(chan struct{}), closed: make(chan struct{}), returned: make(chan struct{})}
}
func (p *heldProbePipe) Read([]byte) (int, error) {
	p.readOnce.Do(func() { close(p.started) })
	<-p.closed
	close(p.returned)
	return 0, os.ErrClosed
}
func (p *heldProbePipe) Close() error { p.closeOnce.Do(func() { close(p.closed) }); return nil }

func TestProbeBoundsPipeDrain(t *testing.T) {
	f := newFreeze(t)
	id, d := probeBuild(t, f, "1.4.4-R5419.1")
	held := newHeldProbePipe()
	t.Cleanup(func() { _ = held.Close() })
	p := finishedProbe("")
	p.out = held // direct child exited; escaped stdout holder is still live
	l := &fakeProbeLauncher{start: func(context.Context, *os.File, []string) (probeprocess.Process, error) { return p, nil }}
	r := NewProbeRunner(l)
	var cleanupEntered bool
	r.budget = func(ctx context.Context, cap time.Duration) (context.Context, context.CancelFunc) {
		if cap != probeCleanupCap {
			return context.WithTimeout(ctx, cap)
		}
		cleanupEntered = true
		c, cancel := context.WithCancel(ctx)
		p.kill = func() { <-held.started; cancel() }
		return c, cancel
	}
	guard, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	type answer struct {
		result ProbeResult
		err    error
	}
	answered := make(chan answer, 1)
	go func() {
		result, err := r.ProbeFrozen(guard, f.config(), d, id, ProbeRequest{AttemptID: "escaped-holder", SelectionID: d.SnapshotID})
		answered <- answer{result, err}
	}()
	var result ProbeResult
	var err error
	select {
	case a := <-answered:
		result, err = a.result, a.err
	case <-guard.Done():
		t.Fatal("escaped stdout holder outlived the bounded cleanup event")
	}
	requireProbeCode(t, err, CodeProbeCleanupIncomplete, StageProbeCleanup)
	if !cleanupEntered || !result.Observation.Execution.Reaped || result.Observation.Cleanup != CleanupIncomplete || result.Record.Receipt == nil {
		t.Fatalf("escaped-holder stage not reached: %+v", result)
	}
	select {
	case <-held.closed:
	default:
		t.Fatal("bounded drain did not close the held stdout pipe")
	}
	select {
	case <-held.returned:
	case <-guard.Done():
		t.Fatal("closed stdout did not release the reader")
	}
	requireCopied(t, f.config(), id)
}

func TestUnconfirmedTeardownNeverStartsFallback(t *testing.T) {
	f := newFreeze(t)
	good := probeOKBuild(t, f, "1.4.3-R5000")
	if _, err := WriteLastGood(context.Background(), f.config(), genuineSuccess(good, "good")); err != nil {
		t.Fatal(err)
	}
	id, d := probeBuild(t, f, "1.4.4-R5419.1")
	l := &fakeProbeLauncher{}
	l.start = func(context.Context, *os.File, []string) (probeprocess.Process, error) {
		p := finishedProbe("Muse Code 1.4.4 (1.4.4-R5419.1)\n")
		p.gone = l.calls > 1 // first direct child reaped, descendant cleanup unknown
		return p, nil
	}
	r := NewProbeRunner(l)
	result, err := r.ProbeFrozen(context.Background(), f.config(), d, id, ProbeRequest{AttemptID: "unknown-cleanup", SelectionID: d.SnapshotID})
	requireProbeCode(t, err, CodeProbeCleanupIncomplete, StageProbeCleanup)
	if result.Record.Receipt == nil || !result.Observation.Execution.Reaped {
		t.Fatalf("cleanup gate not reached: %+v", result)
	}
	_, err = r.ProbeFrozen(context.Background(), f.config(), d, id, ProbeRequest{AttemptID: "second-child", SelectionID: d.SnapshotID})
	requireProbeCode(t, err, CodeProbeCleanupIncomplete, StageProbeCleanup)
	reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP, consent: true}
	_, err = r.Recover(context.Background(), RecoveryUnattended, reporter)
	requireProbeCode(t, err, CodeProbeCleanupIncomplete, StageProbeCleanup)
	if l.calls != 1 || reporter.starts != 0 || reporter.offers != 0 {
		t.Fatalf("unconfirmed cleanup started another child: %d/%d/%d", l.calls, reporter.starts, reporter.offers)
	}
}

func TestFirstProbeVerifiedDescriptorAndClassification(t *testing.T) {
	for _, row := range []struct {
		name, output string
		outcome      ProbeOutcome
		state        string
	}{
		{"match", "Muse Code 1.4.4 (1.4.4-R5419.1)\n", ProbeExitOKMatch, StateProbeOK},
		{"malformed", "invalid", ProbeExitOKUnparseable, StateBAD},
		{"mismatch", "Muse Code 1.4.3 (1.4.3-R5000)\n", ProbeExitOKMismatch, StateBAD},
		{"stdout-cap", strings.Repeat("x", probeStreamCap+1), ProbeOutputLimit, StateCopied},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newFreeze(t)
			id, d := probeBuild(t, f, "1.4.4-R5419.1")
			l := &fakeProbeLauncher{start: func(ctx context.Context, file *os.File, env []string) (probeprocess.Process, error) {
				digest, err := hashFileDescriptor(ctx, StageProbeExecute, file)
				if err != nil || digest != id.SHA256 || file.Name() != id.Path {
					t.Fatalf("launcher received unverified bytes: %s %v", digest, err)
				}
				if strings.Join(env, "|") != "MUSE_NO_AUTO_UPDATE=1" {
					t.Fatalf("uncurated env: %v", env)
				}
				return finishedProbe(row.output), nil
			}}
			r := NewProbeRunner(l)
			result, err := r.ProbeFrozen(context.Background(), f.config(), d, id, ProbeRequest{AttemptID: "observed", SelectionID: d.SnapshotID, Env: []string{"TOKEN=secret", "MUSE_NO_AUTO_UPDATE=0"}})
			if row.outcome == ProbeExitOKMatch {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
			}
			if result.Observation.Outcome != row.outcome || result.Record.State.State != row.state || l.calls != 1 {
				t.Fatalf("wrong observed classification: %+v %v", result, err)
			}
		})
	}
}

func TestFirstProbeRejectsUnverifiedAndCanceledBeforeLaunch(t *testing.T) {
	for _, attack := range []string{"bytes", "cancel"} {
		t.Run(attack, func(t *testing.T) {
			f := newFreeze(t)
			id, d := probeBuild(t, f, "1.4.4-R5419.1")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if attack == "cancel" {
				cancel()
			} else {
				if err := os.Chmod(id.Path, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(id.Path, []byte("changed"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(id.Path, 0o555); err != nil {
					t.Fatal(err)
				}
			}
			l := &fakeProbeLauncher{start: func(context.Context, *os.File, []string) (probeprocess.Process, error) {
				return finishedProbe("invalid"), nil
			}}
			r := NewProbeRunner(l)
			_, err := r.ProbeFrozen(ctx, f.config(), d, id, ProbeRequest{AttemptID: "refused", SelectionID: d.SnapshotID})
			code := CodeProbeStateInconsistent
			if attack == "cancel" {
				code = CodeProbeCanceled
			}
			requireProbeCode(t, err, code, StageProbe)
			if l.calls != 0 {
				t.Fatal("refused first probe started a child")
			}
		})
	}
}

func TestFreezeOrchestratesFirstProbeOnce(t *testing.T) {
	f := newFreeze(t)
	d := f.discoveryFor("1.4.4-R5419.1")
	l := &fakeProbeLauncher{start: func(ctx context.Context, file *os.File, env []string) (probeprocess.Process, error) {
		f.lock.mu.Lock()
		held := f.lock.held
		f.lock.mu.Unlock()
		if held != 0 {
			t.Fatal("global freeze lock held during first probe")
		}
		return finishedProbe("Muse Code 1.4.4 (1.4.4-R5419.1)\n"), nil
	}}
	r := NewProbeRunner(l)
	for _, attempt := range []string{"freeze-first", "freeze-reuse"} {
		result, err := r.Freeze(context.Background(), f.config(), d, ProbeRequest{AttemptID: attempt, SelectionID: d.SnapshotID})
		if err != nil || result.Record.State.State != StateProbeOK {
			t.Fatalf("Freeze did not orchestrate first probe: %+v %v", result, err)
		}
	}
	if l.calls != 1 {
		t.Fatalf("Freeze double-probed: %d", l.calls)
	}
	_, err := ReadLastGood(context.Background(), f.config())
	requireLastGoodCode(t, err, CodeLastGoodAbsent, StageLastGood)
}

func TestProbePreAttemptRefusalNeverOffersRecovery(t *testing.T) {
	f := newFreeze(t)
	good := probeOKBuild(t, f, "1.4.3-R5000")
	if _, err := WriteLastGood(context.Background(), f.config(), genuineSuccess(good, "seed")); err != nil {
		t.Fatal(err)
	}
	id, d := probeBuild(t, f, "1.4.4-R5419.1")
	l := &fakeProbeLauncher{start: func(context.Context, *os.File, []string) (probeprocess.Process, error) {
		return nil, errors.Join(probeprocess.ErrNotAttempted, context.DeadlineExceeded)
	}}
	r := NewProbeRunner(l)
	_, err := r.ProbeFrozen(context.Background(), f.config(), d, id, ProbeRequest{AttemptID: "not-attempted", SelectionID: d.SnapshotID})
	requireProbeCode(t, err, CodeRecoveryUnavailable, StageProbeExecute)
	_, err = ReadAttemptReceipt(context.Background(), f.config(), id, "not-attempted")
	requireProbeCode(t, err, CodeProbeReceiptAbsent, StageProbe)
	reporter := &fakeStartReporter{t: t, cfg: f.config(), consent: true, surface: SurfaceMSP}
	_, err = r.Recover(context.Background(), RecoveryUnattended, reporter)
	requireProbeCode(t, err, CodeRecoveryUnavailable, StageRecovery)
	if reporter.starts != 0 || reporter.offers != 0 {
		t.Fatal("pre-attempt refusal offered an older build")
	}
}

func TestProbeFailureReceiptSurvivesVerdictPublicationFailure(t *testing.T) {
	f := newFreeze(t)
	good := probeOKBuild(t, f, "1.4.3-R5000")
	if _, err := WriteLastGood(context.Background(), f.config(), genuineSuccess(good, "seed")); err != nil {
		t.Fatal(err)
	}
	id, d := probeBuild(t, f, "1.4.4-R5419.1")
	armFsync(t, func(name string, isDir bool) bool { return strings.HasPrefix(filepath.Base(name), ".state-") })
	l := &fakeProbeLauncher{start: func(context.Context, *os.File, []string) (probeprocess.Process, error) {
		return finishedProbe("malformed identity"), nil
	}}
	r := NewProbeRunner(l)
	result, err := r.ProbeFrozen(context.Background(), f.config(), d, id, ProbeRequest{AttemptID: "verdict-write-failed", SelectionID: d.SnapshotID})
	outer := requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
	requireProbeCode(t, outer.Cause, CodeProbePublicationFailed, StageProbe)
	if result.Record.Receipt == nil {
		t.Fatal("failed verdict lost the independent failure receipt")
	}
	reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP, consent: true, fail: errors.New("fallback startup failed")}
	event, err := r.Recover(context.Background(), RecoveryUnattended, reporter)
	if !errors.Is(err, reporter.fail) || event.State != "fallback_failed" || reporter.starts != 1 {
		t.Fatalf("independent receipt did not reach recovery: %+v %v", event, err)
	}
}

func TestRecoveryRefusesDeclineDriftAndMissingEvidence(t *testing.T) {
	for _, attack := range []string{"decline", "drift", "receipt", "same", "none"} {
		t.Run(attack, func(t *testing.T) {
			f := newFreeze(t)
			good := probeOKBuild(t, f, "1.4.3-R5000")
			if _, err := WriteLastGood(context.Background(), f.config(), genuineSuccess(good, "seed")); err != nil {
				t.Fatal(err)
			}
			id, d := probeBuild(t, f, "1.4.4-R5419.1")
			r, _ := fakeTimeoutRunner(t)
			if attack != "none" {
				_, err := r.ProbeFrozen(context.Background(), f.config(), d, id, ProbeRequest{AttemptID: "refusal", SelectionID: d.SnapshotID})
				requireProbeCode(t, err, CodeProbeAttemptFailed, StageProbeExecute)
			}
			if attack == "receipt" {
				path := filepath.Join(metaDir(f.root), freezeKey(id.Build, id.SHA256), attemptsDirName, "refusal.json")
				if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if attack == "same" {
				if _, err := RecordProbeOutcome(context.Background(), f.config(), id, successObservation("other-probe", d.SnapshotID)); err != nil {
					t.Fatal(err)
				}
				if _, err := WriteLastGood(context.Background(), f.config(), genuineSuccess(id, "concurrent-success")); err != nil {
					t.Fatal(err)
				}
			}
			reporter := &fakeStartReporter{t: t, cfg: f.config(), surface: SurfaceMSP, consent: attack != "decline"}
			if attack == "drift" {
				other := probeOKBuild(t, f, "1.4.2-R4800")
				reporter.offer = func(RecoveryEvent) {
					if _, err := WriteLastGood(context.Background(), f.config(), genuineSuccess(other, "during-offer")); err != nil {
						t.Fatal(err)
					}
				}
			}
			_, err := r.Recover(context.Background(), RecoveryInteractive, reporter)
			code := CodeRecoveryUnavailable
			if attack == "drift" {
				code = CodeFallbackTargetChanged
			}
			requireProbeCode(t, err, code, StageRecovery)
			if reporter.starts != 0 {
				t.Fatal("refused recovery started a child")
			}
			if (attack == "decline" || attack == "drift") && reporter.offers != 1 {
				t.Fatal("fixture did not reach interactive offer")
			}
		})
	}
}

// TestProbeFrozenRefusesOlderBuildUnderNewerSelection is the P2B-08
// regression: a valid older COPIED build presented under a newer accepted
// selection is refused at the probe entry with zero launcher calls. The
// older tuple is genuine frozen state and the newer selection is a real
// accepted discovery, so only the selection binding refuses.
func TestProbeFrozenRefusesOlderBuildUnderNewerSelection(t *testing.T) {
	f := newFreeze(t)
	older, _ := probeBuild(t, f, "1.4.3-R5000")
	newer := f.discoveryFor("1.4.4-R5419.1")
	l := &fakeProbeLauncher{start: func(context.Context, *os.File, []string) (probeprocess.Process, error) {
		return finishedProbe("Muse Code 1.4.3 (1.4.3-R5000)\n"), nil
	}}
	r := NewProbeRunner(l)
	_, err := r.ProbeFrozen(context.Background(), f.config(), newer, older, ProbeRequest{AttemptID: "older-under-newer", SelectionID: newer.SnapshotID})
	requireProbeCode(t, err, CodeProbeSelectionMismatch, StageProbe)
	if l.calls != 0 {
		t.Fatal("older build under a newer selection started a child")
	}
	requireCopied(t, f.config(), older)
}
