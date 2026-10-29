//go:build unix

package hostdefaults

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// A real source failure after its successful callback unwinds only its own
// result. Genuine published siblings, foreign owners and replacement leases
// remain usable. No native provider process is exercised by these fixtures.
func TestLegacyAttachResultRollbackPreservesGenuineSurvivors(t *testing.T) {
	for _, mode := range []string{"published-sibling", "foreign-owner", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			f := newLegacyAttachFixture(t)
			refusal := errors.New("fixture source lost verification after callback")
			var survivor *LegacyAttachment
			var originalNative *legacyAttachNativeFixture
			source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
				if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
					return err
				}
				originalNative = f.provider.native
				views, err := f.host.List(f.ctx)
				if err != nil || len(views) != 1 {
					t.Fatalf("callback did not issue original result: %+v %v", views, err)
				}
				q := f.request
				if mode == "foreign-owner" {
					q.Owner = core.Protocol8SessionOwner{Kind: "run", ID: "RUN-fixture-other"}
				}
				if mode == "replacement" {
					if _, err = f.host.Stop(f.ctx, views[0].SessionID); err != nil {
						t.Fatal(err)
					}
					q.ResumeSessionID = views[0].SessionID
				}
				survivor, err = f.handler(t, &legacyAttachSourceFixture{verify: fixtureVerifiedAttach}).attach(f.ctx, q)
				if err != nil || survivor == nil {
					t.Fatalf("genuine survivor attach failed: %+v %v", survivor, err)
				}
				return refusal
			}}
			result, err := f.handler(t, source).attach(f.ctx, f.request)
			if result != nil || !errors.Is(err, refusal) {
				t.Fatalf("source refusal lost: %+v %v", result, err)
			}
			if mode == "replacement" && !errors.Is(err, core.ErrStaleInputController) {
				t.Fatalf("stale result cleanup not reported: %v", err)
			}
			if mode != "replacement" && errors.Is(err, core.ErrStaleInputController) {
				t.Fatalf("current result falsely stale: %v", err)
			}
			current, e := f.host.Session(f.ctx, survivor.Session.SessionID)
			if e != nil || current.AttachedClients != 1 || current.State != core.SessionRunning || f.provider.native.stopped.Load() {
				t.Fatalf("rollback changed genuine survivor: %+v %v", current, e)
			}
			if mode == "published-sibling" && originalNative.stopped.Load() {
				t.Fatal("rollback stopped shared original runner")
			}
			if mode == "foreign-owner" && !originalNative.stopped.Load() {
				t.Fatal("rollback failed to retire only fresh original runner")
			}
			if _, e = f.host.Detach(f.ctx, survivor.Session.SessionID, survivor.AttachmentID); e != nil {
				t.Fatalf("rollback removed exact genuine client: %v", e)
			}
		})
	}
}

type legacyAttachSourceContextKey struct{}

type legacyAttachStopObservation struct{ value any }

type legacyAttachStopFailureRunner struct {
	*legacyAttachNativeFixture
	failure     error
	observation *legacyAttachStopObservation
}

func (r *legacyAttachStopFailureRunner) Stop(ctx context.Context) error {
	if r.observation != nil {
		r.observation.value = ctx.Value(legacyAttachSourceContextKey{})
	}
	if r.failure != nil {
		return r.failure
	}
	return r.legacyAttachNativeFixture.Stop(ctx)
}

type legacyAttachStopFailureFactory struct {
	*legacyAttachProviderFixture
	failure     error
	observation *legacyAttachStopObservation
}

func (f *legacyAttachStopFailureFactory) Launch(ctx context.Context, q core.ProviderLaunchRequest) (core.RunnerHost, error) {
	r, e := f.legacyAttachProviderFixture.Launch(ctx, q)
	if e != nil {
		return nil, e
	}
	return &legacyAttachStopFailureRunner{r.(*legacyAttachNativeFixture), f.failure, f.observation}, nil
}

// An actual provider Stop error during owned rollback must be joined with the
// original refusal/cancellation, never converted to success or a live hidden
// client. The existing valid source/Attach fixtures are the admitted neighbors.
func TestLegacyAttachResultRollbackJoinsActualCleanupFailure(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "source-error", true: "caller-cancel"}[cancelled], func(t *testing.T) {
			f := newLegacyAttachFixture(t)
			f.host.Close()
			stopFailure := errors.New("fixture actual provider stop failure")
			factory := &legacyAttachStopFailureFactory{f.provider, stopFailure, nil}
			notices, e := core.NewSessionNoticeStore(f.layout)
			if e != nil {
				t.Fatal(e)
			}
			h, e := core.NewHost(core.HostOptions{Layout: f.layout, Config: FileConfig{Path: filepath.Join(f.provider.root, "absent.json"), Optional: true, Version: "fixture", Namespace: f.layout.Namespace}, Codec: core.NewLegacyRecordCodec(), Notices: notices, Factories: []core.ProviderHostFactory{factory}})
			if e != nil {
				t.Fatal(e)
			}
			f.host = h
			t.Cleanup(h.Close)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			refusal := errors.New("fixture post-callback source failure")
			var expected error = refusal
			if cancelled {
				expected = context.Canceled
			}
			source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
				if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
					return err
				}
				if cancelled {
					cancel()
					return nil
				}
				return refusal
			}}
			a, e := f.handler(t, source).attach(ctx, f.request)
			if a != nil || !errors.Is(e, expected) || !errors.Is(e, stopFailure) {
				t.Fatalf("rollback failure lost original or cleanup cause: %+v %v", a, e)
			}
			views, e := h.List(f.ctx)
			if e != nil || len(views) != 1 || views[0].AttachedClients != 0 || !views[0].Retiring || views[0].Liveness != core.SessionLivenessUnknown {
				t.Fatalf("failed cleanup became healthy or retained hidden client: %+v %v", views, e)
			}
			if f.provider.launchCount() != 1 || f.provider.native.stopped.Load() {
				t.Fatal("cleanup failure fabricated stopped observation")
			}
		})
	}
}

// Source-owned context values must reach actual owned cleanup even after source
// cancellation. Removing cancellation must not replace the verified context
// with the outer HTTP context. The real provider Stop observes the value.
func TestLegacyAttachResultRollbackPreservesVerifiedContext(t *testing.T) {
	f := newLegacyAttachFixture(t)
	f.host.Close()
	observed := &legacyAttachStopObservation{}
	factory := &legacyAttachStopFailureFactory{f.provider, nil, observed}
	notices, e := core.NewSessionNoticeStore(f.layout)
	if e != nil {
		t.Fatal(e)
	}
	h, e := core.NewHost(core.HostOptions{Layout: f.layout, Config: FileConfig{Path: filepath.Join(f.provider.root, "absent.json"), Optional: true, Version: "fixture", Namespace: f.layout.Namespace}, Codec: core.NewLegacyRecordCodec(), Notices: notices, Factories: []core.ProviderHostFactory{factory}})
	if e != nil {
		t.Fatal(e)
	}
	f.host = h
	t.Cleanup(h.Close)
	marker := new(int)
	refusal := errors.New("fixture source context expired")
	source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		verified, cancel := context.WithCancel(context.WithValue(ctx, legacyAttachSourceContextKey{}, marker))
		defer cancel()
		if e := fn(verified, []string{"FIXTURE_LAUNCH=configured"}); e != nil {
			return e
		}
		return refusal
	}}
	a, e := f.handler(t, source).attach(f.ctx, f.request)
	if a != nil || !errors.Is(e, refusal) {
		t.Fatalf("source refusal lost: %+v %v", a, e)
	}
	if observed.value != marker {
		t.Fatal("rollback dropped verified-source context values")
	}
	if !f.provider.native.stopped.Load() {
		t.Fatal("verified-context cleanup did not retire owned runner")
	}
}
