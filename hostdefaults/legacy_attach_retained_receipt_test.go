//go:build unix

package hostdefaults

import (
	"context"
	"errors"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// rev6 Batch U probes (reviewer-only, not part of candidate).

func rev6Clients(t *testing.T, f *legacyAttachFixture) (int, []core.SessionView) {
	t.Helper()
	views, err := f.host.List(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, v := range views {
		n += v.AttachedClients
	}
	return n, views
}

// P1: duplicate callback after a successful first callback refuses the request,
// retires the fresh runner and leaves no client; exactly one native launch.
func TestRev6DuplicateCallbackUnwindsFirstResult(t *testing.T) {
	f := newLegacyAttachFixture(t)
	var second error
	source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
			return err
		}
		second = fn(ctx, []string{"FIXTURE_LAUNCH=configured"})
		return nil
	}}
	a, err := f.handler(t, source).attach(f.ctx, f.request)
	if a != nil || !errors.Is(err, core.ErrInputScopeClosed) || !errors.Is(second, core.ErrInputScopeClosed) {
		t.Fatalf("duplicate accepted: %+v %v second=%v", a, err, second)
	}
	if n, v := rev6Clients(t, f); n != 0 || f.provider.launchCount() != 1 || !f.provider.native.stopped.Load() {
		t.Fatalf("duplicate left effects: clients=%d launches=%d views=%+v", n, f.provider.launchCount(), v)
	}
}

// P2: callback escaped and invoked after source return cannot launch.
func TestRev6EscapedCallbackCannotLaunch(t *testing.T) {
	f := newLegacyAttachFixture(t)
	var escaped func(context.Context, []string) error
	source := &legacyAttachSourceFixture{verify: func(_ context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		escaped = fn
		return nil
	}}
	a, err := f.handler(t, source).attach(f.ctx, f.request)
	if a != nil || !errors.Is(err, ErrLegacyAttachSourceUnavailable) {
		t.Fatalf("uncalled source accepted: %+v %v", a, err)
	}
	if e := escaped(f.ctx, []string{"FIXTURE_LAUNCH=configured"}); !errors.Is(e, core.ErrInputScopeClosed) {
		t.Fatalf("escaped callback not refused: %v", e)
	}
	if n, _ := rev6Clients(t, f); n != 0 || f.provider.launchCount() != 0 {
		t.Fatalf("escaped callback had effects: clients=%d launches=%d", n, f.provider.launchCount())
	}
}

// P3: source succeeds but session is stopped before commit: commit refuses,
// no attachment returned.
func TestRev6CommitRefusesStoppedSession(t *testing.T) {
	f := newLegacyAttachFixture(t)
	source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
			return err
		}
		views, err := f.host.List(f.ctx)
		if err != nil || len(views) != 1 {
			t.Fatalf("no result: %v", err)
		}
		if _, err := f.host.Stop(f.ctx, views[0].SessionID); err != nil {
			t.Fatal(err)
		}
		return nil
	}}
	a, err := f.handler(t, source).attach(f.ctx, f.request)
	if a != nil || err == nil {
		t.Fatalf("stopped result published: %+v %v", a, err)
	}
	t.Logf("commit-stop error: %v", err)
}

// P4: replacement session between callback and commit: commit refuses, the
// genuine replacement survivor keeps its client and runner.
func TestRev6CommitRefusesReplacementPreservesSurvivor(t *testing.T) {
	f := newLegacyAttachFixture(t)
	var survivor *LegacyAttachment
	source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
			return err
		}
		views, _ := f.host.List(f.ctx)
		if _, err := f.host.Stop(f.ctx, views[0].SessionID); err != nil {
			t.Fatal(err)
		}
		q := f.request
		q.ResumeSessionID = views[0].SessionID
		var err error
		survivor, err = f.handler(t, &legacyAttachSourceFixture{verify: fixtureVerifiedAttach}).attach(f.ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}}
	a, err := f.handler(t, source).attach(f.ctx, f.request)
	if a != nil || !errors.Is(err, core.ErrStaleInputController) {
		t.Fatalf("stale result published or wrong cause: %+v %v", a, err)
	}
	cur, e := f.host.Session(f.ctx, survivor.Session.SessionID)
	if e != nil || cur.AttachedClients != 1 || cur.State != core.SessionRunning || f.provider.native.stopped.Load() {
		t.Fatalf("replacement damaged: %+v %v", cur, e)
	}
}

// P5: two concurrent unpublished results on one fresh runner, both refused.
// Fresh owner A refuses first while sibling B is still unpublished; then B
// refuses. Invariant under test: no refused request leaves a live runner.
func TestRev6InterleavedRefusalsLeaveNoOrphanRunner(t *testing.T) {
	f := newLegacyAttachFixture(t)
	bJoined := make(chan struct{})
	aDone := make(chan struct{})
	bErr := make(chan error, 1)
	refusal := errors.New("rev6 refusal")
	sourceA := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
			return err
		}
		go func() {
			sourceB := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
				if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
					close(bJoined)
					return err
				}
				close(bJoined)
				<-aDone
				return refusal
			}}
			a, err := f.handler(t, sourceB).attach(f.ctx, f.request)
			if a != nil {
				err = errors.Join(err, errors.New("B published"))
			}
			bErr <- err
		}()
		<-bJoined
		return refusal
	}}
	a, errA := f.handler(t, sourceA).attach(f.ctx, f.request)
	close(aDone)
	errB := <-bErr
	t.Logf("A: %v | B: %v", errA, errB)
	if a != nil || !errors.Is(errA, refusal) || !errors.Is(errB, refusal) {
		t.Fatalf("refusals lost")
	}
	n, views := rev6Clients(t, f)
	t.Logf("clients=%d launches=%d stopped=%v views=%+v", n, f.provider.launchCount(), f.provider.native.stopped.Load(), views)
	if n != 0 {
		t.Fatalf("hidden client remains")
	}
	if f.provider.launchCount() == 1 && !f.provider.native.stopped.Load() {
		t.Fatalf("both refused, fresh runner still live with no client: %+v", views)
	}
}

// P5 positive control: B published (success) while A refuses: runner kept.
func TestRev6InterleavedSiblingPublishedKeepsRunner(t *testing.T) {
	f := newLegacyAttachFixture(t)
	bJoined := make(chan struct{})
	aDone := make(chan struct{})
	bRes := make(chan *LegacyAttachment, 1)
	refusal := errors.New("rev6 refusal")
	sourceA := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
			return err
		}
		go func() {
			sourceB := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
				err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"})
				close(bJoined)
				<-aDone
				return err
			}}
			a, _ := f.handler(t, sourceB).attach(f.ctx, f.request)
			bRes <- a
		}()
		<-bJoined
		return refusal
	}}
	a, errA := f.handler(t, sourceA).attach(f.ctx, f.request)
	close(aDone)
	b := <-bRes
	t.Logf("A: %v | B: %+v", errA, b)
	if a != nil || !errors.Is(errA, refusal) || b == nil {
		t.Fatalf("unexpected: a=%+v b=%+v", a, b)
	}
	n, _ := rev6Clients(t, f)
	if n != 1 || f.provider.native.stopped.Load() {
		t.Fatalf("published sibling damaged: clients=%d stopped=%v", n, f.provider.native.stopped.Load())
	}
}

// A last sibling refusal must discharge retained fresh-origin custody using
// the real registered receipts. Two and three refused results leave neither a
// live zero-client runner nor reusable orphan receipt pointers.
func TestLegacyAttachRetainedReceiptLastRefusalRetiresFreshRunner(t *testing.T) {
	for _, count := range []int{2, 3} {
		t.Run(map[int]string{2: "two", 3: "three"}[count], func(t *testing.T) {
			f := newLegacyAttachFixture(t)
			q, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
			if err != nil {
				t.Fatal(err)
			}
			var receipts []*core.AttachmentResultReceipt
			var id string
			for i := 0; i < count; i++ {
				a, r, err := f.host.AttachWithResult(f.ctx, q)
				if err != nil || a == nil || r == nil || (i != 0 && a.Session.SessionID != id) {
					t.Fatalf("unpublished sibling fixture failed: %+v %v", a, err)
				}
				id = a.Session.SessionID
				receipts = append(receipts, r)
			}
			if err := f.host.RollbackAttachResult(f.ctx, receipts[0]); !errors.Is(err, core.ErrLegacyCommitUncertain) {
				t.Fatalf("fresh origin lost pending sibling custody: %v", err)
			}
			for i := 1; i < count; i++ {
				err := f.host.RollbackAttachResult(f.ctx, receipts[i])
				if err != nil && (i == count-1 || !errors.Is(err, core.ErrLegacyCommitUncertain)) {
					t.Fatalf("sibling refusal cleanup failed: %v", err)
				}
			}
			view, err := f.host.Session(f.ctx, id)
			if err != nil || view.AttachedClients != 0 || view.State != core.SessionStopped || !f.provider.native.stopped.Load() || f.provider.launchCount() != 1 {
				t.Fatalf("last refusal orphaned fresh runner: %+v stopped=%v err=%v", view, f.provider.native.stopped.Load(), err)
			}
			for _, r := range receipts {
				if err := f.host.CommitAttachResult(f.ctx, r); !errors.Is(err, core.ErrInvalidInput) {
					t.Fatalf("resolved refused receipt remained registered: %v", err)
				}
				if err := f.host.RollbackAttachResult(f.ctx, r); !errors.Is(err, core.ErrInvalidInput) {
					t.Fatalf("resolved refused receipt retained cleanup authority: %v", err)
				}
			}
		})
	}
}

// Publishing a real sibling releases retained refused metadata without retiring
// its native runner or consuming the genuine client. Unpublished is not success.
func TestLegacyAttachRetainedReceiptPublicationDischargesCustody(t *testing.T) {
	f := newLegacyAttachFixture(t)
	q, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
	if err != nil {
		t.Fatal(err)
	}
	_, origin, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	published, sibling, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.RollbackAttachResult(f.ctx, origin); !errors.Is(err, core.ErrLegacyCommitUncertain) {
		t.Fatalf("origin custody not retained: %v", err)
	}
	if err := f.host.CommitAttachResult(f.ctx, sibling); err != nil {
		t.Fatal(err)
	}
	view, err := f.host.Session(f.ctx, published.Session.SessionID)
	if err != nil || view.AttachedClients != 1 || view.State != core.SessionRunning || f.provider.native.stopped.Load() {
		t.Fatalf("publication damaged genuine survivor: %+v %v", view, err)
	}
	if err := f.host.RollbackAttachResult(f.ctx, origin); !errors.Is(err, core.ErrInvalidInput) {
		t.Fatalf("published sibling left refused origin registered: %v", err)
	}
	if _, err := f.host.Detach(f.ctx, published.Session.SessionID, published.AttachmentID); err != nil {
		t.Fatal("publication cleanup consumed genuine client", err)
	}
}

// A missing client alone is not a refusal: another still-pending source result
// cannot donate fresh-runner custody to a sibling rollback.
func TestLegacyAttachRetainedReceiptRequiresRefusedOrigin(t *testing.T) {
	f := newLegacyAttachFixture(t)
	q, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
	if err != nil {
		t.Fatal(err)
	}
	a, origin, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.host.Detach(f.ctx, a.Session.SessionID, a.AttachmentID); err != nil {
		t.Fatal(err)
	}
	_, sibling, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.RollbackAttachResult(f.ctx, sibling); err != nil {
		t.Fatal(err)
	}
	if f.provider.native.stopped.Load() {
		t.Fatal("unrefused origin donated native retirement authority")
	}
	if err := f.host.RollbackAttachResult(f.ctx, origin); err != nil || !f.provider.native.stopped.Load() {
		t.Fatalf("actual origin refusal did not resolve its own custody: %v", err)
	}
}

// Retained refused pointers from an old controller cannot stop or consume a
// real replacement, even when its retained session ID is the same.
func TestLegacyAttachRetainedReceiptCannotRetireReplacement(t *testing.T) {
	f := newLegacyAttachFixture(t)
	q, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
	if err != nil {
		t.Fatal(err)
	}
	a, origin, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	_, sibling, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.RollbackAttachResult(f.ctx, origin); !errors.Is(err, core.ErrLegacyCommitUncertain) {
		t.Fatal(err)
	}
	if _, err := f.host.Stop(f.ctx, a.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	q.ResumeSessionID = a.Session.SessionID
	replacement, issued, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.CommitAttachResult(f.ctx, issued); err != nil {
		t.Fatal(err)
	}
	for _, old := range []*core.AttachmentResultReceipt{origin, sibling} {
		if err := f.host.RollbackAttachResult(f.ctx, old); !errors.Is(err, core.ErrStaleInputController) {
			t.Fatalf("old retained receipt crossed replacement: %v", err)
		}
	}
	view, err := f.host.Session(f.ctx, replacement.Session.SessionID)
	if err != nil || view.AttachedClients != 1 || view.State != core.SessionRunning || f.provider.native.stopped.Load() || f.provider.launchCount() != 2 {
		t.Fatalf("retained cleanup damaged replacement: %+v %v", view, err)
	}
}

type retainedReceiptObservedFactory struct {
	*legacyAttachProviderFixture
	stateError, stopError error
	stopCalls             int
	stopValue             any
	stopContextError      error
}

type retainedReceiptObservedRunner struct {
	*legacyAttachNativeFixture
	factory *retainedReceiptObservedFactory
}

func (f *retainedReceiptObservedFactory) Launch(ctx context.Context, q core.ProviderLaunchRequest) (core.RunnerHost, error) {
	r, err := f.legacyAttachProviderFixture.Launch(ctx, q)
	if err != nil {
		return nil, err
	}
	return &retainedReceiptObservedRunner{r.(*legacyAttachNativeFixture), f}, nil
}
func (r *retainedReceiptObservedRunner) State(ctx context.Context) (core.ProviderHostState, error) {
	if r.factory.stateError != nil {
		return "", r.factory.stateError
	}
	return r.legacyAttachNativeFixture.State(ctx)
}
func (r *retainedReceiptObservedRunner) Stop(ctx context.Context) error {
	r.factory.stopCalls++
	r.factory.stopValue = ctx.Value(legacyAttachSourceContextKey{})
	r.factory.stopContextError = ctx.Err()
	if r.factory.stopError != nil {
		return r.factory.stopError
	}
	return r.legacyAttachNativeFixture.Stop(ctx)
}

func newRetainedReceiptObservedFixture(t *testing.T) (*legacyAttachFixture, *retainedReceiptObservedFactory) {
	t.Helper()
	f := newLegacyAttachFixture(t)
	f.host.Close()
	factory := &retainedReceiptObservedFactory{legacyAttachProviderFixture: f.provider}
	notices, err := core.NewSessionNoticeStore(f.layout)
	if err != nil {
		t.Fatal(err)
	}
	host, err := core.NewHost(core.HostOptions{Layout: f.layout, Config: FileConfig{Path: f.provider.root + "/absent.json", Optional: true, Version: "fixture", Namespace: f.layout.Namespace}, Codec: core.NewLegacyRecordCodec(), Notices: notices, Factories: []core.ProviderHostFactory{factory}})
	if err != nil {
		t.Fatal(err)
	}
	f.host = host
	t.Cleanup(host.Close)
	return f, factory
}

// Failed observation does not negate proven fresh ownership. This historical
// custody case must finish conditional cleanup on the last refusal, without an
// observation reset or a manual origin retry. Real Stop failure remains separate.
func TestLegacyAttachRetainedReceiptUnknownLivenessKeepsCustody(t *testing.T) {
	f, factory := newRetainedReceiptObservedFixture(t)
	q, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
	if err != nil {
		t.Fatal(err)
	}
	_, origin, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	_, sibling, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.RollbackAttachResult(f.ctx, origin); !errors.Is(err, core.ErrLegacyCommitUncertain) {
		t.Fatal(err)
	}
	unknown := errors.New("fixture native state observation unavailable")
	factory.stateError = unknown
	if err := f.host.RollbackAttachResult(f.ctx, sibling); err != nil {
		t.Fatalf("State observation blocked known-owned conditional retirement: %v", err)
	}
	views, err := f.host.List(f.ctx)
	if err != nil || len(views) != 1 || views[0].AttachedClients != 0 || views[0].State != core.SessionStopped || views[0].Liveness != core.SessionLivenessDead || views[0].Retiring || factory.stopCalls != 1 || !f.provider.native.stopped.Load() {
		t.Fatalf("known-owned cleanup remained orphaned under State failure: %+v stops=%d err=%v", views, factory.stopCalls, err)
	}
	for _, receipt := range []*core.AttachmentResultReceipt{origin, sibling} {
		if err := f.host.RollbackAttachResult(f.ctx, receipt); !errors.Is(err, core.ErrInvalidInput) {
			t.Fatalf("successful conditional cleanup left refused custody registered: %v", err)
		}
	}
}

// The actual two-source interleaving keeps last-refusal cancellation, private
// verified context values and real conditional Stop failure joined and visible.
// Successful cleanup is the nearby valid control; no native process is launched.
func TestLegacyAttachRetainedReceiptLastRefusalCleanupContext(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, cancelled := range []bool{false, true} {
			name := map[bool]string{false: "stopped", true: "stop-failed"}[failed] + "/" + map[bool]string{false: "source-refused", true: "caller-cancelled"}[cancelled]
			t.Run(name, func(t *testing.T) {
				f, factory := newRetainedReceiptObservedFixture(t)
				stopFailure := errors.New("fixture actual retained cleanup Stop failure")
				if failed {
					factory.stopError = stopFailure
				}
				refusal := errors.New("fixture retained source refusal")
				marker := new(int)
				joined, firstReturned := make(chan struct{}), make(chan struct{})
				secondError := make(chan error, 1)
				call, cancel := context.WithCancel(f.ctx)
				defer cancel()
				source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
					if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
						return err
					}
					go func() {
						second := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
							err := fn(context.WithValue(ctx, legacyAttachSourceContextKey{}, marker), []string{"FIXTURE_LAUNCH=configured"})
							close(joined)
							if err != nil {
								return err
							}
							<-firstReturned
							if cancelled {
								cancel()
								return nil
							}
							return refusal
						}}
						attachment, err := f.handler(t, second).attach(call, f.request)
						if attachment != nil {
							err = errors.Join(err, errors.New("refused sibling published"))
						}
						secondError <- err
					}()
					<-joined
					return refusal
				}}
				attachment, firstError := f.handler(t, source).attach(f.ctx, f.request)
				close(firstReturned)
				err := <-secondError
				if attachment != nil || !errors.Is(firstError, refusal) || !errors.Is(firstError, core.ErrLegacyCommitUncertain) {
					t.Fatalf("first refusal lost source or retained custody error: %+v %v", attachment, firstError)
				}
				var expected error = refusal
				if cancelled {
					expected = context.Canceled
				}
				if !errors.Is(err, expected) || (failed && !errors.Is(err, stopFailure)) {
					t.Fatalf("last refusal lost original or cleanup cause: %v", err)
				}
				if factory.stopCalls != 1 || factory.stopValue != marker || factory.stopContextError != nil {
					t.Fatalf("cleanup lost verified context or was duplicated/cancelled: stops=%d value=%v err=%v", factory.stopCalls, factory.stopValue, factory.stopContextError)
				}
				views, err := f.host.List(f.ctx)
				if err != nil || len(views) != 1 || views[0].AttachedClients != 0 || f.provider.launchCount() != 1 {
					t.Fatalf("last refusal retained hidden client or wrong launch: %+v %v", views, err)
				}
				if failed {
					if !views[0].Retiring || views[0].Liveness != core.SessionLivenessUnknown || f.provider.native.stopped.Load() {
						t.Fatalf("real Stop failure fabricated healthy/stopped state: %+v", views)
					}
				} else if views[0].State != core.SessionStopped || !f.provider.native.stopped.Load() {
					t.Fatalf("successful last refusal left fresh native live: %+v", views)
				}
			})
		}
	}
}

// rev4 Batch W unit A probes (reviewer-only, not part of candidate).

type rev4wFactory struct {
	*legacyAttachProviderFixture
	stateError error
	stopCalls  int
}
type rev4wRunner struct {
	*legacyAttachNativeFixture
	f *rev4wFactory
}

func (f *rev4wFactory) Launch(ctx context.Context, q core.ProviderLaunchRequest) (core.RunnerHost, error) {
	r, err := f.legacyAttachProviderFixture.Launch(ctx, q)
	if err != nil {
		return nil, err
	}
	return &rev4wRunner{r.(*legacyAttachNativeFixture), f}, nil
}
func (r *rev4wRunner) State(ctx context.Context) (core.ProviderHostState, error) {
	if r.f.stateError != nil {
		return "", r.f.stateError
	}
	return r.legacyAttachNativeFixture.State(ctx)
}
func (r *rev4wRunner) Stop(ctx context.Context) error {
	r.f.stopCalls++
	return r.legacyAttachNativeFixture.Stop(ctx)
}

func rev4wFixture(t *testing.T) (*legacyAttachFixture, *rev4wFactory) {
	t.Helper()
	f := newLegacyAttachFixture(t)
	f.host.Close()
	factory := &rev4wFactory{legacyAttachProviderFixture: f.provider}
	notices, err := core.NewSessionNoticeStore(f.layout)
	if err != nil {
		t.Fatal(err)
	}
	host, err := core.NewHost(core.HostOptions{Layout: f.layout, Config: FileConfig{Path: f.provider.root + "/absent.json", Optional: true, Version: "fixture", Namespace: f.layout.Namespace}, Codec: core.NewLegacyRecordCodec(), Notices: notices, Factories: []core.ProviderHostFactory{factory}})
	if err != nil {
		t.Fatal(err)
	}
	f.host = host
	t.Cleanup(host.Close)
	return f, factory
}

// W1: single (no sibling) fresh refusal through the real handler while native
// State observation fails but Stop works. Claim under test: the original F-A2
// retirement of a refused fresh owned runner is not narrowed by a State gate.
func TestRev4WSingleFreshRefusalStateErrorStillRetires(t *testing.T) {
	f, factory := rev4wFixture(t)
	refusal := errors.New("rev4w source refusal")
	factory.stateError = errors.New("rev4w transient state observation failure")
	source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
			return err
		}
		return refusal
	}}
	attachment, err := f.handler(t, source).attach(f.ctx, f.request)
	views, lerr := f.host.List(f.ctx)
	t.Logf("attach=%v err=%v stopCalls=%d nativeStopped=%v views=%+v lerr=%v", attachment != nil, err, factory.stopCalls, f.provider.native.stopped.Load(), views, lerr)
	if attachment != nil || !errors.Is(err, refusal) {
		t.Fatalf("refusal not returned: %v", err)
	}
	if factory.stopCalls != 1 || !f.provider.native.stopped.Load() {
		t.Fatalf("refused fresh runner not retired under State failure: stopCalls=%d stopped=%v", factory.stopCalls, f.provider.native.stopped.Load())
	}
}

// W1c (positive control): same single fresh refusal with healthy State retires.
func TestRev4WSingleFreshRefusalHealthyRetires(t *testing.T) {
	f, factory := rev4wFixture(t)
	refusal := errors.New("rev4w source refusal")
	source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
			return err
		}
		return refusal
	}}
	attachment, err := f.handler(t, source).attach(f.ctx, f.request)
	if attachment != nil || !errors.Is(err, refusal) || factory.stopCalls != 1 || !f.provider.native.stopped.Load() {
		t.Fatalf("healthy single refusal not retired: %v stops=%d", err, factory.stopCalls)
	}
}

// W2: non-fresh sibling refuses FIRST while fresh origin is still pending,
// then a later genuine sibling publishes, then origin refuses: runner must
// survive with the genuine client (no stop), and no receipt remains.
func TestRev4WOrderSiblingFirstThenPublishedThenOrigin(t *testing.T) {
	f, factory := rev4wFixture(t)
	q, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
	if err != nil {
		t.Fatal(err)
	}
	_, origin, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.RollbackAttachResult(f.ctx, b); err != nil {
		t.Fatalf("pending-origin sibling refusal: %v", err)
	}
	pub, c, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.CommitAttachResult(f.ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := f.host.RollbackAttachResult(f.ctx, origin); err != nil {
		t.Fatalf("origin refusal with published sibling: %v", err)
	}
	view, err := f.host.Session(f.ctx, pub.Session.SessionID)
	if err != nil || view.AttachedClients != 1 || view.State != core.SessionRunning || factory.stopCalls != 0 {
		t.Fatalf("published survivor damaged: %+v stops=%d %v", view, factory.stopCalls, err)
	}
	for _, r := range []*core.AttachmentResultReceipt{origin, b, c} {
		if err := f.host.RollbackAttachResult(f.ctx, r); !errors.Is(err, core.ErrInvalidInput) {
			t.Fatalf("receipt still registered: %v", err)
		}
	}
}

// W3: origin refused (retained), middle sibling refused (uncertain), last
// sibling PUBLISHES: survivor keeps runner and both refused receipts resolve.
func TestRev4WThreeLastPublishes(t *testing.T) {
	f, factory := rev4wFixture(t)
	q, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
	if err != nil {
		t.Fatal(err)
	}
	_, origin, _ := f.host.AttachWithResult(f.ctx, q)
	_, b, _ := f.host.AttachWithResult(f.ctx, q)
	pub, c, err := f.host.AttachWithResult(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.RollbackAttachResult(f.ctx, origin); !errors.Is(err, core.ErrLegacyCommitUncertain) {
		t.Fatal(err)
	}
	berr := f.host.RollbackAttachResult(f.ctx, b)
	t.Logf("middle sibling rollback err=%v", berr)
	if err := f.host.CommitAttachResult(f.ctx, c); err != nil {
		t.Fatal(err)
	}
	view, err := f.host.Session(f.ctx, pub.Session.SessionID)
	if err != nil || view.AttachedClients != 1 || view.State != core.SessionRunning || factory.stopCalls != 0 {
		t.Fatalf("survivor damaged: %+v %v", view, err)
	}
	for _, r := range []*core.AttachmentResultReceipt{origin, b} {
		if err := f.host.RollbackAttachResult(f.ctx, r); !errors.Is(err, core.ErrInvalidInput) {
			t.Fatalf("refused receipt still registered after publication: %v", err)
		}
	}
}

// W4: active owner work blocks retirement on last sibling refusal; claim:
// retained custody is not discarded while work fences hold (no orphan, no stop).
// W5: last-sibling refusal with ctx already cancelled at rollback entry.
func TestRev4WLastRefusalCancelledCtx(t *testing.T) {
	f, factory := rev4wFixture(t)
	q, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
	if err != nil {
		t.Fatal(err)
	}
	_, origin, _ := f.host.AttachWithResult(f.ctx, q)
	_, b, _ := f.host.AttachWithResult(f.ctx, q)
	if err := f.host.RollbackAttachResult(f.ctx, origin); !errors.Is(err, core.ErrLegacyCommitUncertain) {
		t.Fatal(err)
	}
	dead, cancel := context.WithCancel(f.ctx)
	cancel()
	err = f.host.RollbackAttachResult(dead, b)
	t.Logf("cancelled rollback err=%v stops=%d", err, factory.stopCalls)
	if !errors.Is(err, context.Canceled) || factory.stopCalls != 0 {
		t.Fatalf("cancelled cleanup not honest: %v", err)
	}
	// Retry with live ctx: retained exact custody must still retire.
	if err := f.host.RollbackAttachResult(f.ctx, b); err != nil {
		t.Logf("retry b err=%v", err)
		if err := f.host.RollbackAttachResult(f.ctx, origin); err != nil {
			t.Fatalf("no retained retry path: %v", err)
		}
	}
	if factory.stopCalls != 1 || !f.provider.native.stopped.Load() {
		t.Fatalf("retry did not retire: stops=%d", factory.stopCalls)
	}
}

// Failed State observation does not revoke already-proven fresh ownership.
// The real handler must conditionally retire both a single fresh result and a
// retained origin on last refusal. Actual Stop errors keep Retiring/Unknown;
// they cannot be replaced by an observation gate or a direct receipt retry.
func TestLegacyAttachOwnedRefusalRetiresDespiteStateProbeFailure(t *testing.T) {
	for _, retained := range []bool{false, true} {
		for _, failedStop := range []bool{false, true} {
			name := map[bool]string{false: "single", true: "last-sibling"}[retained] + "/" + map[bool]string{false: "stopped", true: "stop-error"}[failedStop]
			t.Run(name, func(t *testing.T) {
				f, factory := newRetainedReceiptObservedFixture(t)
				refusal := errors.New("fixture refused known owned result")
				stateFailure := errors.New("fixture State observation failed")
				stopFailure := errors.New("fixture conditional Stop failed")
				if failedStop {
					factory.stopError = stopFailure
				}
				joined, firstReturned := make(chan struct{}), make(chan struct{})
				secondError := make(chan error, 1)
				source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
					if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
						return err
					}
					if retained {
						go func() {
							second := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
								err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"})
								close(joined)
								if err != nil {
									return err
								}
								<-firstReturned
								return refusal
							}}
							attachment, err := f.handler(t, second).attach(f.ctx, f.request)
							if attachment != nil {
								err = errors.Join(err, errors.New("refused sibling published"))
							}
							secondError <- err
						}()
						<-joined
					}
					factory.stateError = stateFailure
					return refusal
				}}
				attachment, err := f.handler(t, source).attach(f.ctx, f.request)
				if retained {
					close(firstReturned)
					if attachment != nil || !errors.Is(err, refusal) || !errors.Is(err, core.ErrLegacyCommitUncertain) {
						t.Fatalf("first refusal lost retained origin custody: %+v %v", attachment, err)
					}
					err = <-secondError
				}
				if attachment != nil || !errors.Is(err, refusal) {
					t.Fatalf("owned refusal lost source error: %+v %v", attachment, err)
				}
				if factory.stopCalls != 1 {
					t.Fatalf("known fresh refused runner skipped conditional retirement: stops=%d stopped=%v err=%v", factory.stopCalls, f.provider.native.stopped.Load(), err)
				}
				if failedStop && !errors.Is(err, stopFailure) {
					t.Fatalf("actual conditional Stop failure lost: %v", err)
				}
				views, err := f.host.List(f.ctx)
				if err != nil || len(views) != 1 || views[0].AttachedClients != 0 || f.provider.launchCount() != 1 {
					t.Fatalf("owned refusal retained a client or wrong incarnation: %+v %v", views, err)
				}
				if failedStop {
					if !views[0].Retiring || views[0].Liveness != core.SessionLivenessUnknown || f.provider.native.stopped.Load() {
						t.Fatalf("actual Stop failure invented stopped acknowledgement: %+v", views)
					}
				} else if views[0].State != core.SessionStopped || views[0].Liveness != core.SessionLivenessDead || !f.provider.native.stopped.Load() {
					t.Fatalf("known fresh refusal orphaned runner under State failure: %+v", views)
				}
			})
		}
	}
}

// Observation failure grants no authority to a zero/copied/foreign/consumed
// receipt. Actual registered clients on both hosts must survive those refusals.
func TestLegacyAttachStateProbeFailureGrantsNoReceiptAuthority(t *testing.T) {
	for _, mode := range []string{"zero", "copied", "foreign-host", "committed"} {
		t.Run(mode, func(t *testing.T) {
			f, factory := newRetainedReceiptObservedFixture(t)
			q, err := PrepareLegacyAttach(f.request, []string{"FIXTURE_LAUNCH=configured"})
			if err != nil {
				t.Fatal(err)
			}
			_, receipt, err := f.host.AttachWithResult(f.ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			target := f.host
			var foreign *legacyAttachFixture
			var foreignFactory *retainedReceiptObservedFactory
			switch mode {
			case "zero":
				receipt = &core.AttachmentResultReceipt{}
			case "copied":
				copied := *receipt
				receipt = &copied
			case "foreign-host":
				foreign, foreignFactory = newRetainedReceiptObservedFixture(t)
				other, err := PrepareLegacyAttach(foreign.request, []string{"FIXTURE_LAUNCH=configured"})
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := foreign.host.AttachWithResult(foreign.ctx, other); err != nil {
					t.Fatal(err)
				}
				target = foreign.host
				foreignFactory.stateError = errors.New("fixture foreign State unavailable")
			case "committed":
				if err := f.host.CommitAttachResult(f.ctx, receipt); err != nil {
					t.Fatal(err)
				}
			}
			factory.stateError = errors.New("fixture State unavailable")
			if err := target.RollbackAttachResult(f.ctx, receipt); !errors.Is(err, core.ErrInvalidInput) {
				t.Fatalf("observation failure granted unregistered receipt authority: %v", err)
			}
			views, err := f.host.List(f.ctx)
			if err != nil || len(views) != 1 || views[0].AttachedClients != 1 || factory.stopCalls != 0 || f.provider.native.stopped.Load() {
				t.Fatalf("wrong receipt damaged original client/runner: %+v %v", views, err)
			}
			if foreign != nil {
				views, err := foreign.host.List(foreign.ctx)
				if err != nil || len(views) != 1 || views[0].AttachedClients != 1 || foreignFactory.stopCalls != 0 || foreign.provider.native.stopped.Load() {
					t.Fatalf("foreign receipt damaged target client/runner: %+v %v", views, err)
				}
			}
		})
	}
}
