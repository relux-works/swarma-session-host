//go:build unix

package hostdefaults

import (
	"context"
	"errors"
	"testing"
)

// Post-callback verification failure or caller cancellation cannot publish an
// undisclosed client. Reusing an admitted session preserves its exact original
// client and runner. These are in-process fixture effects, not native providers.
func TestLegacyAttachPostCallbackRefusalPreservesOnlyAdmittedClients(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, cancelled := range []bool{false, true} {
			name := map[bool]string{false: "fresh", true: "existing"}[existing] + "/" + map[bool]string{false: "source_failure", true: "caller_cancelled"}[cancelled]
			t.Run(name, func(t *testing.T) {
				f := newLegacyAttachFixture(t)
				var prior *LegacyAttachment
				if existing {
					var err error
					prior, err = f.handler(t, &legacyAttachSourceFixture{verify: fixtureVerifiedAttach}).attach(f.ctx, f.request)
					if err != nil || prior == nil {
						t.Fatalf("admitted attach control failed: %+v %v", prior, err)
					}
				}
				call, cancel := context.WithCancel(f.ctx)
				defer cancel()
				refusal := errors.New("fixture post-callback verification failure")
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
				result, err := f.handler(t, source).attach(call, f.request)
				if result != nil || !errors.Is(err, expected) {
					t.Fatalf("post-callback refusal became success or lost cause: %+v %v, want %v", result, err, expected)
				}
				views, err := f.host.List(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				want := 0
				if existing {
					want = 1
				}
				count := 0
				for _, v := range views {
					count += v.AttachedClients
				}
				if count != want {
					t.Fatalf("refused operation retained undisclosed client: clients=%d want=%d", count, want)
				}
				if existing {
					if f.provider.native.stopped.Load() || f.provider.launchCount() != 1 {
						t.Fatal("refusal retired or relaunched admitted runner")
					}
					if _, err := f.host.Detach(f.ctx, prior.Session.SessionID, prior.AttachmentID); err != nil {
						t.Fatalf("refusal lost exact admitted client: %v", err)
					}
				}
			})
		}
	}
}

// Removing the refused client alone is insufficient: a freshly owned runner
// must not remain live after the source rejects its successful callback.
func TestLegacyAttachPostCallbackRefusalRetiresFreshOwnedRunner(t *testing.T) {
	f := newLegacyAttachFixture(t)
	refusal := errors.New("fixture rejected fresh launch after callback")
	source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if err := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); err != nil {
			return err
		}
		return refusal
	}}
	result, err := f.handler(t, source).attach(f.ctx, f.request)
	if result != nil || !errors.Is(err, refusal) {
		t.Fatalf("source refusal lost: %+v %v", result, err)
	}
	if f.provider.launchCount() != 1 || f.provider.native == nil {
		t.Fatal("fresh launch control did not execute")
	}
	if !f.provider.native.stopped.Load() {
		t.Fatal("refused attach retained its fresh owned runner")
	}
}
