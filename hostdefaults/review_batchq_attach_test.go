//go:build unix

package hostdefaults

// Batch Q reviewer repro (rev4), not candidate source.

import (
	"context"
	"errors"
	"testing"
)

// Claim: a verified-attach source that runs its single callback successfully
// and then reports a fence/verification failure must not leave the refused
// request's client published. The handler must report the refusal (positive
// half, already held by the candidate) AND leave no attached client for a
// response the caller never receives (the brief's "source failure, no partial
// native effects"). Limit: controlled in-process provider fixture only.
func TestReviewBatchQAttachPostCallbackSourceFailureLeavesNoClient(t *testing.T) {
	f := newLegacyAttachFixture(t)
	fenceLost := errors.New("review fixture: fence lost after callback")
	source := &legacyAttachSourceFixture{verify: func(ctx context.Context, _ LegacyAttachRequest, _ LegacyAttachTarget, fn func(context.Context, []string) error) error {
		if e := fn(ctx, []string{"FIXTURE_LAUNCH=configured"}); e != nil {
			return e
		}
		return fenceLost
	}}
	attachment, err := f.handler(t, source).attach(f.ctx, f.request)
	if !errors.Is(err, fenceLost) || attachment != nil {
		t.Fatalf("post-callback source failure not reported: %+v %v", attachment, err)
	}
	views, err := f.host.List(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, view := range views {
		if view.AttachedClients != 0 {
			t.Fatalf("refused attach left a published client: session=%s clients=%d launches=%d", view.SessionID, view.AttachedClients, f.provider.launchCount())
		}
	}
}
