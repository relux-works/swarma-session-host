package sessionrun

// Batch Q reviewer repro (rev4), not candidate source. It reuses the original
// 13ebd848 fixtures (retainedFeedNative, renderedTextWriter) from
// pty_binding_lifecycle_test.go, which must be present alongside this file.

import (
	"context"
	"path/filepath"
	"testing"

	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type reviewIdentityTextWriter struct{ *renderedTextWriter }

func (*reviewIdentityTextWriter) NativeIdentity() string { return "actual-physical-text" }

// Claim: through the composed Runtime.BindGoal router (the GoalBindings supplier
// that r31 installs with the SAME budget factory), a same-session transition
// from a native Muse observer to a non-native rendered-Text writer at a newer
// epoch must retire the superseded native observer and leave another session's
// observer alone. Positive control: the text binding itself must succeed.
// Limit: fixture native/text writers; no real Muse backend.
func TestReviewBatchQRuntimeTextTransitionRetiresSupersededObserver(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	service, err := goal.Open(filepath.Join(root, "goals"), &authority{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := budget.Open(filepath.Join(root, "budget"))
	if err != nil {
		t.Fatal(err)
	}
	factory, err := budget.NewFactory(ctx, service, store, accountingSource{"native"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := factory.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	runtime := New(store)
	if err = runtime.BindBudget(factory); err != nil {
		t.Fatal(err)
	}
	old := core.ControllerLease{SessionID: "runtime-text-transition", Epoch: 1}
	other := core.ControllerLease{SessionID: "runtime-other-session", Epoch: 1}
	oldNative := &retainedFeedNative{newNative(old), &feed{closed: make(chan struct{})}}
	otherNative := &retainedFeedNative{newNative(other), &feed{closed: make(chan struct{})}}
	for _, n := range []*retainedFeedNative{oldNative, otherNative} {
		if _, _, err = runtime.BindGoal(ctx, n.lease, "muse", n); err != nil {
			t.Fatal(err)
		}
	}
	next := core.ControllerLease{SessionID: old.SessionID, Epoch: 2}
	text := &reviewIdentityTextWriter{&renderedTextWriter{&ptyReceiptWriter{}}}
	if _, _, err = runtime.BindGoal(ctx, next, "muse", text); err != nil {
		t.Fatalf("positive control: runtime text binding refused: %v", err)
	}
	select {
	case <-otherNative.retained.closed:
		t.Fatal("runtime text binding retired unrelated native observer")
	default:
	}
	select {
	case <-oldNative.retained.closed:
	default:
		t.Fatal("runtime: superseded native observer remains active after text transition")
	}
}
