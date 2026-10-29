package sessionrun

import (
	"bytes"
	"context"
	"errors"
	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"testing"
)

type retainedFeedNative struct {
	*nativeFixture
	retained *feed
}

func (n *retainedFeedNative) SubscribeMuse(context.Context, core.ControllerLease) (core.MuseObservationFeed, error) {
	return n.retained, nil
}

type renderedTextWriter struct{ *ptyReceiptWriter }

func (*renderedTextWriter) SupportsNativeGoals() bool { return false }

// Switching only one session to a real text binding closes its superseded
// observation worker, preserves original accounting, and leaves another
// session's worker alone. Constructor refusal must not retire either worker.
func TestRenderedTextBindingRetiresOnlySupersededObserver(t *testing.T) {
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
	old := core.ControllerLease{SessionID: "text-transition", Epoch: 1}
	other := core.ControllerLease{SessionID: "other-session", Epoch: 1}
	oldNative := &retainedFeedNative{newNative(old), &feed{closed: make(chan struct{})}}
	otherNative := &retainedFeedNative{newNative(other), &feed{closed: make(chan struct{})}}
	for _, n := range []*retainedFeedNative{oldNative, otherNative} {
		if _, _, err = factory.BindGoal(ctx, n.lease, "muse", n); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadFile(filepath.Join(root, "budget", "accounting.json"))
	if err != nil {
		t.Fatal(err)
	}
	next := core.ControllerLease{SessionID: old.SessionID, Epoch: 2}
	text := &renderedTextWriter{&ptyReceiptWriter{}}
	if _, _, err = factory.BindRenderedTextGoal(ctx, next, "muse", text, nil); !errors.Is(err, budget.ErrAccountingUnavailable) {
		t.Fatalf("missing renderer not refused: %v", err)
	}
	select {
	case <-oldNative.retained.closed:
		t.Fatal("refused binding retired prior observer")
	default:
	}
	renderer, err := NewPTYDeliveryFactory("actual-physical-text")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = factory.BindRenderedTextGoal(ctx, next, "muse", text, renderer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldNative.retained.closed:
	default:
		t.Fatal("superseded native observer remains active")
	}
	select {
	case <-otherNative.retained.closed:
		t.Fatal("text binding retired unrelated native observer")
	default:
	}
	after, err := os.ReadFile(filepath.Join(root, "budget", "accounting.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("retirement reset original accounting: before%+v after%+v %v", before, after, err)
	}
	if _, err = store.LegacySnapshot(ctx, next); !errors.Is(err, core.ErrStaleInputController) {
		t.Fatalf("text binding invented a new accounting lease: %v", err)
	}
}
