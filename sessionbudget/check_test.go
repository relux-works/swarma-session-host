package sessionbudget

import (
	"context"
	"errors"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

// The run accessor reuses the actual bound observer/pause implementation. It
// cannot manufacture an account or bind a missing/replaced controller.
func TestBudgetCapturedCheckRequiresExistingLease(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	if e := h.factory.CheckBudget(ctx, h.lease); !errors.Is(e, ErrAccountingUnavailable) {
		t.Fatalf("missing binding: %v", e)
	}
	if e := h.set(t, "finite", 0, goal.Budget{Tokens: 10}); e != nil {
		t.Fatal(e)
	}
	h.native.usage(109)
	if e := h.factory.CheckBudget(ctx, h.lease); e != nil {
		t.Fatal(e)
	}
	if h.native.pauses != 0 {
		t.Fatal("below ceiling paused")
	}
	wrong := core.ControllerLease{SessionID: h.lease.SessionID, Epoch: h.lease.Epoch + 1}
	if e := h.factory.CheckBudget(ctx, wrong); !errors.Is(e, ErrAccountingUnavailable) {
		t.Fatalf("missing captured lease admitted: %v", e)
	}
	h.native.usage(110)
	if e := h.factory.CheckBudget(ctx, h.lease); e != nil {
		t.Fatal(e)
	}
	if h.native.pauses != 1 {
		t.Fatalf("actual threshold pause missing: %d", h.native.pauses)
	}
	if e := h.factory.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if e := h.factory.CheckBudget(ctx, h.lease); !errors.Is(e, core.ErrHostClosed) {
		t.Fatalf("closed accessor admitted: %v", e)
	}
	if h.native.pauses != 1 {
		t.Fatal("closed accessor wrote")
	}
}

// Restore validation reads the actual admitted journal, with no binding or
// goal mutation; a closed factory cannot provide a new validation source.
func TestBudgetAssignmentHeadAccessorUsesActualJournal(t *testing.T) {
	h := setup(t)
	if e := h.set(t, "head", 0, goal.Budget{Tokens: 17}); e != nil {
		t.Fatal(e)
	}
	head, e := h.factory.AssignmentHead(h.lease.SessionID)
	if e != nil {
		t.Fatal(e)
	}
	if head.Applied == nil || head.Latest.Ref != *head.Applied || head.Latest.Content.Budget.Tokens != 17 || head.AppliedReceipt == nil {
		t.Fatalf("actual applied journal lost: %+v", head)
	}
	if e = h.factory.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = h.factory.AssignmentHead(h.lease.SessionID); !errors.Is(e, core.ErrHostClosed) {
		t.Fatalf("closed journal accessor admitted: %v", e)
	}
}

// Closed native capability never calls delivery: cancellation, lease mismatch
// and lifetime closure keep their precise refusals without installing state.
func TestUnavailableNativeBudgetNeverInvokesDelivery(t *testing.T) {
	l := core.ControllerLease{SessionID: "session", Epoch: 2}
	for _, name := range []string{"unsupported", "cancelled", "wrong-lease", "closed"} {
		t.Run(name, func(t *testing.T) {
			life, cancelLife := context.WithCancel(context.Background())
			defer cancelLife()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			target := l
			expected := core.ErrHostCapabilityUnavailable
			switch name {
			case "cancelled":
				cancel()
				expected = context.Canceled
			case "wrong-lease":
				target.Epoch++
				expected = core.ErrStaleInputController
			case "closed":
				cancelLife()
				expected = core.ErrHostClosed
			}
			called := false
			e := (unavailableNativeBudget{lease: l, life: life}).WithBudget(ctx, target, goal.Revision{}, func() error { called = true; return nil })
			if !errors.Is(e, expected) || called {
				t.Fatalf("closed capability caused delivery or wrong refusal: %v called=%t", e, called)
			}
		})
	}
}
