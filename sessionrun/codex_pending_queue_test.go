package sessionrun

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// This pure provider queue is local, without an input or native capability. Exact
// identity retries share the same pending object; a changed body refuses. The
// actual assignment/host receipt must qualify success in separate integration
// tests: queuing alone has no acknowledgement and cannot promote a goal.
func TestForkCodexPendingQueueExactRetryAndNoEarlyAcknowledgement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newCodexGoalQueue(true, "")
		envelope := &sm.GoalEnvelope{Owner: "owner", Ref: &sm.RevisionRef{ID: "GOAL-A", Revision: 2}}
		p, handled, err := q.queue(context.Background(), "board-goal:GOAL-A@2", "directive", envelope)
		if !handled || !errors.Is(err, errPendingCodexGoal) || p == nil {
			t.Fatalf("pre-turn goal not deferred: %v / %v / %v", p, handled, err)
		}
		select {
		case <-p.done:
			t.Fatal("queue alone acknowledged before any assignment")
		default:
		}
		retry, handled, err := q.queue(context.Background(), "board-goal:GOAL-A@2", "directive", envelope)
		if !handled || !errors.Is(err, errPendingCodexGoal) || retry != p {
			t.Fatal("exact retry replaced original waiter")
		}
		if _, _, err := q.queue(context.Background(), "board-goal:GOAL-A@2", "changed", envelope); !errors.Is(err, sm.ErrInputIDConflict) {
			t.Fatalf("changed command body accepted: %v", err)
		}
		envelope.Ref.ID = "mutated"
		if p.envelope.Ref.ID != "GOAL-A" {
			t.Fatal("pending scope retained caller envelope pointer")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.await(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queue alone acknowledged: %v", err)
		}
		if q.directive() != "directive" {
			t.Fatal("pending directive lost before assignment")
		}
		q.cancel(p)
		if err := codexQueueAwait(t, p); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled queue waiter remained armed: %v", err)
		}
		if q.directive() != "" {
			t.Fatal("canceled directive persisted")
		}
	})
}

// Capturing a directive binds only its own waiter. A newer revision can queue
// during that assignment, but the old receipt cannot acknowledge it and every
// remaining waiter settles when the assignment returns.
func TestForkCodexPendingQueueCapturedReceiptDoesNotReleaseNewerGoal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newCodexGoalQueue(true, "")
		first, _, _ := q.queue(context.Background(), "first", "first directive", nil)
		text, captured := q.capture()
		if text != "first directive" || captured != first {
			t.Fatal("assignment captured another directive")
		}
		newer, _, err := q.queue(context.Background(), "newer", "new directive", nil)
		if !errors.Is(err, errPendingCodexGoal) {
			t.Fatal(err)
		}
		q.received(captured, "native-turn")
		if err := codexQueueAwait(t, first); err != nil {
			t.Fatalf("captured waiter not acknowledged: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := newer.await(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("old assignment receipt acknowledged newer directive: %v", err)
		}
		if q.directive() != "new directive" {
			t.Fatal("old receipt cleared newer pending directive")
		}
		q.finishAssignment(captured)
		if err := codexQueueAwait(t, newer); !errors.Is(err, sm.ErrInputAcknowledgementMismatch) {
			t.Fatalf("newer waiter left armed after assignment: %v", err)
		}
		if q.directive() != "" {
			t.Fatal("finished assignment retained newer pending text")
		}
	})
}

// Refusal/early return never acknowledges captured work. Superseding a pending
// directive fails its waiter, while the adjacent fresh captured receipt succeeds.
func TestForkCodexPendingQueueRefusalAndSupersessionSettle(t *testing.T) {
	q := newCodexGoalQueue(true, "")
	old, _, _ := q.queue(context.Background(), "old", "old", nil)
	next, _, _ := q.queue(context.Background(), "next", "next", nil)
	if err := codexQueueAwait(t, old); !errors.Is(err, sm.ErrInputIDConflict) {
		t.Fatalf("superseded waiter did not refuse: %v", err)
	}
	_, captured := q.capture()
	if captured != next {
		t.Fatal("supersession lost next waiter")
	}
	q.finishAssignment(captured)
	if err := codexQueueAwait(t, next); !errors.Is(err, sm.ErrInputAcknowledgementMismatch) {
		t.Fatalf("failed assignment falsely acknowledged: %v", err)
	}
	q2 := newCodexGoalQueue(true, "")
	valid, _, _ := q2.queue(context.Background(), "valid", "valid", nil)
	_, capture := q2.capture()
	q2.received(capture, "turn")
	q2.finishAssignment(capture)
	if err := codexQueueAwait(t, valid); err != nil {
		t.Fatalf("valid receipt refused: %v", err)
	}

	// On refusal with a newer queued goal, the captured and pending objects
	// differ. Each must settle independently; one resolver cannot mask the other.
	q3 := newCodexGoalQueue(true, "")
	capturedGoal, _, _ := q3.queue(context.Background(), "captured", "captured", nil)
	_, assigned := q3.capture()
	replacement, _, _ := q3.queue(context.Background(), "replacement", "replacement", nil)
	q3.finishAssignment(assigned)
	if err := codexQueueAwait(t, capturedGoal); !errors.Is(err, sm.ErrInputAcknowledgementMismatch) {
		t.Fatalf("failed captured assignment with newer waiter did not settle: %v", err)
	}
	if err := codexQueueAwait(t, replacement); !errors.Is(err, sm.ErrInputAcknowledgementMismatch) {
		t.Fatalf("failed newer assignment waiter did not settle: %v", err)
	}

}

// Resolved success is reusable only while it is the currently reflected
// directive. A cached receipt after rollback cannot suppress physical delivery
// of an older revision. Stop cancels every still-armed waiter and blocks queue.
func TestForkCodexPendingQueueRollbackInvalidatesCachedSuccess(t *testing.T) {
	q := newCodexGoalQueue(true, "")
	first, _, _ := q.queue(context.Background(), "first", "first", nil)
	_, captured := q.capture()
	q.received(captured, "turn")
	q.finishAssignment(captured)
	if err := codexQueueAwait(t, first); err != nil {
		t.Fatal(err)
	}
	if replay, handled, err := q.queue(context.Background(), "first", "first", nil); !handled || err != nil || replay != first {
		t.Fatalf("current exact success not idempotent: %v / %v / %v", replay, handled, err)
	}
	q.invalidateCurrent()
	if _, handled, err := q.queue(context.Background(), "first", "first", nil); handled || err != nil {
		t.Fatalf("old cached acknowledgement suppressed rollback delivery: %v / %v", handled, err)
	}
	q2 := newCodexGoalQueue(true, "")
	pending, _, _ := q2.queue(context.Background(), "pending", "pending", nil)
	q2.close()
	if err := codexQueueAwait(t, pending); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop left pending waiter: %v", err)
	}
	if _, _, err := q2.queue(context.Background(), "later", "later", nil); !errors.Is(err, sm.ErrProviderHostNotFound) {
		t.Fatalf("stopped queue admitted directive: %v", err)
	}
}

// Bound queue-only assertions so a missing settlement reports the exact claim
// rather than hanging the mutant process. No timeout is positive receipt proof.
func codexQueueAwait(t *testing.T, p *codexPendingGoal) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return p.await(ctx)
}
