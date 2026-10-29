package sessionrun

import (
	"context"
	"sync"

	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// This provider queue holds delivery state and has no canonical authority, input or
// lifecycle capability. A queued directive is not a receipt; the renderer must
// check its actual host writer observation before calling received.
// Internal pending is not a wire result or a canonical success. The actual goal
// service must keep its journal pending until a checked native receipt resolves it.
var errPendingCodexGoal = errors.New("captured Codex goal delivery remains pending")

type codexGoalQueue struct {
	mu                           sync.Mutex
	ownerRun                     bool
	managedTurnID                string
	lockNativeRetry              func(context.Context) (func(), error)
	stopped                      bool
	pendingDirective             string
	pending, assignment, current *codexPendingGoal
	commands                     map[string]*codexPendingGoal
}

type codexPendingGoal struct {
	queue                *codexGoalQueue
	commandID, directive string
	envelope             *sm.GoalEnvelope
	done                 chan struct{}
	err                  error
	resolved             bool
	preflightRefused     bool
}

func newCodexGoalQueue(ownerRun bool, managedTurnID string) *codexGoalQueue {
	return &codexGoalQueue{ownerRun: ownerRun, managedTurnID: managedTurnID, commands: map[string]*codexPendingGoal{}}
}
func copyCodexGoalEnvelope(src *sm.GoalEnvelope) *sm.GoalEnvelope {
	if src == nil {
		return nil
	}
	out := &sm.GoalEnvelope{Owner: src.Owner, Clear: src.Clear}
	if src.Ref != nil {
		out.Ref = &sm.RevisionRef{ID: src.Ref.ID, Revision: src.Ref.Revision}
	}
	return out
}
func equalCodexGoalEnvelope(a, b *sm.GoalEnvelope) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Owner != b.Owner || a.Clear != b.Clear {
		return false
	}
	if a.Ref == nil || b.Ref == nil {
		return a.Ref == b.Ref
	}
	return *a.Ref == *b.Ref
}
func (q *codexGoalQueue) queue(ctx context.Context, commandID, directive string, envelope *sm.GoalEnvelope) (*codexPendingGoal, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if q.stopped {
		return nil, true, sm.ErrProviderHostNotFound
	}
	if previous := q.commands[commandID]; previous != nil {
		if previous.directive != directive || !equalCodexGoalEnvelope(previous.envelope, envelope) {
			return nil, true, sm.ErrInputIDConflict
		}
		if previous.resolved {
			// Only an explicitly proved no-write attempt may get a new waiter.
			// Old receipt history stays failed; unknown/post-write failures and
			// concurrent or already managed work remain fenced.
			retryNoWrite := previous.preflightRefused && previous.err == sm.ErrInputAcknowledgementMismatch && q.pending == nil && q.assignment == nil && q.current == nil && q.managedTurnID == ""
			if retryNoWrite && q.lockNativeRetry != nil {
				unlock, err := q.lockNativeRetry(ctx)
				if err != nil {
					return previous, true, err
				}
				defer unlock() // includes insertion below, never Submit/Await
			}
			if !retryNoWrite {
				if previous.err != nil || q.current == previous && q.pending == nil && q.assignment == nil {
					return previous, true, previous.err
				}
			}
		} else {
			return previous, true, errPendingCodexGoal
		}
	}
	if !q.ownerRun || q.managedTurnID != "" {
		return nil, false, nil
	}
	if q.pending != q.assignment {
		q.resolveLocked(q.pending, sm.ErrInputIDConflict)
	}
	pending := &codexPendingGoal{queue: q, commandID: commandID, directive: directive, envelope: copyCodexGoalEnvelope(envelope), done: make(chan struct{})}
	q.commands[commandID], q.pending = pending, pending
	q.pendingDirective = directive
	return pending, true, errPendingCodexGoal
}
func (q *codexGoalQueue) resolveLocked(p *codexPendingGoal, err error) {
	if p == nil || p.queue != q || p.resolved {
		return
	}
	p.preflightRefused = p.preflightRefused && err == sm.ErrInputAcknowledgementMismatch
	p.err, p.resolved = err, true
	if err == nil {
		q.current = p
	}
	close(p.done)
	if q.pending == p {
		q.pending = nil
		q.pendingDirective = ""
	}
}
func (p *codexPendingGoal) await(ctx context.Context) error {
	select {
	case <-ctx.Done():
		p.queue.revokeRefusal(p)
		return ctx.Err()
	case <-p.done:
		p.queue.mu.Lock()
		defer p.queue.mu.Unlock()
		if err := ctx.Err(); err != nil {
			p.preflightRefused = false
			return err
		}
		return p.err
	}
}
func (q *codexGoalQueue) cancel(p *codexPendingGoal) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if p != nil && p.queue == q {
		p.preflightRefused = false
	}
	q.resolveLocked(p, context.Canceled)
}
func (q *codexGoalQueue) directive() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pendingDirective
}
func (q *codexGoalQueue) capture() (string, *codexPendingGoal) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped || !q.ownerRun || q.managedTurnID != "" {
		return "", nil
	}
	q.assignment = q.pending
	if q.assignment != nil {
		q.current = nil
	}
	return q.pendingDirective, q.assignment
}

// The caller has already checked the real native ThreadID/TurnID receipt.
// A cancelled waiter remains cancelled even if the physical assignment won a
// race with cancellation; that old goal must never become acknowledged again.
func (q *codexGoalQueue) received(captured *codexPendingGoal, turnID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped || turnID == "" || q.assignment != captured || captured != nil && captured.queue != q {
		return false
	}
	q.managedTurnID = turnID
	q.resolveLocked(captured, nil)
	return true
}

// Called only after the renderer checks a native refusal against its actual
// leased writer receipt. It cannot release the waiter or acknowledge a goal.
func (q *codexGoalQueue) preflightRefusal(captured *codexPendingGoal) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.stopped && captured != nil && captured.queue == q && q.assignment == captured && !captured.resolved {
		captured.preflightRefused = true
	}
}

func (q *codexGoalQueue) finishAssignment(captured *codexPendingGoal) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.assignment != captured {
		return
	}
	q.resolveLocked(captured, sm.ErrInputAcknowledgementMismatch)
	q.resolveLocked(q.pending, sm.ErrInputAcknowledgementMismatch)
	q.assignment = nil
}
func (q *codexGoalQueue) invalidateCurrent() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.invalidateCurrentLocked()
}
func (q *codexGoalQueue) invalidateCurrentLocked() {
	q.current = nil
	for _, p := range q.commands {
		p.preflightRefused = false
	}
}
func (q *codexGoalQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
	for _, p := range q.commands {
		q.resolveLocked(p, context.Canceled)
	}
	q.assignment = nil
}

func (q *codexGoalQueue) cancelBeforeAssignment() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.ownerRun || q.managedTurnID != "" {
		return false
	}
	q.invalidateCurrentLocked()
	q.resolveLocked(q.pending, context.Canceled)
	return true
}
func (q *codexGoalQueue) managed() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.managedTurnID
}

func (q *codexGoalQueue) updateManaged(turnID string) {
	q.mu.Lock()
	q.managedTurnID = turnID
	q.mu.Unlock()
}

// Terminal cancellation/timeout cannot leave an older refusal retry-eligible.
// The original settled waiter/error is retained; only retry authority is revoked.
func (q *codexGoalQueue) revokeRefusal(p *codexPendingGoal) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if p != nil && p.queue == q {
		p.preflightRefused = false
	}
}
