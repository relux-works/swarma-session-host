package sessionmanager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"syscall"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

var _ ProviderConditionalStop = (*codexRunner)(nil)

type codexIdleStop struct {
	generation uint64
	released   bool
	attempted  bool
	signal     ProviderStopSignal
	outcome    ProviderStopOutcome
	err        error
	done       chan struct{}
}

// Acquisition reads fresh native activity while serializing native writes and
// the activity gate. Its read client deliberately has no gate observer: the
// gate mutex spans the read and fence, and notifications are folded before the
// fence. It remains an owned RPC operation, cancelled and joined by Close.
func (r *codexRunner) AcquireIdleStop(ctx context.Context) (ProviderIdleStopLease, error) {
	call, finish, err := r.begin(ctx)
	if err != nil {
		return ProviderIdleStopLease{}, err
	}
	defer finish()
	if err := historyguard.LockMutex(call, &r.idleStopMu); err != nil {
		return ProviderIdleStopLease{}, err
	}
	defer r.idleStopMu.Unlock()
	r.mu.Lock()
	active := r.activeIdleStop != ""
	r.mu.Unlock()
	if active {
		return ProviderIdleStopLease{}, errCodexTurnFenced
	}
	if err := historyguard.LockMutex(call, &r.operation); err != nil {
		return ProviderIdleStopLease{}, err
	}
	defer r.operation.Unlock()
	g := r.gate
	if err := historyguard.LockMutex(call, &g.mu); err != nil {
		return ProviderIdleStopLease{}, err
	}
	defer g.mu.Unlock()
	r.mu.Lock()
	snapshot := copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &r.snapshot})
	uncertain := r.uncertain
	r.mu.Unlock()
	if g.fenced || uncertain {
		return ProviderIdleStopLease{}, errCodexTurnFenced
	}
	client, err := r.dialIdleStopRead(call, snapshot.Codex.URL)
	if err != nil {
		return ProviderIdleStopLease{}, err
	}
	defer r.releaseClient(client)
	raw, err := client.request(call, "thread/read", map[string]any{"threadId": snapshot.Codex.ThreadID, "includeTurns": true})
	if err != nil {
		return ProviderIdleStopLease{}, err
	}
	var response struct{ Thread codexThread }
	if json.Unmarshal(raw, &response) != nil || response.Thread.ID != snapshot.Codex.ThreadID {
		return ProviderIdleStopLease{}, ErrInputAcknowledgementMismatch
	}
	if err := validateCodexTurnHistory(response.Thread.Turns); err != nil {
		return ProviderIdleStopLease{}, err
	}
	if !codexIdleStatusConsistent(response.Thread.Status) {
		return ProviderIdleStopLease{}, errCodexTurnFenced
	}
	g.setStatusLocked(response.Thread.Status.Type)
	for _, turn := range response.Thread.Turns {
		switch turn.Status {
		case "completed", "interrupted", "failed":
			delete(g.turns, turn.ID)
		case "inProgress":
			g.trackTurnLocked(turn.ID)
		}
	}
	for _, notification := range client.drainNotifications() {
		g.observeLocked(codexGateMessage{Method: notification.Method, Params: notification.Params})
	}
	if err := call.Err(); err != nil {
		return ProviderIdleStopLease{}, err
	}
	if g.activityLocked().State != ProviderActivityIdle {
		return ProviderIdleStopLease{}, errCodexTurnFenced
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return ProviderIdleStopLease{}, err
	}
	lease := ProviderIdleStopLease{ID: hex.EncodeToString(nonce[:])}
	g.fenced = true
	g.fenceGeneration++
	r.mu.Lock()
	if r.idleStops == nil {
		r.idleStops = map[string]*codexIdleStop{}
	}
	r.idleStops[lease.ID] = &codexIdleStop{generation: g.fenceGeneration, done: make(chan struct{})}
	r.activeIdleStop = lease.ID
	r.mu.Unlock()
	return lease, nil
}

func (r *codexRunner) dialIdleStopRead(ctx context.Context, url string) (*codexRPCClient, error) {
	client, err := dialCodexRPC(ctx, url)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.retiring {
		r.mu.Unlock()
		client.close()
		return nil, ErrHostCapabilityUnavailable
	}
	r.clients[client] = struct{}{}
	r.mu.Unlock()
	if err := initializeCodexRunnerRPC(ctx, client); err != nil {
		r.releaseClient(client)
		return nil, err
	}
	return client, nil
}

// A retry joins the admitted attempt even if its own context was cancelled:
// only that attempt's joined actual result can describe its provider effect.
func (r *codexRunner) retainedIdleStop(lease ProviderIdleStopLease, signal ProviderStopSignal) (ProviderStopOutcome, error, bool) {
	r.mu.Lock()
	transaction := r.idleStops[lease.ID]
	if transaction == nil || transaction.released || r.activeIdleStop != lease.ID {
		r.mu.Unlock()
		return ProviderStopRefused, ErrInvalidInput, true
	}
	if signal != ProviderStopSignalTerminate && signal != ProviderStopSignalKill {
		r.mu.Unlock()
		return ProviderStopRefused, ErrInvalidInput, true
	}
	if !transaction.attempted {
		r.mu.Unlock()
		return "", nil, false
	}
	if transaction.signal != signal {
		r.mu.Unlock()
		return ProviderStopRefused, ErrInputIDConflict, true
	}
	done := transaction.done
	r.mu.Unlock()
	if r.idleStopJoinObserved != nil {
		r.idleStopJoinObserved()
	}
	<-done
	r.mu.Lock()
	outcome, err := transaction.outcome, transaction.err
	r.mu.Unlock()
	return outcome, err, true
}

// SignalIdleStop retains the first admitted attempt's actual effect and error.
// Refusal never closes observation. Once a syscall succeeds, every later error
// retains signal-sent (or confirmed retired); it cannot become a rollback.
func (r *codexRunner) SignalIdleStop(ctx context.Context, lease ProviderIdleStopLease, signal ProviderStopSignal) (ProviderStopOutcome, error) {
	if outcome, err, handled := r.retainedIdleStop(lease, signal); handled {
		return outcome, err
	}
	if err := historyguard.LockMutex(ctx, &r.idleStopMu); err != nil {
		return ProviderStopRefused, err
	}
	defer r.idleStopMu.Unlock()
	if outcome, err, handled := r.retainedIdleStop(lease, signal); handled {
		return outcome, err
	}
	r.mu.Lock()
	transaction := r.idleStops[lease.ID]
	transaction.attempted = true
	transaction.signal = signal
	r.mu.Unlock()
	outcome, err := r.signalIdleStop(ctx, signal)
	r.mu.Lock()
	transaction.outcome, transaction.err = outcome, err
	close(transaction.done)
	r.mu.Unlock()
	return outcome, err
}
func (r *codexRunner) signalIdleStop(ctx context.Context, signal ProviderStopSignal) (ProviderStopOutcome, error) {
	if err := ctx.Err(); err != nil {
		return ProviderStopRefused, err
	}
	r.mu.Lock()
	retiring := r.retiring
	snapshot := copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &r.snapshot})
	r.mu.Unlock()
	if retiring {
		return ProviderStopRefused, ErrHostCapabilityUnavailable
	}
	observer, err := r.retirementObserver()
	if err != nil {
		return ProviderStopRefused, err
	}
	if state, err := observer.State(ctx); err != nil {
		return ProviderStopRefused, err
	} else if state == ProviderHostStopped {
		r.closeObservation()
		r.mu.Lock()
		r.snapshot.Stopped = true
		r.mu.Unlock()
		return ProviderStopRetired, nil
	}
	var identity *providerProcessIdentity
	if p := snapshot.Codex.ProcessIdentity; p != nil {
		identity = &providerProcessIdentity{PID: p.PID, UID: p.UID, Start: p.Start, PGID: p.PGID}
	}
	actual, err := verifyProviderProcessIdentity(snapshot.Codex.PID, identity)
	if err != nil {
		return ProviderStopRefused, err
	}
	if actual.PGID != snapshot.Codex.PID {
		return ProviderStopRefused, &PublicError{Code: ErrorSessionProcessIdentityUnverified, Message: "persisted provider is not its recorded process-group leader; no signal was sent"}
	}
	if err := ctx.Err(); err != nil {
		return ProviderStopRefused, err
	}
	nativeSignal := syscall.SIGTERM
	if signal == ProviderStopSignalKill {
		nativeSignal = syscall.SIGKILL
	}
	if err := syscall.Kill(-actual.PGID, nativeSignal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			if state, stateErr := observer.State(ctx); stateErr == nil && state == ProviderHostStopped {
				r.closeObservation()
				r.mu.Lock()
				r.snapshot.Stopped = true
				r.mu.Unlock()
				return ProviderStopRetired, nil
			}
		}
		return ProviderStopRefused, err
	}
	observer.mu.Lock()
	if signal == ProviderStopSignalKill {
		observer.signalled = true
	} else {
		observer.gracefulRequested = true
	}
	observer.mu.Unlock()
	// This method is not an observation operation: joining cannot wait on itself.
	r.closeObservation()
	// The fork must acknowledge its sent marker before waiting for exit. This
	// port reports the syscall effect; the existing WaitExit observes physical
	// retirement separately. A join crossing the deadline retains that effect.
	return ProviderStopSignalSent, ctx.Err()
}

// Exact release is idempotent, including an older already-released lease. Only
// its original unretired fence generation may be removed; retirement is final.
func (r *codexRunner) ReleaseIdleStop(ctx context.Context, lease ProviderIdleStopLease) error {
	if err := historyguard.LockMutex(ctx, &r.idleStopMu); err != nil {
		return err
	}
	defer r.idleStopMu.Unlock()
	r.mu.Lock()
	transaction := r.idleStops[lease.ID]
	if transaction == nil {
		r.mu.Unlock()
		return ErrInvalidInput
	}
	if transaction.released {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()
	g := r.gate
	if err := historyguard.LockMutex(ctx, &g.mu); err != nil {
		return err
	}
	defer g.mu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.retiring && g.fenceGeneration == transaction.generation {
		g.fenced = false
		g.fenceGeneration++
	}
	transaction.released = true
	if r.activeIdleStop == lease.ID {
		r.activeIdleStop = ""
	}
	return nil
}
