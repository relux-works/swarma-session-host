package sessionmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

// codexRunner owns only native process/protocol state. Canonical directives,
// owners, goal waiters and budgets never enter its typed snapshot or lifecycle.
type codexRunner struct {
	builderConfinement         *BuilderConfinementResult
	builderToolIsolation       *BuilderToolIsolationResult
	mu                         sync.Mutex
	operation                  sync.Mutex
	idleStopMu                 sync.Mutex
	idleStops                  map[string]*codexIdleStop
	activeIdleStop             string
	runMu                      sync.Mutex
	snapshot                   CodexRunnerSnapshot
	gate                       *codexTurnGate
	life                       context.Context
	cancel                     context.CancelFunc
	closeOnce                  sync.Once
	operations                 sync.WaitGroup
	retiring                   bool
	clients                    map[*codexRPCClient]struct{}
	commands                   map[string]*hostInputEntry
	uncertain                  bool
	ackTimeout                 time.Duration
	ownerClient                *codexRPCClient
	ownerStarted               bool
	monitorCancel              context.CancelFunc
	monitorDone                chan struct{}
	retirement                 *CodexRetirementRunner
	command                    *exec.Cmd
	exited                     <-chan struct{}
	consumerNotifications      []CodexConsumerNotification
	consumerEvidenceIncomplete bool
	ownerPollPeriod            time.Duration
	ownerTurnVisibilityLimit   time.Duration
	instructionSources         []string

	// Test-only observation of a validated retained-attempt join; nil in production.
	idleStopJoinObserved func()
}

var _ RunnerHost = (*codexRunner)(nil)
var _ hostInputBackend = (*codexRunner)(nil)

func newCodexRunner(snapshot CodexRunnerSnapshot) (*codexRunner, error) {
	if snapshot.Contract != codexHostSnapshotContract || snapshot.PID <= 0 || strings.TrimSpace(snapshot.URL) == "" || strings.TrimSpace(snapshot.ThreadID) == "" || !filepath.IsAbs(snapshot.ProjectDir) || !filepath.IsAbs(snapshot.CodexHome) {
		return nil, ErrInvalidInput
	}
	owned := copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &snapshot})
	life, cancel := context.WithCancel(context.Background())
	return &codexRunner{snapshot: *owned.Codex, gate: &codexTurnGate{threadID: snapshot.ThreadID}, life: life, cancel: cancel, clients: map[*codexRPCClient]struct{}{}, commands: map[string]*hostInputEntry{}, consumerEvidenceIncomplete: snapshot.ManagedTurnID != ""}, nil
}

// RestoreCodexRunner restores native observation only. The consumer must admit the
// exact committed typed image before calling it; retiring recovery uses the
// distinct no-RPC CodexRetirementRunner constructor instead.
func RestoreCodexRunner(ctx context.Context, snapshot CodexRunnerSnapshot) (RunnerHost, error) {
	r, err := newCodexRunner(snapshot)
	if err != nil {
		return nil, err
	}
	c, err := r.dialInitialized(ctx)
	if err == nil {
		var raw json.RawMessage
		raw, err = c.request(ctx, "thread/resume", map[string]any{"threadId": snapshot.ThreadID, "persistExtendedHistory": true})
		if err == nil {
			var response struct {
				Thread             codexThread `json:"thread"`
				InstructionSources []string    `json:"instructionSources"`
			}
			if json.Unmarshal(raw, &response) != nil || response.Thread.ID != snapshot.ThreadID {
				err = ErrInputAcknowledgementMismatch
			} else {
				r.instructionSources = append([]string(nil), response.InstructionSources...)
			}
		}
		r.releaseClient(c)
	}
	if err != nil {
		r.closeObservation()
		return nil, err
	}
	r.startAutoContinue()
	return r, nil
}
func (r *codexRunner) Snapshot() (RunnerSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &r.snapshot}), nil
}
func (r *codexRunner) State(ctx context.Context) (ProviderHostState, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.mu.Lock()
	snapshot := copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &r.snapshot})
	retirement := r.retirement
	r.mu.Unlock()
	if retirement == nil {
		var err error
		retirement, err = NewCodexRetirementRunner(*snapshot.Codex)
		if err != nil {
			return "", err
		}
		retirement.exited = r.exited
	}
	state, err := retirement.State(ctx)
	if err == nil {
		r.mu.Lock()
		if state == ProviderHostStopped || !r.retiring {
			r.snapshot.Stopped = state == ProviderHostStopped
		}
		r.mu.Unlock()
	}
	return state, err
}
func (r *codexRunner) SupportsNativeGoals() bool { return false }
func (r *codexRunner) ObserveNativeGoal(context.Context) (NativeGoalObservation, error) {
	return NativeGoalObservation{}, ErrInvalidInput
}
func (r *codexRunner) AcceptsInput(input InputSubmission) bool {
	return (input.Expected.Kind == "written" || input.Expected.Kind == "provider-ack") && renderedInputVariants(input.Payload) == 1 && (input.Payload.Text != nil || input.Payload.CodexTurn != nil)
}

func (r *codexRunner) begin(ctx context.Context) (context.Context, func(), error) {
	r.mu.Lock()
	if r.retiring || r.snapshot.Stopped {
		r.mu.Unlock()
		return nil, nil, ErrHostCapabilityUnavailable
	}
	r.operations.Add(1)
	r.mu.Unlock()
	call, cancel := bindInputContext(r.life, ctx)
	return call, func() { cancel(); r.operations.Done() }, nil
}
func (r *codexRunner) dialInitialized(ctx context.Context) (*codexRPCClient, error) {
	r.mu.Lock()
	url := r.snapshot.URL
	r.mu.Unlock()
	c, err := dialCodexRPC(ctx, url)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.retiring {
		r.mu.Unlock()
		c.close()
		return nil, ErrHostCapabilityUnavailable
	}
	r.clients[c] = struct{}{}
	r.mu.Unlock()
	c.gate = r.gate.client()
	if err := initializeCodexRunnerRPC(ctx, c); err != nil {
		r.releaseClient(c)
		return nil, err
	}
	return c, nil
}

func initializeCodexRunnerRPC(ctx context.Context, c *codexRPCClient) error {
	if _, err := c.request(ctx, "initialize", map[string]any{"clientInfo": map[string]any{"name": "task-board-session-manager", "title": "Task-board Session Manager", "version": "1"}, "capabilities": map[string]any{"experimentalApi": true}}); err != nil {
		return err
	}
	return c.notify(ctx, "initialized", map[string]any{})
}
func (r *codexRunner) releaseClient(c *codexRPCClient) {
	if c == nil {
		return
	}
	c.close()
	r.mu.Lock()
	delete(r.clients, c)
	r.mu.Unlock()
}

func (r *codexRunner) closeObservation() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.retiring = true
		monitorCancel, monitorDone := r.monitorCancel, r.monitorDone
		r.monitorCancel = nil
		r.mu.Unlock()
		if monitorCancel != nil {
			monitorCancel()
		}
		r.cancel()
		if monitorDone != nil {
			<-monitorDone
		}
		r.operations.Wait()
		r.mu.Lock()
		clients := make([]*codexRPCClient, 0, len(r.clients))
		for c := range r.clients {
			clients = append(clients, c)
		}
		r.ownerClient = nil
		r.mu.Unlock()
		for _, c := range clients {
			r.releaseClient(c)
		}
	})
}
func (r *codexRunner) retirementObserver() (*CodexRetirementRunner, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retirement == nil {
		observer, err := NewCodexRetirementRunner(r.snapshot)
		if err != nil {
			return nil, err
		}
		observer.exited = r.exited
		r.retirement = observer
	}
	return r.retirement, nil
}
func (r *codexRunner) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.closeObservation()
	observer, err := r.retirementObserver()
	if err != nil {
		return err
	}
	state, err := observer.State(ctx)
	if err != nil {
		return err
	}
	if state != ProviderHostStopped {
		if err := observer.Stop(ctx); err != nil {
			return err
		}
	}
	snapshot, err := observer.Snapshot()
	if err != nil {
		return err
	}
	r.mu.Lock()
	// Retirement changes physical exit evidence only. An earlier read-only
	// WaitExit may have captured the observer before the latest assignment.
	r.snapshot.Stopped = snapshot.Codex.Stopped
	r.mu.Unlock()
	return nil
}
func (r *codexRunner) RequestExit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.closeObservation()
	observer, err := r.retirementObserver()
	if err != nil {
		return err
	}
	return observer.RequestExit(ctx)
}
func (r *codexRunner) WaitExit(ctx context.Context) (bool, error) {
	observer, err := r.retirementObserver()
	if err != nil {
		return false, err
	}
	return observer.WaitExit(ctx)
}

func (r *codexRunner) ExecuteInput(ctx context.Context, input InputSubmission, receipt InputReceipt) (func(context.Context) (InputObservation, error), error) {
	if !r.AcceptsInput(input) {
		return nil, ErrInvalidInput
	}
	if receipt.ID != input.ID || receipt.SessionID != input.Lease.SessionID || receipt.Epoch != input.Lease.Epoch {
		return nil, ErrInputAcknowledgementMismatch
	}
	if input.Payload.CodexTurn != nil {
		if err := validateCodexTurnCommand(input.Payload.CodexTurn); err != nil {
			return nil, err
		}
		if input.Expected.Kind != "provider-ack" || input.Expected.Correlation != input.Payload.CodexTurn.CommandID {
			return nil, ErrInvalidInput
		}
	}
	call, finish, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if err := historyguard.LockMutex(call, &r.operation); err != nil {
		return nil, err
	}
	defer r.operation.Unlock()
	input = copyInputSubmission(input)
	if input.Payload.Text != nil {
		r.mu.Lock()
		uncertain := r.uncertain
		r.mu.Unlock()
		if uncertain {
			return nil, fmt.Errorf("%w: prior native Codex write has no exact receipt", ErrInputAcknowledgementMismatch)
		}
		c, err := r.dialInitialized(call)
		if err != nil {
			return nil, err
		}
		defer r.releaseClient(c)
		r.mu.Lock()
		thread := r.snapshot.ThreadID
		r.mu.Unlock()
		if err := deliverCodexThreadText(call, c, thread, *input.Payload.Text); err != nil {
			return nil, err
		}
		return observedInput(InputObservation{Receipt: receipt, Kind: InputObservationKind(input.Expected.Kind), Correlation: input.Expected.Correlation}), nil
	}
	commandID := input.Payload.CodexTurn.CommandID
	r.mu.Lock()
	if old := r.commands[commandID]; old != nil {
		retry := input
		retry.ID = old.submission.ID
		if !sameInputSubmission(old.submission, retry) {
			r.mu.Unlock()
			return nil, ErrInputIDConflict
		}
		r.mu.Unlock()
		return func(c context.Context) (InputObservation, error) {
			select {
			case <-c.Done():
				return InputObservation{}, c.Err()
			case <-old.done:
				return copyInputObservation(old.observation), old.err
			}
		}, nil
	}
	if r.uncertain {
		r.mu.Unlock()
		return nil, fmt.Errorf("%w: prior native Codex write has no exact receipt", ErrInputAcknowledgementMismatch)
	}
	entry := &hostInputEntry{submission: copyInputSubmission(input), receipt: receipt, done: make(chan struct{})}
	r.commands[commandID] = entry
	timeout := r.ackTimeout
	r.mu.Unlock()
	if timeout <= 0 {
		timeout = codexRPCRequestTimeout
	}
	bounded, cancel := context.WithTimeout(call, timeout)
	defer cancel()
	observation, wrote, err := r.executeTurn(bounded, input, receipt)
	if bounded.Err() != nil {
		err = bounded.Err()
	}
	// This is a refusal outcome, never a provider acknowledgement. Only the
	// native operation knows that no turn/start or turn/steer was attempted.
	// Keep its error and actual receipt through the read-only waiter so the
	// fork can retry an immutable obligation without guessing from error text.
	if err != nil && !wrote && bounded.Err() == nil {
		observation = InputObservation{Receipt: receipt, Kind: "codex-preflight-refused", Correlation: commandID}
	}
	r.mu.Lock()
	if err != nil && wrote {
		r.uncertain = true
	}
	entry.observation = copyInputObservation(observation)
	entry.err = err
	close(entry.done)
	r.mu.Unlock()
	if err != nil {
		if observation.Kind == "codex-preflight-refused" {
			return func(context.Context) (InputObservation, error) {
				return copyInputObservation(observation), err
			}, nil
		}
		return nil, err
	}
	return observedInput(copyInputObservation(observation)), nil
}

func (r *codexRunner) markConsumerEvidenceIncomplete() {
	r.mu.Lock()
	r.consumerEvidenceIncomplete = true
	r.mu.Unlock()
}
func (r *codexRunner) appendConsumerNotifications(batch []codexRPCNotification) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range batch {
		if len(r.consumerNotifications) >= codexConsumerNotificationLimit {
			r.consumerEvidenceIncomplete = true
			break
		}
		r.consumerNotifications = append(r.consumerNotifications, CodexConsumerNotification{Method: n.Method, Params: codexCloneRaw(n.Params)})
	}
}
func (r *codexRunner) ConsumerNotifications() ([]CodexConsumerNotification, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []CodexConsumerNotification
	for _, n := range r.consumerNotifications {
		out = append(out, CodexConsumerNotification{Method: n.Method, Params: codexCloneRaw(n.Params)})
	}
	return out, r.consumerEvidenceIncomplete
}
