package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrInvalidInput                 = errors.New("invalid host input submission")
	ErrInputIDConflict              = errors.New("input submission ID reused with different content")
	ErrInputReceiptUnknown          = errors.New("input receipt is unknown")
	ErrInputAcknowledgementMismatch = errors.New("provider acknowledgement does not match input")
	ErrInputScopeClosed             = errors.New("native goal observation scope has closed")
)

// This structural capability is implemented by host-owned provider transports.
// Its results use the accepted neutral observation and a read-only waiter, so
// a fork wrapper can preserve a transport without a package-private type token.
// The host stamps lease/time metadata; transport-supplied binding is not trusted.
// It is never supplied to a fork renderer. ExecuteInput arms observation before the
// write and returns only after owned writes/replays have quiesced, even on error.
// Its returned acknowledgement can only wait on locally observed evidence; it
// cannot perform a later write. Native write/read/replay completes in ExecuteInput;
// ordinary MSP calls use the host's private per-frame admission to release the
// arbiter during pure waits and re-enter for retries. D1 scopes remain exclusive.
// PTY output scanners can return an armed, read-only waiter.
type hostInputBackend interface {
	SupportsNativeGoals() bool
	AcceptsInput(InputSubmission) bool
	ExecuteInput(context.Context, InputSubmission, InputReceipt) (func(context.Context) (InputObservation, error), error)
	ObserveNativeGoal(context.Context) (NativeGoalObservation, error)
}

type inputAcknowledgement func(context.Context) (InputObservation, error)

func observedInput(observation InputObservation) inputAcknowledgement {
	return func(ctx context.Context) (InputObservation, error) {
		if err := ctx.Err(); err != nil {
			return InputObservation{}, err
		}
		return observation, nil
	}
}

type hostInputEntry struct {
	submission  InputSubmission
	receipt     InputReceipt
	done        chan struct{}
	cancel      context.CancelFunc
	cancelled   bool
	observation InputObservation
	err         error
}

// sessionInput owns one provider's writer fence. Epoch allocation/persistence
// belongs to the manager; construction requires its already-admitted epoch.
// No constructor or controller mutation is exposed to the renderer capability.
type sessionInput struct {
	lifeCtx          context.Context
	epochCtx         context.Context
	epochCancel      context.CancelFunc
	fence            chan struct{}
	mu               sync.Mutex
	sessionID        string
	epoch            ControllerEpoch
	rotating         bool
	beginIO          func(context.Context, ControllerLease) (func(), error)
	admission        func(context.Context) bool // optional only for injected test epochs; durable hosts set it
	backend          hostInputBackend
	codexSource      func(context.Context, ControllerLease) (CodexRunnerSnapshot, error)
	musePending      *hostMuseWaitScope
	museWaiting      []*hostMuseWaitScope
	museWaitChanged  chan struct{}
	observationAlive func(context.Context) error
	entries          map[string]*hostInputEntry
	commandIDs       map[string]string
	// Retry submission IDs bind to the original entry without creating a
	// second receipt, waiter or native operation.
	retryIDs map[string]string
	now      func() time.Time
}

var _ HostInputWriter = (*sessionInput)(nil)
var _ HostInputController = (*sessionInput)(nil)
var _ NativeGoalObserver = (*sessionInput)(nil)

func newSessionInput(lifeCtx context.Context, sessionID string, epoch ControllerEpoch, backend hostInputBackend) (*sessionInput, error) {
	if lifeCtx == nil || sessionID == "" || epoch == 0 || absentHostPort(backend) {
		return nil, ErrInvalidInput
	}
	h := &sessionInput{lifeCtx: lifeCtx, fence: make(chan struct{}, 1), sessionID: sessionID, epoch: epoch, backend: backend, entries: map[string]*hostInputEntry{}, commandIDs: map[string]string{}, now: func() time.Time { return time.Now().UTC() }}
	// The host already owns the frozen runner lifecycle capability. Keep it
	// private; a renderer receives only the read-only observation wrapper.
	if runner, ok := backend.(RunnerHost); ok {
		h.observationAlive = func(ctx context.Context) error {
			state, err := runner.State(ctx)
			if err != nil {
				return err
			}
			if state != ProviderHostRunning {
				return ErrMuseObservationClosed
			}
			return nil
		}
	} else if native, ok := backend.(*museInputBackend); ok {
		h.observationAlive = func(context.Context) error { return native.host.ensureRunning() }
	}
	h.epochCtx, h.epochCancel = context.WithCancel(lifeCtx)
	h.fence <- struct{}{}
	return h, nil
}

func (h *sessionInput) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.fence:
	}
	if err := ctx.Err(); err != nil {
		h.unlock()
		return err
	}
	return nil
}
func (h *sessionInput) unlock() { h.fence <- struct{}{} }

// A scope belongs to its host/outer scope and to its caller. Host cancellation
// is a parent cancellation, so it synchronously revokes a subsequent replay.
// Preserve the caller's deadline and its timeout classification as well.
func bindInputContext(lifetime, caller context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(lifetime)
	cancelDeadline := func() {}
	if deadline, ok := caller.Deadline(); ok {
		ctx, cancelDeadline = context.WithDeadline(ctx, deadline)
	}
	stop := context.AfterFunc(caller, func() {
		// The derived deadline already expires with DeadlineExceeded. Do not
		// race its timer with a cancellation that changes it to Canceled.
		if caller.Err() != context.DeadlineExceeded {
			cancel()
		}
	})
	if caller.Err() == context.Canceled {
		cancel()
	}
	return ctx, func() { stop(); cancelDeadline(); cancel() }
}

func (h *sessionInput) liveLeaseLocked(lease ControllerLease) bool {
	return h.lifeCtx.Err() == nil && !h.rotating && lease.SessionID == h.sessionID && lease.Epoch != 0 && lease.Epoch == h.epoch
}

// requireLease performs disk admission outside mu. A peer waiting on an
// exclusive file lock cannot hold the local state mutex across that wait.
func (h *sessionInput) requireLease(ctx context.Context, lease ControllerLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	live := h.liveLeaseLocked(lease)
	admission := h.admission
	h.mu.Unlock()
	if !live {
		return ErrStaleInputController
	}
	if admission != nil && !admission(ctx) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return ErrStaleInputController
	}
	h.mu.Lock()
	live = h.liveLeaseLocked(lease)
	h.mu.Unlock()
	if !live {
		return ErrStaleInputController
	}
	return ctx.Err()
}
func (h *sessionInput) CurrentLease(ctx context.Context, sessionID string) (ControllerLease, error) {
	h.mu.Lock()
	lease := ControllerLease{SessionID: sessionID, Epoch: h.epoch}
	h.mu.Unlock()
	if err := h.requireLease(ctx, lease); err != nil {
		return ControllerLease{}, err
	}
	return lease, nil
}

// A handoff first fences out new old-epoch work, then cancels pending waits and
// joins the old writer before publishing the manager's durably allocated epoch.
// A failed handoff retains the fence for recovery; it never resurrects leases.
func (h *sessionInput) advanceEpoch(ctx context.Context, epoch ControllerEpoch) error {
	h.mu.Lock()
	if epoch == 0 || epoch <= h.epoch {
		h.mu.Unlock()
		return ErrStaleInputController
	}
	h.rotating = true
	h.epochCancel()
	var cancels []context.CancelFunc
	var pending []<-chan struct{}
	for _, entry := range h.entries {
		if entry.cancel != nil {
			entry.cancelled = true
			cancels = append(cancels, entry.cancel)
			pending = append(pending, entry.done)
		}
	}
	h.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	if err := h.lock(ctx); err != nil {
		return err
	}
	defer h.unlock()
	if err := joinInputWaiters(ctx, pending); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if epoch <= h.epoch {
		return ErrStaleInputController
	}
	h.epoch = epoch
	h.epochCtx, h.epochCancel = context.WithCancel(h.lifeCtx)
	h.rotating = false
	return nil
}

// Revocation closes admission before cancellation and joins the physical
// writer. A reservation cannot become durable while old work can still replay.
func (h *sessionInput) revokeAndDrain(ctx context.Context) error {
	h.mu.Lock()
	h.rotating = true
	h.epochCancel()
	var cancels []context.CancelFunc
	var pending []<-chan struct{}
	for _, entry := range h.entries {
		if entry.cancel != nil {
			entry.cancelled = true
			cancels = append(cancels, entry.cancel)
			pending = append(pending, entry.done)
		}
	}
	h.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	if err := h.lock(ctx); err != nil {
		return err
	}
	h.unlock()
	return joinInputWaiters(ctx, pending)
}

func joinInputWaiters(ctx context.Context, pending []<-chan struct{}) error {
	for _, done := range pending {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (h *sessionInput) duplicate(input InputSubmission) (InputReceipt, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.liveLeaseLocked(input.Lease) {
		return InputReceipt{}, false, ErrStaleInputController
	}
	if input.ID == "" {
		return InputReceipt{}, false, ErrInvalidInput
	}
	if originalID, exists := h.retryIDs[input.ID]; exists {
		entry := h.entries[originalID]
		retry := input
		retry.ID = originalID
		if entry == nil || !sameInputSubmission(entry.submission, retry) {
			return InputReceipt{}, false, ErrInputIDConflict
		}
		return entry.receipt, true, nil
	}
	if entry := h.entries[input.ID]; entry != nil {
		if !sameInputSubmission(entry.submission, input) {
			return InputReceipt{}, false, ErrInputIDConflict
		}
		return entry.receipt, true, nil
	}
	if input.Payload.MuseTurn != nil || input.Payload.CodexTurn != nil || input.Payload.ClaudeGoal != nil {
		if originalID, exists := h.commandIDs[inputCommandID(input)]; exists {
			entry := h.entries[originalID]
			retry := input
			retry.ID = originalID
			if entry == nil || !sameInputSubmission(entry.submission, retry) {
				return InputReceipt{}, false, ErrInputIDConflict
			}
			if h.retryIDs == nil {
				h.retryIDs = make(map[string]string)
			}
			h.retryIDs[input.ID] = originalID
			return entry.receipt, true, nil
		}
	}
	return InputReceipt{}, false, nil
}

func (h *sessionInput) Submit(ctx context.Context, input InputSubmission) (InputReceipt, error) {
	if err := h.requireLease(ctx, input.Lease); err != nil {
		return InputReceipt{}, err
	}
	input = copyInputSubmission(input)
	if receipt, found, err := h.duplicate(input); found || err != nil {
		return receipt, err
	}
	if err := h.lock(ctx); err != nil {
		return InputReceipt{}, err
	}
	type admission struct {
		receipt InputReceipt
		err     error
	}
	ready := make(chan admission, 1)
	// Transfer the held fence to the host worker. Submit returns its admitted
	// receipt before waiting for provider output: the caller may be the very
	// tool shell whose return lets the provider render that acknowledgement.
	// The worker retains each physical admission until its write quiesces;
	// an ordinary native response wait can release that admission earlier.
	go func() {
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(h.unlock) }
		defer release()
		admitted := false
		receipt, err := h.submitUnderFence(ctx, input, nil, func(receipt InputReceipt) {
			admitted = true
			ready <- admission{receipt: receipt}
		}, release)
		if !admitted {
			ready <- admission{receipt: receipt, err: err}
		}
	}()
	result := <-ready
	return result.receipt, result.err
}

func (h *sessionInput) validateInput(input InputSubmission, observed *NativeGoalObservation) error {
	h.mu.Lock()
	live := h.liveLeaseLocked(input.Lease)
	h.mu.Unlock()
	if !live {
		return ErrStaleInputController
	}
	if input.ID == "" {
		return ErrInvalidInput
	}
	if renderedInputVariants(input.Payload) != 1 {
		return ErrInvalidInput
	}
	if input.Expected.Kind != "written" && input.Expected.Kind != "provider-ack" && input.Expected.Kind != "claude-goal-ack" && input.Expected.Kind != "run-observed-after-snapshot" {
		return ErrInvalidInput
	}
	if input.Expected.Kind != "written" && input.Expected.Correlation == "" {
		return ErrInvalidInput
	}
	if input.Expected.Kind == "claude-goal-ack" && input.Payload.ClaudeGoal == nil || input.Expected.Kind == "run-observed-after-snapshot" && input.Payload.Text == nil {
		return ErrInvalidInput
	}
	if command := input.Payload.ClaudeGoal; command != nil {
		if err := validateClaudeGoalCommand(command); err != nil {
			return err
		}
		if input.Expected.Kind != "claude-goal-ack" || input.Expected.Correlation != command.CommandID {
			return ErrInvalidInput
		}
	}
	if !h.backend.AcceptsInput(input) {
		return ErrInvalidInput
	}
	if input.Payload.MuseGoal != nil {
		if !h.backend.SupportsNativeGoals() {
			return ErrInvalidInput
		}
		if err := validateMuseGoalCommand(input.Lease, observed, input.Payload.MuseGoal); err != nil {
			return err
		}
		if input.Expected.Kind != "provider-ack" || input.Expected.Correlation != input.Payload.MuseGoal.CommandID {
			return ErrInvalidInput
		}
	}
	if input.Payload.MuseTurn != nil {
		if err := validateMuseTurnCommand(input.Payload.MuseTurn); err != nil {
			return err
		}
		if input.Expected.Kind != "provider-ack" || input.Expected.Correlation != input.Payload.MuseTurn.CommandID {
			return ErrInvalidInput
		}
	}
	if command := input.Payload.CodexTurn; command != nil {
		if _, ok := h.backend.(RunnerHost); !ok {
			return ErrInvalidInput
		}
		if err := validateCodexTurnCommand(command); err != nil {
			return err
		}
		if input.Expected.Kind != "provider-ack" || input.Expected.Correlation != command.CommandID {
			return ErrInvalidInput
		}
	}
	return nil
}

func (h *sessionInput) submitUnderFence(ctx context.Context, input InputSubmission, observed *NativeGoalObservation, admitted func(InputReceipt), writesQuiesced func()) (InputReceipt, error) {
	if admitted == nil {
		var cancel context.CancelFunc
		ctx, cancel = bindInputContext(h.lifeCtx, ctx)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return InputReceipt{}, err
	}
	if err := h.requireLease(ctx, input.Lease); err != nil {
		return InputReceipt{}, err
	}
	if receipt, found, err := h.duplicate(input); found || err != nil {
		return receipt, err
	}
	if err := h.validateInput(input, observed); err != nil {
		return InputReceipt{}, err
	}
	releaseIO, err := h.acquireIO(ctx, input.Lease)
	if err != nil {
		return InputReceipt{}, err
	}
	defer releaseIO()
	if err := ctx.Err(); err != nil {
		return InputReceipt{}, err
	}
	if command := input.Payload.MuseGoal; command != nil && command.Pause != nil {
		// This is a real provider read under the same fence as the decision/write.
		// A provider change after this read still requires exact post-write evidence;
		// the backend must return uncertainty rather than manufacture an ack.
		readCtx, finishPending := h.beginMuseWaitScope(ctx, input.Lease)
		fresh, err := h.backend.ObserveNativeGoal(readCtx)
		finishPending()
		if err != nil {
			return InputReceipt{}, err
		}
		if fresh.Sequence != observed.Sequence || fresh.Objective != observed.Objective || fresh.Status != observed.Status {
			return InputReceipt{}, ErrNativeGoalChanged
		}
	}
	h.mu.Lock()
	if !h.liveLeaseLocked(input.Lease) {
		h.mu.Unlock()
		return InputReceipt{}, ErrStaleInputController
	}
	// A fast-path retry can bind this ID while acquireIO waits. Recheck
	// submission and alias ownership atomically with the new entry below.
	// Identical retries already joined above; a newly occupied ID conflicts.
	if _, alias := h.retryIDs[input.ID]; alias || h.entries[input.ID] != nil {
		h.mu.Unlock()
		return InputReceipt{}, ErrInputIDConflict
	}
	if commandID := inputCommandID(input); commandID != "" {
		if _, exists := h.commandIDs[commandID]; exists {
			h.mu.Unlock()
			return InputReceipt{}, ErrInputIDConflict
		}
	}
	receipt := InputReceipt{SessionID: h.sessionID, Epoch: h.epoch, ID: input.ID}
	operationParent := ctx
	if admitted != nil {
		// Once admitted, an ordinary/deferred submission belongs to the host,
		// not to the request connection that may close as Submit returns.
		operationParent = h.lifeCtx
	}
	operationCtx, cancel := context.WithCancel(operationParent)
	entry := &hostInputEntry{submission: copyInputSubmission(input), receipt: receipt, done: make(chan struct{}), cancel: cancel}
	h.entries[input.ID] = entry
	if commandID := inputCommandID(input); commandID != "" {
		h.commandIDs[commandID] = input.ID
	}
	h.mu.Unlock()
	if admitted != nil {
		admitted(receipt)
	}
	var writeCtx context.Context
	var finishPending func()
	if writesQuiesced != nil && (input.Payload.MuseTurn != nil || input.Payload.Text != nil) {
		writeCtx, finishPending = h.beginMuseOrdinaryWaitScope(operationCtx, input.Lease, func() {
			releaseIO()
			writesQuiesced()
		})
	} else {
		// Native observe/decision callbacks retain their stronger admission
		// through the entire write, checked response and callback completion.
		writeCtx, finishPending = h.beginMuseWaitScope(operationCtx, input.Lease)
	}
	acknowledge, writeErr := h.backend.ExecuteInput(writeCtx, copyInputSubmission(input), receipt)
	finishPending()
	releaseIO() // The returned acknowledgement waiter is read-only.
	if writesQuiesced != nil {
		// A deferred PTY scanner does no further writes. Ordinary input and
		// notices may proceed while it waits, as with the legacy deferred path.
		// A native observation scope supplies nil and keeps the outer fence
		// across its callback's decision, submit AND acknowledgement.
		writesQuiesced()
	}
	var observation InputObservation
	if writeErr == nil {
		if acknowledge == nil {
			writeErr = ErrInputAcknowledgementMismatch
		} else {
			observation, writeErr = acknowledge(operationCtx)
		}
	}
	// Read the native identity without holding the input mutex: Snapshot owns
	// its own runner lock. An unbound or moved native receipt fails closed.
	validCodex := writeErr == nil && validCodexTurnObservation(h.backend, input, observation)
	cancel()
	h.mu.Lock()
	switch {
	case !h.liveLeaseLocked(input.Lease):
		observation = InputObservation{Receipt: receipt, Kind: "stale-epoch"}
		writeErr = ErrStaleInputController
	case entry.cancelled:
		observation = InputObservation{Receipt: receipt, Kind: "cancelled"}
		writeErr = context.Canceled
	case errors.Is(writeErr, context.DeadlineExceeded):
		observation = InputObservation{Receipt: receipt, Kind: "timeout"}
	case errors.Is(writeErr, context.Canceled):
		observation = InputObservation{Receipt: receipt, Kind: "cancelled"}
	case errors.Is(writeErr, ErrMuseTurnAlreadyCompleted) && observation.Receipt == receipt && observation.Kind == "already-completed" && observation.Correlation == input.Expected.Correlation && validMuseTurnObservation(input, observation, true):
		// A typed no-turn outcome is retained as a refusal, never provider-ack.
	case writeErr != nil && input.Payload.CodexTurn != nil && observation.Kind == "codex-preflight-refused" && observation.Receipt == receipt && observation.Correlation == input.Payload.CodexTurn.CommandID && observation.Correlation == input.Expected.Correlation && observation.CodexTurn == nil && observation.MuseTurn == nil && observation.ClaudeGoal == nil && observation.PTYRun == nil && observation.EvidenceClass == "":
		// Preserve the native preflight no-write refusal, with no turn/ACK
		// evidence. Cancellation, timeout and lease revocation take precedence.
	case writeErr != nil:
		observation = InputObservation{Receipt: receipt}
	case observation.Receipt != receipt || observation.Kind != InputObservationKind(input.Expected.Kind) || observation.Correlation != input.Expected.Correlation || !validMuseTurnObservation(input, observation, false) || !validCodex || !ValidPTYInputObservation(input, observation):
		observation = InputObservation{Receipt: receipt}
		writeErr = ErrInputAcknowledgementMismatch
	}
	observation.ObservedAt = h.now()
	entry.observation = copyInputObservation(observation)
	entry.err = writeErr
	entry.cancel = nil
	close(entry.done)
	h.mu.Unlock()
	// Admission succeeded and has a retained identity in the host's receipt set.
	// Await reports success/error evidence; an identical retry never writes again.
	return receipt, nil
}

func (h *sessionInput) Await(ctx context.Context, receipt InputReceipt) (InputObservation, error) {
	h.mu.Lock()
	entry := h.entries[receipt.ID]
	if entry == nil || entry.receipt != receipt {
		h.mu.Unlock()
		return InputObservation{}, ErrInputReceiptUnknown
	}
	h.mu.Unlock()
	select {
	case <-ctx.Done():
		kind := InputObservationKind("cancelled")
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			kind = "timeout"
		}
		return InputObservation{Receipt: receipt, Kind: kind, ObservedAt: h.now()}, ctx.Err()
	case <-entry.done:
	}
	if err := h.requireLease(ctx, ControllerLease{SessionID: receipt.SessionID, Epoch: receipt.Epoch}); err != nil {
		return InputObservation{Receipt: receipt, Kind: "stale-epoch", ObservedAt: h.now()}, err
	}
	h.mu.Lock()
	observation, err := copyInputObservation(entry.observation), entry.err
	h.mu.Unlock()
	return observation, err
}

func (h *sessionInput) Cancel(ctx context.Context, receipt InputReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	entry := h.entries[receipt.ID]
	if entry == nil || entry.receipt != receipt {
		h.mu.Unlock()
		return ErrInputReceiptUnknown
	}
	cancel := entry.cancel
	if cancel != nil {
		entry.cancelled = true
	}
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return joinInputWaiters(ctx, []<-chan struct{}{entry.done})
}

func (h *sessionInput) WithNativeGoal(ctx context.Context, lease ControllerLease, decide func(NativeGoalObservation, HostInputWriter) error) error {
	if decide == nil || !h.backend.SupportsNativeGoals() {
		return ErrInvalidInput
	}
	ctx, cancel := bindInputContext(h.lifeCtx, ctx)
	defer cancel()
	if err := h.lock(ctx); err != nil {
		return err
	}
	defer h.unlock()
	h.mu.Lock()
	live := h.liveLeaseLocked(lease)
	h.mu.Unlock()
	if !live {
		return ErrStaleInputController
	}
	releaseIO, err := h.acquireIO(ctx, lease)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		releaseIO()
		return err
	}
	readCtx, finishPending := h.beginMuseWaitScope(ctx, lease)
	state, err := h.backend.ObserveNativeGoal(readCtx)
	finishPending()
	releaseIO()
	if err != nil {
		return err
	}
	if err := h.requireLease(ctx, lease); err != nil {
		return err
	}
	observed := NativeGoalObservation{SessionID: h.sessionID, Epoch: lease.Epoch, Objective: state.Objective, Status: state.Status, Sequence: state.Sequence, ObservedAt: h.now(), EffectiveApprovalMode: state.EffectiveApprovalMode, EffectiveModelID: state.EffectiveModelID, Cumulative: copyMuseNativeUsage(state.Cumulative)}
	if err := validateNativeGoalObservation(lease, observed); err != nil {
		return err
	}
	scopeObservation := observed
	scopeObservation.Cumulative = copyMuseNativeUsage(observed.Cumulative)
	scope := &scopedInputWriter{host: h, ctx: ctx, observation: scopeObservation, active: true}
	defer func() { scope.mu.Lock(); scope.active = false; scope.mu.Unlock() }()
	return decide(observed, scope)
}

type scopedInputWriter struct {
	mu          sync.Mutex
	host        *sessionInput
	ctx         context.Context
	observation NativeGoalObservation
	active      bool
}

// Holding the scope mutex through Submit also joins any callback-spawned write
// before the outer scope releases the actual writer fence.
func (s *scopedInputWriter) Submit(ctx context.Context, input InputSubmission) (InputReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return InputReceipt{}, ErrInputScopeClosed
	}
	if input.Lease.SessionID != s.observation.SessionID || input.Lease.Epoch != s.observation.Epoch {
		return InputReceipt{}, ErrStaleInputController
	}
	ctx, cancel := bindInputContext(s.ctx, ctx)
	defer cancel()
	return s.host.submitUnderFence(ctx, copyInputSubmission(input), &s.observation, nil, nil)
}
func (s *scopedInputWriter) Await(ctx context.Context, receipt InputReceipt) (InputObservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return InputObservation{}, ErrInputScopeClosed
	}
	ctx, cancel := bindInputContext(s.ctx, ctx)
	defer cancel()
	return s.host.Await(ctx, receipt)
}
func (s *scopedInputWriter) Cancel(ctx context.Context, receipt InputReceipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return ErrInputScopeClosed
	}
	return s.host.Cancel(ctx, receipt)
}

func sameOptional[T comparable](a, b *T) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
func sameInputSubmission(a, b InputSubmission) bool {
	if a.Lease != b.Lease || a.ID != b.ID || a.Expected != b.Expected || !sameOptional(a.Payload.Text, b.Payload.Text) {
		return false
	}
	ca, cb := a.Payload.ClaudeGoal, b.Payload.ClaudeGoal
	if ca == nil || cb == nil {
		if ca != nil || cb != nil {
			return false
		}
	} else if ca.CommandID != cb.CommandID || !sameOptional(ca.Set, cb.Set) || !sameOptional(ca.Clear, cb.Clear) {
		return false
	}
	ac, bc := a.Payload.MuseGoal, b.Payload.MuseGoal
	if ac == nil || bc == nil {
		if ac != nil || bc != nil {
			return false
		}
	} else if ac.CommandID != bc.CommandID || !sameOptional(ac.Set, bc.Set) || !sameOptional(ac.Edit, bc.Edit) || !sameOptional(ac.Pause, bc.Pause) || !sameOptional(ac.Clear, bc.Clear) {
		return false
	}
	ad, bd := a.Payload.CodexTurn, b.Payload.CodexTurn
	if ad == nil || bd == nil {
		if ad != nil || bd != nil {
			return false
		}
	} else if ad.CommandID != bd.CommandID || !sameOptional(ad.Start, bd.Start) {
		return false
	}
	at, bt := a.Payload.MuseTurn, b.Payload.MuseTurn
	if at == nil || bt == nil {
		return at == nil && bt == nil
	}
	return at.CommandID == bt.CommandID && sameOptional(at.Start, bt.Start) && sameOptional(at.Interrupt, bt.Interrupt) && sameOptional(at.Steer, bt.Steer)
}
func copyInputSubmission(input InputSubmission) InputSubmission {
	out := input
	if c := input.Payload.ClaudeGoal; c != nil {
		command := &ClaudeGoalCommand{CommandID: c.CommandID}
		if c.Set != nil {
			value := *c.Set
			command.Set = &value
		}
		if c.Clear != nil {
			value := *c.Clear
			command.Clear = &value
		}
		out.Payload.ClaudeGoal = command
	}
	if input.Payload.Text != nil {
		text := *input.Payload.Text
		out.Payload.Text = &text
	}
	if c := input.Payload.MuseGoal; c != nil {
		command := &MuseGoalCommand{CommandID: c.CommandID}
		if c.Set != nil {
			command.Set = &MuseGoalSet{Objective: c.Set.Objective}
		}
		if c.Edit != nil {
			command.Edit = &MuseGoalEdit{Objective: c.Edit.Objective}
		}
		if c.Pause != nil {
			command.Pause = &MuseGoalPause{Sequence: c.Pause.Sequence}
		}
		if c.Clear != nil {
			command.Clear = &MuseGoalClear{}
		}
		out.Payload.MuseGoal = command
	}
	if c := input.Payload.MuseTurn; c != nil {
		command := &MuseTurnCommand{CommandID: c.CommandID}
		if c.Start != nil {
			command.Start = &MuseTurnStart{Text: c.Start.Text, IfBusy: c.Start.IfBusy}
		}
		if c.Interrupt != nil {
			command.Interrupt = &MuseTurnInterrupt{TurnID: c.Interrupt.TurnID}
		}
		if c.Steer != nil {
			command.Steer = &MuseTurnSteer{Text: c.Steer.Text, ExpectedTurnID: c.Steer.ExpectedTurnID}
		}
		out.Payload.MuseTurn = command
	}
	if c := input.Payload.CodexTurn; c != nil {
		command := &CodexTurnCommand{CommandID: c.CommandID}
		if c.Start != nil {
			command.Start = &CodexTurnStart{Text: c.Start.Text}
		}
		out.Payload.CodexTurn = command
	}
	return out
}

func (h *sessionInput) writeTerminal(ctx context.Context, lease ControllerLease, data []byte, write func([]byte) (int, error)) (int, error) {
	if write == nil {
		return 0, fmt.Errorf("terminal writer is unavailable")
	}
	owned := append([]byte(nil), data...)
	if err := h.lock(ctx); err != nil {
		return 0, err
	}
	defer h.unlock()
	h.mu.Lock()
	live := h.liveLeaseLocked(lease)
	h.mu.Unlock()
	if !live {
		return 0, ErrStaleInputController
	}
	releaseIO, err := h.acquireIO(ctx, lease)
	if err != nil {
		return 0, err
	}
	defer releaseIO()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	h.mu.Lock()
	live = h.liveLeaseLocked(lease)
	h.mu.Unlock()
	if !live {
		return 0, ErrStaleInputController
	}
	return write(owned)
}

func (h *sessionInput) acquireIO(ctx context.Context, lease ControllerLease) (func(), error) {
	if h.beginIO == nil {
		return func() {}, nil
	}
	release, err := h.beginIO(ctx, lease)
	if err != nil {
		return nil, err
	}
	// Several error/acknowledgement paths join this same release.
	var once sync.Once
	return func() { once.Do(release) }, nil
}
