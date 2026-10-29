package sessionmanager

import (
	"context"
	"strings"
	"sync"
)

// An invocation records only receipts actually returned by its own Submit.
// Reusing an older retained host receipt cannot acknowledge another revision.
// Closing cancels and joins all started calls before the binding can return.
type goalWriterScope struct {
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	active    bool
	pending   sync.WaitGroup
	receipts  map[InputReceipt]bool
	inputs    map[InputReceipt]InputSubmission
	authority string
}
type goalInvocationWriter struct {
	scope  *goalWriterScope
	parent HostInputWriter
}
type goalInvocationNativeWriter struct {
	*goalInvocationWriter
	observer NativeGoalObserver
}

func newGoalWriterScope(ctx context.Context, parent HostInputWriter, envelope *GoalEnvelope, phase string) (*goalWriterScope, HostInputWriter) {
	life, cancel := context.WithCancel(ctx)
	s := &goalWriterScope{ctx: life, cancel: cancel, active: true, receipts: map[InputReceipt]bool{}, inputs: map[InputReceipt]InputSubmission{}, authority: goalInputAuthority(envelope, phase)}
	return s, s.wrap(parent)
}
func (s *goalWriterScope) wrap(parent HostInputWriter) HostInputWriter {
	base := &goalInvocationWriter{scope: s, parent: parent}
	if observer, ok := parent.(NativeGoalObserver); ok {
		return &goalInvocationNativeWriter{goalInvocationWriter: base, observer: observer}
	}
	return base
}
func (s *goalWriterScope) close() {
	s.mu.Lock()
	s.active = false
	s.cancel()
	s.mu.Unlock()
	s.pending.Wait()
}
func (s *goalWriterScope) owns(receipt InputReceipt) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receipts[receipt]
}
func (s *goalWriterScope) begin(caller context.Context) (context.Context, func(), error) {
	s.mu.Lock()
	if !s.active {
		s.mu.Unlock()
		return nil, nil, ErrInputScopeClosed
	}
	s.pending.Add(1)
	s.mu.Unlock()
	ctx, cancel := context.WithCancel(caller)
	stop := context.AfterFunc(s.ctx, cancel)
	if s.ctx.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel(); s.pending.Done() }, nil
}
func (w *goalInvocationWriter) Submit(ctx context.Context, input InputSubmission) (InputReceipt, error) {
	call, end, err := w.scope.begin(ctx)
	if err != nil {
		return InputReceipt{}, err
	}
	defer end()
	if w.scope.authority == "" || !strings.HasPrefix(input.ID, w.scope.authority) {
		return InputReceipt{}, ErrInputIDConflict
	}
	// Retain independent D14 evidence expectations before calling any transport.
	proof := InputSubmission{Lease: input.Lease, ID: input.ID, Expected: input.Expected}
	if input.Payload.Text != nil {
		text := *input.Payload.Text
		proof.Payload.Text = &text
	}
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
		proof.Payload.ClaudeGoal = command
	}
	receipt, err := w.parent.Submit(call, input)
	if err == nil && (!strings.HasPrefix(receipt.ID, w.scope.authority) || receipt.SessionID != input.Lease.SessionID || receipt.Epoch != input.Lease.Epoch) {
		return InputReceipt{}, ErrInputReceiptUnknown
	}
	if err == nil {
		w.scope.mu.Lock()
		w.scope.receipts[receipt] = true
		w.scope.inputs[receipt] = proof
		w.scope.mu.Unlock()
	}
	return receipt, err
}
func (w *goalInvocationWriter) Await(ctx context.Context, receipt InputReceipt) (InputObservation, error) {
	call, end, err := w.scope.begin(ctx)
	if err != nil {
		return InputObservation{}, err
	}
	defer end()
	if !w.scope.owns(receipt) {
		return InputObservation{}, ErrInputReceiptUnknown
	}
	return w.parent.Await(call, receipt)
}
func (w *goalInvocationWriter) Cancel(ctx context.Context, receipt InputReceipt) error {
	call, end, err := w.scope.begin(ctx)
	if err != nil {
		return err
	}
	defer end()
	if !w.scope.owns(receipt) {
		return ErrInputReceiptUnknown
	}
	return w.parent.Cancel(call, receipt)
}
func (w *goalInvocationNativeWriter) WithNativeGoal(ctx context.Context, lease ControllerLease, decide func(NativeGoalObservation, HostInputWriter) error) error {
	if decide == nil {
		return ErrInvalidInput
	}
	call, end, err := w.scope.begin(ctx)
	if err != nil {
		return err
	}
	defer end()
	return w.observer.WithNativeGoal(call, lease, func(value NativeGoalObservation, parent HostInputWriter) error {
		return decide(value, w.scope.wrap(parent))
	})
}
