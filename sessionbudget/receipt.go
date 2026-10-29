package sessionbudget

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"sync"
)

type deliveryTrace struct {
	mu            sync.Mutex
	lease         core.ControllerLease
	input         *core.InputSubmission
	receipt       *core.InputReceipt
	ack           *core.InputObservation
	native        *core.NativeGoalObservation
	onObservation func(core.NativeGoalObservation) error
	beforeSubmit  func(core.InputSubmission) error
	afterReceipt  func(core.InputReceipt) error
}

func (t *deliveryTrace) possibleEffect() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.input != nil
}
func (t *deliveryTrace) checked() (core.InputObservation, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.input == nil || t.receipt == nil || t.ack == nil {
		return core.InputObservation{}, core.ErrInputAcknowledgementMismatch
	}
	i, r, a := *t.input, *t.receipt, *t.ack
	if i.Lease != t.lease || r.SessionID != t.lease.SessionID || r.Epoch != t.lease.Epoch || r.ID != i.ID || r.ID == "" || a.Receipt != r || a.Kind != core.InputObservationKind(i.Expected.Kind) || a.Correlation == "" || a.Correlation != i.Expected.Correlation {
		return core.InputObservation{}, core.ErrInputAcknowledgementMismatch
	}
	if a.Kind != "provider-ack" && a.Kind != "claude-goal-ack" && a.Kind != "run-observed-after-snapshot" {
		return core.InputObservation{}, core.ErrInputAcknowledgementMismatch
	}
	if !core.ValidPTYInputObservation(i, a) {
		return core.InputObservation{}, core.ErrInputAcknowledgementMismatch
	}
	return a, nil
}

type tracedWriter struct {
	parent core.HostInputWriter
	trace  *deliveryTrace
}
type tracedObserver struct {
	*tracedWriter
	observer core.NativeGoalObserver
}

func traceWriter(parent core.HostInputWriter, trace *deliveryTrace) core.HostInputWriter {
	w := &tracedWriter{parent: parent, trace: trace}
	if o, ok := parent.(core.NativeGoalObserver); ok {
		return &tracedObserver{tracedWriter: w, observer: o}
	}
	return w
}
func (w *tracedWriter) Submit(ctx context.Context, i core.InputSubmission) (core.InputReceipt, error) {
	if i.Lease != w.trace.lease {
		return core.InputReceipt{}, core.ErrStaleInputController
	}
	if w.trace.beforeSubmit != nil {
		if e := w.trace.beforeSubmit(i); e != nil {
			return core.InputReceipt{}, e
		}
	}
	w.trace.mu.Lock()
	copy := i
	w.trace.input = &copy
	w.trace.mu.Unlock()
	r, e := w.parent.Submit(ctx, i)
	// A nonzero receipt is retained even when its caller also receives an error.
	if r.ID != "" {
		w.trace.mu.Lock()
		copy := r
		w.trace.receipt = &copy
		w.trace.mu.Unlock()
		if w.trace.afterReceipt != nil {
			if err := w.trace.afterReceipt(r); err != nil {
				return r, errors.Join(e, err)
			}
		}
	}
	return r, e
}
func (w *tracedWriter) Await(ctx context.Context, r core.InputReceipt) (core.InputObservation, error) {
	a, e := w.parent.Await(ctx, r)
	if e == nil {
		w.trace.mu.Lock()
		copy := a
		w.trace.ack = &copy
		w.trace.mu.Unlock()
	}
	return a, e
}
func (w *tracedWriter) Cancel(ctx context.Context, r core.InputReceipt) error {
	return w.parent.Cancel(ctx, r)
}
func (w *tracedObserver) WithNativeGoal(ctx context.Context, l core.ControllerLease, fn func(core.NativeGoalObservation, core.HostInputWriter) error) error {
	if l != w.trace.lease {
		return core.ErrStaleInputController
	}
	return w.observer.WithNativeGoal(ctx, l, func(o core.NativeGoalObservation, p core.HostInputWriter) error {
		if o.SessionID != l.SessionID || o.Epoch != l.Epoch || o.Sequence == 0 || o.ObservedAt.IsZero() {
			return core.ErrInputAcknowledgementMismatch
		}
		if w.trace.onObservation != nil {
			if e := w.trace.onObservation(o); e != nil {
				return e
			}
		}
		copy := o
		if o.Cumulative != nil {
			u := *o.Cumulative
			copy.Cumulative = &u
		}
		w.trace.mu.Lock()
		w.trace.native = &copy
		w.trace.mu.Unlock()
		return fn(o, &tracedWriter{parent: p, trace: w.trace})
	})
}
