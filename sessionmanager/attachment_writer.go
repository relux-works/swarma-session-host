package sessionmanager

import (
	"context"
	"errors"
	"sync"
)

// A provisional writer closes independently of a reused session's legitimate
// clients. Only its own submissions/receipts are cancelled and joined.
type attachmentWriter struct {
	mu          sync.Mutex
	life        context.Context
	cancel      context.CancelFunc
	parent      HostInputWriter
	active      bool
	pending     sync.WaitGroup
	receipts    map[InputReceipt]struct{}
	closeOnce   sync.Once
	closed      chan struct{}
	feedCancels []context.CancelFunc
	feeds       []MuseObservationFeed
}

func newAttachmentWriter(life context.Context, parent HostInputWriter) *attachmentWriter {
	ctx, cancel := context.WithCancel(life)
	return &attachmentWriter{life: ctx, cancel: cancel, parent: parent, active: true, receipts: map[InputReceipt]struct{}{}, closed: make(chan struct{})}
}
func (w *attachmentWriter) enter(ctx context.Context) (context.Context, func(), error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.active {
		return nil, nil, ErrInputScopeClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	w.pending.Add(1)
	call, cancel := bindInputContext(w.life, ctx)
	return call, func() { cancel(); w.pending.Done() }, nil
}
func (w *attachmentWriter) Submit(ctx context.Context, in InputSubmission) (InputReceipt, error) {
	call, end, err := w.enter(ctx)
	if err != nil {
		return InputReceipt{}, err
	}
	defer end()
	r, err := w.parent.Submit(call, in)
	if err == nil {
		w.mu.Lock()
		w.receipts[r] = struct{}{}
		w.mu.Unlock()
	}
	return r, err
}
func (w *attachmentWriter) Await(ctx context.Context, r InputReceipt) (InputObservation, error) {
	call, end, err := w.enter(ctx)
	if err != nil {
		return InputObservation{}, err
	}
	defer end()
	w.mu.Lock()
	_, own := w.receipts[r]
	w.mu.Unlock()
	if !own {
		return InputObservation{}, ErrInputReceiptUnknown
	}
	return w.parent.Await(call, r)
}
func (w *attachmentWriter) Cancel(ctx context.Context, r InputReceipt) error {
	call, end, err := w.enter(ctx)
	if err != nil {
		return err
	}
	defer end()
	w.mu.Lock()
	_, own := w.receipts[r]
	w.mu.Unlock()
	if !own {
		return ErrInputReceiptUnknown
	}
	return w.parent.Cancel(call, r)
}
func (w *attachmentWriter) close(ctx context.Context) error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.active = false
		w.cancel()
		w.mu.Unlock()
		go func() { w.pending.Wait(); close(w.closed) }()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.closed:
	}
	w.mu.Lock()
	receipts := make([]InputReceipt, 0, len(w.receipts))
	for r := range w.receipts {
		receipts = append(receipts, r)
	}
	w.mu.Unlock()
	var err error
	for _, r := range receipts {
		err = errors.Join(err, w.parent.Cancel(ctx, r))
	}
	return err
}

// Preserve the optional native observation scope without exporting a parent
// capability. Callback writes remain owned by this same provisional lifetime.
type attachmentNativeWriter struct{ *attachmentWriter }

// A previously admitted read-only feed remains tied to the same host epoch and
// subscriber lifetime after commit; it is not a lasting input controller.
// Abort/recovery additionally cancels and closes this preparation's readers.
func (w *attachmentNativeWriter) SubscribeMuse(ctx context.Context, l ControllerLease) (MuseObservationFeed, error) {
	w.mu.Lock()
	if !w.active {
		w.mu.Unlock()
		return nil, ErrInputScopeClosed
	}
	if err := ctx.Err(); err != nil {
		w.mu.Unlock()
		return nil, err
	}
	life, cancel := context.WithCancel(ctx)
	w.feedCancels = append(w.feedCancels, cancel)
	w.pending.Add(1)
	w.mu.Unlock()
	defer w.pending.Done()
	source, ok := w.parent.(MuseObservationSource)
	if !ok {
		cancel()
		return nil, ErrMuseObservationUnavailable
	}
	feed, err := source.SubscribeMuse(life, l)
	if err != nil {
		cancel()
		return nil, err
	}
	w.mu.Lock()
	w.feeds = append(w.feeds, feed)
	w.mu.Unlock()
	return feed, nil
}
func (w *attachmentWriter) abort(ctx context.Context) error {
	w.mu.Lock()
	w.active = false
	for _, cancel := range w.feedCancels {
		cancel()
	}
	w.mu.Unlock()
	err := w.close(ctx)
	if err != nil {
		return err
	}
	w.mu.Lock()
	feeds := append([]MuseObservationFeed(nil), w.feeds...)
	w.mu.Unlock()
	for _, feed := range feeds {
		feed.Close()
	}
	return nil
}

func (w *attachmentNativeWriter) WithNativeGoal(ctx context.Context, l ControllerLease, decide func(NativeGoalObservation, HostInputWriter) error) error {
	call, end, err := w.enter(ctx)
	if err != nil {
		return err
	}
	defer end()
	return w.parent.(NativeGoalObserver).WithNativeGoal(call, l, func(o NativeGoalObservation, parent HostInputWriter) error {
		child := newAttachmentWriter(call, parent)
		err := decide(o, child)
		child.mu.Lock()
		child.active = false
		child.cancel()
		child.mu.Unlock()
		child.pending.Wait()
		child.mu.Lock()
		w.mu.Lock()
		for r := range child.receipts {
			w.receipts[r] = struct{}{}
		}
		w.mu.Unlock()
		child.mu.Unlock()
		return err
	})
}
func (w *attachmentWriter) capability() HostInputWriter {
	if _, ok := w.parent.(NativeGoalObserver); ok {
		return &attachmentNativeWriter{w}
	}
	return w
}
