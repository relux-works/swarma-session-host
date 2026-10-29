package sessionmanager

import (
	"context"
	"errors"
	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
	"io"
	"math"
	"sync"
	"time"
)

var (
	ErrMuseObservationUnavailable = errors.New("Muse observation capability unavailable")
	ErrMuseObservationInvalid     = errors.New("invalid Muse observation evidence")
	ErrMuseObservationClosed      = errors.New("Muse observation feed closed")
	ErrMuseObservationUncertain   = errors.New("Muse observation history uncertain")
)

var _ MuseObservationSource = (*sessionInput)(nil)

// SubscribeMuse admits reads under the same durable controller fence as writes.
// The returned value deliberately has only the feed's two read/close methods.
func (h *sessionInput) SubscribeMuse(ctx context.Context, lease ControllerLease) (MuseObservationFeed, error) {
	h.mu.Lock()
	epochCtx := h.epochCtx
	h.mu.Unlock()
	life, cancel := bindInputContext(epochCtx, ctx)
	accepted := false
	defer func() {
		if !accepted {
			cancel()
		}
	}()
	ctx = life
	if err := h.requireMuseObservationLease(ctx, lease); err != nil {
		return nil, err
	}
	if err := h.lock(ctx); err != nil {
		return nil, err
	}
	defer h.unlock()
	release, err := h.acquireIO(ctx, lease)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := h.requireMuseObservationLease(ctx, lease); err != nil {
		return nil, err
	}
	source, ok := h.backend.(MuseObservationSource)
	if !ok {
		return nil, ErrMuseObservationUnavailable
	}
	native, ok := h.backend.(ProviderNativeIdentity)
	if !ok || native.NativeIdentity() == "" {
		return nil, ErrMuseObservationUnavailable
	}
	raw, err := source.SubscribeMuse(life, lease)
	if err != nil {
		cancel()
		return nil, err
	}
	if absentHostPort(raw) {
		cancel()
		return nil, ErrMuseObservationUnavailable
	}
	f := &hostMuseObservationFeed{host: h, lease: lease, nativeID: native.NativeIdentity(), raw: raw, ctx: life, cancel: cancel}
	// Revocation releases provider subscription resources even if Next is idle.
	f.stopClose = context.AfterFunc(life, raw.Close)
	if err := h.requireMuseObservationLease(ctx, lease); err != nil {
		f.Close()
		return nil, err
	}
	accepted = true
	return f, nil
}

type hostMuseObservationFeed struct {
	host      *sessionInput
	lease     ControllerLease
	nativeID  string
	raw       MuseObservationFeed
	ctx       context.Context
	cancel    context.CancelFunc
	stopClose func() bool
	once      sync.Once
	mu        sync.Mutex // One cursor, even for concurrent readers.
	last      uint64
	pending   *MuseObservation
}

func (f *hostMuseObservationFeed) Close() {
	f.once.Do(func() {
		f.cancel()
		if f.stopClose != nil {
			f.stopClose()
		}
		f.raw.Close()
	})
}

func (f *hostMuseObservationFeed) Next(ctx context.Context) (result MuseObservation, resultErr error) {
	publication, cancelPublication := bindInputContext(f.ctx, ctx)
	defer cancelPublication()
	defer func() { resultErr = f.readError(ctx, publication, resultErr) }()
	if err := historyguard.LockMutex(publication, &f.mu); err != nil {
		return MuseObservation{}, err
	}
	defer f.mu.Unlock()
	if err := f.host.requireMuseObservationLease(publication, f.lease); err != nil {
		return MuseObservation{}, err
	}
	if f.ctx.Err() != nil {
		return MuseObservation{}, ErrMuseObservationClosed
	}
	var value MuseObservation
	var err error
	if f.pending != nil {
		value = *f.pending
	} else {
		read, cancel := bindInputContext(publication, ctx)
		// Cross-process epoch changes have no local cancellation channel. Poll only
		// during a pending read, never while holding the physical writer fence.
		joined := make(chan struct{})
		go func() {
			defer close(joined)
			timer := time.NewTicker(20 * time.Millisecond)
			defer timer.Stop()
			for {
				select {
				case <-read.Done():
					return
				case <-timer.C:
					if f.host.requireMuseObservationLease(read, f.lease) != nil {
						cancel()
						return
					}
				}
			}
		}()
		value, err = f.raw.Next(read)
		cancel()
		<-joined
		if err == nil {
			f.pending = &value
		}
	}
	if leaseErr := f.host.requireMuseObservationLease(publication, f.lease); leaseErr != nil {
		return MuseObservation{}, leaseErr
	}
	if f.ctx.Err() != nil {
		return MuseObservation{}, ErrMuseObservationClosed
	}
	if err != nil {
		f.Close()
		if errors.Is(err, io.EOF) {
			return MuseObservation{}, ErrMuseObservationUncertain
		}
		return MuseObservation{}, err
	}
	// Publication is performed by the actual fence owner. During a native
	// response wait it can rendezvous with that worker without releasing the
	// fence; otherwise this reader acquires the ordinary durable admission.
	return f.publishMuseValue(publication, value)
}

// Cancellation classification must never re-enter durable admission: the very
// metadata lock that canceled the read may still be held by another process.
func (f *hostMuseObservationFeed) readError(caller, publication context.Context, err error) error {
	if !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) || publication.Err() == nil {
		return err
	}
	if caller.Err() != nil {
		return caller.Err()
	}
	f.host.mu.Lock()
	live := f.host.liveLeaseLocked(f.lease)
	f.host.mu.Unlock()
	if !live {
		return ErrStaleInputController
	}
	if f.ctx.Err() != nil {
		return ErrMuseObservationClosed
	}
	return err
}

func (f *hostMuseObservationFeed) validate(value MuseObservation) (MuseObservation, error) {
	variants := 0
	if value.Event != nil {
		variants++
	}
	if value.Gap != nil {
		variants++
	}
	if value.RestoreFold != nil {
		variants++
	}
	if value.Lease != f.lease || f.last == math.MaxUint64 || value.Sequence == 0 || variants != 1 {
		return MuseObservation{}, ErrMuseObservationInvalid
	}
	if gap := value.Gap; gap != nil {
		if gap.FromSequence != f.last+1 || gap.ThroughSequence < gap.FromSequence || value.Sequence != gap.ThroughSequence || (gap.Reason != "overflow" && gap.Reason != "restore-incomplete") {
			return MuseObservation{}, ErrMuseObservationInvalid
		}
		return MuseObservation{Sequence: gap.ThroughSequence, Gap: &MuseObservationGap{FromSequence: gap.FromSequence, ThroughSequence: gap.ThroughSequence, Reason: gap.Reason}}, nil
	}
	if value.Sequence != f.last+1 {
		return MuseObservation{}, ErrMuseObservationInvalid
	}
	if value.RestoreFold != nil {
		fold, err := copyMuseRestoreFold(value.RestoreFold)
		if err != nil || fold.SessionID != f.nativeID {
			return MuseObservation{}, ErrMuseObservationInvalid
		}
		return MuseObservation{Sequence: f.last + 1, RestoreFold: fold}, nil
	}
	event, nativeID, err := copyMuseNotification(value.Event)
	if err != nil || nativeID != f.nativeID {
		return MuseObservation{}, ErrMuseObservationInvalid
	}
	return MuseObservation{Sequence: f.last + 1, Event: event}, nil
}

// Lifecycle checks use the runner capability already held by the host; no
// writer, client or extra lifecycle port is attached to a returned feed.
func (h *sessionInput) requireMuseObservationLease(ctx context.Context, lease ControllerLease) error {
	if err := h.requireLease(ctx, lease); err != nil {
		return err
	}
	if h.observationAlive != nil {
		if err := h.observationAlive(ctx); err != nil {
			return err
		}
	}
	return h.requireLease(ctx, lease)
}
