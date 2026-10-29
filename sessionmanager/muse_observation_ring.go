package sessionmanager

import (
	"context"
	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
	"math"
	"sync"
)

const museObservationCapacity = 256

// One ring belongs to one admitted session/epoch. Subscribers own only a cursor
// and cancellation channel; neither subscribers nor notification producers can
// allocate an unbounded pending-event queue.
type museObservationRing struct {
	mu                sync.Mutex
	lease             ControllerLease
	nativeID          string
	requestedApproval string
	requestedModel    string
	values            [museObservationCapacity]MuseObservation
	head, size        int
	last              uint64
	ready             bool
	foldCursor        string
	restoreUsageTotal int64 // Actual served cumulative total survives ring eviction.
	terminal          error
	changed           chan struct{}
}

func newMuseObservationRing(lease ControllerLease, nativeID string) *museObservationRing {
	return &museObservationRing{lease: lease, nativeID: nativeID, changed: make(chan struct{})}
}
func (r *museObservationRing) signalLocked() { close(r.changed); r.changed = make(chan struct{}) }
func (r *museObservationRing) close(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal == nil {
		if err == nil {
			err = ErrMuseObservationClosed
		}
		r.terminal = err
		r.signalLocked()
	}
}
func (r *museObservationRing) appendLocked(event MSPNotification, gap MuseObservationGapReason) {
	value := MuseObservation{Event: event}
	if gap != "" {
		value.Gap = &MuseObservationGap{Reason: gap}
	}
	r.appendValueLocked(value)
}
func (r *museObservationRing) appendValueLocked(value MuseObservation) {
	if r.terminal != nil {
		return
	}
	if r.last == math.MaxUint64 {
		r.terminal = ErrMuseObservationUncertain
		r.signalLocked()
		return
	}
	r.last++
	value.Lease, value.Sequence = r.lease, r.last
	if value.Gap != nil {
		value.Gap.FromSequence, value.Gap.ThroughSequence = r.last, r.last
	}
	slot := (r.head + r.size) % len(r.values)
	if r.size == len(r.values) {
		slot = r.head
		r.head = (r.head + 1) % len(r.values)
	} else {
		r.size++
	}
	r.values[slot] = value
	r.signalLocked()
}
func (r *museObservationRing) publish(event MSPNotification) {
	if raw, ok := event.(MSPRawNotification); ok && !museNotificationRequiresParamsObject(raw.Name) {
		return
	}
	owned, nativeID, err := copyMuseNotification(event)
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.ready || r.terminal != nil {
		return
	}
	// Other valid sessions do not consume this session's order sequence. Missing
	// attribution on a raw unknown/ignored method is likewise not own evidence.
	if err != nil {
		r.terminal = ErrMuseObservationInvalid
		r.signalLocked()
		return
	}
	if nativeID != r.nativeID {
		return
	}
	if cursor := museDurableObservationCursor(owned); cursor != "" {
		repeated := cursor == r.foldCursor
		for i := 0; i < r.size && !repeated; i++ {
			prior := r.values[(r.head+i)%len(r.values)]
			repeated = prior.Event != nil && museDurableObservationCursor(prior.Event) == cursor
		}
		if repeated {
			r.appendLocked(nil, "restore-incomplete")
			return
		}
	}
	r.appendLocked(owned, "")
}
func (r *museObservationRing) subscribe(ctx context.Context, lease ControllerLease) (MuseObservationFeed, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lease != r.lease {
		return nil, ErrStaleInputController
	}
	if r.terminal != nil {
		return nil, r.terminal
	}
	life, cancel := context.WithCancel(ctx)
	return &museRingFeed{ring: r, ctx: life, cancel: cancel}, nil
}

type museRingFeed struct {
	ring   *museObservationRing
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	cursor uint64
}

func (f *museRingFeed) Close() { f.cancel() }
func (f *museRingFeed) Next(ctx context.Context) (MuseObservation, error) {
	if err := historyguard.LockMutex(ctx, &f.mu); err != nil {
		return MuseObservation{}, err
	}
	defer f.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return MuseObservation{}, err
		}
		if f.ctx.Err() != nil {
			return MuseObservation{}, ErrMuseObservationClosed
		}
		r := f.ring
		r.mu.Lock()
		if r.terminal != nil {
			err := r.terminal
			r.mu.Unlock()
			return MuseObservation{}, err
		}
		if r.ready && r.size > 0 {
			first := r.values[r.head]
			from := first.Sequence
			if first.Gap != nil {
				from = first.Gap.FromSequence
			}
			if f.cursor < from-1 {
				through := from - 1
				v := MuseObservation{Lease: r.lease, Sequence: through, Gap: &MuseObservationGap{FromSequence: f.cursor + 1, ThroughSequence: through, Reason: "overflow"}}
				f.cursor = through
				r.mu.Unlock()
				return v, nil
			}
			for i := 0; i < r.size; i++ {
				v := r.values[(r.head+i)%len(r.values)]
				if v.Sequence > f.cursor {
					if v.Event != nil {
						event, _, err := copyMuseNotification(v.Event)
						if err != nil {
							r.mu.Unlock()
							return MuseObservation{}, err
						}
						v.Event = event
					}
					if v.RestoreFold != nil {
						fold, err := copyMuseRestoreFold(v.RestoreFold)
						if err != nil {
							r.mu.Unlock()
							return MuseObservation{}, err
						}
						v.RestoreFold = fold
					}
					if v.Gap != nil {
						v.Gap = &MuseObservationGap{FromSequence: v.Gap.FromSequence, ThroughSequence: v.Gap.ThroughSequence, Reason: v.Gap.Reason}
					}
					f.cursor = v.Sequence
					r.mu.Unlock()
					return v, nil
				}
			}
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return MuseObservation{}, ctx.Err()
		case <-f.ctx.Done():
			return MuseObservation{}, ErrMuseObservationClosed
		case <-changed:
		}
	}
}
