package sessionhost

import (
	"bytes"
	"container/list"
	"encoding/json"
	"strconv"
	"sync"
	"time"
)

const debugContract = "host-debug-attach/1"
const debugTailBytes = 8_388_608
const debugTailAge = 900 * time.Second
const debugFrameBytes = 65_536

// Pull one event at a time: no subscriber queue or per-view retained history.
// Metadata is conservatively charged in addition to the encoded envelope.
type debugTail struct {
	mu        sync.Mutex
	events    list.List
	used      int
	next      uint64
	lost      uint64
	cause     string
	epoch     uint64
	execution string
	now       func() time.Time
	closed    bool
	changed   chan struct{}
}
type debugEvent struct {
	data     []byte
	at       time.Time
	sequence uint64
}
type debugEnvelope struct {
	Contract  string          `json:"contract"`
	Kind      string          `json:"kind"`
	Epoch     uint64          `json:"epoch"`
	Sequence  uint64          `json:"sequence"`
	At        time.Time       `json:"observed_at"`
	Execution string          `json:"execution"`
	Thread    string          `json:"thread,omitempty"`
	Turn      string          `json:"turn,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}
type debugTailRead struct {
	Event                []byte
	Frontier, GapThrough uint64
	Cause                string
	Status               string
}

func (r *debugTail) evictLocked(cause string) {
	e := r.events.Front()
	if e == nil {
		return
	}
	v := e.Value.(*debugEvent)
	r.used -= len(v.data) + 128
	r.lost = v.sequence
	r.cause = cause
	clear(v.data)
	v.data = nil
	r.events.Remove(e)
}
func (r *debugTail) expireLocked() {
	now := r.now()
	for e := r.events.Front(); e != nil; e = r.events.Front() {
		if now.Sub(e.Value.(*debugEvent).at) < debugTailAge {
			break
		}
		r.evictLocked("age")
	}
}
func (r *debugTail) observe(frame []byte) {
	if len(frame) == 0 || len(frame) > debugFrameBytes || !json.Valid(frame) {
		return
	}
	kind, thread, turn, payload := debugProject(frame)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.expireLocked()
	r.next++
	at := r.now()
	data, err := json.Marshal(debugEnvelope{debugContract, kind, r.epoch, r.next, at.UTC(), r.execution, thread, turn, payload})
	if err != nil {
		return
	}
	encoded := make([]byte, len(data)+1)
	copy(encoded, data)
	encoded[len(data)] = '\n'
	data = encoded
	for r.used+len(data)+128 > debugTailBytes {
		r.evictLocked("capacity")
	}
	r.events.PushBack(&debugEvent{data, at, r.next})
	r.used += len(data) + 128
	if r.changed != nil {
		select {
		case r.changed <- struct{}{}:
		default:
		}
	}
}

func (r *debugTail) expiryDelay() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.events.Front(); e != nil {
		return max(0, e.Value.(*debugEvent).at.Add(debugTailAge).Sub(r.now()))
	}
	return debugTailAge
}
func (r *debugTail) read(after uint64) debugTailRead {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireLocked()
	out := debugTailRead{Frontier: r.next}
	if r.closed {
		out.Cause = "observer_closed"
		return out
	}
	if after < r.lost {
		out.GapThrough, out.Cause = r.lost, r.cause
		return out
	}
	if r.cause == "host_replaced" && after == 0 {
		out.Cause = r.cause
		r.cause = ""
		return out
	}
	for e := r.events.Front(); e != nil; e = e.Next() {
		v := e.Value.(*debugEvent)
		if v.sequence > after {
			out.Event = bytes.Clone(v.data)
			break
		}
	}
	return out
}
func (r *debugTail) expire() { r.mu.Lock(); defer r.mu.Unlock(); r.expireLocked() }
func (r *debugTail) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.events.Len() != 0 {
		r.evictLocked("observer_closed")
	}
	r.closed = true
}

// The inert terminal projection quotes all control and non-ASCII codepoints.
// The structured event remains available privately; never print raw payloads.
func debugRender(event []byte) []byte { return []byte(strconv.QuoteToASCII(string(event)) + "\n") }
