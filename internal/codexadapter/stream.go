package codexadapter

import (
	"bytes"
	"errors"
	"sync"
)

// Stream frames one ordered output stream. Offsets are bytes, not message IDs.
// Gaps, rewinds, oversized chunks/lines and EOF close input permanently.
// Metadata restore requires explicit replay/catchup; it never authorizes writes.
type Stream struct {
	mu        sync.Mutex
	protocol  *Protocol
	next      uint64
	partial   []byte
	closed    bool
	replaying bool
}

func NewStream(cfg Config) (*Stream, error) {
	p, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return &Stream{protocol: p}, nil
}

func (s *Stream) Initialize() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.replaying {
		return nil, errors.New("stream_closed")
	}
	return s.protocol.Initialize()
}
func (s *Stream) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.protocol.Snapshot()
	if len(s.partial) != 0 || s.replaying {
		v.State = Unknown
		v.CompletedTurn = ""
	}
	return v
}
func (s *Stream) Submit(text string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.replaying || len(s.partial) != 0 {
		return nil, errors.New("stream_input_closed")
	}
	return s.protocol.Submit(text)
}

func (s *Stream) lost(code string) ([][]byte, error) {
	s.closed = true
	s.partial = nil
	s.protocol.Lost()
	return nil, errors.New(code)
}

// Feed returns only protocol-generated control frames. The caller must durably
// sequence them before transport writes; Feed itself performs no external I/O.
func (s *Stream) Feed(offset uint64, chunk []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("stream_closed")
	}
	if offset != s.next || len(chunk) > MaxFrame || uint64(len(chunk)) > ^uint64(0)-s.next {
		return s.lost("stream_offset_or_bound")
	}
	s.next += uint64(len(chunk))
	var out [][]byte
	for len(chunk) > 0 {
		n := bytes.IndexByte(chunk, '\n')
		take := len(chunk)
		if n >= 0 {
			take = n
		}
		if len(s.partial)+take > MaxFrame {
			return s.lost("stream_frame_bound")
		}
		s.partial = append(s.partial, chunk[:take]...)
		if n < 0 {
			break
		}
		frames, err := s.protocol.Consume(s.partial)
		s.partial = nil
		if err != nil {
			return s.lost("stream_protocol_refused")
		}
		out = append(out, frames...)
		chunk = chunk[take+1:]
	}
	return out, nil
}

func (s *Stream) EOF() { s.mu.Lock(); defer s.mu.Unlock(); s.lost("stream_eof") }

// ConfirmTail is called only after the host has read the runner's actual output
// tail. An offset declaration alone is not proof that the runner was contacted.
func (s *Stream) ConfirmTail(offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || offset != s.next || len(s.partial) != 0 {
		return errors.New("stream_tail_unconfirmed")
	}
	s.replaying = false
	return nil
}
