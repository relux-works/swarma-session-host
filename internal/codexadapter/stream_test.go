package codexadapter

import (
	"bytes"
	"testing"
)

// Arbitrary transport chunk boundaries do not emit handshake actions early.
// Partial frames close submission even if the preceding complete frame was idle.
func TestChunkedStreamAndPartialInput(t *testing.T) {
	s, err := NewStream(config())
	if err != nil {
		t.Fatal(err)
	}
	s.Initialize()
	b := []byte("{\"id\":1,\"result\":{\"userAgent\":\"synthetic\",\"platformFamily\":\"unix\",\"platformOs\":\"macos\"}}\n")
	for i, c := range b {
		out, err := s.Feed(uint64(i), []byte{c})
		if err != nil {
			t.Fatal(err)
		}
		if i < len(b)-1 && len(out) != 0 {
			t.Fatal("partial frame emitted actions")
		}
		if i == len(b)-1 && len(out) != 2 {
			t.Fatal("missing handshake actions")
		}
	}
	tail := []byte("{\"id\":2,\"result\":{\"thread\":{\"id\":\"thread-a\"},\"model\":\"unfamiliar-model\",\"modelProvider\":\"unfamiliar-provider\"}}\n")
	if _, err = s.Feed(uint64(len(b)), tail); err != nil {
		t.Fatal(err)
	}
	if s.Snapshot().State != Idle {
		t.Fatal(s.Snapshot())
	}
	if _, err = s.Feed(uint64(len(b)+len(tail)), []byte("{")); err != nil {
		t.Fatal(err)
	}
	if s.Snapshot().State != Unknown {
		t.Fatal("partial evidence exposed idle")
	}
	if _, err = s.Submit("reference"); err == nil {
		t.Fatal("partial evidence admitted input")
	}
}

// Both offset disagreement directions, exhausted bounds and EOF retain closed input;
// the exact contiguous initialized control is exercised alongside those failures.
func TestStreamLossClosesInput(t *testing.T) {
	for _, offset := range []uint64{0, 2} {
		s, _ := NewStream(config())
		s.Initialize()
		if _, err := s.Feed(0, []byte("{")); err != nil {
			t.Fatal(err)
		}
		if out, err := s.Feed(offset, []byte("}")); err == nil || len(out) != 0 {
			t.Fatal("offset violation admitted")
		}
		if s.Snapshot().State != Unknown {
			t.Fatal(s.Snapshot())
		}
		if _, err := s.Submit("retry"); err == nil {
			t.Fatal("gap retry")
		}
	}
	for _, chunk := range [][]byte{bytes.Repeat([]byte("x"), MaxFrame+1), bytes.Repeat([]byte("x"), MaxFrame)} {
		s, _ := NewStream(config())
		s.Initialize()
		_, err := s.Feed(0, chunk)
		if len(chunk) == MaxFrame {
			_, err = s.Feed(uint64(len(chunk)), []byte("x"))
		}
		if err == nil || s.Snapshot().State != Unknown {
			t.Fatal("bound not enforced")
		}
	}
	s, _ := NewStream(config())
	s.Initialize()
	s.Feed(0, []byte("{"))
	s.EOF()
	if out, err := s.Feed(1, []byte("}\n")); err == nil || len(out) != 0 {
		t.Fatal("EOF resumed")
	}
}
