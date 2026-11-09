package codexadapter

import (
	"bytes"
	"testing"
)

func TestPrediscardPayloadViewSkeleton(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			s, err := NewStream(config())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Initialize(); err != nil {
				t.Fatal(err)
			}
			var captured [][]byte
			if enabled {
				s.debugObserver = func(frame []byte) { captured = append(captured, bytes.Clone(frame)) }
			}
			hello := []byte(`{"id":1,"result":{"userAgent":"synthetic","platformFamily":"unix","platformOs":"macos"}}` + "\n")
			if _, err := s.Feed(0, hello); err != nil {
				t.Fatal(err)
			}
			thread := []byte(`{"id":2,"result":{"thread":{"id":"thread"},"model":"unfamiliar-model","modelProvider":"unfamiliar-provider"}}` + "\n")
			// Use the test config's opaque strings without introducing native mappings.
			thread = bytes.ReplaceAll(thread, []byte("unfamiliar-model"), []byte(config().Model))
			thread = bytes.ReplaceAll(thread, []byte("unfamiliar-provider"), []byte(config().Provider))
			offset := uint64(len(hello))
			if _, err := s.Feed(offset, thread); err != nil {
				t.Fatal(err)
			}
			offset += uint64(len(thread))
			frame := []byte(`{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","text":"private-payload-canary"}}}` + "\n")
			half := len(frame) / 2
			count := len(captured)
			if _, err := s.Feed(offset, frame[:half]); err != nil {
				t.Fatal(err)
			}
			if len(captured) != count {
				t.Fatal("partial frame disclosed")
			}
			if _, err := s.Feed(offset+uint64(half), frame[half:]); err != nil {
				t.Fatal(err)
			}
			if enabled && (len(captured) != count+1 || !bytes.Contains(captured[len(captured)-1], []byte("private-payload-canary"))) {
				t.Fatal("payload discarded before tap")
			}
			if !enabled && len(captured) != 0 {
				t.Fatal("disabled capture")
			}
			checkpoint, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(checkpoint, []byte("private-payload-canary")) {
				t.Fatal("payload in checkpoint")
			}
		})
	}
}
func TestLegacyObserverAbsentGoldenSkeleton(t *testing.T) {
	s, err := NewStream(config())
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Initialize()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":1,"method":"initialize","params":{"clientInfo":{"name":"swarma_session_host","title":"Session Host","version":"0.1.0"}}}` + "\n"
	if string(got) != want {
		t.Fatalf("legacy initialize bytes changed: %s", got)
	}
	if s.debugObserver != nil {
		t.Fatal("default observer enabled")
	}
}
