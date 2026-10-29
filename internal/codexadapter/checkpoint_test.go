package codexadapter

import (
	"bytes"
	"encoding/json"
	"testing"
)

func idleStream(t *testing.T) (*Stream, uint64) {
	t.Helper()
	s, err := NewStream(config())
	if err != nil {
		t.Fatal(err)
	}
	s.Initialize()
	a := []byte("{\"id\":1,\"result\":{\"userAgent\":\"synthetic\",\"platformFamily\":\"unix\",\"platformOs\":\"macos\"}}\n")
	b := []byte("{\"id\":2,\"result\":{\"thread\":{\"id\":\"thread-a\"},\"model\":\"unfamiliar-model\",\"modelProvider\":\"unfamiliar-provider\"}}\n")
	if _, err = s.Feed(0, a); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Feed(uint64(len(a)), b); err != nil {
		t.Fatal(err)
	}
	return s, uint64(len(a) + len(b))
}

// Metadata checkpoints omit partial raw output. Restored idle is closed until
// the actual tail is replayed and confirmed; restore emits no initialization.
func TestCheckpointReplayWithoutPrivateOutput(t *testing.T) {
	s, offset := idleStream(t)
	partial := []byte(`{"method":"item/agentMessage/delta","params":{"delta":"fabricated-private-canary`)
	if _, err := s.Feed(offset, partial); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(checkpoint, []byte("fabricated-private-canary")) {
		t.Fatal("raw output persisted")
	}
	restored, from, err := RestoreStream(config(), checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if from != offset {
		t.Fatal("partial bytes counted as observed")
	}
	if restored.Snapshot().State != Unknown {
		t.Fatal("restored without tail evidence")
	}
	if out, err := restored.Initialize(); err == nil || len(out) != 0 {
		t.Fatal("restore re-initialized")
	}
	if _, err := restored.Submit("retry"); err == nil {
		t.Fatal("restored idle opened before catchup")
	}
	if restored.ConfirmTail(offset+uint64(len(partial))) == nil {
		t.Fatal("unreplayed tail confirmed")
	}
	full := append(append([]byte(nil), partial...), []byte("\"}}\n")...)
	if out, err := restored.Feed(offset, full); err != nil || len(out) != 0 {
		t.Fatal("replay emitted control actions", err)
	}
	if restored.Snapshot().State != Unknown {
		t.Fatal("replay silently opened input")
	}
	if err := restored.ConfirmTail(offset + uint64(len(full))); err != nil {
		t.Fatal(err)
	}
	if restored.Snapshot().State != Idle || restored.Snapshot().Thread != "thread-a" {
		t.Fatal(restored.Snapshot())
	}
	if _, err := restored.Submit("next-reference"); err != nil {
		t.Fatal(err)
	}
}

// A pending turn cannot be submitted again after restore. Matching acceptance
// and completion can be observed without producing any duplicate write.
func TestRestorePendingTurn(t *testing.T) {
	s, offset := idleStream(t)
	if _, err := s.Submit("reference"); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	r, from, err := RestoreStream(config(), checkpoint)
	if err != nil || from != offset {
		t.Fatal(err)
	}
	if err := r.ConfirmTail(offset); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit("duplicate"); err == nil {
		t.Fatal("pending turn replayed")
	}
	a := []byte("{\"id\":3,\"result\":{\"turn\":{\"id\":\"turn-a\",\"status\":\"inProgress\"}}}\n")
	b := []byte("{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"thread-a\",\"turn\":{\"id\":\"turn-a\",\"status\":\"completed\"}}}\n")
	if out, err := r.Feed(offset, append(a, b...)); err != nil || len(out) != 0 {
		t.Fatal("observation produced duplicate write", err)
	}
	if r.Snapshot().CompletedTurn != "turn-a" {
		t.Fatal(r.Snapshot())
	}
}

// Changed selection, malformed counters/version and unknown metadata fail closed;
// a valid checkpoint provides the neighboring positive control.
func TestRejectInvalidCheckpoint(t *testing.T) {
	s, _ := idleStream(t)
	base, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RestoreStream(config(), base); err != nil {
		t.Fatal(err)
	}
	changed := config()
	changed.Model = "different-model"
	if _, _, err := RestoreStream(changed, base); err == nil {
		t.Fatal("changed plan adopted")
	}
	var record map[string]any
	if err := json.Unmarshal(base, &record); err != nil {
		t.Fatal(err)
	}
	for _, modify := range []func(map[string]any){func(x map[string]any) { x["version"] = 99 }, func(x map[string]any) { x["unexpected"] = "fabricated-private-canary" }, func(x map[string]any) { x["protocol"].(map[string]any)["phase"] = "ready-but-unverified" }, func(x map[string]any) { x["protocol"].(map[string]any)["pending"] = 99 }} {
		var copy map[string]any
		json.Unmarshal(base, &copy)
		modify(copy)
		bad, _ := json.Marshal(copy)
		if _, _, err := RestoreStream(config(), bad); err == nil {
			t.Fatal("invalid checkpoint adopted")
		}
	}
	for _, modify := range []func(map[string]any){func(x map[string]any) { delete(x, "offset") }, func(x map[string]any) { delete(x["protocol"].(map[string]any), "pending") }, func(x map[string]any) { x["closed"] = nil }} {
		var copy map[string]any
		json.Unmarshal(base, &copy)
		modify(copy)
		bad, _ := json.Marshal(copy)
		if _, _, err := RestoreStream(config(), bad); err == nil {
			t.Fatal("missing or null required metadata adopted")
		}
	}
}
