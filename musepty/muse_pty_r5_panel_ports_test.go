package musepty

// Round-4 panel probes, ported verbatim to production-entry tests. Each drove
// DeliverSessionNotice/RunOwner through the scripted PTY peer and FAILED on
// the r4 candidate (admitted delivery); all must refuse on r5.
// Sources:
//   - panel A (regression): TestR141* in regression_probes.go
//   - panel B (regression): TestReviewB* in review_probe_test.go
//   - delta (regression): TestDeltaUnknownDELRefuses in delta_sweep_test.go

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestR141UnknownProgressRowMustRefuse(t *testing.T) {
	for _, row := range []string{"◈ Future action required", "◈ Working (future mode)", "◈ Thinkng"} {
		t.Run(row, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			frame := "\x1b[1;1H\x1b[J" + row + "\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")
			h.mu.Lock()
			h.vt.observe([]byte(frame))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
}

func TestR141OrphanTaskStartedMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		payload, ok := record["payload"].(map[string]any)
		if !ok {
			continue
		}
		event, ok := payload["event"].(map[string]any)
		if !ok {
			continue
		}
		if record["payload_type"] == "runtime.session" && payload["kind"] == "task" && event["kind"] == "started" {
			payload["run_id"] = museTestUUID("unobserved-run")
			appendMuseRecord(t, h, museOwnedRecord(h, record))
			assertMuseNoInput(t, h, home)
			return
		}
	}
	t.Fatal("capture missing task started")
}

func TestR141EmptyTaskRunIDMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	for _, raw := range museCorpus(t) {
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		payload, ok := record["payload"].(map[string]any)
		if !ok {
			continue
		}
		event, ok := payload["event"].(map[string]any)
		if !ok {
			continue
		}
		if record["payload_type"] == "runtime.session" && payload["kind"] == "task" && event["kind"] == "started" {
			payload["run_id"] = ""
			appendMuseRecord(t, h, museOwnedRecord(h, record))
			assertMuseNoInput(t, h, home)
			return
		}
	}
	t.Fatal("capture missing task started")
}

func TestReviewBUnknownProgressMustRefuse(t *testing.T) {
	for _, row := range []string{"◆ Future decision surface", "◈ Waiting for remote approval", "◇ Working (future action)", "◆ Thinking"} {
		t.Run(row, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte("\x1b[1;1H\x1b[J" + row + "\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
}

func TestReviewBInvalidUTF8RecordMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	r := museFixtureRecord("session.opened.observed")
	r["id"] = "bad-byte-marker"
	b, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	b = []byte(strings.Replace(string(b), "bad-byte-marker", string([]byte{255}), 1))
	f, e := os.OpenFile(h.snapshot.LogPath, os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.Write(append(b, '\n'))
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	assertMuseNoInput(t, h, home)
}

func TestDeltaUnknownDELRefuses(t *testing.T) {
	for _, control := range []string{"\x7f", "\u009b"} {
		name := "DEL"
		if control != "\x7f" {
			name = "C1_CSI"
		}
		t.Run(name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			raw := strings.Replace(museTestScreen(""), "\x1b[1;1H\x1b[J", "\x1b[1;1H\x1b[J"+control+"\r\n", 1)
			h.vt.observe([]byte(raw))
			t.Logf("unknown=%v safeComposer=%v", h.vt.unknown, h.vt.safeComposer("", nil))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
}
