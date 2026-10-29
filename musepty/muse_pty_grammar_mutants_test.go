package musepty

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestMusePTYDuplicateFieldsRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	targetID, targetSeq := museResumeTarget(t, h)
	b, _ := json.Marshal(museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
	raw := strings.Replace(string(b), `"payload":`, `"payload":null,"payload":`, 1)
	f, err := os.OpenFile(h.snapshot.LogPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString(raw + "\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	// The second opened reopens the session, so the captured resume
	// context witnesses it; the duplicate field itself must still
	// refuse. The resumed reference resolves so the
	// duplicate-payload-only mutant, which admits the field, must
	// deliver through the resume.
	appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
	assertMuseNoInput(t, h, home)
}

func TestMusePTYIncompleteOpenedRecordRefuses(t *testing.T) {
	for _, variant := range []string{"missing_record", "unknown_record", "null_record", "missing_identity", "wrong_identity_type"} {
		t.Run(variant, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.end")))
			if f, err := h.fold(); err != nil || f.idle() {
				t.Fatalf("captured ended control: %+v %v", f, err)
			}
			targetID, targetSeq := museResumeTarget(t, h)
			record := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
			payload := record["payload"].(map[string]any)
			switch variant {
			case "missing_record":
				delete(payload, "record")
			case "unknown_record":
				payload["record"] = map[string]any{"future": true}
			case "null_record":
				payload["record"] = nil
			case "missing_identity":
				delete(payload["record"].(map[string]any), "session_id")
			case "wrong_identity_type":
				payload["record"].(map[string]any)["session_id"] = 123
			}
			appendMuseRecord(t, h, record)
			// The captured resume context follows the reopen: a mutant
			// that admits the malformed opened reaches idle through
			// it, while every variant still refuses at the grammar.
			// The resumed reference resolves to the prior epoch.
			appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
			assertMuseNoInput(t, h, home)
		})
	}
}
