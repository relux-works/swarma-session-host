package musepty

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestR141InvalidUTF8RecordMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	targetID, targetSeq := museResumeTarget(t, h)
	record := museFixtureRecord("session.opened.observed")
	museStampTestEnvelope(record)
	record["stream"].(map[string]any)["id"] = h.NativeIdentity()
	record["payload"].(map[string]any)["record"].(map[string]any)["session_id"] = h.NativeIdentity()
	record["payload"].(map[string]any)["record"].(map[string]any)["credential_backend"] = "UTF8_SENTINEL"
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "UTF8_SENTINEL", "\xff", 1))
	f, err := os.OpenFile(h.snapshot.LogPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write(append(raw, '\n'))
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	// The second opened reopens the session, so the captured resume
	// context witnesses it; the invalid encoding itself must still refuse.
	// The resumed reference resolves so the utf8-ff-only mutant, which
	// admits the encoding, must deliver through the resume.
	appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
	assertMuseNoInput(t, h, home)
}
