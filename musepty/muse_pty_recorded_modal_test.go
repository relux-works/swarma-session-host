package musepty

import (
	"os"
	"strings"
	"testing"
	"time"
)

// Claim: the real permissions modal, replayed from its 1.4.1 recording over
// a known idle log, never passes the screen gate or the host idle
// predicate, so no notice can be pasted into it. Positive control: before
// the modal starts drawing, the same recording over the same log reaches at
// least one frame that both predicates admit, so the refusal comes from the
// modal and not from the replay or the log. This is the recorded-frame
// killer for live-permissions-modal-only; it needs no Muse binary
// (regression, option C). TestMusePTY141VTReplayUnchanged pins the
// recording's bytes; frames are found by content, never by offset.
func TestMusePTYRecordedPermissionsModalNeverAdmits(t *testing.T) {
	const logPath = "testdata/muse-1.4.7/session.jsonl"
	fold, err := readMuseLog(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !fold.idle() {
		t.Fatal("the gate must be isolated with a known idle log")
	}
	raw, err := os.ReadFile("testdata/muse-1.4.1/TestMusePTYRealPermissionsModalNeverPastes.vt")
	if err != nil {
		t.Fatal(err)
	}
	admitted := func(v *museVT) bool {
		lastOutput := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
		now := lastOutput.Add(musePTYQuiet)
		h := &musePTYHost{vt: v, exited: make(chan struct{}), snapshot: musePTYSnapshot{LogPath: logPath, SessionID: fold.SessionID, PID: fold.PID}, lastOutput: lastOutput, testFakeNow: &now}
		return v.safeComposer("", fold) && h.idle("")
	}
	// The slash-command menu and then the modal draw progressively: the
	// first row naming Permissions marks the start of the decision surface.
	begun := func(v *museVT) bool {
		for _, row := range v.lines() {
			if strings.HasPrefix(strings.TrimSpace(row), "Permissions") {
				return true
			}
		}
		return false
	}
	shown := func(v *museVT) bool {
		return strings.Contains(strings.ToLower(strings.Join(v.lines(), "\n")), "selections are saved as your default")
	}
	v := newMuseVT(40, 120)
	controls, modalFrames := 0, 0
	started := false
	for _, b := range raw {
		v.observe([]byte{b})
		if v.unknown {
			t.Fatal("the recording left the modeled VT grammar")
		}
		if len(v.pending) != 0 {
			continue
		}
		started = started || begun(v)
		switch {
		case !started:
			if admitted(v) {
				controls++
			}
		case shown(v):
			modalFrames++
			if admitted(v) {
				t.Fatalf("recorded permissions modal admitted:\n%s", strings.Join(v.lines(), "\n"))
			}
		}
	}
	if controls == 0 {
		t.Fatal("positive control: no frame before the modal was admitted")
	}
	if modalFrames == 0 {
		t.Fatal("the recording shows no complete permissions modal")
	}
}
