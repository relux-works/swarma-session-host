package musepty

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Baseline digests were measured at 2e9f866bb, before the 1.4.7 controls
// were admitted. Hash every byte's modeled screen, cursor, margins, pending
// bytes, title, unknown flag and replies, without persisting their contents.
func TestMusePTY141VTReplayUnchanged(t *testing.T) {
	want := map[string]string{
		"TestMusePTYReal141EchoSmoke-resume-variable-color.vt":    "979c2204778072707f985d7ae8755a57fb7dfa1b0c2a6864343bcdcb39a809c5",
		"TestMusePTYReal141EchoSmoke-resume.vt":                   "098573e6d9d6e65e3ebba49015eed37d6ca8f269f3db76e1fa149e89226709cf",
		"TestMusePTYRealCompletedTranscriptAndLiteralFollowup.vt": "cb9e2b5813fbba08cb860d205a3db8e01ae95d953cb163a620b963404542d1c9",
		"TestMusePTYRealLiteralRuleRoundTrip.vt":                  "157a67ef4f64815dfa1f15a075f9d393cb841b1c2e15da55c3e3dcd7ab16342c",
		"TestMusePTYRealPermissionsModalNeverPastes.vt":           "ddbe87bc843d229e09656c2a6cdf8cc7105122bd9943845c83a2ce98a5d2a9d7",
		"TestMusePTYRealQueueDrainRestoresNotice.vt":              "38e156d34e441611af97842df5f391f73aa33e5001c17e33dad1e3bd6425018e",
	}
	paths, err := filepath.Glob("testdata/muse-1.4.1/*.vt")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != len(want) {
		t.Fatalf("VT capture count=%d, want %d", len(paths), len(want))
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		v := newMuseVT(40, 120)
		h := sha256.New()
		for _, b := range raw {
			replies := v.observe([]byte{b})
			fmt.Fprintf(h, "%q|%d,%d,%d,%d,%d,%d|%q|%t|%q|%q\n", v.lines(), v.row, v.col, v.savedRow, v.savedCol, v.top, v.bottom, v.pending, v.unknown, v.title, replies)
		}
		if got := fmt.Sprintf("%x", h.Sum(nil)); got != want[filepath.Base(path)] {
			t.Errorf("%s replay digest=%s, want %s", filepath.Base(path), got, want[filepath.Base(path)])
		}
	}
}
