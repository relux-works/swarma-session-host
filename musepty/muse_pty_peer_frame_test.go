package musepty

import (
	"bytes"
	"strings"
	"testing"
)

// The delivery clock intentionally bypasses quiet time. Its composer-match
// wait must therefore observe the entire fake frame, including UTF-8 footer
// bytes and dialog state, before the final safety checks can run.
func TestMusePTYPeerPasteFramePublishesComposerLast(t *testing.T) {
	for _, tc := range []struct {
		name, mode, text string
		dialog           bool
	}{
		{"idle", "idle", "nonempty resume control", false},
		{"wrapped", "idle", strings.Repeat("long paste ", 25), false},
		{"multiline", "idle", "first line\nsecond line", false},
		{"permissions", "dialog-after-paste", "dialog control", true},
		{"trust", "trust", "trust control", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var frame bytes.Buffer
			musePeerFrame(&frame, tc.mode, tc.text, tc.dialog)
			v := newMuseVT(40, 120)
			data := frame.Bytes()
			// Force every possible PTY chunk boundary, including inside each
			// footer rune. No scheduler timing or test seam is required.
			for i, b := range data {
				v.observe([]byte{b})
				if i < len(data)-1 && v.composerMatches(tc.text) {
					t.Fatalf("composer published with %d frame bytes still in flight", len(data)-i-1)
				}
			}
			if !v.composerMatches(tc.text) || v.unknown || len(v.pending) != 0 {
				t.Fatal("completed frame must publish a known, matching composer")
			}
			if got := v.safeComposer(tc.text, &museLogFold{}); got == tc.dialog {
				t.Fatalf("completed frame safe=%v, dialog=%v", got, tc.dialog)
			}
		})
	}
}
