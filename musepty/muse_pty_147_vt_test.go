package musepty

import (
	"fmt"
	"reflect"
	"testing"
)

func TestMusePTY147RawStartupVTReplay(t *testing.T) {
	raw := muse147Fixture(t, "startup-echo.raw")
	for _, size := range []int{1, len(raw)} {
		t.Run(fmt.Sprintf("chunk_%d", size), func(t *testing.T) {
			v := newMuseVT(40, 120)
			for at := 0; at < len(raw); at += size {
				v.observe(raw[at:min(at+size, len(raw))])
				if v.unknown {
					t.Fatalf("unknown VT at completion byte %d: %s", min(at+size, len(raw))-1, v.unknownKind)
				}
			}
			if len(v.pending) != 0 || !v.composerMatches("") {
				t.Fatalf("pending=%d composer_match=%t", len(v.pending), v.composerMatches(""))
			}
			lines := v.lines()
			counts := map[string]int{}
			for i, row := range lines {
				if row == "" {
					continue
				}
				class, _ := v.diagnosticRow(lines, i)
				if class == "unknown" {
					t.Errorf("row %d is unclassified", i+1)
				}
				counts[class]++
			}
			for class, want := range map[string]int{"composer": 1, "composer_rule": 2, "footer": 1} {
				if counts[class] != want {
					t.Errorf("%s rows=%d, want %d", class, counts[class], want)
				}
			}
		})
	}
}

func TestMusePTY147OSC7501QueryNoEffect(t *testing.T) {
	for _, ending := range []string{"\x1b\\", "\x07"} {
		t.Run(fmt.Sprintf("terminator_%x", ending), func(t *testing.T) {
			v := newMuseVT(4, 12)
			v.observe([]byte("seed\x1b]0;ws\x07\x1b[2;3H\x1b7"))
			before := v.lines()
			query := []byte("\x1b]7501;?" + ending)
			for i, b := range query {
				if reply := v.observe([]byte{b}); len(reply) != 0 {
					t.Fatal("capability query elicited a reply")
				}
				if i < len(query)-1 && len(v.pending) != i+1 {
					t.Fatal("query consumed before terminator completed")
				}
			}
			if v.unknown || len(v.pending) != 0 || !reflect.DeepEqual(v.lines(), before) || v.row != 1 || v.col != 2 || v.savedRow != 1 || v.savedCol != 2 || v.top != 0 || v.bottom != 3 || v.title != "ws" {
				t.Fatal("capability query changed modeled state or remained pending")
			}
		})
	}
}

func TestMusePTY147BareELErasesCursorSuffix(t *testing.T) {
	v := newMuseVT(3, 8)
	v.observe([]byte("abcdefgh\r\nijklmnop\x1b[1;4H\x1b[K"))
	if v.unknown || len(v.pending) != 0 || v.lines()[0] != "abc" || v.lines()[1] != "ijklmnop" || v.row != 0 || v.col != 3 {
		t.Fatal("bare EL did not erase exactly the cursor suffix")
	}
	v.observe([]byte("\x1b[2K"))
	if v.unknown || v.lines()[0] != "" || v.lines()[1] != "ijklmnop" || v.col != 3 {
		t.Fatal("captured EL2 behavior changed")
	}
	// The old allowlist refuses explicit EL0/EL1; do not broaden it based
	// only on the internal erase switch's support for those parameters.
	for _, raw := range []string{"\x1b[0K", "\x1b[1K", "\x1b[;K", "\x1b[3K"} {
		v := newMuseVT(3, 8)
		v.observe([]byte("abcdefgh\x1b[1;4H"))
		before := v.lines()
		v.observe([]byte(raw))
		if !v.unknown || !reflect.DeepEqual(v.lines(), before) {
			t.Fatal("uncaptured EL form admitted or changed cells")
		}
	}
}

func TestMusePTY147VTClosedNegativeControls(t *testing.T) {
	for _, tc := range []struct{ name, raw, kind string }{
		{"other_7501_payload", "\x1b]7501;private-value\x07", "OSC number=7501"},
		{"extra_7501_payload", "\x1b]7501;??\x1b\\", "OSC number=7501"},
		{"malformed_osc", "\x1b]7501;?\x1bX\x07", "OSC number=7501"},
		{"malformed_number", "\x1b]private-value;?\x07", "OSC number=invalid"},
		{"unknown_csi_final", "\x1b[12345;67890z", "CSI final=0x7a params=2"},
		{"bare_unknown_csi_final", "\x1b[z", "CSI final=0x7a params=0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newMuseVT(40, 120)
			v.observe([]byte(museTestScreen("")))
			before := v.lines()
			for _, b := range []byte(tc.raw) {
				v.observe([]byte{b})
			}
			if !v.unknown || v.unknownKind != tc.kind || len(v.pending) != 0 || v.composerMatches("") || !reflect.DeepEqual(v.lines(), before) {
				t.Fatalf("control not refused intact: unknown=%t kind=%s pending=%d", v.unknown, v.unknownKind, len(v.pending))
			}
		})
	}
}
