package musepty

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestMusePTY147METAStartupHostIdle(t *testing.T) {
	const logPath = "testdata/muse-1.4.7/session.jsonl"
	f, err := readMuseLog(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !f.idle() {
		t.Fatal("startup fold must be idle before checking the screen half")
	}
	for _, file := range []string{"screen-meta-startup-plain.txt", "screen-meta-startup-sgr.txt"} {
		t.Run(file, func(t *testing.T) {
			data := muse147Fixture(t, file)
			if bytes.Contains(data, []byte(museComposerPlaceholder)) {
				t.Fatal("startup capture now contains a placeholder; reassess attribute evidence")
			}
			v := muse147Screen(t, strings.Split(strings.TrimSuffix(string(data), "\n"), "\n"))
			rows, _, _, found := v.composerRows()
			if !found || len(rows) != 1 || rows[0] != "" || !v.composerMatches("") {
				t.Fatalf("startup composer not empty: rows=%q found=%v", rows, found)
			}
			if !v.safeComposer("", f) {
				t.Fatal("idle META startup screen refused")
			}
			// Exercise the production idle predicate with the captured idle log
			// and a fixed quiet boundary, without launching a peer or waiting.
			lastOutput := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
			now := lastOutput.Add(musePTYQuiet)
			h := &musePTYHost{
				vt: v, exited: make(chan struct{}),
				snapshot:   musePTYSnapshot{LogPath: logPath, SessionID: f.SessionID, PID: f.PID},
				lastOutput: lastOutput, testFakeNow: &now,
			}
			h.mu.Lock()
			quiet, ready := h.quietLocked(), h.readyLocked(f, "")
			h.mu.Unlock()
			if !quiet || !ready || !h.idle("") {
				t.Fatalf("host idle refused: quiet=%v ready=%v", quiet, ready)
			}
			if h.idle("human draft") {
				t.Fatal("empty startup screen matched a draft")
			}
			now = lastOutput.Add(musePTYQuiet - time.Nanosecond)
			if h.idle("") {
				t.Fatal("screen admission bypassed the quiet boundary")
			}
		})
	}
}

func TestMusePTY147ClosedProviderFooters(t *testing.T) {
	const footer = "  muse-wonder-1.0 · max · /h/f/ws · Auto-review"
	rows := strings.Split(strings.TrimSuffix(string(muse147Fixture(t, "screen-meta-startup-plain.txt")), "\n"), "\n")
	for _, tc := range []struct {
		name, footer string
		want         bool
	}{
		{"meta", footer, true},
		{"echo", "  echo · ~/ws · Auto-review", true},
		{"contributor", strings.Replace(footer, "muse-wonder-1.0", "muse-wonder-1.0-contributor", 1), true},
		{"full", strings.Replace(footer, "muse-wonder-1.0", "muse-wonder-full-1.0-contributor", 1), true},
		{"full_suffix", strings.Replace(footer, "muse-wonder-1.0", "muse-wonder-1.0-full", 1), true},
		{"unknown_model_shape", strings.Replace(footer, "muse-wonder-1.0", "wonder", 1), false},
		{"missing_model_version", strings.Replace(footer, "muse-wonder-1.0", "muse-wonder", 1), false},
		{"unknown_model_suffix", strings.Replace(footer, "muse-wonder-1.0", "muse-wonder-1.0-future", 1), false},
		{"unknown_effort", strings.Replace(footer, " · max · ", " · future · ", 1), false},
		{"retired_effort", strings.Replace(footer, " · max · ", " · ultra · ", 1), false},
		{"no_effort", strings.Replace(footer, " · max · ", " · none · ", 1), false},
		{"empty_path", strings.Replace(footer, "/h/f/ws", "", 1), false},
		{"unknown_mode", strings.Replace(footer, "Auto-review", "Future-review", 1), false},
		{"yolo", strings.Replace(footer, "Auto-review", "yolo", 1), false},
		{"full_access", strings.Replace(footer, "Auto-review", "Full access", 1), false},
		{"extra_fifth_segment", footer + " · future", false},
		{"echo_extra_segment", "  echo · max · ~/ws · Auto-review", false},
		{"echo_unknown_mode", "  echo · ~/ws · Future-review", false},
		{"echo_empty_path", "  echo ·  · Auto-review", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := museIdleFooter(tc.footer); got != tc.want {
				t.Fatalf("footer admission=%v want=%v", got, tc.want)
			}
			changed := append([]string(nil), rows...)
			matches := 0
			for i, row := range changed {
				if row == footer {
					changed[i] = tc.footer
					matches++
				}
			}
			if matches != 1 {
				t.Fatalf("fixture footer count=%d", matches)
			}
			if got := muse147Screen(t, changed).safeComposer("", nil); got != tc.want {
				t.Fatalf("screen admission=%v want=%v", got, tc.want)
			}
		})
	}
	for _, effort := range []string{"minimal", "low", "medium", "high", "xhigh", "max"} {
		if !museIdleFooter(strings.Replace(footer, " · max · ", " · "+effort+" · ", 1)) {
			t.Errorf("current Muse effort %q refused", effort)
		}
	}
}
