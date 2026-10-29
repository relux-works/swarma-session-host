package musepty

// Revision r6: three strict structures replace the heuristic closures. Every
// round-5 panel reproduction is ported here as a named production-entry test
// through DeliverSessionNotice. Sources:
//   - panel A (regression): TestReviewR5* in regression_probes.go
//   - panel B (regression): TestReviewR5* in probes_test.go
//   - delta (regression): TestIndependent* in independent_test.go
// Ports stamp the owned stream on every non-foreign record, so each refusal
// is attributable to the defect under test rather than to stream binding.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// museExpectDelivery asserts the production entry point admitted the text all
// the way through paste and submit, then settles the post-submit frame
// (pipe handshake plus pump notifications) so the next case starts from
// observed idle instead of racing the pump (tb-R198). It re-anchors the
// test clock past the quiet window first, so long property loops keep
// admitting on the production quiet comparison.
func museExpectDelivery(t *testing.T, h *musePTYHost, home, text string) {
	t.Helper()
	museAdvanceTestClockPastQuiet(h)
	if err := h.DeliverSessionNotice(context.Background(), text); err != nil {
		t.Fatalf("expected delivery admitted %q: got %v (idle=%v ready=%v quiet=%v)",
			text, err, h.idle(""), museScriptedReady(h, ""), museQuietForReport(h))
	}
	b, err := os.ReadFile(filepath.Join(home, "peer-submit"))
	if err != nil || !strings.Contains(string(b), text) {
		t.Fatalf("delivery not submitted %q: %v", b, err)
	}
	museSettleScriptedIdle(t, h, home)
}

func museAppendRawLog(t *testing.T, h *musePTYHost, raw []byte) {
	t.Helper()
	f, err := os.OpenFile(h.snapshot.LogPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write(append(raw, '\n'))
	closeErr := f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}

// Paired escapes and ordinary Unicode still decode: the scan refuses only
// what cannot decode losslessly. An escaped backslash before a u-sequence
// is literal text, not an escape; raw NEL inside a string value is legal
// JSON the log never forwards to the screen or the paste path.
func TestMusePTYR6ValidEscapesPermitNotice(t *testing.T) {
	for _, value := range []string{`"\ud83d\ude00"`, `"\uD83D\uDE00"`, `"ordinary Unicode α"`, `"quoted \"line\"\nnext"`, `"\\ud800 as text"`, "\"raw NEL \u0085 inside\""} {
		t.Run(value, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			targetID, targetSeq := museResumeTarget(t, h)
			record := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
			record["payload"].(map[string]any)["record"].(map[string]any)["credential_backend"] = "SENTINEL"
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			raw = []byte(strings.Replace(string(raw), `"SENTINEL"`, value, 1))
			museAppendRawLog(t, h, raw)
			// The second opened reopens the session, so the
			// captured resume context witnesses it; the escape
			// vehicle itself must decode losslessly either way.
			// The resumed reference resolves to the prior epoch.
			appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
			museExpectDelivery(t, h, home, "valid escape control")
		})
	}
}

// No log line is skipped. A whitespace-only line in any space — JSON blanks
// included — is unknown state, not absence of work. The round-5 blank-line
// control (" \t\r" admitted) is superseded by this structural rule.
func TestMusePTYR6NonJSONWhitespaceMustRefuse(t *testing.T) {
	for _, tc := range []struct{ name, line string }{
		{"vertical_tab", "\x0b"},
		{"form_feed", "\x0c"},
		{"nel", "\u0085"},
		{"nbsp", "\u00a0"},
		{"en_quad", "\u2000"},
		{"spaces", "   "},
		{"tab", "\t"},
		{"json_blanks", " \t\r"},
		{"cr_only", "\r"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			museAppendRawLog(t, h, []byte(tc.line))
			assertMuseNoInput(t, h, home)
		})
	}
	t.Run("interior_empty_line", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		base, err := os.ReadFile(h.snapshot.LogPath)
		if err != nil {
			t.Fatal(err)
		}
		// An empty line between records is a line the host cannot read.
		if err := os.WriteFile(h.snapshot.LogPath, append(append([]byte(nil), base...), '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		assertMuseNoInput(t, h, home)
	})
}

// A run start for an already-active run is unknown state: one terminal
// cannot settle two starts.
func TestMusePTYR6DuplicateRunStartMustRefuse(t *testing.T) {
	t.Run("identical_records", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("started")))
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("started")))
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("terminal")))
		assertMuseNoInput(t, h, home)
	})
	t.Run("distinct_record_ids", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("started")))
		second := museOwnedRecord(h, museFixtureRecord("started"))
		second["id"] = museTestUUID("second-start-record")
		appendMuseRecord(t, h, second)
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("terminal")))
		assertMuseNoInput(t, h, home)
	})
}

// Runtime records must belong to the opened session as well as their run.
func TestMusePTYR6ForeignStreamMustRefuse(t *testing.T) {
	t.Run("foreign_terminal", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		start := museOwnedRecord(h, museFixtureRecord("started"))
		appendMuseRecord(t, h, start)
		end := museOwnedRecord(h, museFixtureRecord("terminal"))
		end["stream"].(map[string]any)["id"] = museTestUUID("foreign-session")
		appendMuseRecord(t, h, end)
		assertMuseNoInput(t, h, home)
	})
	t.Run("foreign_start", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		start := museOwnedRecord(h, museFixtureRecord("started"))
		start["stream"].(map[string]any)["id"] = museTestUUID("foreign-session")
		appendMuseRecord(t, h, start)
		assertMuseNoInput(t, h, home)
	})
	t.Run("empty_stream_id", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		start := museOwnedRecord(h, museFixtureRecord("started"))
		start["stream"].(map[string]any)["id"] = ""
		appendMuseRecord(t, h, start)
		assertMuseNoInput(t, h, home)
	})
	t.Run("second_opened_session", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		opened := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
		opened["stream"].(map[string]any)["id"] = museTestUUID("second-session")
		appendMuseRecord(t, h, opened)
		assertMuseNoInput(t, h, home)
	})
	// r9 item 6: every reopen waits for the captured resume context,
	// including a reopen without an end. The r6 same-session control
	// pinned the PID-only reopen admission and is inverted here; the
	// resumed reopen still admits.
	t.Run("same_session_reopen_without_resume_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		assertMuseNoInput(t, h, home)
	})
	t.Run("same_session_reopen_with_resume_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		targetID, targetSeq := museResumeTarget(t, h)
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
		museExpectDelivery(t, h, home, "same-session reopen control")
	})
}

// A content row never hides the row above it: the whole region between the
// last transcript row and the composer must match.
func TestMusePTYR6BuriedProgressRowMustRefuse(t *testing.T) {
	for _, tc := range []struct{ name, rows string }{
		{"unknown_under_detail", "◈ Future action required\r\n  status detail\r\n"},
		{"known_busy_under_detail", "◆ Working (2s · esc to interrupt)\r\n  status detail\r\n"},
		{"unknown_under_echo_tail", "◈ Future action required\r\n◆ echo: harmless tail\r\n"},
		{"busy_under_echo_tail", "◆ Working (2s · esc to interrupt)\r\n◆ echo: harmless tail\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte("\x1b[1;1H\x1b[J" + tc.rows + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
}

// A lone indented row is unknown state with no captured parent, glyph-led
// or plain: round 6 admitted plain lone rows as continuations and round 7
// closes that catch-all with the row-sequence grammar (the lone plain row
// is the round-6 orphan reproduction). Past a transcript parent any
// indented text admits as quoted continuation; see continuation_permits.
func TestMusePTYR6IndentedStatusMustRefuse(t *testing.T) {
	for _, row := range []string{
		"  ◈ Future action required",
		"  ◆ Working (2s · esc to interrupt)",
		"  ◇ Thinking (1s · esc to interrupt)",
		"  • Queued input",
		"  ❯ smuggled turn",
		"   ◈ deep unknown",
		"     • deep queue claim",
	} {
		t.Run(row, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte("\x1b[1;1H\x1b[J" + row + "\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
			h.mu.Unlock()
			assertMuseNoInput(t, h, home)
		})
	}
	// r7 sequence grammar: continuations admit only past a transcript
	// parent. The lone-row shape now refuses (round-6 orphan ports); the
	// parent keeps these controls proving wrapped/queued content admits.
	for _, row := range []string{"  Future action required", "  status detail", "  ↳ second harvest", "   deep continuation", "      code block"} {
		t.Run("continuation_permits/"+row, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte("\x1b[1;1H\x1b[J❯ first harvest line\r\n" + row + "\r\n" + strings.TrimPrefix(museTestScreen(""), "\x1b[1;1H\x1b[J")))
			h.mu.Unlock()
			museExpectDelivery(t, h, home, "indented continuation control")
		})
	}
}

// An OSC title payload the host cannot fully read is unknown state: invalid
// UTF-8, controls, an embedded escape, or an unbounded length. A readable
// title stores and still delivers.
func TestMusePTYR6MalformedOSCTitleMustRefuse(t *testing.T) {
	overlong := strings.Repeat("t", 4097)
	for _, tc := range []struct{ name, raw string }{
		{"invalid_utf8", "\x1b]0;bad\xff\a"},
		{"DEL", "\x1b]0;bad\x7f\a"},
		{"C1", "\x1b]0;bad\u009b\a"},
		{"embedded_escape", "\x1b]0;bad\x1b[999z\a"},
		{"NUL", "\x1b]0;bad\x00\a"},
		{"overlong", "\x1b]0;" + overlong + "\a"},
		{"st_terminated_malformed", "\x1b]0;bad\x7f\x1b\\"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte(tc.raw))
			unknown, title := h.vt.unknown, h.vt.title
			h.mu.Unlock()
			t.Logf("unknown=%v title=%q", unknown, title)
			assertMuseNoInput(t, h, home)
		})
	}
	t.Run("readable_title_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		h.mu.Lock()
		h.vt.observe([]byte("\x1b]0;muse — project\a"))
		unknown, title := h.vt.unknown, h.vt.title
		h.mu.Unlock()
		if unknown || title != "muse — project" {
			t.Fatalf("readable title rejected: unknown=%v title=%q", unknown, title)
		}
		museExpectDelivery(t, h, home, "readable title control")
	})
	for _, tc := range []struct{ name, raw, title string }{
		{"st_terminated", "\x1b]0;st title\x1b\\", "st title"},
		{"empty", "\x1b]0;\a", ""},
		{"exact_bound", "\x1b]0;" + strings.Repeat("b", 4096) + "\a", strings.Repeat("b", 4096)},
	} {
		t.Run("title_permits/"+tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			h.mu.Lock()
			h.vt.observe([]byte(tc.raw))
			unknown, title := h.vt.unknown, h.vt.title
			h.mu.Unlock()
			if unknown || title != tc.title {
				t.Fatalf("valid title rejected: unknown=%v title=%q", unknown, title)
			}
			museExpectDelivery(t, h, home, "title boundary control")
		})
	}
}
