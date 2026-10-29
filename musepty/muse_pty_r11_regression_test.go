package musepty

// Revision r11: the pre-submit fold recheck, and every stated bound
// fails closed. Every round-10 panel reproduction is ported here as a
// named production-entry test through DeliverSessionNotice. Sources:
//   - delta (regression): TestDeltaR10* in the verdict appendix
//   - panel A (regression): TestR141R10* in
//     regression_probes_test.go
//   - panel B (regression): TestPanelR10* in
//     regression_probes_test.go
//
// The frame digest CONTENT stays unverified as a pinned Stop-The-Line
// admission: no derivation from the captured children reproduces the
// captured digests (see the r11 handoff evidence), and refusing real
// frames would brick real sessions. The digest SPELLING is pinned and
// fails closed.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The final recheck right before paste and Enter re-reads the log and
// requires the fresh fold to prove idle: a log change between the
// first check and the submit defers, even when the screen never moved.
// Ports panel B TestPanelR10FinalFoldMustStillBeIdle: the FIFO feeds
// the real readMuseLog consecutive complete snapshots at its I/O
// boundary (reads 1-4 certify idle; read 5 is the final pre-Enter
// reread and contains a newly started run).
func TestMusePTYR11PreSubmitRecheckIncludesFold(t *testing.T) {
	h, home := musePeer(t, "idle")
	baseline, err := os.ReadFile(h.snapshot.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	r := museOwnedRecord(h, museFixtureRecord("started"))
	r["payload"].(map[string]any)["run_id"] = museTestUUID("final-fold-racing-run")
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	active := append(append([]byte(nil), baseline...), append(b, '\n')...)
	fifo := filepath.Join(home, "review-log-fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	h.snapshot.LogPath = fifo
	h.mu.Unlock()
	// Drain stale settle folds: the server below sequences one snapshot
	// per fresh fold notification, so leftovers would skip snapshots.
	for {
		select {
		case <-h.foldNotify:
		default:
			goto drained
		}
	}
drained:
	server := make(chan error, 1)
	go func() {
		for i := 1; i <= 6; i++ {
			// Each snapshot is a rendezvous: the open blocks until
			// the reader opens for this fold, and the fold
			// notification below blocks until the reader has
			// consumed (closed) before the next write. No sleep.
			w, e := os.OpenFile(fifo, os.O_WRONLY, 0600)
			if e != nil {
				server <- e
				return
			}
			data := baseline
			if i >= 5 {
				data = active
			}
			_, e = w.Write(data)
			_ = w.Close()
			if e != nil {
				server <- e
				return
			}
			<-h.foldNotify
		}
		server <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = h.DeliverSessionNotice(ctx, "must hold at the final fold")
	if !errors.Is(err, ErrMusePTYHeld) {
		submitted, _ := os.ReadFile(filepath.Join(home, "peer-submit"))
		t.Fatalf("final fold contains active run; notice must be held, got %v; actual Enter witness=%q", err, submitted)
	}
	if _, e := os.Stat(filepath.Join(home, "peer-submit")); e == nil {
		t.Fatal("Enter reached peer on busy final fold")
	}
}

// The off-frame path admits only runes whose display width is
// established by the closed allowlist (r12: printable ASCII plus the
// captured U+2502). Anything else fails closed. Ports delta
// TestDeltaR10WideColumnsMustFailClosed, plus combining-mark and
// unprintable pins. The r11 column-counting rule is superseded: the
// CJK subtests below now refuse on unestablished rendering rather
// than on a 160-column measure, and the combining control became a
// refusal for the same reason (U+0301 is not in the captured
// committed output).
func TestMusePTYR11WideColumnsMustFailClosed(t *testing.T) {
	t.Run("cjk_wide_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		wide := strings.Repeat("界", 80)
		// Unallowlisted runes have no established width: the run backs nothing.
		museR10TerminatedEcho(t, h, museTestUUID("wide-cjk"), "echo: a\n"+wide+"\nzzz")
		museShowFrame(t, h, "  zzz")
		museR9Refuse(t, h, home)
	})
	t.Run("combining_unestablished_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		// The combining acute is not in the captured committed
		// output: unestablished rendering fails closed even though
		// the line is short.
		marked := strings.Repeat("é", 50)
		museR10TerminatedEcho(t, h, museTestUUID("combining-run"), "echo: a\n"+marked+"\nzzz")
		museShowFrame(t, h, "  zzz")
		museR9Refuse(t, h, home)
	})
	t.Run("wide_with_combining_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		// Unestablished runes fail closed with or without marks.
		marked := strings.Repeat("界́", 60)
		museR10TerminatedEcho(t, h, museTestUUID("wide-marked"), "echo: a\n"+marked+"\nzzz")
		museShowFrame(t, h, "  zzz")
		museR9Refuse(t, h, home)
	})
	t.Run("unprintable_fails_closed", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		// A tab has no established column rendering: the run backs nothing.
		museR10TerminatedEcho(t, h, museTestUUID("tab-run"), "echo: a\nbad\tline\nzzz")
		museShowFrame(t, h, "  zzz")
		museR9Refuse(t, h, home)
	})
}

// Narrow pins for the display-width allowlist (r12): printable ASCII
// and the captured U+2502 establish widths; every other rune — CJK,
// hangul, fullwidth, symbols, supplementary planes, combining marks,
// controls — is unestablished.
func TestMusePTYR11DisplayWidthPins(t *testing.T) {
	for _, tc := range []struct {
		name  string
		line  string
		width int
		ok    bool
	}{
		{"ascii", "echo: alpha", 11, true},
		{"captured_box_vertical", "│", 1, true},
		{"cjk", "界界", 0, false},
		{"hangul", "한글", 0, false},
		{"fullwidth", "ａｂ", 0, false},
		{"supplementary", "𛄲", 0, false},
		{"combining", "éé", 0, false},
		{"symbol_unestablished", "◆", 0, false},
		{"empty", "", 0, true},
		{"tab", "a\tb", 0, false},
		{"control", "a\x01b", 0, false},
		{"del", "a\x7fb", 0, false},
		{"zwj", "a\u200db", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			width, ok := museDisplayWidth(tc.line)
			if ok != tc.ok || ok && width != tc.width {
				t.Fatalf("width %q = %d,%v, want %d,%v", tc.line, width, ok, tc.width, tc.ok)
			}
		})
	}
}

// museR11OwnedFrame returns the first captured retained frame with its
// children re-stamped as the owned session's own, so the verdict is
// the digest under test rather than a foreign stream.
func museR11OwnedFrame(t *testing.T, h *musePTYHost) map[string]any {
	t.Helper()
	var found map[string]any
	for _, raw := range museCorpus(t) {
		var r map[string]any
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatal(err)
		}
		if r["retained_frame"] != nil {
			found = r
			break
		}
	}
	if found == nil {
		t.Fatal("no retained frame in corpus")
	}
	for _, child := range found["children"].([]any) {
		c := child.(map[string]any)
		var r map[string]any
		if err := json.Unmarshal([]byte(c["record_json"].(string)), &r); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(museOwnedRecord(h, r))
		if err != nil {
			t.Fatal(err)
		}
		c["record_json"] = string(raw)
	}
	return found
}

// A malformed frame digest is unknown state: 3/3 captured digests read
// sha256: plus 64 hex digits. Ports delta
// TestDeltaR10FrameHashUnverifiedBoundMustFailClosed.
func TestMusePTYR11FrameDigestMalformedMustRefuse(t *testing.T) {
	for _, tc := range []struct{ name, digest string }{
		{"not_hex", "sha256:invalid"},
		{"truncated", "sha256:7f3634af1aebc333020e683d6b525bc6ae6ae267e1bac9c196e6f4841a36dbb"},
		{"wrong_prefix", "md5:7f3634af1aebc333020e683d6b525bc6ae6ae267e1bac9c196e6f4841a36dbb4"},
		{"non_hex_char", "sha256:7f3634af1aebc333020e683d6b525bc6ae6ae267e1bac9c196e6f4841a36dbzz"},
		{"empty", ""},
		{"bare_hex", "7f3634af1aebc333020e683d6b525bc6ae6ae267e1bac9c196e6f4841a36dbb4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			frame := museR11OwnedFrame(t, h)
			frame["content_sha256"] = tc.digest
			appendMuseRecord(t, h, frame)
			museR9Refuse(t, h, home)
		})
	}
	t.Run("uppercase_prefix_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		frame := museR11OwnedFrame(t, h)
		// The prefix is lowercase in every capture and stays pinned;
		// only the hex digits are case-lenient (next subtest).
		frame["content_sha256"] = "SHA256:7F3634AF1AEB00C33020E683D6B525BC6AE6AE267E1BAC9C196E6F4841A36DBB4"
		appendMuseRecord(t, h, frame)
		museR9Refuse(t, h, home)
	})
	t.Run("uppercase_hex_digits_permit", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		frame := museR11OwnedFrame(t, h)
		frame["content_sha256"] = "sha256:7F3634AF1AEBC333020E683D6B525BC6AE6AE267E1BAC9C196E6F4841A36DBB4"
		appendMuseRecord(t, h, frame)
		museExpectDelivery(t, h, home, "uppercase hex control")
	})
}

// STOP-THE-LINE (r11 finding 3): the digest CONTENT is unverified. No
// derivation from the three captured frames reproduces their digests
// (concatenation, canonical JSON, per-child hashes and envelope mixes
// were all tried; see the r11 handoff evidence), so refusing
// well-formed digests would brick the real captured sessions that
// carry frames. A well-formed but wrong digest still delivers when
// the children are the captured ones; this test pins that admission
// so it cannot silently widen.
func TestMusePTYR11FrameDigestContentUnverifiedStopTheLine(t *testing.T) {
	h, home := musePeer(t, "idle")
	frame := museR11OwnedFrame(t, h)
	frame["content_sha256"] = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	appendMuseRecord(t, h, frame)
	museExpectDelivery(t, h, home, "unverifiable digest Stop-The-Line")
}

// A resumed record outside a pending reopen is unknown state: no
// complete capture shows one in a legitimate flow. Ports delta
// TestDeltaR10OrphanResumedIsNoopBoundMustFailClosed.
func TestMusePTYR11OrphanResumedMustRefuse(t *testing.T) {
	t.Run("orphan_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museCorpusRecord(t, "session.resumed", "", "")))
		museR9Refuse(t, h, home)
	})
	t.Run("reopen_resumed_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		targetID, targetSeq := museResumeTarget(t, h)
		appendMuseRecord(t, h, museOwnedRecord(h, museFixtureRecord("session.opened.observed")))
		appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
		museExpectDelivery(t, h, home, "resumed reopen control")
	})
}

// A delivery record id resolves to one stored queue identity: a later
// queue reusing an observed envelope for another source item aliases
// the observation and is unknown state. Ports panel A
// TestR141R10AmbiguousQueueIdentity verbatim. Since r12 the duplicate
// mode refuses at global envelope uniqueness before it can reach the
// per-envelope source join, which stays as defense in depth behind
// the store the drain arm resolves.
func TestMusePTYR11QueueEnvelopeAliasMustRefuse(t *testing.T) {
	for _, mode := range []string{"unique_wrong_queue_refuses", "unique_matching_queue_delivers", "duplicate_conflicting_queue_refuses"} {
		t.Run(mode, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			museEmitRunBounded(t, h, "started", museTestUUID("owner-A"))
			museEmitRunBounded(t, h, "started", museTestUUID("owner-B"))
			museEmitRunBounded(t, h, "started", museTestUUID("target-A"))
			museEmitRunBounded(t, h, "terminal", museTestUUID("target-A"))
			museEmitRunBounded(t, h, "started", museTestUUID("target-B"))
			museEmitRunBounded(t, h, "terminal", museTestUUID("target-B"))
			a := museR9AcceptIntent(t, h, museTestUUID("target-B"))
			q1 := museR10QueueAndDrain(t, h, museTestUUID("owner-A"), museTestUUID("target-A"), museTestUUID("target-A"))
			q2 := museOwnedRecord(h, museFixtureRecord("inbox_item_queued"))
			q2["id"] = q1["id"]
			if mode != "duplicate_conflicting_queue_refuses" {
				q2["id"] = museTestUUID("distinct-queue-envelope-B")
			}
			qp := q2["payload"].(map[string]any)
			qp["run_id"] = museTestUUID("owner-B")
			qp["source_run_record_id"] = museTestUUID("distinct-item-B")
			q2["causation_id"] = museTestUUID("target-B")
			appendMuseRecord(t, h, q2)
			d := museOwnedRecord(h, museFixtureRecord("inbox_item_drained"))
			dp := d["payload"].(map[string]any)
			dp["run_id"] = museTestUUID("target-B")
			ev := dp["event"].(map[string]any)
			ev["item_id"] = museTestUUID("distinct-item-B")
			ev["drain_target_run_stream"].(map[string]any)["id"] = museTestUUID("target-B")
			appendMuseRecord(t, h, d)
			cite := q1
			if mode == "unique_matching_queue_delivers" {
				cite = q2
			}
			museR10ActiveDelivery(t, h, a, cite, museTestUUID("target-B"), museTestUUID("owner-B"))
			museEmitRunBounded(t, h, "terminal", museTestUUID("owner-A"))
			museEmitRunBounded(t, h, "terminal", museTestUUID("owner-B"))
			if mode == "unique_matching_queue_delivers" {
				museExpectDelivery(t, h, home, "unique identity control")
			} else {
				museR9Refuse(t, h, home)
			}
		})
	}
}
