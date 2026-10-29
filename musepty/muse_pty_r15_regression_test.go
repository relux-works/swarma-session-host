package musepty

import (
	"context"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Round-15 ports: log-backed provenance runs BEFORE chrome
// classification. Every round-14 panel reproduction drives the
// production entry point (DeliverSessionNotice via museR9Refuse /
// museExpectDelivery). Delta panel =
// regression/regression_verdict.md (finding
// quoted-header-at-frame-start), panel A =
// regression/regression_verdict.md (finding
// quoted-chrome-regression, repeat-of rev13/quoted-chrome-regression).
// Both panels converged on one mechanism: a scrolled committed
// transcript quoting the header was classified as chrome before the
// r10 log-backed check ran, so its continuation refused.

// A scrolled frame whose first nonblank row is a log-backed quote of
// the header text, followed by its log-backed continuation, is idle
// and delivers. Ports delta TestDeltaBackedQuotedChromeAtTop (all
// three variants) and panel A TestPanelR14ScrolledQuotedHeaderDelivers.
func TestMusePTYR15BackedQuotedHeaderAtTopDelivers(t *testing.T) {
	cases := []struct {
		name      string
		run       string
		committed string
		rows      []string
	}{
		{
			name:      "ordinary_line",
			run:       "r15-backed-quote-ordinary",
			committed: "echo: earlier\nordinary line\nnext quoted line",
			rows:      []string{"  ordinary line", "  next quoted line"},
		},
		{
			name:      "resumed_notice",
			run:       "r15-backed-quote-notice",
			committed: "echo: earlier\nresumed session 01a0fae9-44f2-72f0-877c-03a372db40f7\nnext quoted line",
			rows:      []string{"  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7", "  next quoted line"},
		},
		{
			name:      "header_delta",
			run:       "r15-backed-quote-header-delta",
			committed: "echo: earlier\nMuse Code 1.4.1\nnext quoted line",
			rows:      []string{"  Muse Code 1.4.1", "  next quoted line"},
		},
		{
			name:      "header_panel_a",
			run:       "panel-scrolled-header",
			committed: "echo: prefix\nMuse Code 1.4.1\nnext quoted line",
			rows:      []string{museHeaderRow, "  next quoted line"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			museR10TerminatedEcho(t, h, museTestUUID(c.run), c.committed)
			f, err := h.fold()
			if err != nil || !f.idle() {
				t.Fatalf("not an idle transcript: %v", err)
			}
			if !museLogBackedRowsOK(c.rows, 120, f) {
				t.Fatal("precondition: rows must be a committed suffix")
			}
			museShowFrame(t, h, c.rows...)
			museExpectDelivery(t, h, home, "backed quote at screen top")
		})
	}
}

// The same header text that is NOT log-backed at the frame start
// still classifies as chrome, and an orphan indented row after it
// refuses. The unrelated_run subtest proves the refusal comes from
// the backing check failing, not from the log being absent.
func TestMusePTYR15UnbackedHeaderStaysChrome(t *testing.T) {
	if got := museScreenRowStateAt(museHeaderRow, 0, museRowStart); got != museRowChromeHeader {
		t.Fatalf("unbacked header at frame start classifies as %q, want chrome_header", got)
	}
	t.Run("unrelated_run", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museR10TerminatedEcho(t, h, museTestUUID("r15-unbacked-header-run"), "echo: earlier\nordinary line\nnext quoted line")
		rows := []string{museHeaderRow, "  next quoted line"}
		f, err := h.fold()
		if err != nil || !f.idle() {
			t.Fatalf("not an idle transcript: %v", err)
		}
		if museLogBackedRowsOK(rows, 120, f) {
			t.Fatal("precondition: rows must NOT be a committed suffix")
		}
		museShowFrame(t, h, rows...)
		museR9Refuse(t, h, home)
	})
	t.Run("no_runs", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		rows := []string{museHeaderRow, "  orphan continuation"}
		f, err := h.fold()
		if err != nil || !f.idle() {
			t.Fatalf("not an idle transcript: %v", err)
		}
		if museLogBackedRowsOK(rows, 120, f) {
			t.Fatal("precondition: rows must NOT be a committed suffix")
		}
		museShowFrame(t, h, rows...)
		museR9Refuse(t, h, home)
	})
}

// A real long turn quoting the header on every line leaves quoted
// header text at the top of the viewport and still delivers. Ports
// delta TestDeltaRealLongQuotedHeaderAtTop verbatim.
func TestMusePTYR15RealLongQuotedHeaderDelivers(t *testing.T) {
	h, ctx := museRealEchoHost(t, "")
	text := strings.TrimSuffix(strings.Repeat("Muse Code 1.4.1\n", 28), "\n")
	result, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: text})
	if err != nil || result == nil || result.Status != sm.ProviderRunIdle || result.Output != "echo: "+text {
		t.Fatalf("long quoted Echo content: %+v %v", result, err)
	}
	settle, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	museSmokeBoundedWait(t, settle, "real quoted header idle", func() bool { return h.idle("") })
	h.mu.Lock()
	screen := strings.Join(h.vt.lines(), "\n")
	h.mu.Unlock()
	t.Logf("settled screen=%q", screen)
	if err := h.DeliverSessionNotice(ctx, "notice after long quoted header"); err != nil {
		t.Fatalf("legitimate stable quoted-header transcript refused: %v", err)
	}
}

// A real long turn with the exact quoted header at the scrolled
// first visible row delivers. Ports panel A
// TestPanelR14RealScrolledQuotedHeaderDelivers verbatim.
func TestMusePTYR15RealScrolledQuotedHeaderDelivers(t *testing.T) {
	h, ctx := museRealEchoHost(t, "")
	// The existing real long-turn control leaves continuation 22 as the first
	// visible assistant row. Put the exact quoted header at that position.
	lines := make([]string, 28)
	for i := range lines {
		lines[i] = "ordinary offline echo line"
	}
	lines[22] = "Muse Code 1.4.1"
	text := strings.Join(lines, "\n")
	result, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: text})
	if err != nil || result == nil || result.Status != sm.ProviderRunIdle || result.Output != "echo: "+text {
		t.Fatalf("content: %+v %v", result, err)
	}
	settle, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	museSmokeBoundedWait(t, settle, "real scrolled header idle", func() bool { return h.idle("") })
	h.mu.Lock()
	frame := h.vt.lines()
	screen := strings.Join(frame, "\n")
	h.mu.Unlock()
	t.Logf("screen=%q", screen)
	f, e := h.fold()
	if e != nil || !f.idle() || frame[0] != museHeaderRow || !museLogBackedRowsOK(frame[:6], 120, f) {
		t.Fatalf("real frame is not the backed header suffix: %v", e)
	}
	if err := h.DeliverSessionNotice(ctx, "notice after scrolled quoted header"); err != nil {
		t.Fatalf("stable committed transcript refuses notice: %v", err)
	}
}
