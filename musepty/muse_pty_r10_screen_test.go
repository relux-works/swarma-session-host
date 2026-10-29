package musepty

// Revision r10 screen provenance: an indented row whose parent is not
// in the current frame is admitted only as a contiguous suffix of one
// terminated run's rendered committed output. Rows with an in-frame
// parent keep the r9 sequence rule (pinned by the kept r9 controls).
// No cross-frame retained rows, no glyph lists, no timers.

import (
	"context"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// museR10TerminatedEcho appends a started/message/terminal run with
// the given committed text. No created response is emitted: the
// captured message-without-created shape, so the response gate stays
// out of the screen tests' way.
func museR10TerminatedEcho(t *testing.T, h *musePTYHost, run, text string) {
	t.Helper()
	museEmitRunBounded(t, h, "started", run)
	museR10EmitMessage(t, h, run, "resp-"+run, museTestUUID("msg-"+run), text)
	museEmitRunBounded(t, h, "terminal", run)
}

// Log-backed off-frame rows admit: the leading indented run is a
// contiguous suffix of one terminated run's rendered output.
func TestMusePTYR10LogBackedOffFrameAdmits(t *testing.T) {
	setup := func(t *testing.T) (*musePTYHost, string) {
		t.Helper()
		h, home := musePeer(t, "idle")
		museR10TerminatedEcho(t, h, museTestUUID("echo-run"), "echo: alpha\nbeta\ngamma")
		return h, home
	}
	t.Run("tail_suffix", func(t *testing.T) {
		h, home := setup(t)
		museShowFrame(t, h, "  beta", "  gamma")
		museExpectDelivery(t, h, home, "backed tail delivers")
	})
	t.Run("single_last_row", func(t *testing.T) {
		h, home := setup(t)
		museShowFrame(t, h, "  gamma")
		museExpectDelivery(t, h, home, "backed last row delivers")
	})
	t.Run("older_terminated_run_backs", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museR10TerminatedEcho(t, h, museTestUUID("older-run"), "echo: a1\na2")
		museR10TerminatedEcho(t, h, museTestUUID("newer-run"), "echo: b1\nb2")
		// A transcript tail from an older terminated run is
		// still genuine transcript; any terminated run backs.
		museShowFrame(t, h, "  a2")
		museExpectDelivery(t, h, home, "older run backs")
	})
	t.Run("in_frame_parent_needs_no_log", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		// No runs at all: rows with an in-frame parent keep
		// the r9 sequence rule without any log backing.
		museShowFrame(t, h, "❯ first harvest line", "  wrapped tail")
		museExpectDelivery(t, h, home, "in-frame parent delivers")
	})
}

// Anything else is unknown state and defers: no matching committed
// output, a gap, a reordered or foreign line, rows mixed across runs.
func TestMusePTYR10LogBackedOffFrameRefuses(t *testing.T) {
	setup := func(t *testing.T) (*musePTYHost, string) {
		t.Helper()
		h, home := musePeer(t, "idle")
		museR10TerminatedEcho(t, h, museTestUUID("echo-run"), "echo: alpha\nbeta\ngamma")
		return h, home
	}
	t.Run("foreign_row", func(t *testing.T) {
		h, home := setup(t)
		museShowFrame(t, h, "  foreign line")
		museR9Refuse(t, h, home)
	})
	t.Run("reordered_suffix", func(t *testing.T) {
		h, home := setup(t)
		museShowFrame(t, h, "  gamma", "  beta")
		museR9Refuse(t, h, home)
	})
	t.Run("gap_breaks_contiguity", func(t *testing.T) {
		h, home := setup(t)
		museShowFrame(t, h, "  beta", "  foreign", "  gamma")
		museR9Refuse(t, h, home)
	})
	t.Run("segment_longer_than_output", func(t *testing.T) {
		h, home := setup(t)
		museShowFrame(t, h, "  beta", "  gamma", "  extra")
		museR9Refuse(t, h, home)
	})
	t.Run("mixed_runs", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museR10TerminatedEcho(t, h, museTestUUID("older-run"), "echo: a1\na2")
		museR10TerminatedEcho(t, h, museTestUUID("newer-run"), "echo: b1\nb2")
		// Each row matches some run, but no single run's
		// output contains both in order.
		museShowFrame(t, h, "  a2", "  b2")
		museR9Refuse(t, h, home)
	})
	t.Run("long_line_fails_closed", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		wide := strings.Repeat("w", 119)
		museR10TerminatedEcho(t, h, museTestUUID("wide-run"), "echo: a\n"+wide+"\nzzz")
		// The short tail row matches, but the run carries a
		// line with no captured rendering, so the run backs
		// nothing.
		museShowFrame(t, h, "  zzz")
		museR9Refuse(t, h, home)
	})
	t.Run("active_run_output_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museEmitRunBounded(t, h, "started", museTestUUID("active-run"))
		museR10EmitMessage(t, h, museTestUUID("active-run"), "resp-active", museTestUUID("msg-active"), "echo: alpha\nbeta")
		// The run never terminated. The refusal is the log
		// gate (an active run is never idle), which dominates
		// the screen gate; the screen branch itself is pinned
		// by TestMusePTYR10LogBackedRowsSkipsActiveRun below.
		museShowFrame(t, h, "  beta")
		museR9Refuse(t, h, home)
	})
}

// The r9 frame-local rule rejected this real idle shape (round-9
// panel A): after a legitimate 28-line Echo turn the parent of the
// first in-frame continuation has scrolled off. The synthetic mirror
// of that capture admits under log-backed provenance and delivers.
func TestMusePTYR10SyntheticLongTurnAdmits(t *testing.T) {
	h, home := musePeer(t, "idle")
	line := "ordinary offline echo line"
	text := "echo: " + line + strings.Repeat("\n"+line, 27)
	museR10TerminatedEcho(t, h, museTestUUID("long-run"), text)
	rows := make([]string, 0, 35)
	for i := 0; i < 6; i++ {
		rows = append(rows, "  "+line)
	}
	rows = append(rows, "")
	rows = append(rows, "◆ echo: "+line)
	for i := 0; i < 27; i++ {
		rows = append(rows, "  "+line)
	}
	museShowFrame(t, h, rows...)
	museExpectDelivery(t, h, home, "long backed turn delivers")
}

// Narrow pins for branches DeliverSessionNotice cannot isolate: a nil
// fold backs nothing, an active run's output never backs, and a line
// wider than the viewport has no captured rendering.
func TestMusePTYR10LogBackedRowsSkipsActiveRun(t *testing.T) {
	fold := &museLogFold{
		RunLifecycle: map[string]museRunState{museTestUUID("active-run"): museRunActive},
		Messages:     map[string]string{museTestUUID("active-run"): "echo: alpha\nbeta"},
	}
	if museLogBackedRowsOK([]string{"  beta"}, 120, fold) {
		t.Fatal("active run backed an off-frame row")
	}
	if museLogBackedRowsOK([]string{"  beta"}, 120, nil) {
		t.Fatal("nil fold backed an off-frame row")
	}
	if museLogBackedRowsOK(nil, 120, fold) {
		t.Fatal("empty segment backed")
	}
	terminated := &museLogFold{
		RunLifecycle: map[string]museRunState{museTestUUID("done-run"): museRunTerminated},
		Messages:     map[string]string{museTestUUID("done-run"): "echo: alpha\nbeta"},
	}
	if !museLogBackedRowsOK([]string{"  beta"}, 120, terminated) {
		t.Fatal("terminated run refused its own tail")
	}
	if _, ok := museRenderCommittedOutput("echo: a\n"+strings.Repeat("w", 119), 120); ok {
		t.Fatal("wide line rendered without a capture")
	}
	rendered, ok := museRenderCommittedOutput("echo: alpha\nbeta", 120)
	if !ok || len(rendered) != 2 || rendered[0] != "◆ echo: alpha" || rendered[1] != "  beta" {
		t.Fatalf("captured wrap misrendered: %q %v", rendered, ok)
	}
}

// The real 28-line Echo turn as a positive control: content asserted,
// then a notice delivers into the idle long transcript, then a short
// followup turn proves the host is fully functional. Ports panel A's
// TestR141R9RealLongIdleTranscript, which the r9 frame-local rule
// refused (the r9 brief's Stop-The-Line trigger).
func TestMusePTYR10RealLongIdleTranscriptDelivers(t *testing.T) {
	h, ctx := museRealEchoHost(t, "")
	text := strings.TrimSuffix(strings.Repeat("ordinary offline echo line\n", 28), "\n")
	result, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: text})
	if err != nil || result == nil || result.Status != sm.ProviderRunIdle || result.Output != "echo: "+text {
		t.Fatalf("long Echo content failed: %+v %v", result, err)
	}
	settle, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	museSmokeBoundedWait(t, settle, "real long transcript idle", func() bool { return h.idle("") })
	if err := h.DeliverSessionNotice(ctx, "notice after legitimate completed long transcript"); err != nil {
		h.mu.Lock()
		screen := strings.Join(h.vt.lines(), "\n")
		t.Logf("live unknown=%v pending=%q title=%q screen=%q", h.vt.unknown, h.vt.pending, h.vt.title, screen)
		h.mu.Unlock()
		t.Fatalf("legitimate stable idle Echo transcript cannot accept next notice: %v", err)
	}
	museSmokeBoundedWait(t, ctx, "real idle after long notice", func() bool { return h.idle("") })
	followup, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: "short followup"})
	if err != nil || followup == nil || followup.Status != sm.ProviderRunIdle || followup.Output != "echo: short followup" {
		t.Fatalf("followup echo %+v: %v", followup, err)
	}
}
