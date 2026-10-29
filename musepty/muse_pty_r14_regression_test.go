package musepty

import (
	"context"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Round-14 ports: every round-13 panel reproduction drives the
// production entry point (DeliverSessionNotice via museR9Refuse /
// museExpectDelivery). Panel A =
// regression/regression_verdict.md, panel B =
// regression/regression_verdict.md.

// Duplicated or reordered chrome refuses: a header text past the first
// row and a notice text past the header are transcript by position,
// and no captured (previous, current) pair admits a continuation
// there. Ports panel A duplicate_header + duplicate_notice and panel B
// notice_after_notice + header_after_notice. Panel A's two turn-led
// probes ([turn, header], [turn, notice]) are NOT refusals: under
// positional chrome they are quoted transcript and port as positive
// controls in TestMusePTYR14QuotedChromeDelivers below (panel B F2 +
// the kept r7 header_after_turn_permits control take precedence over
// panel A's refuse expectation).
func TestMusePTYR14DuplicateChromeMustRefuse(t *testing.T) {
	notice := "  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7"
	t.Run("duplicate_header", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, museHeaderRow, museHeaderRow)
		museR9Refuse(t, h, home)
	})
	t.Run("duplicate_notice", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, museHeaderRow, notice, notice)
		museR9Refuse(t, h, home)
	})
	t.Run("header_after_notice", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, museHeaderRow, notice, museHeaderRow)
		museR9Refuse(t, h, home)
	})
}

// Chrome text inside the transcript is transcript content and must not
// break its legitimate continuation. Positive controls: panel A's two
// turn-led shapes with the corrected (deliver) expectation, panel B's
// two quoted-chrome shapes verbatim, and the r13
// notice_after_turn_refuses_child shape, whose refuse expectation
// encoded the panel-B F2 regression and inverted in r14.
func TestMusePTYR14QuotedChromeDelivers(t *testing.T) {
	notice := "  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7"
	t.Run("quoted_header_after_turn", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "❯ first harvest line", museHeaderRow)
		museExpectDelivery(t, h, home, "quoted header after turn")
	})
	t.Run("quoted_notice_after_turn", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "❯ first harvest line", notice)
		museExpectDelivery(t, h, home, "quoted notice after turn")
	})
	t.Run("quoted_notice_after_echo", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "◆ echo: quoted transcript", notice, "  next quoted line")
		museExpectDelivery(t, h, home, "quoted notice after echo")
	})
	t.Run("quoted_header_after_echo", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "◆ echo: quoted transcript", "  Muse Code 1.4.1", "  next quoted line")
		museExpectDelivery(t, h, home, "quoted header after echo")
	})
	t.Run("turn_notice_continuation", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museShowFrame(t, h, "❯ first harvest", notice, "  Future action required")
		museExpectDelivery(t, h, home, "quoted notice keeps its continuation")
	})
}

// museR14ConfiguredCitation builds a config record carrying configured
// plus a citation record citing cited, on one started run. Both values
// flow through the closed grammar; the join resolves only when both
// are well-formed and equal.
func museR14ConfiguredCitation(t *testing.T, h *musePTYHost, kind, configured, cited string) {
	t.Helper()
	run := museTestUUID("r14-format-" + kind + "-run")
	museEmitRunBounded(t, h, "started", run)
	cfg := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
	cfg["payload"].(map[string]any)["run_id"] = run
	switch kind {
	case "context":
		messages := cfg["payload"].(map[string]any)["event"].(map[string]any)["run_context_messages"].([]any)
		messages[0].(map[string]any)["id"] = configured
		appendMuseRecord(t, h, cfg)
		diag := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "context_block_diagnostic"))
		diag["payload"].(map[string]any)["run_id"] = run
		diag["payload"].(map[string]any)["event"].(map[string]any)["block_id"] = cited
		appendMuseRecord(t, h, diag)
	case "roster":
		agents := cfg["payload"].(map[string]any)["event"].(map[string]any)["reminder_roster"].(map[string]any)["agents"].([]any)
		agents[0].(map[string]any)["id"] = configured
		appendMuseRecord(t, h, cfg)
		task := museTestUUID("r14-format-roster-task")
		link := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "memory_reminder_child_session_linked"))
		p := link["payload"].(map[string]any)
		p["run_id"] = run
		e := p["event"].(map[string]any)
		e["parent_session_id"] = h.NativeIdentity()
		e["parent_run_id"] = run
		e["task_id"] = task
		e["task_stream"].(map[string]any)["id"] = task
		e["reminder_agent_id"] = cited
		appendMuseRecord(t, h, link)
		for _, k := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"} {
			r := museR7TaskRecord(t, h, k, run, task)
			event := r["payload"].(map[string]any)["event"].(map[string]any)
			if k == "scheduled" || k == "side_effect_intent" {
				event["idempotency_key"] = museTestUUID("r14-format-roster-effect")
			}
			appendMuseRecord(t, h, r)
		}
	default:
		t.Fatalf("unknown citation kind %q", kind)
	}
	museEmitRunBounded(t, h, "terminal", run)
}

// Quoted chrome text in a REAL Echo transcript delivers with a
// follow-up notice. Ports panel B
// TestPanelR13RealQuotedChromeTranscriptDelivers (settle logic via the
// smoke bounded wait; the panel's sketch named helpers that do not
// exist).
func TestMusePTYR14RealQuotedChromeTranscriptDelivers(t *testing.T) {
	h, ctx := museRealEchoHost(t, "")
	notice := "  resumed session 01a0fae9-44f2-72f0-877c-03a372db40f7"
	header := "  Muse Code 1.4.1"
	result, err := h.RunOwner(ctx, sm.ProviderRunRequest{
		Prompt: "quote both lines back: " + notice + " / " + header,
	})
	if err != nil || result == nil || result.Status != sm.ProviderRunIdle {
		t.Fatalf("quoted-chrome run: %+v %v", result, err)
	}
	settle, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	museSmokeBoundedWait(t, settle, "real quoted chrome idle", func() bool { return h.idle("") })
	h.mu.Lock()
	text := strings.Join(h.vt.lines(), "\n")
	h.mu.Unlock()
	if !strings.Contains(text, "echo: quote both lines") {
		t.Fatalf("quoted transcript missing echo:\n%s", text)
	}
	if err := h.DeliverSessionNotice(ctx, "quoted-chrome followup: notice after quoted chrome"); err != nil {
		t.Fatalf("notice after quoted chrome refused: %v", err)
	}
}
