package musepty

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

func TestPanelAPartialMetadataMustRefuse(t *testing.T) {
	for _, typ := range []string{"runtime.session.permission_profile_committed", "runtime.user_intent.accepted", "session.name.changed", "session.opened.observed"} {
		t.Run(typ, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			appendMuseRecord(t, h, map[string]any{"payload_type": typ, "stream": map[string]any{"id": museTestUUID("peer-session")}, "payload": map[string]any{"kind": "future_unknown_kind"}})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := h.DeliverSessionNotice(ctx, "must refuse partial metadata")
			if err != ErrMusePTYDeferred {
				b, _ := os.ReadFile(filepath.Join(home, "peer-submit"))
				t.Fatalf("partial metadata admitted: %v submitted=%q", err, b)
			}
		})
	}
}
func TestPanelATerminalTextMustNotOverrideCommittedMessage(t *testing.T) {
	h, _ := musePeer(t, "idle")
	for _, kind := range []string{"started", "assistant_message_committed", "terminal"} {
		rec := museFixtureRecord(kind)
		if kind == "terminal" {
			rec["payload"].(map[string]any)["event"].(map[string]any)["text"] = "BLOCKED: invented terminal field"
		}
		appendMuseRecord(t, h, rec)
	}
	f, err := h.fold()
	if err != nil {
		return
	}
	for _, term := range f.Terminals {
		if museTextResult(term).Status == sm.ProviderRunBlocked {
			t.Fatal("uncaptured terminal.text overrode captured committed message")
		}
	}
}

func TestPanelATerminalErrorShapeIsUncaptured(t *testing.T) {
	h, home := musePeer(t, "idle")
	if e := os.WriteFile(filepath.Join(home, "peer-terminal"), []byte(`{"terminal":"failed","error":{"kind":"authRequired","message":"synthetic cause","retryable":true}}`), 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := h.RunOwner(ctx, sm.ProviderRunRequest{Prompt: "probe uncaptured error shape"})
	if err == nil && strings.Contains(result.ProviderStatus, "authRequired") {
		t.Fatalf("uncaptured terminal.error accepted as authoritative: %+v", result)
	}
}
func TestPanelBPartialMetadataRefuses(t *testing.T) {
	for _, kind := range []string{"runtime.session.permission_format_declared", "runtime.session.permission_profile_committed", "session.name.changed", "runtime.user_intent.accepted", "runtime.user_intent.materialized", "session.opened.observed"} {
		t.Run(kind, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			appendMuseRecord(t, h, map[string]any{"payload_type": kind, "stream": map[string]any{"id": h.NativeIdentity()}, "payload": map[string]any{"kind": "future_unparsed_kind"}})
			assertMuseNoInput(t, h, home)
		})
	}
}
func TestPanelBPartialNonemptyMetadataRefuses(t *testing.T) {
	h, home := musePeer(t, "idle")
	appendMuseRecord(t, h, map[string]any{"payload_type": "runtime.session.metadata", "payload": map[string]any{"kind": "metadata", "record": map[string]any{"unexpected": true}}})
	assertMuseNoInput(t, h, home)
}

// L3G required versus additive (regression, M56): terminal text
// and error members are additive and ignored by the fold — the committed
// message stays authoritative — so the exact-shape refusal is superseded
// and the invented members must refuse to take effect instead. The run
// delivers, and the terminal text is the committed message, never the
// invented one. The original name is kept; the assertion is intentionally
// amended, with TestAdditiveOptionalFieldTolerated as the broader pin.
func TestPanelBUncapturedTerminalShapeRefuses(t *testing.T) {
	for _, field := range []string{"text", "error"} {
		t.Run(field, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			runID := museTestUUID("panel-b-terminal-" + field)
			committed := l3gAppendAlignedRun(t, h, runID, func(kind string, record map[string]any) {
				if kind != "terminal" {
					return
				}
				event := record["payload"].(map[string]any)["event"].(map[string]any)
				if field == "text" {
					event[field] = "BLOCKED: invented terminal text"
				} else {
					event[field] = map[string]any{"kind": "authRequired", "message": "invented", "retryable": false}
				}
			})
			f, err := h.fold()
			if err != nil {
				t.Fatalf("additive terminal refused: %v", err)
			}
			if got := f.Terminals[runID].Text; got != committed {
				t.Fatalf("terminal text = %q, want committed %q", got, committed)
			}
			museExpectDelivery(t, h, home, "additive terminal control")
		})
	}
}
