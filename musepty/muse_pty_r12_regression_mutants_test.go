package musepty

import (
	"fmt"
	"strings"
	"testing"
)

// One session envelope id attests one run: two runs sharing a started
// envelope id are unknown state, even with distinct source ids. Ports
// panel B TestPanelR11AliasedStartedIdentityMustRefuse verbatim. The
// fixed shared id is the r12-envelope-id-only mutant target.
func TestMusePTYR12AliasedStartedIdentityMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	for _, run := range []string{museTestUUID("panel-alias-A"), museTestUUID("panel-alias-B")} {
		a := museR9AcceptIntent(t, h, run)
		s := museOwnedRecord(h, museFixtureRecord("started"))
		s["id"] = museTestUUID("shared-start-envelope")
		p := s["payload"].(map[string]any)
		p["run_id"] = run
		p["source_run_record_id"] = museTestUUID("source-" + run)
		appendMuseRecord(t, h, s)
		appendMuseRecord(t, h, museR9TopLevelOutcome(t, h, a, s))
		museEmitRunBounded(t, h, "terminal", run)
	}
	museR9Refuse(t, h, home)
}

// Every sequence in the owned stream is unique: two records sharing
// a sequence are unknown state even with distinct ids. The fixed
// shared sequence is the r12-envelope-seq-only mutant target. Both
// runs terminate so the shared sequence is the sole refusal.
func TestMusePTYR12AliasedSequenceMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	a := museOwnedRecord(h, museFixtureRecord("started"))
	a["payload"].(map[string]any)["run_id"] = museTestUUID("seq-run-a")
	a["sequence"] = 424242
	appendMuseRecord(t, h, a)
	b := museOwnedRecord(h, museFixtureRecord("started"))
	b["payload"].(map[string]any)["run_id"] = museTestUUID("seq-run-b")
	b["sequence"] = 424242
	appendMuseRecord(t, h, b)
	museEmitRunBounded(t, h, "terminal", museTestUUID("seq-run-a"))
	museEmitRunBounded(t, h, "terminal", museTestUUID("seq-run-b"))
	museR9Refuse(t, h, home)
}

// A received command stays pending until its matching settle, both
// spellings alike: received-but-unsettled intake never certifies
// idle. Ports panel A TestPanelR11UnsettledIntakeMustRefuse (both
// spellings, pending and settled) and panel B
// TestPanelR11UnsettledIntakeMustRefuse (the turn_submit pending
// member). The fixed pending command id is the
// r12-intake-pending-only mutant target. The fold stays clean: pending
// intake is known-busy work, not unknown state.
func TestMusePTYR12PendingIntakeMustRefuse(t *testing.T) {
	for _, tc := range []struct{ name, typ string }{
		{"turn_submit", "runtime.command_intake.received"},
		{"session_name", "runtime.command_intake.session_name.received"},
	} {
		t.Run(tc.name+"/pending_refuses", func(t *testing.T) {
			h, home := musePeer(t, "idle")
			r := museOwnedRecord(h, museCorpusRecord(t, tc.typ, "command_intake", ""))
			r["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("r12-intake-pending-sentinel")
			appendMuseRecord(t, h, r)
			f, err := h.fold()
			if err != nil {
				t.Fatalf("pending intake misfolded: %v", err)
			}
			if f.pendingIntakeCount() != 1 || f.idle() {
				t.Fatalf("pending intake not pending: %+v", f)
			}
			museR9Refuse(t, h, home)
		})
		t.Run(tc.name+"/settled_delivers", func(t *testing.T) {
			h, home := musePeer(t, "idle")
			r := museOwnedRecord(h, museCorpusRecord(t, tc.typ, "command_intake", ""))
			r["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("r12-intake-pending-sentinel")
			appendMuseRecord(t, h, r)
			s := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
			s["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("r12-intake-pending-sentinel")
			appendMuseRecord(t, h, s)
			museExpectDelivery(t, h, home, "settled control")
		})
	}
}

// Runes outside the closed width allowlist have no established
// rendering: a committed line carrying one cannot back the screen,
// however short it counts. Ports delta
// TestDeltaR11SupplementaryWideColumnsMustRefuse (U+1B132, U+18B00,
// U+1B100) and panel A TestPanelR11UnlistedWideCharactersMustRefuse
// (U+18B00, U+18D00, U+1B170). U+1B132 is the
// r12-width-allowlist-only mutant target. The captured U+2502 member
// still backs its tail.
func TestMusePTYR12UnestablishedWidthMustRefuse(t *testing.T) {
	for _, r := range []rune{0x1B132, 0x18B00, 0x1B100, 0x18D00, 0x1B170} {
		t.Run(fmt.Sprintf("U+%X", r), func(t *testing.T) {
			h, home := musePeer(t, "idle")
			line := strings.Repeat(string(r), 80)
			museR10TerminatedEcho(t, h, museTestUUID("supplementary-wide"), "echo: a\n"+line+"\nzzz")
			museShowFrame(t, h, "  zzz")
			museR9Refuse(t, h, home)
		})
	}
	t.Run("captured_box_vertical_backs", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		museR10TerminatedEcho(t, h, museTestUUID("box-run"), "echo: a\nx│y\nzzz")
		museShowFrame(t, h, "  zzz")
		museExpectDelivery(t, h, home, "allowlisted tail delivers")
	})
}

// An allowlisted line wider than the viewport still has no captured
// rendering: 119 columns refuse, 118 deliver. The fixed sentinel
// prefix is the r12-width-bound-only mutant target.
func TestMusePTYR12OverwideASCIILineMustRefuse(t *testing.T) {
	t.Run("sentinel_119_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		line := "r12-bound-sentinel-" + strings.Repeat("w", 100)
		if w, ok := museDisplayWidth(line); !ok || w != 119 {
			t.Fatalf("sentinel setup measures %d,%v, want 119,true", w, ok)
		}
		museR10TerminatedEcho(t, h, museTestUUID("bound-run"), "echo: a\n"+line+"\nzzz")
		museShowFrame(t, h, "  zzz")
		museR9Refuse(t, h, home)
	})
	t.Run("boundary_118_delivers", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		line := strings.Repeat("w", 118)
		if w, ok := museDisplayWidth(line); !ok || w != 118 {
			t.Fatalf("boundary setup measures %d,%v, want 118,true", w, ok)
		}
		museR10TerminatedEcho(t, h, museTestUUID("bound-run"), "echo: a\n"+line+"\nzzz")
		museShowFrame(t, h, "  zzz")
		museExpectDelivery(t, h, home, "boundary tail delivers")
	})
}

// A resumed record clears its pending reopen only when its record
// and sequence references resolve exactly to a record of the prior
// epoch. Ports panel A TestPanelR11ResumeReferencesMustResolve: the
// wrong-record and wrong-sequence members refuse, and so does the
// captured fragment panel A ran as a delivering control — its target
// sits outside any peer log, so under the r12 structural rule it is
// unknown state, not a positive control. The positive controls are
// the resolved member below and the real 1.4.1 resume smoke, whose
// captured reference resolves to the prior run terminal.
func TestMusePTYR12ResumeReferenceMustResolve(t *testing.T) {
	setup := func(t *testing.T) (*musePTYHost, string, string, int, map[string]any) {
		t.Helper()
		h, home := musePeer(t, "idle")
		targetID, targetSeq := museResumeTarget(t, h)
		opened := museOwnedRecord(h, museFixtureRecord("session.opened.observed"))
		return h, home, targetID, targetSeq, opened
	}
	t.Run("wrong_record_refuses", func(t *testing.T) {
		h, home, targetID, targetSeq, opened := setup(t)
		appendMuseRecord(t, h, opened)
		r := museResumedRecord(t, h, targetID, targetSeq)
		// Fixed sentinel id: the r12-resume-id-only mutant admits
		// the resumed record for exactly this reference.
		r["payload"].(map[string]any)["record"].(map[string]any)["resumed_from_record_id"] = museTestUUID("never-observed-prior-record")
		appendMuseRecord(t, h, r)
		museR9Refuse(t, h, home)
	})
	t.Run("wrong_sequence_refuses", func(t *testing.T) {
		h, home, targetID, targetSeq, opened := setup(t)
		appendMuseRecord(t, h, opened)
		r := museResumedRecord(t, h, targetID, targetSeq)
		// Fixed sentinel sequence: the r12-resume-seq-only mutant
		// admits the resumed record for exactly this reference.
		r["payload"].(map[string]any)["record"].(map[string]any)["resumed_from_sequence"] = 999999
		appendMuseRecord(t, h, r)
		museR9Refuse(t, h, home)
	})
	t.Run("empty_citation_refuses", func(t *testing.T) {
		h, home, targetID, targetSeq, opened := setup(t)
		appendMuseRecord(t, h, opened)
		r := museResumedRecord(t, h, targetID, targetSeq)
		r["payload"].(map[string]any)["record"].(map[string]any)["resumed_from_record_id"] = ""
		appendMuseRecord(t, h, r)
		museR9Refuse(t, h, home)
	})
	t.Run("current_epoch_refuses", func(t *testing.T) {
		h, home, _, _, opened := setup(t)
		// Fixed sentinel id: the r12-resume-epoch-only mutant
		// admits the resumed record for exactly this reference.
		opened["id"] = museTestUUID("r12-resume-epoch-sentinel")
		appendMuseRecord(t, h, opened)
		r := museResumedRecord(t, h, opened["id"].(string), opened["sequence"].(int))
		appendMuseRecord(t, h, r)
		museR9Refuse(t, h, home)
	})
	t.Run("captured_fragment_refuses", func(t *testing.T) {
		h, home, _, _, opened := setup(t)
		appendMuseRecord(t, h, opened)
		// Panel A's captured_control shape, inverted by the r12
		// rule: the fragment target is unobserved in this log.
		appendMuseRecord(t, h, museOwnedRecord(h, museCorpusRecord(t, "session.resumed", "", "")))
		museR9Refuse(t, h, home)
	})
	t.Run("resolved_delivers", func(t *testing.T) {
		h, home, targetID, targetSeq, opened := setup(t)
		appendMuseRecord(t, h, opened)
		appendMuseRecord(t, h, museResumedRecord(t, h, targetID, targetSeq))
		museExpectDelivery(t, h, home, "resume control")
	})
}
