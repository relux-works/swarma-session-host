package musepty

// Revision r12: global envelope uniqueness, every pending map in idle,
// resolved resume references, width by allowlist. Every round-11 panel
// reproduction is ported here as a named production-entry test through
// DeliverSessionNotice. Sources:
//   - delta (regression): TestDeltaR11* in the verdict appendix
//   - panel A (regression): TestPanelR11* in
//     regression_probes_test.go
//   - panel B (regression): TestPanelR11* in the verdict appendix
//
// The frame digest CONTENT stays unverified as a pinned Stop-The-Line
// admission (r11, kept): no derivation from the captured children
// reproduces the captured digests, and refusing real frames would
// brick real sessions. The digest SPELLING is pinned and fails
// closed.

import (
	"testing"
)

// One accepted envelope attests one intent: two intents sharing an
// accepted envelope id and sequence are unknown state. Ports panel B
// TestPanelR11AliasedAcceptedEnvelopeMustRefuse verbatim.
func TestMusePTYR12AliasedAcceptedEnvelopeMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	for _, run := range []string{museTestUUID("panel-intent-A"), museTestUUID("panel-intent-B")} {
		a := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", ""))
		a["id"] = museTestUUID("shared-accepted-envelope")
		a["sequence"] = 100
		a["payload"].(map[string]any)["intent_id"] = run
		appendMuseRecord(t, h, a)
		s := museOwnedRecord(h, museFixtureRecord("started"))
		s["id"] = museTestUUID("started-" + run)
		p := s["payload"].(map[string]any)
		p["run_id"] = run
		p["source_run_record_id"] = museTestUUID("source-" + run)
		appendMuseRecord(t, h, s)
		appendMuseRecord(t, h, museR9TopLevelOutcome(t, h, a, s))
		museEmitRunBounded(t, h, "terminal", run)
	}
	museR9Refuse(t, h, home)
}

// The alias rule is global: an id repeat across record kinds is
// unknown state too, not just within one kind.
func TestMusePTYR12CrossKindEnvelopeAliasMustRefuse(t *testing.T) {
	h, home := musePeer(t, "idle")
	s := museOwnedRecord(h, museFixtureRecord("started"))
	s["payload"].(map[string]any)["run_id"] = museTestUUID("cross-kind-run")
	s["id"] = museTestUUID("r12-cross-kind-envelope")
	appendMuseRecord(t, h, s)
	e := museOwnedRecord(h, museFixtureRecord("terminal"))
	e["payload"].(map[string]any)["run_id"] = museTestUUID("cross-kind-run")
	e["id"] = museTestUUID("r12-cross-kind-envelope")
	appendMuseRecord(t, h, e)
	museR9Refuse(t, h, home)
}

// Unique envelope identities still deliver. Ports panel B
// TestPanelR11AlignedIdentityControl verbatim.
func TestMusePTYR12AlignedIdentityControl(t *testing.T) {
	h, home := musePeer(t, "idle")
	for _, run := range []string{museTestUUID("control-A"), museTestUUID("control-B")} {
		a := museOwnedRecord(h, museCorpusRecord(t, "runtime.user_intent.accepted", "", ""))
		a["id"] = museTestUUID("accepted-" + run)
		a["payload"].(map[string]any)["intent_id"] = run
		appendMuseRecord(t, h, a)
		s := museOwnedRecord(h, museFixtureRecord("started"))
		s["id"] = museTestUUID("started-" + run)
		p := s["payload"].(map[string]any)
		p["run_id"] = run
		p["source_run_record_id"] = museTestUUID("source-" + run)
		appendMuseRecord(t, h, s)
		appendMuseRecord(t, h, museR9TopLevelOutcome(t, h, a, s))
		museEmitRunBounded(t, h, "terminal", run)
	}
	museExpectDelivery(t, h, home, "aligned identity control")
}
