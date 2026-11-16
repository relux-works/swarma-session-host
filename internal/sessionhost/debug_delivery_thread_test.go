package sessionhost

import (
	"testing"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

// Regression tests for a nil grant delivery and a lost native thread.

// a rejected nil delivery must
// retire the issued capability, like broken-pipe/non-pipe delivery. The nearby
// valid live-pipe control is TestFailedGrantDeliveryRevokesToken.
func TestNilGrantDeliveryRevokes(t *testing.T) {
	h, _ := debugEffectFixture(t)
	token := debugToken(t, h, "view", "writer_control")
	debugWant(t, h.debugDeliverGrant(token, nil), "input_invalid")
	if _, live := h.debug.grants[token]; live {
		t.Fatal("nil delivery retained the abandoned grant")
	}
	debugWant(t, h.debugAttachView(token), "attach_unauthorized")
}

// an authoritative transition
// losing native-thread knowledge cannot authorize an old request through a
// newly issued threadless grant. TestAttachCurrentThreadAnswerPositive is the
// paired current-thread positive; this transition uses the production validator.
func TestLostThreadCannotAnswerRetainedInteraction(t *testing.T) {
	h, e := debugEffectFixture(t)
	setCurrentThread(t, h, "thread-a")
	offered := []string{"allow", "deny"}
	id, err := h.debugObserveInteraction("thread-a", "turn-a", 73, "item/tool/requestUserInput", offered, false)
	debugWant(t, err, "")
	h.mu.Lock()
	err = h.persistTransitionLocked(ap.Transition{Checkpoint: []byte(`{}`), View: ap.View{Activity: ap.Unknown}})
	h.mu.Unlock()
	debugWant(t, err, "")
	if !validProtocolState(h.state) {
		t.Fatal("counterexample requires valid authoritative transition")
	}
	token, revision := debugWriter(t, h, "view", "writer_control", "native_response")
	before := h.debug.journal.sequence
	_, err = h.debugInput(token, debugInputIntent{Kind: "answer_interaction", ID: "lost-thread-answer", ExpectedRevision: revision, ExpectedTurn: "turn-a", Interaction: id, RequestID: 73, OptionsDigest: debugOptionsDigest(offered), Option: "allow"})
	debugWant(t, err, "interaction_stale")
	if e.count() != 0 || h.debug.journal.sequence != before || h.state.Debug.Interactions[0].State != "pending" {
		t.Fatal("unproved current thread reached reservation, journal or effect")
	}
}
