package sessionhost

import (
	"context"
	"strings"
	"testing"
)

// Shape/booleans alone are insufficient. A missing admitted result route or a
// well-shaped receipt for another issuer/goal/turn cannot release the slot;
// afterward its exact admitted positive still ACKs successfully.
func TestManagerTimerUsefulAuthorityAndCorrelation(t *testing.T) {
	for _, which := range []string{"unverified", "check_hash", "issuer", "goal", "turn", "attempt"} {
		t.Run(which, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			if err := e.poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			good := f.useful()
			bad := timerFixtureClone(good)
			switch which {
			case "unverified":
				// Shape and exact check hash remain valid; only the route refuses.
			case "check_hash":
				timerObject(bad, "proof")["evidence_digest"] = strings.Repeat("a", 64)
			case "issuer":
				timerObject(bad, "proof")["issuer_binding_digest"] = strings.Repeat("a", 64)
			case "goal":
				bad["goal_digest"] = strings.Repeat("a", 64)
				timerObject(bad, "check")["goal_digest"] = bad["goal_digest"]
			case "turn":
				timerObject(bad, "native")["turn_id"] = "foreign-turn"
				timerObject(bad, "check")["turn_id"] = "foreign-turn"
			case "attempt":
				bad["attempt_id"] = strings.Repeat("a", 64)
				timerObject(bad, "check")["attempt_id"] = bad["attempt_id"]
			}
			verifier := e.ports.verifyUseful
			if which == "goal" || which == "turn" || which == "attempt" {
				timerObject(bad, "proof")["evidence_digest"] = timerFixtureHash(timerFixtureBytes(timerObject(bad, "check")))
			}
			if which == "unverified" {
				e.ports.verifyUseful = func(map[string]any) bool { return false }
			} else {
				e.ports.verifyUseful = func(map[string]any) bool { return true }
			}
			if err := e.recordUseful(bad); err != managerTimerAckUnproven {
				t.Fatalf("foreign useful=%v", err)
			}
			e.ports.verifyUseful = verifier
			if e.ledger["active"] == nil || timerObject(e.ledger, "active")["useful"] != nil {
				t.Fatal("invalid receipt changed retained proof/slot")
			}
			if err := e.recordUseful(good); err != nil {
				t.Fatal(err)
			}
			if err := e.recordUseful(good); err != nil {
				t.Fatalf("exact useful retry=%v", err)
			}
			ack := managerTimerAckPayload(e)
			wrong := timerFixtureClone(ack)
			wrong["completion_digest"] = strings.Repeat("a", 64)
			if err := e.ack(context.Background(), wrong); err != managerTimerAckConflict {
				t.Fatalf("wrong completion ACK=%v", err)
			}
			if e.ledger["active"] == nil {
				t.Fatal("conflicting ACK freed slot")
			}
			if err := e.ack(context.Background(), ack); err != nil {
				t.Fatal(err)
			}
			if f.turns != 1 || e.ledger["active"] != nil {
				t.Fatal("valid proof did not ACK exact one turn")
			}
		})
	}
}
