package sessionhost

import (
	"context"
	"testing"
)

// The checker observes counters, not the Host's returned label. The adjacent
// active control must work; absent/cleared must have no snapshot or effect.
// P12A models a defective source adapter that presents active regardless of the
// authoritative fictional primary. It establishes no actual board authority.
func TestManagerTimerGoalPresenceControlPlant(t *testing.T) {
	for _, goal := range []string{"active", "absent", "cleared"} {
		t.Run(goal, func(t *testing.T) {
			for _, planted := range []bool{false, true} {
				e, f := managerTimerEngineFixture(t)
				f.owner.primary.state = goal
				if planted {
					observe := e.ports.observe
					e.ports.observe = func(ctx context.Context, id string) (managerTimerOwnerObservation, *managerTimerClaimObservation, error) {
						o, c, err := observe(ctx, id)
						o.primary.state = "active" // Known violation: bypass only presence.
						return o, c, err
					}
				}
				err := e.poll(context.Background())
				if goal == "active" {
					if err != nil || f.snapshots != 1 || f.claims != 1 || f.items != 1 || f.turns != 1 {
						t.Fatalf("plant broke valid control: %v", err)
					}
					continue
				}
				if err != managerTimerInputIneligible {
					t.Fatalf("wrong failure, not a detected presence violation: %v", err)
				}
				violated := f.snapshots != 0 || f.counts["reserve"] != 0 || f.claims != 0 || f.items != 0 || f.turns != 0
				if violated != planted {
					t.Fatalf("plant=%v snapshot=%d Reserve=%d claim=%d item=%d turn=%d", planted, f.snapshots, f.counts["reserve"], f.claims, f.items, f.turns)
				}
				if planted && (f.snapshots != 1 || f.counts["reserve"] != 1 || f.turns != 0) {
					t.Fatal("presence plant failed for an unrelated reason")
				}
			}
		})
	}
}

// P12B deliberately removes only the fictional provider's goal invalidation
// at start. The checker rejects its FIRST stale turn, independently of a
// duplicate-turn checker. Correct suppression and current start both pass.
func TestManagerTimerStaleFirstTurnControlPlant(t *testing.T) {
	for _, cleared := range []bool{false, true} {
		for _, planted := range []bool{false, true} {
			e, f := managerTimerEngineFixture(t)
			f.commitDisposition = "queued"
			if err := e.poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if f.turns != 0 || f.items != 1 {
				t.Fatal("setup did not retain one unstarted item")
			}
			if cleared {
				f.owner.primary.state = "cleared"
				f.invalidated = true
			}
			if planted {
				state, invalidated := f.owner.primary.state, f.invalidated
				f.owner.primary.state = "active"
				f.invalidated = false // Known violation: stale cached active.
				f.startQueued()
				f.owner.primary.state = state
				f.invalidated = invalidated
			} else {
				f.startQueued()
			}
			f.owner.primary.state = "active"
			f.startQueued()
			if f.turns > 1 {
				t.Fatal("plant detected a duplicate instead of the first stale turn")
			}
			staleFirst := cleared && f.turns != 0
			if staleFirst != (cleared && planted) {
				t.Fatalf("clear=%v plant=%v turns=%d", cleared, planted, f.turns)
			}
			if !cleared && f.turns != 1 {
				t.Fatal("valid current-start neighbor refused")
			}
		}
	}
}

// A lost suppression proof is uncertainty, even with current goal absent. No
// caller may turn goal absence or a lost response into a freed pending slot.
func TestManagerTimerLostSuppressionProof(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	f.commitDisposition = "queued"
	if err := e.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.owner.primary.state = "cleared"
	f.startQueued()
	f.drop = "query"
	if err := e.recover(context.Background()); err != managerTimerQueueUnknown {
		t.Fatalf("lost suppression=%v", err)
	}
	if e.ledger["active"] == nil || f.turns != 0 {
		t.Fatal("lost proof freed pending work or started stale item")
	}
	if err := e.poll(context.Background()); err != managerTimerCoalesced {
		t.Fatalf("unknown flush=%v", err)
	}
	if err := e.recover(context.Background()); err != managerTimerInputIneligible {
		t.Fatalf("proven suppression=%v", err)
	}
	if e.ledger["active"] != nil || f.turns != 0 {
		t.Fatal("proven zero-turn suppression did not resolve exact item")
	}
}

// A later new primary/interval can progress after proven suppression. Permanent
// invalidation belongs to the old opportunity, never to all future work.
func TestManagerTimerFreshIntervalAfterSuppression(t *testing.T) {
	e, f := managerTimerEngineFixture(t)
	f.goalAfterReserve = "cleared"
	if err := e.poll(context.Background()); err != managerTimerInputIneligible {
		t.Fatal(err)
	}
	if e.ledger["active"] != nil || f.turns != 0 {
		t.Fatal("old claim was not cleanly suppressed")
	}
	f.goalAfterReserve = ""
	f.owner.primary.state = "active"
	f.owner.primary.revision++
	f.owner.clock["monotonic_ns"] = e.ledger["next_due_ns"]
	f.owner.clock["observed_at"] = "2026-11-17T17:45:00Z"
	if err := e.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.snapshots != 2 || f.claims != 2 || f.items != 1 || f.turns != 1 || timerNumber(e.ledger, "opportunity_high_water") != 2 {
		t.Fatal("fresh interval refused or old opportunity revived")
	}
	if err := e.recordUseful(f.useful()); err != nil {
		t.Fatal(err)
	}
	if err := e.ack(context.Background(), managerTimerAckPayload(e)); err != nil {
		t.Fatal(err)
	}
}
