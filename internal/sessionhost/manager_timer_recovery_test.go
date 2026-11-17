package sessionhost

import (
	"context"
	"errors"
	"testing"
)

func reopenManagerTimerFixture(t *testing.T, e *managerTimerEngine) *managerTimerEngine {
	t.Helper()
	witness := e.store.witness
	if err := e.store.Close(); err != nil {
		t.Fatal(err)
	}
	s, _, err := openManagerTimerStore(witness, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	result, err := newManagerTimerEngine(s, e.gate, e.ports)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// Real fixture close/reopen preserves uncertain immutable payloads and IDs.
// Cleanup works with primary cleared and STOP still set; Query alone resolves
// lost effects without rereading a changed goal or sending Reserve/Commit again.
func TestManagerTimerRestartUncertaintyAndCleanup(t *testing.T) {
	for _, op := range []string{"reserve", "commit", "release"} {
		t.Run(op, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			f.drop = op
			if op == "release" {
				f.goalAfterReserve = "cleared"
			}
			if err := e.poll(context.Background()); err != managerTimerQueueUnknown {
				t.Fatalf("lost %s=%v", op, err)
			}
			intent := timerFixtureClone(timerObject(e.ledger, "active"))
			e = reopenManagerTimerFixture(t, e)
			if !timerEqual(intent, timerObject(e.ledger, "active")) || timerNumber(e.ledger, "opportunity_high_water") != 1 {
				t.Fatal("reopen changed immutable uncertain intent")
			}
			f.owner.primary.state = "cleared"
			f.owner.input = InputSecurityStop
			e.ports.snapshot = func(context.Context, string) (string, error) {
				t.Error("recovery reread goal")
				return "", errors.New("unexpected_read")
			}
			f.missingMapping = true
			if err := e.recover(context.Background()); err != managerTimerQueueUnknown || e.ledger["active"] == nil {
				t.Fatalf("missing map=%v", err)
			}
			f.missingMapping = false
			if err := e.recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if op == "commit" {
				if err := e.recordUseful(f.useful()); err != nil {
					t.Fatal(err)
				}
				if err := e.ack(context.Background(), managerTimerAckPayload(e)); err != nil {
					t.Fatal(err)
				}
			}
			if e.ledger["active"] != nil || f.snapshots != 1 || f.counts["reserve"] != 1 || f.counts["commit"] > 1 || f.counts["release"] > 1 {
				t.Fatalf("restart resent an effect: %v", f.counts)
			}
			if f.owner.input != InputSecurityStop || f.owner.primary.state != "cleared" {
				t.Fatal("cleanup altered ordinary owner STOP/primary")
			}
		})
	}
}

// Crashing after preparing, or losing directory-sync acknowledgement after a
// successful rename, consumes that ordinal permanently. Reopen closes only the
// known pre-provider attempt and never resumes its snapshot continuation.
func TestManagerTimerRestartAfterPreparing(t *testing.T) {
	for _, where := range []string{"pre_snapshot", "directory_sync"} {
		t.Run(where, func(t *testing.T) {
			e, f := managerTimerEngineFixture(t)
			if where == "pre_snapshot" {
				e.ports.snapshot = func(context.Context, string) (string, error) { panic("fixture_crash_before_snapshot") }
				crashed := false
				func() {
					defer func() {
						if p := recover(); p == "fixture_crash_before_snapshot" {
							crashed = true
						} else if p != nil {
							panic(p)
						}
					}()
					_ = e.poll(context.Background())
				}()
				if !crashed {
					t.Fatal("crash plant not applied")
				}
			} else {
				e.store.durability = func(stage string) error {
					if stage == "replace_directory" {
						return errors.New("fixture_sync_failure")
					}
					return nil
				}
				if err := e.poll(context.Background()); err != managerTimerIOUnavailable {
					t.Fatalf("directory sync=%v", err)
				}
			}
			e = reopenManagerTimerFixture(t, e)
			if err := e.recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if timerNumber(e.ledger, "opportunity_high_water") != 1 || e.ledger["active"] != nil {
				t.Fatal("preparing replay reset high-water or retained callback")
			}
			if err := e.poll(context.Background()); err != managerTimerNotDue {
				t.Fatalf("reopened consumption=%v", err)
			}
			if f.snapshots != 0 || f.counts["reserve"] != 0 || f.turns != 0 {
				t.Fatal("reopen replayed snapshot/effect")
			}
		})
	}
}
