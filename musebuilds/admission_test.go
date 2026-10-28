//go:build darwin || linux

package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestRampRecheckedUnderLock proves both ramp reads happen under the
// global lock: the entry read refuses a HOLD that arrived while
// waiting even when the pre-copy read would see CLEAR again, and the
// pre-copy read refuses a HOLD that arrived after the reservation.
func TestRampRecheckedUnderLock(t *testing.T) {
	t.Run("hold under the lock refuses despite a later clear", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		f.lock.onAcquire = func() { f.writeRamp("ramp_down=1\n") }
		reserves := 0
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeAfterReserve {
				reserves++
				f.writeRamp("ramp_down=0\n")
			}
		})
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeRampHold, StageAdmit)
		if reserves != 0 {
			t.Fatalf("reservation ran %d times past the entry refusal", reserves)
		}
		if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 0 {
			t.Fatalf("ledger = %v err=%v, want no reservation", entries, err)
		}
	})

	t.Run("hold before the copy refuses and fails the slot", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeAfterReserve {
				f.writeRamp("ramp_down=1\n")
			}
		})
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeRampHold, StageAdmit)
		entries, err := os.ReadDir(f.ledger)
		if err != nil || len(entries) != 1 {
			t.Fatalf("ledger = %v err=%v, want the failed reservation", entries, err)
		}
		reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Outcome != outcomeFailed {
			t.Fatalf("outcome = %s, want failed", reservation.Outcome)
		}
		if _, err := os.Stat(frozenBinaryPath(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256), discovery.Identity.Build)); !os.IsNotExist(err) {
			t.Fatalf("copy started past the pre-copy refusal: %v", err)
		}
	})

	t.Run("unknown before the copy refuses and fails the slot", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeAfterReserve {
				_ = os.Remove(f.rampFile)
			}
		})
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeRampUnknown, StageAdmit)
		entries, err := os.ReadDir(f.ledger)
		if err != nil || len(entries) != 1 {
			t.Fatalf("ledger = %v err=%v, want the failed reservation", entries, err)
		}
		reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Outcome != outcomeFailed {
			t.Fatalf("outcome = %s, want failed", reservation.Outcome)
		}
		if _, err := os.Stat(frozenBinaryPath(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256), discovery.Identity.Build)); !os.IsNotExist(err) {
			t.Fatalf("copy started past the pre-copy refusal: %v", err)
		}
	})

	t.Run("steady hold refuses", func(t *testing.T) {
		f := newFreeze(t)
		f.writeRamp("ramp_down=1\n")
		_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeRampHold, StageAdmit)
	})

	t.Run("steady clear publishes", func(t *testing.T) {
		f := newFreeze(t)
		result, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomePublished {
			t.Fatalf("outcome = %s, want published", result.Outcome)
		}
	})
}

// TestUnknownRampFailsClosed proves every UNKNOWN ramp shape refuses
// with its own code: a signal that cannot vouch for CLEAR is never
// treated as CLEAR, and nothing is reserved.
func TestUnknownRampFailsClosed(t *testing.T) {
	steady := func(f *freezeFixture) {}
	cells := map[string]func(f *freezeFixture){
		"missing state file": func(f *freezeFixture) { _ = os.Remove(f.rampFile) },
		"missing key":        func(f *freezeFixture) { f.writeRamp("ingress_down=0\n") },
		"value two":          func(f *freezeFixture) { f.writeRamp("ramp_down=2\n") },
		"empty value":        func(f *freezeFixture) { f.writeRamp("ramp_down=\n") },
		"padded value":       func(f *freezeFixture) { f.writeRamp("ramp_down=0 \n") },
		"oversized state":    func(f *freezeFixture) { f.writeRamp("ramp_down=0\n" + strings.Repeat("#", rampStateMaxBytes)) },
		"stale state mtime":  func(f *freezeFixture) { f.chtimes(f.rampFile, f.now.Add(-rampFreshness-time.Second)) },
		"future state mtime": func(f *freezeFixture) { f.chtimes(f.rampFile, f.now.Add(time.Hour)) },
		"missing log":        func(f *freezeFixture) { _ = os.Remove(f.rampLog) },
		"empty log":          func(f *freezeFixture) { f.writeRampLog("\n") },
		"malformed stamp":    func(f *freezeFixture) { f.writeRampLog("not-a-stamp\tcount=0\n") },
		"stale stamp": func(f *freezeFixture) {
			f.writeRampLog(f.now.Add(-rampFreshness-time.Second).Format(time.RFC3339) + "\tcount=0\n")
		},
		"future stamp":  func(f *freezeFixture) { f.writeRampLog(f.now.Add(time.Hour).Format(time.RFC3339) + "\tcount=0\n") },
		"oversized log": func(f *freezeFixture) { f.writeRampLog(strings.Repeat("x", rampLogMaxBytes+1)) },
		"fifo state": func(f *freezeFixture) {
			_ = os.Remove(f.rampFile)
			if err := syscall.Mkfifo(f.rampFile, 0o644); err != nil {
				f.t.Fatal(err)
			}
		},
		"symlink state": func(f *freezeFixture) {
			valid := filepath.Join(f.t.TempDir(), "state")
			if err := os.WriteFile(valid, []byte("ramp_down=0\n"), 0o644); err != nil {
				f.t.Fatal(err)
			}
			_ = os.Remove(f.rampFile)
			if err := os.Symlink(valid, f.rampFile); err != nil {
				f.t.Fatal(err)
			}
		},
		"fifo log": func(f *freezeFixture) {
			_ = os.Remove(f.rampLog)
			if err := syscall.Mkfifo(f.rampLog, 0o644); err != nil {
				f.t.Fatal(err)
			}
		},
		"symlink log": func(f *freezeFixture) {
			valid := filepath.Join(f.t.TempDir(), "log")
			if err := os.WriteFile(valid, []byte(f.now.Format("2006-01-02T15:04:05Z07:00")+"\tcount=0\n"), 0o644); err != nil {
				f.t.Fatal(err)
			}
			_ = os.Remove(f.rampLog)
			if err := os.Symlink(valid, f.rampLog); err != nil {
				f.t.Fatal(err)
			}
		},
		"steady clear": steady,
	}
	for name, breakRamp := range cells {
		t.Run(name, func(t *testing.T) {
			f := newFreeze(t)
			breakRamp(f)
			_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
			if name == "steady clear" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			requireFreezeCode(t, err, CodeRampUnknown, StageAdmit)
			acquires, releases, _ := f.lock.counts()
			if acquires != 1 || releases != 1 {
				t.Fatalf("lock acquires=%d releases=%d, want the under-lock read and release", acquires, releases)
			}
			if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 0 {
				t.Fatalf("ledger = %v err=%v, want no reservation", entries, err)
			}
		})
	}

	t.Run("unreadable state file", func(t *testing.T) {
		skipIfRoot(t)
		f := newFreeze(t)
		if err := os.Chmod(f.rampFile, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(f.rampFile, 0o644) })
		_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeRampUnknown, StageAdmit)
	})
}

// TestDailyFreezeQuotaIsHostWide proves the runaway guard counts
// every reservation on the host's UTC day across users and outcomes:
// the fourth new freeze deflects to the newest already-frozen build
// with the typed warning, while an unset cap and yesterday's slots
// never deflect.
func TestDailyFreezeQuotaIsHostWide(t *testing.T) {
	// seedFrozen publishes three builds and marks them PROBE_OK so
	// the deferral has verified targets to choose from.
	seedFrozen := func(t *testing.T, f *freezeFixture) map[string]string {
		t.Helper()
		digests := map[string]string{}
		for _, build := range []string{"1.4.2-R4000", "1.4.3-R5000", "1.4.4-R5419.1"} {
			result := f.publishFor(build)
			digests[build] = result.Frozen.SHA256
			key := freezeKey(build, result.Frozen.SHA256)
			info, err := os.Stat(result.Frozen.Path)
			if err != nil {
				t.Fatal(err)
			}
			f.plantState(key, BuildState{
				Schema: buildStateSchema, Build: build, SHA256: result.Frozen.SHA256,
				Size: info.Size(), Transaction: "txn-seed", ProbeContract: ProbeContractID, State: StateProbeOK,
			})
		}
		return digests
	}

	t.Run("fourth new freeze deflects across users", func(t *testing.T) {
		f := newFreeze(t)
		digests := seedFrozen(t, f)
		// The three seed publications already hold today's slots;
		// replant them across users to prove the count is shared.
		for _, build := range []string{"1.4.2-R4000", "1.4.3-R5000"} {
			name := reservationFileName("2026-10-10", build, digests[build])
			if err := os.Remove(filepath.Join(f.ledger, name)); err != nil {
				t.Fatal(err)
			}
		}
		f.plantReservationAs("2026-10-10", "1.4.2-R4000", digests["1.4.2-R4000"], outcomePublished, "night-shift")
		f.plantReservationAs("2026-10-10", "1.4.3-R5000", digests["1.4.3-R5000"], outcomePending, "day-shift")
		cfg := f.config()
		cfg.MaxFreezesPerDay = 3
		_, err := PublishFrozen(context.Background(), cfg, f.discoveryFor("1.4.5-R5500"))
		refusal := requireFreezeCode(t, err, CodeDeferralHostCap, StageAdmit)
		if refusal.Frozen == nil || refusal.Frozen.Build != "1.4.4-R5419.1" || refusal.Frozen.SHA256 != digests["1.4.4-R5419.1"] {
			t.Fatalf("deflected to %+v, want the newest frozen 1.4.4-R5419.1", refusal.Frozen)
		}
		if digestOfFile(t, refusal.Frozen.Path) != digests["1.4.4-R5419.1"] {
			t.Fatal("deflection target bytes disagree with the ledger digest")
		}
		if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 3 {
			t.Fatalf("ledger = %v err=%v, want exactly the three spent slots", entries, err)
		}
	})

	t.Run("third new freeze still publishes", func(t *testing.T) {
		f := newFreeze(t)
		seedFrozen(t, f)
		// Drop one slot so two remain spent.
		entries, err := os.ReadDir(f.ledger)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(f.ledger, entries[0].Name())); err != nil {
			t.Fatal(err)
		}
		cfg := f.config()
		cfg.MaxFreezesPerDay = 3
		result, err := PublishFrozen(context.Background(), cfg, f.discoveryFor("1.4.5-R5500"))
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomePublished {
			t.Fatalf("outcome = %s, want published", result.Outcome)
		}
	})

	t.Run("unset cap never deflects", func(t *testing.T) {
		f := newFreeze(t)
		seedFrozen(t, f)
		cfg := f.config()
		cfg.MaxFreezesPerDay = 0
		result, err := PublishFrozen(context.Background(), cfg, f.discoveryFor("1.4.5-R5500"))
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomePublished {
			t.Fatalf("outcome = %s, want published", result.Outcome)
		}
	})

	t.Run("yesterday slots do not count", func(t *testing.T) {
		f := newFreeze(t)
		digests := seedFrozen(t, f)
		for build, digest := range digests {
			name := reservationFileName("2026-10-10", build, digest)
			if err := os.Remove(filepath.Join(f.ledger, name)); err != nil {
				t.Fatal(err)
			}
			f.plantReservation("2026-10-09", build, digest, outcomePublished)
		}
		cfg := f.config()
		cfg.MaxFreezesPerDay = 3
		result, err := PublishFrozen(context.Background(), cfg, f.discoveryFor("1.4.5-R5500"))
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomePublished {
			t.Fatalf("outcome = %s, want published", result.Outcome)
		}
	})

	t.Run("nothing frozen refuses instead of deflecting", func(t *testing.T) {
		f := newFreeze(t)
		f.plantReservation("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64), outcomePublished)
		f.plantReservation("2026-10-10", "1.4.3-R5000", strings.Repeat("d", 64), outcomePublished)
		f.plantReservation("2026-10-10", "1.4.4-R5419.1", strings.Repeat("e", 64), outcomePublished)
		cfg := f.config()
		cfg.MaxFreezesPerDay = 3
		_, err := PublishFrozen(context.Background(), cfg, f.discoveryFor("1.4.5-R5500"))
		requireFreezeCode(t, err, CodeFreezeUnavailable, StageAdmit)
	})

	t.Run("newest frozen with bad state refuses", func(t *testing.T) {
		f := newFreeze(t)
		digests := seedFrozen(t, f)
		key := freezeKey("1.4.4-R5419.1", digests["1.4.4-R5419.1"])
		raw, err := os.ReadFile(frozenBinaryPath(f.root, key, "1.4.4-R5419.1"))
		if err != nil {
			t.Fatal(err)
		}
		f.plantState(key, BuildState{
			Schema: buildStateSchema, Build: "1.4.4-R5419.1", SHA256: digests["1.4.4-R5419.1"],
			Size: int64(len(raw)), Transaction: "txn-seed", ProbeContract: ProbeContractID,
			State: StateBAD, Verdict: &BadVerdict{Code: "probe_identity_mismatch", Detail: "x", RecordedAt: "2026-10-10T08:00:00Z"},
		})
		cfg := f.config()
		cfg.MaxFreezesPerDay = 3
		_, err = PublishFrozen(context.Background(), cfg, f.discoveryFor("1.4.5-R5500"))
		requireFreezeCode(t, err, CodeFreezeUnavailable, StageAdmit)
	})
}

// TestDayGuardReadFailureNeverResetsQuota proves every ledger read
// failure refuses without resetting the day guard: no reservation is
// created, no copy starts, and corrupt content is never mistaken for
// an empty day.
func TestDayGuardReadFailureNeverResetsQuota(t *testing.T) {
	breakers := map[string]func(f *freezeFixture){
		"missing directory": func(f *freezeFixture) {
			if err := os.RemoveAll(f.ledger); err != nil {
				f.t.Fatal(err)
			}
		},
		"ledger is a file": func(f *freezeFixture) {
			if err := os.RemoveAll(f.ledger); err != nil {
				f.t.Fatal(err)
			}
			f.writeFileSync(f.ledger, []byte("x"), 0o644)
		},
		"foreign file": func(f *freezeFixture) {
			f.writeFileSync(filepath.Join(f.ledger, "notes.txt"), []byte("x"), 0o644)
		},
		"foreign directory": func(f *freezeFixture) {
			if err := os.Mkdir(filepath.Join(f.ledger, "stash"), 0o700); err != nil {
				f.t.Fatal(err)
			}
		},
		"corrupt json": func(f *freezeFixture) {
			f.writeFileSync(filepath.Join(f.ledger, reservationFileName("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64))), []byte(`{"schema":`), 0o600)
		},
		"duplicate keys": func(f *freezeFixture) {
			f.writeFileSync(filepath.Join(f.ledger, reservationFileName("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64))),
				[]byte(`{"schema":1,"schema":1,"host":"h","user":"u","build":"1.4.2-R4000","sha256":"`+strings.Repeat("c", 64)+`","transaction":"t","day":"2026-10-10","reserved_at":"2026-10-10T08:00:00Z","outcome":"pending","outcome_detail":"","outcome_at":""}`), 0o600)
		},
		"content disagrees with name": func(f *freezeFixture) {
			f.writeFileSync(filepath.Join(f.ledger, reservationFileName("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64))),
				[]byte(`{"schema":1,"host":"h","user":"u","build":"1.4.3-R5000","sha256":"`+strings.Repeat("c", 64)+`","transaction":"t","day":"2026-10-10","reserved_at":"2026-10-10T08:00:00Z","outcome":"pending","outcome_detail":"","outcome_at":""}`), 0o600)
		},
		"unknown outcome": func(f *freezeFixture) {
			f.writeFileSync(filepath.Join(f.ledger, reservationFileName("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64))),
				[]byte(`{"schema":1,"host":"h","user":"u","build":"1.4.2-R4000","sha256":"`+strings.Repeat("c", 64)+`","transaction":"t","day":"2026-10-10","reserved_at":"2026-10-10T08:00:00Z","outcome":"maybe","outcome_detail":"","outcome_at":""}`), 0o600)
		},
		"future day": func(f *freezeFixture) {
			f.plantReservation("2026-10-11", "1.4.2-R4000", strings.Repeat("c", 64), outcomePublished)
		},
		"same-key corrupt content": func(f *freezeFixture) {
			discovery := f.discoveryFor("1.4.4-R5419.1")
			f.writeFileSync(filepath.Join(f.ledger, reservationFileName("2026-10-10", discovery.Identity.Build, discovery.Identity.SHA256)), []byte("garbage"), 0o600)
		},
		"fifo reservation": func(f *freezeFixture) {
			if err := syscall.Mkfifo(filepath.Join(f.ledger, reservationFileName("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64))), 0o600); err != nil {
				f.t.Fatal(err)
			}
		},
		"symlink reservation": func(f *freezeFixture) {
			planted := f.plantReservation("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64), outcomePublished)
			target := filepath.Join(f.ledger, reservationFileName(planted.Day, planted.Build, planted.SHA256))
			if err := os.Symlink(target, filepath.Join(f.ledger, reservationFileName("2026-10-10", "1.4.3-R5000", strings.Repeat("d", 64)))); err != nil {
				f.t.Fatal(err)
			}
		},
		"oversized reservation": func(f *freezeFixture) {
			f.writeFileSync(filepath.Join(f.ledger, reservationFileName("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64))),
				[]byte(`{"schema":1,`+strings.Repeat(" ", reservationMaxBytes)+`}`), 0o600)
		},
		"foreign host": func(f *freezeFixture) {
			f.plantReservationOnHost("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64), outcomePublished, "foreign-host", "test-user")
		},
	}
	for name, breakLedger := range breakers {
		t.Run(name, func(t *testing.T) {
			f := newFreeze(t)
			breakLedger(f)
			_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
			requireFreezeCode(t, err, CodeFreezeUnavailable, StageAdmit)
			if builds, err := os.ReadDir(buildsDir(f.root)); err != nil || len(builds) != 0 {
				t.Fatalf("builds = %v err=%v, want no copy", builds, err)
			}
		})
	}

	t.Run("unreadable reservation", func(t *testing.T) {
		skipIfRoot(t)
		f := newFreeze(t)
		reservation := f.plantReservation("2026-10-10", "1.4.2-R4000", strings.Repeat("c", 64), outcomePublished)
		path := filepath.Join(f.ledger, reservationFileName(reservation.Day, reservation.Build, reservation.SHA256))
		if err := os.Chmod(path, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
		_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeFreezeUnavailable, StageAdmit)
	})
}

// TestFreezeReservationExclusiveCreate proves the reservation is an
// exclusive create: a second claimant for the same day and build
// reads back the winner instead of overwriting it, and copies
// nothing under the spent slot.
func TestFreezeReservationExclusiveCreate(t *testing.T) {
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	planted := f.plantReservation("2026-10-10", discovery.Identity.Build, discovery.Identity.SHA256, outcomePending)
	_, err := PublishFrozen(context.Background(), f.config(), discovery)
	requireFreezeCode(t, err, CodeFreezeUnavailable, StageAdmit)
	entries, err := os.ReadDir(f.ledger)
	if err != nil || len(entries) != 1 {
		t.Fatalf("ledger = %v err=%v, want the single winner", entries, err)
	}
	winner, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if winner.Transaction != planted.Transaction {
		t.Fatalf("winner txn = %s, want the planted %s", winner.Transaction, planted.Transaction)
	}
	if builds, err := os.ReadDir(buildsDir(f.root)); err != nil || len(builds) != 0 {
		t.Fatalf("builds = %v err=%v, want no copy", builds, err)
	}
	if staging, err := os.ReadDir(stagingDir(f.root)); err != nil || len(staging) != 0 {
		t.Fatalf("staging = %v err=%v, want no copy", staging, err)
	}
}

// TestCreateReservationSecondCreateReadsWinner proves the reservation
// create is exclusive without relying on a race: a second create for
// the same day, build and digest reads back the winner instead of
// truncating it. The public entry always inventories first, so only
// this direct sequential create reaches the O_EXCL gate
// deterministically.
func TestCreateReservationSecondCreateReadsWinner(t *testing.T) {
	f := newFreeze(t)
	first := Reservation{
		SchemaVersion: reservationSchema,
		Host:          "test-host",
		User:          "test-user",
		Build:         "1.4.4-R5419.1",
		SHA256:        strings.Repeat("a", 64),
		Transaction:   "txn-first",
		Day:           "2026-10-10",
		ReservedAt:    "2026-10-10T08:00:00Z",
		Outcome:       outcomePending,
	}
	created, _, err := createReservation(context.Background(), f.ledger, first)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first create did not win")
	}
	second := first
	second.Transaction = "txn-second"
	created, winner, err := createReservation(context.Background(), f.ledger, second)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("second create overwrote the winner")
	}
	if winner.Transaction != "txn-first" {
		t.Fatalf("winner txn = %s, want txn-first", winner.Transaction)
	}
	winner, err = readReservation(context.Background(), filepath.Join(f.ledger, reservationFileName(first.Day, first.Build, first.SHA256)))
	if err != nil {
		t.Fatal(err)
	}
	if winner.Transaction != "txn-first" {
		t.Fatalf("stored txn = %s, want txn-first", winner.Transaction)
	}
}

// TestConcurrentFreezeReservesBeforeCopy proves the reservation holds
// before the first byte copy on every attempt: two concurrent new
// builds both publish with ordered events and no interleaved copy,
// while two concurrent same-build attempts elect exactly one winner
// through the exclusive create.
func TestConcurrentFreezeReservesBeforeCopy(t *testing.T) {
	t.Run("ordered attempt holds reserve through release", func(t *testing.T) {
		f := newFreeze(t)
		log := &freezeEventLog{}
		f.lock.log = log
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			switch point {
			case freezeAfterReserve:
				log.add("reserved", hook.Build)
			case freezeBeforeCopy:
				log.add("copy-start", hook.Build)
			case freezeAfterPublish:
				log.add("published", hook.Build)
			}
		})
		for _, build := range []string{"1.4.3-R5000", "1.4.4-R5419.1"} {
			result, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor(build))
			if err != nil {
				t.Fatal(err)
			}
			if result.Outcome != OutcomePublished {
				t.Fatalf("%s outcome = %s, want published", build, result.Outcome)
			}
			names := log.namesFor(build)
			if len(names) != 3 || names[0] != "reserved" || names[1] != "copy-start" || names[2] != "published" {
				t.Fatalf("%s events = %v, want reserved, copy-start, published", build, names)
			}
		}
		global := log.names()
		firstCopy, firstRelease := -1, -1
		for i, name := range global {
			if name == "copy-start" && firstCopy < 0 {
				firstCopy = i
			}
			if name == "lock-released" && firstRelease < 0 {
				firstRelease = i
			}
		}
		if firstCopy < 0 || firstRelease < 0 || firstRelease < firstCopy {
			t.Fatalf("global events = %v, want every release after every copy start", global)
		}
		if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 2 {
			t.Fatalf("ledger = %v err=%v, want two reservations", entries, err)
		}
	})

	t.Run("different builds publish concurrently", func(t *testing.T) {
		f := newFreeze(t)
		log := &freezeEventLog{}
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			switch point {
			case freezeAfterReserve:
				log.add("reserved", hook.Build)
			case freezeBeforeCopy:
				log.add("copy-start", hook.Build)
			case freezeAfterPublish:
				log.add("published", hook.Build)
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		discoveries := []Discovery{f.discoveryFor("1.4.3-R5000"), f.discoveryFor("1.4.4-R5419.1")}
		var wg sync.WaitGroup
		results := make([]FrozenCopy, 2)
		errs := make([]error, 2)
		for i := range discoveries {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i], errs[i] = PublishFrozen(ctx, f.config(), discoveries[i])
			}(i)
		}
		wg.Wait()
		for i, build := range []string{"1.4.3-R5000", "1.4.4-R5419.1"} {
			if errs[i] != nil {
				t.Fatalf("%s: %v", build, errs[i])
			}
			if results[i].Outcome != OutcomePublished {
				t.Fatalf("%s outcome = %s, want published", build, results[i].Outcome)
			}
			names := log.namesFor(build)
			if len(names) != 3 || names[0] != "reserved" || names[1] != "copy-start" || names[2] != "published" {
				t.Fatalf("%s events = %v, want reserved, copy-start, published", build, names)
			}
		}
	})

	t.Run("same build elects one winner", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		type outcome struct {
			result FrozenCopy
			err    error
		}
		outcomes := make([]outcome, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				outcomes[i].result, outcomes[i].err = PublishFrozen(ctx, f.config(), discovery)
			}(i)
		}
		wg.Wait()
		published := 0
		for _, o := range outcomes {
			if o.err == nil {
				if o.result.Outcome == OutcomePublished {
					published++
					continue
				}
				// The loser re-deduped under the lock after the
				// winner published and resumed without copying.
				if o.result.Outcome != OutcomeResumed {
					t.Fatalf("winning outcome = %s, want published or resumed", o.result.Outcome)
				}
				continue
			}
			requireFreezeCode(t, o.err, CodeFreezeUnavailable, StageAdmit)
		}
		if published != 1 {
			t.Fatalf("published = %d, want exactly one", published)
		}
		entries, err := os.ReadDir(f.ledger)
		if err != nil || len(entries) != 1 {
			t.Fatalf("ledger = %v err=%v, want the single winner", entries, err)
		}
	})
}

// TestFreezeSecondWaiterResumesUnderLock proves the under-lock
// dedupe: a waiter that observed absence before the lock returns the
// winner's published copy when it reaches the lock after the
// publication, without another reservation or copy.
func TestFreezeSecondWaiterResumesUnderLock(t *testing.T) {
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	var firstResult FrozenCopy
	var firstErr error
	ranFirst := false
	f.lock.onAcquire = func() {
		if ranFirst {
			return
		}
		ranFirst = true
		firstResult, firstErr = PublishFrozen(context.Background(), f.config(), discovery)
	}
	second, secondErr := PublishFrozen(context.Background(), f.config(), discovery)
	if firstErr != nil {
		t.Fatalf("first waiter: %v", firstErr)
	}
	if firstResult.Outcome != OutcomePublished {
		t.Fatalf("first outcome = %s, want published", firstResult.Outcome)
	}
	if secondErr != nil {
		t.Fatalf("second waiter: %v", secondErr)
	}
	if second.Outcome != OutcomeResumed {
		t.Fatalf("second outcome = %s, want resumed", second.Outcome)
	}
	if second.Frozen.Path != firstResult.Frozen.Path || second.Frozen.SHA256 != firstResult.Frozen.SHA256 {
		t.Fatalf("second copy %+v is not the winner's %+v", second.Frozen, firstResult.Frozen)
	}
	entries, err := os.ReadDir(f.ledger)
	if err != nil || len(entries) != 1 {
		t.Fatalf("ledger = %v err=%v, want the single winner", entries, err)
	}
}

// TestAllLaunchWaitsHonorOuterDeadline proves every freeze wait ends
// with its context: cancellation refuses before and during the lock
// wait, the lease waits and the copy, failures never retry on a
// fresh budget, and contended local locks refuse at their cap.
func TestAllLaunchWaitsHonorOuterDeadline(t *testing.T) {
	t.Run("canceled context refuses before the lock", func(t *testing.T) {
		f := newFreeze(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := PublishFrozen(ctx, f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeFreezeCanceled, StageAdmit)
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times past cancellation", acquires)
		}
	})

	t.Run("cancel during the lock wait refuses", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		f.lock.waitFor = make(chan struct{})
		f.lock.started = make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		type result struct {
			copy FrozenCopy
			err  error
		}
		done := make(chan result, 1)
		go func() {
			got, err := PublishFrozen(ctx, f.config(), discovery)
			done <- result{copy: got, err: err}
		}()
		select {
		case <-f.lock.started:
		case <-time.After(60 * time.Second):
			t.Fatal("lock wait never started")
		}
		cancel()
		select {
		case r := <-done:
			requireFreezeCode(t, r.err, CodeFreezeCanceled, StageAdmit)
		case <-time.After(60 * time.Second):
			t.Fatal("cancelled lock wait never returned")
		}
	})

	t.Run("lock failure never retries", func(t *testing.T) {
		f := newFreeze(t)
		f.lock.failFirst = 1
		_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeFreezeLockFailed, StageAdmit)
		if acquires, _, _ := f.lock.counts(); acquires != 1 {
			t.Fatalf("lock acquired %d times, want exactly one attempt", acquires)
		}
	})

	t.Run("cancel before the copy refuses without streaming", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		streams := 0
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeBeforeCopy {
				cancel()
			}
			if point == freezeAfterCopyStream {
				streams++
			}
		})
		_, err := PublishFrozen(ctx, f.config(), discovery)
		requireFreezeCode(t, err, CodeFreezeCanceled, StageCopy)
		if streams != 0 {
			t.Fatalf("copy streamed %d times past cancellation", streams)
		}
	})

	t.Run("cancel during the stream stops mid-copy", func(t *testing.T) {
		f := newFreeze(t)
		install := newInstall(t)
		install.writeLauncher(plainScript)
		build := "1.4.4-R5419.1"
		big := []byte("#!/bin/sh\n")
		for len(big) < 3*hashReadChunk+1 {
			big = append(big, "echo mid-stream-cancel-test-data\n"...)
		}
		big = big[:3*hashReadChunk+1]
		install.writeFileSync(filepath.Join(install.dir, pinnedNamePrefix+build), big, 0o755)
		install.writeSidecar(build + "\n")
		discovery, err := ResolveNewest(context.Background(), install.env())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		chunks := 0
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeCopyChunk {
				chunks++
				if chunks == 1 {
					cancel()
				}
			}
		})
		_, err = PublishFrozen(ctx, f.config(), discovery)
		refusal := requireFreezeCode(t, err, CodeFreezeCopyFailed, StageCopy)
		canceled, ok := AsFreezeError(refusal.Cause)
		if !ok || canceled.Code != CodeFreezeCanceled {
			t.Fatalf("copy failure cause = %v, want the chained cancellation", refusal.Cause)
		}
		if chunks != 1 {
			t.Fatalf("copy streamed %d chunks past cancellation, want the first only", chunks)
		}
		entries, err := os.ReadDir(f.ledger)
		if err != nil || len(entries) != 1 {
			t.Fatalf("ledger = %v err=%v, want the failed reservation", entries, err)
		}
		reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Outcome != outcomeFailed {
			t.Fatalf("outcome = %s, want failed", reservation.Outcome)
		}
	})

	t.Run("contended maintenance lease refuses bounded", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		holder, err := os.OpenFile(filepath.Join(f.root, "maintenance.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Close() }()
		if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = syscall.Flock(int(holder.Fd()), syscall.LOCK_UN) }()
		type result struct {
			copy FrozenCopy
			err  error
		}
		done := make(chan result, 1)
		go func() {
			got, err := PublishFrozen(context.Background(), f.config(), discovery)
			done <- result{copy: got, err: err}
		}()
		select {
		case r := <-done:
			requireFreezeCode(t, r.err, CodeFreezeLeaseFailed, StagePublish)
		case <-time.After(60 * time.Second):
			t.Fatal("contended lease never refused")
		}
		if _, err := os.Stat(frozenBinaryPath(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256), discovery.Identity.Build)); !os.IsNotExist(err) {
			t.Fatalf("copy started past the lease refusal: %v", err)
		}
	})

	t.Run("contended metadata lock refuses bounded", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		holder, err := os.OpenFile(filepath.Join(f.root, "state.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Close() }()
		if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = syscall.Flock(int(holder.Fd()), syscall.LOCK_UN) }()
		type result struct {
			copy FrozenCopy
			err  error
		}
		done := make(chan result, 1)
		go func() {
			got, err := PublishFrozen(context.Background(), f.config(), discovery)
			done <- result{copy: got, err: err}
		}()
		select {
		case r := <-done:
			requireFreezeCode(t, r.err, CodeFreezeLeaseFailed, StagePublish)
		case <-time.After(60 * time.Second):
			t.Fatal("contended metadata lock never refused")
		}
	})
}

// TestFreezeLockLossRefuses proves a global lock that fails mid
// section or on release refuses with the lock code: an unprovably
// serialized publication is never claimed as locked.
func TestFreezeLockLossRefuses(t *testing.T) {
	t.Run("lost before the ramp refuses", func(t *testing.T) {
		f := newFreeze(t)
		f.lock.checkErr = context.Canceled
		ramps := 0
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeAfterRamp {
				ramps++
			}
		})
		_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeFreezeLockFailed, StageAdmit)
		if ramps != 0 {
			t.Fatalf("ramp read ran %d times past the loss", ramps)
		}
		if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 0 {
			t.Fatalf("ledger = %v err=%v, want no reservation", entries, err)
		}
	})

	t.Run("lost after the ramp refuses without reserving", func(t *testing.T) {
		f := newFreeze(t)
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeAfterRamp {
				f.lock.mu.Lock()
				f.lock.checkErr = context.Canceled
				f.lock.mu.Unlock()
			}
		})
		_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeFreezeLockFailed, StageAdmit)
		if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 0 {
			t.Fatalf("ledger = %v err=%v, want no reservation", entries, err)
		}
	})

	t.Run("lost before the copy fails the slot", func(t *testing.T) {
		f := newFreeze(t)
		armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
			if point == freezeAfterReserve {
				f.lock.mu.Lock()
				f.lock.checkErr = context.Canceled
				f.lock.mu.Unlock()
			}
		})
		_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeFreezeLockFailed, StageAdmit)
		entries, err := os.ReadDir(f.ledger)
		if err != nil || len(entries) != 1 {
			t.Fatalf("ledger = %v err=%v, want the failed reservation", entries, err)
		}
		reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Outcome != outcomeFailed {
			t.Fatalf("outcome = %s, want failed", reservation.Outcome)
		}
	})

	t.Run("lost on release refuses", func(t *testing.T) {
		f := newFreeze(t)
		f.lock.releaseErr = context.Canceled
		_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeFreezeLockFailed, StageAdmit)
	})
}
