//go:build darwin || linux

package musebuilds

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestFreezeMetadataParentSymlinkRefuses(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "existing metadata parent"
		if late {
			name = "publication metadata parent"
		}
		t.Run(name, func(t *testing.T) {
			f := newFreeze(t)
			d := f.discoveryFor("1.4.4-R5419.1")
			key := freezeKey(d.Identity.Build, d.Identity.SHA256)
			external, err := filepath.EvalSymlinks(storeRootFixture(t))
			if err != nil {
				t.Fatal(err)
			}
			if err := ensureFrozenStore(context.Background(), f.root); err != nil {
				t.Fatal(err)
			}
			plant := func() {
				if err := os.Symlink(external, filepath.Join(metaDir(f.root), key)); err != nil {
					t.Fatal(err)
				}
			}
			reached := !late
			if late {
				armFreezeInterpose(t, func(point freezeInterposePoint, _ FreezeHook) {
					if point == freezeBeforePublish {
						reached = true
						plant()
					}
				})
			} else {
				plant()
			}
			_, err = PublishFrozen(context.Background(), f.config(), d)
			code, stage := CodeStateInconsistent, StageDedupe
			if late {
				code, stage = CodeFreezePublicationFailed, StagePublish
			}
			requireFreezeCode(t, err, code, stage)
			if !reached {
				t.Fatal("publication stage not reached")
			}
			entries, err := os.ReadDir(external)
			if err != nil || len(entries) != 0 {
				t.Fatalf("external metadata written: %v %v", entries, err)
			}
			info, err := os.Stat(external)
			if err != nil || info.Mode().Perm() != 0o700 {
				t.Fatalf("external directory modified: %v %v", info, err)
			}
		})
	}
}

func TestFreezeFrozenOwnershipRefuses(t *testing.T) {
	for _, state := range []string{StateProbeOK, StateCopied, "ABSENT"} {
		for _, object := range []string{"leaf", "binary"} {
			t.Run(state+"/"+object, func(t *testing.T) {
				f := newFreeze(t)
				d := f.discoveryFor("1.4.4-R5419.1")
				copy := f.publishForOther(t, d)
				key := freezeKey(d.Identity.Build, d.Identity.SHA256)
				metadata, _, err := readBuildState(context.Background(), statePath(f.root, key))
				if err != nil {
					t.Fatal(err)
				}
				if state == "ABSENT" {
					if err := os.Remove(statePath(f.root, key)); err != nil {
						t.Fatal(err)
					}
				} else {
					metadata.State = state
					f.plantState(key, metadata)
				}
				target := frozenKeyDir(f.root, key)
				if object == "binary" {
					target = copy.Frozen.Path
				}
				observed := false
				storeUIDForTest = func(path string, uid uint32) uint32 {
					if path == target {
						observed = true
						return uid + 1
					}
					return uid
				}
				t.Cleanup(func() { storeUIDForTest = nil })
				_, err = PublishFrozen(context.Background(), f.config(), d)
				code := CodeStateInconsistent
				if state == "ABSENT" {
					code = CodeFrozenCollision
				}
				requireFreezeCode(t, err, code, StageDedupe)
				if !observed {
					t.Fatal("target descriptor owner was never read")
				}
				storeUIDForTest = nil
			})
		}
	}
}

// A failed public copy leaves a spent tuple. Advancing UTC and reconstructing
// config must not authorize a second stream, even with the original bytes back.
func TestFreezeReservationSurvivesUTCRollover(t *testing.T) {
	f := newFreeze(t)
	d := f.discoveryFor("1.4.4-R5419.1")
	original, err := os.ReadFile(d.Identity.Path)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), original...)
	bad[len(bad)-1] ^= 1
	f.writeFileSync(d.Identity.Path, bad, 0o755)
	_, err = PublishFrozen(context.Background(), f.config(), d)
	requireFreezeCode(t, err, CodeSourceUnusable, StageCopy)
	f.writeFileSync(d.Identity.Path, original, 0o755)
	f.now = f.now.Add(24 * time.Hour)
	f.writeRamp("ramp_down=0\n")
	f.writeRampLog(f.now.Format(time.RFC3339) + "\tcount=0\n")
	copies := 0
	armFreezeInterpose(t, func(point freezeInterposePoint, _ FreezeHook) {
		if point == freezeCopyChunk {
			copies++
		}
	})
	_, err = PublishFrozen(context.Background(), f.config(), d)
	requireFreezeCode(t, err, CodeFreezeUnavailable, StageAdmit)
	if copies != 0 {
		t.Fatalf("spent tuple copied again: %d chunks", copies)
	}
	entries, err := os.ReadDir(f.ledger)
	if err != nil || len(entries) != 1 {
		t.Fatalf("ledger reset: %v %v", entries, err)
	}
	r, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
	if err != nil || r.Day != "2026-10-10" || r.Outcome != outcomeFailed {
		t.Fatalf("spent reservation changed: %+v %v", r, err)
	}
}

// Direct O_EXCL contention is an additional bound: an inventory miss cannot
// turn a new day into a fresh key for the same exact bytes.
func TestFreezeLifetimeReservationExclusiveCreate(t *testing.T) {
	f := newFreeze(t)
	d := f.discoveryFor("1.4.4-R5419.1")
	first := f.plantReservation("2026-10-10", d.Identity.Build, d.Identity.SHA256, outcomePending)
	next := first
	next.Day, next.ReservedAt, next.Transaction = "2026-10-11", "2026-10-11T08:00:00Z", "other-transaction"
	created, winner, err := createReservation(context.Background(), f.ledger, next)
	if err != nil || created || winner != first {
		t.Fatalf("lifetime O_EXCL = %v %+v %v", created, winner, err)
	}
}

func TestFreezeCrashAdoptionAcrossUTCRollover(t *testing.T) {
	f := newFreeze(t)
	d := f.discoveryFor("1.4.4-R5419.1")
	first := f.publishForOther(t, d)
	key := freezeKey(d.Identity.Build, d.Identity.SHA256)
	if err := os.Remove(statePath(f.root, key)); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(24 * time.Hour)
	f.writeRamp("ramp_down=0\n")
	f.writeRampLog(f.now.Format(time.RFC3339) + "\tcount=0\n")
	copies := 0
	armFreezeInterpose(t, func(point freezeInterposePoint, _ FreezeHook) {
		if point == freezeCopyChunk {
			copies++
		}
	})
	got, err := PublishFrozen(context.Background(), f.config(), d)
	if err != nil || got.Outcome != OutcomeAdopted || got.Frozen != first.Frozen || copies != 0 {
		t.Fatalf("adoption = %+v err=%v chunks=%d", got, err, copies)
	}
	entries, err := os.ReadDir(f.ledger)
	if err != nil || len(entries) != 1 {
		t.Fatalf("rebilled adoption: %v %v", entries, err)
	}
}

func TestFreezeReuseProtectionRefuses(t *testing.T) {
	for _, state := range []string{StateProbeOK, StateCopied} {
		for _, object := range []string{"leaf", "binary"} {
			t.Run(state+"/"+object, func(t *testing.T) {
				f := newFreeze(t)
				d := f.discoveryFor("1.4.4-R5419.1")
				copy := f.publishForOther(t, d)
				key := freezeKey(d.Identity.Build, d.Identity.SHA256)
				metadata, _, err := readBuildState(context.Background(), statePath(f.root, key))
				if err != nil {
					t.Fatal(err)
				}
				metadata.State = state
				f.plantState(key, metadata)
				target := frozenKeyDir(f.root, key)
				if object == "binary" {
					target = copy.Frozen.Path
				}
				if err := os.Chmod(target, 0o755); err != nil {
					t.Fatal(err)
				}
				_, err = PublishFrozen(context.Background(), f.config(), d)
				requireFreezeCode(t, err, CodeStateInconsistent, StageDedupe)
				info, err := os.Stat(target)
				if err != nil || info.Mode().Perm() != 0o755 {
					t.Fatalf("reuse repaired permissions: %v %v", info, err)
				}
			})
		}
	}
}

// PublishFrozen closes the writable copy handle before exposing COPIED. Merely
// chmodding an open writer leaves Linux's ETXTBSY condition in place.
func TestFreezePublicationClosesWriter(t *testing.T) {
	f := newFreeze(t)
	observed := false
	armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
		if point != freezeBeforePublish {
			return
		}
		observed = true
		if hook.stagedFile == nil {
			t.Fatal("publication did not carry its held file")
		}
		if hook.closedWriter == nil {
			t.Fatal("publication lost its original writer")
		}
		if _, err := hook.closedWriter.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("original copy writer is still open: %v", err)
		}
		flags, err := unix.FcntlInt(hook.stagedFile.Fd(), unix.F_GETFL, 0)
		if err != nil {
			t.Fatal(err)
		}
		if flags&unix.O_ACCMODE != unix.O_RDONLY {
			t.Fatal("publication retained a writable copy handle")
		}
	})
	result, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
	if err != nil || result.Outcome != OutcomePublished || !observed {
		t.Fatalf("publication = %+v %v, stage observed=%v", result, err, observed)
	}
}

// Adoption retains its root authority even if its diagnostic pathname changes.
// The old name must never redirect a state-lock write to the external target.
func TestFreezeAdoptionUsesHeldRoot(t *testing.T) {
	f := newFreeze(t)
	d := f.discoveryFor("1.4.4-R5419.1")
	f.publishForOther(t, d)
	key := freezeKey(d.Identity.Build, d.Identity.SHA256)
	if err := os.Remove(statePath(f.root, key)); err != nil {
		t.Fatal(err)
	}
	relocation, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(relocation, "held-root")
	external, err := filepath.EvalSymlinks(storeRootFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	reached := false
	t.Cleanup(func() {
		if reached {
			_ = os.Remove(f.root)
			if err := os.Rename(moved, f.root); err != nil {
				t.Errorf("restore fixture root: %v", err)
			}
		}
	})
	armFreezeInterpose(t, func(point freezeInterposePoint, _ FreezeHook) {
		if point == freezeLeaseAcquired && !reached {
			if err := os.Rename(f.root, moved); err != nil {
				t.Fatal(err)
			}
			reached = true
			if err := os.Symlink(external, f.root); err != nil {
				t.Fatal(err)
			}
		}
	})
	got, err := PublishFrozen(context.Background(), f.config(), d)
	if err != nil || got.Outcome != OutcomeAdopted || !reached {
		t.Fatalf("held-root adoption = %+v %v, stage reached=%v", got, err, reached)
	}
	state, present, err := readBuildState(context.Background(), statePath(moved, key))
	if err != nil || !present || state.State != StateCopied {
		t.Fatalf("held-root metadata = %+v present=%v %v", state, present, err)
	}
	entries, err := os.ReadDir(external)
	if err != nil || len(entries) != 0 {
		t.Fatalf("external root mutated: %v %v", entries, err)
	}
}
