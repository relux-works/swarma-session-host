//go:build darwin || linux

package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// quarantineSettable reports whether the quarantine marker can be set
// on this filesystem. Linux cannot set the Apple namespace
// unprivileged, so quarantine-present cells are Darwin-only; the
// absent cell runs everywhere.
func quarantineSettable(t *testing.T) bool {
	t.Helper()
	scratch := filepath.Join(t.TempDir(), "probe")
	if err := os.WriteFile(scratch, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return unix.Setxattr(scratch, quarantineAttr, []byte("0081;probe;probe"), 0) == nil
}

// TestFreezeQuarantineRealAttributes proves the production quarantine
// check reads real attributes: an unmarked source is clear on every
// platform (Darwin ENOATTR and Linux ENODATA both mean absence),
// while a marked source refuses before any reservation or lock.
func TestFreezeQuarantineRealAttributes(t *testing.T) {
	t.Run("absent marker is clear", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		result, err := PublishFrozen(context.Background(), f.config(), discovery)
		if err != nil {
			t.Fatal(err)
		}
		if result.Outcome != OutcomePublished {
			t.Fatalf("outcome = %s, want published", result.Outcome)
		}
	})

	t.Run("present marker refuses", func(t *testing.T) {
		if !quarantineSettable(t) {
			t.Skip("quarantine marker not settable on this filesystem")
		}
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		if err := unix.Setxattr(discovery.Identity.Path, quarantineAttr, []byte("0081;test;test"), 0); err != nil {
			t.Fatalf("fixture quarantine not settable: %v", err)
		}
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeSourceQuarantined, StageCopy)
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times past quarantine", acquires)
		}
		if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 0 {
			t.Fatalf("ledger = %v err=%v, want no reservation", entries, err)
		}
	})
}

// TestFreezeQuarantineAddedDuringAdmissionRefuses proves quarantine
// added after the pre-lock check still refuses: the copy binds its
// verdict to the pinned copy descriptor, and the spent reservation
// records the failure instead of publishing.
func TestFreezeQuarantineAddedDuringAdmissionRefuses(t *testing.T) {
	if !quarantineSettable(t) {
		t.Skip("quarantine marker not settable on this filesystem")
	}
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	var hookErr error
	chunks := 0
	armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
		if point == freezeCopyChunk {
			chunks++
		}
		if point == freezeBeforeCopy {
			hookErr = unix.Setxattr(discovery.Identity.Path, quarantineAttr, []byte("0081;late;late"), 0)
		}
	})
	_, err := PublishFrozen(context.Background(), f.config(), discovery)
	if hookErr != nil {
		t.Fatalf("late quarantine not settable: %v", hookErr)
	}
	requireFreezeCode(t, err, CodeSourceQuarantined, StageCopy)
	if chunks != 0 {
		t.Fatalf("quarantined source streamed %d chunks before refusal", chunks)
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
	if _, err := os.Stat(frozenBinaryPath(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256), discovery.Identity.Build)); !os.IsNotExist(err) {
		t.Fatalf("copy published past the late quarantine: %v", err)
	}
}

// TestFreezeSameByteReplacementWithQuarantineRefuses proves a source
// swapped for same bytes carrying quarantine refuses: the digest
// gate passes, but the descriptor-bound quarantine verdict does not.
func TestFreezeSameByteReplacementWithQuarantineRefuses(t *testing.T) {
	if !quarantineSettable(t) {
		t.Skip("quarantine marker not settable on this filesystem")
	}
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	raw, err := os.ReadFile(discovery.Identity.Path)
	if err != nil {
		t.Fatal(err)
	}
	var hookErr error
	armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
		if point != freezeBeforeCopy {
			return
		}
		dir := filepath.Dir(discovery.Identity.Path)
		tmp, err := os.CreateTemp(dir, ".swap-*")
		if err != nil {
			hookErr = err
			return
		}
		tmpName := tmp.Name()
		if _, err := tmp.Write(raw); err != nil {
			_ = tmp.Close()
			hookErr = err
			return
		}
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			hookErr = err
			return
		}
		if err := tmp.Close(); err != nil {
			hookErr = err
			return
		}
		if err := os.Chmod(tmpName, 0o755); err != nil {
			hookErr = err
			return
		}
		if err := unix.Setxattr(tmpName, quarantineAttr, []byte("0081;swap;swap"), 0); err != nil {
			hookErr = err
			return
		}
		hookErr = os.Rename(tmpName, discovery.Identity.Path)
	})
	_, err = PublishFrozen(context.Background(), f.config(), discovery)
	if hookErr != nil {
		t.Fatalf("same-byte swap failed: %v", hookErr)
	}
	requireFreezeCode(t, err, CodeSourceQuarantined, StageCopy)
	if _, err := os.Stat(frozenBinaryPath(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256), discovery.Identity.Build)); !os.IsNotExist(err) {
		t.Fatalf("copy published past the quarantined swap: %v", err)
	}
}

// TestFreezeStagedXattrTamperingRefuses proves the staged attributes
// are verified against the pinned source before publication: a
// removed or added staged attribute fails the copy instead of
// publishing mixed bytes and attributes.
func TestFreezeStagedXattrTamperingRefuses(t *testing.T) {
	const attr = "user.musebuilds-freeze-test"
	scratch := filepath.Join(t.TempDir(), "probe")
	if err := os.WriteFile(scratch, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(scratch, attr, []byte("provenance"), 0); err != nil {
		t.Skipf("fixture xattr not settable: %v", err)
	}
	for _, cell := range []struct {
		name   string
		tamper func(staged string) error
	}{
		{"removed attribute", func(staged string) error { return unix.Removexattr(staged, attr) }},
		{"added attribute", func(staged string) error {
			return unix.Setxattr(staged, "user.musebuilds-evil", []byte("x"), 0)
		}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			f := newFreeze(t)
			discovery := f.discoveryFor("1.4.4-R5419.1")
			if err := unix.Setxattr(discovery.Identity.Path, attr, []byte("provenance"), 0); err != nil {
				t.Fatalf("fixture xattr not settable: %v", err)
			}
			var hookErr error
			armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
				if point != freezeAfterXattrCopy {
					return
				}
				staged := filepath.Join(stagingDir(f.root), hook.Transaction, frozenBinaryName(hook.Build))
				hookErr = cell.tamper(staged)
			})
			_, err := PublishFrozen(context.Background(), f.config(), discovery)
			if hookErr != nil {
				t.Fatalf("staged tampering failed: %v", hookErr)
			}
			requireFreezeCode(t, err, CodeFreezeCopyFailed, StageCopy)
		})
	}
}

// Late source attributes cannot join the already captured snapshot. A changed
// source descriptor refuses, and a staged quarantine flag refuses directly.
func TestFreezeQuarantineAfterStreamingRefuses(t *testing.T) {
	if !quarantineSettable(t) {
		t.Skip("requires Darwin quarantine attributes")
	}
	for _, target := range []string{"source", "staging"} {
		t.Run(target, func(t *testing.T) {
			f := newFreeze(t)
			d := f.discoveryFor("1.4.4-R5419.1")
			reached := false
			armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
				at := freezeAfterCopyStream
				path := d.Identity.Path
				if target == "staging" {
					at = freezeAfterXattrCopy
					path = filepath.Join(stagingDir(f.root), hook.Transaction, frozenBinaryName(hook.Build))
				}
				if point != at {
					return
				}
				reached = true
				if err := unix.Setxattr(path, quarantineAttr, []byte("0081;late;after-stream"), 0); err != nil {
					t.Fatal(err)
				}
			})
			_, err := PublishFrozen(context.Background(), f.config(), d)
			code := CodeSourceUnusable
			if target == "staging" {
				code = CodeSourceQuarantined
			}
			requireFreezeCode(t, err, code, StageCopy)
			if !reached {
				t.Fatal("streaming stage not reached")
			}
			if _, err := os.Stat(frozenBinaryPath(f.root, freezeKey(d.Identity.Build, d.Identity.SHA256), d.Identity.Build)); !os.IsNotExist(err) {
				t.Fatalf("rejected copy published: %v", err)
			}
			entries, err := os.ReadDir(stagingDir(f.root))
			if err != nil || len(entries) != 1 {
				t.Fatalf("failed staging lost: %v %v", entries, err)
			}
		})
	}
}
