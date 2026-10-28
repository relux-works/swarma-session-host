//go:build darwin || linux

package musebuilds

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// TestFreezePublishesNewBuild proves the happy path end to end: a new
// discovery publishes verified bytes with the exact store layout —
// 0700 shared parents, 0555 leaf and binary — a valid COPIED state, a
// published reservation and a retained (emptied) staging transaction.
func TestFreezePublishesNewBuild(t *testing.T) {
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	result, err := PublishFrozen(context.Background(), f.config(), discovery)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomePublished {
		t.Fatalf("outcome = %s, want %s", result.Outcome, OutcomePublished)
	}
	if result.Runnable() {
		t.Fatal("a fresh publication is probe-pending, never runnable")
	}
	key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
	if result.Frozen.Build != discovery.Identity.Build || result.Frozen.SHA256 != discovery.Identity.SHA256 {
		t.Fatalf("frozen %+v does not bind the discovery tuple", result.Frozen)
	}
	wantPath := frozenBinaryPath(f.root, key, discovery.Identity.Build)
	if result.Frozen.Path != wantPath {
		t.Fatalf("path = %s, want %s", result.Frozen.Path, wantPath)
	}
	for _, dir := range []string{f.root, buildsDir(f.root), stagingDir(f.root), metaDir(f.root)} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("%s mode = %s, want drwx------", dir, info.Mode())
		}
	}
	leaf, err := os.Stat(frozenKeyDir(f.root, key))
	if err != nil {
		t.Fatal(err)
	}
	if leaf.Mode().Perm() != 0o555 {
		t.Fatalf("leaf mode = %s, want dr-xr-xr-x", leaf.Mode())
	}
	binary, err := os.Stat(wantPath)
	if err != nil {
		t.Fatal(err)
	}
	if binary.Mode().Perm() != 0o555 {
		t.Fatalf("binary mode = %s, want r-xr-xr-x", binary.Mode())
	}
	if digestOfFile(t, wantPath) != discovery.Identity.SHA256 {
		t.Fatal("published bytes disagree with the discovery digest")
	}
	state, present, err := readBuildState(context.Background(), statePath(f.root, key))
	if err != nil || !present {
		t.Fatalf("state present=%v err=%v", present, err)
	}
	if state.State != StateCopied || state.Build != discovery.Identity.Build || state.SHA256 != discovery.Identity.SHA256 {
		t.Fatalf("state %+v does not bind the tuple as COPIED", state)
	}
	entries, err := os.ReadDir(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("ledger holds %d reservations, want 1", len(entries))
	}
	reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if reservation.Outcome != outcomePublished || reservation.Build != discovery.Identity.Build {
		t.Fatalf("reservation %+v is not the published outcome", reservation)
	}
	if state.Transaction != reservation.Transaction {
		t.Fatalf("state txn %s does not match reservation txn %s", state.Transaction, reservation.Transaction)
	}
	staging, err := os.ReadDir(stagingDir(f.root))
	if err != nil {
		t.Fatal(err)
	}
	if len(staging) != 1 || staging[0].Name() != reservation.Transaction {
		t.Fatalf("staging holds %v, want the spent transaction", staging)
	}
	acquires, releases, _ := f.lock.counts()
	if acquires != 1 || releases != 1 {
		t.Fatalf("lock acquires=%d releases=%d, want 1 and 1", acquires, releases)
	}
}

// TestFreezePublishesProbeMetadataAsOrdinaryUser proves the frozen
// metadata stays writable by an ordinary user: the STATE file is
// owner-only and its metadata parent is an owner-writable directory,
// so later probe verdicts can publish beside it.
func TestFreezePublishesProbeMetadataAsOrdinaryUser(t *testing.T) {
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	f.publishForOther(t, discovery)
	key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
	parent := filepath.Join(metaDir(f.root), key)
	info, err := os.Stat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("metadata parent mode = %s, want drwx------", info.Mode())
	}
	state, err := os.Stat(statePath(f.root, key))
	if err != nil {
		t.Fatal(err)
	}
	if state.Mode().Perm() != 0o600 {
		t.Fatalf("state mode = %s, want rw-------", state.Mode())
	}
	probe := filepath.Join(parent, ".probe-write")
	if err := os.WriteFile(probe, []byte("writable"), 0o600); err != nil {
		t.Fatalf("metadata parent not writable: %v", err)
	}
	if err := os.Remove(probe); err != nil {
		t.Fatal(err)
	}
}

// TestSecondDistinctBuildPublishesAsOrdinaryUser proves the shared
// builds parent stays writable across publications while every
// published leaf and binary is protected: a second distinct build
// publishes beside the first with exact modes.
func TestSecondDistinctBuildPublishesAsOrdinaryUser(t *testing.T) {
	f := newFreeze(t)
	first := f.publishFor("1.4.3-R5000")
	second := f.publishFor("1.4.4-R5419.1")
	builds, err := os.Stat(buildsDir(f.root))
	if err != nil {
		t.Fatal(err)
	}
	if builds.Mode().Perm() != 0o700 {
		t.Fatalf("builds mode = %s, want drwx------", builds.Mode())
	}
	for _, result := range []FrozenCopy{first, second} {
		leaf, err := os.Stat(filepath.Dir(result.Frozen.Path))
		if err != nil {
			t.Fatal(err)
		}
		if leaf.Mode().Perm() != 0o555 {
			t.Fatalf("leaf mode = %s, want dr-xr-xr-x", leaf.Mode())
		}
		binary, err := os.Stat(result.Frozen.Path)
		if err != nil {
			t.Fatal(err)
		}
		if binary.Mode().Perm() != 0o555 {
			t.Fatalf("binary mode = %s, want r-xr-xr-x", binary.Mode())
		}
		if digestOfFile(t, result.Frozen.Path) != result.Frozen.SHA256 {
			t.Fatalf("published bytes disagree for %s", result.Frozen.Build)
		}
	}
	if entries, err := os.ReadDir(f.ledger); err != nil || len(entries) != 2 {
		t.Fatalf("ledger = %v err=%v, want two reservations", entries, err)
	}
}

// TestFreezeInvalidConfigRefuses proves every HostConfig and Discovery
// field is validated with the invalid code before any admission.
func TestFreezeInvalidConfigRefuses(t *testing.T) {
	base := func() (HostConfig, Discovery) {
		f := newFreeze(t)
		return f.config(), f.discoveryFor("1.4.4-R5419.1")
	}
	configCases := map[string]func(*HostConfig){
		"empty frozen root":   func(c *HostConfig) { c.FrozenRoot = "" },
		"relative frozen":     func(c *HostConfig) { c.FrozenRoot = "relative/root" },
		"empty ledger":        func(c *HostConfig) { c.LedgerDir = "" },
		"relative ledger":     func(c *HostConfig) { c.LedgerDir = "relative/ledger" },
		"empty ramp state":    func(c *HostConfig) { c.RampStatePath = "" },
		"relative ramp state": func(c *HostConfig) { c.RampStatePath = "relative/state" },
		"empty ramp log":      func(c *HostConfig) { c.RampLogPath = "" },
		"relative ramp log":   func(c *HostConfig) { c.RampLogPath = "relative/log" },
		"negative day cap":    func(c *HostConfig) { c.MaxFreezesPerDay = -1 },
		"empty host":          func(c *HostConfig) { c.Host = "" },
		"empty user":          func(c *HostConfig) { c.User = "" },
		"missing locker":      func(c *HostConfig) { c.Locker = nil },
	}
	for name, breakConfig := range configCases {
		t.Run(name, func(t *testing.T) {
			cfg, discovery := base()
			breakConfig(&cfg)
			_, err := PublishFrozen(context.Background(), cfg, discovery)
			requireFreezeCode(t, err, CodeFreezeInvalid, StageAdmit)
		})
	}
	discoveryCases := map[string]func(*Discovery){
		"unparseable build": func(d *Discovery) { d.Identity.Build = "tomorrow" },
		"malformed digest":  func(d *Discovery) { d.Identity.SHA256 = "ZZZ" },
		"short digest":      func(d *Discovery) { d.Identity.SHA256 = strings.Repeat("a", 63) },
		"empty source path": func(d *Discovery) { d.Identity.Path = "" },
		"relative source":   func(d *Discovery) { d.Identity.Path = "relative/muse-bin" },
	}
	for name, breakDiscovery := range discoveryCases {
		t.Run(name, func(t *testing.T) {
			cfg, discovery := base()
			breakDiscovery(&discovery)
			_, err := PublishFrozen(context.Background(), cfg, discovery)
			requireFreezeCode(t, err, CodeFreezeInvalid, StageDedupe)
		})
	}
}

// TestFreezeRefusesWidenedStoreModes proves the frozen shared parents
// must be exactly owner-only and owner-held: a widened or
// foreign-owned directory refuses before any admission.
func TestFreezeRefusesWidenedStoreModes(t *testing.T) {
	widen := func(t *testing.T, dir func(string) string) {
		t.Helper()
		f := newFreeze(t)
		if err := os.MkdirAll(dir(f.root), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir(f.root), 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor("1.4.4-R5419.1"))
		requireFreezeCode(t, err, CodeFreezeUnavailable, StageDedupe)
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times before the store refusal", acquires)
		}
	}
	t.Run("widened root", func(t *testing.T) { widen(t, func(root string) string { return root }) })
	t.Run("widened builds", func(t *testing.T) { widen(t, buildsDir) })
	t.Run("widened staging", func(t *testing.T) { widen(t, stagingDir) })
	t.Run("widened meta", func(t *testing.T) { widen(t, metaDir) })

	t.Run("foreign owner", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		// Scoped to the store seam: a global EUID fake would also
		// foreign-own the discovery sidecar target and refuse there first.
		observed := false
		storeUIDForTest = func(path string, uid uint32) uint32 {
			if path == f.root {
				observed = true
				return uid + 1
			}
			return uid
		}
		t.Cleanup(func() { storeUIDForTest = nil })
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFreezeUnavailable, StageDedupe)
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times before the store refusal", acquires)
		}
		if !observed {
			t.Fatal("store root owner was never read")
		}
		storeUIDForTest = nil
	})
}

// TestFreezePreservesModeAndMtime proves the copy preserves the
// source mtime while the publication pins the 0555 protection: the
// frozen binary reads the source's mtime with no write bits.
func TestFreezePreservesModeAndMtime(t *testing.T) {
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	past := time_2024()
	if err := os.Chtimes(discovery.Identity.Path, past, past); err != nil {
		t.Fatal(err)
	}
	result := f.publishForOther(t, discovery)
	binary, err := os.Stat(result.Frozen.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !binary.ModTime().Equal(past) {
		t.Fatalf("frozen mtime = %s, want %s", binary.ModTime().UTC(), past.UTC())
	}
	if binary.Mode().Perm() != 0o555 {
		t.Fatalf("frozen mode = %s, want r-xr-xr-x", binary.Mode())
	}
	leaf, err := os.Stat(filepath.Dir(result.Frozen.Path))
	if err != nil {
		t.Fatal(err)
	}
	if leaf.Mode().Perm() != 0o555 {
		t.Fatalf("leaf mode = %s, want dr-xr-xr-x", leaf.Mode())
	}
}

// TestFreezeCopiesXattrs proves a source xattr survives the freeze.
// Hosts that cannot set the fixture xattr skip with the reason; the
// evidence plan requires an xattr-capable filesystem.
func TestFreezeCopiesXattrs(t *testing.T) {
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	const attr = "user.musebuilds-freeze-test"
	if err := unix.Setxattr(discovery.Identity.Path, attr, []byte("provenance"), 0); err != nil {
		t.Skipf("fixture xattr not settable: %v", err)
	}
	result := f.publishForOther(t, discovery)
	buf := make([]byte, 64)
	size, err := unix.Getxattr(result.Frozen.Path, attr, buf)
	if err != nil {
		t.Fatalf("frozen copy lost xattr %q: %v", attr, err)
	}
	if string(buf[:size]) != "provenance" {
		t.Fatalf("frozen xattr = %q, want provenance", buf[:size])
	}
}

// TestFreezeFailedCopyRetainsStaging proves a copy that fails
// verification records the failed outcome and retains its staging:
// nothing is deleted, and nothing is published.
func TestFreezeFailedCopyRetainsStaging(t *testing.T) {
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	raw, err := os.ReadFile(discovery.Identity.Path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	tmp, err := os.CreateTemp(filepath.Dir(discovery.Identity.Path), ".swap-*")
	if err != nil {
		t.Fatal(err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		t.Fatal(err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tmpName, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpName, discovery.Identity.Path); err != nil {
		t.Fatal(err)
	}
	_, err = PublishFrozen(context.Background(), f.config(), discovery)
	requireFreezeCode(t, err, CodeSourceUnusable, StageCopy)
	entries, err := os.ReadDir(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("ledger holds %d reservations, want the failed one", len(entries))
	}
	reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if reservation.Outcome != outcomeFailed {
		t.Fatalf("outcome = %s, want failed", reservation.Outcome)
	}
	staged := filepath.Join(stagingDir(f.root), reservation.Transaction, frozenBinaryName(discovery.Identity.Build))
	if _, err := os.Stat(staged); err != nil {
		t.Fatalf("failed staging was removed: %v", err)
	}
	if _, err := os.Stat(frozenBinaryPath(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256), discovery.Identity.Build)); !os.IsNotExist(err) {
		t.Fatalf("failed copy published bytes: %v", err)
	}
}

// TestFreezeStateSyncFailureReportsUnknown proves durability failures
// are reported honestly: a file sync failure fails the publication,
// while a directory sync failure after rename reports UNKNOWN
// instead of claiming the old record intact.
func TestFreezeStateSyncFailureReportsUnknown(t *testing.T) {
	t.Run("key directory sync fails after rename", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		keyDir := frozenKeyDir(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256))
		armFsync(t, func(name string, isDir bool) bool { return isDir && name == keyDir })
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFreezePublicationUnknown, StagePublish)
	})
	t.Run("state directory sync fails after rename", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		metaKey := filepath.Join(metaDir(f.root), freezeKey(discovery.Identity.Build, discovery.Identity.SHA256))
		armFsync(t, func(name string, isDir bool) bool { return isDir && name == metaKey })
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFreezePublicationUnknown, StagePublish)
	})
	t.Run("state file sync fails", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		metaKey := filepath.Join(metaDir(f.root), freezeKey(discovery.Identity.Build, discovery.Identity.SHA256))
		armFsync(t, func(name string, isDir bool) bool { return !isDir && filepath.Dir(name) == metaKey })
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFreezePublicationFailed, StagePublish)
		entries, err := os.ReadDir(f.ledger)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("ledger holds %d reservations, want 1", len(entries))
		}
		reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Outcome != outcomeFailed {
			t.Fatalf("outcome = %s, want failed", reservation.Outcome)
		}
	})
}

// TestFreezeNonemptyKeyDirRefuses proves a leftover empty key
// directory is adopted while any content refuses as a collision:
// the next day's slot proceeds without deletion, and foreign bytes
// are never published beside.
func TestFreezeNonemptyKeyDirRefuses(t *testing.T) {
	t.Run("empty directory is adopted", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		keyDir := frozenKeyDir(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256))
		if err := os.MkdirAll(keyDir, 0o700); err != nil {
			t.Fatal(err)
		}
		result := f.publishForOther(t, discovery)
		if _, err := os.Stat(result.Frozen.Path); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("foreign content collides", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		keyDir := frozenKeyDir(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256))
		if err := os.MkdirAll(keyDir, 0o700); err != nil {
			t.Fatal(err)
		}
		f.writeFileSync(filepath.Join(keyDir, "foreign"), []byte("x"), 0o644)
		before, err := os.ReadFile(filepath.Join(keyDir, "foreign"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFrozenCollision, StagePublish)
		after, err := os.ReadFile(filepath.Join(keyDir, "foreign"))
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != string(before) {
			t.Fatal("collision overwrote the foreign content")
		}
	})
}

// TestFreezeSymlinkDestinationRefuses proves a malformed store cannot
// redirect publication outside the frozen root: a symlinked key
// directory and a symlinked binary both refuse as collisions, the
// external targets keep their content and modes, and an adoption
// candidate behind a symlink refuses before verification.
func TestFreezeSymlinkDestinationRefuses(t *testing.T) {
	t.Run("symlink key dir publishes nothing outside", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		external, err := filepath.EvalSymlinks(storeRootFixture(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(buildsDir(f.root), 0o700); err != nil {
			t.Fatal(err)
		}
		reached := false
		armFreezeInterpose(t, func(point freezeInterposePoint, _ FreezeHook) {
			if point != freezeBeforePublish {
				return
			}
			reached = true
			if err := os.Symlink(external, frozenKeyDir(f.root, key)); err != nil {
				t.Fatal(err)
			}
		})
		_, err = PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFrozenCollision, StagePublish)
		if !reached {
			t.Fatal("publication hook not reached")
		}
		entries, err := os.ReadDir(external)
		if err != nil || len(entries) != 0 {
			t.Fatalf("external dir = %v err=%v, want untouched and empty", entries, err)
		}
		info, err := os.Stat(external)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Fatalf("external mode = %s, want drwx------ (publication chmod must not follow the link)", info.Mode())
		}
		ledger, err := os.ReadDir(f.ledger)
		if err != nil || len(ledger) != 1 {
			t.Fatalf("ledger = %v err=%v, want the failed reservation", ledger, err)
		}
		reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, ledger[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Outcome != outcomeFailed {
			t.Fatalf("outcome = %s, want failed", reservation.Outcome)
		}
	})

	t.Run("symlink binary leaves the target alone", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		keyDir := frozenKeyDir(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256))
		if err := os.MkdirAll(keyDir, 0o700); err != nil {
			t.Fatal(err)
		}
		external := filepath.Join(t.TempDir(), "external")
		if err := os.WriteFile(external, []byte("external"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(external, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(external, filepath.Join(keyDir, frozenBinaryName(discovery.Identity.Build))); err != nil {
			t.Fatal(err)
		}
		_, err := PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFrozenCollision, StageDedupe)
		after, err := os.ReadFile(external)
		if err != nil || string(after) != "external" {
			t.Fatalf("external = %q err=%v, want untouched", after, err)
		}
		info, err := os.Stat(external)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o644 {
			t.Fatalf("external mode = %s, want -rw-r--r--", info.Mode())
		}
	})

	t.Run("adoption behind a symlink refuses", func(t *testing.T) {
		f := newFreeze(t)
		discovery := f.discoveryFor("1.4.4-R5419.1")
		raw, err := os.ReadFile(discovery.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
		keyDir := frozenKeyDir(f.root, key)
		if err := os.MkdirAll(keyDir, 0o700); err != nil {
			t.Fatal(err)
		}
		external := filepath.Join(t.TempDir(), "external")
		if err := os.WriteFile(external, raw, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(external, filepath.Join(keyDir, frozenBinaryName(discovery.Identity.Build))); err != nil {
			t.Fatal(err)
		}
		f.plantReservation("2026-10-10", discovery.Identity.Build, discovery.Identity.SHA256, outcomePending)
		_, err = PublishFrozen(context.Background(), f.config(), discovery)
		requireFreezeCode(t, err, CodeFrozenCollision, StageDedupe)
		after, err := os.ReadFile(external)
		if err != nil || string(after) != string(raw) {
			t.Fatal("external bytes changed past the refusal")
		}
		if acquires, _, _ := f.lock.counts(); acquires != 0 {
			t.Fatalf("lock acquired %d times past the dedupe refusal", acquires)
		}
	})
}

// TestFreezeReadBackRefusesCorruptedStaging proves the read-back
// hash catches bytes that changed after the copy stream: the staged
// copy refuses, the corruption is retained as evidence, and nothing
// is published.
func TestFreezeReadBackRefusesCorruptedStaging(t *testing.T) {
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
		if point != freezeAfterCopyStream {
			return
		}
		matches, err := filepath.Glob(filepath.Join(stagingDir(f.root), "*", frozenBinaryName(discovery.Identity.Build)))
		if err != nil || len(matches) != 1 {
			t.Errorf("staged matches = %v err=%v", matches, err)
			return
		}
		staged, err := os.OpenFile(matches[0], os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Errorf("open staged: %v", err)
			return
		}
		defer func() { _ = staged.Close() }()
		if _, err := staged.Write([]byte{0}); err != nil {
			t.Errorf("corrupt staged: %v", err)
		}
	})
	_, err := PublishFrozen(context.Background(), f.config(), discovery)
	requireFreezeCode(t, err, CodeFreezeCopyFailed, StageCopy)
	if _, err := os.Stat(frozenBinaryPath(f.root, freezeKey(discovery.Identity.Build, discovery.Identity.SHA256), discovery.Identity.Build)); !os.IsNotExist(err) {
		t.Fatalf("corrupted staging published: %v", err)
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
}

// TestFreezeReservationOutcomeFailureRefuses proves a publication
// whose outcome cannot be recorded fails honestly even though its
// bytes and state are already durable: the next attempt dedupes
// through the state, and this one claims no success.
func TestFreezeReservationOutcomeFailureRefuses(t *testing.T) {
	skipIfRoot(t)
	f := newFreeze(t)
	discovery := f.discoveryFor("1.4.4-R5419.1")
	armFreezeInterpose(t, func(point freezeInterposePoint, hook FreezeHook) {
		if point != freezeBeforePublish {
			return
		}
		if err := os.Chmod(f.ledger, 0o555); err != nil {
			t.Errorf("chmod ledger: %v", err)
		}
	})
	t.Cleanup(func() { _ = os.Chmod(f.ledger, 0o700) })
	_, err := PublishFrozen(context.Background(), f.config(), discovery)
	requireFreezeCode(t, err, CodeFreezePublicationFailed, StagePublish)
	key := freezeKey(discovery.Identity.Build, discovery.Identity.SHA256)
	if _, err := os.Stat(frozenBinaryPath(f.root, key, discovery.Identity.Build)); err != nil {
		t.Fatalf("bytes missing after the outcome failure: %v", err)
	}
	if _, present, err := readBuildState(context.Background(), statePath(f.root, key)); err != nil || !present {
		t.Fatalf("state present=%v err=%v after the outcome failure", present, err)
	}
	_ = os.Chmod(f.ledger, 0o700)
	entries, err := os.ReadDir(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("ledger holds %d reservations, want 1", len(entries))
	}
	reservation, err := readReservation(context.Background(), filepath.Join(f.ledger, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if reservation.Outcome != outcomePending {
		t.Fatalf("outcome = %s, want the untouched pending", reservation.Outcome)
	}
}
