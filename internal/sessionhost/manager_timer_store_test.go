package sessionhost

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Store fixtures need the same safe ancestry as a real store. The shared system
// temporary directory can be writable by other users, so it is not a valid
// positive fixture. Resolve only this test's cache parent; symlink rejection in
// the store itself is exercised with a separate, deliberately linked child.
func managerTimerPrivateFixtureDirectory(t *testing.T) string {
	t.Helper()
	cache, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cache, 0700); err != nil {
		t.Fatal(err)
	}
	cache, err = filepath.EvalSymlinks(cache)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(cache, "sessionhost-timer-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	return directory
}

func managerTimerInitialLedger(t *testing.T) map[string]any {
	t.Helper()
	data := `{"version":1,"execution_id":"fixture-execution","ledger_id":"00000000-0000-4000-8000-000000000003","revision_high_water":0,"opportunity_high_water":0,"registration":null,"registration_state":"disabled","phase_revision":0,"activation_request":null,"expires_boot_id":null,"expires_monotonic_ns":null,"next_due_boot_id":null,"next_due_ns":null,"active":null,"terminal":[]}` + "\n"
	o, err := decodeManagerTimerObject([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func managerTimerStoreFixture(t *testing.T, durability func(string) error) (*managerTimerStore, managerTimerStoreWitness, map[string]any) {
	t.Helper()
	directory := managerTimerPrivateFixtureDirectory(t)
	ledger := managerTimerInitialLedger(t)
	var witness managerTimerStoreWitness
	s, err := initializeManagerTimerStore(directory, ledger, func(w managerTimerStoreWitness) error { witness = w; return nil }, durability)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, witness, ledger
}

// A single immutable lock serializes independent opens; explicit initialization
// witnesses survive close/reopen, while journal high-waters cannot regress.
func TestManagerTimerStorePersistenceAndLock(t *testing.T) {
	s, witness, ledger := managerTimerStoreFixture(t, nil)
	info, err := os.Stat(filepath.Join(witness.directory, "timer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if other, _, err := openManagerTimerStore(witness, nil); err != managerTimerLocked || other != nil {
		t.Fatalf("parallel open=%v", err)
	}
	ledger["opportunity_high_water"] = json.Number("35")
	if err := s.save(ledger); err != nil {
		t.Fatal(err)
	}
	ledger["opportunity_high_water"] = json.Number("34")
	if err := s.save(ledger); err != managerTimerStateInvalid {
		t.Fatalf("regressing HWM=%v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, got, err := openManagerTimerStore(witness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if timerNumber(got, "opportunity_high_water") != 35 {
		t.Fatalf("lost HWM: %v", got["opportunity_high_water"])
	}
	after, err := os.Stat(filepath.Join(witness.directory, "timer.lock"))
	if err != nil || !os.SameFile(info, after) {
		t.Fatalf("lock inode changed: %v", err)
	}
}

// Missing/corrupt/replaced/unsafe state is never treated as empty or recreated.
// All mutations are isolated fixture files; no live host state is opened.
func TestManagerTimerStoreRefusesChangedFootprints(t *testing.T) {
	for _, which := range []string{"missing_ledger", "corrupt_ledger", "pending_ledger", "missing_lock", "replaced_lock", "linked_lock", "wide_lock", "linked_ledger", "wide_directory", "foreign_witness"} {
		t.Run(which, func(t *testing.T) {
			s, witness, _ := managerTimerStoreFixture(t, nil)
			s.Close()
			dir := witness.directory
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			switch which {
			case "missing_ledger":
				must(os.Remove(filepath.Join(dir, "ledger.json")))
			case "corrupt_ledger":
				must(os.WriteFile(filepath.Join(dir, "ledger.json"), []byte("{}\n"), 0600))
			case "pending_ledger":
				must(os.WriteFile(filepath.Join(dir, "ledger.pending"), []byte("uncertain"), 0600))
			case "missing_lock":
				must(os.Remove(filepath.Join(dir, "timer.lock")))
			case "replaced_lock":
				must(os.Rename(filepath.Join(dir, "timer.lock"), filepath.Join(dir, "old.lock")))
				must(os.WriteFile(filepath.Join(dir, "timer.lock"), nil, 0600))
			case "linked_lock":
				must(os.Link(filepath.Join(dir, "timer.lock"), filepath.Join(dir, "alias.lock")))
			case "wide_lock":
				must(os.Chmod(filepath.Join(dir, "timer.lock"), 0644))
			case "linked_ledger":
				must(os.Link(filepath.Join(dir, "ledger.json"), filepath.Join(dir, "alias.json")))
			case "wide_directory":
				must(os.Chmod(dir, 0755))
			case "foreign_witness":
				witness.ledgerID = "00000000-0000-4000-8000-000000000099"
			}
			if reopened, _, err := openManagerTimerStore(witness, nil); err != managerTimerStateInvalid || reopened != nil {
				t.Fatalf("unsafe reopen=%v", err)
			}
			if which == "missing_ledger" {
				if _, err := os.Lstat(filepath.Join(dir, "ledger.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("missing ledger recreated: %v", err)
				}
			}
		})
	}
}

// Every modeled file/directory durability failure fences the current handle;
// initialization never records an owner witness before all its syncs succeed.
func TestManagerTimerStoreDurabilityFailures(t *testing.T) {
	for _, stage := range []string{"initialize_file", "initialize_lock", "initialize_directory"} {
		t.Run(stage, func(t *testing.T) {
			calls := 0
			directory := managerTimerPrivateFixtureDirectory(t)
			s, err := initializeManagerTimerStore(directory, managerTimerInitialLedger(t), func(managerTimerStoreWitness) error { calls++; return nil }, func(at string) error {
				if at == stage {
					return errors.New("fixture_sync_failure")
				}
				return nil
			})
			if err != managerTimerIOUnavailable || s != nil || calls != 0 {
				t.Fatalf("init=%v witness calls=%d", err, calls)
			}
			if next, err := initializeManagerTimerStore(directory, managerTimerInitialLedger(t), func(managerTimerStoreWitness) error { calls++; return nil }, nil); err != managerTimerStateInvalid || next != nil {
				t.Fatalf("silent initialization retry=%v", err)
			}
		})
	}
	for _, stage := range []string{"replace_file", "replace_directory"} {
		t.Run(stage, func(t *testing.T) {
			s, _, ledger := managerTimerStoreFixture(t, nil)
			s.durability = func(at string) error {
				if at == stage {
					return errors.New("fixture_sync_failure")
				}
				return nil
			}
			ledger["opportunity_high_water"] = json.Number("1")
			if err := s.save(ledger); err != managerTimerIOUnavailable {
				t.Fatalf("save=%v", err)
			}
			s.durability = nil
			if err := s.save(ledger); err != managerTimerStateInvalid {
				t.Fatalf("unfenced retry=%v", err)
			}
		})
	}
}

// Symlink or shared-writable ancestry and a swapped lock cannot redirect an
// owned store's writes.
// A normal named directory is the nearby positive control above.
func TestManagerTimerStoreHeldAncestryAndLiveLock(t *testing.T) {
	parent := managerTimerPrivateFixtureDirectory(t)
	real := filepath.Join(parent, "real")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	if s, err := initializeManagerTimerStore(alias, managerTimerInitialLedger(t), func(managerTimerStoreWitness) error { return nil }, nil); err != managerTimerStateInvalid || s != nil {
		t.Fatalf("symlink ancestry=%v", err)
	}
	unsafe := filepath.Join(parent, "unsafe")
	if err := os.Mkdir(unsafe, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafe, 0777); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(unsafe, "store")
	if err := os.Mkdir(child, 0700); err != nil {
		t.Fatal(err)
	}
	if s, err := initializeManagerTimerStore(child, managerTimerInitialLedger(t), func(managerTimerStoreWitness) error { return nil }, nil); err != managerTimerStateInvalid || s != nil {
		t.Fatalf("shared-writable ancestry=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(child, "timer.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe ancestry wrote a lock: %v", err)
	}
	s, witness, ledger := managerTimerStoreFixture(t, nil)
	if err := os.Rename(filepath.Join(witness.directory, "timer.lock"), filepath.Join(witness.directory, "old.lock")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(witness.directory, "timer.lock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	ledger["opportunity_high_water"] = json.Number("1")
	if err := s.save(ledger); err != managerTimerStateInvalid {
		t.Fatalf("write after lock replacement=%v", err)
	}
}

// Explicit init needs an actual witness writer and a wholly unused footprint;
// it cannot repair a surviving journal by silently creating a replacement lock.
func TestManagerTimerStoreInitializationWitnessAndFootprint(t *testing.T) {
	directory := managerTimerPrivateFixtureDirectory(t)
	if s, err := initializeManagerTimerStore(directory, managerTimerInitialLedger(t), nil, nil); err != managerTimerStateInvalid || s != nil {
		t.Fatalf("unwitnessed init=%v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("unwitnessed init wrote files: %v %v", entries, err)
	}
	if err := os.WriteFile(filepath.Join(directory, "ledger.json"), []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := initializeManagerTimerStore(directory, managerTimerInitialLedger(t), func(managerTimerStoreWitness) error { return nil }, nil); err != managerTimerStateInvalid || s != nil {
		t.Fatalf("existing footprint=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(directory, "timer.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement lock created: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(directory, "ledger.json"))
	if err != nil || string(data) != "preserved" {
		t.Fatalf("journal changed: %q %v", data, err)
	}
}
