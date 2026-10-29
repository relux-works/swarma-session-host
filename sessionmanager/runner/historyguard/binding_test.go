package historyguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type bindingFixture struct {
	Seal    ImageSeal `json:"seal"`
	Entries []string  `json:"entries"`
}

func sealedFixture(t *testing.T, sequence uint64, entries ...string) []byte {
	t.Helper()
	doc := bindingFixture{Seal: ImageSeal{StoreID: StoreIdentity("test", "namespace"), Sequence: sequence}, Entries: entries}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc.Seal.SHA256 = Digest(raw)
	raw, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func bindFixture(t *testing.T, path string, raw []byte) {
	t.Helper()
	identity := StoreIdentity("test", "namespace")
	if err := PrepareBound(path, raw, identity, false); err != nil {
		t.Fatal(err)
	}
	if err := FinishBound(path, raw, identity); err != nil {
		t.Fatal(err)
	}
}

// Whole-document binding catches partial history edits and stale restoration
// in either file. A checked pending image closes both old/new ordinary reads
// and retains the pre-rename attempt for explicit recovery.
func TestBoundWitnessBindsCompleteCommittedAndPendingImages(t *testing.T) {
	identity := StoreIdentity("test", "namespace")
	path := filepath.Join(t.TempDir(), "state.lock")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	first := sealedFixture(t, 1, "retired-a")
	second := sealedFixture(t, 2, "retired-a", "live-b")
	bindFixture(t, path, first)
	oldWitness, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckBound(path, first, identity, false); err != nil {
		t.Fatal(err)
	}
	if err := PrepareBound(path, second, identity, false); err != nil {
		t.Fatal(err)
	}
	for _, image := range [][]byte{first, second} {
		if err := CheckBound(path, image, identity, false); err == nil {
			t.Fatal("pending transaction admitted")
		}
		if err := CheckBound(path, image, identity, true); err != nil {
			t.Fatal(err)
		}
	}
	images, err := RecoveryImages(path, first, identity)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range images {
		found = found || bytes.Equal(v, second)
	}
	if !found {
		t.Fatal("pre-rename pending history lost")
	}
	if err := FinishBound(path, second, identity); err != nil {
		t.Fatal(err)
	}
	if err := CheckBound(path, first, identity, false); err == nil {
		t.Fatal("older JSON image admitted")
	}
	var partial bindingFixture
	if err := json.Unmarshal(second, &partial); err != nil {
		t.Fatal(err)
	}
	partial.Entries = partial.Entries[1:]
	raw, _ := json.Marshal(partial)
	if _, err := VerifyImage(raw, identity); err == nil {
		t.Fatal("partial image retained a valid seal")
	}
	if err := CheckBound(path, raw, identity, false); err == nil {
		t.Fatal("partial nonempty history admitted")
	}
	if err := os.WriteFile(path, oldWitness, 0600); err != nil {
		t.Fatal(err)
	}
	if err := CheckBound(path, second, identity, false); err == nil {
		t.Fatal("restored older witness admitted")
	}
}

// Lost/empty/torn witnesses refuse normal reads while the independent verified
// JSON image remains a recovery candidate. Missing/corrupt JSON instead uses
// the full witness image. This is bounded file fault injection, not power loss.
func TestBoundWitnessSingleFileLossRetainsRecovery(t *testing.T) {
	identity := StoreIdentity("test", "namespace")
	for _, variant := range []string{"intact", "empty", "partial", "missing", "bad-json", "missing-json"} {
		t.Run(variant, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.lock")
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			raw := sealedFixture(t, 1, "retired-a", "live-b")
			bindFixture(t, path, raw)
			input := append([]byte(nil), raw...)
			switch variant {
			case "empty":
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "partial":
				if err := os.WriteFile(path, []byte(`{"version":`), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "bad-json":
				input = []byte("partial")
			case "missing-json":
				input = nil
			}
			err := CheckBound(path, input, identity, false)
			if variant == "intact" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("single-file loss admitted")
			}
			images, err := RecoveryImages(path, input, identity)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, v := range images {
				found = found || bytes.Equal(v, raw)
			}
			if !found {
				t.Fatal("verified independent recovery image absent")
			}
		})
	}
}

// A failed final clear remains pending for another reader. A following explicit
// successful completion opens exactly the checked image, with no history loss.
func TestBoundWitnessFailedClearRetainsPending(t *testing.T) {
	identity := StoreIdentity("test", "namespace")
	path := filepath.Join(t.TempDir(), "state.lock")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	raw := sealedFixture(t, 1, "retired-a")
	if err := PrepareBound(path, raw, identity, false); err != nil {
		t.Fatal(err)
	}
	fault := errors.New("bound witness fsync fault")
	if err := finishBoundWithSync(path, raw, identity, func(*os.File) error { return fault }); !errors.Is(err, fault) {
		t.Fatal(err)
	}
	if pending, err := BoundPending(path, identity); err != nil || !pending {
		t.Fatalf("failed clear opened: %v %v", pending, err)
	}
	if err := CheckBound(path, raw, identity, false); err == nil {
		t.Fatal("failed clear admitted peer")
	}
	if err := FinishBound(path, raw, identity); err != nil {
		t.Fatal(err)
	}
	if err := SyncBound(path, raw, identity); err != nil {
		t.Fatal(err)
	}
}

// An interrupted pending-witness write must retain its previous committed
// image. Losing only the separate JSON image afterward still has an explicit
// verified recovery route; neither the partial attempt nor empty data is used.
func TestBoundWitnessInterruptedPrepareRetainsCommittedCopy(t *testing.T) {
	identity := StoreIdentity("test", "namespace")
	path := filepath.Join(t.TempDir(), "state.lock")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	first := sealedFixture(t, 1, "retired-a")
	bindFixture(t, path, first)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := readBound(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	w.Pending = sealedFixture(t, 2, "retired-a", "unreturned-b")
	fault := errors.New("interrupted witness file sync")
	if err := writeBoundWithSync(path, w, func(f *os.File) error {
		if err := f.Truncate(17); err != nil {
			return err
		}
		return fault
	}); !errors.Is(err, fault) {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("interrupted witness overwrote committed recovery copy: %v", err)
	}
	images, err := RecoveryImages(path, nil, identity)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, image := range images {
		found = found || bytes.Equal(first, image)
	}
	if !found {
		t.Fatal("single JSON loss after interrupted prepare lost committed history")
	}
	if err := PrepareBound(path, sealedFixture(t, 2, "retired-a", "live-b"), identity, false); err != nil {
		t.Fatal(err)
	}
	if err := FinishBound(path, sealedFixture(t, 2, "retired-a", "live-b"), identity); err != nil {
		t.Fatal(err)
	}
}
