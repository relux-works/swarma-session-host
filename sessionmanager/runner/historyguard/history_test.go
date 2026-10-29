package historyguard

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A failed final witness fsync cannot expose a clean witness to an already
// reconciled reader; the adjacent successful clear retains nonempty history.
// Injection is at the filesystem call boundary, not simulated power loss.
func TestCommitWitnessFailedClearRemainsUncertain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.lock")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if used, err := HasHistory(path); err != nil || used {
		t.Fatalf("bootstrap=%v %v", used, err)
	}
	if err := BeginCommit(path); err != nil {
		t.Fatal(err)
	}
	fault := errors.New("witness sync fault")
	if err := writeMarkerWithSync(path, marker, func(*os.File) error { return fault }); !errors.Is(err, fault) {
		t.Fatal(err)
	}
	if dirty, err := CommitUncertain(path); err != nil || !dirty {
		t.Fatalf("failed clear admitted: %v %v", dirty, err)
	}
	if err := FinishCommit(path); err != nil {
		t.Fatal(err)
	}
	if dirty, err := CommitUncertain(path); err != nil || dirty {
		t.Fatalf("successful clear refused: %v %v", dirty, err)
	}
	if used, err := HasHistory(path); err != nil || !used {
		t.Fatalf("clear erased history: %v %v", used, err)
	}
}

// Partial/corrupt/symlink/insecure witnesses never count as virgin storage.
// A valid history witness immediately beside these refusals remains readable.
func TestHistoryWitnessRefusesMalformedOrInsecureState(t *testing.T) {
	for _, variant := range []string{"valid", "partial", "trailing", "mode", "symlink"} {
		t.Run(variant, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "admission.lock")
			raw := []byte(marker)
			switch variant {
			case "partial":
				raw = raw[:3]
			case "trailing":
				raw = append(raw, 'x')
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if variant == "mode" {
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if variant == "symlink" {
				if err := os.Rename(path, path+".actual"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".actual", path); err != nil {
					t.Fatal(err)
				}
			}
			used, err := HasHistory(path)
			if variant == "valid" {
				if err != nil || !used {
					t.Fatalf("valid=%v %v", used, err)
				}
			} else if err == nil || used {
				t.Fatalf("bad witness accepted=%v %v", used, err)
			}
			if variant != "valid" {
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := BeginCommit(path); err == nil {
					t.Fatal("malformed witness began a commit")
				}
				if err := FinishCommit(path); err == nil {
					t.Fatal("malformed witness cleared")
				}
				after, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if string(before) != string(after) {
					t.Fatal("refusal rewrote malformed witness")
				}
			}
		})
	}
}
