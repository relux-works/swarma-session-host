package sessionbudget

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The production durable store refuses nonprivate/corrupt records instead of
// replacing them with fresh accounting. A private empty store is the neighbor.
func TestBudgetStoreRefusesUnsafeOrCorruptState(t *testing.T) {
	for _, mode := range []string{"private", "public_directory", "corrupt_record", "public_record"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "budget")
			if e := os.Mkdir(dir, 0700); e != nil {
				t.Fatal(e)
			}
			if mode == "public_directory" {
				if e := os.Chmod(dir, 0755); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "corrupt_record" {
				if e := os.WriteFile(filepath.Join(dir, "accounting.json"), []byte("invalid"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "public_record" {
				if e := os.WriteFile(filepath.Join(dir, "accounting.json"), []byte(`{"schema":1,"sessions":{}}`), 0644); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "public_record" {
				if e := os.Chmod(filepath.Join(dir, "accounting.json"), 0644); e != nil {
					t.Fatal(e)
				}
				st, e := os.Stat(filepath.Join(dir, "accounting.json"))
				if e != nil || st.Mode().Perm() != 0644 {
					t.Fatal("unsafe mode plant was not applied")
				}
			}
			_, e := Open(dir)
			if mode == "private" {
				if e != nil {
					t.Fatal(e)
				}
			} else if !errors.Is(e, ErrAccountingUnavailable) {
				t.Fatalf("unsafe accounting admitted: %v", e)
			}
		})
	}
}

// Independent store instances use the actual same OS lock; a rejected update
// retains exact previous bytes, not a partial account or a zeroed baseline.
func TestBudgetStoreRefusalRetainsBytes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "budget")
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.transaction(context.Background(), true, func(j *journal) error { return nil }); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(dir, "accounting.json"))
	if e != nil {
		t.Fatal(e)
	}
	other, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	sentinel := errors.New("refuse")
	if e = other.transaction(context.Background(), true, func(j *journal) error { return sentinel }); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "accounting.json"))
	if string(before) != string(after) {
		t.Fatal("failed accounting transaction wrote bytes")
	}
}
