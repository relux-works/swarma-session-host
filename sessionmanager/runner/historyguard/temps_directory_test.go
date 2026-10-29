package historyguard

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Missing areas are recreated with 0700. Regular files, symlinks and unsafe
// modes refuse without changing their target, contents, or a prior state file.
func TestTempDirectoryValidation(t *testing.T) {
	for _, store := range []string{"controller-epochs.v1.json", "session-owner-keys.v1.json"} {
		for _, fault := range []string{"missing", "regular", "symlink", "mode0755", "mode0000", "sticky"} {
			t.Run(store+"/"+fault, func(t *testing.T) {
				dir := t.TempDir()
				target := filepath.Join(dir, store)
				area := TempDirectory(target)
				outside := t.TempDir()
				marker := filepath.Join(outside, "marker")
				original := []byte("unrelated bytes")
				if err := os.WriteFile(marker, original, 0600); err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "regular":
					if err := os.WriteFile(area, original, 0600); err != nil {
						t.Fatal(err)
					}
				case "symlink":
					if err := os.Symlink(outside, area); err != nil {
						t.Fatal(err)
					}
				case "mode0755", "mode0000", "sticky":
					if err := os.Mkdir(area, 0700); err != nil {
						t.Fatal(err)
					}
					mode := os.FileMode(0755)
					if fault == "mode0000" {
						mode = 0
					}
					if fault == "sticky" {
						mode = 0700 | os.ModeSticky
					}
					if err := os.Chmod(area, mode); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = os.Chmod(area, 0700) })
				}
				err := ValidateTempDirectories(dir)
				if fault == "missing" {
					if err != nil {
						t.Fatal(err)
					}
					info, err := os.Lstat(area)
					if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
						t.Fatalf("missing area not safely recreated: %v", err)
					}
					file, err := CreateTemp(target, "protocol-")
					if err != nil {
						t.Fatal(err)
					}
					defer file.Close()
					if filepath.Dir(file.Name()) != area {
						t.Fatal("temporary escaped owned directory")
					}
				} else {
					if !errors.Is(err, ErrUncertain) {
						t.Fatalf("unsafe temp directory admitted: %v", err)
					}
					if f, err := CreateTemp(target, "protocol-"); err == nil {
						f.Close()
						t.Fatal("writer used unsafe temp directory")
					}
					entries, err := os.ReadDir(outside)
					if err != nil || len(entries) != 1 {
						t.Fatal("writer escaped through symlink")
					}
				}
				after, err := os.ReadFile(marker)
				if err != nil || !bytes.Equal(after, original) {
					t.Fatal("unrelated target bytes changed")
				}
			})
		}
	}
}

// Actual witness publication syncs the written file, committed directory and
// source temporary directory in order. Each injected directory error refuses.
func TestBoundWitnessSyncsBothDirectories(t *testing.T) {
	for _, fault := range []string{"none", "target", "source"} {
		t.Run(fault, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.lock")
			area := TempDirectory(path)
			injected := errors.New("directory sync failure")
			var calls []string
			err := writeBoundWithSync(path, boundWitness{StoreID: StoreIdentity("test", "namespace"), Pending: sealedFixture(t, 1, "retired")}, func(f *os.File) error {
				info, err := f.Stat()
				if err != nil {
					return err
				}
				stage := "file"
				if info.IsDir() {
					stage = "target"
					if filepath.Clean(f.Name()) == area {
						stage = "source"
					}
				}
				calls = append(calls, stage)
				if stage == fault {
					return injected
				}
				return f.Sync()
			})
			if fault == "none" {
				if err != nil {
					t.Fatal(err)
				}
				if len(calls) != 3 || calls[0] != "file" || calls[1] != "target" || calls[2] != "source" {
					t.Fatalf("witness skipped directory durability: %v", calls)
				}
			} else if !errors.Is(err, injected) {
				t.Fatalf("directory failure admitted: %v", err)
			}
		})
	}
}

// Every real store/witness target creates its exclusive 0600 file inside its
// owned area. This direct boundary check does not depend on store bootstrap.
func TestAtomicTemporariesStayInOwnedDirectory(t *testing.T) {
	for _, name := range []string{"controller-epochs.v1.json", "controller-epochs.v1.json.lock", "controller-epochs.v1.json.lock.intent", "session-owner-keys.v1.json", "session-owner-keys.v1.json.lock", "session-owner-keys.v1.json.lock.intent"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(dir, name)
			f, err := CreateTemp(target, "protocol-")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if filepath.Dir(f.Name()) != TempDirectory(target) {
				t.Fatal("temporary escaped owned directory")
			}
			info, err := f.Stat()
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("temporary permissions: %v %v", info, err)
			}
		})
	}
}
