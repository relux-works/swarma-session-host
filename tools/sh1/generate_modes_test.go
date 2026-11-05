package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Set umask only in the CLI child, so the test process and other tests retain
// their permissions policy. Both supported platforms provide this POSIX shell.
func TestGeneratePreservesModesAcrossUmasks(t *testing.T) {
	for _, mask := range []string{"0022", "0077"} {
		t.Run(mask, func(t *testing.T) {
			src := fixture(t)
			for path, mode := range map[string]os.FileMode{
				"session.go":               0750,
				"testdata/identity.golden": 0640,
			} {
				if err := os.Chmod(filepath.Join(src, "internal/session", path), mode); err != nil {
					t.Fatal(err)
				}
			}
			out := filepath.Join(t.TempDir(), "candidate")
			mod := filepath.Join(t.TempDir(), "go.mod")
			save(t, mod, []byte("module example.org/sh1-host\n\ngo 1.25.5\n"))
			args := []string{"sh", "-c", `umask "$1"; shift; exec "$@"`, "sh1-test", mask, tool}
			command(t, ".", append(args, generateArgs(src, out, mod)...)...)
			m, _, err := candidate(out)
			if err != nil {
				t.Fatal(err)
			}
			_, sourceModes, err := tree(filepath.Join(src, "internal/session"))
			if err != nil {
				t.Fatal(err)
			}
			for _, record := range m.Files {
				want := sourceModes[record.Path]
				if record.Path == "go.mod" {
					want = 0644
				}
				if record.Mode != uint32(want) {
					t.Fatalf("%s: manifest mode %o, want %o", record.Path, record.Mode, want)
				}
			}
			// Restoring generated modes must not weaken verification of later edits.
			if err := os.Chmod(filepath.Join(out, "go.mod"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := candidate(out); err == nil || !strings.Contains(err.Error(), "mode or classification differs") {
				t.Fatalf("mode tampering was not refused: %v", err)
			}
		})
	}
}
