//go:build darwin || linux

package testsupport

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestUmaskParityWithLegacyWriteFile proves the helper applies the umask
// exactly like the bare os.WriteFile it replaced, in both modes: under umask
// 0077 a requested 0755 lands 0700 and a 0644 lands 0600.
// Non-parallel by design: the umask is process-wide. Do not add t.Parallel.
func TestUmaskParityWithLegacyWriteFile(t *testing.T) {
	// Retention lasts through the assertions; the parent owns fixture cleanup.
	t.Setenv("TMPDIR", t.TempDir())
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)
	body := []byte("#!/bin/sh\nexit 0\n")
	for _, mode := range []fs.FileMode{0o755, 0o644} {
		t.Run(fmt.Sprintf("mode-%o", mode), func(t *testing.T) {
			legacy := filepath.Join(t.TempDir(), "legacy")
			if err := os.WriteFile(legacy, body, mode); err != nil {
				t.Fatal(err)
			}
			legacyMode := statPerm(t, legacy)

			t.Setenv(KeepExecutablesEnv, "0")
			def := WriteExecutable(t, fmt.Sprintf("helper-%o", mode), body, mode)
			if got := statPerm(t, def); got != legacyMode {
				t.Errorf("default: mode %o, legacy bare write %o", got, legacyMode)
			}

			t.Setenv(KeepExecutablesEnv, "1")
			keep := WriteExecutable(t, fmt.Sprintf("helper-%o", mode), body, mode)
			if got := statPerm(t, keep); got != legacyMode {
				t.Errorf("keep: mode %o, legacy bare write %o", got, legacyMode)
			}
		})
	}
}

func statPerm(t *testing.T, path string) fs.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}
