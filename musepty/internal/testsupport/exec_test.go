package testsupport

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

// Cache the first observed process run ID independently of each invocation
// so -count repeats must agree with it as well as with each other.
var modeParityRunID = sync.OnceValue(func() string { return execRunID() })

// TestWriteExecutableModeParity proves the keep switch changes no assertion:
// in both modes the helper returns an executable path, the file runs, and
// mode and contents are identical. Both modes run in-process via subtests.
func TestWriteExecutableModeParity(t *testing.T) {
	wantRunID := modeParityRunID()
	owner := t
	body := []byte("#!/bin/sh\nexit 0\n")
	runIDs := map[string]bool{}
	for _, mode := range []fs.FileMode{0o755, 0o700} {
		t.Run(fmt.Sprintf("mode-%o", mode), func(t *testing.T) {
			paths := map[string]string{}
			var defData []byte
			var defMode fs.FileMode
			t.Run("default", func(t *testing.T) {
				prev, had := os.LookupEnv(KeepExecutablesEnv)
				if err := os.Unsetenv(KeepExecutablesEnv); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if had {
						_ = os.Setenv(KeepExecutablesEnv, prev)
					} else {
						_ = os.Unsetenv(KeepExecutablesEnv)
					}
				})
				paths["default"] = WriteExecutable(t, "probe", body, mode)
				assertRunnableFile(t, paths["default"], body, mode)
				// The default file vanishes with this subtest; capture
				// bytes now for the cross-mode comparison below.
				var err error
				if defData, err = os.ReadFile(paths["default"]); err != nil {
					t.Fatal(err)
				}
				if info, err := os.Stat(paths["default"]); err != nil {
					t.Fatal(err)
				} else {
					defMode = info.Mode().Perm()
				}
				if strings.Contains(paths["default"], "spm-test-exec") {
					t.Fatalf("default path %q lives in the keep tree", paths["default"])
				}
				if filepath.Base(filepath.Dir(paths["default"])) != "exec" {
					t.Fatalf("default path %q is not in a TempDir exec subdir", paths["default"])
				}
			})
			t.Run("keep", func(t *testing.T) {
				t.Setenv(KeepExecutablesEnv, "1")
				paths["keep"] = WriteExecutable(t, "probe", body, mode)
				dir := filepath.Dir(paths["keep"])
				// Keep files survive their owning subtest for the comparisons
				// below. Remove only this fixture leaf when the parent ends;
				// process reservations still force suffixes under -count.
				owner.Cleanup(func() {
					if err := os.RemoveAll(dir); err != nil {
						owner.Errorf("remove keep fixture %s: %v", dir, err)
					}
				})
				assertRunnableFile(t, paths["keep"], body, mode)
				rel, err := filepath.Rel(filepath.Join(os.TempDir(), "spm-test-exec"), dir)
				if err != nil || strings.HasPrefix(rel, "..") {
					t.Fatalf("keep path %q is not under $TMPDIR/spm-test-exec", paths["keep"])
				}
				parts := strings.Split(rel, string(os.PathSeparator))
				if len(parts) != 2 {
					t.Fatalf("keep path %q is not <run-id>/<test>", paths["keep"])
				}
				if !regexp.MustCompile(`^[0-9]+-[0-9]+$`).MatchString(parts[0]) {
					t.Fatalf("keep run-id %q is not <pid>-<nanos>", parts[0])
				}
				runIDs[parts[0]] = true
				base := sanitizeTestName(t.Name())
				if !regexp.MustCompile(`^` + regexp.QuoteMeta(base) + `(-[1-9][0-9]*)?$`).MatchString(parts[1]) {
					t.Fatalf("keep leaf %q, want %q or %q followed by -<positive integer>", parts[1], base, base)
				}
				if info, err := os.Stat(dir); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
					t.Fatalf("keep dir %s stat=%v", dir, err)
				}
			})
			if paths["default"] == paths["keep"] {
				t.Fatal("both modes returned the same path")
			}
			keep, err := os.ReadFile(paths["keep"])
			if err != nil {
				t.Fatal(err)
			}
			if string(defData) != string(keep) || string(defData) != string(body) {
				t.Fatal("contents differ across modes")
			}
			keepInfo, _ := os.Stat(paths["keep"])
			if defMode != keepInfo.Mode().Perm() || defMode != mode {
				t.Fatalf("modes differ across modes: %o vs %o, want %o",
					defMode, keepInfo.Mode().Perm(), mode)
			}
		})
	}
	if len(runIDs) != 1 || !runIDs[wantRunID] {
		t.Fatalf("run-id not stable per process: %v, want only %q", runIDs, wantRunID)
	}
}

func assertRunnableFile(t *testing.T, path string, body []byte, mode fs.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != mode {
		t.Fatalf("%s mode = %o, want regular %o", path, info.Mode().Perm(), mode)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != string(body) {
		t.Fatalf("%s contents = %q, want %q", path, data, body)
	}
	if out, err := exec.Command(path).CombinedOutput(); err != nil {
		t.Fatalf("run %s: %v\n%s", path, err, out)
	}
}
