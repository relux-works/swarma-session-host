package testsupport

import (
	"fmt"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// KeepExecutablesEnv is the default-off switch that keeps test fake
// executables on disk. Unset (or any value other than "1"), fakes live in a
// t.TempDir() subdir and vanish with the test, exactly as before. Set to
// "1", fakes live in $TMPDIR/spm-test-exec/<run-id>/<test>[-N]/ and are
// never removed by the test; macmini-infra's go-work-reaper sweeps those
// directories after 2 h with no holder. Each live *testing.T owns its own
// unique leaf (a same-name repeat gets <test>-1, <test>-2, ...), so a repeat
// never sees a stale file and retained files are never deleted for it.
const KeepExecutablesEnv = "TB_TEST_KEEP_EXECUTABLES"

// execRunID is stable per `go test` process: the pid plus first-use time.
var execRunID = sync.OnceValue(func() string {
	return fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
})

// execDefaultDirs keeps one default-mode directory per live test so the
// several fakes of a test stay colocated, matching today's single-binDir
// layout. It is keyed by the *testing.T lifetime, not by name: a repeated
// run (-count>1) gets a fresh *testing.T and therefore a fresh directory,
// and the entry is dropped when the test ends, so no stale state survives
// across runs.
var execDefaultDirs = struct {
	sync.Mutex
	byTest map[*testing.T]string
}{byTest: map[*testing.T]string{}}

// execKeepDirs maps each live test to its own unique keep directory, so the
// several fakes of a test stay colocated while a same-name repeat (a new
// *testing.T, e.g. under -count>1) gets a different leaf and never sees the
// earlier instance's files. used reserves every allocated leaf for the whole
// process: a retained leaf is never handed out again, which is what makes
// keep mode delete nothing — there is no shared leaf to clear and no stale
// file to remove. byTest entries are evicted at test end; used entries are
// not, because the retained directories still exist. used keys are
// case-folded (see foldKeepPath): on a case-insensitive volume two leaves
// differing only in case are one directory, so the fold is what reserves it.
var execKeepDirs = struct {
	sync.Mutex
	byTest map[*testing.T]string
	used   map[string]bool
}{byTest: map[*testing.T]string{}, used: map[string]bool{}}

// WriteExecutable writes body to a file named name with the given mode and
// returns its path. The directory depends on KeepExecutablesEnv (see above);
// mode and content pass through untouched in both modes.
//
// Neither mode chmods, unlinks, or removes anything: both are a plain
// os.WriteFile, so the umask applies exactly as it did to the migrated bare
// writes, and a same-path rewrite truncates the existing file in place — the
// inode survives, the returned path is unchanged (the repaint fixture pins
// that), and, as with every os.WriteFile overwrite, the requested mode
// applies only when the file is created. Keep mode additionally refuses
// symlinks rather than following them out of the keep root.
func WriteExecutable(t *testing.T, name string, body []byte, mode fs.FileMode) string {
	t.Helper()
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
		t.Fatalf("WriteExecutable: refusing unsafe name %q", name)
	}
	if os.Getenv(KeepExecutablesEnv) == "1" {
		dir, err := keepExecDir(t)
		if err != nil {
			t.Fatalf("WriteExecutable: %v", err)
		}
		path, err := writeKeepFile(dir, name, body, mode)
		if err != nil {
			t.Fatalf("WriteExecutable: %v", err)
		}
		return path
	}
	dir := defaultExecDir(t)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, body, mode); err != nil {
		t.Fatalf("WriteExecutable: write %s: %v", path, err)
	}
	return path
}

// defaultExecDir returns the default-mode directory for t, creating it on
// first use. The directory is t.TempDir()-backed, so the test framework
// removes it when t ends.
func defaultExecDir(t *testing.T) string {
	t.Helper()
	execDefaultDirs.Lock()
	defer execDefaultDirs.Unlock()
	if dir, ok := execDefaultDirs.byTest[t]; ok {
		return dir
	}
	dir := filepath.Join(t.TempDir(), "exec")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("executable directory %s: %v", dir, err)
	}
	execDefaultDirs.byTest[t] = dir
	t.Cleanup(func() {
		execDefaultDirs.Lock()
		defer execDefaultDirs.Unlock()
		delete(execDefaultDirs.byTest, t)
	})
	return dir
}

// keepExecDir returns the keep directory for t, creating it 0o700. Each test
// instance owns a unique leaf (see keepLeafForTest), so the directory starts
// empty without deleting anything, and it is never removed at test end.
// Every component under $TMPDIR is refused when it is a symlink or a
// non-directory, and the resolved leaf must stay under the resolved keep
// root. The checks are Lstat-based: they guard against stale or planted
// filesystem state, not a live adversary racing the test.
func keepExecDir(t *testing.T) (string, error) {
	dir, fresh := keepLeafForTest(t)
	if fresh {
		t.Cleanup(func() {
			execKeepDirs.Lock()
			defer execKeepDirs.Unlock()
			delete(execKeepDirs.byTest, t)
		})
	}
	root := filepath.Join(os.TempDir(), "spm-test-exec")
	for _, p := range []string{root, filepath.Dir(dir), dir} {
		fi, err := os.Lstat(p)
		if err != nil {
			if !os.IsNotExist(err) {
				return "", fmt.Errorf("stat keep path %s: %w", p, err)
			}
			continue
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("refusing symlinked keep path component %s", p)
		}
		if !fi.IsDir() {
			return "", fmt.Errorf("refusing non-directory keep path component %s", p)
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("executable directory %s: %w", dir, err)
	}
	evalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve keep root %s: %w", root, err)
	}
	evalDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", fmt.Errorf("resolve keep directory %s: %w", dir, err)
	}
	if evalDir != evalRoot && !strings.HasPrefix(evalDir, evalRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("keep directory %s escapes %s", dir, root)
	}
	return dir, nil
}

// keepLeafForTest returns the keep directory owned by t, allocating a unique
// leaf on first use: the sanitized test name, or that name with a -N suffix
// when it is already taken (a same-name repeat, a sanitization collision, or
// a case-fold collision on a case-insensitive volume). The lookup and the
// reservation happen under one lock, so concurrent first calls for the same
// test still converge on one directory. The reservation lasts for the
// process, never reused, which is why keep mode never deletes.
func keepLeafForTest(t *testing.T) (string, bool) {
	execKeepDirs.Lock()
	defer execKeepDirs.Unlock()
	if dir, ok := execKeepDirs.byTest[t]; ok {
		return dir, false
	}
	base := sanitizeTestName(t.Name())
	leaf := base
	dir := filepath.Join(os.TempDir(), "spm-test-exec", execRunID(), leaf)
	for n := 1; execKeepDirs.used[foldKeepPath(dir)]; n++ {
		leaf = fmt.Sprintf("%s-%d", base, n)
		dir = filepath.Join(os.TempDir(), "spm-test-exec", execRunID(), leaf)
	}
	execKeepDirs.used[foldKeepPath(dir)] = true
	execKeepDirs.byTest[t] = dir
	return dir, true
}

// foldKeepPath folds a keep path for reservation. The Mac mini's APFS volume
// is case-insensitive by default, so two leaves differing only in case would
// resolve to the same directory while their strings compare unequal.
// Reserving the folded form makes the second allocator take a -N suffix
// instead of sharing the first leaf. strings.ToLower is the whole fold:
// Unicode simple case folding is all this reservation needs.
func foldKeepPath(dir string) string {
	return strings.ToLower(dir)
}

// writeKeepFile writes a keep-mode file without following symlinks and
// without unlinking anything: a symlink or non-regular leaf is refused, and
// so is a leaf whose rewrite could land outside the keep root (multiply
// linked or foreign-owned on Unix — a planted hard link is a regular inode
// and passes the checks above). The write itself is a plain os.WriteFile
// with no chmod, so a rewrite truncates the existing file in place — the
// inode and path survive, exactly as in default mode. Keep mode therefore
// performs no deletion of any kind; the only remover of keep files is the
// external reaper.
func writeKeepFile(dir, name string, body []byte, mode fs.FileMode) (string, error) {
	path := filepath.Join(dir, name)
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("refusing to follow symlink %s", path)
		}
		if !fi.Mode().IsRegular() {
			return "", fmt.Errorf("refusing non-regular keep file %s", path)
		}
		if err := refuseLinkedOrForeignKeepFile(path, fi); err != nil {
			return "", err
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("stat keep file %s: %w", path, err)
	}
	if err := os.WriteFile(path, body, mode); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}

// sanitizeTestName escapes every byte outside [A-Za-z0-9.-] (including '_'
// itself) as _XX, so distinct test names never share a keep directory.
func sanitizeTestName(name string) string {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '-' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "_%02X", c)
		}
	}
	s := b.String()
	if len(s) > 120 {
		sum := fnv.New64a()
		_, _ = sum.Write([]byte(name))
		s = s[:100] + fmt.Sprintf("-%016x", sum.Sum64())
	}
	return s
}
