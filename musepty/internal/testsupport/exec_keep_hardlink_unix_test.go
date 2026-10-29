//go:build darwin || linux

package testsupport

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestKeepLeafHardlinkRefused plants a hard link in the keep directory to a
// file outside the keep root and proves writeKeepFile refuses to rewrite
// through it. A hard link is a regular inode and passes the symlink checks,
// so without the link-count refusal the in-place write would truncate the
// outside file. The outside bytes must be untouched and the leaf left in
// place (keep mode deletes nothing). Unix-only: the refusal reads
// syscall.Stat_t link counts.
func TestKeepLeafHardlinkRefused(t *testing.T) {
	isolated := t.TempDir()
	t.Setenv("TMPDIR", isolated)
	t.Setenv(KeepExecutablesEnv, "1")
	dir, err := keepExecDir(t)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(isolated, "outside")
	if err := os.WriteFile(outside, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Link(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := writeKeepFile(dir, "link", []byte("replaced"), 0o600); err == nil {
		t.Fatal("writeKeepFile rewrote a multiply linked leaf")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "original" {
		t.Fatalf("outside file = %q, %v", data, err)
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatalf("planted leaf missing after refusal: %v; keep mode must never delete", err)
	}
}

// TestKeepHardlinkLeafRefused is the public-API form of
// TestKeepLeafHardlinkRefused: the write runs in a child test process
// because refusal is t.Fatal. The child must exit non-zero and the outside
// file must be untouched.
func TestKeepHardlinkLeafRefused(t *testing.T) {
	if os.Getenv("SPM_KEEP_HARDLINK_CHILD") == "1" {
		keepHardlinkLeafChild(t)
		return
	}
	isolated := t.TempDir()
	outside := filepath.Join(isolated, "outside")
	if err := os.WriteFile(outside, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", isolated)
	t.Setenv(KeepExecutablesEnv, "1")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestKeepHardlinkLeafRefused$")
	cmd.Env = append(os.Environ(),
		"SPM_KEEP_HARDLINK_CHILD=1",
		"SPM_KEEP_HARDLINK_OUTSIDE="+outside,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("child exited 0, want hardlink refusal; output:\n%s", out)
	}
	data, readErr := os.ReadFile(outside)
	if readErr != nil || string(data) != "original" {
		t.Fatalf("outside file = %q, %v; output:\n%s", data, readErr, out)
	}
}

func keepHardlinkLeafChild(t *testing.T) {
	outside := os.Getenv("SPM_KEEP_HARDLINK_OUTSIDE")
	seed := WriteExecutable(t, "seed", []byte("seed"), 0o600)
	if err := os.Link(outside, filepath.Join(filepath.Dir(seed), "link")); err != nil {
		t.Fatalf("plant link: %v", err)
	}
	WriteExecutable(t, "link", []byte("replaced"), 0o600)
}
