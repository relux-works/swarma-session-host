package testsupport

import (
	"os"
	"path/filepath"
	"testing"
)

// Unit tests for the keepExecDir/writeKeepFile refusal paths. The public-API
// regression for symlink containment is TestKeepSymlinkLeafRefused in
// exec_regress_test.go, which runs the write in a child process because
// refusal is t.Fatal.

func TestKeepRootSymlinkRefused(t *testing.T) {
	isolated := t.TempDir()
	t.Setenv("TMPDIR", isolated)
	t.Setenv(KeepExecutablesEnv, "1")
	sibling := filepath.Join(isolated, "sibling")
	if err := os.MkdirAll(sibling, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sibling, filepath.Join(isolated, "spm-test-exec")); err != nil {
		t.Fatal(err)
	}
	if _, err := keepExecDir(t); err == nil {
		t.Fatal("keepExecDir followed a symlinked keep root")
	}
	entries, err := os.ReadDir(sibling)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("created %v outside the keep root", entries)
	}
}

func TestKeepRunDirSymlinkRefused(t *testing.T) {
	isolated := t.TempDir()
	t.Setenv("TMPDIR", isolated)
	t.Setenv(KeepExecutablesEnv, "1")
	root := filepath.Join(isolated, "spm-test-exec")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	sibling := filepath.Join(isolated, "sibling")
	if err := os.MkdirAll(sibling, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sibling, filepath.Join(root, execRunID())); err != nil {
		t.Fatal(err)
	}
	if _, err := keepExecDir(t); err == nil {
		t.Fatal("keepExecDir followed a symlinked run directory")
	}
}

func TestKeepLeafSymlinkRefused(t *testing.T) {
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
	if err := os.Symlink(outside, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := writeKeepFile(dir, "link", []byte("replaced"), 0o600); err == nil {
		t.Fatal("writeKeepFile followed a symlink leaf")
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "original" {
		t.Fatalf("outside file = %q, %v", data, err)
	}
}

func TestKeepLeafDirRefused(t *testing.T) {
	isolated := t.TempDir()
	t.Setenv("TMPDIR", isolated)
	t.Setenv(KeepExecutablesEnv, "1")
	dir, err := keepExecDir(t)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := writeKeepFile(dir, "subdir", []byte("x"), 0o600); err == nil {
		t.Fatal("writeKeepFile accepted a directory leaf")
	}
}
