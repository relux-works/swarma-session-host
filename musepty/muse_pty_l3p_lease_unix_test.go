//go:build unix

package musepty

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// L3P r2 lease probes (finding 1): the same exclusive-flock proof L2M
// uses (muse_newest_build_lease_unix_test.go). The frozen-store use
// lease is a shared flock; an exclusive nonblocking probe of its file
// succeeds only when no live host holds it.

// museL3PRequireLeaseProbes is the platform gate for the lease test:
// a no-op where flock probes exist.
func museL3PRequireLeaseProbes(t *testing.T) {
	t.Helper()
}

// museL3PUseLeaseFile opens the section-3.5 lease file the test
// couples to by name (musebuilds locks_unix.go); it fails loudly if
// the file is missing or moved.
func museL3PUseLeaseFile(t *testing.T, frozenRoot string) *os.File {
	t.Helper()
	path := filepath.Join(frozenRoot, "maintenance.lock")
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open the use-lease file %s: %v", path, err)
	}
	return file
}

// requireMuseL3PUseLeaseHeld proves a live frozen host holds the use
// lease: the exclusive probe must refuse while any started child is
// unreaped.
func requireMuseL3PUseLeaseHeld(t *testing.T, frozenRoot string) {
	t.Helper()
	file := museL3PUseLeaseFile(t, frozenRoot)
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		t.Fatal("exclusive probe of the use lease succeeded while a frozen host runs: the leg holds no lease")
	}
}

// requireMuseL3PUseLeaseReleased proves no live host holds the use
// lease: the exclusive probe succeeds only after every holder
// released it across reap or its error path.
func requireMuseL3PUseLeaseReleased(t *testing.T, frozenRoot string) {
	t.Helper()
	file := museL3PUseLeaseFile(t, frozenRoot)
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("exclusive probe of the use lease refused (%v): a retained host still holds it", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatalf("release the use-lease probe: %v", err)
	}
}

// holdMuseL3PUseLeaseExclusive takes the exclusive lease and holds it
// until the test ends, so a contended acquisition refuses. The hold
// itself is nonblocking and fails loudly when the store is already
// held.
func holdMuseL3PUseLeaseExclusive(t *testing.T, frozenRoot string) {
	t.Helper()
	file := museL3PUseLeaseFile(t, frozenRoot)
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		t.Fatalf("hold the use lease exclusive: %v", err)
	}
	t.Cleanup(func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	})
}
