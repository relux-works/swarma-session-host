package sessionmanager

import (
	"os"
	"path/filepath"
	"testing"
)

// Unix socket names need a short private test root (sockaddr_un); only this
// test-owned directory is removed. Repository/trace fixtures stay in t.TempDir.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "build-gw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return filepath.Join(dir, "gw.sock")
}
func gateTestLayout(t *testing.T, name string) Layout {
	return Layout{StateDir: t.TempDir(), SocketPath: shortSocketPath(t)}
}
