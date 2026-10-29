package sessionclient

import (
	"fmt"
	"os"
	"path/filepath"
)

// ShortSocketPath owns a private socket directory independent of TMPDIR.
// Darwin's sun_path holds 104 bytes including the terminating NUL. This is
// the socket-only exception to t.TempDir: long runner TMPDIRs cannot bind there.
func ShortSocketPath(t interface {
	Helper()
	Fatalf(string, ...any)
	Cleanup(func())
}) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "s")
	if err != nil {
		t.Fatalf("create short socket directory: %v", err)
		return ""
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("remove short socket directory: %v", err)
		}
	})
	path, err := checkedSocketPath(dir)
	if err != nil {
		t.Fatalf("short socket path: %v", err)
		return ""
	}
	return path
}

func checkedSocketPath(dir string) (string, error) {
	path := filepath.Join(dir, "g.sock")
	if len(path) >= 104 {
		return "", fmt.Errorf("socket path is %d bytes; Darwin permits at most 103", len(path))
	}
	return path, nil
}
