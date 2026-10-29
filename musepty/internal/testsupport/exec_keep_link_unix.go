//go:build darwin || linux

package testsupport

import (
	"fmt"
	"os"
	"syscall"
)

// refuseLinkedOrForeignKeepFile refuses a keep leaf whose in-place rewrite
// could land outside the keep root: a file with more than one hard link, or
// a file owned by another user. A hard link planted in the leaf to a file
// outside the root is a regular inode and passes the symlink checks, so the
// link count and the owner are the only static signals left. Like the other
// keep checks, this guards against stale or planted filesystem state, not a
// live adversary racing the test.
func refuseLinkedOrForeignKeepFile(path string, fi os.FileInfo) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st == nil {
		return fmt.Errorf("refusing keep file %s without stat identity", path)
	}
	if st.Nlink > 1 {
		return fmt.Errorf("refusing multiply linked keep file %s", path)
	}
	if st.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("refusing foreign-owned keep file %s", path)
	}
	return nil
}
