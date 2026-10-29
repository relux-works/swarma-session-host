//go:build !darwin && !linux

package testsupport

import "os"

// refuseLinkedOrForeignKeepFile is a no-op outside darwin and linux: the
// link-count and owner check reads syscall.Stat_t, which only the supported
// Unix hosts populate. The keep switch's retention target is the Mac mini,
// where the unix build covers it.
func refuseLinkedOrForeignKeepFile(path string, fi os.FileInfo) error {
	return nil
}
