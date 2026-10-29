//go:build unix

package historyguard

import (
	"os"
	"syscall"
)

func tempDirectoryOwner(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}
