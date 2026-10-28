//go:build unix

package musebuilds

import (
	"os"
	"syscall"
)

// openInstallSource opens an installed source for freeze verification
// and copying: read-only, never following a trailing symlink, never
// blocking on a FIFO. The caller gates the descriptor before reading.
func openInstallSource(path string) (*os.File, error) {
	return openNoFollowNonblock(path)
}

// openMetadataFile opens a freeze metadata document — STATE, ledger
// reservation or ramp signal — with the same no-follow, nonblocking
// discipline: a symlink fails the open, a FIFO opens without
// blocking so the type gate can refuse it. The caller gates the
// descriptor (regular file, size bound, ownership) before reading.
func openMetadataFile(path string) (*os.File, error) {
	return openNoFollowNonblock(path)
}

func openNoFollowNonblock(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}
