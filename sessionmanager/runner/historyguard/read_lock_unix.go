//go:build unix

package historyguard

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"os"
)

// ReadLock joins an existing stable fence without creating, chmodding or
// repairing anything. Codec reads must refuse missing fences without writes.
func ReadLock(ctx context.Context, path string) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0600)
		if err != nil {
			return nil, err
		}
		kind := unix.LOCK_SH
		for {
			err = unix.Flock(int(f.Fd()), kind|unix.LOCK_NB)
			if err == nil {
				break
			}
			if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
				_ = f.Close()
				return nil, err
			}
			if err = wait(ctx); err != nil {
				_ = f.Close()
				return nil, err
			}
		}
		actual, statErr := f.Stat()
		current, pathErr := os.Lstat(path)
		if statErr == nil && pathErr == nil && actual.Mode().IsRegular() && actual.Mode().Perm() == 0600 && os.SameFile(actual, current) && current.Mode().IsRegular() {
			if err := ctx.Err(); err != nil {
				_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
				_ = f.Close()
				return nil, err
			}
			return func() { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close() }, nil
		}
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		_ = f.Close()
		if statErr != nil {
			return nil, statErr
		}
		if pathErr != nil && !errors.Is(pathErr, os.ErrNotExist) {
			return nil, pathErr
		}
		if pathErr == nil && (!current.Mode().IsRegular() || current.Mode().Perm() != 0600) {
			return nil, ErrUncertain
		}
		if err := wait(ctx); err != nil {
			return nil, err
		}
	}
}
