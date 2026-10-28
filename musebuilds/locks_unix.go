//go:build unix

package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Lock-file poll interval for nonblocking flock attempts. The waits
// themselves are bounded by the caller's context and the design's
// per-operation caps, never by this interval.
const lockPollInterval = 20 * time.Millisecond

// acquireMaintenanceLeaseAt takes the shared per-user maintenance lease
// that serializes freeze against reviewed cleanup. The lease is held
// across the copy and publication critical section and released
// before any probe runs.
func acquireMaintenanceLeaseAt(ctx context.Context, root *pinnedDir) (func() error, error) {
	return acquireLockFileAt(ctx, root, "maintenance.lock", syscall.LOCK_SH, leaseWaitCap, "maintenance lease")
}

// acquireStateLockAt serializes metadata through the attempt's held root.
// The lock cap remains inside the caller's outer context budget.
func acquireStateLockAt(ctx context.Context, root *pinnedDir) (func() error, error) {
	return acquireLockFileAt(ctx, root, "state.lock", syscall.LOCK_EX, stateLockWaitCap, "metadata lock")
}
func acquireLockFileAt(ctx context.Context, dir *pinnedDir, name string, operation int, cap time.Duration, what string) (func() error, error) {
	path := filepath.Join(dir.path, name)
	if err := freezeRefuseIfCanceled(ctx, StagePublish); err != nil {
		return nil, err
	}
	file, err := dir.openFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, &FreezeError{Stage: StagePublish, Code: CodeFreezeLeaseFailed, Cause: fmt.Errorf("%s %s: %w", what, path, err)}
	}
	if err := checkFrozenFile(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	deadline := time.Now().Add(cap)
	for {
		if err := freezeRefuseIfCanceled(ctx, StagePublish); err != nil {
			_ = file.Close()
			return nil, err
		}
		err := flockNonblocking(file, operation)
		if err == nil {
			freezeInterpose(freezeLeaseAcquired, FreezeHook{})
			return func() error {
				unlockErr := flockNonblocking(file, syscall.LOCK_UN)
				closeErr := file.Close()
				return errors.Join(unlockErr, closeErr)
			}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = file.Close()
			return nil, &FreezeError{Stage: StagePublish, Code: CodeFreezeLeaseFailed, Cause: fmt.Errorf("%s %s: %w", what, path, err)}
		}
		if time.Now().After(deadline) {
			_ = file.Close()
			return nil, &FreezeError{Stage: StagePublish, Code: CodeFreezeLeaseFailed, Cause: fmt.Errorf("%s %s: contended past %s", what, path, cap)}
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, &FreezeError{Stage: StagePublish, Code: CodeFreezeCanceled, Cause: ctx.Err()}
		case <-time.After(lockPollInterval):
		}
	}
}

func flockNonblocking(file *os.File, operation int) error {
	for {
		err := syscall.Flock(int(file.Fd()), operation|syscall.LOCK_NB)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}
