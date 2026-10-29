//go:build unix

package sessionmanager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

var ErrManagerAlreadyRunning = errors.New("session manager is already running")

type instanceLock struct {
	file *os.File
}

func acquireInstanceLock(path string) (*instanceLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening session manager lock: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("securing session manager lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrManagerAlreadyRunning
		}
		return nil, fmt.Errorf("locking session manager instance: %w", err)
	}
	return &instanceLock{file: file}, nil
}

func instanceLockHeld(path string) (bool, error) {
	lockDir := filepath.Dir(path)
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		return false, fmt.Errorf("creating session manager lock directory: %w", err)
	}
	if err := os.Chmod(lockDir, 0o700); err != nil {
		return false, fmt.Errorf("securing session manager lock directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false, fmt.Errorf("opening session manager lock probe: %w", err)
	}
	defer file.Close()
	if err := file.Chmod(0o600); err != nil {
		return false, fmt.Errorf("securing session manager lock probe: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return true, nil
		}
		return false, fmt.Errorf("probing session manager instance lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		return false, fmt.Errorf("unlocking session manager lock probe: %w", err)
	}
	return false, nil
}

func processAlive(pid int) (bool, error) {
	err := unix.Kill(pid, 0)
	if err == nil || errors.Is(err, unix.EPERM) {
		return true, nil
	}
	if errors.Is(err, unix.ESRCH) {
		return false, nil
	}
	return false, err
}

func requestProcessTermination(pid int) error {
	return unix.Kill(pid, unix.SIGTERM)
}

func forceProcessTermination(pid int) error {
	return unix.Kill(pid, unix.SIGKILL)
}

func (l *instanceLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	closeErr := l.file.Close()
	return errors.Join(unlockErr, closeErr)
}
