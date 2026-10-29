//go:build !unix

package sessionmanager

import (
	"errors"
	"fmt"
)

var ErrManagerAlreadyRunning = errors.New("session manager is already running")

type instanceLock struct{}

func acquireInstanceLock(string) (*instanceLock, error) {
	return nil, fmt.Errorf("task-board Session Manager transport is not implemented on this platform")
}

func instanceLockHeld(string) (bool, error) {
	return false, fmt.Errorf("task-board Session Manager transport is not implemented on this platform")
}

func processAlive(int) (bool, error) {
	return false, fmt.Errorf("task-board Session Manager process inspection is not implemented on this platform")
}

func requestProcessTermination(int) error {
	return fmt.Errorf("task-board Session Manager process termination is not implemented on this platform")
}

func forceProcessTermination(int) error {
	return fmt.Errorf("task-board Session Manager process termination is not implemented on this platform")
}

func (l *instanceLock) Close() error { return nil }
