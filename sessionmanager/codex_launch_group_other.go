//go:build !linux

package sessionmanager

import (
	"errors"
	"syscall"
)

// Without a kernel group handle, startup holds its unreaped child until the
// verified group signal has been attempted. Restart never gains that authority.
type codexLaunchGroup struct{}

func prepareCodexLaunchGroup(*syscall.SysProcAttr) *codexLaunchGroup {
	return &codexLaunchGroup{}
}
func (*codexLaunchGroup) retainsOriginalGroup() bool { return false }
func (*codexLaunchGroup) signal(pid int, identity *providerProcessIdentity, signal syscall.Signal) error {
	return signalPersistedProviderGroup(pid, identity, signal)
}
func (*codexLaunchGroup) gone(pid int) bool {
	return errors.Is(syscall.Kill(-pid, 0), syscall.ESRCH)
}
func (*codexLaunchGroup) close() {}
