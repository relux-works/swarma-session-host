package sessionmanager

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

// include/uapi/linux/pidfd.h, Linux 6.9+. The pinned x/sys version exposes the
// syscall but does not yet generate this flag.
const codexPIDFDSignalProcessGroup = 1 << 2

// The clone-created pidfd refers to the exact child's kernel pid object, not
// a later process with the same number. Linux 6.9+ can signal/probe that object
// as a process group even after its leader is reaped. No descriptor is restored
// from a private record or retained beyond startup.
type codexLaunchGroup struct {
	fd       int
	retained bool
}

func prepareCodexLaunchGroup(attr *syscall.SysProcAttr) *codexLaunchGroup {
	g := &codexLaunchGroup{fd: -1}
	attr.PidFD = &g.fd
	return g
}

func (g *codexLaunchGroup) retainsOriginalGroup() bool {
	if g.fd >= 0 {
		err := unix.PidfdSendSignal(g.fd, 0, nil, codexPIDFDSignalProcessGroup)
		g.retained = err == nil || errors.Is(err, syscall.ESRCH)
	}
	return g.retained
}

func (g *codexLaunchGroup) signal(pid int, identity *providerProcessIdentity, signal syscall.Signal) error {
	if g.retained {
		err := unix.PidfdSendSignal(g.fd, unix.Signal(signal), nil, codexPIDFDSignalProcessGroup)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	return signalPersistedProviderGroup(pid, identity, signal)
}

func (g *codexLaunchGroup) gone(pid int) bool {
	if g.retained {
		return errors.Is(unix.PidfdSendSignal(g.fd, 0, nil, codexPIDFDSignalProcessGroup), syscall.ESRCH)
	}
	return errors.Is(syscall.Kill(-pid, 0), syscall.ESRCH)
}

func (g *codexLaunchGroup) close() {
	if g.fd >= 0 {
		_ = unix.Close(g.fd)
		g.fd = -1
	}
}
