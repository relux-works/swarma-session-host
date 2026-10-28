//go:build unix

package minilock

import (
	"os/exec"
	"syscall"
)

// Unix holds run in their own process group so Release signals the
// script and its hold command together. The script's TERM trap then
// withdraws the ticket and drops the lock.
const platformSupported = true

func setGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killGroup(pid int, forced bool) error {
	sig := syscall.SIGTERM
	if forced {
		sig = syscall.SIGKILL
	}
	return syscall.Kill(-pid, sig)
}
