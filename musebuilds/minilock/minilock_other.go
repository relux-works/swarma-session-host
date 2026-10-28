//go:build !unix

package minilock

import (
	"errors"
	"os/exec"
)

// Non-unix platforms cannot hold the lock: the script, the hold
// process group and the sentinel protocol are all unix-shaped.
const platformSupported = false

func setGroup(cmd *exec.Cmd) {}

func killGroup(pid int, forced bool) error {
	return errors.New("minilock: process groups need unix")
}
