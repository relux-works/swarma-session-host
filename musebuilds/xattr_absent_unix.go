//go:build unix && !darwin

package musebuilds

import (
	"errors"

	"golang.org/x/sys/unix"
)

// isXattrAbsent reports whether err means the named attribute is
// simply not present. Outside Darwin the kernel reports ENODATA;
// ENOATTR does not exist there.
func isXattrAbsent(err error) bool {
	return errors.Is(err, unix.ENODATA)
}
