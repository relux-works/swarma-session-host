//go:build darwin

package musebuilds

import (
	"errors"

	"golang.org/x/sys/unix"
)

// isXattrAbsent reports whether err means the named attribute is
// simply not present. Darwin reports a missing attribute with
// ENOATTR, which is distinct from ENODATA; both mean absence, not
// an error.
func isXattrAbsent(err error) bool {
	return errors.Is(err, unix.ENODATA) || errors.Is(err, unix.ENOATTR)
}
