//go:build !unix

package musebuilds

import (
	"errors"
	"os"
)

// Freeze refuses before reaching quarantine on platforms without
// xattrs, so these stubs are unreachable in production. They fail
// closed if ever reached.
func defaultQuarantineCheck(path string) (bool, error) {
	return false, errors.New("quarantine checks need unix xattrs")
}

func quarantineFDCheck(file *os.File) (bool, error) {
	return false, errors.New("quarantine checks need unix xattrs")
}

const quarantineAttr = "com.apple.quarantine"
