//go:build !unix

package musebuilds

import (
	"errors"
	"os"
)

// Freeze refuses before reaching the source on platforms without
// no-follow opens, so these stubs are unreachable in production.
func openInstallSource(path string) (*os.File, error) {
	return nil, errors.New("no-follow source opens need unix")
}

func openMetadataFile(path string) (*os.File, error) {
	return nil, errors.New("no-follow metadata opens need unix")
}
