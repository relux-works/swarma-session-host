//go:build !unix

package musebuilds

import (
	"errors"
	"os"
)

// Freeze refuses before reaching the store on platforms without
// owner identity, so this stub is unreachable in production.
func checkStoreOwner(path string, info os.FileInfo) error {
	return errors.New("store ownership needs unix file identity")
}
