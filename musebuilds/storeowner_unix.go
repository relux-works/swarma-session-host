//go:build unix

package musebuilds

import (
	"fmt"
	"os"
	"syscall"
)

// checkStoreOwner requires frozen shared parents to be owned by the
// current user. The expected owner rides the currentEUID seam so
// tests can prove the refusal without privileges.
func checkStoreOwner(path string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s owner is unknown", path)
	}
	uid := stat.Uid
	if storeUIDForTest != nil {
		uid = storeUIDForTest(path, uid)
	}
	if uid != uint32(currentEUID()) {
		return fmt.Errorf("%s is owned by another user", path)
	}
	return nil
}

// Tests inject only the descriptor's UID observation, never an admission result.
// The production value always comes from fstat on the opened object.
var storeUIDForTest func(string, uint32) uint32
