//go:build !unix

package historyguard

import "os"

func tempDirectoryOwner(os.FileInfo) bool { return false }
