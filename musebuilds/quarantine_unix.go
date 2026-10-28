//go:build unix

package musebuilds

import (
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// quarantineAttr is the Gatekeeper quarantine marker G2 A1 refuses
// to freeze: a quarantined copy waits on an assessment that can hang
// indefinitely on the mini.
const quarantineAttr = "com.apple.quarantine"

// quarantineFDCheck reports whether the file open on file carries
// the quarantine marker. The check reads through the held descriptor,
// never by path, so the verdict binds the exact bytes the caller
// will hash and copy. A present marker refuses; a merely absent one
// is clear. On kernels without the Apple namespace the lookup is
// clear by definition; every other lookup failure refuses, because
// an unknown quarantine state is not a clear one.
func quarantineFDCheck(file *os.File) (bool, error) {
	probe := make([]byte, 4096)
	_, err := unix.Fgetxattr(int(file.Fd()), quarantineAttr, probe)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, unix.ERANGE) {
		return true, nil
	}
	if isXattrAbsent(err) {
		return false, nil
	}
	if runtime.GOOS != "darwin" && errors.Is(err, unix.EOPNOTSUPP) {
		return false, nil
	}
	return false, err
}

// defaultQuarantineCheck reports whether path carries the quarantine
// marker. It opens the source without following symlinks and checks
// through that descriptor, so the verdict cannot be swapped between
// the open and the read.
func defaultQuarantineCheck(path string) (bool, error) {
	file, err := openInstallSource(path)
	if err != nil {
		return false, err
	}
	defer func() { _ = file.Close() }()
	return quarantineFDCheck(file)
}
