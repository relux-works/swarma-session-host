//go:build darwin || linux

package musebuilds

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

const supportedPlatform = true

// Raw OS operations live here. The snapshot wrappers check ctx immediately
// before and after each operation and own every returned descriptor.
func openDirectoryRoot() (*os.File, error) {
	fd, err := unix.Open("/", directorySearchFlags|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "/"), nil
}

func openDirectoryAt(parent *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(parent.Fd()), name, directorySearchFlags|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func openFileAt(parent *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func descriptorIdentity(file *os.File) (fileIdentity, error) {
	var st unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &st); err != nil {
		return fileIdentity{}, err
	}
	return identityFromStat(st), nil
}

func identityAt(parent *os.File, name string) (fileIdentity, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fileIdentity{}, err
	}
	return identityFromStat(st), nil
}

func readLinkAt(parent *os.File, name string) (string, error) {
	// Both supported kernels bound a symlink below 64 KiB. Equal-to-buffer
	// is refused as possibly truncated, never accepted as a whole target.
	buf := make([]byte, 64*1024)
	n, err := unix.Readlinkat(int(parent.Fd()), name, buf)
	if err != nil {
		return "", err
	}
	if n == len(buf) {
		return "", errors.New("symlink target exceeds read bound")
	}
	return string(buf[:n]), nil
}

func identityFromStat(st unix.Stat_t) fileIdentity {
	mode := os.FileMode(st.Mode & 07777)
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		mode |= os.ModeDir
	case unix.S_IFLNK:
		mode |= os.ModeSymlink
	case unix.S_IFIFO:
		mode |= os.ModeNamedPipe
	case unix.S_IFSOCK:
		mode |= os.ModeSocket
	case unix.S_IFCHR:
		mode |= os.ModeDevice | os.ModeCharDevice
	case unix.S_IFBLK:
		mode |= os.ModeDevice
	case unix.S_IFREG:
	default:
		mode |= os.ModeIrregular
	}
	ms, mn, cs, cn := statTimes(st)
	return fileIdentity{dev: uint64(st.Dev), ino: st.Ino, nlink: uint64(st.Nlink),
		size: st.Size, mtimeS: ms, mtimeN: mn, ctimeS: cs, ctimeN: cn, mode: mode, uid: st.Uid}
}
