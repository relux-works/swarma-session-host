//go:build unix

package musebuilds

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// pinnedDir is directory authority. path is only a diagnostic identity;
// all descendant operations are relative to the retained descriptor.
type pinnedDir struct {
	file *os.File
	path string
}

func validChild(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name
}

// openDirPath bootstraps a root without following ANY symlink component.
// Ancestors may belong to the OS; the final directory must belong to this
// account. Descendants are subsequently opened through this held root.
func openDirPath(ctx context.Context, path string, create bool) (*pinnedDir, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("noncanonical directory %q", path)
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	dir := &pinnedDir{file: os.NewFile(uintptr(fd), "/"), path: "/"}
	for _, name := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if name == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			_ = dir.close()
			return nil, err
		}
		if create {
			if _, err := dir.mkdirChild(name); err != nil {
				_ = dir.close()
				return nil, err
			}
		}
		next, err := dir.openDirectory(name)
		_ = dir.close()
		if err != nil {
			return nil, err
		}
		dir = next
	}
	info, err := dir.file.Stat()
	if err == nil {
		err = checkStoreOwner(path, info)
	}
	if err != nil {
		_ = dir.close()
		return nil, err
	}
	return dir, nil
}

func openPinnedDir(path string) (*pinnedDir, error) {
	return openDirPath(context.Background(), path, false)
}

func (d *pinnedDir) close() error { return d.file.Close() }

func (d *pinnedDir) mkdirChild(name string) (bool, error) {
	if !validChild(name) {
		return false, fmt.Errorf("invalid child %q", name)
	}
	if err := unix.Mkdirat(int(d.file.Fd()), name, 0o700); err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, err
	}
	// A created directory needs its parent's durable entry before use.
	if err := d.sync(); err != nil {
		return false, err
	}
	return true, nil
}

func (d *pinnedDir) openDirectory(name string) (*pinnedDir, error) {
	if !validChild(name) {
		return nil, fmt.Errorf("invalid child %q", name)
	}
	fd, err := unix.Openat(int(d.file.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return &pinnedDir{file: os.NewFile(uintptr(fd), filepath.Join(d.path, name)), path: filepath.Join(d.path, name)}, nil
}

func (d *pinnedDir) openChildDir(name string) (*pinnedDir, error) {
	child, err := d.openDirectory(name)
	if err != nil {
		return nil, err
	}
	info, err := child.file.Stat()
	if err == nil {
		err = checkStoreOwner(child.path, info)
	}
	if err != nil {
		_ = child.close()
		return nil, err
	}
	return child, nil
}

func (d *pinnedDir) entries() ([]os.DirEntry, error) {
	// A fresh descriptor resets the inventory cursor without reopening a path.
	fd, err := unix.Openat(int(d.file.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), d.path)
	defer file.Close()
	return file.ReadDir(-1)
}

func (d *pinnedDir) openFile(name string, flags int, mode os.FileMode) (*os.File, error) {
	if !validChild(name) {
		return nil, fmt.Errorf("invalid child %q", name)
	}
	fd, err := unix.Openat(int(d.file.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), filepath.Join(d.path, name)), nil
}
func (d *pinnedDir) openChildFile(name string) (*os.File, error) {
	return d.openFile(name, unix.O_RDONLY, 0)
}

func (d *pinnedDir) childExists(name string) (bool, error) {
	if !validChild(name) {
		return false, fmt.Errorf("invalid child %q", name)
	}
	var st unix.Stat_t
	err := unix.Fstatat(int(d.file.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func renamePinnedChild(srcDir, dstDir *pinnedDir, oldName, newName string) error {
	if !validChild(oldName) || !validChild(newName) {
		return fmt.Errorf("invalid rename")
	}
	return unix.Renameat(int(srcDir.file.Fd()), oldName, int(dstDir.file.Fd()), newName)
}

func (d *pinnedDir) chmodChild(name string, mode os.FileMode) error {
	file, err := d.openChildFile(name)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := checkFrozenFile(file); err != nil {
		return err
	}
	return file.Chmod(mode)
}
func (d *pinnedDir) chmodSelf(mode os.FileMode) error { return d.file.Chmod(mode) }
func (d *pinnedDir) sync() error                      { return fsyncFile(d.file) }

// Nanosecond-preserving, FD-relative timestamps; never follow a leaf link.
func (d *pinnedDir) chtimesChild(name string, mtime time.Time) error {
	if !validChild(name) {
		return fmt.Errorf("invalid child %q", name)
	}
	ts := unix.NsecToTimespec(mtime.UnixNano())
	return unix.UtimesNanoAt(int(d.file.Fd()), name, []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
}
