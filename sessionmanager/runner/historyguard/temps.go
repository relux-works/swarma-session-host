package historyguard

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const ControllerTempDir = ".controller-epochs.tmp"
const OwnerTempDir = ".session-owner-keys.tmp"

// TempDirectory is a sibling of the committed store. Both witness replicas
// share their store's area; no temporary is written at a prior-state location.
func TempDirectory(target string) string {
	name := strings.TrimSuffix(filepath.Base(target), ".intent")
	name = strings.TrimSuffix(name, ".lock")
	switch name {
	case "controller-epochs.v1.json":
		name = ControllerTempDir
	case "session-owner-keys.v1.json":
		name = OwnerTempDir
	default:
		name = "." + name + ".tmp"
	}
	return filepath.Join(filepath.Dir(target), name)
}

func openTempDirectory(path string, create bool) (*os.Root, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && create {
		if err = os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		parent, err := os.Open(filepath.Dir(path))
		if err != nil {
			return nil, err
		}
		err = parent.Sync()
		closeErr := parent.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0700 || !tempDirectoryOwner(info) {
		return nil, ErrUncertain
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	actual, err := root.Stat(".")
	current, pathErr := os.Lstat(path)
	if err != nil || pathErr != nil || !os.SameFile(info, actual) || !os.SameFile(info, current) || !current.IsDir() || current.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0700 {
		root.Close()
		return nil, ErrUncertain
	}
	return root, nil
}

// Missing areas are safely recreated under the store fence. Existing
// symlinks, files, foreign-owned directories and permissions are never repaired.
func ValidateTempDirectories(stateDir string) error {
	for _, name := range []string{ControllerTempDir, OwnerTempDir} {
		root, err := openTempDirectory(filepath.Join(stateDir, name), true)

		if err != nil {
			return err
		}
		if err := root.Close(); err != nil {
			return err
		}
	}
	return nil
}

// SkipTempDirectory recognizes a location, never a filename or file's bytes.
// A walker must return filepath.SkipDir when true: it never reads any content.
func SkipTempDirectory(stateDir, path string) (bool, error) {
	if path != filepath.Join(stateDir, ControllerTempDir) && path != filepath.Join(stateDir, OwnerTempDir) {
		return false, nil
	}
	root, err := openTempDirectory(path, false)
	if err != nil {
		return false, err
	}
	return true, root.Close()
}

func CreateTemp(target, prefix string) (*os.File, error) {
	root, err := openTempDirectory(TempDirectory(target), true)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	for attempt := 0; attempt < 32; attempt++ {
		var random [8]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, err
		}
		name := prefix + hex.EncodeToString(random[:])
		f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		actual, err := root.Stat(".")
		current, pathErr := os.Lstat(TempDirectory(target))
		if err != nil || pathErr != nil || !os.SameFile(actual, current) || !current.IsDir() || current.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0700 {
			f.Close()
			_ = root.Remove(name)
			return nil, ErrUncertain
		}
		return f, nil
	}
	return nil, ErrUncertain
}

// Rename removes the source name as well as adding the committed target name.
// Both directory entries must be synced before the value can be returned.
func SyncTempDirectory(target string, syncFile func(*os.File) error) error {
	root, err := openTempDirectory(TempDirectory(target), false)
	if err != nil {
		return err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return syncFile(dir)
}
