//go:build unix

package sessionmanager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

const ErrorBoardlessSocketUnavailable = "session_boardless_socket_unavailable"

func boardlessSocketRefusal(reason string) error {
	return &PublicError{Code: ErrorBoardlessSocketUnavailable, Message: "boardless control socket unavailable: " + reason}
}

// sun_path includes its terminating NUL: a 104-byte field holds 103 path bytes.
func boardlessSocketLimit(platform string) int {
	if platform == "linux" {
		return 108
	}
	return 104
}

func checkBoardlessSocketLength(path string) error {
	if !filepath.IsAbs(path) || len(path) >= boardlessSocketLimit(runtime.GOOS) {
		return boardlessSocketRefusal("path exceeds the platform limit")
	}
	return nil
}

// A 96-bit root-key digest and uid keep boardless endpoints deterministic and
// separate. Mkdir has exclusive-create semantics; an existing entry is accepted
// only after Lstat verifies its type, owner and exact private permissions.
// root is injected by isolated tests; production always uses the short /tmp root.
func boardlessSocketPathAt(root, fingerprint string) (string, error) {
	// The hash prefix is an explicit socket identifier, not truncation of a
	// filesystem path. Leave capacity for attachment sockets under this root.
	if len(fingerprint) != 64 {
		return "", boardlessSocketRefusal("invalid root fingerprint")
	}
	dir := filepath.Join(root, fmt.Sprintf("tbbl-%d-%s", os.Getuid(), fingerprint[:24]))
	path := filepath.Join(dir, "control.sock")
	if err := checkBoardlessSocketLength(path); err != nil {
		return "", err
	}
	if err := ensureBoardlessSocketDirectory(dir); err != nil {
		return "", err
	}
	return path, nil
}

func ensureBoardlessSocketDirectory(dir string) error {
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return boardlessSocketRefusal("cannot create private directory")
	}
	return checkBoardlessSocketDirectory(dir)
}

func checkBoardlessSocketDirectory(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return boardlessSocketRefusal("cannot inspect private directory")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode() != os.ModeDir|0700 || owner.Uid != uint32(os.Getuid()) {
		return boardlessSocketRefusal("directory must be owned by this user, mode 0700, and not a symlink")
	}
	return nil
}

// Check before generic directory setup can chmod anything, then again just
// before bind. A missing state directory may be created by the normal setup.
func checkBoardlessSocketBeforeSetup(path string) error {
	if err := checkBoardlessSocketLength(path); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return checkBoardlessSocketDirectory(dir)
}
