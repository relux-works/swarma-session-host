package sessionhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const maxLauncherExecutable = 64 << 20

// launcherExecutable retains a configured, canonical installed file and its
// parent directory. No request chooses the file; no PATH lookup, version probe,
// installation, repair, profile composition or credential read occurs here.
type launcherExecutable struct {
	path, digest string
	root         *os.Root
	file         *os.File
	parent       os.FileInfo
	identity     os.FileInfo
	mu           sync.Mutex
	lost         bool
}

// The positive boundary accepts ordinary regular executable files and directory
// permissions, owned by root or this operator, without group/other writes or
// special mode bits. Owner-writable installed binaries remain legitimate.
func launcherInfoSafe(info os.FileInfo, directory bool) bool {
	if info == nil || info.IsDir() != directory || info.Mode().Perm()&0022 != 0 {
		return false
	}
	mode := info.Mode()
	if directory {
		if mode & ^(os.ModeDir|os.ModePerm) != 0 || mode.Perm()&0100 == 0 {
			return false
		}
	} else if !mode.IsRegular() || mode & ^os.ModePerm != 0 || mode.Perm()&0111 == 0 || info.Size() <= 0 || info.Size() > maxLauncherExecutable {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == 0 || stat.Uid == uint32(os.Geteuid()))
}

func openLauncherExecutable(path, digest string) (*launcherExecutable, error) {
	if !canonicalRecordDirectory(path) || !notificationSHA256(digest) {
		return nil, errOrdinaryWireUnavailable
	}
	directory := filepath.Dir(path)
	parent, err := os.Lstat(directory)
	if err != nil || !launcherInfoSafe(parent, true) {
		return nil, errOrdinaryWireUnavailable
	}
	before, err := os.Lstat(path)
	if err != nil || !launcherInfoSafe(before, false) {
		return nil, errOrdinaryWireUnavailable
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, errOrdinaryWireUnavailable
	}
	l := &launcherExecutable{path: path, digest: digest, root: root, parent: parent, identity: before}
	// NOFOLLOW refuses the final symlink and NONBLOCK avoids a raced FIFO open.
	l.file, err = root.OpenFile(filepath.Base(path), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil || l.recheck(context.Background()) != nil {
		l.close()
		return nil, errOrdinaryWireUnavailable
	}
	return l, nil
}

func sameLauncherFile(a, b os.FileInfo) bool {
	return launcherInfoSafe(a, false) && launcherInfoSafe(b, false) && os.SameFile(a, b) &&
		a.Size() == b.Size() && a.Mode() == b.Mode() && a.ModTime().Equal(b.ModTime())
}

func (l *launcherExecutable) recheck(ctx context.Context) error {
	if l == nil || ctx == nil || ctx.Err() != nil {
		return errOrdinaryWireUnavailable
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost || l.root == nil || l.file == nil {
		return errOrdinaryWireUnavailable
	}
	fail := func() error { l.lost = true; return errOrdinaryWireUnavailable }
	if !canonicalRecordDirectory(l.path) {
		return fail()
	}
	currentParent, e1 := os.Lstat(filepath.Dir(l.path))
	heldParent, e2 := l.root.Stat(".")
	current, e3 := os.Lstat(l.path)
	held, e4 := l.file.Stat()
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil ||
		!launcherInfoSafe(currentParent, true) || !launcherInfoSafe(heldParent, true) ||
		!os.SameFile(currentParent, l.parent) || !os.SameFile(heldParent, l.parent) ||
		!sameLauncherFile(current, l.identity) || !sameLauncherFile(held, l.identity) {
		return fail()
	}
	h := sha256.New()
	n, err := io.Copy(h, io.NewSectionReader(l.file, 0, maxLauncherExecutable+1))
	after, e5 := l.file.Stat()
	currentAfter, e6 := os.Lstat(l.path)
	if err != nil || e5 != nil || e6 != nil || n != l.identity.Size() ||
		!sameLauncherFile(after, l.identity) || !sameLauncherFile(currentAfter, l.identity) ||
		hex.EncodeToString(h.Sum(nil)) != l.digest {
		return fail()
	}
	if ctx.Err() != nil {
		return errOrdinaryWireUnavailable
	}
	return nil
}

func (l *launcherExecutable) close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lost = true
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
	if l.root != nil {
		_ = l.root.Close()
		l.root = nil
	}
}
