package sessionhost

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unicode/utf8"
)

const goalSnapshotLimit = 65536
const goalLocatorLimit = 4096

type goalSourceFailure string

func (e goalSourceFailure) Error() string { return string(e) }

const (
	goalConfigInvalid     goalSourceFailure = "goal_config_invalid"
	goalSourceUnavailable goalSourceFailure = "goal_source_unavailable"
	goalSourceInvalid     goalSourceFailure = "goal_source_invalid"
	goalSourceTooLarge    goalSourceFailure = "goal_source_too_large"
	goalSnapshotChanged   goalSourceFailure = "goal_snapshot_changed"
)

// This reader grants no manager role or delivery authority. Only admitted
// composition may supply its source directory and locator. It retains no goal
// content, starts no timer and does not encode or submit a native request.
type goalFileSource struct {
	mu       sync.Mutex
	root     *os.Root
	parent   string
	identity os.FileInfo
	locator  string
	name     string
	uid      uint32
}

type goalFileSnapshot struct {
	locator string
	digest  string
}

func validGoalLocator(path string) bool {
	if path == "" || len(path) > goalLocatorLimit || !utf8.ValidString(path) || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	for _, c := range path {
		if c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

func goalDirectorySafe(info os.FileInfo, uid uint32) bool {
	if info == nil || !info.IsDir() || info.Mode() & ^(os.ModeDir|os.ModePerm|os.ModeSticky) != 0 {
		return false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || (st.Uid != 0 && st.Uid != uid) {
		return false
	}
	return info.Mode().Perm()&0022 == 0 || (st.Uid == 0 && info.Mode()&os.ModeSticky != 0)
}

func goalAncestrySafe(path string, uid uint32) bool {
	for {
		info, err := os.Lstat(path)
		if err != nil || !goalDirectorySafe(info, uid) {
			return false
		}
		parent := filepath.Dir(path)
		if parent == path {
			return true
		}
		path = parent
	}
}

// Construction observes directories only. A missing optional goal file is
// diagnosed at the first eligible read, never read during initialization.
func newGoalFileSource(directory, locator string) (*goalFileSource, error) {
	if !validGoalLocator(directory) || !validGoalLocator(locator) {
		return nil, goalSourceInvalid
	}
	relative, err := filepath.Rel(directory, locator)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, goalSourceInvalid
	}
	parent := filepath.Dir(locator)
	uid := uint32(os.Geteuid())
	if !goalAncestrySafe(parent, uid) {
		return nil, goalSourceInvalid
	}
	before, err := os.Lstat(parent)
	if err != nil {
		return nil, goalSourceUnavailable
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return nil, goalSourceUnavailable
	}
	s := &goalFileSource{root: root, parent: parent, identity: before, locator: locator, name: filepath.Base(locator), uid: uid}
	if !s.directoryCurrent() {
		_ = root.Close()
		return nil, goalSourceInvalid
	}
	return s, nil
}

// The held parent must remain the declared physical directory. File inode
// replacement is intentionally checked per snapshot so atomic rename works.
func (s *goalFileSource) directoryCurrent() bool {
	if s.root == nil || !goalAncestrySafe(s.parent, s.uid) {
		return false
	}
	held, err1 := s.root.Stat(".")
	named, err2 := os.Lstat(s.parent)
	return err1 == nil && err2 == nil && goalDirectorySafe(held, s.uid) && goalDirectorySafe(named, s.uid) &&
		os.SameFile(s.identity, held) && os.SameFile(s.identity, named)
}

func goalFileSafe(info os.FileInfo, uid uint32) error {
	if info == nil || !info.Mode().IsRegular() || info.Mode() & ^os.ModePerm != 0 || info.Mode().Perm()&0022 != 0 {
		return goalSourceInvalid
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uid || st.Nlink != 1 {
		return goalSourceInvalid
	}
	if info.Mode().Perm()&0400 == 0 {
		return goalSourceUnavailable
	}
	if info.Size() > goalSnapshotLimit {
		return goalSourceTooLarge
	}
	return nil
}

func sameGoalFileSnapshot(a, b os.FileInfo) bool {
	return a != nil && b != nil && os.SameFile(a, b) && a.Mode() == b.Mode() &&
		a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

// This is a bounded optimistic snapshot, not a writer-exclusion lock or proof
// against an admitted writer changing and restoring bytes between observations.
// No raw filesystem error or file bytes escape this function.
func (s *goalFileSource) read() (goalFileSnapshot, error) {
	if s == nil {
		return goalFileSnapshot{}, goalSourceUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root == nil {
		return goalFileSnapshot{}, goalSourceUnavailable
	}
	if !s.directoryCurrent() {
		return goalFileSnapshot{}, goalSourceInvalid
	}
	before, err := s.root.Lstat(s.name)
	if err != nil {
		return goalFileSnapshot{}, goalSourceUnavailable
	}
	if err := goalFileSafe(before, s.uid); err != nil {
		return goalFileSnapshot{}, err
	}
	f, err := s.root.OpenFile(s.name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return goalFileSnapshot{}, goalSourceUnavailable
	}
	defer f.Close()
	held, err := f.Stat()
	if err != nil {
		return goalFileSnapshot{}, goalSourceUnavailable
	}
	if err := goalFileSafe(held, s.uid); err != nil {
		return goalFileSnapshot{}, err
	}
	if !sameGoalFileSnapshot(before, held) {
		return goalFileSnapshot{}, goalSnapshotChanged
	}
	data, err := io.ReadAll(io.LimitReader(f, goalSnapshotLimit+1))
	if err != nil {
		return goalFileSnapshot{}, goalSourceUnavailable
	}
	if len(data) > goalSnapshotLimit {
		return goalFileSnapshot{}, goalSourceTooLarge
	}
	after, err1 := f.Stat()
	namedAfter, err2 := s.root.Lstat(s.name)
	if err1 != nil || err2 != nil || !s.directoryCurrent() {
		return goalFileSnapshot{}, goalSnapshotChanged
	}
	if err := goalFileSafe(after, s.uid); err != nil {
		return goalFileSnapshot{}, err
	}
	if err := goalFileSafe(namedAfter, s.uid); err != nil {
		return goalFileSnapshot{}, err
	}
	if !sameGoalFileSnapshot(before, after) || !sameGoalFileSnapshot(before, namedAfter) || int64(len(data)) != before.Size() {
		return goalFileSnapshot{}, goalSnapshotChanged
	}
	digest := sha256.Sum256(data)
	return goalFileSnapshot{locator: s.locator, digest: hex.EncodeToString(digest[:])}, nil
}

func (s *goalFileSource) close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root == nil {
		return nil
	}
	err := s.root.Close()
	s.root = nil
	if err != nil {
		return goalSourceUnavailable
	}
	return nil
}

// The eventual adapter must check the complete serialized native request
// against its own frame budget. Rendering this text alone is not admission.
func renderGoalWake(snapshot goalFileSnapshot) (string, error) {
	if !validGoalLocator(snapshot.locator) || !notificationSHA256(snapshot.digest) {
		return "", goalConfigInvalid
	}
	locator, err := json.Marshal(snapshot.locator)
	if err != nil {
		return "", goalConfigInvalid
	}
	return "Advance your goals: " + string(locator) + " sha256 " + snapshot.digest, nil
}
