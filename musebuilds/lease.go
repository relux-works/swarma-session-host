package musebuilds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// fileIdentity deliberately omits atime: our own reads may change it. All
// fields which describe the object/bytes or static source gates are retained.
type fileIdentity struct {
	dev, ino, nlink uint64
	size            int64
	mtimeS, mtimeN  int64
	ctimeS, ctimeN  int64
	mode            os.FileMode
	uid             uint32
}

func sameIdentity(a, b fileIdentity) bool {
	return a == b
}

// sameDirectoryIdentity compares the object identity of two directories:
// device, inode, type+mode and owner. A directory's mtime, ctime, size and
// link count change whenever ANY entry is added or removed, including
// entries discovery never relied on, so they are not part of a held
// directory's identity. The names discovery actually depends on are
// verified separately through s.names (including negative lookups) and,
// for the install directory, through the held inventory comparison in
// recheckSelection.
func sameDirectoryIdentity(a, b fileIdentity) bool {
	return a.dev == b.dev && a.ino == b.ino && a.mode == b.mode && a.uid == b.uid
}

// sameLeaseIdentity compares two observations of one leased object. Held
// directories compare by object identity; every other object — regular
// files above all — keeps the full comparison including mtime, ctime and
// size, so in-place writes are still detected. A directory compared
// against a non-directory never matches: the mode carries the type.
func sameLeaseIdentity(a, b fileIdentity) bool {
	if a.mode.IsDir() && b.mode.IsDir() {
		return sameDirectoryIdentity(a, b)
	}
	return sameIdentity(a, b)
}

type fileLease struct {
	file *os.File
	path string
	id   fileIdentity
}

type nameObservation struct {
	parent *fileLease
	name   string
	id     fileIdentity
	err    error // ENOENT is an observation, not a reason to forget the name.
}

// discoverySnapshot is the sole descriptor owner. Even failed PATH candidates
// contribute the directory/name observations which let a later candidate win.
// Search handles need no read permission; the scan acquires a separate readable
// directory handle, so execute-only directories fail at StageScan.
type discoverySnapshot struct {
	dirs          map[string]*fileLease
	files         []*fileLease
	names         []nameObservation
	inventoryFile *fileLease
	inventory     []string
	sidecar       *fileLease
	sidecarBytes  []byte
}

func (s *discoverySnapshot) close() error {
	var errs []error
	for i := len(s.files) - 1; i >= 0; i-- {
		if err := s.files[i].file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.files = nil
	return errors.Join(errs...)
}

func statLease(ctx context.Context, f *os.File, stage string) (fileIdentity, error) {
	if err := refuseIfCanceled(ctx, stage); err != nil {
		return fileIdentity{}, err
	}
	id, err := descriptorIdentity(f)
	if canceled := refuseIfCanceled(ctx, stage); canceled != nil {
		return fileIdentity{}, canceled
	}
	return id, err
}

func statName(ctx context.Context, parent *fileLease, name, stage string) (fileIdentity, error) {
	if err := refuseIfCanceled(ctx, stage); err != nil {
		return fileIdentity{}, err
	}
	id, err := identityAt(parent.file, name)
	if canceled := refuseIfCanceled(ctx, stage); canceled != nil {
		return fileIdentity{}, canceled
	}
	return id, err
}

func sameObservation(a fileIdentity, aerr error, b fileIdentity, berr error) bool {
	if aerr != nil || berr != nil {
		return aerr != nil && berr != nil && errors.Is(berr, aerr)
	}
	return sameLeaseIdentity(a, b)
}

func (s *discoverySnapshot) observe(ctx context.Context, parent *fileLease, name, stage string) (fileIdentity, error) {
	id, err := statName(ctx, parent, name, stage)
	if e, ok := AsDiscoveryError(err); ok && e.Code == CodeCanceled {
		return id, err
	}
	for _, earlier := range s.names {
		if earlier.parent == parent && earlier.name == name {
			if !sameObservation(earlier.id, earlier.err, id, err) {
				return id, changed(fmt.Errorf("%s/%s changed during lookup", parent.path, name))
			}
			return id, err
		}
	}
	s.names = append(s.names, nameObservation{parent, name, id, err})
	return id, err
}

// hold records an opened FD before fstat, including errors/cancellation, so
// every exit releases it. The expected identity comes from the held parent.
func (s *discoverySnapshot) hold(ctx context.Context, f *os.File, path, stage string, expected *fileIdentity) (*fileLease, error) {
	lease := &fileLease{file: f, path: path}
	s.files = append(s.files, lease)
	id, err := statLease(ctx, f, stage)
	if err != nil {
		return nil, err
	}
	lease.id = id
	if expected != nil && !sameLeaseIdentity(id, *expected) {
		return nil, changed(fmt.Errorf("%s changed between lookup and open", path))
	}
	return lease, nil
}

func (s *discoverySnapshot) root(ctx context.Context, stage string) (*fileLease, error) {
	if root := s.dirs["/"]; root != nil {
		return root, nil
	}
	if err := refuseIfCanceled(ctx, stage); err != nil {
		return nil, err
	}
	f, err := openDirectoryRoot()
	if err != nil {
		return nil, err
	}
	root, err := s.hold(ctx, f, "/", stage, nil)
	if err == nil {
		s.dirs["/"] = root
	}
	return root, err
}

func (s *discoverySnapshot) directory(ctx context.Context, parent *fileLease, name, stage string, id fileIdentity) (*fileLease, error) {
	path := filepath.Join(parent.path, name)
	if dir := s.dirs[path]; dir != nil {
		if !sameLeaseIdentity(dir.id, id) {
			return nil, changed(fmt.Errorf("directory %s changed", path))
		}
		return dir, nil
	}
	if err := refuseIfCanceled(ctx, stage); err != nil {
		return nil, err
	}
	f, err := openDirectoryAt(parent.file, name)
	if err != nil {
		return nil, err
	}
	dir, err := s.hold(ctx, f, path, stage, &id)
	if err == nil {
		s.dirs[path] = dir
	}
	return dir, err
}

// lookup follows symlinks one component at a time, keeping physical parents.
// In particular, a relative link target's .. is applied AFTER preceding links
// are resolved, not lexically cleaned across a symlink. All reads are relative
// to leased directory FDs; acceptance needs no new lookup or open by path.
func (s *discoverySnapshot) lookup(ctx context.Context, path, stage string) (*fileLease, string, fileIdentity, error) {
	dir, err := s.root(ctx, stage)
	if err != nil {
		return nil, "", fileIdentity{}, err
	}
	parts := strings.Split(path, "/")
	links := 0
	for len(parts) > 0 {
		if err := refuseIfCanceled(ctx, stage); err != nil {
			return nil, "", fileIdentity{}, err
		}
		name := parts[0]
		parts = parts[1:]
		if name == "" || name == "." {
			continue
		}
		if name == ".." {
			dir = s.dirs[filepath.Dir(dir.path)]
			continue
		}
		id, err := s.observe(ctx, dir, name, stage)
		if err != nil {
			return nil, "", fileIdentity{}, err
		}
		if id.mode&os.ModeSymlink != 0 {
			links++
			if links > 255 {
				return nil, "", fileIdentity{}, fmt.Errorf("too many symlinks resolving %s", path)
			}
			if err := refuseIfCanceled(ctx, stage); err != nil {
				return nil, "", fileIdentity{}, err
			}
			target, err := readLinkAt(dir.file, name)
			if canceled := refuseIfCanceled(ctx, stage); canceled != nil {
				return nil, "", fileIdentity{}, canceled
			}
			if err != nil {
				return nil, "", fileIdentity{}, err
			}
			if filepath.IsAbs(target) {
				dir = s.dirs["/"]
			}
			parts = append(strings.Split(target, "/"), parts...)
			continue
		}
		if len(parts) == 0 {
			return dir, name, id, nil
		}
		if !id.mode.IsDir() {
			return nil, "", fileIdentity{}, fmt.Errorf("%s is not a directory", filepath.Join(dir.path, name))
		}
		dir, err = s.directory(ctx, dir, name, stage, id)
		if err != nil {
			return nil, "", fileIdentity{}, err
		}
	}
	return dir, ".", dir.id, nil
}

func (s *discoverySnapshot) open(ctx context.Context, dir *fileLease, name, stage string) (*fileLease, error) {
	id, err := s.observe(ctx, dir, name, stage)
	if err != nil {
		return nil, err
	}
	if err := refuseIfCanceled(ctx, stage); err != nil {
		return nil, err
	}
	f, err := openFileAt(dir.file, name)
	if err != nil {
		return nil, err
	}
	return s.hold(ctx, f, filepath.Join(dir.path, name), stage, &id)
}

// verify checks all held objects and directory entries, including negative
// lookups. For regular files ctime detects in-place writes even if size and
// mtime are restored; held directories compare by object identity, so an
// unrelated entry created in a shared ancestor is not a disagreement.
func (s *discoverySnapshot) verify(ctx context.Context) error {
	for _, lease := range s.files {
		id, err := statLease(ctx, lease.file, StageRecheck)
		if err != nil {
			return refusal(ctx, StageRecheck, CodeDiscoveryChanged, err)
		}
		if !sameLeaseIdentity(id, lease.id) {
			return changed(fmt.Errorf("held object %s changed", lease.path))
		}
	}
	for _, name := range s.names {
		id, err := statName(ctx, name.parent, name.name, StageRecheck)
		if canceled := refuseIfCanceled(ctx, StageRecheck); canceled != nil {
			return canceled
		}
		if !sameObservation(name.id, name.err, id, err) {
			return changed(fmt.Errorf("name %s/%s changed", name.parent.path, name.name))
		}
	}
	return refuseIfCanceled(ctx, StageRecheck)
}
