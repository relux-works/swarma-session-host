package musebuilds

import (
	"context"
	"fmt"
	"os"
)

// frozenStore holds every shared destination directory until the attempt
// exits. No publication step rediscovers those directories from a path.
type frozenStore struct{ root, builds, staging, meta *pinnedDir }

func (s *frozenStore) close() {
	for _, d := range []*pinnedDir{s.meta, s.staging, s.builds, s.root} {
		if d != nil {
			_ = d.close()
		}
	}
}

func openFrozenStore(ctx context.Context, root string) (*frozenStore, error) {
	s := &frozenStore{}
	var err error
	s.root, err = openDirPath(ctx, root, true)
	if err != nil {
		return nil, err
	}
	if err = requireDirMode(s.root, 0o700); err != nil {
		s.close()
		return nil, err
	}
	for _, child := range []struct {
		name string
		dest **pinnedDir
	}{
		{"builds", &s.builds}, {"staging", &s.staging}, {"meta", &s.meta},
	} {
		if err = ctx.Err(); err != nil {
			s.close()
			return nil, err
		}
		if _, err = s.root.mkdirChild(child.name); err != nil {
			s.close()
			return nil, err
		}
		*child.dest, err = s.root.openChildDir(child.name)
		if err == nil {
			err = requireDirMode(*child.dest, 0o700)
		}
		if err != nil {
			s.close()
			return nil, err
		}
	}
	return s, nil
}

func requireDirMode(dir *pinnedDir, mode os.FileMode) error {
	info, err := dir.file.Stat()
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != mode {
		return fmt.Errorf("%s mode is %s, want %o", dir.path, info.Mode(), mode)
	}
	return nil
}

// atomicDocumentAt keeps failed temporaries for reviewed cleanup too.
// No automatic deletion is needed for correctness or admission.
func atomicDocumentAt(ctx context.Context, dir *pinnedDir, name, prefix string, raw []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	txn, err := newTransactionID()
	if err != nil {
		return err
	}
	tmpName := prefix + txn
	file, err := dir.openFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(raw); err != nil {
		return err
	}
	if err := fsyncFile(file); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := renamePinnedChild(dir, dir, tmpName, name); err != nil {
		return err
	}
	if err := dir.sync(); err != nil {
		return fmt.Errorf("%w: %v", errPublicationUnknown, err)
	}
	return nil
}

func checkFrozenFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not regular", file.Name())
	}
	return checkStoreOwner(file.Name(), info)
}

func sameChildFile(dir *pinnedDir, name string, held *os.File) error {
	file, err := dir.openChildFile(name)
	if err != nil {
		return err
	}
	defer file.Close()
	a, err := file.Stat()
	if err != nil {
		return err
	}
	b, err := held.Stat()
	if err != nil {
		return err
	}
	if !a.Mode().IsRegular() || !os.SameFile(a, b) {
		return fmt.Errorf("staging name changed after verification")
	}
	return nil
}
