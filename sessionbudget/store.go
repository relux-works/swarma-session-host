package sessionbudget

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type journal struct {
	Schema   int                 `json:"schema"`
	Sessions map[string]*account `json:"sessions"`
}

// Store retains host accounting and exact outstanding effect identities. It is
// not a goal authority, revision journal or replacement for provider snapshots.
type Store struct {
	root      string
	mu        sync.Mutex
	uncertain bool
}

func Open(directory string) (*Store, error) {
	root, e := filepath.Abs(directory)
	if e != nil {
		return nil, e
	}
	if e = os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	s, e := os.Lstat(root)
	if e != nil {
		return nil, e
	}
	if !s.IsDir() || s.Mode().Perm() != 0700 {
		return nil, ErrAccountingUnavailable
	}
	out := &Store{root: root}
	if e = out.transaction(context.Background(), false, func(*journal) error { return nil }); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Store) transaction(ctx context.Context, write bool, fn func(*journal) error) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain {
		return ErrAccountingUncertain
	}
	lockPath := filepath.Join(s.root, "accounting.lock")
	if i, e := os.Lstat(lockPath); e == nil {
		if !i.Mode().IsRegular() || i.Mode().Perm() != 0600 {
			return ErrAccountingUnavailable
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	lock, e := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	for {
		e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if e == nil {
			break
		}
		if e != syscall.EAGAIN && e != syscall.EWOULDBLOCK {
			return e
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	j := &journal{Schema: 1, Sessions: map[string]*account{}}
	path := filepath.Join(s.root, "accounting.json")
	if i, e := os.Lstat(path); e == nil {
		if !i.Mode().IsRegular() || i.Mode().Perm() != 0600 || i.Size() >= 64<<20 {
			return ErrAccountingUnavailable
		}
		f, e := os.Open(path)
		if e != nil {
			return e
		}
		d := json.NewDecoder(io.LimitReader(f, 64<<20))
		d.DisallowUnknownFields()
		e = d.Decode(j)
		if e == nil {
			var extra any
			if d.Decode(&extra) != io.EOF {
				e = ErrAccountingUnavailable
			}
		}
		closeErr := f.Close()
		if e != nil {
			return fmt.Errorf("%w: %v", ErrAccountingUnavailable, e)
		}
		if closeErr != nil {
			return closeErr
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if e = validateJournal(j); e != nil {
		return e
	}
	if e = fn(j); e != nil {
		return e
	}
	if !write {
		return nil
	}
	if e = validateJournal(j); e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	b, e := json.Marshal(j)
	if e != nil {
		return e
	}
	if len(b) >= 64<<20 {
		return ErrAccountingUnavailable
	}
	f, e := os.CreateTemp(s.root, ".accounting-")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, e = f.Write(b); e != nil {
		_ = f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		_ = f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(tmp, path); e != nil {
		return e
	}
	dir, e := os.Open(s.root)
	if e == nil {
		e = dir.Sync()
		_ = dir.Close()
	}
	if e != nil {
		s.uncertain = true
		return errors.Join(ErrAccountingUncertain, e)
	}
	return nil
}
func validateJournal(j *journal) error {
	if j.Schema != 1 || j.Sessions == nil {
		return ErrAccountingUnavailable
	}
	for id, a := range j.Sessions {
		if a == nil || id != a.Lease.SessionID || a.Lease.Epoch == 0 || a.NativeID == "" {
			return ErrAccountingUnavailable
		}
		if e := a.validate(); e != nil {
			return e
		}
	}
	return nil
}
