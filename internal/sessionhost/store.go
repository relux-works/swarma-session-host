package sessionhost

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

type store struct {
	dir   string
	lock  *os.File
	fresh bool
}

func openStore(dir string) (*store, error) {
	if !filepath.IsAbs(dir) {
		return nil, errors.New("invalid_state_directory")
	}
	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, errors.New("state_directory_not_private")
	}
	lockPath := filepath.Join(dir, "host.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	fresh := err == nil
	if os.IsExist(err) {
		lock, err = os.OpenFile(lockPath, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	}
	if err != nil {
		return nil, errors.New("state_lock_unavailable")
	}
	info, err = lock.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		lock.Close()
		return nil, errors.New("invalid_state_lock")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("host_already_active")
	}
	return &store{dir: dir, lock: lock, fresh: fresh}, nil
}

func (s *store) read() (State, bool, error) {
	if _, err := os.Lstat(filepath.Join(s.dir, "state.pending")); !os.IsNotExist(err) {
		return State{}, false, errors.New("state_update_unknown")
	}
	file, err := os.OpenFile(filepath.Join(s.dir, "state.json"), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if os.IsNotExist(err) {
		if !s.fresh {
			return State{}, false, errors.New("state_missing")
		}
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, errors.New("state_unreadable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return State{}, false, errors.New("state_not_private")
	}
	var state State
	if err := localipc.Decode(file, &state); err != nil {
		return State{}, false, errors.New("state_corrupt")
	}
	if state.Version != 1 || !validInputState(state.InputState) || (state.InputState == InputSecurityStop && !state.SecurityStopped) || state.Execution == "" || state.Epoch == 0 || len(state.Records) > MaxRecords ||
		(state.Fence != "open" && state.Fence != "held" && state.Fence != "unknown") {
		return State{}, false, errors.New("state_corrupt")
	}
	seen := make(map[string]bool)
	var maxSequence uint64
	for _, record := range state.Records {
		if !validNotice(record.Notice) || seen[record.Notice.ID] || record.Sequence != maxSequence+1 ||
			(record.State != "queued" && record.State != "unknown" && record.State != "applied") || record.Digest != digestNotice(record.Notice, state.Selection) {
			return State{}, false, errors.New("state_corrupt")
		}
		seen[record.Notice.ID] = true
		maxSequence = record.Sequence
	}
	if state.Sequence != maxSequence || !validProtocolState(state) {
		return State{}, false, errors.New("state_corrupt")
	}
	return state, true, nil
}

func (s *store) write(state State) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil || len(data) > localipc.MaxFrame {
		return errors.New("state_capacity_exceeded")
	}
	// A crash before commit leaves this marker. Do not interpret a complete
	// but uncommitted snapshot as permission to resume queued effects.
	pendingPath := filepath.Join(s.dir, "state.pending")
	pending, err := os.OpenFile(pendingPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return errors.New("state_write_failed")
	}
	_, writeErr := pending.Write([]byte("update-in-progress\n"))
	syncErr := pending.Sync()
	closeErr := pending.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil || s.syncDirectory() != nil {
		return errors.New("state_sync_unknown")
	}
	file, err := os.CreateTemp(s.dir, ".state-")
	if err != nil {
		return errors.New("state_write_failed")
	}
	defer os.Remove(file.Name())
	n, err := file.Write(append(data, '\n'))
	if err == nil && n != len(data)+1 {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr = file.Close()
	if err != nil || closeErr != nil {
		return errors.New("state_write_failed")
	}
	if err := os.Rename(file.Name(), filepath.Join(s.dir, "state.json")); err != nil {
		return errors.New("state_write_failed")
	}
	if err := s.syncDirectory(); err != nil {
		return err
	}
	if err := os.Remove(pendingPath); err != nil {
		return errors.New("state_sync_unknown")
	}
	return s.syncDirectory()
}

func (s *store) syncDirectory() error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return errors.New("state_sync_unknown")
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errors.New("state_sync_unknown")
	}
	return nil
}

func (s *store) close() error { return s.lock.Close() }
