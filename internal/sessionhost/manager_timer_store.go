package sessionhost

import (
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
)

const managerTimerJournalLimit = 256 * 1024

const (
	managerTimerStateInvalid  managerTimerCode = "timer_state_invalid"
	managerTimerIOUnavailable managerTimerCode = "timer_io_unavailable"
	managerTimerLocked        managerTimerCode = "timer_locked"
)

type managerTimerFileIdentity struct{ device, inode uint64 }

// An existing ordinary execution's admitted witness is required on reopen.
// A caller inventing this value is not an authenticated execution owner.
type managerTimerStoreWitness struct {
	directory                       string
	executionID, ledgerID           string
	directoryIdentity, lockIdentity managerTimerFileIdentity
}

type managerTimerHeldDirectory struct {
	root     *os.Root
	parent   *os.Root
	name     string
	identity managerTimerFileIdentity
}

type managerTimerStore struct {
	mu                                                     sync.Mutex
	directories                                            []managerTimerHeldDirectory
	root                                                   *os.Root
	lock                                                   *os.File
	witness                                                managerTimerStoreWitness
	revisionHighWater, opportunityHighWater, phaseRevision int64
	failed, closed                                         bool
	// Test-only injected failures supplement real file/directory Sync. Each
	// returned error permanently fences this store instance before more effects.
	durability func(string) error
}

func managerTimerIdentity(info os.FileInfo) (managerTimerFileIdentity, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return managerTimerFileIdentity{}, false
	}
	return managerTimerFileIdentity{uint64(st.Dev), uint64(st.Ino)}, true
}

func managerTimerSafeDirectory(info os.FileInfo, private bool) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	if private {
		return st.Uid == uint32(os.Getuid()) && info.Mode().Perm() == 0700
	}
	return (st.Uid == 0 || st.Uid == uint32(os.Getuid())) && info.Mode().Perm()&0022 == 0
}

func managerTimerSafeFile(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && st.Uid == uint32(os.Getuid()) && st.Nlink == 1 && info.Mode().Perm() == 0600
}

// Walk one component at a time from a held root, rejecting symlinks and unsafe
// ownership/modes. Every subsequent operation uses the held directory, and
// named ancestry is rechecked against its original inode before a mutation.
func managerTimerOpenDirectories(directory string) ([]managerTimerHeldDirectory, error) {
	if managerTimerField("PATH", directory) != nil || directory == "/" {
		return nil, managerTimerStateInvalid
	}
	root, err := os.OpenRoot("/")
	if err != nil {
		return nil, managerTimerIOUnavailable
	}
	directories := []managerTimerHeldDirectory{{root: root}}
	fail := func() ([]managerTimerHeldDirectory, error) {
		for i := len(directories) - 1; i >= 0; i-- {
			directories[i].root.Close()
		}
		return nil, managerTimerStateInvalid
	}
	parts := strings.Split(strings.TrimPrefix(directory, "/"), "/")
	for i, name := range parts {
		info, err := root.Lstat(name)
		if err != nil || !managerTimerSafeDirectory(info, i == len(parts)-1) {
			return fail()
		}
		identity, ok := managerTimerIdentity(info)
		if !ok {
			return fail()
		}
		next, err := root.OpenRoot(name)
		if err != nil {
			return fail()
		}
		f, err := next.Open(".")
		if err != nil {
			next.Close()
			return fail()
		}
		held, err := f.Stat()
		f.Close()
		actual, ok := managerTimerFileIdentity{}, false
		if err == nil {
			actual, ok = managerTimerIdentity(held)
		}
		if !ok || actual != identity {
			next.Close()
			return fail()
		}
		directories = append(directories, managerTimerHeldDirectory{root: next, parent: root, name: name, identity: identity})
		root = next
	}
	return directories, nil
}

func newManagerTimerStore(directory string) (*managerTimerStore, error) {
	dirs, err := managerTimerOpenDirectories(directory)
	if err != nil {
		return nil, err
	}
	return &managerTimerStore{directories: dirs, root: dirs[len(dirs)-1].root}, nil
}

// Initialization is explicit and leaves any uncertain footprint for owner
// reconciliation. There is no create-if-missing recovery or automatic cleanup.
func initializeManagerTimerStore(directory string, ledger map[string]any, recordWitness func(managerTimerStoreWitness) error, durability func(string) error) (*managerTimerStore, error) {
	if recordWitness == nil || validateManagerTimerRecord("Ledger", ledger) != nil || ledger["registration"] != nil || timerNumber(ledger, "revision_high_water") != 0 || timerNumber(ledger, "opportunity_high_water") != 0 || timerNumber(ledger, "phase_revision") != 0 || len(ledger["terminal"].([]any)) != 0 {
		return nil, managerTimerStateInvalid
	}
	s, err := newManagerTimerStore(directory)
	if err != nil {
		return nil, err
	}
	s.durability = durability
	fail := func(err error) (*managerTimerStore, error) { s.Close(); return nil, err }
	for _, name := range []string{"timer.lock", "ledger.json", "ledger.pending"} {
		if _, err := s.root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			return fail(managerTimerStateInvalid)
		}
	}
	s.lock, err = s.root.OpenFile("timer.lock", os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return fail(managerTimerStateInvalid)
	}
	info, err := s.lock.Stat()
	if err != nil || !managerTimerSafeFile(info) {
		return fail(managerTimerStateInvalid)
	}
	lockID, ok := managerTimerIdentity(info)
	if !ok {
		return fail(managerTimerStateInvalid)
	}
	if err = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(managerTimerLocked)
	}
	s.witness = managerTimerStoreWitness{directory: directory, executionID: timerString(ledger, "execution_id"), ledgerID: timerString(ledger, "ledger_id"), directoryIdentity: s.directories[len(s.directories)-1].identity, lockIdentity: lockID}
	data, err := canonicalManagerTimerValue(ledger)
	if err != nil {
		return fail(managerTimerStateInvalid)
	}
	data = append(data, '\n')
	f, err := s.root.OpenFile("ledger.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return fail(managerTimerStateInvalid)
	}
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = s.syncFile(f, "initialize_file")
	}
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		return fail(managerTimerIOUnavailable)
	}
	if err = s.syncFile(s.lock, "initialize_lock"); err != nil {
		return fail(managerTimerIOUnavailable)
	}
	if err = s.syncDirectory("initialize_directory"); err != nil {
		return fail(managerTimerIOUnavailable)
	}
	if err = recordWitness(s.witness); err != nil {
		return fail(managerTimerIOUnavailable)
	}
	if err = s.checkHeld(); err != nil {
		return fail(err)
	}
	return s, nil
}

func openManagerTimerStore(witness managerTimerStoreWitness, durability func(string) error) (*managerTimerStore, map[string]any, error) {
	if managerTimerField("ID", witness.executionID) != nil || managerTimerField("UUID", witness.ledgerID) != nil {
		return nil, nil, managerTimerStateInvalid
	}
	s, err := newManagerTimerStore(witness.directory)
	if err != nil {
		return nil, nil, err
	}
	s.witness = witness
	s.durability = durability
	fail := func(err error) (*managerTimerStore, map[string]any, error) { s.Close(); return nil, nil, err }
	if s.directories[len(s.directories)-1].identity != witness.directoryIdentity {
		return fail(managerTimerStateInvalid)
	}
	s.lock, err = s.root.OpenFile("timer.lock", os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fail(managerTimerStateInvalid)
	}
	if err = s.checkHeld(); err != nil {
		return fail(err)
	}
	if err = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return fail(managerTimerLocked)
		}
		return fail(managerTimerIOUnavailable)
	}
	ledger, err := s.loadLocked()
	if err != nil {
		return fail(err)
	}
	s.revisionHighWater = timerNumber(ledger, "revision_high_water")
	s.opportunityHighWater = timerNumber(ledger, "opportunity_high_water")
	s.phaseRevision = timerNumber(ledger, "phase_revision")
	return s, ledger, nil
}

func (s *managerTimerStore) checkHeld() error {
	if s.closed || s.failed || s.lock == nil {
		return managerTimerStateInvalid
	}
	for i := 1; i < len(s.directories); i++ {
		d := s.directories[i]
		info, err := d.parent.Lstat(d.name)
		if err != nil || !managerTimerSafeDirectory(info, i == len(s.directories)-1) {
			return managerTimerStateInvalid
		}
		id, ok := managerTimerIdentity(info)
		if !ok || id != d.identity {
			return managerTimerStateInvalid
		}
	}
	held, err := s.lock.Stat()
	if err != nil || !managerTimerSafeFile(held) {
		return managerTimerStateInvalid
	}
	info, err := s.root.Lstat("timer.lock")
	if err != nil || !managerTimerSafeFile(info) {
		return managerTimerStateInvalid
	}
	a, ok := managerTimerIdentity(held)
	b, other := managerTimerIdentity(info)
	if !ok || !other || a != b || a != s.witness.lockIdentity {
		return managerTimerStateInvalid
	}
	return nil
}

func (s *managerTimerStore) loadLocked() (map[string]any, error) {
	if err := s.checkHeld(); err != nil {
		return nil, err
	}
	if _, err := s.root.Lstat("ledger.pending"); !errors.Is(err, os.ErrNotExist) {
		return nil, managerTimerStateInvalid
	}
	f, err := s.root.OpenFile("ledger.json", os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, managerTimerStateInvalid
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !managerTimerSafeFile(info) {
		return nil, managerTimerStateInvalid
	}
	data, err := io.ReadAll(io.LimitReader(f, managerTimerJournalLimit+1))
	if err != nil {
		return nil, managerTimerIOUnavailable
	}
	ledger, err := decodeManagerTimerObjectLimit(data, managerTimerJournalLimit)
	if err != nil || validateManagerTimerRecord("Ledger", ledger) != nil || ledger["execution_id"] != s.witness.executionID || ledger["ledger_id"] != s.witness.ledgerID {
		return nil, managerTimerStateInvalid
	}
	return ledger, nil
}

func (s *managerTimerStore) save(ledger map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkHeld(); err != nil {
		s.failed = true
		return err
	}
	current, err := s.loadLocked()
	if err != nil {
		s.failed = true
		return err
	}
	if timerNumber(current, "revision_high_water") != s.revisionHighWater || timerNumber(current, "opportunity_high_water") != s.opportunityHighWater || timerNumber(current, "phase_revision") != s.phaseRevision {
		return managerTimerStateInvalid
	}
	if validateManagerTimerRecord("Ledger", ledger) != nil || ledger["execution_id"] != s.witness.executionID || ledger["ledger_id"] != s.witness.ledgerID || timerNumber(ledger, "revision_high_water") < s.revisionHighWater || timerNumber(ledger, "opportunity_high_water") < s.opportunityHighWater || timerNumber(ledger, "phase_revision") < s.phaseRevision {
		return managerTimerStateInvalid
	}
	data, err := canonicalManagerTimerValue(ledger)
	if err != nil || len(data)+1 > managerTimerJournalLimit {
		return managerTimerStateInvalid
	}
	data = append(data, '\n')
	f, err := s.root.OpenFile("ledger.pending", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		s.failed = true
		return managerTimerIOUnavailable
	}
	_, err = f.Write(data)
	if err == nil {
		err = s.syncFile(f, "replace_file")
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = s.root.Rename("ledger.pending", "ledger.json")
	}
	if err == nil {
		err = s.syncDirectory("replace_directory")
	}
	if err == nil {
		err = s.checkHeld()
	}
	if err != nil {
		s.failed = true
		return managerTimerIOUnavailable
	}
	s.revisionHighWater = timerNumber(ledger, "revision_high_water")
	s.opportunityHighWater = timerNumber(ledger, "opportunity_high_water")
	s.phaseRevision = timerNumber(ledger, "phase_revision")
	return nil
}

func (s *managerTimerStore) syncFile(file *os.File, stage string) error {
	if s.durability != nil {
		if err := s.durability(stage); err != nil {
			return err
		}
	}
	return file.Sync()
}

func (s *managerTimerStore) syncDirectory(stage string) error {
	f, err := s.root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	return s.syncFile(f, stage)
}

func (s *managerTimerStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var result error
	if s.lock != nil {
		result = s.lock.Close()
	}
	for i := len(s.directories) - 1; i >= 0; i-- {
		if err := s.directories[i].root.Close(); result == nil {
			result = err
		}
	}
	return result
}
