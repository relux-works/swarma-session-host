package sessionhost

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"sync"
	"syscall"

	schema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
)

var selectedOperationID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var errSelectedJournal = errors.New("operation_journal_unavailable")

type selectedOperationEntry struct {
	Cancelled bool            `json:"cancelled,omitempty"`
	Digest    string          `json:"digest"`
	Kind      string          `json:"kind"`
	Submitted string          `json:"submitted_at"`
	Result    json.RawMessage `json:"result,omitempty"`
	Refusal   json.RawMessage `json:"refusal,omitempty"`
}

// Entries and tombstones are retained indefinitely (at least seven days).
// Capacity exhaustion refuses new operations instead of evicting obligations.
// A pending marker or failed sync is unknown, never permission to re-execute.
type selectedOperationJournal struct {
	mu            sync.Mutex
	store         *store
	root          *os.Root
	directory     *os.File
	index         *os.File
	indexIdentity os.FileInfo
	known         map[string]bool
	identity      os.FileInfo
	poisoned      bool
}

func openSelectedOperationJournal(directory string) (*selectedOperationJournal, error) {
	s, err := openStore(directory)
	if err != nil {
		return nil, errSelectedJournal
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		s.close()
		return nil, errSelectedJournal
	}
	f, err := root.OpenFile(".", os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		root.Close()
		s.close()
		return nil, errSelectedJournal
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		root.Close()
		s.close()
		return nil, errSelectedJournal
	}
	_, indexErr := root.Lstat("operations.index")
	if os.IsNotExist(indexErr) && !s.fresh {
		f.Close()
		root.Close()
		s.close()
		return nil, errSelectedJournal
	}
	index, err := root.OpenFile("operations.index", os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if os.IsExist(err) {
		index, err = root.OpenFile("operations.index", os.O_RDWR|os.O_APPEND|syscall.O_NOFOLLOW, 0)
	}
	if err != nil {
		f.Close()
		root.Close()
		s.close()
		return nil, errSelectedJournal
	}
	indexInfo, err := index.Stat()
	if err != nil || !indexInfo.Mode().IsRegular() || indexInfo.Mode().Perm() != 0600 {
		index.Close()
		f.Close()
		root.Close()
		s.close()
		return nil, errSelectedJournal
	}
	data, err := io.ReadAll(io.LimitReader(index, 1024*37+1))
	known := make(map[string]bool)
	if err == nil && len(data) <= 1024*37 && (len(data) == 0 || data[len(data)-1] == '\n') {
		for _, line := range bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'}) {
			if len(data) == 0 {
				break
			}
			id := string(line)
			if !selectedOperationID.MatchString(id) || known[id] {
				err = errSelectedJournal
				break
			}
			known[id] = true
		}
	} else {
		err = errSelectedJournal
	}
	if err == nil {
		err = index.Sync()
	}
	if err == nil {
		err = f.Sync()
	}
	if err != nil {
		index.Close()
		f.Close()
		root.Close()
		s.close()
		return nil, errSelectedJournal
	}
	return &selectedOperationJournal{store: s, root: root, directory: f, identity: info, index: index, indexIdentity: indexInfo, known: known}, nil
}

func (j *selectedOperationJournal) current() bool {
	if j.poisoned || j.root == nil {
		return false
	}
	held, e1 := j.directory.Stat()
	named, e2 := os.Lstat(j.store.dir)
	if e1 != nil || e2 != nil || !named.IsDir() || named.Mode().Perm() != 0700 || !os.SameFile(j.identity, held) || !os.SameFile(held, named) {
		j.poisoned = true
		return false
	}
	index, e3 := j.index.Stat()
	namedIndex, e4 := j.root.Lstat("operations.index")
	if e3 != nil || e4 != nil || !namedIndex.Mode().IsRegular() || namedIndex.Mode().Perm() != 0600 || index.Size() != int64(len(j.known)*37) || !os.SameFile(j.indexIdentity, index) || !os.SameFile(index, namedIndex) {
		j.poisoned = true
		return false
	}
	return true
}

func (j *selectedOperationJournal) read(id string) (selectedOperationEntry, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.readLocked(id)
}
func (j *selectedOperationJournal) readLocked(id string) (selectedOperationEntry, bool, error) {
	var entry selectedOperationEntry
	if !selectedOperationID.MatchString(id) || !j.current() {
		return entry, false, errSelectedJournal
	}
	_, e := j.root.Lstat(id + ".pending")
	if !os.IsNotExist(e) {
		return entry, false, errSelectedJournal
	}
	// Root confines resolution; Lstat rejects even an in-root symlink before
	// OpenFile, since Root may resolve links before the native O_NOFOLLOW open.
	info, err := j.root.Lstat(id + ".json")
	if os.IsNotExist(err) {
		if j.known[id] {
			return entry, false, errSelectedJournal
		}
		return entry, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || !j.known[id] {
		return entry, false, errSelectedJournal
	}
	f, err := j.root.OpenFile(id+".json", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return entry, false, errSelectedJournal
	}
	defer f.Close()
	held, err := f.Stat()
	if err != nil || !held.Mode().IsRegular() || held.Mode().Perm() != 0600 || !os.SameFile(info, held) {
		return entry, false, errSelectedJournal
	}
	data, err := io.ReadAll(io.LimitReader(f, schema.Limit+1025))
	if err != nil || len(data) > schema.Limit+1024 || json.Unmarshal(data, &entry) != nil || (!entry.Cancelled && (!notificationSHA256(entry.Digest) || (entry.Kind != "launch" && entry.Kind != "resume"))) || (len(entry.Result) != 0 && !validSelectedResult(entry.Result)) {
		return selectedOperationEntry{}, false, errSelectedJournal
	}
	if (len(entry.Refusal) != 0 && schema.Validate("refusal.schema.json", entry.Refusal) != nil) || (len(entry.Result) != 0 && len(entry.Refusal) != 0) {
		return selectedOperationEntry{}, false, errSelectedJournal
	}
	return entry, true, nil
}

func (j *selectedOperationJournal) write(id string, entry selectedOperationEntry) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !selectedOperationID.MatchString(id) || !j.current() {
		return errSelectedJournal
	}
	data, err := json.Marshal(entry)
	if err != nil || len(data) > schema.Limit+1024 {
		return errSelectedJournal
	}
	_, exists, err := j.readLocked(id)
	if err != nil {
		return err
	}
	if !exists {
		if len(j.known) >= 1024 {
			return errSelectedJournal
		}
		// Retain an append-only obligation before the first operation snapshot.
		// Losing one snapshot can therefore never make its ID new after restart.
		if _, err = j.index.Write([]byte(id + "\n")); err == nil {
			err = j.index.Sync()
		}
		if err != nil {
			j.poisoned = true
			return errSelectedJournal
		}
		j.known[id] = true
	}
	marker := id + ".pending"
	f, err := j.root.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return errSelectedJournal
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = j.directory.Sync()
	}
	if err == nil {
		err = j.root.Rename(marker, id+".json")
	}
	if err == nil {
		err = j.directory.Sync()
	}
	if err != nil {
		j.poisoned = true
		return errSelectedJournal
	}
	return nil
}

func (j *selectedOperationJournal) close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.root == nil {
		return nil
	}
	err := j.root.Close()
	err = errors.Join(err, j.directory.Close(), j.index.Close())
	j.root = nil
	return errors.Join(err, j.store.close())
}
