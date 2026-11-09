package sessionhost

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const debugJournalCap = 4_194_304
const debugControlReserve = 65_536
const debugReceiptReserve = 512

type debugJournal struct {
	file                      *os.File
	dir                       string
	used, reserved            int64
	sequence                  uint64
	started                   time.Time
	failed                    bool
	stops, revokes, terminals int
}
type debugJournalEntry struct {
	Contract       string     `json:"contract"`
	Sequence       uint64     `json:"sequence"`
	At             time.Time  `json:"at"`
	Elapsed        int64      `json:"elapsed_ns"`
	Actor          string     `json:"actor"`
	Role           string     `json:"role"`
	Kind           string     `json:"kind"`
	ID             string     `json:"input_id"`
	Execution      string     `json:"execution"`
	Epoch          uint64     `json:"epoch"`
	GrantRevision  uint64     `json:"grant_revision"`
	WriterRevision uint64     `json:"writer_revision"`
	Observation    InputState `json:"observation"`
	Digest         string     `json:"digest"`
	Body           []byte     `json:"body,omitempty"`
	Stage          string     `json:"stage"`
}

func debugStorageError() error { return errors.New("input_storage_unavailable") }
func openDebugJournal(root string, replacement bool) (*debugJournal, error) {
	dir := filepath.Join(root, "debug-attach")
	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return nil, debugStorageError()
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, debugStorageError()
	}
	file, err := os.OpenFile(filepath.Join(dir, "input.journal"), os.O_CREATE|os.O_APPEND|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, debugStorageError()
	}
	fail := func() (*debugJournal, error) { file.Close(); return nil, debugStorageError() }
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > debugJournalCap {
		return fail()
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Uid != uint32(os.Getuid()) || st.Nlink != 1 {
		return fail()
	}
	parent, err := os.Open(dir)
	if err != nil {
		return fail()
	}
	err = parent.Sync()
	parent.Close()
	if err != nil {
		return fail()
	}
	// Recovery deliberately does not reconstruct or replay private input. The
	// host starts held; hardening will reconcile the journal/outbox transaction.
	return &debugJournal{file: file, dir: dir, used: info.Size(), started: time.Now(), failed: replacement && info.Size() != 0}, nil
}
func (j *debugJournal) append(e debugJournalEntry, control bool, now time.Time) error {
	if j == nil || j.file == nil || j.failed {
		return debugStorageError()
	}
	e.Contract = debugContract
	e.Sequence = j.sequence + 1
	e.At = now.UTC()
	e.Elapsed = now.Sub(j.started).Nanoseconds()
	e.Stage = "accepted"
	if e.Elapsed < 0 {
		e.Elapsed = 0
	}
	data, err := json.Marshal(e)
	if err != nil {
		return debugStorageError()
	}
	data = append(data, '\n')
	// 65,536 protected bytes cover one stop (2,048), at most sixteen
	// grant revocations (1,536 each), and thirty-two terminal facts (1,216
	// each), including every future receipt reservation. Routine controls
	// cannot borrow these slots. Allocation refuses new grants before their
	// outstanding revoke obligation can exceed the remaining slots.
	limit := int64(debugJournalCap - debugControlReserve)
	reserve := int64(debugReceiptReserve)
	cost := int64(len(data)) + reserve
	var counter *int
	var slots, slotBytes int
	if control {
		switch e.Kind {
		case "stop":
			counter, slots, slotBytes = &j.stops, 1, 2048
		case "revoke":
			counter, slots, slotBytes = &j.revokes, 16, 1536
		case "terminal_observed":
			counter, slots, slotBytes = &j.terminals, MaxRecords, 1216
		}
	}
	if counter != nil {
		if *counter >= slots || cost > int64(slotBytes) {
			return errors.New("input_capacity_exceeded")
		}
		limit = debugJournalCap
	}
	if j.used+j.reserved+cost > limit {
		return errors.New("input_capacity_exceeded")
	}
	n, err := j.file.Write(data)
	j.used += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = j.file.Sync()
	}
	if err != nil {
		j.failed = true
		return debugStorageError()
	}
	j.reserved += reserve
	j.sequence++
	if counter != nil {
		*counter++
	}
	return nil
}
func (j *debugJournal) close() {
	if j != nil && j.file != nil {
		j.file.Close()
		j.file = nil
	}
}
func (j *debugJournal) purge() error {
	j.close()
	// Do not traverse or delete unrecognized artifacts. A later hardening
	// revision owns incomplete-transaction reconciliation and cleanup.
	if err := os.Remove(filepath.Join(j.dir, "input.journal")); err != nil && !os.IsNotExist(err) {
		return debugStorageError()
	}
	entries, err := os.ReadDir(j.dir)
	if err != nil || len(entries) != 0 {
		return debugStorageError()
	}
	f, err := os.Open(j.dir)
	if err != nil {
		return debugStorageError()
	}
	err = f.Sync()
	f.Close()
	if err != nil {
		return debugStorageError()
	}
	if err = os.Remove(j.dir); err != nil {
		return debugStorageError()
	}
	f, err = os.Open(filepath.Dir(j.dir))
	if err != nil {
		return debugStorageError()
	}
	defer f.Close()
	if f.Sync() != nil {
		return debugStorageError()
	}
	j.used, j.reserved = 0, 0
	return nil
}
