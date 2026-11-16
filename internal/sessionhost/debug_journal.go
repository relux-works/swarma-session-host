package sessionhost

import (
	"bytes"
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
	// torn marks a reopened journal whose evidence could not be fully framed
	// or cross-referenced. It holds new effects until retirement; appends for
	// stop, revocation and controls stay possible so cleanup remains reachable.
	torn bool
	// orphans are effect frames found at reopen without a receipt; the host
	// cross-references them with its snapshot and retains unknown effects.
	orphans []debugJournalEntry
	// sync and closeFile are the storage primitives used on every durability
	// boundary. Fixtures inject failures here; production uses the defaults.
	sync      func(*os.File) error
	closeFile func(*os.File) error
}

func (j *debugJournal) fsync(f *os.File) error {
	if j.sync != nil {
		return j.sync(f)
	}
	return f.Sync()
}
func (j *debugJournal) release(f *os.File) error {
	if j.closeFile != nil {
		return j.closeFile(f)
	}
	return f.Close()
}

// Attribution members are omitted only in receipt frames, which reference the
// attributed accepted frame (actor plus input ID) by its journal sequence in
// Ref together with the input ID and digest, and must fit the reserve.
type debugJournalEntry struct {
	Contract       string     `json:"contract"`
	Sequence       uint64     `json:"sequence"`
	At             time.Time  `json:"at"`
	Elapsed        int64      `json:"elapsed_ns"`
	Actor          string     `json:"actor,omitempty"`
	Role           string     `json:"role,omitempty"`
	Kind           string     `json:"kind"`
	ID             string     `json:"input_id"`
	Execution      string     `json:"execution,omitempty"`
	Epoch          uint64     `json:"epoch,omitempty"`
	GrantRevision  uint64     `json:"grant_revision,omitempty"`
	WriterRevision uint64     `json:"writer_revision,omitempty"`
	Observation    InputState `json:"observation,omitempty"`
	Digest         string     `json:"digest"`
	Body           []byte     `json:"body,omitempty"`
	Stage          string     `json:"stage"`
	Ref            uint64     `json:"ref,omitempty"`
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
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Uid != uint32(os.Getuid()) {
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
	syncErr := parent.Sync()
	closeErr := parent.Close()
	if syncErr != nil || closeErr != nil {
		return fail()
	}
	j := &debugJournal{file: file, dir: dir, used: info.Size(), started: time.Now()}
	// Retained frames belong to this execution's earlier incarnation or are
	// foreign to a fresh one; both are reconstructed conservatively, so the
	// replacement flag changes nothing here and is kept for the caller's record.
	if info.Size() != 0 || replacement {
		if err := j.reconstruct(); err != nil {
			return fail()
		}
	}
	return j, nil
}

// reconstruct replays the retained frames into counters without replaying any
// effect. Every complete frame must be this contract's JSON with increasing
// sequence; an unframed tail is terminated with one newline so later frames
// stay parseable, and the journal is marked torn. Bodies are not retained.
func (j *debugJournal) reconstruct() error {
	data, err := io.ReadAll(io.LimitReader(io.NewSectionReader(j.file, 0, j.used), debugJournalCap+1))
	if err != nil || int64(len(data)) != j.used {
		return debugStorageError()
	}
	complete := data
	if len(data) != 0 && data[len(data)-1] != '\n' {
		j.torn = true
		if cut := bytes.LastIndexByte(data, '\n'); cut >= 0 {
			complete = data[:cut+1]
		} else {
			complete = nil
		}
		if j.used+1 > debugJournalCap {
			return debugStorageError()
		}
		n, writeErr := j.file.Write([]byte{'\n'})
		if writeErr == nil && n != 1 {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = j.fsync(j.file)
		}
		if writeErr != nil {
			return debugStorageError()
		}
		j.used++
	}
	// Effect frames are tracked by their own sequence, which is the durable
	// composite-key reference a receipt names in Ref; an ID alone would merge
	// two actors' inputs that share a client ID.
	pending := map[uint64]debugJournalEntry{}
	order := []uint64{}
	var entries, receipts int64
	for _, line := range bytes.Split(complete, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var e debugJournalEntry
		if json.Unmarshal(line, &e) != nil || e.Contract != debugContract || e.Sequence <= j.sequence {
			j.torn = true
			continue
		}
		j.sequence = e.Sequence
		e.Body = nil
		switch e.Kind {
		case "receipt":
			receipts++
			if _, known := pending[e.Ref]; !known {
				j.torn = true
			}
			delete(pending, e.Ref)
		case "follow_up", "answer_interaction":
			entries++
			order = append(order, e.Sequence)
			pending[e.Sequence] = e
		case "stop":
			entries++
			j.stops++
		case "revoke":
			entries++
			j.revokes++
		case "terminal_observed":
			entries++
			j.terminals++
		default:
			entries++
		}
	}
	if receipts > entries {
		j.torn = true
		receipts = entries
	}
	j.reserved = (entries - receipts) * debugReceiptReserve
	for _, sequence := range order {
		if e, ok := pending[sequence]; ok {
			j.orphans = append(j.orphans, e)
		}
	}
	return nil
}
func (j *debugJournal) encode(e *debugJournalEntry, now time.Time) ([]byte, error) {
	e.Contract = debugContract
	e.Sequence = j.sequence + 1
	e.At = now.UTC()
	e.Elapsed = now.Sub(j.started).Nanoseconds()
	if e.Elapsed < 0 {
		e.Elapsed = 0
	}
	data, err := json.Marshal(e)
	if err != nil {
		return nil, debugStorageError()
	}
	return append(data, '\n'), nil
}
func (j *debugJournal) write(data []byte) error {
	n, err := j.file.Write(data)
	j.used += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = j.fsync(j.file)
	}
	if err != nil {
		j.failed = true
		return debugStorageError()
	}
	return nil
}
func (j *debugJournal) append(e debugJournalEntry, control bool, now time.Time) error {
	if j == nil || j.file == nil || j.failed {
		return debugStorageError()
	}
	e.Stage = "accepted"
	data, err := j.encode(&e, now)
	if err != nil {
		return err
	}
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
	if err := j.write(data); err != nil {
		return err
	}
	j.reserved += reserve
	j.sequence++
	if counter != nil {
		*counter++
	}
	return nil
}

// receipt consumes the reservation made when its input was accepted. It is
// never refused for ordinary capacity: the space was reserved before the
// effect, and a receipt larger than the reserve still fits the total cap.
func (j *debugJournal) receipt(e debugJournalEntry, now time.Time) error {
	if j == nil || j.file == nil || j.failed {
		return debugStorageError()
	}
	e.Kind = "receipt"
	data, err := j.encode(&e, now)
	if err != nil {
		return err
	}
	reserve := min(int64(debugReceiptReserve), j.reserved)
	if j.used+j.reserved-reserve+int64(len(data)) > debugJournalCap {
		return errors.New("input_capacity_exceeded")
	}
	if err := j.write(data); err != nil {
		return err
	}
	j.reserved -= reserve
	j.sequence++
	return nil
}

// close releases the journal handle once and reports a failed close as the
// storage class. The handle is dropped either way; a later purge retry still
// unlinks the artifact, so the failure is surfaced, not silently absorbed.
func (j *debugJournal) close() error {
	if j == nil || j.file == nil {
		return nil
	}
	file := j.file
	j.file = nil
	if err := j.release(file); err != nil {
		j.failed = true
		return debugStorageError()
	}
	return nil
}

// purge is the logical unlink of the private artifacts plus directory and
// parent sync. Every close and sync result on the path is checked; any failure
// leaves retirement incomplete for retry. It refuses to traverse or delete
// unrecognized artifacts and is idempotent for a partially completed earlier
// attempt. Unlink is logical purge, not a claim of physical erasure.
func (j *debugJournal) purge() error {
	if err := j.close(); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(j.dir, "input.journal")); err != nil && !os.IsNotExist(err) {
		return debugStorageError()
	}
	entries, err := os.ReadDir(j.dir)
	if err != nil && !os.IsNotExist(err) {
		return debugStorageError()
	}
	if len(entries) != 0 {
		return debugStorageError()
	}
	if err == nil {
		dir, err := os.Open(j.dir)
		if err != nil {
			return debugStorageError()
		}
		syncErr := j.fsync(dir)
		closeErr := j.release(dir)
		if syncErr != nil || closeErr != nil {
			return debugStorageError()
		}
		if err = os.Remove(j.dir); err != nil {
			return debugStorageError()
		}
	}
	parent, err := os.Open(filepath.Dir(j.dir))
	if err != nil {
		return debugStorageError()
	}
	syncErr := j.fsync(parent)
	closeErr := j.release(parent)
	if syncErr != nil || closeErr != nil {
		return debugStorageError()
	}
	if _, err := os.Lstat(j.dir); !os.IsNotExist(err) {
		return debugStorageError()
	}
	j.used, j.reserved = 0, 0
	return nil
}
