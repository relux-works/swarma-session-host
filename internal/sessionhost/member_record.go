package sessionhost

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"

	managerschema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
)

var errMemberRecordUnavailable = errors.New("member_record_unavailable")
var memberRecordID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)
var memberAgent = regexp.MustCompile(`^[a-z0-9][a-z0-9._=-]{0,63}$`)
var memberConnection = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

const maxMemberRecords = 1024

// These private values are parsed observations, never authority DTOs. In
// particular they do not implement notificationManagerCustody. Full operation,
// resolved profile/workflow and Source admission remain separate conjuncts.
type memberNotificationInput struct {
	Member struct {
		MXID              string `json:"mxid"`
		Agent             string `json:"agent_selector"`
		Connection        string `json:"connection"`
		ConfigurationRoot string `json:"configuration_root"`
	} `json:"member"`
	AdmissionReference string `json:"admission_reference"`
	CallTimeoutMS      int    `json:"call_timeout_ms"`
}

type memberRecord struct {
	ID           string                   `json:"id"`
	Runtime      string                   `json:"runtime"`
	Profile      string                   `json:"profile"`
	ProfileLock  string                   `json:"profile_lock_sha256"`
	Clone        string                   `json:"clone"`
	Doorbell     json.RawMessage          `json:"doorbell"`
	Identity     string                   `json:"identity"`
	Notification *memberNotificationInput `json:"notification"`
}

// memberRecordSource is created only by the actual continuing owner, from its
// composition-selected directory. No request, environment or Source root is
// consulted here. The retained root restricts every later lookup to this source.
// This is an optimistic current-record observation, not a writer exclusion lock.
type memberRecordSource struct {
	owner    *continuingHostOwner
	path     string
	root     *os.Root
	identity os.FileInfo
	mu       sync.Mutex
	closed   bool
	done     chan struct{}
	records  map[*retainedMemberRecord]struct{}
}

type retainedMemberRecord struct {
	source    *memberRecordSource
	file      *os.File
	identity  os.FileInfo
	revision  [32]byte
	record    memberRecord
	workspace *os.File
	lost      bool // Terminal once an actual source/revision loss has been observed.
}

func recordInfoSafe(info os.FileInfo, directory bool) bool {
	if info == nil || info.Mode().Perm()&0022 != 0 || info.IsDir() != directory || (!directory && !info.Mode().IsRegular()) {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid()) && (directory || stat.Nlink == 1)
}

func canonicalRecordDirectory(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	actual, err := filepath.EvalSymlinks(path)
	return err == nil && actual == path
}

func (o *continuingHostOwner) openMemberRecordSource(directory string) (*memberRecordSource, error) {
	if o == nil || o.checkCurrentHost(o.host) != nil || !canonicalRecordDirectory(directory) {
		return nil, errMemberRecordUnavailable
	}
	before, err := os.Lstat(directory)
	if err != nil || !recordInfoSafe(before, true) {
		return nil, errMemberRecordUnavailable
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, errMemberRecordUnavailable
	}
	s := &memberRecordSource{owner: o, path: directory, root: root, identity: before, done: make(chan struct{}), records: make(map[*retainedMemberRecord]struct{})}
	if !s.current() {
		_ = root.Close()
		return nil, errMemberRecordUnavailable
	}
	go func() {
		select {
		case <-o.ctx.Done():
			_ = s.close()
		case <-s.done:
		}
	}()
	return s, nil
}

// Caller holds s.mu after construction. Check both the retained directory and
// its original canonical pathname: an identical replacement is still foreign.
func (s *memberRecordSource) current() bool {
	if s == nil || s.closed || s.root == nil || s.owner == nil || s.owner.checkCurrentHost(s.owner.host) != nil || !canonicalRecordDirectory(s.path) {
		return false
	}
	named, err := os.Lstat(s.path)
	if err != nil || !recordInfoSafe(named, true) || !os.SameFile(s.identity, named) {
		return false
	}
	f, err := s.root.Open(".")
	if err != nil {
		return false
	}
	defer f.Close()
	held, err := f.Stat()
	return err == nil && recordInfoSafe(held, true) && os.SameFile(named, held)
}

func (s *memberRecordSource) close() error {
	if s == nil || s.root == nil || s.done == nil {
		return errMemberRecordUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for record := range s.records {
		record.lost = true
		if record.file != nil {
			_ = record.file.Close()
			record.file = nil
		}
		if record.workspace != nil {
			_ = record.workspace.Close()
			record.workspace = nil
		}
		delete(s.records, record)
	}
	err := s.root.Close()
	close(s.done)
	return err
}

// The genuine published input schema validates the complete original document
// before projection. Its closed reader rejects duplicate keys and unknown sig.
func parseMemberRecord(data []byte) (memberRecord, error) {
	var r memberRecord
	if managerschema.Validate("werkstatt-record.schema.json", data) != nil {
		return r, errMemberRecordUnavailable
	}
	if _, err := managerschema.CanonicalJSON(data); err != nil {
		return r, errMemberRecordUnavailable
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil || object == nil {
		return r, errMemberRecordUnavailable
	}
	notification, present := object["notification"]
	if json.Unmarshal(data, &r) != nil {
		return memberRecord{}, errMemberRecordUnavailable
	}
	if !present {
		return r, nil
	} // Legitimate legacy records coexist in the source.
	var n memberNotificationInput
	decoder := json.NewDecoder(bytes.NewReader(notification))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&n) != nil || !validMemberInput(n) {
		return memberRecord{}, errMemberRecordUnavailable
	}
	// Required keys are exact, including nested case, null and integer spelling.
	var shape map[string]json.RawMessage
	if json.Unmarshal(notification, &shape) != nil || len(shape) != 3 || shape["member"] == nil || shape["admission_reference"] == nil || shape["call_timeout_ms"] == nil {
		return memberRecord{}, errMemberRecordUnavailable
	}
	var member map[string]json.RawMessage
	if json.Unmarshal(shape["member"], &member) != nil || len(member) != 4 {
		return memberRecord{}, errMemberRecordUnavailable
	}
	for _, key := range []string{"mxid", "agent_selector", "connection", "configuration_root"} {
		if member[key] == nil {
			return memberRecord{}, errMemberRecordUnavailable
		}
	}
	if n.Member.Agent != r.Identity || !notificationSHA256(r.ProfileLock) {
		return memberRecord{}, errMemberRecordUnavailable
	}
	r.Notification = &n
	return r, nil
}

func validMemberInput(n memberNotificationInput) bool {
	// Source's public API still owns its exact server-name predicate and physical
	// named selection. Here only the agreed expected-input subset is checked.
	prefix := "@" + n.Member.Agent + ":"
	return memberAgent.MatchString(n.Member.Agent) && memberConnection.MatchString(n.Member.Connection) &&
		notificationRef(n.Member.MXID, 255) && len(n.Member.MXID) > len(prefix) && n.Member.MXID[:len(prefix)] == prefix &&
		notificationRef(n.Member.ConfigurationRoot, 4096) && filepath.IsAbs(n.Member.ConfigurationRoot) && filepath.Clean(n.Member.ConfigurationRoot) == n.Member.ConfigurationRoot &&
		notificationRef(n.AdmissionReference, 512) && n.CallTimeoutMS >= 1 && n.CallTimeoutMS <= 60000
}

func readRecordFile(f *os.File) ([]byte, os.FileInfo, error) {
	before, err := f.Stat()
	if err != nil || !recordInfoSafe(before, false) || before.Size() > managerschema.Limit {
		return nil, nil, errMemberRecordUnavailable
	}
	data, err := io.ReadAll(io.NewSectionReader(f, 0, managerschema.Limit+1))
	after, statErr := f.Stat()
	if err != nil || statErr != nil || len(data) > managerschema.Limit || !recordInfoSafe(after, false) || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return nil, nil, errMemberRecordUnavailable
	}
	return data, after, nil
}

// resolve independently scans only the composition-selected source. A public
// reference selects; it cannot grant custody. Every eligible record is parsed,
// and zero/multiple matches or an indeterminate source fail closed. Caller holds
// s.mu; the returned file belongs to the caller even on a later failed recheck.
func (s *memberRecordSource) resolve(reference string) (*retainedMemberRecord, error) {
	if !notificationRef(reference, 512) || !s.current() {
		return nil, errMemberRecordUnavailable
	}
	directory, err := s.root.Open(".")
	if err != nil {
		return nil, errMemberRecordUnavailable
	}
	defer directory.Close()
	entries, err := directory.ReadDir(maxMemberRecords + 1)
	if err != nil && err != io.EOF || len(entries) > maxMemberRecords {
		return nil, errMemberRecordUnavailable
	}
	var selected *retainedMemberRecord
	fail := func() (*retainedMemberRecord, error) {
		if selected != nil {
			_ = selected.file.Close()
		}
		return nil, errMemberRecordUnavailable
	}
	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		id := name[:len(name)-5]
		if !memberRecordID.MatchString(id) {
			return fail()
		}
		// os.Root may resolve symlinks internally before the native open.
		// Independently witness the named leaf on both sides of that open.
		named, err := s.root.Lstat(name)
		if err != nil || !recordInfoSafe(named, false) {
			return fail()
		}
		file, err := s.root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return fail()
		}
		data, info, err := readRecordFile(file)
		after, namedErr := s.root.Lstat(name)
		if err != nil || namedErr != nil || !recordInfoSafe(after, false) || !os.SameFile(named, info) || !os.SameFile(info, after) {
			_ = file.Close()
			return fail()
		}
		record, err := parseMemberRecord(data)
		if err != nil || record.ID != id {
			_ = file.Close()
			return fail()
		}
		if record.Notification == nil || record.Notification.AdmissionReference != reference {
			_ = file.Close()
			continue
		}
		if selected != nil {
			_ = file.Close()
			return fail()
		}
		selected = &retainedMemberRecord{source: s, file: file, identity: info, revision: sha256.Sum256(data), record: record}
	}
	if selected == nil || !s.current() {
		return fail()
	}
	return selected, nil
}

func (s *memberRecordSource) retain(reference string) (*retainedMemberRecord, error) {
	if s == nil {
		return nil, errMemberRecordUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.resolve(reference)
	if err == nil {
		s.records[r] = struct{}{}
	}
	return r, err
}

// Exact physical owner and source pointer checks prevent matching decoded data
// or a separately opened source from substituting for this retained origin.
func (r *retainedMemberRecord) recheck(owner *continuingHostOwner, source *memberRecordSource) error {
	if r == nil || r.source == nil || owner != r.source.owner || source != r.source {
		return errMemberRecordUnavailable
	}
	s := r.source
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.lost || r.file == nil || r.record.Notification == nil {
		return errMemberRecordUnavailable
	}
	candidate, err := s.resolve(r.record.Notification.AdmissionReference)
	if err == nil {
		defer candidate.file.Close()
		data, info, readErr := readRecordFile(r.file)
		if readErr == nil && os.SameFile(r.identity, candidate.identity) && os.SameFile(r.identity, info) && r.revision == candidate.revision && r.revision == sha256.Sum256(data) && s.current() {
			return nil
		}
	}
	r.lost = true
	return errMemberRecordUnavailable
}

func (r *retainedMemberRecord) close() {
	if r == nil || r.source == nil {
		return
	}
	r.source.mu.Lock()
	defer r.source.mu.Unlock()
	r.lost = true
	delete(r.source.records, r)
	if r.workspace != nil {
		_ = r.workspace.Close()
		r.workspace = nil
	}
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}
}
