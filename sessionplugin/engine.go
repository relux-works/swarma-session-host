package sessionplugin

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Host owns provider execution. An error after a start may have escaped must be
// Uncertain: the reserved key then remains held rather than inviting a retry.
type Host interface {
	Launch(context.Context, Request) (HostSession, error)
	Inspect(context.Context, string) (HostSession, error)
}
type Draining struct {
	Reason      string `json:"reason"`
	Remediation string `json:"remediation"`
}

type HostSession struct {
	Draining       *Draining
	ID             string
	NativeID       *string
	State          string
	LastActivityAt *string
}
type Uncertain struct{}

func (Uncertain) Error() string { return "provider outcome uncertain" }

type Doorbell struct {
	State      string  `json:"state"`
	Uncertain  bool    `json:"uncertain"`
	Generation *string `json:"generation"`
	Reason     *string `json:"reason"`
}
type Record struct {
	Draining       *Draining         `json:"draining,omitempty"`
	Session        string            `json:"session"`
	OperationID    *string           `json:"operation_id"`
	Manager        string            `json:"manager"`
	Runtime        *string           `json:"runtime"`
	NativeID       *string           `json:"native_id"`
	Readable       bool              `json:"readable"`
	State          string            `json:"state"`
	Labels         map[string]string `json:"labels"`
	Generation     *int64            `json:"generation"`
	StartedAt      *string           `json:"started_at"`
	LastActivityAt *string           `json:"last_activity_at"`
	Doorbell       Doorbell          `json:"doorbell"`
}
type storedSession struct {
	Record   Record
	Target   string
	CWD      string
	Name     string
	Profile  Profile
	Binding  bool
	Detached bool
}
type operation struct {
	Digest      string
	Key         *string
	State       string
	Code        string
	Details     map[string]string
	Session     *storedSession
	Generation  int64
	StartIntent bool
	Resume      *storedSession
}
type page struct {
	Labels map[string]string
	Limit  int
	Rows   []Record
	Offset int
}
type ledger struct {
	Version    int
	Operations map[string]*operation
	Pages      map[string]page
}

// Engine's ledger is separate from provider lifecycle records. It serializes only
// reservations and atomic snapshots; provider starts run outside that lock.
// No requested environment, credentials, prompt or native plan is persisted.
type Engine struct {
	// Manager and Capabilities identify the embedding protocol consumer.
	// Empty values select the neutral host identity and supported operations.
	Manager      string
	Capabilities []string
	Dir          string
	Host         Host
	Version      string
	Now          func() time.Time
	// Checkpoint is constructor-only fault instrumentation. Normal daemon
	// construction leaves it nil; no request or environment field enables it.
	Checkpoint func(string) error
}

func (e Engine) manager() string {
	if e.Manager != "" {
		return e.Manager
	}
	return "swarma-session-host"
}
func (e Engine) capabilities() []string {
	if e.Capabilities != nil {
		return append([]string(nil), e.Capabilities...)
	}
	return []string{"launch-delegated/1", "admission/1", "status/1", "list/1", "labels/1", "resume/1", "attach/1", "stop/1"}
}

func (e Engine) checkpoint(phase string) error {
	if e.Checkpoint != nil {
		return e.Checkpoint(phase)
	}
	return nil
}

func (e Engine) now() time.Time {
	if e.Now != nil {
		return e.Now().UTC().Truncate(time.Second)
	}
	return time.Now().UTC().Truncate(time.Second)
}
func opaque() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func unavailable() Refusal { return Fail("session_host_unavailable") }
func indexError() Refusal {
	return Fail("session_host_unavailable", map[string]string{"reason": "index_unreadable"})
}

func (e Engine) transaction(fn func(*ledger) (bool, error)) error {
	return e.transactionContext(context.Background(), fn)
}
func (e Engine) transactionContext(ctx context.Context, fn func(*ledger) (bool, error)) error {
	if err := os.MkdirAll(e.Dir, 0700); err != nil {
		return unavailable()
	}
	fi, err := os.Lstat(e.Dir)
	if err != nil || !fi.IsDir() || fi.Mode().Perm() != 0700 {
		return unavailable()
	}
	// O_NOFOLLOW closes the final-component symlink race, including lock files.
	fd, err := unix.Open(filepath.Join(e.Dir, "lock"), unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return unavailable()
	}
	lock := os.NewFile(uintptr(fd), "lock")
	defer lock.Close()
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Uid != uint32(os.Getuid()) || st.Mode&0777 != 0600 || st.Mode&unix.S_IFMT != unix.S_IFREG {
		return unavailable()
	}
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if err != unix.EWOULDBLOCK && err != unix.EAGAIN && err != unix.EINTR {
			return unavailable()
		}
		select {
		case <-ctx.Done():
			return unavailable()
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer unix.Flock(fd, unix.LOCK_UN)
	state := ledger{Version: 1, Operations: map[string]*operation{}, Pages: map[string]page{}}
	path := filepath.Join(e.Dir, "ledger.json")
	data, err := readPrivate(path)
	if errors.Is(err, os.ErrNotExist) {
		if _, err := os.Lstat(filepath.Join(e.Dir, "initialized")); !errors.Is(err, os.ErrNotExist) {
			return indexError()
		}
		if err := os.WriteFile(filepath.Join(e.Dir, "initialized"), []byte("1\n"), 0600); err != nil {
			return unavailable()
		}
		if err := saveLedger(e.Dir, path, state); err != nil {
			return err
		}
	} else if err != nil {
		return indexError()
	} else {
		if decodeLedger(data, &state) != nil {
			return indexError()
		}
		if !validLedger(state) {
			return indexError()
		}
	}
	changed, err := fn(&state)
	if err != nil {
		return err
	}
	if !changed && data != nil {
		return nil
	}
	return saveLedger(e.Dir, path, state)
}
func saveLedger(dirPath, path string, state ledger) error {
	b, err := json.Marshal(state)
	if err != nil || len(b) > 8<<20 {
		return unavailable()
	}
	tmp, err := os.CreateTemp(dirPath, "ledger-")
	if err != nil {
		return unavailable()
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(b); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil || closeErr != nil {
		return unavailable()
	}
	if os.Rename(name, path) != nil {
		return unavailable()
	}
	dir, err := os.Open(dirPath)
	if err != nil {
		return unavailable()
	}
	defer dir.Close()
	if dir.Sync() != nil {
		return unavailable()
	}
	return nil
}
func readPrivate(path string) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Uid != uint32(os.Getuid()) || st.Mode&0777 != 0600 || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Size > 8<<20 {
		return nil, unavailable()
	}
	b := make([]byte, st.Size)
	_, err = f.ReadAt(b, 0)
	return b, err
}
func (e Engine) project(ctx context.Context, s *storedSession) Record {
	r := s.Record
	h, err := e.Host.Inspect(ctx, s.Target)
	if err != nil {
		return Record{Session: r.Session, Manager: r.Manager, Labels: r.Labels, State: "unknown", Doorbell: Doorbell{State: "off"}}
	}
	r.State = h.State
	r.LastActivityAt = h.LastActivityAt
	r.Draining = nil
	if h.State == "draining" {
		r.Draining = h.Draining
	}
	if h.NativeID != nil {
		r.NativeID = h.NativeID
	}
	return r
}
func outcome(state string) map[string]any { return Envelope(map[string]any{"outcome": state}) }
func (e Engine) result(ctx context.Context, op *operation) map[string]any {
	if op == nil {
		return outcome("not_found")
	}
	if op.State == "started" && op.Session != nil {
		if host, ok := e.Host.(ResumeHost); ok {
			if err := host.ValidateResume(ctx, op.Session.Target); err != nil {
				var r Refusal
				if errors.As(err, &r) && r.Code == "session_curator_identity_stale" {
					v := outcome("failed")
					v["code"] = r.Code
					return v
				}
			}
		}
		r := e.project(ctx, op.Session)
		if r.Readable {
			v := outcome("started")
			v["session"] = r
			return v
		}
		return outcome("unknown")
	}
	v := outcome(op.State)
	if op.State == "failed" {
		v["code"] = op.Code
	}
	return v
}
func labelsMatch(actual, wanted map[string]string) bool {
	for k, v := range wanted {
		if actual[k] != v {
			return false
		}
	}
	return true
}
func normalizedLabels(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// Handle validates every request before reading state or invoking a host. Only
// this qualified subset is advertised; omitted lifecycle calls always refuse.
func (e Engine) Handle(ctx context.Context, op string, b []byte) (string, any, error) {
	q, err := Validate(op, b)
	if err != nil {
		return "", nil, err
	}
	if op == "describe" {
		v := e.Version
		if v == "" {
			v = "dev"
		}
		return "describe", Envelope(map[string]any{"id": e.manager(), "version": v, "capabilities": e.capabilities(), "runtimes": map[string][]string{"codex_cli": {"launch", "resume", "attach", "stop", "status", "cancel", "list"}}}), nil
	}
	if op != "launch" && op != "resume" && op != "attach" && op != "stop" && op != "status" && op != "cancel" && op != "list" {
		return "", nil, Fail("session_host_capability_missing", map[string]string{"capability": op + "/1"})
	}
	if op == "launch" || op == "resume" {
		return e.start(ctx, q, b, op == "resume")
	}
	if op == "stop" || op == "attach" {
		return e.control(ctx, op, q)
	}
	var value any
	name := "outcome"
	err = e.transaction(func(s *ledger) (bool, error) {
		switch op {
		case "status":
			if q.Session == "" {
				value = e.result(ctx, s.Operations[q.OperationID])
				return false, nil
			}
			for _, current := range currentSessions(s) {
				if current.Record.Session == q.Session {
					p := &operation{Session: current}
					name = "session"
					r := e.project(ctx, p.Session)
					raw, _ := json.Marshal(r)
					var m map[string]any
					_ = json.Unmarshal(raw, &m)
					value = Envelope(m)
					return false, nil
				}
			}
			return false, Fail("session_resume_not_found")
		case "cancel":
			p := s.Operations[q.OperationID]
			changed := p == nil || p.State == "prepared"
			if changed {
				s.Operations[q.OperationID] = &operation{State: "cancelled"}
			}
			value = e.result(ctx, s.Operations[q.OperationID])
			return changed, nil
		case "list":
			name = "list-page"
			limit := q.Limit
			if limit == 0 {
				limit = 50
			}
			labels := normalizedLabels(q.Labels)
			var pg page
			if q.Cursor != "" {
				var ok bool
				pg, ok = s.Pages[q.Cursor]
				if !ok || pg.Limit != limit || !reflect.DeepEqual(normalizedLabels(pg.Labels), labels) {
					return false, Fail("launch_plan_invalid")
				}
			} else {
				pg = page{Labels: labels, Limit: limit, Rows: []Record{}}
				for _, current := range currentSessions(s) {
					if labelsMatch(current.Record.Labels, labels) {
						pg.Rows = append(pg.Rows, e.project(ctx, current))
					}
				}
				sort.Slice(pg.Rows, func(i, j int) bool { return pg.Rows[i].Session < pg.Rows[j].Session })
			}
			end := pg.Offset + limit
			if end > len(pg.Rows) {
				end = len(pg.Rows)
			}
			// The requested limit is a maximum; large records must still fit
			// one bounded wire packet. Reserve room for a continuation token.
			for end > pg.Offset {
				probe := Envelope(map[string]any{"sessions": pg.Rows[pg.Offset:end], "next_cursor": strings.Repeat("0", 32)})
				packet, _ := json.Marshal(probe)
				if len(packet)+1 <= Limit {
					break
				}
				end--
			}
			if end == pg.Offset && end < len(pg.Rows) {
				return false, unavailable()
			}
			rows := pg.Rows[pg.Offset:end]
			var next *string
			if end < len(pg.Rows) {
				if len(s.Pages) >= 128 {
					return false, unavailable()
				}
				token := opaque()
				pg.Offset = end
				s.Pages[token] = pg
				next = &token
			}
			value = Envelope(map[string]any{"sessions": rows, "next_cursor": next})
			return next != nil, nil
		}
		return false, Fail("launch_plan_invalid")
	})
	return name, value, err
}
func (e Engine) start(ctx context.Context, q Request, b []byte, resume bool) (string, any, error) {
	if q.Runtime != "codex_cli" {
		return "", nil, Fail("session_host_provider_unsupported", map[string]string{"environment": q.Runtime})
	}
	if string(q.Doorbell) != "null" {
		return "", nil, Fail("session_host_capability_missing", map[string]string{"capability": "doorbell-codex/1"})
	}
	if string(q.Credential) != "null" {
		return "", nil, Fail("session_host_capability_missing", map[string]string{"capability": "credential-node/1"})
	}
	digest, err := Digest(b)
	if err != nil {
		return "", nil, Fail("launch_plan_invalid")
	}
	if err := e.checkpoint("before-admission"); err != nil {
		return "", nil, err
	}
	if resume {
		if _, err := os.Lstat(filepath.Join(e.Dir, "ledger.json")); errors.Is(err, os.ErrNotExist) {
			if _, marker := os.Lstat(filepath.Join(e.Dir, "initialized")); errors.Is(marker, os.ErrNotExist) {
				submitted, _ := time.Parse(time.RFC3339, q.SubmittedAt)
				if e.now().Sub(submitted) > 7*24*time.Hour {
					return "", nil, Fail("session_operation_expired")
				}
				return "", nil, Fail("session_resume_not_found")
			}
		}
	}
	var replay any
	generation := int64(1)
	err = e.transaction(func(s *ledger) (bool, error) {
		if p := s.Operations[q.OperationID]; p != nil {
			if p.State == "cancelled" {
				return false, Fail("session_host_unavailable", map[string]string{"reason": "cancelled"})
			}
			if p.Digest != digest {
				return false, Fail("session_operation_conflict")
			}
			switch p.State {
			case "started":
				if host, ok := e.Host.(ResumeHost); ok && p.Session.Binding {
					if err := host.ValidateResume(ctx, p.Session.Target); err != nil {
						return false, err
					}
				}
				r := e.project(ctx, p.Session)
				if !r.Readable {
					return false, Fail("session_liveness_unknown")
				}
				raw, _ := json.Marshal(r)
				var m map[string]any
				_ = json.Unmarshal(raw, &m)
				replay = Envelope(m)
				return false, nil
			case "failed":
				return false, Fail(p.Code, p.Details)
			case "prepared":
				return false, nil
			default:
				return false, Fail("session_liveness_unknown")
			}
		}
		submitted, _ := time.Parse(time.RFC3339, q.SubmittedAt)
		if e.now().Sub(submitted) > 7*24*time.Hour {
			return false, Fail("session_operation_expired")
		}
		var target *storedSession
		if resume {
			var err error
			target, err = e.selectResume(ctx, s, q)
			if err != nil {
				return false, err
			}
			generation = *target.Record.Generation + 1
		}
		s.Operations[q.OperationID] = &operation{Digest: digest, Key: q.AdmissionKey, State: "prepared", Generation: generation, Resume: target}
		return true, nil
	})
	if err != nil {
		return "", nil, err
	}
	if replay != nil {
		return "session", replay, nil
	}
	if err := e.checkpoint("prepared"); err != nil {
		return "", nil, err
	}
	err = e.transaction(func(s *ledger) (bool, error) {
		p := s.Operations[q.OperationID]
		if p == nil || p.State == "cancelled" {
			return false, Fail("session_host_unavailable", map[string]string{"reason": "cancelled"})
		}
		if p.Digest != digest || p.State != "prepared" {
			return false, Fail("session_liveness_unknown")
		}
		generation = p.Generation
		if p.Resume != nil {
			current := currentSessions(s)[p.Resume.Record.Session]
			if current == nil || *current.Record.Generation != *p.Resume.Record.Generation {
				return false, Fail("werkstatt_already_up")
			}
			for id, other := range s.Operations {
				if id != q.OperationID && other.Resume != nil && other.Resume.Target == p.Resume.Target && (other.State == "admitted" || other.State == "unknown") {
					return false, Fail("werkstatt_already_up")
				}
			}
			if row := e.project(ctx, current); !row.Readable || row.State != "down" {
				return false, Fail("werkstatt_already_up")
			}
		}
		if q.AdmissionKey != nil {
			for _, p := range s.Operations {
				if p.State == "prepared" {
					continue
				}
				if p.Key != nil && *p.Key == *q.AdmissionKey {
					if p.Generation >= generation {
						generation = p.Generation + 1
					}
					if p.State == "admitted" || p.State == "unknown" {
						return false, Fail("werkstatt_already_up")
					}
					if p.State == "started" {
						r := e.project(ctx, p.Session)
						if !r.Readable || r.State != "down" {
							return false, Fail("werkstatt_already_up")
						}
					}
				}
			}
		}
		if generation > 9007199254740991 {
			return false, unavailable()
		}
		p.State, p.Generation = "admitted", generation
		return true, nil
	})
	if err != nil {
		return "", nil, err
	}
	if err := e.checkpoint("after-reservation"); err != nil {
		return "", nil, err
	}
	// Persist intent before calling any provider code. Recovery may distinguish
	// a reservation that provably never entered this boundary from uncertainty.
	err = e.transaction(func(s *ledger) (bool, error) {
		p := s.Operations[q.OperationID]
		if p == nil || p.State != "admitted" || p.StartIntent {
			return false, unavailable()
		}
		p.StartIntent = true
		return true, nil
	})
	if err != nil {
		return "", nil, err
	}
	var target *storedSession
	if resume {
		err = e.transaction(func(s *ledger) (bool, error) { target = s.Operations[q.OperationID].Resume; return false, nil })
		if err != nil {
			return "", nil, err
		}
	}
	var h HostSession
	startErr := e.checkpoint("provider-start")
	if startErr == nil {
		if resume {
			host, ok := e.Host.(ResumeHost)
			if !ok || target == nil {
				startErr = Fail("session_resume_unreadable")
			} else {
				h, startErr = host.Resume(ctx, target.Target, q)
			}
		} else {
			h, startErr = e.Host.Launch(ctx, q)
		}
	}
	if startErr == nil {
		if err := e.checkpoint("after-start"); err != nil {
			return "", nil, err
		}
	}
	var value any
	err = e.transaction(func(s *ledger) (bool, error) {
		p := s.Operations[q.OperationID]
		if p == nil || p.Digest != digest || p.State != "admitted" {
			return false, unavailable()
		}
		if startErr != nil {
			var refusal Refusal
			if errors.As(startErr, &refusal) {
				p.State = "failed"
				p.Code = refusal.Code
				p.Details = refusal.Details
			} else {
				p.State = "unknown"
			}
			return true, nil
		}
		when := e.now().Format(time.RFC3339)
		runtime, operationID := q.Runtime, q.OperationID
		r := Record{Session: "SES-" + opaque(), OperationID: &operationID, Manager: e.manager(), Runtime: &runtime, NativeID: h.NativeID, Readable: true, State: h.State, Labels: q.Labels, Generation: &generation, StartedAt: &when, LastActivityAt: h.LastActivityAt, Doorbell: Doorbell{State: "off"}}

		stored := &storedSession{Record: r, Target: h.ID, CWD: q.CWD, Name: q.Name, Profile: q.Profile, Binding: true, Detached: q.Detach}
		if resume {
			if target == nil || h.ID != target.Target {
				p.State = "unknown"
				startErr = Uncertain{}
				return true, nil
			}
			stored.Record.Session = target.Record.Session
			stored.Record.Labels = target.Record.Labels
			stored.CWD, stored.Name, stored.Profile = target.CWD, target.Name, target.Profile
			r = stored.Record
		}
		if _, err := Reply("session-record", r); err != nil {
			p.State = "unknown"
			startErr = Uncertain{}
			return true, nil
		}
		p.State = "started"
		p.Session = stored
		raw, _ := json.Marshal(r)
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		value = Envelope(m)
		return true, nil
	})
	if err != nil {
		return "", nil, err
	}
	if startErr != nil {
		var refusal Refusal
		if errors.As(startErr, &refusal) {
			return "", nil, refusal
		}
		return "", nil, Fail("session_liveness_unknown")
	}
	return "session", value, nil
}

// Recover is called once before the daemon accepts clients. An abandoned
// reservation cannot become a fresh start merely because its runner exited.
func (e Engine) Recover() error {
	if _, err := os.Lstat(filepath.Join(e.Dir, "ledger.json")); errors.Is(err, os.ErrNotExist) {
		if _, marker := os.Lstat(filepath.Join(e.Dir, "initialized")); errors.Is(marker, os.ErrNotExist) {
			return nil
		}
	}
	return e.transaction(func(s *ledger) (bool, error) {
		changed := false
		for _, p := range s.Operations {
			if p.State == "admitted" && p.StartIntent {
				p.State = "unknown"
				changed = true
			}
		}
		return changed, nil
	})
}
func decodeLedger(data []byte, state *ledger) error {
	// Reject duplicate object keys independently of the typed decoder, so a
	// damaged index cannot overwrite one operation with another at decode time.
	tokens := json.NewDecoder(bytes.NewReader(data))
	tokens.UseNumber()
	if err := uniqueJSON(tokens, 0); err != nil {
		return err
	}
	if _, err := tokens.Token(); err != io.EOF {
		return indexError()
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(state); err != nil {
		return err
	}
	return nil
}
func uniqueJSON(d *json.Decoder, depth int) error {
	if depth > 32 {
		return indexError()
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return indexError()
			}
			seen[name] = true
			if err := uniqueJSON(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueJSON(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return indexError()
	}
	_, err = d.Token()
	return err
}

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var admissionPattern = regexp.MustCompile(`^werkstatt:[a-z0-9][a-z0-9-]{0,47}$`)

func validLedger(state ledger) bool {
	if state.Version != 1 || state.Operations == nil || state.Pages == nil {
		return false
	}
	handles := map[string]string{}
	generations := map[string]bool{}
	for id, p := range state.Operations {
		if _, err := Validate("status", marshalID(id)); err != nil || p == nil {
			return false
		}
		if p.Key != nil && !admissionPattern.MatchString(*p.Key) {
			return false
		}
		switch p.State {
		case "cancelled":
			if p.Session != nil {
				return false
			}
			continue
		case "prepared", "admitted", "unknown", "failed", "started":
		default:
			return false
		}
		if !digestPattern.MatchString(p.Digest) || p.Generation < 1 || p.Generation > 9007199254740991 {
			return false
		}
		if p.State == "failed" && Fail(p.Code, p.Details).Code != p.Code {
			return false
		}
		if p.State != "started" && p.Session != nil {
			return false
		}
		if r := p.Resume; r != nil {
			if r.Record.OperationID == nil || r.Record.Generation == nil || *r.Record.Generation >= p.Generation {
				return false
			}
			prior := state.Operations[*r.Record.OperationID]
			if prior == nil || prior.State != "started" || prior.Session == nil || !reflect.DeepEqual(r, prior.Session) {
				return false
			}
		}
		if p.State == "started" {
			s := p.Session
			if s == nil || s.Target == "" || !filepath.IsAbs(s.CWD) || s.Record.OperationID == nil || *s.Record.OperationID != id || s.Record.Generation == nil || *s.Record.Generation != p.Generation || !s.Record.Readable || (handles[s.Record.Session] != "" && handles[s.Record.Session] != s.Target) || generations[s.Record.Session+":"+fmt.Sprint(p.Generation)] {
				return false
			}
			if _, err := Reply("session-record", s.Record); err != nil {
				return false
			}
			handles[s.Record.Session] = s.Target
			generations[s.Record.Session+":"+fmt.Sprint(p.Generation)] = true
		}
	}
	for _, pg := range state.Pages {
		if pg.Rows == nil || pg.Offset < 0 || pg.Offset > len(pg.Rows) || pg.Limit < 1 || pg.Limit > 100 {
			return false
		}
		for _, r := range pg.Rows {
			if _, err := Reply("session-record", r); err != nil {
				return false
			}
		}
	}
	return true
}
func marshalID(id string) []byte {
	b, _ := json.Marshal(map[string]string{"operation_id": id})
	return b
}
