// Package admission verifies synthetic hosted evidence. It implements no launcher
// wire protocol, credential handling, process control or lease mutation.
package admission

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const Version = 1
const ProcessTime = "2006-01-02T15:04:05.000000Z"

var id = regexp.MustCompile(`^[A-Za-z0-9._:/@+\-$%]{1,128}$`)
var digest = regexp.MustCompile(`^[a-f0-9]{64}$`)
var cdhash = regexp.MustCompile(`^[a-f0-9]{40}$`)
var boot = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

type Process struct {
	PID       int    `json:"pid"`
	UID       uint32 `json:"uid"`
	BootUUID  string `json:"boot_uuid"`
	StartedAt string `json:"started_at"`
	CDHash    string `json:"cdhash"`
}

// Binding is a non-secret immutable projection, not an executable launch plan.
// The runner/harness roles are explicit; this synthetic route has no wrapper.
type Binding struct {
	Execution       string  `json:"execution"`
	Registration    string  `json:"registration"`
	Operation       string  `json:"operation"`
	Session         string  `json:"session"`
	Generation      uint64  `json:"generation"`
	PlanDigest      string  `json:"plan_digest"`
	Profile         string  `json:"profile"`
	ProfileLock     string  `json:"profile_lock"`
	Environment     string  `json:"environment"`
	ProfileFragment string  `json:"profile_fragment"`
	Transport       string  `json:"transport"`
	Permissions     string  `json:"permissions"`
	Workspace       string  `json:"workspace"`
	Home            string  `json:"home"`
	Runtime         string  `json:"runtime"`
	Vendor          string  `json:"vendor"`
	Model           string  `json:"model"`
	Effort          string  `json:"effort"`
	NativeRelease   string  `json:"native_release"`
	NativeSession   string  `json:"native_session"`
	RunnerInstance  string  `json:"runner_instance"`
	RunnerSocket    string  `json:"runner_socket"`
	Runner          Process `json:"runner"`
	Harness         Process `json:"harness"`
}
type Record struct {
	Version   int     `json:"version"`
	Revision  uint64  `json:"revision"`
	Lifecycle string  `json:"lifecycle"` // Independent of activity/state and operation phases.
	Binding   Binding `json:"binding"`
}

// Lease carries only pid/started_at for its process holder. The full tuple is
// verified separately in Record. This is synthetic evidence, not Curator wire.
type Lease struct {
	Version      int    `json:"version"`
	Revision     uint64 `json:"revision"`
	ID           string `json:"id"`
	AcquireID    string `json:"acquire_id"`
	Disposition  string `json:"disposition"`
	Registration string `json:"registration"`
	Session      string `json:"session"`
	Generation   uint64 `json:"generation"`
	Home         string `json:"home"`
	Profile      string `json:"profile"`
	ProfileLock  string `json:"profile_lock"`
	Environment  string `json:"environment"`
	PID          int    `json:"pid"`
	StartedAt    string `json:"started_at"`
}
type Snapshot struct {
	Record Record `json:"record"`
	Lease  Lease  `json:"lease"`
	Failed bool   `json:"failed"`
}
type Config struct {
	Expected Snapshot
	Observe  func(context.Context) (Record, Lease, error)
	Process  func(context.Context, int) (Process, error)
}

func validPath(p string) bool {
	return len(p) <= 1024 && filepath.IsAbs(p) && filepath.Clean(p) == p && !strings.ContainsAny(p, "\x00\r\n")
}
func validProcess(p Process) bool {
	t, err := time.Parse(ProcessTime, p.StartedAt)
	return p.PID > 0 && boot.MatchString(p.BootUUID) && cdhash.MatchString(p.CDHash) && err == nil && t.Format(ProcessTime) == p.StartedAt && t.Unix() > 0
}
func Valid(s Snapshot) bool {
	r, l := s.Record, s.Lease
	b := r.Binding
	if r.Version != Version || l.Version != Version || r.Revision == 0 || l.Revision == 0 || r.Revision > 1<<53-1 || l.Revision > 1<<53-1 || r.Lifecycle != "started" || l.Disposition != "bound" {
		return false
	}
	for _, v := range []string{b.Execution, b.Registration, b.Operation, b.Session, b.Profile, b.Environment, b.Permissions, b.Runtime, b.Vendor, b.Model, b.Effort, b.NativeRelease, b.NativeSession, b.RunnerInstance, l.ID, l.AcquireID} {
		if !id.MatchString(v) {
			return false
		}
	}
	if b.Generation == 0 || b.Generation > 1<<53-1 || !digest.MatchString(b.PlanDigest) || !digest.MatchString(b.ProfileLock) || !digest.MatchString(b.ProfileFragment) || b.Transport != "pipe" {
		return false
	}
	if !validPath(b.Workspace) || !validPath(b.Home) || !validPath(b.RunnerSocket) || len(b.RunnerSocket) > 100 {
		return false
	}
	if !validProcess(b.Runner) || !validProcess(b.Harness) || b.Runner.PID == b.Harness.PID || b.Runner.BootUUID != b.Harness.BootUUID {
		return false
	}
	return l.Registration == b.Registration && l.Session == b.Session && l.Generation == b.Generation && l.Home == b.Home && l.Profile == b.Profile && l.Environment == b.Environment && l.ProfileLock == b.ProfileLock && l.PID == b.Harness.PID && l.StartedAt == b.Harness.StartedAt
}
func ValidateConfig(c Config) error {
	if c.Observe == nil || c.Process == nil || c.Expected.Failed || !Valid(c.Expected) {
		return errors.New("admission_config_invalid")
	}
	return nil
}
func sameLease(a, b Lease) bool { a.Revision = 0; b.Revision = 0; return a == b }

// Verify performs read-only, deadline-bounded observations. A successful tuple
// check does not confer signal authority or solve a later check/signal race.
func Verify(ctx context.Context, c Config, prior *Snapshot, instance string, childPID int) (Snapshot, error) {
	if ValidateConfig(c) != nil {
		return Snapshot{}, errors.New("admission_config_invalid")
	}
	if _, ok := ctx.Deadline(); !ok {
		return Snapshot{}, errors.New("admission_deadline_required")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, errors.New("admission_unavailable")
	}
	if prior != nil && (prior.Failed || !Valid(*prior)) {
		return Snapshot{}, errors.New("admission_reconciliation_required")
	}
	r, l, err := c.Observe(ctx)
	if err != nil || ctx.Err() != nil {
		return Snapshot{}, errors.New("admission_unavailable")
	}
	s := Snapshot{Record: r, Lease: l}
	if !Valid(s) {
		return Snapshot{}, errors.New("admission_evidence_invalid")
	}
	if r.Binding != c.Expected.Record.Binding || !sameLease(l, c.Expected.Lease) || instance != r.Binding.RunnerInstance || childPID != r.Binding.Harness.PID {
		return Snapshot{}, errors.New("admission_binding_conflict")
	}
	if r.Revision < c.Expected.Record.Revision || l.Revision < c.Expected.Lease.Revision {
		return Snapshot{}, errors.New("admission_revision_rollback")
	}
	if prior != nil && (r.Binding != prior.Record.Binding || !sameLease(l, prior.Lease) || r.Revision < prior.Record.Revision || l.Revision < prior.Lease.Revision) {
		return Snapshot{}, errors.New("admission_revision_conflict")
	}
	for _, want := range []Process{r.Binding.Runner, r.Binding.Harness} {
		got, err := c.Process(ctx, want.PID)
		if err != nil || ctx.Err() != nil {
			return Snapshot{}, errors.New("admission_process_unavailable")
		}
		if got != want {
			return Snapshot{}, errors.New("admission_process_conflict")
		}
	}
	return s, nil
}
