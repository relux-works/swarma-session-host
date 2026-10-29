//go:build darwin && cgo

package sessionhost_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/admission"
	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

// This provider contract is read-only: it has no acquire/bind/release/break or
// process-control methods. Zero lease operations follows from that absence;
// no synthetic mutation counter is presented as a real lease-system observation.
type readOnlyEvidence interface {
	observe(context.Context) (admission.Record, admission.Lease, error)
}

var _ readOnlyEvidence = (*evidenceProvider)(nil)

// Every mismatch is checked at real Host.Open against a nearby valid execution:
// exact refusal, zero writes, same runner/child, and durable unknown failure.
// This complements the post-first-write loss and protocol conflict witnesses.
func TestAdmissionHostPairMatrix(t *testing.T) {
	tests := []struct {
		name, code string
		change     func(*admission.Snapshot)
		process    func(admission.Process) (admission.Process, error)
	}{
		{name: "valid"},
		{"generation", "admission_binding_conflict", func(s *admission.Snapshot) { s.Record.Binding.Generation++; s.Lease.Generation++ }, nil},
		{"registration", "admission_binding_conflict", func(s *admission.Snapshot) { s.Record.Binding.Registration = "other"; s.Lease.Registration = "other" }, nil},
		{"uid", "admission_process_conflict", nil, func(p admission.Process) (admission.Process, error) { p.UID++; return p, nil }},
		{"boot", "admission_process_conflict", nil, func(p admission.Process) (admission.Process, error) {
			p.BootUUID = "87654321-1234-1234-1234-123456789abc"
			return p, nil
		}},
		{"birth", "admission_process_conflict", nil, func(p admission.Process) (admission.Process, error) {
			at, _ := time.Parse(admission.ProcessTime, p.StartedAt)
			p.StartedAt = at.Add(time.Microsecond).Format(admission.ProcessTime)
			return p, nil
		}},
		{"executable", "admission_process_conflict", nil, func(p admission.Process) (admission.Process, error) {
			p.CDHash = strings.Repeat("f", 40)
			return p, nil
		}},
		{"plan", "admission_binding_conflict", func(s *admission.Snapshot) { s.Record.Binding.PlanDigest = strings.Repeat("d", 64) }, nil},
		{"profile", "admission_binding_conflict", func(s *admission.Snapshot) { s.Record.Binding.Profile = "other"; s.Lease.Profile = "other" }, nil},
		{"workspace", "admission_binding_conflict", func(s *admission.Snapshot) { s.Record.Binding.Workspace = "/other" }, nil},
		{"home", "admission_binding_conflict", func(s *admission.Snapshot) { s.Record.Binding.Home = "/other"; s.Lease.Home = "/other" }, nil},
		{"runner", "admission_binding_conflict", func(s *admission.Snapshot) { s.Record.Binding.RunnerInstance = "other" }, nil},
		{"native", "admission_binding_conflict", func(s *admission.Snapshot) { s.Record.Binding.NativeSession = "other" }, nil},
		{"missing-process-proof", "admission_evidence_invalid", func(s *admission.Snapshot) { s.Record.Binding.Runner = admission.Process{} }, nil},
		{"missing-native", "admission_evidence_invalid", func(s *admission.Snapshot) { s.Record.Binding.NativeSession = "" }, nil},
		{"broken-lease", "admission_evidence_invalid", func(s *admission.Snapshot) { s.Lease.Disposition = "broken" }, nil},
		{"conflicting-lease", "admission_evidence_invalid", func(s *admission.Snapshot) { s.Lease.Home = "/other" }, nil},
		{"record-rollback", "admission_revision_rollback", func(s *admission.Snapshot) { s.Record.Revision-- }, nil},
		{"lease-rollback", "admission_revision_rollback", func(s *admission.Snapshot) { s.Lease.Revision-- }, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, _ := setup(t, fixtureadapters.JSON{}, false)
			cfg, p := admitted(t, cfg)
			before := inspectRunner(t, cfg)
			if tt.change != nil {
				tt.change(&p.proof)
			}
			if tt.process != nil {
				cfg.Admission.Process = func(ctx context.Context, pid int) (admission.Process, error) {
					got, err := admission.ObserveProcess(ctx, pid)
					if err != nil {
						return got, err
					}
					return tt.process(got)
				}
			}
			h, err := sessionhost.Open(cfg)
			if tt.code == "" {
				if err != nil {
					t.Fatal(err)
				}
				defer h.Close()
				if _, err := h.Enqueue(sessionhost.Notice{ID: "control", Reference: "fixture/control"}); err != nil {
					t.Fatal(err)
				}
				await(t, h, "control", "applied")
				if got := inspectRunner(t, cfg); got.Writes != 1 || got.Instance != before.Instance || got.PID != before.PID {
					t.Fatal("valid neighbour changed execution or suppressed write")
				}
				return
			}
			if err == nil {
				h.Close()
				t.Fatal("mismatch admitted")
			}
			if err.Error() != tt.code {
				t.Fatalf("got %v want %s", err, tt.code)
			}
			got := inspectRunner(t, cfg)
			if got.Writes != 0 || got.Instance != before.Instance || got.PID != before.PID || got.Exited {
				t.Fatal("initial refusal changed execution or wrote")
			}
			disk := readAdmissionDisk(t, cfg)
			if disk.Admission == nil || !disk.Admission.Failed || disk.InputState != sessionhost.InputUnknown || disk.Fence != "unknown" || len(disk.Records) != 0 {
				t.Fatal("initial refusal lost failed marker")
			}
		})
	}
}

// Higher record AND lease revisions persist through a real host reconnect;
// the same execution then accepts exactly one further notice, without replay.
func TestAdmissionHigherRevisionReconnect(t *testing.T) {
	cfg, _ := setup(t, fixtureadapters.JSON{}, false)
	cfg, p := admitted(t, cfg)
	h := openHost(t, cfg)
	if _, err := h.Enqueue(sessionhost.Notice{ID: "one", Reference: "fixture/one"}); err != nil {
		t.Fatal(err)
	}
	await(t, h, "one", "applied")
	before := inspectRunner(t, cfg)
	h.Close()
	p.mu.Lock()
	p.proof.Record.Revision++
	p.proof.Lease.Revision++
	p.mu.Unlock()
	replacement := openHost(t, cfg)
	if s := replacement.Snapshot(); s.Admission.Record.Revision != 3 || s.Admission.Lease.Revision != 3 {
		t.Fatal("higher revisions not persisted")
	}
	if _, err := replacement.Enqueue(sessionhost.Notice{ID: "two", Reference: "fixture/two"}); err != nil {
		t.Fatal(err)
	}
	await(t, replacement, "two", "applied")
	if got := inspectRunner(t, cfg); got.Writes != 2 || got.Instance != before.Instance || got.PID != before.PID || got.Exited {
		t.Fatal("reconnect replayed or replaced execution")
	}
}
