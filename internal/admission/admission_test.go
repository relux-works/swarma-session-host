package admission

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func fixture() Snapshot {
	p := Process{PID: 101, UID: 501, BootUUID: "12345678-1234-1234-1234-123456789abc", StartedAt: "2026-10-29T20:00:00.123456Z", CDHash: strings.Repeat("a", 40)}
	child := p
	child.PID = 102
	b := Binding{Execution: "execution", Registration: "registration", Operation: "operation", Session: "session", Generation: 1, PlanDigest: strings.Repeat("a", 64), Profile: "synthetic", Environment: "synthetic-env", ProfileFragment: strings.Repeat("c", 64), Transport: "pipe", Permissions: "synthetic", ProfileLock: strings.Repeat("b", 64), Workspace: "/fixture/work", Home: "/fixture/home", Runtime: "fixture", Vendor: "new-vendor", Model: "new-model", Effort: "new-effort", NativeRelease: "synthetic", NativeSession: "native", RunnerInstance: "instance", RunnerSocket: "/fixture/runner.sock", Runner: p, Harness: child}
	return Snapshot{Record: Record{Version: Version, Revision: 1, Lifecycle: "started", Binding: b}, Lease: Lease{Version: Version, Revision: 1, ID: "lease", AcquireID: "acquire", Disposition: "bound", Registration: b.Registration, Session: b.Session, Generation: b.Generation, Home: b.Home, Profile: b.Profile, Environment: b.Environment, ProfileLock: b.ProfileLock, PID: child.PID, StartedAt: child.StartedAt}}
}
func config(s Snapshot) Config {
	return Config{Expected: s, Observe: func(context.Context) (Record, Lease, error) { return s.Record, s.Lease, nil }, Process: func(_ context.Context, pid int) (Process, error) {
		if pid == s.Record.Binding.Runner.PID {
			return s.Record.Binding.Runner, nil
		}
		return s.Record.Binding.Harness, nil
	}}
}
func verify(t *testing.T, c Config, prior *Snapshot) (Snapshot, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return Verify(ctx, c, prior, c.Expected.Record.Binding.RunnerInstance, c.Expected.Record.Binding.Harness.PID)
}

// Exact immutable evidence and monotonic record/lease revisions are accepted;
// malformed, missing, conflicting and terminal evidence never qualifies.
func TestEvidenceMatrix(t *testing.T) {
	base := fixture()
	tests := []struct {
		name   string
		mutate func(*Snapshot)
		code   string
	}{
		{"valid", func(*Snapshot) {}, ""},
		{"record-version", func(s *Snapshot) { s.Record.Version++ }, "admission_evidence_invalid"},
		{"lease-version", func(s *Snapshot) { s.Lease.Version++ }, "admission_evidence_invalid"},
		{"missing-native", func(s *Snapshot) { s.Record.Binding.NativeSession = "" }, "admission_evidence_invalid"},
		{"unknown-lifecycle", func(s *Snapshot) { s.Record.Lifecycle = "unknown" }, "admission_evidence_invalid"},
		{"released", func(s *Snapshot) { s.Lease.Disposition = "released" }, "admission_evidence_invalid"},
		{"broken", func(s *Snapshot) { s.Lease.Disposition = "broken" }, "admission_evidence_invalid"},
		{"lease-home", func(s *Snapshot) { s.Lease.Home = "/other" }, "admission_evidence_invalid"},
		{"lease-pid", func(s *Snapshot) { s.Lease.PID++ }, "admission_evidence_invalid"},
		{"lease-birth", func(s *Snapshot) { s.Lease.StartedAt = "2026-10-29T20:00:00.123457Z" }, "admission_evidence_invalid"},
		{"generation", func(s *Snapshot) { s.Record.Binding.Generation++; s.Lease.Generation++ }, "admission_binding_conflict"},
		{"registration", func(s *Snapshot) { s.Record.Binding.Registration = "other"; s.Lease.Registration = "other" }, "admission_binding_conflict"},
		{"home", func(s *Snapshot) { s.Record.Binding.Home = "/other"; s.Lease.Home = "/other" }, "admission_binding_conflict"},
		{"profile", func(s *Snapshot) { s.Record.Binding.Profile = "other"; s.Lease.Profile = "other" }, "admission_binding_conflict"},
		{"environment", func(s *Snapshot) { s.Record.Binding.Environment = "other"; s.Lease.Environment = "other" }, "admission_binding_conflict"},
		{"profile-lock", func(s *Snapshot) {
			s.Record.Binding.ProfileLock = strings.Repeat("d", 64)
			s.Lease.ProfileLock = s.Record.Binding.ProfileLock
		}, "admission_binding_conflict"},
		{"fragment", func(s *Snapshot) { s.Record.Binding.ProfileFragment = strings.Repeat("d", 64) }, "admission_binding_conflict"},
		{"posture", func(s *Snapshot) { s.Record.Binding.Permissions = "other" }, "admission_binding_conflict"},
		{"transport", func(s *Snapshot) { s.Record.Binding.Transport = "pty" }, "admission_evidence_invalid"},
		{"bad-path", func(s *Snapshot) { s.Record.Binding.Workspace = "/other\npath" }, "admission_evidence_invalid"},
		{"workspace", func(s *Snapshot) { s.Record.Binding.Workspace = "/other" }, "admission_binding_conflict"},
		{"plan", func(s *Snapshot) { s.Record.Binding.PlanDigest = strings.Repeat("c", 64) }, "admission_binding_conflict"},
		{"runner", func(s *Snapshot) { s.Record.Binding.RunnerInstance = "other" }, "admission_binding_conflict"},
		{"native", func(s *Snapshot) { s.Record.Binding.NativeSession = "other" }, "admission_binding_conflict"},
		{"unknown-lease", func(s *Snapshot) { s.Lease.Disposition = "unknown" }, "admission_evidence_invalid"},
		{"record-revision-zero", func(s *Snapshot) { s.Record.Revision = 0 }, "admission_evidence_invalid"},
		{"lease-revision-zero", func(s *Snapshot) { s.Lease.Revision = 0 }, "admission_evidence_invalid"},
		{"whole-second-birth", func(s *Snapshot) {
			s.Record.Binding.Harness.StartedAt = "2026-10-29T20:00:00Z"
			s.Lease.StartedAt = s.Record.Binding.Harness.StartedAt
		}, "admission_evidence_invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observed := base
			tt.mutate(&observed)
			c := config(base)
			c.Observe = func(context.Context) (Record, Lease, error) { return observed.Record, observed.Lease, nil }
			_, err := verify(t, c, nil)
			if tt.code == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != tt.code {
				t.Fatalf("got %v want %s", err, tt.code)
			}
		})
	}
	// Both revision directions are checked against independent persisted evidence.
	prior := base
	prior.Record.Revision = 3
	prior.Lease.Revision = 4
	for _, revs := range [][2]uint64{{3, 4}, {4, 5}, {2, 4}, {3, 3}} {
		c := config(base)
		observed := base
		observed.Record.Revision = revs[0]
		observed.Lease.Revision = revs[1]
		c.Observe = func(context.Context) (Record, Lease, error) { return observed.Record, observed.Lease, nil }
		_, err := verify(t, c, &prior)
		want := revs[0] < 3 || revs[1] < 4
		if (want && (err == nil || err.Error() != "admission_revision_conflict")) || (!want && err != nil) {
			t.Fatalf("revisions %v: %v", revs, err)
		}
	}
}

// A live provider control succeeds; each kernel tuple mismatch independently
// refuses, including same-PID/different-birth and same-birth/different-image.
func TestProcessMismatch(t *testing.T) {
	for _, field := range []string{"valid", "uid", "boot", "birth", "image", "pid", "unavailable"} {
		t.Run(field, func(t *testing.T) {
			s := fixture()
			c := config(s)
			base := c.Process
			c.Process = func(ctx context.Context, pid int) (Process, error) {
				p, err := base(ctx, pid)
				if pid == s.Record.Binding.Harness.PID {
					switch field {
					case "uid":
						p.UID++
					case "boot":
						p.BootUUID = "87654321-1234-1234-1234-123456789abc"
					case "birth":
						p.StartedAt = "2026-10-29T20:00:00.123457Z"
					case "image":
						p.CDHash = strings.Repeat("b", 40)
					case "pid":
						p.PID++
					case "unavailable":
						err = errors.New("synthetic failure")
					}
				}
				return p, err
			}
			_, err := verify(t, c, nil)
			if field == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("invalid process admitted")
			} else if field == "unavailable" && err.Error() != "admission_process_unavailable" {
				t.Fatal(err)
			} else if field != "unavailable" && err.Error() != "admission_process_conflict" {
				t.Fatal(err)
			}
		})
	}
}

// Provider failure, expired deadlines, failed recovery and absent callbacks
// cannot turn a prior or expected snapshot into fresh admission.
func TestUnavailableAndFailed(t *testing.T) {
	s := fixture()
	c := config(s)
	c.Observe = func(context.Context) (Record, Lease, error) {
		return s.Record, s.Lease, errors.New("synthetic-private-canary")
	}
	if _, err := verify(t, c, nil); err == nil || err.Error() != "admission_unavailable" {
		t.Fatal(err)
	}
	c = config(s)
	c.Observe = func(ctx context.Context) (Record, Lease, error) { <-ctx.Done(); return s.Record, s.Lease, nil }
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := Verify(ctx, c, nil, s.Record.Binding.RunnerInstance, s.Record.Binding.Harness.PID); err == nil || err.Error() != "admission_unavailable" {
		t.Fatal(err)
	}
	prior := s
	prior.Failed = true
	if _, err := verify(t, config(s), &prior); err == nil || err.Error() != "admission_reconciliation_required" {
		t.Fatal(err)
	}
	c = config(s)
	c.Process = nil
	if ValidateConfig(c) == nil {
		t.Fatal("missing process observer admitted")
	}
	if _, err := Verify(context.Background(), config(s), nil, s.Record.Binding.RunnerInstance, s.Record.Binding.Harness.PID); err == nil || err.Error() != "admission_deadline_required" {
		t.Fatal(err)
	}
}
