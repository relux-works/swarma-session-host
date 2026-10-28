//go:build darwin || linux

package musebuilds

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// requireLastGoodCode fails unless err chains to a last-good refusal
// with the exact code and stage. Refusal identity is the code, never
// message prose.
func requireLastGoodCode(t *testing.T, err error, code, stage string) *LastGoodError {
	t.Helper()
	if err == nil {
		t.Fatalf("want refusal %s, got success", code)
	}
	refusal, ok := AsLastGoodError(err)
	if !ok {
		t.Fatalf("want refusal %s, got untyped %v", code, err)
	}
	if refusal.Code != code {
		t.Fatalf("want refusal %s, got %s (%v)", code, refusal.Code, err)
	}
	if refusal.Stage != stage {
		t.Fatalf("want stage %s for %s, got %s (%v)", stage, code, refusal.Stage, err)
	}
	return refusal
}

// probeOKBuild publishes one frozen build through the public freeze
// entry and records its clean first probe, returning the verified
// tuple a genuine startup can be admitted for.
func probeOKBuild(t *testing.T, f *freezeFixture, build string) FrozenIdentity {
	t.Helper()
	frozen, discovery := probeBuild(t, f, build)
	if _, err := RecordProbeOutcome(context.Background(), f.config(), frozen, successObservation("attempt-success-1", discovery.SnapshotID)); err != nil {
		t.Fatal(err)
	}
	return frozen
}

// genuineSuccess bodies an MSP startup the host actually observed:
// the verified tuple, a startup attempt, the surface and the observed
// readiness timestamp.
func genuineSuccess(frozen FrozenIdentity, attempt string) StartSuccess {
	return StartSuccess{
		Frozen: frozen, AttemptID: attempt, Surface: SurfaceMSP,
		ReadyAt: "2026-10-10T09:30:00Z", By: "test-msp",
	}
}

// TestLastGoodWriteLockDeadline proves last-good admission refuses
// bounded when the metadata lock is contended: no sequence is
// allocated and no receipt is published.
func TestLastGoodWriteLockDeadline(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
	holder, err := os.OpenFile(filepath.Join(f.root, "state.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Flock(int(holder.Fd()), syscall.LOCK_UN) }()
	type result struct {
		receipt LastGood
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		done <- result{receipt: receipt, err: err}
	}()
	select {
	case r := <-done:
		requireLastGoodCode(t, r.err, CodeLastGoodLockFailed, StageLastGood)
	case <-time.After(60 * time.Second):
		t.Fatal("contended last-good admission never refused")
	}
	if _, err := os.Stat(filepath.Join(f.root, lastGoodFileName)); !os.IsNotExist(err) {
		t.Fatalf("contended admission published a receipt: %v", err)
	}
}

// TestLastGoodWrittenOnlyOnSuccess proves only an actual launch success
// writes: a probe-only value without observed readiness refuses, a
// malformed readiness refuses, and the genuine success on either
// surface admits. Malformed inputs refuse as invalid, never as absent.
func TestLastGoodWrittenOnlyOnSuccess(t *testing.T) {
	t.Run("probe-only refuses", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		probeOnly := genuineSuccess(frozen, "startup-1")
		probeOnly.ReadyAt = ""
		_, err := WriteLastGood(context.Background(), cfg, probeOnly)
		requireLastGoodCode(t, err, CodeLastGoodNotStartup, StageLastGood)
		if _, err := os.Stat(filepath.Join(f.root, lastGoodFileName)); !os.IsNotExist(err) {
			t.Fatalf("probe-only admission published a receipt: %v", err)
		}
	})

	t.Run("malformed readiness refuses", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		bad := genuineSuccess(frozen, "startup-1")
		bad.ReadyAt = "tomorrow"
		_, err := WriteLastGood(context.Background(), cfg, bad)
		requireLastGoodCode(t, err, CodeLastGoodNotStartup, StageLastGood)
	})

	t.Run("genuine msp success admits", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		receipt, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Sequence != 1 || receipt.AttemptID != "startup-1" || receipt.ReadyAt != "2026-10-10T09:30:00Z" {
			t.Fatalf("receipt = %+v, want the admitted startup", receipt)
		}
		if receipt.Frozen.Build != frozen.Build || receipt.Frozen.SHA256 != frozen.SHA256 || receipt.Frozen.Path != frozen.Path {
			t.Fatalf("receipt frozen = %+v, want %+v", receipt.Frozen, frozen)
		}
		if receipt.Surface != SurfaceMSP {
			t.Fatalf("receipt surface = %q, want %q", receipt.Surface, SurfaceMSP)
		}
		got, err := ReadLastGood(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got.Surface != SurfaceMSP || got != receipt {
			t.Fatalf("read %+v, wrote %+v", got, receipt)
		}
	})

	t.Run("genuine pty success admits", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		pty := genuineSuccess(frozen, "startup-pty-1")
		pty.Surface = SurfacePTY
		pty.By = "test-pty"
		receipt, err := WriteLastGood(context.Background(), cfg, pty)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Sequence != 1 || receipt.AttemptID != "startup-pty-1" {
			t.Fatalf("receipt = %+v, want the admitted PTY startup", receipt)
		}
		if receipt.Surface != SurfacePTY {
			t.Fatalf("receipt surface = %q, want %q", receipt.Surface, SurfacePTY)
		}
		got, err := ReadLastGood(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got.Surface != SurfacePTY || got != receipt {
			t.Fatalf("read %+v, wrote %+v", got, receipt)
		}
	})
}

// TestConcurrentLastGoodWritesSerialize proves concurrent admissions
// serialize: every writer observes the lock-held sequence and no two
// receipts share one. The allocation and the publication happen under
// the same lock hold.
func TestConcurrentLastGoodWritesSerialize(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
	const writers = 24
	start := make(chan struct{})
	var group sync.WaitGroup
	type outcome struct {
		receipt LastGood
		err     error
	}
	results := make(chan outcome, writers)
	for i := 0; i < writers; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			<-start
			receipt, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, strings.Repeat("s", 8)+string(rune('a'+i/8))+string(rune('0'+i%8))))
			results <- outcome{receipt: receipt, err: err}
		}(i)
	}
	close(start)
	done := make(chan struct{})
	go func() { group.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent admissions never returned")
	}
	seen := map[uint64]string{}
	for i := 0; i < writers; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if first, dup := seen[r.receipt.Sequence]; dup {
			t.Fatalf("sequence %d admitted twice: %s and %s", r.receipt.Sequence, first, r.receipt.AttemptID)
		}
		seen[r.receipt.Sequence] = r.receipt.AttemptID
	}
	for want := uint64(1); want <= writers; want++ {
		if _, ok := seen[want]; !ok {
			t.Fatalf("sequence %d never admitted; got %v", want, seen)
		}
	}
	latest, err := ReadLastGood(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if latest.Sequence != writers {
		t.Fatalf("latest sequence = %d, want %d", latest.Sequence, writers)
	}
	if _, ok := seen[latest.Sequence]; !ok {
		t.Fatalf("latest receipt %+v names an unadmitted sequence", latest)
	}
}

// plantLastGood writes one raw last-good receipt for read-class cells.
// Fixture setup may plant bytes directly; every killer drives the
// public entry.
func (f *freezeFixture) plantLastGood(t *testing.T, raw []byte) {
	t.Helper()
	f.writeFileSync(filepath.Join(f.root, lastGoodFileName), raw, 0o600)
}

// TestLastGoodReadClasses proves last-good reads stay typed: only a
// missing file reads absent, while malformed, disagreeing or
// unverified receipts are unreadable, never a silent reset and never
// an invented target.
func TestLastGoodReadClasses(t *testing.T) {
	setup := func(t *testing.T) (*freezeFixture, HostConfig, FrozenIdentity) {
		t.Helper()
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		return f, cfg, frozen
	}

	t.Run("absent reads absent", func(t *testing.T) {
		f, cfg, _ := setup(t)
		_, err := ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodAbsent, StageLastGood)
		_ = f
	})

	t.Run("admitted receipt reads back", func(t *testing.T) {
		_, cfg, frozen := setup(t)
		wrote, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		if err != nil {
			t.Fatal(err)
		}
		got, err := ReadLastGood(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got != wrote {
			t.Fatalf("read %+v, wrote %+v", got, wrote)
		}
	})

	t.Run("malformed reads unreadable", func(t *testing.T) {
		f, cfg, _ := setup(t)
		f.plantLastGood(t, []byte("{malformed"))
		_, err := ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("foreign schema reads unreadable", func(t *testing.T) {
		f, cfg, frozen := setup(t)
		wrote, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		if err != nil {
			t.Fatal(err)
		}
		wrote.Schema = 2
		raw, err := json.Marshal(wrote)
		if err != nil {
			t.Fatal(err)
		}
		f.plantLastGood(t, raw)
		_, err = ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("unparseable build reads unreadable", func(t *testing.T) {
		f, cfg, frozen := setup(t)
		wrote, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		if err != nil {
			t.Fatal(err)
		}
		wrote.Frozen.Build = "tomorrow"
		raw, err := json.Marshal(wrote)
		if err != nil {
			t.Fatal(err)
		}
		f.plantLastGood(t, raw)
		_, err = ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("digest mismatch reads unreadable", func(t *testing.T) {
		f, cfg, frozen := setup(t)
		wrote, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		if err != nil {
			t.Fatal(err)
		}
		wrote.Frozen.SHA256 = strings.Repeat("0", 64)
		raw, err := json.Marshal(wrote)
		if err != nil {
			t.Fatal(err)
		}
		f.plantLastGood(t, raw)
		_, err = ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("state digest disagreement reads unreadable", func(t *testing.T) {
		f, cfg, frozen := setup(t)
		if _, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1")); err != nil {
			t.Fatal(err)
		}
		key := freezeKey(frozen.Build, frozen.SHA256)
		stateRaw, err := os.ReadFile(statePath(f.root, key))
		if err != nil {
			t.Fatal(err)
		}
		var state BuildState
		if err := json.Unmarshal(stateRaw, &state); err != nil {
			t.Fatal(err)
		}
		other := "0" + frozen.SHA256[1:]
		if other == frozen.SHA256 {
			other = "1" + frozen.SHA256[1:]
		}
		state.SHA256 = other
		raw, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		f.writeFileSync(statePath(f.root, key), raw, 0o600)
		_, err = ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("missing surface reads unreadable", func(t *testing.T) {
		f, cfg, frozen := setup(t)
		wrote, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		if err != nil {
			t.Fatal(err)
		}
		wrote.Surface = ""
		raw, err := json.Marshal(wrote)
		if err != nil {
			t.Fatal(err)
		}
		f.plantLastGood(t, raw)
		_, err = ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("invalid surface reads unreadable", func(t *testing.T) {
		f, cfg, frozen := setup(t)
		wrote, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		if err != nil {
			t.Fatal(err)
		}
		wrote.Surface = "cron"
		raw, err := json.Marshal(wrote)
		if err != nil {
			t.Fatal(err)
		}
		f.plantLastGood(t, raw)
		_, err = ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("tampered bytes read unreadable", func(t *testing.T) {
		_, cfg, frozen := setup(t)
		if _, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1")); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(frozen.Path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(frozen.Path, []byte("#!/bin/sh\necho tampered\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(frozen.Path, 0o555); err != nil {
			t.Fatal(err)
		}
		_, err := ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("bad state reads unreadable", func(t *testing.T) {
		f, cfg, _ := setup(t)
		frozen, discovery := probeBuild(t, f, "1.4.3-R5000")
		bad := ProbeObservation{
			AttemptID: "attempt-unparseable-1", SelectionID: discovery.SnapshotID,
			Outcome: ProbeExitOKUnparseable, Detail: "recorded", Cleanup: CleanupConfirmed,
		}
		if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, bad); err != nil {
			t.Fatal(err)
		}
		planted := LastGood{
			Schema: lastGoodSchema, Sequence: 1,
			Frozen:     FrozenReceipt{Build: frozen.Build, SHA256: frozen.SHA256, Path: frozen.Path},
			AttemptID:  "startup-planted",
			Surface:    SurfaceMSP,
			By:         "test-planter",
			ReadyAt:    "2026-10-10T09:30:00Z",
			AdmittedAt: "2026-10-10T09:00:00Z",
		}
		raw, err := json.Marshal(planted)
		if err != nil {
			t.Fatal(err)
		}
		f.plantLastGood(t, raw)
		_, err = ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("missing state reads unreadable", func(t *testing.T) {
		f, cfg, frozen := setup(t)
		if _, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1")); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(statePath(f.root, freezeKey(frozen.Build, frozen.SHA256))); err != nil {
			t.Fatal(err)
		}
		_, err := ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("zero sequence reads unreadable", func(t *testing.T) {
		f, cfg, frozen := setup(t)
		wrote, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		if err != nil {
			t.Fatal(err)
		}
		wrote.Sequence = 0
		raw, err := json.Marshal(wrote)
		if err != nil {
			t.Fatal(err)
		}
		f.plantLastGood(t, raw)
		_, err = ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})

	t.Run("linked receipt reads unreadable", func(t *testing.T) {
		f, cfg, frozen := setup(t)
		if _, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1")); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(f.root, lastGoodFileName)
		staged := filepath.Join(f.root, "staged-receipt.json")
		if err := os.Rename(target, staged); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(staged, target); err != nil {
			t.Fatal(err)
		}
		_, err := ReadLastGood(context.Background(), cfg)
		requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	})
}

// TestTransientStartFailurePreservesLastGood proves recording probe
// failures never modifies the durable receipt: the last-good bytes
// stay identical across every transient class, a deterministic BAD on
// another build, and a success recording elsewhere.
func TestTransientStartFailurePreservesLastGood(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	good := probeOKBuild(t, f, "1.4.3-R5000")
	seeded, err := WriteLastGood(context.Background(), cfg, genuineSuccess(good, "startup-seed"))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := os.ReadFile(filepath.Join(f.root, lastGoodFileName))
	if err != nil {
		t.Fatal(err)
	}
	requireSealed := func(stage string) {
		t.Helper()
		now, err := os.ReadFile(filepath.Join(f.root, lastGoodFileName))
		if err != nil {
			t.Fatal(err)
		}
		if string(now) != string(sealed) {
			t.Fatalf("%s modified last-good:\n got %s\nwant %s", stage, now, sealed)
		}
		current, err := ReadLastGood(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if current != seeded {
			t.Fatalf("%s moved last-good:\n got %+v\nwant %+v", stage, current, seeded)
		}
	}
	failing, failingDiscovery := probeBuild(t, f, "1.4.4-R5419.1")
	for _, outcome := range []ProbeOutcome{
		ProbeTimeout, ProbeExecFailed, ProbeExitNonzero, ProbeSignaled, ProbeOutputLimit, ProbeDrainFailed,
	} {
		obs := ProbeObservation{
			AttemptID: "attempt-" + string(outcome) + "-1", SelectionID: failingDiscovery.SnapshotID,
			Outcome: outcome, Detail: "observed " + string(outcome), Cleanup: CleanupIncomplete,
		}
		if _, err := RecordProbeOutcome(context.Background(), cfg, failing, obs); err != nil {
			t.Fatal(err)
		}
		requireSealed(string(outcome))
	}
	condemned, condemnedDiscovery := probeBuild(t, f, "1.4.5-R5500")
	bad := ProbeObservation{
		AttemptID: "attempt-unparseable-1", SelectionID: condemnedDiscovery.SnapshotID,
		Outcome: ProbeExitOKUnparseable, Detail: "recorded contradiction", Cleanup: CleanupConfirmed,
	}
	if _, err := RecordProbeOutcome(context.Background(), cfg, condemned, bad); err != nil {
		t.Fatal(err)
	}
	requireSealed("deterministic-bad")
	other, otherDiscovery := probeBuild(t, f, "1.4.6-R5600")
	if _, err := RecordProbeOutcome(context.Background(), cfg, other, successObservation("attempt-success-1", otherDiscovery.SnapshotID)); err != nil {
		t.Fatal(err)
	}
	requireSealed("elsewhere-success")
}

// TestLastGoodWriteReadRoundTrip proves sequential admissions order by
// durable sequence: each success allocates the next number with the
// configured clock's admission timestamp, and the latest receipt reads
// back exactly.
func TestLastGoodWriteReadRoundTrip(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
	first, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || first.AdmittedAt != "2026-10-10T09:00:00Z" {
		t.Fatalf("first = %+v, want sequence 1 admitted at the configured clock", first)
	}
	second := genuineSuccess(frozen, "startup-2")
	second.ReadyAt = "2026-10-10T10:30:00Z"
	admitted, err := WriteLastGood(context.Background(), cfg, second)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Sequence != 2 || admitted.AttemptID != "startup-2" || admitted.ReadyAt != "2026-10-10T10:30:00Z" {
		t.Fatalf("second = %+v, want sequence 2 with its own readiness", admitted)
	}
	latest, err := ReadLastGood(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if latest != admitted {
		t.Fatalf("read %+v, wrote %+v", latest, admitted)
	}
}

// TestLastGoodUnverifiedRefuses proves admission verifies the bytes it
// would attest: builds without PROBE_OK state and builds whose frozen
// bytes disagree refuse, and no receipt is published.
func TestLastGoodUnverifiedRefuses(t *testing.T) {
	t.Run("copied bytes refuse", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, _ := probeBuild(t, f, "1.4.4-R5419.1")
		_, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		requireLastGoodCode(t, err, CodeLastGoodUnverified, StageLastGood)
		if _, err := os.Stat(filepath.Join(f.root, lastGoodFileName)); !os.IsNotExist(err) {
			t.Fatalf("unverified admission published a receipt: %v", err)
		}
	})

	t.Run("bad bytes refuse", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen, discovery := probeBuild(t, f, "1.4.4-R5419.1")
		bad := ProbeObservation{
			AttemptID: "attempt-unparseable-1", SelectionID: discovery.SnapshotID,
			Outcome: ProbeExitOKUnparseable, Detail: "recorded", Cleanup: CleanupConfirmed,
		}
		if _, err := RecordProbeOutcome(context.Background(), cfg, frozen, bad); err != nil {
			t.Fatal(err)
		}
		_, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		requireLastGoodCode(t, err, CodeLastGoodUnverified, StageLastGood)
	})

	t.Run("disagreeing bytes refuse", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		if err := os.Chmod(frozen.Path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(frozen.Path, []byte("#!/bin/sh\necho tampered\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(frozen.Path, 0o555); err != nil {
			t.Fatal(err)
		}
		_, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		requireLastGoodCode(t, err, CodeLastGoodUnverified, StageLastGood)
	})

	t.Run("missing state refuses", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		if err := os.Remove(statePath(f.root, freezeKey(frozen.Build, frozen.SHA256))); err != nil {
			t.Fatal(err)
		}
		_, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-1"))
		requireLastGoodCode(t, err, CodeLastGoodUnverified, StageLastGood)
	})
}

// TestLastGoodInvalidSuccessRefuses proves malformed success values
// refuse before any store work: unusable frozen references, bad
// attempt tokens, unknown surfaces and empty admitters.
func TestLastGoodInvalidSuccessRefuses(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	digest := strings.Repeat("ab", 32)
	build := "1.4.4-R5419.1"
	id := FrozenIdentity{Build: build, SHA256: digest, Path: frozenBinaryPath(f.root, freezeKey(build, digest), build)}
	valid := StartSuccess{
		Frozen: id, AttemptID: "startup-1", Surface: SurfaceMSP,
		ReadyAt: "2026-10-10T09:30:00Z", By: "test-msp",
	}
	cells := []struct {
		name string
		mut  func(*StartSuccess)
	}{
		{"unparseable build", func(s *StartSuccess) { s.Frozen.Build = "tomorrow" }},
		{"malformed digest", func(s *StartSuccess) { s.Frozen.SHA256 = "xyz" }},
		{"relative path", func(s *StartSuccess) { s.Frozen.Path = "muse-bin-1.4.4-R5419.1" }},
		{"disagreeing path", func(s *StartSuccess) { s.Frozen.Path = filepath.Join(f.root, "elsewhere") }},
		{"empty attempt", func(s *StartSuccess) { s.AttemptID = "" }},
		{"slashed attempt", func(s *StartSuccess) { s.AttemptID = "a/b" }},
		{"unknown surface", func(s *StartSuccess) { s.Surface = "cron" }},
		{"empty surface", func(s *StartSuccess) { s.Surface = "" }},
		{"empty admitter", func(s *StartSuccess) { s.By = "" }},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			next := valid
			cell.mut(&next)
			_, err := WriteLastGood(context.Background(), cfg, next)
			requireLastGoodCode(t, err, CodeLastGoodInvalid, StageLastGood)
		})
	}
	if _, err := os.Stat(filepath.Join(f.root, lastGoodFileName)); !os.IsNotExist(err) {
		t.Fatalf("invalid successes published a receipt: %v", err)
	}
}

// TestLastGoodHonorsCancel proves a canceled context refuses before
// any store work: no receipt is read and none is published.
func TestLastGoodHonorsCancel(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	digest := strings.Repeat("ab", 32)
	build := "1.4.4-R5419.1"
	id := FrozenIdentity{Build: build, SHA256: digest, Path: frozenBinaryPath(f.root, freezeKey(build, digest), build)}
	success := StartSuccess{
		Frozen: id, AttemptID: "startup-1", Surface: SurfaceMSP,
		ReadyAt: "2026-10-10T09:30:00Z", By: "test-msp",
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := WriteLastGood(ctx, cfg, success)
	requireLastGoodCode(t, err, CodeLastGoodCanceled, StageLastGood)
	_, err = ReadLastGood(ctx, cfg)
	requireLastGoodCode(t, err, CodeLastGoodCanceled, StageLastGood)
	if _, err := os.Stat(filepath.Join(f.root, lastGoodFileName)); !os.IsNotExist(err) {
		t.Fatalf("canceled admission published a receipt: %v", err)
	}
}

// TestLastGoodOversizedByRefuses proves an oversized admitting path
// refuses before publication: the complete serialized receipt,
// including JSON escaping and the trailing newline, is bounded, and
// the prior receipt is preserved.
func TestLastGoodOversizedByRefuses(t *testing.T) {
	t.Run("plain oversize preserves prior", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		seeded, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-seed"))
		if err != nil {
			t.Fatal(err)
		}
		sealed, err := os.ReadFile(filepath.Join(f.root, lastGoodFileName))
		if err != nil {
			t.Fatal(err)
		}
		big := genuineSuccess(frozen, "startup-big")
		big.By = strings.Repeat("a", 70*1024)
		_, err = WriteLastGood(context.Background(), cfg, big)
		requireLastGoodCode(t, err, CodeLastGoodInvalid, StageLastGood)
		now, err := os.ReadFile(filepath.Join(f.root, lastGoodFileName))
		if err != nil {
			t.Fatal(err)
		}
		if string(now) != string(sealed) {
			t.Fatalf("oversized admission modified last-good:\n got %d bytes\nwant %d bytes", len(now), len(sealed))
		}
		current, err := ReadLastGood(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if current != seeded {
			t.Fatalf("oversized admission moved last-good:\n got %+v\nwant %+v", current, seeded)
		}
	})

	t.Run("escaping counts toward the bound", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		quoted := genuineSuccess(frozen, "startup-quoted")
		quoted.By = strings.Repeat("\"", 40*1024)
		_, err := WriteLastGood(context.Background(), cfg, quoted)
		requireLastGoodCode(t, err, CodeLastGoodInvalid, StageLastGood)
		if _, err := os.Stat(filepath.Join(f.root, lastGoodFileName)); !os.IsNotExist(err) {
			t.Fatalf("oversized admission published a receipt: %v", err)
		}
	})

	t.Run("oversize without prior publishes nothing", func(t *testing.T) {
		f := newFreeze(t)
		cfg := f.config()
		frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
		big := genuineSuccess(frozen, "startup-big")
		big.By = strings.Repeat("b", 70*1024)
		_, err := WriteLastGood(context.Background(), cfg, big)
		requireLastGoodCode(t, err, CodeLastGoodInvalid, StageLastGood)
		if _, err := os.Stat(filepath.Join(f.root, lastGoodFileName)); !os.IsNotExist(err) {
			t.Fatalf("oversized admission published a receipt: %v", err)
		}
	})
}

// TestLastGoodInconsistentExistingRefusesAdmission proves admission
// validates the existing receipt's exact path/build/hash binding
// before allocating a sequence: an inconsistent durable tuple refuses
// as unreadable and is preserved, never silently replaced.
func TestLastGoodInconsistentExistingRefusesAdmission(t *testing.T) {
	f := newFreeze(t)
	cfg := f.config()
	frozen := probeOKBuild(t, f, "1.4.4-R5419.1")
	seeded, err := WriteLastGood(context.Background(), cfg, genuineSuccess(frozen, "startup-seed"))
	if err != nil {
		t.Fatal(err)
	}
	_ = seeded
	raw, err := os.ReadFile(filepath.Join(f.root, lastGoodFileName))
	if err != nil {
		t.Fatal(err)
	}
	var planted LastGood
	if err := json.Unmarshal(raw, &planted); err != nil {
		t.Fatal(err)
	}
	other := "0" + planted.Frozen.SHA256[1:]
	if other == planted.Frozen.SHA256 {
		other = "1" + planted.Frozen.SHA256[1:]
	}
	planted.Frozen.SHA256 = other
	poisoned, err := json.Marshal(planted)
	if err != nil {
		t.Fatal(err)
	}
	f.plantLastGood(t, poisoned)
	sealed, err := os.ReadFile(filepath.Join(f.root, lastGoodFileName))
	if err != nil {
		t.Fatal(err)
	}
	next := genuineSuccess(frozen, "startup-next")
	_, err = WriteLastGood(context.Background(), cfg, next)
	requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
	now, err := os.ReadFile(filepath.Join(f.root, lastGoodFileName))
	if err != nil {
		t.Fatal(err)
	}
	if string(now) != string(sealed) {
		t.Fatalf("admission over inconsistency modified last-good:\n got %s\nwant %s", now, sealed)
	}
	_, err = ReadLastGood(context.Background(), cfg)
	requireLastGoodCode(t, err, CodeLastGoodUnreadable, StageLastGood)
}
