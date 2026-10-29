package musepty

// L3P PTY integration (regression; design regression §8.1
// row L3P): the imported plan selection binding, both exec entries
// verifying the frozen tuple (launch AND resume), the exact resume
// identity, and host-env isolation (M34, M60, M62), plus the PTY leg
// of the fallback warning triple (M50). Production entries:
// BuildMusePTYLaunchPlan, BuildMusePTYLaunchPlanFromImport,
// StartMusePTYHost, ResumeMusePTYHost, StartMusePTYHostViaBuilds and
// ResumeMusePTYHostViaBuilds. Fixtures are synthesized installs
// (test-binary peer copies under versioned names) with real
// selection, real freeze and real first probes; the serve legs
// execute the frozen peers through PTY startup. R198-clean:
// scripted peers with pipe handshakes, no sleeps.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	"github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
	musebuilds "github.com/relux-works/swarma-session-host/musebuilds"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Install builds used across the L3P tests. Numeric ordering the
// installs rely on: 9.9.9 > 1.4.2 > 1.4.1.
const (
	museL3PBuild141 = "1.4.1-R4503.1"
	museL3PBuild142 = "1.4.2-R4684.1"
	museL3PBuild999 = "9.9.9-R0.1"
)

// museL3PFixedNow is the frozen-store clock for every L3P fixture:
// the ledger day, ramp freshness and receipt timestamps stay
// deterministic while the PTY legs run on the real clock.
var museL3PFixedNow = time.Date(2026, time.October, 11, 9, 0, 0, 0, time.UTC)

// museL3PFakeLock is a trivial HostLock: the tests never contend, so
// acquisition always succeeds and records the hold for assertions.
type museL3PFakeLock struct {
	mu       sync.Mutex
	acquires int
	releases int
}

type museL3PFakeHeld struct {
	lock     *museL3PFakeLock
	released bool
}

func (l *museL3PFakeLock) Acquire(ctx context.Context) (musebuilds.HostLockHeld, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acquires++
	return &museL3PFakeHeld{lock: l}, nil
}

func (h *museL3PFakeHeld) Check() error {
	if h == nil || h.released {
		return errors.New("lock not held")
	}
	return nil
}

func (h *museL3PFakeHeld) Release() error {
	if h == nil || h.released || h.lock == nil {
		return nil
	}
	h.released = true
	h.lock.mu.Lock()
	defer h.lock.mu.Unlock()
	h.lock.releases++
	return nil
}

// museL3PFixture is one frozen store plus the shared peer bytes every
// install in the test copies. Installs are per-launch; the store,
// ledger, ramp and lock are shared across the launches of one
// subtest like one host account.
type museL3PFixture struct {
	t         *testing.T
	root      string
	frozen    string
	ledger    string
	rampFile  string
	rampLog   string
	now       time.Time
	maxPerDay int
	lock      *museL3PFakeLock
	peerBytes []byte
	sha       string
}

func newMuseL3PFixture(t *testing.T, maxPerDay int) *museL3PFixture {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	peerBytes, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// The frozen key directories publish as 0555, so TempDir's
	// RemoveAll cannot unlink their entries. Relax the whole
	// fixture tree for removal only; this cleanup runs before
	// TempDir's because it registers after.
	t.Cleanup(func() {
		_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			_ = os.Chmod(path, 0o700)
			return nil
		})
	})
	ledger := filepath.Join(root, "ledger")
	if err := os.MkdirAll(ledger, 0o700); err != nil {
		t.Fatal(err)
	}
	now := museL3PFixedNow
	rampFile := filepath.Join(root, "ramp.state")
	rampLog := filepath.Join(root, "ramp.log")
	if err := os.WriteFile(rampFile, []byte("ramp_down=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rampLog, []byte(now.UTC().Format(time.RFC3339)+"\tcount=41\tprobe_s=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{rampFile, rampLog} {
		if err := os.Chtimes(path, now, now); err != nil {
			t.Fatal(err)
		}
	}
	digest := sha256.Sum256(peerBytes)
	return &museL3PFixture{
		t: t, root: root, frozen: filepath.Join(root, "frozen"), ledger: ledger,
		rampFile: rampFile, rampLog: rampLog, now: now, maxPerDay: maxPerDay,
		lock: &museL3PFakeLock{}, peerBytes: peerBytes, sha: hex.EncodeToString(digest[:]),
	}
}

func (f *museL3PFixture) hostConfig() musebuilds.HostConfig {
	return musebuilds.HostConfig{
		FrozenRoot: f.frozen, LedgerDir: f.ledger,
		RampStatePath: f.rampFile, RampLogPath: f.rampLog,
		Locker: f.lock, Host: "test-host", User: "test-user",
		MaxFreezesPerDay: f.maxPerDay,
		Now:              func() time.Time { return f.now },
		Quarantine:       func(string) (bool, error) { return false, nil },
	}
}

// install writes one launcher install: an inert launcher script, the
// sidecar naming newest, and the peer bytes under each versioned
// candidate name. The first build must be the numeric maximum;
// selection proves it. Copies are real files (never hardlinks):
// discovery refuses hard-link aliases.
func (f *museL3PFixture) install(newest string, others ...string) (string, []string) {
	f.t.Helper()
	dir, err := filepath.EvalSymlinks(f.t.TempDir())
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "muse"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".muse-version"), []byte(newest+"\n"), 0o644); err != nil {
		f.t.Fatal(err)
	}
	for _, build := range append([]string{newest}, others...) {
		f.writeExecutable(filepath.Join(dir, "muse-bin-"+build))
	}
	return dir, []string{"PATH=" + dir}
}

// writeExecutable copies the shared peer bytes to path with the
// temp, fsync, close, rename discipline, so a reader never sees a
// partial executable.
func (f *museL3PFixture) writeExecutable(path string) {
	f.t.Helper()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".exec-")
	if err != nil {
		f.t.Fatal(err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(f.peerBytes); err != nil {
		tmp.Close()
		f.t.Fatal(err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		f.t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chmod(tmpName, 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		f.t.Fatal(err)
	}
}

// binding assembles one invocation binding: the production probe
// runner (real freeze and real first probes of the frozen peers),
// the install PATH, and the caller-supplied mode and attempt.
func (f *museL3PFixture) binding(env []string, mode musebuilds.RecoveryMode, attempt string) *MusePTYBuildsBinding {
	return &MusePTYBuildsBinding{
		Builds: f.hostConfig(), Runner: musebuilds.NewProbeRunner(nil),
		Env: env, Mode: mode, AttemptID: attempt,
	}
}

// prepare runs real selection, freeze and first probe for one
// install and returns the prepared newest tuple. Every L3P frozen
// plan binds a tuple prepared this way; no store is hand-built.
func (f *museL3PFixture) prepare(env []string, route musebuilds.LaunchRoute, attempt string) musebuilds.FrozenIdentity {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prep, err := musebuilds.PrepareNewestLaunch(ctx, f.hostConfig(), musebuilds.NewProbeRunner(nil), musebuilds.LaunchRequest{
		Route: route, Surface: musebuilds.SurfacePTY, Mode: musebuilds.RecoveryUnattended,
		Env: env, AttemptID: attempt,
	})
	if err != nil {
		f.t.Fatalf("prepare newest: %v", err)
	}
	if prep.Outcome() != musebuilds.OutcomeRunNewest {
		f.t.Fatalf("prepare outcome = %q, want run-newest", string(prep.Outcome()))
	}
	return prep.Newest()
}

// museL3PPeerHome creates one scripted-peer home running mode:
// peer-mode plus the frame FIFO the peer frame-handshakes.
func museL3PPeerHome(t *testing.T, mode string) string {
	t.Helper()
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, "peer-mode"), []byte(mode), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(home, musePeerFrameFIFO), 0o600); err != nil {
		t.Fatal(err)
	}
	return home
}

// museL3PReq composes one frozen request for the tuple: trust plus
// the tuple members, an optional resume selection, and plan-owned
// HOME/PATH/data-home. dataHome empty defaults to home/data; resume
// legs share the seed data home while keeping a private peer-mode
// HOME. Home is the peer home: the module's sealed-import shape
// requires an absolute Home, and Muse declares no DefaultHome, so a
// request without it seals a plan no import accepts.
func museL3PReq(home, dataHome string, tuple musebuilds.FrozenIdentity, resume string) agentic.LaunchRequest {
	if dataHome == "" {
		dataHome = filepath.Join(home, "data")
	}
	req := agentic.LaunchRequest{
		System: "muse", Model: agentic.Model{ID: "echo"}, WorkDir: home, Home: home,
		Env:              []string{"HOME=" + home, "PATH=" + home, "XDG_DATA_HOME=" + dataHome, "TERM=xterm-256color"},
		NativeArgs:       []string{"--trust-workspace"},
		FrozenToolBinary: tuple.Path, FrozenToolBuild: tuple.Build, FrozenToolSHA256: tuple.SHA256,
	}
	if resume != "" {
		req.NativeArgs = append(req.NativeArgs, "resume", resume)
	}
	return req
}

// museL3PComposedPlan composes a frozen fresh plan for the tuple and
// binds the fixture store for the pre-exec gate.
func museL3PComposedPlan(t *testing.T, f *museL3PFixture, home, dataHome string, tuple musebuilds.FrozenIdentity, resume string) *MusePTYLaunchPlan {
	t.Helper()
	plan, err := BuildMusePTYLaunchPlan(museL3PReq(home, dataHome, tuple, resume))
	if err != nil {
		t.Fatal(err)
	}
	plan.frozenStore = f.hostConfig()
	return plan
}

// museL3PImportedPlan composes a frozen resume plan for the tuple,
// exports its module seal, and verified-imports it: the
// module-produced seal the resume entry consumes.
func museL3PImportedPlan(t *testing.T, f *museL3PFixture, home, dataHome string, tuple musebuilds.FrozenIdentity, sessionID string) *MusePTYLaunchPlan {
	t.Helper()
	composed, err := BuildMusePTYLaunchPlan(museL3PReq(home, dataHome, tuple, sessionID))
	if err != nil {
		t.Fatal(err)
	}
	seal, err := composed.plan.ExportSeal()
	if err != nil {
		t.Fatal(err)
	}
	imported, err := BuildMusePTYLaunchPlanFromImport(seal, composed.plan, tuple)
	if err != nil {
		t.Fatal(err)
	}
	imported.frozenStore = f.hostConfig()
	return imported
}

// museL3PStart starts one plan through the production fresh entry
// and stops the host on cleanup.
func museL3PStart(t *testing.T, plan *MusePTYLaunchPlan) *musePTYHost {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	h, err := StartMusePTYHost(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })
	return h
}

func museL3PStop(t *testing.T, host *musePTYHost) {
	t.Helper()
	if host == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := host.Stop(ctx); err != nil {
		t.Fatalf("stop PTY host: %v", err)
	}
}

// museL3PSeedSession starts one fresh session, snapshots it, stops it
// and closes its epoch with an end record, returning the durable
// record plus the native session identity.
func museL3PSeedSession(t *testing.T, plan *MusePTYLaunchPlan) (sm.ProviderHostRecord, string) {
	t.Helper()
	h := museL3PStart(t, plan)
	snapshot, err := museTestSnapshot(h)
	if err != nil {
		t.Fatal(err)
	}
	id := h.NativeIdentity()
	logPath := h.snapshot.LogPath
	museL3PStop(t, h)
	log, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	endRecord := museOwnedRecord(h, museFixtureRecord("session.end"))
	endBytes, err := json.Marshal(endRecord)
	if err != nil {
		t.Fatal(err)
	}
	_, err = log.Write(append(endBytes, '\n'))
	_ = log.Close()
	if err != nil {
		t.Fatal(err)
	}
	return sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(snapshot)}, id
}

// museL3PCorruptFrozen flips the first frozen byte: the filename
// still parses and the tuple still names the path, but the bytes no
// longer hash to the tuple SHA-256.
func museL3PCorruptFrozen(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		t.Fatal("frozen binary is empty")
	}
	b[0] ^= 0xff
	// Linux refuses open-for-write on a running executable with ETXTBSY, so swap the bytes via rename.
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Single deferred restore: t.Fatal runs defers via Goexit, so every path below restores the dir.
	defer func() { _ = os.Chmod(dir, 0o555) }()
	// CreateTemp yields a 0600 sibling; the rename carries that mode and the frozen-copy mode is restored after.
	tmp, err := os.CreateTemp(dir, ".corrupt-")
	if err != nil {
		t.Fatal(err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o555); err != nil {
		t.Fatal(err)
	}
}

// requireMuseL3PExecCode asserts a typed L3P refusal with the exact
// code, failing on admission or on an untyped refusal.
func requireMuseL3PExecCode(t *testing.T, err error, wantCode string) *MusePTYExecError {
	t.Helper()
	if err == nil {
		t.Fatalf("admitted, want typed refusal %q", wantCode)
	}
	refusal, ok := AsMusePTYExecError(err)
	if !ok {
		t.Fatalf("untyped refusal %v, want %q", err, wantCode)
	}
	if refusal.Code != wantCode {
		t.Fatalf("refusal code = %q, want %q (%v)", refusal.Code, wantCode, err)
	}
	return refusal
}

// requireMuseL3PReceipt asserts the last-good receipt names the
// expected build at the expected sequence.
func requireMuseL3PReceipt(t *testing.T, f *museL3PFixture, build string, sequence uint64) musebuilds.LastGood {
	t.Helper()
	receipt, err := musebuilds.ReadLastGood(context.Background(), f.hostConfig())
	if err != nil {
		t.Fatalf("read last-good receipt: %v", err)
	}
	if receipt.Frozen.Build != build || receipt.Sequence != sequence {
		t.Fatalf("last-good = %s seq %d, want %s seq %d", receipt.Frozen.Build, receipt.Sequence, build, sequence)
	}
	return receipt
}

// requireMuseL3PNoReceipt asserts the last-good store holds no
// receipt: nothing was ever admitted.
func requireMuseL3PNoReceipt(t *testing.T, f *museL3PFixture) {
	t.Helper()
	if _, err := musebuilds.ReadLastGood(context.Background(), f.hostConfig()); err == nil {
		t.Fatalf("last-good receipt exists, want none admitted")
	}
}

// requireMuseL3PNoAttemptReceipt asserts no attempt receipt was
// recorded for the attempt: a pre-attempt refusal reports nothing.
func requireMuseL3PNoAttemptReceipt(t *testing.T, f *museL3PFixture, tuple musebuilds.FrozenIdentity, attempt string) {
	t.Helper()
	if _, err := musebuilds.ReadAttemptReceipt(context.Background(), f.hostConfig(), tuple, attempt); err == nil {
		t.Fatalf("attempt receipt exists for %q, want none recorded", attempt)
	}
}

// requireMuseL3PNoBad asserts the named tuple carries no BAD verdict.
func requireMuseL3PNoBad(t *testing.T, f *museL3PFixture, tuple musebuilds.FrozenIdentity) {
	t.Helper()
	verdict, present, err := musebuilds.BadRecord(context.Background(), f.hostConfig(), tuple)
	if err != nil {
		t.Fatalf("read BAD record for %s: %v", tuple.Build, err)
	}
	if present {
		t.Fatalf("BAD verdict exists for %s, want none (verdict %+v)", tuple.Build, verdict)
	}
}

func captureMusePTYRecoveryWarning(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := musePTYRecoveryWarningWriter
	musePTYRecoveryWarningWriter = &buf
	t.Cleanup(func() { musePTYRecoveryWarningWriter = previous })
	return &buf
}

// TestPTYSealVerifiedEveryLeg (M34) proves both PTY exec entries run
// the pre-exec gate — frozen bytes plus the module seal — after
// composition/import and before the child starts: a launch and a
// resume with swapped frozen bytes both refuse typed, a frozen plan
// without its store refuses on both legs, and a caller-supplied
// tuple that is not the selected newest refuses before any child
// starts, admitting and recording nothing. Intact frozen plans
// start on both legs. Production entries: StartMusePTYHost,
// ResumeMusePTYHost and StartMusePTYHostViaBuilds.
func TestPTYSealVerifiedEveryLeg(t *testing.T) {
	t.Run("launch_admits_intact_frozen", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m34-launch-ok")
		home := museL3PPeerHome(t, "idle")
		h := museL3PStart(t, museL3PComposedPlan(t, f, home, "", tuple, ""))
		if got := h.ProviderLaunchMetadata().Executable; got != tuple.Path {
			t.Fatalf("executable = %q, want the frozen %q", got, tuple.Path)
		}
	})
	t.Run("resume_admits_intact_imported", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m34-resume-ok")
		seedHome := museL3PPeerHome(t, "idle")
		record, id := museL3PSeedSession(t, museL3PComposedPlan(t, f, seedHome, "", tuple, ""))
		resumeHome := museL3PPeerHome(t, "idle")
		dataHome := filepath.Join(seedHome, "data")
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		resumed, err := ResumeMusePTYHost(ctx, museL3PImportedPlan(t, f, resumeHome, dataHome, tuple, id), record)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resumed.Stop(context.Background()) })
		if resumed.NativeIdentity() != id {
			t.Fatal("resume changed identity")
		}
	})
	t.Run("launch_refuses_swapped_bytes", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m34-launch-swap")
		home := museL3PPeerHome(t, "idle")
		plan := museL3PComposedPlan(t, f, home, "", tuple, "")
		museL3PCorruptFrozen(t, tuple.Path)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		h, err := StartMusePTYHost(ctx, plan)
		if h != nil {
			_ = h.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeFrozenVerifyFailed)
	})
	t.Run("resume_refuses_swapped_bytes", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m34-resume-swap")
		seedHome := museL3PPeerHome(t, "idle")
		record, id := museL3PSeedSession(t, museL3PComposedPlan(t, f, seedHome, "", tuple, ""))
		resumeHome := museL3PPeerHome(t, "idle")
		dataHome := filepath.Join(seedHome, "data")
		plan := museL3PImportedPlan(t, f, resumeHome, dataHome, tuple, id)
		museL3PCorruptFrozen(t, tuple.Path)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		h, err := ResumeMusePTYHost(ctx, plan, record)
		if h != nil {
			_ = h.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeFrozenVerifyFailed)
	})
	t.Run("launch_refuses_missing_store", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m34-launch-nostore")
		home := museL3PPeerHome(t, "idle")
		plan, err := BuildMusePTYLaunchPlan(museL3PReq(home, "", tuple, ""))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		h, err := StartMusePTYHost(ctx, plan)
		if h != nil {
			_ = h.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeStoreMissing)
	})
	t.Run("resume_refuses_missing_store", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m34-resume-nostore")
		seedHome := museL3PPeerHome(t, "idle")
		record, id := museL3PSeedSession(t, museL3PComposedPlan(t, f, seedHome, "", tuple, ""))
		resumeHome := museL3PPeerHome(t, "idle")
		composed, err := BuildMusePTYLaunchPlan(museL3PReq(resumeHome, filepath.Join(seedHome, "data"), tuple, id))
		if err != nil {
			t.Fatal(err)
		}
		seal, err := composed.plan.ExportSeal()
		if err != nil {
			t.Fatal(err)
		}
		plan, err := BuildMusePTYLaunchPlanFromImport(seal, composed.plan, tuple)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		h, err := ResumeMusePTYHost(ctx, plan, record)
		if h != nil {
			_ = h.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeStoreMissing)
	})
	t.Run("fresh_flow_refuses_caller_tuple_mismatch", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		binding := f.binding(env, musebuilds.RecoveryUnattended, "l3p-m34-caller-tuple")
		home := museL3PPeerHome(t, "idle")
		selected := f.prepare(env, musebuilds.RouteLaunch, "l3p-m34-caller-prepare")
		req := museL3PReq(home, "", selected, "")
		// The caller asserts a tuple that is not the selected
		// newest: the flow must refuse it, never select past it.
		// The 9.9.9 build is the m34_fresh sentinel: the narrowing
		// row admits exactly this member.
		req.FrozenToolBinary, req.FrozenToolBuild, req.FrozenToolSHA256 =
			"/nonexistent-muse-stale-newest", museL3PBuild999, strings.Repeat("0", 64)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		host, err := StartMusePTYHostViaBuilds(ctx, binding, req)
		if host != nil {
			_ = host.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeInvalidConfig)
		requireMuseL3PNoReceipt(t, f)
		requireMuseL3PNoAttemptReceipt(t, f, selected, "l3p-m34-caller-tuple")
	})
	t.Run("resume_flow_refuses_invalid_record", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		binding := f.binding(env, musebuilds.RecoveryUnattended, "l3p-m34-bad-record")
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		host, err := ResumeMusePTYHostViaBuilds(ctx, binding, agentic.Seal{}, agentic.Plan{},
			sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(json.RawMessage(`{"contract":"bogus"}`))})
		if host != nil {
			_ = host.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeInvalidConfig)
		requireMuseL3PNoReceipt(t, f)
	})
}

// museL3PResumeProcess returns one well-formed final process for
// import tests: the composed frozen resume plan's own process.
func museL3PResumeProcess(t *testing.T, f *museL3PFixture, home, dataHome string, tuple musebuilds.FrozenIdentity, sessionID string) agentic.Plan {
	t.Helper()
	composed, err := BuildMusePTYLaunchPlan(museL3PReq(home, dataHome, tuple, sessionID))
	if err != nil {
		t.Fatal(err)
	}
	return composed.plan
}

// TestSealedImportRefusesSelfMintedEvidence (M60) proves the resume
// leg authenticates its evidence through the verified import: a
// self-minted seal, an unsealed guard, a foreign-system seal and a
// non-interactive process all refuse at the constructor, a stale
// seal bound to another selection refuses for outer recomposition,
// and — the resume-side enforcement — a frozen resume plan the host
// composed instead of importing refuses at the resume entry.
// Production entries: BuildMusePTYLaunchPlanFromImport and
// ResumeMusePTYHost.
func TestSealedImportRefusesSelfMintedEvidence(t *testing.T) {
	t.Run("self_minted_seal_refuses", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m60-minted")
		home := museL3PPeerHome(t, "idle")
		sessionID := museTestUUID("m60-minted-session")
		process := museL3PResumeProcess(t, f, home, "", tuple, sessionID)
		// Hand-written evidence: the envelope shape is right but no
		// module minted its integrity digest.
		minted := agentic.Seal{
			Schema: agentic.ExecGuardSchema, SchemaVersion: agentic.ExecGuardVersion,
			Data: agentic.SealData{Kind: agentic.SealKindSealed, Sealed: &agentic.SealedData{
				System: "muse", Binary: tuple.Path, Argv: process.Argv,
				Selectors: map[string]string{"tool-release": tuple.Build},
				Digest:    "self-minted-digest",
			}},
		}
		_, err := BuildMusePTYLaunchPlanFromImport(minted, process, tuple)
		requireMuseL3PExecCode(t, err, musePTYCodeImportRefused)
	})
	t.Run("unsealed_guard_refuses", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m60-unsealed")
		home := museL3PPeerHome(t, "idle")
		sessionID := museTestUUID("m60-unsealed-session")
		process := museL3PResumeProcess(t, f, home, "", tuple, sessionID)
		_, err := BuildMusePTYLaunchPlanFromImport(agentic.Seal{
			Schema: agentic.ExecGuardSchema, SchemaVersion: agentic.ExecGuardVersion,
			Data: agentic.SealData{Kind: agentic.SealKindUnsealed},
		}, process, tuple)
		requireMuseL3PExecCode(t, err, musePTYCodeImportRefused)
	})
	t.Run("foreign_system_seal_refuses", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m60-foreign")
		home := museL3PPeerHome(t, "idle")
		sessionID := museTestUUID("m60-foreign-session")
		process := museL3PResumeProcess(t, f, home, "", tuple, sessionID)
		// Foreign evidence never imports: the integrity gate fires
		// before any system dispatch (the module's documented
		// order), so a hand-minted foreign seal refuses tampered.
		foreign := agentic.Seal{
			Schema: agentic.ExecGuardSchema, SchemaVersion: agentic.ExecGuardVersion,
			Data: agentic.SealData{Kind: agentic.SealKindSealed, Sealed: &agentic.SealedData{
				System: "codex", Binary: tuple.Path, Argv: process.Argv,
				Artifacts: []agentic.SealedArtifact{},
				Selectors: map[string]string{"tool-release": tuple.Build},
				Digest:    "foreign-digest",
			}},
		}
		_, err := BuildMusePTYLaunchPlanFromImport(foreign, process, tuple)
		requireMuseL3PExecCode(t, err, musePTYCodeImportRefused)
		if !errors.Is(err, agentic.ErrSealTampered) {
			t.Fatalf("foreign refusal = %v, want the integrity gate in its chain", err)
		}
	})
	t.Run("non_interactive_mode_refuses", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m60-mode")
		home := museL3PPeerHome(t, "idle")
		sessionID := museTestUUID("m60-mode-session")
		composed, err := BuildMusePTYLaunchPlan(museL3PReq(home, "", tuple, sessionID))
		if err != nil {
			t.Fatal(err)
		}
		seal, err := composed.plan.ExportSeal()
		if err != nil {
			t.Fatal(err)
		}
		process := composed.plan
		process.Mode = agentic.LaunchModeExec
		_, err = BuildMusePTYLaunchPlanFromImport(seal, process, tuple)
		requireMuseL3PExecCode(t, err, musePTYCodeModeRefused)
	})
	t.Run("stale_seal_refuses_for_recomposition", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m60-stale")
		home := museL3PPeerHome(t, "idle")
		sessionID := museTestUUID("m60-stale-session")
		composed, err := BuildMusePTYLaunchPlan(museL3PReq(home, "", tuple, sessionID))
		if err != nil {
			t.Fatal(err)
		}
		seal, err := composed.plan.ExportSeal()
		if err != nil {
			t.Fatal(err)
		}
		// An authentic seal bound to another selection's path: the
		// same build under another store, so only the binding
		// differs, never the seal's own validity.
		other := musebuilds.FrozenIdentity{Build: tuple.Build, SHA256: strings.Repeat("1", 64),
			Path: filepath.Join(string(filepath.Separator), "other-store", "builds",
				tuple.Build+"-"+strings.Repeat("1", 64), "muse-bin-"+tuple.Build)}
		_, err = BuildMusePTYLaunchPlanFromImport(seal, composed.plan, other)
		requireMuseL3PExecCode(t, err, musePTYCodeSealedStale)
	})
	t.Run("frozen_resume_refuses_composed_plan", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m60-provenance")
		seedHome := museL3PPeerHome(t, "idle")
		record, id := museL3PSeedSession(t, museL3PComposedPlan(t, f, seedHome, "", tuple, ""))
		resumeHome := museL3PPeerHome(t, "idle")
		// Composed (not imported): host-side recomposition is
		// unauthenticated evidence on the frozen resume leg. The
		// 1.4.1 build is the m60_provenance sentinel: the narrowing
		// row admits exactly this member.
		plan := museL3PComposedPlan(t, f, resumeHome, filepath.Join(seedHome, "data"), tuple, id)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		h, err := ResumeMusePTYHost(ctx, plan, record)
		if h != nil {
			_ = h.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeResumeNotImported)
	})
}

// TestSealedImportStaleHelpDigestRefuses (M60) proves both imported
// legs re-verify the sealed help evidence immediately before exec: a
// yolo plan sealed against one help text refuses when the binary's
// help no longer declares the sealed option, while an unchanged help
// starts. The direct imported-launch leg (L3P r2, finding 4) runs the
// same refusal through the fresh entry with unchanged-help and
// stale-help controls. The frozen bytes are intact throughout, so
// only the seal re-verification can refuse. Production entries:
// ResumeMusePTYHost and StartMusePTYHost with verified-import plans.
func TestSealedImportStaleHelpDigestRefuses(t *testing.T) {
	resumeYolo := func(t *testing.T, rewriteHelp bool) (*musePTYHost, error) {
		t.Helper()
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m60-help")
		seedHome := museL3PPeerHome(t, "idle")
		record, id := museL3PSeedSession(t, museL3PComposedPlan(t, f, seedHome, "", tuple, ""))
		resumeHome := museL3PPeerHome(t, "idle")
		req := museL3PReq(resumeHome, filepath.Join(seedHome, "data"), tuple, id)
		req.PermissionMode = agentic.PermissionModeYolo
		composed, err := BuildMusePTYLaunchPlan(req)
		if err != nil {
			t.Fatal(err)
		}
		seal, err := composed.plan.ExportSeal()
		if err != nil {
			t.Fatal(err)
		}
		plan, err := BuildMusePTYLaunchPlanFromImport(seal, composed.plan, tuple)
		if err != nil {
			t.Fatal(err)
		}
		plan.frozenStore = f.hostConfig()
		if rewriteHelp {
			// The sealed declaration is gone from the binary
			// help; the bytes themselves are untouched.
			if err := os.WriteFile(filepath.Join(resumeHome, "peer-help"),
				[]byte("Usage: muse [options]\n\nOptions:\n  --trust-workspace  Trust this workspace\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		return ResumeMusePTYHost(ctx, plan, record)
	}
	// launchYolo is the direct imported-launch leg: a fresh yolo plan
	// is composed, sealed by the module and verified-imported, then
	// started through the fresh entry — so the launch-leg seal
	// re-verification is what admits or refuses. The import itself
	// stays pure: it succeeds in both cells and the refusal lands at
	// pre-exec time.
	launchYolo := func(t *testing.T, rewriteHelp bool) (string, *musePTYHost, error) {
		t.Helper()
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m60-help-launch")
		home := museL3PPeerHome(t, "idle")
		req := museL3PReq(home, "", tuple, "")
		req.PermissionMode = agentic.PermissionModeYolo
		composed, err := BuildMusePTYLaunchPlan(req)
		if err != nil {
			t.Fatal(err)
		}
		seal, err := composed.plan.ExportSeal()
		if err != nil {
			t.Fatal(err)
		}
		plan, err := BuildMusePTYLaunchPlanFromImport(seal, composed.plan, tuple)
		if err != nil {
			t.Fatal(err)
		}
		plan.frozenStore = f.hostConfig()
		if rewriteHelp {
			// The sealed declaration is gone from the binary
			// help; the bytes themselves are untouched.
			if err := os.WriteFile(filepath.Join(home, "peer-help"),
				[]byte("Usage: muse [options]\n\nOptions:\n  --trust-workspace  Trust this workspace\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		h, err := StartMusePTYHost(ctx, plan)
		return home, h, err
	}
	t.Run("unchanged_help_resumes", func(t *testing.T) {
		h, err := resumeYolo(t, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Stop(context.Background()) })
	})
	t.Run("changed_help_refuses", func(t *testing.T) {
		h, err := resumeYolo(t, true)
		if h != nil {
			_ = h.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeSealVerifyFailed)
		if !errors.Is(err, muse.ErrMuseSealHelpChanged) {
			t.Fatalf("stale-help refusal = %v, want the sealed help-drift error in its chain", err)
		}
	})
	t.Run("launch_unchanged_help_launches", func(t *testing.T) {
		_, h, err := launchYolo(t, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Stop(context.Background()) })
	})
	t.Run("launch_changed_help_refuses_before_exec", func(t *testing.T) {
		home, h, err := launchYolo(t, true)
		if h != nil {
			_ = h.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeSealVerifyFailed)
		if !errors.Is(err, muse.ErrMuseSealHelpChanged) {
			t.Fatalf("stale-help refusal = %v, want the sealed help-drift error in its chain", err)
		}
		// The refusal lands before any session child runs: the help
		// re-probe prints and exits, so no session state exists.
		sessions := filepath.Join(home, "data", "muse", "sessions")
		if _, serr := os.Stat(sessions); !os.IsNotExist(serr) {
			t.Fatalf("refused launch left session state at %s (stat err=%v): the refusal came after exec", sessions, serr)
		}
	})
}

// museL3PPoisonHome creates one peer home whose `muse` marks every
// invocation and answers a valid version: a PATH probe would pass,
// so only the marker proves whether it ran.
func museL3PPoisonHome(t *testing.T) string {
	t.Helper()
	home := museL3PPeerHome(t, "idle")
	script := "#!/bin/sh\necho \"probed $*\" >> \"$HOME/peer-poison-marker\"\necho \"Muse Code 9.9.9 (9.9.9-R0.1)\"\n"
	if err := os.WriteFile(filepath.Join(home, "muse"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return home
}

func requireMuseL3PNoPoison(t *testing.T, homes ...string) {
	t.Helper()
	for _, home := range homes {
		if _, err := os.Stat(filepath.Join(home, "peer-poison-marker")); !os.IsNotExist(err) {
			t.Fatalf("PATH poison executed in %s (marker present, err=%v)", home, err)
		}
	}
}

// TestSealNeverExecsUnfrozenOnOverridePath (M62) proves no earlier
// probe executes an unfrozen binary on the frozen path: with a
// poisoned `muse` first on the plan PATH, a frozen fresh start and a
// frozen resume both run the frozen binary and never touch the
// poison. A hostile ambient environment likewise changes nothing:
// every path derives from the plan final env only (host-env
// isolation). Production entries: StartMusePTYHost and
// ResumeMusePTYHost.
func TestSealNeverExecsUnfrozenOnOverridePath(t *testing.T) {
	t.Run("fresh_start_never_probes_path", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m62-fresh")
		home := museL3PPoisonHome(t)
		h := museL3PStart(t, museL3PComposedPlan(t, f, home, "", tuple, ""))
		if got := h.ProviderLaunchMetadata().Executable; got != tuple.Path {
			t.Fatalf("executable = %q, want the frozen %q", got, tuple.Path)
		}
		requireMuseL3PNoPoison(t, home)
	})
	t.Run("resume_never_probes_path", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m62-resume")
		seedHome := museL3PPoisonHome(t)
		record, id := museL3PSeedSession(t, museL3PComposedPlan(t, f, seedHome, "", tuple, ""))
		resumeHome := museL3PPoisonHome(t)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		resumed, err := ResumeMusePTYHost(ctx,
			museL3PImportedPlan(t, f, resumeHome, filepath.Join(seedHome, "data"), tuple, id), record)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resumed.Stop(context.Background()) })
		if resumed.NativeIdentity() != id {
			t.Fatal("resume changed identity")
		}
		requireMuseL3PNoPoison(t, seedHome, resumeHome)
	})
	t.Run("hostile_ambient_env_still_starts", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild141)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-m62-ambient")
		home := museL3PPeerHome(t, "idle")
		plan := museL3PComposedPlan(t, f, home, "", tuple, "")
		t.Setenv("PATH", "/nonexistent-hostile")
		t.Setenv("HOME", "/nonexistent-hostile")
		h := museL3PStart(t, plan)
		if got := h.ProviderLaunchMetadata().Executable; got != tuple.Path {
			t.Fatalf("executable = %q, want the frozen %q", got, tuple.Path)
		}
	})
}

// requireMuseL3PFallbackTriple asserts the three fallback warning
// channels on a host that fell back: the snapshot record, the stderr
// emission and the pending note, plus the typed accessors. The event
// binds the seed receipt (sequence 1); the admitted receipt is
// asserted separately.
func requireMuseL3PFallbackTriple(t *testing.T, newest, lastGood musebuilds.FrozenIdentity, host *musePTYHost, stderr *bytes.Buffer, wantMode string, wantConsent bool, wantStage string) {
	t.Helper()
	raw, err := museTestSnapshot(host)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot musePTYSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.VersionFallback || snapshot.VersionFallbackFrom != newest.Path {
		t.Fatalf("fallback record = %v/%q, want true/%q",
			snapshot.VersionFallback, snapshot.VersionFallbackFrom, newest.Path)
	}
	event := snapshot.VersionFallbackEvent
	if event == nil {
		t.Fatal("fallback event is missing from the snapshot")
	}
	if event.Code != musebuilds.CodeFallbackWarning || event.State != "fallback_started" {
		t.Fatalf("fallback event = %q/%q, want %q/fallback_started", event.Code, event.State, musebuilds.CodeFallbackWarning)
	}
	if event.Newest.Build != newest.Build || event.FailureStage != wantStage || event.FailureCode != musePTYStartupCode {
		t.Fatalf("fallback failure = %q/%q/%q, want %q/%q/%q",
			event.Newest.Build, event.FailureStage, event.FailureCode, newest.Build, wantStage, musePTYStartupCode)
	}
	if event.LastGood.Build != lastGood.Build || event.LastGoodSequence != 1 {
		t.Fatalf("fallback last-good = %q seq %d, want %q seq 1", event.LastGood.Build, event.LastGoodSequence, lastGood.Build)
	}
	if event.Mode != wantMode || event.Consent != wantConsent {
		t.Fatalf("fallback mode/consent = %q/%v, want %q/%v", event.Mode, event.Consent, wantMode, wantConsent)
	}
	suffix := ""
	if wantConsent {
		suffix = ", consented"
	}
	wantNote := fmt.Sprintf("%s: newest %s failed at %s (%s); last-good %s fallback_started (receipt 1, mode %s%s)",
		musebuilds.CodeFallbackWarning, newest.Build, wantStage, musePTYStartupCode, lastGood.Build, wantMode, suffix)
	if host.FallbackNote() != wantNote {
		t.Fatalf("terminal pending note = %q, want %q", host.FallbackNote(), wantNote)
	}
	if n := strings.Count(stderr.String(), "warning: "+wantNote+"\n"); n != 1 {
		t.Fatalf("terminal stderr record count = %d, want exactly 1 for %q in:\n%s", n, wantNote, stderr.String())
	}
	if warning := host.FallbackWarning(); warning == nil || *warning != *event {
		t.Fatalf("fallback accessor = %+v, want the snapshot event %+v", warning, event)
	}
}

// museL3PSeedFallback launches the last-good build newest-fresh to a
// real successful start through the production entry and stops it,
// returning its tuple. The seed admits receipt sequence 1.
func museL3PSeedFallback(t *testing.T, f *museL3PFixture, env []string, attempt string) musebuilds.FrozenIdentity {
	t.Helper()
	binding := f.binding(env, musebuilds.RecoveryUnattended, attempt)
	home := museL3PPeerHome(t, "idle")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	host, err := StartMusePTYHostViaBuilds(ctx, binding, museL3PReq(home, "", musebuilds.FrozenIdentity{}, ""))
	if err != nil {
		t.Fatalf("seed launch: %v", err)
	}
	executable := host.ProviderLaunchMetadata().Executable
	museL3PStop(t, host)
	seeded := musebuilds.FrozenIdentity{Build: museL3PBuild142, SHA256: f.sha, Path: executable}
	requireMuseL3PReceipt(t, f, museL3PBuild142, 1)
	return seeded
}

// museL3PSeedResumeSession launches the last-good build newest-fresh
// to a real start through the production entry, snapshots the
// session, stops it and closes its epoch, returning the durable
// record, the native identity, the seed data home and the tuple.
func museL3PSeedResumeSession(t *testing.T, f *museL3PFixture, env []string, attempt string) (sm.ProviderHostRecord, string, string, musebuilds.FrozenIdentity) {
	t.Helper()
	binding := f.binding(env, musebuilds.RecoveryUnattended, attempt)
	seedHome := museL3PPeerHome(t, "idle")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	host, err := StartMusePTYHostViaBuilds(ctx, binding, museL3PReq(seedHome, "", musebuilds.FrozenIdentity{}, ""))
	if err != nil {
		t.Fatalf("seed launch: %v", err)
	}
	tuple := musebuilds.FrozenIdentity{Build: museL3PBuild142, SHA256: f.sha, Path: host.ProviderLaunchMetadata().Executable}
	snapshot, err := museTestSnapshot(host)
	if err != nil {
		t.Fatal(err)
	}
	id := host.NativeIdentity()
	logPath := host.snapshot.LogPath
	museL3PStop(t, host)
	requireMuseL3PReceipt(t, f, museL3PBuild142, 1)
	log, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	endRecord := museOwnedRecord(host, museFixtureRecord("session.end"))
	endBytes, err := json.Marshal(endRecord)
	if err != nil {
		t.Fatal(err)
	}
	_, err = log.Write(append(endBytes, '\n'))
	_ = log.Close()
	if err != nil {
		t.Fatal(err)
	}
	return sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(snapshot)}, id, filepath.Join(seedHome, "data"), tuple
}

// TestPTYFallbackWarningTriple (M50 PTY leg) proves a completed PTY
// fallback warns on all three channels — snapshot, stderr and
// pending note — on the fresh and resume legs alike, in unattended
// and interactive mode. A failed fallback follows up on stderr and
// admits nothing further, and a host-cap deferral warns its own
// triple on the already-frozen target. Production entries:
// StartMusePTYHostViaBuilds and ResumeMusePTYHostViaBuilds.
func TestPTYFallbackWarningTriple(t *testing.T) {
	// newestFails composes per-leg requests: the newest leg fails
	// fast after a healthy start, the fallback leg runs idle.
	newestFails := func(t *testing.T, fallbackMode string) func(context.Context, MusePTYComposeLeg, musebuilds.FrozenIdentity) (agentic.LaunchRequest, error) {
		return func(_ context.Context, leg MusePTYComposeLeg, tuple musebuilds.FrozenIdentity) (agentic.LaunchRequest, error) {
			mode := "exit-early"
			if leg == MusePTYComposeFallback || leg == MusePTYComposeDeferred {
				mode = fallbackMode
			}
			return museL3PReq(museL3PPeerHome(t, mode), "", tuple, ""), nil
		}
	}
	t.Run("fresh/unattended", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, seedEnv := f.install(museL3PBuild142)
		lastGood := museL3PSeedFallback(t, f, seedEnv, "l3p-m50-fresh-seed")
		_, env := f.install(museL3PBuild999, museL3PBuild142)
		stderr := captureMusePTYRecoveryWarning(t)
		offers := 0
		binding := f.binding(env, musebuilds.RecoveryUnattended, "l3p-m50-fresh-unattended")
		binding.Offer = func(context.Context, musebuilds.RecoveryEvent) (bool, error) {
			offers++
			return true, nil
		}
		binding.DeferredOffer = func(context.Context, musebuilds.DeferralStart) (bool, error) {
			return true, nil
		}
		binding.Compose = newestFails(t, "idle")
		newest := f.prepare(env, musebuilds.RouteLaunch, "l3p-m50-fresh-probe")
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		host, err := StartMusePTYHostViaBuilds(ctx, binding,
			museL3PReq(museL3PPeerHome(t, "exit-early"), "", musebuilds.FrozenIdentity{}, ""))
		if err != nil {
			t.Fatalf("fallback launch: %v", err)
		}
		t.Cleanup(func() { _ = host.Stop(context.Background()) })
		if offers != 0 {
			t.Fatalf("fallback offers = %d, want 0 (unattended auto-fallback)", offers)
		}
		requireMuseL3PFallbackTriple(t, newest, lastGood, host, stderr, "unattended", false, musePTYStageLaunch)
		if got := host.ProviderLaunchMetadata().Executable; got != lastGood.Path {
			t.Fatalf("executable = %q, want the last-good %q", got, lastGood.Path)
		}
		requireMuseL3PReceipt(t, f, museL3PBuild142, 2)
	})
	t.Run("fresh/interactive", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, seedEnv := f.install(museL3PBuild142)
		lastGood := museL3PSeedFallback(t, f, seedEnv, "l3p-m50-inter-seed")
		_, env := f.install(museL3PBuild999, museL3PBuild142)
		stderr := captureMusePTYRecoveryWarning(t)
		offers := 0
		binding := f.binding(env, musebuilds.RecoveryInteractive, "l3p-m50-fresh-interactive")
		binding.Offer = func(context.Context, musebuilds.RecoveryEvent) (bool, error) {
			offers++
			return true, nil
		}
		binding.DeferredOffer = func(context.Context, musebuilds.DeferralStart) (bool, error) {
			return true, nil
		}
		binding.Compose = newestFails(t, "idle")
		newest := f.prepare(env, musebuilds.RouteLaunch, "l3p-m50-inter-probe")
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		host, err := StartMusePTYHostViaBuilds(ctx, binding,
			museL3PReq(museL3PPeerHome(t, "exit-early"), "", musebuilds.FrozenIdentity{}, ""))
		if err != nil {
			t.Fatalf("fallback launch: %v", err)
		}
		t.Cleanup(func() { _ = host.Stop(context.Background()) })
		if offers != 1 {
			t.Fatalf("fallback offers = %d, want 1 (interactive consent)", offers)
		}
		requireMuseL3PFallbackTriple(t, newest, lastGood, host, stderr, "interactive", true, musePTYStageLaunch)
		requireMuseL3PReceipt(t, f, museL3PBuild142, 2)
	})
	t.Run("resume/unattended", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, seedEnv := f.install(museL3PBuild142)
		record, id, dataHome, lastGood := museL3PSeedResumeSession(t, f, seedEnv, "l3p-m50-resume-seed")
		_, env := f.install(museL3PBuild999, museL3PBuild142)
		stderr := captureMusePTYRecoveryWarning(t)
		binding := f.binding(env, musebuilds.RecoveryUnattended, "l3p-m50-resume")
		binding.Offer = func(context.Context, musebuilds.RecoveryEvent) (bool, error) {
			return true, nil
		}
		binding.DeferredOffer = func(context.Context, musebuilds.DeferralStart) (bool, error) {
			return true, nil
		}
		// The fallback recomposer resumes the recorded session on
		// the permitted tuple, sharing the seed data home so the
		// prior epoch resolves exactly like production.
		binding.Recompose = func(_ context.Context, tuple musebuilds.FrozenIdentity, sessionID string) (agentic.Seal, agentic.Plan, error) {
			composed, err := BuildMusePTYLaunchPlan(museL3PReq(museL3PPeerHome(t, "idle"), dataHome, tuple, sessionID))
			if err != nil {
				return agentic.Seal{}, agentic.Plan{}, err
			}
			seal, err := composed.plan.ExportSeal()
			if err != nil {
				return agentic.Seal{}, agentic.Plan{}, err
			}
			return seal, composed.plan, nil
		}
		// The recorded seal resumes the failing newest: its
		// selection binds at import time inside the flow.
		newestTuple := f.prepare(env, musebuilds.RouteResume, "l3p-m50-resume-probe")
		failHome := museL3PPeerHome(t, "exit-early")
		composed, err := BuildMusePTYLaunchPlan(museL3PReq(failHome, "", newestTuple, id))
		if err != nil {
			t.Fatal(err)
		}
		seal, err := composed.plan.ExportSeal()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		host, err := ResumeMusePTYHostViaBuilds(ctx, binding, seal, composed.plan, record)
		if err != nil {
			t.Fatalf("fallback resume: %v", err)
		}
		t.Cleanup(func() { _ = host.Stop(context.Background()) })
		if host.NativeIdentity() != id {
			t.Fatal("fallback resume changed identity")
		}
		requireMuseL3PFallbackTriple(t, newestTuple, lastGood, host, stderr, "unattended", false, musePTYStageResume)
		if got := host.ProviderLaunchMetadata().Executable; got != lastGood.Path {
			t.Fatalf("executable = %q, want the last-good %q", got, lastGood.Path)
		}
		requireMuseL3PReceipt(t, f, museL3PBuild142, 2)
	})
	t.Run("fresh/fallback-fails", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, seedEnv := f.install(museL3PBuild142)
		lastGood := museL3PSeedFallback(t, f, seedEnv, "l3p-m50-fail-seed")
		_, env := f.install(museL3PBuild999, museL3PBuild142)
		newest := f.prepare(env, musebuilds.RouteLaunch, "l3p-m50-fail-probe")
		stderr := captureMusePTYRecoveryWarning(t)
		binding := f.binding(env, musebuilds.RecoveryUnattended, "l3p-m50-fail-fresh")
		binding.Compose = newestFails(t, "exit-early")
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		host, err := StartMusePTYHostViaBuilds(ctx, binding,
			museL3PReq(museL3PPeerHome(t, "exit-early"), "", musebuilds.FrozenIdentity{}, ""))
		if err == nil || host != nil {
			t.Fatalf("double-failing launch = (%v, %v), want refusal without a host", host, err)
		}
		if !strings.Contains(stderr.String(), "fallback_failed") {
			t.Fatalf("stderr misses the failed-follow-up state in:\n%s", stderr.String())
		}
		var recovery *MusePTYRecoveryError
		if !errors.As(err, &recovery) || recovery.Warnings == nil {
			t.Fatalf("error = %v, want the recovery channels", err)
		}
		raw, rerr := recovery.Warnings.Snapshot()
		if rerr != nil {
			t.Fatal(rerr)
		}
		var snapshot musePTYRecoverySnapshot
		if rerr := json.Unmarshal(raw, &snapshot); rerr != nil {
			t.Fatal(rerr)
		}
		event := snapshot.VersionFallbackEvent
		if event == nil || event.State != "fallback_failed" {
			t.Fatalf("terminal fallback event = %+v, want fallback_failed", event)
		}
		if event.Newest.Build != museL3PBuild999 || event.LastGood.Build != museL3PBuild142 {
			t.Fatalf("terminal fallback = %q/%q, want %q/%q",
				event.Newest.Build, event.LastGood.Build, museL3PBuild999, museL3PBuild142)
		}
		requireMuseL3PReceipt(t, f, museL3PBuild142, 1)
		requireMuseL3PNoBad(t, f, newest)
		requireMuseL3PNoBad(t, f, lastGood)
	})
	t.Run("fresh/deferral-unattended", func(t *testing.T) {
		f := newMuseL3PFixture(t, 1)
		_, seedEnv := f.install(museL3PBuild142)
		target := museL3PSeedFallback(t, f, seedEnv, "l3p-m50-defer-seed")
		_, env := f.install(museL3PBuild999, museL3PBuild142)
		stderr := captureMusePTYRecoveryWarning(t)
		binding := f.binding(env, musebuilds.RecoveryUnattended, "l3p-m50-defer-run")
		binding.Compose = newestFails(t, "idle")
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		host, err := StartMusePTYHostViaBuilds(ctx, binding,
			museL3PReq(museL3PPeerHome(t, "idle"), "", musebuilds.FrozenIdentity{}, ""))
		if err != nil {
			t.Fatalf("deferred launch: %v", err)
		}
		t.Cleanup(func() { _ = host.Stop(context.Background()) })
		if got := host.ProviderLaunchMetadata().Executable; got != target.Path {
			t.Fatalf("executable = %q, want the already-frozen %q", got, target.Path)
		}
		raw, err := museTestSnapshot(host)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot musePTYSnapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		event := snapshot.VersionDeferralEvent
		if event == nil {
			t.Fatal("deferral event is missing from the snapshot")
		}
		if event.Code != musebuilds.CodeDeferralHostCap || event.State != "deferral_started" {
			t.Fatalf("deferral event = %q/%q, want %q/deferral_started",
				event.Code, event.State, musebuilds.CodeDeferralHostCap)
		}
		if event.Newest.Build != museL3PBuild999 || event.Target.Build != museL3PBuild142 {
			t.Fatalf("deferral = newest %q target %q, want %q/%q",
				event.Newest.Build, event.Target.Build, museL3PBuild999, museL3PBuild142)
		}
		if event.Mode != "unattended" || event.Consent {
			t.Fatalf("deferral mode/consent = %q/%v, want unattended/false", event.Mode, event.Consent)
		}
		wantNote := fmt.Sprintf("%s: newest %s could not freeze (host cap); running newest already-frozen %s (mode unattended)",
			musebuilds.CodeDeferralHostCap, museL3PBuild999, museL3PBuild142)
		if host.DeferralNote() != wantNote {
			t.Fatalf("terminal deferral note = %q, want %q", host.DeferralNote(), wantNote)
		}
		if n := strings.Count(stderr.String(), "warning: "+wantNote+"\n"); n != 1 {
			t.Fatalf("terminal stderr record count = %d, want exactly 1 for %q in:\n%s", n, wantNote, stderr.String())
		}
		if warning := host.DeferralWarning(); warning == nil || *warning != *event {
			t.Fatalf("deferral accessor = %+v, want the snapshot event %+v", warning, event)
		}
		requireMuseL3PReceipt(t, f, museL3PBuild142, 2)
	})
}

// TestPTYFrozenUseLeaseHeldAcrossExec (L3P r2, finding 1) proves the
// frozen-use lease is acquired BEFORE verification and held across
// verification, child startup and reap on BOTH PTY legs, and released
// on every error path (design section 7; the
// musebuilds/execverify.go caller contract; L2M's startMuseChild
// pattern). The hold is observed through the exclusive-flock probe,
// never through timing. Production entries: StartMusePTYHost and
// ResumeMusePTYHost with frozen plans.
func TestPTYFrozenUseLeaseHeldAcrossExec(t *testing.T) {
	museL3PRequireLeaseProbes(t)
	t.Run("launch/holds_across_start_releases_after_reap", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-r2-lease-launch")
		home := museL3PPeerHome(t, "idle")
		h := museL3PStart(t, museL3PComposedPlan(t, f, home, "", tuple, ""))
		requireMuseL3PUseLeaseHeld(t, f.frozen)
		museL3PStop(t, h)
		requireMuseL3PUseLeaseReleased(t, f.frozen)
	})
	t.Run("resume/holds_across_start_releases_after_reap", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-r2-lease-resume")
		seedHome := museL3PPeerHome(t, "idle")
		record, id := museL3PSeedSession(t, museL3PComposedPlan(t, f, seedHome, "", tuple, ""))
		resumeHome := museL3PPeerHome(t, "idle")
		dataHome := filepath.Join(seedHome, "data")
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		resumed, err := ResumeMusePTYHost(ctx, museL3PImportedPlan(t, f, resumeHome, dataHome, tuple, id), record)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resumed.Stop(context.Background()) })
		requireMuseL3PUseLeaseHeld(t, f.frozen)
		museL3PStop(t, resumed)
		requireMuseL3PUseLeaseReleased(t, f.frozen)
	})
	t.Run("launch/contention_refuses_before_verification", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-r2-lease-contend")
		home := museL3PPeerHome(t, "idle")
		plan := museL3PComposedPlan(t, f, home, "", tuple, "")
		// Corrupted bytes under a held store: a verify-first order
		// would refuse on the bytes, an acquire-first order on the
		// lease. The chained module code tells them apart.
		museL3PCorruptFrozen(t, tuple.Path)
		holdMuseL3PUseLeaseExclusive(t, f.frozen)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		h, err := StartMusePTYHost(ctx, plan)
		if h != nil {
			_ = h.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeFrozenVerifyFailed)
		execErr, ok := musebuilds.AsExecError(err)
		if !ok || execErr.Code != musebuilds.CodeExecLeaseFailed {
			t.Fatalf("contention refusal = %v, want the chained module lease failure: acquisition runs before verification", err)
		}
	})
	t.Run("launch/verify_failure_releases", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		tuple := f.prepare(env, musebuilds.RouteLaunch, "l3p-r2-lease-verifyfail")
		home := museL3PPeerHome(t, "idle")
		plan := museL3PComposedPlan(t, f, home, "", tuple, "")
		museL3PCorruptFrozen(t, tuple.Path)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		h, err := StartMusePTYHost(ctx, plan)
		if h != nil {
			_ = h.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeFrozenVerifyFailed)
		execErr, ok := musebuilds.AsExecError(err)
		if !ok || execErr.Code != musebuilds.CodeExecVerifyFailed {
			t.Fatalf("verify refusal = %v, want the chained module bytes failure", err)
		}
		requireMuseL3PUseLeaseReleased(t, f.frozen)
	})
}

// TestPTYStaticRefusalNeverFallsBack (L3P r2, finding 3) proves a
// refusal before any child runs is pre-attempt: never Attempted,
// never a start-failure receipt, never a last-good launch — the same
// strict rule as L2M's pre-attempt refusal. Cells: a fresh flow whose
// request names a non-Muse system while a last-good sits ready, and
// a resume flow whose verified seal resumes a session other than the
// recorded one. Production entries: StartMusePTYHostViaBuilds and
// ResumeMusePTYHostViaBuilds.
func TestPTYStaticRefusalNeverFallsBack(t *testing.T) {
	t.Run("fresh/non_muse_system_refuses_pre_attempt", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, seedEnv := f.install(museL3PBuild142)
		museL3PSeedFallback(t, f, seedEnv, "l3p-r2-preattempt-seed")
		_, env := f.install(museL3PBuild999, museL3PBuild142)
		stderr := captureMusePTYRecoveryWarning(t)
		binding := f.binding(env, musebuilds.RecoveryUnattended, "l3p-r2-preattempt-fresh")
		home := museL3PPeerHome(t, "idle")
		selected := f.prepare(env, musebuilds.RouteLaunch, "l3p-r2-preattempt-probe")
		req := museL3PReq(home, "", musebuilds.FrozenIdentity{}, "")
		// The r2 compose sentinel: the narrowing row admits
		// exactly this member unwrapped, routing it to fallback.
		req.System = "codex"
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		host, err := StartMusePTYHostViaBuilds(ctx, binding, req)
		if host != nil {
			_ = host.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeInvalidConfig)
		if !strings.Contains(err.Error(), "requires the Muse system") {
			t.Fatalf("static refusal = %v, want the composition refusal naming the Muse system", err)
		}
		requireMuseL3PNoAttemptReceipt(t, f, selected, "l3p-r2-preattempt-fresh")
		requireMuseL3PReceipt(t, f, museL3PBuild142, 1)
		if stderr.Len() != 0 {
			t.Fatalf("stderr warned without any attempt:\n%s", stderr.String())
		}
	})
	t.Run("resume/seal_for_other_session_refuses_pre_attempt", func(t *testing.T) {
		f := newMuseL3PFixture(t, 0)
		_, env := f.install(museL3PBuild142)
		newest := f.prepare(env, musebuilds.RouteResume, "l3p-r2-preattempt-rprobe")
		// A well-formed crafted record: the refusal lands before any
		// log is read, so no seed session is needed. The nil session
		// is the r2 selection sentinel: the narrowing row admits
		// exactly this recorded member past the selection check.
		record := sm.ProviderHostRecord{Provider: "muse", Snapshot: museTestCoreSnapshot(json.RawMessage(`{"contract":"task-board.muse-pty-host-v1","session_id":"00000000-0000-0000-0000-000000000000","log_path":"/nonexistent-muse-log","pid":424242}`))}
		home := museL3PPeerHome(t, "idle")
		composed, err := BuildMusePTYLaunchPlan(museL3PReq(home, "", newest, ""))
		if err != nil {
			t.Fatal(err)
		}
		seal, err := composed.plan.ExportSeal()
		if err != nil {
			t.Fatal(err)
		}
		stderr := captureMusePTYRecoveryWarning(t)
		binding := f.binding(env, musebuilds.RecoveryUnattended, "l3p-r2-preattempt-resume")
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		host, err := ResumeMusePTYHostViaBuilds(ctx, binding, seal, composed.plan, record)
		if host != nil {
			_ = host.Stop(context.Background())
		}
		requireMuseL3PExecCode(t, err, musePTYCodeInvalidConfig)
		if !strings.Contains(err.Error(), "does not resume the recorded session") {
			t.Fatalf("static refusal = %v, want the resume-selection refusal", err)
		}
		requireMuseL3PNoAttemptReceipt(t, f, newest, "l3p-r2-preattempt-resume")
		requireMuseL3PNoReceipt(t, f)
		if stderr.Len() != 0 {
			t.Fatalf("stderr warned without any attempt:\n%s", stderr.String())
		}
	})
}

// TestPTYResumeFailureRecordsStartedChild (L3P r2, finding 2) proves a
// resume leg whose child started but never established startup reports
// Started with the startup code — never a spawn error. The M50 resume
// control (TestPTYFallbackWarningTriple/resume/unattended) pins the
// end-to-end triple; this test pins the recorded receipt directly with
// no last-good seeded, so no fallback can follow, and it is the named
// killer for the evidence-drop row. Production entry:
// ResumeMusePTYHostViaBuilds.
func TestPTYResumeFailureRecordsStartedChild(t *testing.T) {
	f := newMuseL3PFixture(t, 0)
	_, seedEnv := f.install(museL3PBuild142)
	seedHome := museL3PPeerHome(t, "idle")
	seedTuple := f.prepare(seedEnv, musebuilds.RouteLaunch, "l3p-r2-evidence-seed")
	record, id := museL3PSeedSession(t, museL3PComposedPlan(t, f, seedHome, "", seedTuple, ""))
	_, env := f.install(museL3PBuild999, museL3PBuild142)
	newest := f.prepare(env, musebuilds.RouteResume, "l3p-r2-evidence-probe")
	// The recorded seal resumes the failing newest: its selection
	// binds at import time inside the flow.
	failHome := museL3PPeerHome(t, "exit-early")
	composed, err := BuildMusePTYLaunchPlan(museL3PReq(failHome, "", newest, id))
	if err != nil {
		t.Fatal(err)
	}
	seal, err := composed.plan.ExportSeal()
	if err != nil {
		t.Fatal(err)
	}
	binding := f.binding(env, musebuilds.RecoveryUnattended, "l3p-r2-evidence-resume")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	host, err := ResumeMusePTYHostViaBuilds(ctx, binding, seal, composed.plan, record)
	if err == nil || host != nil {
		t.Fatalf("failing newest resume = (%v, %v), want refusal without a host (no last-good seeded)", host, err)
	}
	receipt, rerr := musebuilds.ReadAttemptReceipt(ctx, f.hostConfig(), newest, "l3p-r2-evidence-resume")
	if rerr != nil {
		t.Fatalf("read the newest attempt receipt: %v", rerr)
	}
	if !receipt.Execution.Started {
		t.Fatal("recorded Started = false, want true for a child that started")
	}
	if want := musePTYStageResume + " " + musePTYStartupCode; !strings.HasPrefix(receipt.Detail, want) {
		t.Fatalf("recorded detail = %q, want prefix %q", receipt.Detail, want)
	}
	requireMuseL3PNoReceipt(t, f)
}
