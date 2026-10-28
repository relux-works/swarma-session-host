package musebuilds

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// freezeClock is the fixed UTC clock every freeze fixture runs on.
// Ramp mtimes and ledger days derive from it, so freshness and
// day-rollover cells are deterministic.
var freezeClock = time.Date(2026, time.October, 10, 9, 0, 0, 0, time.UTC)

// fakeLock is a scriptable HostLock. The zero value acquires cleanly
// forever; tests script failures, flips and blocking waits. It is
// safe for concurrent use.
type fakeLock struct {
	mu         sync.Mutex
	gate       chan struct{}
	acquires   int
	releases   int
	checks     int
	held       int
	acquireErr error
	checkErr   error
	releaseErr error
	failFirst  int
	onAcquire  func()
	waitFor    chan struct{}
	started    chan struct{}
	log        *freezeEventLog
}

type fakeHeld struct {
	lock *fakeLock
	done bool
}

func (l *fakeLock) Acquire(ctx context.Context) (HostLockHeld, error) {
	if l.waitFor != nil {
		select {
		case l.started <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-l.waitFor:
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.acquires++
	if l.gate == nil {
		l.gate = make(chan struct{}, 1)
		l.gate <- struct{}{}
	}
	gate := l.gate
	hook := l.onAcquire
	fail := l.failFirst > 0
	if fail {
		l.failFirst--
	}
	l.mu.Unlock()
	if hook != nil {
		hook()
	}
	if fail {
		return nil, errors.New("injected lock failure")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-gate:
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.acquireErr != nil {
		gate <- struct{}{}
		return nil, l.acquireErr
	}
	l.held++
	if l.log != nil {
		l.log.add("lock-acquired", "")
	}
	return &fakeHeld{lock: l}, nil
}

func (h *fakeHeld) Check() error {
	l := h.lock
	l.mu.Lock()
	defer l.mu.Unlock()
	l.checks++
	if h.done {
		return context.Canceled
	}
	return l.checkErr
}

func (h *fakeHeld) Release() error {
	l := h.lock
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releases++
	if h.done {
		return context.Canceled
	}
	h.done = true
	l.held--
	l.gate <- struct{}{}
	if l.log != nil {
		l.log.add("lock-released", "")
	}
	return l.releaseErr
}

func (l *fakeLock) counts() (acquires, releases, checks int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquires, l.releases, l.checks
}

// freezeEventLog is a mutex-guarded hook/lock event log for ordering
// assertions across concurrent attempts.
type freezeEventLog struct {
	mu     sync.Mutex
	events []freezeEvent
}

type freezeEvent struct {
	name  string
	build string
}

func (l *freezeEventLog) add(name, build string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, freezeEvent{name: name, build: build})
}

func (l *freezeEventLog) namesFor(build string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var names []string
	for _, event := range l.events {
		if event.build == build {
			names = append(names, event.name)
		}
	}
	return names
}

func (l *freezeEventLog) names() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var names []string
	for _, event := range l.events {
		names = append(names, event.name)
	}
	return names
}

// freezeFixture is one synthetic freeze host: a frozen root, a
// provisioned ledger directory, a CLEAR ramp with a fresh log, a
// fixed clock and a fake global lock. The install fixture supplies
// discovery sources resolved through the real ResolveNewest.
type freezeFixture struct {
	t        *testing.T
	root     string
	ledger   string
	rampFile string
	rampLog  string
	now      time.Time
	lock     *fakeLock
}

// storeRootFixture returns a fresh temporary directory permissioned
// exactly 0700 for use as a frozen, ledger, state or attempts root.
// t.TempDir creates its numbered subdirectory with 0777 masked by the
// umask (0755 on CI), while production refuses any store root that is
// not owner-only, so every store-root fixture goes through this helper.
func storeRootFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func newFreeze(t *testing.T) *freezeFixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(storeRootFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	// Restore directory write permission only after assertions, so TempDir's
	// ordinary-user cleanup can remove published 0555 fixture directories.
	// WalkDir never follows links; production retention remains unchanged.
	t.Cleanup(func() {
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return os.Chmod(path, 0o700)
			}
			return nil
		}); err != nil {
			t.Errorf("restore fixture directory permissions: %v", err)
		}
	})
	ledger, err := filepath.EvalSymlinks(storeRootFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	f := &freezeFixture{t: t, root: root, ledger: ledger, now: freezeClock, lock: &fakeLock{}}
	f.rampFile = filepath.Join(root, "ramp.state")
	f.rampLog = filepath.Join(root, "watch.tsv")
	f.writeRamp("ramp_down=0\n")
	f.writeRampLog(f.now.Format(time.RFC3339) + "\tcount=0\tprobe_s=0\tmatrix=200\tingress=404\tload1=1.0\tload5=1.0\texec_s=1\tgap_s=300\tmatrix_s=0.001\n")
	return f
}

func (f *freezeFixture) writeFileSync(path string, data []byte, mode os.FileMode) {
	f.t.Helper()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".writing-*")
	if err != nil {
		f.t.Fatal(err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		f.t.Fatal(err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		f.t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		f.t.Fatal(err)
	}
}

func (f *freezeFixture) writeRamp(content string) {
	f.t.Helper()
	f.writeFileSync(f.rampFile, []byte(content), 0o644)
	f.chtimes(f.rampFile, f.now)
}

func (f *freezeFixture) writeRampLog(content string) {
	f.t.Helper()
	f.writeFileSync(f.rampLog, []byte(content), 0o644)
	f.chtimes(f.rampLog, f.now)
}

func (f *freezeFixture) chtimes(path string, at time.Time) {
	f.t.Helper()
	if err := os.Chtimes(path, at, at); err != nil {
		f.t.Fatal(err)
	}
}

// config returns a valid HostConfig for this fixture: temporary
// roots, fixed clock, fake lock and no daily cap.
func (f *freezeFixture) config() HostConfig {
	return HostConfig{
		FrozenRoot:       f.root,
		LedgerDir:        f.ledger,
		RampStatePath:    f.rampFile,
		RampLogPath:      f.rampLog,
		MaxFreezesPerDay: 0,
		Host:             "test-host",
		User:             "test-user",
		Now:              func() time.Time { return f.now },
		Locker:           f.lock,
	}
}

// discoveryFor resolves one installed build through the real
// ResolveNewest and returns the discovery freeze must bind to. Each
// call uses an isolated install so builds never contend for newest.
func (f *freezeFixture) discoveryFor(build string) Discovery {
	f.t.Helper()
	install := newInstall(f.t)
	install.writeLauncher(plainScript)
	install.writeCandidate(build)
	install.writeSidecar(build + "\n")
	discovery, err := ResolveNewest(context.Background(), install.env())
	if err != nil {
		f.t.Fatal(err)
	}
	if discovery.Identity.Build != build {
		f.t.Fatalf("selected %s, want %s", discovery.Identity.Build, build)
	}
	return discovery
}

// time_2024 is a fixed past timestamp for mtime-preservation cells.
func time_2024() time.Time {
	return time.Date(2024, time.March, 2, 15, 4, 5, 0, time.UTC)
}

// publishForOther freezes an already-resolved discovery and fails
// unless the outcome is a fresh publication.
func (f *freezeFixture) publishForOther(t *testing.T, discovery Discovery) FrozenCopy {
	t.Helper()
	result, err := PublishFrozen(context.Background(), f.config(), discovery)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomePublished {
		t.Fatalf("outcome = %s, want %s", result.Outcome, OutcomePublished)
	}
	return result
}

// publishFor freezes one build through the public entry and fails
// unless the outcome is a fresh publication.
func (f *freezeFixture) publishFor(build string) FrozenCopy {
	f.t.Helper()
	result, err := PublishFrozen(context.Background(), f.config(), f.discoveryFor(build))
	if err != nil {
		f.t.Fatal(err)
	}
	if result.Outcome != OutcomePublished {
		f.t.Fatalf("outcome = %s, want %s", result.Outcome, OutcomePublished)
	}
	return result
}

// plantReservation writes one ledger reservation directly. Fixture
// setup may use the seam; every killer drives the public entry.
func (f *freezeFixture) plantReservation(day, build, sha, outcome string) Reservation {
	return f.plantReservationAs(day, build, sha, outcome, "test-user")
}

// plantReservationAs plants a reservation attributed to one user,
// for host-wide counting cells.
func (f *freezeFixture) plantReservationAs(day, build, sha, outcome, user string) Reservation {
	return f.plantReservationOnHost(day, build, sha, outcome, "test-host", user)
}

// plantReservationOnHost plants a reservation attributed to one host
// and user, for host-mismatch refusal cells.
func (f *freezeFixture) plantReservationOnHost(day, build, sha, outcome, host, user string) Reservation {
	f.t.Helper()
	reservation := Reservation{
		SchemaVersion: reservationSchema,
		Host:          host,
		User:          user,
		Build:         build,
		SHA256:        sha,
		Transaction:   "txn-planted-" + day + "-" + build,
		Day:           day,
		ReservedAt:    day + "T08:00:00Z",
		Outcome:       outcome,
	}
	created, _, err := createReservation(context.Background(), f.ledger, reservation)
	if err != nil {
		f.t.Fatal(err)
	}
	if !created {
		f.t.Fatalf("reservation for %s/%s already exists", day, build)
	}
	return reservation
}

// plantState writes one STATE document directly for dedupe cells.
func (f *freezeFixture) plantState(key string, state BuildState) {
	f.t.Helper()
	if err := writeBuildState(statePath(f.root, key), state); err != nil {
		f.t.Fatal(err)
	}
}

// plantFrozenBytes publishes one frozen binary with production modes
// but without state, for adoption and unknown-transaction cells.
func (f *freezeFixture) plantFrozenBytes(t *testing.T, key, build string, data []byte) string {
	t.Helper()
	keyDir := frozenKeyDir(f.root, key)
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(keyDir, frozenBinaryName(build))
	f.writeFileSync(path, data, 0o755)
	if err := os.Chmod(keyDir, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o555); err != nil {
		t.Fatal(err)
	}
	return path
}

// requireFreezeCode fails unless err chains to a freeze refusal with
// the exact code and stage. Refusal identity is the code, never
// message prose.
func requireFreezeCode(t *testing.T, err error, code, stage string) *FreezeError {
	t.Helper()
	if err == nil {
		t.Fatalf("want refusal %s, got success", code)
	}
	refusal, ok := AsFreezeError(err)
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

// armFreezeInterpose arms the deterministic freeze hook for one test
// and guarantees it is disarmed afterwards.
func armFreezeInterpose(t *testing.T, hook func(freezeInterposePoint, FreezeHook)) {
	t.Helper()
	if freezeInterposeForTest != nil {
		t.Fatal("freeze interposition hook already armed")
	}
	freezeInterposeForTest = hook
	t.Cleanup(func() { freezeInterposeForTest = nil })
}

// armFsync fails the fsync calls selected by fail and restores the
// seam. The selector sees the synced path and its directory bit, so
// tests fail exactly the post-rename directory sync that reports
// unknown durability.
func armFsync(t *testing.T, fail func(name string, isDir bool) bool) {
	t.Helper()
	if fsyncForTest != nil {
		t.Fatal("fsync hook already armed")
	}
	fsyncForTest = func(f *os.File) error {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		if fail(f.Name(), info.IsDir()) {
			return os.ErrInvalid
		}
		return nil
	}
	t.Cleanup(func() { fsyncForTest = nil })
}

// Fixture wrappers retain historical helpers while production carries its
// directory authority explicitly.
func writeBuildState(path string, state BuildState) error {
	root := filepath.Dir(filepath.Dir(filepath.Dir(path)))
	store, err := openFrozenStore(context.Background(), root)
	if err != nil {
		return err
	}
	defer store.close()
	return writeBuildStateAt(context.Background(), store.meta, filepath.Base(filepath.Dir(path)), state)
}
func readBuildState(ctx context.Context, path string) (BuildState, bool, error) {
	meta, err := openDirPath(ctx, filepath.Dir(filepath.Dir(path)), false)
	if err != nil {
		return BuildState{}, false, err
	}
	defer meta.close()
	return readBuildStateAt(ctx, meta, filepath.Base(filepath.Dir(path)))
}
