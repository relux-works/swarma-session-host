//go:build darwin || linux

package minilock

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fixtureScript is the POSIX-sh stand-in for the canonical
// mini-build-lock script. It implements the script's real grammar —
// `run OWNER -- CMD...`, exit 2 on misuse, TERM/INT traps that log
// the withdrawal/release and exit 143 — plus an early-exit mode.
// FIXTURE_WAIT delays acquisition to exercise
// cancellation; the timeout test fires the client's test hook after
// STARTED appears instead of sleeping a short bound.
const fixtureScript = `#!/bin/sh
set -u
log() { printf '%s\n' "$1" >> "$FIXTURE_LOG"; }
fail() { log "reject: $1"; exit 2; }
[ "${1:-}" = "run" ] || fail "no run verb"
owner="${2:-}"; [ "$owner" = "muse-freeze" ] || fail "owner=$owner"
[ "${3:-}" = "--" ] || fail "no separator"
shift 3
[ "$#" -gt 0 ] || fail "no hold command"
log "argv owner=$owner hold=$*"
mode="${FIXTURE_MODE:-normal}"
if [ "$mode" = "crash" ]; then exit 0; fi
wait_secs="${FIXTURE_WAIT:-0}"
hold=""
if [ "$wait_secs" != "0" ]; then
  sleep "$wait_secs" &
  waiter=$!
  trap 'kill "$waiter" 2>/dev/null; wait "$waiter" 2>/dev/null; log withdrawn; exit 143' TERM INT
  printf '%s\n' "$$" > "$FIXTURE_STARTED.tmp"
  mv "$FIXTURE_STARTED.tmp" "$FIXTURE_STARTED"
  wait "$waiter"
fi
trap 'if [ -n "$hold" ]; then kill "$hold" 2>/dev/null; wait "$hold" 2>/dev/null; fi; log released; exit 143' TERM INT
printf '%s\n' "$$" > "$FIXTURE_STARTED.tmp"
mv "$FIXTURE_STARTED.tmp" "$FIXTURE_STARTED"
"$@" &
hold=$!
wait "$hold"
log "hold-exit=$?"
`

func writeFixture(t *testing.T) (script, started, log, holder string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "mini-build-lock")
	tmp, err := os.CreateTemp(dir, ".writing-*")
	if err != nil {
		t.Fatal(err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write([]byte(fixtureScript)); err != nil {
		_ = tmp.Close()
		t.Fatal(err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tmpName, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		t.Fatal(err)
	}
	started = filepath.Join(dir, "started")
	log = filepath.Join(dir, "fixture.log")
	holder = filepath.Join(dir, "holder")
	t.Setenv("FIXTURE_STARTED", started)
	t.Setenv("FIXTURE_LOG", log)
	t.Setenv("FIXTURE_HOLDER", holder)
	t.Cleanup(func() {
		if raw, err := os.ReadFile(started); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			}
		}
	})
	return path, started, log, holder
}

func clientFor(script string) Client {
	return Client{ScriptPath: script, Owner: OwnerName, WaitMax: 60 * time.Second, HoldMax: 300 * time.Second}
}

// waitForFile polls for path until it exists or the 60-second hang
// guard fires. Coordination is readiness-driven; there is no short
// wall-clock bound.
func waitForFile(t *testing.T, path, what string) {
	t.Helper()
	deadline := time.After(60 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-deadline:
			t.Fatalf("%s never appeared", what)
		default:
		}
	}
}

// TestMinilockAcquireReleaseOk proves one hold round-trips: the
// script observes the owner, the hold starts, Check stays silent
// while held, and Release terminates the hold group.
func TestMinilockAcquireReleaseOk(t *testing.T) {
	script, started, logPath, _ := writeFixture(t)
	t.Setenv("FIXTURE_MODE", "normal")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	held, err := clientFor(script).Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(started); err != nil {
		t.Fatalf("hold never started: %v", err)
	}
	if err := held.Check(); err != nil {
		t.Fatalf("held check: %v", err)
	}
	if err := held.Release(); err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"owner=muse-freeze", "released"} {
		if !strings.Contains(string(log), want) {
			t.Fatalf("fixture log %q lacks %q", log, want)
		}
	}
	if err := held.Release(); err == nil {
		t.Fatal("second release succeeded")
	}
}

// TestMinilockRejectsBadConfig proves empty script and owner refuse
// before any process starts.
func TestMinilockRejectsBadConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, client := range []Client{{Owner: OwnerName}, {ScriptPath: "/bin/true"}} {
		if _, err := client.Acquire(ctx); err == nil {
			t.Fatalf("client %+v acquired", client)
		}
	}
}

// TestMinilockWaitTimeout proves the client-side wait bound fires:
// after the fixture signals waiting, the test hook expires the wait,
// the waiter is signalled, the ticket is withdrawn by the trap, and
// Acquire reports the typed timeout with the trap's 143 status. The
// trigger is the fixture's readiness, not a short sleep.
func TestMinilockWaitTimeout(t *testing.T) {
	script, started, logPath, _ := writeFixture(t)
	t.Setenv("FIXTURE_MODE", "normal")
	t.Setenv("FIXTURE_WAIT", "300")
	client := clientFor(script)
	timeoutCh := make(chan time.Time)
	client.waitTimeoutForTest = timeoutCh
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	type result struct {
		held *Held
		err  error
	}
	done := make(chan result, 1)
	go func() {
		held, err := client.Acquire(ctx)
		done <- result{held: held, err: err}
	}()
	waitForFile(t, started, "wait")
	close(timeoutCh)
	select {
	case r := <-done:
		if !errors.Is(r.err, ErrWaitTimeout) {
			t.Fatalf("error = %v, want the wait timeout", r.err)
		}
		var lockErr *Error
		if !errors.As(r.err, &lockErr) || lockErr.Exit != 143 {
			t.Fatalf("error = %#v, want the trap exit 143", r.err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("expired wait never returned")
	}
	log, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(log), "withdrawn") {
		t.Fatalf("log %q lacks the withdrawal", log)
	}
	if strings.Contains(string(log), "hold-exit") {
		t.Fatalf("timeout ran the hold it never took: %q", log)
	}
}

// TestMinilockCancelDuringWait proves cancellation ends the wait:
// the waiter is signalled, the ticket is withdrawn by the trap, and
// Acquire reports the context instead of a hold.
func TestMinilockCancelDuringWait(t *testing.T) {
	script, started, logPath, _ := writeFixture(t)
	t.Setenv("FIXTURE_MODE", "normal")
	t.Setenv("FIXTURE_WAIT", "300")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		held *Held
		err  error
	}
	done := make(chan result, 1)
	go func() {
		held, err := clientFor(script).Acquire(ctx)
		done <- result{held: held, err: err}
	}()
	waitForFile(t, started, "wait")
	cancel()
	select {
	case r := <-done:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("error = %v, want the cancellation", r.err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("cancelled wait never returned")
	}
	log, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(log), "withdrawn") {
		t.Fatalf("log %q lacks the withdrawal", log)
	}
}

// TestMinilockTermResistantWaitCancelled proves cancellation still
// returns when the waiter ignores SIGTERM: the client escalates to
// SIGKILL within its bounded graces instead of waiting out the
// fixture's long sleep. Coordination is the fixture's readiness,
// not a short bound.
func TestMinilockTermResistantWaitCancelled(t *testing.T) {
	client, started, command := processFixtureClient(t, "ignore-term")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.Acquire(ctx); done <- err }()
	waitForFile(t, started, "TERM-resistant readiness")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrTeardownUnconfirmed) {
			t.Fatalf("Acquire = %v, want cancellation with confirmed teardown", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("TERM-resistant cancellation did not return")
	}
	cmd := <-command
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("TERM-resistant process was not reaped via KILL: %v", cmd.ProcessState)
	}
	requireGroupGone(t, cmd.Process.Pid)
}

func TestMinilockPipeHolderDoesNotHangWait(t *testing.T) {
	client, started, command := processFixtureClient(t, "pipe-holder")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := client.Acquire(ctx)
	var lockErr *Error
	if !errors.As(err, &lockErr) || lockErr.Exit != 0 || !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("Acquire = %v, want exit 0 plus retained pipe-drain failure", err)
	}
	waitForFile(t, started, "pipe-holder readiness")
	cmd := <-command
	requireGroupGone(t, cmd.Process.Pid)
}

// processFixtureClient redirects only process creation to this test binary.
// Acquire's argv, group, cancellation, reaper and latching are unchanged.
func processFixtureClient(t *testing.T, mode string) (Client, string, <-chan *exec.Cmd) {
	t.Helper()
	started := filepath.Join(t.TempDir(), "ready")
	commands := make(chan *exec.Cmd, 1)
	client := clientFor("fixture")
	client.commandForTest = func(_ string, args ...string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestMinilockProcessFixture$", "--"}, args...)...)
		cmd.Env = append(os.Environ(), "MUSE_LOCK_PROCESS_FIXTURE="+mode, "MUSE_LOCK_PROCESS_READY="+started)
		commands <- cmd
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		})
		return cmd
	}
	return client, started, commands
}

// A Go process installs its signal behavior before readiness. No shell trap
// can replace it. Pipe-holder mode makes a descendant acknowledge the inherited
// stderr before its parent exits, deterministically exercising WaitDelay.
func TestMinilockProcessFixture(t *testing.T) {
	mode := os.Getenv("MUSE_LOCK_PROCESS_FIXTURE")
	if mode == "" {
		return
	}
	switch mode {
	case "ignore-term", "pipe-child":
		signal.Ignore(syscall.SIGTERM, syscall.SIGINT)
		if mode == "pipe-child" {
			ready := os.NewFile(3, "ready")
			if _, err := ready.Write([]byte("R")); err != nil {
				os.Exit(91)
			}
			_ = ready.Close()
		} else {
			if err := os.WriteFile(os.Getenv("MUSE_LOCK_PROCESS_READY"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
				os.Exit(92)
			}
		}
		<-time.After(300 * time.Second) // fixture backstop; tests cancel by event
	case "pipe-holder":
		read, write, err := os.Pipe()
		if err != nil {
			os.Exit(93)
		}
		child := exec.Command(os.Args[0], "-test.run=^TestMinilockProcessFixture$")
		child.Env = append(os.Environ(), "MUSE_LOCK_PROCESS_FIXTURE=pipe-child")
		child.Stderr = os.Stderr
		child.ExtraFiles = []*os.File{write}
		if err := child.Start(); err != nil {
			os.Exit(94)
		}
		_ = write.Close()
		var ready [1]byte
		if _, err := read.Read(ready[:]); err != nil {
			os.Exit(95)
		}
		_ = read.Close()
		if err := os.WriteFile(os.Getenv("MUSE_LOCK_PROCESS_READY"), []byte("inherited"), 0o600); err != nil {
			os.Exit(96)
		}
	default:
		os.Exit(97)
	}
	os.Exit(0)
}

func requireGroupGone(t *testing.T, pgid int) {
	t.Helper()
	guard := time.After(60 * time.Second)
	for {
		if err := syscall.Kill(-pgid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		select {
		case <-guard:
			t.Fatalf("process group %d still exists", pgid)
		default:
			runtime.Gosched()
		}
	}
}

// TestMinilockEarlyExitRefuses proves a script that exits before the
// hold starts is an error, never a hold: there is no sentinel, so
// there is no acquisition.
func TestMinilockEarlyExitRefuses(t *testing.T) {
	script, _, _, _ := writeFixture(t)
	t.Setenv("FIXTURE_MODE", "crash")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := clientFor(script).Acquire(ctx)
	if err == nil {
		t.Fatal("early exit acquired")
	}
	if errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("error = %v, want a plain early exit, not a timeout", err)
	}
}

// TestMinilockWrongOwnerRefuses proves the script's grammar gate is
// load-bearing: a client naming another owner is rejected with the
// script's usage status.
func TestMinilockWrongOwnerRefuses(t *testing.T) {
	script, _, _, _ := writeFixture(t)
	t.Setenv("FIXTURE_MODE", "normal")
	client := clientFor(script)
	client.Owner = "intruder"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := client.Acquire(ctx)
	var lockErr *Error
	if !errors.As(err, &lockErr) || lockErr.Exit != 2 {
		t.Fatalf("error = %#v, want the usage exit", err)
	}
}

// TestMinilockExitedHoldReportsLoss proves Check and Release latch
// lock loss when the hold process already ended: a dead hold is
// never claimed as held, not even on a second read.
func TestMinilockExitedHoldReportsLoss(t *testing.T) {
	script, _, _, _ := writeFixture(t)
	t.Setenv("FIXTURE_MODE", "normal")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	held, err := clientFor(script).Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	held.killForced()
	select {
	case <-held.done:
	case <-time.After(60 * time.Second):
		t.Fatal("hold was not reaped")
	}
	if err := held.Check(); !errors.Is(err, ErrLockLost) {
		t.Fatalf("first check = %v, want lock loss", err)
	}
	if err := held.Check(); !errors.Is(err, ErrLockLost) {
		t.Fatalf("second check = %v, want latched lock loss", err)
	}
	if err := held.Release(); !errors.Is(err, ErrLockLost) {
		t.Fatalf("first release = %v, want lock loss", err)
	}
	if err := held.Release(); !errors.Is(err, ErrLockLost) {
		t.Fatalf("second release = %v, want latched lock loss", err)
	}
}

// TestMinilockSignalReachesHoldGroup proves Release terminates the
// hold command itself, not only the script: the fixture's hold pid
// would outlive a script-only kill.
func TestMinilockSignalReachesHoldGroup(t *testing.T) {
	script, _, _, _ := writeFixture(t)
	t.Setenv("FIXTURE_MODE", "normal")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := clientFor(script)
	client.HoldMax = 90 * time.Second
	held, err := client.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pgid, err := syscall.Getpgid(held.cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if pgid != held.cmd.Process.Pid {
		t.Fatalf("hold pgid = %d, want its own group %d", pgid, held.cmd.Process.Pid)
	}
	if err := held.Release(); err != nil {
		t.Fatal(err)
	}
}
