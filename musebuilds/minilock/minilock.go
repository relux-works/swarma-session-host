// Package minilock holds the global mini-build-lock inside a Go process.
//
// The lock itself is the infra-owned mini-build-lock script (G2 Q2
// binding); this package never reimplements its queue, ticket or
// staleness rules. Acquire starts
//
//	mini-build-lock run <owner> -- <hold command>
//
// and treats the hold command's first observed side effect as proof of
// acquisition: the script runs its command only after taking the lock,
// or, on the re-entrant path, when an ancestor of this process already
// holds it — in which case the critical section still runs under that
// held lock. Release terminates the whole hold process group, so the
// script's own trap withdraws the ticket and drops the lock.
//
// The argument grammar above is the canonical script's real CLI,
// read from the installed mini-build-lock helper: `run
// <owner> -- <command...>` and `status`, exit 2 on usage, 130 on
// INT, 143 on TERM, otherwise the command's status. The script waits
// unboundedly (FIFO ticket, one-second poll, stale-holder break),
// so the WaitMax bound is enforced client-side: on expiry the waiter
// is signalled, reaped and reported as a wait timeout, and the
// script's TERM trap withdraws the ticket so nothing runs. Context
// cancellation bounds every wait the same way: on cancellation the
// waiter is signalled and reaped before Acquire returns, so no hold
// outlives its context.
//
// Termination is bounded and escalating: SIGTERM to the hold process
// group, a grace wait, then SIGKILL and a second grace wait. The
// stderr capture also stops waiting after its drain grace
// (WaitDelay), so a grandchild that inherits the pipe cannot wedge
// the reaper. A hold that survives SIGKILL is unkillable by
// definition (the design's kernel-bound residual); the hold
// command's own HoldMax backstop still bounds the lock, not the
// process.
//
// Lock loss latches: once the hold process has exited, every later
// Check and every Release reports loss with the recorded exit
// status. There is no second read that observes the loss as held.
//
// This package lives outside musebuilds on purpose: discovery must
// never start a process (TestDiscoverySourcesImportNoExec pins that no
// production file in the parent package imports os/exec), while host
// admission must shell out to the canonical lock. The seam between
// them is the musebuilds.HostLock interface; tests inject fakes and
// drive this client only through the production-wiring test with a
// fixture script.
package minilock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OwnerName is the global-lock owner every freeze acquisition uses.
const OwnerName = "muse-freeze"

// DefaultWaitMax bounds one lock acquisition when the client leaves
// WaitMax unset. DefaultHoldMax bounds the hold backstop: the hold
// command exits on its own after this long even if Release never runs,
// so a leaked hold cannot wedge the global lock forever. Release kills
// the hold promptly; an early hold exit is reported as lock loss.
const (
	DefaultWaitMax = 120 * time.Second
	DefaultHoldMax = 300 * time.Second
	// releaseSigGrace bounds each termination wait: TERM grace, then
	// KILL grace. pipeDrainGrace bounds the stderr reaper after the
	// process exits.
	releaseSigGrace = 5 * time.Second
	pipeDrainGrace  = 5 * time.Second
	sentinelPoll    = 10 * time.Millisecond
)

var (
	// ErrWaitTimeout reports a client-side wait bound expiry: the
	// waiter was signalled, the script's trap withdrew the ticket
	// and nothing ran.
	ErrWaitTimeout = errors.New("minilock: lock wait timed out")
	// ErrLockLost reports a hold that ended before Release: the
	// critical section is no longer provably serialized and its
	// result must not be claimed as a locked publication.
	ErrLockLost = errors.New("minilock: lock hold ended early")
	// ErrUnsupported reports a platform without process-group holds.
	ErrUnsupported         = errors.New("minilock: holds need process groups")
	ErrTeardownUnconfirmed = errors.New("minilock: teardown was not confirmed")
)

// Error is a typed lock failure: the operation that failed and the
// cause. Exit carries the script's exit status when the script ran
// and exited; it is -1 when no status exists.
type Error struct {
	Op    string
	Exit  int
	Cause error
}

// Error renders the failure with its operation and exit status.
func (e *Error) Error() string {
	if e == nil {
		return "minilock error"
	}
	if e.Exit >= 0 {
		return "minilock " + e.Op + " exit=" + strconv.Itoa(e.Exit) + ": " + e.Cause.Error()
	}
	return "minilock " + e.Op + ": " + e.Cause.Error()
}

// Unwrap returns the underlying cause.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Client acquires the global lock through the canonical script.
// ScriptPath and Owner are required; WaitMax and HoldMax default when
// not positive. ShellPath names the shell that runs the hold probe and
// defaults to /bin/sh; the hold needs sleep on PATH.
type Client struct {
	ScriptPath string
	Owner      string
	WaitMax    time.Duration
	HoldMax    time.Duration
	ShellPath  string
	// AccountHome is resolved from the account by production wiring. An empty
	// value preserves explicit fixture environments; production always sets it.
	AccountHome string
	// waitTimeoutForTest replaces the WaitMax timer when set: closing
	// or sending on it fires the wait timeout. Tests close it after
	// the fixture signals waiting, so the timeout path is
	// readiness-driven without a short wall-clock bound. Nil in
	// production.
	waitTimeoutForTest <-chan time.Time
	commandForTest     func(string, ...string) *exec.Cmd
}

func (c Client) waitMax() time.Duration {
	if c.WaitMax > 0 {
		return c.WaitMax
	}
	return DefaultWaitMax
}

func (c Client) holdMax() time.Duration {
	if c.HoldMax > 0 {
		return c.HoldMax
	}
	return DefaultHoldMax
}

func (c Client) shell() string {
	if c.ShellPath != "" {
		return c.ShellPath
	}
	return "/bin/sh"
}

// Held is one acquired global lock. Check reports lock loss without
// releasing; Release drops the hold and reports lock loss when the
// hold already ended. Loss latches: once observed, every later Check
// and Release reports it. Neither is safe for concurrent use, except
// that the internal waiter records the exit exactly once.
type Held struct {
	cmd *exec.Cmd
	mu  sync.Mutex
	// exited and waitErr are set once by the waiter goroutine when
	// cmd.Wait returns; done is closed after they are set.
	exited      bool
	waitErr     error
	done        chan struct{}
	sentinelDir string
	released    bool
}

// waitResult records the reaped exit under the latch mutex.
func (h *Held) waitResult(err error) {
	h.mu.Lock()
	h.exited = true
	h.waitErr = err
	h.mu.Unlock()
	close(h.done)
}

// latched reports whether the hold has exited and its recorded exit.
func (h *Held) latched() (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.exited, h.waitErr
}

// waitDone reports whether the waiter recorded the exit within d.
func (h *Held) waitDone(d time.Duration) bool {
	select {
	case <-h.done:
		return true
	case <-time.After(d):
		return false
	}
}

// terminate signals the hold group and reaps it within two bounded
// graces: SIGTERM, wait, SIGKILL, wait. It reports whether the exit
// was reaped; a false return means the hold survived SIGKILL and is
// unkillable (the kernel-bound residual).
func (h *Held) terminate() bool {
	h.kill()
	if h.waitDone(releaseSigGrace) {
		h.killForced() // a reaped parent may leave TERM-resistant descendants
		return true
	}
	h.killForced()
	return h.waitDone(releaseSigGrace)
}

// Acquire takes the global lock, bounded by ctx and by WaitMax.
// Success means the hold command was observed running, which the
// script does only under the lock. The caller must call Release.
func (c Client) Acquire(ctx context.Context) (*Held, error) {
	if !platformSupported {
		return nil, &Error{Op: "acquire", Exit: -1, Cause: ErrUnsupported}
	}
	if err := ctx.Err(); err != nil {
		return nil, &Error{Op: "acquire", Exit: -1, Cause: err}
	}
	if c.ScriptPath == "" || c.Owner == "" {
		return nil, &Error{Op: "acquire", Exit: -1, Cause: errors.New("script path and owner are required")}
	}
	holdSecs := int64(c.holdMax() / time.Second)
	if holdSecs < 1 {
		holdSecs = 1
	}
	sentinelDir, err := os.MkdirTemp("", "muse-freeze-lock-")
	if err != nil {
		return nil, &Error{Op: "acquire", Exit: -1, Cause: err}
	}
	sentinel := filepath.Join(sentinelDir, "acquired")
	// The hold probe touches the sentinel and only then sleeps: the
	// sentinel's appearance proves the script reached its command.
	hold := []string{c.shell(), "-c", `touch "$1" && exec sleep "$2"`, "minilock-hold", sentinel, strconv.FormatInt(holdSecs, 10)}
	argv := []string{"run", c.Owner, "--"}
	argv = append(argv, hold...)
	command := exec.Command
	if c.commandForTest != nil {
		command = c.commandForTest
	}
	cmd := command(c.ScriptPath, argv...)
	if c.AccountHome != "" {
		env := make([]string, 0, len(os.Environ())+1)
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "HOME=") {
				env = append(env, entry)
			}
		}
		cmd.Env = append(env, "HOME="+c.AccountHome)
	}
	setGroup(cmd)
	// WaitDelay bounds the stderr reaper: a grandchild that inherits
	// the capture pipe cannot wedge cmd.Wait past this grace after
	// the script exits.
	cmd.WaitDelay = pipeDrainGrace
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = nil
	cmd.Stdin = nil
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(sentinelDir)
		return nil, &Error{Op: "acquire", Exit: -1, Cause: err}
	}
	held := &Held{cmd: cmd, done: make(chan struct{}), sentinelDir: sentinelDir}
	go func() { held.waitResult(cmd.Wait()) }()
	timeoutCh := c.waitTimeoutForTest
	var timer *time.Timer
	if timeoutCh == nil {
		timer = time.NewTimer(c.waitMax())
		defer timer.Stop()
		timeoutCh = timer.C
	}
	ticker := time.NewTicker(sentinelPoll)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(sentinel); err == nil {
			if cause := ctx.Err(); cause != nil {
				if !held.terminate() {
					cause = errors.Join(cause, ErrTeardownUnconfirmed)
				}
				_ = os.RemoveAll(sentinelDir)
				return nil, &Error{Op: "acquire", Exit: -1, Cause: cause}
			}
			if checkErr := held.Check(); checkErr != nil {
				held.killForced()
				return nil, checkErr
			}
			return held, nil
		} else if !os.IsNotExist(err) {
			cause := err
			if !held.terminate() {
				cause = errors.Join(cause, ErrTeardownUnconfirmed)
			}
			_ = os.RemoveAll(sentinelDir)
			return nil, &Error{Op: "acquire", Exit: -1, Cause: cause}
		}
		select {
		case <-held.done:
			held.killForced() // the exited parent can leave pipe-holding descendants
			_ = os.RemoveAll(sentinelDir)
			if err := ctx.Err(); err != nil {
				_, waitErr := held.latched()
				return nil, &Error{Op: "acquire", Exit: exitOf(waitErr), Cause: err}
			}
			_, waitErr := held.latched()
			return nil, c.earlyExit(waitErr, &stderr)
		case <-ctx.Done():
			cause := ctx.Err()
			if !held.terminate() {
				cause = errors.Join(cause, ErrTeardownUnconfirmed)
			}
			_ = os.RemoveAll(sentinelDir)
			return nil, &Error{Op: "acquire", Exit: -1, Cause: cause}
		case <-timeoutCh:
			cause := ErrWaitTimeout
			if !held.terminate() {
				cause = errors.Join(cause, ErrTeardownUnconfirmed)
			}
			_ = os.RemoveAll(sentinelDir)
			_, waitErr := held.latched()
			return nil, &Error{Op: "acquire", Exit: exitOf(waitErr), Cause: cause}
		case <-ticker.C:
		}
	}
}

func (c Client) earlyExit(err error, stderr *bytes.Buffer) error {
	cause := err
	exit := exitOf(err)
	tail := stderr.Bytes()
	if len(tail) > 4096 {
		tail = tail[len(tail)-4096:]
	}
	if err == nil {
		cause = errors.New("script exited before the hold started")
	}
	return &Error{Op: "acquire", Exit: exit, Cause: fmt.Errorf("%w: %s", cause, tail)}
}

// Check reports whether the hold is still alive. A nil result means
// the hold has not observably exited; any error means the hold ended
// and the critical section lost its serialization. Loss latches:
// once Check reports loss, every later Check reports loss too.
func (h *Held) Check() error {
	if h == nil || h.cmd == nil {
		return &Error{Op: "check", Exit: -1, Cause: ErrLockLost}
	}
	if exited, waitErr := h.latched(); exited {
		return &Error{Op: "check", Exit: exitOf(waitErr), Cause: fmt.Errorf("%w: %v", ErrLockLost, waitErr)}
	}
	return nil
}

// Release drops the hold: terminate the hold process group, reap the
// script and remove the sentinel directory. Release is bounded — a
// hold that ignores termination is killed — and reports lock loss
// when the hold already ended before Release ran. Loss latches:
// every Release on a lost hold reports loss, including repeats.
func (h *Held) Release() error {
	if h == nil || h.cmd == nil {
		return &Error{Op: "release", Exit: -1, Cause: ErrLockLost}
	}
	h.mu.Lock()
	exited, waitErr := h.exited, h.waitErr
	already := h.released
	h.released = true
	h.mu.Unlock()
	defer func() { _ = os.RemoveAll(h.sentinelDir) }()
	if exited {
		return &Error{Op: "release", Exit: exitOf(waitErr), Cause: fmt.Errorf("%w: %v", ErrLockLost, waitErr)}
	}
	if already {
		return &Error{Op: "release", Exit: -1, Cause: errors.New("hold already released")}
	}
	if h.terminate() {
		return nil
	}
	return &Error{Op: "release", Exit: -1, Cause: ErrTeardownUnconfirmed}
}

func (h *Held) kill() {
	if h.cmd.Process != nil {
		_ = killGroup(h.cmd.Process.Pid, false)
	}
}

func (h *Held) killForced() {
	if h.cmd.Process != nil {
		_ = killGroup(h.cmd.Process.Pid, true)
	}
}

func exitOf(err error) int {
	if err == nil || errors.Is(err, exec.ErrWaitDelay) {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}
