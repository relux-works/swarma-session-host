package musepty

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// tb-R198 deterministic scripted-peer synchronization.
//
// The scripted peer (musePeer) and the host output pump are separate
// processes/goroutines. Wall-clock quiet windows cannot synchronize them:
// under load the pump observes a frame late and a back-to-back delivery
// defers on stale VT state (CI 37324323786 attempt 2). These helpers
// replace every sleep, poll-until ticker and quiet-window check in
// scripted tests with event-driven handshakes:
//
//   - The peer signals each completed frame batch (startup, submit,
//     redraw) through a named pipe (FIFO) in HOME. The write is
//     non-blocking (O_RDWR, buffered): the test drains the signal
//     when it settles, and the pump hook below provides the actual
//     synchronization. Paste frames carry no signal: deliver's own
//     composer-match poll (production) synchronizes those. The
//     partial-log half uses a blocking rendezvous instead (see
//     musePeerHandshakeWrite): the peer waits for the test's ack so
//     RunOwner's poll provably observes the partial tail.
//   - The host pump signals every observed PTY chunk through
//     h.outputNotify (see muse_pty_host.go). Settle loops block on
//     that channel until the non-quiet idle predicate (readyLocked)
//     holds: no ticker, no sleep, no wall-clock window.
//   - Quiet itself is driven by an injected test clock
//     (h.testFakeNow): the production quiet/settle comparisons always
//     run, and scripted tests advance the fake instant past
//     musePTYQuiet deterministically. The pump keeps stamping
//     lastOutput with the wall clock, so production timing is
//     untouched and a frozen fake clock holds quiet false forever
//     (the failure injection).
//
// Every scripted wait below is event-driven with a watchdog bound
// (museWaitScripted/museWaitNotify): the bound only fails a genuinely
// stalled peer, no test passes through it. The sole POLLING wait in
// this package is museSmokeBoundedWait, for real-binary tests whose
// external muse process the test hooks cannot observe.

const (
	musePeerFrameFIFO   = "peer-frame-done"
	musePeerPartialHalf = "peer-partial-half"
	musePeerPartialAck  = "peer-partial-ack"
	// The started rendezvous holds the submitted run's started
	// record complete on disk until a deliver fold has provably
	// observed it, so the partial half below always lands while a
	// RunOwner poll is watching (see
	// TestMusePTYPartialAppendWaitsForCompleteRecord).
	musePeerStartedEmit = "peer-started-emit"
	musePeerStartedAck  = "peer-started-ack"
	// The staged session tree holds the whole session directory the
	// test moved aside before resume; the peer completes it with the
	// pid-matching poison and publishes it with one atomic rename, so
	// no traversal can list the session without the poison (see
	// TestMusePTYResumeIgnoresSubagentLogGrammar).
	musePeerStagedSession = "peer-staged-session"
)

// museTestQuietMargin is how far past the quiet window a scripted
// admit jumps the fake clock: every real pump stamp within the next
// hour still reads quiet, so paste and redraw frames inside a
// delivery cannot un-quiet the host. Boundary tests set exact
// instants instead.
const museTestQuietMargin = time.Hour

// museScriptedWatchdog bounds every scripted wait. It fires only on
// a genuinely stalled peer (no pump event, no handshake); reaching
// it fails the test with the awaited condition named.
const museScriptedWatchdog = 30 * time.Second

// musePeerHandshakeWrite is called by the scripted peer (child process)
// for rendezvous handshakes (partial-log half). It blocks until the
// test reads. Missing FIFO means no handshake requested.
func musePeerHandshakeWrite(home, name string) {
	path := filepath.Join(home, name)
	if _, err := os.Stat(path); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	_, _ = f.Write([]byte{1})
	_ = f.Close()
}

// musePeerRendezvous is called by the scripted peer (child process)
// to sequence an emission with the test: it signals emit (blocking
// until the test reads) and waits for ack (blocking until the test
// writes). Missing FIFOs mean no rendezvous requested.
func musePeerRendezvous(home, emit, ack string) {
	musePeerHandshakeWrite(home, emit)
	if _, err := os.Stat(filepath.Join(home, ack)); err != nil {
		return
	}
	if f, err := os.OpenFile(filepath.Join(home, ack), os.O_RDONLY, 0600); err == nil {
		var one [1]byte
		_, _ = f.Read(one[:])
		_ = f.Close()
	}
}

// musePeerFrameHandshakeWrite signals a completed frame batch
// (startup, submit, redraw) without blocking: the FIFO buffers the
// byte (O_RDWR open never blocks) and the test drains it when it
// settles. A blocking write would deadlock deliver: the test may
// settle before the peer signals (missing a non-blocking read) and
// then deliver a paste while the peer blocks on the handshake,
// never reading the paste. Paste frames carry no signal.
func musePeerFrameHandshakeWrite(home string) {
	path := filepath.Join(home, musePeerFrameFIFO)
	if _, err := os.Stat(path); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return
	}
	_, _ = f.Write([]byte{1})
	_ = f.Close()
}

// musePeerHandshakeRead drains one pending handshake signal without
// blocking. When the peer has not signalled (no writer), it returns
// immediately: refuse paths with no submit carry no signal.
func musePeerHandshakeRead(home, name string) {
	path := filepath.Join(home, name)
	if _, err := os.Stat(path); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	var b [1]byte
	_, _ = f.Read(b[:])
}

// museTestClockFrozen reports the failure-injection mode: the fake
// clock never advances past the quiet window, so every scripted
// delivery must defer. Set only for the injected subprocess (see
// TestMusePTYR7DrainFrozenClockFailsFast).
func museTestClockFrozen() bool {
	return os.Getenv("MUSE_FROZEN_CLOCK") == "1"
}

// museInjectTestSeams installs the deterministic scripted-peer seams
// on a host: a fake clock parked at the last observed output
// (elapsed 0, quiet false until advanced) and the pump/fold/settle
// event channels. Production leaves the clock nil and allocates no
// channels. Test-only, same package.
func museInjectTestSeams(t *testing.T, h *musePTYHost) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	fake := h.lastOutput
	h.testFakeNow = &fake
	h.outputNotify = make(chan struct{}, 1024)
	h.foldNotify = make(chan struct{}, 4096)
	h.settleNotify = make(chan struct{}, 16)
}

// museAdvanceTestClockPastQuiet jumps the fake clock past the quiet
// window so the production quiet comparison admits. A no-op for
// real-binary hosts (wall clock) and under the frozen-clock
// injection. Call after the ready predicate holds and before every
// scripted admission.
func museAdvanceTestClockPastQuiet(h *musePTYHost) {
	if museTestClockFrozen() {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.testFakeNow == nil {
		return
	}
	ahead := h.lastOutput.Add(musePTYQuiet + museTestQuietMargin)
	h.testFakeNow = &ahead
}

// museSetTestClockAt parks the fake clock at an exact instant for
// boundary tests. The caller owns lastOutput stability (no peer
// output in flight after a settle).
func museSetTestClockAt(t *testing.T, h *musePTYHost, at time.Time) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.testFakeNow == nil {
		t.Fatal("boundary clock without injected test seams")
	}
	h.testFakeNow = &at
}

// museFreezeTestClock parks the fake clock at the last observed
// output and never advances it: quiet stays false forever. The
// direct failure injection (see TestMusePTYIdleNeverKnownFailsFast).
func museFreezeTestClock(t *testing.T, h *musePTYHost) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.testFakeNow == nil {
		t.Fatal("freeze without injected test seams")
	}
	frozen := h.lastOutput
	h.testFakeNow = &frozen
}

// museLastOutput reads the pump's last-output stamp.
func museLastOutput(h *musePTYHost) time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastOutput
}

// museQuietForReport reads the quiet predicate for structured
// failure messages.
func museQuietForReport(h *musePTYHost) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.quietLocked()
}

// museScriptedReady reports the idle predicate minus the quiet
// window through the single production definition (readyLocked):
// running, idle fold, unstopped, no pending run, safe composer.
// Settle loops wait on this plus pump events; quiet is driven
// separately by the test clock, so a frozen clock never hangs a
// helper.
func museScriptedReady(h *musePTYHost, expected string) bool {
	if state, _ := h.State(context.Background()); state != sm.ProviderHostRunning {
		return false
	}
	f, err := h.fold()
	if err != nil || !f.idle() {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.readyLocked(f, expected)
}

// museWaitScripted blocks until cond holds, making progress on pump
// notifications. The watchdog bounds a genuinely stalled peer; it is
// a failure detector, not synchronization.
func museWaitScripted(t *testing.T, h *musePTYHost, what string, cond func() bool) {
	t.Helper()
	if h.outputNotify == nil {
		t.Fatalf("scripted wait %q without pump notifications", what)
	}
	ctx, cancel := context.WithTimeout(t.Context(), museScriptedWatchdog)
	defer cancel()
	for !cond() {
		select {
		case <-h.outputNotify:
		case <-ctx.Done():
			t.Fatalf("scripted wait %q exceeded %s bound (idle=%v ready=%v quiet=%v): %v",
				what, museScriptedWatchdog, h.idle(""), museScriptedReady(h, ""), museQuietForReport(h), ctx.Err())
		}
	}
}

// museWaitNotify blocks for one event on a test notify channel
// (fold/settle). The watchdog bounds a peer or delivery that never
// signals; it is a failure detector, not synchronization.
func museWaitNotify(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), museScriptedWatchdog)
	defer cancel()
	if err := museWaitNotifyErr(ctx, ch, what); err != nil {
		t.Fatal(err)
	}
}

// museWaitNotifyErr is the non-fatal form for helper goroutines,
// which must report through a channel instead of t.Fatal.
func museWaitNotifyErr(ctx context.Context, ch <-chan struct{}, what string) error {
	if ch == nil {
		return fmt.Errorf("scripted wait %q without notify channel", what)
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("scripted wait %q exceeded bound: %v", what, ctx.Err())
	}
}

// museDrainNotify discards pending events so the next wait observes
// only fresh signals.
func museDrainNotify(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// museSettleScriptedIdle blocks until the scripted peer's last frame
// batch is emitted (pipe handshake) and observed (pump notification)
// with the host ready, then advances the test clock past the quiet
// window and requires idle. It performs no wall-clock wait. Call
// after startup and after every successful scripted delivery, before
// the next delivery. Under the frozen-clock injection it requires
// idle false instead, proving the injection is armed.
func museSettleScriptedIdle(t *testing.T, h *musePTYHost, home string) {
	t.Helper()
	musePeerHandshakeRead(home, musePeerFrameFIFO)
	museWaitScripted(t, h, "scripted idle", func() bool { return museScriptedReady(h, "") })
	museAdvanceTestClockPastQuiet(h)
	if museTestClockFrozen() {
		if h.idle("") {
			t.Fatal("frozen clock must hold idle false (injection armed)")
		}
		return
	}
	if !h.idle("") {
		t.Fatalf("scripted settle: ready but not idle after clock advance (quiet=%v)", museQuietForReport(h))
	}
}

// museSyncPeerEmission drains the frame handshake and blocks until the
// pump has observed enough for the caller's condition, or returns
// immediately when the condition already holds. Use after operations
// whose final state is not idle (dialog redraw, unknown terminal):
// the caller names the VT condition, and no ticker is involved.
func museSyncPeerEmission(t *testing.T, h *musePTYHost, home string, cond func() bool) {
	t.Helper()
	musePeerHandshakeRead(home, musePeerFrameFIFO)
	museWaitScripted(t, h, "peer emission", cond)
}

// museSmokeBoundedWait is the SOLE polling wait in the musepty corpus
// (tb-R198). Only real-binary tests (external muse process, which the
// test hooks cannot observe) may call it. Scripted-peer tests use the
// event-driven settle/sync helpers above, whose watchdog bounds are
// failure detectors, not synchronization. The bound is the caller's
// context; the ticker is the only polling ticker in musepty tests.
func museSmokeBoundedWait(t *testing.T, ctx context.Context, what string, cond func() bool) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatalf("smoke bounded wait %q exceeded: %v", what, ctx.Err())
		case <-ticker.C:
		}
	}
}

// TestMusePTYQuietWindowBoundaryDefersInside pins the quiet gate to
// its production comparison: every frame strictly inside the window
// defers, including 1ns inside, and exactly at the window the >=
// admits. The interior probes at 2ms and 500µs both refuse, proving
// the boundary sits exactly at the production comparison.
func TestMusePTYQuietWindowBoundaryDefersInside(t *testing.T) {
	h, home := musePeer(t, "idle")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, eps := range []time.Duration{2 * time.Millisecond, 500 * time.Microsecond, time.Nanosecond} {
		base := museLastOutput(h)
		museSetTestClockAt(t, h, base.Add(musePTYQuiet-eps))
		if h.idle("") {
			t.Fatalf("quiet gate admitted %s inside the window", eps)
		}
		if err := h.DeliverSessionNotice(ctx, "must defer inside quiet window"); err != ErrMusePTYDeferred {
			t.Fatalf("inside-window delivery (%s): got %v want %v", eps, err, ErrMusePTYDeferred)
		}
		for _, name := range []string{"peer-paste", "peer-submit"} {
			if b, e := os.ReadFile(filepath.Join(home, name)); !os.IsNotExist(e) {
				t.Fatalf("unsafe input %s=%q", name, b)
			}
		}
	}
	base := museLastOutput(h)
	museSetTestClockAt(t, h, base.Add(musePTYQuiet))
	if !h.idle("") {
		t.Fatal("quiet gate refused exactly at the window")
	}
	museAdvanceTestClockPastQuiet(h)
	if err := h.DeliverSessionNotice(ctx, "past-window control"); err != nil {
		t.Fatalf("past-window control: %v", err)
	}
	if b, e := os.ReadFile(filepath.Join(home, "peer-submit")); e != nil || !strings.Contains(string(b), "past-window control") {
		t.Fatalf("control not submitted %q: %v", b, e)
	}
}

// TestMusePTYIdleNeverKnownFailsFast is the tb-R198 direct failure
// injection: with the test clock frozen at the last output, quiet is
// never known, and a delivery that would admit on idle evidence
// defers deterministically after the production settle wait instead
// of hanging. See TestMusePTYR7DrainFrozenClockFailsFast for the
// injection against the drain property itself.
func TestMusePTYIdleNeverKnownFailsFast(t *testing.T) {
	h, home := musePeer(t, "idle")
	if !h.idle("") {
		t.Fatal("precondition: scripted idle must hold before injection")
	}
	museFreezeTestClock(t, h)
	if h.idle("") {
		t.Fatal("frozen clock must not report idle")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	if err := h.DeliverSessionNotice(ctx, "must defer when idle never known"); err != ErrMusePTYDeferred {
		t.Fatalf("frozen clock must defer, got %v", err)
	}
	t.Logf("frozen-clock deferral in %s", time.Since(start))
	for _, name := range []string{"peer-paste", "peer-submit"} {
		if b, e := os.ReadFile(filepath.Join(home, name)); !os.IsNotExist(e) {
			t.Fatalf("unsafe input %s=%q", name, b)
		}
	}
}

// TestMusePTYR7DrainFrozenClockFailsFast proves the drain property
// fails deterministically and fast when idle is never known: it runs
// TestMusePTYR7DrainProperty in a subprocess with the test clock
// frozen and requires the named structured delivery failure well
// under the subprocess timeout (a hang would hit the timeout or
// panic instead).
func TestMusePTYR7DrainFrozenClockFailsFast(t *testing.T) {
	if museTestClockFrozen() {
		t.Skip("injection target run, not the runner")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	start := time.Now()
	cmd := exec.CommandContext(ctx, "go", "test", ".", "-run", `^TestMusePTYR7DrainProperty$`, "-count=1", "-timeout=4m", "-v")
	cmd.Env = append(os.Environ(), "MUSE_FROZEN_CLOCK=1", "GOWORK=off")
	out, err := cmd.CombinedOutput()
	elapsed := time.Since(start)
	t.Logf("drain injection elapsed=%s", elapsed)
	if err == nil {
		t.Fatalf("drain property admitted under frozen clock, want FAIL:\n%s", out)
	}
	if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("drain injection exit=%d, want 1:\n%s", code, out)
	}
	if !strings.Contains(string(out), "--- FAIL: TestMusePTYR7DrainProperty") {
		t.Fatalf("no named drain failure:\n%s", out)
	}
	if !strings.Contains(string(out), "expected delivery admitted") {
		t.Fatalf("no structured delivery failure:\n%s", out)
	}
	if strings.Contains(string(out), "test timed out") || strings.Contains(string(out), "panic:") {
		t.Fatalf("injection hung instead of failing fast:\n%s", out)
	}
	if elapsed >= 3*time.Minute {
		t.Fatalf("injection not fast: %s", elapsed)
	}
}
