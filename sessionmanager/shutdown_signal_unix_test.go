//go:build unix

package sessionmanager

import (
	"context"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestTheShutdownWatchKeepsTheSignalNotifyContextThrowsAway drives the real
// delivery path: signal.Notify, a real SIGTERM to this process, and the
// cancellation the daemon reads.
//
// signal.NotifyContext cancels and discards which signal it was, and that
// discard is what left the 2026-09-06 postmortem reading a Claude transcript to
// answer "was this a SIGTERM, and from where".
func TestTheShutdownWatchKeepsTheSignalNotifyContextThrowsAway(t *testing.T) {
	ctx, watch, stop := WatchShutdownSignals(context.Background(), syscall.SIGTERM)
	defer stop()

	// Safe only because Notify is installed above: until it is, SIGTERM's
	// default disposition would end the test binary.
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling this process: %v", err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the watch never cancelled its context after a real SIGTERM")
	}

	observed, ok := watch.Observed()
	if !ok {
		t.Fatal("the watch cancelled without recording the signal, which is what NotifyContext already did")
	}
	if observed.Signal != syscall.SIGTERM.String() {
		t.Errorf("observed signal = %q, want %q", observed.Signal, syscall.SIGTERM.String())
	}
	if observed.ParentPID != os.Getppid() {
		t.Errorf("observed parent pid = %d, want this process's parent %d", observed.ParentPID, os.Getppid())
	}
	if observed.At.IsZero() {
		t.Error("the observation carries no timestamp")
	}
}

// TestTheWatchReportsNothingUntilASignalArrives is the control for the test
// above: a watch that always claimed an observation would satisfy every
// assertion in this file while attributing kills that never happened.
func TestTheWatchReportsNothingUntilASignalArrives(t *testing.T) {
	ctx, watch, stop := WatchShutdownSignals(context.Background(), syscall.SIGTERM)
	defer stop()
	if _, ok := watch.Observed(); ok {
		t.Fatal("an unsignalled watch reported an observation")
	}
	if ctx.Err() != nil {
		t.Fatalf("an unsignalled watch cancelled its context: %v", ctx.Err())
	}
	if _, ok := (*ShutdownSignalWatch)(nil).Observed(); ok {
		t.Fatal("a nil watch reported an observation")
	}
}

// TestTheOperatorLogNeverClaimsASenderPidThePlatformDidNotExpose is the
// negative side of AC3's "where the platform exposes it".
//
// Neither platform does, through os/signal. The failure this rejects is not a
// missing field but a fabricated one: `signal_sender_pid: 0` reads as a real
// sender to whoever opens the log next, and pid 0 sent nothing.
func TestTheOperatorLogNeverClaimsASenderPidThePlatformDidNotExpose(t *testing.T) {
	watch := newShutdownSignalWatch()
	watch.record(syscall.SIGTERM)
	observed, ok := watch.Observed()
	if !ok {
		t.Fatal("the watch dropped a recorded signal")
	}
	fields := observed.operatorLogFields()
	if _, present := fields["signal_sender_pid"]; present {
		t.Errorf("the log claims a sender pid the platform never reported: %v", fields)
	}
	sender, _ := fields["signal_sender"].(string)
	if !strings.HasPrefix(sender, "unknown: ") || strings.TrimSpace(strings.TrimPrefix(sender, "unknown: ")) == "" {
		t.Errorf("signal_sender = %q, want an explicit unknown carrying its reason", sender)
	}
	if observed.SenderKnown || observed.SenderPID != 0 {
		t.Errorf("an unexposed sender was recorded as known: %+v", observed)
	}
}

// TestAPlatformThatDoesExposeTheSenderGetsItLogged is the positive control. It
// substitutes a platform that reports si_pid, which is the branch this build
// cannot otherwise reach — without it, "never emit a sender pid" would pass by
// never emitting one at all.
func TestAPlatformThatDoesExposeTheSenderGetsItLogged(t *testing.T) {
	watch := newShutdownSignalWatch()
	watch.sender = func(os.Signal) (int, bool, string) { return 4242, true, "" }
	watch.record(syscall.SIGINT)

	observed, ok := watch.Observed()
	if !ok {
		t.Fatal("the watch dropped a recorded signal")
	}
	fields := observed.operatorLogFields()
	if fields["signal_sender_pid"] != 4242 {
		t.Errorf("signal_sender_pid = %v, want 4242: an exposed sender was dropped", fields["signal_sender_pid"])
	}
	if _, present := fields["signal_sender"]; present {
		t.Errorf("a known sender still logged an unknown-reason field: %v", fields)
	}
	if !strings.Contains(observed.senderSentence(), "4242") {
		t.Errorf("sender sentence = %q, want it to name pid 4242", observed.senderSentence())
	}
}

// TestTheWatchKeepsTheFirstSignalNotTheFollowUpKill matters because the kill
// that ends a daemon is rarely alone: a tree walk sends SIGTERM and then
// SIGKILL to the same pid. The first one is the one that cancelled the startup.
func TestTheWatchKeepsTheFirstSignalNotTheFollowUpKill(t *testing.T) {
	watch := newShutdownSignalWatch()
	watch.record(syscall.SIGTERM)
	watch.record(syscall.SIGINT)
	observed, _ := watch.Observed()
	if observed.Signal != syscall.SIGTERM.String() {
		t.Errorf("observed signal = %q, want the FIRST signal %q", observed.Signal, syscall.SIGTERM.String())
	}
}
