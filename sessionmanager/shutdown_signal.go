package sessionmanager

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
)

// ObservedShutdownSignal is the signal that cancelled a daemon's root context.
//
// The 2026-09-06 incident needed a Claude transcript to answer "who killed the
// replacement daemon 1.2 s into startup". Everything the daemon left on disk
// was "repairing session manager state before listen: context canceled", which
// names neither the signal nor anything attributable about its origin. This
// record is what the operator log carries instead.
type ObservedShutdownSignal struct {
	// Signal is the platform's name for the signal, e.g. "terminated".
	Signal string
	// At is when this process OBSERVED the signal, which is not when it was
	// sent. The distinction matters in an incident reconstruction, so the field
	// is named for the reading rather than for the event.
	At time.Time
	// ParentPID is this process's parent at the moment the signal arrived. It
	// is emphatically NOT the sender; it is the one attributable process the
	// platform does expose for free. After the detached relaunch a correctly
	// reparented daemon reads 1 here, which is itself most of the answer to
	// "was this a tree kill walking down from my launcher".
	ParentPID int
	// SenderPID is meaningful only when SenderKnown is true. A zero pid is not
	// a sender, it is an absence, and reporting it as one would hand the next
	// incident a fabricated fact.
	SenderPID   int
	SenderKnown bool
	// SenderDetail explains an unknown sender and is never empty when
	// SenderKnown is false.
	SenderDetail string
}

// shutdownSignalSender reports the process that sent a signal when the platform
// exposes it to a Go program.
//
// Neither of this project's platforms does, through this path. The Go runtime's
// signal handler discards siginfo_t before the signal reaches os/signal, so
// si_pid never becomes observable: Linux could recover it by taking delivery
// over signalfd instead, and Darwin has no equivalent at all. Rather than guess
// from ppid or from the process table, the daemon reports the sender as unknown
// and names the reason. It is a package variable so a test can substitute a
// platform that does expose it and drive the reporting branch this build cannot
// reach.
var shutdownSignalSender = func(os.Signal) (pid int, known bool, detail string) {
	return 0, false, "the Go runtime delivers signals without siginfo_t, so the sending pid is not observable"
}

// ShutdownSignalWatch records the FIRST shutdown signal this process received.
//
// First, not last: the daemon's job here is attribution of the kill that ended
// it, and a later SIGKILL follow-up in the same tree walk would overwrite the
// signal that actually cancelled the startup.
type ShutdownSignalWatch struct {
	mu       sync.Mutex
	observed *ObservedShutdownSignal
	now      func() time.Time
	parent   func() int
	sender   func(os.Signal) (int, bool, string)
}

func newShutdownSignalWatch() *ShutdownSignalWatch {
	return &ShutdownSignalWatch{
		now:    func() time.Time { return time.Now().UTC() },
		parent: os.Getppid,
		sender: shutdownSignalSender,
	}
}

func (w *ShutdownSignalWatch) record(received os.Signal) {
	if w == nil || received == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.observed != nil {
		return
	}
	pid, known, detail := w.sender(received)
	if !known && detail == "" {
		detail = "the sending process was not reported"
	}
	if !known {
		pid = 0
	}
	w.observed = &ObservedShutdownSignal{
		Signal:       received.String(),
		At:           w.now(),
		ParentPID:    w.parent(),
		SenderPID:    pid,
		SenderKnown:  known,
		SenderDetail: detail,
	}
}

// Observed reports the recorded signal. The boolean is the whole contract: a
// zero-valued record must never be read as "signal number 0 from pid 0".
func (w *ShutdownSignalWatch) Observed() (ObservedShutdownSignal, bool) {
	if w == nil {
		return ObservedShutdownSignal{}, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.observed == nil {
		return ObservedShutdownSignal{}, false
	}
	return *w.observed, true
}

// operatorLogFields renders the record for the operator log.
//
// A sender the platform did not expose produces `signal_sender` with the reason
// and NO `signal_sender_pid` key at all. Emitting the key with 0 would be the
// forged-evidence shape: the next incident reads a pid that never sent
// anything.
func (o ObservedShutdownSignal) operatorLogFields() map[string]any {
	fields := map[string]any{
		"signal":            o.Signal,
		"signal_at":         o.At.Format(time.RFC3339Nano),
		"signal_parent_pid": o.ParentPID,
	}
	if o.SenderKnown {
		fields["signal_sender_pid"] = o.SenderPID
		return fields
	}
	fields["signal_sender"] = "unknown: " + o.SenderDetail
	return fields
}

// senderSentence is the same fact in prose, for an error an operator reads
// before they ever open the log.
func (o ObservedShutdownSignal) senderSentence() string {
	if o.SenderKnown {
		return fmt.Sprintf("sender pid %d", o.SenderPID)
	}
	return "sender unknown (" + o.SenderDetail + ")"
}

// WatchShutdownSignals is signal.NotifyContext with the signal kept.
//
// NotifyContext cancels and throws the signal away, which is exactly the
// information the 2026-09-06 postmortem had to reconstruct from a transcript.
// The returned watch answers "which signal, observed when, with which parent"
// for whatever reads the cancellation afterwards.
func WatchShutdownSignals(
	parent context.Context,
	signals ...os.Signal,
) (context.Context, *ShutdownSignalWatch, func()) {
	if parent == nil {
		parent = context.Background()
	}
	watch := newShutdownSignalWatch()
	ctx, cancel := context.WithCancel(parent)
	if len(signals) == 0 {
		return ctx, watch, cancel
	}
	received := make(chan os.Signal, 1)
	signal.Notify(received, signals...)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case incoming := <-received:
			watch.record(incoming)
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, watch, func() {
		signal.Stop(received)
		cancel()
		<-stopped
	}
}

// OperatorLogFields renders only observed signal facts for a legacy consumer.
func (o ObservedShutdownSignal) OperatorLogFields() map[string]any { return o.operatorLogFields() }

// SenderSentence never attributes a sender the platform did not expose.
func (o ObservedShutdownSignal) SenderSentence() string { return o.senderSentence() }
