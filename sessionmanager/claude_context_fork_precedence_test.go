package sessionmanager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The B2 flake this file exists for: a resume that the provider refused out loud
// was reported as `claude_context_fork_unverified` on a loaded runner and as
// `claude_session_resume_failed` everywhere else.
//
// Nothing about the provider differed between the two. The provider prints "no
// conversation found with session ID ..." and exits on the next line, so by the
// time the fork-verification loop wakes up, both the refusal waiter and the
// process-exit channel are ready and Go picks between them at random — and the
// exit branch used to return the weaker classification without ever asking
// whether the stream already carried the refusal. The race is worse than a coin
// flip on a loaded machine, where the exit is observed by a goroutine that only
// has to reap a process while the refusal has to travel through the PTY reader.
//
// The fix is a precedence rule — an observed refusal outranks an unproven fork,
// in every branch — plus the ordering that makes the rule answerable: the exit
// is not classified until the PTY reader has drained what the provider printed
// on its way out.

// claudeForkStreamHarness drives the real verifyContextFork over a socket pair
// standing in for the PTY master, so the manager's own writes (the /context
// flush) and the provider's output are one duplex stream, as in production.
//
// A socket pair is deliberate: data written before the peer shuts down its write
// half is delivered before the reader sees EOF, so "the refusal was printed and
// then the stream ended" is a fact of the fixture rather than a timing hope.
type claudeForkStreamHarness struct {
	t      *testing.T
	host   *claudeHost
	peer   *os.File
	peerFD int
	forked *claudeForkMaterialization
	once   sync.Once
	// faults carries provider-side fixture failures back to the test goroutine.
	// The provider side outlives a failed verification — it is still blocked on
	// the manager's next write — so it must never touch *testing.T itself.
	faults chan string
}

func newClaudeForkStreamHarness(t *testing.T, verifyTimeout time.Duration) *claudeForkStreamHarness {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	ptmx := os.NewFile(uintptr(fds[0]), "fake-claude-ptmx")
	peer := os.NewFile(uintptr(fds[1]), "fake-claude-provider")
	host := &claudeHost{
		ptmx:              ptmx,
		scanner:           newClaudeOutputScanner(),
		exited:            make(chan struct{}),
		drained:           make(chan struct{}),
		subscribers:       make(map[chan []byte]struct{}),
		submitDelay:       time.Millisecond,
		forkVerifyTimeout: verifyTimeout,
	}
	// The production reader loop, not a stand-in: it is what hands the scanner
	// its bytes and what closes drained when the provider's output ends.
	go host.pump()
	t.Cleanup(func() {
		_ = ptmx.Close()
		_ = peer.Close()
	})
	return &claudeForkStreamHarness{
		t:      t,
		host:   host,
		peer:   peer,
		peerFD: fds[1],
		faults: make(chan string, 4),
		forked: &claudeForkMaterialization{
			ChildCtxID:         "CTX-260905-child0",
			ForkSessionUUID:    "11111111-2222-4333-8444-555555555555",
			ForkTranscriptPath: filepath.Join(t.TempDir(), "never-written.jsonl"),
		},
	}
}

// awaitFlush blocks until the manager submits the one command it uses to make
// the provider commit its fork. Every scenario keys off it, so the provider side
// can never act before the verification loop has started.
func (h *claudeForkStreamHarness) awaitFlush() bool {
	buffer := make([]byte, 256)
	var seen strings.Builder
	for !strings.Contains(seen.String(), strings.TrimSpace(claudeContextForkFlushCommand)) {
		n, err := h.peer.Read(buffer)
		if n > 0 {
			seen.Write(buffer[:n])
		}
		if err != nil {
			h.fault("the manager never submitted its fork-commit request: %v", err)
			return false
		}
	}
	return true
}

func (h *claudeForkStreamHarness) fault(format string, args ...any) {
	select {
	case h.faults <- fmt.Sprintf(format, args...):
	default:
	}
}

func (h *claudeForkStreamHarness) refuseTheResume() {
	if _, err := h.peer.Write(
		[]byte("No conversation found with session ID 11111111-2222-4333-8444-555555555555\r\n"),
	); err != nil {
		h.fault("writing the provider refusal: %v", err)
	}
}

// endTheStream is the provider's exit as the reader sees it: everything already
// written stays readable and is delivered before EOF.
func (h *claudeForkStreamHarness) endTheStream() {
	if err := syscall.Shutdown(h.peerFD, syscall.SHUT_WR); err != nil {
		h.fault("ending the provider stream: %v", err)
	}
}

func (h *claudeForkStreamHarness) observeProcessExit() {
	h.once.Do(func() { close(h.host.exited) })
}

func (h *claudeForkStreamHarness) verify() error {
	h.t.Helper()
	err := h.host.verifyContextFork(context.Background(), h.forked)
	select {
	case fault := <-h.faults:
		h.t.Fatalf("the provider fixture failed: %s", fault)
	default:
	}
	return err
}

// TestClaudeHostPrefersAnObservedRefusalOverAnUnprovenFork is the regression at
// the production call site: Launch calls verifyContextFork, and this is the
// state Launch was in when CI disagreed with every developer machine.
//
// The exit is observed BEFORE the call, which is the losing order — the refusal
// is still unwritten, so a classification taken at the exit observation can only
// say "fork unverified". The provider then prints its refusal and ends the
// stream. Nothing in the fixture makes the refusal win a race; it wins because
// the exit branch waits for the reader to finish before it classifies, so the
// ordering is a property of the code under test rather than of the machine.
func TestClaudeHostPrefersAnObservedRefusalOverAnUnprovenFork(t *testing.T) {
	harness := newClaudeForkStreamHarness(t, 30*time.Second)
	harness.observeProcessExit()
	go func() {
		if !harness.awaitFlush() {
			return
		}
		harness.refuseTheResume()
		harness.endTheStream()
	}()

	err := harness.verify()
	assertPublicErrorCode(t, err, ClaudeErrorResumeFailed)
}

// TestClaudeHostReportsAnUnprovenForkWhenNothingRefusedTheResume is the negative
// half: the precedence rule must not manufacture a refusal out of an exit. A
// provider that dies silently without committing its fork is an unproven fork
// and must keep saying so.
func TestClaudeHostReportsAnUnprovenForkWhenNothingRefusedTheResume(t *testing.T) {
	harness := newClaudeForkStreamHarness(t, 30*time.Second)
	harness.observeProcessExit()
	go func() {
		if !harness.awaitFlush() {
			return
		}
		harness.endTheStream()
	}()

	err := harness.verify()
	assertPublicErrorCode(t, err, claudeContextForkUnverified)
	if !strings.Contains(err.Error(), "exited before it committed") {
		t.Fatalf("the exit was not named as the reason the fork is unproven: %v", err)
	}
}

// TestClaudeHostReportsAnUnprovenForkWhenTheProviderNeverCommitsIt keeps the
// deadline branch honest: a live provider that simply never writes the fork is
// still a deadline, not a refusal.
func TestClaudeHostReportsAnUnprovenForkWhenTheProviderNeverCommitsIt(t *testing.T) {
	harness := newClaudeForkStreamHarness(t, 750*time.Millisecond)
	go func() { _ = harness.awaitFlush() }()

	err := harness.verify()
	assertPublicErrorCode(t, err, claudeContextForkUnverified)
	if !strings.Contains(err.Error(), "did not produce its forked transcript") {
		t.Fatalf("a live provider that never forked was not reported as a deadline: %v", err)
	}
}

// TestClaudeHostWaitsForTheProviderStreamBeforeClassifyingAnExit pins the
// ordering the precedence rule depends on. awaitStreamDrain must resolve on the
// reader ending and on nothing else: an implementation that returns as soon as
// the process is gone is exactly the implementation that reads a refusal as an
// unproven fork.
func TestClaudeHostWaitsForTheProviderStreamBeforeClassifyingAnExit(t *testing.T) {
	host := &claudeHost{exited: make(chan struct{}), drained: make(chan struct{})}
	close(host.exited)

	returned := make(chan struct{})
	go func() {
		host.awaitStreamDrain(context.Background())
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("the exit classification did not wait for the provider stream to drain")
	case <-time.After(250 * time.Millisecond):
	}
	close(host.drained)
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("the drain wait outlived the stream it waits for")
	}
}

// TestClaudeUnverifiedForkFailureRanksAnObservedRefusalFirst drives the
// precedence rule over its whole input space. The loop reaches it from two
// branches with the same question, so both are stated here as facts rather than
// as timings.
//
// The deadline-with-refusal row is only reachable through this function in a
// test: inside the loop a refusal that arrives after the waiter is armed
// resolves on the waiter or on the exit, and the deadline branch can observe it
// only when the select finds several cases ready at once — a state a test cannot
// force. That is a stated bound on where the row is driven, not on whether the
// rule covers it.
func TestClaudeUnverifiedForkFailureRanksAnObservedRefusalFirst(t *testing.T) {
	for _, test := range []struct {
		name     string
		facts    claudeForkFailureFacts
		wantCode string
		wantText string
	}{
		{
			name:     "a refusal observed as the provider exits",
			facts:    claudeForkFailureFacts{RefusalObserved: true, ProcessExited: true},
			wantCode: ClaudeErrorResumeFailed,
			wantText: "could not find the session",
		},
		{
			name:     "a refusal observed while the provider is still up",
			facts:    claudeForkFailureFacts{RefusalObserved: true},
			wantCode: ClaudeErrorResumeFailed,
			wantText: "could not find the session",
		},
		{
			name:     "an exit with nothing said about the session",
			facts:    claudeForkFailureFacts{ProcessExited: true},
			wantCode: claudeContextForkUnverified,
			wantText: "exited before it committed",
		},
		{
			name:     "a live provider that never committed the fork",
			facts:    claudeForkFailureFacts{},
			wantCode: claudeContextForkUnverified,
			wantText: "did not produce its forked transcript",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			facts := test.facts
			facts.ChildCtxID = "CTX-260905-child0"
			facts.TranscriptPath = "/tmp/fork.jsonl"
			facts.VerifyTimeout = 60 * time.Second
			err := claudeUnverifiedForkFailure(facts)
			assertPublicErrorCode(t, err, test.wantCode)
			if !strings.Contains(err.Error(), test.wantText) {
				t.Errorf("classification %q does not say %q", err.Error(), test.wantText)
			}
		})
	}
}
