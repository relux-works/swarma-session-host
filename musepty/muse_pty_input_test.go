package musepty

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

type musePTYInputBackend interface {
	SupportsNativeGoals() bool
	AcceptsInput(sm.InputSubmission) bool
	ExecuteInput(context.Context, sm.InputSubmission, sm.InputReceipt) (func(context.Context) (sm.InputObservation, error), error)
	ObserveNativeGoal(context.Context) (sm.NativeGoalObservation, error)
}

type musePTYLockContext struct {
	context.Context
	checks chan struct{}
}

func (c musePTYLockContext) Err() error {
	select {
	case c.checks <- struct{}{}:
	default:
	}
	return c.Context.Err()
}

func musePTYWriter(t *testing.T, h *musePTYHost) musePTYInputBackend {
	t.Helper()
	w, ok := any(h).(musePTYInputBackend)
	if !ok {
		t.Fatal("actual Muse PTY host lacks neutral Text backend")
	}
	return w
}

func musePTYInput(h *musePTYHost, kind sm.AckKind) (sm.InputSubmission, sm.InputReceipt) {
	text := "Observe the current task and report its progress."
	in := sm.InputSubmission{ID: "pty-input", Lease: sm.ControllerLease{SessionID: "logical-host", Epoch: 7}, Payload: sm.RenderedInput{Text: &text}, Expected: sm.AckExpectation{Kind: kind, Correlation: h.NativeIdentity()}}
	return in, sm.InputReceipt{ID: in.ID, SessionID: in.Lease.SessionID, Epoch: in.Lease.Epoch}
}

func musePTYNoInput(t *testing.T, h *musePTYHost, home string) {
	t.Helper()
	for _, name := range []string{"peer-paste", "peer-submit"} {
		if _, err := os.Stat(filepath.Join(home, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("refused input changed %s: %v", name, err)
		}
	}
	h.mu.Lock()
	pending := h.pendingRun
	h.mu.Unlock()
	if h.held != "" || pending {
		t.Fatalf("refused input retained state: held=%q pending=%v", h.held, pending)
	}
}

// The real native PTY fixture supplies a fresh run-start in its owned log.
// D14 names this weaker evidence explicitly. These backend checks do not
// claim public Host lease/epoch admission coverage or post-Enter causality.
func TestMusePTYWriterObservedSessionAck(t *testing.T) {
	for _, kind := range []sm.AckKind{"run-observed-after-snapshot", "written"} {
		t.Run(string(kind), func(t *testing.T) {
			h, home := musePeer(t, "idle")
			w := musePTYWriter(t, h)
			in, receipt := musePTYInput(h, kind)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			wait, err := w.ExecuteInput(ctx, in, receipt)
			if err != nil || wait == nil {
				t.Fatalf("valid Text refused: %v", err)
			}
			museWaitScripted(t, h, "initial native submission", func() bool {
				b, err := os.ReadFile(filepath.Join(home, "peer-submit"))
				return err == nil && string(b) == "1:"+*in.Payload.Text
			})
			before, err := os.ReadFile(filepath.Join(home, "peer-submit"))
			if err != nil || !strings.HasPrefix(string(before), "1:") {
				t.Fatalf("native submission absent: %q %v", before, err)
			}
			observed, err := wait(ctx)
			if err != nil || observed.Receipt != receipt || observed.Kind != sm.InputObservationKind(kind) || observed.Correlation != h.NativeIdentity() || observed.MuseTurn != nil || observed.CodexTurn != nil {
				t.Fatalf("owned native run acknowledgement mismatch: %+v %v", observed, err)
			}
			fold, err := h.fold()
			if err != nil || len(fold.Started) == 0 || fold.SessionID != observed.Correlation {
				t.Fatalf("ack lacks actual owned new run: %+v %v", fold, err)
			}
			if len(fold.Started) != 1 {
				t.Fatalf("acknowledgement waiter wrote native input: observed %d native runs", len(fold.Started))
			}
			cancelled, stop := context.WithCancel(context.Background())
			stop()
			if _, err := wait(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled waiter fabricated ack: %v", err)
			}
			if again, err := wait(ctx); err != nil || again != observed {
				t.Fatalf("read-only repeat changed observation: %+v %v", again, err)
			}
			// A native redraw is a FIFO barrier: the peer has consumed
			// every preceding byte from the waiters before it marks it.
			// It submits no instruction and creates no run-start itself.
			if _, err := h.ptmx.Write([]byte{'\f'}); err != nil {
				t.Fatal(err)
			}
			// The marker is written after rendering, so output notifications
			// cannot order its creation. Poll that actual filesystem record.
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				b, err := os.ReadFile(filepath.Join(home, "peer-redraw"))
				if err == nil && string(b) == "rendered" {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("native waiter I/O barrier unobserved: %v", ctx.Err())
				case <-tick.C:
				}
			}
			after, err := os.ReadFile(filepath.Join(home, "peer-submit"))
			if err != nil || string(after) != string(before) {
				t.Fatalf("acknowledgement waiter wrote native input: %q %v", after, err)
			}
		})
	}
}

// Every rejected shape stays before paste, Enter and held/pending mutations;
// the adjacent actual-provider positive prevents rejecting all Text.
func TestMusePTYWriterAdmissionRefusals(t *testing.T) {
	h, home := musePeer(t, "idle")
	w := musePTYWriter(t, h)
	if w.SupportsNativeGoals() {
		t.Fatal("PTY invented native goal capability")
	}
	if _, err := w.ObserveNativeGoal(context.Background()); !errors.Is(err, sm.ErrHostCapabilityUnavailable) {
		t.Fatalf("PTY invented native goal observation: %v", err)
	}
	for _, name := range []string{"foreign-correlation", "empty-correlation", "empty-text", "control-text", "muse-goal", "muse-turn", "codex-turn", "mixed", "unknown-kind", "empty-id", "empty-session", "zero-epoch", "foreign-receipt"} {
		t.Run(name, func(t *testing.T) {
			in, receipt := musePTYInput(h, "run-observed-after-snapshot")
			want := sm.ErrInvalidInput
			switch name {
			case "foreign-correlation":
				in.Expected.Correlation = "foreign-native-session"
			case "empty-correlation":
				in.Expected.Correlation = ""
			case "empty-text":
				*in.Payload.Text = "  "
			case "control-text":
				*in.Payload.Text = "escape\x1b[3J"
			case "muse-goal":
				in.Payload = sm.RenderedInput{MuseGoal: &sm.MuseGoalCommand{CommandID: in.ID, Clear: &sm.MuseGoalClear{}}}
			case "muse-turn":
				in.Payload = sm.RenderedInput{MuseTurn: &sm.MuseTurnCommand{CommandID: in.ID}}
			case "codex-turn":
				in.Payload = sm.RenderedInput{CodexTurn: &sm.CodexTurnCommand{CommandID: in.ID}}
			case "mixed":
				in.Payload.MuseGoal = &sm.MuseGoalCommand{CommandID: in.ID}
			case "unknown-kind":
				in.Expected.Kind = "caller-asserted"
			case "empty-id":
				in.ID, receipt.ID = "", ""
			case "empty-session":
				in.Lease.SessionID, receipt.SessionID = "", ""
			case "zero-epoch":
				in.Lease.Epoch, receipt.Epoch = 0, 0
			case "foreign-receipt":
				receipt.ID = "foreign"
				want = sm.ErrInputAcknowledgementMismatch
			}
			if _, err := w.ExecuteInput(context.Background(), in, receipt); !errors.Is(err, want) {
				t.Fatalf("invalid input admitted or wrong refusal: %v", err)
			}
			musePTYNoInput(t, h, home)
		})
	}
}

// The backend uses the original native gate, including busy and trust states.
func TestMusePTYWriterBusyNoWrite(t *testing.T) {
	for _, mode := range []string{"busy", "trust", "queued"} {
		t.Run(mode, func(t *testing.T) {
			h, home := musePeer(t, mode)
			in, receipt := musePTYInput(h, "run-observed-after-snapshot")
			if _, err := musePTYWriter(t, h).ExecuteInput(context.Background(), in, receipt); !errors.Is(err, ErrMusePTYDeferred) {
				t.Fatalf("busy native PTY admitted input: %v", err)
			}
			musePTYNoInput(t, h, home)
		})
	}
}

// This fixture establishes unsafe native evidence independently of the safety
// predicate under mutation. Original legacy fixtures/assertions remain intact.
func TestMusePTYWriterIndependentRefusalFixture(t *testing.T) {
	for _, mode := range []string{"trust", "queued"} {
		t.Run(mode, func(t *testing.T) {
			plan, home := musePeerPlan(t, mode, "")
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			h, err := StartMusePTYHost(ctx, plan)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Stop(context.Background()) })
			museInjectTestSeams(t, h)
			museWaitScripted(t, h, "independent unsafe native fixture", func() bool {
				f, err := h.fold()
				if err != nil {
					return false
				}
				h.mu.Lock()
				screen := strings.Join(h.vt.lines(), "\n")
				h.mu.Unlock()
				if mode == "trust" {
					return strings.Contains(screen, "Do you trust this workspace?") && strings.Contains(screen, "2 Quit")
				}
				return len(f.Queued) == 1 && strings.Contains(screen, "echo · ~ · Auto-review")
			})
			museAdvanceTestClockPastQuiet(h)
			in, receipt := musePTYInput(h, "run-observed-after-snapshot")
			wait, err := musePTYWriter(t, h).ExecuteInput(ctx, in, receipt)
			if wait != nil {
				_, _ = wait(ctx)
			}
			if !errors.Is(err, ErrMusePTYDeferred) {
				t.Fatalf("known unsafe native fixture admitted input: %v", err)
			}
			musePTYNoInput(t, h, home)
		})
	}
}

// A cancelled lock wait returns before its owner releases the lock and leaves
// no worker or physical write behind. Both the operation and worker are joined.
func TestMusePTYWriterCancelledLockWait(t *testing.T) {
	h, home := musePeer(t, "idle")
	w := musePTYWriter(t, h)
	in, receipt := musePTYInput(h, "run-observed-after-snapshot")
	h.writeMu.Lock()
	locked := true
	defer func() {
		if locked {
			h.writeMu.Unlock()
		}
	}()
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := musePTYLockContext{Context: base, checks: make(chan struct{}, 64)}
	result := make(chan error, 1)
	go func() { _, err := w.ExecuteInput(ctx, in, receipt); result <- err }()
	// One check is admission; the next is the attempted mutex acquisition.
	// A deliberately blocking mutant reaches neither the second check nor
	// cancellation; the watchdog then lets the cancellation assertion expose it.
	watchdog := time.NewTimer(60 * time.Second)
	for n := 0; n < 2; n++ {
		select {
		case <-ctx.checks:
		case <-watchdog.C:
			n = 2
		}
	}
	watchdog.Stop()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled lock wait: %v", err)
		}
	case <-time.After(60 * time.Second):
		h.writeMu.Unlock()
		locked = false
		<-result
		t.Fatal("cancelled lock acquisition remained blocked")
	}
	h.writeMu.Unlock()
	locked = false
	musePTYNoInput(t, h, home)
}

// A successful Enter without a new owned run-start is not a provider receipt.
// Cancelling the operation joins its bounded native observer with no extra I/O.
func TestMusePTYWriterNoAckCancellation(t *testing.T) {
	for _, mode := range []string{"no-ack", "completed-before-submit"} {
		t.Run(mode, func(t *testing.T) {
			h, home := musePeer(t, mode)
			in, receipt := musePTYInput(h, "run-observed-after-snapshot")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wait, err := musePTYWriter(t, h).ExecuteInput(ctx, in, receipt)
			if err != nil || wait == nil {
				t.Fatalf("physical submission failed: %v", err)
			}
			museWaitScripted(t, h, "actual native Enter", func() bool {
				b, err := os.ReadFile(filepath.Join(home, "peer-submit"))
				return err == nil && string(b) == "1:"+*in.Payload.Text
			})
			if mode == "completed-before-submit" {
				f, err := h.fold()
				if err != nil || len(f.Started) != 1 || !f.idle() {
					t.Fatalf("pre-submit completed native run missing: %+v %v", f, err)
				}
				bounded, stop := context.WithTimeout(context.Background(), time.Second)
				_, err = wait(bounded)
				stop()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("pre-submit unrelated run fabricated provider ack: %v", err)
				}
			}
			cancel()
			joined, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			if _, err := wait(joined); !errors.Is(err, context.Canceled) {
				t.Fatalf("unobserved run-start fabricated provider ack: %v", err)
			}
			b, err := os.ReadFile(filepath.Join(home, "peer-submit"))
			if err != nil || !strings.HasPrefix(string(b), "1:") {
				t.Fatalf("expected one native submit: %q %v", b, err)
			}
		})
	}
}
