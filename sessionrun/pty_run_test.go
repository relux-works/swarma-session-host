package sessionrun

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The provider is isolated typed IO; real host admission, durable codec,
// controller and writer execute. This is not native executable qualification.
type capturedPTYFixture struct {
	ptyCapabilityFixture
	mode             string
	prompt, observed string
	entered          chan struct{}
	stoppedRun       chan struct{}
	stopOnce         sync.Once
	hold             bool
}

func (f *capturedPTYFixture) AcceptsInput(in core.InputSubmission) bool {
	if f.mode == "preflight-refusal" {
		return false
	}
	return in.Payload.Text != nil && strings.TrimSpace(*in.Payload.Text) != "" && in.Payload.MuseGoal == nil && in.Payload.MuseTurn == nil && in.Payload.CodexTurn == nil && in.Payload.ClaudeGoal == nil && in.Expected.Kind == "run-observed-after-snapshot" && in.Expected.Correlation == f.base.SessionID
}
func (f *capturedPTYFixture) ExecuteInput(_ context.Context, in core.InputSubmission, r core.InputReceipt) (func(context.Context) (core.InputObservation, error), error) {
	if !f.AcceptsInput(in) {
		return nil, core.ErrInvalidInput
	}
	f.mu.Lock()
	f.writes++
	f.prompt = *in.Payload.Text
	mode := f.mode
	f.mu.Unlock()
	o := core.InputObservation{Receipt: r, Kind: "run-observed-after-snapshot", Correlation: in.Expected.Correlation, EvidenceClass: core.InputEvidenceRunObservedAfterSnapshot, PTYRun: &core.PTYRunObservation{RunID: "actual-pty-run"}}
	if mode == "wrong-ack" {
		o.Correlation = "foreign"
	}
	return func(context.Context) (core.InputObservation, error) { return o, nil }, nil
}
func (f *capturedPTYFixture) ObserveOwnerRun(ctx context.Context, id string) (*core.ProviderRunResult, error) {
	f.mu.Lock()
	f.observed = id
	f.mu.Unlock()
	if f.entered != nil {
		f.entered <- struct{}{}
	}
	if f.hold {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.stoppedRun:
			return &core.ProviderRunResult{Status: core.ProviderRunFailed, ProviderStatus: "transport_lost", Recoverable: true}, nil
		}
	}
	return &core.ProviderRunResult{Status: core.ProviderRunComplete, ProviderStatus: "completed", Output: "pty result"}, nil
}
func (f *capturedPTYFixture) Stop(ctx context.Context) error {
	err := f.ptyCapabilityFixture.Stop(ctx)
	f.stopOnce.Do(func() { close(f.stoppedRun) })
	return err
}

// A legacy runner would write the prompt twice. This fixture exposes it solely
// to prove that the renderer never chooses that physical-write entry point.
func (f *capturedPTYFixture) RunOwner(context.Context, core.ProviderRunRequest) (*core.ProviderRunResult, error) {
	return nil, errors.New("unleased legacy RunOwner invoked")
}

type capturedPTYFactory struct{ native *capturedPTYFixture }

func (capturedPTYFactory) Provider() string { return "muse" }
func (f capturedPTYFactory) Launch(context.Context, core.ProviderLaunchRequest) (core.RunnerHost, error) {
	return f.native, nil
}
func (f capturedPTYFactory) Restore(context.Context, core.ProviderHostRecord) (core.RunnerHost, error) {
	return f.native, nil
}
func newCapturedPTY(t *testing.T, provenance bool) (*Runtime, *core.SessionHost, *capturedPTYFixture, *core.Attachment, core.CapturedOwnerRun) {
	t.Helper()
	root := t.TempDir()
	f := &capturedPTYFixture{ptyCapabilityFixture: ptyCapabilityFixture{base: core.MusePTYSnapshot{Contract: "task-board.muse-pty-host-v1", SessionID: "physical-pty", PID: os.Getpid(), LogPath: filepath.Join(root, "native.jsonl")}, provenance: provenance}, entered: make(chan struct{}, 1), stoppedRun: make(chan struct{})}
	r := New(nil)
	h, err := core.NewHost(core.HostOptions{OwnerRuns: r, Layout: core.Layout{Namespace: "captured-pty-fixture", StateDir: root, SessionsDir: filepath.Join(root, "sessions"), LockFile: filepath.Join(root, "manager.lock")}, Config: configFixture{}, Notices: noticesFixture{}, Codec: core.NewLegacyRecordCodec(), Factories: []core.ProviderHostFactory{capturedPTYFactory{f}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	if err = r.BindHost(h); err != nil {
		t.Fatal(err)
	}
	a, err := h.Attach(context.Background(), core.AttachRequest{Provider: "muse", Owner: "parent/primary"})
	if err != nil {
		t.Fatal(err)
	}
	l, _, err := h.Input(context.Background(), a.Session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	c, err := h.OwnerRunState(context.Background(), l)
	if err != nil {
		t.Fatal(err)
	}
	return r, h, f, a, c
}

// Both real PTY snapshot shapes dispatch one leased Text assignment, then fold
// exactly its observed run without the legacy physical RunOwner write path.
func TestCapturedPTYOwnerRunDispatch(t *testing.T) {
	for _, provenance := range []bool{false, true} {
		t.Run(map[bool]string{false: "pty", true: "provenance"}[provenance], func(t *testing.T) {
			_, h, f, a, c := newCapturedPTY(t, provenance)
			result, err := h.RunOwner(context.Background(), a.Session.SessionID, a.AttachmentID, core.ProviderRunRequest{Prompt: "  actual owner\npayload  "})
			if err != nil || result == nil || result.Output != "pty result" || f.writes != 1 || f.prompt != "  actual owner\npayload  " || f.observed != "actual-pty-run" {
				t.Fatalf("captured PTY dispatch: %+v %v writes%d prompt%q observed%q", result, err, f.writes, f.prompt, f.observed)
			}
			current, err := h.OwnerRunState(context.Background(), c.Lease)
			if err != nil || current.Record.OwnerRunError != "" {
				t.Fatalf("durable PTY outcome: %+v %v", current.Record, err)
			}
		})
	}
}

// Foreign physical identity and stale committed cuts have no physical effects;
// an actually submitted wrong ACK remains unacknowledged and is not observed.
func TestCapturedPTYOwnerRunRefusals(t *testing.T) {
	for _, mode := range []string{"wrong-session", "wrong-log", "corrupt-image", "pending-image", "wrong-ack"} {
		t.Run(mode, func(t *testing.T) {
			r, _, f, _, c := newCapturedPTY(t, false)
			f.mode = mode
			want := core.ErrLegacyViewStale
			writes := 0
			switch mode {
			case "wrong-session":
				f.base.SessionID = "foreign"
			case "wrong-log":
				f.base.LogPath += "-foreign"
			case "corrupt-image":
				c.Committed.SHA256 = "bad"
			case "pending-image":
				c.Record.ProviderHostPendingFork = true
				want = core.ErrStaleInputController
			case "wrong-ack":
				want = core.ErrInputAcknowledgementMismatch
				writes = 1
			}
			result, err := r.RunCapturedOwner(context.Background(), c, core.ProviderRunRequest{Prompt: "actual work"})
			if !errors.Is(err, want) || result != nil || f.writes != writes || f.observed != "" {
				t.Fatalf("forbidden PTY effect: result%+v err%v writes%d observed%q", result, err, f.writes, f.observed)
			}
		})
	}
}

// Real Stop must reach the physical provider while its read-only run observer
// waits; the retired captured writer subsequently refuses without another write.
func TestCapturedPTYStopReachesAwaitingProvider(t *testing.T) {
	r, h, f, a, c := newCapturedPTY(t, true)
	f.hold = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := r.RunCapturedOwner(ctx, c, core.ProviderRunRequest{Prompt: "held"}); done <- err }()
	select {
	case <-f.entered:
	case <-ctx.Done():
		t.Fatal("PTY observer never entered")
	}
	if _, err := h.Stop(ctx, a.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.stoppedRun:
	default:
		t.Fatal("Stop did not reach provider during PTY await")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("PTY observer did not join Stop")
	}
	_, err := submitPTY(ctx, c.Input, c.Lease, "physical-pty", "stale")
	if !errors.Is(err, core.ErrStaleInputController) || f.writes != 1 {
		t.Fatalf("retired PTY writer wrote: %v writes%d", err, f.writes)
	}
}
