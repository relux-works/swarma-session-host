package sessionrun

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type configFixture struct{}

func (configFixture) Snapshot(context.Context) (core.HostSettings, error) {
	return core.HostSettings{}, nil
}

type noticesFixture struct{}

func (noticesFixture) Enqueue(context.Context, string, core.Notice) error     { return nil }
func (noticesFixture) Pending(context.Context, string) ([]core.Notice, error) { return nil, nil }
func (noticesFixture) Acknowledge(context.Context, string, string) error      { return nil }

// The in-process provider fixture exposes the real structural native ports.
// SessionHost, durable codec, epochs and HostInputWriter remain production code.
type codexFixture struct {
	mu       sync.Mutex
	snapshot core.CodexRunnerSnapshot
	writes   int
	started  chan struct{}
	stopped  chan struct{}
	once     sync.Once
	hold     bool
	wrongACK bool
}

func (f *codexFixture) State(context.Context) (core.ProviderHostState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapshot.Stopped {
		return core.ProviderHostStopped, nil
	}
	return core.ProviderHostRunning, nil
}
func (f *codexFixture) Snapshot() (core.RunnerSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := f.snapshot
	return core.RunnerSnapshot{Kind: "codex", Codex: &v}, nil
}
func (f *codexFixture) Stop(context.Context) error {
	f.mu.Lock()
	f.snapshot.Stopped = true
	f.mu.Unlock()
	f.once.Do(func() { close(f.stopped) })
	return nil
}
func (f *codexFixture) SupportsNativeGoals() bool                { return false }
func (f *codexFixture) AcceptsInput(i core.InputSubmission) bool { return i.Payload.CodexTurn != nil }
func (f *codexFixture) ObserveNativeGoal(context.Context) (core.NativeGoalObservation, error) {
	return core.NativeGoalObservation{}, core.ErrHostCapabilityUnavailable
}
func (f *codexFixture) ExecuteInput(_ context.Context, i core.InputSubmission, r core.InputReceipt) (func(context.Context) (core.InputObservation, error), error) {
	f.mu.Lock()
	f.writes++
	f.snapshot.ManagedTurnID = "turn"
	thread := f.snapshot.ThreadID
	wrong := f.wrongACK
	f.mu.Unlock()
	o := core.InputObservation{Receipt: r, Kind: "provider-ack", Correlation: i.Expected.Correlation, CodexTurn: &core.CodexTurnObservation{CommandID: i.Payload.CodexTurn.CommandID, ThreadID: thread, TurnID: "turn", StartedNewTurn: true}}
	if wrong {
		o.Correlation = "foreign"
	}
	return func(context.Context) (core.InputObservation, error) { return o, nil }, nil
}
func (f *codexFixture) RunOwner(ctx context.Context, _ core.ProviderRunRequest) (*core.ProviderRunResult, error) {
	if f.started != nil {
		select {
		case f.started <- struct{}{}:
		default:
		}
	}
	if f.hold {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.stopped:
			return &core.ProviderRunResult{Status: core.ProviderRunFailed, ProviderStatus: "stopped"}, nil
		}
	}
	f.mu.Lock()
	f.snapshot.ManagedTurnID = ""
	f.snapshot.LastCompletedTurnID = "turn"
	f.mu.Unlock()
	return &core.ProviderRunResult{Status: core.ProviderRunComplete, ProviderStatus: "turn_completed", Output: "captured output"}, nil
}

type codexFactory struct{ f *codexFixture }

func (codexFactory) Provider() string { return "codex" }
func (f codexFactory) Launch(context.Context, core.ProviderLaunchRequest) (core.RunnerHost, error) {
	return f.f, nil
}
func (f codexFactory) Restore(context.Context, core.ProviderHostRecord) (core.RunnerHost, error) {
	return f.f, nil
}
func runtimeFixture(t *testing.T) (*Runtime, *core.SessionHost, *codexFixture, *core.Attachment, core.CapturedOwnerRun) {
	t.Helper()
	root := t.TempDir()
	f := &codexFixture{snapshot: core.CodexRunnerSnapshot{Contract: "task-board.codex-provider-host-v1", PID: os.Getpid(), URL: "ws://localhost/fixture", ThreadID: "thread", ProjectDir: root, CodexHome: root, Executable: "isolated-fixture"}, started: make(chan struct{}, 1), stopped: make(chan struct{})}
	runtime := New(nil)
	host, e := core.NewHost(core.HostOptions{OwnerRuns: runtime, Layout: core.Layout{Namespace: "run-fixture", StateDir: root, SessionsDir: filepath.Join(root, "sessions"), LockFile: filepath.Join(root, "manager.lock")}, Config: configFixture{}, Notices: noticesFixture{}, Codec: core.NewLegacyRecordCodec(), Factories: []core.ProviderHostFactory{codexFactory{f}}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(host.Close)
	if e = runtime.BindHost(host); e != nil {
		t.Fatal(e)
	}
	a, e := host.Attach(context.Background(), core.AttachRequest{Owner: "parent/primary", Provider: "codex"})
	if e != nil {
		t.Fatal(e)
	}
	l, _, e := host.Input(context.Background(), a.Session.SessionID)
	if e != nil {
		t.Fatal(e)
	}
	c, e := host.OwnerRunState(context.Background(), l)
	if e != nil {
		t.Fatal(e)
	}
	return runtime, host, f, a, c
}

// Actual host input admission writes once, folds an acknowledged native result,
// and rejects a corrupt committed cut or foreign physical thread before writes.
func TestCapturedCodexOwnerNativeAssignment(t *testing.T) {
	for _, name := range []string{"valid", "corrupt-image", "wrong-thread", "wrong-ack", "blank", "pending-image"} {
		t.Run(name, func(t *testing.T) {
			runtime, _, f, _, c := runtimeFixture(t)
			q := core.ProviderRunRequest{Prompt: "actual work"}
			switch name {
			case "corrupt-image":
				c.Committed.SHA256 = "bad"
			case "wrong-thread":
				f.mu.Lock()
				f.snapshot.ThreadID = "foreign"
				f.mu.Unlock()
			case "wrong-ack":
				f.wrongACK = true
			case "blank":
				q.Prompt = " "
			case "pending-image":
				c.Record.ProviderHostPendingFork = true
			}
			result, e := runtime.RunCapturedOwner(context.Background(), c, q)
			f.mu.Lock()
			writes := f.writes
			f.mu.Unlock()
			if name == "valid" {
				if e != nil || result == nil || result.Status != core.ProviderRunComplete || result.Output != "captured output" || writes != 1 {
					t.Fatalf("captured run %+v %v writes=%d", result, e, writes)
				}
			} else {
				if e == nil || result != nil {
					t.Fatalf("forbidden run %+v %v", result, e)
				}
				want := 0
				if name == "wrong-ack" {
					want = 1
				}
				if writes != want {
					t.Fatalf("forbidden native effects %d != %d", writes, want)
				}
			}
		})
	}
}

// This joins the actual core Stop path while sessionrun waits for its provider.
// Stop must reach native Stop; the same captured writer then has no effect.
func TestCapturedOwnerStopReachesAwaitingProvider(t *testing.T) {
	runtime, host, f, a, c := runtimeFixture(t)
	f.hold = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := runtime.RunCapturedOwner(ctx, c, core.ProviderRunRequest{Prompt: "held"}); done <- e }()
	select {
	case <-f.started:
	case <-ctx.Done():
		t.Fatal("provider observer never entered")
	}
	if _, e := host.Stop(ctx, a.Session.SessionID); e != nil {
		t.Fatal(e)
	}
	select {
	case <-f.stopped:
	default:
		t.Fatal("Stop did not reach provider during run await")
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("run did not join provider stop")
	}
	f.mu.Lock()
	before := f.writes
	f.mu.Unlock()
	_, e := submitCodex(ctx, c.Input, c.Lease, "thread", "stale work")
	if !errors.Is(e, core.ErrStaleInputController) {
		t.Fatalf("retired writer admitted: %v", e)
	}
	f.mu.Lock()
	after := f.writes
	f.mu.Unlock()
	if after != before {
		t.Fatal("retired captured lease wrote native input")
	}
}

// An intervening actual sole-writer commit invalidates the captured image;
// refreshing only the image under the same real controller restores admission.
func TestCapturedRecordChangedBeforeAssignment(t *testing.T) {
	runtime, host, f, _, c := runtimeFixture(t)
	ctx := context.Background()
	_, err := host.CommitRecordMetadata(ctx, core.RecordMetadataCommit{Lease: c.Lease, ID: "metadata", Owner: c.Record.Owner, Provider: c.Record.Provider, BaseSHA256: c.Committed.SHA256, Patch: core.RecordMetadataPatch{OwnerRunError: &core.OwnerRunMetadata{Value: "retained diagnostic"}}})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.RunCapturedOwner(ctx, c, core.ProviderRunRequest{Prompt: "old image"}); result != nil || !errors.Is(err, core.ErrLegacyViewStale) {
		t.Fatalf("stale committed image admitted: %+v %v", result, err)
	}
	if f.writes != 0 {
		t.Fatal("stale committed image wrote")
	}
	fresh, err := host.OwnerRunState(ctx, c.Lease)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.RunCapturedOwner(ctx, fresh, core.ProviderRunRequest{Prompt: "fresh image"}); err != nil || result == nil || result.Status != core.ProviderRunComplete || f.writes != 1 {
		t.Fatalf("fresh image rejected: %+v %v writes=%d", result, err, f.writes)
	}
}

// Recovery allocates a genuinely later durable controller for the same physical
// fixture. The old captured lease cannot refresh itself or write; the new one can.
func TestCapturedControllerReplacementNeverRefreshes(t *testing.T) {
	_, old, f, _, c := runtimeFixture(t)
	old.Close()
	root := f.snapshot.ProjectDir
	runtime := New(nil)
	host, err := core.NewHost(core.HostOptions{OwnerRuns: runtime, Layout: core.Layout{Namespace: "run-fixture", StateDir: root, SessionsDir: filepath.Join(root, "sessions"), LockFile: filepath.Join(root, "manager.lock")}, Config: configFixture{}, Notices: noticesFixture{}, Codec: core.NewLegacyRecordCodec(), Factories: []core.ProviderHostFactory{codexFactory{f}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(host.Close)
	if err = runtime.BindHost(host); err != nil {
		t.Fatal(err)
	}
	if err = host.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, _, err := host.Input(context.Background(), c.Lease.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Epoch <= c.Lease.Epoch {
		t.Fatal("fixture did not replace actual durable controller")
	}
	if result, err := runtime.RunCapturedOwner(context.Background(), c, core.ProviderRunRequest{Prompt: "stale controller"}); result != nil || !errors.Is(err, core.ErrStaleInputController) {
		t.Fatalf("captured controller refreshed: %+v %v", result, err)
	}
	if f.writes != 0 {
		t.Fatal("replaced controller wrote native input")
	}
	fresh, err := host.OwnerRunState(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.RunCapturedOwner(context.Background(), fresh, core.ProviderRunRequest{Prompt: "new controller"}); err != nil || result == nil || result.Status != core.ProviderRunComplete || f.writes != 1 {
		t.Fatalf("current controller refused: %+v %v writes=%d", result, err, f.writes)
	}
}

// The real public host entry performs captured rendering and persists its native
// result through the actual codec/writer, with no unsupported dispatch fallback.
func TestCapturedHostOwnerRunUsesNativeRenderer(t *testing.T) {
	_, host, f, a, c := runtimeFixture(t)
	ctx := context.Background()
	result, err := host.RunOwner(ctx, a.Session.SessionID, a.AttachmentID, core.ProviderRunRequest{Prompt: "host owner assignment"})
	if err != nil || result == nil || result.Status != core.ProviderRunComplete || f.writes != 1 {
		t.Fatalf("actual host/native renderer: %+v %v writes=%d", result, err, f.writes)
	}
	committed, err := host.OwnerRunState(ctx, c.Lease)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Record.OwnerRunError != "" || committed.Record.ProviderHost.Snapshot.Codex == nil || committed.Record.ProviderHost.Snapshot.Codex.LastCompletedTurnID != "turn" {
		t.Fatalf("native outcome not committed: %+v", committed.Record)
	}
	if committed.Committed.SHA256 == c.Committed.SHA256 {
		t.Fatal("native result did not reach actual sole record writer")
	}
}

// A pre-existing goal-only managed turn cannot attest submission of the owner
// prompt. Without the real combined-payload receipt, refuse before join or write.
func TestCapturedCodexGoalOnlyTurnCannotDropOwnerPrompt(t *testing.T) {
	runtime, host, f, _, c := runtimeFixture(t)
	ctx := context.Background()
	f.mu.Lock()
	f.snapshot.ManagedTurnID = "goal-only-turn"
	f.mu.Unlock()
	ref := &core.RevisionRef{ID: "GOAL-SG-TEST", Revision: 1}
	_, err := host.CommitRecordMetadata(ctx, core.RecordMetadataCommit{Lease: c.Lease, ID: "goal-ref", Owner: c.Record.Owner, Provider: c.Record.Provider, BaseSHA256: c.Committed.SHA256, Patch: core.RecordMetadataPatch{AppliedGoalRef: &core.AppliedGoalMetadata{Value: ref}}})
	if err != nil {
		t.Fatal(err)
	}
	current, err := host.OwnerRunState(ctx, c.Lease)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.RunCapturedOwner(ctx, current, core.ProviderRunRequest{Prompt: "must not vanish"})
	if result != nil || !errors.Is(err, core.ErrHostCapabilityUnavailable) || f.writes != 0 {
		t.Fatalf("goal-only turn dropped owner prompt: %+v %v writes=%d", result, err, f.writes)
	}
	select {
	case <-f.started:
		t.Fatal("unbound goal-only observer was joined")
	default:
	}
}
