package sessionrun

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// This fixture uses the transferred PTY backend's real typed capability/input
// shapes. The actual host, codec, controller, goal service and factory are real;
// no PTY subprocess/model or fake native observation is started.
type ptyCapabilityFixture struct {
	mu                   sync.Mutex
	base                 core.MusePTYSnapshot
	provenance           bool
	writes, observations int
	stopped              bool
}

func (f *ptyCapabilityFixture) SupportsNativeGoals() bool { return false }
func (f *ptyCapabilityFixture) NativeIdentity() string    { return f.base.SessionID }
func (f *ptyCapabilityFixture) State(context.Context) (core.ProviderHostState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped {
		return core.ProviderHostStopped, nil
	}
	return core.ProviderHostRunning, nil
}
func (f *ptyCapabilityFixture) Stop(context.Context) error {
	f.mu.Lock()
	f.stopped = true
	f.mu.Unlock()
	return nil
}
func (f *ptyCapabilityFixture) Snapshot() (core.RunnerSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := f.base
	if f.provenance {
		return core.RunnerSnapshot{Kind: "muse-pty-provenance", MusePTYProvenance: &core.MusePTYProvenanceSnapshot{Base: v, Profile: "fixture-profile", ProfileLock: "fixture-lock", ManagedHome: filepath.Dir(v.LogPath), FragmentSHA256: "fixture-fragment"}}, nil
	}
	return core.RunnerSnapshot{Kind: "muse-pty", MusePTY: &v}, nil
}
func (f *ptyCapabilityFixture) ObserveNativeGoal(context.Context) (core.NativeGoalObservation, error) {
	f.mu.Lock()
	f.observations++
	f.mu.Unlock()
	return core.NativeGoalObservation{}, core.ErrHostCapabilityUnavailable
}
func (f *ptyCapabilityFixture) AcceptsInput(in core.InputSubmission) bool {
	p := in.Payload
	return in.ID != "" && in.Lease.SessionID != "" && in.Lease.Epoch != 0 && p.Text != nil && p.MuseGoal == nil && p.MuseTurn == nil && p.CodexTurn == nil && p.ClaudeGoal == nil && strings.TrimSpace(*p.Text) != "" && !strings.ContainsAny(*p.Text, "\x1b\x00\r") && in.Expected.Kind == "written"
}
func (f *ptyCapabilityFixture) ExecuteInput(_ context.Context, in core.InputSubmission, r core.InputReceipt) (func(context.Context) (core.InputObservation, error), error) {
	if !f.AcceptsInput(in) {
		return nil, core.ErrInvalidInput
	}
	f.mu.Lock()
	f.writes++
	f.mu.Unlock()
	o := core.InputObservation{Receipt: r, Kind: "written", Correlation: in.Expected.Correlation}
	return func(context.Context) (core.InputObservation, error) { return o, nil }, nil
}

type ptyCapabilityFactory struct{ native *ptyCapabilityFixture }

func (ptyCapabilityFactory) Provider() string { return "muse" }
func (f ptyCapabilityFactory) Launch(context.Context, core.ProviderLaunchRequest) (core.RunnerHost, error) {
	return f.native, nil
}
func (f ptyCapabilityFactory) Restore(context.Context, core.ProviderHostRecord) (core.RunnerHost, error) {
	return f.native, nil
}

type ptyBindingFixture struct {
	runtime *Runtime
	host    *core.SessionHost
	native  *ptyCapabilityFixture
	service *goal.Service
	factory *budget.Factory
	store   *budget.Store
	caller  *authority
}

func newPTYBindingFixture(t *testing.T, provenance bool) *ptyBindingFixture {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	native := &ptyCapabilityFixture{base: core.MusePTYSnapshot{Contract: "task-board.muse-pty-host-v1", SessionID: "native-pty", PID: os.Getpid(), LogPath: filepath.Join(root, "native.jsonl")}, provenance: provenance}
	caller := &authority{}
	service, e := goal.Open(filepath.Join(root, "goals"), caller)
	if e != nil {
		t.Fatal(e)
	}
	store, e := budget.Open(filepath.Join(root, "budget"))
	if e != nil {
		t.Fatal(e)
	}
	runtime := New(store, core.HostSettings{GoalAckTimeout: time.Second})
	factory, e := budget.NewFactory(ctx, service, store, runtime.AccountingSource())
	if e != nil {
		t.Fatal(e)
	}
	if e = runtime.BindBudget(factory); e != nil {
		t.Fatal(e)
	}
	host, e := core.NewHost(core.HostOptions{OwnerRuns: runtime, Goals: service, GoalBindings: factory, Layout: core.Layout{Namespace: "pty-capability-fixture", StateDir: filepath.Join(root, "host"), SessionsDir: filepath.Join(root, "host", "sessions"), LockFile: filepath.Join(root, "host", "manager.lock")}, Config: configFixture{}, Notices: noticesFixture{}, Codec: core.NewLegacyRecordCodec(), Factories: []core.ProviderHostFactory{ptyCapabilityFactory{native}}})
	if e != nil {
		t.Fatal(e)
	}
	if e = runtime.BindHost(host); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := factory.Close(ctx); e != nil {
			t.Error(e)
		}
		host.Close()
	})
	return &ptyBindingFixture{runtime, host, native, service, factory, store, caller}
}
func (f *ptyBindingFixture) noNativeAccount(t *testing.T, l core.ControllerLease) {
	t.Helper()
	if _, e := f.store.LegacySnapshot(context.Background(), l); !errors.Is(e, core.ErrStaleInputController) {
		t.Fatalf("goal-less PTY allocated fake native accounting: %v", e)
	}
	if _, e := os.Stat(filepath.Join(filepath.Dir(f.native.base.LogPath), "budget", "accounting.json")); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("goal-less PTY wrote an accounting journal: %v", e)
	}
	if f.native.observations != 0 {
		t.Fatal("PTY was probed as a fake native goal observer")
	}
}

// Both actual PTY snapshot kinds can attach/reconcile an absent goal without
// MSP accounting, while ordinary typed input remains usable under its lease.
func TestMusePTYGoalLessAttachUsesActualCapability(t *testing.T) {
	for _, provenance := range []bool{false, true} {
		t.Run(map[bool]string{false: "pty", true: "provenance"}[provenance], func(t *testing.T) {
			f := newPTYBindingFixture(t, provenance)
			ctx := context.Background()
			a, e := f.host.Attach(ctx, core.AttachRequest{Provider: "muse", Owner: "parent/primary"})
			if e != nil {
				t.Fatal(e)
			}
			l, w, e := f.host.Input(ctx, a.Session.SessionID)
			if e != nil {
				t.Fatal(e)
			}
			capability, ok := w.(interface{ SupportsNativeGoals() bool })
			if !ok || capability.SupportsNativeGoals() {
				t.Fatal("actual host writer did not preserve PTY native-goal capability")
			}
			f.noNativeAccount(t, l)
			text := "ordinary work"
			receipt, e := w.Submit(ctx, core.InputSubmission{Lease: l, ID: "ordinary", Payload: core.RenderedInput{Text: &text}, Expected: core.AckExpectation{Kind: "written", Correlation: "opaque"}})
			if e != nil {
				t.Fatal(e)
			}
			observed, e := w.Await(ctx, receipt)
			if e != nil || observed.Kind != "written" || observed.Receipt != receipt || f.native.writes != 1 {
				t.Fatalf("goal-less PTY lost ordinary input: %+v %v writes%d", observed, e, f.native.writes)
			}
			f.noNativeAccount(t, l)
		})
	}
}

// An active goal cannot be promoted or translated into an MSP command on a
// non-native backend. The real canonical command remains pending without ACK.
func TestMusePTYActiveGoalRefusesBeforeNativeAccounting(t *testing.T) {
	f := newPTYBindingFixture(t, true)
	ctx := context.Background()
	a, e := f.host.Attach(ctx, core.AttachRequest{Provider: "muse", Owner: "parent/primary"})
	if e != nil {
		t.Fatal(e)
	}
	l, w, e := f.host.Input(ctx, a.Session.SessionID)
	if e != nil {
		t.Fatal(e)
	}
	_, binding, e := f.factory.BindGoal(ctx, l, "muse", w)
	if e != nil {
		t.Fatal(e)
	}
	f.caller.facts = goal.VerifiedCaller{SessionID: l.SessionID, Principal: "holder", Holder: "holder", Generation: 1, Assignment: true, UnlimitedBudget: true}
	_, e = f.service.Execute(ctx, goal.Command{EnvelopeID: "unsupported-active", SessionID: l.SessionID, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "work", Condition: "holder accepts", Budget: goal.Budget{Tokens: 50}}}, binding)
	if !errors.Is(e, core.ErrHostCapabilityUnavailable) {
		t.Fatalf("unsupported PTY goal not refused precisely: %v", e)
	}
	head, e := f.service.Snapshot(l.SessionID, goal.Assignment)
	if e != nil {
		t.Fatal(e)
	}
	if head.Applied != nil || head.AppliedReceipt != nil || head.Latest.Ref.Revision != 1 {
		t.Fatalf("unsupported goal promoted: %+v", head)
	}
	if _, b, e := f.factory.BindGoal(ctx, l, "muse", w); b != nil || !errors.Is(e, core.ErrHostCapabilityUnavailable) {
		t.Fatalf("active/pending PTY head admitted new native binding: %v", e)
	}
	if f.native.writes != 0 {
		t.Fatal("unsupported PTY goal wrote native input")
	}
	f.noNativeAccount(t, l)
}

// Reused provisional writers preserve the same actual capability; staging
// and commit do not manufacture an MSP observer/account for a goal-less PTY.
func TestMusePTYGoalLessStagedBindingPreservesCapability(t *testing.T) {
	f := newPTYBindingFixture(t, false)
	ctx := context.Background()
	if _, e := f.host.Attach(ctx, core.AttachRequest{Provider: "muse", Owner: "parent/primary"}); e != nil {
		t.Fatal(e)
	}
	p, e := f.host.PrepareAttach(ctx, core.AttachRequest{Provider: "muse", Owner: "parent/primary"})
	if e != nil {
		t.Fatal(e)
	}
	capability, ok := p.Writer.(interface{ SupportsNativeGoals() bool })
	if !ok || capability.SupportsNativeGoals() {
		t.Fatal("staged writer lost actual PTY capability")
	}
	owner, b, e := f.factory.BindGoal(ctx, p.Lease, "muse", p.Writer)
	if e != nil {
		t.Fatal(e)
	}
	activation, e := f.service.Reconcile(owner, b)
	if e != nil || activation == nil || activation.Pending || activation.Ref != nil {
		t.Fatalf("goal-less staged binding: %+v %v", activation, e)
	}
	if _, e = f.host.CommitAttach(ctx, core.AttachmentCommit{Token: p.Token, Lease: p.Lease, BaseSHA256: p.BaseSHA256}); e != nil {
		t.Fatal(e)
	}
	if f.native.writes != 0 {
		t.Fatal("goal-less staged attach wrote native input")
	}
	f.noNativeAccount(t, p.Lease)
}

// Plain codec has no fresh staged discard acknowledgement. That separate
// optional admission must refuse honestly; capability dispatch does not fake it.
func TestMusePTYFreshStagingRequiresActualDiscardCapability(t *testing.T) {
	f := newPTYBindingFixture(t, false)
	if _, e := f.host.PrepareAttach(context.Background(), core.AttachRequest{Provider: "muse", Owner: "parent/primary"}); !errors.Is(e, core.ErrHostCapabilityUnavailable) {
		t.Fatalf("fresh staging without discard supplier admitted: %v", e)
	}
	sessions, e := f.host.List(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if len(sessions) != 0 || f.native.writes != 0 || f.native.observations != 0 {
		t.Fatal("fresh staged refusal published state or performed native IO")
	}
}
