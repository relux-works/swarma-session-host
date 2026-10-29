package sessionrun

import (
	"context"
	"errors"
	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type journalCodexNative struct {
	*codexFixture
	before func(core.InputSubmission, core.InputReceipt)
}

func (f *journalCodexNative) ExecuteInput(ctx context.Context, i core.InputSubmission, r core.InputReceipt) (func(context.Context) (core.InputObservation, error), error) {
	if f.before != nil {
		f.before(i, r)
	}
	return f.codexFixture.ExecuteInput(ctx, i, r)
}

// This transparent observation probe delegates every operation and actual
// receipt to the real host writer. It inspects the service at Await return,
// after that service has journaled Submit's returned receipt; the native worker
// itself may observe earlier. It creates no receipt or acknowledgement.
type goalAwaitProbe struct {
	parent  core.HostInputWriter
	observe func(core.InputObservation)
}

func (w goalAwaitProbe) Submit(ctx context.Context, i core.InputSubmission) (core.InputReceipt, error) {
	return w.parent.Submit(ctx, i)
}
func (w goalAwaitProbe) Await(ctx context.Context, r core.InputReceipt) (core.InputObservation, error) {
	o, err := w.parent.Await(ctx, r)
	if err == nil {
		w.observe(o)
	}
	return o, err
}
func (w goalAwaitProbe) Cancel(ctx context.Context, r core.InputReceipt) error {
	return w.parent.Cancel(ctx, r)
}

type journalCodexFactory struct{ native *journalCodexNative }

func (journalCodexFactory) Provider() string { return "codex" }
func (f journalCodexFactory) Launch(context.Context, core.ProviderLaunchRequest) (core.RunnerHost, error) {
	return f.native, nil
}
func (f journalCodexFactory) Restore(context.Context, core.ProviderHostRecord) (core.RunnerHost, error) {
	return f.native, nil
}

// This isolated composition selects the actual managed renderer from the
// fixture's explicit original owner-run mode. It supplies no canonical verdict
// or receipt; all admission/journaling/checks remain real service/host code.
// Production hydration of the retained owner-run fact is still unqualified.
type pendingCodexComposition struct {
	runtime *Runtime
	budgets *budget.Factory
	capture *core.CapturedOwnerRun
}

func (f *pendingCodexComposition) BindGoal(ctx context.Context, l core.ControllerLease, provider string, w core.HostInputWriter) (core.OwnerKey, core.GoalBinding, error) {
	if f.capture == nil {
		return f.runtime.BindGoal(ctx, l, provider, w)
	}
	return f.budgets.BindCodexGoal(ctx, l, w, &codexRevisionFactory{runtime: f.runtime, capture: *f.capture})
}
func (f *pendingCodexComposition) BindOwnerRunGoal(ctx context.Context, c core.CapturedOwnerRun, q core.ProviderRunRequest) (core.OwnerKey, core.GoalBinding, error) {
	return f.runtime.BindOwnerRunGoal(ctx, c, q)
}

type pendingCodexSystem struct {
	runtime    *Runtime
	host       *core.SessionHost
	native     *journalCodexNative
	service    *goal.Service
	budgets    *budget.Factory
	attachment *core.Attachment
	capture    core.CapturedOwnerRun
	caller     *authority
}

func newPendingCodexSystem(t *testing.T) *pendingCodexSystem {
	return newPendingCodexSystemWithBinding(t, true)
}
func newPendingCodexSystemWithBinding(t *testing.T, combined bool) *pendingCodexSystem {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	caller := &authority{}
	service, err := goal.Open(filepath.Join(root, "goals"), caller)
	if err != nil {
		t.Fatal(err)
	}
	store, err := budget.Open(filepath.Join(root, "budget"))
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(store)
	budgets, err := budget.NewFactory(ctx, service, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.BindBudget(budgets); err != nil {
		t.Fatal(err)
	}
	native := &journalCodexNative{codexFixture: &codexFixture{snapshot: core.CodexRunnerSnapshot{Contract: "task-board.codex-provider-host-v1", PID: os.Getpid(), URL: "ws://localhost/fixture", ThreadID: "thread", ProjectDir: root, CodexHome: root, Executable: "isolated-fixture"}, started: make(chan struct{}, 1), stopped: make(chan struct{})}}
	state := filepath.Join(root, "host")
	composition := &pendingCodexComposition{runtime: runtime, budgets: budgets}
	var goalBindings core.GoalBindingFactory = composition
	if !combined {
		goalBindings = budgets
	}
	host, err := core.NewHost(core.HostOptions{OwnerRuns: runtime, GoalBindings: goalBindings, Goals: service, Layout: core.Layout{Namespace: "pending-codex", StateDir: state, SessionsDir: filepath.Join(state, "sessions"), LockFile: filepath.Join(state, "manager.lock")}, Config: configFixture{}, Notices: noticesFixture{}, Codec: core.NewLegacyRecordCodec(), Factories: []core.ProviderHostFactory{journalCodexFactory{native}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(host.Close)
	t.Cleanup(func() {
		if err := budgets.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if err = runtime.BindHost(host); err != nil {
		t.Fatal(err)
	}
	a, err := host.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	lease, _, err := host.Input(ctx, a.Session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := host.OwnerRunState(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	composition.capture = &capture
	caller.facts = goal.VerifiedCaller{SessionID: lease.SessionID, Principal: "holder", Holder: "holder", Generation: 1, Assignment: true, Operator: true, UnlimitedBudget: true}
	return &pendingCodexSystem{runtime, host, native, service, budgets, a, capture, caller}
}
func (f *pendingCodexSystem) stage(t *testing.T, unlimited bool) goal.Receipt {
	t.Helper()
	ctx := context.Background()
	// Explicit isolated renderer setup models the original retained owner-run
	// fact. This does not qualify production hydration of that missing fact.
	renderer := &codexRevisionFactory{runtime: f.runtime, capture: f.capture}
	_, binding, err := f.budgets.BindCodexGoal(ctx, f.capture.Lease, f.capture.Input, renderer)
	if err != nil {
		t.Fatal(err)
	}
	selected := goal.Budget{Tokens: 75}
	if unlimited {
		selected = goal.Budget{Unlimited: true}
	}
	receipt, err := f.service.Execute(ctx, goal.Command{EnvelopeID: "admitted-codex", SessionID: f.capture.Lease.SessionID, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "actual work", Condition: "holder accepts", Budget: selected}}, binding)
	if !errors.Is(err, goal.ErrNativeDeferred) || !receipt.Pending || f.native.writes != 0 {
		t.Fatalf("staging was not truthful no-write Pending: %+v %v writes%d", receipt, err, f.native.writes)
	}
	head, err := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
	if err != nil || head.Applied != nil || head.Attempt != nil || head.Acknowledged != nil {
		t.Fatalf("staging invented journal effect: %+v %v", head, err)
	}
	return receipt
}

// Real admitted full revisions are staged with no input. The SAME durable goal
// service journals one combined native Start before input; only actual ACK may
// promote. A native completed turn keeps the stored goal active.
func TestPendingCodexCombinedServiceJournal(t *testing.T) {
	for _, unlimited := range []bool{false, true} {
		t.Run(map[bool]string{false: "finite", true: "none"}[unlimited], func(t *testing.T) {
			f := newPendingCodexSystem(t)
			ref := f.stage(t, unlimited).Ref
			f.native.before = func(in core.InputSubmission, r core.InputReceipt) {
				head, err := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
				if err != nil || head.Attempt == nil || head.Attempt.Input.ID != in.ID || head.Attempt.Receipt != nil || head.Applied != nil || head.Acknowledged != nil || head.Attempt.Input.Payload.CodexTurn.Start.Text != in.Payload.CodexTurn.Start.Text {
					t.Errorf("combined bytes not journaled before input: %+v %v", head, err)
				}
				text := in.Payload.CodexTurn.Start.Text
				want := "75 tokens"
				if unlimited {
					want = "none"
				}
				if !strings.Contains(text, "Token budget: "+want) || !strings.HasSuffix(text, "\n\nowner work") {
					t.Errorf("combined directive/owner work lost: %q", text)
				}
			}
			observe := func(observed core.InputObservation) {
				head, err := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
				if err != nil || head.Attempt == nil || head.Attempt.Receipt == nil || *head.Attempt.Receipt != observed.Receipt || head.Applied != nil || head.Acknowledged != nil || observed.CodexTurn == nil || observed.CodexTurn.ThreadID != "thread" {
					t.Errorf("actual receipt not retained before ACK/promotion: %+v %v", head, err)
				}
			}
			_, binding, err := f.budgets.BindCodexGoal(context.Background(), f.capture.Lease, goalAwaitProbe{parent: f.capture.Input, observe: observe}, &codexRevisionFactory{runtime: f.runtime, capture: f.capture, prompt: strings.TrimSpace("  owner work  ")})
			if err != nil {
				t.Fatal(err)
			}
			activation, err := f.service.Reconcile(goal.Owner(f.capture.Lease.SessionID, goal.Assignment), binding)
			head, readErr := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
			if err != nil || readErr != nil || activation.Pending || head.Applied == nil || *head.Applied != ref || head.AppliedReceipt == nil || head.Attempt != nil || len(head.Applications) != 1 || f.native.writes != 1 {
				t.Fatalf("combined receipt did not promote: %+v %+v %v %v writes%d", activation, head, err, readErr, f.native.writes)
			}
		})
	}
}

// A retained wrong native ACK cannot be healed by a later good backend or a
// changed owner prompt. The exact persisted input/receipt remains unresolved.
func TestPendingCodexWrongACKRetryNoDuplicate(t *testing.T) {
	f := newPendingCodexSystem(t)
	f.stage(t, false)
	f.native.wrongACK = true
	_, b, err := f.runtime.BindOwnerRunGoal(context.Background(), f.capture, core.ProviderRunRequest{Prompt: "owner work"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.service.Reconcile(goal.Owner(f.capture.Lease.SessionID, goal.Assignment), b)
	if !errors.Is(err, core.ErrInputAcknowledgementMismatch) {
		t.Fatalf("wrong native ACK not identified: %v", err)
	}
	f.native.wrongACK = false
	for _, prompt := range []string{"owner work", "different work"} {
		_, b, err = f.runtime.BindOwnerRunGoal(context.Background(), f.capture, core.ProviderRunRequest{Prompt: prompt})
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.service.Reconcile(goal.Owner(f.capture.Lease.SessionID, goal.Assignment), b)
		head, readErr := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
		if err == nil || readErr != nil || head.Applied != nil || head.Attempt == nil || head.Attempt.Receipt == nil || head.Acknowledged != nil || f.native.writes != 1 {
			t.Fatalf("retained wrong ACK healed or duplicated: %+v %v writes%d", head, err, f.native.writes)
		}
	}
}

// Captured image/physical identity and the journal's current holder/revision
// are checked before input. Each fixture starts with a valid staged neighbor.
func TestPendingCodexCapturedFactsRefuseBeforeInput(t *testing.T) {
	for _, mode := range []string{"image", "thread", "lease", "holder"} {
		t.Run(mode, func(t *testing.T) {
			f := newPendingCodexSystem(t)
			f.stage(t, false)
			capture := f.capture
			switch mode {
			case "image":
				capture.Committed.SHA256 = "bad"
			case "thread":
				f.native.snapshot.ThreadID = "foreign"
			case "lease":
				capture.Lease.Epoch++
			case "holder":
				f.caller.facts.Holder = "replacement"
				f.caller.facts.Generation++
				f.caller.facts.Working = true
				f.caller.facts.WorkingPrincipal = f.caller.facts.Principal
				_, err := f.service.Handle(context.Background(), goal.Command{EnvelopeID: "new-holder", SessionID: capture.Lease.SessionID, Namespace: goal.Working, Action: goal.Set, Content: &goal.Content{Objective: "working", Condition: "accepted", Budget: goal.Budget{Tokens: 10}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			_, b, err := f.runtime.BindOwnerRunGoal(context.Background(), capture, core.ProviderRunRequest{Prompt: "owner work"})
			if err == nil {
				_, err = f.service.Reconcile(goal.Owner(capture.Lease.SessionID, goal.Assignment), b)
			}
			if err == nil || f.native.writes != 0 {
				t.Fatalf("stale captured %s had native effects: %v writes%d", mode, err, f.native.writes)
			}
		})
	}
}

// The production Host RunOwner callsite reconciles the actual service before
// joining the same acknowledged native turn. Native end leaves assignment
// acceptance with the holder; a later accepted revision remains deliverable.
func TestPendingCodexHostOwnerContinuation(t *testing.T) {
	f := newPendingCodexSystem(t)
	f.stage(t, false)
	result, err := f.host.RunOwner(context.Background(), f.capture.Lease.SessionID, f.attachment.AttachmentID, core.ProviderRunRequest{Prompt: "owner work"})
	head, readErr := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
	if err != nil || result == nil || result.Output != "captured output" || readErr != nil || head.Latest.State != goal.Active || head.Applied == nil || f.native.writes != 1 {
		t.Fatalf("host failed combined owner continuation or native end accepted goal: %+v %v head%+v writes%d", result, err, head, f.native.writes)
	}
	receipt, err := f.service.Handle(context.Background(), goal.Command{EnvelopeID: "holder-accepts", SessionID: f.capture.Lease.SessionID, Namespace: goal.Assignment, Action: goal.Accept, ExpectedRevision: 1, Acceptance: &goal.Acceptance{Kind: goal.HolderAcceptance, Evidence: "reviewed outcome"}})
	if err != nil {
		t.Fatal(err)
	}
	result, err = f.host.RunOwner(context.Background(), f.capture.Lease.SessionID, f.attachment.AttachmentID, core.ProviderRunRequest{Prompt: "next owner work"})
	head, readErr = f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
	if err != nil || result == nil || readErr != nil || head.Latest.State != goal.Accepted || head.Applied == nil || *head.Applied != receipt.Ref || f.native.writes != 2 {
		t.Fatalf("accepted owner neighbor lost: %+v %v head%+v writes%d", result, err, head, f.native.writes)
	}
}

// Stop reaches the real isolated provider while the combined run observes its
// native turn. No map lock or cross-process epoch fence spans that long await.
func TestPendingCodexHostStopDuringOwnerAwait(t *testing.T) {
	f := newPendingCodexSystem(t)
	f.stage(t, false)
	f.native.hold = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.host.RunOwner(ctx, f.capture.Lease.SessionID, f.attachment.AttachmentID, core.ProviderRunRequest{Prompt: "held owner work"})
		done <- err
	}()
	select {
	case <-f.native.started:
	case <-ctx.Done():
		t.Fatal("combined owner observation never started")
	}
	if _, err := f.host.Stop(ctx, f.capture.Lease.SessionID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.native.stopped:
	default:
		t.Fatal("Stop did not reach provider during combined await")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("combined owner await did not join Stop")
	}
	if f.native.writes != 1 {
		t.Fatalf("combined Stop duplicated input: %d", f.native.writes)
	}
}

// A missing actual budget/service supplier refuses before any renderer input.
func TestPendingCodexMissingSupplierRefuses(t *testing.T) {
	runtime, _, native, _, capture := runtimeFixture(t)
	_, binding, err := runtime.BindOwnerRunGoal(context.Background(), capture, core.ProviderRunRequest{Prompt: "work"})
	if !errors.Is(err, core.ErrHostCapabilityUnavailable) || binding != nil || native.writes != 0 {
		t.Fatalf("missing supplier became success: %v writes%d", err, native.writes)
	}
}

// A genuine pending service head cannot turn a plain budget/service binding
// into a combined owner renderer. No goal-only first native Start is permitted.
func TestPendingCodexMissingCombinedFactoryNoGoalOnlyInput(t *testing.T) {
	f := newPendingCodexSystemWithBinding(t, false)
	f.stage(t, false)
	result, err := f.host.RunOwner(context.Background(), f.capture.Lease.SessionID, f.attachment.AttachmentID, core.ProviderRunRequest{Prompt: "owner work"})
	head, readErr := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
	if !errors.Is(err, core.ErrHostCapabilityUnavailable) || result != nil || readErr != nil || head.Applied != nil || head.Attempt != nil || head.Acknowledged != nil || f.native.writes != 0 {
		t.Fatalf("missing combined factory emitted goal-only input: %+v %v head%+v writes%d", result, err, head, f.native.writes)
	}
}

// A previously captured full revision cannot be submitted after a genuine
// newer service revision. The actual service checks current head before input.
func TestPendingCodexSupersededRevisionNoEffects(t *testing.T) {
	f := newPendingCodexSystem(t)
	ref := f.stage(t, false).Ref
	owner, binding, err := f.runtime.BindOwnerRunGoal(context.Background(), f.capture, core.ProviderRunRequest{Prompt: "old owner work"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.service.Handle(context.Background(), goal.Command{EnvelopeID: "superseding-goal", SessionID: f.capture.Lease.SessionID, Namespace: goal.Assignment, Action: goal.Set, ExpectedRevision: 1, Content: &goal.Content{Objective: "new objective", Condition: "new condition", Budget: goal.Budget{Tokens: 76}}})
	if err != nil {
		t.Fatal(err)
	}
	err = binding.Apply(&core.GoalEnvelope{Owner: owner, Ref: &ref})
	head, readErr := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
	if !errors.Is(err, goal.ErrRevisionConflict) || readErr != nil || head.Latest.Ref.Revision != 2 || head.Applied != nil || head.Attempt != nil || f.native.writes != 0 {
		t.Fatalf("superseded revision had effects: %v head%+v writes%d", err, head, f.native.writes)
	}
}

// Ordinary attachment may preserve only a real no-Submit service deferral. The
// active stored revision stays pending, the same input remains usable, and the
// later actual RunOwner consumes one combined receipt through the real service.
// The fixture explicitly selects the managed renderer; it does not prove the
// missing production owner-mode carrier/hydration source.
func TestPendingCodexNoWriteAttachThenOwner(t *testing.T) {
	f := newPendingCodexSystem(t)
	ref := f.stage(t, false).Ref
	attached, err := f.host.Attach(context.Background(), core.AttachRequest{Owner: f.capture.Record.Owner, Provider: "codex"})
	if err != nil || attached == nil || f.native.writes != 0 {
		t.Fatalf("real no-write deferral revoked attachment: %v writes%d", err, f.native.writes)
	}
	l, w, err := f.host.Input(context.Background(), f.capture.Lease.SessionID)
	head, readErr := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
	if err != nil || readErr != nil || l != f.capture.Lease || w != f.capture.Input || head.Applied != nil || head.Attempt != nil || head.Acknowledged != nil || head.Latest.Ref != ref {
		t.Fatalf("no-write deferral changed input/applied state: %v head%+v", err, head)
	}
	result, err := f.host.RunOwner(context.Background(), f.capture.Lease.SessionID, attached.AttachmentID, core.ProviderRunRequest{Prompt: "owner work"})
	if err != nil || result == nil || f.native.writes != 1 {
		t.Fatalf("no-write attach did not continue through combined owner: %+v %v writes%d", result, err, f.native.writes)
	}
}

// Original managed deferral is idle-only. An existing physical turn cannot
// become a newly queued no-write goal or borrow another assignment's receipt.
func TestPendingCodexExistingTurnIsNotDeferred(t *testing.T) {
	f := newPendingCodexSystem(t)
	f.native.snapshot.ManagedTurnID = "existing-turn"
	owner, binding, err := f.budgets.BindCodexGoal(context.Background(), f.capture.Lease, f.capture.Input, &codexRevisionFactory{runtime: f.runtime, capture: f.capture})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.service.Execute(context.Background(), goal.Command{EnvelopeID: "already-managed", SessionID: f.capture.Lease.SessionID, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "work", Condition: "accepted", Budget: goal.Budget{Tokens: 75}}}, binding)
	head, readErr := f.service.Snapshot(f.capture.Lease.SessionID, goal.Assignment)
	if !errors.Is(err, goal.ErrDeliveryUncertain) || errors.Is(err, goal.ErrNativeDeferred) || readErr != nil || head.Applied != nil || head.Attempt != nil || f.native.writes != 0 {
		t.Fatalf("existing native turn became deferred assignment: owner%s %v head%+v writes%d", owner, err, head, f.native.writes)
	}
}
