package sessionrun

import (
	"context"
	"errors"
	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"testing"
)

type activePTYSystem struct {
	runtime    *Runtime
	host       *core.SessionHost
	native     *capturedPTYFixture
	service    *goal.Service
	factory    *budget.Factory
	store      *budget.Store
	caller     *authority
	root       string
	attachment *core.Attachment
	lease      core.ControllerLease
	writer     core.HostInputWriter
}

func newActivePTYSystem(t *testing.T) *activePTYSystem {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	native := &capturedPTYFixture{ptyCapabilityFixture: ptyCapabilityFixture{base: core.MusePTYSnapshot{Contract: "task-board.muse-pty-host-v1", SessionID: "active-pty", PID: os.Getpid(), LogPath: filepath.Join(root, "native.jsonl")}}, stoppedRun: make(chan struct{})}
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
	factory, err := budget.NewFactory(ctx, service, store, runtime.AccountingSource())
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.BindBudget(factory); err != nil {
		t.Fatal(err)
	}
	hostRoot := filepath.Join(root, "host")
	host, err := core.NewHost(core.HostOptions{OwnerRuns: runtime, GoalBindings: runtime, Goals: service, Layout: core.Layout{Namespace: "active-pty-fixture", StateDir: hostRoot, SessionsDir: filepath.Join(hostRoot, "sessions"), LockFile: filepath.Join(hostRoot, "manager.lock")}, Config: configFixture{}, Notices: noticesFixture{}, Codec: core.NewLegacyRecordCodec(), Factories: []core.ProviderHostFactory{capturedPTYFactory{native}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(host.Close)
	t.Cleanup(func() {
		if err := factory.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if err = runtime.BindHost(host); err != nil {
		t.Fatal(err)
	}
	attachment, err := host.Attach(ctx, core.AttachRequest{Provider: "muse", Owner: "parent/primary"})
	if err != nil {
		t.Fatal(err)
	}
	lease, writer, err := host.Input(ctx, attachment.Session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	caller.facts = goal.VerifiedCaller{SessionID: lease.SessionID, Principal: "holder", Holder: "holder", Generation: 1, Assignment: true, UnlimitedBudget: true}
	return &activePTYSystem{runtime, host, native, service, factory, store, caller, root, attachment, lease, writer}
}
func (f *activePTYSystem) binding(t *testing.T) core.GoalBinding {
	t.Helper()
	_, binding, err := f.runtime.BindGoal(context.Background(), f.lease, "muse", f.writer)
	if err != nil {
		t.Fatal(err)
	}
	return binding
}
func (f *activePTYSystem) command(unlimited bool) goal.Command {
	b := goal.Budget{Tokens: 75}
	if unlimited {
		b = goal.Budget{Unlimited: true}
	}
	return goal.Command{EnvelopeID: "admitted-pty", SessionID: f.lease.SessionID, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "actual work", Condition: "holder accepts", Budget: b}}
}
func (f *activePTYSystem) noAccount(t *testing.T) {
	t.Helper()
	if _, err := f.store.LegacySnapshot(context.Background(), f.lease); !errors.Is(err, core.ErrStaleInputController) {
		t.Fatalf("PTY invented accounting: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.root, "budget", "accounting.json")); !errors.Is(err, os.ErrNotExist) || f.native.observations != 0 {
		t.Fatalf("PTY invented journal/observer: %v reads%d", err, f.native.observations)
	}
}

// Real verified-admission service, selected budget, journal, host and writer
// promote only an actual Text receipt, then execute owner work once. No native
// MSP goal or hard accounting is inferred from the finite/unlimited text.
func TestActivePTYGoalUsesActualServiceAndOwnerRun(t *testing.T) {
	for _, unlimited := range []bool{false, true} {
		t.Run(map[bool]string{false: "finite", true: "unlimited"}[unlimited], func(t *testing.T) {
			f := newActivePTYSystem(t)
			if unlimited {
				f.caller.facts.Operator = true
			}
			receipt, err := f.service.Execute(context.Background(), f.command(unlimited), f.binding(t))
			if err != nil || receipt.Pending {
				t.Fatalf("actual PTY goal not acknowledged: %+v %v", receipt, err)
			}
			head, err := f.service.Snapshot(f.lease.SessionID, goal.Assignment)
			if err != nil || head.Applied == nil || head.AppliedReceipt == nil || head.AppliedReceipt.Epoch != f.lease.Epoch || head.Attempt != nil || f.native.writes != 1 {
				t.Fatalf("actual PTY journal did not retain receipt: %+v %v writes%d", head, err, f.native.writes)
			}
			f.noAccount(t)
			result, err := f.host.RunOwner(context.Background(), f.attachment.Session.SessionID, f.attachment.AttachmentID, core.ProviderRunRequest{Prompt: "owner work"})
			if err != nil || result == nil || result.Output != "pty result" || f.native.writes != 2 || f.native.prompt != "owner work" {
				t.Fatalf("active PTY owner work lost: %+v %v writes%d", result, err, f.native.writes)
			}
			f.noAccount(t)
			head, err = f.service.Snapshot(f.lease.SessionID, goal.Assignment)
			if err != nil || head.Latest.State != goal.Active {
				t.Fatalf("native completion accepted goal: %+v %v", head, err)
			}
		})
	}
}

// Wrong retained native evidence stays unresolved after a retry, even if later
// backend behavior changes. Foreign session authority and unlimited budget
// without its separately required operator admission have no native effects.
func TestActivePTYWrongACKAndCallerNeverPromote(t *testing.T) {
	for _, mode := range []string{"wrong-ack", "foreign-caller", "unlimited-not-operator"} {
		t.Run(mode, func(t *testing.T) {
			f := newActivePTYSystem(t)
			if mode == "wrong-ack" {
				f.native.mode = "wrong-ack"
			} else if mode == "foreign-caller" {
				f.caller.facts.SessionID = "foreign"
			}
			receipt, err := f.service.Execute(context.Background(), f.command(mode == "unlimited-not-operator"), f.binding(t))
			want := 0
			if mode == "wrong-ack" {
				want = 1
				if !errors.Is(err, core.ErrInputAcknowledgementMismatch) {
					t.Fatalf("wrong ACK was not identified: %v", err)
				}
			} else if !errors.Is(err, goal.ErrForbidden) {
				t.Fatalf("foreign caller was not refused: %v", err)
			}
			head, readErr := f.service.Snapshot(f.lease.SessionID, goal.Assignment)
			if readErr != nil || head.Applied != nil || head.AppliedReceipt != nil || f.native.writes != want {
				t.Fatalf("invalid PTY facts promoted: %+v receipt%+v %v writes%d", head, receipt, readErr, f.native.writes)
			}
			if mode == "wrong-ack" {
				f.native.mode = ""
				_, err = f.service.Reconcile(goal.Owner(f.lease.SessionID, goal.Assignment), f.binding(t))
				head, readErr = f.service.Snapshot(f.lease.SessionID, goal.Assignment)
				if err == nil || readErr != nil || head.Applied != nil || head.Attempt == nil || f.native.writes != 1 {
					t.Fatalf("uncertain old ACK healed: %+v %v %v writes%d", head, err, readErr, f.native.writes)
				}
			}
			f.noAccount(t)
		})
	}
}

// A proved preflight no-write leaves the exact durable input available. Reopen
// and a fresh binding retry it unchanged once; generating a new ID must fail.
func TestActivePTYReopenedAttemptRetainsExactInput(t *testing.T) {
	f := newActivePTYSystem(t)
	f.native.mode = "preflight-refusal"
	_, err := f.service.Execute(context.Background(), f.command(false), f.binding(t))
	if !errors.Is(err, core.ErrInvalidInput) || f.native.writes != 0 {
		t.Fatalf("preflight fixture had native effect: %v writes%d", err, f.native.writes)
	}
	head, err := f.service.Snapshot(f.lease.SessionID, goal.Assignment)
	if err != nil || head.Attempt == nil || head.Attempt.Receipt != nil || head.Applied != nil {
		t.Fatalf("no-write attempt not retained: %+v %v", head, err)
	}
	inputID := head.Attempt.Input.ID
	inputText := *head.Attempt.Input.Payload.Text
	if err = f.factory.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	service, err := goal.Open(filepath.Join(f.root, "goals"), f.caller)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := budget.NewFactory(context.Background(), service, f.store, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = factory.Close(context.Background()) })
	renderer, err := NewPTYDeliveryFactory(f.writer.(core.ProviderNativeIdentity).NativeIdentity())
	if err != nil {
		t.Fatal(err)
	}
	_, binding, err := factory.BindRenderedTextGoal(context.Background(), f.lease, "muse", f.writer, renderer)
	if err != nil {
		t.Fatal(err)
	}
	f.native.mode = ""
	activation, err := service.Reconcile(goal.Owner(f.lease.SessionID, goal.Assignment), binding)
	after, readErr := service.Snapshot(f.lease.SessionID, goal.Assignment)
	if err != nil || readErr != nil || activation == nil || activation.Pending || after.Applied == nil || after.AppliedReceipt == nil || after.AppliedReceipt.ID != inputID || after.Attempt != nil || f.native.writes != 1 || f.native.prompt != inputText {
		t.Fatalf("actual persisted Text retry not resumed: %+v head%+v err%v read%v writes%d", activation, after, err, readErr, f.native.writes)
	}
	f.noAccount(t)
}
