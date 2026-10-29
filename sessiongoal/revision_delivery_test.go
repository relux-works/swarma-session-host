package sessiongoal

import (
	"context"
	"errors"
	"reflect"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Admission and native evidence here are explicitly trusted in-process fixtures.
// Real Service, core receipt scope/checker and durable journal execute; no bridge,
// native provider, launcher or external provenance qualification is claimed.
type revisionFactoryFixture struct {
	mode, provider, prompt string
	got                    Revision
	escaped                core.HostInputWriter
	calls                  int
}

func (f *revisionFactoryFixture) DeliveryForRevision(r Revision) (core.GoalDelivery, error) {
	f.got = r
	f.calls++
	return &revisionProviderFixture{factory: f, revision: r}, nil
}

type revisionProviderFixture struct {
	factory  *revisionFactoryFixture
	revision Revision
}

func (d *revisionProviderFixture) ApplyGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	return d.deliver(ctx, e, l, w, "apply")
}
func (d *revisionProviderFixture) RollbackGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	return d.deliver(ctx, e, l, w, "rollback")
}
func (d *revisionProviderFixture) deliver(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter, phase string) (core.InputObservation, error) {
	f := d.factory
	f.escaped = w
	if f.mode == "deferred" {
		return core.InputObservation{}, ErrNativeDeferred
	}
	if f.mode == "no_receipt" {
		return core.InputObservation{}, nil
	}
	text := d.revision.Content.Objective + "\n\n" + f.prompt
	input := core.InputSubmission{Lease: l, ID: "fixture-goal-command", Expected: core.AckExpectation{Kind: "run-observed-after-snapshot", Correlation: "fixture-native-session"}, Payload: core.RenderedInput{Text: &text}}
	if f.provider == "codex" {
		input.Payload = core.RenderedInput{CodexTurn: &core.CodexTurnCommand{CommandID: "fixture-native-command", Start: &core.CodexTurnStart{Text: text}}}
		input.Expected = core.AckExpectation{Kind: "provider-ack", Correlation: "fixture-native-command"}
	}
	switch f.mode {
	case "wrong_lease":
		input.Lease.Epoch++
	case "weak_kind":
		input.Expected.Kind = "written"
	case "multiple_payloads":
		input.Payload.MuseGoal = &core.MuseGoalCommand{}
	case "changed_payload":
		text += "changed retry payload"
		if input.Payload.CodexTurn != nil {
			input.Payload.CodexTurn.Start.Text = text
		}
	}
	var err error
	input, err = core.PrepareGoalInput(e, phase, input)
	if err != nil {
		return core.InputObservation{}, err
	}
	receipt, err := w.Submit(ctx, input)
	if err != nil {
		return core.InputObservation{}, err
	}
	if f.mode == "postwrite_deferred" {
		return core.InputObservation{}, ErrNativeDeferred
	}
	if f.mode == "double_submit" {
		_, _ = w.Submit(ctx, input)
	}
	observation, err := w.Await(ctx, receipt)
	if f.mode == "swallow_await" {
		return core.InputObservation{}, nil
	}
	if err != nil {
		return core.InputObservation{}, err
	}
	if f.mode == "fabricated_native" {
		observation.CodexTurn.ThreadID = "different native thread"
	}
	return observation, nil
}

type revisionWriterFixture struct {
	*writerFixture
	calls       int
	mode        string
	beforeAwait func()
}

func (w *revisionWriterFixture) Await(ctx context.Context, r core.InputReceipt) (core.InputObservation, error) {
	w.calls++
	if w.beforeAwait != nil {
		w.beforeAwait()
	}
	o, e := w.writerFixture.Await(ctx, r)
	if e != nil {
		return o, e
	}
	input := w.inputs[len(w.inputs)-1]
	if input.Payload.CodexTurn != nil {
		o.CodexTurn = &core.CodexTurnObservation{CommandID: input.Payload.CodexTurn.CommandID, ThreadID: "fixture-thread", TurnID: "fixture-turn", StartedNewTurn: true}
	} else {
		o.EvidenceClass = core.InputEvidenceRunObservedAfterSnapshot
		o.PTYRun = &core.PTYRunObservation{RunID: "fixture-observed-run"}
	}
	switch w.mode {
	case "wrong_class":
		o.EvidenceClass = core.InputEvidenceExactProviderAck
	case "missing_native":
		o.CodexTurn = nil
		o.PTYRun = nil
	case "foreign_union":
		o.CodexTurn = &core.CodexTurnObservation{CommandID: "foreign"}
	case "late_mismatch":
		if w.calls == 2 {
			o.Correlation = "foreign recheck"
		}
	}
	return o, nil
}
func revisionBindingFixture(t *testing.T, s *Service, f *revisionFactoryFixture, w *revisionWriterFixture, epoch core.ControllerEpoch) core.GoalBinding {
	t.Helper()
	provider := "muse"
	if f.provider == "codex" {
		provider = "codex"
	}
	b, e := s.NewDeliveryBinding(caller("launcher"), core.ControllerLease{SessionID: "session-a", Epoch: epoch}, Assignment, provider, w, budgetFixture{}, f)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

// Provider-owned PTY Text needs no NativeGoalObserver/MuseGoal. The complete
// admitted revision reaches the factory, and Pending survives until core recheck.
func TestGoalRevisionDeliveryPTYCheckedReceiptOrder(t *testing.T) {
	s, _, _ := fixture(t)
	f := &revisionFactoryFixture{prompt: "owner prompt"}
	w := &revisionWriterFixture{writerFixture: newWriter()}
	b := revisionBindingFixture(t, s, f, w, 1)
	w.beforeAwait = func() {
		h, e := s.Snapshot("session-a", Assignment)
		if e != nil || h.Applied != nil || h.Attempt == nil || h.Attempt.Receipt == nil || h.Acknowledged != nil {
			t.Fatalf("early native promotion: %+v %v", h, e)
		}
		g, e := s.Get(Owner("session-a", Assignment))
		if e != nil || !g.Pending {
			t.Fatal("pending lost before checked receipt", e)
		}
	}
	receipt, e := s.Execute(caller("launcher"), command("provider-text", 0), b)
	h, he := s.Snapshot("session-a", Assignment)
	if e != nil || he != nil || receipt.Pending || h.Applied == nil || *h.Applied != receipt.Ref || h.Attempt != nil || len(h.Applications) != 1 || w.submits != 1 || w.calls != 2 {
		t.Fatalf("actual receipt not promoted once: %+v %+v %v %v", receipt, h, e, he)
	}
	if !reflect.DeepEqual(f.got, h.Latest) || w.inputs[0].Payload.Text == nil || w.inputs[0].Payload.MuseGoal != nil || w.inputs[0].Expected.Correlation != "fixture-native-session" || *w.inputs[0].Payload.Text != h.Latest.Content.Objective+"\n\nowner prompt" {
		t.Fatal("revision/text/correlation changed")
	}
	if _, e = f.escaped.Submit(context.Background(), w.inputs[0]); !errors.Is(e, core.ErrInputScopeClosed) || w.submits != 1 {
		t.Fatal("expired writer escaped", e)
	}
}

// Staging is not receipt/application success. A fresh invocation performs one
// combined directive+owner Start and checks real retained native thread/turn data.
func TestGoalRevisionDeliveryCodexDeferredCombinedReceipt(t *testing.T) {
	s, _, root := fixture(t)
	c := command("staged-directive", 0)
	if _, e := s.Handle(caller("launcher"), c); e != nil {
		t.Fatal(e)
	}
	f := &revisionFactoryFixture{mode: "deferred", provider: "codex", prompt: "owner request"}
	w := &revisionWriterFixture{writerFixture: newWriter()}
	b := revisionBindingFixture(t, s, f, w, 1)
	before := bytesAt(root)
	activation, e := s.Reconcile(Owner("session-a", Assignment), b)
	if !errors.Is(e, ErrNativeDeferred) || activation == nil || !activation.Pending || bytesAt(root) != before || w.submits != 0 {
		t.Fatalf("staging became success/effect: %+v %v", activation, e)
	}
	if _, e = f.escaped.Submit(context.Background(), core.InputSubmission{}); !errors.Is(e, core.ErrInputScopeClosed) {
		t.Fatal("staged writer retained", e)
	}
	replay, e := s.Handle(caller("launcher"), c)
	if e != nil || !replay.Pending {
		t.Fatal("staged command receipt lost Pending", e)
	}
	reopened, e := Open(root, s.authority)
	if e != nil {
		t.Fatal(e)
	}
	f.mode = ""
	b = revisionBindingFixture(t, reopened, f, w, 1)
	w.beforeAwait = func() {
		h, e := reopened.Snapshot("session-a", Assignment)
		if e != nil || h.Applied != nil || h.Attempt == nil {
			t.Fatal("combined Start promoted before actual receipt", e)
		}
	}
	activation, e = reopened.Reconcile(Owner("session-a", Assignment), b)
	if e != nil || activation.Pending || w.submits != 1 || w.inputs[0].Payload.CodexTurn == nil || w.inputs[0].Payload.CodexTurn.Start.Text != c.Content.Objective+"\n\nowner request" {
		t.Fatalf("combined receipt lost: %+v %v", activation, e)
	}
	replay, e = reopened.Handle(caller("launcher"), c)
	if e != nil || replay.Pending {
		t.Fatal("checked combined receipt did not resolve Pending", e)
	}
	h, _ := reopened.Snapshot("session-a", Assignment)
	if h.Latest.State != Active || h.Latest.Acceptance != nil || len(h.History) != 1 {
		t.Fatal("native receipt accepted/cleared intent")
	}
}

// Invalid pre-effect input, fabricated/deferred post-write observations, weak or
// foreign native evidence and swallowed/double calls cannot promote intent.
func TestGoalRevisionDeliveryRefusalsRetainPending(t *testing.T) {
	for _, name := range []string{"wrong_lease", "weak_kind", "multiple_payloads", "no_receipt", "postwrite_deferred", "double_submit", "swallow_await", "fabricated_native", "wrong_class", "missing_native", "foreign_union", "late_mismatch"} {
		t.Run(name, func(t *testing.T) {
			s, _, root := fixture(t)
			c := command("refused-delivery", 0)
			if _, e := s.Handle(caller("launcher"), c); e != nil {
				t.Fatal(e)
			}
			f := &revisionFactoryFixture{mode: name, prompt: "owner prompt"}
			if name == "fabricated_native" {
				f.provider = "codex"
			}
			w := &revisionWriterFixture{writerFixture: newWriter(), mode: name}
			if name == "swallow_await" {
				w.awaitErr = core.ErrInputAcknowledgementMismatch
			}
			b := revisionBindingFixture(t, s, f, w, 1)
			before := bytesAt(root)
			a, e := s.Reconcile(Owner("session-a", Assignment), b)
			h, he := s.Snapshot("session-a", Assignment)
			want := core.ErrInputAcknowledgementMismatch
			switch name {
			case "wrong_lease":
				want = ErrBindingUnavailable
			case "weak_kind", "multiple_payloads":
				want = core.ErrInvalidInput
			case "double_submit":
				want = core.ErrInputIDConflict
			case "postwrite_deferred":
				want = ErrDeliveryUncertain
			}
			if !errors.Is(e, want) || he != nil || a == nil || !a.Pending || h.Applied != nil || h.Acknowledged != nil || len(h.Applications) != 0 || h.Latest.State != Active || w.submits > 1 {
				t.Fatalf("wrong refusal/promotion: %v head=%+v", e, h)
			}
			if name == "postwrite_deferred" && errors.Is(e, ErrNativeDeferred) {
				t.Fatal("post-write uncertainty masqueraded as no-write staging")
			}
			if name == "wrong_lease" || name == "weak_kind" || name == "multiple_payloads" || name == "no_receipt" {
				if w.submits != 0 || bytesAt(root) != before {
					t.Fatal("pre-effect refusal wrote input/journal")
				}
			}
		})
	}
}

// Retry/restart preserves the complete original input. Changed payload or lease
// cannot overwrite uncertainty or borrow its receipt; actual host dedup remains
// the writer's responsibility, not a physical-native qualification of this fixture.
func TestGoalRevisionDeliveryRestartRetainsExactAttempt(t *testing.T) {
	s, _, root := fixture(t)
	c := command("restart-delivery", 0)
	if _, e := s.Handle(caller("launcher"), c); e != nil {
		t.Fatal(e)
	}
	f := &revisionFactoryFixture{provider: "codex", prompt: "owner request"}
	w := &revisionWriterFixture{writerFixture: newWriter()}
	w.awaitErr = core.ErrInputAcknowledgementMismatch
	if _, e := s.Reconcile(Owner("session-a", Assignment), revisionBindingFixture(t, s, f, w, 1)); !errors.Is(e, core.ErrInputAcknowledgementMismatch) {
		t.Fatal(e)
	}
	head, _ := s.Snapshot("session-a", Assignment)
	prior := head.Attempt.Input
	before := bytesAt(root)
	reopened, e := Open(root, s.authority)
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"changed_payload", "changed_lease"} {
		f.mode = "changed_payload"
		epoch := core.ControllerEpoch(1)
		if name == "changed_lease" {
			f.mode = ""
			epoch = 2
		}
		next := &revisionWriterFixture{writerFixture: newWriter()}
		if _, e = reopened.Reconcile(Owner("session-a", Assignment), revisionBindingFixture(t, reopened, f, next, epoch)); !errors.Is(e, ErrDeliveryUncertain) || next.submits != 0 || bytesAt(root) != before {
			t.Fatalf("uncertain input replaced: %s %v", name, e)
		}
	}
	f.mode = ""
	next := &revisionWriterFixture{writerFixture: newWriter()}
	if _, e = reopened.Reconcile(Owner("session-a", Assignment), revisionBindingFixture(t, reopened, f, next, 1)); e != nil || next.submits != 1 || !reflect.DeepEqual(prior, next.inputs[0]) {
		t.Fatalf("exact retained retry lost: %v", e)
	}
}

// Existing exact-session authority and holder handoff guard the new renderer:
// an old holder/session-agent cannot invoke it or change pending intent.
func TestGoalRevisionDeliveryPreservesAdmissionAndNamespace(t *testing.T) {
	for _, name := range []string{"old_holder", "agent", "wrong_session", "missing_grant"} {
		t.Run(name, func(t *testing.T) {
			s, a, root := fixture(t)
			f := &revisionFactoryFixture{}
			w := &revisionWriterFixture{writerFixture: newWriter()}
			b := revisionBindingFixture(t, s, f, w, 1)
			ctx := caller("launcher")
			switch name {
			case "old_holder":
				a.facts.Holder = "new-holder"
				a.facts.Generation = 2
			case "agent":
				ctx = caller("agent")
			case "wrong_session":
				a.facts.SessionID = "other"
			case "missing_grant":
				a.facts.Assignment = false
			}
			before := bytesAt(root)
			if _, e := s.Execute(ctx, command("not-admitted", 0), b); !errors.Is(e, ErrForbidden) || bytesAt(root) != before || w.submits != 0 || f.calls != 0 {
				t.Fatal("unauthorized renderer effect", e)
			}
		})
	}
	s, _, _ := fixture(t)
	f := &revisionFactoryFixture{}
	w := &revisionWriterFixture{writerFixture: newWriter()}
	if _, e := s.NewDeliveryBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Working, "muse", w, budgetFixture{}, f); !errors.Is(e, ErrBindingUnavailable) {
		t.Fatal("working namespace became native assignment", e)
	}
	if _, e := s.NewDeliveryBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "muse", w, budgetFixture{}, nil); !errors.Is(e, ErrBindingUnavailable) {
		t.Fatal("missing renderer accepted", e)
	}
}

// Actual provider-rendered rollback changes only known native application. The
// latest durable assignment remains pending/active, including clear-to-unbound.
func TestGoalRevisionDeliveryRollbackPreservesIntent(t *testing.T) {
	s, _, _ := fixture(t)
	f := &revisionFactoryFixture{prompt: "owner"}
	w := &revisionWriterFixture{writerFixture: newWriter()}
	b := revisionBindingFixture(t, s, f, w, 1)
	first, e := s.Execute(caller("launcher"), command("first-delivery", 0), b)
	if e != nil {
		t.Fatal(e)
	}
	next := command("second-delivery", 1)
	next.Content.Objective = "new assignment"
	second, e := s.Handle(caller("launcher"), next)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Rollback(&core.GoalEnvelope{Owner: Owner("session-a", Assignment), Ref: &first.Ref}); e != nil {
		t.Fatal(e)
	}
	h, e := s.Snapshot("session-a", Assignment)
	if e != nil || h.Latest.Ref != second.Ref || h.Latest.State != Active || h.Applied == nil || *h.Applied != first.Ref || h.Attempt != nil || h.Rollback != nil || len(h.Rollbacks) != 1 || w.submits != 2 {
		t.Fatalf("rollback rewrote intent: %+v %v", h, e)
	}
	if e = b.Rollback(&core.GoalEnvelope{Owner: Owner("session-a", Assignment), Clear: true}); e != nil {
		t.Fatal(e)
	}
	h, e = s.Snapshot("session-a", Assignment)
	if e != nil || h.Latest.Ref != second.Ref || h.Latest.State != Active || h.Applied != nil || len(h.History) != 2 || len(h.Rollbacks) != 2 || w.submits != 3 {
		t.Fatalf("native unbind cleared assignment: %+v %v", h, e)
	}
}

// A revision staged under the old holder cannot be delivered after a real
// journal handoff, even when a privileged caller retains its old envelope.
func TestGoalRevisionDeliveryCapturedRevisionFencedByHandoff(t *testing.T) {
	s, a, root := fixture(t)
	first, e := s.Handle(caller("launcher"), command("old-holder-stage", 0))
	if e != nil {
		t.Fatal(e)
	}
	f := &revisionFactoryFixture{}
	w := &revisionWriterFixture{writerFixture: newWriter()}
	b := revisionBindingFixture(t, s, f, w, 1)
	a.facts.Holder = "new-holder"
	a.facts.Generation = 2
	next := command("holder-handoff", 1)
	if _, e = s.Handle(caller("new-holder"), next); e != nil {
		t.Fatal(e)
	}
	before := bytesAt(root)
	if e = b.Apply(&core.GoalEnvelope{Owner: Owner("session-a", Assignment), Ref: &first.Ref}); !errors.Is(e, ErrRevisionConflict) || w.submits != 0 || bytesAt(root) != before {
		t.Fatal("old captured revision wrote after handoff", e)
	}
}

// Missing real budget binding cannot be replaced by a renderer's success. The
// valid budget-bound Text control is TestGoalRevisionDeliveryPTYCheckedReceiptOrder.
func TestGoalRevisionDeliveryMissingBudgetRefusesBeforeRenderer(t *testing.T) {
	s, _, root := fixture(t)
	if _, e := s.Handle(caller("launcher"), command("missing-budget", 0)); e != nil {
		t.Fatal(e)
	}
	f := &revisionFactoryFixture{}
	w := &revisionWriterFixture{writerFixture: newWriter()}
	b, e := s.NewDeliveryBinding(caller("launcher"), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "muse", w, nil, f)
	if e != nil {
		t.Fatal(e)
	}
	before := bytesAt(root)
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); !errors.Is(e, ErrBudgetUnavailable) || f.calls != 0 || w.submits != 0 || bytesAt(root) != before {
		t.Fatal("missing budget bypassed", e)
	}
}

type retainedRevisionFactoryFixture struct {
	base     *revisionFactoryFixture
	got      core.InputSubmission
	revision Revision
	calls    int
}

func (f *retainedRevisionFactoryFixture) DeliveryForRevision(r Revision) (core.GoalDelivery, error) {
	return f.base.DeliveryForRevision(r)
}
func (f *retainedRevisionFactoryFixture) DeliveryForAttempt(r Revision, input core.InputSubmission) (core.GoalDelivery, error) {
	f.calls++
	f.got = input
	f.revision = r
	return retainedRevisionDeliveryFixture{input: input}, nil
}

type retainedRevisionDeliveryFixture struct{ input core.InputSubmission }

func (d retainedRevisionDeliveryFixture) ApplyGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	r, err := w.Submit(ctx, d.input)
	if err != nil {
		return core.InputObservation{}, err
	}
	return w.Await(ctx, r)
}
func (d retainedRevisionDeliveryFixture) RollbackGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	return d.ApplyGoal(ctx, e, l, w)
}

// A renderer with random native IDs can resume the actual retained input after
// reopening, without a reconstructed command or a new native authority claim.
func TestGoalRevisionDeliveryAttemptFactoryGetsExactPersistedInput(t *testing.T) {
	s, _, root := fixture(t)
	c := command("retained-renderer-input", 0)
	if _, e := s.Handle(caller("launcher"), c); e != nil {
		t.Fatal(e)
	}
	base := &revisionFactoryFixture{provider: "codex", prompt: "owner request"}
	w := &revisionWriterFixture{writerFixture: newWriter()}
	w.awaitErr = core.ErrInputAcknowledgementMismatch
	if _, e := s.Reconcile(Owner("session-a", Assignment), revisionBindingFixture(t, s, base, w, 1)); !errors.Is(e, core.ErrInputAcknowledgementMismatch) {
		t.Fatal(e)
	}
	h, _ := s.Snapshot("session-a", Assignment)
	want := h.Attempt.Input
	reopened, e := Open(root, s.authority)
	if e != nil {
		t.Fatal(e)
	}
	factory := &retainedRevisionFactoryFixture{base: base}
	next := &revisionWriterFixture{writerFixture: newWriter()}
	b, e := reopened.NewDeliveryBinding(caller("launcher"), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", next, budgetFixture{}, factory)
	if e != nil {
		t.Fatal(e)
	}
	a, e := reopened.Reconcile(Owner("session-a", Assignment), b)
	if e != nil || a.Pending || factory.calls != 1 || !reflect.DeepEqual(factory.got, want) || !reflect.DeepEqual(factory.revision, h.Latest) || !reflect.DeepEqual(next.inputs[0], want) {
		t.Fatalf("retained input replaced or partial: %v", e)
	}
}
