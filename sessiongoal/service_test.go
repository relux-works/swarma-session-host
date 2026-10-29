package sessiongoal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Admission here is an explicitly trusted library fixture, not authentication:
// no bridge, signature, kernel-credential or dispatcher qualification is claimed.
type callerKey struct{}
type authorityFixture struct {
	mu    sync.Mutex
	facts VerifiedCaller
	err   error
}

func (a *authorityFixture) WithVerifiedCaller(ctx context.Context, session string, fn func(VerifiedCaller) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	f := a.facts
	if p, ok := ctx.Value(callerKey{}).(string); ok {
		f.Principal = p
	}
	return fn(f)
}
func caller(p string) context.Context { return context.WithValue(context.Background(), callerKey{}, p) }
func fixture(t *testing.T) (*Service, *authorityFixture, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "goals")
	a := &authorityFixture{facts: VerifiedCaller{SessionID: "session-a", Principal: "launcher", Holder: "launcher", Generation: 1, Assignment: true, Working: true, WorkingPrincipal: "agent"}}
	s, e := Open(root, a)
	if e != nil {
		t.Fatal(e)
	}
	return s, a, root
}
func command(id string, previous int) Command {
	return Command{EnvelopeID: id, SessionID: "session-a", Namespace: Assignment, Action: Set, ExpectedRevision: previous, Content: &Content{Objective: "Preserve the session", Condition: "holder accepts the outcome", Budget: Budget{Tokens: 100}}}
}
func bytesAt(root string) string {
	b, _ := os.ReadFile(filepath.Join(root, "journal.json"))
	return string(b)
}

// Actual Handle policy admits the session's launcher and an entitled operator;
// wrong session, session-agent, absent grant and old holder leave bytes unchanged.
func TestGoalAuthorityAndNoUnauthorizedEffects(t *testing.T) {
	for _, name := range []string{"launcher", "operator", "unrelated_principal", "session_agent", "wrong_session", "no_grant", "old_holder", "missing_supplier"} {
		t.Run(name, func(t *testing.T) {
			s, a, root := fixture(t)
			ctx := caller("launcher")
			valid := name == "launcher" || name == "operator"
			switch name {
			case "operator":
				a.facts.Operator = true
				ctx = caller("operator")
			case "unrelated_principal":
				ctx = caller("board-client")
			case "session_agent":
				ctx = caller("agent")
			case "wrong_session":
				a.facts.SessionID = "session-b"
			case "no_grant":
				a.facts.Assignment = false
			case "old_holder":
				a.facts.Holder = "new-launcher"
				a.facts.Generation = 2
			case "missing_supplier":
				s.authority = nil
			}
			before := bytesAt(root)
			writer := newWriter()
			binding, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", writer, budgetFixture{})
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.Execute(ctx, command("envelope", 0), binding)
			if valid {
				if e != nil || writer.submits != 1 {
					t.Fatalf("entitled command refused: %v / %d", e, writer.submits)
				}
			} else {
				want := ErrForbidden
				if name == "missing_supplier" {
					want = ErrAuthorityUnavailable
				}
				if !errors.Is(e, want) || bytesAt(root) != before || writer.submits != 0 {
					t.Fatalf("unauthorized effect: %v writes=%d changed=%t", e, writer.submits, bytesAt(root) != before)
				}
			}
		})
	}
}

// Working grants never confer assignment grants, nor the reverse. Assignment
// and working revisions are independent durable heads in the same session.
func TestGoalNamespacesDoNotCrossGrant(t *testing.T) {
	s, a, root := fixture(t)
	a.facts.Assignment = false
	work := command("working", 0)
	work.Namespace = Working
	if _, e := s.Handle(caller("agent"), work); e != nil {
		t.Fatal(e)
	}
	before := bytesAt(root)
	if _, e := s.Handle(caller("agent"), command("assignment-refused", 0)); !errors.Is(e, ErrForbidden) || bytesAt(root) != before {
		t.Fatalf("working grant crossed into assignment: %v", e)
	}
	a.facts.Assignment = true
	a.facts.Working = false
	if _, e := s.Handle(caller("launcher"), command("assignment", 0)); e != nil {
		t.Fatal(e)
	}
	before = bytesAt(root)
	work.EnvelopeID = "working-refused"
	work.ExpectedRevision = 1
	if _, e := s.Handle(caller("launcher"), work); !errors.Is(e, ErrForbidden) || bytesAt(root) != before {
		t.Fatalf("assignment grant crossed into working: %v", e)
	}
	assignment, e := s.Snapshot("session-a", Assignment)
	if e != nil {
		t.Fatal(e)
	}
	working, e := s.Snapshot("session-a", Working)
	if e != nil {
		t.Fatal(e)
	}
	if assignment.Latest.Ref.Revision != 1 || working.Latest.Ref.Revision != 1 || assignment.Latest.Ref.ID == working.Latest.Ref.ID {
		t.Fatal("namespace heads mixed")
	}
}

// CAS/replay operate on actual durable bytes. Exact envelope replay is inert;
// altered replay and stale CAS refuse. Independent reopened stores serialize
// concurrent CAS and recover immutable envelope/holder/revision history.
func TestGoalCASReplayAndRestart(t *testing.T) {
	s, a, root := fixture(t)
	cmd := command("first", 0)
	first, e := s.Handle(caller("launcher"), cmd)
	if e != nil {
		t.Fatal(e)
	}
	before := bytesAt(root)
	replay, e := s.Handle(caller("launcher"), cmd)
	if e != nil || !replay.Replayed || replay.Ref != first.Ref || bytesAt(root) != before {
		t.Fatalf("exact replay changed state: %+v %v", replay, e)
	}
	cmd.Content.Objective = "altered"
	if _, e = s.Handle(caller("launcher"), cmd); !errors.Is(e, ErrReplayConflict) || bytesAt(root) != before {
		t.Fatalf("altered envelope admitted: %v", e)
	}
	if _, e = s.Handle(caller("launcher"), command("stale", 0)); !errors.Is(e, ErrRevisionConflict) || bytesAt(root) != before {
		t.Fatalf("stale CAS admitted: %v", e)
	}
	// Independent admission fixtures allow the two store transactions to race;
	// a common admission mutex would serialize them before the OS journal lock.
	other, e := Open(root, &authorityFixture{facts: a.facts})
	if e != nil {
		t.Fatal(e)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for i, service := range []*Service{s, other} {
		go func(n int, s *Service) {
			<-start
			_, err := s.Handle(caller("launcher"), command([]string{"second-a", "second-b"}[n], 1))
			results <- err
		}(i, service)
	}
	close(start)
	success, conflict := 0, 0
	for range 2 {
		e = <-results
		if e == nil {
			success++
		} else if errors.Is(e, ErrRevisionConflict) {
			conflict++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("non-atomic CAS: %d %d", success, conflict)
	}
	reopened, e := Open(root, a)
	if e != nil {
		t.Fatal(e)
	}
	head, e := reopened.Snapshot("session-a", Assignment)
	if e != nil {
		t.Fatal(e)
	}
	if head.Latest.Ref.Revision != 2 || head.Latest.Holder != "launcher" || head.Latest.SourceEnvelopeID == "" {
		t.Fatalf("restart lost binding: %+v", head)
	}
	a.facts.Holder = "successor"
	a.facts.Generation = 2
	before = bytesAt(root)
	if _, e = reopened.Handle(caller("launcher"), command("old", 2)); !errors.Is(e, ErrForbidden) || bytesAt(root) != before {
		t.Fatalf("old holder retained authority: %v", e)
	}
	if _, e = reopened.Handle(caller("successor"), command("successor", 2)); e != nil {
		t.Fatal(e)
	}
	// Even stale admission facts cannot revive an old envelope after durable handoff.
	a.facts.Holder = "launcher"
	a.facts.Generation = 1
	before = bytesAt(root)
	if _, e = reopened.Handle(caller("launcher"), command("first", 0)); !errors.Is(e, ErrForbidden) || bytesAt(root) != before {
		t.Fatalf("stale holder replay survived durable handoff: %v", e)
	}
}

// Completion requires explicit holder acceptance or its relayed barrier fact.
// Native/turn/process observations and an operator's acceptance cannot clear
// an assignment. An accepted head and immutable predecessor survive restart.
func TestGoalCompletionIsOnlyExplicitHolderAcceptance(t *testing.T) {
	for _, kind := range []AcceptanceKind{HolderAcceptance, RelayedBarrier} {
		t.Run(string(kind), func(t *testing.T) {
			s, a, root := fixture(t)
			if _, e := s.Handle(caller("launcher"), command("set", 0)); e != nil {
				t.Fatal(e)
			}
			before := bytesAt(root)
			for _, event := range []Action{"turn.end", "native.done", "process.exit", "park", "usage.limit"} {
				c := Command{EnvelopeID: string(event), SessionID: "session-a", Namespace: Assignment, Action: event, ExpectedRevision: 1}
				if _, e := s.Handle(caller("launcher"), c); !errors.Is(e, ErrInvalidCommand) || bytesAt(root) != before {
					t.Fatalf("implicit event completed goal: %s %v", event, e)
				}
			}
			c := Command{EnvelopeID: "accept", SessionID: "session-a", Namespace: Assignment, Action: Accept, ExpectedRevision: 1, Acceptance: &Acceptance{Kind: kind, Evidence: "holder-relayed-fact"}}
			a.facts.Operator = true
			if _, e := s.Handle(caller("operator"), c); !errors.Is(e, ErrForbidden) || bytesAt(root) != before {
				t.Fatalf("non-holder completed assignment: %v", e)
			}
			if _, e := s.Handle(caller("launcher"), c); e != nil {
				t.Fatal(e)
			}
			reopened, e := Open(root, a)
			if e != nil {
				t.Fatal(e)
			}
			head, e := reopened.Snapshot("session-a", Assignment)
			if e != nil {
				t.Fatal(e)
			}
			if head.Latest.State != Accepted || head.Latest.Ref.Revision != 2 || head.Latest.Content.Objective == "" || head.Latest.Acceptance == nil {
				t.Fatalf("acceptance cleared/lost ledger: %+v", head)
			}
		})
	}
}

// Receipt binding uses the real core checker, not a renderer verdict. Submit,
// Await, stale-lease and mismatch errors cannot promote the applied reference.
func TestGoalReceiptBindingAndErrorPropagation(t *testing.T) {
	for _, mode := range []string{"valid", "submit_error", "await_error", "wrong_receipt", "wrong_correlation", "missing_budget", "budget_error"} {
		t.Run(mode, func(t *testing.T) {
			s, _, root := fixture(t)
			w := newWriter()
			sentinel := errors.New("fixture fence failed")
			switch mode {
			case "submit_error":
				w.submitErr = sentinel
			case "await_error":
				w.awaitErr = sentinel
			case "wrong_receipt":
				w.wrongReceipt = true
			case "wrong_correlation":
				w.wrongCorrelation = true
			}
			var budget BudgetBinding = budgetFixture{}
			if mode == "missing_budget" {
				budget = nil
			}
			if mode == "budget_error" {
				budget = budgetFixture{err: sentinel}
			}
			binding, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 4}, Assignment, "codex", w, budget)
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.Execute(caller("launcher"), command("set", 0), binding)
			head, re := s.Snapshot("session-a", Assignment)
			if re != nil {
				t.Fatal(re)
			}
			if mode == "valid" {
				if e != nil || head.Applied == nil || *head.Applied != head.Latest.Ref || head.AppliedReceipt == nil || head.AppliedReceipt.Epoch != 4 {
					t.Fatalf("receipt not durable: %+v %v", head, e)
				}
				reopened, re := Open(root, s.authority)
				if re != nil {
					t.Fatal(re)
				}
				read, re := reopened.Get(Owner("session-a", Assignment))
				if re != nil || read.Pending || read.Applied == nil {
					t.Fatalf("applied restart lost: %+v %v", read, re)
				}
			} else {
				if e == nil || head.Applied != nil {
					t.Fatalf("false applied revision: %+v %v", head, e)
				}
				if (mode == "wrong_receipt" || mode == "wrong_correlation") && !errors.Is(e, core.ErrInputAcknowledgementMismatch) {
					t.Fatalf("receipt rejection class lost: %v", e)
				}
				if mode == "missing_budget" && !errors.Is(e, ErrBudgetUnavailable) {
					t.Fatalf("budget rejection class lost: %v", e)
				}
				if (mode == "submit_error" || mode == "await_error" || mode == "budget_error") && !errors.Is(e, sentinel) {
					t.Fatalf("cause lost: %v", e)
				}
				if (mode == "missing_budget" || mode == "budget_error") && w.submits != 0 {
					t.Fatal("budget refusal wrote native input")
				}
			}
		})
	}
}

// A pending same-epoch attempt retains stable input identity; after a controller
// epoch change it remains explicitly uncertain, never a new blind native write.
func TestGoalUncertainRestartAndExactConfirmation(t *testing.T) {
	s, a, root := fixture(t)
	w := newWriter()
	w.awaitErr = errors.New("ack unavailable")
	b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Execute(caller("launcher"), command("set", 0), b); e == nil {
		t.Fatal("missing ack admitted")
	}
	firstID := w.inputs[0].ID
	s, e = Open(root, a)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ConfirmNativeApplied(Owner("session-a", Assignment), core.RevisionRef{ID: "invented", Revision: 1}); e == nil {
		t.Fatal("unobserved confirmation admitted")
	}
	w2 := newWriter()
	b, e = s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 2}, Assignment, "codex", w2, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); !errors.Is(e, ErrDeliveryUncertain) || w2.submits != 0 {
		t.Fatalf("uncertain new epoch wrote: %v", e)
	}
	w.awaitErr = nil
	b, e = s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "codex", w, budgetFixture{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); e != nil {
		t.Fatal(e)
	}
	if w.inputs[len(w.inputs)-1].ID != firstID {
		t.Fatal("retry identity changed")
	}
	if _, e = s.Reconcile(Owner("session-a", Assignment), b); e != nil {
		t.Fatal(e)
	}
	if w.submits != 2 {
		t.Fatalf("already applied revision resubmitted: %d", w.submits)
	}
}

// Corrupt or foreign-mode journals fail closed without replacement. Returned
// records are copies; caller mutation cannot edit immutable stored content.
func TestGoalJournalRefusesCorruptionAndCopiesReads(t *testing.T) {
	s, a, root := fixture(t)
	if _, e := s.Handle(caller("launcher"), command("set", 0)); e != nil {
		t.Fatal(e)
	}
	head, e := s.Snapshot("session-a", Assignment)
	if e != nil {
		t.Fatal(e)
	}
	head.Latest.Content.Objective = "caller mutation"
	reread, e := s.Snapshot("session-a", Assignment)
	if e != nil || reread.Latest.Content.Objective == "caller mutation" {
		t.Fatal("mutable read escaped")
	}
	path := filepath.Join(root, "journal.json")
	if e = os.WriteFile(path, []byte("{"), 0600); e != nil {
		t.Fatal(e)
	}
	before := bytesAt(root)
	if _, e = Open(root, a); !errors.Is(e, ErrJournalInvalid) || bytesAt(root) != before {
		t.Fatalf("corrupt state replaced: %v", e)
	}
}

type budgetFixture struct{ err error }

func (b budgetFixture) WithBudget(ctx context.Context, lease core.ControllerLease, revision Revision, fn func() error) error {
	if b.err != nil {
		return b.err
	}
	return fn()
}

type writerFixture struct {
	submits                        int
	inputs                         []core.InputSubmission
	submitErr, awaitErr            error
	wrongReceipt, wrongCorrelation bool
	last                           core.InputReceipt
	expected                       core.AckExpectation
}

func newWriter() *writerFixture { return &writerFixture{} }
func (w *writerFixture) Submit(ctx context.Context, in core.InputSubmission) (core.InputReceipt, error) {
	w.submits++
	w.inputs = append(w.inputs, in)
	if w.submitErr != nil {
		return core.InputReceipt{}, w.submitErr
	}
	w.last = core.InputReceipt{SessionID: in.Lease.SessionID, Epoch: in.Lease.Epoch, ID: in.ID}
	w.expected = in.Expected
	return w.last, nil
}
func (w *writerFixture) Await(ctx context.Context, r core.InputReceipt) (core.InputObservation, error) {
	if w.awaitErr != nil {
		return core.InputObservation{}, w.awaitErr
	}
	o := core.InputObservation{Receipt: r, Kind: core.InputObservationKind(w.expected.Kind), Correlation: w.expected.Correlation}
	if w.wrongReceipt {
		o.Receipt.SessionID = "other"
	}
	if w.wrongCorrelation {
		o.Correlation = "other"
	}
	return o, nil
}
func (w *writerFixture) Cancel(context.Context, core.InputReceipt) error { return nil }

// Budget changes cannot create an unlimited assignment from launcher rights.
// Explicit operator authorization is the finite-budget positive neighbor.
func TestGoalBudgetAndCommandShapeRefusals(t *testing.T) {
	s, a, root := fixture(t)
	for _, mutate := range []func(*Command){func(c *Command) { c.Content.Budget.Tokens = 0 }, func(c *Command) { c.Content.Budget = Budget{Unlimited: true} }, func(c *Command) { c.Content.Condition = "" }, func(c *Command) { c.Namespace = "foreign" }, func(c *Command) { c.ExpectedRevision = -1 }} {
		c := command("invalid", 0)
		mutate(&c)
		before := bytesAt(root)
		if _, e := s.Handle(caller("launcher"), c); e == nil || bytesAt(root) != before {
			t.Fatalf("invalid goal gained writes: %+v %v", c, e)
		}
	}
	a.facts.Operator = true
	a.facts.UnlimitedBudget = true
	c := command("operator-unlimited", 0)
	c.Content.Budget = Budget{Unlimited: true}
	if _, e := s.Handle(caller("operator"), c); e != nil {
		t.Fatal(e)
	}
}
