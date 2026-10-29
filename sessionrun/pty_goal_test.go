package sessionrun

import (
	"context"
	"errors"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
	"testing"
)

// Real host/typed receipt admission wraps the provider renderer. These tests
// prove physical delivery, not caller admission or durable goal promotion.
func TestPTYRevisionDeliveryUsesCheckedText(t *testing.T) {
	for _, unlimited := range []bool{false, true} {
		t.Run(map[bool]string{false: "finite", true: "unlimited"}[unlimited], func(t *testing.T) {
			_, _, f, _, capture := newCapturedPTY(t, false)
			revision := goal.Revision{SessionID: capture.Lease.SessionID, Namespace: goal.Assignment, Ref: core.RevisionRef{ID: "goal", Revision: 2}, State: goal.Active, Content: goal.Content{Objective: "original objective", Condition: "holder accepts", Budget: goal.Budget{Tokens: 50}}}
			if unlimited {
				revision.Content.Budget = goal.Budget{Unlimited: true}
			}
			factory, err := NewPTYDeliveryFactory(f.NativeIdentity())
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := factory.DeliveryForRevision(revision)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := core.NewReceiptCheckedGoalBinding(context.Background(), capture.Lease, capture.Input, delivery)
			if err != nil {
				t.Fatal(err)
			}
			ref := revision.Ref
			err = binding.Apply(&core.GoalEnvelope{Owner: goal.Owner(revision.SessionID, goal.Assignment), Ref: &ref})
			budget := `{"tokens":50,"unlimited":false}`
			if unlimited {
				budget = `{"tokens":0,"unlimited":true}`
			}
			want := "Session goal goal revision 2; state active.\nProvider condition: holder accepts\nObjective: original objective\nBudget: " + budget + "\nEnd your turn while waiting, do not poll. If an external decision blocks progress, begin a line with BLOCKED: and name the required decision."
			if err != nil || f.writes != 1 || f.prompt != want || f.observed != "" {
				t.Fatalf("actual revision Text receipt changed: %v writes%d prompt%q observer%q", err, f.writes, f.prompt, f.observed)
			}
		})
	}
}

// Foreign owner/ref/session, missing physical identity, invalid budget and
// mismatched clear state cannot offer a physical write. Wrong ACK cannot pass
// the checker; a nearby actual finite and explicit-unlimited receipt passes.
func TestPTYRevisionDeliveryRefusesForeignFacts(t *testing.T) {
	for _, mode := range []string{"owner", "ref", "session", "budget", "clear", "native", "wrong-ack"} {
		t.Run(mode, func(t *testing.T) {
			_, _, f, _, c := newCapturedPTY(t, false)
			r := goal.Revision{SessionID: c.Lease.SessionID, Namespace: goal.Assignment, Ref: core.RevisionRef{ID: "goal", Revision: 1}, State: goal.Active, Content: goal.Content{Objective: "work", Condition: "accept", Budget: goal.Budget{Tokens: 20}}}
			ref := r.Ref
			env := &core.GoalEnvelope{Owner: goal.Owner(r.SessionID, goal.Assignment), Ref: &ref}
			native := f.NativeIdentity()
			switch mode {
			case "owner":
				env.Owner = "foreign"
			case "ref":
				ref.Revision++
			case "session":
				r.SessionID = "foreign"
			case "budget":
				r.Content.Budget.Tokens = 0
			case "clear":
				env.Clear = true
			case "native":
				native = ""
			case "wrong-ack":
				f.mode = mode
			}
			factory, err := NewPTYDeliveryFactory(native)
			if err == nil {
				var delivery core.GoalDelivery
				delivery, err = factory.DeliveryForRevision(r)
				if err == nil {
					_, err = delivery.ApplyGoal(context.Background(), env, c.Lease, c.Input)
				}
			}
			want := 0
			if mode == "wrong-ack" {
				want = 1
				if !errors.Is(err, core.ErrInputAcknowledgementMismatch) {
					t.Fatalf("wrong ACK promoted: %v", err)
				}
			}
			if err == nil || f.writes != want || f.observed != "" {
				t.Fatalf("foreign revision offered physical input: %v writes%d observer%q", err, f.writes, f.observed)
			}
		})
	}
}

// Original rollback uses ordinary Text/run-observed evidence too; it neither
// fabricates a native clear ACK nor launches the owner request's work.
func TestPTYRevisionRollbackUsesCheckedText(t *testing.T) {
	_, _, f, _, c := newCapturedPTY(t, true)
	factory, err := NewPTYDeliveryFactory(f.NativeIdentity())
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := factory.DeliveryForRevision(goal.Revision{SessionID: c.Lease.SessionID, Namespace: goal.Assignment, State: goal.Cleared})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := core.NewReceiptCheckedGoalBinding(context.Background(), c.Lease, c.Input, delivery)
	if err != nil {
		t.Fatal(err)
	}
	err = binding.Rollback(&core.GoalEnvelope{Owner: goal.Owner(c.Lease.SessionID, goal.Assignment), Clear: true})
	if err != nil || f.writes != 1 || !strings.Contains(f.prompt, "binding has been rolled back") || f.observed != "" {
		t.Fatalf("rollback evidence: %v writes%d prompt%q", err, f.writes, f.prompt)
	}
}
