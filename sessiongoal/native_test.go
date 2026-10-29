package sessiongoal

import (
	"context"
	"errors"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type nativeWriterFixture struct {
	*writerFixture
	native core.NativeGoalObservation
}

func (w *nativeWriterFixture) WithNativeGoal(ctx context.Context, lease core.ControllerLease, fn func(core.NativeGoalObservation, core.HostInputWriter) error) error {
	return fn(w.native, w)
}

// A native completed observation can choose native set versus edit, but cannot
// accept/clear the durable assignment. Wrong-session/epoch observations cause
// no input. The observation and writer are explicit library test capabilities.
func TestGoalNativeObservationNeverAcceptsAssignment(t *testing.T) {
	for _, mode := range []string{"completed", "wrong_session", "wrong_epoch"} {
		t.Run(mode, func(t *testing.T) {
			s, _, _ := fixture(t)
			w := &nativeWriterFixture{writerFixture: newWriter(), native: core.NativeGoalObservation{SessionID: "session-a", Epoch: 1, Objective: "previous", Status: "completed"}}
			if mode == "wrong_session" {
				w.native.SessionID = "other"
			}
			if mode == "wrong_epoch" {
				w.native.Epoch = 2
			}
			b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "muse", w, budgetFixture{})
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.Execute(caller("launcher"), command("set", 0), b)
			head, re := s.Snapshot("session-a", Assignment)
			if re != nil {
				t.Fatal(re)
			}
			if head.Latest.State != Active || head.Latest.Acceptance != nil {
				t.Fatalf("native observation accepted assignment: %+v", head)
			}
			if mode == "completed" {
				if e != nil || w.submits != 1 || w.inputs[0].Payload.MuseGoal.Set == nil {
					t.Fatalf("valid fenced native delivery failed: %v", e)
				}
			} else if !errors.Is(e, core.ErrInputAcknowledgementMismatch) || w.submits != 0 || head.Applied != nil {
				t.Fatalf("foreign native fact admitted: %v", e)
			}
		})
	}
}

type claudeWriterFixture struct {
	*writerFixture
	text, token string
	evidence    core.InputEvidenceClass
}

func (w *claudeWriterFixture) Await(ctx context.Context, r core.InputReceipt) (core.InputObservation, error) {
	input := w.inputs[len(w.inputs)-1]
	return core.InputObservation{Receipt: r, Kind: core.InputObservationKind(input.Expected.Kind), Correlation: input.Expected.Correlation, EvidenceClass: w.evidence, ClaudeGoal: &core.ClaudeGoalObservation{CommandID: input.Payload.ClaudeGoal.CommandID, Operation: core.ClaudeGoalOperationSet, ObservedText: w.text, NormalizedToken: w.token}}, nil
}

// The REAL core PTY evidence checker admits a conditioned exact-revision
// marker and rejects command echo/another revision even when receipt metadata
// looks successful. These are synthetic observed-output fixtures, not a native
// provider qualification or a substitute scanner implementation.
func TestGoalClaudeBindingRequiresConditionedObservedRevision(t *testing.T) {
	for _, mode := range []string{"conditioned", "lossy_conditioned", "command_echo", "foreign_revision"} {
		t.Run(mode, func(t *testing.T) {
			s, _, _ := fixture(t)
			receipt, e := s.Handle(caller("launcher"), command("set", 0))
			if e != nil {
				t.Fatal(e)
			}
			token := RevisionToken(receipt.Ref)
			w := &claudeWriterFixture{writerFixture: newWriter(), text: "goal set: " + token, token: token, evidence: core.InputEvidenceClaudePTYConditioned}
			if mode == "lossy_conditioned" {
				w.text = "Goal set: " + token
				w.evidence = core.InputEvidenceClaudePTYLossyConditioned
			}
			if mode == "command_echo" {
				w.text = "/goal " + token
			}
			if mode == "foreign_revision" {
				w.token = "tbg-other"
				w.text = "Goal set: tbg-other"
			}
			b, e := s.NewBinding(context.Background(), core.ControllerLease{SessionID: "session-a", Epoch: 1}, Assignment, "claude", w, budgetFixture{})
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.Reconcile(Owner("session-a", Assignment), b)
			head, re := s.Snapshot("session-a", Assignment)
			if re != nil {
				t.Fatal(re)
			}
			if mode == "conditioned" || mode == "lossy_conditioned" {
				if e != nil || head.Applied == nil || len(head.Applications) != 1 {
					t.Fatalf("valid conditioned receipt lost: %v", e)
				}
			} else if !errors.Is(e, core.ErrInputAcknowledgementMismatch) || head.Applied != nil {
				t.Fatalf("unobserved revision applied: %v", e)
			}
		})
	}
}
