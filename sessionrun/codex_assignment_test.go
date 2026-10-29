package sessionrun

import (
	"context"
	"errors"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

type retainedAssignmentWriter struct {
	core.HostInputWriter
	input core.InputSubmission
}

func (w *retainedAssignmentWriter) Submit(ctx context.Context, in core.InputSubmission) (core.InputReceipt, error) {
	w.input = in
	return w.HostInputWriter.Submit(ctx, in)
}

type combinedAssignmentDelivery struct {
	thread, directive, prompt string
	observed                  core.InputObservation
}

func (d *combinedAssignmentDelivery) ApplyGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	o, err := submitCodexAssignment(ctx, w, l, d.thread, d.directive, d.prompt, e, "apply")
	d.observed = o
	return o, err
}
func (d *combinedAssignmentDelivery) RollbackGoal(context.Context, *core.GoalEnvelope, core.ControllerLease, core.HostInputWriter) (core.InputObservation, error) {
	return core.InputObservation{}, core.ErrHostCapabilityUnavailable
}

// One actual leased Start retains both directive and owner prompt; a fresh core
// scope verifies the native thread/turn receipt. This component does not supply
// canonical staging/promotion or infer the original owner-run launch fact.
func TestCodexCombinedAssignmentRetainsBothPayloads(t *testing.T) {
	_, _, native, _, c := runtimeFixture(t)
	writer := &retainedAssignmentWriter{HostInputWriter: c.Input}
	delivery := &combinedAssignmentDelivery{thread: "thread", directive: "admitted directive\nselected budget: none", prompt: "  actual owner prompt  "}
	binding, err := core.NewReceiptCheckedGoalBinding(context.Background(), c.Lease, writer, delivery)
	if err != nil {
		t.Fatal(err)
	}
	ref := core.RevisionRef{ID: "goal", Revision: 3}
	err = binding.Apply(&core.GoalEnvelope{Owner: goal.Owner(c.Lease.SessionID, goal.Assignment), Ref: &ref})
	want := "admitted directive\nselected budget: none\n\nactual owner prompt"
	if err != nil || native.writes != 1 || writer.input.Payload.CodexTurn == nil || writer.input.Payload.CodexTurn.Start == nil || writer.input.Payload.CodexTurn.Start.Text != want || delivery.observed.CodexTurn == nil || delivery.observed.CodexTurn.ThreadID != "thread" || delivery.observed.CodexTurn.TurnID != "turn" {
		t.Fatalf("combined assignment lost goal or owner prompt: err%v writes%d input%+v observed%+v", err, native.writes, writer.input, delivery.observed)
	}
	if writer.input.ID == "" || writer.input.Payload.CodexTurn.CommandID != writer.input.Expected.Correlation {
		t.Fatal("prepared assignment lost exact command identity")
	}
}

// Blank work/identity and a malformed envelope have no native effects; a wrong
// actual ACK may have written once but cannot produce an admitted receipt.
func TestCodexCombinedAssignmentRefusals(t *testing.T) {
	for _, mode := range []string{"prompt", "thread", "envelope", "wrong-ack"} {
		t.Run(mode, func(t *testing.T) {
			_, _, native, _, c := runtimeFixture(t)
			thread, prompt := "thread", "work"
			ref := core.RevisionRef{ID: "goal", Revision: 1}
			e := &core.GoalEnvelope{Owner: goal.Owner(c.Lease.SessionID, goal.Assignment), Ref: &ref}
			switch mode {
			case "prompt":
				prompt = " "
			case "thread":
				thread = ""
			case "envelope":
				e.Owner = ""
			case "wrong-ack":
				native.wrongACK = true
			}
			o, err := submitCodexAssignment(context.Background(), c.Input, c.Lease, thread, "directive", prompt, e, "apply")
			want := 0
			if mode == "wrong-ack" {
				want = 1
				if !errors.Is(err, core.ErrInputAcknowledgementMismatch) {
					t.Fatalf("wrong combined ACK promoted: %+v %v", o, err)
				}
			}
			if err == nil || native.writes != want {
				t.Fatalf("forbidden combined assignment effect: %+v %v writes%d", o, err, native.writes)
			}
		})
	}
}
