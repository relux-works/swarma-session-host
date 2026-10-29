package sessionmanager

import (
	"context"
	"errors"
	"testing"
)

type receiptDeliveryFunc func(context.Context, *GoalEnvelope, ControllerLease, HostInputWriter) (InputObservation, error)

func (f receiptDeliveryFunc) ApplyGoal(c context.Context, e *GoalEnvelope, l ControllerLease, w HostInputWriter) (InputObservation, error) {
	return f(c, e, l, w)
}
func (f receiptDeliveryFunc) RollbackGoal(c context.Context, e *GoalEnvelope, l ControllerLease, w HostInputWriter) (InputObservation, error) {
	return f(c, e, l, w)
}

// Real sessionInput retains the receipts; only the provider boundary is the
// existing test backend. A delivery cannot borrow another invocation's receipt,
// alter its observed correlation, or keep writing after its binding returns.
// These assertions do not qualify any native provider's ACK parser.
func TestReceiptCheckedBindingOwnsInvocationAndObservation(t *testing.T) {
	for _, mode := range []string{"valid", "borrowed", "foreign-correlation", "wrong-authority"} {
		t.Run(mode, func(t *testing.T) {
			ctx, h, lease, backend := inputFixture(t, true)
			envelope := &GoalEnvelope{Owner: "owner", Ref: &RevisionRef{ID: "goal", Revision: 1}}
			var retained HostInputWriter
			var borrowed InputObservation
			if mode == "borrowed" {
				i, _ := prepareGoalInput(envelope, "apply", nativeInput(lease, "borrowed", &MuseGoalCommand{CommandID: "borrowed", Set: &MuseGoalSet{Objective: "old"}}))
				r, err := h.Submit(ctx, i)
				if err != nil {
					t.Fatal(err)
				}
				borrowed, err = h.Await(ctx, r)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := backend.writeCount()
			delivery := receiptDeliveryFunc(func(c context.Context, e *GoalEnvelope, l ControllerLease, w HostInputWriter) (InputObservation, error) {
				retained = w
				if mode == "borrowed" {
					return borrowed, nil
				}
				i, err := prepareGoalInput(e, "apply", nativeInput(l, "new", &MuseGoalCommand{CommandID: "new", Set: &MuseGoalSet{Objective: "new"}}))
				if err != nil {
					return InputObservation{}, err
				}
				if mode == "wrong-authority" {
					i.ID = "unscoped"
				}
				r, err := w.Submit(c, i)
				if err != nil {
					return InputObservation{}, err
				}
				o, err := w.Await(c, r)
				if mode == "foreign-correlation" {
					o.Correlation = "another-command"
				}
				return o, err
			})
			binding, err := NewReceiptCheckedGoalBinding(ctx, lease, h, delivery)
			if err != nil {
				t.Fatal(err)
			}
			err = binding.Apply(envelope)
			want := map[string]error{"borrowed": ErrInputReceiptUnknown, "foreign-correlation": ErrInputAcknowledgementMismatch, "wrong-authority": ErrInputIDConflict}[mode]
			if !errors.Is(err, want) {
				t.Fatalf("%s: got %v want %v", mode, err, want)
			}
			expected := 1
			if mode == "borrowed" || mode == "wrong-authority" {
				expected = 0
			}
			if backend.writeCount() != before+expected {
				t.Fatal("rejection or positive control changed provider write count")
			}
			if _, err := retained.Submit(ctx, textInput(lease, "late", "late")); !errors.Is(err, ErrInputScopeClosed) {
				t.Fatalf("retained invocation writer remained live: %v", err)
			}
		})
	}
}
