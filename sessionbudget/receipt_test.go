package sessionbudget

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
	"time"
)

type receiptFixture struct {
	input       core.InputSubmission
	observed    core.InputObservation
	wrong       bool
	effectError error
}

func (f *receiptFixture) Submit(_ context.Context, i core.InputSubmission) (core.InputReceipt, error) {
	f.input = i
	return core.InputReceipt{SessionID: i.Lease.SessionID, Epoch: i.Lease.Epoch, ID: i.ID}, f.effectError
}
func (f *receiptFixture) Await(_ context.Context, r core.InputReceipt) (core.InputObservation, error) {
	o := core.InputObservation{Receipt: r, Kind: core.InputObservationKind(f.input.Expected.Kind), Correlation: f.input.Expected.Correlation, ObservedAt: time.Unix(1, 0)}
	if f.wrong {
		o.Correlation = "wrong"
	}
	f.observed = o
	return o, nil
}
func (*receiptFixture) Cancel(context.Context, core.InputReceipt) error { return nil }

// Only a receipt issued for this exact lease/input and its matching observed
// correlation can justify accounting promotion. A writer error is not proof
// that native input did not occur; an issued receipt remains recorded.
func TestBudgetReceiptIdentityAndUncertainEffects(t *testing.T) {
	for _, mode := range []string{"valid", "wrong_ack", "wrong_epoch", "issued_then_error", "no_receipt"} {
		t.Run(mode, func(t *testing.T) {
			lease := core.ControllerLease{SessionID: "s", Epoch: 2}
			f := &receiptFixture{}
			trace := &deliveryTrace{lease: lease}
			w := traceWriter(f, trace)
			if mode == "wrong_ack" {
				f.wrong = true
			}
			if mode == "issued_then_error" {
				f.effectError = errors.New("lost response")
			}
			in := core.InputSubmission{Lease: lease, ID: "i", Expected: core.AckExpectation{Kind: "provider-ack", Correlation: "c"}}
			if mode == "wrong_epoch" {
				in.Lease.Epoch = 1
			}
			if mode != "no_receipt" {
				r, e := w.Submit(context.Background(), in)
				if e == nil {
					_, _ = w.Await(context.Background(), r)
				}
			}
			if mode == "valid" {
				if _, e := trace.checked(); e != nil {
					t.Fatal(e)
				}
			} else if _, e := trace.checked(); e == nil {
				t.Fatal("unmatched receipt promoted accounting")
			}
			if mode == "issued_then_error" && !trace.possibleEffect() {
				t.Fatal("lost issued receipt was treated as no effect")
			}
		})
	}
}
