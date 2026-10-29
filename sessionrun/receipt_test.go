package sessionrun

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

type receiptWriter struct {
	observation core.InputObservation
	receipt     core.InputReceipt
	err         error
	submissions []core.InputSubmission
}

func (w *receiptWriter) Submit(_ context.Context, s core.InputSubmission) (core.InputReceipt, error) {
	w.submissions = append(w.submissions, s)
	return w.receipt, nil
}
func (w *receiptWriter) Await(context.Context, core.InputReceipt) (core.InputObservation, error) {
	return w.observation, w.err
}
func (w *receiptWriter) Cancel(context.Context, core.InputReceipt) error { return nil }

// A matched receipt/epoch/command is required; an ACK for another physical turn
// cannot become successful assignment evidence. These are isolated typed ports.
func TestCapturedMuseReceiptIdentity(t *testing.T) {
	l := core.ControllerLease{SessionID: "session", Epoch: 3}
	c := core.MuseTurnCommand{CommandID: "cmd", Start: &core.MuseTurnStart{Text: "work", IfBusy: "queue"}}
	r := core.InputReceipt{SessionID: l.SessionID, Epoch: l.Epoch, ID: c.CommandID}
	for _, name := range []string{"valid", "epoch", "receipt", "command", "correlation", "empty-turn", "kind"} {
		t.Run(name, func(t *testing.T) {
			w := &receiptWriter{receipt: r, observation: core.InputObservation{Receipt: r, Kind: "provider-ack", Correlation: c.CommandID, MuseTurn: &core.MuseTurnObservation{CommandID: c.CommandID, TurnID: "turn", Disposition: "started", StartedNewTurn: true}}}
			switch name {
			case "epoch":
				w.receipt.Epoch++
				w.observation.Receipt = w.receipt
			case "receipt":
				w.observation.Receipt.ID = "other"
			case "command":
				w.observation.MuseTurn.CommandID = "other"
			case "correlation":
				w.observation.Correlation = "other"
			case "empty-turn":
				w.observation.MuseTurn.TurnID = ""
			case "kind":
				w.observation.Kind = "timeout"
			}
			_, err := submitMuse(context.Background(), w, l, c)
			if name == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, core.ErrInputAcknowledgementMismatch) {
				t.Fatalf("foreign acknowledgement promoted: %v", err)
			}
		})
	}
}

// The no-turn interrupt outcome stays distinct from a new-turn ACK; only its
// exact target may resolve an already completed interrupt.
func TestCapturedMuseAlreadyCompletedTarget(t *testing.T) {
	l := core.ControllerLease{SessionID: "session", Epoch: 3}
	c := core.MuseTurnCommand{CommandID: "cmd", Interrupt: &core.MuseTurnInterrupt{TurnID: "turn"}}
	r := core.InputReceipt{SessionID: l.SessionID, Epoch: l.Epoch, ID: c.CommandID}
	w := &receiptWriter{receipt: r, err: core.ErrMuseTurnAlreadyCompleted, observation: core.InputObservation{Receipt: r, Kind: "already-completed", Correlation: c.CommandID, MuseTurn: &core.MuseTurnObservation{CommandID: c.CommandID, TurnID: "turn", Disposition: "already-completed"}}}
	if _, err := submitMuse(context.Background(), w, l, c); err != nil {
		t.Fatal(err)
	}
	w.observation.MuseTurn.TurnID = "foreign"
	if _, err := submitMuse(context.Background(), w, l, c); !errors.Is(err, core.ErrInputAcknowledgementMismatch) {
		t.Fatalf("wrong terminal target accepted: %v", err)
	}
}
