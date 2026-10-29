package sessionrun

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

type ptyReceiptWriter struct {
	mode   string
	writes int
	input  core.InputSubmission
}

func (w *ptyReceiptWriter) Submit(_ context.Context, in core.InputSubmission) (core.InputReceipt, error) {
	w.writes++
	w.input = in
	return core.InputReceipt{SessionID: in.Lease.SessionID, Epoch: in.Lease.Epoch, ID: in.ID}, nil
}
func (w *ptyReceiptWriter) Await(_ context.Context, r core.InputReceipt) (core.InputObservation, error) {
	o := core.InputObservation{Receipt: r, Kind: "run-observed-after-snapshot", Correlation: w.input.Expected.Correlation, EvidenceClass: core.InputEvidenceRunObservedAfterSnapshot, PTYRun: &core.PTYRunObservation{RunID: "actual-run"}}
	switch w.mode {
	case "epoch":
		o.Receipt.Epoch++
	case "correlation":
		o.Correlation = "foreign"
	case "class":
		o.EvidenceClass = core.InputEvidenceExactProviderAck
	case "kind":
		o.Kind = "provider-ack"
	case "run":
		o.PTYRun.RunID = ""
	case "extra-native":
		o.MuseTurn = &core.MuseTurnObservation{TurnID: "foreign"}
	}
	return o, nil
}
func (*ptyReceiptWriter) Cancel(context.Context, core.InputReceipt) error { return nil }

// A physical PTY receipt proves only the actual run set difference after the
// host's snapshot. It is never relabelled as an exact provider/native goal ACK.
func TestCapturedPTYRunReceiptRetainsEvidenceClass(t *testing.T) {
	l := core.ControllerLease{SessionID: "session", Epoch: 4}
	for _, name := range []string{"valid", "epoch", "correlation", "class", "kind", "run", "extra-native"} {
		t.Run(name, func(t *testing.T) {
			writer := &ptyReceiptWriter{mode: name}
			observation, e := submitPTY(context.Background(), writer, l, "native-session", "  payload\nkept  ")
			if name == "valid" {
				if e != nil || observation.PTYRun.RunID != "actual-run" || observation.EvidenceClass != core.InputEvidenceRunObservedAfterSnapshot || writer.input.Payload.Text == nil || *writer.input.Payload.Text != "  payload\nkept  " {
					t.Fatalf("actual PTY evidence/payload changed: %+v %v", observation, e)
				}
			} else if !errors.Is(e, core.ErrInputAcknowledgementMismatch) {
				t.Fatalf("foreign PTY evidence promoted: %+v %v", observation, e)
			}
		})
	}
}

// Missing actual native identity or invalid physical text refuses before the
// writer, with a nearby multiline valid payload exercised above.
func TestCapturedPTYRunInputRefusalsBeforeEffect(t *testing.T) {
	l := core.ControllerLease{SessionID: "session", Epoch: 4}
	for _, tc := range []struct{ name, native, text string }{{"native", "", "work"}, {"blank", "native", " \n"}, {"escape", "native", "work\x1b"}, {"nul", "native", "work\x00"}, {"carriage", "native", "work\r"}} {
		t.Run(tc.name, func(t *testing.T) {
			w := &ptyReceiptWriter{}
			if _, e := submitPTY(context.Background(), w, l, tc.native, tc.text); !errors.Is(e, core.ErrInvalidInput) || w.writes != 0 {
				t.Fatalf("invalid PTY input reached native effect: %v writes%d", e, w.writes)
			}
		})
	}
}
