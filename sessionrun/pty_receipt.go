package sessionrun

import (
	"context"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

// submitPTY preserves the original typed Text/run-observed-after-snapshot
// protocol. The identity must come from the actual captured native host; it
// is not a caller-controlled evidence label or a spending/accounting fact.
func submitPTY(ctx context.Context, w core.HostInputWriter, l core.ControllerLease, native, text string) (core.InputObservation, error) {
	if w == nil || l.SessionID == "" || l.Epoch == 0 || strings.TrimSpace(native) == "" || strings.TrimSpace(text) == "" || strings.ContainsAny(text, "\x1b\x00\r") {
		return core.InputObservation{}, core.ErrInvalidInput
	}
	id, e := commandID()
	if e != nil {
		return core.InputObservation{}, e
	}
	input := core.InputSubmission{Lease: l, ID: id, Payload: core.RenderedInput{Text: &text}, Expected: core.AckExpectation{Kind: "run-observed-after-snapshot", Correlation: native}}
	receipt, e := w.Submit(ctx, input)
	if e != nil {
		return core.InputObservation{}, e
	}
	observation, e := w.Await(ctx, receipt)
	if e != nil {
		return observation, e
	}
	if receipt.SessionID != l.SessionID || receipt.Epoch != l.Epoch || receipt.ID != id || observation.Receipt != receipt || observation.Kind != "run-observed-after-snapshot" || observation.Correlation != native || observation.MuseTurn != nil || observation.CodexTurn != nil || !core.ValidPTYInputObservation(input, observation) {
		return observation, core.ErrInputAcknowledgementMismatch
	}
	return observation, nil
}
