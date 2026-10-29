package sessionrun

import (
	"context"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

// The actual captured pending directive and owner work share one native Start.
// A canonical caller must invoke this only inside its fresh receipt scope; no
// scoped writer or queued directive can acknowledge a revision by itself.
func submitCodexAssignment(ctx context.Context, w core.HostInputWriter, l core.ControllerLease, thread, directive, prompt string, envelope *core.GoalEnvelope, phase string) (core.InputObservation, error) {
	if w == nil || l.SessionID == "" || l.Epoch == 0 || strings.TrimSpace(thread) == "" || strings.TrimSpace(prompt) == "" {
		return core.InputObservation{}, core.ErrInvalidInput
	}
	text := strings.TrimSpace(prompt)
	if directive != "" {
		text = directive + "\n\n" + text
	}
	id, err := commandID()
	if err != nil {
		return core.InputObservation{}, err
	}
	input := core.InputSubmission{ID: id, Lease: l, Payload: core.RenderedInput{CodexTurn: &core.CodexTurnCommand{CommandID: id, Start: &core.CodexTurnStart{Text: text}}}, Expected: core.AckExpectation{Kind: "provider-ack", Correlation: id}}
	if envelope != nil {
		input, err = core.PrepareGoalInput(envelope, phase, input)
		if err != nil {
			return core.InputObservation{}, err
		}
	}
	return submitCodexInput(ctx, w, l, thread, input)
}
