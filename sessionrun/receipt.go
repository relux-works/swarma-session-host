package sessionrun

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

func commandID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	return hex.EncodeToString(b[:]), nil
}

// Physical writes retain the captured lease. Await joins the host's retained
// receipt after its write fence is released; no run-wide admission lock is held.
func submitMuse(ctx context.Context, w core.HostInputWriter, l core.ControllerLease, c core.MuseTurnCommand) (core.InputObservation, error) {
	input := core.InputSubmission{Lease: l, ID: c.CommandID, Payload: core.RenderedInput{MuseTurn: &c}, Expected: core.AckExpectation{Kind: "provider-ack", Correlation: c.CommandID}}
	r, e := w.Submit(ctx, input)
	if e != nil {
		return core.InputObservation{}, e
	}
	o, e := w.Await(ctx, r)
	if e != nil && !errors.Is(e, core.ErrMuseTurnAlreadyCompleted) {
		return o, e
	}
	if r.SessionID != l.SessionID || r.Epoch != l.Epoch || r.ID != c.CommandID || o.Receipt != r || o.Correlation != c.CommandID || o.MuseTurn == nil || o.MuseTurn.CommandID != c.CommandID || strings.TrimSpace(o.MuseTurn.TurnID) == "" {
		return o, core.ErrInputAcknowledgementMismatch
	}
	if o.Kind == "already-completed" {
		if !errors.Is(e, core.ErrMuseTurnAlreadyCompleted) || o.MuseTurn.Disposition != "already-completed" || o.MuseTurn.StartedNewTurn {
			return o, core.ErrInputAcknowledgementMismatch
		}
		if c.Interrupt != nil {
			if o.MuseTurn.TurnID != c.Interrupt.TurnID {
				return o, core.ErrInputAcknowledgementMismatch
			}
			return o, nil
		}
		return o, core.ErrMuseTurnAlreadyCompleted
	}
	if e != nil {
		return o, e
	}
	if o.Kind != "provider-ack" {
		return o, core.ErrInputAcknowledgementMismatch
	}
	return o, nil
}

func submitCodex(ctx context.Context, w core.HostInputWriter, l core.ControllerLease, thread, text string) (core.InputObservation, error) {
	id, e := commandID()
	if e != nil {
		return core.InputObservation{}, e
	}
	input := core.InputSubmission{Lease: l, ID: id, Payload: core.RenderedInput{CodexTurn: &core.CodexTurnCommand{CommandID: id, Start: &core.CodexTurnStart{Text: text}}}, Expected: core.AckExpectation{Kind: "provider-ack", Correlation: id}}
	return submitCodexInput(ctx, w, l, thread, input)
}

func submitCodexInput(ctx context.Context, w core.HostInputWriter, l core.ControllerLease, thread string, input core.InputSubmission) (core.InputObservation, error) {
	id := input.ID
	r, e := w.Submit(ctx, input)
	if e != nil {
		return core.InputObservation{}, e
	}
	o, e := w.Await(ctx, r)
	if e != nil {
		return o, e
	}
	if r.SessionID != l.SessionID || r.Epoch != l.Epoch || r.ID != id || o.Receipt != r || o.Kind != "provider-ack" || o.Correlation != input.Expected.Correlation || o.CodexTurn == nil || o.CodexTurn.CommandID != input.Payload.CodexTurn.CommandID || o.CodexTurn.ThreadID != thread || strings.TrimSpace(o.CodexTurn.TurnID) == "" {
		return o, core.ErrInputAcknowledgementMismatch
	}
	return o, nil
}
