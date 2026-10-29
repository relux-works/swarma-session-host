package sessionmanager

import (
	"context"
	"errors"
)

// executeTurn runs only within sessionInput's epoch/writer fence. It owns
// native protocol validation; no directive identity or accounting enters it.
func (b *museInputBackend) executeTurn(ctx context.Context, input InputSubmission, receipt InputReceipt) (func(context.Context) (InputObservation, error), error) {
	command := input.Payload.MuseTurn
	if err := validateMuseTurnCommand(command); err != nil {
		return nil, err
	}
	if err := b.host.ensureRunning(); err != nil {
		return nil, err
	}
	completed := func() (func(context.Context) (InputObservation, error), error) {
		observation := InputObservation{Receipt: receipt, Kind: "already-completed", Correlation: command.CommandID, MuseTurn: &MuseTurnObservation{CommandID: command.CommandID, TurnID: museTurnTarget(command), Disposition: "already-completed"}}
		return func(c context.Context) (InputObservation, error) {
			if err := c.Err(); err != nil {
				return InputObservation{}, err
			}
			return copyInputObservation(observation), ErrMuseTurnAlreadyCompleted
		}, nil
	}
	if museTurnTarget(command) != "" && b.host.client.museTurnCompleted(b.host.sessionID, museTurnTarget(command)) {
		return completed()
	}
	var result MuseTurnObservation
	var operation string
	var call func(context.Context) error
	if start := command.Start; start != nil {
		operation = "turn/start"
		params := MSPTurnStartParams{CommandID: command.CommandID, SessionID: b.host.sessionID, Input: []MSPTurnInputPart{{Type: "text", Text: start.Text}}, IfBusy: MSPIfBusy(start.IfBusy)}
		call = func(c context.Context) error {
			ack, err := b.host.client.TurnStart(c, params)
			if err != nil {
				return err
			}
			ack, err = validateMuseTurnStartAck(ack, command.CommandID)
			if err != nil {
				return err
			}
			result = MuseTurnObservation{CommandID: ack.CommandID, TurnID: ack.TurnID, Disposition: MuseTurnDisposition(ack.Disposition), StartedNewTurn: ack.StartedNewTurn}
			return nil
		}
	} else if steer := command.Steer; steer != nil {
		operation = "turn/steer"
		params := MSPTurnSteerParams{CommandID: command.CommandID, SessionID: b.host.sessionID, ExpectedTurnID: steer.ExpectedTurnID, Input: []MSPTurnInputPart{{Type: "text", Text: steer.Text}}}
		call = func(c context.Context) error {
			ack, err := b.host.client.TurnSteer(c, params)
			if err != nil {
				if b.host.client.museTurnCompleted(b.host.sessionID, params.ExpectedTurnID) {
					return ErrMuseTurnAlreadyCompleted
				}
				return err
			}
			if ack.CommandID != command.CommandID || ack.Status != MSPCommandAccepted {
				return ErrInputAcknowledgementMismatch
			}
			if b.host.client.museTurnCompleted(b.host.sessionID, params.ExpectedTurnID) {
				return ErrMuseTurnAlreadyCompleted
			}
			if ack.TurnID != params.ExpectedTurnID {
				return ErrInputAcknowledgementMismatch
			}
			result = MuseTurnObservation{CommandID: ack.CommandID, TurnID: ack.TurnID, Disposition: "steered", StartedNewTurn: false}
			return nil
		}
	} else {
		operation = "turn/interrupt"
		params := MSPTurnInterruptParams{CommandID: command.CommandID, SessionID: b.host.sessionID, TurnID: command.Interrupt.TurnID}
		call = func(c context.Context) error {
			ack, err := b.host.client.TurnInterrupt(c, params)
			if err != nil {
				return err
			}
			completed := b.host.client.museTurnCompleted(b.host.sessionID, params.TurnID)
			if ack.CommandID != command.CommandID {
				if !completed {
					return museActiveInterruptMismatch()
				}
				return ErrInputAcknowledgementMismatch
			}
			if ack.Status != MSPCommandAccepted {
				return ErrInputAcknowledgementMismatch
			}
			if completed || ack.TurnID == "" {
				return ErrMuseTurnAlreadyCompleted
			}
			if ack.TurnID != params.TurnID {
				return museActiveInterruptMismatch()
			}
			result = MuseTurnObservation{CommandID: ack.CommandID, TurnID: ack.TurnID, Disposition: "interrupted"}
			return nil
		}
	}
	attempt := func() error {
		callCtx, cancel := b.host.goalAckContext(ctx)
		defer cancel()
		return call(callCtx)
	}
	err := attempt()
	if err != nil && ctx.Err() == nil && museGoalOperationRetryable(err, b.host.client) {
		err = attempt()
	}
	if errors.Is(err, ErrMuseTurnAlreadyCompleted) {
		return completed()
	}
	if err != nil {
		return nil, b.host.wrapOperationError(operation, err)
	}
	return observedInput(InputObservation{Receipt: receipt, Kind: "provider-ack", Correlation: command.CommandID, MuseTurn: &result}), nil
}

// q19: only a command/target mismatch for an uncompleted native interrupt
// carries terminal protocol authority. No canonical goal or budget enters it.
func museActiveInterruptMismatch() error {
	return &MuseHostError{Code: "active_interrupt_ack_mismatch", Cause: errors.Join(ErrInputAcknowledgementMismatch, ErrMuseObservationUncertain)}
}
