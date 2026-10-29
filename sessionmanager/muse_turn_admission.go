package sessionmanager

import (
	"errors"
	"strings"
)

var ErrMuseTurnAlreadyCompleted = errors.New("Muse turn already completed")

func validateMuseTurnCommand(command *MuseTurnCommand) error {
	if command == nil || strings.TrimSpace(command.CommandID) == "" || museTurnVariants(command) != 1 {
		return ErrInvalidInput
	}
	if s := command.Start; s != nil && (strings.TrimSpace(s.Text) == "" || s.IfBusy != "queue" && s.IfBusy != "steer") {
		return ErrInvalidInput
	}
	if command.Interrupt != nil && strings.TrimSpace(command.Interrupt.TurnID) == "" {
		return ErrInvalidInput
	}
	if command.Steer != nil && (strings.TrimSpace(command.Steer.Text) == "" || strings.TrimSpace(command.Steer.ExpectedTurnID) == "") {
		return ErrInvalidInput
	}
	return nil
}

func copyInputObservation(observation InputObservation) InputObservation {
	if observation.ClaudeGoal != nil {
		value := *observation.ClaudeGoal
		observation.ClaudeGoal = &value
	}
	if observation.PTYRun != nil {
		value := *observation.PTYRun
		observation.PTYRun = &value
	}
	if observation.CodexTurn != nil {
		turn := *observation.CodexTurn
		observation.CodexTurn = &turn
	}
	if observation.MuseTurn != nil {
		turn := *observation.MuseTurn
		observation.MuseTurn = &turn
	}
	return observation
}

func validMuseTurnObservation(input InputSubmission, observation InputObservation, completed bool) bool {
	command, turn := input.Payload.MuseTurn, observation.MuseTurn
	if command == nil {
		return !completed && turn == nil
	}
	if turn == nil || turn.CommandID != command.CommandID {
		return false
	}
	if completed {
		return museTurnTarget(command) != "" && turn.TurnID == museTurnTarget(command) && turn.Disposition == "already-completed" && !turn.StartedNewTurn
	}
	if command.Interrupt != nil {
		return turn.TurnID == command.Interrupt.TurnID && turn.Disposition == "interrupted" && !turn.StartedNewTurn
	}
	if command.Steer != nil {
		return turn.TurnID == command.Steer.ExpectedTurnID && turn.Disposition == "steered" && !turn.StartedNewTurn
	}
	return strings.TrimSpace(turn.TurnID) != "" && (turn.Disposition == "started" || turn.Disposition == "queued" || turn.Disposition == "steered") && turn.StartedNewTurn == (turn.Disposition == "started")
}

func inputCommandID(input InputSubmission) string {
	if input.Payload.ClaudeGoal != nil {
		return input.Payload.ClaudeGoal.CommandID
	}
	if input.Payload.CodexTurn != nil {
		return input.Payload.CodexTurn.CommandID
	}
	if input.Payload.MuseGoal != nil {
		return input.Payload.MuseGoal.CommandID
	}
	if input.Payload.MuseTurn != nil {
		return input.Payload.MuseTurn.CommandID
	}
	return ""
}

func renderedInputVariants(input RenderedInput) int {
	variants := 0
	if input.ClaudeGoal != nil {
		variants++
	}
	if input.Text != nil {
		variants++
	}
	if input.MuseGoal != nil {
		variants++
	}
	if input.MuseTurn != nil {
		variants++
	}
	if input.CodexTurn != nil {
		variants++
	}
	return variants
}

func museTurnVariants(command *MuseTurnCommand) int {
	n := 0
	if command.Start != nil {
		n++
	}
	if command.Interrupt != nil {
		n++
	}
	if command.Steer != nil {
		n++
	}
	return n
}
func museTurnTarget(command *MuseTurnCommand) string {
	if command.Interrupt != nil {
		return command.Interrupt.TurnID
	}
	if command.Steer != nil {
		return command.Steer.ExpectedTurnID
	}
	return ""
}
