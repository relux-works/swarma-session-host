package sessionmanager

import "strings"

func validateCodexTurnCommand(command *CodexTurnCommand) error {
	if command == nil || strings.TrimSpace(command.CommandID) == "" || command.Start == nil || strings.TrimSpace(command.Start.Text) == "" {
		return ErrInvalidInput
	}
	return nil
}

// The host owns the native runner, independently of transport-supplied receipt
// fields. Require its accepted thread and managed turn before publishing an ack.
// No current snapshot identity means refusal, never a guessed successful receipt.
func validCodexTurnObservation(backend hostInputBackend, input InputSubmission, observation InputObservation) bool {
	command, turn := input.Payload.CodexTurn, observation.CodexTurn
	if command == nil {
		return turn == nil
	}
	if turn == nil || turn.CommandID != command.CommandID || strings.TrimSpace(turn.ThreadID) == "" || strings.TrimSpace(turn.TurnID) == "" {
		return false
	}
	runner, ok := backend.(RunnerHost)
	if !ok {
		return false
	}
	snapshot, err := runner.Snapshot()
	return err == nil && snapshot.Kind == "codex" && snapshot.Codex != nil && !snapshot.Codex.Stopped && snapshot.Codex.ThreadID == turn.ThreadID && snapshot.Codex.ManagedTurnID == turn.TurnID
}
