package sessionmanager

import (
	"errors"
	"strings"
)

var (
	ErrStaleInputController             = errors.New("input controller epoch is stale or unbound")
	ErrNativeGoalObservationUnavailable = errors.New("native goal observation is unavailable")
	ErrNativeGoalChanged                = errors.New("native goal changed since observation")
	ErrInvalidMuseGoalCommand           = errors.New("invalid typed Muse goal command")
)

// The host calls these checks under its writer fence, using its own observed
// native state. A caller-supplied observation cannot substitute for that state.
func validateNativeGoalObservation(lease ControllerLease, observed NativeGoalObservation) error {
	if lease.SessionID == "" || lease.Epoch == 0 || observed.SessionID != lease.SessionID || observed.Epoch == 0 || observed.Epoch != lease.Epoch {
		return ErrStaleInputController
	}
	if observed.Sequence == 0 || observed.ObservedAt.IsZero() {
		return ErrNativeGoalObservationUnavailable
	}
	if usage := observed.Cumulative; usage != nil && (usage.OutputTokens < 0 || usage.PromptTokens < 0 || usage.TotalTokens < 0) {
		return ErrNativeGoalObservationUnavailable
	}
	return nil
}

func validateMuseGoalCommand(lease ControllerLease, observed *NativeGoalObservation, command *MuseGoalCommand) error {
	if command == nil || strings.TrimSpace(command.CommandID) == "" {
		return ErrInvalidMuseGoalCommand
	}
	variants := 0
	if command.Set != nil {
		variants++
	}
	if command.Edit != nil {
		variants++
	}
	if command.Pause != nil {
		variants++
	}
	if command.Clear != nil {
		variants++
	}
	if variants != 1 {
		return ErrInvalidMuseGoalCommand
	}
	if command.Set != nil && strings.TrimSpace(command.Set.Objective) == "" {
		return ErrInvalidMuseGoalCommand
	}
	if command.Edit != nil && strings.TrimSpace(command.Edit.Objective) == "" {
		return ErrInvalidMuseGoalCommand
	}
	if command.Pause != nil {
		if observed == nil {
			return ErrNativeGoalObservationUnavailable
		}
		if err := validateNativeGoalObservation(lease, *observed); err != nil {
			return err
		}
		if command.Pause.Sequence != observed.Sequence {
			return ErrNativeGoalChanged
		}
	}
	return nil
}
