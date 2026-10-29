package sessionmanager

import (
	"errors"
	"fmt"
)

var (
	ErrMuseHostStopped              = errors.New("Muse provider host is stopped")
	ErrMuseServerCrashed            = errors.New("Muse serve process crashed")
	ErrMuseUnknownRevisionAck       = errors.New("Muse resume snapshot did not contain the requested board revision")
	ErrMuseGoalAckTimeout           = errors.New("timed out reading the Muse goal after command replay")
	ErrMuseSessionIdentity          = errors.New("Muse session identity changed during restore")
	ErrMuseInteractiveApproval      = errors.New("Muse provider host requires a non-interactive approval mode")
	ErrMuseUnpinnedBinary           = errors.New("Muse provider host requires the injected pinned Muse binary")
	ErrMuseInvalidHostConfig        = errors.New("invalid Muse provider host configuration")
	ErrMuseGoalStateUnsupported     = errors.New("Muse provider host only applies active board goals")
	ErrMuseGoalCommandRejected      = errors.New("Muse server rejected the goal command")
	ErrMuseBudgetPauseNotApplicable = errors.New("Muse budget pause is not applicable in the native goal state")
	ErrMuseNoActiveTurn             = errors.New("Muse session has no active turn to steer")
	ErrMuseInvalidSessionStartup    = errors.New("Muse session/start returned no session identity")
	ErrMuseDurabilityUnsupported    = errors.New("Muse provider host requires durable session history")
	ErrMuseTurnAckRejected          = errors.New("Muse server turn acknowledgement was rejected")
	ErrMuseCommandIDConflict        = errors.New("Muse server rejected an idempotent command replay")
	ErrMuseGoalSnapshotUnavailable  = errors.New("Muse goal snapshot could not be read authoritatively")
	ErrMuseServerIdentity           = errors.New("Muse server identity does not match the pinned export")
	ErrMuseServerKnownBad           = errors.New("Muse server identity is on the known-bad list")
	ErrMuseBudgetPauseFailed        = errors.New("Muse budget goal/pause was rejected or timed out")
	ErrMuseApprovalDenyUnavailable  = errors.New("Muse pending approval has no once-scoped plain denial")
	ErrMuseUserInputUnresolvable    = errors.New("Muse pending user input cannot be resolved")
	ErrMuseBudgetPauseStale         = errors.New("Muse budget pause intent is stale: the live goal or its budget moved")
	ErrMuseFrozenVerifyFailed       = errors.New("Muse frozen tuple failed pre-exec verification")
)

type MuseHostError struct {
	Code         string
	NativeStatus string
	Cause        error
}

func (e *MuseHostError) Error() string {
	if e == nil {
		return "Muse provider host error"
	}
	if e.Cause != nil {
		if e.NativeStatus != "" {
			return fmt.Sprintf("Muse provider host %s (native status %q): %v", e.Code, e.NativeStatus, e.Cause)
		}
		return fmt.Sprintf("Muse provider host %s: %v", e.Code, e.Cause)
	}
	if e.NativeStatus != "" {
		return fmt.Sprintf("Muse provider host %s (native status %q)", e.Code, e.NativeStatus)
	}
	return "Muse provider host " + e.Code
}

func (e *MuseHostError) Unwrap() error { return e.Cause }

func (e *MuseHostError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch e.Code {
	case "stopped":
		return target == ErrMuseHostStopped
	case "server_crashed":
		return target == ErrMuseServerCrashed
	case "unknown_revision_ack":
		return target == ErrMuseUnknownRevisionAck
	case "goal_ack_timeout":
		return target == ErrMuseGoalAckTimeout
	case "session_identity":
		return target == ErrMuseSessionIdentity
	case "interactive_approval":
		return target == ErrMuseInteractiveApproval
	case "unpinned_binary":
		return target == ErrMuseUnpinnedBinary
	case "invalid_config":
		return target == ErrMuseInvalidHostConfig
	case "unsupported_goal_state":
		return target == ErrMuseGoalStateUnsupported
	case "goal_command_rejected":
		return target == ErrMuseGoalCommandRejected
	case "budget_pause_not_applicable":
		return target == ErrMuseBudgetPauseNotApplicable
	case "no_active_turn":
		return target == ErrMuseNoActiveTurn
	case "invalid_session_start":
		return target == ErrMuseInvalidSessionStartup
	case "durability_unsupported":
		return target == ErrMuseDurabilityUnsupported
	case "turn_ack":
		return target == ErrMuseTurnAckRejected
	case "command_id_conflict":
		return target == ErrMuseCommandIDConflict
	case "goal_snapshot_unavailable":
		return target == ErrMuseGoalSnapshotUnavailable
	case "server_identity":
		return target == ErrMuseServerIdentity
	case "server_known_bad":
		return target == ErrMuseServerKnownBad
	case "budget_pause_failed":
		return target == ErrMuseBudgetPauseFailed
	case "approval_deny_unavailable":
		return target == ErrMuseApprovalDenyUnavailable
	case "user_input_unresolvable":
		return target == ErrMuseUserInputUnresolvable
	case "budget_pause_stale":
		return target == ErrMuseBudgetPauseStale
	case "frozen_verify_failed":
		return target == ErrMuseFrozenVerifyFailed
	default:
		return false
	}
}
