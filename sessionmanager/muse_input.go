package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"math"
)

// museInputBackend is a host-owned MSP transport. It knows native commands and
// observations only; canonical refs, policies and accounting stay in the fork.
// All access to observed is serialized by its sessionInput writer fence.
type museInputBackend struct {
	host     *museInputTransport
	observed NativeGoalObservation
}

var _ hostInputBackend = (*museInputBackend)(nil)

func (b *museInputBackend) SupportsNativeGoals() bool {
	return b.host.nativeGoals
}

func (b *museInputBackend) AcceptsInput(input InputSubmission) bool {
	if input.Payload.MuseGoal != nil {
		return b.SupportsNativeGoals()
	}
	if input.Payload.MuseTurn != nil {
		return input.Expected.Kind == "provider-ack"
	}
	return input.Payload.Text != nil && input.Expected.Kind == "written"
}

func (b *museInputBackend) ObserveNativeGoal(ctx context.Context) (NativeGoalObservation, error) {
	var drain mspWriteDrain
	defer drain.wait()
	return b.observe(drain.bind(ctx))
}

func (b *museInputBackend) observe(ctx context.Context) (NativeGoalObservation, error) {
	if err := b.host.ensureRunning(); err != nil {
		return NativeGoalObservation{}, err
	}
	readCtx, cancel := b.host.goalAckContext(ctx)
	defer cancel()
	for {
		resumed, goal, responseSequence, err := b.host.readResumeGoal(readCtx, MSPHistorySnapshot)
		if err != nil {
			return NativeGoalObservation{}, err
		}
		b.host.client.mu.Lock()
		sequence := b.host.client.goalEventSeq
		b.host.client.mu.Unlock()
		if sequence > responseSequence {
			if err := readCtx.Err(); err != nil {
				return NativeGoalObservation{}, err
			}
			continue // A newer goal event invalidated the snapshot while it was read.
		}
		if !goal.Known || !goal.Present || sequence == math.MaxUint64 {
			return NativeGoalObservation{}, ErrNativeGoalObservationUnavailable
		}
		// The response-frame sequence itself changes on every read. Use the
		// goal-change watermark (+1 for nonzero initial state) so rereading an
		// unchanged goal does not falsely invalidate the decision's sequence.
		state := NativeGoalObservation{Objective: goal.Objective, Status: goal.Status, Sequence: sequence + 1, Cumulative: copyMuseNativeUsage(goal.Cumulative)}
		state.EffectiveApprovalMode, state.EffectiveModelID = museStatedNativeTerms(resumed.Session, goal, b.host.requestedApproval, b.host.requestedModel)
		if goal.Cleared {
			state.Objective, state.Status = "", ""
		}
		b.observed = state
		b.observed.Cumulative = copyMuseNativeUsage(state.Cumulative)
		return state, nil
	}
}

func (b *museInputBackend) ExecuteInput(ctx context.Context, input InputSubmission, receipt InputReceipt) (func(context.Context) (InputObservation, error), error) {
	var drain mspWriteDrain
	defer drain.wait()
	ctx = drain.bind(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !b.AcceptsInput(input) {
		return nil, ErrInvalidInput
	}
	if input.Payload.MuseTurn != nil {
		return b.executeTurn(ctx, input, receipt)
	}
	if input.Payload.Text != nil {
		callCtx, cancel := b.host.goalAckContext(ctx)
		defer cancel()
		if _, err := b.host.sendTurn(callCtx, *input.Payload.Text, MSPIfBusyQueue); err != nil {
			return nil, err
		}
		return observedInput(InputObservation{Receipt: receipt, Kind: "written", Correlation: input.Expected.Correlation}), nil
	}
	command := input.Payload.MuseGoal
	operation, expectedObjective, expectedStatus := "", "", ""
	sessionID := b.host.snapshotSessionID()
	var send func(context.Context) (MSPGoalCommandResult, error)
	switch {
	case command.Set != nil:
		operation, expectedObjective = "goal/set", command.Set.Objective
		send = func(c context.Context) (MSPGoalCommandResult, error) {
			return b.host.client.GoalSet(c, MSPGoalSetParams{CommandID: command.CommandID, SessionID: sessionID, Objective: command.Set.Objective})
		}
	case command.Edit != nil:
		operation, expectedObjective = "goal/edit", command.Edit.Objective
		send = func(c context.Context) (MSPGoalCommandResult, error) {
			return b.host.client.GoalEdit(c, MSPGoalEditParams{CommandID: command.CommandID, SessionID: sessionID, Objective: command.Edit.Objective})
		}
	case command.Pause != nil:
		if command.Pause.Sequence != b.observed.Sequence || b.observed.Objective == "" {
			return nil, ErrNativeGoalChanged
		}
		operation, expectedObjective, expectedStatus = "goal/pause", b.observed.Objective, "paused"
		send = func(c context.Context) (MSPGoalCommandResult, error) {
			return b.host.client.GoalPause(c, MSPGoalPauseParams{CommandID: command.CommandID, SessionID: sessionID})
		}
	case command.Clear != nil:
		operation = "goal/clear"
		send = func(c context.Context) (MSPGoalCommandResult, error) {
			return b.host.client.GoalClear(c, MSPGoalClearParams{CommandID: command.CommandID, SessionID: sessionID})
		}
	default:
		return nil, ErrInvalidMuseGoalCommand
	}
	call := func(parent context.Context) (MSPGoalCommandResult, error) {
		callCtx, cancel := b.host.goalAckContext(parent)
		defer cancel()
		return send(callCtx)
	}
	result, err := call(ctx)
	replayed := false
	if err != nil && ctx.Err() == nil && museGoalOperationRetryable(err, b.host.client) {
		replayed = true
		// Per-attempt deadlines may replay while the host operation is live.
		// An explicit Cancel or lost controller cannot acquire a new lifetime.
		result, err = call(ctx) // Same typed command and stable CommandID.
	}
	if err != nil {
		return nil, b.host.wrapGoalWriteError(operation, err)
	}
	if result.CommandID != command.CommandID || result.Status != MSPCommandAccepted {
		return nil, &MuseHostError{Code: "unknown_revision_ack", Cause: ErrInputAcknowledgementMismatch}
	}
	state, err := b.observe(ctx)
	if err != nil && !replayed && ctx.Err() == nil && museGoalOperationRetryable(err, b.host.client) {
		result, err = call(ctx)
		if err != nil {
			return nil, b.host.wrapGoalWriteError(operation, err)
		}
		if result.CommandID != command.CommandID || result.Status != MSPCommandAccepted {
			return nil, &MuseHostError{Code: "unknown_revision_ack", Cause: ErrInputAcknowledgementMismatch}
		}
		state, err = b.observe(ctx)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, &MuseHostError{Code: "goal_ack_timeout", Cause: errors.Join(ErrMuseGoalAckTimeout, err)}
		}
		return nil, &MuseHostError{Code: "goal_snapshot_unavailable", Cause: errors.Join(ErrMuseGoalSnapshotUnavailable, err)}
	}
	if state.Objective != expectedObjective || expectedStatus != "" && state.Status != expectedStatus || command.Clear != nil && state.Status != "" {
		return nil, &MuseHostError{Code: "unknown_revision_ack", Cause: fmt.Errorf("%s observed a different native goal: %w", operation, ErrNativeGoalChanged)}
	}
	return observedInput(InputObservation{Receipt: receipt, Kind: "provider-ack", Correlation: command.CommandID}), nil
}
