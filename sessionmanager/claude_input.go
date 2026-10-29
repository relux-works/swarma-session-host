package sessionmanager

import (
	"context"
	"time"

	"github.com/relux-works/swarma-session-host/sessionmanager/runner/historyguard"
)

func (*claudeHost) SupportsNativeGoals() bool { return false }
func (*claudeHost) ObserveNativeGoal(context.Context) (NativeGoalObservation, error) {
	return NativeGoalObservation{}, ErrHostCapabilityUnavailable
}

func (*claudeHost) AcceptsInput(input InputSubmission) bool {
	return input.ID != "" && input.Lease.SessionID != "" && input.Lease.Epoch != 0 && renderedInputVariants(input.Payload) == 1 &&
		validateClaudeGoalCommand(input.Payload.ClaudeGoal) == nil && input.Expected.Kind == "claude-goal-ack" && input.Expected.Correlation == input.Payload.ClaudeGoal.CommandID
}

// The scanner is armed under the physical writer before either command or Enter.
// Execute returns after writes quiesce; the bounded returned waiter only reads.
func (h *claudeHost) ExecuteInput(ctx context.Context, input InputSubmission, receipt InputReceipt) (func(context.Context) (InputObservation, error), error) {
	if !h.AcceptsInput(input) {
		return nil, ErrInvalidInput
	}
	if receipt != (InputReceipt{SessionID: input.Lease.SessionID, Epoch: input.Lease.Epoch, ID: input.ID}) {
		return nil, ErrInputAcknowledgementMismatch
	}
	command := copyInputSubmission(input).Payload.ClaudeGoal
	text, token, operation := "", "", ClaudeGoalOperationClear
	var conditioned []claudeConditionedMarker
	success := []string{"goal cleared", "no goal set"}
	if command.Set != nil {
		text, token, operation = command.Set.Text, command.Set.Token, ClaudeGoalOperationSet
		success = nil
		conditioned = claudeGoalConditionedMarkers(token)
	} else {
		text = command.Clear.Text
	}
	if err := historyguard.LockMutex(ctx, &h.writeMu); err != nil {
		return nil, err
	}
	waiter, err := h.submitGoalCommandLocked(ctx, text, success, conditioned, []string{"unknown command"})
	h.writeMu.Unlock()
	if err != nil {
		return nil, err
	}
	budget, _ := h.goalAckBudget()
	deadline := time.NewTimer(budget)
	// sessionInput calls this waiter once and owns cancellation/join. A direct
	// backend caller also receives deterministic read-only repeated results.
	done := make(chan struct{})
	var result InputObservation
	var resultErr error
	go func() {
		defer close(done)
		defer deadline.Stop()
		defer h.scanner.cancel(waiter)
		select {
		case <-ctx.Done():
			resultErr = ctx.Err()
			return
		case <-h.exited:
			resultErr = ErrProviderHostNotFound
			return
		case <-deadline.C:
			resultErr = context.DeadlineExceeded
			return
		case match := <-waiter.done:
			if err := ctx.Err(); err != nil {
				resultErr = err
				return
			}
			if err := claudeGoalAckError(match); err != nil {
				resultErr = err
				return
			}
			class := match.evidence
			if operation == ClaudeGoalOperationClear {
				class = InputEvidenceClaudePTYClearObserved
			}
			result = InputObservation{Receipt: receipt, Kind: "claude-goal-ack", Correlation: command.CommandID, EvidenceClass: class,
				ClaudeGoal: &ClaudeGoalObservation{CommandID: command.CommandID, Operation: operation, ObservedText: match.observed, NormalizedToken: token}}
		}
	}()
	return func(wait context.Context) (InputObservation, error) {
		if ctx.Err() != nil {
			<-done
		}
		if err := wait.Err(); err != nil {
			return InputObservation{}, err
		}
		select {
		case <-wait.Done():
			if ctx.Err() != nil {
				<-done
			}
			return InputObservation{}, wait.Err()
		case <-done:
		}
		if err := wait.Err(); err != nil {
			return InputObservation{}, err
		}
		return copyInputObservation(result), resultErr
	}, nil
}

// Typed native input already carries the admitted immutable revision token.
// Both delivery and captured-rendering regression tests use the actual scanner
// markers; canonical revision-to-token derivation remains private.
func claudeGoalConditionedMarkers(token string) []claudeConditionedMarker {
	result := make([]claudeConditionedMarker, 0, 4)
	for _, prefix := range []string{"goal set:", "goal updated:", "goal active:", "goal:"} {
		result = append(result, claudeConditionedMarker{prefix: prefix, key: token, threshold: claudeGoalTokenCoverageThreshold})
	}
	return result
}
