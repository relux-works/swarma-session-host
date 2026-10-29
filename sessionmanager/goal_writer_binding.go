package sessionmanager

import (
	"context"
	"reflect"
)

// ReconcileWithWriter keeps the fork's canonical transaction behind actual
// host receipt admission. A renderer/feed's lookalike observation is not an
// acknowledgement; the host's retained receipt and checked Await are required.
// NewReceiptCheckedGoalBinding admits only receipts from this invocation's writer.
// Canonical services invoke this binding through their existing reconciliation transaction.
func NewReceiptCheckedGoalBinding(ctx context.Context, lease ControllerLease, writer HostInputWriter, delivery GoalDelivery) (GoalBinding, error) {
	if absentHostPort(writer) || absentHostPort(delivery) {
		return nil, ErrHostCapabilityUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return receiptCheckedGoalBinding{ctx: ctx, lease: lease, writer: writer, delivery: delivery}, nil
}

type receiptCheckedGoalBinding struct {
	ctx      context.Context
	lease    ControllerLease
	writer   HostInputWriter
	delivery GoalDelivery
}

func (b receiptCheckedGoalBinding) Apply(envelope *GoalEnvelope) error {
	scope, writer := newGoalWriterScope(b.ctx, b.writer, envelope, "apply")
	defer scope.close()
	observation, err := b.delivery.ApplyGoal(b.ctx, envelope, b.lease, writer)
	if err != nil {
		return err
	}
	return b.check(observation, scope)
}
func (b receiptCheckedGoalBinding) Rollback(envelope *GoalEnvelope) error {
	scope, writer := newGoalWriterScope(b.ctx, b.writer, envelope, "rollback")
	defer scope.close()
	observation, err := b.delivery.RollbackGoal(b.ctx, envelope, b.lease, writer)
	if err != nil {
		return err
	}
	return b.check(observation, scope)
}
func (b receiptCheckedGoalBinding) check(observation InputObservation, scope *goalWriterScope) error {
	receipt := observation.Receipt
	if receipt.ID == "" || receipt.SessionID != b.lease.SessionID || receipt.Epoch != b.lease.Epoch {
		return ErrInputAcknowledgementMismatch
	}
	if !scope.owns(receipt) {
		return ErrInputReceiptUnknown
	}
	checked, err := b.writer.Await(b.ctx, receipt)
	if err != nil {
		return err
	}
	scope.mu.Lock()
	input, own := scope.inputs[receipt]
	scope.mu.Unlock()
	if !own || checked.Receipt != receipt || checked.Kind != InputObservationKind(input.Expected.Kind) || checked.Correlation == "" || checked.Correlation != input.Expected.Correlation || observation.Kind != checked.Kind || observation.Correlation != checked.Correlation {
		return ErrInputAcknowledgementMismatch
	}
	if checked.Kind != "provider-ack" && checked.Kind != "claude-goal-ack" && checked.Kind != "run-observed-after-snapshot" {
		return ErrInputAcknowledgementMismatch
	}
	if !ValidPTYInputObservation(input, checked) || !ValidPTYInputObservation(input, observation) || checked.EvidenceClass != observation.EvidenceClass || !reflect.DeepEqual(checked.ClaudeGoal, observation.ClaudeGoal) || !reflect.DeepEqual(checked.PTYRun, observation.PTYRun) {
		return ErrInputAcknowledgementMismatch
	}
	return nil
}
