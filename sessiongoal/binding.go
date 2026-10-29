package sessiongoal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type receiptBinding struct {
	service   *Service
	ctx       context.Context
	lease     core.ControllerLease
	namespace Namespace
	provider  string
	writer    core.HostInputWriter
	budget    BudgetBinding
	factory   RevisionDeliveryFactory
}
type delivery struct {
	binding     *receiptBinding
	revision    Revision
	phase       string
	observation core.InputObservation
}

var _ core.GoalDelivery = (*delivery)(nil)

func absent(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return v.IsNil()
	}
	return false
}

// NewBinding binds one real host writer/lease/provider to this goal service.
// It is constructed by host orchestration, never from command-supplied fields.
func (s *Service) NewBinding(ctx context.Context, lease core.ControllerLease, namespace Namespace, provider string, writer core.HostInputWriter, budget BudgetBinding) (core.GoalBinding, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lease.SessionID == "" || lease.Epoch == 0 || namespace != Assignment || absent(writer) {
		return nil, ErrBindingUnavailable
	}
	if provider != "codex" && provider != "claude" && provider != "muse" {
		return nil, core.ErrHostCapabilityUnavailable
	}
	return &receiptBinding{service: s, ctx: ctx, lease: lease, namespace: namespace, provider: provider, writer: writer, budget: budget}, nil
}
func (b *receiptBinding) Apply(env *core.GoalEnvelope) error    { return b.perform(env, "apply") }
func (b *receiptBinding) Rollback(env *core.GoalEnvelope) error { return b.perform(env, "rollback") }
func (b *receiptBinding) perform(env *core.GoalEnvelope, phase string) error {
	if env == nil || env.Owner != Owner(b.lease.SessionID, b.namespace) {
		return ErrBindingUnavailable
	}
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if absent(b.budget) {
		return ErrBudgetUnavailable
	}
	var revision Revision
	var err error
	if env.Ref != nil {
		revision, err = b.service.revision(env.Owner, env.Ref)
		if err != nil {
			return err
		}
		if (revision.State != Active) != env.Clear {
			return ErrInvalidCommand
		}
	} else {
		if phase != "rollback" || !env.Clear {
			return ErrInvalidCommand
		}
		revision = Revision{SessionID: b.lease.SessionID, Namespace: b.namespace, State: Cleared}
	}
	d := &delivery{binding: b, revision: revision, phase: phase}
	var nativeDelivery core.GoalDelivery = d
	if b.factory != nil {
		// A provider receives independent complete revision data, not mutable
		// journal storage or the ability to acknowledge without a real receipt.
		encoded, copyErr := json.Marshal(revision)
		if copyErr != nil {
			return copyErr
		}
		var snapshot Revision
		if copyErr = json.Unmarshal(encoded, &snapshot); copyErr != nil {
			return copyErr
		}
		var providerDelivery core.GoalDelivery
		var factoryErr error
		if resume, ok := b.factory.(RevisionAttemptFactory); ok {
			head, readErr := b.service.Snapshot(b.lease.SessionID, b.namespace)
			if readErr != nil {
				return readErr
			}
			attempt := head.Attempt
			if phase == "rollback" {
				attempt = head.Rollback
			}
			if attempt != nil {
				if attempt.Ref != revision.Ref || attempt.Lease != b.lease || attempt.Provider != b.provider {
					return ErrDeliveryUncertain
				}
				providerDelivery, factoryErr = resume.DeliveryForAttempt(snapshot, attempt.Input)
			} else {
				providerDelivery, factoryErr = b.factory.DeliveryForRevision(snapshot)
			}
		} else {
			providerDelivery, factoryErr = b.factory.DeliveryForRevision(snapshot)
		}
		if factoryErr != nil {
			return factoryErr
		}
		if absent(providerDelivery) {
			return ErrBindingUnavailable
		}
		nativeDelivery = &revisionDelivery{record: d, provider: providerDelivery}
	}
	checked, err := core.NewReceiptCheckedGoalBinding(b.ctx, b.lease, b.writer, nativeDelivery)
	if err != nil {
		return err
	}
	invoked := false
	var effectErr error
	err = b.budget.WithBudget(b.ctx, b.lease, revision, func() error {
		if invoked {
			effectErr = errors.Join(effectErr, ErrBudgetUnavailable)
			return ErrBudgetUnavailable
		}
		invoked = true
		if phase == "rollback" {
			effectErr = checked.Rollback(env)
			return effectErr
		}
		effectErr = checked.Apply(env)
		return effectErr
	})
	if err != nil || effectErr != nil {
		cause := errors.Join(err, effectErr)
		if invoked && effectErr == nil && phase == "apply" {
			return &acknowledgedEffectError{cause: cause}
		}
		return cause
	}
	if !invoked {
		return ErrBudgetUnavailable
	}
	if phase == "rollback" {
		return b.resolveRollback(d)
	}
	// Only the actual core checker has validated this observation at this point.
	err = b.service.transaction(b.ctx, true, func(j *journal) error {
		state := j.Sessions[b.lease.SessionID]
		if state == nil {
			return ErrRevisionConflict
		}
		head := state.Heads[b.namespace]
		if head == nil || head.Latest.Ref != revision.Ref || head.Attempt == nil || head.Attempt.Lease != b.lease {
			return ErrRevisionConflict
		}
		head.Acknowledged = &acknowledged{Ref: revision.Ref, Receipt: d.observation.Receipt}
		return nil
	})
	if err != nil {
		return &acknowledgedEffectError{cause: err}
	}
	return nil
}
func (d *delivery) ApplyGoal(ctx context.Context, env *core.GoalEnvelope, lease core.ControllerLease, writer core.HostInputWriter) (core.InputObservation, error) {
	return d.deliver(ctx, env, lease, writer)
}
func (d *delivery) RollbackGoal(ctx context.Context, env *core.GoalEnvelope, lease core.ControllerLease, writer core.HostInputWriter) (core.InputObservation, error) {
	return d.deliver(ctx, env, lease, writer)
}
func (d *delivery) deliver(ctx context.Context, env *core.GoalEnvelope, lease core.ControllerLease, writer core.HostInputWriter) (core.InputObservation, error) {
	if lease != d.binding.lease {
		return core.InputObservation{}, ErrBindingUnavailable
	}
	var observation core.InputObservation
	send := func(current *core.NativeGoalObservation, writer core.HostInputWriter) error {
		payload, expected, err := render(d.revision, d.binding.provider, d.phase, env.Clear, current)
		if err != nil {
			return err
		}
		input, err := d.prepareAttempt(ctx, core.InputSubmission{Lease: lease, Payload: payload, Expected: expected}, env)
		if err != nil {
			return err
		}
		receipt, err := writer.Submit(ctx, input)
		if err != nil {
			return err
		}
		if err = d.recordReceipt(ctx, receipt); err != nil {
			return errors.Join(err, writer.Cancel(ctx, receipt))
		}
		observation, err = writer.Await(ctx, receipt)
		if err != nil {
			return errors.Join(err, writer.Cancel(ctx, receipt))
		}
		d.observation = observation
		return nil
	}
	var err error
	if d.binding.provider == "muse" {
		observer, ok := writer.(core.NativeGoalObserver)
		if !ok {
			return observation, core.ErrHostCapabilityUnavailable
		}
		err = observer.WithNativeGoal(ctx, lease, func(native core.NativeGoalObservation, fenced core.HostInputWriter) error {
			if native.SessionID != lease.SessionID || native.Epoch != lease.Epoch {
				return core.ErrInputAcknowledgementMismatch
			}
			return send(&native, fenced)
		})
	} else {
		err = send(nil, writer)
	}
	return observation, err
}

// Each new native effect has a durable serial. A retry reuses the exact
// persisted input, including a Muse set/edit decision made before its first send.
func (d *delivery) prepareAttempt(ctx context.Context, input core.InputSubmission, env *core.GoalEnvelope) (core.InputSubmission, error) {
	var prepared core.InputSubmission
	err := d.binding.service.transaction(ctx, true, func(j *journal) error {
		state := j.Sessions[d.binding.lease.SessionID]
		if state == nil {
			return ErrRevisionConflict
		}
		head := state.Heads[d.binding.namespace]
		if head == nil {
			return ErrRevisionConflict
		}
		prior := head.Attempt
		if d.phase == "rollback" {
			prior = head.Rollback
		} else {
			if head.Latest.Ref != d.revision.Ref {
				return ErrRevisionConflict
			}
			if head.Rollback != nil {
				return ErrDeliveryUncertain
			}
		}
		if prior != nil {
			if prior.Ref != d.revision.Ref || prior.Lease != input.Lease || prior.Provider != d.binding.provider {
				return ErrDeliveryUncertain
			}
			prepared = prior.Input
			return errNoChange
		}
		if head.DeliverySerial == ^uint64(0) {
			return ErrDeliveryUncertain
		}
		head.DeliverySerial++
		serial := head.DeliverySerial
		input.ID = fmt.Sprintf("native/%d", serial)
		var err error
		input, err = deliveryIdentity(input, serial)
		if err != nil {
			return err
		}
		prepared, err = core.PrepareGoalInput(env, d.phase, input)
		if err != nil {
			return err
		}
		candidate := &deliveryAttempt{Serial: serial, Input: prepared, Ref: d.revision.Ref, Lease: input.Lease, InputID: prepared.ID, Provider: d.binding.provider}
		if d.phase == "rollback" {
			head.Rollback = candidate
		} else {
			head.Attempt = candidate
			head.Acknowledged = nil
		}
		return nil
	})
	return prepared, err
}

// A core-checked rollback is a known native outcome. Retire its attempt with its
// receipt, preserving the goal head; a subsequent apply gets a fresh serial.
// Failed or uncommitted rollbacks retain the closed recovery obligation.
func (b *receiptBinding) resolveRollback(d *delivery) error {
	return b.service.transaction(b.ctx, true, func(j *journal) error {
		state := j.Sessions[b.lease.SessionID]
		if state == nil {
			return ErrRevisionConflict
		}
		head := state.Heads[b.namespace]
		if head == nil || head.Rollback == nil || head.Rollback.Lease != b.lease || head.Rollback.Receipt == nil || *head.Rollback.Receipt != d.observation.Receipt {
			return core.ErrInputAcknowledgementMismatch
		}
		receipt := d.observation.Receipt
		var target *core.RevisionRef
		head.Applied = nil
		head.AppliedReceipt = nil
		if d.revision.Ref.Revision > 0 {
			ref := d.revision.Ref
			target = &ref
			head.Applied = &ref
			head.AppliedReceipt = &receipt
			r := d.revision
			head.Applications = append(head.Applications, Application{Ref: ref, SourceEnvelopeID: r.SourceEnvelopeID, Holder: r.Holder, HolderGeneration: r.HolderGeneration, Receipt: receipt})
		}
		head.Rollbacks = append(head.Rollbacks, NativeRollback{Target: target, Receipt: receipt})
		head.Attempt = nil
		head.Rollback = nil
		head.Acknowledged = nil
		record := j.Envelopes[head.Latest.SourceEnvelopeID]
		record.Receipt.Pending = head.Applied == nil || *head.Applied != head.Latest.Ref
		j.Envelopes[head.Latest.SourceEnvelopeID] = record
		return nil
	})
}
func (d *delivery) recordReceipt(ctx context.Context, receipt core.InputReceipt) error {
	if receipt.ID == "" || receipt.SessionID != d.binding.lease.SessionID || receipt.Epoch != d.binding.lease.Epoch {
		return core.ErrInputAcknowledgementMismatch
	}
	return d.binding.service.transaction(ctx, true, func(j *journal) error {
		state := j.Sessions[d.binding.lease.SessionID]
		if state == nil {
			return ErrRevisionConflict
		}
		head := state.Heads[d.binding.namespace]
		if head == nil {
			return ErrRevisionConflict
		}
		attempt := head.Attempt
		if d.phase == "rollback" {
			attempt = head.Rollback
		}
		if attempt == nil || attempt.Ref != d.revision.Ref || attempt.Lease != d.binding.lease || receipt.ID != attempt.InputID {
			return core.ErrInputAcknowledgementMismatch
		}
		attempt.Receipt = &receipt
		return nil
	})
}
