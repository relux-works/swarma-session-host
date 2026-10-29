package sessiongoal

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// ErrNativeDeferred means that a provider queued a directive without submitting
// native input. It is not an acknowledgement. Reconcile returns Pending with
// this error; a combined owner turn must execute in a fresh checked scope.
var ErrNativeDeferred = core.ErrGoalNativeDeferred

// RevisionDeliveryFactory belongs to the actual provider renderer. It receives
// the complete stored revision, never a request-authored projection or grant.
// A delivery may stage a directive and return ErrNativeDeferred before Submit.
// It must not retain the callback writer after ApplyGoal/RollbackGoal returns.
type RevisionDeliveryFactory interface {
	DeliveryForRevision(Revision) (core.GoalDelivery, error)
}

// RevisionAttemptFactory lets a renderer resume the exact already-journaled
// input instead of generating a new native command ID. It is optional for
// renderers whose retry input is already stable. Changed input/lease still
// refuses before effects; retained input data is not a caller grant.
type RevisionAttemptFactory interface {
	DeliveryForAttempt(Revision, core.InputSubmission) (core.GoalDelivery, error)
}

// NewDeliveryBinding is the provider-rendered form of NewBinding. Host
// orchestration supplies the admitted lease/writer/budget and keeps its existing
// caller/holder fence across reconciliation. This constructor grants no rights.
// No native goal observer or native accounting model is inferred from a name.
func (s *Service) NewDeliveryBinding(ctx context.Context, lease core.ControllerLease, namespace Namespace, provider string, writer core.HostInputWriter, budget BudgetBinding, factory RevisionDeliveryFactory) (core.GoalBinding, error) {
	if absent(factory) {
		return nil, ErrBindingUnavailable
	}
	binding, err := s.NewBinding(ctx, lease, namespace, provider, writer, budget)
	if err != nil {
		return nil, err
	}
	binding.(*receiptBinding).factory = factory
	return binding, nil
}

type revisionDelivery struct {
	record   *delivery
	provider core.GoalDelivery
}

func (d *revisionDelivery) ApplyGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	return d.perform(ctx, e, l, w, false)
}
func (d *revisionDelivery) RollbackGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	return d.perform(ctx, e, l, w, true)
}
func (d *revisionDelivery) perform(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter, rollback bool) (core.InputObservation, error) {
	if l != d.record.binding.lease {
		return core.InputObservation{}, ErrBindingUnavailable
	}
	life, cancel := context.WithCancel(ctx)
	defer cancel()
	journalWriter := &revisionWriter{record: d.record, envelope: e, parent: w, life: life, active: true}
	var observed core.InputObservation
	var err error
	if rollback {
		observed, err = d.provider.RollbackGoal(ctx, e, l, journalWriter)
	} else {
		observed, err = d.provider.ApplyGoal(ctx, e, l, journalWriter)
	}
	cancel() // stop and join any provider call that outlived its callback
	journalWriter.mu.Lock()
	defer journalWriter.mu.Unlock()
	journalWriter.active = false
	if errors.Is(err, ErrNativeDeferred) {
		if journalWriter.offered || journalWriter.failure != nil {
			return core.InputObservation{}, errors.Join(ErrDeliveryUncertain, journalWriter.failure)
		}
		return core.InputObservation{}, err
	}
	if err != nil || journalWriter.failure != nil {
		return core.InputObservation{}, errors.Join(err, journalWriter.failure)
	}
	if !journalWriter.awaited || !reflect.DeepEqual(observed, journalWriter.observed) {
		return core.InputObservation{}, core.ErrInputAcknowledgementMismatch
	}
	d.record.observation = journalWriter.observed
	return observed, nil
}

type revisionWriter struct {
	mu                       sync.Mutex
	record                   *delivery
	envelope                 *core.GoalEnvelope
	parent                   core.HostInputWriter
	life                     context.Context
	active, offered, awaited bool
	receipt                  core.InputReceipt
	input                    core.InputSubmission
	observed                 core.InputObservation
	failure                  error
}

// Hold mu for each whole call. Closing cancels life before taking mu, joining
// source transactions and writer calls before this delivery invocation returns.
func (w *revisionWriter) begin(ctx context.Context) (context.Context, func(), error) {
	if !w.active || w.life.Err() != nil {
		return nil, nil, core.ErrInputScopeClosed
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(w.life, cancel)
	if w.life.Err() != nil {
		cancel()
	}
	return call, func() { stop(); cancel() }, nil
}
func (w *revisionWriter) fail(err error) error { w.failure = errors.Join(w.failure, err); return err }
func (w *revisionWriter) Submit(ctx context.Context, in core.InputSubmission) (core.InputReceipt, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	call, end, err := w.begin(ctx)
	if err != nil {
		return core.InputReceipt{}, err
	}
	defer end()
	if w.offered {
		return core.InputReceipt{}, w.fail(core.ErrInputIDConflict)
	}
	w.offered = true
	prepared, err := w.record.prepareProviderAttempt(call, in, w.envelope)
	if err != nil {
		return core.InputReceipt{}, w.fail(err)
	}
	w.input = prepared
	receipt, err := w.parent.Submit(call, prepared)
	if err != nil {
		return core.InputReceipt{}, w.fail(err)
	}
	if err = w.record.recordReceipt(call, receipt); err != nil {
		return core.InputReceipt{}, w.fail(errors.Join(err, w.parent.Cancel(call, receipt)))
	}
	w.receipt = receipt
	return receipt, nil
}
func (w *revisionWriter) Await(ctx context.Context, receipt core.InputReceipt) (core.InputObservation, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	call, end, err := w.begin(ctx)
	if err != nil {
		return core.InputObservation{}, err
	}
	defer end()
	if w.receipt.ID == "" || receipt != w.receipt {
		return core.InputObservation{}, w.fail(core.ErrInputReceiptUnknown)
	}
	input := w.input
	observation, err := w.parent.Await(call, receipt)
	if err != nil {
		return core.InputObservation{}, w.fail(errors.Join(err, w.parent.Cancel(call, receipt)))
	}
	if observation.Receipt != receipt || observation.Kind != core.InputObservationKind(input.Expected.Kind) || observation.Correlation != input.Expected.Correlation || !core.ValidPTYInputObservation(input, observation) || !providerObservationMatchesInput(input, observation) {
		return core.InputObservation{}, w.fail(core.ErrInputAcknowledgementMismatch)
	}
	// Retain an independent native observation before the renderer can mutate it.
	var retained core.InputObservation
	encoded, err := json.Marshal(observation)
	if err == nil {
		err = json.Unmarshal(encoded, &retained)
	}
	if err != nil {
		return core.InputObservation{}, w.fail(err)
	}
	w.awaited = true
	w.observed = retained
	return observation, nil
}
func (w *revisionWriter) Cancel(ctx context.Context, receipt core.InputReceipt) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	call, end, err := w.begin(ctx)
	if err != nil {
		return err
	}
	defer end()
	if w.receipt.ID == "" || receipt != w.receipt {
		return w.fail(core.ErrInputReceiptUnknown)
	}
	return w.parent.Cancel(call, receipt)
}

// Provider input is journaled without rewriting native command IDs, exact text,
// session correlation or expectations. Retries must supply the same full input;
// a different payload cannot borrow a pending native receipt after restart.
func (d *delivery) prepareProviderAttempt(ctx context.Context, input core.InputSubmission, env *core.GoalEnvelope) (core.InputSubmission, error) {
	if input.Lease != d.binding.lease || input.Expected.Correlation == "" {
		return core.InputSubmission{}, ErrBindingUnavailable
	}
	switch input.Expected.Kind {
	case "provider-ack", "claude-goal-ack", "run-observed-after-snapshot":
	default:
		return core.InputSubmission{}, core.ErrInvalidInput
	}
	// One actual payload variant is mandatory before any journal/native effect.
	variants := 0
	for _, present := range []bool{input.Payload.Text != nil, input.Payload.MuseGoal != nil, input.Payload.MuseTurn != nil, input.Payload.CodexTurn != nil, input.Payload.ClaudeGoal != nil} {
		if present {
			variants++
		}
	}
	if variants != 1 {
		return core.InputSubmission{}, core.ErrInvalidInput
	}
	prepared, err := core.PrepareGoalInput(env, d.phase, input)
	if err != nil {
		return core.InputSubmission{}, err
	}
	encoded, err := json.Marshal(prepared)
	if err != nil {
		return core.InputSubmission{}, err
	}
	if err = json.Unmarshal(encoded, &prepared); err != nil {
		return core.InputSubmission{}, err
	}
	err = d.binding.service.transaction(ctx, true, func(j *journal) error {
		state := j.Sessions[d.binding.lease.SessionID]
		if state == nil {
			return ErrRevisionConflict
		}
		head := state.Heads[d.binding.namespace]
		if head == nil {
			return ErrRevisionConflict
		}
		if state.Holder != head.Latest.Holder || state.Generation != head.Latest.HolderGeneration {
			return ErrForbidden
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
			if prior.Ref != d.revision.Ref || prior.Lease != input.Lease || prior.Provider != d.binding.provider || !reflect.DeepEqual(prior.Input, prepared) {
				return ErrDeliveryUncertain
			}
			return errNoChange
		}
		// A completed/rolled-back effect cannot be reused as a fresh submission.
		for _, a := range head.Applications {
			if a.Receipt.ID == prepared.ID {
				return core.ErrInputIDConflict
			}
		}
		for _, r := range head.Rollbacks {
			if r.Receipt.ID == prepared.ID {
				return core.ErrInputIDConflict
			}
		}
		if head.DeliverySerial == ^uint64(0) {
			return ErrDeliveryUncertain
		}
		head.DeliverySerial++
		candidate := &deliveryAttempt{Serial: head.DeliverySerial, Input: prepared, Ref: d.revision.Ref, Lease: input.Lease, InputID: prepared.ID, Provider: d.binding.provider}
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

// These are shape/identity checks on the real writer observation, not external
// authentication. The host remains responsible for its accepted native snapshot.
func providerObservationMatchesInput(input core.InputSubmission, observation core.InputObservation) bool {
	if input.Payload.MuseTurn == nil && observation.MuseTurn != nil {
		return false
	}
	if input.Payload.CodexTurn == nil {
		return observation.CodexTurn == nil
	}
	command, actual := input.Payload.CodexTurn, observation.CodexTurn
	return command.Start != nil && actual != nil && actual.CommandID == command.CommandID && strings.TrimSpace(actual.ThreadID) != "" && strings.TrimSpace(actual.TurnID) != "" && observation.MuseTurn == nil && observation.ClaudeGoal == nil && observation.PTYRun == nil
}
