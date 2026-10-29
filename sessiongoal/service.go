package sessiongoal

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

var errNoChange = errors.New("goal journal unchanged")
var _ core.GoalSource = (*Service)(nil)

func validNamespace(n Namespace) bool { return n == Assignment || n == Working }

// Owner is an opaque core routing key, never a caller authority claim.
func Owner(session string, namespace Namespace) core.OwnerKey {
	return core.OwnerKey("session-goal/" + base64.RawURLEncoding.EncodeToString([]byte(session)) + "/" + string(namespace))
}
func target(owner core.OwnerKey) (string, Namespace, error) {
	parts := strings.Split(string(owner), "/")
	if len(parts) != 3 || parts[0] != "session-goal" || !validNamespace(Namespace(parts[2])) {
		return "", "", ErrInvalidCommand
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(raw) == 0 || !utf8.Valid(raw) {
		return "", "", ErrInvalidCommand
	}
	return string(raw), Namespace(parts[2]), nil
}
func referenceID(session string, namespace Namespace) string {
	sum := sha256.Sum256([]byte(string(Owner(session, namespace))))
	return "GOAL-SG-" + strings.ToUpper(hex.EncodeToString(sum[:16]))
}
func validateCommand(c Command) error {
	if strings.TrimSpace(c.EnvelopeID) == "" || strings.TrimSpace(c.SessionID) == "" || !utf8.ValidString(c.SessionID) || !validNamespace(c.Namespace) || c.ExpectedRevision < 0 {
		return ErrInvalidCommand
	}
	switch c.Action {
	case Set:
		if c.Content == nil || c.Acceptance != nil || !validContent(*c.Content) {
			return ErrInvalidCommand
		}
	case Clear:
		if c.Content != nil || c.Acceptance != nil {
			return ErrInvalidCommand
		}
	case Accept:
		if c.Content != nil || c.Acceptance == nil || (c.Acceptance.Kind != HolderAcceptance && c.Acceptance.Kind != RelayedBarrier) || strings.TrimSpace(c.Acceptance.Evidence) == "" {
			return ErrInvalidCommand
		}
	default:
		return ErrInvalidCommand
	}
	return nil
}
func validContent(c Content) bool {
	return utf8.ValidString(c.Objective) && utf8.ValidString(c.Condition) && strings.TrimSpace(c.Objective) != "" && strings.TrimSpace(c.Condition) != "" && !strings.ContainsAny(c.Condition, "\r\n\x00") && ((c.Budget.Tokens > 0 && !c.Budget.Unlimited) || (c.Budget.Tokens == 0 && c.Budget.Unlimited))
}
func policy(c Command, f VerifiedCaller) error {
	if f.SessionID != c.SessionID || f.Principal == "" || f.Holder == "" || f.Generation == 0 {
		return ErrForbidden
	}
	switch c.Namespace {
	case Assignment:
		if !f.Assignment || (f.Principal != f.Holder && !f.Operator) {
			return ErrForbidden
		}
		if c.Action == Accept && f.Principal != f.Holder {
			return ErrForbidden
		}
	case Working:
		if !f.Working || f.WorkingPrincipal == "" || f.Principal != f.WorkingPrincipal {
			return ErrForbidden
		}
	default:
		return ErrForbidden
	}
	if c.Action == Set && c.Content.Budget.Unlimited && (!f.Operator || !f.UnlimitedBudget) {
		return ErrForbidden
	}
	return nil
}

func (s *Service) authorized(ctx context.Context, c Command, fn func(Command, VerifiedCaller) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Snapshot caller-owned pointers before either the admission callback or I/O.
	encoded, err := json.Marshal(c)
	if err != nil {
		return ErrInvalidCommand
	}
	if err = json.Unmarshal(encoded, &c); err != nil {
		return ErrInvalidCommand
	}
	if err = validateCommand(c); err != nil {
		return err
	}
	if absent(s.authority) {
		return ErrAuthorityUnavailable
	}
	called := false
	var operationErr error
	err = s.authority.WithVerifiedCaller(ctx, c.SessionID, func(f VerifiedCaller) error {
		if called {
			operationErr = errors.Join(operationErr, ErrForbidden)
			return ErrForbidden
		}
		called = true
		if err := policy(c, f); err != nil {
			operationErr = err
			return err
		}
		operationErr = fn(c, f)
		return operationErr
	})
	if err == nil && !called {
		return ErrAuthorityUnavailable
	}
	return errors.Join(err, operationErr)
}

// Handle durably accepts an authorized typed command. Native application is a
// separate, receipt-checked reconciliation and is visible as Pending until then.
func (s *Service) Handle(ctx context.Context, c Command) (Receipt, error) {
	var result Receipt
	err := s.authorized(ctx, c, func(c Command, f VerifiedCaller) error { var err error; result, err = s.handle(ctx, c, f); return err })
	return result, err
}

// Execute keeps admission's holder fence across command acceptance and native
// reconciliation. An unauthorized request reaches neither journal nor writer.
func (s *Service) Execute(ctx context.Context, c Command, binding core.GoalBinding) (Receipt, error) {
	var result Receipt
	err := s.authorized(ctx, c, func(c Command, f VerifiedCaller) error {
		var err error
		result, err = s.handle(ctx, c, f)
		if err != nil {
			return err
		}
		head, err := s.Snapshot(c.SessionID, c.Namespace)
		if err != nil {
			return err
		}
		if head.Latest.Ref == result.Ref {
			if _, err = s.Reconcile(Owner(c.SessionID, c.Namespace), binding); err != nil {
				return err
			}
		}
		return s.transaction(ctx, false, func(j *journal) error { result.Pending = j.Envelopes[c.EnvelopeID].Receipt.Pending; return nil })
	})
	return result, err
}
func (s *Service) handle(ctx context.Context, c Command, f VerifiedCaller) (Receipt, error) {
	var result Receipt
	encoded, _ := json.Marshal(c)
	sum := sha256.Sum256(encoded)
	digest := hex.EncodeToString(sum[:])
	err := s.transaction(ctx, true, func(j *journal) error {
		// Persisted handoff also fences an exact replay from stale admission.
		if current := j.Sessions[c.SessionID]; current != nil && (f.Generation < current.Generation || (f.Generation == current.Generation && f.Holder != current.Holder)) {
			return ErrForbidden
		}
		if prior, ok := j.Envelopes[c.EnvelopeID]; ok {
			if prior.Digest != digest || prior.Principal != f.Principal || prior.Generation != f.Generation || prior.SessionID != c.SessionID {
				return ErrReplayConflict
			}
			result = prior.Receipt
			result.Replayed = true
			return errNoChange
		}
		session := j.Sessions[c.SessionID]
		if session == nil {
			session = &sessionState{Holder: f.Holder, Generation: f.Generation, Heads: map[Namespace]*Head{}}
			j.Sessions[c.SessionID] = session
		}
		if f.Generation < session.Generation || (f.Generation == session.Generation && f.Holder != session.Holder) {
			return ErrForbidden
		}
		head := session.Heads[c.Namespace]
		previous := 0
		if head != nil {
			previous = head.Latest.Ref.Revision
			if head.Attempt != nil || head.Rollback != nil {
				return ErrDeliveryUncertain
			}
		}
		if previous != c.ExpectedRevision || previous == int(^uint(0)>>1) {
			return ErrRevisionConflict
		}
		if c.Action != Set && (head == nil || head.Latest.State != Active) {
			return ErrRevisionConflict
		}
		revision := Revision{Ref: core.RevisionRef{ID: referenceID(c.SessionID, c.Namespace), Revision: previous + 1}, SessionID: c.SessionID, Namespace: c.Namespace, SourceEnvelopeID: c.EnvelopeID, Principal: f.Principal, Holder: f.Holder, HolderGeneration: f.Generation, State: Active}
		if c.Action == Set {
			revision.Content = *c.Content
		} else {
			revision.Content = head.Latest.Content
			revision.State = Cleared
			if c.Action == Accept {
				revision.State = Accepted
				revision.Acceptance = c.Acceptance
			}
		}
		if head == nil {
			head = &Head{}
			session.Heads[c.Namespace] = head
		}
		head.Latest = revision
		head.History = append(head.History, revision)
		head.Acknowledged = nil
		session.Holder = f.Holder
		session.Generation = f.Generation
		result = Receipt{Ref: revision.Ref, SourceEnvelopeID: c.EnvelopeID, Holder: f.Holder, Pending: c.Namespace == Assignment}
		j.Envelopes[c.EnvelopeID] = envelopeRecord{Digest: digest, Principal: f.Principal, Generation: f.Generation, SessionID: c.SessionID, Receipt: result}
		return nil
	})
	return result, err
}

// Snapshot is host-internal inspection, not a public read-authority endpoint.
func (s *Service) Snapshot(session string, namespace Namespace) (Head, error) {
	var result Head
	if session == "" || !validNamespace(namespace) {
		return result, ErrInvalidCommand
	}
	err := s.transaction(context.Background(), false, func(j *journal) error {
		if state := j.Sessions[session]; state != nil {
			if h := state.Heads[namespace]; h != nil {
				result = *h
			}
		}
		return nil
	})
	return result, err
}
func (s *Service) Get(owner core.OwnerKey) (*core.GoalRead, error) {
	session, namespace, err := target(owner)
	if err != nil {
		return nil, err
	}
	head, err := s.Snapshot(session, namespace)
	if err != nil {
		return nil, err
	}
	result := &core.GoalRead{Applied: head.Applied}
	if head.Latest.Ref.Revision > 0 {
		r := head.Latest.Ref
		result.Latest = &r
		result.Pending = namespace == Assignment && (head.Attempt != nil || head.Rollback != nil || head.Applied == nil || *head.Applied != r)
	}
	return result, nil
}
func envelope(r Revision) *core.GoalEnvelope {
	ref := r.Ref
	return &core.GoalEnvelope{Owner: Owner(r.SessionID, r.Namespace), Ref: &ref, Clear: r.State != Active}
}
func (s *Service) Reconcile(owner core.OwnerKey, binding core.GoalBinding) (*core.GoalActivation, error) {
	session, namespace, err := target(owner)
	if err != nil {
		return nil, err
	}
	head, err := s.Snapshot(session, namespace)
	if err != nil {
		return nil, err
	}
	if head.Latest.Ref.Revision == 0 {
		return &core.GoalActivation{}, nil
	}
	ref := head.Latest.Ref
	own, ok := binding.(*receiptBinding)
	if !ok || own.service != s || own.lease.SessionID != session || own.namespace != namespace {
		return nil, ErrBindingUnavailable
	}
	// Recover the crash window after core-checked Apply persisted its exact
	// acknowledgement but before durable source promotion. An attempt without
	// that acknowledgement remains uncertain under a changed controller.
	if head.Acknowledged != nil && head.Attempt != nil && head.Rollback == nil {
		if err = own.ctx.Err(); err != nil {
			return &core.GoalActivation{Ref: &ref, Pending: true}, err
		}
		if err = s.ConfirmNativeApplied(owner, ref); err != nil {
			return &core.GoalActivation{Ref: &ref, Pending: true}, err
		}
		head, err = s.Snapshot(session, namespace)
		if err != nil {
			return nil, err
		}
		ref = head.Latest.Ref
	}
	if head.Applied != nil && *head.Applied == ref && head.AppliedReceipt != nil && head.AppliedReceipt.Epoch == own.lease.Epoch && head.Attempt == nil && head.Rollback == nil {
		return &core.GoalActivation{Ref: &ref}, nil
	}
	if err = own.Apply(envelope(head.Latest)); err != nil {
		var acknowledged *acknowledgedEffectError
		if errors.As(err, &acknowledged) {
			err = errors.Join(err, own.Rollback(previousEnvelope(owner, head)))
		}
		return &core.GoalActivation{Ref: &ref, Pending: true}, err
	}
	if err = s.ConfirmNativeApplied(owner, ref); err != nil {
		// Preserve the original rollback obligation if durable promotion fails
		// after native acknowledgement. An unresolved attempt remains auditable.
		return &core.GoalActivation{Ref: &ref, Pending: true}, errors.Join(err, own.Rollback(previousEnvelope(owner, head)))
	}
	return &core.GoalActivation{Ref: &ref}, nil
}
func previousEnvelope(owner core.OwnerKey, head Head) *core.GoalEnvelope {
	if head.Applied != nil {
		return envelope(head.History[head.Applied.Revision-1])
	}
	return &core.GoalEnvelope{Owner: owner, Clear: true}
}

// ConfirmNativeApplied requires the exact receipt already verified and retained
// by this service's core-checked binding. A reference alone cannot acknowledge.
func (s *Service) ConfirmNativeApplied(owner core.OwnerKey, ref core.RevisionRef) error {
	session, namespace, err := target(owner)
	if err != nil {
		return err
	}
	return s.transaction(context.Background(), true, func(j *journal) error {
		state := j.Sessions[session]
		if state == nil {
			return ErrRevisionConflict
		}
		head := state.Heads[namespace]
		if head == nil || head.Latest.Ref != ref {
			return ErrRevisionConflict
		}
		if head.Applied != nil && *head.Applied == ref && head.Attempt == nil {
			return errNoChange
		}
		if head.Acknowledged == nil || head.Acknowledged.Ref != ref || head.Acknowledged.Receipt.SessionID != session || head.Acknowledged.Receipt.Epoch == 0 {
			return core.ErrInputAcknowledgementMismatch
		}
		if head.Attempt == nil || head.Attempt.Receipt == nil || head.Acknowledged.Receipt != *head.Attempt.Receipt || head.Acknowledged.Receipt.Epoch != head.Attempt.Lease.Epoch || head.Rollback != nil {
			return core.ErrInputAcknowledgementMismatch
		}
		receipt := head.Acknowledged.Receipt
		head.Applied = &ref
		head.AppliedReceipt = &receipt
		head.Attempt = nil
		head.Acknowledged = nil
		head.Applications = append(head.Applications, Application{Ref: ref, SourceEnvelopeID: head.Latest.SourceEnvelopeID, Holder: head.Latest.Holder, HolderGeneration: head.Latest.HolderGeneration, Receipt: receipt})
		record := j.Envelopes[head.Latest.SourceEnvelopeID]
		record.Receipt.Pending = false
		j.Envelopes[head.Latest.SourceEnvelopeID] = record
		return nil
	})
}

func (s *Service) revision(owner core.OwnerKey, ref *core.RevisionRef) (Revision, error) {
	session, namespace, err := target(owner)
	if err != nil {
		return Revision{}, err
	}
	if ref == nil {
		return Revision{}, ErrInvalidCommand
	}
	head, err := s.Snapshot(session, namespace)
	if err != nil {
		return Revision{}, err
	}
	if ref.ID != referenceID(session, namespace) || ref.Revision < 1 || ref.Revision > len(head.History) {
		return Revision{}, fmt.Errorf("%w: unknown goal reference", ErrRevisionConflict)
	}
	return head.History[ref.Revision-1], nil
}
