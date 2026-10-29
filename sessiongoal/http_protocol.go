//go:build unix

package sessiongoal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Protocol8GoalSession is a read-only mapping plus operations capturing its
// exact physical lease. Lookup must not bind or hydrate. The handler invokes
// operations only inside Service's verified exact-session caller fence.
// Reconcile returns the refreshed view only after the scoped binding commits.
type Protocol8GoalSession struct {
	Session         core.SessionView
	Gate            core.GoalGate
	Live            bool
	Reconcile       func() (core.SessionView, error)
	Rollback        func(*core.GoalEnvelope) error
	BeginActivation func() (string, bool, error)
	EndActivation   func(string) error
}

// Protocol8Runtime observes the current mapping without effects. Each operation
// must pin the same captured controller before binding, hydration or effects.
// These are trusted internal ports, never a replacement for caller admission.
type Protocol8Runtime interface {
	WithGoalSession(context.Context, core.OwnerKey, func(Protocol8GoalSession) error) error
}

// Protocol8RevisionProof validates the complete legacy envelope as data against
// an already-stored immutable host revision. It must verify every relevant
// legacy field and mapping, not merely compare an ID, owner or objective. It
// grants no caller rights and never creates intent. Missing proof refuses.
// Keeping raw bytes avoids copying a canonical ledger DTO into the host.
type Protocol8RevisionProof interface {
	VerifyLegacyRevision(context.Context, json.RawMessage, Revision) error
}

type protocol8Handler struct {
	service *Service
	runtime Protocol8Runtime
	proof   Protocol8RevisionProof
}

// NewProtocol8Handler serves the original five goal/owner routes. Compose under
// the core ControlServer's real authenticated/draining handler. Each effect also
// requires Service's independently verified exact-session caller. Missing ports
// return the original route-specific refusal before journal or native effects.
func NewProtocol8Handler(service *Service, runtime Protocol8Runtime, proof Protocol8RevisionProof) (http.Handler, error) {
	if service == nil {
		return nil, ErrBindingUnavailable
	}
	return &protocol8Handler{service: service, runtime: runtime, proof: proof}, nil
}

type protocol8GoalRequester struct {
	PID int `json:"pid,omitempty"`
}
type protocol8GoalApplyRequest struct {
	Revision  json.RawMessage        `json:"revision"`
	Requester protocol8GoalRequester `json:"requester,omitempty"`
}
type protocol8GoalApplyResponse struct {
	Applied bool   `json:"applied"`
	Outcome string `json:"outcome"`
}
type protocol8GoalOwnerRequest struct {
	Owner core.Protocol8SessionOwner `json:"owner"`
}
type protocol8GoalEndRequest struct {
	Owner   core.Protocol8SessionOwner `json:"owner"`
	LeaseID string                     `json:"lease_id"`
}
type protocol8GoalRollbackRequest struct {
	Owner    core.Protocol8SessionOwner `json:"owner"`
	Previous json.RawMessage            `json:"previous,omitempty"`
}
type protocol8RevisionLocator struct {
	ID       string                     `json:"goal_id"`
	Revision int                        `json:"revision"`
	Owner    core.Protocol8SessionOwner `json:"owner"`
}

func decodeGoalProtocol8(r *http.Request, target any) error {
	d := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	d.DisallowUnknownFields()
	if e := d.Decode(target); e != nil {
		return fmt.Errorf("decoding request: %w", e)
	}
	return nil
}
func goalProtocol8JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func goalProtocol8Failure(w http.ResponseWriter, status int, code, message string) {
	goalProtocol8JSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func goalProtocol8Error(w http.ResponseWriter, code, message string, err error) {
	// Inspect joined persistence failures first: they can wrap an otherwise public
	// error and must never disclose journal paths or private storage diagnostics.
	var pathError *fs.PathError
	private := errors.As(err, &pathError)
	for _, sentinel := range []error{ErrJournalInvalid, ErrCommitUncertain, core.ErrLegacyViewStale, core.ErrLegacyCommitUncertain, core.ErrLegacyCodecMismatch} {
		private = private || errors.Is(err, sentinel)
	}
	if !private {
		var public *core.PublicError
		if errors.As(err, &public) {
			code, message = public.Code, public.Message
		}
	}
	goalProtocol8Failure(w, http.StatusConflict, code, message)
}
func locateProtocol8Revision(raw json.RawMessage) (protocol8RevisionLocator, error) {
	var result protocol8RevisionLocator
	// Only routing/ref data is read here. Complete validation is mandatory at the
	// separate proof boundary; the original envelope is retained byte-for-byte.
	if len(raw) == 0 || string(raw) == "null" {
		return result, ErrInvalidCommand
	}
	if e := json.Unmarshal(raw, &result); e != nil {
		return result, e
	}
	if result.ID == "" || result.Revision < 1 {
		return result, ErrInvalidCommand
	}
	if _, e := core.DecodeProtocol8Owner(result.Owner); e != nil {
		return result, e
	}
	return result, nil
}

func (s *Service) withProtocol8Caller(ctx context.Context, session string, fn func() error) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if absent(s.authority) {
		return ErrAuthorityUnavailable
	}
	called := false
	var operationErr error
	err := s.authority.WithVerifiedCaller(ctx, session, func(f VerifiedCaller) error {
		if called {
			operationErr = errors.Join(operationErr, ErrForbidden)
			return ErrForbidden
		}
		called = true
		if e := policy(Command{SessionID: session, Namespace: Assignment, Action: Clear}, f); e != nil {
			operationErr = e
			return e
		}
		// Persisted handoff also fences stale verified facts; inspection does not
		// create a session/head or turn reflection into a new assignment command.
		operationErr = s.transaction(ctx, false, func(j *journal) error {
			if current := j.Sessions[session]; current != nil && (f.Generation < current.Generation || (f.Generation == current.Generation && f.Holder != current.Holder)) {
				return ErrForbidden
			}
			return nil
		})
		if operationErr == nil {
			operationErr = fn()
		}
		return operationErr
	})
	if !called && err == nil {
		return ErrAuthorityUnavailable
	}
	return errors.Join(err, operationErr)
}
func (h *protocol8Handler) withSession(ctx context.Context, owner core.OwnerKey, fn func(Protocol8GoalSession) error) error {
	if absent(h.runtime) {
		return ErrBindingUnavailable
	}
	called := false
	var operationErr error
	err := h.runtime.WithGoalSession(ctx, owner, func(session Protocol8GoalSession) error {
		if called {
			operationErr = errors.Join(operationErr, ErrBindingUnavailable)
			return operationErr
		}
		called = true
		if session.Session.SessionID == "" || session.Session.Owner != owner {
			operationErr = ErrBindingUnavailable
			return operationErr
		}
		operationErr = h.service.withProtocol8Caller(ctx, session.Session.SessionID, func() error { return fn(session) })
		return operationErr
	})
	if !called && err == nil {
		return ErrBindingUnavailable
	}
	return errors.Join(err, operationErr)
}
func (h *protocol8Handler) storedRevision(ctx context.Context, session Protocol8GoalSession, raw json.RawMessage) (Revision, error) {
	if absent(h.proof) {
		return Revision{}, ErrBindingUnavailable
	}
	locator, e := locateProtocol8Revision(raw)
	if e != nil {
		return Revision{}, e
	}
	owner, e := core.DecodeProtocol8Owner(locator.Owner)
	if e != nil || owner != session.Session.Owner {
		return Revision{}, ErrRevisionConflict
	}
	head, e := h.service.Snapshot(session.Session.SessionID, Assignment)
	if e != nil {
		return Revision{}, e
	}
	if head.Latest.Ref.ID != locator.ID || locator.Revision > len(head.History) {
		return Revision{}, ErrRevisionConflict
	}
	// The proof receives its own byte slice. It cannot replace stored intent.
	revision := head.History[locator.Revision-1]
	if e = h.proof.VerifyLegacyRevision(ctx, append(json.RawMessage(nil), raw...), revision); e != nil {
		return Revision{}, e
	}
	return revision, nil
}
func (h *protocol8Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		goalProtocol8Failure(w, 404, "not_found", "control route was not found")
		return
	}
	code, message := "", ""
	switch r.URL.Path {
	case "/v1/goals/apply":
		code, message = "goal_apply_failed", "provider goal could not be applied"
	case "/v1/goals/rollback":
		code, message = "goal_rollback_failed", "provider goal could not be rolled back"
	case "/v1/owners/reconcile":
		code, message = "reconcile_failed", "owner goal could not be reconciled into the live session"
	case "/v1/owners/activation/begin":
		code, message = "activation_begin_failed", "owner activation could not begin"
	case "/v1/owners/activation/end":
		code, message = "activation_end_failed", "owner activation could not end"
	default:
		goalProtocol8Failure(w, 404, "not_found", "control route was not found")
		return
	}
	var ownerWire core.Protocol8SessionOwner
	var raw json.RawMessage
	var lease string
	var decodeErr error
	switch r.URL.Path {
	case "/v1/goals/apply":
		var request protocol8GoalApplyRequest
		decodeErr = decodeGoalProtocol8(r, &request)
		raw = request.Revision
		if decodeErr == nil {
			var locator protocol8RevisionLocator
			locator, decodeErr = locateProtocol8Revision(raw)
			ownerWire = locator.Owner
		}
		// Requester.PID is deliberately not admission or a deferral decision. Real
		// descendant proof and waiter ownership require a separate native port.
	case "/v1/goals/rollback":
		var request protocol8GoalRollbackRequest
		decodeErr = decodeGoalProtocol8(r, &request)
		ownerWire, raw = request.Owner, request.Previous
	case "/v1/owners/activation/end":
		var request protocol8GoalEndRequest
		decodeErr = decodeGoalProtocol8(r, &request)
		ownerWire, lease = request.Owner, request.LeaseID
	default:
		var request protocol8GoalOwnerRequest
		decodeErr = decodeGoalProtocol8(r, &request)
		ownerWire = request.Owner
	}
	if decodeErr != nil {
		var typeError *json.UnmarshalTypeError
		if errors.Is(decodeErr, ErrInvalidCommand) {
			goalProtocol8Error(w, code, message, decodeErr)
		} else if errors.As(decodeErr, &typeError) || strings.HasPrefix(decodeErr.Error(), "decoding request:") {
			goalProtocol8Failure(w, 400, "invalid_request", decodeErr.Error())
		} else {
			goalProtocol8Error(w, code, message, decodeErr)
		}
		return
	}
	owner, e := core.DecodeProtocol8Owner(ownerWire)
	if e != nil {
		goalProtocol8Error(w, code, message, e)
		return
	}
	var response any
	e = h.withSession(r.Context(), owner, func(session Protocol8GoalSession) error {
		// Apply/reconcile re-read the actual configured gate at the native
		// boundary. Rollback remains available to undo an earlier effect.
		if (r.URL.Path == "/v1/goals/apply" || r.URL.Path == "/v1/owners/reconcile") && !absent(session.Gate) {
			reason, err := session.Gate.DisabledGoalPlane(owner)
			if err != nil {
				return err
			}
			if reason != "" {
				return core.ErrHostCapabilityUnavailable
			}
		}
		switch r.URL.Path {
		case "/v1/owners/activation/begin":
			if session.BeginActivation == nil {
				return ErrBindingUnavailable
			}
			id, live, e := session.BeginActivation()
			response = map[string]any{"lease_id": id, "live": live}
			return e
		case "/v1/owners/activation/end":
			if session.EndActivation == nil {
				return ErrBindingUnavailable
			}
			e := session.EndActivation(lease)
			response = map[string]bool{"ok": true}
			return e
		case "/v1/owners/reconcile":
			response = map[string]any{"reconciled": false}
			if !session.Live {
				return nil
			}
			if session.Reconcile == nil {
				return ErrBindingUnavailable
			}
			refreshed, e := session.Reconcile()
			if e != nil {
				return e
			}
			if refreshed.SessionID != session.Session.SessionID || refreshed.Owner != owner {
				return ErrBindingUnavailable
			}
			projected, e := core.ProjectProtocol8Session(refreshed)
			if e != nil {
				return e
			}
			response = map[string]any{"reconciled": true, "session": projected}
			return nil
		case "/v1/goals/apply":
			revision, e := h.storedRevision(r.Context(), session, raw)
			if e != nil {
				return e
			}
			head, e := h.service.Snapshot(session.Session.SessionID, Assignment)
			if e != nil {
				return e
			}
			if head.Latest.Ref != revision.Ref {
				return ErrRevisionConflict
			}
			if !session.Live || session.Reconcile == nil {
				return ErrBindingUnavailable
			}
			if _, e := session.Reconcile(); e != nil {
				return e
			}
			response = protocol8GoalApplyResponse{Applied: true, Outcome: "acknowledged"}
			return nil
		case "/v1/goals/rollback":
			if !session.Live || session.Rollback == nil {
				return ErrBindingUnavailable
			}
			var ref *core.RevisionRef
			clear := true
			if len(raw) > 0 && string(raw) != "null" {
				revision, e := h.storedRevision(r.Context(), session, raw)
				if e != nil {
					return e
				}
				ref = &revision.Ref
				clear = revision.State != Active
			}
			e := session.Rollback(&core.GoalEnvelope{Owner: Owner(session.Session.SessionID, Assignment), Ref: ref, Clear: clear})
			response = map[string]bool{"rolled_back": true}
			return e
		}
		return ErrInvalidCommand
	})
	if e != nil {
		goalProtocol8Error(w, code, message, e)
		return
	}
	goalProtocol8JSON(w, 200, response)
}
