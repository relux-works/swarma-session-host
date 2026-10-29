//go:build unix

package hostdefaults

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

var ErrLegacyAttachSourceUnavailable = errors.New("verified original session launch source is unavailable")

// LegacyAttachTarget is a read-only physical owner-slot observation, not a grant
// to create a session. A zero lease is an absence assertion checked by the host
// under reservation; Live=false does not establish that an unknown slot is free.
type LegacyAttachTarget struct {
	Session core.SessionView
	Lease   core.ControllerLease
	Live    bool
}

// LegacyAttachSource is the raising-side verification/environment boundary.
// It must verify the complete original request, selected plan/location, current
// holder and launch/adoption/materialization rights, and fence them throughout
// exactly one callback. The environment is the actual configured projection;
// ambient daemon credentials and body RuntimeControl are never verification.
// No production supplier/registrar is supplied here. An in-process fixture is
// not qualification of that missing source or of SessionStart identity binding.
type LegacyAttachSource interface {
	WithVerifiedAttach(context.Context, LegacyAttachRequest, LegacyAttachTarget, func(context.Context, []string) error) error
}

// LegacyAttachment retains the complete original protocol8 response. Native
// client-only identity stays outside the safe public session projection.
type LegacyAttachment struct {
	AttachmentID     string                `json:"attachment_id"`
	ManagerEndpoint  string                `json:"manager_endpoint,omitempty"`
	ClientHome       string                `json:"client_home,omitempty"`
	CodexThreadID    string                `json:"codex_thread_id,omitempty"`
	ClientToolPolicy core.ClientToolPolicy `json:"client_tool_policy,omitzero"`
	Session          core.Protocol8Session `json:"session"`
}

type legacyAttachHandler struct {
	host   *core.SessionHost
	source LegacyAttachSource
}

// NewLegacyAttachHandler is composed inside the actual ControlServer's
// authenticated listener. It decodes the original envelope, verifies the source
// against a read-only mapping, then uses actual typed preparation and Attach.
// Missing source fails before binding, hydration, native launch or publication.
func NewLegacyAttachHandler(host *core.SessionHost, source LegacyAttachSource) (http.Handler, error) {
	if host == nil {
		return nil, fmt.Errorf("session host required")
	}
	return &legacyAttachHandler{host: host, source: source}, nil
}

func legacyAttachJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func legacyAttachFailure(w http.ResponseWriter, status int, code, message string) {
	legacyAttachJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func legacyAttachError(w http.ResponseWriter, err error) {
	code, message := "attach_failed", "provider session could not be attached"
	// Check the full joined private chain before any explicitly public error.
	var pathError *fs.PathError
	private := errors.As(err, &pathError)
	for _, sentinel := range []error{core.ErrLegacyViewStale, core.ErrLegacyCommitUncertain, core.ErrLegacyCodecMismatch} {
		private = private || errors.Is(err, sentinel)
	}
	var public *core.PublicError
	if !private && errors.As(err, &public) {
		code, message = public.Code, public.Message
	}
	legacyAttachFailure(w, http.StatusConflict, code, message)
}

func (h *legacyAttachHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/sessions/attach" {
		legacyAttachFailure(w, http.StatusNotFound, "not_found", "control route was not found")
		return
	}
	var request LegacyAttachRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		legacyAttachFailure(w, http.StatusBadRequest, "invalid_request", "decoding request: "+err.Error())
		return
	}
	attachment, err := h.attach(r.Context(), request)
	if err != nil {
		legacyAttachError(w, err)
		return
	}
	legacyAttachJSON(w, http.StatusOK, attachment)
}

func (h *legacyAttachHandler) attach(ctx context.Context, request LegacyAttachRequest) (*LegacyAttachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	parsed, err := DecodeLegacyAttachRequest(raw)
	if err != nil {
		return nil, err
	}
	view, lease, mapped, err := h.host.LookupOwnerMapping(ctx, parsed.Owner)
	if err != nil {
		return nil, err
	}
	// A cross-provider mapping is enough to reach the actual conflict refusal
	// after verification; it is not a live-provider observation. Preserve the
	// existing same-provider liveness refusal before any attachment effects.
	live := false
	if mapped && view.Provider == strings.ToLower(strings.TrimSpace(parsed.Request.Provider)) {
		view, lease, live, err = h.host.OwnerSession(ctx, parsed.Owner)
		if err != nil {
			return nil, err
		}
		if !live {
			return nil, core.ErrStaleInputController
		}
	}
	target := LegacyAttachTarget{Session: view, Lease: lease, Live: live}
	if missingLegacyAttachSource(h.source) {
		return nil, ErrLegacyAttachSourceUnavailable
	}
	// Source receives independent wire bytes: it cannot mutate the request that
	// is prepared after verification, or change the privately captured lease.
	sourceRequest := request
	sourceRequest.Launch = append(json.RawMessage(nil), request.Launch...)
	var mu sync.Mutex
	open, called := true, false
	var result *LegacyAttachment
	var receipt *core.AttachmentResultReceipt
	var resultContext context.Context
	var operationErr error
	err = h.source.WithVerifiedAttach(ctx, sourceRequest, target, func(fenced context.Context, environment []string) error {
		mu.Lock()
		defer mu.Unlock()
		if !open || called {
			operationErr = errors.Join(operationErr, core.ErrInputScopeClosed)
			return core.ErrInputScopeClosed
		}
		called = true
		if fenced == nil {
			operationErr = ErrLegacyAttachSourceUnavailable
			return operationErr
		}
		call, cancel := context.WithCancel(fenced)
		stop := context.AfterFunc(ctx, cancel)
		defer cancel()
		defer stop()
		if e := ctx.Err(); e != nil {
			operationErr = e
			return e
		}
		prepared, e := PrepareLegacyAttach(parsed.Request, environment)
		if e != nil {
			operationErr = e
			return e
		}
		pin := lease
		prepared.ExpectedOwnerLease = &pin
		actual, issued, e := h.host.AttachWithResult(call, prepared)
		receipt = issued
		resultContext = call
		if e != nil {
			operationErr = e
			return e
		}
		if actual == nil || receipt == nil {
			operationErr = core.ErrHostCapabilityUnavailable
			return operationErr
		}
		projected, e := core.ProjectProtocol8Session(actual.Session)
		if e != nil {
			operationErr = e
			return e
		}
		result = &LegacyAttachment{AttachmentID: actual.AttachmentID, ManagerEndpoint: actual.ManagerEndpoint, ClientHome: actual.ClientHome, CodexThreadID: actual.CodexThreadID, ClientToolPolicy: actual.ClientToolPolicy, Session: projected}
		return nil
	})
	// Join any operation already running, and close the callback before returning.
	// A captured callback cannot acquire launch rights after this source expires.
	mu.Lock()
	open = false
	if !called {
		operationErr = errors.Join(operationErr, ErrLegacyAttachSourceUnavailable)
	}
	combined := errors.Join(err, operationErr, ctx.Err())
	mu.Unlock()
	if combined == nil {
		if result == nil || receipt == nil {
			combined = core.ErrHostCapabilityUnavailable
		} else {
			// The native Attach remained inside the source fence. Commit only
			// releases custody of this issued result after all checks succeed.
			combined = h.host.CommitAttachResult(ctx, receipt)
		}
	}
	if combined != nil {
		if receipt != nil {
			// Cancellation removes launch rights, not responsibility for this
			// already-owned result. Cleanup is bounded and keeps private values;
			// the host alone decides fresh/current/exclusive retirement rights.
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(resultContext), 5*time.Second)
			rollbackErr := h.host.RollbackAttachResult(cleanup, receipt)
			cancel()
			combined = errors.Join(combined, rollbackErr)
		}
		return nil, combined
	}
	return result, nil
}

func missingLegacyAttachSource(source LegacyAttachSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	}
	return false
}
