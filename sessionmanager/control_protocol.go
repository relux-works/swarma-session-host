//go:build unix

package sessionmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Protocol8Handler moves the original safe session projection and physical
// session routes onto the real host. ControlServer owns authentication, listener
// lifetime and drain. Supplemental is the actual owning service's handler for
// the enumerated context/goal/owner/launch/run/takeover/doctor routes; its absence
// is explicit unavailable capability, never authority inferred from request text.
type Protocol8Handler struct {
	host         *SessionHost
	status       func() ControlStatus
	supplemental http.Handler
}

func NewProtocol8Handler(host *SessionHost, status func() ControlStatus, supplemental http.Handler) (http.Handler, error) {
	if host == nil || status == nil {
		return nil, fmt.Errorf("session host and status supplier are required")
	}
	return &Protocol8Handler{host: host, status: status, supplemental: supplemental}, nil
}

// Original protocol8 output shape, independent of private provider persistence.
type protocol8Session struct {
	BindingScope          ContextBindingScope    `json:"binding_scope,omitempty"`
	BindingElementID      string                 `json:"binding_element_id,omitempty"`
	TargetElementID       string                 `json:"target_element_id,omitempty"`
	Retiring              bool                   `json:"retiring,omitempty"`
	CuratorProvenance     CuratorProvenanceState `json:"curator_provenance,omitempty"`
	SessionID             string                 `json:"session_id"`
	Owner                 legacySessionOwner     `json:"owner"`
	Provider              string                 `json:"provider"`
	State                 SessionState           `json:"state"`
	Liveness              SessionLiveness        `json:"liveness,omitempty"`
	LivenessDetail        string                 `json:"liveness_detail,omitempty"`
	OwnerRunError         string                 `json:"owner_run_error,omitempty"`
	AttachedClients       int                    `json:"attached_clients"`
	AppliedGoalRef        *LegacyGoalRef         `json:"applied_goal_ref,omitempty"`
	CreatedAt             time.Time              `json:"created_at"`
	UpdatedAt             time.Time              `json:"updated_at"`
	QuarantineReason      string                 `json:"quarantine_reason,omitempty"`
	QuarantinedAt         *time.Time             `json:"quarantined_at,omitempty"`
	Eviction              *SessionEvictionNotice `json:"eviction,omitempty"`
	CtxID                 string                 `json:"ctx_id,omitempty"`
	MaterializationState  MaterializationState   `json:"materialization_state,omitempty"`
	MaterializationSource MaterializationSource  `json:"materialization_source,omitempty"`
	ActiveRunID           string                 `json:"active_run_id,omitempty"`
}

func protocol8View(v SessionView) (protocol8Session, error) {
	owner, e := legacyOwnerForKey(v.Owner)
	if e != nil {
		return protocol8Session{}, e
	}
	out := protocol8Session{BindingScope: v.BindingScope, BindingElementID: v.BindingElementID, TargetElementID: v.TargetElementID, Retiring: v.Retiring, CuratorProvenance: v.CuratorProvenance, SessionID: v.SessionID, Owner: owner, Provider: v.Provider, State: v.State, Liveness: v.Liveness, LivenessDetail: v.LivenessDetail, OwnerRunError: v.OwnerRunError, AttachedClients: v.AttachedClients, CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt, QuarantineReason: v.QuarantineReason, QuarantinedAt: v.QuarantinedAt, Eviction: v.Eviction, CtxID: v.CtxID, MaterializationState: v.MaterializationState, MaterializationSource: v.MaterializationSource, ActiveRunID: v.ActiveRunID}
	if v.AppliedGoalRef != nil {
		out.AppliedGoalRef = &LegacyGoalRef{ID: v.AppliedGoalRef.ID, Revision: v.AppliedGoalRef.Revision}
	}
	return out, nil
}
func protocol8JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func protocol8Failure(w http.ResponseWriter, status int, code, message string) {
	protocol8JSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func protocol8Error(w http.ResponseWriter, status int, code, message string, err error) {
	// Check the entire joined chain before public diagnostics. Codec/store
	// failures can include local paths; their route's fixed fallback stays public.
	if !errors.Is(err, ErrLegacyViewStale) && !errors.Is(err, ErrLegacyCommitUncertain) && !errors.Is(err, ErrLegacyCodecMismatch) {
		var public *PublicError
		if errors.As(err, &public) {
			protocol8Failure(w, status, public.Code, public.Message)
			return
		}
		switch code {
		case "resize_failed", "detach_failed", "terminal_failed":
			message = err.Error()
		}
	}
	protocol8Failure(w, status, code, message)
}
func decodeProtocol8JSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if e := decoder.Decode(target); e != nil {
		return fmt.Errorf("decoding request: %w", e)
	}
	return nil
}
func (h *Protocol8Handler) writeSession(w http.ResponseWriter, v SessionView) {
	out, e := protocol8View(v)
	if e != nil {
		protocol8Error(w, 500, "session_projection_failed", "session metadata could not be projected", e)
		return
	}
	protocol8JSON(w, 200, out)
}
func (h *Protocol8Handler) external(w http.ResponseWriter, r *http.Request, code, message string) {
	if !absentHostPort(h.supplemental) {
		h.supplemental.ServeHTTP(w, r)
		return
	}
	protocol8Error(w, 409, code, message, ErrHostCapabilityUnavailable)
}
func (h *Protocol8Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
		protocol8JSON(w, 200, h.status())
		return
	case r.Method == http.MethodGet && r.URL.Path == "/v1/sessions":
		sessions, e := h.host.List(r.Context())
		if e != nil {
			protocol8Error(w, 409, "sessions_failed", "sessions could not be listed", e)
			return
		}
		views := make([]protocol8Session, 0, len(sessions))
		for _, s := range sessions {
			v, e := protocol8View(s)
			if e != nil {
				protocol8Error(w, 500, "session_projection_failed", "session metadata could not be projected", e)
				return
			}
			views = append(views, v)
		}
		protocol8JSON(w, 200, map[string]any{"sessions": views})
		return
	}
	// These original routes belong to their real runtime/service owners. Retain
	// their exact route/method/error boundaries while composition is explicit.
	if r.Method == http.MethodPost {
		var code, message string
		switch r.URL.Path {
		case "/v1/doctor":
			report, e := h.host.Doctor(r.Context())
			if e != nil {
				protocol8Error(w, 409, "doctor_failed", "session manager repair sweep failed", e)
				return
			}
			protocol8JSON(w, 200, report)
			return
		case "/v1/sessions/attach":
			code, message = "attach_failed", "provider session could not be attached"
		case "/v1/owners/reconcile":
			code, message = "reconcile_failed", "owner goal could not be reconciled into the live session"
		case "/v1/owners/activation/begin":
			code, message = "activation_begin_failed", "owner activation could not begin"
		case "/v1/owners/activation/end":
			code, message = "activation_end_failed", "owner activation could not end"
		case "/v1/goals/apply":
			code, message = "goal_apply_failed", "provider goal could not be applied"
		case "/v1/goals/rollback":
			code, message = "goal_rollback_failed", "provider goal could not be rolled back"
		case "/v1/contexts/reclaim":
			code, message = "context_reclamation_failed", "board-owned context payload reclamation failed"
		case "/v1/contexts/sweep":
			code, message = "context_retention_sweep_failed", "context retention sweep failed"
		case "/v1/contexts/economics":
			code, message = "context_economics_failed", "realized context economics could not be read"
		case "/v1/contexts/completeness":
			code, message = "context_completeness_failed", "context completeness feedback could not be read"
		case "/v1/contexts/materializations/prepare":
			code, message = "context_materialization_failed", "the run's writable context child could not be materialized"
		case "/v1/contexts/materializations/complete":
			code, message = "context_completion_failed", "the run's writable context child could not be completed"
		case "/v1/contexts/materializations/fail":
			code, message = "context_failure_failed", "the failed run's writable context child could not be released"
		case "/v1/contexts/builder-gateway/finalize":
			code, message = "builder_gateway_finalization_failed", "the context-builder repository-evidence gateway could not be closed and attested"
		}
		if code != "" {
			h.external(w, r, code, message)
			return
		}
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/contexts/materializations" {
		h.external(w, r, "context_materialization_failed", "context materializations could not be read")
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/v1/sessions/") {
		protocol8Failure(w, 404, "not_found", "control route was not found")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/sessions/"), "/")
	if len(parts) == 1 && parts[0] != "" && r.Method == http.MethodGet {
		session, e := h.host.Session(r.Context(), parts[0])
		if e != nil {
			protocol8Error(w, 404, "session_not_found", "session was not found", e)
			return
		}
		h.writeSession(w, session)
		return
	}
	if len(parts) != 2 || parts[0] == "" || r.Method != http.MethodPost {
		protocol8Failure(w, 404, "not_found", "control route was not found")
		return
	}
	id := parts[0]
	switch parts[1] {
	case "terminal":
		h.serveTerminal(w, r, id)
	case "resize":
		var request struct {
			AttachmentID string `json:"attachment_id"`
			Rows         uint16 `json:"rows"`
			Cols         uint16 `json:"cols"`
		}
		if e := decodeProtocol8JSON(r, &request); e != nil {
			protocol8Failure(w, 400, "invalid_request", e.Error())
			return
		}
		if e := h.host.ResizeTerminal(r.Context(), id, request.AttachmentID, request.Rows, request.Cols); e != nil {
			protocol8Error(w, 409, "resize_failed", "provider terminal could not be resized", e)
			return
		}
		protocol8JSON(w, 200, map[string]bool{"ok": true})
	case "detach":
		var request struct {
			AttachmentID string `json:"attachment_id"`
		}
		if e := decodeProtocol8JSON(r, &request); e != nil {
			protocol8Failure(w, 400, "invalid_request", e.Error())
			return
		}
		session, e := h.host.Detach(r.Context(), id, request.AttachmentID)
		if e != nil {
			protocol8Error(w, 409, "detach_failed", "provider session could not be detached", e)
			return
		}
		h.writeSession(w, session)
	case "stop":
		var request struct {
			Detail string `json:"detail,omitempty"`
		}
		if e := decodeProtocol8JSON(r, &request); e != nil {
			protocol8Failure(w, 400, "invalid_request", e.Error())
			return
		}
		session, e := h.host.StopWithDetail(r.Context(), id, request.Detail)
		if e != nil {
			protocol8Error(w, 409, "stop_failed", "provider session could not be stopped", e)
			return
		}
		h.writeSession(w, session)
	case "stop-graceful":
		session, e := h.host.StopGraceful(r.Context(), id)
		if e != nil {
			protocol8Error(w, 409, "stop_failed", "provider session could not be stopped", e)
			return
		}
		h.writeSession(w, session)
	case "run":
		var request struct {
			AttachmentID string `json:"attachment_id"`
			Prompt       string `json:"prompt"`
		}
		if e := decodeProtocol8JSON(r, &request); e != nil {
			protocol8Failure(w, 400, "invalid_request", e.Error())
			return
		}
		result, e := h.host.RunOwner(r.Context(), id, request.AttachmentID, ProviderRunRequest{Prompt: request.Prompt})
		if e != nil {
			var persist *ownerRunPersistError
			var retired *runOwnerRetiredError
			if errors.As(e, &persist) {
				protocol8Failure(w, 500, "owner_run_persist_failed", ownerRunPersistMessage)
				return
			}
			if errors.As(e, &retired) {
				protocol8Failure(w, 409, "spawn_moved", retired.Error())
				return
			}
			protocol8Error(w, 409, "owner_run_failed", "managed owner run failed", e)
			return
		}
		protocol8JSON(w, 200, result)
	case "takeover":
		var request struct {
			Owner     Protocol8SessionOwner `json:"owner"`
			Provider  string                `json:"provider"`
			SessionID string                `json:"session_id"`
			Observed  SessionObservation    `json:"observed"`
		}
		if e := decodeProtocol8JSON(r, &request); e != nil {
			protocol8Failure(w, 400, "invalid_request", e.Error())
			return
		}
		if request.SessionID != "" && request.SessionID != id {
			protocol8Failure(w, 400, "invalid_request", "takeover target must match the routed session")
			return
		}
		owner, e := DecodeProtocol8Owner(request.Owner)
		if e != nil {
			protocol8Error(w, 409, "takeover_failed", "owner-slot takeover failed", e)
			return
		}
		v, e := h.host.TakeOverOwnerSlot(r.Context(), TakeoverRequest{Owner: owner, Provider: request.Provider, SessionID: id, Observed: request.Observed})
		if e != nil {
			protocol8Error(w, 409, "takeover_failed", "owner-slot takeover failed", e)
			return
		}
		h.writeSession(w, v)
	default:
		protocol8Failure(w, 404, "not_found", "control route was not found")
	}
}

// Exact original protocol8 terminal preamble and streaming handshake.
const terminalStreamPreamble = "TBSM-TERMINAL-STREAM-V1\n"

func (h *Protocol8Handler) serveTerminal(w http.ResponseWriter, r *http.Request, sessionID string) {
	var request struct {
		AttachmentID string `json:"attachment_id"`
		Rows         uint16 `json:"rows"`
		Cols         uint16 `json:"cols"`
	}
	if err := decodeProtocol8JSON(r, &request); err != nil {
		protocol8Failure(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	terminal, err := h.host.Terminal(r.Context(), sessionID, request.AttachmentID)
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, ErrTerminalUnsupported) {
			status = http.StatusNotImplemented
		}
		protocol8Error(w, status, "terminal_failed", "provider terminal could not be opened", err)
		return
	}
	if request.Rows > 0 && request.Cols > 0 {
		_ = h.host.ResizeTerminal(r.Context(), sessionID, request.AttachmentID, request.Rows, request.Cols)
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = terminal.Close()
		protocol8Failure(w, http.StatusInternalServerError, "terminal_failed", "control transport cannot stream a terminal")
		return
	}
	conn, buffered, err := hijacker.Hijack()
	if err != nil {
		_ = terminal.Close()
		protocol8Failure(w, http.StatusInternalServerError, "terminal_failed", "control connection hijack failed")
		return
	}
	defer func() {
		_ = terminal.Close()
		_ = conn.Close()
	}()
	if _, err := buffered.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n" + terminalStreamPreamble); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}

	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(terminal, buffered)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(conn, terminal)
		done <- struct{}{}
	}()
	<-done
}

// Protocol8SessionOwner and Protocol8Session expose the actual existing wire
// projection to owning protocol packages, without a duplicate canonical DTO.
type Protocol8SessionOwner = legacySessionOwner
type Protocol8Session = protocol8Session

// DecodeProtocol8Owner validates immutable routing/slot data only. It does not
// verify a principal, holder, grant or session authority.
func DecodeProtocol8Owner(owner Protocol8SessionOwner) (OwnerKey, error) { return owner.key() }
func EncodeProtocol8Owner(key OwnerKey) (Protocol8SessionOwner, error)   { return legacyOwnerForKey(key) }
func ProjectProtocol8Session(session SessionView) (Protocol8Session, error) {
	return protocol8View(session)
}
