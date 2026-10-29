package sessioncontext

import (
	"encoding/json"
	"errors"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"io"
	"net/http"
)

// NewHTTPHandler returns only the original context protocol8 routes. The core
// must mount it behind its actual socket/bearer authentication and hosted
// ordering; this package opens no listener and accepts no body principal.
func NewHTTPHandler(service *Service) (http.Handler, error) {
	if service == nil {
		return nil, fmt.Errorf("context service is required")
	}
	return &contextHTTPHandler{service: service}, nil
}

type contextHTTPHandler struct{ service *Service }

func (h *contextHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	decode := func(target any) bool {
		if e := decodeContextHTTP(w, r, target); e != nil {
			contextHTTPError(w, http.StatusBadRequest, "invalid_request", e.Error())
			return false
		}
		return true
	}
	fail := func(code, message string, e error) {
		h.service.logOperatorError("control_"+code, e, nil)
		var public *sm.PublicError
		if errors.As(e, &public) && public != nil {
			contextHTTPError(w, http.StatusConflict, public.Code, public.Message)
			return
		}
		contextHTTPError(w, http.StatusConflict, code, message)
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/contexts/reclaim":
		var q ContextReclamationRequest
		if !decode(&q) {
			return
		}
		out, e := h.service.ReclaimContextPayloads(r.Context(), q)
		if e != nil {
			fail("context_reclamation_failed", "board-owned context payload reclamation failed", e)
			return
		}
		contextHTTPJSON(w, http.StatusOK, out)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/contexts/sweep":
		var q ContextRetentionSweepRequest
		if !decode(&q) {
			return
		}
		out, e := h.service.SweepContextRetention(r.Context(), q)
		if e != nil {
			fail("context_retention_sweep_failed", "context retention sweep failed", e)
			return
		}
		contextHTTPJSON(w, http.StatusOK, out)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/contexts/economics":
		var q struct {
			Basis                 string  `json:"basis"`
			StorageUSDPerGiBMonth float64 `json:"storage_usd_per_gib_month"`
			TransferUSDPerGiB     float64 `json:"transfer_usd_per_gib"`
		}
		if !decode(&q) {
			return
		}
		out, e := h.service.ContextEconomics(ContextPayloadCostRates{Basis: q.Basis, StorageUSDPerGiBMonth: q.StorageUSDPerGiBMonth, TransferUSDPerGiB: q.TransferUSDPerGiB})
		if e != nil {
			fail("context_economics_failed", "realized context economics could not be read", e)
			return
		}
		contextHTTPJSON(w, http.StatusOK, out)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/contexts/completeness":
		var q struct {
			CtxID string `json:"ctx_id"`
		}
		if !decode(&q) {
			return
		}
		out, e := h.service.ContextCompleteness(q.CtxID)
		if e != nil {
			fail("context_completeness_unavailable", "per-fork completeness telemetry could not be read", e)
			return
		}
		contextHTTPJSON(w, http.StatusOK, out)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/contexts/materializations":
		contextHTTPJSON(w, http.StatusOK, map[string]any{"materializations": h.service.ListMaterializations()})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/contexts/materializations/prepare":
		var q PrepareRunMaterializationRequest
		if !decode(&q) {
			return
		}
		out, e := h.service.PrepareRunMaterialization(r.Context(), q)
		if e != nil {
			fail("context_materialization_failed", "the run's writable context child could not be materialized", e)
			return
		}
		contextHTTPJSON(w, http.StatusOK, out)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/contexts/materializations/complete":
		var q CompleteRunMaterializationRequest
		if !decode(&q) {
			return
		}
		out, e := h.service.CompleteRunMaterialization(r.Context(), q)
		if e != nil {
			fail("context_completion_failed", "the run's writable context child could not be completed", e)
			return
		}
		contextHTTPJSON(w, http.StatusOK, out)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/contexts/materializations/fail":
		var q CompleteRunMaterializationRequest
		if !decode(&q) {
			return
		}
		out, e := h.service.FailRunMaterialization(r.Context(), q.CtxID, q.RunID)
		if e != nil {
			fail("context_failure_failed", "the failed run's writable context child could not be released", e)
			return
		}
		contextHTTPJSON(w, http.StatusOK, out)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/contexts/builder-gateway/finalize":
		var q struct {
			CtxID string `json:"ctx_id"`
		}
		if !decode(&q) {
			return
		}
		out, e := h.service.FinalizeBuilderGateway(r.Context(), q.CtxID)
		if e != nil {
			fail("builder_gateway_finalization_failed", "the context-builder repository-evidence gateway could not be closed and attested", e)
			return
		}
		contextHTTPJSON(w, http.StatusOK, out)
	default:
		contextHTTPError(w, http.StatusNotFound, "not_found", "control route was not found")
	}
}

const contextHTTPBodyLimit = 1 << 20

func decodeContextHTTP(w http.ResponseWriter, r *http.Request, target any) error {
	if r.Body == nil {
		return fmt.Errorf("decoding request: %w", io.EOF)
	}
	body := http.MaxBytesReader(w, r.Body, contextHTTPBodyLimit)
	defer body.Close()
	d := json.NewDecoder(body)
	d.DisallowUnknownFields()
	if e := d.Decode(target); e != nil {
		return fmt.Errorf("decoding request: %w", e)
	}
	var tail any
	if e := d.Decode(&tail); !errors.Is(e, io.EOF) {
		if e != nil {
			return fmt.Errorf("decoding request: %w", e)
		}
		return fmt.Errorf("decoding request: multiple JSON values")
	}
	return nil
}
func contextHTTPJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func contextHTTPError(w http.ResponseWriter, status int, code, message string) {
	contextHTTPJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
