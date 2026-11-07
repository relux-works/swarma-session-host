package sessionhost

import (
	"context"
	"encoding/json"
	"errors"

	schema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
)

// Session status observes the ordinary owner; operation status/cancel reconcile
// the durable reservation. The journal is not a current session-state index.
// Observation contexts never own the daemon or execution. No absence or failed
// transport observation is converted into a negative claim about start effects.
func (d *selectedManagerDaemon) observe(ctx context.Context, kind string, data []byte) ([]byte, int) {
	if schema.Validate(kind+"-request.schema.json", data) != nil {
		return selectedRefusal("launch_plan_invalid", nil)
	}
	d.mu.Lock()
	if d.closed || d.ctx.Err() != nil || ctx.Err() != nil {
		d.mu.Unlock()
		return selectedRefusal("session_host_unavailable", nil)
	}
	if kind == "describe" {
		d.mu.Unlock()
		capabilities := []string{"launch-delegated/1", "resume/1", "status/1", "admission/1"}
		if d.owner.notificationAvailable() {
			capabilities = append(capabilities, NotificationContract)
		}
		result, _ := json.Marshal(map[string]any{"protocol": selectedManagerProtocol, "ok": true, "id": "session-host", "version": "1", "capabilities": capabilities, "runtimes": map[string][]string{"codex_cli": {"launch", "resume", "status", "cancel"}}})
		return result, 0
	}
	var q struct {
		Operation string `json:"operation_id"`
		Session   string `json:"session"`
	}
	if json.Unmarshal(data, &q) != nil {
		d.mu.Unlock()
		return selectedRefusal("launch_plan_invalid", nil)
	}
	if kind == "status" && q.Session != "" {
		d.mu.Unlock()
		return d.observeSession(ctx, q.Session)
	}
	if !selectedOperationID.MatchString(q.Operation) {
		d.mu.Unlock()
		return selectedRefusal("launch_plan_invalid", nil)
	}
	entry, exists, err := d.journal.read(q.Operation)
	if err != nil {
		d.mu.Unlock()
		return selectedRefusal("session_host_unavailable", nil)
	}
	if kind == "cancel" && !exists {
		entry = selectedOperationEntry{Cancelled: true}
		if d.journal.write(q.Operation, entry) != nil {
			d.mu.Unlock()
			return selectedRefusal("session_host_unavailable", nil)
		}
		exists = true
	}
	_, pending := d.pending[q.Operation]
	d.mu.Unlock()
	outcome := "not_found"
	if pending {
		outcome = "prepared"
	}
	if exists {
		outcome = "unknown"
		if entry.Cancelled {
			outcome = "cancelled"
		}
	}
	response := map[string]any{"protocol": selectedManagerProtocol, "ok": true, "outcome": outcome}
	if exists && len(entry.Refusal) != 0 {
		var refusal struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(entry.Refusal, &refusal)
		response["outcome"] = "failed"
		response["code"] = refusal.Code
	}
	if exists && len(entry.Result) != 0 {
		current, _ := d.replay(entry)
		var record map[string]json.RawMessage
		if json.Unmarshal(current, &record) == nil {
			delete(record, "protocol")
			delete(record, "ok")
			response["outcome"] = "started"
			response["session"] = record
		}
	}
	result, _ := json.Marshal(response)
	if schema.Validate("outcome.schema.json", result) != nil {
		return selectedRefusal("session_host_unavailable", nil)
	}
	return result, 0
}

func (d *selectedManagerDaemon) observeSession(ctx context.Context, session string) ([]byte, int) {
	// Never hold the operation mutex across an ordinary owner's observation.
	// The owner must honor the request deadline; cancellation does not end custody.
	result, err := d.owner.status(ctx, session)
	if ctx.Err() != nil || d.ctx.Err() != nil {
		return selectedRefusal("session_host_unavailable", nil)
	}
	if err != nil {
		var refusal ordinaryManagerRefusal
		if errors.As(err, &refusal) {
			data, exit := selectedRefusal(refusal.code, refusal.details)
			if schema.Validate("refusal.schema.json", data) == nil {
				return data, exit
			}
		}
		return selectedRefusal("session_host_unavailable", nil)
	}
	if !validSelectedSessionObservation(result, session) {
		return selectedRefusal("session_host_unavailable", nil)
	}
	// Ordinary records cannot grant notification authority. As for ordinary
	// launch results, absence of live attachment evidence stays unresolved.
	var record map[string]json.RawMessage
	_ = json.Unmarshal(result, &record)
	delete(record, "notification")
	result, _ = json.Marshal(record)
	return result, 0
}

func validSelectedSessionObservation(data []byte, session string) bool {
	var record struct {
		Session string `json:"session"`
		Manager string `json:"manager"`
	}
	return validSelectedResult(data) && json.Unmarshal(data, &record) == nil && record.Session == session && record.Manager == "session-host"
}

func storedSelectedRefusal(data []byte) ([]byte, int) {
	var refusal struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(data, &refusal) != nil || schema.Validate("refusal.schema.json", data) != nil {
		return selectedRefusal("session_host_unavailable", nil)
	}
	_, exit := selectedRefusal(refusal.Code, nil)
	return append([]byte(nil), data...), exit
}

func (d *selectedManagerDaemon) recordRefusal(id string, entry selectedOperationEntry, result []byte, exit int) ([]byte, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	old, exists, err := d.journal.read(id)
	if err == nil && exists && old.Cancelled {
		return selectedRefusal("session_host_unavailable", map[string]string{"reason": "cancelled"})
	}
	entry.Refusal = append([]byte(nil), result...)
	if err != nil || d.journal.write(id, entry) != nil {
		return selectedRefusal("session_host_unavailable", nil)
	}
	return result, exit
}
