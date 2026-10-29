package musepty

import (
	"strings"
	"testing"
)

// Received and settled join by command_id: a settled record for
// another command is unknown state. Ports delta
// TestDeltaR10IntakePairMustFailClosed, plus the never-received,
// duplicate-settled and session_name-spelling arms.
func TestMusePTYR11IntakePairMustMatch(t *testing.T) {
	t.Run("aligned_control", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		received := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.received", "command_intake", ""))
		settled := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
		rp := received["payload"].(map[string]any)["record"].(map[string]any)
		sp := settled["payload"].(map[string]any)["record"].(map[string]any)
		sp["command_id"] = rp["command_id"]
		appendMuseRecord(t, h, received)
		appendMuseRecord(t, h, settled)
		museExpectDelivery(t, h, home, "intake control")
	})
	t.Run("wrong_command_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		received := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.received", "command_intake", ""))
		settled := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
		rp := received["payload"].(map[string]any)["record"].(map[string]any)
		sp := settled["payload"].(map[string]any)["record"].(map[string]any)
		sp["command_id"] = rp["command_id"]
		// Fixed sentinel id: the r11-intake-settled-only mutant
		// admits the settled record for exactly this command.
		sp["command_id"] = museTestUUID("different-command")
		appendMuseRecord(t, h, received)
		// The received command settles under its own id, so the
		// wrong settle is the sole refusal (r12: an unsettled
		// receive would refuse at idle instead and subsume the
		// mutant).
		discharge := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
		discharge["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = rp["command_id"]
		appendMuseRecord(t, h, discharge)
		appendMuseRecord(t, h, settled)
		museR9Refuse(t, h, home)
	})
	t.Run("settled_without_received_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		appendMuseRecord(t, h, museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", "")))
		museR9Refuse(t, h, home)
	})
	t.Run("duplicate_settled_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		received := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.received", "command_intake", ""))
		settled := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
		// Fixed sentinel id: the r11-intake-settled-dup-only
		// mutant admits the second settle for exactly this command.
		received["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("r11-intake-settled-dup")
		settled["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("r11-intake-settled-dup")
		appendMuseRecord(t, h, received)
		appendMuseRecord(t, h, settled)
		// A fresh envelope on the copy: the refusal is the second
		// settle, not the reused envelope.
		appendMuseRecord(t, h, museRestampRecord(museCloneRecord(t, settled)))
		museR9Refuse(t, h, home)
	})
	t.Run("duplicate_received_refuses", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		received := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.received", "command_intake", ""))
		// Fixed sentinel id: the r11-intake-received-dup-only
		// mutant admits the second receive for exactly this command.
		received["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("r11-intake-received-dup")
		appendMuseRecord(t, h, received)
		// A fresh envelope on the copy: the refusal is the second
		// receive, not the reused envelope.
		appendMuseRecord(t, h, museRestampRecord(museCloneRecord(t, received)))
		// The command settles, so the duplicate receive is the sole
		// refusal (r12: an unsettled receive would refuse at idle
		// instead and subsume the mutant).
		discharge := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
		discharge["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = museTestUUID("r11-intake-received-dup")
		appendMuseRecord(t, h, discharge)
		museR9Refuse(t, h, home)
	})
	t.Run("session_name_spelling_permits", func(t *testing.T) {
		h, home := musePeer(t, "idle")
		received := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.session_name.received", "command_intake", ""))
		settled := museOwnedRecord(h, museCorpusRecord(t, "runtime.command_intake.settled", "command_intake", ""))
		settled["payload"].(map[string]any)["record"].(map[string]any)["command_id"] = received["payload"].(map[string]any)["record"].(map[string]any)["command_id"]
		appendMuseRecord(t, h, received)
		appendMuseRecord(t, h, settled)
		museExpectDelivery(t, h, home, "session_name intake control")
	})
}

// Every admitted cleanup pair follows the witnessed start -> child
// terminal -> cleanup terminal order. Ports panel A
// TestR141R10CleanupStartsAfterChildTerminal verbatim.
func TestMusePTYR11CleanupOrderMustMatch(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "captured_order_control"
		if reverse {
			name = "child_terminal_before_cleanup_start"
		}
		t.Run(name, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			pair := museR8CleanupPairs(t)[0]
			start := museOwnedRecord(h, pair.Start)
			p := start["payload"].(map[string]any)
			run := p["run_id"].(string)
			task := p["record"].(map[string]any)["child_task"].(map[string]any)["task_id"].(string)
			end := museOwnedRecord(h, pair.Terminal)
			museEmitRunBounded(t, h, "started", run)
			museEmitTaskPlanned(t, h, run, task)
			if reverse {
				museR9ChildTerminal(t, h, run, task, "cancelled")
			}
			appendMuseRecord(t, h, start)
			if !reverse {
				museR9ChildTerminal(t, h, run, task, "cancelled")
			}
			appendMuseRecord(t, h, end)
			museEmitRunBounded(t, h, "terminal", run)
			if reverse {
				// The refusal is the start gate: the terminal
				// index gate would refuse it too, so the
				// start-gate mutant is killed by the error
				// identity, not by the admission.
				if f, err := h.fold(); err == nil || !strings.Contains(err.Error(), "starts after its child terminal") {
					t.Fatalf("reversed cleanup misfolded: %+v %v", f, err)
				}
				museR9Refuse(t, h, home)
			} else {
				museExpectDelivery(t, h, home, "captured cleanup control")
			}
		})
	}
}

// Every trace request_record_id resolves to its own run's configured
// request source identity. Ports panel B
// TestPanelR10TraceRequestIdentityMustMatch verbatim.
func TestMusePTYR11TraceRequestIdentityMustMatch(t *testing.T) {
	for _, variant := range []string{"matched_control", "unknown_request", "cross_run_request"} {
		t.Run(variant, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			museEmitRunBounded(t, h, "started", museTestUUID("trace-owner"))
			cfg := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
			cfg["payload"].(map[string]any)["run_id"] = museTestUUID("trace-owner")
			source := cfg["payload"].(map[string]any)["source_run_record_id"].(string)
			appendMuseRecord(t, h, cfg)
			trace := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_input_trace_recorded"))
			p := trace["payload"].(map[string]any)
			p["run_id"] = museTestUUID("trace-owner")
			p["event"].(map[string]any)["request_record_id"] = source
			if variant == "unknown_request" {
				// Fixed sentinel id: the r11-trace-request-only
				// mutant admits the trace for exactly this request.
				p["event"].(map[string]any)["request_record_id"] = museTestUUID("never-configured-request")
			}
			if variant == "cross_run_request" {
				museEmitRunBounded(t, h, "started", museTestUUID("foreign-request-owner"))
				other := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
				op := other["payload"].(map[string]any)
				op["run_id"] = museTestUUID("foreign-request-owner")
				op["source_run_record_id"] = museTestUUID("foreign-request-id")
				appendMuseRecord(t, h, other)
				museEmitRunBounded(t, h, "terminal", museTestUUID("foreign-request-owner"))
				p["event"].(map[string]any)["request_record_id"] = museTestUUID("foreign-request-id")
			}
			appendMuseRecord(t, h, trace)
			museEmitRunBounded(t, h, "terminal", museTestUUID("trace-owner"))
			if variant == "matched_control" {
				museExpectDelivery(t, h, home, "matched trace control")
				return
			}
			museR9Refuse(t, h, home)
		})
	}
}

// Every diagnostic block_id resolves to a configured
// run_context_messages id of its own run, including the captured
// forward references. Ports panel B
// TestPanelR10ContextBlockIdentityMustMatch; r14 migrates the unknown
// citation from a synthetic id to a captured-but-unconfigured token
// (closed block grammar type).
func TestMusePTYR11ContextBlockIdentityMustMatch(t *testing.T) {
	for _, variant := range []string{"matched_control", "unknown_block"} {
		t.Run(variant, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			museEmitRunBounded(t, h, "started", museTestUUID("block-owner"))
			cfg := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
			cfg["payload"].(map[string]any)["run_id"] = museTestUUID("block-owner")
			diag := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "context_block_diagnostic"))
			p := diag["payload"].(map[string]any)
			p["run_id"] = museTestUUID("block-owner")
			messages := cfg["payload"].(map[string]any)["event"].(map[string]any)["run_context_messages"].([]any)
			p["event"].(map[string]any)["block_id"] = messages[0].(map[string]any)["id"]
			if variant == "unknown_block" {
				// Captured-but-unconfigured citation (r14): block
				// ids are a closed 11 token grammar type, so the
				// unknown variant cites a captured token the log
				// does not configure instead of a synthetic id.
				// Fixed sentinel: the r11-context-block-only
				// mutant admits the diagnostic for exactly this block.
				if got := messages[1].(map[string]any)["id"]; got != "security_mode" {
					t.Fatalf("corpus drift: messages[1] = %v, want security_mode", got)
				}
				cfg["payload"].(map[string]any)["event"].(map[string]any)["run_context_messages"] = messages[:1]
				p["event"].(map[string]any)["block_id"] = "security_mode"
			}
			appendMuseRecord(t, h, diag)
			appendMuseRecord(t, h, cfg)
			museEmitRunBounded(t, h, "terminal", museTestUUID("block-owner"))
			if variant == "matched_control" {
				museExpectDelivery(t, h, home, "matched block control")
				return
			}
			museR9Refuse(t, h, home)
		})
	}
}

// Every linked reminder_agent_id names a member of the owning run's
// configured reminder roster. Ports panel B
// TestPanelR10ReminderRosterIdentityMustMatch; r14 migrates the unknown
// citation from a synthetic id to a captured-but-unconfigured token
// (closed roster grammar type).
func TestMusePTYR11ReminderRosterIdentityMustMatch(t *testing.T) {
	for _, variant := range []string{"matched_control", "unknown_agent"} {
		t.Run(variant, func(t *testing.T) {
			h, home := musePeer(t, "idle")
			run := museTestUUID("roster-owner")
			task := museTestUUID("roster-task")
			museEmitRunBounded(t, h, "started", run)
			cfg := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "model_request_configured"))
			cfg["payload"].(map[string]any)["run_id"] = run
			agents := cfg["payload"].(map[string]any)["event"].(map[string]any)["reminder_roster"].(map[string]any)["agents"].([]any)
			link := museOwnedRecord(h, museCorpusRecord(t, "runtime.session", "run", "memory_reminder_child_session_linked"))
			p := link["payload"].(map[string]any)
			p["run_id"] = run
			e := p["event"].(map[string]any)
			e["parent_session_id"] = h.NativeIdentity()
			e["parent_run_id"] = run
			e["task_id"] = task
			e["task_stream"].(map[string]any)["id"] = task
			e["reminder_agent_id"] = agents[0].(map[string]any)["id"]
			if variant == "unknown_agent" {
				// Captured-but-unconfigured citation (r14): roster
				// ids are a closed 2 token grammar type, so the
				// unknown variant cites a captured agent the log
				// does not configure instead of a synthetic id.
				// Fixed sentinel: the r11-reminder-roster-only
				// mutant admits the link for exactly this agent.
				if got := agents[1].(map[string]any)["id"]; got != "verify-reminder" {
					t.Fatalf("corpus drift: agents[1] = %v, want verify-reminder", got)
				}
				cfg["payload"].(map[string]any)["event"].(map[string]any)["reminder_roster"].(map[string]any)["agents"] = agents[:1]
				e["reminder_agent_id"] = "verify-reminder"
			}
			appendMuseRecord(t, h, cfg)
			appendMuseRecord(t, h, link)
			for _, kind := range []string{"proposed", "accepted", "scheduled", "side_effect_intent", "started", "completed"} {
				r := museR7TaskRecord(t, h, kind, run, task)
				event := r["payload"].(map[string]any)["event"].(map[string]any)
				if kind == "scheduled" || kind == "side_effect_intent" {
					event["idempotency_key"] = museTestUUID("roster-effect")
				}
				appendMuseRecord(t, h, r)
			}
			museEmitRunBounded(t, h, "terminal", run)
			if variant == "matched_control" {
				museExpectDelivery(t, h, home, "matched roster control")
				return
			}
			museR9Refuse(t, h, home)
		})
	}
}
