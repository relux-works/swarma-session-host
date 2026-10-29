package sessionmanager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Strict MSP result decoding for the Muse host (r17): a method result the
// host does not FULLY validate is never success.
//
// Every RPC result the host consumes is validated against the pinned
// 1.4.1-R4503.1 export before its typed form is trusted: required members
// are present, types are exact, null is rejected where the schema forbids
// it, and closed enums are closed. Unknown object fields are IGNORED — the
// export carries no additionalProperties:false and Session-class shapes
// evolve additive-optionally, so rejecting unknown fields would break
// forward compatibility with the pinned server. The fake-emission gate
// reuses the same core with field closure enabled: emission must state
// exactly the known contract.
//
// A result that fails validation is a typed MSPProtocolError, never
// success. Validation runs only on strict clients (the Muse host); the
// shared legacy path is unchanged.

// museMethodResultDefs maps every RPC method the Muse host consumes to its
// pinned result definition. The table is explicit (reviewable, mutantable)
// and drift-checked against the export's own methods table by
// TestMuseMethodResultTableMatchesExport: a row that names a different
// definition than the export fails the suite instead of validating against
// a stale shape.
var museMethodResultDefs = map[string]string{
	"initialize":           "InitializeResult",
	"session/start":        "SessionStartResult",
	"session/resume":       "SessionResumeResult",
	"goal/set":             "GoalCommandResult",
	"goal/edit":            "GoalCommandResult",
	"goal/pause":           "GoalCommandResult",
	"goal/resume":          "GoalCommandResult",
	"goal/clear":           "GoalCommandResult",
	"turn/start":           "TurnStartResult",
	"turn/steer":           "TurnSteerResult",
	"turn/interrupt":       "TurnInterruptResult",
	"view/page":            "ViewPageResult",
	"approval/listPending": "ApprovalListPendingResult",
	"approval/decide":      "ApprovalDecideResult",
	"userInput/cancel":     "UserInputCancelResult",
}

// museMethodResultDef resolves the pinned result definition for a consumed
// RPC method. It reports false for methods the host never calls; the
// strict client fails closed on those rather than decoding blind.
func museMethodResultDef(method string) (map[string]any, bool) {
	defs, _, err := musePinnedSchema()
	if err != nil {
		return nil, false
	}
	name, ok := museMethodResultDefs[method]
	if !ok {
		return nil, false
	}
	def, ok := defs[name].(map[string]any)
	return def, ok && def != nil
}

// museStrictDecodeMethodResult validates one consumed RPC result against
// the pinned export. Production decoding ignores unknown fields; only the
// declared contract is enforced.
func museStrictDecodeMethodResult(method string, result json.RawMessage) error {
	def, ok := museMethodResultDef(method)
	if !ok {
		return &MSPProtocolError{Reason: fmt.Sprintf("method %q has no pinned result definition", method)}
	}
	trimmed := bytes.TrimSpace(result)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return &MSPProtocolError{Reason: fmt.Sprintf("method %q result must be an object", method)}
	}
	value, err := museDecodePinnedJSON(result)
	if err != nil {
		return &MSPProtocolError{Reason: fmt.Sprintf("method %q result is not valid JSON", method), Cause: err}
	}
	defs, _, err := musePinnedSchema()
	if err != nil {
		return err
	}
	if err := museValidatePinnedValue(defs, def, value, false); err != nil {
		return &MSPProtocolError{Reason: "result failed pinned-schema validation", Cause: fmt.Errorf("%s: %w", method, err)}
	}
	if method == "view/page" {
		if err := museStrictDecodeViewPageEvents(value); err != nil {
			return &MSPProtocolError{Reason: "result failed pinned-schema validation", Cause: err}
		}
	}
	return nil
}

// museStrictDecodeViewPageEvents validates each paged event's params
// against its own notification definition when the export knows the
// method. The host consumes nested session/goalChanged params from pages,
// so those face the same strictness as a live delivery; events from
// methods the export does not know still face the shared base members
// every view notification carries. Unknown object fields are ignored,
// exactly as on the live path.
func museStrictDecodeViewPageEvents(value any) error {
	page, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("view/page: result is not an object")
	}
	events, ok := page["events"].([]any)
	if !ok {
		return fmt.Errorf("view/page: result has no events array")
	}
	defs, table, err := musePinnedSchema()
	if err != nil {
		return err
	}
	base, ok := defs["UnframedViewNotificationParams"].(map[string]any)
	if !ok {
		return fmt.Errorf("view/page: pinned schema lacks UnframedViewNotificationParams")
	}
	for i, event := range events {
		frame, ok := event.(map[string]any)
		if !ok {
			return fmt.Errorf("view/page: event %d is not an object", i)
		}
		method, _ := frame["method"].(string)
		if strings.TrimSpace(method) == "" {
			return fmt.Errorf("view/page: event %d names no method", i)
		}
		params, ok := frame["params"].(map[string]any)
		if !ok {
			return fmt.Errorf("view/page: event %d has no params object", i)
		}
		def := base
		if ref, known := table[method]; known && ref != "" {
			resolved, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
			if !ok {
				return fmt.Errorf("view/page: event %d resolves to unknown %s", i, ref)
			}
			def = resolved
		}
		if err := museValidatePinnedValue(defs, def, params, false); err != nil {
			return fmt.Errorf("view/page: event %d (%s): %w", i, method, err)
		}
	}
	return nil
}

// museGoalOrNullDef returns the effective Goal-or-null node: the same
// anyOf shape the C1 deviation applies to session/goalChanged. Restored
// snapshot goals validate against it before the host trusts them.
func museGoalOrNullDef() (map[string]any, bool) {
	defs, _, err := musePinnedSchema()
	if err != nil {
		return nil, false
	}
	params, ok := defs["SessionGoalChangedParams"].(map[string]any)
	if !ok {
		return nil, false
	}
	props, ok := params["properties"].(map[string]any)
	if !ok {
		return nil, false
	}
	goal, ok := props["goal"].(map[string]any)
	return goal, ok && goal != nil
}

// museStrictDecodeRestoredGoal validates one goal block restored from a
// resume snapshot before the host consumes it. A restored goal that is
// neither null nor a pinned Goal fails closed at the caller instead of
// silently vanishing or, worse, attesting a clean ledger.
func museStrictDecodeRestoredGoal(raw json.RawMessage) error {
	def, ok := museGoalOrNullDef()
	if !ok {
		return fmt.Errorf("pinned schema lacks the effective goal node")
	}
	value, err := museDecodePinnedJSON(raw)
	if err != nil {
		return fmt.Errorf("restored goal is not valid JSON: %w", err)
	}
	defs, _, err := musePinnedSchema()
	if err != nil {
		return err
	}
	if err := museValidatePinnedValue(defs, def, value, false); err != nil {
		return fmt.Errorf("restored goal failed pinned-schema validation: %w", err)
	}
	return nil
}
