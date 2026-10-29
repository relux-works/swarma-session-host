package sessionmanager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// Strict MSP decoding for the Muse host (r16): a notification or record the
// host does not FULLY validate is never success.
//
// Every notification family the host consumes is validated against the pinned
// 1.4.1-R4503.1 schema export before its typed form is trusted: required
// fields are present, types are exact, null is rejected where the schema
// forbids it, and closed enums are closed. Unknown object fields are IGNORED
// by production decoding — the export carries no additionalProperties:false
// (TEST-024) and Session-class shapes evolve additive-optionally, so rejecting
// unknown fields would break forward compatibility with the pinned server.
// The fake-emission gate reuses this same core with field closure enabled:
// emission must state exactly the known contract.
//
// Malformed input fails closed at the consumer: the MSP client terminates the
// transport with a typed protocol error, unreadable items set accounting
// incomplete (text) or taint the turn (native), and malformed restored history
// sets accounting incomplete. Zero values from a malformed payload never
// establish a successful owner result.

// museValidatePinnedValue validates a generic JSON value against one schema
// node from the pinned export. It implements the keyword subset the consumed
// closure uses ($ref, type, required, properties, items, enum with
// x-msp-openness, anyOf, pattern, maximum, minimum) and fails closed on any
// other assertion keyword. The numeric bounds are closed assertions: they
// apply only to JSON numbers, the bound itself is admitted, and a
// non-numeric or non-finite bound in the schema fails closed as malformed.
// Bounds beside anyOf are outside this subset and refused, never bypassed
// by a successful branch. Bounds inside a branch remain supported. When
// rejectUnknownFields is set, object members the schema does not declare
// are rejected; otherwise they are ignored.
func museValidatePinnedValue(defs map[string]any, schema map[string]any, value any, rejectUnknownFields bool) error {
	return museValidatePinnedNode(defs, schema, value, rejectUnknownFields, "$", 0)
}

func museValidatePinnedNode(defs map[string]any, schema map[string]any, value any, rejectUnknownFields bool, path string, depth int) error {
	if depth > 64 {
		return fmt.Errorf("%s: schema nesting exceeds the bounded depth", path)
	}
	for key := range schema {
		switch key {
		case "$ref", "type", "required", "properties", "items", "enum", "x-msp-openness", "anyOf", "pattern", "maximum", "minimum", "description":
		default:
			return fmt.Errorf("%s: unsupported schema keyword %q", path, key)
		}
	}
	if ref, ok := schema["$ref"].(string); ok {
		for key := range schema {
			if key != "$ref" && key != "description" {
				return fmt.Errorf("%s: unsupported $ref sibling %q", path, key)
			}
		}
		target, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
		if !ok {
			return fmt.Errorf("%s: unknown schema reference %q", path, ref)
		}
		return museValidatePinnedNode(defs, target, value, rejectUnknownFields, path, depth+1)
	}
	// anyOf returns after the first matching branch in this subset. Refuse
	// numeric siblings before that return rather than silently dropping an
	// assertion. Unbounded nodes retain their existing branch semantics.
	if _, hasAnyOf := schema["anyOf"]; hasAnyOf {
		for _, keyword := range []string{"maximum", "minimum"} {
			if _, bounded := schema[keyword]; bounded {
				return &museNumericBoundError{Path: path, Keyword: keyword, Reason: "beside anyOf is unsupported"}
			}
		}
	}
	if branches, ok := schema["anyOf"].([]any); ok {
		var failures []string
		for i, branch := range branches {
			node, ok := branch.(map[string]any)
			if !ok {
				return fmt.Errorf("%s: anyOf branch %d is not a schema object", path, i)
			}
			if err := museValidatePinnedNode(defs, node, value, rejectUnknownFields, path, depth+1); err == nil {
				return nil
			} else {
				failures = append(failures, err.Error())
			}
		}
		return fmt.Errorf("%s: no anyOf branch matched: %s", path, strings.Join(failures, "; "))
	}
	if expected, ok := schema["type"]; ok {
		var names []string
		switch typed := expected.(type) {
		case string:
			names = []string{typed}
		case []any:
			for _, entry := range typed {
				name, ok := entry.(string)
				if !ok {
					return fmt.Errorf("%s: type union member is not a string", path)
				}
				names = append(names, name)
			}
		default:
			return fmt.Errorf("%s: type is not a string or union", path)
		}
		matched := false
		for _, name := range names {
			if museJSONTypeMatches(name, value) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s: expected %s, got %s", path, strings.Join(names, "|"), museJSONTypeName(value))
		}
	}
	if pattern, ok := schema["pattern"].(string); ok {
		text, isString := value.(string)
		if isString {
			matched, err := regexp.MatchString(pattern, text)
			if err != nil {
				return fmt.Errorf("%s: invalid schema pattern %q", path, pattern)
			}
			if !matched {
				return fmt.Errorf("%s: %q does not match %q", path, text, pattern)
			}
		}
	}
	if values, ok := schema["enum"].([]any); ok && schema["x-msp-openness"] != "open" {
		found := false
		for _, want := range values {
			if museJSONScalarEqual(want, value) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%s: %v is outside the closed enum", path, value)
		}
	}
	if bound, ok := schema["maximum"]; ok {
		limit, boundOK := museJSONNumberValue(bound)
		if !boundOK || limit == nil {
			return &museNumericBoundError{Path: path, Keyword: "maximum", Reason: fmt.Sprintf("schema bound %v is not a finite JSON number", bound)}
		}
		number, isNumber := museJSONNumberValue(value)
		if isNumber && number == nil {
			return &museNumericBoundError{Path: path, Keyword: "maximum", Reason: fmt.Sprintf("value %v is not a finite JSON number", value)}
		}
		if isNumber && number.Cmp(limit) > 0 {
			return &museNumericBoundError{Path: path, Keyword: "maximum", Reason: fmt.Sprintf("%v exceeds bound %v", value, bound)}
		}
	}
	if bound, ok := schema["minimum"]; ok {
		limit, boundOK := museJSONNumberValue(bound)
		if !boundOK || limit == nil {
			return &museNumericBoundError{Path: path, Keyword: "minimum", Reason: fmt.Sprintf("schema bound %v is not a finite JSON number", bound)}
		}
		number, isNumber := museJSONNumberValue(value)
		if isNumber && number == nil {
			return &museNumericBoundError{Path: path, Keyword: "minimum", Reason: fmt.Sprintf("value %v is not a finite JSON number", value)}
		}
		if isNumber && number.Cmp(limit) < 0 {
			return &museNumericBoundError{Path: path, Keyword: "minimum", Reason: fmt.Sprintf("%v is below bound %v", value, bound)}
		}
	}
	if object, ok := value.(map[string]any); ok {
		if err := museRejectFoldAmbiguousKeys(path, object); err != nil {
			return err
		}
		if required, ok := schema["required"].([]any); ok {
			for _, key := range required {
				name, ok := key.(string)
				if !ok {
					return fmt.Errorf("%s: required member is not a string", path)
				}
				if _, ok := object[name]; !ok {
					return fmt.Errorf("%s: missing required %q", path, name)
				}
			}
		}
		props, _ := schema["properties"].(map[string]any)
		for key, member := range object {
			prop, ok := props[key].(map[string]any)
			if !ok {
				if declared := museFoldDeclaredName(key, props); declared != "" {
					return fmt.Errorf("%s: ambiguous key %q collides with declared %q", path, key, declared)
				}
				if rejectUnknownFields {
					return fmt.Errorf("%s: undeclared field %q", path, key)
				}
				continue
			}
			if err := museValidatePinnedNode(defs, prop, member, rejectUnknownFields, path+"."+key, depth+1); err != nil {
				return err
			}
		}
	}
	if array, ok := value.([]any); ok {
		items, ok := schema["items"].(map[string]any)
		if !ok {
			return fmt.Errorf("%s: array value without an items schema", path)
		}
		for i, member := range array {
			if err := museValidatePinnedNode(defs, items, member, rejectUnknownFields, fmt.Sprintf("%s[%d]", path, i), depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func museJSONTypeMatches(name string, value any) bool {
	switch name {
	case "null":
		return value == nil
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "integer":
		// One numeric model for validation and typed decoding (r21 C1):
		// every consumer decodes integers with int64, so a literal int64
		// cannot read (2.0, 1e0, 2^63) is contract-void here rather than
		// a validated payload the consumer silently drops. Production
		// entries decode with UseNumber so the literal survives; the
		// float64 arm stays for direct (test) callers and additionally
		// refuses what int64 cannot represent.
		switch n := value.(type) {
		case json.Number:
			_, err := strconv.ParseInt(n.String(), 10, 64)
			return err == nil
		case float64:
			return n == math.Trunc(n) && n >= -9223372036854775808.0 && n < 9223372036854775808.0
		default:
			return false
		}
	case "number":
		switch value.(type) {
		case float64, json.Number:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

// museNumericBoundError keeps the assertion and payload path available to
// callers; production entries additionally wrap it in MSPProtocolError.
type museNumericBoundError struct {
	Path    string
	Keyword string
	Reason  string
}

func (e *museNumericBoundError) Error() string {
	return fmt.Sprintf("%s: %s %s", e.Path, e.Keyword, e.Reason)
}

// museJSONNumberValue preserves a JSON literal exactly, or the exact binary
// value of a plain-decoded float64. (nil, true) denotes an invalid numeric
// representation, distinct from a non-number (nil, false) that bounds ignore.
func museJSONNumberValue(value any) (*big.Rat, bool) {
	switch n := value.(type) {
	case json.Number:
		literal := n.String()
		// Rat also accepts fractions and non-decimal literals; only the
		// JSON number grammar is valid here, including for direct callers.
		if len(literal) == 0 || (literal[0] != '-' && (literal[0] < '0' || literal[0] > '9')) || !json.Valid([]byte(literal)) {
			return nil, true
		}
		number, _ := new(big.Rat).SetString(literal)
		return number, true
	case float64:
		return new(big.Rat).SetFloat64(n), true
	default:
		return nil, false
	}
}

func museJSONTypeName(value any) string {
	switch value := value.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "boolean"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case json.Number:
		if _, err := strconv.ParseInt(value.String(), 10, 64); err == nil {
			return "integer"
		}
		return "number"
	case float64:
		if value == math.Trunc(value) {
			return "integer"
		}
		return "number"
	default:
		return fmt.Sprintf("%T", value)
	}
}

// museDecodePinnedJSON decodes wire bytes for pinned-schema validation with
// number literals preserved: 2.0 and 2 are the same float64 but only the
// latter decodes into the int64 consumers, so the integer rule above must
// see the literal the server sent.
func museDecodePinnedJSON(raw json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	// One reader per payload (r24 S3/S5): typed decoding rejects trailing
	// bytes, so the validator must see exactly one JSON value too.
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("trailing bytes after the JSON value")
	}
	return value, nil
}

func museJSONScalarEqual(want, got any) bool {
	return want == got
}

// museFoldDeclaredName reports the declared property name an inexact member
// key would shadow under case-insensitive typed decoding (r24 S3/S5). The
// validator reads exact keys while encoding/json matches struct fields
// case-insensitively with last-wins duplicates, so a payload the validator
// accepts as terminal=failed could decode as completed through a trailing
// TERMINAL member. Any inexact key that folds to a declared name is wire
// ambiguity and fails closed here, in both closure modes: forward
// compatibility covers new names, never case variants of known ones.
func museFoldDeclaredName(key string, props map[string]any) string {
	for declared := range props {
		if declared != key && strings.EqualFold(declared, key) {
			return declared
		}
	}
	return ""
}

// museRejectFoldAmbiguousKeys rejects an object carrying two distinct keys
// that fold to the same name (r24 S3/S5). With no fold-equal pair in the
// payload, exact-key map semantics and case-insensitive struct decoding
// coincide on every member, so the typed decode consumes exactly what the
// validator accepted, key case included.
func museRejectFoldAmbiguousKeys(path string, object map[string]any) error {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if strings.EqualFold(keys[i], keys[j]) {
				return fmt.Errorf("%s: ambiguous keys %q and %q differ only by case", path, keys[i], keys[j])
			}
		}
	}
	return nil
}

// museStrictDecodeClientNotification validates the params of a client-typed
// notification against the pinned export. Families the client leaves raw
// (items, gaps, unconsumed broadcasts) report nil here; their consumers
// validate them instead.
func museStrictDecodeClientNotification(method string, params json.RawMessage) error {
	def, ok := museStrictClientParamsDef(method)
	if !ok {
		return nil
	}
	return museValidatePinnedParams(method, def, params, false)
}

// museStrictDecodeItemEnvelope validates an item notification envelope and
// returns the raw item for typed decoding. The pinned Item contract further
// requires revision >= 1 for the kinds the host consumes; a schema-valid
// integer below that floor is contract-void and fails here rather than
// vanishing inside a fold.
func museStrictDecodeItemEnvelope(method string, params json.RawMessage) (json.RawMessage, error) {
	def, ok := museStrictItemParamsDef(method)
	if !ok {
		return nil, fmt.Errorf("item family %q has no pinned params definition", method)
	}
	var decoded struct {
		Item json.RawMessage `json:"item"`
	}
	if err := museValidatePinnedParams(method, def, params, false); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(params, &decoded); err != nil || len(decoded.Item) == 0 {
		return nil, fmt.Errorf("%s: envelope has no decodable item", method)
	}
	var shape struct {
		Kind     string  `json:"kind"`
		Revision float64 `json:"revision"`
	}
	if err := json.Unmarshal(decoded.Item, &shape); err != nil {
		return nil, fmt.Errorf("%s: item shape is not decodable", method)
	}
	switch shape.Kind {
	case "agentMessage", "subagent", "workflow", "reminderChild":
		if shape.Revision < 1 {
			return nil, fmt.Errorf("%s: %s revision %v is below the pinned floor of 1", method, shape.Kind, shape.Revision)
		}
	}
	return decoded.Item, nil
}

// museStrictDecodeViewGap validates a view/gap notification. A gap names
// dropped deliveries; only its session attribution is trusted before this
// check, and a malformed gap for our session still fails closed to
// accounting incomplete at the caller.
func museStrictDecodeViewGap(params json.RawMessage) error {
	def, ok := museStrictViewGapParamsDef()
	if !ok {
		return fmt.Errorf("view/gap has no pinned params definition")
	}
	return museValidatePinnedParams("view/gap", def, params, false)
}

// museStrictDecodeSessionClosed validates a session/closed notification: the
// orderly-unload notice the owner wait ends on (r21 C4).
func museStrictDecodeSessionClosed(params json.RawMessage) error {
	def, ok := museStrictSessionClosedParamsDef()
	if !ok {
		return fmt.Errorf("session/closed has no pinned params definition")
	}
	return museValidatePinnedParams("session/closed", def, params, false)
}

// museStrictDecodeViewHealthChanged validates a session/viewHealthChanged
// notification: the stream-loss notice that taints the turn (r21 C1).
func museStrictDecodeViewHealthChanged(params json.RawMessage) error {
	def, ok := museStrictViewHealthChangedParamsDef()
	if !ok {
		return fmt.Errorf("session/viewHealthChanged has no pinned params definition")
	}
	return museValidatePinnedParams("session/viewHealthChanged", def, params, false)
}

// museStrictDecodeRawNotification validates the params of any tabled raw
// notification against the pinned export. The closed dispatch choke runs
// this before attribution and routing, so every raw-routed family faces
// pinned-schema validation however its route consumes it (r22).
func museStrictDecodeRawNotification(method string, params json.RawMessage) error {
	def, ok := museStrictNotificationParamsDef(method)
	if !ok {
		return fmt.Errorf("%s has no pinned params definition", method)
	}
	return museValidatePinnedParams(method, def, params, false)
}

// museStrictDecodeServerRequest validates a server-initiated request's params
// against the pinned export's requests table. The strict client runs this
// before receipting or routing the request: a malformed request is a typed
// protocol error, never a receipted trigger (r20 rule 1).
func museStrictDecodeServerRequest(method string, params json.RawMessage) error {
	def, ok := museStrictServerRequestParamsDef(method)
	if !ok {
		return &MSPProtocolError{Reason: fmt.Sprintf("server request %q has no pinned params definition", method)}
	}
	if len(params) == 0 || bytes.Equal(bytes.TrimSpace(params), []byte("null")) {
		return &MSPProtocolError{Reason: fmt.Sprintf("server request %q params must be an object", method)}
	}
	value, err := museDecodePinnedJSON(params)
	if err != nil {
		return &MSPProtocolError{Reason: fmt.Sprintf("server request %q params are not valid JSON", method), Cause: err}
	}
	defs, _, err := musePinnedSchema()
	if err != nil {
		return err
	}
	if err := museValidatePinnedValue(defs, def, value, false); err != nil {
		return &MSPProtocolError{Reason: "server request params failed pinned-schema validation", Cause: fmt.Errorf("%s: %w", method, err)}
	}
	return nil
}

func museValidatePinnedParams(method string, def map[string]any, params json.RawMessage, rejectUnknownFields bool) error {
	if len(params) == 0 || bytes.Equal(bytes.TrimSpace(params), []byte("null")) {
		return fmt.Errorf("%s: notification params must be an object", method)
	}
	value, err := museDecodePinnedJSON(params)
	if err != nil {
		return fmt.Errorf("%s: notification params are not valid JSON: %w", method, err)
	}
	defs, _, err := musePinnedSchema()
	if err != nil {
		return err
	}
	if err := museValidatePinnedValue(defs, def, value, rejectUnknownFields); err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	return nil
}
