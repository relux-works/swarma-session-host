package codexadapter

import (
	"bytes"
	"encoding/json"
)

// DebugPayloadEnvelope checks the canonical parser's stateless method-envelope
// predicates for the private pre-discard projection. It grants no native family,
// turn or response authority. RPC results/errors never qualify for payload view.
func DebugPayloadEnvelope(frame []byte) bool {
	if len(frame) == 0 || len(frame) > MaxFrame || !json.Valid(frame) || bytes.TrimSpace(frame)[0] != '{' || !uniqueKeys(frame) {
		return false
	}
	var e envelope
	return json.Unmarshal(frame, &e) == nil && e.Method != "" && len(e.Result) == 0 && len(e.Error) == 0
}
