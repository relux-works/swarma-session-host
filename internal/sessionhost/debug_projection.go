package sessionhost

import (
	"encoding/json"

	"github.com/relux-works/swarma-session-host/internal/codexadapter"
)

// These are presentation-only, fixture-qualified families. No field acquires
// native response authority. In particular, never retain a raw params/result,
// map, or unknown nested member, even on a recognized method.
type debugPayload struct {
	Method string             `json:"method"`
	ID     json.RawMessage    `json:"id,omitempty"`
	Params debugPayloadParams `json:"params"`
}
type debugPayloadParams struct {
	Thread    string            `json:"threadId"`
	Turn      string            `json:"turnId"`
	ItemID    string            `json:"itemId,omitempty"`
	Delta     *string           `json:"delta,omitempty"`
	Item      *debugPayloadItem `json:"item,omitempty"`
	Questions []debugQuestion   `json:"questions,omitempty"`
}
type debugPayloadItem struct {
	ID       string        `json:"id"`
	Type     string        `json:"type"`
	Text     *string       `json:"text,omitempty"`
	Command  *string       `json:"command,omitempty"`
	Output   *string       `json:"aggregatedOutput,omitempty"`
	ExitCode *int          `json:"exitCode,omitempty"`
	Changes  []debugChange `json:"changes,omitempty"`
}
type debugChange struct {
	Path string `json:"path"`
	Diff string `json:"diff"`
}
type debugQuestion struct {
	ID       string        `json:"id"`
	Header   string        `json:"header"`
	Question string        `json:"question"`
	Options  []debugOption `json:"options"`
}
type debugOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

func debugProject(frame []byte) (kind, thread, turn string, payload []byte) {
	diagnostic := func() (string, string, string, []byte) {
		return "diagnostic", "", "", []byte(`{"diagnostic":"unqualified_payload"}`)
	}
	if !codexadapter.DebugPayloadEnvelope(frame) {
		return diagnostic()
	}
	var in debugPayload
	if json.Unmarshal(frame, &in) != nil {
		return diagnostic()
	}
	p := in.Params
	if !scalar.MatchString(p.Thread) || !scalar.MatchString(p.Turn) {
		return diagnostic()
	}
	out := debugPayload{Method: in.Method, Params: debugPayloadParams{Thread: p.Thread, Turn: p.Turn}}
	kind = "structured_payload"
	switch in.Method {
	case "item/agentMessage/delta", "item/commandExecution/outputDelta", "item/fileChange/outputDelta":
		if len(in.ID) != 0 || !scalar.MatchString(p.ItemID) || p.Delta == nil {
			return diagnostic()
		}
		out.Params.ItemID, out.Params.Delta = p.ItemID, p.Delta
	case "item/started", "item/completed":
		if len(in.ID) != 0 || p.Item == nil || !scalar.MatchString(p.Item.ID) {
			return diagnostic()
		}
		item := &debugPayloadItem{ID: p.Item.ID, Type: p.Item.Type}
		switch item.Type {
		case "agentMessage":
			if p.Item.Text == nil {
				return diagnostic()
			}
			item.Text = p.Item.Text
		case "commandExecution":
			if p.Item.Command == nil {
				return diagnostic()
			}
			item.Command, item.Output, item.ExitCode = p.Item.Command, p.Item.Output, p.Item.ExitCode
		case "fileChange":
			if len(p.Item.Changes) == 0 || len(p.Item.Changes) > 64 {
				return diagnostic()
			}
			item.Changes = p.Item.Changes
		default:
			return diagnostic()
		}
		out.Params.Item = item
	case "item/tool/requestUserInput":
		var id uint64
		if json.Unmarshal(in.ID, &id) != nil || id == 0 || !scalar.MatchString(p.ItemID) || len(p.Questions) == 0 || len(p.Questions) > 16 {
			return diagnostic()
		}
		for _, q := range p.Questions {
			if !scalar.MatchString(q.ID) || q.Question == "" || len(q.Options) == 0 || len(q.Options) > 32 {
				return diagnostic()
			}
			for _, o := range q.Options {
				if o.Label == "" {
					return diagnostic()
				}
			}
		}
		// Re-encode the scalar ID; do not preserve even its original wire encoding.
		out.ID, _ = json.Marshal(id)
		out.Params.ItemID, out.Params.Questions = p.ItemID, p.Questions
		kind = "protocol_interaction"
	default:
		return diagnostic()
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return diagnostic()
	}
	return kind, p.Thread, p.Turn, payload
}
