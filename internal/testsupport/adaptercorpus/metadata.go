package adaptercorpus

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

const MetadataSentinel = "fabricated-offline-output-only-canary"

// ScanMetadata is an independent, fail-closed output oracle. Documents and
// cumulative decoded-string bytes are each capped at 64 KiB; nesting at 16 and
// visited values at 4096. JSON held inside strings receives the same inspection.
// These finite witnesses do not claim to detect every encoding of arbitrary data.
func ScanMetadata(b []byte) error {
	if len(b) > 65536 {
		return fmt.Errorf("OFFLINE_METADATA_ONLY: document_bound")
	}
	if bytes.Contains(b, []byte(MetadataSentinel)) {
		return fmt.Errorf("OFFLINE_METADATA_ONLY: incoming output")
	}
	var root any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if !json.Valid(b) || dec.Decode(&root) != nil {
		return fmt.Errorf("OFFLINE_METADATA_ONLY: invalid_json")
	}
	remaining, nodes := 65536, 4096
	var scan func(any, int) error
	scan = func(v any, depth int) error {
		nodes--
		if depth > 16 || nodes < 0 {
			return fmt.Errorf("OFFLINE_METADATA_ONLY: inspection_bound")
		}
		switch x := v.(type) {
		case string:
			if strings.Contains(x, MetadataSentinel) {
				return fmt.Errorf("OFFLINE_METADATA_ONLY: incoming output")
			}
			trimmed := strings.TrimSpace(x)
			if len(trimmed) != 0 && strings.ContainsAny(trimmed[:1], "\"{[") && json.Valid([]byte(trimmed)) {
				remaining -= len(trimmed)
				if remaining < 0 {
					return fmt.Errorf("OFFLINE_METADATA_ONLY: decoded_string_bound")
				}
				var inner any
				d := json.NewDecoder(strings.NewReader(trimmed))
				d.UseNumber()
				if d.Decode(&inner) != nil {
					return fmt.Errorf("OFFLINE_METADATA_ONLY: invalid_inner_json")
				}
				if err := scan(inner, depth+1); err != nil {
					return err
				}
			}
		case map[string]any:
			for k, value := range x {
				if err := scan(k, depth+1); err != nil {
					return err
				}
				if err := scan(value, depth+1); err != nil {
					return err
				}
			}
		case []any:
			for _, value := range x {
				if err := scan(value, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return scan(root, 0)
}

// MetadataValues are supplied from the synthetic fixture's independent inputs
// and live runner identity, never copied from the persisted document under test.
type MetadataValues struct {
	Model, Provider, Effort, Workspace string
	Thread, RunnerInstance             string
	Turns, Inputs                      []string
}

type metadataCatalogue struct{ err error }

func (c *metadataCatalogue) need(ok bool, field string) {
	if !ok && c.err == nil {
		c.err = fmt.Errorf("OFFLINE_METADATA_ONLY: catalogue %s", field)
	}
}
func (c *metadataCatalogue) object(v any, required, optional string) map[string]any {
	m, ok := v.(map[string]any)
	c.need(ok, "object")
	for _, k := range strings.Fields(required) {
		_, present := m[k]
		c.need(present, "missing "+k)
	}
	keys := strings.Fields(required + " " + optional)
	for k := range m {
		c.need(slices.Contains(keys, k), "unexpected "+k)
	}
	return m
}
func (c *metadataCatalogue) text(v any, values ...string) string {
	s, ok := v.(string)
	c.need(ok && slices.Contains(values, s), "string value")
	return s
}
func (c *metadataCatalogue) number(v any, lo, hi uint64) uint64 {
	n, ok := v.(json.Number)
	u, err := strconv.ParseUint(string(n), 10, 64)
	c.need(ok && err == nil && u >= lo && u <= hi, "integer value")
	return u
}
func (c *metadataCatalogue) boolean(v any, expected *bool) {
	b, ok := v.(bool)
	c.need(ok && (expected == nil || b == *expected), "boolean value")
}
func (c *metadataCatalogue) array(v any, max int) []any {
	if v == nil {
		return nil
	}
	a, ok := v.([]any)
	c.need(ok && len(a) <= max, "array value")
	return a
}
func metadataDecode(b []byte) any {
	var value any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	_ = d.Decode(&value) // ScanMetadata has already required a valid document.
	return value
}
func metadataHash(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func metadataPlan(v MetadataValues) string {
	// The fixture contract names these four config members in this order.
	b, _ := json.Marshal(struct{ Model, Provider, Effort, Workspace string }{v.Model, v.Provider, v.Effort, v.Workspace})
	return metadataHash(b)
}

// The catalogues enumerate this finite corpus's permitted metadata values and
// closed members, independently of production restore/state validators. k names
// only a deliberately supplied corruption, not permission for arbitrary invalid data.
func (c *metadataCatalogue) checkpoint(value any, v MetadataValues, k int) {
	m := c.object(value, "version stream input_id accepted completed begun", "")
	version := uint64(1)
	if k == 1 {
		version = 99
	}
	c.number(m["version"], version, version)
	c.text(m["input_id"], append([]string{""}, v.Inputs...)...)
	c.text(m["accepted"], append([]string{""}, v.Turns...)...)
	c.text(m["completed"], append([]string{""}, v.Turns...)...)
	yes, no := true, false
	c.boolean(m["begun"], &yes)
	s := c.object(m["stream"], "version protocol offset closed", "")
	c.number(s["version"], 1, 1)
	c.number(s["offset"], 0, 262144)
	if k == 2 {
		c.need(s["closed"] == nil, "deliberate null closed")
	} else {
		c.boolean(s["closed"], nil)
	}
	p := c.object(s["protocol"], "plan_digest phase view request pending operation", "")
	c.text(p["plan_digest"], metadataPlan(v))
	c.text(p["phase"], "ready", "turn", "lost")
	r := c.number(p["request"], 2, 4)
	if k == 3 {
		c.number(p["pending"], r+1, r+1)
	} else {
		c.number(p["pending"], 0, r)
	}
	c.text(p["operation"], "", "turn/start")
	w := c.object(p["view"], "State Thread Turn CompletedTurn AuthorityRequired", "")
	c.text(w["State"], "idle", "busy", "unknown")
	c.text(w["Thread"], v.Thread)
	c.text(w["Turn"], append([]string{""}, v.Turns...)...)
	c.text(w["CompletedTurn"], append([]string{""}, v.Turns...)...)
	c.boolean(w["AuthorityRequired"], &no)
}
func CheckpointMetadata(b []byte, v MetadataValues, k int) error {
	if err := ScanMetadata(b); err != nil {
		return err
	}
	c := new(metadataCatalogue)
	c.checkpoint(metadataDecode(b), v, k)
	return c.err
}

func JournalMetadata(b []byte, v MetadataValues, k int) error {
	if err := ScanMetadata(b); err != nil {
		return err
	}
	c := new(metadataCatalogue)
	m := c.object(metadataDecode(b), "version execution runner_instance adapter selection epoch sequence fence records stop_requested input_state security_stopped protocol", "")
	c.number(m["version"], 1, 1)
	c.text(m["execution"], "offline-execution")
	c.text(m["runner_instance"], v.RunnerInstance)
	c.text(m["adapter"], "codex")
	s := c.object(m["selection"], "vendor model reasoning", "")
	c.text(s["vendor"], v.Provider)
	c.text(s["model"], v.Model)
	c.text(s["reasoning"], v.Effort)
	c.number(m["epoch"], 1, 2)
	c.text(m["fence"], "open", "held", "unknown")
	c.text(m["input_state"], "idle", "busy", "unknown")
	no, yes := false, true
	c.boolean(m["stop_requested"], &no)
	c.boolean(m["security_stopped"], &no)
	records := c.array(m["records"], 2)
	c.number(m["sequence"], uint64(len(records)), uint64(len(records)))
	for i, value := range records {
		r := c.object(value, "notice digest sequence state", "native_accepted native_session native_turn native_receipt outcome")
		n := c.object(r["notice"], "id reference", "")
		id := c.text(n["id"], "one", "two")
		ref := c.text(n["reference"], "synthetic/"+id)
		digestInput := fmt.Sprintf("{\"Notice\":{\"id\":%q,\"reference\":%q},\"Selection\":{\"vendor\":%q,\"model\":%q,\"reasoning\":%q}}", id, ref, v.Provider, v.Model, v.Effort)
		c.text(r["digest"], metadataHash([]byte(digestInput)))
		c.number(r["sequence"], uint64(i+1), uint64(i+1))
		c.text(r["state"], "queued", "unknown", "applied")
		for key, value := range r {
			switch key {
			case "native_accepted":
				c.boolean(value, &yes)
			case "native_session":
				c.text(value, v.Thread)
			case "native_turn":
				c.text(value, v.Turns...)
			case "native_receipt":
				c.text(value, "")
			case "outcome":
				c.text(value, "completed")
			}
		}
	}
	p := c.object(m["protocol"], "boundary_version observed_release checkpoint wire_sequence pending view", "")
	c.number(p["boundary_version"], 1, 1)
	c.text(p["observed_release"], "0.164.0")
	c.checkpoint(p["checkpoint"], v, k)
	c.number(p["wire_sequence"], 3, 5)
	view := c.object(p["view"], "InputID Activity NativeSession CurrentTurn", "")
	c.text(view["InputID"], "", "one", "two")
	c.text(view["Activity"], "idle", "busy", "unknown")
	c.text(view["NativeSession"], "", v.Thread)
	c.text(view["CurrentTurn"], append([]string{""}, v.Turns...)...)
	for _, value := range c.array(p["pending"], 1) {
		a := c.object(value, "sequence key input_id data digest", "receipt")
		seq := c.number(a["sequence"], 4, 5)
		id := c.text(a["input_id"], "one", "two")
		c.text(a["key"], "notice-"+metadataHash([]byte(id)))
		encoded, ok := a["data"].(string)
		data, err := base64.StdEncoding.DecodeString(encoded)
		c.need(ok && err == nil && len(data) <= 4096, "outgoing frame encoding")
		c.text(a["digest"], metadataHash(data))
		if receipt, ok := a["receipt"]; ok {
			c.text(receipt, "write_missing", "write_unknown", "sequence_conflict")
		}
		expected := map[string]any{"id": json.Number(strconv.FormatUint(seq-1, 10)), "method": "turn/start", "params": map[string]any{"threadId": v.Thread, "model": v.Model, "effort": v.Effort, "input": []any{map[string]any{"type": "text", "text": "Notice reference: synthetic/" + id + ". For this synthetic notice, reply ok without tools."}}}}
		c.need(json.Valid(data) && reflect.DeepEqual(metadataDecode(data), expected), "outgoing intent catalogue")
	}
	return c.err
}
