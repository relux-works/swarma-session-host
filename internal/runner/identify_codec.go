package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

const identifyMaxFrame = 1024
const identifyRequestSchema = "runner-identify-request-v1"
const identifyResponseSchema = "runner-identify-response-v1"

var errIdentifyFrame = errors.New("invalid_identify_frame")

type identifyRequest struct {
	Schema           string "json:\"schema\""
	Execution        string "json:\"execution\""
	ExpectedInstance string "json:\"expected_instance\""
}
type identifyBirth struct {
	Sec  uint64 "json:\"sec\""
	Usec uint64 "json:\"usec\""
}
type identifyChild struct {
	PID   int           "json:\"pid\""
	Birth identifyBirth "json:\"birth\""
}
type identifyResponse struct {
	Schema    string         "json:\"schema\""
	Execution string         "json:\"execution\""
	Instance  *string        "json:\"instance\""
	Result    string         "json:\"result\""
	Reason    *string        "json:\"reason\""
	Child     *identifyChild "json:\"child\""
}

// encoding/json replaces malformed Unicode and accepts duplicate keys. Inspect
// the original bounded bytes before using its tokens; never repair identifiers.
func identifyUnicode(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		i++
		for ; i < len(data) && data[i] != '"'; i++ {
			if data[i] != '\\' {
				continue
			}
			i++
			if i >= len(data) {
				return false
			}
			if data[i] != 'u' {
				continue
			}
			if i+4 >= len(data) {
				return false
			}
			n, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
			if err != nil {
				return false
			}
			i += 4
			if n >= 0xdc00 && n <= 0xdfff {
				return false
			}
			if n >= 0xd800 && n <= 0xdbff {
				if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
					return false
				}
				low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
				if err != nil || low < 0xdc00 || low > 0xdfff {
					return false
				}
				i += 6
			}
		}
	}
	return true
}

func identifyTree(data []byte) (map[string]any, error) {
	if len(data) == 0 || len(data) > identifyMaxFrame || data[len(data)-1] != '\n' || !identifyUnicode(data) {
		return nil, errIdentifyFrame
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	v, err := identifyValue(d, 0)
	if err != nil {
		return nil, errIdentifyFrame
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errIdentifyFrame
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errIdentifyFrame
	}
	return m, nil
}
func identifyValue(d *json.Decoder, depth int) (any, error) {
	if depth > 3 {
		return nil, errIdentifyFrame
	}
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch v := t.(type) {
	case json.Delim:
		if v != '{' {
			return nil, errIdentifyFrame
		}
		m := make(map[string]any)
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := k.(string)
			if !ok {
				return nil, errIdentifyFrame
			}
			if _, exists := m[key]; exists {
				return nil, errIdentifyFrame
			}
			value, err := identifyValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			m[key] = value
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return nil, errIdentifyFrame
		}
		return m, nil
	case string, json.Number, nil:
		return v, nil
	default:
		return nil, errIdentifyFrame
	}
}
func identifyMembers(m map[string]any, names ...string) bool {
	if len(m) != len(names) {
		return false
	}
	for _, name := range names {
		if _, ok := m[name]; !ok {
			return false
		}
	}
	return true
}
func identifyText(v any) (string, bool) { s, ok := v.(string); return s, ok }
func identifyExecution(s string) bool {
	if len(s) == 0 || len(s) > 128 || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return false
		}
	}
	return true
}
func identifyInstance(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, b := range []byte(s) {
		if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return false
		}
	}
	return true
}
func identifyInteger(v any, min, max uint64) (uint64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	s := string(n)
	if len(s) == 0 {
		return 0, false
	}
	for _, b := range []byte(s) {
		if b < '0' || b > '9' {
			return 0, false
		}
	}
	u, err := strconv.ParseUint(s, 10, 64)
	return u, err == nil && u >= min && u <= max
}
func decodeIdentifyRequest(data []byte) (identifyRequest, error) {
	m, err := identifyTree(data)
	if err != nil || !identifyMembers(m, "schema", "execution", "expected_instance") {
		return identifyRequest{}, errIdentifyFrame
	}
	s, ok := identifyText(m["schema"])
	if !ok || s != identifyRequestSchema {
		return identifyRequest{}, errIdentifyFrame
	}
	e, ok := identifyText(m["execution"])
	if !ok || !identifyExecution(e) {
		return identifyRequest{}, errIdentifyFrame
	}
	i, ok := identifyText(m["expected_instance"])
	if !ok || !identifyInstance(i) {
		return identifyRequest{}, errIdentifyFrame
	}
	return identifyRequest{Schema: s, Execution: e, ExpectedInstance: i}, nil
}
func decodeIdentifyResponse(data []byte) (identifyResponse, error) {
	m, err := identifyTree(data)
	if err != nil || !identifyMembers(m, "schema", "execution", "instance", "result", "reason", "child") {
		return identifyResponse{}, errIdentifyFrame
	}
	s, ok := identifyText(m["schema"])
	if !ok || s != identifyResponseSchema {
		return identifyResponse{}, errIdentifyFrame
	}
	e, ok := identifyText(m["execution"])
	if !ok || !identifyExecution(e) {
		return identifyResponse{}, errIdentifyFrame
	}
	result, ok := identifyText(m["result"])
	if !ok {
		return identifyResponse{}, errIdentifyFrame
	}
	r := identifyResponse{Schema: s, Execution: e, Result: result}
	switch result {
	case "verified":
		i, ok := identifyText(m["instance"])
		if !ok || !identifyInstance(i) || m["reason"] != nil {
			return identifyResponse{}, errIdentifyFrame
		}
		child, ok := m["child"].(map[string]any)
		if !ok || !identifyMembers(child, "pid", "birth") {
			return identifyResponse{}, errIdentifyFrame
		}
		birth, ok := child["birth"].(map[string]any)
		if !ok || !identifyMembers(birth, "sec", "usec") {
			return identifyResponse{}, errIdentifyFrame
		}
		pid, p := identifyInteger(child["pid"], 1, 2147483647)
		sec, s := identifyInteger(birth["sec"], 0, 9007199254740991)
		usec, u := identifyInteger(birth["usec"], 0, 999999)
		if !p || !s || !u {
			return identifyResponse{}, errIdentifyFrame
		}
		r.Instance = &i
		r.Child = &identifyChild{PID: int(pid), Birth: identifyBirth{Sec: sec, Usec: usec}}
	case "conflict", "unavailable":
		reason, ok := identifyText(m["reason"])
		if !ok || m["instance"] != nil || m["child"] != nil {
			return identifyResponse{}, errIdentifyFrame
		}
		allowed := "|birth_unavailable|child_reaped|wait_unknown|runner_closed|busy|unsupported_platform|"
		if result == "conflict" {
			allowed = "|execution_mismatch|instance_mismatch|"
		}
		if reason == "" || strings.Contains(reason, "|") || !strings.Contains(allowed, "|"+reason+"|") {
			return identifyResponse{}, errIdentifyFrame
		}
		r.Reason = &reason
	default:
		return identifyResponse{}, errIdentifyFrame
	}
	return r, nil
}
func encodeIdentify(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil || len(b)+1 > identifyMaxFrame {
		return nil, errIdentifyFrame
	}
	return append(b, '\n'), nil
}
