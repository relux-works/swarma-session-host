package sessionhost

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"unicode/utf8"
)

const MaxNotificationFrame = 65536 // Includes the final LF.

// ReadNotificationRequest consumes one bounded frame, retaining the reader for
// successive calls. On error the attachment owner must close, without dispatch
// or scanning for another frame. Ownership, deadlines and single-flight exchange
// belong to that attachment; this function neither enrolls nor dispatches.
func ReadNotificationRequest(r *bufio.Reader) (NotificationRequest, error) {
	b, err := readNotificationFrame(r)
	if err != nil {
		return NotificationRequest{}, ErrNotificationFrame
	}
	return DecodeNotificationRequest(b)
}

// ReadNotificationResponse returns unresolved on any incomplete/mismatched
// response. EOF, including clean EOF, never establishes non-application.
func ReadNotificationResponse(r *bufio.Reader, q NotificationRequest) (NotificationResponse, error) {
	b, err := readNotificationFrame(r)
	if err != nil {
		return NotificationResponse{}, ErrNotificationUnresolved
	}
	return DecodeNotificationResponse(b, q)
}

func readNotificationFrame(r *bufio.Reader) ([]byte, error) {
	// Bound the caller's buffer as well: ReadSlice may otherwise fill an
	// arbitrarily large reader before discovering a missing delimiter.
	if r == nil || r.Size() > MaxNotificationFrame {
		return nil, ErrNotificationFrame
	}
	frame := make([]byte, 0, 4096)
	for {
		part, err := r.ReadSlice('\n')
		if len(part) > MaxNotificationFrame-len(frame) {
			return nil, ErrNotificationFrame
		}
		frame = append(frame, part...)
		if err == nil {
			return frame, nil
		}
		if err != bufio.ErrBufferFull || len(frame) >= MaxNotificationFrame {
			return nil, ErrNotificationFrame
		}
	}
}

func DecodeNotificationRequest(frame []byte) (NotificationRequest, error) {
	m, err := notificationObject(frame)
	if err != nil || !notificationRequestShape(m) {
		return NotificationRequest{}, ErrNotificationFrame
	}
	var q NotificationRequest
	if json.Unmarshal(frame[:len(frame)-1], &q) != nil || !q.valid() {
		return NotificationRequest{}, ErrNotificationFrame
	}
	return q, nil
}

func DecodeNotificationResponse(frame []byte, q NotificationRequest) (NotificationResponse, error) {
	m, err := notificationObject(frame)
	if err != nil || !notificationResponseShape(m) {
		return NotificationResponse{}, ErrNotificationUnresolved
	}
	var r NotificationResponse
	if json.Unmarshal(frame[:len(frame)-1], &r) != nil || !r.validFor(q) {
		return NotificationResponse{}, ErrNotificationUnresolved
	}
	return r, nil
}

func EncodeNotificationRequest(q NotificationRequest) ([]byte, error) {
	if !q.valid() {
		return nil, ErrNotificationFrame
	}
	return encodeNotificationFrame(q, ErrNotificationFrame)
}

func EncodeNotificationResponse(r NotificationResponse, q NotificationRequest) ([]byte, error) {
	if !r.validFor(q) {
		return nil, ErrNotificationUnresolved
	}
	return encodeNotificationFrame(r, ErrNotificationUnresolved)
}

func encodeNotificationFrame(v any, failure error) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil || len(b) >= MaxNotificationFrame {
		return nil, failure
	}
	return append(b, '\n'), nil
}

// Token inspection precedes typed decoding: encoding/json alone accepts
// duplicate and case-insensitive keys. Bound depth before descending; reject
// null/arrays throughout because no contract object admits them.
func notificationObject(frame []byte) (map[string]any, error) {
	if len(frame) < 3 || len(frame) > MaxNotificationFrame || frame[len(frame)-1] != '\n' ||
		frame[len(frame)-2] != '}' || !utf8.Valid(frame) || bytes.IndexByte(frame[:len(frame)-1], '\n') >= 0 {
		return nil, ErrNotificationFrame
	}
	d := json.NewDecoder(bytes.NewReader(frame[:len(frame)-1]))
	d.UseNumber()
	v, err := notificationValue(d, 0)
	if err != nil {
		return nil, ErrNotificationFrame
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, ErrNotificationFrame
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, ErrNotificationFrame
	}
	return m, nil
}

func notificationValue(d *json.Decoder, depth int) (any, error) {
	t, err := d.Token()
	if err != nil || t == nil {
		return nil, ErrNotificationFrame
	}
	if delim, ok := t.(json.Delim); ok {
		if delim != '{' || depth >= 6 {
			return nil, ErrNotificationFrame
		}
		m := make(map[string]any)
		for d.More() {
			k, err := d.Token()
			key, ok := k.(string)
			if err != nil || !ok {
				return nil, ErrNotificationFrame
			}
			if _, exists := m[key]; exists {
				return nil, ErrNotificationFrame
			}
			value, err := notificationValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			m[key] = value
		}
		if end, err := d.Token(); err != nil || end != json.Delim('}') {
			return nil, ErrNotificationFrame
		}
		return m, nil
	}
	return t, nil
}

func notificationKeys(m map[string]any, keys ...string) bool {
	if len(m) != len(keys) {
		return false
	}
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}

func notificationStrings(m map[string]any, keys ...string) bool {
	for _, k := range keys {
		if _, ok := m[k].(string); !ok {
			return false
		}
	}
	return true
}

var notificationBindingKeys = []string{"execution", "controller_epoch", "runner_instance", "native_thread", "member_ref", "connection_ref", "configuration_ref", "profile_lock_ref", "consumer_generation", "enrollment_ref", "workflow_reference", "workflow_sha256"}

func notificationBindingShape(value any) bool {
	m, ok := value.(map[string]any)
	return ok && notificationKeys(m, notificationBindingKeys...) && notificationStrings(m, notificationBindingKeys...)
}

func notificationRequestShape(m map[string]any) bool {
	keys := []string{"contract", "request_id", "method", "binding"}
	if m["method"] == "trigger" || m["method"] == "lookup_attempt" {
		keys = append(keys, "notice_id")
		if !notificationStrings(m, "notice_id") {
			return false
		}
	}
	return notificationKeys(m, keys...) && notificationStrings(m, "contract", "request_id", "method") && notificationBindingShape(m["binding"])
}

func notificationResponseShape(m map[string]any) bool {
	keys := []string{"contract", "request_id", "method", "result", "reason"}
	switch m["result"] {
	case "observed":
		keys = append(keys, "view")
		v, ok := m["view"].(map[string]any)
		if !ok || !notificationKeys(v, "binding", "adapter", "declared_native_release", "notice_mode", "gate", "records_remaining") ||
			!notificationStrings(v, "adapter", "declared_native_release", "notice_mode", "gate") || !notificationBindingShape(v["binding"]) {
			return false
		}
		n, ok := v["records_remaining"].(json.Number)
		if !ok {
			return false
		}
		// Counts are JSON integers, unlike the explicitly canonical u64
		// strings. ParseInt rejects fractions/exponents but preserves -0.
		count, err := strconv.ParseInt(string(n), 10, 8)
		if err != nil || count < 0 || count > 32 {
			return false
		}
	case "accepted", "existing-attempt":
		keys = append(keys, "attempt")
		a, ok := m["attempt"].(map[string]any)
		fields := []string{"notice_id", "workflow_reference", "workflow_sha256", "sequence", "custody", "native_stage"}
		if !ok || !notificationKeys(a, fields...) || !notificationStrings(a, fields...) {
			return false
		}
	case "refused", "unresolved":
	default:
		return false
	}
	return notificationKeys(m, keys...) && notificationStrings(m, "contract", "request_id", "method", "result", "reason")
}
