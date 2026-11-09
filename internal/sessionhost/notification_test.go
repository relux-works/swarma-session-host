package sessionhost

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func notificationFixture(method string) NotificationRequest {
	q := NotificationRequest{
		Contract: NotificationContract, RequestID: "call-1", Method: method,
		Binding: NotificationBinding{
			Execution: "execution-1", ControllerEpoch: "1", RunnerInstance: "runner-1", NativeThread: "thread-1",
			MemberRef: "member:fixture", ConnectionRef: "connection-1", ConfigurationRef: "selection-1",
			ProfileLockRef: "lock-1", ConsumerGeneration: "generation-1", EnrollmentRef: "enrollment-1",
			WorkflowReference: "host-notification-inbox-v1", WorkflowSHA256: strings.Repeat("a", 64),
		},
	}
	if method != "observe_binding" {
		q.NoticeID = "opaque-notice-1"
	}
	return q
}

func notificationReply(q NotificationRequest, result string) NotificationResponse {
	r := NotificationResponse{Contract: q.Contract, RequestID: q.RequestID, Method: q.Method, Result: result}
	switch result {
	case "observed":
		r.View = &NotificationView{Binding: q.Binding, Adapter: "codex", DeclaredNativeRelease: "0.167.0", NoticeMode: WorkReference, Gate: "open", RecordsRemaining: 32}
	case "accepted", "existing-attempt":
		r.Attempt = &NotificationAttempt{NoticeID: q.NoticeID, WorkflowReference: q.Binding.WorkflowReference, WorkflowSHA256: q.Binding.WorkflowSHA256,
			Sequence: "1", Custody: "host-accepted", NativeStage: "not-evidenced"}
	case "refused":
		r.Reason = "input-not-ready"
	case "unresolved":
		r.Reason = "attempt-unresolved"
	}
	return r
}

func notificationJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return append(b, '\n')
}

func notificationMap(t *testing.T, v any) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(notificationJSON(t, v), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// All three requests and every legal method/result pairing round-trip. Neither
// this syntax check nor the declared native release proves enrollment/execution.
func TestNotificationWireRoundTrip(t *testing.T) {
	for _, method := range []string{"observe_binding", "trigger", "lookup_attempt"} {
		q := notificationFixture(method)
		frame, err := EncodeNotificationRequest(q)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ReadNotificationRequest(bufio.NewReaderSize(bytes.NewReader(frame), 16))
		if err != nil || got != q {
			t.Fatalf("request %s: %+v %v", method, got, err)
		}
		for _, result := range []string{"observed", "accepted", "existing-attempt", "refused", "unresolved"} {
			legal := result == "refused" || result == "unresolved" ||
				result == "observed" && method == "observe_binding" ||
				result == "accepted" && method == "trigger" ||
				result == "existing-attempt" && method != "observe_binding"
			r := notificationReply(q, result)
			b, err := EncodeNotificationResponse(r, q)
			if !legal {
				if !errors.Is(err, ErrNotificationUnresolved) || b != nil {
					t.Fatalf("illegal %s/%s encoded", method, result)
				}
				if got, err := DecodeNotificationResponse(notificationJSON(t, r), q); err != ErrNotificationUnresolved || !reflect.DeepEqual(got, NotificationResponse{}) {
					t.Fatalf("illegal %s/%s decoded: %+v %v", method, result, got, err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadNotificationResponse(bufio.NewReader(bytes.NewReader(b)), q)
			if err != nil || !reflect.DeepEqual(got, r) {
				t.Fatalf("response %s/%s: %+v %v", method, result, got, err)
			}
		}
	}
}

// Independent literal violations exercise the actual decoder, including key
// escapes that standard JSON decoding merges. Every failure returns no request.
func TestNotificationMalformedFrames(t *testing.T) {
	q := notificationFixture("trigger")
	good := string(notificationJSON(t, q))
	cases := map[string]string{
		"missing-lf":        strings.TrimSuffix(good, "\n"),
		"crlf":              strings.TrimSuffix(good, "\n") + "\r\n",
		"trailing-space":    strings.TrimSuffix(good, "\n") + " \n",
		"second-value":      strings.TrimSuffix(good, "\n") + "{}\n",
		"bom":               "\xef\xbb\xbf" + good,
		"invalid-utf8":      strings.Replace(good, "call-1", "call-\xff", 1),
		"duplicate":         strings.Replace(good, `"request_id":"call-1"`, `"request_id":"call-1","request_id":"call-2"`, 1),
		"decoded-duplicate": strings.Replace(good, `"request_id":"call-1"`, `"request_id":"call-1","request_\u0069d":"call-2"`, 1),
		"nested-duplicate":  strings.Replace(good, `"execution":"execution-1"`, `"execution":"execution-1","execut\u0069on":"other"`, 1),
		"case-alias":        strings.Replace(good, `"execution":`, `"Execution":`, 1),
		"unknown":           strings.Replace(good, `"binding":`, `"extra":"private-canary","binding":`, 1),
		"null":              strings.Replace(good, `"notice_id":"opaque-notice-1"`, `"notice_id":null`, 1),
		"array":             strings.Replace(good, `"notice_id":"opaque-notice-1"`, `"notice_id":[]`, 1),
		"bool":              strings.Replace(good, `"notice_id":"opaque-notice-1"`, `"notice_id":true`, 1),
		"number":            strings.Replace(good, `"controller_epoch":"1"`, `"controller_epoch":1`, 1),
		"deep":              `{"x":` + strings.Repeat(`{"x":`, 6) + `"v"` + strings.Repeat(`}`, 7) + "\n",
		"scalar":            "\"value\"\n", "empty-object": "{}\n", "empty": "", "internal-lf": "{\n}\n",
	}
	if got, err := DecodeNotificationRequest([]byte(good)); err != nil || got != q {
		t.Fatalf("valid control: %+v %v", got, err)
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			for _, read := range []bool{false, true} {
				var got NotificationRequest
				var err error
				if read {
					got, err = ReadNotificationRequest(bufio.NewReaderSize(strings.NewReader(bad), 16))
				} else {
					got, err = DecodeNotificationRequest([]byte(bad))
				}
				if err != ErrNotificationFrame || got != (NotificationRequest{}) || strings.Contains(err.Error(), "private-canary") {
					t.Fatalf("decode=%t: got=%+v err=%v", !read, got, err)
				}
			}
		})
	}
}

// Every required field is mandatory and case sensitive, both at the top level
// and in the complete binding. A valid non-ASCII opaque ref is permitted.
func TestNotificationRequiredFields(t *testing.T) {
	q := notificationFixture("trigger")
	for _, nested := range []bool{false, true} {
		base := notificationMap(t, q)
		fields := base
		if nested {
			fields = base["binding"].(map[string]any)
		}
		for key := range fields {
			for _, change := range []string{"missing", "null", "wrong-type", "empty"} {
				m := notificationMap(t, q)
				x := m
				if nested {
					x = m["binding"].(map[string]any)
				}
				switch change {
				case "missing":
					delete(x, key)
				case "null":
					x[key] = nil
				case "wrong-type":
					x[key] = true
				case "empty":
					x[key] = ""
				}
				if _, err := DecodeNotificationRequest(notificationJSON(t, m)); err != ErrNotificationFrame {
					t.Fatalf("nested=%t %s/%s: %v", nested, key, change, err)
				}
			}
		}
	}
	q.Binding.MemberRef = "member/é/名字"
	q.RequestID = "appel-é"
	b, err := EncodeNotificationRequest(q)
	if err != nil {
		t.Fatal(err)
	}
	// Escaped spelling is legitimate, but duplicate decoded spellings are not.
	b = bytes.Replace(b, []byte(`"request_id"`), []byte(`"request_\u0069d"`), 1)
	if got, err := DecodeNotificationRequest(b); err != nil || got != q {
		t.Fatalf("unicode/escape: %+v %v", got, err)
	}
	q = notificationFixture("observe_binding")
	m := notificationMap(t, q)
	m["notice_id"] = ""
	if _, err := DecodeNotificationRequest(notificationJSON(t, m)); err != ErrNotificationFrame {
		t.Fatal("observe admitted notice_id")
	}
}

// Byte bounds and canonical decimal strings are checked against independently
// selected boundary witnesses; plugin-specific notice prefixes are not imposed.
func TestNotificationPrimitiveBounds(t *testing.T) {
	for _, tc := range []struct {
		s    string
		max  int
		want bool
	}{
		{"a", 128, true}, {strings.Repeat("é", 64), 128, true}, {strings.Repeat("é", 65), 128, false},
		{strings.Repeat("a", 512), 512, true}, {strings.Repeat("a", 513), 512, false}, {"", 128, false},
		{"x\x00", 128, false}, {"x\x1f", 128, false}, {"x\u007f", 128, false}, {"x\u0080", 128, false},
		{"x\u009f", 128, false}, {"x\u00a0", 128, true}, {"\xff", 128, false},
	} {
		if notificationRef(tc.s, tc.max) != tc.want {
			t.Fatalf("ref len=%d max=%d", len(tc.s), tc.max)
		}
	}
	for _, s := range []string{"0", "1", "18446744073709551615"} {
		if !notificationU64(s, false) || notificationU64(s, true) != (s != "0") {
			t.Fatalf("u64 valid %s", s)
		}
	}
	for _, s := range []string{"", "00", "01", "-1", "+1", "1.0", "1e0", " 1", "1 ", "18446744073709551616"} {
		if notificationU64(s, false) {
			t.Fatalf("u64 admitted %q", s)
		}
	}
	for _, s := range []string{"a", "._:/@+-09Az", strings.Repeat("x", 128)} {
		q := notificationFixture("trigger")
		q.NoticeID = s
		if _, err := EncodeNotificationRequest(q); err != nil {
			t.Fatalf("valid opaque ID: %v", err)
		}
	}
	for _, s := range []string{"", strings.Repeat("x", 129), "é", "a b", "a\n"} {
		q := notificationFixture("trigger")
		q.NoticeID = s
		if _, err := EncodeNotificationRequest(q); err != ErrNotificationFrame {
			t.Fatal("invalid host ID admitted")
		}
	}
	for _, s := range []string{strings.Repeat("a", 63), strings.Repeat("a", 65), strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		q := notificationFixture("trigger")
		q.Binding.WorkflowSHA256 = s
		if _, err := EncodeNotificationRequest(q); err != ErrNotificationFrame {
			t.Fatal("invalid digest admitted")
		}
	}
}

// Framing accepts exactly the total byte ceiling, rejects the next byte, bounds
// reads of an endless stream, and preserves the next frame in one shared reader.
func TestNotificationFrameBoundsAndSuccessive(t *testing.T) {
	q := notificationFixture("trigger")
	b := notificationJSON(t, q)
	exact := append(bytes.Repeat([]byte{' '}, MaxNotificationFrame-len(b)), b...)
	if got, err := ReadNotificationRequest(bufio.NewReaderSize(bytes.NewReader(exact), 16)); err != nil || got != q {
		t.Fatalf("exact ceiling: %v", err)
	}
	for _, bad := range [][]byte{append([]byte{' '}, exact...), bytes.Repeat([]byte{'x'}, MaxNotificationFrame)} {
		if _, err := ReadNotificationRequest(bufio.NewReaderSize(bytes.NewReader(bad), 16)); err != ErrNotificationFrame {
			t.Fatal("oversize/missing LF admitted")
		}
		if _, err := DecodeNotificationRequest(bad); err != ErrNotificationFrame {
			t.Fatal("direct oversize/missing LF admitted")
		}
	}
	r := bufio.NewReaderSize(bytes.NewReader(append(bytes.Clone(b), b...)), 16)
	for i := 0; i < 2; i++ {
		if got, err := ReadNotificationRequest(r); err != nil || got != q {
			t.Fatalf("frame %d: %v", i, err)
		}
	}
	endless := &notificationEndless{}
	if _, err := ReadNotificationRequest(bufio.NewReaderSize(endless, 4096)); err != ErrNotificationFrame || endless.read > MaxNotificationFrame {
		t.Fatalf("unbounded read: %d %v", endless.read, err)
	}
	oversizedReader := &notificationEndless{}
	if _, err := ReadNotificationRequest(bufio.NewReaderSize(oversizedReader, 2*MaxNotificationFrame)); err != ErrNotificationFrame || oversizedReader.read != 0 {
		t.Fatalf("oversized reader consumed input: %d %v", oversizedReader.read, err)
	}
	// Depth six parses, seven rejects before traversing another object. Neither
	// arbitrary nested shape is an admitted method request.
	for _, depth := range []int{6, 7} {
		frame := []byte(strings.Repeat(`{"x":`, depth) + `"v"` + strings.Repeat(`}`, depth) + "\n")
		_, err := notificationObject(frame)
		if (err == nil) != (depth == 6) {
			t.Fatalf("depth %d: %v", depth, err)
		}
	}
}

type notificationEndless struct{ read int }

func (r *notificationEndless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	r.read += len(p)
	return len(p), nil
}

// A successful-looking response with a changed call, binding, notice or workflow
// is unresolved, with no exported partial success. Later malformed/EOF responses
// cannot mutate an earlier accepted value retained by the caller.
func TestNotificationResponseCorrelation(t *testing.T) {
	q := notificationFixture("trigger")
	accepted := notificationReply(q, "accepted")
	good := notificationJSON(t, accepted)
	retained, err := DecodeNotificationResponse(good, q)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*NotificationResponse){
		func(r *NotificationResponse) { r.Contract = "other/1" }, func(r *NotificationResponse) { r.RequestID = "other" },
		func(r *NotificationResponse) { r.Method = "lookup_attempt" }, func(r *NotificationResponse) { r.Reason = "not-empty" },
		func(r *NotificationResponse) { r.Attempt.NoticeID = "other" }, func(r *NotificationResponse) { r.Attempt.WorkflowReference = "other" },
		func(r *NotificationResponse) { r.Attempt.WorkflowSHA256 = strings.Repeat("b", 64) }, func(r *NotificationResponse) { r.Attempt.Sequence = "0" },
		func(r *NotificationResponse) { r.Attempt.Custody = "mailbox-received" }, func(r *NotificationResponse) { r.Attempt.NativeStage = "native-failed" },
		func(r *NotificationResponse) {
			r.View = notificationReply(notificationFixture("observe_binding"), "observed").View
		},
	} {
		r := notificationReply(q, "accepted")
		mutate(&r)
		if _, err := EncodeNotificationResponse(r, q); err != ErrNotificationUnresolved {
			t.Fatal("bad response encoded")
		}
		if got, err := DecodeNotificationResponse(notificationJSON(t, r), q); err != ErrNotificationUnresolved || !reflect.DeepEqual(got, NotificationResponse{}) {
			t.Fatal("bad response accepted")
		}
	}
	for _, b := range [][]byte{nil, good[:len(good)-1], append(bytes.Clone(good), []byte("{}\n")...)} {
		if _, err := DecodeNotificationResponse(b, q); err != ErrNotificationUnresolved {
			t.Fatal("bad frame response not unresolved")
		}
	}
	if _, err := ReadNotificationResponse(bufio.NewReader(strings.NewReader("")), q); err != ErrNotificationUnresolved {
		t.Fatal("EOF not unresolved")
	}
	if !reflect.DeepEqual(retained, accepted) {
		t.Fatal("earlier custody changed")
	}
	q = notificationFixture("observe_binding")
	r := notificationReply(q, "observed")
	r.View.Binding.ConsumerGeneration = "other"
	if _, err := DecodeNotificationResponse(notificationJSON(t, r), q); err != ErrNotificationUnresolved {
		t.Fatal("changed binding admitted")
	}
}

// Exhaustive closed reason/gate/stage vocabularies accept valid controls, reject
// a planted cross-result reason/payload, and require integer count semantics.
func TestNotificationResponseClosedVocabulary(t *testing.T) {
	for _, method := range []string{"observe_binding", "trigger", "lookup_attempt"} {
		q := notificationFixture(method)
		for result, reasons := range map[string][]string{
			"refused":    {"unsupported-contract", "attachment-unavailable", "enrollment-unverified", "binding-conflict", "stale-binding", "input-not-ready", "external-held", "security-stopped", "capacity-full", "invalid-attempt", "workflow-conflict"},
			"unresolved": {"attempt-unresolved", "observation-unavailable"},
		} {
			for _, reason := range reasons {
				r := notificationReply(q, result)
				r.Reason = reason
				if _, err := DecodeNotificationResponse(notificationJSON(t, r), q); err != nil {
					t.Fatalf("valid %s/%s: %v", result, reason, err)
				}
			}
			for _, reason := range []string{"", "not-found", "private-canary", map[string]string{"refused": "attempt-unresolved", "unresolved": "input-not-ready"}[result]} {
				r := notificationReply(q, result)
				r.Reason = reason
				if _, err := DecodeNotificationResponse(notificationJSON(t, r), q); err != ErrNotificationUnresolved || strings.Contains(err.Error(), "private-canary") {
					t.Fatal("invalid reason accepted/leaked")
				}
			}
			m := notificationMap(t, notificationReply(q, result))
			m["attempt"] = nil
			if _, err := DecodeNotificationResponse(notificationJSON(t, m), q); err != ErrNotificationUnresolved {
				t.Fatal("null success payload admitted")
			}
		}
	}
	q := notificationFixture("observe_binding")
	zero := bytes.Replace(notificationJSON(t, notificationReply(q, "observed")), []byte(`"records_remaining":32`), []byte(`"records_remaining":-0`), 1)
	if got, err := DecodeNotificationResponse(zero, q); err != nil || got.View.RecordsRemaining != 0 {
		t.Fatalf("JSON integer negative zero: %v", err)
	}
	for _, gate := range []string{"open", "input-not-ready", "external-held", "security-stopped", "capacity-full", "observation-unavailable"} {
		for _, count := range []int{0, 32} {
			r := notificationReply(q, "observed")
			r.View.Gate = gate
			r.View.RecordsRemaining = count
			if _, err := DecodeNotificationResponse(notificationJSON(t, r), q); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, count := range []string{"-1", "33", "0.0", "1e0", "\"0\"", "true", "null"} {
		b := bytes.Replace(notificationJSON(t, notificationReply(q, "observed")), []byte(`"records_remaining":32`), []byte(`"records_remaining":`+count), 1)
		if _, err := DecodeNotificationResponse(b, q); err != ErrNotificationUnresolved {
			t.Fatalf("count %s admitted", count)
		}
	}
	for field, bad := range map[string]string{"adapter": "other", "declared_native_release": "0.164.1", "notice_mode": "synthetic", "gate": "unknown"} {
		m := notificationMap(t, notificationReply(q, "observed"))
		m["view"].(map[string]any)[field] = bad
		if _, err := DecodeNotificationResponse(notificationJSON(t, m), q); err != ErrNotificationUnresolved {
			t.Fatalf("invalid view %s accepted", field)
		}
	}
	q = notificationFixture("trigger")
	for _, stage := range []string{"not-evidenced", "write-unresolved", "native-accepted", "native-completed"} {
		r := notificationReply(q, "accepted")
		r.Attempt.NativeStage = stage
		r.Attempt.Sequence = "18446744073709551615"
		if _, err := DecodeNotificationResponse(notificationJSON(t, r), q); err != nil {
			t.Fatal(err)
		}
	}
}

// Response fields and nested success objects admit no omissions, aliases or
// extras. The success reason is required even when its value is empty.
func TestNotificationResponseRequiredFields(t *testing.T) {
	for _, tc := range []struct{ method, result, nested string }{{"trigger", "accepted", "attempt"}, {"observe_binding", "observed", "view"}} {
		q := notificationFixture(tc.method)
		r := notificationReply(q, tc.result)
		for _, nested := range []string{"", tc.nested} {
			m := notificationMap(t, r)
			fields := m
			if nested != "" {
				fields = m[nested].(map[string]any)
			}
			for key := range fields {
				bad := notificationMap(t, r)
				x := bad
				if nested != "" {
					x = bad[nested].(map[string]any)
				}
				delete(x, key)
				if _, err := DecodeNotificationResponse(notificationJSON(t, bad), q); err != ErrNotificationUnresolved {
					t.Fatalf("missing %s/%s accepted", nested, key)
				}
			}
			for _, key := range []string{"unknown", "Reason"} {
				bad := notificationMap(t, r)
				x := bad
				if nested != "" {
					x = bad[nested].(map[string]any)
				}
				x[key] = "extra"
				if _, err := DecodeNotificationResponse(notificationJSON(t, bad), q); err != ErrNotificationUnresolved {
					t.Fatal("extra response key accepted")
				}
			}
		}
	}
	// A reader error is also unresolved; the underlying error is not disclosed.
	q := notificationFixture("trigger")
	good := notificationJSON(t, notificationReply(q, "accepted"))
	bad := bytes.Replace(good, []byte(`"custody":"host-accepted"`), []byte(`"custody":"host-accepted","cust\u006fdy":"other"`), 1)
	if _, err := DecodeNotificationResponse(bad, q); err != ErrNotificationUnresolved {
		t.Fatal("response duplicate decoded key admitted")
	}
	if _, err := ReadNotificationResponse(bufio.NewReader(notificationBroken{}), q); err != ErrNotificationUnresolved {
		t.Fatal("read failure not unresolved")
	}
}

type notificationBroken struct{}

func (notificationBroken) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
