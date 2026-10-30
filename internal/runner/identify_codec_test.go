package runner

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

const identifyTestInstance = "0123456789abcdef0123456789abcdef"

func identifyRequestWire(execution string) []byte {
	b, _ := json.Marshal(map[string]any{"schema": "runner-identify-request-v1", "execution": execution, "expected_instance": identifyTestInstance})
	return append(b, '\n')
}
func identifySuccessWire() []byte {
	return []byte("{\"schema\":\"runner-identify-response-v1\",\"execution\":\"exec-a\",\"instance\":\"" + identifyTestInstance + "\",\"result\":\"verified\",\"reason\":null,\"child\":{\"pid\":123,\"birth\":{\"sec\":1800000000,\"usec\":123456}}}\n")
}

// Closed request grammar rejects each neighboring malformed form; equivalent
// JSON escapes preserve exact decoded bytes, including the 128-byte boundary.
func TestIdentifyRequestCodec(t *testing.T) {
	good := identifyRequestWire("exec-a")
	valid := [][]byte{good, identifyRequestWire(strings.Repeat("a", 128)),
		bytes.Replace(good, []byte("exec-a"), []byte("exec-\\u0061"), 1),
		identifyRequestWire("é"), identifyRequestWire("e\u0301"), identifyRequestWire(strings.Repeat("é", 64))}
	for i, b := range valid {
		if _, err := decodeIdentifyRequest(b); err != nil {
			t.Fatalf("REQUEST_VALID_%d: %v", i, err)
		}
	}
	cases := map[string][]byte{
		"unknown":          bytes.Replace(good, []byte("{"), []byte("{\"epoch\":1,"), 1),
		"duplicate":        bytes.Replace(good, []byte("{"), []byte("{\"schema\":\"runner-identify-request-v1\","), 1),
		"escapedDuplicate": bytes.Replace(good, []byte("{"), []byte("{\"\\u0073chema\":\"runner-identify-request-v1\","), 1),
		"missing":          []byte("{\"schema\":\"runner-identify-request-v1\",\"execution\":\"exec-a\"}\n"),
		"case":             bytes.Replace(good, []byte("execution"), []byte("Execution"), 1),
		"version":          bytes.Replace(good, []byte("request-v1"), []byte("request-v2"), 1),
		"null":             bytes.Replace(good, []byte("\"exec-a\""), []byte("null"), 1),
		"empty":            identifyRequestWire(""), "long": identifyRequestWire(strings.Repeat("a", 129)),
		"control": identifyRequestWire("a\u0001"), "c1": identifyRequestWire("a\u0085"),
		"utf8":          bytes.Replace(good, []byte("exec-a"), []byte{0xff}, 1),
		"highSurrogate": bytes.Replace(good, []byte("exec-a"), []byte("\\ud800"), 1),
		"lowSurrogate":  bytes.Replace(good, []byte("exec-a"), []byte("\\udc00"), 1),
		"wrongPair":     bytes.Replace(good, []byte("exec-a"), []byte("\\ud800\\u0061"), 1),
		"instance":      bytes.Replace(good, []byte(identifyTestInstance), []byte(strings.ToUpper(identifyTestInstance)), 1),
		"instanceShort": bytes.Replace(good, []byte(identifyTestInstance), []byte("abc"), 1),
		"trailing":      append(bytes.Clone(bytes.TrimSuffix(good, []byte("\n"))), []byte("{}\n")...),
		"array":         []byte("[]\n"), "boolean": []byte("true\n"),
		"truncated": []byte("{\"schema\":\n"), "noLF": bytes.TrimSuffix(good, []byte("\n")),
		"oversize": append(bytes.Repeat([]byte(" "), 1025-len(good)), good...),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeIdentifyRequest(b); err == nil {
				t.Fatal("REQUEST_REJECT_" + name)
			}
		})
	}
	escaped := bytes.Replace(good, []byte("exec-a"), []byte("\\ud83d\\ude00"), 1)
	if req, err := decodeIdentifyRequest(escaped); err != nil || req.Execution != "😀" {
		t.Fatalf("REQUEST_SURROGATE_PAIR: %+v %v", req, err)
	}
	exact := append(bytes.Repeat([]byte(" "), 1024-len(good)), good...)
	if _, err := decodeIdentifyRequest(exact); err != nil {
		t.Fatalf("REQUEST_BOUNDARY: %v", err)
	}
}

// Every accepted response shape has a paired lexical/range/cross-rule refusal;
// this tests this internal codec, not an external verifier or live process.
func TestIdentifyResponseCodec(t *testing.T) {
	good := identifySuccessWire()
	if _, err := decodeIdentifyResponse(good); err != nil {
		t.Fatal("RESPONSE_VALID", err)
	}
	for _, pair := range [][2]string{{"conflict", "execution_mismatch"}, {"conflict", "instance_mismatch"},
		{"unavailable", "birth_unavailable"}, {"unavailable", "unsupported_platform"}, {"unavailable", "child_reaped"},
		{"unavailable", "wait_unknown"}, {"unavailable", "runner_closed"}, {"unavailable", "busy"}} {
		b := []byte("{\"schema\":\"runner-identify-response-v1\",\"execution\":\"exec-a\",\"instance\":null,\"result\":\"" + pair[0] + "\",\"reason\":\"" + pair[1] + "\",\"child\":null}\n")
		if _, err := decodeIdentifyResponse(b); err != nil {
			t.Fatalf("RESPONSE_REFUSAL_%s: %v", pair[1], err)
		}
	}
	cases := map[string][]byte{
		"schema":         bytes.Replace(good, []byte("response-v1"), []byte("response-v0"), 1),
		"unknown":        bytes.Replace(good, []byte("{"), []byte("{\"extra\":1,"), 1),
		"duplicate":      bytes.Replace(good, []byte("{"), []byte("{\"result\":\"verified\","), 1),
		"childNull":      bytes.Replace(good, []byte("{\"pid\":123,\"birth\":{\"sec\":1800000000,\"usec\":123456}}"), []byte("null"), 1),
		"reason":         bytes.Replace(good, []byte("\"reason\":null"), []byte("\"reason\":\"busy\""), 1),
		"instanceNull":   bytes.Replace(good, []byte("\"instance\":\""+identifyTestInstance+"\""), []byte("\"instance\":null"), 1),
		"result":         bytes.Replace(good, []byte("\"verified\""), []byte("\"unknown\""), 1),
		"refusalPayload": bytes.Replace(good, []byte("\"result\":\"verified\",\"reason\":null"), []byte("\"result\":\"unavailable\",\"reason\":\"busy\""), 1),
		"nestedExtra":    bytes.Replace(good, []byte("\"pid\":123"), []byte("\"pid\":123,\"extra\":0"), 1),
		"birthExtra":     bytes.Replace(good, []byte("\"sec\":1800000000"), []byte("\"sec\":1800000000,\"extra\":0"), 1),
	}
	for _, field := range []struct {
		name, old string
		bad       []string
	}{
		{"pid", "123", []string{"0", "-1", "2147483648", "1.5", "1e2", "-0", "\"123\"", "null"}},
		{"sec", "1800000000", []string{"-1", "9007199254740992", "1.0", "1e2", "-0", "\"1\""}},
		{"usec", "123456", []string{"-1", "1000000", "0.5", "1e2", "-0", "\"1\""}},
	} {
		for _, bad := range field.bad {
			cases[field.name+"_"+bad] = bytes.Replace(good, []byte("\""+field.name+"\":"+field.old), []byte("\""+field.name+"\":"+bad), 1)
		}
	}

	refusal := []byte("{\"schema\":\"runner-identify-response-v1\",\"execution\":\"exec-a\",\"instance\":null,\"result\":\"unavailable\",\"reason\":\"busy\",\"child\":null}\n")
	cases["unknownReason"] = bytes.Replace(refusal, []byte("busy"), []byte("other"), 1)
	cases["wrongReasonClass"] = bytes.Replace(refusal, []byte("busy"), []byte("instance_mismatch"), 1)
	cases["nullReason"] = bytes.Replace(refusal, []byte("\"reason\":\"busy\""), []byte("\"reason\":null"), 1)
	cases["instanceLeak"] = bytes.Replace(refusal, []byte("\"instance\":null"), []byte("\"instance\":\""+identifyTestInstance+"\""), 1)
	cases["missingReason"] = bytes.Replace(good, []byte("\"reason\":null,"), nil, 1)
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeIdentifyResponse(b); err == nil {
				t.Fatal("RESPONSE_REJECT_" + name)
			}
		})
	}
	for _, x := range []struct{ old, new string }{{"\"pid\":123", "\"pid\":1"}, {"\"pid\":123", "\"pid\":2147483647"}, {"\"sec\":1800000000", "\"sec\":0"}, {"\"sec\":1800000000", "\"sec\":9007199254740991"}, {"\"usec\":123456", "\"usec\":0"}, {"\"usec\":123456", "\"usec\":999999"}} {
		if _, err := decodeIdentifyResponse(bytes.Replace(good, []byte(x.old), []byte(x.new), 1)); err != nil {
			t.Fatalf("RESPONSE_BOUNDARY: %v", err)
		}
	}
}
