package runner

import (
	"bytes"
	"strings"
	"testing"
)

func v2FixtureIdentity() LinuxPIDFSIdentity {
	return LinuxPIDFSIdentity{"linux-pidfs-v1", "12345678-1234-1234-1234-123456789abc", "18446744073709551615", "4026531836", "4026531837"}
}
func v2FixtureResponse() IdentifyV2Response {
	i := identifyTestInstance
	return IdentifyV2Response{Schema: identifyResponseSchemaV2, Execution: "exec-a", Instance: &i, Result: "verified", Child: &IdentifyV2Child{123, v2FixtureIdentity()}}
}
func TestIdentifyV2ClosedCodec(t *testing.T) {
	good, _ := encodeIdentifyV2(v2FixtureResponse())
	if _, err := decodeIdentifyV2Response(good); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeIdentifyResponse(good); err == nil {
		t.Fatal("v1 accepted v2")
	}
	if _, err := decodeIdentifyV2Response(identifySuccessWire()); err == nil {
		t.Fatal("v2 accepted v1")
	}
	for _, field := range []string{"pidfs_inode", "pid_namespace_inode", "user_namespace_inode"} {
		for _, bad := range []string{`""`, `"0"`, `"01"`, `"-1"`, `"+1"`, `" 1"`, `"1.0"`, `"1e2"`, `"18446744073709551616"`, `1`, `null`} {
			r := v2FixtureResponse()
			b, _ := encodeIdentifyV2(r)
			old := `"` + field + `":"`
			pos := bytes.Index(b, []byte(old))
			end := pos + len(old) + bytes.IndexByte(b[pos+len(old):], '"') + 1
			b = append(append(bytes.Clone(b[:pos]), []byte(`"`+field+`":`+bad)...), b[end:]...)
			if _, err := decodeIdentifyV2Response(b); err == nil {
				t.Fatalf("accepted %s=%s", field, bad)
			}
		}
	}
	for _, pair := range [][2]string{
		{`"kind":"linux-pidfs-v1"`, `"kind":"linux-pidfd-v1"`},
		{`"kind":"linux-pidfs-v1"`, `"kind":"linux-pidfs-v1","kind":"linux-pidfs-v1"`},
		{`"kind":"linux-pidfs-v1"`, `"kind":"linux-pidfs-v1","\u006bind":"linux-pidfs-v1"`},
		{`"kind":"linux-pidfs-v1"`, `"kind":"linux-pidfs-v1","extra":1`},
		{`"pid":123`, `"pid":123,"birth":{"sec":1,"usec":2}`},
		{`"identity":`, `"birth":`}, {`"pid":123`, `"pid":2147483648`},
		{`"reason":null`, `"reason":"busy"`}, {`"result":"verified"`, `"result":"unavailable"`},
		{`12345678-1234-1234-1234-123456789abc`, `12345678-1234-1234-1234-123456789ABC`},
		{`12345678-1234-1234-1234-123456789abc`, `00000000-0000-0000-0000-000000000000`},
	} {
		b := bytes.Replace(good, []byte(pair[0]), []byte(pair[1]), 1)
		if _, err := decodeIdentifyV2Response(b); err == nil {
			t.Fatalf("accepted %s", b)
		}
	}
	for _, reason := range []string{"birth_unavailable", "child_reaped", "wait_unknown", "runner_closed", "busy", "unsupported_platform"} {
		b, _ := encodeIdentifyV2(identifyV2Refusal(IdentifyV2Request{Execution: "exec-a"}, "unavailable", reason))
		if _, err := decodeIdentifyV2Response(b); err != nil {
			t.Fatal(reason, err)
		}
	}
	req := IdentifyV2Request{identifyRequestSchemaV2, "exec-a", identifyTestInstance}
	b, _ := encodeIdentifyV2(req)
	if _, err := decodeIdentifyV2Request(b); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{bytes.Replace(b, []byte("request-v2"), []byte("request-v1"), 1), bytes.Replace(b, []byte("exec-a"), []byte("\\ud800"), 1), bytes.Replace(b, []byte("{"), []byte(`{"execution":"exec-a",`), 1), append(bytes.Repeat([]byte(" "), 1025), b...), bytes.TrimSuffix(b, []byte("\n"))} {
		if _, err := decodeIdentifyV2Request(bad); err == nil {
			t.Fatal("accepted malformed request")
		}
	}
	boundary := append(bytes.Repeat([]byte(" "), 1024-len(b)), b...)
	if _, err := decodeIdentifyV2Request(boundary); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeIdentifyV2Response(append(bytes.TrimSuffix(good, []byte("\n")), []byte("{}\n")...)); err == nil {
		t.Fatal("trailing json")
	}
	if canonicalInode(strings.Repeat("9", 21)) {
		t.Fatal("overflow")
	}
}

func TestIdentifyV2SnapshotStatesAndLocks(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		change       func(*Server)
	}{
		{"owned", "", func(*Server) {}},
		{"reaped", "child_reaped", func(s *Server) { s.childState = childReaped }},
		{"unknown", "wait_unknown", func(s *Server) { s.childState = childWaitUnknown }},
		{"closed", "runner_closed", func(s *Server) { close(s.closed) }},
		{"unsupported", "unsupported_platform", func(s *Server) { s.linux.err = errChildBirthUnsupported }},
		{"missing", "birth_unavailable", func(s *Server) { s.linux.identity = LinuxPIDFSIdentity{} }},
		{"failed", "birth_unavailable", func(s *Server) { s.linux.err = errChildBirthUnavailable }},
		{"handle", "wait_unknown", func(s *Server) { s.child = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := identifyMemoryFixture()
			s.linux.identity = v2FixtureIdentity()
			tc.change(s)
			req := IdentifyV2Request{identifyRequestSchemaV2, s.cfg.Execution, s.instance}
			before := s.linux.identity
			s.writeMu.Lock()
			r, _ := s.identifyV2Snapshot(req)
			s.writeMu.Unlock()
			if s.linux.identity != before {
				t.Fatal("snapshot mutated identity")
			}
			if tc.reason == "" {
				if r.Result != "verified" || r.Child.Identity != before {
					t.Fatal(r)
				}
			} else if r.Reason == nil || *r.Reason != tc.reason || r.Instance != nil || r.Child != nil {
				t.Fatal(r)
			}
			req.Execution = "other"
			r, _ = s.identifyV2Snapshot(req)
			if r.Reason == nil || *r.Reason != "execution_mismatch" {
				t.Fatal(r)
			}
		})
	}
	for _, lock := range []string{"process", "mu"} {
		s := identifyMemoryFixture()
		if lock == "process" {
			s.processMu.Lock()
		} else {
			s.mu.Lock()
		}
		r, _ := s.identifyV2Snapshot(IdentifyV2Request{identifyRequestSchemaV2, s.cfg.Execution, s.instance})
		if r.Reason == nil || *r.Reason != "busy" {
			t.Fatal(r)
		}
		if lock == "process" {
			s.processMu.Unlock()
		} else {
			s.mu.Unlock()
			if !s.processMu.TryLock() {
				t.Fatal("process lock leaked")
			}
			s.processMu.Unlock()
		}
	}
}

func TestIdentifyV2ExclusiveKeysAndCanonicalNumbers(t *testing.T) {
	good, _ := encodeIdentifyV2(v2FixtureResponse())
	levels := []struct {
		needle string
		keys   []string
	}{
		{`{`, []string{"schema", "execution", "instance", "result", "reason", "child"}},
		{`"child":{`, []string{"pid", "identity"}},
		{`"identity":{`, []string{"kind", "boot_id", "pidfs_inode", "pid_namespace_inode", "user_namespace_inode"}},
	}
	for _, level := range levels {
		// Unknown and duplicate decoded members are forbidden at every object depth.
		b := bytes.Replace(good, []byte(level.needle), []byte(level.needle+`"extra":null,`), 1)
		if _, err := decodeIdentifyV2Response(b); err == nil {
			t.Fatalf("unknown key: %s", b)
		}
		for _, key := range level.keys {
			b = bytes.Replace(good, []byte(level.needle), []byte(level.needle+`"`+key+`":null,`), 1)
			if _, err := decodeIdentifyV2Response(b); err == nil {
				t.Fatalf("duplicate key: %s", b)
			}
		}
	}
	for _, bad := range []string{`0`, `-1`, `1.0`, `1e0`, `2147483648`, `"123"`, `null`} {
		b := bytes.Replace(good, []byte(`"pid":123`), []byte(`"pid":`+bad), 1)
		if _, err := decodeIdentifyV2Response(b); err == nil {
			t.Fatalf("pid lexical/range: %s", b)
		}
	}
	for _, field := range []string{"pidfs_inode", "pid_namespace_inode", "user_namespace_inode"} {
		if !canonicalInode("1") || !canonicalInode("18446744073709551615") {
			t.Fatal(field, "u64 boundary rejected")
		}
	}
	req, _ := encodeIdentifyV2(IdentifyV2Request{identifyRequestSchemaV2, "exec-a", identifyTestInstance})
	for _, key := range []string{"schema", "execution", "expected_instance"} {
		b := bytes.Replace(req, []byte(`{`), []byte(`{"`+key+`":null,`), 1)
		if _, err := decodeIdentifyV2Request(b); err == nil {
			t.Fatalf("request duplicate: %s", b)
		}
	}
	if _, err := decodeIdentifyV2Request(bytes.Replace(req, []byte(`{`), []byte(`{"extra":null,`), 1)); err == nil {
		t.Fatal("request unknown key")
	}
	for _, pair := range [][2]string{
		{`"result":"verified"`, `"result":"refused"`},
		{`"instance":"` + identifyTestInstance + `"`, `"instance":null`},
		{`"reason":null`, `"reason":"unsupported_platform"`},
		{`"child":{`, `"missing_child":{`},
		{`"pid":123,`, ``},
		{`"identity":{`, `"missing_identity":{`},
		{`"kind":"linux-pidfs-v1",`, ``},
	} {
		b := bytes.Replace(good, []byte(pair[0]), []byte(pair[1]), 1)
		if bytes.Equal(b, good) {
			t.Fatalf("bad fixture replacement %s", pair[0])
		}
		if _, err := decodeIdentifyV2Response(b); err == nil {
			t.Fatalf("invalid envelope/member: %s", b)
		}
	}
}

func TestIdentifyV2ConstructiveFrameBounds(t *testing.T) {
	for _, execution := range []string{strings.Repeat("\"", 128), strings.Repeat("\\", 128), strings.Repeat("<", 128), strings.Repeat("&", 128), strings.Repeat("é", 64), strings.Repeat("\u2028", 42) + "ab"} {
		req := IdentifyV2Request{identifyRequestSchemaV2, execution, identifyTestInstance}
		wire, err := encodeIdentifyV2(req)
		if err != nil || len(wire) > 366 {
			t.Fatal("request budget", len(wire), err)
		}
		decoded, err := decodeIdentifyV2Request(wire)
		if err != nil || decoded.Execution != execution {
			t.Fatal("execution not preserved", err)
		}
		response := v2FixtureResponse()
		response.Execution = execution
		response.Child.PID = 2147483647
		response.Child.Identity.PIDFSInode = "18446744073709551615"
		response.Child.Identity.PIDNamespaceInode = "18446744073709551615"
		response.Child.Identity.UserNamespaceInode = "18446744073709551615"
		wire, err = encodeIdentifyV2(response)
		if err != nil || len(wire) > 633 {
			t.Fatal("verified budget", len(wire), err)
		}
		if _, err = decodeIdentifyV2Response(wire); err != nil {
			t.Fatal(err)
		}
		if execution == strings.Repeat("\"", 128) && len(wire) != 633 {
			t.Fatal("verified constructive maximum", len(wire))
		}
		if execution == strings.Repeat("<", 128) && bytes.Contains(wire, []byte(`\u003c`)) {
			t.Fatal("v2 HTML escaping")
		}
		refusal := identifyV2Refusal(req, "unavailable", "unsupported_platform")
		wire, err = encodeIdentifyV2(refusal)
		if err != nil || len(wire) > 396 {
			t.Fatal("refusal budget", len(wire), err)
		}
		if _, err = decodeIdentifyV2Response(wire); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []string{"", strings.Repeat("x", 129), "x\x00", "x\x7f", "x\u0080"} {
		wire, _ := encodeIdentifyV2(IdentifyV2Request{identifyRequestSchemaV2, bad, identifyTestInstance})
		if _, err := decodeIdentifyV2Request(wire); err == nil {
			t.Fatalf("accepted execution %q", bad)
		}
	}
	oversized := v2FixtureResponse()
	oversized.Execution = strings.Repeat("x", 1024)
	if _, err := encodeIdentifyV2(oversized); err == nil {
		t.Fatal("v2 writer ignored total frame bound")
	}
	// V1's writer keeps its original HTML-escaping behavior.
	wire, _ := encodeIdentify(identifyRequest{identifyRequestSchema, "<", identifyTestInstance})
	if !bytes.Contains(wire, []byte(`\u003c`)) {
		t.Fatal("v1 writer changed")
	}
}
