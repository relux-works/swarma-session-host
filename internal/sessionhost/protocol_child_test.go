package sessionhost

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	managerschema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
)

func compileProtocolChildFixture(kind string, data []byte) (protocolChildInvocation, error) {
	return compileProtocolChildInvocation(kind, data, protocolChildMemberSelectors{})
}

func protocolChildRequestFixture(t *testing.T) map[string]any {
	t.Helper()
	b, err := managerschema.Documents.ReadFile("vectors/launch.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		Document map[string]any `json:"document"`
	}
	if json.Unmarshal(b, &rows) != nil || len(rows) == 0 {
		t.Fatal("published launch control unavailable")
	}
	q := rows[0].Document
	q["cwd"] = "/fixture/work space"
	record, _ := recordFixture(t)
	n := record["notification"].(map[string]any)
	n["contract"], n["backend"] = NotificationContract, "host"
	n["workflow"] = map[string]any{"reference": "workflow-1", "sha256": strings.Repeat("c", 64)}
	q["notification"] = n
	return q
}

// The published nullable baseline and explicit selectors map to the fixed CLI
// without resolving defaults or granting admission. Each permission provenance
// emitted by the wire contract stays valid. No process or filesystem is opened.
func TestProtocolChildInvocationPositive(t *testing.T) {
	for _, mode := range []string{"native", "yolo"} {
		for _, source := range []string{"flag", "profile", "global", "default-interactive", "default-headless"} {
			for _, explicit := range []bool{false, true} {
				t.Run(mode+"/"+source+map[bool]string{false: "/null", true: "/explicit"}[explicit], func(t *testing.T) {
					q := protocolChildRequestFixture(t)
					q["permissions"] = map[string]any{"mode": mode, "source": source}
					model, effort := "null", "null"
					if explicit {
						q["model"], q["effort"] = "gpt-6.1-sol", "high"
						model, effort = `"gpt-6.1-sol"`, `"high"`
					}
					data := memberOperationBytes(t, q)
					got, err := compileProtocolChildFixture("launch", data)
					if err != nil {
						t.Fatal(err)
					}
					want := []string{"protocol-child-v1", "--runtime=codex", "--kind=app-server", "--profile=sample",
						"--expected-lock-sha256=" + strings.Repeat("1", 64), "--expected-fragment-sha256=" + strings.Repeat("2", 64),
						"--workspace=/fixture/work space", "--model-json=" + model, "--effort-json=" + effort,
						"--permissions=" + mode, "--service-tier-json=null", "--weights-json=null", "--credential-source-json=null",
						"--child-env-json={}", "--native-tail-json=[]", "--operation-id=11111111-1111-4111-8111-111111111111", "--lifecycle=create",
						"--member-a2a-agent-json=null", "--member-a2a-config-dir-json=null"}
					if !reflect.DeepEqual(got.args, want) || len(got.args) != 19 || got.workspace != q["cwd"] ||
						got.operation != q["operation_id"] || got.digest != sha256.Sum256(data) {
						t.Fatal("complete request did not map to the fixed command")
					}
					data[0] = ' '
					if !reflect.DeepEqual(got.args, want) {
						t.Fatal("invocation retained mutable input")
					}
					if _, ok := any(got).(managerOperationAdmission); ok {
						t.Fatal("command data granted ordinary admission")
					}
					if _, ok := any(got).(notificationManagerCustody); ok {
						t.Fatal("command data granted notification custody")
					}
				})
			}
		}
	}
}

// These are valid complete wire requests whose semantics the initial child CLI
// cannot carry. Refusal is specific and returns no partial invocation; the nearby
// unmodified request still compiles. No accepted option is silently dropped.
func TestProtocolChildInvocationUnsupported(t *testing.T) {
	cases := map[string]func(map[string]any){
		"runtime": func(q map[string]any) { q["runtime"] = "claude_code" },
		"tier":    func(q map[string]any) { q["service_tier"] = "fast" },
		"weights": func(q map[string]any) { q["weights_id"] = "gguf-sha256:" + strings.Repeat("a", 64) },
		"source": func(q map[string]any) {
			q["credential_source"] = map[string]any{"source": map[string]any{"mode": "node", "source_ref": "local:" + strings.Repeat("1", 32), "harness_channel": "CLAUDE_CODE_OAUTH_TOKEN"}, "source_generation": 1, "policy_generation": 1}
		},
		"env":    func(q map[string]any) { q["child_env"] = map[string]any{"TERM": "dumb"} },
		"tail":   func(q map[string]any) { q["native_tail"] = []string{"app-server"} },
		"prompt": func(q map[string]any) { q["system_prompt"] = map[string]any{"semantics": "append"} },
		"sandbox": func(q map[string]any) {
			q["sandbox"] = map[string]any{"mode": "workspace-write", "writable_roots": []string{"/fixture/work space"}}
		},
		"memory":   func(q map[string]any) { q["harness_memory"] = true },
		"goal":     func(q map[string]any) { q["board_goal"] = "workspace" },
		"detach":   func(q map[string]any) { q["detach"] = true },
		"path":     func(q map[string]any) { q["cwd"] = "/fixture/../other" },
		"control":  func(q map[string]any) { q["cwd"] = "/fixture/line\nbreak" },
		"path-cap": func(q map[string]any) { q["cwd"] = "/" + strings.Repeat("a", 4096) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			q := protocolChildRequestFixture(t)
			if _, err := compileProtocolChildFixture("launch", memberOperationBytes(t, q)); err != nil {
				t.Fatal("nearby valid control refused", err)
			}
			change(q)
			data := memberOperationBytes(t, q)
			if err := managerschema.Validate("launch.schema.json", data); err != nil {
				t.Fatal("unsupported-semantics witness is not otherwise wire-valid", err)
			}
			got, err := compileProtocolChildFixture("launch", data)
			if !errors.Is(err, errProtocolChildUnsupported) || !reflect.DeepEqual(got, protocolChildInvocation{}) {
				t.Fatal("unsupported semantics were dropped or partially compiled", err)
			}
		})
	}
}

// Schema-invalid, ambiguous and incomplete requests never reach construction.
// A syntactically valid native-resume request is a separate unsupported case,
// not a create request with its existing session discarded.
func TestProtocolChildInvocationRequestRefusal(t *testing.T) {
	base := memberOperationBytes(t, protocolChildRequestFixture(t))
	for _, data := range [][]byte{
		append(append([]byte(nil), base...), []byte(" {}")...),
		append([]byte(`{"runtime":"codex_cli",`), base[1:]...),
		[]byte(`{"runtime":"codex_cli"}`),
	} {
		got, err := compileProtocolChildFixture("launch", data)
		if !errors.Is(err, errMemberRecordUnavailable) || !reflect.DeepEqual(got, protocolChildInvocation{}) {
			t.Fatal("invalid request produced command data", err)
		}
	}
	b, err := managerschema.Documents.ReadFile("vectors/resume.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct{ Document map[string]any }
	if json.Unmarshal(b, &rows) != nil || len(rows) == 0 {
		t.Fatal("published resume control unavailable")
	}
	q := rows[0].Document
	q["notification"] = protocolChildRequestFixture(t)["notification"]
	data := memberOperationBytes(t, q)
	if _, err := parseMemberOperation("resume", data); err != nil {
		t.Fatal("resume witness invalid", err)
	}
	got, err := compileProtocolChildFixture("resume", data)
	if !errors.Is(err, errProtocolChildUnsupported) || !reflect.DeepEqual(got, protocolChildInvocation{}) {
		t.Fatal("native resume was reinterpreted as create", err)
	}
}

// Values remain individual argv entries, including a path containing shell
// punctuation. This proves construction only, not executable/path admission.
// The finite scalar cap and decoded control/UTF-8 checks pair boundary positives
// with exact neighbors. A string spelling "null" remains distinct from null.
func TestProtocolChildInvocationFraming(t *testing.T) {
	q := protocolChildRequestFixture(t)
	q["cwd"], q["model"] = "/fixture/$(not-a-command); space", "null"
	got, err := compileProtocolChildFixture("launch", memberOperationBytes(t, q))
	if err != nil || got.args[6] != "--workspace=/fixture/$(not-a-command); space" || got.args[7] != `--model-json="null"` {
		t.Fatal("argv value was interpreted or null semantics changed", err)
	}
	for _, value := range []string{strings.Repeat("a", 4096), "ordinary space", "é"} {
		if !protocolChildValue(value) {
			t.Fatal("valid bounded value refused")
		}
	}
	for _, value := range []string{strings.Repeat("a", 4097), "line\nbreak", "nul\x00byte", string([]byte{0xff}), "del\x7fbyte"} {
		if protocolChildValue(value) {
			t.Fatal("invalid bounded value admitted")
		}
	}
	q["cwd"] = "/" + strings.Repeat("a", 4095)
	if _, err := compileProtocolChildFixture("launch", memberOperationBytes(t, q)); err != nil {
		t.Fatal("exact workspace limit refused", err)
	}
}

// Member selectors are paired opaque command data. Only the retained-record
// path below provides their record-origin conjunct; these syntactic examples
// neither inspect a real identity root nor grant launch/admission authority.
func TestProtocolChildInvocationMemberPair(t *testing.T) {
	data := memberOperationBytes(t, protocolChildRequestFixture(t))
	got, err := compileProtocolChildInvocation("launch", data, protocolChildMemberSelectors{"fixture.orch", "/fixture/member root"})
	if err != nil || len(got.args) != 19 || got.args[17] != `--member-a2a-agent-json="fixture.orch"` || got.args[18] != `--member-a2a-config-dir-json="/fixture/member root"` {
		t.Fatal("exact member pair did not survive construction", err)
	}
	for _, member := range []protocolChildMemberSelectors{
		{"fixture.orch", ""}, {"", "/fixture/member"}, {"Foreign", "/fixture/member"},
		{"fixture.orch", "relative"}, {"fixture.orch", "/fixture/../foreign"},
		{"fixture.orch", "/fixture/line\nbreak"},
	} {
		got, err := compileProtocolChildInvocation("launch", data, member)
		if !errors.Is(err, errProtocolChildUnsupported) || !reflect.DeepEqual(got, protocolChildInvocation{}) {
			t.Fatal("invalid member pair partially compiled", err)
		}
	}
}

// Actual owned fixture records supply the pair. A foreign source, copied or
// changed request, and an observed record revision cannot supply that conjunct.
// These are inert local Host/file fixtures, not actual pre-launch/runtime proof.
func TestProtocolChildRetainedMemberOrigin(t *testing.T) {
	o, source, q := memberOperationFixture(t, "launch")
	data := memberOperationBytes(t, q)
	op, err := source.retainOperation("launch", data)
	if err != nil {
		t.Fatal(err)
	}
	defer op.close()
	got, err := op.childInvocation(o, source, "launch", data)
	if err != nil || got.args[17] != `--member-a2a-agent-json="fixture.orch"` || got.args[18] != `--member-a2a-config-dir-json="/configured/source"` {
		t.Fatal("actual retained member did not supply its exact selectors", err)
	}
	if _, err := op.childInvocation(o, &memberRecordSource{owner: o}, "launch", data); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("foreign source substituted for retained origin", err)
	}
	changed := append(append([]byte(nil), data...), ' ')
	if _, err := op.childInvocation(o, source, "launch", changed); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("changed request substituted for the retained operation", err)
	}
	var absent *retainedMemberOperation
	if _, err := absent.childInvocation(o, source, "launch", data); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("request data constructed absent record origin", err)
	}
	record, _ := recordFixture(t)
	record["clone"] = q["cwd"]
	record["notification"].(map[string]any)["member"].(map[string]any)["configuration_root"] = "/configured/foreign"
	recordWrite(t, source.path, "member-1.json", memberOperationBytes(t, record))
	if _, err := op.childInvocation(o, source, "launch", data); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("changed actual member root survived retained-origin recheck", err)
	}
	recordNoAttachment(t, o)
}
