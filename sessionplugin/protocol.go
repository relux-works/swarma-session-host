// Package sessionplugin implements the Curator session-manager engine and wire boundary.
// The pinned launcher's exported schemas are the protocol authority.
package sessionplugin

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"

	schema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
	"github.com/relux-works/skill-agents-management/pkg/agentic"
	_ "github.com/relux-works/skill-agents-management/pkg/agentic/systems/claude"
	_ "github.com/relux-works/skill-agents-management/pkg/agentic/systems/codex"
	_ "github.com/relux-works/skill-agents-management/pkg/agentic/systems/muse"
)

const Protocol = "curator-session-manager/1"
const Limit = 64 << 10

type Profile struct {
	Name   string `json:"name"`
	Lock   string `json:"lock_sha256"`
	Digest string `json:"fragment_digest"`
}
type Request struct {
	OperationID  string            `json:"operation_id"`
	SubmittedAt  string            `json:"submitted_at"`
	AdmissionKey *string           `json:"admission_key"`
	Runtime      string            `json:"runtime"`
	Profile      Profile           `json:"profile"`
	CWD          string            `json:"cwd"`
	Labels       map[string]string `json:"labels"`
	Model        *string           `json:"model"`
	Effort       *string           `json:"effort"`
	Tier         *string           `json:"service_tier"`
	Weights      *string           `json:"weights_id"`
	Permissions  struct {
		Mode   string `json:"mode"`
		Source string `json:"source"`
	} `json:"permissions"`
	SystemPrompt json.RawMessage `json:"system_prompt"`
	Sandbox      struct {
		Mode  string   `json:"mode"`
		Roots []string `json:"writable_roots"`
	} `json:"sandbox"`
	Memory     bool              `json:"harness_memory"`
	Name       string            `json:"session_name"`
	ChildEnv   map[string]string `json:"child_env"`
	Doorbell   json.RawMessage   `json:"doorbell"`
	Credential json.RawMessage   `json:"credential_source"`
	NativeTail []string          `json:"native_tail"`
	Detach     bool              `json:"detach"`
	Selector   struct {
		Kind   string `json:"kind"`
		Handle string `json:"handle"`
		ID     string `json:"id"`
	} `json:"selector"`
	Scope struct {
		CWD    string            `json:"cwd"`
		Labels map[string]string `json:"labels"`
	} `json:"scope"`
	Session            string `json:"session"`
	ExpectedGeneration int64  `json:"expected_generation"`
	Mode               string `json:"mode"`
	Precondition       string `json:"precondition"`
	Cursor             string `json:"cursor"`
	Limit              int    `json:"limit"`
}

type Refusal struct {
	Protocol string            `json:"protocol"`
	OK       bool              `json:"ok"`
	Code     string            `json:"code"`
	Details  map[string]string `json:"details"`
}

func Fail(code string, details ...map[string]string) Refusal {
	d := map[string]string{}
	if len(details) > 0 && details[0] != nil {
		d = details[0]
	}
	r := Refusal{Protocol: Protocol, Code: code, Details: d}
	b, _ := json.Marshal(r)
	if schema.Validate("refusal.schema.json", b) != nil {
		return Refusal{Protocol: Protocol, Code: "session_host_unavailable", Details: map[string]string{}}
	}
	return r
}
func (r Refusal) Error() string { return r.Code }
func (r Refusal) Exit() int {
	if r.Code == "policy_refused" || r.Code == "secret_policy_violation" {
		return 16
	}
	if r.Code == "launch_plan_invalid" || strings.HasPrefix(r.Code, "session_host_capability_") || r.Code == "session_host_provider_unsupported" {
		return 6
	}
	return 1
}
func Envelope(v map[string]any) map[string]any { v["protocol"] = Protocol; v["ok"] = true; return v }
func Read(r io.Reader) ([]byte, error) {
	b, e := io.ReadAll(io.LimitReader(r, Limit+1))
	if e != nil || len(b) > Limit {
		return nil, Fail("launch_plan_invalid")
	}
	return b, nil
}
func Validate(op string, b []byte) (Request, error) {
	name := op + "-request.schema.json"
	if op == "launch" || op == "resume" {
		name = op + ".schema.json"
	}
	if schema.Validate(name, b) != nil {
		if op == "launch" {
			var env struct {
				ChildEnv map[string]json.RawMessage `json:"child_env"`
			}
			if json.Unmarshal(b, &env) == nil {
				for k := range env.ChildEnv {
					if schema.ReservedChildName(k) {
						return Request{}, Fail("secret_policy_violation")
					}
				}
			}
			var doc map[string]any
			if json.Unmarshal(b, &doc) == nil {
				if v, ok := doc["child_env"]; ok {
					x, _ := json.Marshal(v)
					if schema.Validate("child-env.schema.json", x) != nil {
						return Request{}, Fail("secret_policy_violation")
					}
				}
			}
		}
		return Request{}, Fail("launch_plan_invalid")
	}
	var q Request
	if json.Unmarshal(b, &q) != nil {
		return q, Fail("launch_plan_invalid")
	}
	// Provider semantics deliberately live outside the standalone wire schema.
	// Elevation must neither select a session nor consume any native arguments.
	if op == "launch" {
		system := map[string]agentic.SystemID{"codex_cli": "codex", "claude_code": "claude-code", "muse": "muse"}[q.Runtime]
		elevated, err := agentic.ElevateResumeIntent(agentic.Default, system, nil, q.NativeTail)
		if err != nil || elevated.Intent.Kind != agentic.ResumeNew || !slices.Equal(elevated.NativeArgs, q.NativeTail) {
			return Request{}, Fail("launch_plan_invalid")
		}
	}
	return q, nil
}
func Reply(name string, v any) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil || len(b)+1 > Limit || schema.Validate(name+".schema.json", b) != nil {
		return nil, Fail("session_host_unavailable")
	}
	return append(b, '\n'), nil
}

// Digest uses CCJ-1 after closed-schema validation has rejected duplicate keys,
// unsafe integers, malformed UTF-8 and alternate timestamp spellings.
func Digest(b []byte) (string, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	if e := d.Decode(&v); e != nil {
		return "", e
	}
	var out strings.Builder
	if e := canonical(&out, v); e != nil {
		return "", e
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(out.String()))), nil
}
func canonical(out *strings.Builder, v any) error {
	switch x := v.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		out.WriteString(strconv.FormatBool(x))
	case json.Number:
		n, e := strconv.ParseFloat(string(x), 64)
		if e != nil || n < -9007199254740991 || n > 9007199254740991 || n != float64(int64(n)) {
			return fmt.Errorf("unsafe integer")
		}
		out.WriteString(strconv.FormatInt(int64(n), 10))
	case string:
		out.WriteByte('"')
		for _, r := range x {
			switch r {
			case '"':
				out.WriteString(`\"`)
			case '\\':
				out.WriteString(`\\`)
			case '\b':
				out.WriteString(`\b`)
			case '\f':
				out.WriteString(`\f`)
			case '\n':
				out.WriteString(`\n`)
			case '\r':
				out.WriteString(`\r`)
			case '\t':
				out.WriteString(`\t`)
			default:
				if r < 32 {
					fmt.Fprintf(out, `\u%04x`, r)
				} else {
					out.WriteRune(r)
				}
			}
		}
		out.WriteByte('"')
	case []any:
		out.WriteByte('[')
		for i, w := range x {
			if i > 0 {
				out.WriteByte(',')
			}
			if e := canonical(out, w); e != nil {
				return e
			}
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			if e := canonical(out, k); e != nil {
				return e
			}
			out.WriteByte(':')
			if e := canonical(out, x[k]); e != nil {
				return e
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("unsupported canonical value")
	}
	return nil
}
