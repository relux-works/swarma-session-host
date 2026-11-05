package sessionhost

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"
)

var errProtocolChildUnsupported = errors.New("protocol_child_unsupported")

// protocolChildInvocation is command data, never ordinary operation admission.
// The owning handler must separately retain the operation, admit its executable,
// claim it once before execution, and correlate the actual runner/native driver.
// No native environment, executable override or launch callback comes from JSON.
type protocolChildInvocation struct {
	args      []string
	workspace string
	operation string
	digest    [32]byte
}

type protocolChildMemberSelectors struct{ agent, root string }

// childInvocation derives identity only from this operation's retained record.
// This is the record-origin conjunct, not a substitute for actual runtime
// admission or the future pre-execution manager owner and once-claim.
func (o *retainedMemberOperation) childInvocation(owner *continuingHostOwner, source *memberRecordSource, kind string, data []byte) (protocolChildInvocation, error) {
	if o.recheck(owner, source, kind, data) != nil || o.record.record.Notification == nil {
		return protocolChildInvocation{}, errMemberRecordUnavailable
	}
	n := o.record.record.Notification
	if o.record.record.Identity != n.Member.Agent || o.request.Notification.memberNotificationInput != *n {
		return protocolChildInvocation{}, errMemberRecordUnavailable
	}
	result, err := compileProtocolChildInvocation(kind, data, protocolChildMemberSelectors{o.record.record.Identity, n.Member.ConfigurationRoot})
	if err != nil {
		return protocolChildInvocation{}, err
	}
	if o.recheck(owner, source, kind, data) != nil {
		return protocolChildInvocation{}, errMemberRecordUnavailable
	}
	return result, nil
}

// compileProtocolChildInvocation consumes the complete validated launch request.
// The initial backend has no representation for the nonbaseline semantics below,
// so they refuse instead of disappearing during construction. Null model/effort
// remain null; their actual default/locked semantics belong to the Launcher.
func compileProtocolChildInvocation(kind string, data []byte, member protocolChildMemberSelectors) (protocolChildInvocation, error) {
	q, err := parseMemberOperation(kind, data)
	if err != nil {
		return protocolChildInvocation{}, err
	}
	if kind != "launch" || q.Runtime != "codex_cli" {
		return protocolChildInvocation{}, errProtocolChildUnsupported
	}
	// Explicit tags preserve the published snake_case names; anonymous Go field
	// matching must not silently turn a supplied field into its zero value.
	var fields struct {
		Model            json.RawMessage            `json:"model"`
		Effort           json.RawMessage            `json:"effort"`
		ServiceTier      json.RawMessage            `json:"service_tier"`
		Weights          json.RawMessage            `json:"weights_id"`
		CredentialSource json.RawMessage            `json:"credential_source"`
		SystemPrompt     json.RawMessage            `json:"system_prompt"`
		ChildEnv         map[string]json.RawMessage `json:"child_env"`
		NativeTail       []string                   `json:"native_tail"`
		Permissions      struct{ Mode string }      `json:"permissions"`
		Sandbox          struct {
			Mode          string
			WritableRoots []string `json:"writable_roots"`
		} `json:"sandbox"`
		HarnessMemory bool   `json:"harness_memory"`
		BoardGoal     string `json:"board_goal"`
		Detach        bool   `json:"detach"`
	}
	if json.Unmarshal(data, &fields) != nil {
		return protocolChildInvocation{}, errMemberRecordUnavailable
	}
	if !protocolChildNull(fields.ServiceTier) || !protocolChildNull(fields.Weights) ||
		!protocolChildNull(fields.CredentialSource) || !protocolChildNull(fields.SystemPrompt) ||
		len(fields.ChildEnv) != 0 || len(fields.NativeTail) != 0 ||
		fields.Sandbox.Mode != "none" || len(fields.Sandbox.WritableRoots) != 0 ||
		fields.HarnessMemory || fields.BoardGoal != "none" || fields.Detach {
		return protocolChildInvocation{}, errProtocolChildUnsupported
	}
	model, err := protocolChildNullable(fields.Model)
	if err != nil {
		return protocolChildInvocation{}, err
	}
	effort, err := protocolChildNullable(fields.Effort)
	if err != nil {
		return protocolChildInvocation{}, err
	}
	if fields.Permissions.Mode != "native" && fields.Permissions.Mode != "yolo" {
		return protocolChildInvocation{}, errProtocolChildUnsupported
	}
	agent, root := "null", "null"
	if member.agent != "" || member.root != "" {
		if !memberAgent.MatchString(member.agent) || !filepath.IsAbs(member.root) ||
			filepath.Clean(member.root) != member.root || !protocolChildValue(member.root) {
			return protocolChildInvocation{}, errProtocolChildUnsupported
		}
		a, e1 := json.Marshal(member.agent)
		r, e2 := json.Marshal(member.root)
		if e1 != nil || e2 != nil {
			return protocolChildInvocation{}, errProtocolChildUnsupported
		}
		agent, root = string(a), string(r)
	}
	// The wire validator already requires sha256:<lowercase hex>. Do not accept
	// a second digest spelling or normalize a malformed value into a valid one.
	if !strings.HasPrefix(q.Profile.FragmentDigest, "sha256:") ||
		!notificationSHA256(strings.TrimPrefix(q.Profile.FragmentDigest, "sha256:")) ||
		!notificationSHA256(q.Profile.Lock) || !filepath.IsAbs(q.Cwd) || filepath.Clean(q.Cwd) != q.Cwd {
		return protocolChildInvocation{}, errProtocolChildUnsupported
	}
	values := []string{"codex", "app-server", q.Profile.Name, q.Profile.Lock,
		strings.TrimPrefix(q.Profile.FragmentDigest, "sha256:"), q.Cwd, model, effort,
		fields.Permissions.Mode, "null", "null", "null", "{}", "[]", q.Operation, "create", agent, root}
	names := []string{"runtime", "kind", "profile", "expected-lock-sha256", "expected-fragment-sha256",
		"workspace", "model-json", "effort-json", "permissions", "service-tier-json", "weights-json",
		"credential-source-json", "child-env-json", "native-tail-json", "operation-id", "lifecycle",
		"member-a2a-agent-json", "member-a2a-config-dir-json"}
	args := []string{"protocol-child-v1"}
	total := len(args[0])
	for i, value := range values {
		if !protocolChildValue(value) {
			return protocolChildInvocation{}, errProtocolChildUnsupported
		}
		arg := "--" + names[i] + "=" + value
		total += len(arg)
		if total > 16384 {
			return protocolChildInvocation{}, errProtocolChildUnsupported
		}
		args = append(args, arg)
	}
	return protocolChildInvocation{args: args, workspace: q.Cwd, operation: q.Operation, digest: sha256.Sum256(data)}, nil
}

func protocolChildNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func protocolChildNullable(raw json.RawMessage) (string, error) {
	if protocolChildNull(raw) {
		return "null", nil
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || !protocolChildValue(value) {
		return "", errProtocolChildUnsupported
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", errProtocolChildUnsupported
	}
	return string(encoded), nil
}

func protocolChildValue(value string) bool {
	return len(value) <= 4096 && utf8.ValidString(value) && strings.IndexFunc(value, unicode.IsControl) < 0
}
