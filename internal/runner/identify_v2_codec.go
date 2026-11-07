package runner

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

const identifyRequestSchemaV2 = "runner-identify-request-v2"
const identifyResponseSchemaV2 = "runner-identify-response-v2"

// LinuxPIDFSIdentity is an incarnation, never a timestamp or an image proof.
// In-memory only: never persist or replay across runner/verifier restarts.
// Boot ID cannot detect VM snapshot restore or cloning; operators must restart
// both endpoints after either event. No VM generation attestation is implied.
type LinuxPIDFSIdentity struct {
	Kind               string `json:"kind"`
	BootID             string `json:"boot_id"`
	PIDFSInode         string `json:"pidfs_inode"`
	PIDNamespaceInode  string `json:"pid_namespace_inode"`
	UserNamespaceInode string `json:"user_namespace_inode"`
}
type IdentifyV2Request struct {
	Schema           string `json:"schema"`
	Execution        string `json:"execution"`
	ExpectedInstance string `json:"expected_instance"`
}
type IdentifyV2Child struct {
	PID      int                `json:"pid"`
	Identity LinuxPIDFSIdentity `json:"identity"`
}
type IdentifyV2Response struct {
	Schema    string           `json:"schema"`
	Execution string           `json:"execution"`
	Instance  *string          `json:"instance"`
	Result    string           `json:"result"`
	Reason    *string          `json:"reason"`
	Child     *IdentifyV2Child `json:"child"`
}

func canonicalInode(s string) bool {
	if len(s) == 0 || len(s) > 20 || s[0] == '0' {
		return false
	}
	for _, b := range []byte(s) {
		if b < '0' || b > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(s, 10, 64)
	return err == nil
}
func canonicalBootID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, b := range []byte(s) {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if b != '-' {
				return false
			}
			continue
		}
		if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return false
		}
	}
	return s != "00000000-0000-0000-0000-000000000000"
}
func validLinuxIdentity(v LinuxPIDFSIdentity) bool {
	return v.Kind == "linux-pidfs-v1" && canonicalBootID(v.BootID) && canonicalInode(v.PIDFSInode) && canonicalInode(v.PIDNamespaceInode) && canonicalInode(v.UserNamespaceInode)
}
func decodeIdentifyV2Request(data []byte) (IdentifyV2Request, error) {
	// Identical bounded/closed request structure, with an explicit schema selector.
	m, err := identifyTree(data)
	if err != nil || !identifyMembers(m, "schema", "execution", "expected_instance") {
		return IdentifyV2Request{}, errIdentifyFrame
	}
	s, ok := identifyText(m["schema"])
	if !ok || s != identifyRequestSchemaV2 {
		return IdentifyV2Request{}, errIdentifyFrame
	}
	e, ok := identifyText(m["execution"])
	if !ok || !identifyExecution(e) {
		return IdentifyV2Request{}, errIdentifyFrame
	}
	i, ok := identifyText(m["expected_instance"])
	if !ok || !identifyInstance(i) {
		return IdentifyV2Request{}, errIdentifyFrame
	}
	return IdentifyV2Request{s, e, i}, nil
}
func decodeIdentifyV2Response(data []byte) (IdentifyV2Response, error) {
	m, err := identifyTree(data)
	if err != nil || !identifyMembers(m, "schema", "execution", "instance", "result", "reason", "child") {
		return IdentifyV2Response{}, errIdentifyFrame
	}
	schema, ok := identifyText(m["schema"])
	if !ok || schema != identifyResponseSchemaV2 {
		return IdentifyV2Response{}, errIdentifyFrame
	}
	var child *IdentifyV2Child
	if m["result"] == "verified" {
		c, ok := m["child"].(map[string]any)
		if !ok || !identifyMembers(c, "pid", "identity") {
			return IdentifyV2Response{}, errIdentifyFrame
		}
		pid, ok := identifyInteger(c["pid"], 1, 2147483647)
		if !ok {
			return IdentifyV2Response{}, errIdentifyFrame
		}
		id, ok := c["identity"].(map[string]any)
		if !ok || !identifyMembers(id, "kind", "boot_id", "pidfs_inode", "pid_namespace_inode", "user_namespace_inode") {
			return IdentifyV2Response{}, errIdentifyFrame
		}
		v := LinuxPIDFSIdentity{}
		targets := map[string]*string{"kind": &v.Kind, "boot_id": &v.BootID, "pidfs_inode": &v.PIDFSInode, "pid_namespace_inode": &v.PIDNamespaceInode, "user_namespace_inode": &v.UserNamespaceInode}
		for k, p := range targets {
			s, ok := identifyText(id[k])
			if !ok {
				return IdentifyV2Response{}, errIdentifyFrame
			}
			*p = s
		}
		if !validLinuxIdentity(v) {
			return IdentifyV2Response{}, errIdentifyFrame
		}
		child = &IdentifyV2Child{int(pid), v}

	}
	e, ok := identifyText(m["execution"])
	if !ok || !identifyExecution(e) {
		return IdentifyV2Response{}, errIdentifyFrame
	}
	result, ok := identifyText(m["result"])
	if !ok {
		return IdentifyV2Response{}, errIdentifyFrame
	}
	r := IdentifyV2Response{Schema: schema, Execution: e, Result: result, Child: child}
	switch result {
	case "verified":
		i, ok := identifyText(m["instance"])
		if !ok || !identifyInstance(i) || m["reason"] != nil {
			return IdentifyV2Response{}, errIdentifyFrame
		}
		r.Instance = &i
	case "conflict", "unavailable":
		reason, ok := identifyText(m["reason"])
		if !ok || m["instance"] != nil || m["child"] != nil {
			return IdentifyV2Response{}, errIdentifyFrame
		}
		allowed := "|birth_unavailable|child_reaped|wait_unknown|runner_closed|busy|unsupported_platform|"
		if result == "conflict" {
			allowed = "|execution_mismatch|instance_mismatch|"
		}
		if reason == "" || strings.Contains(reason, "|") || !strings.Contains(allowed, "|"+reason+"|") {
			return IdentifyV2Response{}, errIdentifyFrame
		}
		r.Reason = &reason
	default:
		return IdentifyV2Response{}, errIdentifyFrame
	}
	return r, nil
}

// V2 has its own compact writer: HTML escaping could otherwise expand an
// accepted 128-byte execution beyond the frame budget. The v1 writer is intact.
func encodeIdentifyV2(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil || b.Len() > identifyMaxFrame {
		return nil, errIdentifyFrame
	}
	return b.Bytes(), nil
}
