package sessionhost

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	managerschema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
)

func memberOperationFixture(t *testing.T, kind string) (*continuingHostOwner, *memberRecordSource, map[string]any) {
	t.Helper()
	o, s, _ := recordSourceFixture(t)
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	record, _ := recordFixture(t)
	record["clone"] = workspace
	b, _ := json.Marshal(record)
	recordWrite(t, s.path, "member-1.json", b)
	vectors, err := managerschema.Documents.ReadFile("vectors/" + kind + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		Document map[string]any `json:"document"`
	}
	if json.Unmarshal(vectors, &rows) != nil || len(rows) == 0 {
		t.Fatal("published baseline fixture unavailable")
	}
	q := rows[0].Document
	q["profile"] = map[string]any{"name": "swarm-orch", "lock_sha256": strings.Repeat("a", 64), "fragment_digest": "sha256:" + strings.Repeat("b", 64)}
	if kind == "launch" {
		q["cwd"] = workspace
	} else {
		q["scope"].(map[string]any)["cwd"] = workspace
	}
	input := record["notification"].(map[string]any)
	input["contract"] = NotificationContract
	input["backend"] = "host"
	input["workflow"] = map[string]any{"reference": "workflow-1", "sha256": strings.Repeat("c", 64)}
	q["notification"] = input
	return o, s, q
}
func memberOperationBytes(t *testing.T, q map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Actual configured record and workspace agree with complete published baseline
// launch/resume controls. Plain selection preserves null admission_key, while an
// explicit station key is checked, never inferred from the selected member.
func TestMemberOperationPositive(t *testing.T) {
	for _, kind := range []string{"launch", "resume"} {
		for _, station := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "-plain", true: "-station"}[station], func(t *testing.T) {
				o, s, q := memberOperationFixture(t, kind)
				if station {
					q["admission_key"] = "werkstatt:member-1"
				}
				b := memberOperationBytes(t, q)
				op, err := s.retainOperation(kind, b)
				if err != nil {
					t.Fatal(err)
				}
				defer op.close()
				if err := op.recheck(o, s, kind, b); err != nil {
					t.Fatal(err)
				}
				if (op.request.AdmissionKey != nil) != station {
					t.Fatal("member selection changed station admission")
				}
				if _, ok := any(op).(notificationManagerCustody); ok {
					t.Fatal("operation correlation granted complete custody")
				}
				recordNoAttachment(t, o)
			})
		}
	}
}

// Every changed expected selector or profile/workspace is rejected against the
// independent actual source before any attachment. The valid controls above use
// the same baseline schema. Workflow origin itself is intentionally not proven.
func TestMemberOperationRefusals(t *testing.T) {
	changes := map[string]func(*testing.T, map[string]any){
		"runtime": func(t *testing.T, q map[string]any) { q["runtime"] = "claude_code" },
		"profile": func(t *testing.T, q map[string]any) { q["profile"].(map[string]any)["name"] = "other-profile" },
		"lock": func(t *testing.T, q map[string]any) {
			q["profile"].(map[string]any)["lock_sha256"] = strings.Repeat("d", 64)
		},
		"workspace":   func(t *testing.T, q map[string]any) { q["cwd"] = t.TempDir() },
		"station-key": func(t *testing.T, q map[string]any) { q["admission_key"] = "werkstatt:other-member" },
		"missing":     func(t *testing.T, q map[string]any) { delete(q, "notification") },
		"null":        func(t *testing.T, q map[string]any) { q["notification"] = nil },
		"legacy-doorbell": func(t *testing.T, q map[string]any) {
			q["doorbell"] = map[string]any{"identity": "fixture.orch", "heartbeat": "30s", "max_wakes_per_hour": 1}
		},
		"reference": func(t *testing.T, q map[string]any) {
			q["notification"].(map[string]any)["admission_reference"] = "not-this-reference"
		},
		"timeout": func(t *testing.T, q map[string]any) { q["notification"].(map[string]any)["call_timeout_ms"] = 999 },
		"member": func(t *testing.T, q map[string]any) {
			q["notification"].(map[string]any)["member"].(map[string]any)["mxid"] = "@fixture.orch:other.test"
		},
		"agent": func(t *testing.T, q map[string]any) {
			q["notification"].(map[string]any)["member"].(map[string]any)["agent_selector"] = "other.orch"
		},
		"connection": func(t *testing.T, q map[string]any) {
			q["notification"].(map[string]any)["member"].(map[string]any)["connection"] = "other-gr"
		},
		"root": func(t *testing.T, q map[string]any) {
			q["notification"].(map[string]any)["member"].(map[string]any)["configuration_root"] = "/other/source"
		},
		"unknown":         func(t *testing.T, q map[string]any) { q["notification"].(map[string]any)["extra"] = true },
		"missing-timeout": func(t *testing.T, q map[string]any) { delete(q["notification"].(map[string]any), "call_timeout_ms") },
		"wrong-member-case": func(t *testing.T, q map[string]any) {
			m := q["notification"].(map[string]any)["member"].(map[string]any)
			m["MXID"] = m["mxid"]
			delete(m, "mxid")
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			o, s, q := memberOperationFixture(t, "launch")
			change(t, q)
			op, err := s.retainOperation("launch", memberOperationBytes(t, q))
			if op != nil || !errors.Is(err, errMemberRecordUnavailable) {
				if op != nil {
					op.close()
				}
				t.Fatal("mismatched operation admitted", err)
			}
			recordNoAttachment(t, o)
		})
	}
}

// Rechecks bind the exact bytes/kind and original physical workspace. Foreign
// queries do not destroy a valid owner; actual directory replacement is terminal.
func TestMemberOperationCurrent(t *testing.T) {
	o, s, q := memberOperationFixture(t, "launch")
	b := memberOperationBytes(t, q)
	op, err := s.retainOperation("launch", b)
	if err != nil {
		t.Fatal(err)
	}
	defer op.close()
	for _, changed := range [][]byte{append(append([]byte(nil), b...), ' '), []byte(strings.Replace(string(b), "workflow-1", "workflow-2", 1))} {
		if err := op.recheck(o, s, "launch", changed); !errors.Is(err, errMemberRecordUnavailable) {
			t.Fatal("operation bytes substituted", err)
		}
	}
	if err := op.recheck(o, s, "resume", b); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("operation kind substituted", err)
	}
	if err := op.recheck(o, s, "launch", b); err != nil {
		t.Fatal("foreign query damaged operation", err)
	}
	workspace := q["cwd"].(string)
	old := workspace + "-original"
	if err := os.Rename(workspace, old); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(old) })
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	if err := op.recheck(o, s, "launch", b); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("substituted workspace admitted", err)
	}
	recordNoAttachment(t, o)
}

// Actual owner cancellation closes both retained record and workspace handles.
func TestMemberOperationOwnerClose(t *testing.T) {
	o, s, q := memberOperationFixture(t, "resume")
	b := memberOperationBytes(t, q)
	op, err := s.retainOperation("resume", b)
	if err != nil {
		t.Fatal(err)
	}
	defer op.close()
	wait, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := o.close(wait); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.done:
	case <-wait.Done():
		t.Fatal("source cleanup did not join")
	}
	if _, err := op.workspace.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("workspace handle leaked", err)
	}
	if err := op.recheck(o, s, "resume", b); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("dead operation admitted", err)
	}
}

// Strict checks apply before separating the optional object from the original
// schema; escaped duplicate keys and missing workflow members cannot survive.
func TestMemberOperationStrictInput(t *testing.T) {
	_, _, q := memberOperationFixture(t, "launch")
	b := memberOperationBytes(t, q)
	if _, err := parseMemberOperation("launch", b); err != nil {
		t.Fatal("positive input refused", err)
	}
	for name, changed := range map[string][]byte{
		"escaped-duplicate": []byte(strings.Replace(string(b), `"backend":"host"`, `"backend":"host","back\u0065nd":"host"`, 1)),
		"unknown-workflow":  []byte(strings.Replace(string(b), `"reference":"workflow-1"`, `"other":"value","reference":"workflow-1"`, 1)),
		"null-workflow":     []byte(strings.Replace(string(b), `"reference":"workflow-1"`, `"reference":null`, 1)),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseMemberOperation("launch", changed); !errors.Is(err, errMemberRecordUnavailable) {
				t.Fatal("invalid input admitted", err)
			}
		})
	}
}

// Existing Launcher semantics compare physical workspaces. A legitimate
// symlink spelling remains valid; a later target substitution is refused.
func TestMemberOperationWorkspaceAlias(t *testing.T) {
	o, s, q := memberOperationFixture(t, "launch")
	actual := q["cwd"].(string)
	alias := actual + "-alias"
	if err := os.Symlink(actual, alias); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(alias) })
	q["cwd"] = alias
	b := memberOperationBytes(t, q)
	op, err := s.retainOperation("launch", b)
	if err != nil {
		t.Fatal("legitimate workspace alias refused", err)
	}
	defer op.close()
	if err := op.recheck(o, s, "launch", b); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), alias); err != nil {
		t.Fatal(err)
	}
	if err := op.recheck(o, s, "launch", b); !errors.Is(err, errMemberRecordUnavailable) {
		t.Fatal("changed workspace alias admitted", err)
	}
	recordNoAttachment(t, o)
}
