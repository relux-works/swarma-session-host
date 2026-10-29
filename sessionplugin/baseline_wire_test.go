package sessionplugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

type baselineWireExchange struct {
	Name      string `json:"name"`
	Operation string `json:"operation"`
	Request   string `json:"request"`
	Schema    string `json:"schema,omitempty"`
	Response  string `json:"response"`
	Exit      int    `json:"exit"`
}

func baselineWireGolden(t *testing.T, name string, value any) {
	t.Helper()
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	path := filepath.Join("testdata", "baseline", name+".json")
	if os.Getenv("TASK_BOARD_UPDATE_BASELINE") == "1" {
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, b) {
		t.Fatalf("%s drift: got\n%s", path, b)
	}
}

// Only the observed opaque handle and the exact synthetic cwd are normalized.
// Unknown occurrences fail closed instead of silently scrubbing provider data.
func baselineWireNormalize(t *testing.T, b []byte, handle string) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	var walk func(any, string)
	walk = func(v any, key string) {
		switch x := v.(type) {
		case map[string]any:
			for k, v := range x {
				walk(v, k)
			}
		case []any:
			for _, v := range x {
				walk(v, key)
			}
		case string:
			if handle != "" && strings.Contains(x, handle) && (x != handle || (key != "session" && key != "handle")) {
				t.Fatalf("unexpected handle at %s", key)
			}
			if strings.Contains(x, "/baseline/workspace") && (x != "/baseline/workspace" || key != "cwd") {
				t.Fatalf("unexpected cwd at %s", key)
			}
		}
	}
	walk(v, "")
	if handle != "" {
		b = bytes.ReplaceAll(b, []byte(`"`+handle+`"`), []byte(`"SES-<opaque>"`))
	}
	b = bytes.ReplaceAll(b, []byte(`"/baseline/workspace"`), []byte(`"<fixture-cwd>"`))
	return string(b)
}

// Pins exact JSON for every implemented engine operation, semantic replay,
// conflict/admission/cancel fences and busy/unknown refusal without host effects.
// The host is deterministic; this is not a provider/runtime qualification.
func TestBaselineWireOperations(t *testing.T) {
	e, h := testEngine(t)
	// Select the original embedding consumer explicitly. The neutral default is
	// covered independently; golden bytes and assertions remain unchanged.
	e.Manager = "task-board"
	e.Capabilities = []string{"launch-delegated/1", "admission/1", "status/1", "list/1", "labels/1", "board-goal/1", "resume/1", "attach/1", "stop/1"}
	e.Version = "baseline-v1"
	var transcript []baselineWireExchange
	handle := ""
	exchange := func(name, op string, q any, code string) any {
		t.Helper()
		b := marshal(q)
		schema, v, err := e.Handle(context.Background(), op, b)
		var reply []byte
		exit := 0
		if code != "" {
			var r Refusal
			if !errors.As(err, &r) || r.Code != code {
				t.Fatalf("%s: %v, want %s", name, err, code)
			}
			reply = append(marshal(r), '\n')
			exit = r.Exit()
		} else {
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if op == "launch" && handle == "" {
				r := decode[Record](t, v)
				handle = r.Session
				if !regexp.MustCompile(`^SES-[0-9a-f]{32}$`).MatchString(handle) {
					t.Fatalf("bad opaque handle %q", handle)
				}
			}
			if schema == "attachment" {
				reply = marshal(v)
			} else {
				reply, err = Reply(schema, v)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		transcript = append(transcript, baselineWireExchange{name, op, baselineWireNormalize(t, b, handle), schema, baselineWireNormalize(t, reply, handle), exit})
		return v
	}
	exchange("describe", "describe", map[string]any{}, "")
	q := request(1)
	q["cwd"] = "/baseline/workspace"
	q["detach"] = false
	q["admission_key"] = "werkstatt:baseline"
	first := exchange("launch", "launch", q, "")
	replay := exchange("same-body-replay", "launch", q, "")
	if !bytes.Equal(marshal(first), marshal(replay)) || h.starts != 1 {
		t.Fatal("replay changed result or started twice")
	}
	q["session_name"] = "changed"
	exchange("different-body-conflict", "launch", q, "session_operation_conflict")
	q["session_name"] = "example"
	competing := request(2)
	competing["cwd"] = "/baseline/workspace"
	competing["admission_key"] = q["admission_key"]
	exchange("active-key-refused", "launch", competing, "werkstatt_already_up")
	if h.starts != 1 {
		t.Fatal("refused launch started host")
	}
	exchange("status-session", "status", map[string]any{"session": handle}, "")
	exchange("status-operation", "status", map[string]any{"operation_id": q["operation_id"]}, "")
	exchange("list", "list", map[string]any{"labels": map[string]string{}, "limit": 1}, "")
	exchange("attach-private-result", "attach", map[string]any{"session": handle, "expected_generation": 1}, "")
	stop := map[string]any{"session": handle, "expected_generation": 1, "mode": "graceful", "precondition": "not-busy"}
	for _, state := range []string{"unknown", "busy"} {
		s := h.sessions["1"]
		s.State = state
		h.sessions["1"] = s
		before, err := os.ReadFile(filepath.Join(e.Dir, "ledger.json"))
		if err != nil {
			t.Fatal(err)
		}
		exchange("stop-"+state, "stop", stop, "werkstatt_busy")
		after, err := os.ReadFile(filepath.Join(e.Dir, "ledger.json"))
		if err != nil || !bytes.Equal(before, after) || h.sessions["1"].State != state {
			t.Fatal("busy stop had effects")
		}
	}
	s := h.sessions["1"]
	s.State = "idle"
	h.sessions["1"] = s
	exchange("stop-idle", "stop", stop, "")
	r := resumeRequest(q, handle, 3)
	r["detach"] = false
	exchange("resume", "resume", r, "")
	exchange("resume-replay", "resume", r, "")
	if h.starts != 2 {
		t.Fatal("resume replay started twice")
	}
	exchange("stale-generation", "stop", stop, "session_generation_mismatch")
	exchange("cancel-started", "cancel", map[string]any{"operation_id": r["operation_id"]}, "")
	fence := request(4)
	fence["cwd"] = "/baseline/workspace"
	exchange("status-missing", "status", map[string]any{"operation_id": fence["operation_id"]}, "")
	exchange("cancel-missing", "cancel", map[string]any{"operation_id": fence["operation_id"]}, "")
	exchange("late-delivery", "launch", fence, "session_host_unavailable")
	if h.starts != 2 {
		t.Fatal("fenced delivery started host")
	}
	baselineWireGolden(t, "operations", transcript)
}

// Invalid and unsupported requests pin the emitted refusal/exit and leave no
// ledger or host start. Valid describe is a nearby positive control.
func TestBaselineWireRefusals(t *testing.T) {
	e, h := testEngine(t)
	var rows []baselineWireExchange
	cases := []struct {
		name, op string
		q        any
		code     string
	}{
		{"unknown-operation", "not-an-operation", map[string]any{}, "launch_plan_invalid"},
		{"invalid-describe", "describe", map[string]any{"extra": true}, "launch_plan_invalid"},
		{"missing-launch", "launch", map[string]any{}, "launch_plan_invalid"},
		{"invalid-status", "status", map[string]any{}, "launch_plan_invalid"},
	}
	for _, tc := range []struct {
		name, key string
		value     any
		code      string
	}{
		{"provider", "runtime", "muse", "session_host_provider_unsupported"},
		{"doorbell", "doorbell", map[string]any{"identity": "example", "heartbeat": "30s", "max_wakes_per_hour": 1}, "session_host_capability_missing"},
		{"secret", "child_env", map[string]string{"API_TOKEN": "fixture-placeholder"}, "secret_policy_violation"},
		{"selector-tail", "native_tail", []string{"--resume", "example"}, "launch_plan_invalid"},
	} {
		q := request(1)
		q["cwd"] = "/baseline/workspace"
		q[tc.key] = tc.value
		cases = append(cases, struct {
			name, op string
			q        any
			code     string
		}{tc.name, "launch", q, tc.code})
	}
	for _, tc := range cases {
		b := marshal(tc.q)
		_, _, err := e.Handle(context.Background(), tc.op, b)
		var r Refusal
		if !errors.As(err, &r) || r.Code != tc.code {
			t.Fatalf("%s: %v, want %s", tc.name, err, tc.code)
		}
		rows = append(rows, baselineWireExchange{Name: tc.name, Operation: tc.op, Request: baselineWireNormalize(t, b, ""), Response: string(append(marshal(r), '\n')), Exit: r.Exit()})
	}
	// Construction cannot reflect unknown codes, launcher-local codes or free
	// detail values; the valid control retains its allowed capability detail.
	for _, tc := range []struct {
		name, code string
		details    map[string]string
		want       string
	}{
		{"valid-detail", "session_host_capability_missing", map[string]string{"capability": "weights-pin"}, "session_host_capability_missing"},
		{"unknown-code", "unknown-fixture-code", nil, "session_host_unavailable"},
		{"launcher-local-code", "session_manager_registry_invalid", nil, "session_host_unavailable"},
		{"free-detail", "werkstatt_busy", map[string]string{"state": "PRIVATE-FIXTURE-CANARY"}, "session_host_unavailable"},
	} {
		r := Fail(tc.code, tc.details)
		if r.Code != tc.want {
			t.Fatalf("%s: %s", tc.name, r.Code)
		}
		b := marshal(r)
		if bytes.Contains(b, []byte("PRIVATE-FIXTURE-CANARY")) {
			t.Fatal("refusal reflected free detail")
		}
		rows = append(rows, baselineWireExchange{Name: tc.name, Operation: "refusal-constructor", Request: string(marshal(map[string]any{"code": tc.code, "details": tc.details})), Response: string(append(b, '\n')), Exit: r.Exit()})
	}
	if h.starts != 0 {
		t.Fatal("refusal started provider")
	}
	if _, err := os.Stat(e.Dir); !os.IsNotExist(err) {
		t.Fatalf("refusal created ledger directory: %v", err)
	}
	call(t, e, "describe", map[string]any{})
	baselineWireGolden(t, "refusals", rows)
}
