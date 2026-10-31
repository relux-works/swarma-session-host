package codexadapter

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
)

func openNoticeMode(mode string, saved []byte) (*Driver, uint64, error) {
	opts := DriverOptions{NoticeMode: mode}
	if mode == WorkReference {
		opts.PermissionPolicy = YoloPolicy
	}
	return OpenDriverWithOptions(driverConfig(), opts, saved)
}

func idleWorkDriver(t *testing.T) (*Driver, uint64) {
	t.Helper()
	d, _, err := openNoticeMode(WorkReference, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Begin(); err != nil {
		t.Fatal(err)
	}
	var offset uint64
	observe(t, d, &offset, `{"id":1,"result":{"userAgent":"synthetic","platformFamily":"unix","platformOs":"macos"}}`)
	x := observe(t, d, &offset, `{"id":2,"result":{"thread":{"id":"thread-a"},"model":"unfamiliar-model","modelProvider":"unfamiliar-provider","approvalPolicy":"never","sandbox":{"type":"dangerFullAccess"},"cwd":"/tmp/synthetic"}}`)
	if x.View.Activity != ap.Idle {
		t.Fatal("fixture did not reach idle")
	}
	return d, offset
}

// The opt-in renders only a task reference, preserves opaque selection, and
// refuses malformed references without changing metadata. Default bytes stay exact.
func TestWorkReferenceRendering(t *testing.T) {
	d, _ := idleWorkDriver(t)
	before := d.Snapshot().Checkpoint
	for _, ref := range []string{"", "raw task body", "task;command", "task\nbody", `task"suffix`} {
		x, err := d.Prepare(ap.Input{ID: "one", Reference: ref, Mode: ap.NextTurn})
		if err == nil || err.Error() != "invalid_notice_framing" || len(x.Actions) != 0 || !bytes.Equal(x.Checkpoint, before) {
			t.Fatal("malformed reference changed intent", err)
		}
	}
	x, err := d.Prepare(ap.Input{ID: "one", Reference: "tasks/document-index", Mode: ap.NextTurn})
	if err != nil || len(x.Actions) != 1 {
		t.Fatal(err)
	}
	var wire struct {
		Method string `json:"method"`
		Params struct {
			Model  string                        `json:"model"`
			Effort string                        `json:"effort"`
			Input  []struct{ Type, Text string } `json:"input"`
		} `json:"params"`
	}
	if json.Unmarshal(x.Actions[0].Bytes, &wire) != nil || wire.Method != "turn/start" || wire.Params.Model != "unfamiliar-model" || wire.Params.Effort != "unfamiliar-effort" || len(wire.Params.Input) != 1 || wire.Params.Input[0].Type != "text" {
		t.Fatal("work reference changed protocol framing")
	}
	want := "Work reference: tasks/document-index. Resolve this admitted task reference using this workspace's existing workflow. Perform only its authorized work and record the required output. If the reference is missing, ambiguous or outside the admitted scope, stop and report the constraint. This notice grants no additional permission."
	if wire.Params.Input[0].Text != want {
		t.Fatal("work rendering changed")
	}
	legacy, _ := idleDriver(t)
	x, err = legacy.Prepare(ap.Input{ID: "one", Reference: "tasks/document-index", Mode: ap.NextTurn})
	if err != nil || json.Unmarshal(x.Actions[0].Bytes, &wire) != nil || wire.Params.Input[0].Text != "Notice reference: tasks/document-index. For this synthetic notice, reply ok without tools." {
		t.Fatal("default synthetic rendering changed", err)
	}
}

// Legacy checkpoints restore only as synthetic; work checkpoints require their
// explicit mode and version. Cross-mode/corrupt plants fail before Begin exists.
func TestWorkReferenceCheckpointBinding(t *testing.T) {
	legacy, _ := idleDriver(t)
	work, offset := idleWorkDriver(t)
	for _, tc := range []struct {
		mode  string
		saved []byte
	}{
		{WorkReference, legacy.Snapshot().Checkpoint},
		{SyntheticNotice, work.Snapshot().Checkpoint},
		{"unknown", nil},
	} {
		if d, _, err := openNoticeMode(tc.mode, tc.saved); err == nil || d != nil {
			t.Fatal("mode mismatch constructed a driver")
		}
	}
	saved := work.Snapshot().Checkpoint
	for _, mutation := range []string{"missing", "unknown", "synthetic", "old-version", "old-work-version", "missing-policy", "wrong-policy", "stream-mode", "unbound-policy"} {
		var r map[string]json.RawMessage
		json.Unmarshal(saved, &r)
		switch mutation {
		case "missing":
			delete(r, "notice_mode")
		case "unknown":
			r["notice_mode"] = json.RawMessage(`"future"`)
		case "synthetic":
			r["notice_mode"] = json.RawMessage(`"synthetic"`)
		case "old-version":
			r["version"] = json.RawMessage(`1`)
		case "old-work-version":
			r["version"] = json.RawMessage(`2`)
		case "missing-policy":
			delete(r, "permission_policy")
		case "wrong-policy":
			r["permission_policy"] = json.RawMessage(`"workspace-write"`)
		case "stream-mode":
			var old map[string]json.RawMessage
			json.Unmarshal(legacy.Snapshot().Checkpoint, &old)
			r["stream"] = old["stream"]
		case "unbound-policy":
			var stream map[string]json.RawMessage
			json.Unmarshal(r["stream"], &stream)
			var protocol map[string]json.RawMessage
			json.Unmarshal(stream["protocol"], &protocol)
			c := config()
			// The intentionally bad digest binds mode but omits policy. This
			// expectation is independent of the production planDigest helper.
			unbound, _ := json.Marshal(struct{ Model, Provider, Effort, Workspace, NoticeMode string }{c.Model, c.Provider, c.Effort, c.Workspace, WorkReference})
			sum := sha256.Sum256(unbound)
			protocol["plan_digest"], _ = json.Marshal(hex.EncodeToString(sum[:]))
			stream["protocol"], _ = json.Marshal(protocol)
			r["stream"], _ = json.Marshal(stream)
		}
		bad, _ := json.Marshal(r)
		if d, _, err := openNoticeMode(WorkReference, bad); err == nil || d != nil {
			t.Fatalf("%s checkpoint accepted", mutation)
		}
	}
	r, from, err := openNoticeMode(WorkReference, saved)
	if err != nil || from != offset {
		t.Fatal("same-mode restore refused", err)
	}
	x, err := r.Begin()
	if err != nil || len(x.Actions) != 0 || x.View.Activity != ap.Unknown {
		t.Fatal("restore repeated init or skipped catchup", err)
	}
	if _, err := r.Prepare(ap.Input{ID: "early", Reference: "task/early", Mode: ap.NextTurn}); err == nil {
		t.Fatal("restore skipped retained-tail gate")
	}
	x, err = r.Observe(ap.Chunk{Offset: from, AtTail: true})
	if err != nil || x.View.Activity != ap.Idle {
		t.Fatal("valid catchup failed", err)
	}
}

// Rendering never supplies authority implicitly. Work requires a separate known
// policy; default synthetic emits no policy overrides, and valid work serializes
// the pinned thread/turn fields without accepting arbitrary native configuration.
func TestWorkReferenceExplicitPolicy(t *testing.T) {
	for _, opts := range []DriverOptions{{NoticeMode: WorkReference}, {NoticeMode: WorkReference, PermissionPolicy: "workspace-write"}, {NoticeMode: SyntheticNotice, PermissionPolicy: YoloPolicy}} {
		if d, _, err := OpenDriverWithOptions(driverConfig(), opts, nil); err == nil || err.Error() != "unsupported_permission_policy" || d != nil {
			t.Fatal("policy was inferred or widened", err)
		}
	}
	if d, _, err := OpenDriverWithMode(driverConfig(), WorkReference, nil); err == nil || err.Error() != "unsupported_permission_policy" || d != nil {
		t.Fatal("mode-only constructor inferred authority", err)
	}
	c := config()
	c.NoticeMode = WorkReference
	if p, err := New(c); err == nil || err.Error() != "unsupported_permission_policy" || p != nil {
		t.Fatal("protocol bypassed explicit policy", err)
	}
	d, _, err := openNoticeMode(WorkReference, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Begin(); err != nil {
		t.Fatal(err)
	}
	var offset uint64
	x := observe(t, d, &offset, `{"id":1,"result":{"userAgent":"synthetic","platformFamily":"unix","platformOs":"macos"}}`)
	var request struct {
		Method string                     `json:"method"`
		Params map[string]json.RawMessage `json:"params"`
	}
	if len(x.Actions) != 2 || json.Unmarshal(x.Actions[1].Bytes, &request) != nil || request.Method != "thread/start" || string(request.Params["approvalPolicy"]) != `"never"` || string(request.Params["sandbox"]) != `"danger-full-access"` || request.Params["sandboxPolicy"] != nil {
		t.Fatal("incorrect thread policy transport")
	}
	observe(t, d, &offset, `{"id":2,"result":{"thread":{"id":"thread-a"},"model":"unfamiliar-model","modelProvider":"unfamiliar-provider","approvalPolicy":"never","sandbox":{"type":"dangerFullAccess"},"cwd":"/tmp/synthetic"}}`)
	x, err = d.Prepare(ap.Input{ID: "one", Reference: "task/one", Mode: ap.NextTurn})
	request.Params = nil
	if err != nil || len(x.Actions) != 1 || json.Unmarshal(x.Actions[0].Bytes, &request) != nil || request.Method != "turn/start" || string(request.Params["approvalPolicy"]) != `"never"` || string(request.Params["sandboxPolicy"]) != `{"type":"dangerFullAccess"}` || request.Params["sandbox"] != nil {
		t.Fatal("incorrect turn policy transport", err)
	}
	legacy, _ := idleDriver(t)
	x, err = legacy.Prepare(ap.Input{ID: "one", Reference: "task/one", Mode: ap.NextTurn})
	if err != nil || bytes.Contains(x.Actions[0].Bytes, []byte("approvalPolicy")) || bytes.Contains(x.Actions[0].Bytes, []byte("sandboxPolicy")) {
		t.Fatal("synthetic default gained policy overrides", err)
	}
}

// Work thread replies must echo the requested approval/sandbox policy and exact
// workspace. Missing/conflicting echoes close input before a work turn; the
// valid control is the same initialization used by idleWorkDriver. No nonexistent
// turn-policy echo is required, and these fixtures do not qualify a real binary.
func TestWorkReferencePolicyEchoRefusal(t *testing.T) {
	for _, plant := range []string{"missing-approval", "wrong-approval", "missing-sandbox", "wrong-sandbox", "wrong-workspace"} {
		t.Run(plant, func(t *testing.T) {
			d, _, err := openNoticeMode(WorkReference, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := d.Begin(); err != nil {
				t.Fatal(err)
			}
			var offset uint64
			observe(t, d, &offset, `{"id":1,"result":{"userAgent":"synthetic","platformFamily":"unix","platformOs":"macos"}}`)
			result := map[string]any{"thread": map[string]string{"id": "thread-a"}, "model": "unfamiliar-model", "modelProvider": "unfamiliar-provider", "approvalPolicy": "never", "sandbox": map[string]string{"type": "dangerFullAccess"}, "cwd": "/tmp/synthetic"}
			switch plant {
			case "missing-approval":
				delete(result, "approvalPolicy")
			case "wrong-approval":
				result["approvalPolicy"] = "on-request"
			case "missing-sandbox":
				delete(result, "sandbox")
			case "wrong-sandbox":
				result["sandbox"] = map[string]string{"type": "workspaceWrite"}
			case "wrong-workspace":
				result["cwd"] = "/tmp/different-workspace"
			}
			wire, _ := json.Marshal(map[string]any{"id": 2, "result": result})
			wire = append(wire, '\n')
			x, err := d.Observe(ap.Chunk{Offset: offset, Bytes: wire, AtTail: true})
			if err == nil || err.Error() != "stream_protocol_refused" || x.View.Activity != ap.Unknown || len(x.Actions) != 0 || len(x.Facts) != 0 {
				t.Fatal("policy echo accepted", err)
			}
			if x, err = d.Prepare(ap.Input{ID: "one", Reference: "task/one", Mode: ap.NextTurn}); err == nil || len(x.Actions) != 0 {
				t.Fatal("policy refusal allowed work")
			}
		})
	}
}

// Real-work-shaped item notifications remain progress, omit private payloads
// from checkpoints, and cannot replace matching native completion. These are
// schema-grounded inert fixtures, not evidence of a live native tool execution.
func TestWorkReferenceToolProgress(t *testing.T) {
	d, offset := idleWorkDriver(t)
	d.Prepare(ap.Input{ID: "one", Reference: "task/one", Mode: ap.NextTurn})
	observe(t, d, &offset, `{"id":3,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}`)
	for _, item := range []string{"userMessage", "reasoning", "plan", "commandExecution", "fileChange", "agentMessage"} {
		for _, method := range []string{"item/started", "item/completed"} {
			b, _ := json.Marshal(map[string]any{"method": method, "params": map[string]any{"threadId": "thread-a", "turnId": "turn-a", "startedAtMs": 1, "completedAtMs": 2, "item": map[string]any{"id": "item-a", "type": item, "output": "private-work-item-canary"}}})
			x := observe(t, d, &offset, string(b))
			if x.View.Activity != ap.Busy || len(x.Actions) != 0 || len(x.Facts) != 0 || bytes.Contains(x.Checkpoint, []byte("private-work-item-canary")) {
				t.Fatal("tool progress authorized an effect/completion or persisted output")
			}
		}
	}
	x := observe(t, d, &offset, `{"method":"turn/completed","params":{"threadId":"thread-a","turn":{"id":"turn-a","status":"completed"}}}`)
	if x.View.Activity != ap.Idle || len(x.Facts) != 1 || x.Facts[0].Kind != ap.TurnCompleted {
		t.Fatal("matching native completion refused")
	}
}

// Unsupported/malformed/current-turn-conflicting items close the actual parser
// without output leakage or fabricated completion. Authority requests stay closed.
func TestWorkReferenceToolRefusals(t *testing.T) {
	for _, tc := range []struct{ params, want string }{
		{`{"threadId":"thread-a","turnId":"turn-a","item":{"id":"i","type":"futureTool"}}`, "unsupported_work_item"},
		{`{"threadId":"thread-a","turnId":"turn-b","item":{"id":"i","type":"commandExecution"}}`, "work_item_identity_conflict"},
		{`{"threadId":"thread-a","item":{"id":"i","type":"fileChange"}}`, "work_item_refused"},
	} {
		d, offset := idleWorkDriver(t)
		d.Prepare(ap.Input{ID: "one", Reference: "task/one", Mode: ap.NextTurn})
		observe(t, d, &offset, `{"id":3,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}`)
		wire := []byte(`{"method":"item/started","params":` + tc.params + "}\n")
		x, err := d.Observe(ap.Chunk{Offset: offset, Bytes: wire, AtTail: true})
		if err == nil || err.Error() != tc.want || x.View.Activity != ap.Unknown || len(x.Actions) != 0 || len(x.Facts) != 0 {
			t.Fatal("wrong tool refusal", err)
		}
		if x, err = d.Prepare(ap.Input{ID: "two", Reference: "task/two", Mode: ap.NextTurn}); err == nil || len(x.Actions) != 0 {
			t.Fatal("refusal reopened work")
		}
	}
	d, offset := idleWorkDriver(t)
	d.Prepare(ap.Input{ID: "one", Reference: "task/one", Mode: ap.NextTurn})
	observe(t, d, &offset, `{"id":3,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}`)
	x := observe(t, d, &offset, `{"id":"server-request","method":"item/commandExecution/requestApproval","params":{"threadId":"thread-a"}}`)
	if x.View.Activity != ap.Approval || len(x.Actions) != 0 || len(x.Facts) != 0 || len(x.Attention) != 1 {
		t.Fatal("work mode answered or hid approval")
	}
}

// An uncertain work submission restores without initialization/replay; a real
// retained-output gap closes it permanently and cannot turn into another write.
func TestWorkReferencePendingRestoreAndGap(t *testing.T) {
	d, offset := idleWorkDriver(t)
	x, err := d.Prepare(ap.Input{ID: "one", Reference: "task/one", Mode: ap.NextTurn})
	if err != nil || len(x.Actions) != 1 {
		t.Fatal("work prepare", err)
	}
	r, from, err := openNoticeMode(WorkReference, x.Checkpoint)
	if err != nil || from != offset {
		t.Fatal("pending restore", err)
	}
	if x, err = r.Begin(); err != nil || len(x.Actions) != 0 {
		t.Fatal("pending replay", err)
	}
	if x, err = r.Observe(ap.Chunk{Offset: offset, AtTail: true}); err != nil || x.View.Activity != ap.Busy || len(x.Actions) != 0 {
		t.Fatal("pending catchup", err)
	}
	if x, err = r.Prepare(ap.Input{ID: "two", Reference: "task/two", Mode: ap.NextTurn}); err == nil || len(x.Actions) != 0 {
		t.Fatal("busy work submitted")
	}
	x, err = r.Observe(ap.Chunk{Offset: offset + 1, AtTail: true})
	if err == nil || x.View.Activity != ap.Unknown || len(x.Actions) != 0 || len(x.Facts) != 0 {
		t.Fatal("gap admitted work", err)
	}
	if x, err = r.Prepare(ap.Input{ID: "two", Reference: "task/two", Mode: ap.NextTurn}); err == nil || len(x.Actions) != 0 {
		t.Fatal("gap retried work")
	}
}
