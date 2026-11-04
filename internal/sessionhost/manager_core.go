package sessionhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os/exec"
	"sync"
	"time"

	managerschema "github.com/relux-works/curator-agent-launcher/schemas/session-manager"
)

// Core is the existing selected-source reader, not an executable supplied by a
// request. No repair, composition, credential read or native launch is requested.
func readManagerCore(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "curator", args...)
	cmd.Dir, cmd.Env = dir, env
	return runManagerCore(cmd)
}

type coreOutput struct{ buffer bytes.Buffer }

func (b *coreOutput) Write(p []byte) (int, error) {
	if len(p) > managerschema.Limit-b.buffer.Len() {
		return 0, errMemberRecordUnavailable
	}
	return b.buffer.Write(p)
}
func runManagerCore(cmd *exec.Cmd) ([]byte, error) {
	var out coreOutput
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, &out, io.Discard
	cmd.WaitDelay = time.Second
	if cmd.Run() != nil {
		return nil, errMemberRecordUnavailable
	}
	return out.buffer.Bytes(), nil
}

// Consume only the workflow and profile identity. Other valid Core fragment
// fields remain opaque, covered by the complete canonical digest; they are not
// composed or interpreted here. The Launcher retains its full fragment parser.
func correlateManagerCore(selected, fragment []byte, q memberOperationRequest) ([32]byte, error) {
	var zero [32]byte
	if len(selected) > managerschema.Limit || len(fragment) > managerschema.Limit {
		return zero, errMemberRecordUnavailable
	}
	if _, err := managerschema.CanonicalJSON(selected); err != nil {
		return zero, errMemberRecordUnavailable
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(selected, &root) != nil || len(root) != 4 {
		return zero, errMemberRecordUnavailable
	}
	for _, k := range []string{"schema", "config_schema", "config_path", "extensions"} {
		if root[k] == nil {
			return zero, errMemberRecordUnavailable
		}
	}
	var schema, path string
	var version int
	var extensions map[string]json.RawMessage
	if json.Unmarshal(root["schema"], &schema) != nil || schema != "curator.launcher-config/2" || json.Unmarshal(root["config_schema"], &version) != nil || version != 8 || json.Unmarshal(root["config_path"], &path) != nil || !notificationRef(path, 4096) || json.Unmarshal(root["extensions"], &extensions) != nil || extensions == nil {
		return zero, errMemberRecordUnavailable
	}
	for k := range extensions {
		if k != "notification" && k != "proxy" && k != "claude_auth" {
			return zero, errMemberRecordUnavailable
		}
	}
	var workflow struct {
		Backend   string `json:"backend"`
		Contract  string `json:"contract"`
		Reference string `json:"workflow_reference"`
		SHA256    string `json:"workflow_sha256"`
	}
	var shape map[string]json.RawMessage
	n := extensions["notification"]
	if json.Unmarshal(n, &shape) != nil || len(shape) != 4 {
		return zero, errMemberRecordUnavailable
	}
	for _, k := range []string{"backend", "contract", "workflow_reference", "workflow_sha256"} {
		if shape[k] == nil {
			return zero, errMemberRecordUnavailable
		}
	}
	d := json.NewDecoder(bytes.NewReader(n))
	d.DisallowUnknownFields()
	if d.Decode(&workflow) != nil || workflow.Backend != "host" || workflow.Contract != NotificationContract || workflow.Reference != q.Notification.Workflow.Reference || workflow.SHA256 != q.Notification.Workflow.SHA256 {
		return zero, errMemberRecordUnavailable
	}
	canonical, err := managerschema.CanonicalJSON(fragment)
	if err != nil {
		return zero, errMemberRecordUnavailable
	}
	// Use a fresh map: json.Unmarshal merges into existing maps.
	root = nil
	if json.Unmarshal(fragment, &root) != nil || root == nil {
		return zero, errMemberRecordUnavailable
	}
	var id, environment string
	if json.Unmarshal(root["fragment"], &id) != nil || json.Unmarshal(root["environment"], &environment) != nil || environment != q.Runtime {
		return zero, errMemberRecordUnavailable
	}
	version = 0
	switch id {
	case "launch-env-fragment-v1":
		version = 1
	case "launch-env-fragment-v2":
		version = 2
	case "launch-env-fragment-v3":
		version = 3
	case "launch-env-fragment-v4":
		version = 4
	default:
		return zero, errMemberRecordUnavailable
	}
	allowed := map[string]bool{"fragment": true, "environment": true, "profile": true, "precedence": true, "env": true, "system_prompt": true, "mcp": true, "path_prepend": true}
	if version >= 2 {
		allowed["permissions"] = true
	}
	if version >= 4 {
		allowed["root_context"] = true
		allowed["credential_store"] = true
	}
	for k := range root {
		if !allowed[k] {
			return zero, errMemberRecordUnavailable
		}
	}
	required := []string{"fragment", "environment", "profile", "precedence", "env"}
	if version >= 2 {
		required = append(required, "permissions")
	}
	for _, k := range required {
		if root[k] == nil || bytes.Equal(bytes.TrimSpace(root[k]), []byte("null")) {
			return zero, errMemberRecordUnavailable
		}
	}
	var profile struct {
		Name string `json:"name"`
		Lock string `json:"lock_sha256"`
	}
	shape = nil
	if json.Unmarshal(root["profile"], &shape) != nil || len(shape) != 2 || shape["name"] == nil || shape["lock_sha256"] == nil || json.Unmarshal(root["profile"], &profile) != nil || profile.Name != q.Profile.Name || profile.Lock != q.Profile.Lock {
		return zero, errMemberRecordUnavailable
	}
	digest := sha256.Sum256(canonical)
	if q.Profile.FragmentDigest != "sha256:"+hex.EncodeToString(digest[:]) {
		return zero, errMemberRecordUnavailable
	}
	// Retain the whole selected projection as well as the operation's complete
	// fragment digest. A later observed configuration-source change is terminal.
	selectedCanonical, err := managerschema.CanonicalJSON(selected)
	if err != nil {
		return zero, errMemberRecordUnavailable
	}
	return sha256.Sum256(selectedCanonical), nil
}

type managerCoreRead func(context.Context, string, []string, ...string) ([]byte, error)
type managerCustody struct {
	operation *retainedMemberOperation
	data      []byte
	env       []string
	ctx       context.Context
	cancel    context.CancelFunc
	selected  [32]byte
	selection notificationSelection
	mu        sync.Mutex
	read      managerCoreRead // private; production always readManagerCore
}

func admitManagerCustody(ctx context.Context, op *retainedMemberOperation, data []byte, env []string, read managerCoreRead) (*managerCustody, error) {
	if ctx == nil || ctx.Err() != nil || op == nil || op.record == nil || read == nil {
		return nil, errMemberRecordUnavailable
	}
	o := op.record.source.owner
	if op.recheck(o, op.record.source, op.kind, data) != nil {
		return nil, errMemberRecordUnavailable
	}
	cctx, cancel := context.WithCancel(o.ctx)
	c := &managerCustody{operation: op, data: append([]byte(nil), data...), env: append([]string(nil), env...), ctx: cctx, cancel: cancel, read: read}
	n := op.request.Notification
	c.selection = notificationSelection{Member: n.Member.MXID, Connection: n.Member.Connection, Configuration: n.Member.ConfigurationRoot, ProfileLock: op.request.Profile.Lock, WorkflowReference: n.Workflow.Reference, WorkflowSHA256: n.Workflow.SHA256}
	selected, err := c.observe(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	c.selected = selected
	if ctx.Err() != nil || c.ctx.Err() != nil || op.recheck(o, op.record.source, op.kind, data) != nil {
		cancel()
		return nil, errMemberRecordUnavailable
	}
	return c, nil
}
func (c *managerCustody) observe(ctx context.Context) ([32]byte, error) {
	var zero [32]byte
	q := c.operation.request
	selected, err := c.read(ctx, c.operation.workspacePath, c.env, "config", "launcher")
	if err != nil {
		return zero, errMemberRecordUnavailable
	}
	fragment, err := c.read(ctx, c.operation.workspacePath, c.env, "env", "resolve", q.Runtime, "--profile", q.Profile.Name, "--format", "json")
	if err != nil {
		return zero, errMemberRecordUnavailable
	}
	return correlateManagerCore(selected, fragment, q)
}
func (c *managerCustody) lifetime() (context.Context, time.Time) {
	deadline, _ := c.ctx.Deadline()
	return c.ctx, deadline
}
func (c *managerCustody) checkNotificationOwner(h *Host, s notificationSelection) error {
	if c == nil || c.operation == nil || c.operation.record == nil {
		return errMemberRecordUnavailable
	}
	source := c.operation.record.source
	if h != source.owner.host || s != c.selection {
		return errMemberRecordUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx.Err() != nil {
		return errMemberRecordUnavailable
	}
	selected, err := c.observe(c.ctx)
	if err != nil || selected != c.selected || c.operation.recheck(source.owner, source, c.operation.kind, c.data) != nil {
		c.cancel()
		return errMemberRecordUnavailable
	}
	return nil
}
func (c *managerCustody) close() { c.cancel(); c.operation.close() }
