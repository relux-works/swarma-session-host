package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

const fixedSession = "11111111-2222-4333-8444-555555555555"

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestResolveReferenceConfinement(t *testing.T) {
	ws := canonicalTempDir(t)
	if err := os.MkdirAll(filepath.Join(ws, "prompts"), 0700); err != nil {
		t.Fatal(err)
	}
	prompt := filepath.Join(ws, "prompts", "r1.md")
	if err := os.WriteFile(prompt, []byte("do the task\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := resolveReference(ws, "prompts/r1.md")
	if err != nil || got != prompt {
		t.Fatalf("valid reference refused: %v %q", err, got)
	}
	for _, bad := range []string{"", "/etc/passwd", "../outside.md", "prompts/../../outside.md", "prompts/missing.md", "prompts"} {
		if _, err := resolveReference(ws, bad); err == nil {
			t.Fatalf("reference %q accepted", bad)
		}
	}
	outside := filepath.Join(canonicalTempDir(t), "secret.md")
	if err := os.WriteFile(outside, []byte("x\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(ws, "prompts", "link.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveReference(ws, "prompts/link.md"); err == nil {
		t.Fatal("symlink escape accepted")
	}
}

func TestLoadConfigRefusals(t *testing.T) {
	env := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	base := map[string]string{
		"CLAUDE_CHILD_WORKSPACE": "/tmp/ws",
		"CLAUDE_CHILD_BIN":       "/tmp/claude",
		"CLAUDE_CHILD_MODEL":     "claude-test-1",
	}
	cfg, err := loadConfig(env(base), "/tmp")
	if err != nil || cfg.permissionMode != "manual" {
		t.Fatalf("default config refused: %v %+v", err, cfg)
	}
	for name, m := range map[string]map[string]string{
		"missing model": {
			"CLAUDE_CHILD_WORKSPACE": "/tmp/ws", "CLAUDE_CHILD_BIN": "/tmp/claude"},
		"relative workspace": {
			"CLAUDE_CHILD_WORKSPACE": "relative", "CLAUDE_CHILD_BIN": "/tmp/claude", "CLAUDE_CHILD_MODEL": "m"},
		"bypass permissions": {
			"CLAUDE_CHILD_WORKSPACE": "/tmp/ws", "CLAUDE_CHILD_BIN": "/tmp/claude", "CLAUDE_CHILD_MODEL": "m",
			"CLAUDE_CHILD_PERMISSION_MODE": "bypassPermissions"},
		"bad effort": {
			"CLAUDE_CHILD_WORKSPACE": "/tmp/ws", "CLAUDE_CHILD_BIN": "/tmp/claude", "CLAUDE_CHILD_MODEL": "m",
			"CLAUDE_CHILD_EFFORT": "ultra"},
		"bad timeout": {
			"CLAUDE_CHILD_WORKSPACE": "/tmp/ws", "CLAUDE_CHILD_BIN": "/tmp/claude", "CLAUDE_CHILD_MODEL": "m",
			"CLAUDE_CHILD_TIMEOUT_SECONDS": "5"},
	} {
		if _, err := loadConfig(env(m), "/tmp"); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestClaudeArgsShape(t *testing.T) {
	args := claudeArgs(config{model: "claude-test-1", effort: "high", permissionMode: "manual"}, fixedSession)
	joined := strings.Join(args, " ")
	for _, want := range []string{"-p", "--output-format json", "--no-session-persistence",
		"--session-id " + fixedSession, "--model=claude-test-1", "--effort high",
		"--permission-mode manual", "--permission-prompts none"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args missing %q: %v", want, args)
		}
	}
	for _, banned := range []string{"bypassPermissions", "dangerously-skip-permissions", "--resume", "--continue"} {
		if strings.Contains(joined, banned) {
			t.Fatalf("args contain banned %q: %v", banned, args)
		}
	}
}

func resultJSON(session string, isError bool, denials int) string {
	d := ""
	for i := 0; i < denials; i++ {
		if d != "" {
			d += ","
		}
		d += `{"tool_name":"Bash","tool_use_id":"x"}`
	}
	return `{"type":"result","subtype":"success","is_error":` + map[bool]string{true: "true", false: "false"}[isError] +
		`,"session_id":"` + session + `","num_turns":2,"result":"done",` +
		`"permission_denials":[` + d + `],` +
		`"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":3}}` + "\n"
}

func fakeChild(t *testing.T, exit int, payload string) (*child, *bytes.Buffer, string, *string) {
	t.Helper()
	ws := canonicalTempDir(t)
	if err := os.MkdirAll(filepath.Join(ws, "prompts"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, "prompts", "r1.md"), []byte("do the task\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	var sawStdin string
	c := &child{
		cfg:       config{workspace: ws, claudeBin: "/unused/claude", model: "claude-test-1", permissionMode: "manual", outDir: filepath.Join(ws, "output"), timeout: time.Minute},
		parent:    context.Background(),
		stdout:    &out,
		sessionID: func() (string, error) { return fixedSession, nil },
		runCmd: func(ctx context.Context, args []string, stdin io.Reader, w, e io.Writer) (int, error) {
			b, err := io.ReadAll(stdin)
			if err != nil {
				return -1, err
			}
			sawStdin = string(b)
			if _, err := io.WriteString(w, payload); err != nil {
				return -1, err
			}
			return exit, nil
		},
	}
	return c, &out, ws, &sawStdin
}

func noticeEnv(id string) fixtureadapters.Envelope {
	return fixtureadapters.Envelope{Kind: "notice",
		Notice:    sessionhost.Notice{ID: id, Reference: "prompts/r1.md"},
		Selection: sessionhost.Selection{Vendor: "anthropic", Model: "claude-test-1", Reasoning: "high"}}
}

func TestHandleAppliedOnlyAfterCleanResult(t *testing.T) {
	payload := resultJSON(fixedSession, false, 0)
	c, out, ws, sawStdin := fakeChild(t, 0, payload)
	env := noticeEnv("task-001")
	if err := c.handle(env); err != nil {
		t.Fatal(err)
	}
	if *sawStdin != "do the task\n" {
		t.Fatalf("prompt not fed on stdin: %q", *sawStdin)
	}
	var applied fixtureadapters.Envelope
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &applied); err != nil {
		t.Fatal(err)
	}
	if applied.Kind != "applied" || applied.Notice != env.Notice || applied.Selection != env.Selection {
		t.Fatalf("bad applied envelope: %+v", applied)
	}
	raw, err := os.ReadFile(filepath.Join(ws, "output", "task-001.receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rec receipt
	if err := json.Unmarshal(bytes.TrimSpace(raw), &rec); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(payload))
	if rec.ClaudeExit != 0 || rec.SessionID != fixedSession || rec.IsError == nil || *rec.IsError ||
		rec.StdoutSHA256 != hex.EncodeToString(sum[:]) || rec.Error != "" || rec.InputTokens != 10 {
		t.Fatalf("bad receipt: %+v", rec)
	}
	captured, err := os.ReadFile(filepath.Join(ws, "output", "task-001.claude.json"))
	if err != nil || string(captured) != payload {
		t.Fatalf("bad capture: %v %q", err, captured)
	}
	// The result text is payload: it must never enter the receipt.
	if bytes.Contains(raw, []byte(`"result"`)) || bytes.Contains(raw, []byte("done")) {
		t.Fatal("receipt leaked result payload")
	}
}

func TestHandleErrorResultNeverApplied(t *testing.T) {
	// Mirrors the observed 2.1.317 not-logged-in shape: exit 1,
	// subtype "success", is_error true.
	payload := resultJSON(fixedSession, true, 0)
	c, out, ws, _ := fakeChild(t, 1, payload)
	if err := c.handle(noticeEnv("task-002")); err == nil {
		t.Fatal("error result accepted")
	}
	if out.Len() != 0 {
		t.Fatalf("applied emitted on failure: %q", out.String())
	}
	var rec receipt
	raw, err := os.ReadFile(filepath.Join(ws, "output", "task-002.receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bytes.TrimSpace(raw), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.ClaudeExit != 1 || rec.Error != "claude_result_error" || rec.IsError == nil || !*rec.IsError {
		t.Fatalf("bad failure receipt: %+v", rec)
	}
}

func TestHandleSessionMismatchAndDenialsNeverApplied(t *testing.T) {
	c, out, _, _ := fakeChild(t, 0, resultJSON("99999999-8888-4777-8666-555555555555", false, 0))
	if err := c.handle(noticeEnv("task-003")); err == nil {
		t.Fatal("foreign session result accepted")
	}
	if out.Len() != 0 {
		t.Fatalf("applied emitted on mismatch: %q", out.String())
	}
	c2, out2, ws2, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 1))
	if err := c2.handle(noticeEnv("task-004")); err == nil {
		t.Fatal("denied-permission result accepted")
	}
	if out2.Len() != 0 {
		t.Fatalf("applied emitted on denial: %q", out2.String())
	}
	var rec receipt
	raw, err := os.ReadFile(filepath.Join(ws2, "output", "task-004.receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bytes.TrimSpace(raw), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Error != "permission_denied" || rec.Denials != 1 {
		t.Fatalf("bad denial receipt: %+v", rec)
	}
}

func TestHandleGarbageAndDuplicateNeverApplied(t *testing.T) {
	c, out, _, _ := fakeChild(t, 0, "not json\n")
	if err := c.handle(noticeEnv("task-005")); err == nil {
		t.Fatal("garbage stdout accepted")
	}
	if out.Len() != 0 {
		t.Fatalf("applied emitted on garbage: %q", out.String())
	}
	c2, _, _, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
	env := noticeEnv("task-006")
	if err := c2.handle(env); err != nil {
		t.Fatal(err)
	}
	if err := c2.handle(env); err == nil {
		t.Fatal("duplicate notice id overwrote prior capture")
	}
}

func TestNewSessionIDShape(t *testing.T) {
	id, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 36 || id[14] != '4' || (id[19] != '8' && id[19] != '9' && id[19] != 'a' && id[19] != 'b') {
		t.Fatalf("bad uuid: %q", id)
	}
}
