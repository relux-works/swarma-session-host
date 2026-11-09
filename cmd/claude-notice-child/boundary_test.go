package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestPromptOpenedObjectAndBound(t *testing.T) {
	ws := canonicalTempDir(t)
	outside := canonicalTempDir(t)
	if err := os.Mkdir(filepath.Join(ws, "prompts"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, "prompts", "task"), []byte("admitted"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "task"), []byte("SYNTHETIC_OUTSIDE"), 0600); err != nil {
		t.Fatal(err)
	}
	// Deterministic ancestor replacement after pathname checking, before open.
	got, err := readPrompt(ws, "prompts/task", func() {
		if err := os.Rename(filepath.Join(ws, "prompts"), filepath.Join(ws, "original")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(ws, "prompts")); err != nil {
			t.Fatal(err)
		}
	})
	if err == nil || bytes.Contains(got, []byte("SYNTHETIC_OUTSIDE")) {
		t.Fatalf("opened escaped object: %q %v", got, err)
	}
	// Both relative and absolute internal symlinks remain compatible.
	for _, target := range []string{"original/task", filepath.Join(ws, "original/task")} {
		link := filepath.Join(ws, "link")
		_ = os.Remove(link)
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		got, err = readPrompt(ws, "link", nil)
		if err != nil || string(got) != "admitted" {
			t.Fatalf("internal link refused: %q %v", got, err)
		}
	}
	// Growth after the metadata check is bounded by the actual read limit.
	big := filepath.Join(ws, "growing")
	if err := os.WriteFile(big, []byte("small"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err = readPrompt(ws, "growing", func() {
		if e := os.WriteFile(big, bytes.Repeat([]byte("x"), maxPromptBytes+1), 0600); e != nil {
			t.Fatal(e)
		}
	})
	if err == nil || len(got) > maxPromptBytes {
		t.Fatal("growing input accepted")
	}
}

func TestOutputConfinementAndIDDefense(t *testing.T) {
	for _, id := range []string{"../../x", "../x", "/absolute", "a/b", "a\\b", "..", "", "x\x00y"} {
		c, out, _, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
		if err := c.writeReceipt(receipt{ID: id}); err == nil || err.Error() != "invalid_notice_id" {
			t.Fatalf("unsafe receipt id: %q %v", id, err)
		}
		if err := c.handle(noticeEnv(id)); err == nil || err.Error() != "invalid_notice_id" || out.Len() != 0 {
			t.Fatalf("unsafe notice id: %q %v", id, err)
		}
		if _, err := os.Stat(c.cfg.outDir); !os.IsNotExist(err) {
			t.Fatal("invalid id touched output")
		}
	}
	for _, symlink := range []bool{false, true} {
		c, out, ws, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
		outside := canonicalTempDir(t)
		if symlink {
			if err := os.Symlink(outside, filepath.Join(ws, "output")); err != nil {
				t.Fatal(err)
			}
		} else {
			c.cfg.outDir = outside
		}
		if err := c.handle(noticeEnv("safe")); err == nil || out.Len() != 0 {
			t.Fatalf("outside output accepted: %v", err)
		}
		entries, err := os.ReadDir(outside)
		if err != nil || len(entries) != 0 {
			t.Fatalf("outside output touched: %v %v", entries, err)
		}
	}
}

func TestStrictResultCompatibility(t *testing.T) {
	good := resultJSON(fixedSession, false, 0)
	for name, payload := range map[string]string{
		"bool_string":         strings.Replace(good, `"is_error":false`, `"is_error":"false"`, 1),
		"denials_object":      strings.Replace(good, `"permission_denials":[]`, `"permission_denials":{}`, 1),
		"denials_string":      strings.Replace(good, `"permission_denials":[]`, `"permission_denials":""`, 1),
		"duplicate_session":   strings.Replace(good, `"session_id":`, `"session_id":"foreign","session_id":`, 1),
		"duplicate_extension": strings.Replace(good, `"usage":{`, `"extension":{"x":1,"x":2},"usage":{`, 1),
		"non_object":          "null",
	} {
		t.Run(name, func(t *testing.T) {
			c, out, _, _ := fakeChild(t, 0, payload)
			if err := c.handle(noticeEnv("strict")); err == nil || out.Len() != 0 {
				t.Fatalf("invalid evidence applied: %v", err)
			}
		})
	}
	for _, payload := range []string{good, strings.Replace(good, `"usage":{`, `"extension":{"future":[null,true,{},[1,2]]},"usage":{`, 1)} {
		c, out, _, _ := fakeChild(t, 0, payload)
		if err := c.handle(noticeEnv("extension")); err != nil || out.Len() == 0 {
			t.Fatalf("valid extension refused: %v", err)
		}
	}
	for _, payload := range []string{
		strings.Replace(resultJSON(fixedSession, true, 0), `"is_error":true`, `"is_error":true,"IS_ERROR":false`, 1),
		strings.Replace(resultJSON(fixedSession, false, 1), `"permission_denials":[`, `"PERMISSION_DENIALS":[],"permission_denials":[`, 1),
	} {
		c, out, _, _ := fakeChild(t, 0, payload)
		if err := c.handle(noticeEnv("case-alias")); err == nil || out.Len() != 0 {
			t.Fatalf("case alias overrode evidence: %v", err)
		}
	}

	c, _, _, _ := fakeChild(t, 0, strings.Replace(good, `"is_error":false,`, "", 1))
	if err := c.handle(noticeEnv("missing")); err == nil {
		t.Fatal("missing bool accepted")
	}
	raw, err := os.ReadFile(filepath.Join(c.cfg.outDir, "missing.receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["is_error"]; ok {
		t.Fatal("receipt invented is_error")
	}
}

func TestRunClaudeIgnoreTERMWithinRunnerBudget(t *testing.T) {
	ws := canonicalTempDir(t)
	marker := filepath.Join(ws, "direct-pid")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := runClaude("/bin/sh", ws)(ctx, []string{"-c", `trap '' TERM; echo $$ > "$1"; exec /bin/sleep 60`, "sh", marker}, strings.NewReader(""), io.Discard, io.Discard)
		done <- err
	}()
	var pid int
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(marker)
		pid, _ = strconv.Atoi(strings.TrimSpace(string(b)))
		if pid > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid <= 0 {
		t.Fatal("native fixture did not start")
	}
	if group, err := syscall.Getpgid(pid); err != nil || group != pid {
		t.Fatalf("fixture ownership: %d %v", group, err)
	}
	defer func() {
		if g, e := syscall.Getpgid(pid); e == nil && g == pid {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}()
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancellation returned success")
		}
	case <-time.After(12 * time.Second):
		t.Fatal("native wait unbounded")
	}
	if elapsed := time.Since(started); elapsed >= 4*time.Second {
		t.Errorf("cleanup exceeded runner budget margin: %v", elapsed)
	}
	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatal("direct native child survived")
	}
}

// This matrix qualifies config/argv containment only. Native settings and env
// permission behavior require a separate logged-in capture before productive use.
func TestPermissionConfigArgvMatrix(t *testing.T) {
	for _, mode := range []string{"", "manual", "dontAsk", "acceptEdits", "plan", "auto", "bypassPermissions", "--dangerously-skip-permissions", "manual --dangerously-skip-permissions", "BYPASSPERMISSIONS", "--permission-mode=bypassPermissions"} {
		env := map[string]string{"CLAUDE_CHILD_WORKSPACE": "/workspace", "CLAUDE_CHILD_BIN": "/binary", "CLAUDE_CHILD_MODEL": "model", "CLAUDE_CHILD_PERMISSION_MODE": mode}
		cfg, err := loadConfig(func(k string) string { return env[k] }, "/workspace")
		allowed := mode == "" || mode == "manual" || mode == "dontAsk" || mode == "acceptEdits" || mode == "plan" || mode == "auto"
		if !allowed {
			if err == nil {
				t.Errorf("bypass accepted: %q", mode)
			}
			continue
		}
		if err != nil {
			t.Fatalf("legitimate mode refused: %q %v", mode, err)
		}
		wantMode := mode
		if wantMode == "" {
			wantMode = "manual"
		}
		want := []string{"-p", "--output-format", "json", "--no-session-persistence", "--session-id", fixedSession, "--model=model", "--permission-mode", wantMode, "--permission-prompts", "none"}
		if got := claudeArgs(cfg, fixedSession); !reflect.DeepEqual(got, want) {
			t.Fatalf("unexpected argv: %q", got)
		}
	}
	// Values stay single argv entries; neither prompt nor hostile values become flags.
	cfg := config{model: "--dangerously-skip-permissions", permissionMode: "manual", allowedTools: "--dangerously-skip-permissions"}
	args := claudeArgs(cfg, fixedSession)
	if !strings.Contains(strings.Join(args, " "), "--model=--dangerously-skip-permissions") || !strings.Contains(strings.Join(args, " "), "--allowedTools=--dangerously-skip-permissions") {
		t.Fatal("flag-like values were not bound to their options")
	}
	for _, arg := range args {
		if arg == "--dangerously-skip-permissions" {
			t.Fatal("value injected flag")
		}
	}
}

func TestContractPublicationHygiene(t *testing.T) {
	raw, err := os.ReadFile("CLAUDE-ADAPTER.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if regexp.MustCompile(`(?m)^(Producer:|First status|.*event.*\$|.*topic.*#)`).MatchString(text) {
		t.Error("contract contains operational provenance")
	}
	for _, forbidden := range []string{"/Users/", "/home/", "~/workbench", "ready today", "no engineering unknown"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("publication contains operational material: %q", forbidden)
		}
	}
	for _, required := range []string{"Effective native permission containment is unqualified", "prerequisite for productive use", "five-second", "one second", "stream-json", "exact"} {
		if !strings.Contains(text, required) {
			t.Errorf("contract omits material limit: %q", required)
		}
	}
}

func TestEffectivePermissionQualificationRequired(t *testing.T) {
	raw, err := os.ReadFile("CLAUDE-ADAPTER.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, claim := range []string{"Effective native permission containment is unqualified", "prerequisite for productive use of this shim", "Fake binaries", "Native environment"} {
		if !strings.Contains(string(raw), claim) {
			t.Errorf("missing effective-permission prerequisite: %s", claim)
		}
	}
}

type ackObserver func([]byte) (int, error)

func (f ackObserver) Write(p []byte) (int, error) { return f(p) }

func TestReceiptExistsBeforeAcknowledgment(t *testing.T) {
	c, _, _, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
	called := false
	c.stdout = ackObserver(func(p []byte) (int, error) {
		raw, err := os.ReadFile(filepath.Join(c.cfg.outDir, "ordered.receipt.json"))
		if err != nil {
			t.Fatal("ack before receipt", err)
		}
		var rec receipt
		if err := json.Unmarshal(raw, &rec); err != nil || rec.Error != "" {
			t.Fatalf("ack before successful receipt: %s %v", raw, err)
		}
		info, err := os.Stat(filepath.Join(c.cfg.outDir, "ordered.receipt.json"))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("receipt mode", info, err)
		}
		called = true
		return len(p), nil
	})
	if err := c.handle(noticeEnv("ordered")); err != nil || !called {
		t.Fatalf("completion missing: %v", err)
	}
	c2, out, _, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
	c2.syncReceipt = func(*os.File) error { return io.ErrClosedPipe }
	if err := c2.handle(noticeEnv("sync-failure")); err == nil || err.Error() != "receipt_write_failed" || out.Len() != 0 {
		t.Fatalf("sync error applied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(c2.cfg.outDir, "sync-failure.receipt.json")); !os.IsNotExist(err) {
		t.Fatal("failed sync published receipt")
	}
}

func TestVersionExactMatch(t *testing.T) {
	bin := filepath.Join(canonicalTempDir(t), "native")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf '9.9.9-test (Claude Code)\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		expected string
		ok       bool
	}{{"9.9.9-test (Claude Code)", true}, {"9.9.9-test", false}, {"9.9.9-other (Claude Code)", false}, {"", true}} {
		observed, err := checkVersion(context.Background(), bin, filepath.Dir(bin), tc.expected)
		if observed != "9.9.9-test (Claude Code)" || (err == nil) != tc.ok {
			t.Errorf("exact version comparison: %q %q %v", tc.expected, observed, err)
		}
	}
}

func TestWorkspaceAliasOutputCompatibility(t *testing.T) {
	for _, canonicalOutput := range []bool{false, true} {
		c, out, ws, _ := fakeChild(t, 0, resultJSON(fixedSession, false, 0))
		alias := filepath.Join(canonicalTempDir(t), "workspace")
		if err := os.Symlink(ws, alias); err != nil {
			t.Fatal(err)
		}
		c.cfg.workspace = alias
		if !canonicalOutput {
			c.cfg.outDir = filepath.Join(alias, "output")
		}
		if err := c.handle(noticeEnv("alias")); err != nil || out.Len() == 0 {
			t.Fatalf("legitimate workspace alias refused: %v", err)
		}
	}
}
