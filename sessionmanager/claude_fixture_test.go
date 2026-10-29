package sessionmanager

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var claudeMaterializationTestClock = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

func claudeTestLayout(t *testing.T, name string) Layout {
	t.Helper()
	root := filepath.Join(t.TempDir(), name)
	l := Layout{StateDir: root, SessionsDir: filepath.Join(root, "sessions"), MaterializationsDir: filepath.Join(root, "materializations"), QuarantineDir: filepath.Join(root, "quarantine"), MaterializationQuarantine: filepath.Join(root, "materialization-quarantine"), LogsDir: filepath.Join(root, "logs")}
	for _, p := range []string{l.StateDir, l.SessionsDir, l.MaterializationsDir, l.QuarantineDir, l.MaterializationQuarantine, l.LogsDir} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	return l
}
func claudeHasContextFailure(err error, code string) bool {
	var typed *ClaudeContextFailure
	return errors.As(err, &typed) && typed != nil && typed.Code == code
}

func claudeWriteExecutable(t *testing.T, name string, contents []byte, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, contents, mode); err != nil {
		t.Fatal(err)
	}
	return p
}

// Script fixtures receive only test-owned values and fixed tool lookup. An
// assertion dumping a synthetic snapshot cannot print inherited credentials.
func claudeFixtureEnvironment() []string {
	env := []string{"PATH=/usr/bin:/bin"}
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if ok && (strings.HasPrefix(key, "FAKE_CLAUDE_") || key == "CLAUDE_RELOCATION_SYNTHETIC_REQUIRED" || key == "CLAUDE_CONFIG_DIR") {
			env = append(env, entry)
		}
	}
	return env
}

// Original file-barrier observer used by the retained mutation-order regression.
func claudeWaitForFixturePath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatalf("inspect fixture barrier %s: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fixture barrier %s was not reached", path)
}
