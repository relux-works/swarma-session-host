package environment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Custom and default Claude stores are captured outside the shared home, and
// restored for every adapter/probe. The provider selector stays managed.
func TestCapturedClaudeNativeHome(t *testing.T) {
	for _, custom := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "custom"}[custom], func(t *testing.T) {
			home, native, managedHome, session := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("CODEX_HOME", "")
			t.Setenv(ClaudeCaptureEnv, "")
			t.Setenv("CLAUDE_CONFIG_DIR", "")
			if custom {
				t.Setenv("CLAUDE_CONFIG_DIR", native)
			} else {
				native = filepath.Join(home, ".claude")
			}
			path, err := CaptureClaudeHome(session, managedHome)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("capture mode: %v", err)
			}
			if _, err := CaptureClaudeHome(session, managedHome); err != nil {
				t.Fatal("identical capture not reusable")
			}
			entries, err := os.ReadDir(managedHome)
			if err != nil || len(entries) != 0 {
				t.Fatal("Curator home mutated")
			}
			t.Setenv("CLAUDE_CONFIG_DIR", managedHome)
			t.Setenv(ClaudeCaptureEnv, path)
			for _, runtime := range []string{"codex_cli", "claude_code", "muse", "pi", "opencode", ""} {
				env, err := Environment(runtime)
				if err != nil || selected(env, "CLAUDE_CONFIG_DIR") != native || os.Getenv("CLAUDE_CONFIG_DIR") != managedHome {
					t.Fatalf("captured Claude %s: %v", runtime, err)
				}
			}
			// Nested hosted launches preserve the original native store, not the parent.
			nested := t.TempDir()
			nextHome := t.TempDir()
			next, err := CaptureClaudeHome(nested, nextHome)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(ClaudeCaptureEnv, next)
			t.Setenv("CLAUDE_CONFIG_DIR", nextHome)
			got, err := ClaudeNativeHome()
			if err != nil || got != native {
				t.Fatalf("nested capture: %q %v", got, err)
			}
		})
	}
}

// Missing, forged, symlinked, noncanonical and managed-native records refuse;
// failed capture leaves a new session directory absent and existing bytes intact.
func TestClaudeCaptureRefusesUnknownProvenance(t *testing.T) {
	for _, variant := range []string{"missing", "malformed", "symlink", "mismatch", "duplicate", "managed-native", "relative-record", "relative-native", "self", "oversize"} {
		t.Run(variant, func(t *testing.T) {
			native, managedHome, session := t.TempDir(), t.TempDir(), t.TempDir()
			t.Setenv(ClaudeCaptureEnv, "")
			t.Setenv("CLAUDE_CONFIG_DIR", native)
			path, err := CaptureClaudeHome(session, managedHome)
			if err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				writeFixture(t, path, []byte(`{}`))
			case "symlink":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(native, "absent"), path); err != nil {
					t.Fatal(err)
				}
			case "mismatch":
				managedHome = t.TempDir()
			case "duplicate":
				writeFixture(t, path, []byte(`{"version":0,"version":1,"native_home":"`+native+`","managed_home":"`+managedHome+`"}`))
			case "managed-native":
				writeFixture(t, filepath.Join(native, ".agent-environment.json"), []byte(`{"version":1,"mode":"managed-home"}`))
			case "relative-record":
				path = "relative-record"
			case "relative-native":
				writeFixture(t, path, []byte(`{"version":1,"native_home":"relative","managed_home":"`+managedHome+`"}`))
			case "self":
				writeFixture(t, path, []byte(`{"version":1,"native_home":"`+managedHome+`","managed_home":"`+managedHome+`"}`))
			case "oversize":
				writeFixture(t, path, make([]byte, (64<<10)+1))
			}
			t.Setenv("CLAUDE_CONFIG_DIR", managedHome)
			t.Setenv(ClaudeCaptureEnv, path)
			var refusal *Error
			if _, err := ClaudeNativeHome(); !errors.As(err, &refusal) {
				t.Fatalf("unknown capture admitted: %v", err)
			}
			absent := filepath.Join(t.TempDir(), "session")
			if _, err := CaptureClaudeHome(absent, t.TempDir()); !errors.As(err, &refusal) {
				t.Fatalf("unknown capture writable: %v", err)
			}
			if _, err := os.Stat(absent); !os.IsNotExist(err) {
				t.Fatal("unknown native wrote session state")
			}
		})
	}
	// A different native input cannot replace a valid existing session capture.
	native, managedHome, session := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv(ClaudeCaptureEnv, "")
	t.Setenv("CLAUDE_CONFIG_DIR", native)
	path, err := CaptureClaudeHome(session, managedHome)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	if _, err := CaptureClaudeHome(session, managedHome); err == nil {
		t.Fatal("different native overwrote capture")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(before) {
		t.Fatal("existing capture changed")
	}
}
