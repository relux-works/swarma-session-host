package environment

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func selected(env []string, key string) string {
	for _, entry := range env {
		if strings.HasPrefix(entry, key+"=") {
			return strings.TrimPrefix(entry, key+"=")
		}
	}
	return ""
}

// Captured custom and default native selectors survive the private-home switch;
// the returned environment changes only Curator's selector, never the caller's.
func TestCapturedNativeHome(t *testing.T) {
	for _, custom := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "custom"}[custom], func(t *testing.T) {
			home, native, private := t.TempDir(), t.TempDir(), t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("TASK_BOARD_SESSION_ID", "")
			t.Setenv("CODEX_HOME", "")
			if custom {
				t.Setenv("CODEX_HOME", native)
			} else {
				native = filepath.Join(home, ".codex")
			}
			if err := CaptureCodexHome(private); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CODEX_HOME", private)
			t.Setenv("TASK_BOARD_SESSION_ID", "SES-fixture")
			env, err := Environment("codex_cli")
			if err != nil {
				t.Fatal(err)
			}
			if got := selected(env, "CODEX_HOME"); got != native {
				t.Fatalf("native=%q want %q", got, native)
			}
			if os.Getenv("CODEX_HOME") != private {
				t.Fatal("mutated parent selector")
			}
		})
	}
}

// Missing, malformed, linked, self-referential, and managed provenance fail
// closed before Curator; no fallback to HOME or credential-link inference.
func TestUnknownNativeHome(t *testing.T) {
	for _, variant := range []string{"missing", "malformed", "symlink", "managed", "self", "nested-managed", "relative", "duplicate"} {
		t.Run(variant, func(t *testing.T) {
			private, native := t.TempDir(), t.TempDir()
			t.Setenv("CODEX_HOME", private)
			t.Setenv("TASK_BOARD_SESSION_ID", "SES-fixture")
			record := filepath.Join(private, CaptureName)
			switch variant {
			case "missing":
				private = filepath.Join(t.TempDir(), "task-board", "session-manager", strings.Repeat("a", 64), "sessions", "SES-legacy", "codex-home")
				if err := os.MkdirAll(private, 0700); err != nil {
					t.Fatal(err)
				}
				t.Setenv("CODEX_HOME", private)
			case "malformed":
				writeFixture(t, record, []byte("{}"))
			case "symlink":
				if err := os.Symlink(filepath.Join(native, "missing"), record); err != nil {
					t.Fatal(err)
				}
			case "managed":
				writeFixture(t, filepath.Join(private, ".agent-environment.json"), []byte(`{"mode":"managed-home"}`))
				t.Setenv("TASK_BOARD_SESSION_ID", "")
			case "self":
				writeFixture(t, record, []byte(`{"version":1,"native_home":"`+private+`"}`))
			case "nested-managed":
				writeFixture(t, filepath.Join(native, ".agent-environment.json"), []byte(`{"mode":"managed-home"}`))
				writeFixture(t, record, []byte(`{"version":1,"native_home":"`+native+`"}`))
			case "relative":
				t.Setenv("CODEX_HOME", "relative")
			case "duplicate":
				writeFixture(t, record, []byte(`{"version":0,"version":1,"native_home":"`+native+`"}`))
			}
			env, err := Environment("codex_cli")
			var typed *Error
			if env != nil || !errors.As(err, &typed) || err.Error() != NativeHomeUnknown {
				t.Fatalf("CODEX_HOME=%q err=%v", selected(env, "CODEX_HOME"), err)
			}
		})
	}
}

// Capture reuse is idempotent only for the same native home; a changed source
// or output symlink never overwrites an existing record or its target.
func TestCaptureNeverOverwrites(t *testing.T) {
	native, private := t.TempDir(), t.TempDir()
	t.Setenv("TASK_BOARD_SESSION_ID", "")
	t.Setenv("CODEX_HOME", native)
	if err := CaptureCodexHome(private); err != nil {
		t.Fatal(err)
	}
	if err := CaptureCodexHome(private); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(private, CaptureName))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_HOME", t.TempDir())
	if err := CaptureCodexHome(private); err == nil {
		t.Fatal("changed native selector accepted")
	}
	after, err := os.ReadFile(filepath.Join(private, CaptureName))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("record overwritten")
	}
	linkedPrivate, target := t.TempDir(), filepath.Join(t.TempDir(), "target")
	writeFixture(t, target, []byte("sentinel"))
	if err := os.Symlink(target, filepath.Join(linkedPrivate, CaptureName)); err != nil {
		t.Fatal(err)
	}
	if err := CaptureCodexHome(linkedPrivate); err == nil {
		t.Fatal("symlink record accepted")
	}
	unchanged, err := os.ReadFile(target)
	if err != nil || string(unchanged) != "sentinel" {
		t.Fatalf("symlink target changed: %q %v", unchanged, err)
	}
}

func writeFixture(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

// Other resolve wrappers also reject known managed native selectors, including
// XDG descendants; explicit native directories remain a positive control.
func TestRuntimeSelectorsNeverReuseManagedHome(t *testing.T) {
	for runtime, key := range map[string]string{"claude_code": "CLAUDE_CONFIG_DIR", "pi": "PI_CODING_AGENT_DIR", "opencode": "XDG_CONFIG_HOME", "muse": "XDG_CONFIG_HOME"} {
		t.Run(runtime, func(t *testing.T) {
			native, managedHome := t.TempDir(), t.TempDir()
			t.Setenv(key, native)
			if _, err := Environment(runtime); err != nil {
				t.Fatalf("native control refused: %v", err)
			}
			writeFixture(t, filepath.Join(managedHome, ".agent-environment.json"), []byte(`{"version":1,"mode":"managed-home"}`))
			t.Setenv(key, filepath.Join(managedHome, "config"))
			var refusal *Error
			if env, err := Environment(runtime); env != nil || !errors.As(err, &refusal) {
				t.Fatalf("managed selector admitted: %s=%q err=%v", key, selected(env, key), err)
			}
		})
	}
}

// Legacy native in-place markers use version 3; linked/copied remain native,
// unlike a managed-home marker. This is the host regression witness.
func TestLegacyNativeMarker(t *testing.T) {
	for _, mode := range []string{"linked", "copied"} {
		t.Run(mode, func(t *testing.T) {
			native := t.TempDir()
			writeFixture(t, filepath.Join(native, ".agent-environment.json"), []byte(`{"version":3,"mode":"`+mode+`"}`))
			t.Setenv("CODEX_HOME", native)
			t.Setenv("TASK_BOARD_SESSION_ID", "")
			home, err := CodexNativeHome()
			if err != nil || home != native {
				t.Fatalf("legacy native marker rejected: %q %v", home, err)
			}
		})
	}
}

// Every adapter invocation restores a captured Codex selector because supported
// 0.15 repairs can migrate sibling environments. Missing hosted provenance
// refuses before execution; neither case mutates the provider's selector.
func TestCapturedCodexSelectorAcrossAllResolves(t *testing.T) {
	native, private := t.TempDir(), t.TempDir()
	t.Setenv("TASK_BOARD_SESSION_ID", "")
	t.Setenv("CODEX_HOME", native)
	for _, key := range []string{"CLAUDE_CONFIG_DIR", "PI_CODING_AGENT_DIR", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME"} {
		t.Setenv(key, "")
	}
	if err := CaptureCodexHome(private); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TASK_BOARD_SESSION_ID", "SES-fixture")
	for _, runtime := range []string{"codex_cli", "claude_code", "muse", "pi", "opencode"} {
		t.Run(runtime, func(t *testing.T) {
			t.Setenv("CODEX_HOME", private)
			env, err := Environment(runtime)
			if err != nil || selected(env, "CODEX_HOME") != native || os.Getenv("CODEX_HOME") != private {
				t.Fatalf("captured selector lost: %v", err)
			}
			unknown := t.TempDir()
			writeFixture(t, filepath.Join(unknown, ".agent-environment.json"), []byte(`{"version":1,"mode":"managed-home"}`))
			t.Setenv("CODEX_HOME", unknown)
			env, err = Environment(runtime)
			var refusal *Error
			if env != nil || !errors.As(err, &refusal) || os.Getenv("CODEX_HOME") != unknown {
				t.Fatalf("unknown selector admitted or changed: %v", err)
			}
		})
	}
}

// The effective final selector wins, and the child command's logical PWD is
// retained. A managed Claude selector refuses even for Codex/Muse/version calls.
func TestCommandEnvironmentKeepsPWDAndEffectiveSelectors(t *testing.T) {
	native, managedHome := t.TempDir(), t.TempDir()
	t.Setenv("TASK_BOARD_SESSION_ID", "")
	t.Setenv("CODEX_HOME", native)
	t.Setenv("CLAUDE_CONFIG_DIR", native)
	writeFixture(t, filepath.Join(managedHome, ".agent-environment.json"), []byte(`{"version":1,"mode":"managed-home"}`))
	command := exec.Command("unused")
	command.Dir = t.TempDir()
	env, err := ForCommand(command, "codex_cli")
	if err != nil || selected(env, "PWD") != command.Dir || selected(env, "CLAUDE_CONFIG_DIR") != native {
		t.Fatalf("child environment: %v", err)
	}
	for _, key := range []string{"CODEX_HOME", "CLAUDE_CONFIG_DIR"} {
		for _, runtime := range []string{"codex_cli", "claude_code", "muse", ""} {
			command.Env = append(os.Environ(), key+"="+managedHome, key+"="+native)
			env, err = ForCommand(command, runtime)
			if err != nil || selected(env, key) != native {
				t.Fatalf("native effective selector %s/%s: %v", key, runtime, err)
			}
			command.Env = append(os.Environ(), key+"="+native, key+"="+managedHome)
			env, err = ForCommand(command, runtime)
			var refusal *Error
			if env != nil || !errors.As(err, &refusal) {
				t.Fatalf("managed effective selector %s/%s admitted: %v", key, runtime, err)
			}
		}
	}
}

// Native directory aliases are preserved; managed aliases and aliases into a
// managed subtree refuse. Relative, malformed/special and oversized records
// also refuse; a valid native marker at the exact read bound remains admitted.
func TestSelectorAliasesAndMarkerBounds(t *testing.T) {
	for _, key := range []string{"CODEX_HOME", "CLAUDE_CONFIG_DIR"} {
		t.Run(key, func(t *testing.T) {
			for _, variant := range []string{"native", "native-alias", "managed", "managed-alias", "managed-subtree-alias", "managed-absent-subtree-alias", "relative", "malformed", "oversized", "at-limit", "marker-symlink"} {
				t.Run(variant, func(t *testing.T) {
					t.Setenv("TASK_BOARD_SESSION_ID", "")
					t.Setenv("CODEX_HOME", "")
					t.Setenv("CLAUDE_CONFIG_DIR", "")
					home := t.TempDir()
					admitted := variant == "native" || variant == "native-alias" || variant == "at-limit"
					switch variant {
					case "managed", "managed-alias", "managed-subtree-alias", "managed-absent-subtree-alias":
						writeFixture(t, filepath.Join(home, ".agent-environment.json"), []byte(`{"version":1,"mode":"managed-home"}`))
					case "malformed":
						writeFixture(t, filepath.Join(home, ".agent-environment.json"), []byte("{"))
					case "oversized":
						writeFixture(t, filepath.Join(home, ".agent-environment.json"), []byte(strings.Repeat("x", (64<<10)+1)))
					case "at-limit":
						marker := `{"version":1,"mode":"linked"}`
						writeFixture(t, filepath.Join(home, ".agent-environment.json"), []byte(marker+strings.Repeat(" ", (64<<10)-len(marker))))
					case "marker-symlink":
						target := filepath.Join(t.TempDir(), "marker")
						writeFixture(t, target, []byte(`{"version":1,"mode":"linked"}`))
						if err := os.Symlink(target, filepath.Join(home, ".agent-environment.json")); err != nil {
							t.Fatal(err)
						}
					}
					if variant == "relative" {
						home = "relative-home"
					}
					if strings.HasSuffix(variant, "alias") {
						if variant == "managed-subtree-alias" || variant == "managed-absent-subtree-alias" {
							home = filepath.Join(home, "child")
							if err := os.Mkdir(home, 0700); err != nil {
								t.Fatal(err)
							}
						}
						alias := filepath.Join(t.TempDir(), "alias")
						if err := os.Symlink(home, alias); err != nil {
							t.Fatal(err)
						}
						home = alias
						if variant == "managed-absent-subtree-alias" {
							home = filepath.Join(alias, "absent")
						}
					}
					t.Setenv(key, home)
					env, err := Environment("codex_cli")
					if admitted {
						if err != nil || selected(env, key) != home {
							t.Fatalf("native control: %v", err)
						}
					} else {
						var refusal *Error
						if env != nil || !errors.As(err, &refusal) {
							t.Fatalf("unknown home admitted: %v", err)
						}
					}
				})
			}
		})
	}
}

// A stale daemon session id does not change real native input. Legacy homes
// under either manager layout refuse without a capture, including via aliases.
func TestNativeCodexProvenanceIgnoresAmbientSessionID(t *testing.T) {
	native := t.TempDir()
	t.Setenv("TASK_BOARD_SESSION_ID", "SES-stale")
	for _, selectedHome := range []string{native, native + string(filepath.Separator)} {
		t.Setenv("CODEX_HOME", selectedHome)
		got, err := CodexNativeHome()
		if err != nil || got != native {
			t.Fatalf("native daemon selector=%q err=%v", got, err)
		}
	}
	t.Setenv("HOME", native)
	t.Setenv("CODEX_HOME", "")
	if got, err := CodexNativeHome(); err != nil || got != filepath.Join(native, ".codex") {
		t.Fatalf("unset native daemon: %q %v", got, err)
	}
	for _, boardless := range []bool{false, true} {
		root := filepath.Join(t.TempDir(), "task-board", "session-manager")
		if boardless {
			root = filepath.Join(root, "boardless")
		}
		private := filepath.Join(root, strings.Repeat("a", 64), "sessions", "SES-legacy", "codex-home")
		if err := os.MkdirAll(private, 0700); err != nil {
			t.Fatal(err)
		}
		alias := filepath.Join(t.TempDir(), "alias")
		if err := os.Symlink(private, alias); err != nil {
			t.Fatal(err)
		}
		for _, home := range []string{private, alias} {
			for _, id := range []string{"", "SES-stale"} {
				t.Setenv("CODEX_HOME", home)
				t.Setenv("TASK_BOARD_SESSION_ID", id)
				var refusal *Error
				if _, err := CodexNativeHome(); !errors.As(err, &refusal) {
					t.Fatalf("legacy private admitted: %v", err)
				}
			}
		}
	}
}
