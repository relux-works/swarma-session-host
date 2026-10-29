// Package environment separates the operator's native home selector from the
// private home used by a hosted provider. It never reads credential contents.
package environment

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const NativeHomeUnknown = "curator_native_home_unknown"
const NativeHomeGuidance = "use an absolute native selector or restart from the operator native environment"
const CredentialConflictCode = "curator_credential_conflict"
const CaptureName = ".task-board-native-codex-home.json"

// Error is a safe, typed refusal; private paths and parser errors stay private.
type Error struct{}

func (*Error) Error() string { return NativeHomeUnknown }

// CredentialError refuses credential conflicts without exposing diagnostics.
type CredentialError struct{}

func (*CredentialError) Error() string { return CredentialConflictCode }

type capture struct {
	Version    int    `json:"version"`
	NativeHome string `json:"native_home"`
}

// Environment returns a copy for a Curator invocation, never for the provider.
// A hosted Codex home must carry the selector captured by its session manager.
// A standalone explicit selector is operator input; a Curator managed marker
// without native provenance is insufficient and refuses instead of guessing.
func Environment(environment string) ([]string, error) {
	return fromEnvironment(os.Environ(), environment)
}

// ForCommand preserves exec.Cmd's logical PWD for Dir before restoring native
// selectors. Callers use this for every Curator resolve, repair and version probe.
func ForCommand(command *exec.Cmd, environment string) ([]string, error) {
	return fromEnvironment(command.Environ(), environment)
}

func fromEnvironment(base []string, environment string) ([]string, error) {
	result := append([]string(nil), base...)
	keys := map[string][]string{
		"codex_cli": {"CODEX_HOME"}, "claude_code": {"CLAUDE_CONFIG_DIR"},
		"pi": {"PI_CODING_AGENT_DIR"}, "opencode": {"XDG_CONFIG_HOME"},
		"muse": {"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME"},
	}[environment]
	// A 0.15 repair may migrate the whole profile, including Codex when
	// resolving another adapter. Never pass a hosted Codex selector through.
	if environment != "codex_cli" && envValue(base, "CODEX_HOME") != "" {
		keys = append(keys, "CODEX_HOME")
	}
	// Profile-wide repair may also touch Claude while resolving another adapter.
	if environment != "claude_code" && (envValue(base, "CLAUDE_CONFIG_DIR") != "" || envValue(base, ClaudeCaptureEnv) != "") {
		keys = append(keys, "CLAUDE_CONFIG_DIR")
	}
	for _, key := range keys {
		value := envValue(base, key)
		if key == "CODEX_HOME" {
			native, err := codexNativeHome(base)
			if err != nil {
				return nil, err
			}
			result = replace(result, key, native)
		} else if key == "CLAUDE_CONFIG_DIR" {
			native, err := claudeNativeHome(base)
			if err != nil {
				return nil, err
			}
			result = replace(result, key, native)
		} else if value != "" {
			if !filepath.IsAbs(value) || managed(value) {
				return nil, &Error{}
			}
		}
	}
	return result, nil
}

// CodexNativeHome resolves only captured provenance or an operator selector.
// The unset selector uses Curator's documented HOME/.codex native default.
func CodexNativeHome() (string, error) {
	return codexNativeHome(os.Environ())
}

func codexNativeHome(base []string) (string, error) {
	selected := envValue(base, "CODEX_HOME")
	if selected != "" {
		if !filepath.IsAbs(selected) {
			return "", &Error{}
		}
		data, err := readRecord(filepath.Join(selected, CaptureName))
		if err == nil {
			var record capture
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&record) != nil || decoder.Decode(new(any)) != io.EOF || record.Version != 1 || !validNative(record.NativeHome) || filepath.Clean(record.NativeHome) == filepath.Clean(selected) {
				return "", &Error{}
			}
			canonical, _ := json.Marshal(record)
			if !bytes.Equal(data, canonical) {
				return "", &Error{}
			}
			return record.NativeHome, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", &Error{}
		}
	}
	if privateCodexLocation(selected) {
		return "", &Error{}
	}
	if selected == "" {
		selected = nativeDefault(base, ".codex")
	}
	selected = filepath.Clean(selected)
	if !validNative(selected) {
		return "", &Error{}
	}
	return filepath.Clean(selected), nil
}

func nativeDefault(base []string, leaf string) string {
	homeKey := "HOME"
	if runtime.GOOS == "windows" {
		homeKey = "USERPROFILE"
	}
	if runtime.GOOS == "plan9" {
		homeKey = "home"
	}
	home := envValue(base, homeKey)
	if !filepath.IsAbs(home) {
		return ""
	}
	return filepath.Join(home, leaf)
}

// Legacy task-board private homes have a deterministic manager-owned location.
// Session ids in the ambient environment are deliberately not provenance.
func privateCodexLocation(home string) bool {
	if resolved, err := resolveHomeAliases(home); err == nil {
		home = resolved
	}
	for current := filepath.Clean(home); ; current = filepath.Dir(current) {
		if filepath.Base(current) == "codex-home" {
			session := filepath.Dir(current)
			sessions := filepath.Dir(session)
			manager := filepath.Dir(sessions)
			fingerprint := filepath.Base(manager)
			decoded, err := hex.DecodeString(fingerprint)
			if filepath.Base(sessions) == "sessions" && filepath.Base(session) != "." && len(decoded) == 32 && err == nil {
				parent := filepath.Dir(manager)
				if filepath.Base(parent) == "boardless" {
					parent = filepath.Dir(parent)
				}
				if filepath.Base(parent) == "session-manager" && filepath.Base(filepath.Dir(parent)) == "task-board" {
					return true
				}
			}
		}
		if filepath.Dir(current) == current {
			return false
		}
	}
}

func validNative(home string) bool {
	if !filepath.IsAbs(home) || filepath.Clean(home) != home || privateCodexLocation(home) || managed(home) {
		return false
	}
	_, err := os.Lstat(filepath.Join(home, CaptureName))
	return errors.Is(err, os.ErrNotExist)
}

// resolveHomeAliases resolves existing ancestors too, so an absent leaf below
// a directory alias cannot conceal the managed marker above its physical root.
func resolveHomeAliases(home string) (string, error) {
	var tail []string
	current := filepath.Clean(home)
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for i := len(tail) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, tail[i])
			}
			return resolved, nil
		}
		if !errors.Is(err, os.ErrNotExist) || filepath.Dir(current) == current {
			return "", err
		}
		tail = append(tail, filepath.Base(current))
		current = filepath.Dir(current)
	}
}

// managed detects a Curator home without interpreting or following credentials.
// An unreadable or malformed marker is unknown provenance, hence also refuses.
func managed(home string) bool {
	// Follow directory aliases before walking ancestors: a symlink can point
	// inside a managed root whose marker is above the selector itself.
	if resolved, err := resolveHomeAliases(home); err == nil {
		home = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return true
	}
	// XDG selectors may name a descendant of the managed environment root.
	// Markers classify that root; no directory shape or native target is guessed.
	for current := filepath.Clean(home); ; current = filepath.Dir(current) {
		data, err := readRecord(filepath.Join(current, ".agent-environment.json"))
		if err == nil {
			var marker struct {
				Version int    `json:"version"`
				Mode    string `json:"mode"`
			}
			if json.Unmarshal(data, &marker) != nil || (marker.Version != 1 && marker.Version != 3) || (marker.Mode != "linked" && marker.Mode != "copied") {
				return true
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return true
		}
		if filepath.Dir(current) == current {
			return false
		}
	}
}

func readRecord(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return nil, &Error{}
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return nil, &Error{}
	}
	return data, nil
}

// CaptureCodexHome records the manager's resolved native selector before
// CODEX_HOME is replaced for the provider. Reuse requires identical provenance;
// existing files, symlinks, and unknown managed sources are never overwritten.
func CaptureCodexHome(privateHome string) error {
	native, err := CodexNativeHome()
	if err != nil {
		return err
	}
	data, err := json.Marshal(capture{Version: 1, NativeHome: native})
	if err != nil {
		return &Error{}
	}
	return writeCapture(filepath.Join(privateHome, CaptureName), data)
}

// CredentialConflict always wins over a wrapping stale-home diagnostic.
func CredentialConflict(stderr string) bool {
	return strings.Contains(stderr, "environment_credential_conflict")
}

func replace(environment []string, key, value string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, key+"=") {
			result = append(result, entry)
		}
	}
	return append(result, key+"="+value)
}

// Environment duplicates follow exec.Cmd's effective-last-value convention.
func envValue(environment []string, key string) string {
	for i := len(environment) - 1; i >= 0; i-- {
		if value, ok := strings.CutPrefix(environment[i], key+"="); ok {
			return value
		}
	}
	return ""
}
