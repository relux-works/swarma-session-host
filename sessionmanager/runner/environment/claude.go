package environment

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

const ClaudeCaptureEnv = "TASK_BOARD_NATIVE_CLAUDE_HOME_RECORD"
const ClaudeCaptureName = "native-claude-home.json"

type claudeCapture struct {
	Version     int    `json:"version"`
	NativeHome  string `json:"native_home"`
	ManagedHome string `json:"managed_home"`
}

// ClaudeNativeHome reads explicit task-board provenance, never credentials or
// a guessed replacement for an unknown managed selector.
func ClaudeNativeHome() (string, error) { return claudeNativeHome(os.Environ()) }

func claudeNativeHome(base []string) (string, error) {
	selected := envValue(base, "CLAUDE_CONFIG_DIR")
	if recordPath := envValue(base, ClaudeCaptureEnv); recordPath != "" {
		if !filepath.IsAbs(recordPath) || filepath.Clean(recordPath) != recordPath {
			return "", &Error{}
		}
		data, err := readRecord(recordPath)
		if err != nil {
			return "", &Error{}
		}
		var record claudeCapture
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&record) != nil || decoder.Decode(new(any)) != io.EOF || record.Version != 1 || !filepath.IsAbs(record.ManagedHome) || filepath.Clean(record.ManagedHome) != record.ManagedHome || selected == "" || filepath.Clean(selected) != record.ManagedHome || !validClaudeNative(record.NativeHome) || filepath.Clean(record.NativeHome) == record.ManagedHome {
			return "", &Error{}
		}
		canonical, _ := json.Marshal(record)
		if !bytes.Equal(data, canonical) {
			return "", &Error{}
		}
		return record.NativeHome, nil
	}
	if selected == "" {
		selected = nativeDefault(base, ".claude")
	}
	if !filepath.IsAbs(selected) {
		return "", &Error{}
	}
	selected = filepath.Clean(selected)
	if !validClaudeNative(selected) {
		return "", &Error{}
	}
	return selected, nil
}

func validClaudeNative(home string) bool {
	return filepath.IsAbs(home) && filepath.Clean(home) == home && !managed(home)
}

// CaptureClaudeHome stores provenance beside the session state, never in the
// shared Curator home. Resolve source before mkdir; reuse only identical bytes.
func CaptureClaudeHome(sessionDir, managedHome string) (string, error) {
	native, err := ClaudeNativeHome()
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(sessionDir) || !filepath.IsAbs(managedHome) {
		return "", &Error{}
	}
	managedHome = filepath.Clean(managedHome)
	if managedHome == native {
		return "", &Error{}
	}
	data, _ := json.Marshal(claudeCapture{Version: 1, NativeHome: native, ManagedHome: managedHome})
	if err := os.MkdirAll(sessionDir, 0700); err != nil {
		return "", &Error{}
	}
	path := filepath.Join(sessionDir, ClaudeCaptureName)
	if err := writeCapture(path, data); err != nil {
		return "", err
	}
	return path, nil
}

func writeCapture(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		existing, readErr := readRecord(path)
		if readErr == nil && bytes.Equal(existing, data) {
			return nil
		}
		return &Error{}
	}
	if err != nil {
		return &Error{}
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return &Error{}
	}
	return nil
}
