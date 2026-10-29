package sessionmanager

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func normalizeProvider(provider string) (string, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		return "", fmt.Errorf("provider is required")
	}
	for _, r := range provider {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return "", fmt.Errorf("provider %q contains unsupported characters", provider)
		}
	}
	return provider, nil
}

func opaqueID(prefix string) (string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating opaque ID: %w", err)
	}
	return prefix + "-" + base64.RawURLEncoding.EncodeToString(raw), nil
}

const (
	stoppedHostTerminatedLivenessDetail = "provider host was terminated by the session stop path"
	stoppedHostAbsentLivenessDetail     = "provider host was already absent when the session was stopped"
	stoppedStaleLiveLivenessDetail      = "stopped session recorded a stale live liveness; corrected at load"
)

func writePrivateFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("creating private temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("writing private temporary file: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("securing private temporary file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("syncing private temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("closing private temporary file: %w", err)
	}
	if existing, readErr := os.ReadFile(path); readErr == nil && bytes.Equal(existing, data) {
		_ = os.Remove(tmpPath)
		if err := os.Chmod(path, 0o600); err != nil {
			return fmt.Errorf("securing existing private file: %w", err)
		}
		return nil
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("publishing private file: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return fmt.Errorf("syncing private file directory: %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

const (
	sessionRecordContract         = "task-board-session-record-v3"
	legacySessionRecordContractV2 = "task-board-session-record-v2"
	legacySessionRecordContract   = "task-board-session-record-v1"
)

const ErrorSessionProviderConflict = "session_provider_conflict"

func validateClaudeGoalCommand(command *ClaudeGoalCommand) error {
	if command == nil || strings.TrimSpace(command.CommandID) == "" || (command.Set == nil) == (command.Clear == nil) {
		return ErrInvalidInput
	}
	text := ""
	if command.Set != nil {
		text = command.Set.Text
		if strings.TrimSpace(command.Set.Token) == "" || normalizeClaudeMarkerText(command.Set.Token) != command.Set.Token || strings.ContainsAny(command.Set.Token, " :\x1b\x00\r\n") {
			return ErrInvalidInput
		}
	} else {
		text = command.Clear.Text
	}
	if strings.TrimSpace(text) == "" || strings.ContainsAny(text, "\x1b\x00\r\n") {
		return ErrInvalidInput
	}
	return nil
}
