package sessionmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Original temporary-file roots retain rotation, modes, redaction and integer evidence.
func testLayout(t *testing.T, _ string) Layout {
	t.Helper()
	return Layout{Namespace: "fixture", OperatorLogFile: filepath.Join(t.TempDir(), "logs", "sessiond.log")}
}
func TestOperatorLoggerRotatesStructuredLinesAndRedactsSecrets(t *testing.T) {
	layout := testLayout(t, "operator-log-redaction")
	logger, err := newOperatorLogger(layout)
	if err != nil {
		t.Fatal(err)
	}
	logger.maxSize = 280
	logger.maxBackups = 2

	const (
		apiToken     = "api-token-should-never-appear"
		password     = "hunter2-should-never-appear"
		bearer       = "bearer-should-never-appear"
		providerName = "secret-provider-token"
	)
	logger.Info("provider_launched", map[string]any{
		"provider":   "codex",
		"pid":        4242,
		"executable": "/usr/local/bin/codex-app-server",
		"endpoint":   "wss://operator:" + password + "@provider.example/v1?token=" + apiToken,
	})
	logger.Error(
		"provider_launch_failed",
		fmt.Errorf("launch error token=%s bearer %s https://user:%s@provider.example/start %s", apiToken, bearer, password, providerName),
		map[string]any{
			"authorization": "Bearer " + bearer,
			"nested": map[string]any{
				"api_key": apiToken,
			},
		},
	)
	logger.Info("session_lifecycle", map[string]any{
		"session_id": "SES-operator-log",
		"transition": "stopped",
	})

	for _, path := range []string{layout.OperatorLogFile, layout.OperatorLogFile + ".1"} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatalf("Stat(%s): %v", path, statErr)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode = %o, want 0600", path, info.Mode().Perm())
		}
	}

	entries, raw := readOperatorLogEntries(t, layout.OperatorLogFile)
	if len(entries) < 2 {
		t.Fatalf("operator log entries = %d, want multiple: %s", len(entries), raw)
	}
	for _, secret := range []string{apiToken, password, bearer, providerName, "wss://operator", "https://user"} {
		if strings.Contains(raw, secret) {
			t.Fatalf("operator log leaked %q: %s", secret, raw)
		}
	}
	if !operatorLogContains(entries, "provider_launch_failed", nil) {
		t.Fatalf("operator log missing structured error event: %s", raw)
	}
}

func TestRecordOperatorPreparationOutcomeUsesPrivateStructuredLog(t *testing.T) {
	layout := testLayout(t, "operator-log-wrapper-preparation")
	if err := RecordOperatorInfo(layout, "infrastructure_prepared", map[string]any{
		"provider":       "codex",
		"project_dir":    "/tmp/project",
		"artifact_kinds": []string{"codex-config", "project-instructions"},
	}); err != nil {
		t.Fatal(err)
	}
	entries, raw := readOperatorLogEntries(t, layout.OperatorLogFile)
	if len(entries) != 1 || entries[0].Event != "infrastructure_prepared" {
		t.Fatalf("entries = %#v\n%s", entries, raw)
	}
	if entries[0].Fields["provider"] != "codex" {
		t.Fatalf("fields = %#v", entries[0].Fields)
	}
}

func readOperatorLogEntries(t *testing.T, path string) ([]operatorLogEntry, string) {
	t.Helper()
	paths := []string{path + ".2", path + ".1", path}
	var entries []operatorLogEntry
	var raw strings.Builder
	for _, candidate := range paths {
		data, err := os.ReadFile(candidate)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", candidate, err)
		}
		raw.Write(data)
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			// Decode numbers as json.Number, not float64: fmt.Sprint of a
			// float64 switches to exponent form at 1e6, so a 7-digit pid
			// would never match its integer text in operatorLogContains.
			var entry operatorLogEntry
			decoder := json.NewDecoder(strings.NewReader(line))
			decoder.UseNumber()
			if err := decoder.Decode(&entry); err != nil {
				t.Fatalf("invalid JSONL %s: %v\n%s", candidate, err, line)
			}
			if entry.Timestamp.IsZero() || entry.Event == "" || entry.Level == "" {
				t.Fatalf("incomplete operator log entry: %#v", entry)
			}
			entries = append(entries, entry)
		}
	}
	return entries, raw.String()
}

func operatorLogContains(entries []operatorLogEntry, event string, wanted map[string]string) bool {
	for _, entry := range entries {
		if entry.Event != event {
			continue
		}
		matched := true
		for key, value := range wanted {
			if fmt.Sprint(entry.Fields[key]) != value {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func TestOperatorLogContainsMatchesLargeIntegerFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operator.log")
	line := `{"timestamp":"2026-10-12T00:00:00Z","level":"info","event":"stale_instance_takeover","board_key":"test","fields":{"old_pid":1056279,"new_pid":42}}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, raw := readOperatorLogEntries(t, path)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1:\n%s", len(entries), raw)
	}
	if !operatorLogContains(entries, "stale_instance_takeover", map[string]string{"old_pid": "1056279", "new_pid": "42"}) {
		t.Fatalf("7-digit pid did not match its integer text:\n%s", raw)
	}
	if operatorLogContains(entries, "stale_instance_takeover", map[string]string{"old_pid": "1056278"}) {
		t.Fatalf("wrong pid matched:\n%s", raw)
	}
}
