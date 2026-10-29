package sessionmanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	sessionManagerLogFileName = "sessiond.log"
	defaultOperatorLogMaxSize = 1 << 20
	defaultOperatorLogBackups = 4
)

// operatorLogSink deliberately accepts only explicit event fields. Manager
// callers must never pass launch envelopes or provider snapshots: they may
// contain endpoints, tokens, and provider-private identities.
type operatorLogSink interface {
	Info(event string, fields map[string]any)
	Error(event string, cause error, fields map[string]any)
}

type operatorLogEntry struct {
	Timestamp time.Time      `json:"timestamp"`
	Level     string         `json:"level"`
	Event     string         `json:"event"`
	BoardKey  string         `json:"board_key"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// operatorLogger writes one private JSON object per line. Opening the file
// for each write keeps rotation reliable even when a daemon is restarted
// between events; every accepted line is synced before the lifecycle call
// returns, so a failed startup remains diagnosable immediately.
type operatorLogger struct {
	mu         sync.Mutex
	path       string
	boardKey   string
	maxSize    int64
	maxBackups int
	now        func() time.Time
}

func newOperatorLogger(layout Layout) (*operatorLogger, error) {
	if strings.TrimSpace(layout.OperatorLogFile) == "" {
		return nil, fmt.Errorf("session manager operator log path is required")
	}
	if err := os.MkdirAll(filepath.Dir(layout.OperatorLogFile), 0o700); err != nil {
		return nil, fmt.Errorf("creating session manager log directory: %w", err)
	}
	if err := os.Chmod(filepath.Dir(layout.OperatorLogFile), 0o700); err != nil {
		return nil, fmt.Errorf("securing session manager log directory: %w", err)
	}
	return &operatorLogger{
		path:       layout.OperatorLogFile,
		boardKey:   string(layout.Namespace),
		maxSize:    defaultOperatorLogMaxSize,
		maxBackups: defaultOperatorLogBackups,
		now:        func() time.Time { return time.Now().UTC() },
	}, nil
}

func (l *operatorLogger) Info(event string, fields map[string]any) {
	_ = l.writeChecked("info", event, nil, fields)
}

func (l *operatorLogger) Error(event string, cause error, fields map[string]any) {
	_ = l.writeChecked("error", event, cause, fields)
}

// RecordOperatorInfo appends one sanitized wrapper-side event to the same
// private operator log used by the Session Manager daemon.
func RecordOperatorInfo(layout Layout, event string, fields map[string]any) error {
	logger, err := newOperatorLogger(layout)
	if err != nil {
		return err
	}
	return logger.writeChecked("info", event, nil, fields)
}

// RecordOperatorError is the error-level counterpart to RecordOperatorInfo.
func RecordOperatorError(layout Layout, event string, cause error, fields map[string]any) error {
	logger, err := newOperatorLogger(layout)
	if err != nil {
		return err
	}
	return logger.writeChecked("error", event, cause, fields)
}

func (l *operatorLogger) writeChecked(level, event string, cause error, fields map[string]any) error {
	if l == nil {
		return fmt.Errorf("operator logger is required")
	}
	entryFields := cloneAndRedactLogFields(fields)
	if cause != nil {
		if entryFields == nil {
			entryFields = make(map[string]any, 2)
		}
		entryFields["cause"] = redactOperatorLogText(cause.Error())
		entryFields["cause_type"] = fmt.Sprintf("%T", cause)
	}
	entry := operatorLogEntry{
		Timestamp: l.now().UTC(),
		Level:     level,
		Event:     redactOperatorLogText(event),
		BoardKey:  l.boardKey,
		Fields:    entryFields,
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encoding operator log entry: %w", err)
	}
	line = append(line, '\n')

	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.rotateIfNeededLocked(int64(len(line))); err != nil {
		return fmt.Errorf("rotating operator log: %w", err)
	}
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("opening operator log: %w", err)
	}
	defer file.Close()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("securing operator log: %w", err)
	}
	if _, err := file.Write(line); err != nil {
		return fmt.Errorf("writing operator log: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing operator log: %w", err)
	}
	return nil
}

func (l *operatorLogger) rotateIfNeededLocked(incomingSize int64) error {
	if l.maxSize <= 0 {
		return nil
	}
	info, err := os.Stat(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Size()+incomingSize <= l.maxSize {
		return nil
	}
	if l.maxBackups <= 0 {
		return os.Remove(l.path)
	}
	oldest := fmt.Sprintf("%s.%d", l.path, l.maxBackups)
	if err := os.Remove(oldest); err != nil && !os.IsNotExist(err) {
		return err
	}
	for index := l.maxBackups - 1; index >= 1; index-- {
		from := fmt.Sprintf("%s.%d", l.path, index)
		to := fmt.Sprintf("%s.%d", l.path, index+1)
		if err := os.Rename(from, to); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.Rename(l.path, l.path+".1")
}

func cloneAndRedactLogFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	result := make(map[string]any, len(fields))
	for key, value := range fields {
		if isSensitiveOperatorLogField(key) {
			result[key] = "[REDACTED]"
			continue
		}
		result[key] = redactOperatorLogValue(value)
	}
	return result
}

func redactOperatorLogValue(value any) any {
	switch typed := value.(type) {
	case string:
		return redactOperatorLogText(typed)
	case error:
		return redactOperatorLogText(typed.Error())
	case []string:
		result := make([]string, len(typed))
		for index, item := range typed {
			result[index] = redactOperatorLogText(item)
		}
		return result
	case map[string]any:
		return cloneAndRedactLogFields(typed)
	default:
		return value
	}
}

// publicOperatorLogFields are field names whose values are stable taxonomy
// enums rather than free text.
//
// secret_class is the motivating case: the key contains "secret", so the
// substring rule would redact the classification an operator is reading the
// audit line to find. The value it holds is one of two fixed enum values that
// no transcript can influence, so redacting it costs information and protects
// nothing.
var publicOperatorLogFields = map[string]struct{}{
	"secret_class": {},
}

func isSensitiveOperatorLogField(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(key), "-", "_"))
	if _, public := publicOperatorLogFields[normalized]; public {
		return false
	}
	for _, marker := range []string{
		"token", "secret", "password", "authorization", "credential", "cookie", "api_key", "apikey", "endpoint", "url", "uri",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

// publicOperatorLogTokens are stable taxonomy values that survive redaction
// verbatim.
//
// The redactor is deliberately aggressive about anything containing "secret",
// which collides with a public failure code named context_secret_class_denied
// and with the secret_class enum itself. Redacting those turns the one line an
// operator needs into "[REDACTED]" while protecting nothing: every value here is
// a fixed enum defined in this repository, not something a transcript can
// supply. The allowlist is exact-match only, so a token that merely resembles a
// code is still redacted.
var publicOperatorLogTokens = map[string]struct{}{
	"ancestor_invalidated":                       {},
	"ancestry_drift":                             {},
	"building_abandoned":                         {},
	"building_failed":                            {},
	"classification_failed":                      {},
	"context_already_exists":                     {},
	"context_assay_isolation_unavailable":        {},
	"context_assay_tool_use_detected":            {},
	"context_assertion_unverified":               {},
	"context_binding_invalid":                    {},
	"context_builder_allowlist_denied":           {},
	"context_builder_confinement_unavailable":    {},
	"context_builder_tool_isolation_unavailable": {},
	"context_builder_trace_unavailable":          {},
	"context_classification_failed":              {},
	"context_compacted":                          {},
	"context_contaminated":                       {},
	"context_contract_invalid":                   {},
	"context_correctness_failed":                 {},
	"context_integrity_unverified":               {},
	"context_lineage_invalid":                    {},
	"context_materialization_divergent":          {},
	"context_not_found":                          {},
	"context_not_materialized":                   {},
	"context_not_ready":                          {},
	"context_payload_capability_required":        {},
	"context_payload_unverified":                 {},
	"context_payload_vanished":                   {},
	"context_persistence_failed":                 {},
	"context_pointer_invalid":                    {},
	"context_provider_incompatible":              {},
	"context_provider_mismatch":                  {},
	"context_publication_denied":                 {},
	"context_registry_unsupported":               {},
	"context_revision_conflict":                  {},
	"context_secret_class_denied":                {},
	"context_secret_detected":                    {},
	"context_stale":                              {},
	"context_state_invalid":                      {},
	"context_validation_budget_exceeded":         {},
	"context_validation_unavailable":             {},
	"element_closed":                             {},
	"freshness_drift":                            {},
	"payload_vanished":                           {},
	"provider_incompatible":                      {},
	"publication_not_requested":                  {},
	"restricted":                                 {},
	"retention_expired":                          {},
	"security_reclassification_required":         {},
	"superseded":                                 {},
	"transcript_compacted":                       {},
	"unrestricted":                               {},
	"validation_failed":                          {},
	"validation_overdue":                         {},
	"validation_refuted":                         {},
}

func isPublicOperatorLogToken(value string) bool {
	_, ok := publicOperatorLogTokens[strings.TrimSpace(value)]
	return ok
}

var (
	operatorLogURLPattern            = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s]+`)
	operatorLogSensitiveValuePattern = regexp.MustCompile(`(?i)(["']?(?:access[_-]?token|api[_-]?key|authorization|bearer|cookie|credential|password|secret|token)["']?\s*(?:=|:)\s*)([^\s,;"'}]+|"[^"]*"|'[^']*')`)
	operatorLogBearerPattern         = regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+`)
	operatorLogNamedSecretPattern    = regexp.MustCompile(`(?i)\b[A-Za-z0-9_-]*(?:token|secret|password|credential)[A-Za-z0-9_-]*\b`)
)

func redactOperatorLogText(value string) string {
	if isPublicOperatorLogToken(value) {
		return value
	}
	result := operatorLogURLPattern.ReplaceAllString(value, "[REDACTED_URL]")
	result = operatorLogSensitiveValuePattern.ReplaceAllString(result, "$1[REDACTED]")
	result = operatorLogBearerPattern.ReplaceAllString(result, "Bearer [REDACTED]")
	return operatorLogNamedSecretPattern.ReplaceAllString(result, "[REDACTED]")
}
