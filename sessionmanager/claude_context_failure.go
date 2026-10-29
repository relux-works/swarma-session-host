package sessionmanager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ClaudeContextFailure describes a provider mechanism refusal. Canonical board
// error typing and authority transitions belong to the private consumer.
type ClaudeContextFailure struct {
	Code    string
	Message string
	Cause   error
}

func (e *ClaudeContextFailure) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return e.Code + ": " + e.Message
}
func (e *ClaudeContextFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type ClaudeTranscriptCompaction struct {
	Provider  string
	Marker    string
	Timestamp string
}

// ClaudePayloadPresenceError retains the actual probe and original publication
// anchor. It does not apply canonical retention policy or mark a registry stale.
type ClaudePayloadPresenceError struct {
	ContextID string
	ReadyAt   time.Time
	Mode      ContextResolutionMode
	CheckedAt time.Time
	Presence  string
	Detail    string
}

func (e *ClaudePayloadPresenceError) Error() string {
	return fmt.Sprintf("Claude context %s payload %s: %s", e.ContextID, e.Presence, e.Detail)
}

func requireClaudePayloadPresence(id, path string, ready time.Time, mode ContextResolutionMode, at time.Time) error {
	f := &ClaudePayloadPresenceError{ContextID: id, ReadyAt: ready, Mode: mode, CheckedAt: at.UTC(), Presence: "unverifiable"}
	if strings.TrimSpace(path) == "" {
		f.Detail = "the base transcript path could not be derived on this machine"
		return f
	}
	switch info, err := os.Stat(path); {
	case err == nil && info.IsDir():
		f.Detail = "the base transcript path resolves to a directory"
	case err == nil:
		return nil
	case errors.Is(err, os.ErrNotExist):
		f.Presence = "vanished"
		f.Detail = "the provider session directory holds no transcript for this base session id"
	default:
		f.Detail = err.Error()
	}
	return f
}

// ClaudeResumeAttempt describes an actual failed provider call, never predicts
// compatibility or grants canonical recovery. The original cause stays intact.
type ClaudeResumeAttempt struct {
	CtxID           string
	Provider        string
	RecordedVersion string
	ActiveVersion   string
	Operation       string
	Cause           error
}

func (e *ClaudeResumeAttempt) Error() string {
	return fmt.Sprintf("Claude context %s resume operation %s failed: %v", e.CtxID, e.Operation, e.Cause)
}
func (e *ClaudeResumeAttempt) Unwrap() error           { return e.Cause }
func claudeResumeUnusable(f ClaudeResumeAttempt) error { return &f }

// Canonical CTX spelling is validated by the private conversion. The provider
// only accepts an immutable opaque safe path component for its local artifacts.
func normalizeClaudeContextKey(id string) string { return strings.TrimSpace(id) }
func validateClaudeContextKey(id string) error {
	if id == "" || id != strings.TrimSpace(id) || id == "." || id == ".." || filepath.Base(id) != id || strings.ContainsAny(id, "/\\\x00") {
		return fmt.Errorf("invalid Claude context artifact key")
	}
	return nil
}
