package sessionmanager

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Nil and allocated-empty fields both accept a real cause; no-cause events keep
// their existing shape. Populated caller maps remain unchanged while persisted
// fields/cause are redacted and private. These are local file I/O checks only.
func TestRecordOperatorErrorNilAndEmptyFields(t *testing.T) {
	for _, fieldsKind := range []string{"nil", "empty", "populated"} {
		for _, hasCause := range []bool{false, true} {
			name := fieldsKind + "/no-cause"
			if hasCause {
				name = fieldsKind + "/cause"
			}
			t.Run(name, func(t *testing.T) {
				defer func() {
					if p := recover(); p != nil {
						t.Fatalf("error logging must not panic for %s: %v", name, p)
					}
				}()
				var fields map[string]any
				if fieldsKind == "empty" {
					fields = map[string]any{}
				}
				if fieldsKind == "populated" {
					fields = map[string]any{"stage": "close", "authorization": "fixture-private-field", "nested": map[string]any{"password": "fixture-private-nested"}}
				}
				before := map[string]any(nil)
				if fields != nil {
					before = map[string]any{}
					for k, v := range fields {
						before[k] = v
					}
					if fieldsKind == "populated" {
						before["nested"] = map[string]any{"password": "fixture-private-nested"}
					}
				}
				var cause error
				if hasCause {
					cause = errors.New("observation unavailable token=fixture-private-cause")
				}
				layout := testLayout(t, "nil-fields")
				if err := RecordOperatorError(layout, "builder_gateway_close_failed", cause, fields); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(fields, before) {
					t.Fatalf("caller fields mutated: %#v", fields)
				}
				entries, raw := readOperatorLogEntries(t, layout.OperatorLogFile)
				if len(entries) != 1 || entries[0].Event != "builder_gateway_close_failed" || entries[0].Level != "error" {
					t.Fatalf("missing error event: %s", raw)
				}
				got := entries[0].Fields
				if hasCause {
					if text, ok := got["cause"].(string); !ok || !strings.Contains(text, "observation unavailable") || got["cause_type"] != "*errors.errorString" {
						t.Fatalf("missing real sanitized cause: %#v", got)
					}
				} else if _, exists := got["cause"]; exists {
					t.Fatalf("nil cause invented: %#v", got)
				}
				if !hasCause {
					if _, exists := got["cause_type"]; exists {
						t.Fatalf("nil cause type invented: %#v", got)
					}
				}
				if fieldsKind == "populated" {
					if got["stage"] != "close" || got["authorization"] != "[REDACTED]" {
						t.Fatalf("field semantics lost: %#v", got)
					}
				} else if !hasCause && len(got) != 0 {
					t.Fatalf("empty event gained fields: %#v", got)
				}
				for _, forbidden := range []string{"fixture-private-field", "fixture-private-nested", "fixture-private-cause"} {
					if strings.Contains(raw, forbidden) {
						t.Fatalf("private fixture value leaked: %s", raw)
					}
				}
				for path, mode := range map[string]os.FileMode{layout.OperatorLogFile: 0600, filepath.Dir(layout.OperatorLogFile): 0700} {
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if info.Mode().Perm() != mode {
						t.Fatalf("private log mode = %o, want %o", info.Mode().Perm(), mode)
					}
				}
			})
		}
	}
}

// A real unusable log target must return its filesystem failure, including when
// cause fields need allocation; nil logger remains an explicit refusal.
func TestRecordOperatorErrorNilFieldsPreservesIOFailure(t *testing.T) {
	layout := testLayout(t, "write-failure")
	if err := os.MkdirAll(layout.OperatorLogFile, 0700); err != nil {
		t.Fatal(err)
	}
	err := RecordOperatorError(layout, "builder_gateway_close_failed", errors.New("observation unavailable"), nil)
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) || !strings.Contains(err.Error(), "opening operator log") {
		t.Fatalf("real filesystem failure lost: %v", err)
	}
	var logger *operatorLogger
	if err := logger.writeChecked("error", "close", errors.New("unavailable"), nil); err == nil || err.Error() != "operator logger is required" {
		t.Fatalf("nil logger accepted: %v", err)
	}
}
