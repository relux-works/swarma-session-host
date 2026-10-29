package hostdefaults

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func parseRetention(t *testing.T, body string) (*ContextRetentionConfig, error) {
	t.Helper()
	fields, err := object(json.RawMessage(body), true)
	if err != nil {
		return nil, err
	}
	raw, found := fields["retention"]
	if !found {
		return nil, nil
	}
	return ParseContextRetentionConfig(raw)
}

// TestRetentionAcceptsEveryDocumentedKnob is the coverage check: all nine
// tunables must round-trip, or a knob documented in the reference would be one
// the runtime silently ignores.
func TestRetentionAcceptsEveryDocumentedKnob(t *testing.T) {
	config, err := parseRetention(t, `{"retention":{
		"ready_ttl":"14d",
		"stale_ttl":"72h",
		"building_ttl":"90m",
		"retired_ttl":"2d",
		"unpublished_materialization_ttl":"12h",
		"materialization_idle_ttl":"36h",
		"payload_ttl":"3d",
		"max_ready_per_binding":5,
		"max_descendants_per_base":16
	}}`)
	if err != nil {
		t.Fatalf("a fully configured retention section was refused: %v", err)
	}
	retention := config
	for field, want := range map[string]time.Duration{
		"ready_ttl":                       14 * 24 * time.Hour,
		"stale_ttl":                       72 * time.Hour,
		"building_ttl":                    90 * time.Minute,
		"retired_ttl":                     2 * 24 * time.Hour,
		"unpublished_materialization_ttl": 12 * time.Hour,
		"materialization_idle_ttl":        36 * time.Hour,
		"payload_ttl":                     3 * 24 * time.Hour,
	} {
		got, ok := retention.ResolvedDuration(field)
		if !ok || got != want {
			t.Fatalf("%s = %s (configured %t), want %s", field, got, ok, want)
		}
	}
	for field, want := range map[string]int{
		"max_ready_per_binding":    5,
		"max_descendants_per_base": 16,
	} {
		got, ok := retention.ResolvedCount(field)
		if !ok || got != want {
			t.Fatalf("%s = %d (configured %t), want %d", field, got, ok, want)
		}
	}
}

// TestAnUnsetKnobIsNotConfigured keeps the config layer from inventing a
// default. "Not configured" has to reach the runtime as such, because the
// runtime is the layer that owns what unset means.
func TestAnUnsetKnobIsNotConfigured(t *testing.T) {
	config, err := parseRetention(t, `{"retention":{"ready_ttl":"14d"}}`)
	if err != nil {
		t.Fatalf("a partial retention section was refused: %v", err)
	}
	retention := config
	if _, ok := retention.ResolvedDuration("stale_ttl"); ok {
		t.Fatalf("an unset knob was reported as configured")
	}
	if _, ok := retention.ResolvedCount("max_ready_per_binding"); ok {
		t.Fatalf("an unset cap was reported as configured")
	}
}

// TestRetentionRefusesValuesThatWouldEmptyTheRegistry is the typo gate.
//
// A zero or negative window is not a tighter policy; it expires everything on
// the first pass. A misspelled key is worse, because it reads as configured
// while nothing changed and sends an operator to debug the wrong layer.
func TestRetentionRefusesValuesThatWouldEmptyTheRegistry(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"a zero window", `{"retention":{"ready_ttl":"0s"}}`, "positive duration"},
		{"a negative window", `{"retention":{"stale_ttl":"-1h"}}`, "positive duration"},
		{"a zero cap", `{"retention":{"max_ready_per_binding":0}}`, "positive integer"},
		{"a negative cap", `{"retention":{"max_descendants_per_base":-4}}`, "positive integer"},
		{"a misspelled knob", `{"retention":{"ready_tll":"30d"}}`, "not recognized"},
		{"a window that is not a duration", `{"retention":{"payload_ttl":"soon"}}`, "duration"},
		{"a window given as a number", `{"retention":{"payload_ttl":7}}`, "duration"},
		{"a cap given as a string", `{"retention":{"max_ready_per_binding":"3"}}`, "positive integer"},
		{"retention that is not an object", `{"retention":[]}`, "object or null"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseRetention(t, test.body)
			if err == nil {
				t.Fatalf("the config layer admitted %s", test.name)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("refusal = %q, want it to name %q", err.Error(), test.want)
			}
		})
	}
}

// TestAbsentRetentionIsNotAnError keeps the section optional: a project that
// never mentions retention gets the documented defaults rather than a refusal.
func TestAbsentRetentionIsNotAnError(t *testing.T) {
	for _, body := range []string{`{}`, `{"retention":null}`, `null`} {
		config, err := parseRetention(t, body)
		if err != nil {
			t.Fatalf("an absent retention section was refused (%s): %v", body, err)
		}
		if config != nil {
			t.Fatalf("an absent retention section produced a policy (%s)", body)
		}
	}
}

// TestDaySuffixIsAcceptedAlongsideGoDurations exists because every documented
// default above six hours is expressed in days, and making an operator write
// "720h" for thirty days is how a value gets mistyped by a factor of ten.
func TestDaySuffixIsAcceptedAlongsideGoDurations(t *testing.T) {
	for spelling, want := range map[string]time.Duration{
		"30d":  30 * 24 * time.Hour,
		"720h": 30 * 24 * time.Hour,
		"0.5d": 12 * time.Hour,
		"90m":  90 * time.Minute,
	} {
		got, err := ParseContextRetentionDuration(spelling)
		if err != nil || got != want {
			t.Fatalf("%q parsed as %s (%v), want %s", spelling, got, err, want)
		}
	}
}
