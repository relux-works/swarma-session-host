package hostdefaults

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Retention is configured as nine independent values with documented defaults
// because tuning it must never be a schema change.
//
// The measured economics behind this feature is explicit that the numbers and
// the model have different owners: storage sits four to five orders of magnitude
// below the token saving, so disk is not what expires a context — staleness and
// secret exposure are. Which means an operator will want to move these values,
// and every move has to be a config edit rather than a redesign.
//
// The parse is strict in both directions. An unrecognized key is refused rather
// than ignored, because a misspelled ready_ttl that fell through to the default
// would read as configured while nothing changed. And a zero or negative value
// is refused rather than accepted, because "0s" would expire the whole registry
// on the first sweep and would look like a policy decision instead of a typo.

// ContextRetentionConfig is the project's context retention and GC policy.
//
// Every field is a pointer so "not configured" stays distinguishable from a
// configured value. The runtime resolves an absent field to its documented
// default; it never resolves it to the zero duration.
type ContextRetentionConfig struct {
	ReadyTTL                      *string `json:"ready_ttl,omitempty"`
	StaleTTL                      *string `json:"stale_ttl,omitempty"`
	BuildingTTL                   *string `json:"building_ttl,omitempty"`
	RetiredTTL                    *string `json:"retired_ttl,omitempty"`
	UnpublishedMaterializationTTL *string `json:"unpublished_materialization_ttl,omitempty"`
	MaterializationIdleTTL        *string `json:"materialization_idle_ttl,omitempty"`
	PayloadTTL                    *string `json:"payload_ttl,omitempty"`
	MaxReadyPerBinding            *int    `json:"max_ready_per_binding,omitempty"`
	MaxDescendantsPerBase         *int    `json:"max_descendants_per_base,omitempty"`
}

// ContextRetentionDurationFields are the seven duration knobs, in the order the
// reference documents them.
var ContextRetentionDurationFields = []string{
	"ready_ttl",
	"stale_ttl",
	"building_ttl",
	"retired_ttl",
	"unpublished_materialization_ttl",
	"materialization_idle_ttl",
	"payload_ttl",
}

// ContextRetentionCountFields are the two cap knobs.
var ContextRetentionCountFields = []string{
	"max_ready_per_binding",
	"max_descendants_per_base",
}

// ResolvedDuration returns one configured duration, or false when the project
// left it to the runtime default.
//
// It deliberately does not know the defaults. Those live with the sweep that
// applies them, and duplicating them here is how a config layer and a runtime
// layer end up disagreeing about what "unset" means.
func (r *ContextRetentionConfig) ResolvedDuration(field string) (time.Duration, bool) {
	if r == nil {
		return 0, false
	}
	var raw *string
	switch field {
	case "ready_ttl":
		raw = r.ReadyTTL
	case "stale_ttl":
		raw = r.StaleTTL
	case "building_ttl":
		raw = r.BuildingTTL
	case "retired_ttl":
		raw = r.RetiredTTL
	case "unpublished_materialization_ttl":
		raw = r.UnpublishedMaterializationTTL
	case "materialization_idle_ttl":
		raw = r.MaterializationIdleTTL
	case "payload_ttl":
		raw = r.PayloadTTL
	default:
		return 0, false
	}
	if raw == nil {
		return 0, false
	}
	parsed, err := ParseContextRetentionDuration(*raw)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

// ResolvedCount returns one configured cap, or false when it was left unset.
func (r *ContextRetentionConfig) ResolvedCount(field string) (int, bool) {
	if r == nil {
		return 0, false
	}
	switch field {
	case "max_ready_per_binding":
		if r.MaxReadyPerBinding == nil {
			return 0, false
		}
		return *r.MaxReadyPerBinding, true
	case "max_descendants_per_base":
		if r.MaxDescendantsPerBase == nil {
			return 0, false
		}
		return *r.MaxDescendantsPerBase, true
	default:
		return 0, false
	}
}

// ParseContextRetentionDuration reads a retention window.
//
// It accepts Go's own duration spelling and adds a day suffix, because every
// documented default above six hours is expressed in days and forcing an
// operator to write "720h" for thirty days is how a config value gets mistyped
// by a factor of ten.
func ParseContextRetentionDuration(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, fmt.Errorf("must not be empty")
	}
	if strings.HasSuffix(trimmed, "d") {
		days, err := strconv.ParseFloat(strings.TrimSuffix(trimmed, "d"), 64)
		if err != nil {
			return 0, fmt.Errorf("%q is not a valid day count", raw)
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	parsed, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf(`%q is not a duration such as "30d", "6h" or "90m"`, raw)
	}
	return parsed, nil
}

// parseContextRetention reads the retention section.
func parseContextRetention(section string, raw json.RawMessage) (*ContextRetentionConfig, error) {
	if isRetentionNull(raw) {
		return nil, nil
	}
	fields, err := object(raw, true)
	if err != nil || fields == nil {
		return nil, newRetentionConfigError(section, fmt.Sprintf("must be an object or null: %v", err))
	}
	config := &ContextRetentionConfig{}
	durations := map[string]**string{
		"ready_ttl":                       &config.ReadyTTL,
		"stale_ttl":                       &config.StaleTTL,
		"building_ttl":                    &config.BuildingTTL,
		"retired_ttl":                     &config.RetiredTTL,
		"unpublished_materialization_ttl": &config.UnpublishedMaterializationTTL,
		"materialization_idle_ttl":        &config.MaterializationIdleTTL,
		"payload_ttl":                     &config.PayloadTTL,
	}
	counts := map[string]**int{
		"max_ready_per_binding":    &config.MaxReadyPerBinding,
		"max_descendants_per_base": &config.MaxDescendantsPerBase,
	}
	for _, field := range keys(fields) {
		fieldSection := section + "." + field
		if target, ok := durations[field]; ok {
			value, err := parseContextRetentionDurationField(fieldSection, fields[field])
			if err != nil {
				return nil, err
			}
			*target = value
			continue
		}
		if target, ok := counts[field]; ok {
			value, err := parseContextRetentionCountField(fieldSection, fields[field])
			if err != nil {
				return nil, err
			}
			*target = value
			continue
		}
		return nil, newRetentionConfigError(fieldSection, "context retention config key is not recognized")
	}
	return config, nil
}

func parseContextRetentionDurationField(section string, raw json.RawMessage) (*string, error) {
	if isRetentionNull(raw) {
		return nil, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, newRetentionConfigError(
			section, `must be a duration string such as "30d", "6h" or "90m"`)
	}
	parsed, err := ParseContextRetentionDuration(value)
	if err != nil {
		return nil, newRetentionConfigError(section, err.Error())
	}
	if parsed <= 0 {
		// A zero window is not a shorter window; it is "expire everything on the
		// next pass". Refusing it keeps a typo from reading as a policy.
		return nil, newRetentionConfigError(
			section, fmt.Sprintf("must be a positive duration, got %q", value))
	}
	stored := strings.TrimSpace(value)
	return &stored, nil
}

func parseContextRetentionCountField(section string, raw json.RawMessage) (*int, error) {
	if isRetentionNull(raw) {
		return nil, nil
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, newRetentionConfigError(section, "must be a positive integer")
	}
	if value <= 0 {
		// Zero would make the cap retire every ready snapshot in the group, which
		// is a registry with no contexts in it rather than a tighter policy.
		return nil, newRetentionConfigError(
			section, fmt.Sprintf("must be a positive integer, got %d", value))
	}
	return &value, nil
}

// ParseContextRetentionConfig validates the actual optional retention section.
// It retains unset fields for the context runtime to resolve; it does not apply
// a policy, discover project configuration or grant collection authority.
func ParseContextRetentionConfig(raw json.RawMessage) (*ContextRetentionConfig, error) {
	return parseContextRetention("contexts.retention", raw)
}

func isRetentionNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}

func newRetentionConfigError(key, detail string) error {
	return fmt.Errorf("host configuration %s: %s", key, detail)
}
