package sessioncontext

import (
	"context"
	"fmt"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// RetentionConfig supplies the original project's nine optional retention knobs
// after the owning configuration parser has validated them. It must return an
// error for malformed/unknown keys or explicitly configured nonpositive values;
// these must not be collapsed into an unset field. No board authority is implied.
type RetentionConfig interface {
	RetentionSnapshot(context.Context) (RetentionConfigSnapshot, error)
}

// RetentionConfigSnapshot separates a successfully read, possibly absent policy
// from unavailable configuration. Known is true only after successful source
// validation. In Settings, zero means verified unset and a positive value means
// configured; the parser owns the distinction between explicit zero and absence.
type RetentionConfigSnapshot struct {
	Known    bool
	Settings sm.ContextRetentionSettings
}

// ContextRetentionSettings performs the original command-side configuration
// resolution: configured values, then runtime defaults for each unset field.
// A nil Options.RetentionConfig explicitly selects the shipped defaults. An
// installed but unavailable source refuses; it never silently selects defaults.
// Callers must propagate this error before constructing a configured sweep.
// SweepContextRetention continues to normalize its whole explicit request; this
// method does not introduce a second configuration/request precedence rule.
func (m *Service) ContextRetentionSettings(ctx context.Context) (sm.ContextRetentionSettings, error) {
	if err := ctx.Err(); err != nil {
		return sm.ContextRetentionSettings{}, err
	}
	if m.retentionConfig == nil {
		return DefaultContextRetentionSettings(), nil
	}
	snapshot, err := m.retentionConfig.RetentionSnapshot(ctx)
	if err != nil {
		return sm.ContextRetentionSettings{}, fmt.Errorf("read retention configuration: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return sm.ContextRetentionSettings{}, err
	}
	if !snapshot.Known {
		return sm.ContextRetentionSettings{}, fmt.Errorf("retention configuration visibility unavailable")
	}
	settings := snapshot.Settings
	for _, field := range []struct {
		name  string
		value time.Duration
	}{
		{"ready_ttl", settings.ReadyTTL},
		{"stale_ttl", settings.StaleTTL},
		{"building_ttl", settings.BuildingTTL},
		{"retired_ttl", settings.RetiredTTL},
		{"unpublished_materialization_ttl", settings.UnpublishedMaterializationTTL},
		{"materialization_idle_ttl", settings.MaterializationIdleTTL},
		{"payload_ttl", settings.PayloadTTL},
	} {
		if field.value < 0 {
			return sm.ContextRetentionSettings{}, fmt.Errorf("retention configuration %s must be positive when configured", field.name)
		}
	}
	for _, field := range []struct {
		name  string
		value int
	}{
		{"max_ready_per_binding", settings.MaxReadyPerBinding},
		{"max_descendants_per_base", settings.MaxDescendantsPerBase},
	} {
		if field.value < 0 {
			return sm.ContextRetentionSettings{}, fmt.Errorf("retention configuration %s must be positive when configured", field.name)
		}
	}
	return normalizeRetentionSettings(settings), nil
}
