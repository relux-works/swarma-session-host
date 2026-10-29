package hostdefaults

import (
	"context"
	"fmt"

	"github.com/relux-works/swarma-session-host/sessioncontext"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

var _ sessioncontext.RetentionConfig = FileConfig{}

// RetentionSnapshot is the original nine-knob command-side mapping, after the
// real file parser succeeds. Known is never manufactured after a read/parse
// error; zero fields mean verified unset, and the service supplies defaults.
func (f FileConfig) RetentionSnapshot(ctx context.Context) (sessioncontext.RetentionConfigSnapshot, error) {
	if ctx == nil {
		return sessioncontext.RetentionConfigSnapshot{}, fmt.Errorf("retention request context required")
	}
	loaded, err := f.Load(ctx)
	if err != nil {
		return sessioncontext.RetentionConfigSnapshot{}, err
	}
	result := sessioncontext.RetentionConfigSnapshot{Known: true}
	r := loaded.Retention
	if r == nil {
		return result, nil
	}
	if value, ok := r.ResolvedDuration("ready_ttl"); ok {
		result.Settings.ReadyTTL = value
	}
	if value, ok := r.ResolvedDuration("stale_ttl"); ok {
		result.Settings.StaleTTL = value
	}
	if value, ok := r.ResolvedDuration("building_ttl"); ok {
		result.Settings.BuildingTTL = value
	}
	if value, ok := r.ResolvedDuration("retired_ttl"); ok {
		result.Settings.RetiredTTL = value
	}
	if value, ok := r.ResolvedDuration("unpublished_materialization_ttl"); ok {
		result.Settings.UnpublishedMaterializationTTL = value
	}
	if value, ok := r.ResolvedDuration("materialization_idle_ttl"); ok {
		result.Settings.MaterializationIdleTTL = value
	}
	if value, ok := r.ResolvedDuration("payload_ttl"); ok {
		result.Settings.PayloadTTL = value
	}
	if value, ok := r.ResolvedCount("max_ready_per_binding"); ok {
		result.Settings.MaxReadyPerBinding = value
	}
	if value, ok := r.ResolvedCount("max_descendants_per_base"); ok {
		result.Settings.MaxDescendantsPerBase = value
	}
	return result, nil
}

// This concrete configuration is private to the constructor that installs BOTH
// HostConfig and the actual context RetentionConfig. The public FileConfig
// Snapshot keeps its original unbound-retention refusal.
type composedFileConfig struct{ FileConfig }

func (f composedFileConfig) Snapshot(ctx context.Context) (core.HostSettings, error) {
	loaded, err := f.Load(ctx)
	if err != nil {
		return core.HostSettings{}, err
	}
	return loaded.Host, nil
}
