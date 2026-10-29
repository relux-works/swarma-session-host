package sessioncontext

import (
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"time"
)

const (
	// DefaultContextReadyTTL bounds how long a published base stays selectable
	// without being re-selected. It is long because expiry is about secret
	// exposure and drift, not disk.
	DefaultContextReadyTTL = 30 * 24 * time.Hour
	// DefaultContextStaleTTL bounds how long an invalidated snapshot is kept for
	// inspection. A stale record is the only place the evidence of a drift lives,
	// so it outlives the drift by a week rather than vanishing with it.
	DefaultContextStaleTTL = 7 * 24 * time.Hour
	// DefaultContextBuildingTTL bounds an unfinished provisional record. It is
	// short: a building CTX that outlives its run is a crashed manager, and
	// leaving it in place would let it accumulate as permanent debris that no
	// completion will ever resolve.
	DefaultContextBuildingTTL = 6 * time.Hour
	// DefaultContextRetiredTTL is the grace period between a retirement and the
	// deletion of the local provider handle behind it. It exists so a retirement
	// that turns out to be wrong can be diagnosed before the bytes are gone.
	DefaultContextRetiredTTL = 24 * time.Hour
	// DefaultContextUnpublishedMaterializationTTL bounds a detached successful
	// session that was never published. The window is what makes a deliberate
	// reattach possible; past it the transcript is unreachable debris that still
	// holds whatever the run read.
	DefaultContextUnpublishedMaterializationTTL = 24 * time.Hour
	// DefaultContextMaterializationIdleTTL bounds a local handle nothing has used.
	// The registry record survives it: dropping an idle handle costs a rebuild or
	// a fetch, never a lineage.
	DefaultContextMaterializationIdleTTL = 24 * time.Hour
	// DefaultContextPayloadTTL bounds how long retired payload bytes may remain in
	// the cross-machine payload store.
	DefaultContextPayloadTTL = 7 * 24 * time.Hour
	// DefaultContextMaxReadyPerBinding bounds how many historical snapshots
	// compete for one selection group.
	DefaultContextMaxReadyPerBinding = 3
	// DefaultContextMaxDescendantsPerBase bounds one base's ready fork fan-out.
	DefaultContextMaxDescendantsPerBase = 64
)

func DefaultContextRetentionSettings() sm.ContextRetentionSettings {
	return sm.ContextRetentionSettings{
		ReadyTTL:                      DefaultContextReadyTTL,
		StaleTTL:                      DefaultContextStaleTTL,
		BuildingTTL:                   DefaultContextBuildingTTL,
		RetiredTTL:                    DefaultContextRetiredTTL,
		UnpublishedMaterializationTTL: DefaultContextUnpublishedMaterializationTTL,
		MaterializationIdleTTL:        DefaultContextMaterializationIdleTTL,
		PayloadTTL:                    DefaultContextPayloadTTL,
		MaxReadyPerBinding:            DefaultContextMaxReadyPerBinding,
		MaxDescendantsPerBase:         DefaultContextMaxDescendantsPerBase,
	}
}

// Normalized fills every unset field with its documented default.
//
// Zero means "not configured", never "zero seconds". The distinction is the
// whole safety property: a settings struct built by a caller that only cared
// about one knob would otherwise expire every context in the registry on its
// first sweep, and the failure would look like a policy decision rather than a
// missing field.
func normalizeRetentionSettings(s sm.ContextRetentionSettings) sm.ContextRetentionSettings {
	defaults := DefaultContextRetentionSettings()
	if s.ReadyTTL <= 0 {
		s.ReadyTTL = defaults.ReadyTTL
	}
	if s.StaleTTL <= 0 {
		s.StaleTTL = defaults.StaleTTL
	}
	if s.BuildingTTL <= 0 {
		s.BuildingTTL = defaults.BuildingTTL
	}
	if s.RetiredTTL <= 0 {
		s.RetiredTTL = defaults.RetiredTTL
	}
	if s.UnpublishedMaterializationTTL <= 0 {
		s.UnpublishedMaterializationTTL = defaults.UnpublishedMaterializationTTL
	}
	if s.MaterializationIdleTTL <= 0 {
		s.MaterializationIdleTTL = defaults.MaterializationIdleTTL
	}
	if s.PayloadTTL <= 0 {
		s.PayloadTTL = defaults.PayloadTTL
	}
	if s.MaxReadyPerBinding <= 0 {
		s.MaxReadyPerBinding = defaults.MaxReadyPerBinding
	}
	if s.MaxDescendantsPerBase <= 0 {
		s.MaxDescendantsPerBase = defaults.MaxDescendantsPerBase
	}
	return s
}

func FormatContextRetentionDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	}
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return d.String()
}
