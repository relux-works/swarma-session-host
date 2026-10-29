package sessioncontext

import "time"

type ContextConsumerObservationStatus string

const ContextConsumerObservationPartial ContextConsumerObservationStatus = "partial"

type ContextForkTelemetry struct {
	ContractVersion string                           `json:"contract_version"`
	CtxID           string                           `json:"ctx_id"`
	RunID           string                           `json:"run_id,omitempty"`
	Role            string                           `json:"role,omitempty"`
	Provider        ContextProvider                  `json:"provider"`
	Status          ContextConsumerObservationStatus `json:"status"`
	Observable      bool                             `json:"observable"`

	AnyRepositoryRead   bool `json:"any_repository_read"`
	DistinctPaths       int  `json:"distinct_paths"`
	InsideBoundaryReads int  `json:"inside_boundary_reads"`
	// OutsideBoundaryReads is distinct paths the base held no content evidence
	// for. These are the reads the base failed to save.
	OutsideBoundaryReads int `json:"outside_boundary_reads"`
	// SpotChecks is distinct in-boundary paths the fork re-read. They are the
	// cheap, expected behaviour — a run verifying a claim it is about to act on —
	// and are counted separately so they never inflate the economic warning.
	SpotChecks         int `json:"spot_checks"`
	UnclassifiedAccess int `json:"unclassified_access"`
	// WriteAccesses is repository writes and edits. It is reported so the row is
	// not silently lossy, and it feeds no read metric: a write is evidence that
	// the consumer changed a path, never that it had to go and learn one.
	WriteAccesses int `json:"write_accesses,omitempty"`
	// ReExplorationEpisode is one fork that had to go outside the boundary at
	// all. It is a boolean rather than a count because CV-6's threshold is a rate
	// of forks, not a rate of reads: a fork that read thirty uncovered files had
	// one bad base, not thirty.
	ReExplorationEpisode bool `json:"re_exploration_episode"`
	// ExtraReads is how many outside-boundary paths that episode cost.
	ExtraReads int `json:"extra_reads"`
	// AssayAbstentions is how many assay questions this fork's base could not
	// answer. It travels with the telemetry because an abstaining base and a
	// re-exploring consumer are the same gap seen from two sides.
	AssayAbstentions int `json:"assay_abstentions"`
	// OutsidePrefixes is the top-level areas the fork went to. The widening
	// recommendation is keyed on these.
	OutsidePrefixes []string  `json:"outside_prefixes,omitempty"`
	ObservedAt      time.Time `json:"observed_at"`
}

// ContextForkTelemetryContract is the folded per-fork row.
const ContextForkTelemetryContract = "context-fork-telemetry-v1"

const ContextConsumerObservationComplete ContextConsumerObservationStatus = "complete"
const ContextConsumerObservationUnavailable ContextConsumerObservationStatus = "unavailable"
