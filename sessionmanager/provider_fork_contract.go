package sessionmanager

import "time"

type ClaudeSupplementalContext struct {
	CtxID   string `json:"ctx_id"`
	Handoff string `json:"handoff"`
	// Raw opts this input into the format-guarded raw-history splice instead of
	// the distilled default. It is off unless a caller sets it and is refused
	// outside the verified transcript layout.
	Raw bool `json:"raw,omitempty"`
	// RawSessionUUID and RawProjectDir locate the session whose history is
	// spliced. Both are required when Raw is set.
	RawSessionUUID string `json:"raw_session_uuid,omitempty"`
	RawProjectDir  string `json:"raw_project_dir,omitempty"`
}

type ClaudeContextForkPlan struct {
	Contract string `json:"contract"`
	// ChildCtxID is the materialization being written and therefore the identity
	// the one-writer lease is taken on. It is deliberately not the primary: the
	// accepted research measured concurrent forks from one base as safe and named
	// them the only concurrency mechanism available, so a lease keyed on the base
	// would forbid exactly the case the design depends on.
	ChildCtxID   string `json:"child_ctx_id"`
	PrimaryCtxID string `json:"primary_ctx_id"`
	// BaseSessionUUID and BaseProjectDir locate the immutable base transcript.
	BaseSessionUUID string `json:"base_session_uuid"`
	BaseProjectDir  string `json:"base_project_dir"`
	// BaseSHA256 is the digest the base carried when the plan was made. It is
	// re-checked at materialization time and again after the copy, which is how
	// base immutability stops being an assumption.
	BaseSHA256  string `json:"base_sha256"`
	BaseRecords int    `json:"base_records"`
	// BaseFormatVersion is the provider version stamped on the base records.
	BaseFormatVersion string `json:"base_format_version"`
	// TargetProjectDir is the working directory the child will run in and the
	// only directory its materialization can be discovered from.
	TargetProjectDir string                      `json:"target_project_dir"`
	Supplemental     []ClaudeSupplementalContext `json:"supplemental,omitempty"`
	// PrimaryReadyAt is when the base was published. It is the anchor the 30-day
	// Claude deletion horizon is measured from, so it is what lets a vanished
	// payload be attributed to the provider's own timer instead of reported as an
	// unexplained absence.
	PrimaryReadyAt time.Time `json:"primary_ready_at,omitempty"`
	// PrimaryMode is the resolution that chose this base, carried so a refusal
	// keeps the right to fall through to the next ranked candidate.
	PrimaryMode ContextResolutionMode `json:"primary_mode,omitempty"`
	// BoundaryBanner is the rendered coverage statement composed onto the fork.
	//
	// It is on the plan rather than rebuilt at materialization time because the
	// banner must describe the base that was admitted, at the moment it was
	// admitted. Rebuilding it later would let a base drift between the check and
	// the injection and still be described by a banner claiming the older
	// validation age.
	BoundaryBanner string    `json:"boundary_banner,omitempty"`
	PreparedAt     time.Time `json:"prepared_at"`
}

type CodexSupplementalContext struct {
	CtxID   string `json:"ctx_id"`
	Handoff string `json:"handoff"`
}

type CodexContextForkPlan struct {
	Contract     string                     `json:"contract"`
	PrimaryCtxID string                     `json:"primary_ctx_id"`
	Primary      ProviderHostRecord         `json:"primary"`
	Supplemental []CodexSupplementalContext `json:"supplemental,omitempty"`
	// PrimaryReadyAt is when the base was published, carried so the adapter can
	// explain a vanished payload in terms of the provider's own retention
	// behaviour. It is the only registry fact the plan holds, and it is here
	// rather than fetched because a provider adapter that could read the registry
	// would be a second authority over it.
	PrimaryReadyAt time.Time `json:"primary_ready_at,omitempty"`
	// PrimaryMode is the resolution that chose this base. It decides whether a
	// refusal may fall through to the next ranked candidate, so it has to survive
	// into the adapter that produces the refusal.
	PrimaryMode ContextResolutionMode `json:"primary_mode,omitempty"`
	// BoundaryBanner is the rendered coverage statement injected ahead of every
	// inherited handoff, so the fork is told what its base never looked at before
	// it is told what its predecessors decided.
	BoundaryBanner string    `json:"boundary_banner,omitempty"`
	PreparedAt     time.Time `json:"prepared_at"`
}
