package sessionmanager

// Original actualization provenance data only; no registry engine or policy.
type ContextActualizationEvidence struct {
	ContractVersion       string                            `json:"contract_version,omitempty"`
	Action                ContextActualizationAction        `json:"action,omitempty"`
	FromCtxID             string                            `json:"from_ctx_id,omitempty"`
	FromRevision          int                               `json:"from_revision,omitempty"`
	FromCommitSHA         string                            `json:"from_commit_sha,omitempty"`
	ToCommitSHA           string                            `json:"to_commit_sha,omitempty"`
	Triggers              []ContextActualizationTrigger     `json:"triggers,omitempty"`
	DeltaSHA256           string                            `json:"delta_sha256,omitempty"`
	Metrics               ContextActualizationMetrics       `json:"metrics,omitempty"`
	OwnershipTier         ContextActualizationOwnershipTier `json:"ownership_tier,omitempty"`
	FrontierModelRequired bool                              `json:"frontier_model_required"`
}

type ContextActualizationAction string

type ContextActualizationTrigger string

type ContextActualizationMetrics struct {
	AffectedIngestedCount    int   `json:"affected_ingested_count"`
	IngestedPathCount        int   `json:"ingested_path_count"`
	RelevantAdditionCount    int   `json:"relevant_addition_count"`
	DiffBytes                int64 `json:"diff_bytes"`
	DiffTokenEstimate        int64 `json:"diff_token_estimate"`
	ScoutBaselineTokens      int64 `json:"scout_baseline_tokens"`
	ScoutBaselineFallback    bool  `json:"scout_baseline_fallback"`
	BaseLiveTokens           int64 `json:"base_live_tokens"`
	TaskReserveTokens        int64 `json:"task_reserve_tokens"`
	ProjectedPromptTokens    int64 `json:"projected_prompt_tokens"`
	ModelContextWindowTokens int64 `json:"model_context_window_tokens"`
	WorkingSetLimitTokens    int64 `json:"working_set_limit_tokens"`
}

type ContextActualizationOwnershipTier string
