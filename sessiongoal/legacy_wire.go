// Pure legacy protocol data and validation. No actor authentication or storage.
package sessiongoal

import (
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"regexp"
	"time"
)

var legacyGoalIDPattern = regexp.MustCompile(`^GOAL-[A-Z0-9]+(?:-[A-Z0-9]+)+$`)

const (
	legacyGoalContractVersion       = "board-goal-v1"
	legacyGoalTextLimit             = 4000
	legacyRepositoryStopBoundaryRef = "Stop-The-Line: No Forced Fits"
)

type legacyGoalKind string

const (
	legacyGoalKindDeliveryPool    legacyGoalKind = "delivery_pool"
	legacyGoalKindRoleHandoff     legacyGoalKind = "role_handoff"
	legacyGoalKindReviewerVerdict legacyGoalKind = "reviewer_verdict"
)

type legacyGoalSuccessPredicate string

const (
	legacyGoalSuccessAcceptedDone    legacyGoalSuccessPredicate = "accepted_done"
	legacyGoalSuccessRoleHandoff     legacyGoalSuccessPredicate = "role_handoff"
	legacyGoalSuccessReviewerVerdict legacyGoalSuccessPredicate = "reviewer_verdict"
)

type legacyGoalReviewerVerdict string

const (
	legacyGoalReviewerVerdictAccepted      legacyGoalReviewerVerdict = "accepted"
	legacyGoalReviewerVerdictChangesNeeded legacyGoalReviewerVerdict = "changes_requested"
	legacyGoalReviewerVerdictStopTheLine   legacyGoalReviewerVerdict = "stop_the_line"
)

var legacyCanonicalReviewerVerdicts = []legacyGoalReviewerVerdict{
	legacyGoalReviewerVerdictAccepted,
	legacyGoalReviewerVerdictChangesNeeded,
	legacyGoalReviewerVerdictStopTheLine,
}

type legacyGoalRef = core.LegacyGoalRef

type legacyGoalRoleBinding struct {
	Role        string `json:"role"`
	Archetype   string `json:"archetype"`
	StartStatus string `json:"start_status,omitempty"`
	EndStatus   string `json:"end_status,omitempty"`
}

type legacyGoalRolePolicy struct {
	HandoffEndStatus        string                      `json:"handoff_end_status,omitempty"`
	AllowedReviewerVerdicts []legacyGoalReviewerVerdict `json:"allowed_reviewer_verdicts,omitempty"`
}

type legacyGoalOwner struct {
	Role               legacyGoalRoleBinding `json:"role"`
	Agent              string                `json:"agent"`
	RunID              string                `json:"run_id"`
	ParentGoalID       string                `json:"parent_goal_id,omitempty"`
	ParentGoalRevision int                   `json:"parent_goal_revision,omitempty"`
}

type legacyGoalScopeItem struct {
	ID             string             `json:"id"`
	Title          string             `json:"title"`
	ReviewPolicy   legacyReviewPolicy `json:"review_policy"`
	BaselineStatus legacyStatus       `json:"baseline_status"`
}

type legacyGoalScope struct {
	RequestedIDs        []string              `json:"requested_ids"`
	ResolvedLeafItems   []legacyGoalScopeItem `json:"resolved_leaf_items"`
	ExcludedTerminalIDs []string              `json:"excluded_terminal_ids,omitempty"`
}

func (s legacyGoalScope) ResolvedLeafIDs() []string {
	ids := make([]string, 0, len(s.ResolvedLeafItems))
	for _, item := range s.ResolvedLeafItems {
		ids = append(ids, item.ID)
	}
	return ids
}

type legacyGoalActorKind string

const (
	legacyGoalActorAgent    legacyGoalActorKind = "agent"
	legacyGoalActorOperator legacyGoalActorKind = "operator"
	legacyGoalActorSystem   legacyGoalActorKind = "system"
)

type legacyGoalActorContext struct {
	Kind    legacyGoalActorKind `json:"kind"`
	ActorID string              `json:"actor_id,omitempty"`
	RunID   string              `json:"run_id,omitempty"`
}

type legacyGoalAuthority string

const (
	legacyGoalAuthorityAgentDiscretion      legacyGoalAuthority = "agent_discretion"
	legacyGoalAuthorityOperatorAcknowledged legacyGoalAuthority = "operator_acknowledged"
	legacyGoalAuthoritySystemAuthorized     legacyGoalAuthority = "system_authorized"
)

type legacyGoalMutationKind string

const (
	legacyGoalMutationCreated                  legacyGoalMutationKind = "created"
	legacyGoalMutationScopeExpanded            legacyGoalMutationKind = "scope_expanded"
	legacyGoalMutationObjectiveIntentRefreshed legacyGoalMutationKind = "objective_intent_refreshed"
	legacyGoalMutationReplay                   legacyGoalMutationKind = "replay"
	legacyGoalMutationScopeRemoved             legacyGoalMutationKind = "scope_removed"
	legacyGoalMutationStructuralReplacement    legacyGoalMutationKind = "structural_replacement"
	legacyGoalMutationAcceptanceWeakened       legacyGoalMutationKind = "acceptance_weakened"
	legacyGoalMutationKindDowngraded           legacyGoalMutationKind = "kind_downgraded"
	legacyGoalMutationCancelled                legacyGoalMutationKind = "cancelled"
)

type legacyGoalMutation struct {
	Kind      legacyGoalMutationKind `json:"kind"`
	Reason    string                 `json:"reason"`
	Actor     legacyGoalActorContext `json:"actor_context"`
	Authority legacyGoalAuthority    `json:"authority"`
}

type legacyGoalBudgetKind string

const (
	legacyGoalBudgetNone     legacyGoalBudgetKind = "none"
	legacyGoalBudgetTokens   legacyGoalBudgetKind = "tokens"
	legacyGoalBudgetTurns    legacyGoalBudgetKind = "turns"
	legacyGoalBudgetDuration legacyGoalBudgetKind = "duration"
)

type legacyGoalBudgetSource string

const legacyGoalBudgetSourceOperatorExplicit legacyGoalBudgetSource = "operator_explicit"

type legacyGoalBudget struct {
	Kind       legacyGoalBudgetKind       `json:"kind"`
	Limit      int64                      `json:"limit,omitempty"`
	Duration   string                     `json:"duration,omitempty"`
	Source     legacyGoalBudgetSource     `json:"source"`
	Provenance legacyGoalBudgetProvenance `json:"provenance"`
}

type legacyGoalBudgetProvenance struct {
	Actor     legacyGoalActorContext `json:"actor_context"`
	Authority legacyGoalAuthority    `json:"authority"`
}

type legacyGoalContract struct {
	Version                 string                     `json:"version"`
	ID                      string                     `json:"id"`
	Kind                    legacyGoalKind             `json:"kind"`
	Owner                   legacyGoalOwner            `json:"owner"`
	Scope                   legacyGoalScope            `json:"scope"`
	RolePolicy              legacyGoalRolePolicy       `json:"role_policy"`
	ObjectiveIntent         string                     `json:"objective_intent,omitempty"`
	Objective               string                     `json:"objective"`
	ProviderCondition       string                     `json:"provider_condition"`
	SuccessPredicate        legacyGoalSuccessPredicate `json:"success_predicate"`
	StopBoundaryRef         string                     `json:"stop_boundary_ref"`
	Revision                int                        `json:"revision"`
	PreviousRevision        int                        `json:"previous_revision,omitempty"`
	PredecessorGoalID       string                     `json:"predecessor_goal_id,omitempty"`
	PredecessorGoalRevision int                        `json:"predecessor_goal_revision,omitempty"`
	Mutation                legacyGoalMutation         `json:"mutation"`
	Budget                  *legacyGoalBudget          `json:"budget,omitempty"`
	CreatedAt               string                     `json:"created_at"`
}

func (g legacyGoalContract) Ref() legacyGoalRef {
	return legacyGoalRef{ID: g.ID, Revision: g.Revision}
}

const legacyBoardGoalContractVersion = "board-goal-v2"

var (
	legacyBoardGoalIDPattern    = regexp.MustCompile(`^(?:PRIMARY-)?GOAL-[A-Z0-9]+(?:-[A-Z0-9]+)+$`)
	legacyBoardGoalRunIDPattern = regexp.MustCompile(`^RUN-[A-Za-z0-9][A-Za-z0-9-]*$`)
)

type legacyBoardGoalOwnerKind = string

const (
	legacyBoardGoalOwnerParent legacyBoardGoalOwnerKind = "parent"
	legacyBoardGoalOwnerRun    legacyBoardGoalOwnerKind = "run"
)

type legacyBoardGoalOwner = core.Protocol8SessionOwner

var legacyPrimaryBoardGoalOwner = legacyBoardGoalOwner{Kind: legacyBoardGoalOwnerParent, ID: "primary"}

type legacyBoardGoalRef = core.LegacyGoalRef
type legacyBoardGoalState string

const (
	legacyBoardGoalStateActive    legacyBoardGoalState = "active"
	legacyBoardGoalStateCleared   legacyBoardGoalState = "cleared"
	legacyBoardGoalStateCancelled legacyBoardGoalState = "cancelled"
)

type legacyBoardGoalNativeReflection string

const (
	legacyBoardGoalNativeRequired     legacyBoardGoalNativeReflection = "required"
	legacyBoardGoalNativeNotSupported legacyBoardGoalNativeReflection = "not-supported"
)

type legacyBoardGoalNativeStatus string

const (
	legacyBoardGoalNativeApplied        legacyBoardGoalNativeStatus = "applied"
	legacyBoardGoalNativePendingUnbound legacyBoardGoalNativeStatus = "pending-unbound"
	legacyBoardGoalNativeUnavailable    legacyBoardGoalNativeStatus = "not-supported"
	legacyBoardGoalNativePendingAck     legacyBoardGoalNativeStatus = "pending-ack"
)

const (
	legacyGoalKindAlignment                 legacyGoalKind             = "alignment"
	legacyGoalSuccessAlignmentHandled       legacyGoalSuccessPredicate = "alignment_handled"
	legacyGoalSuccessRoutingDutyClear       legacyGoalSuccessPredicate = "routing_duty_clear"
	legacyGoalMutationCleared               legacyGoalMutationKind     = "cleared"
	legacyGoalMutationAlignmentRealigned    legacyGoalMutationKind     = "alignment_realigned"
	legacyGoalMutationImported              legacyGoalMutationKind     = "imported"
	legacyGoalErrorOwnerInvalid             legacyGoalErrorCode        = "goal_owner_invalid"
	legacyGoalErrorNotFound                 legacyGoalErrorCode        = "goal_not_found"
	legacyGoalErrorPersistenceFailed        legacyGoalErrorCode        = "goal_persistence_failed"
	legacyGoalErrorNativeReflectionRequired legacyGoalErrorCode        = "goal_native_reflection_required"
)

type legacyBoardGoalLegacyRef struct {
	Contract string `json:"contract"`
	ID       string `json:"id"`
	Revision int    `json:"revision,omitempty"`
}

type legacyBoardGoalRevision struct {
	ContractVersion          string                          `json:"contract_version"`
	GoalID                   string                          `json:"goal_id"`
	Revision                 int                             `json:"revision"`
	ExpectedPreviousRevision int                             `json:"expected_previous_revision"`
	PreviousRef              *legacyBoardGoalRef             `json:"previous_ref,omitempty"`
	Owner                    legacyBoardGoalOwner            `json:"owner"`
	Kind                     legacyGoalKind                  `json:"kind"`
	Objective                string                          `json:"objective"`
	Scope                    legacyGoalScope                 `json:"scope,omitempty"`
	ParentGoalRef            *legacyBoardGoalRef             `json:"parent_goal_ref,omitempty"`
	MutationAuthority        legacyGoalAuthority             `json:"mutation_authority"`
	SuccessPredicate         legacyGoalSuccessPredicate      `json:"success_predicate"`
	ProviderCondition        string                          `json:"provider_condition"`
	NativeReflection         legacyBoardGoalNativeReflection `json:"native_reflection"`
	State                    legacyBoardGoalState            `json:"state"`
	LegacyRefs               []legacyBoardGoalLegacyRef      `json:"legacy_refs,omitempty"`
	RolePolicy               legacyGoalRolePolicy            `json:"role_policy,omitempty"`
	DeliveryOwner            *legacyGoalOwner                `json:"delivery_owner,omitempty"`
	DeliveryPreviousRevision int                             `json:"delivery_previous_revision,omitempty"`
	DeliveryPredecessorRef   *legacyGoalRef                  `json:"delivery_predecessor_ref,omitempty"`
	ObjectiveIntent          string                          `json:"objective_intent,omitempty"`
	StopBoundaryRef          string                          `json:"stop_boundary_ref,omitempty"`
	Mutation                 legacyGoalMutation              `json:"mutation"`
	Budget                   *legacyGoalBudget               `json:"budget,omitempty"`
	CreatedAt                time.Time                       `json:"created_at"`
	UpdatedAt                time.Time                       `json:"updated_at"`
	CreatedBy                string                          `json:"created_by"`
	Reason                   string                          `json:"reason"`
}

func (r *legacyBoardGoalRevision) Ref() legacyBoardGoalRef {
	if r == nil {
		return legacyBoardGoalRef{}
	}
	return legacyBoardGoalRef{ID: r.GoalID, Revision: r.Revision}
}

type legacyBoardGoalHead struct {
	ContractVersion string                      `json:"contract_version"`
	Owner           legacyBoardGoalOwner        `json:"owner"`
	ActiveRef       *legacyBoardGoalRef         `json:"active_ref,omitempty"`
	LatestRef       *legacyBoardGoalRef         `json:"latest_ref,omitempty"`
	NativeStatus    legacyBoardGoalNativeStatus `json:"native_status,omitempty"`
}

type legacyGoalErrorCode string

const (
	legacyGoalErrorProducerClauseUnsupported legacyGoalErrorCode = "goal_producer_clause_unsupported"
	legacyGoalErrorInvalidContract           legacyGoalErrorCode = "goal_contract_invalid"
	legacyGoalErrorRoleIncompatible          legacyGoalErrorCode = "goal_role_incompatible"
	legacyGoalErrorScopeInvalid              legacyGoalErrorCode = "goal_scope_invalid"
	legacyGoalErrorScopeEmpty                legacyGoalErrorCode = "goal_scope_empty"
	legacyGoalErrorScopeBlockedOnly          legacyGoalErrorCode = "goal_scope_blocked_only"
	legacyGoalErrorDependencyClosure         legacyGoalErrorCode = "goal_dependency_closure_invalid"
	legacyGoalErrorObjectiveTooLong          legacyGoalErrorCode = "goal_objective_too_long"
	legacyGoalErrorProviderTooLong           legacyGoalErrorCode = "goal_provider_condition_too_long"
	legacyGoalErrorRevisionRequired          legacyGoalErrorCode = "goal_revision_required"
	legacyGoalErrorRevisionConflict          legacyGoalErrorCode = "goal_revision_conflict"
	legacyGoalErrorMutationRestricted        legacyGoalErrorCode = "goal_mutation_restricted"
	legacyGoalErrorActorUnauthorized         legacyGoalErrorCode = "goal_actor_unauthorized"
	legacyGoalErrorActivationFailed          legacyGoalErrorCode = "goal_activation_failed"
	legacyGoalErrorBudgetInvalid             legacyGoalErrorCode = "goal_budget_invalid"
	legacyGoalErrorManagerProtocolSkew       legacyGoalErrorCode = "goal_manager_protocol_skew"
	legacyGoalErrorPlaneDisabled             legacyGoalErrorCode = "spawned_run_goal_plane_disabled"
)

type legacyStatus string

const (
	legacyStatusBacklog     legacyStatus = "backlog"
	legacyStatusAnalysis    legacyStatus = "analysis"
	legacyStatusToDev       legacyStatus = "to-dev"
	legacyStatusDevelopment legacyStatus = "development"
	legacyStatusToReview    legacyStatus = "to-review"
	legacyStatusReviewing   legacyStatus = "reviewing"
	legacyStatusIntegrating legacyStatus = "integrating"
	legacyStatusDone        legacyStatus = "done"
	legacyStatusClosed      legacyStatus = "closed"
	legacyStatusBlocked     legacyStatus = "blocked"
)

type legacyReviewPolicy string

const (
	legacyReviewRequired legacyReviewPolicy = "required"
	legacyReviewLight    legacyReviewPolicy = "light"
	legacyReviewNone     legacyReviewPolicy = "none"
)

type legacyGoalMutationDecision struct {
	Kind             legacyGoalMutationKind `json:"kind"`
	Authority        legacyGoalAuthority    `json:"authority"`
	PreviousRevision int                    `json:"previous_revision"`
	Idempotent       bool                   `json:"idempotent"`
}

const legacyPrimaryGoalObjectiveLimit = 4000
