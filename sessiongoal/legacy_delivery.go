// Pure legacy protocol data and validation. No actor authentication or storage.
package sessiongoal

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

func legacyValidateGoalRef(ref legacyGoalRef) error {
	if !legacyGoalIDPattern.MatchString(strings.ToUpper(strings.TrimSpace(ref.ID))) {
		return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("invalid goal id %q", ref.ID), nil)
	}
	if ref.Revision < 1 {
		return legacyGoalError(legacyGoalErrorInvalidContract, "goal revision must be at least 1", nil)
	}
	return nil
}

func legacyValidateGoalContract(goal *legacyGoalContract) error {
	if goal == nil {
		return legacyGoalError(legacyGoalErrorInvalidContract, "goal is required", nil)
	}
	if goal.Version != legacyGoalContractVersion {
		return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("unsupported version %q", goal.Version), nil)
	}
	if err := legacyValidateGoalRef(goal.Ref()); err != nil {
		return err
	}
	if goal.Revision == 1 && goal.PreviousRevision != 0 {
		return legacyGoalError(legacyGoalErrorInvalidContract, "revision 1 cannot have previous_revision", nil)
	}
	if goal.Revision > 1 && (goal.PreviousRevision < 1 || goal.PreviousRevision >= goal.Revision) {
		return legacyGoalError(legacyGoalErrorInvalidContract, "previous_revision must identify an earlier active revision", nil)
	}
	if (goal.PredecessorGoalID == "") != (goal.PredecessorGoalRevision == 0) {
		return legacyGoalError(legacyGoalErrorInvalidContract, "predecessor id and revision must be supplied together", nil)
	}
	if goal.PredecessorGoalID != "" {
		if err := legacyValidateGoalRef(legacyGoalRef{ID: goal.PredecessorGoalID, Revision: goal.PredecessorGoalRevision}); err != nil {
			return err
		}
	}
	if strings.TrimSpace(goal.Owner.Agent) == "" || strings.TrimSpace(goal.Owner.RunID) == "" {
		return legacyGoalError(legacyGoalErrorInvalidContract, "owner agent and run_id are required", nil)
	}
	if (goal.Owner.ParentGoalID == "") != (goal.Owner.ParentGoalRevision == 0) {
		return legacyGoalError(legacyGoalErrorInvalidContract, "parent goal id and revision must be supplied together", nil)
	}
	if goal.Owner.ParentGoalID != "" {
		if err := legacyValidateBoardGoalRef(legacyBoardGoalRef{ID: goal.Owner.ParentGoalID, Revision: goal.Owner.ParentGoalRevision}); err != nil {
			return err
		}
	}
	kind, predicate, policy, normalizedRole, err := legacyDeriveGoalRoleContract(goal.Owner.Role)
	if err != nil {
		return err
	}
	if kind == legacyGoalKindRoleHandoff && goal.SuccessPredicate != predicate {
		return legacyProducerClauseError("success_predicate", string(goal.SuccessPredicate))
	}
	if goal.Kind != kind || goal.SuccessPredicate != predicate || !legacyGoalRolePolicyEqual(goal.RolePolicy, policy) {
		return legacyGoalError(legacyGoalErrorRoleIncompatible, fmt.Sprintf("role %s requires %s/%s", normalizedRole.Role, kind, predicate), nil)
	}
	if normalizedRole != goal.Owner.Role {
		return legacyGoalError(legacyGoalErrorRoleIncompatible, "owner role metadata is not normalized", nil)
	}
	if len(goal.Scope.ResolvedLeafItems) == 0 {
		return legacyGoalError(legacyGoalErrorScopeEmpty, "resolved scope has no active leaf items", nil)
	}
	if err := legacyValidateGoalScopeSnapshot(goal.Scope); err != nil {
		return err
	}
	if strings.TrimSpace(goal.StopBoundaryRef) == "" {
		return legacyGoalError(legacyGoalErrorInvalidContract, "stop_boundary_ref is required", nil)
	}
	if err := legacyValidateGoalBudget(goal.Budget); err != nil {
		return err
	}
	if strings.TrimSpace(goal.Mutation.Reason) == "" {
		return legacyGoalError(legacyGoalErrorInvalidContract, "mutation reason is required", nil)
	}
	if err := legacyValidatePersistedGoalMutationKind(goal.Mutation.Kind); err != nil {
		return err
	}
	if _, err := legacyValidateGoalMutationProvenance(goal.Mutation.Kind, goal.Mutation.Actor, goal.Mutation.Authority); err != nil {
		return err
	}
	if _, err := time.Parse(time.RFC3339Nano, goal.CreatedAt); err != nil {
		return legacyGoalError(legacyGoalErrorInvalidContract, "created_at must be RFC3339Nano", err)
	}
	wantObjective, err := legacyRenderGoalObjective(goal)
	if err != nil {
		return err
	}
	if goal.Objective != wantObjective {
		if kind == legacyGoalKindRoleHandoff {
			return legacyProducerClauseError("objective", goal.Objective)
		}
		return legacyGoalError(legacyGoalErrorInvalidContract, "objective is not the deterministic normative rendering", nil)
	}
	wantProvider, err := legacyRenderGoalProviderCondition(goal)
	if err != nil {
		return err
	}
	if goal.ProviderCondition != wantProvider {
		if kind == legacyGoalKindRoleHandoff {
			return legacyProducerClauseError("provider_condition", goal.ProviderCondition)
		}
		return legacyGoalError(legacyGoalErrorInvalidContract, "provider_condition is not the deterministic stable rendering", nil)
	}
	return nil
}

func legacyCloneGoalScope(scope legacyGoalScope) legacyGoalScope {
	return legacyGoalScope{
		RequestedIDs:        append([]string(nil), scope.RequestedIDs...),
		ResolvedLeafItems:   append([]legacyGoalScopeItem(nil), scope.ResolvedLeafItems...),
		ExcludedTerminalIDs: append([]string(nil), scope.ExcludedTerminalIDs...),
	}
}

func legacyValidateGoalScopeSnapshot(scope legacyGoalScope) error {
	if len(scope.RequestedIDs) == 0 {
		return legacyGoalError(legacyGoalErrorScopeInvalid, "requested_ids is empty", nil)
	}
	if !legacyGoalStringSlicesEqual(scope.RequestedIDs, legacySortedUniqueGoalIDs(scope.RequestedIDs)) {
		return legacyGoalError(legacyGoalErrorScopeInvalid, "requested_ids must be unique and sorted", nil)
	}
	if !legacyGoalStringSlicesEqual(scope.ExcludedTerminalIDs, legacySortedUniqueGoalIDs(scope.ExcludedTerminalIDs)) {
		return legacyGoalError(legacyGoalErrorScopeInvalid, "excluded_terminal_ids must be unique and sorted", nil)
	}
	for _, id := range scope.RequestedIDs {
		upper := strings.ToUpper(id)
		if !strings.HasPrefix(upper, "EPIC-") && !strings.HasPrefix(upper, "STORY-") && !strings.HasPrefix(upper, "TASK-") && !strings.HasPrefix(upper, "BUG-") {
			return legacyGoalError(legacyGoalErrorScopeInvalid, fmt.Sprintf("unsupported requested scope id %s", id), nil)
		}
	}
	excluded := legacyGoalStringSet(scope.ExcludedTerminalIDs)
	seen := make(map[string]struct{}, len(scope.ResolvedLeafItems))
	previous := ""
	for _, item := range scope.ResolvedLeafItems {
		if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.Title) == "" {
			return legacyGoalError(legacyGoalErrorScopeInvalid, "resolved item id and title are required", nil)
		}
		upperID := strings.ToUpper(item.ID)
		if !strings.HasPrefix(upperID, "TASK-") && !strings.HasPrefix(upperID, "BUG-") {
			return legacyGoalError(legacyGoalErrorScopeInvalid, fmt.Sprintf("resolved item %s is not a task/bug leaf", item.ID), nil)
		}
		if _, overlaps := excluded[upperID]; overlaps {
			return legacyGoalError(legacyGoalErrorScopeInvalid, fmt.Sprintf("resolved item %s must not also appear in excluded_terminal_ids", item.ID), nil)
		}
		if previous != "" && item.ID <= previous {
			return legacyGoalError(legacyGoalErrorScopeInvalid, "resolved items must be unique and sorted", nil)
		}
		previous = item.ID
		if _, ok := seen[item.ID]; ok {
			return legacyGoalError(legacyGoalErrorScopeInvalid, fmt.Sprintf("duplicate resolved item %s", item.ID), nil)
		}
		seen[item.ID] = struct{}{}
		if _, err := legacyParseReviewPolicy(string(item.ReviewPolicy)); err != nil {
			return legacyGoalError(legacyGoalErrorScopeInvalid, fmt.Sprintf("item %s: %v", item.ID, err), err)
		}
		if _, err := legacyParseStatus(string(item.BaselineStatus)); err != nil {
			return legacyGoalError(legacyGoalErrorScopeInvalid, fmt.Sprintf("item %s: %v", item.ID, err), err)
		}
		if item.BaselineStatus == legacyStatusDone || item.BaselineStatus == legacyStatusClosed {
			return legacyGoalError(legacyGoalErrorScopeInvalid, fmt.Sprintf("terminal item %s belongs in excluded_terminal_ids", item.ID), nil)
		}
	}
	return nil
}

func legacyValidateGoalBudget(budget *legacyGoalBudget) error {
	if budget == nil {
		return nil
	}
	if budget.Source != legacyGoalBudgetSourceOperatorExplicit {
		return legacyGoalError(legacyGoalErrorBudgetInvalid, "budget must come from an explicit operator request", nil)
	}
	if budget.Provenance.Actor.Kind != legacyGoalActorOperator && budget.Provenance.Actor.Kind != legacyGoalActorSystem {
		return legacyGoalError(legacyGoalErrorBudgetInvalid, "budget provenance requires a derived operator or system actor", nil)
	}
	wantAuthority, err := legacyGoalAuthorityForActor(budget.Provenance.Actor)
	if err != nil || budget.Provenance.Authority != wantAuthority {
		return legacyGoalError(legacyGoalErrorBudgetInvalid, "budget provenance authority does not match its derived actor", err)
	}
	return legacyValidateGoalBudgetShape(budget)
}

func legacyValidateGoalBudgetShape(budget *legacyGoalBudget) error {
	switch budget.Kind {
	case legacyGoalBudgetNone:
		if budget.Limit != 0 || strings.TrimSpace(budget.Duration) != "" {
			return legacyGoalError(legacyGoalErrorBudgetInvalid, "none budget cannot set a limit or duration", nil)
		}
	case legacyGoalBudgetTokens, legacyGoalBudgetTurns:
		if budget.Limit <= 0 || strings.TrimSpace(budget.Duration) != "" {
			return legacyGoalError(legacyGoalErrorBudgetInvalid, fmt.Sprintf("%s budget requires a positive integer limit", budget.Kind), nil)
		}
	case legacyGoalBudgetDuration:
		if budget.Limit != 0 {
			return legacyGoalError(legacyGoalErrorBudgetInvalid, "duration budget cannot set an integer limit", nil)
		}
		duration, err := time.ParseDuration(strings.TrimSpace(budget.Duration))
		if err != nil || duration <= 0 {
			return legacyGoalError(legacyGoalErrorBudgetInvalid, "duration budget requires a positive Go duration", err)
		}
	default:
		return legacyGoalError(legacyGoalErrorBudgetInvalid, fmt.Sprintf("unknown budget kind %q", budget.Kind), nil)
	}
	return nil
}

func legacyValidateGoalText(text string, code legacyGoalErrorCode, field string) error {
	length := utf8.RuneCountInString(text)
	if length > legacyGoalTextLimit {
		return legacyGoalError(code, fmt.Sprintf("%s is %d characters; limit is %d", field, length, legacyGoalTextLimit), nil)
	}
	return nil
}

func legacyCloneGoalBudget(budget *legacyGoalBudget) *legacyGoalBudget {
	if budget == nil {
		return nil
	}
	clone := *budget
	return &clone
}

func legacyValidatePersistedGoalMutationKind(kind legacyGoalMutationKind) error {
	switch kind {
	case legacyGoalMutationCreated,
		legacyGoalMutationScopeExpanded,
		legacyGoalMutationObjectiveIntentRefreshed,
		legacyGoalMutationScopeRemoved,
		legacyGoalMutationStructuralReplacement,
		legacyGoalMutationAcceptanceWeakened,
		legacyGoalMutationKindDowngraded,
		legacyGoalMutationCancelled,
		legacyGoalMutationCleared,
		legacyGoalMutationAlignmentRealigned,
		legacyGoalMutationImported:
		return nil
	case legacyGoalMutationReplay:
		return legacyGoalError(legacyGoalErrorInvalidContract, "idempotent replay must not be persisted as a revision", nil)
	default:
		return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("unknown mutation kind %q", kind), nil)
	}
}

func legacyGoalStringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func legacySortedUniqueGoalIDs(ids []string) []string {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.ToUpper(strings.TrimSpace(id))
		if id != "" {
			set[id] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for id := range set {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func legacyProducerClauseError(field, clause string) error {
	return legacyGoalError(legacyGoalErrorProducerClauseUnsupported, fmt.Sprintf("unsupported producer clause %s=%q: only the canonical live-producer role_handoff contract may be bound; Change Request construction and passing CR validation evidence are post-handoff obligations and cannot be required before termination", field, clause), nil)
}

func legacyNormalizeGoalRoleBinding(binding legacyGoalRoleBinding) legacyGoalRoleBinding {
	binding.Role = strings.ToLower(strings.TrimSpace(binding.Role))
	if binding.Role == "coordinator" {
		binding.Role = "orchestrator"
	}
	binding.Archetype = strings.ToLower(strings.TrimSpace(binding.Archetype))
	binding.StartStatus = strings.ToLower(strings.TrimSpace(binding.StartStatus))
	binding.EndStatus = strings.ToLower(strings.TrimSpace(binding.EndStatus))
	return binding
}

func legacyDeriveGoalRoleContract(binding legacyGoalRoleBinding) (legacyGoalKind, legacyGoalSuccessPredicate, legacyGoalRolePolicy, legacyGoalRoleBinding, error) {
	binding = legacyNormalizeGoalRoleBinding(binding)
	if binding.Role == "" || binding.Archetype == "" {
		return "", "", legacyGoalRolePolicy{}, binding, legacyGoalError(legacyGoalErrorRoleIncompatible, "resolved role name and archetype are required", nil)
	}

	if binding.Role == "orchestrator" {
		if binding.Archetype != "analyst" || binding.StartStatus != "" || binding.EndStatus != "" {
			return "", "", legacyGoalRolePolicy{}, binding, legacyGoalError(
				legacyGoalErrorRoleIncompatible,
				"orchestrator/coordinator requires analyst archetype and no lifecycle status defaults",
				nil,
			)
		}
		return legacyGoalKindDeliveryPool, legacyGoalSuccessAcceptedDone, legacyGoalRolePolicy{}, binding, nil
	}

	if binding.Archetype == "reviewer" {
		if binding.StartStatus != string(legacyStatusReviewing) || binding.EndStatus != "" {
			return "", "", legacyGoalRolePolicy{}, binding, legacyGoalError(
				legacyGoalErrorRoleIncompatible,
				"reviewer roles require start_status=reviewing and verdict-driven empty end_status",
				nil,
			)
		}
		return legacyGoalKindReviewerVerdict, legacyGoalSuccessReviewerVerdict, legacyGoalRolePolicy{
			AllowedReviewerVerdicts: append([]legacyGoalReviewerVerdict(nil), legacyCanonicalReviewerVerdicts...),
		}, binding, nil
	}

	switch binding.Archetype {
	case "implementer", "analyst", "tester":
	default:
		return "", "", legacyGoalRolePolicy{}, binding, legacyGoalError(
			legacyGoalErrorRoleIncompatible,
			fmt.Sprintf("unknown role archetype %q", binding.Archetype),
			nil,
		)
	}
	if binding.EndStatus == "" {
		return "", "", legacyGoalRolePolicy{}, binding, legacyGoalError(
			legacyGoalErrorRoleIncompatible,
			fmt.Sprintf("non-reviewer specialist role %q requires an explicit end_status", binding.Role),
			nil,
		)
	}
	endStatus, err := legacyParseStatus(binding.EndStatus)
	if err != nil {
		return "", "", legacyGoalRolePolicy{}, binding, legacyGoalError(legacyGoalErrorRoleIncompatible, err.Error(), err)
	}
	if endStatus == legacyStatusDone || endStatus == legacyStatusClosed || endStatus == legacyStatusBlocked {
		return "", "", legacyGoalRolePolicy{}, binding, legacyGoalError(
			legacyGoalErrorRoleIncompatible,
			fmt.Sprintf("specialist end_status %q is terminal rather than a role handoff", endStatus),
			nil,
		)
	}
	if binding.StartStatus != "" {
		if _, err := legacyParseStatus(binding.StartStatus); err != nil {
			return "", "", legacyGoalRolePolicy{}, binding, legacyGoalError(legacyGoalErrorRoleIncompatible, err.Error(), err)
		}
	}
	return legacyGoalKindRoleHandoff, legacyGoalSuccessRoleHandoff, legacyGoalRolePolicy{HandoffEndStatus: string(endStatus)}, binding, nil
}

func legacyGoalRolePolicyEqual(left, right legacyGoalRolePolicy) bool {
	if left.HandoffEndStatus != right.HandoffEndStatus || len(left.AllowedReviewerVerdicts) != len(right.AllowedReviewerVerdicts) {
		return false
	}
	for i := range left.AllowedReviewerVerdicts {
		if left.AllowedReviewerVerdicts[i] != right.AllowedReviewerVerdicts[i] {
			return false
		}
	}
	return true
}

func legacyValidateGoalMutationProvenance(kind legacyGoalMutationKind, actor legacyGoalActorContext, authority legacyGoalAuthority) (legacyGoalMutationDecision, error) {
	restricted := legacyIsRestrictedGoalMutation(kind)
	decision := legacyGoalMutationDecision{Kind: kind, Authority: authority, Idempotent: kind == legacyGoalMutationReplay}
	switch actor.Kind {
	case legacyGoalActorAgent:
		if strings.TrimSpace(actor.RunID) == "" {
			return decision, legacyGoalError(legacyGoalErrorActorUnauthorized, "agent mutation provenance requires a run_id", nil)
		}
		if restricted {
			return decision, legacyGoalError(legacyGoalErrorMutationRestricted, fmt.Sprintf("mutation %s requires operator acknowledgement", kind), nil)
		}
		if authority != legacyGoalAuthorityAgentDiscretion {
			return decision, legacyGoalError(legacyGoalErrorActorUnauthorized, fmt.Sprintf("agent mutation %s requires authority=%s", kind, legacyGoalAuthorityAgentDiscretion), nil)
		}
	case legacyGoalActorOperator:
		if authority != legacyGoalAuthorityOperatorAcknowledged {
			return decision, legacyGoalError(legacyGoalErrorActorUnauthorized, "operator mutation requires operator_acknowledged provenance", nil)
		}
	case legacyGoalActorSystem:
		if authority != legacyGoalAuthoritySystemAuthorized {
			return decision, legacyGoalError(legacyGoalErrorActorUnauthorized, "system mutation requires system_authorized provenance", nil)
		}
	default:
		return decision, legacyGoalError(legacyGoalErrorActorUnauthorized, fmt.Sprintf("unknown actor kind %q", actor.Kind), nil)
	}
	return decision, nil
}

func legacyGoalAuthorityForActor(actor legacyGoalActorContext) (legacyGoalAuthority, error) {
	switch actor.Kind {
	case legacyGoalActorAgent:
		return legacyGoalAuthorityAgentDiscretion, nil
	case legacyGoalActorOperator:
		return legacyGoalAuthorityOperatorAcknowledged, nil
	case legacyGoalActorSystem:
		return legacyGoalAuthoritySystemAuthorized, nil
	default:
		return "", legacyGoalError(legacyGoalErrorActorUnauthorized, fmt.Sprintf("unknown actor kind %q", actor.Kind), nil)
	}
}

func legacyIsRestrictedGoalMutation(kind legacyGoalMutationKind) bool {
	switch kind {
	case legacyGoalMutationScopeRemoved,
		legacyGoalMutationStructuralReplacement,
		legacyGoalMutationAcceptanceWeakened,
		legacyGoalMutationKindDowngraded,
		legacyGoalMutationCancelled:
		return true
	default:
		return false
	}
}

func legacyGoalStringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[strings.ToUpper(strings.TrimSpace(value))] = struct{}{}
	}
	return set
}

func legacyParseStatus(s string) (legacyStatus, error) {
	switch strings.ToLower(s) {
	case "backlog":
		return legacyStatusBacklog, nil
	case "analysis":
		return legacyStatusAnalysis, nil
	case "to-dev", "todev":
		return legacyStatusToDev, nil
	case "development", "dev":
		return legacyStatusDevelopment, nil
	case "to-review", "toreview":
		return legacyStatusToReview, nil
	case "reviewing", "review":
		return legacyStatusReviewing, nil
	case "integrating", "integrate":
		return legacyStatusIntegrating, nil
	case "done":
		return legacyStatusDone, nil
	case "closed":
		return legacyStatusClosed, nil
	case "blocked":
		return legacyStatusBlocked, nil
	default:
		return "", fmt.Errorf("unknown status: %s (valid: backlog, analysis, to-dev, development, to-review, reviewing, integrating, done, closed, blocked)", s)
	}
}

func legacyParseReviewPolicy(s string) (legacyReviewPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "required":
		return legacyReviewRequired, nil
	case "light":
		return legacyReviewLight, nil
	case "none":
		return legacyReviewNone, nil
	default:
		return "", fmt.Errorf("unknown review policy: %s (valid: required, light, none)", s)
	}
}
