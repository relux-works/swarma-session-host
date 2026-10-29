// Pure legacy protocol data and validation. No actor authentication or storage.
package sessiongoal

import (
	"fmt"
	"strings"
)

func legacyRenderGoalObjective(goal *legacyGoalContract) (string, error) {
	if goal == nil {
		return "", legacyGoalError(legacyGoalErrorInvalidContract, "goal is required for objective rendering", nil)
	}
	ids := goal.Scope.ResolvedLeafIDs()
	if len(ids) == 0 {
		return "", legacyGoalError(legacyGoalErrorScopeEmpty, "cannot render an objective without resolved leaf items", nil)
	}
	intent := strings.TrimSpace(goal.ObjectiveIntent)
	if intent == "" {
		intent = "(none supplied)"
	}
	stopRef := strings.TrimSpace(goal.StopBoundaryRef)
	if stopRef == "" {
		stopRef = legacyRepositoryStopBoundaryRef
	}
	prefix := fmt.Sprintf("Intent summary (non-authoritative): %s\nReview-policy snapshot: %s\n\n%s\n\n", intent, legacyRenderGoalReviewSnapshot(goal.Scope.ResolvedLeafItems), legacyRenderGoalEnvironmentFraming(stopRef))

	var normative string
	switch goal.Kind {
	case legacyGoalKindDeliveryPool:
		normative = fmt.Sprintf(
			"Own board scope **%s** through accepted delivery. Do not stop or hand routine work to the human while any scoped item is not accepted `done`. For every item, autonomously route producer -> reviewer -> rework -> reviewer until the board records `done`; treat `to-review`, requested changes, failed checks, early child exits, transient runtime failures, usage or budget limits, and other recoverable conditions as continuation, retry, or reroute signals. Before marking this goal complete, query the board and surface every scoped ID with its `done` status, current outcome evidence, and an accepted reviewer verdict for `review=required|light`, or the orchestrator evidence check for `review=none`. The only agent-initiated unresolved terminal is the repository's existing **%s** contract; apply it by reference, persist its required evidence packet, and request only the exact human-only decision or external input. Never force-fit an impossible design, weaken acceptance, self-accept review-required producer work, or change status merely to satisfy this goal. A revision-bound owner agent may create a role-compatible goal, monotonically expand its scope, and refresh its non-authoritative `objective_intent` without operator acknowledgement. Only an acknowledged operator cancel or reroute may remove scope, replace role-compatible structural goal semantics, weaken acceptance, or terminate ownership; those restricted mutations supersede or cancel the prior goal and are never successful delivery.",
			strings.Join(ids, ", "), stopRef,
		)
	case legacyGoalKindRoleHandoff:
		endStatus := strings.TrimSpace(goal.RolePolicy.HandoffEndStatus)
		if endStatus == "" {
			return "", legacyGoalError(legacyGoalErrorRoleIncompatible, "role_handoff objective requires an immutable end_status", nil)
		}
		normative = fmt.Sprintf(
			"Keep working on board scope %s until every assigned item reaches the role end status `%s` with all task acceptance criteria and checklist gates satisfied and a new or updated task-scoped outcome artifact, or the repository's existing **%s** boundary is evidenced. Routine failed checks, recoverable runtime errors, and rework remain continuation signals; they are not successful handoff.",
			strings.Join(ids, ", "), endStatus, stopRef,
		)
	case legacyGoalKindReviewerVerdict:
		normative = fmt.Sprintf(
			"Review board scope %s until exactly one evidence-backed verdict is recorded for every assigned item: accepted -> `done`; changes requested -> `to-dev` or `analysis` with verdict evidence; genuine **%s** boundary -> `blocked` with the canonical evidence packet. A producer handoff, missing evidence, or a recoverable runtime failure is not an accepted verdict.",
			strings.Join(ids, ", "), stopRef,
		)
	default:
		return "", legacyGoalError(legacyGoalErrorRoleIncompatible, fmt.Sprintf("unsupported goal kind %q", goal.Kind), nil)
	}

	budgetClause, err := legacyRenderGoalBudget(goal.Budget)
	if err != nil {
		return "", err
	}
	objective := prefix + normative + budgetClause
	if err := legacyValidateGoalText(objective, legacyGoalErrorObjectiveTooLong, "objective"); err != nil {
		return "", err
	}
	return objective, nil
}

func legacyRenderGoalEnvironmentFraming(stopRef string) string {
	return fmt.Sprintf(
		"Environment contract: Act with maximum autonomy and keep progressing through recoverable work without asking for routine confirmation. After surfacing final board evidence for a satisfied success predicate, clear the provider goal through its successful-completion path; never early-clear it to evade acceptance. If the requested outcome objectively does not fit, never force it or fake a fit: make the next optimal assumption only when it is unambiguously derivable; otherwise invoke the repository's existing **%s** boundary, persist its evidence packet, and surface only the exact human-only decision or external input needed.",
		stopRef,
	)
}

func legacyRenderGoalProviderCondition(goal *legacyGoalContract) (string, error) {
	if goal == nil {
		return "", legacyGoalError(legacyGoalErrorInvalidContract, "goal is required for provider-condition rendering", nil)
	}
	if strings.TrimSpace(goal.ID) == "" || strings.TrimSpace(goal.Owner.RunID) == "" {
		return "", legacyGoalError(legacyGoalErrorInvalidContract, "goal id and owner run_id are required for provider-condition rendering", nil)
	}
	condition := fmt.Sprintf(
		"Own the latest active revision of board goal **%s** for run **%s** through its board-defined success predicate. Before nested spawn, at every directive checkpoint, and before claiming success, run `task-board spawn goal %s`; treat the returned objective, resolved scope, and revision as authoritative and surface them with the required board acceptance evidence. A stale revision, cancelled/superseded goal, or missing scoped acceptance evidence is not success.",
		goal.ID, goal.Owner.RunID, goal.Owner.RunID,
	)
	if err := legacyValidateGoalText(condition, legacyGoalErrorProviderTooLong, "provider_condition"); err != nil {
		return "", err
	}
	return condition, nil
}

func legacyRenderGoalReviewSnapshot(items []legacyGoalScopeItem) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%s=%s", item.ID, item.ReviewPolicy))
	}
	return strings.Join(parts, ", ")
}

func legacyRenderGoalBudget(budget *legacyGoalBudget) (string, error) {
	if err := legacyValidateGoalBudget(budget); err != nil {
		return "", err
	}
	if budget == nil {
		return "", nil
	}
	switch budget.Kind {
	case legacyGoalBudgetNone:
		return " Operator-explicit budget: none.", nil
	case legacyGoalBudgetTokens, legacyGoalBudgetTurns:
		return fmt.Sprintf(" Operator-explicit budget: %d %s; exhaustion is not acceptance and must be surfaced for runtime recovery.", budget.Limit, budget.Kind), nil
	case legacyGoalBudgetDuration:
		return fmt.Sprintf(" Operator-explicit budget: %s duration; exhaustion is not acceptance and must be surfaced for runtime recovery.", budget.Duration), nil
	default:
		return "", legacyGoalError(legacyGoalErrorBudgetInvalid, fmt.Sprintf("unknown budget kind %q", budget.Kind), nil)
	}
}
