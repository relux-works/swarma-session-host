// Pure legacy protocol data and validation. No actor authentication or storage.
package sessiongoal

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

func legacyValidateBoardGoalOwner(owner legacyBoardGoalOwner) error {
	owner.ID = strings.TrimSpace(owner.ID)
	switch owner.Kind {
	case legacyBoardGoalOwnerParent:
		if owner.ID != legacyPrimaryBoardGoalOwner.ID {
			return legacyGoalError(legacyGoalErrorOwnerInvalid, `parent goal owner id must be "primary"`, nil)
		}
	case legacyBoardGoalOwnerRun:
		if !legacyBoardGoalRunIDPattern.MatchString(owner.ID) {
			return legacyGoalError(legacyGoalErrorOwnerInvalid, fmt.Sprintf("invalid run goal owner %q", owner.ID), nil)
		}
	default:
		return legacyGoalError(legacyGoalErrorOwnerInvalid, fmt.Sprintf("unsupported goal owner kind %q", owner.Kind), nil)
	}
	return nil
}

func legacyCanonicalBoardGoalID(id string) string {
	return strings.ToUpper(strings.TrimSpace(id))
}

func legacyValidateBoardGoalRef(ref legacyBoardGoalRef) error {
	id := legacyCanonicalBoardGoalID(ref.ID)
	if !legacyBoardGoalIDPattern.MatchString(id) {
		return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("invalid board goal id %q", ref.ID), nil)
	}
	if ref.Revision < 1 {
		return legacyGoalError(legacyGoalErrorInvalidContract, "board goal revision must be at least 1", nil)
	}
	return nil
}

func legacyValidateBoardGoalRevision(revision *legacyBoardGoalRevision) error {
	if revision == nil {
		return legacyGoalError(legacyGoalErrorInvalidContract, "board goal revision is required", nil)
	}
	if revision.ContractVersion != legacyBoardGoalContractVersion {
		return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("unsupported board goal contract %q", revision.ContractVersion), nil)
	}
	if err := legacyValidateBoardGoalRef(revision.Ref()); err != nil {
		return err
	}
	if err := legacyValidateBoardGoalOwner(revision.Owner); err != nil {
		return err
	}
	if revision.ExpectedPreviousRevision < 0 {
		return legacyGoalError(legacyGoalErrorInvalidContract, "expected_previous_revision cannot be negative", nil)
	}
	if revision.PreviousRef != nil {
		if err := legacyValidateBoardGoalRef(*revision.PreviousRef); err != nil {
			return fmt.Errorf("previous_ref: %w", err)
		}
	}
	if revision.Revision == 1 && revision.PreviousRef != nil && strings.EqualFold(revision.GoalID, revision.PreviousRef.ID) {
		return legacyGoalError(legacyGoalErrorInvalidContract, "revision 1 previous_ref must identify a predecessor goal", nil)
	}
	if revision.Revision > 1 {
		if revision.PreviousRef == nil ||
			!strings.EqualFold(revision.GoalID, revision.PreviousRef.ID) ||
			revision.PreviousRef.Revision >= revision.Revision {
			return legacyGoalError(legacyGoalErrorInvalidContract, "later revisions must reference an earlier logical-goal revision", nil)
		}
	}
	objective := strings.TrimSpace(revision.Objective)
	if objective == "" || !utf8.ValidString(objective) || utf8.RuneCountInString(objective) > legacyGoalTextLimit {
		return legacyGoalError(legacyGoalErrorObjectiveTooLong, "objective must be valid UTF-8 with 1 to 4,000 characters", nil)
	}
	if strings.TrimSpace(revision.Reason) == "" {
		return legacyGoalError(legacyGoalErrorInvalidContract, "reason is required", nil)
	}
	if revision.CreatedAt.IsZero() || revision.UpdatedAt.IsZero() || revision.UpdatedAt.Before(revision.CreatedAt) {
		return legacyGoalError(legacyGoalErrorInvalidContract, "valid ordered timestamps are required", nil)
	}
	switch revision.State {
	case legacyBoardGoalStateActive, legacyBoardGoalStateCleared, legacyBoardGoalStateCancelled:
	default:
		return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("unsupported board goal state %q", revision.State), nil)
	}
	switch revision.NativeReflection {
	case legacyBoardGoalNativeRequired, legacyBoardGoalNativeNotSupported:
	default:
		return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("unsupported native_reflection %q", revision.NativeReflection), nil)
	}
	switch revision.Owner.Kind {
	case legacyBoardGoalOwnerParent:
		if revision.Kind != legacyGoalKindAlignment || !legacyIsParentGoalSuccessPredicate(revision.SuccessPredicate) {
			return legacyGoalError(legacyGoalErrorRoleIncompatible, "parent/primary requires alignment/routing_duty_clear", nil)
		}
		if revision.DeliveryOwner != nil {
			return legacyGoalError(legacyGoalErrorInvalidContract, "alignment goal cannot carry delivery_owner", nil)
		}
		if revision.State == legacyBoardGoalStateActive {
			if _, err := legacyRequirePrimaryGoalProviderCondition(revision.ProviderCondition); err != nil {
				return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("parent/primary provider_condition: %v", err), err)
			}
		}
	case legacyBoardGoalOwnerRun:
		if revision.Kind == legacyGoalKindAlignment || legacyIsParentGoalSuccessPredicate(revision.SuccessPredicate) {
			return legacyGoalError(legacyGoalErrorRoleIncompatible, "run owner requires a delivery goal kind and acceptance predicate", nil)
		}
		if revision.DeliveryOwner == nil || !strings.EqualFold(revision.DeliveryOwner.RunID, revision.Owner.ID) {
			return legacyGoalError(legacyGoalErrorInvalidContract, "run delivery_owner must match owner id", nil)
		}
		if revision.State == legacyBoardGoalStateActive {
			if _, err := legacyGoalContractFromBoardGoalRevision(revision); err != nil {
				return err
			}
		}
	}
	if revision.ParentGoalRef != nil {
		if err := legacyValidateBoardGoalRef(*revision.ParentGoalRef); err != nil {
			return fmt.Errorf("parent_goal_ref: %w", err)
		}
	}
	for _, legacy := range revision.LegacyRefs {
		if strings.TrimSpace(legacy.Contract) == "" || strings.TrimSpace(legacy.ID) == "" || legacy.Revision < 0 {
			return legacyGoalError(legacyGoalErrorInvalidContract, "legacy_refs entries require contract/id and a non-negative revision", nil)
		}
	}
	return nil
}

func legacyValidateBoardGoalHead(head *legacyBoardGoalHead) error {
	if head == nil {
		return legacyGoalError(legacyGoalErrorInvalidContract, "board goal head is required", nil)
	}
	if head.ContractVersion != legacyBoardGoalContractVersion {
		return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("unsupported board goal head contract %q", head.ContractVersion), nil)
	}
	if err := legacyValidateBoardGoalOwner(head.Owner); err != nil {
		return err
	}
	if head.ActiveRef != nil {
		if err := legacyValidateBoardGoalRef(*head.ActiveRef); err != nil {
			return err
		}
		if head.LatestRef == nil || !legacyRefEqual(*head.ActiveRef, *head.LatestRef) {
			return legacyGoalError(legacyGoalErrorInvalidContract, "active_ref must equal latest_ref", nil)
		}
	}
	if head.LatestRef != nil {
		if err := legacyValidateBoardGoalRef(*head.LatestRef); err != nil {
			return err
		}
		switch head.NativeStatus {
		case legacyBoardGoalNativeApplied,
			legacyBoardGoalNativePendingUnbound,
			legacyBoardGoalNativePendingAck,
			legacyBoardGoalNativeUnavailable:
		default:
			return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("unsupported native status %q", head.NativeStatus), nil)
		}
	} else if head.NativeStatus != "" {
		return legacyGoalError(legacyGoalErrorInvalidContract, "empty owner head cannot carry native status", nil)
	}
	return nil
}

func legacyValidateBoardGoalCommit(revision *legacyBoardGoalRevision, head *legacyBoardGoalHead) error {
	if err := legacyValidateBoardGoalRevision(revision); err != nil {
		return err
	}
	if err := legacyValidateBoardGoalHead(head); err != nil {
		return err
	}
	if !legacyOwnerEqual(revision.Owner, head.Owner) {
		return legacyGoalError(legacyGoalErrorInvalidContract, "revision and head owners differ", nil)
	}
	ref := revision.Ref()
	if head.LatestRef == nil || !legacyRefEqual(*head.LatestRef, ref) {
		return legacyGoalError(legacyGoalErrorInvalidContract, "head latest_ref must reference committed revision", nil)
	}
	if revision.State == legacyBoardGoalStateActive {
		if head.ActiveRef == nil || !legacyRefEqual(*head.ActiveRef, ref) {
			return legacyGoalError(legacyGoalErrorInvalidContract, "active revision must be the active owner head", nil)
		}
	} else if head.ActiveRef != nil {
		return legacyGoalError(legacyGoalErrorInvalidContract, "cleared or cancelled revision must remove active_ref", nil)
	}
	if err := legacyValidateBoardGoalNativeStatus(revision, head.NativeStatus); err != nil {
		return err
	}
	return nil
}

func legacyValidateBoardGoalNativeStatus(revision *legacyBoardGoalRevision, status legacyBoardGoalNativeStatus) error {
	if revision == nil {
		return legacyGoalError(legacyGoalErrorInvalidContract, "board goal revision is required for native status validation", nil)
	}
	switch revision.NativeReflection {
	case legacyBoardGoalNativeRequired:
		if status != legacyBoardGoalNativeApplied &&
			status != legacyBoardGoalNativePendingUnbound &&
			status != legacyBoardGoalNativePendingAck {
			return legacyGoalError(
				legacyGoalErrorNativeReflectionRequired,
				"required native reflection must be applied, pending-unbound, or pending-ack",
				nil,
			)
		}
	case legacyBoardGoalNativeNotSupported:
		if status != legacyBoardGoalNativeUnavailable {
			return legacyGoalError(legacyGoalErrorInvalidContract, "not-supported native reflection requires not-supported status", nil)
		}
	default:
		return legacyGoalError(legacyGoalErrorInvalidContract, fmt.Sprintf("unsupported native_reflection %q", revision.NativeReflection), nil)
	}
	return nil
}

func legacyGoalContractFromBoardGoalRevision(revision *legacyBoardGoalRevision) (*legacyGoalContract, error) {
	if revision == nil {
		return nil, legacyGoalError(legacyGoalErrorInvalidContract, "board goal revision is required", nil)
	}
	if revision.Owner.Kind != legacyBoardGoalOwnerRun || revision.DeliveryOwner == nil {
		return nil, legacyGoalError(legacyGoalErrorOwnerInvalid, "board goal is not a run delivery revision", nil)
	}
	compatibilityRef := revision.Ref()
	for _, legacy := range revision.LegacyRefs {
		if legacy.Contract == legacyGoalContractVersion && legacy.Revision > 0 {
			compatibilityRef = legacyBoardGoalRef{ID: legacy.ID, Revision: legacy.Revision}
			break
		}
	}
	goal := &legacyGoalContract{
		Version:           legacyGoalContractVersion,
		ID:                compatibilityRef.ID,
		Kind:              revision.Kind,
		Owner:             *revision.DeliveryOwner,
		Scope:             legacyCloneGoalScope(revision.Scope),
		RolePolicy:        revision.RolePolicy,
		ObjectiveIntent:   revision.ObjectiveIntent,
		Objective:         revision.Objective,
		ProviderCondition: revision.ProviderCondition,
		SuccessPredicate:  revision.SuccessPredicate,
		StopBoundaryRef:   revision.StopBoundaryRef,
		Revision:          compatibilityRef.Revision,
		PreviousRevision:  revision.DeliveryPreviousRevision,
		Mutation:          revision.Mutation,
		Budget:            legacyCloneGoalBudget(revision.Budget),
		CreatedAt:         revision.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	if revision.DeliveryPredecessorRef != nil {
		goal.PredecessorGoalID = revision.DeliveryPredecessorRef.ID
		goal.PredecessorGoalRevision = revision.DeliveryPredecessorRef.Revision
	}
	if revision.ParentGoalRef != nil {
		goal.Owner.ParentGoalID = revision.ParentGoalRef.ID
		goal.Owner.ParentGoalRevision = revision.ParentGoalRef.Revision
	}
	if err := legacyValidateGoalContract(goal); err != nil {
		return nil, err
	}
	return goal, nil
}

func legacyIsParentGoalSuccessPredicate(predicate legacyGoalSuccessPredicate) bool {
	return predicate == legacyGoalSuccessRoutingDutyClear || predicate == legacyGoalSuccessAlignmentHandled
}
func legacyRequirePrimaryGoalProviderCondition(condition string) (string, error) {
	if !utf8.ValidString(condition) {
		return "", legacyPrimaryGoalProviderConditionError("provider condition must be valid UTF-8")
	}
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return "", legacyPrimaryGoalProviderConditionError("provider condition must not be empty")
	}
	if utf8.RuneCountInString(condition) > legacyPrimaryGoalObjectiveLimit {
		return "", legacyPrimaryGoalProviderConditionError(
			fmt.Sprintf("provider condition exceeds %d UTF-8 characters", legacyPrimaryGoalObjectiveLimit),
		)
	}
	for _, r := range condition {
		switch {
		case r == ':':
			return "", legacyPrimaryGoalProviderConditionError(
				"provider condition must not contain a colon; every native acknowledgement event prefix ends in one",
			)
		case r < 0x20 || r == 0x7f:
			return "", legacyPrimaryGoalProviderConditionError("provider condition must not contain control characters")
		}
	}
	return condition, nil
}
