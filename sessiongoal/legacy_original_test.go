package sessiongoal

import (
	"testing"
	"time"
)

// Original parent predicate rejection remains exact, with neutral wire types.
func TestBoardGoalV2ParentAlignmentCannotBecomeBoardAcceptance(t *testing.T) {
	now := time.Date(2026, 7, 24, 14, 0, 0, 0, time.UTC)
	revision := &legacyBoardGoalRevision{
		ContractVersion:   legacyBoardGoalContractVersion,
		GoalID:            "PRIMARY-GOAL-000000-EXAMPL",
		Revision:          1,
		Owner:             legacyPrimaryBoardGoalOwner,
		Kind:              legacyGoalKindAlignment,
		Objective:         "Keep the parent aligned.",
		MutationAuthority: legacyGoalAuthorityOperatorAcknowledged,
		SuccessPredicate:  legacyGoalSuccessAcceptedDone,
		ProviderCondition: "The parent handled this alignment.",
		NativeReflection:  legacyBoardGoalNativeRequired,
		State:             legacyBoardGoalStateActive,
		Mutation: legacyGoalMutation{
			Kind:      legacyGoalMutationCreated,
			Reason:    "alignment validation test",
			Actor:     legacyGoalActorContext{Kind: legacyGoalActorOperator, ActorID: "test"},
			Authority: legacyGoalAuthorityOperatorAcknowledged,
		},
		CreatedAt: now,
		UpdatedAt: now,
		CreatedBy: "test",
		Reason:    "alignment validation test",
	}
	if err := legacyValidateBoardGoalRevision(revision); !legacyTestCode(err, legacyGoalErrorRoleIncompatible) {
		t.Fatalf("legacyValidateBoardGoalRevision error = %v, want role-incompatible acceptance predicate", err)
	}
}

// Original required/pending/not-supported commit positives and refusals remain.
func TestBoardGoalV2NativeStatusMatchesReflectionContract(t *testing.T) {
	now := time.Date(2026, 7, 24, 14, 30, 0, 0, time.UTC)
	revision := &legacyBoardGoalRevision{
		ContractVersion:   legacyBoardGoalContractVersion,
		GoalID:            "PRIMARY-GOAL-000000-EXAMPL",
		Revision:          1,
		Owner:             legacyPrimaryBoardGoalOwner,
		Kind:              legacyGoalKindAlignment,
		Objective:         "Keep the parent aligned.",
		MutationAuthority: legacyGoalAuthorityOperatorAcknowledged,
		SuccessPredicate:  legacyGoalSuccessAlignmentHandled,
		ProviderCondition: "The parent handled this alignment.",
		NativeReflection:  legacyBoardGoalNativeRequired,
		State:             legacyBoardGoalStateActive,
		Mutation: legacyGoalMutation{
			Kind:      legacyGoalMutationCreated,
			Reason:    "native status validation",
			Actor:     legacyGoalActorContext{Kind: legacyGoalActorOperator, ActorID: "test"},
			Authority: legacyGoalAuthorityOperatorAcknowledged,
		},
		CreatedAt: now,
		UpdatedAt: now,
		CreatedBy: "test",
		Reason:    "native status validation",
	}
	ref := revision.Ref()
	head := &legacyBoardGoalHead{
		ContractVersion: legacyBoardGoalContractVersion,
		Owner:           revision.Owner,
		ActiveRef:       &ref,
		LatestRef:       &ref,
		NativeStatus:    legacyBoardGoalNativeUnavailable,
	}
	if err := legacyValidateBoardGoalCommit(revision, head); !legacyTestCode(err, legacyGoalErrorNativeReflectionRequired) {
		t.Fatalf("required/unavailable commit error = %v", err)
	}
	head.NativeStatus = legacyBoardGoalNativePendingUnbound
	if err := legacyValidateBoardGoalCommit(revision, head); err != nil {
		t.Fatalf("required/pending commit: %v", err)
	}
	revision.NativeReflection = legacyBoardGoalNativeNotSupported
	if err := legacyValidateBoardGoalCommit(revision, head); !legacyTestCode(err, legacyGoalErrorInvalidContract) {
		t.Fatalf("not-supported/pending commit error = %v", err)
	}
	head.NativeStatus = legacyBoardGoalNativeUnavailable
	if err := legacyValidateBoardGoalCommit(revision, head); err != nil {
		t.Fatalf("not-supported/unavailable commit: %v", err)
	}
}

// Original role normalization, role-derived policy and refusal table is retained.
func TestDeriveGoalRoleContract_TableDriven(t *testing.T) {
	tests := []struct {
		name          string
		binding       legacyGoalRoleBinding
		wantRole      string
		wantKind      legacyGoalKind
		wantPredicate legacyGoalSuccessPredicate
		wantEnd       string
		wantVerdicts  int
		wantCode      legacyGoalErrorCode
	}{
		{
			name:          "orchestrator delivery",
			binding:       legacyGoalRoleBinding{Role: " orchestrator ", Archetype: "ANALYST"},
			wantRole:      "orchestrator",
			wantKind:      legacyGoalKindDeliveryPool,
			wantPredicate: legacyGoalSuccessAcceptedDone,
		},
		{
			name:          "coordinator normalizes to orchestrator",
			binding:       legacyGoalRoleBinding{Role: "coordinator", Archetype: "analyst"},
			wantRole:      "orchestrator",
			wantKind:      legacyGoalKindDeliveryPool,
			wantPredicate: legacyGoalSuccessAcceptedDone,
		},
		{
			name:          "specialist handoff",
			binding:       legacyGoalRoleBinding{Role: "developer", Archetype: "implementer", StartStatus: "development", EndStatus: "to-review"},
			wantRole:      "developer",
			wantKind:      legacyGoalKindRoleHandoff,
			wantPredicate: legacyGoalSuccessRoleHandoff,
			wantEnd:       "to-review",
		},
		{
			name:          "reviewer verdict",
			binding:       legacyGoalRoleBinding{Role: "architecture-reviewer", Archetype: "reviewer", StartStatus: "reviewing"},
			wantRole:      "architecture-reviewer",
			wantKind:      legacyGoalKindReviewerVerdict,
			wantPredicate: legacyGoalSuccessReviewerVerdict,
			wantVerdicts:  3,
		},
		{name: "unknown role metadata", binding: legacyGoalRoleBinding{Role: "mystery"}, wantCode: legacyGoalErrorRoleIncompatible},
		{name: "orchestrator incompatible lifecycle", binding: legacyGoalRoleBinding{Role: "orchestrator", Archetype: "analyst", EndStatus: "to-review"}, wantCode: legacyGoalErrorRoleIncompatible},
		{name: "specialist missing handoff", binding: legacyGoalRoleBinding{Role: "developer", Archetype: "implementer", StartStatus: "development"}, wantCode: legacyGoalErrorRoleIncompatible},
		{name: "reviewer cannot have end status", binding: legacyGoalRoleBinding{Role: "reviewer", Archetype: "reviewer", StartStatus: "reviewing", EndStatus: "done"}, wantCode: legacyGoalErrorRoleIncompatible},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, predicate, policy, normalized, err := legacyDeriveGoalRoleContract(tt.binding)
			if tt.wantCode != "" {
				if !legacyTestCode(err, tt.wantCode) {
					t.Fatalf("error = %v, want %s", err, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("legacyDeriveGoalRoleContract: %v", err)
			}
			if normalized.Role != tt.wantRole || kind != tt.wantKind || predicate != tt.wantPredicate {
				t.Fatalf("got role=%s kind=%s predicate=%s", normalized.Role, kind, predicate)
			}
			if policy.HandoffEndStatus != tt.wantEnd || len(policy.AllowedReviewerVerdicts) != tt.wantVerdicts {
				t.Fatalf("policy = %#v", policy)
			}
		})
	}
}
