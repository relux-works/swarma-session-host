package sessiongoal

import (
	"strings"
	"testing"
)

// Original commit validation ties revision/state, head owner/ref and native
// reflection together; valid active/cleared/cancelled records are kept distinct.
func TestLegacyCommitHeadAndNativeBoundaries(t *testing.T) {
	v := LegacyValidator{}
	for _, state := range []legacyBoardGoalState{legacyBoardGoalStateActive, legacyBoardGoalStateCleared, legacyBoardGoalStateCancelled} {
		r := legacyParentFixture()
		r.State = state
		ref := r.Ref()
		head := LegacyHead{ContractVersion: legacyBoardGoalContractVersion, Owner: r.Owner, LatestRef: &ref, NativeStatus: legacyBoardGoalNativeApplied}
		if state == legacyBoardGoalStateActive {
			head.ActiveRef = &ref
		}
		if e := v.ValidateCommit(&r, &head); e != nil {
			t.Fatal(e)
		}
	}
	for _, name := range []string{"nil_head", "head_version", "owner", "missing_latest", "invalid_latest", "wrong_latest", "missing_active", "different_active", "inactive_active", "unknown_status", "not_supported_required", "empty_head_status"} {
		t.Run(name, func(t *testing.T) {
			r := legacyParentFixture()
			ref := r.Ref()
			head := &LegacyHead{ContractVersion: legacyBoardGoalContractVersion, Owner: r.Owner, LatestRef: &ref, ActiveRef: &ref, NativeStatus: legacyBoardGoalNativeApplied}
			want := legacyGoalErrorInvalidContract
			switch name {
			case "nil_head":
				head = nil
			case "head_version":
				head.ContractVersion = "other"
			case "owner":
				head.Owner = LegacyOwner{Kind: "run", ID: "RUN-other"}
			case "missing_latest":
				head.LatestRef = nil
			case "invalid_latest":
				head.ActiveRef = nil
				head.LatestRef = &LegacyRef{ID: "invalid", Revision: 1}
			case "wrong_latest":
				head.ActiveRef = nil
				head.LatestRef = &LegacyRef{ID: ref.ID, Revision: 2}
			case "missing_active":
				head.ActiveRef = nil
			case "different_active":
				head.ActiveRef = &LegacyRef{ID: ref.ID, Revision: 2}
			case "inactive_active":
				r.State = legacyBoardGoalStateCleared
			case "unknown_status":
				head.NativeStatus = "unknown"
			case "not_supported_required":
				head.NativeStatus = legacyBoardGoalNativeUnavailable
				want = legacyGoalErrorNativeReflectionRequired
			case "empty_head_status":
				head.ActiveRef = nil
				head.LatestRef = nil
			}
			if e := v.ValidateCommit(&r, head); !legacyTestCode(e, want) {
				t.Fatalf("wrong commit refusal: %v", e)
			}
		})
	}
	if e := v.ValidateNativeStatus(nil, "applied"); !legacyTestCode(e, legacyGoalErrorInvalidContract) {
		t.Fatal(e)
	}
	r := legacyParentFixture()
	for _, status := range []string{"applied", "pending-unbound", "pending-ack"} {
		if e := v.ValidateNativeStatus(&r, status); e != nil {
			t.Fatal(e)
		}
	}
}

// Explicit budget provenance and shape are record validation only. Each valid
// kind is paired with malformed/source/actor refusals; no native budget is spent.
func TestLegacyDeliveryBudgetAndScopeBoundaries(t *testing.T) {
	base := legacyDeliveryFixture(t, legacyGoalRoleBinding{Role: "developer", Archetype: "implementer", StartStatus: "development", EndStatus: "to-review"})
	render := func(r *LegacyRevision) {
		t.Helper()
		c := &legacyGoalContract{ID: r.GoalID, Owner: *r.DeliveryOwner, Kind: r.Kind, Scope: r.Scope, RolePolicy: r.RolePolicy, StopBoundaryRef: r.StopBoundaryRef, Budget: r.Budget}
		var e error
		r.Objective, e = legacyRenderGoalObjective(c)
		if e != nil {
			t.Fatal(e)
		}
		r.ProviderCondition, e = legacyRenderGoalProviderCondition(c)
		if e != nil {
			t.Fatal(e)
		}
	}
	for _, kind := range []legacyGoalBudgetKind{legacyGoalBudgetNone, legacyGoalBudgetTokens, legacyGoalBudgetTurns, legacyGoalBudgetDuration} {
		r := legacyClone(t, base)
		b := &legacyGoalBudget{Kind: kind, Source: legacyGoalBudgetSourceOperatorExplicit, Provenance: legacyGoalBudgetProvenance{Actor: legacyGoalActorContext{Kind: legacyGoalActorOperator, ActorID: "operator"}, Authority: legacyGoalAuthorityOperatorAcknowledged}}
		if kind == legacyGoalBudgetTokens || kind == legacyGoalBudgetTurns {
			b.Limit = 3
		}
		if kind == legacyGoalBudgetDuration {
			b.Duration = "1m"
		}
		r.Budget = b
		render(&r)
		if e := (LegacyValidator{}).ValidateRevision(&r); e != nil {
			t.Fatal(e)
		}
		b.Provenance.Actor.Kind = legacyGoalActorAgent
		b.Provenance.Actor.RunID = r.Owner.ID
		b.Provenance.Authority = legacyGoalAuthorityAgentDiscretion
		if e := (LegacyValidator{}).ValidateRevision(&r); !legacyTestCode(e, legacyGoalErrorBudgetInvalid) {
			t.Fatal(e)
		}
	}
	for _, name := range []string{"none_limit", "none_duration", "tokens_zero", "turns_duration", "duration_integer", "duration_zero", "unknown_budget", "budget_source", "budget_authority", "requested_duplicate", "requested_unsorted", "excluded_overlap", "leaf_title", "leaf_kind", "leaf_duplicate", "leaf_status", "scope_case"} {
		t.Run(name, func(t *testing.T) {
			r := legacyClone(t, base)
			r.Budget = &legacyGoalBudget{Kind: legacyGoalBudgetNone, Source: legacyGoalBudgetSourceOperatorExplicit, Provenance: legacyGoalBudgetProvenance{Actor: legacyGoalActorContext{Kind: legacyGoalActorSystem}, Authority: legacyGoalAuthoritySystemAuthorized}}
			want := legacyGoalErrorBudgetInvalid
			switch name {
			case "none_limit":
				r.Budget.Limit = 1
			case "none_duration":
				r.Budget.Duration = "1m"
			case "tokens_zero":
				r.Budget.Kind = legacyGoalBudgetTokens
			case "turns_duration":
				r.Budget.Kind = legacyGoalBudgetTurns
				r.Budget.Limit = 1
				r.Budget.Duration = "1m"
			case "duration_integer":
				r.Budget.Kind = legacyGoalBudgetDuration
				r.Budget.Limit = 1
				r.Budget.Duration = "1m"
			case "duration_zero":
				r.Budget.Kind = legacyGoalBudgetDuration
				r.Budget.Duration = "0s"
			case "unknown_budget":
				r.Budget.Kind = "other"
			case "budget_source":
				r.Budget.Source = "agent_explicit"
			case "budget_authority":
				r.Budget.Provenance.Authority = legacyGoalAuthorityOperatorAcknowledged
			case "requested_duplicate":
				r.Scope.RequestedIDs = append(r.Scope.RequestedIDs, r.Scope.RequestedIDs[0])
			case "requested_unsorted":
				r.Scope.RequestedIDs = []string{"TASK-Z", "TASK-A"}
			case "excluded_overlap":
				r.Scope.ExcludedTerminalIDs = []string{r.Scope.ResolvedLeafItems[0].ID}
			case "leaf_title":
				r.Scope.ResolvedLeafItems[0].Title = ""
			case "leaf_kind":
				r.Scope.ResolvedLeafItems[0].ID = "EPIC-EXAMPLE"
			case "leaf_duplicate":
				r.Scope.ResolvedLeafItems = append(r.Scope.ResolvedLeafItems, r.Scope.ResolvedLeafItems[0])
			case "leaf_status":
				r.Scope.ResolvedLeafItems[0].BaselineStatus = "unknown"
			case "scope_case":
				r.Scope.RequestedIDs[0] = strings.ToLower(r.Scope.RequestedIDs[0])
			}
			if strings.Contains(name, "requested") || strings.Contains(name, "leaf") || name == "excluded_overlap" || name == "scope_case" {
				want = legacyGoalErrorScopeInvalid
				r.Budget = nil
			}
			if e := (LegacyValidator{}).ValidateRevision(&r); !legacyTestCode(e, want) {
				t.Fatalf("wrong boundary refusal: %v", e)
			}
		})
	}
}

// Original v1 compatibility lineage and parent refs survive the v2 projection;
// invalid delivery predecessor/previous revision remains a contract refusal.
func TestLegacyDeliveryRetainsCompatibilityLineage(t *testing.T) {
	r := legacyDeliveryFixture(t, legacyGoalRoleBinding{Role: "developer", Archetype: "implementer", StartStatus: "development", EndStatus: "to-review"})
	r.ParentGoalRef = &LegacyRef{ID: "PRIMARY-GOAL-000000-PARENT", Revision: 2}
	r.DeliveryOwner.ParentGoalID = r.ParentGoalRef.ID
	r.DeliveryOwner.ParentGoalRevision = 2
	r.LegacyRefs = []legacyBoardGoalLegacyRef{{Contract: legacyGoalContractVersion, ID: "GOAL-000000-COMPAT", Revision: 3}}
	r.DeliveryPreviousRevision = 2
	r.DeliveryPredecessorRef = &LegacyRef{ID: "GOAL-000000-PREDEC", Revision: 1}
	c := &legacyGoalContract{ID: r.LegacyRefs[0].ID, Owner: *r.DeliveryOwner, Kind: r.Kind, Scope: r.Scope, RolePolicy: r.RolePolicy, StopBoundaryRef: r.StopBoundaryRef}
	var e error
	r.Objective, e = legacyRenderGoalObjective(c)
	if e != nil {
		t.Fatal(e)
	}
	r.ProviderCondition, e = legacyRenderGoalProviderCondition(c)
	if e != nil {
		t.Fatal(e)
	}
	if e = (LegacyValidator{}).ValidateRevision(&r); e != nil {
		t.Fatal(e)
	}
	got, e := legacyGoalContractFromBoardGoalRevision(&r)
	if e != nil || got.ID != c.ID || got.Revision != 3 || got.PreviousRevision != 2 || got.PredecessorGoalID != r.DeliveryPredecessorRef.ID || got.Owner.ParentGoalID != r.ParentGoalRef.ID || got.Owner.ParentGoalRevision != 2 {
		t.Fatalf("lineage lost: %+v %v", got, e)
	}
	r.DeliveryPreviousRevision = 3
	if e = (LegacyValidator{}).ValidateRevision(&r); !legacyTestCode(e, legacyGoalErrorInvalidContract) {
		t.Fatal(e)
	}
	r.DeliveryPreviousRevision = 2
	r.DeliveryPredecessorRef.Revision = 0
	if e = (LegacyValidator{}).ValidateRevision(&r); !legacyTestCode(e, legacyGoalErrorInvalidContract) {
		t.Fatal(e)
	}
}
