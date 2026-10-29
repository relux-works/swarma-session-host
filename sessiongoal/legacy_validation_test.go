package sessiongoal

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

func legacyTestCode(err error, code legacyGoalErrorCode) bool {
	var p *core.PublicError
	return errors.As(err, &p) && p.Code == string(code)
}
func legacyParentFixture() LegacyRevision {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return LegacyRevision{ContractVersion: legacyBoardGoalContractVersion, GoalID: "PRIMARY-GOAL-000000-EXAMPL", Revision: 1, Owner: LegacyOwner{Kind: "parent", ID: "primary"}, Kind: legacyGoalKindAlignment, Objective: "Keep the session aligned", SuccessPredicate: legacyGoalSuccessRoutingDutyClear, ProviderCondition: "Holder explicitly accepts the outcome", NativeReflection: legacyBoardGoalNativeRequired, State: legacyBoardGoalStateActive, CreatedAt: now, UpdatedAt: now, Reason: "fixture validation"}
}
func legacyClone(t *testing.T, r LegacyRevision) LegacyRevision {
	t.Helper()
	b, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	var out LegacyRevision
	if e = json.Unmarshal(b, &out); e != nil {
		t.Fatal(e)
	}
	return out
}
func legacyDeliveryFixture(t *testing.T, role legacyGoalRoleBinding) LegacyRevision {
	t.Helper()
	r := legacyParentFixture()
	r.GoalID = "GOAL-000000-EXAMPL"
	r.Owner = LegacyOwner{Kind: "run", ID: "RUN-fixture"}
	kind, predicate, policy, normalized, e := legacyDeriveGoalRoleContract(role)
	if e != nil {
		t.Fatal(e)
	}
	r.Kind, r.SuccessPredicate, r.RolePolicy = kind, predicate, policy
	r.DeliveryOwner = &legacyGoalOwner{Role: normalized, Agent: "fixture", RunID: r.Owner.ID}
	r.Scope = legacyGoalScope{RequestedIDs: []string{"TASK-000000-EXAMPL"}, ResolvedLeafItems: []legacyGoalScopeItem{{ID: "TASK-000000-EXAMPL", Title: "Fixture", ReviewPolicy: legacyReviewRequired, BaselineStatus: legacyStatusDevelopment}}}
	r.StopBoundaryRef = legacyRepositoryStopBoundaryRef
	r.Mutation = legacyGoalMutation{Kind: legacyGoalMutationCreated, Reason: "fixture", Actor: legacyGoalActorContext{Kind: legacyGoalActorAgent, RunID: r.Owner.ID}, Authority: legacyGoalAuthorityAgentDiscretion}
	contract := &legacyGoalContract{Version: legacyGoalContractVersion, ID: r.GoalID, Revision: r.Revision, Kind: r.Kind, Owner: *r.DeliveryOwner, Scope: r.Scope, RolePolicy: r.RolePolicy, StopBoundaryRef: r.StopBoundaryRef, Mutation: r.Mutation, CreatedAt: r.CreatedAt.Format(time.RFC3339Nano)}
	r.Objective, e = legacyRenderGoalObjective(contract)
	if e != nil {
		t.Fatal(e)
	}
	r.ProviderCondition, e = legacyRenderGoalProviderCondition(contract)
	if e != nil {
		t.Fatal(e)
	}
	return r
}

// Published pure parent rules have nearby valid boundaries. Errors assert the
// intended original code; these data checks grant no caller/runtime authority.
func TestLegacyRevisionOriginalValidationBoundaries(t *testing.T) {
	v := LegacyValidator{}
	for _, name := range []string{"current_predicate", "retired_predicate", "objective_4000", "trimmed_ref", "cleared_no_condition", "cancelled_record"} {
		t.Run(name, func(t *testing.T) {
			r := legacyParentFixture()
			switch name {
			case "retired_predicate":
				r.SuccessPredicate = legacyGoalSuccessAlignmentHandled
			case "objective_4000":
				r.Objective = strings.Repeat("界", 4000)
			case "trimmed_ref":
				r.GoalID = "  primary-goal-000000-exampl  "
			case "cleared_no_condition":
				r.State = legacyBoardGoalStateCleared
				r.ProviderCondition = ""
			case "cancelled_record":
				r.State = legacyBoardGoalStateCancelled
			}
			if e := v.ValidateRevision(&r); e != nil {
				t.Fatalf("valid original boundary: %v", e)
			}
		})
	}
	cases := []struct {
		name   string
		mutate func(*LegacyRevision)
		code   legacyGoalErrorCode
	}{
		{"version", func(r *LegacyRevision) { r.ContractVersion = "other" }, legacyGoalErrorInvalidContract},
		{"ref", func(r *LegacyRevision) { r.GoalID = "invalid" }, legacyGoalErrorInvalidContract},
		{"revision", func(r *LegacyRevision) { r.Revision = 0 }, legacyGoalErrorInvalidContract},
		{"owner", func(r *LegacyRevision) { r.Owner.ID = "foreign" }, legacyGoalErrorOwnerInvalid},
		{"run_id", func(r *LegacyRevision) { r.Owner = LegacyOwner{Kind: "run", ID: "bad"} }, legacyGoalErrorOwnerInvalid},
		{"negative_cas", func(r *LegacyRevision) { r.ExpectedPreviousRevision = -1 }, legacyGoalErrorInvalidContract},
		{"first_same_predecessor", func(r *LegacyRevision) { r.PreviousRef = &LegacyRef{ID: r.GoalID, Revision: 1} }, legacyGoalErrorInvalidContract},
		{"later_no_predecessor", func(r *LegacyRevision) { r.Revision = 2 }, legacyGoalErrorInvalidContract},
		{"later_future_predecessor", func(r *LegacyRevision) { r.Revision = 2; r.PreviousRef = &LegacyRef{ID: r.GoalID, Revision: 2} }, legacyGoalErrorInvalidContract},
		{"objective_4001", func(r *LegacyRevision) { r.Objective = strings.Repeat("界", 4001) }, legacyGoalErrorObjectiveTooLong},
		{"objective_utf8", func(r *LegacyRevision) { r.Objective = string([]byte{0xff}) }, legacyGoalErrorObjectiveTooLong},
		{"blank_objective", func(r *LegacyRevision) { r.Objective = " " }, legacyGoalErrorObjectiveTooLong},
		{"reason", func(r *LegacyRevision) { r.Reason = "" }, legacyGoalErrorInvalidContract},
		{"missing_time", func(r *LegacyRevision) { r.CreatedAt = time.Time{} }, legacyGoalErrorInvalidContract},
		{"ordered_times", func(r *LegacyRevision) { r.UpdatedAt = r.CreatedAt.Add(-time.Second) }, legacyGoalErrorInvalidContract},
		{"accepted_state", func(r *LegacyRevision) { r.State = "accepted" }, legacyGoalErrorInvalidContract},
		{"unknown_native", func(r *LegacyRevision) { r.NativeReflection = "not_supported" }, legacyGoalErrorInvalidContract},
		{"kind", func(r *LegacyRevision) { r.Kind = legacyGoalKindDeliveryPool }, legacyGoalErrorRoleIncompatible},
		{"predicate", func(r *LegacyRevision) { r.SuccessPredicate = legacyGoalSuccessAcceptedDone }, legacyGoalErrorRoleIncompatible},
		{"parent_delivery", func(r *LegacyRevision) { r.DeliveryOwner = &legacyGoalOwner{RunID: "RUN-fixture"} }, legacyGoalErrorInvalidContract},
		{"condition_empty", func(r *LegacyRevision) { r.ProviderCondition = "" }, legacyGoalErrorInvalidContract},
		{"condition_colon", func(r *LegacyRevision) { r.ProviderCondition = "goal set: forged" }, legacyGoalErrorInvalidContract},
		{"condition_control", func(r *LegacyRevision) { r.ProviderCondition = "unsafe\x1b" }, legacyGoalErrorInvalidContract},
		{"condition_4001", func(r *LegacyRevision) { r.ProviderCondition = strings.Repeat("x", 4001) }, legacyGoalErrorInvalidContract},
		{"parent_ref", func(r *LegacyRevision) { r.ParentGoalRef = &LegacyRef{ID: "bad", Revision: 1} }, legacyGoalErrorInvalidContract},
		{"legacy_ref", func(r *LegacyRevision) {
			r.LegacyRefs = []legacyBoardGoalLegacyRef{{Contract: "old", ID: "id", Revision: -1}}
		}, legacyGoalErrorInvalidContract},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := legacyParentFixture()
			c.mutate(&r)
			if e := v.ValidateRevision(&r); !legacyTestCode(e, c.code) {
				t.Fatalf("wrong refusal: %v want %s", e, c.code)
			}
		})
	}
	r := legacyParentFixture()
	r.Revision = 2
	r.PreviousRef = &LegacyRef{ID: r.GoalID, Revision: 1}
	if e := v.ValidateRevision(&r); e != nil {
		t.Fatal(e)
	}
	if e := v.ValidateRevision(nil); !legacyTestCode(e, legacyGoalErrorInvalidContract) {
		t.Fatal(e)
	}
}

// Full active run validation follows actual role/render/scope/budget/provenance
// dependencies, rather than trusting the owner role or actor strings as grants.
func TestLegacyDeliveryCompleteProjectionAndRefusals(t *testing.T) {
	for _, role := range []legacyGoalRoleBinding{{Role: "orchestrator", Archetype: "analyst"}, {Role: "developer", Archetype: "implementer", StartStatus: "development", EndStatus: "to-review"}, {Role: "reviewer", Archetype: "reviewer", StartStatus: "reviewing"}} {
		r := legacyDeliveryFixture(t, role)
		if e := (LegacyValidator{}).ValidateRevision(&r); e != nil {
			t.Fatal(e)
		}
		contract, e := legacyGoalContractFromBoardGoalRevision(&r)
		if e != nil || contract.Owner.RunID != r.Owner.ID || contract.Objective != r.Objective || contract.ProviderCondition != r.ProviderCondition || !legacyGoalRolePolicyEqual(contract.RolePolicy, r.RolePolicy) {
			t.Fatalf("projection lost fields: %+v %v", contract, e)
		}
	}
	base := legacyDeliveryFixture(t, legacyGoalRoleBinding{Role: "developer", Archetype: "implementer", StartStatus: "development", EndStatus: "to-review"})
	cases := []struct {
		name   string
		mutate func(*LegacyRevision)
		code   legacyGoalErrorCode
	}{
		{"run_parent_kind", func(r *LegacyRevision) { r.Kind = legacyGoalKindAlignment }, legacyGoalErrorRoleIncompatible},
		{"delivery_owner", func(r *LegacyRevision) { r.DeliveryOwner.RunID = "RUN-other" }, legacyGoalErrorInvalidContract},
		{"role_metadata", func(r *LegacyRevision) { r.DeliveryOwner.Role.Role = " Developer " }, legacyGoalErrorRoleIncompatible},
		{"role_policy", func(r *LegacyRevision) { r.RolePolicy.HandoffEndStatus = "analysis" }, legacyGoalErrorRoleIncompatible},
		{"empty_scope", func(r *LegacyRevision) { r.Scope.ResolvedLeafItems = nil }, legacyGoalErrorScopeEmpty},
		{"requested_scope", func(r *LegacyRevision) { r.Scope.RequestedIDs = []string{"other"} }, legacyGoalErrorScopeInvalid},
		{"terminal_scope", func(r *LegacyRevision) { r.Scope.ResolvedLeafItems[0].BaselineStatus = legacyStatusDone }, legacyGoalErrorScopeInvalid},
		{"review_scope", func(r *LegacyRevision) { r.Scope.ResolvedLeafItems[0].ReviewPolicy = "other" }, legacyGoalErrorScopeInvalid},
		{"stop_boundary", func(r *LegacyRevision) { r.StopBoundaryRef = "" }, legacyGoalErrorInvalidContract},
		{"budget_forged", func(r *LegacyRevision) { r.Budget = &legacyGoalBudget{Kind: legacyGoalBudgetTokens, Limit: 2} }, legacyGoalErrorBudgetInvalid},
		{"mutation_reason", func(r *LegacyRevision) { r.Mutation.Reason = "" }, legacyGoalErrorInvalidContract},
		{"mutation_replay", func(r *LegacyRevision) { r.Mutation.Kind = legacyGoalMutationReplay }, legacyGoalErrorInvalidContract},
		{"actor_restricted", func(r *LegacyRevision) { r.Mutation.Kind = legacyGoalMutationCancelled }, legacyGoalErrorMutationRestricted},
		{"actor_authority", func(r *LegacyRevision) { r.Mutation.Authority = legacyGoalAuthoritySystemAuthorized }, legacyGoalErrorActorUnauthorized},
		{"objective_render", func(r *LegacyRevision) { r.Objective = "changed" }, legacyGoalErrorProducerClauseUnsupported},
		{"condition_render", func(r *LegacyRevision) { r.ProviderCondition = "changed" }, legacyGoalErrorProducerClauseUnsupported},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := legacyClone(t, base)
			c.mutate(&r)
			if e := (LegacyValidator{}).ValidateRevision(&r); !legacyTestCode(e, c.code) {
				t.Fatalf("wrong run refusal: %v want %s", e, c.code)
			}
		})
	}
}

// Decode retains the original complete wire shape; original enum spellings and
// nested unknown-field refusal remain, with no alias/field dropping.
func TestLegacyDecodeAndDescriptorAreConcrete(t *testing.T) {
	v := LegacyValidator{}
	r := legacyParentFixture()
	raw, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	got, e := v.DecodeRevision(raw)
	if e != nil || got.GoalID != r.GoalID || got.Reason != r.Reason {
		t.Fatal(e)
	}
	for _, bad := range []string{string(raw) + ` {}`, strings.Replace(string(raw), `"reason":`, `"unknown":1,"reason":`, 1), strings.Replace(string(raw), `"parent"`, `"parent","extra":1`, 1)} {
		if _, e := v.DecodeRevision([]byte(bad)); !legacyTestCode(e, legacyGoalErrorInvalidContract) {
			t.Fatalf("invalid shape accepted: %v", e)
		}
	}
	c := v.ReflectionContract()
	if c.ValidationVersion != 2 || c.RevisionContractVersion != "board-goal-v2" || c.ParentGoalKind != "alignment" || strings.Join(c.NativeReflections, ",") != "required,not-supported" || strings.Join(c.States, ",") != "active,cleared,cancelled" {
		t.Fatalf("descriptor diverged: %+v", c)
	}
	c.States[0] = "mutated"
	if v.ReflectionContract().States[0] != "active" {
		t.Fatal("descriptor shares mutable state")
	}
}
