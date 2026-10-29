package sessionmanager

import (
	"errors"
	"reflect"
	"testing"
)

// Full original native accounting survives the typed carrier/project/merge,
// including baseline/ledger/pause, without aliasing. This is data preservation,
// not active Restore admission; the default codec guard remains unchanged.
func TestMuseNativeAccountingCarrierPreservesAndCopies(t *testing.T) {
	original := legacyMuseSnapshot{GoalTransport: "native", SessionID: "native-id", LegacyMuseNativeAccounting: LegacyMuseNativeAccounting{GoalObserved: true, GoalRef: &LegacyGoalRef{ID: "GOAL-SG-EXAMPLE", Revision: 2}, GoalObjective: "objective", GoalStatus: "paused", TokenBudget: 30, TokenUsageTotal: 57, TokenUsageBaseline: 27, BudgetPaused: true, BudgetPauseOutcome: &LegacyMuseBudgetPauseOutcome{Code: "matched", NativeStatus: "paused"}, ExternalPause: true, ActiveTurnID: "turn", LastAnnouncedGoal: &LegacyMuseGoalAnnouncement{Objective: "objective", Status: "paused"}, LastGoalLifecycle: &LegacyMuseGoalAnnouncement{Status: "paused"}, GoalLedger: map[string]LegacyMuseGoalRevisionFacts{"GOAL-SG-EXAMPLE@2": {Baseline: 27, Spend: 30, BudgetPaused: true, PauseOutcome: &LegacyMuseBudgetPauseOutcome{Code: "matched"}}}}}
	projected := projectMuseSnapshot(original)
	if projected.NativeAccounting == nil || !reflect.DeepEqual(*projected.NativeAccounting, original.LegacyMuseNativeAccounting) {
		t.Fatal("projection lost full accounting")
	}
	merged := mergeMuseSnapshot(legacyMuseSnapshot{}, projected)
	if !reflect.DeepEqual(merged.LegacyMuseNativeAccounting, original.LegacyMuseNativeAccounting) {
		t.Fatal("merge lost accounting")
	}
	cloned := copyRunnerSnapshot(RunnerSnapshot{Kind: "muse-msp", MuseMSP: &projected})
	cloned.MuseMSP.NativeAccounting.GoalRef.ID = "changed"
	v := cloned.MuseMSP.NativeAccounting.GoalLedger["GOAL-SG-EXAMPLE@2"]
	v.PauseOutcome.Code = "changed"
	cloned.MuseMSP.NativeAccounting.LastAnnouncedGoal.Objective = "changed"
	cloned.MuseMSP.NativeAccounting.BudgetPauseOutcome.Code = "changed"
	if projected.NativeAccounting.GoalRef.ID != "GOAL-SG-EXAMPLE" || projected.NativeAccounting.GoalLedger["GOAL-SG-EXAMPLE@2"].PauseOutcome.Code != "matched" || projected.NativeAccounting.LastAnnouncedGoal.Objective != "objective" || projected.NativeAccounting.BudgetPauseOutcome.Code != "matched" {
		t.Fatal("native accounting carrier aliases another cut")
	}
}

// Cold native process observations already have typed counter/pause/turn fields;
// adding an empty carrier must not make the real sole-writer fixed point fail.
func TestMuseNativeAccountingCarrierColdAbsence(t *testing.T) {
	physical := legacyMuseSnapshot{GoalTransport: "native", LegacyMuseNativeAccounting: LegacyMuseNativeAccounting{TokenUsageTotal: 7, ExternalPause: true, ActiveTurnID: "turn"}}
	got := projectMuseSnapshot(physical)
	if got.NativeAccounting != nil || got.TokenUsageTotal != 7 || !got.ExternalPause || got.ActiveTurnID != "turn" {
		t.Fatal("cold observations gained an empty goal carrier or lost physical facts")
	}
}

// A physical sample cannot reset/credit canonical counters or swap native ID;
// the real accounting consumer must commit its fold before host snapshots move it.
func TestMuseNativeAccountingPhysicalSnapshotKeepsCommittedGroup(t *testing.T) {
	group := LegacyMuseNativeAccounting{GoalObserved: true, TokenUsageTotal: 57, TokenUsageBaseline: 27, TokenBudget: 30, GoalLedger: map[string]LegacyMuseGoalRevisionFacts{"revision": {Baseline: 27, Spend: 30}}, ExternalPause: true, ActiveTurnID: "committed-turn"}
	record := RunnerRecord{ProviderHost: ProviderHostRecord{Snapshot: RunnerSnapshot{Kind: "muse-msp", MuseMSP: &MuseRunnerSnapshot{SessionID: "same-native", NativeAccounting: &group}}}}
	physical := RunnerSnapshot{Kind: "muse-msp", MuseMSP: &MuseRunnerSnapshot{SessionID: "same-native", TokenUsageTotal: 999, ActiveTurnID: "unfolded-turn"}}
	got, e := snapshotWithCommittedAccounting(record, physical)
	if e != nil || got.MuseMSP.TokenUsageTotal != 57 || got.MuseMSP.ActiveTurnID != "committed-turn" || !reflect.DeepEqual(*got.MuseMSP.NativeAccounting, group) {
		t.Fatalf("accounting reset or synthetic fold: %+v %v", got, e)
	}
	physical.MuseMSP.SessionID = "different-native"
	if _, e = snapshotWithCommittedAccounting(record, physical); !errors.Is(e, ErrLegacyViewStale) {
		t.Fatalf("native identity replaced accounting: %v", e)
	}
}
