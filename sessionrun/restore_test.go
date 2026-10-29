package sessionrun

import (
	"encoding/json"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"reflect"
	"testing"
)

// Native restore credits only the high-water delta and retains the entire
// original goal/baseline/ledger/pause group. This pure fold grants no authority.
func TestMuseRestoreNativeAccountingLossless(t *testing.T) {
	prior := core.LegacyMuseAccountingSnapshot{SessionID: "native", WorkspaceRoot: "/workspace", GoalTransport: "native", LegacyMuseNativeAccounting: core.LegacyMuseNativeAccounting{GoalObserved: true, GoalRef: &core.LegacyGoalRef{ID: "GOAL-ABC", Revision: 2}, GoalObjective: "current", GoalStatus: "paused", TokenBudget: 100, TokenUsageTotal: 30, TokenUsageBaseline: 7, BudgetPaused: true, BudgetPauseOutcome: &core.LegacyMuseBudgetPauseOutcome{Code: "budget_pause_not_applicable", NativeStatus: "paused"}, ExternalPause: true, ActiveTurnID: "old", GoalLedger: map[string]core.LegacyMuseGoalRevisionFacts{"old": {Baseline: 2, Spend: 19, BudgetPaused: true}, "current": {Baseline: 7, Spend: 23, ExternalPause: true}}}}
	fold := core.MuseRestoreFold{SessionID: "native", Cursor: "cursor", Cumulative: core.MuseRestoreUsage{TotalTokens: 33, PromptTokens: 20, OutputTokens: 13}, Items: []json.RawMessage{}, ActiveTurn: &core.MuseRestoreTurnRef{TurnID: "fallback"}, SessionActiveTurnID: "current-turn"}
	next, err := restoreMuseAccounting(prior, &fold)
	if err != nil {
		t.Fatal(err)
	}
	want := prior
	want.TokenUsageTotal = 33
	want.ActiveTurnID = "current-turn"
	if !reflect.DeepEqual(next, want) {
		t.Fatalf("restore changed accounting: got %+v want %+v", next, want)
	}
	next.GoalLedger["old"] = core.LegacyMuseGoalRevisionFacts{}
	if prior.GoalLedger["old"].Spend != 19 {
		t.Fatal("restored state aliases committed ledger")
	}
}

// Foreign physical identity, unknown items and regressing counters refuse;
// none may reset or mutate the input's committed accounting.
func TestMuseRestoreNativeAccountingRefusals(t *testing.T) {
	prior := core.LegacyMuseAccountingSnapshot{SessionID: "native", GoalTransport: "native", LegacyMuseNativeAccounting: core.LegacyMuseNativeAccounting{TokenUsageTotal: 10, TokenUsageBaseline: 5}}
	for _, name := range []string{"valid", "foreign", "regression", "negative", "unknown-items"} {
		t.Run(name, func(t *testing.T) {
			f := core.MuseRestoreFold{SessionID: "native", Cursor: "cursor", Cumulative: core.MuseRestoreUsage{TotalTokens: 10}, Items: []json.RawMessage{}}
			switch name {
			case "foreign":
				f.SessionID = "other"
			case "regression":
				f.Cumulative.TotalTokens = 9
			case "negative":
				f.Cumulative.OutputTokens = -1
			case "unknown-items":
				f.Items = nil
			}
			_, e := restoreMuseAccounting(prior, &f)
			if (e == nil) != (name == "valid") {
				t.Fatalf("restore %s: %v", name, e)
			}
			if prior.TokenUsageTotal != 10 || prior.TokenUsageBaseline != 5 {
				t.Fatal("failed restore reset counters")
			}
		})
	}
}
