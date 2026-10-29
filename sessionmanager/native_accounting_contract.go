package sessionmanager

import "context"

// MuseAccountingRestorer is the actual two-stage native accounting consumer.
// Validation runs before physical Restore; hydration uses the real current
// controller/native cut before goal reconciliation or attachment publication.
// A carrier or a nonnil supplier alone never licenses codec admission.
type MuseAccountingRestorer interface {
	ValidateMuseAccountingRestore(context.Context, RunnerRecord, LegacyRecordInput) error
	RestoreMuseAccounting(context.Context, CapturedOwnerRun) error
}

func copyLegacyMuseNativeAccounting(src LegacyMuseNativeAccounting) LegacyMuseNativeAccounting {
	out := src
	if src.GoalRef != nil {
		v := *src.GoalRef
		out.GoalRef = &v
	}
	if src.LastAnnouncedGoal != nil {
		v := *src.LastAnnouncedGoal
		out.LastAnnouncedGoal = &v
	}
	if src.LastGoalLifecycle != nil {
		v := *src.LastGoalLifecycle
		out.LastGoalLifecycle = &v
	}
	if src.BudgetPauseOutcome != nil {
		v := *src.BudgetPauseOutcome
		out.BudgetPauseOutcome = &v
	}
	if src.GoalLedger != nil {
		out.GoalLedger = make(map[string]LegacyMuseGoalRevisionFacts, len(src.GoalLedger))
		for k, v := range src.GoalLedger {
			if v.PauseOutcome != nil {
				p := *v.PauseOutcome
				v.PauseOutcome = &p
			}
			out.GoalLedger[k] = v
		}
	}
	return out
}
