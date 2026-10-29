package sessionmanager

import (
	"fmt"
	"math"
	"math/bits"
	"strings"
)

type legacyMuseSnapshot struct {
	Text                      legacyMuseTextState `json:"text"`
	GoalTransport             string              `json:"goal_transport"`
	Provider                  string              `json:"provider"`
	State                     ProviderHostState   `json:"state"`
	SessionID                 string              `json:"sessionId"`
	WorkspaceRoot             string              `json:"workspaceRoot,omitempty"`
	ModelID                   string              `json:"modelId,omitempty"`
	ApprovalMode              string              `json:"approvalMode"`
	ServerVersion             string              `json:"serverVersion,omitempty"`
	ServerVersionReported     bool                `json:"serverVersionReported"`
	SchemaFingerprint         string              `json:"schemaFingerprint,omitempty"`
	SchemaFingerprintReported bool                `json:"schemaFingerprintReported"`

	VersionUnqualified bool `json:"versionUnqualified,omitempty"`

	VersionFallback     bool   `json:"versionFallback,omitempty"`
	VersionFallbackFrom string `json:"versionFallbackFrom,omitempty"`

	VersionFallbackEvent *legacyMuseFallbackWarning `json:"versionFallbackEvent,omitempty"`

	VersionDeferralEvent *legacyMuseDeferralWarning `json:"versionDeferralEvent,omitempty"`
	LegacyMuseNativeAccounting

	ObservedApprovalMode string `json:"observedApprovalMode,omitempty"`
	ObservedModelID      string `json:"observedModelId,omitempty"`
	ModelRouteUnserved   bool   `json:"modelRouteUnserved,omitempty"`
}

// LegacyMuseNativeAccounting is the original native goal/accounting wire field
// group. It excludes text-child accounting and grants no runtime authority.
type LegacyMuseNativeAccounting struct {
	GoalObserved       bool                          `json:"goalObserved"`
	GoalRef            *LegacyGoalRef                `json:"goalRef,omitempty"`
	GoalObjective      string                        `json:"goalObjective,omitempty"`
	GoalStatus         string                        `json:"goalStatus,omitempty"`
	LastAnnouncedGoal  *LegacyMuseGoalAnnouncement   `json:"lastAnnouncedGoal,omitempty"`
	LastGoalLifecycle  *LegacyMuseGoalAnnouncement   `json:"lastGoalLifecycle,omitempty"`
	TokenBudget        int64                         `json:"tokenBudget,omitempty"`
	TokenUsageTotal    int64                         `json:"tokenUsageTotal,omitempty"`
	TokenUsageBaseline int64                         `json:"tokenUsageBaseline"`
	BudgetPaused       bool                          `json:"budgetPaused,omitempty"`
	BudgetPauseOutcome *LegacyMuseBudgetPauseOutcome `json:"budgetPauseOutcome,omitempty"`

	ExternalPause bool   `json:"externalPause,omitempty"`
	ActiveTurnID  string `json:"activeTurnId,omitempty"`

	GoalLedger map[string]LegacyMuseGoalRevisionFacts `json:"goalLedger,omitempty"`
}

type legacyMuseTextState struct {
	ChildByGoal           map[string]int64 `json:"childByGoal,omitempty"`
	ChildAttributed       bool             `json:"childAttributed,omitempty"`
	ChildObserved         int64            `json:"childObserved,omitempty"`
	ChildUnboundBaseline  int64            `json:"childUnboundBaseline,omitempty"`
	ParentUnboundExcluded int64            `json:"parentUnboundExcluded,omitempty"`

	TurnGoals             map[string]string                   `json:"turnGoals,omitempty"`
	ParentByGoal          map[string]int64                    `json:"parentByGoal,omitempty"`
	AccountingInterrupted bool                                `json:"accountingInterrupted,omitempty"`
	Delivery              string                              `json:"delivery,omitempty"`
	Turns                 map[string]int                      `json:"turns,omitempty"`
	Items                 map[string]legacyMuseUsageItem      `json:"items,omitempty"`
	ChildBaseline         int64                               `json:"childBaseline"`
	CacheConvention       string                              `json:"cacheConvention,omitempty"`
	AccountingIncomplete  bool                                `json:"accountingIncomplete,omitempty"`
	TurnCapLimited        map[string]bool                     `json:"turnCapLimited,omitempty"`
	BudgetLimited         bool                                `json:"budgetLimited,omitempty"`
	Incidents             []string                            `json:"incidents,omitempty"`
	LastResult            *ProviderRunResult                  `json:"lastResult,omitempty"`
	RecoveryAttempts      map[string]int                      `json:"recoveryAttempts,omitempty"`
	FinalItems            map[string]*legacyMuseFinalItemFold `json:"finalItems,omitempty"`
}
type legacyMuseUsageItem struct {
	TurnID        string                 `json:"turnId"`
	ItemID        string                 `json:"itemId"`
	Revision      int64                  `json:"revision"`
	Kind          string                 `json:"kind"`
	Status        string                 `json:"status"`
	WorkflowRunID string                 `json:"workflowRunId"`
	Usage         *legacyMuseTokenUsage  `json:"usage"`
	Children      []legacyMuseUsageChild `json:"children"`
}
type legacyMuseTokenUsage struct {
	CacheReadTokens  *int64 `json:"cacheReadTokens,omitempty"`
	CacheWriteTokens *int64 `json:"cacheWriteTokens,omitempty"`
	CachedTokens     int64  `json:"cachedTokens"`
	InputTokens      int64  `json:"inputTokens"`
	OutputTokens     int64  `json:"outputTokens"`
	ReasoningTokens  int64  `json:"reasoningTokens"`
}
type legacyMuseUsageChild struct {
	ChildID string                `json:"childId"`
	Attempt int                   `json:"attempt"`
	Status  string                `json:"status"`
	Usage   *legacyMuseTokenUsage `json:"usage"`
}
type legacyMuseFinalItemFold struct {
	Order []string                          `json:"order"`
	Items map[string]legacyMuseAgentMessage `json:"items"`
}
type legacyMuseAgentMessage struct {
	ItemID   string `json:"itemId"`
	Revision int64  `json:"revision"`
	Kind     string `json:"kind"`
	TurnID   string `json:"turnId"`
	Text     string `json:"text"`
}
type legacyMuseFallbackWarning struct {
	Code             string                    `json:"code"`
	State            string                    `json:"state"`
	Newest           legacyMuseFrozenTupleJSON `json:"newest"`
	FailureAttempt   string                    `json:"failureAttempt"`
	FailureStage     string                    `json:"failureStage"`
	FailureCode      string                    `json:"failureCode"`
	LastGood         legacyMuseFrozenTupleJSON `json:"lastGood"`
	LastGoodSequence uint64                    `json:"lastGoodSequence"`
	Mode             string                    `json:"mode"`
	Consent          bool                      `json:"consent"`
}
type legacyMuseFrozenTupleJSON struct {
	Build  string `json:"build"`
	SHA256 string `json:"sha256"`
	Path   string `json:"path"`
}
type legacyMuseDeferralWarning struct {
	Code    string                    `json:"code"`
	State   string                    `json:"state"`
	Target  legacyMuseFrozenTupleJSON `json:"target"`
	Newest  legacyMuseFrozenTupleJSON `json:"newest"`
	Attempt string                    `json:"attempt"`
	Mode    string                    `json:"mode"`
	Consent bool                      `json:"consent"`
}
type LegacyMuseGoalAnnouncement struct {
	Objective string `json:"objective,omitempty"`
	Status    string `json:"status,omitempty"`
	Cleared   bool   `json:"cleared,omitempty"`
}
type LegacyMuseBudgetPauseOutcome struct {
	Code         string `json:"code"`
	NativeStatus string `json:"nativeStatus"`
}
type LegacyMuseGoalRevisionFacts struct {
	Baseline int64 `json:"baseline"`

	Spend int64 `json:"spend"`

	PauseOutcome *LegacyMuseBudgetPauseOutcome `json:"pauseOutcome,omitempty"`

	BudgetPaused  bool `json:"budgetPaused,omitempty"`
	ExternalPause bool `json:"externalPause,omitempty"`
}

func validateLegacyMuseSnapshot(prior *legacyMuseSnapshot) error {
	invalid := func(format string, args ...any) error {
		return &MuseHostError{Code: "invalid_config", Cause: fmt.Errorf("invalid Muse restore record: "+format, args...)}
	}

	if prior.GoalTransport != "text" && prior.GoalTransport != "native" {
		return invalid("goal_transport %q is not text or native", prior.GoalTransport)
	}
	for key, turns := range prior.Text.Turns {
		if strings.TrimSpace(key) == "" {
			return invalid("text turn-cap key is empty")
		}
		if turns < 0 {
			return invalid("text turn-cap counter is negative")
		}
	}
	for _, spend := range prior.Text.ParentByGoal {
		if spend < 0 {
			return invalid("text parent spend is negative")
		}
	}

	if prior.Text.ChildUnboundBaseline < 0 || prior.Text.ChildObserved < 0 || prior.Text.ParentUnboundExcluded < 0 {
		return invalid("negative observation-ledger counter")
	}
	var attributed int64
	for _, spend := range prior.Text.ChildByGoal {
		if spend < 0 {
			return invalid("negative child credit")
		}
		var overflow bool
		attributed, overflow = legacyMuseCheckedAdd(attributed, spend)
		if overflow {
			return invalid("child attribution overflow")
		}
	}
	if prior.Text.ChildAttributed && (attributed != prior.Text.ChildObserved || prior.Text.ChildUnboundBaseline > prior.Text.ChildByGoal[""]) {
		return invalid("inconsistent child observation ledger")
	}
	if prior.Text.ChildBaseline < 0 {
		return invalid("text child baseline is negative")
	}
	for id, item := range prior.Text.Items {
		if item.ItemID == "" || item.ItemID != id {
			return invalid("text ledger item %q has no stable identity", id)
		}
		if item.Kind != "subagent" && item.Kind != "workflow" {
			return invalid("text ledger item %q has kind %q", id, item.Kind)
		}
		if item.Revision < 1 {
			return invalid("text ledger item %q is below the pinned revision floor", id)
		}
		if legacyMuseUsageNegative(item.Usage) {
			return invalid("text ledger item %q carries negative usage", id)
		}
		for _, child := range item.Children {
			if legacyMuseUsageNegative(child.Usage) {
				return invalid("text ledger item %q carries a negative child usage", id)
			}
		}
	}
	for _, attempts := range prior.Text.RecoveryAttempts {
		if attempts < 0 {
			return invalid("text recovery counter is negative")
		}
	}
	for key := range prior.Text.TurnCapLimited {
		if strings.TrimSpace(key) == "" {
			return invalid("text turn-cap exhaustion key is empty")
		}
	}
	for turnID, fold := range prior.Text.FinalItems {
		if strings.TrimSpace(turnID) == "" {
			return invalid("final-item fold names no turn")
		}
		if fold == nil {
			continue
		}
		if len(fold.Order) != len(fold.Items) {
			return invalid("final-item fold for turn %q is inconsistent", turnID)
		}
		seen := make(map[string]bool, len(fold.Order))
		for _, id := range fold.Order {
			message, ok := fold.Items[id]
			if !ok || seen[id] {
				return invalid("final-item fold for turn %q is inconsistent", turnID)
			}
			seen[id] = true
			if message.ItemID != id || message.Kind != "agentMessage" || message.TurnID != turnID || message.Revision < 1 {
				return invalid("final-item fold for turn %q carries an unattributable message", turnID)
			}
		}
	}
	if prior.GoalRef != nil {
		if err := validateLegacyGoalRef(*prior.GoalRef); err != nil {
			return &MuseHostError{Code: "invalid_config", Cause: fmt.Errorf("invalid Muse restore record goal ref: %w", err)}
		}
	}
	for objective, facts := range prior.GoalLedger {
		if strings.TrimSpace(objective) == "" {
			return invalid("goal revision ledger names no objective")
		}
		if facts.Baseline < 0 {
			return invalid("goal revision ledger for %q carries a negative baseline", objective)
		}
		if facts.PauseOutcome != nil {
			switch facts.PauseOutcome.Code {
			case "budget_pause_not_applicable", "budget_pause_failed":
			default:
				return invalid("goal revision ledger for %q carries outcome %q", objective, facts.PauseOutcome.Code)
			}
		}
	}
	return nil
}
func legacyMuseCheckedAdd(a, b int64) (int64, bool) {
	sum, carry := bits.Add64(uint64(a), uint64(b), 0)
	if carry != 0 || sum > uint64(math.MaxInt64) {
		return math.MaxInt64, true
	}
	return int64(sum), false
}
func legacyMuseUsageNegative(usage *legacyMuseTokenUsage) bool {
	if usage == nil {
		return false
	}
	if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.CachedTokens < 0 || usage.ReasoningTokens < 0 {
		return true
	}
	if usage.CacheReadTokens != nil && *usage.CacheReadTokens < 0 {
		return true
	}
	if usage.CacheWriteTokens != nil && *usage.CacheWriteTokens < 0 {
		return true
	}
	return false
}
