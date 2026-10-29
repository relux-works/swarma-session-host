package sessionmanager

type museDispatchDisposition string
type museDispatchSection string

const (
	museDispatchFold          museDispatchDisposition = "FOLD"
	museDispatchTaint         museDispatchDisposition = "TAINT"
	museDispatchEndWait       museDispatchDisposition = "END-WAIT"
	museDispatchIgnore        museDispatchDisposition = "IGNORE"
	museDispatchNotification  museDispatchSection     = "notification"
	museDispatchServerRequest museDispatchSection     = "server-request"
	museDispatchRPCResult     museDispatchSection     = "rpc-result"
)

type museProtocolEntry struct {
	Method      string
	Section     museDispatchSection
	Disposition museDispatchDisposition
	Consumer    string
	Test        string
}

var museDispatchTable = []museProtocolEntry{
	{Method: "approval/requested", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "resolution is driven by the listPending re-read on attention/request/resume triggers; the broadcast duplicates the approval/request twin which already triggers the pass",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "approval/resolved", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "terminal broadcast; the pass re-lists authoritative pending state on every trigger, superseding lifecycle broadcasts",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "approval/updated", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "lifecycle broadcast, not a pass trigger; a multi-stage decide is followed by a fresh approval/request server request which triggers the authoritative re-list",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "item/completed", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleTextItem (text usage ledger) + handleRawNotification (final fold, both transports)",
		Test:     "TestMuseTextFinalItemRevisionOrdering"},
	{Method: "item/delta", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "deltas carry partial updates superseded by the full item in started/updated/completed; the host folds full items only",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "item/started", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleTextItem (text usage ledger); native ignores without validation, parent spend rides tokenUsage cumulative (bound N11)",
		Test:     "TestMuseTextPinnedChildOriginClass"},
	{Method: "item/updated", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleTextItem (text usage ledger); native ignores without validation, parent spend rides tokenUsage cumulative (bound N11)",
		Test:     "TestMuseTextPinnedChildOriginClass"},
	{Method: "session/approvalModeChanged", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleApprovalModeChanged records ObservedApprovalMode; every owner result FAILS with the taint status while it diverges from launch (r24 S11)",
		Test:     "TestMuseR22ReconfigurationTaintsAndRecords"},
	{Method: "session/branchChanged", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "branch metadata; session identity and workspace root are launch/resume state, and no goal, budget, or terminal state rides it",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "session/closed", Section: museDispatchNotification, Disposition: museDispatchEndWait,
		Consumer: "session_closed (recoverable: resume reloads)",
		Test:     "TestMuseR21OwnerWaitEndsOnUnrunTurn"},
	{Method: "session/contextUsage", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "advisory context pressure; spend enforcement rides session/tokenUsage cumulative and child usage, which the pinned server emits on every turn",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "session/deleteCompleted", Section: museDispatchNotification, Disposition: museDispatchEndWait,
		Consumer: "session_deleted (not recoverable: a deleted session cannot run turns); only outcome=completed aborts — failed leaves the session loaded and unknown leaves the command pending",
		Test:     "TestMuseR22DeleteCompletedEndsWait"},
	{Method: "session/goalChanged", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleGoalChanged queues the authoritative resume re-read (native) or foreign-goal containment (text)",
		Test:     "TestMuseGoalChangedAfterResumeReadTriggersAuthoritativeReread"},
	{Method: "session/listChanged", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "other-session lifecycle metadata; carries no own-session goal, budget, or terminal state",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "session/modelChanged", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleModelChanged records ObservedModelID; every owner result FAILS with the taint status while a promised model diverges (r24 S11)",
		Test:     "TestMuseR22ReconfigurationTaintsAndRecords"},
	{Method: "session/modelRouteUnserved", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleModelRouteUnserved latches ModelRouteUnserved; every owner result FAILS with the taint status while latched: the session cannot run as configured (r24 S11)",
		Test:     "TestMuseR22ReconfigurationTaintsAndRecords"},
	{Method: "session/nameChanged", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "display name; session identity is start/resume result state",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "session/reasoningEffortChanged", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "quality signal; the host sets no reasoning effort and no spend, identity, or terminal state rides it",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "session/started", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "session identity comes from the session/start and session/resume RESULTS, never the broadcast",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "session/statusChanged", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleSessionStatusChanged: attention drives the approval pass; notLoaded END-WAITs session_not_loaded; liveness never erases the session-scoped abort cache",
		Test:     "TestMuseR21OwnerWaitEndsOnUnrunTurn"},
	{Method: "session/todoListChanged", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "model progress display; not goal, budget, or terminal state",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "session/tokenUsage", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleTokenUsage (native cumulative + budget queue) / handleTextTokenUsage (text parent ledger)",
		Test:     "TestMuseBudgetPauseUsesTokenUsageAtCeiling"},
	{Method: "session/viewHealthChanged", Section: museDispatchNotification, Disposition: museDispatchEndWait,
		Consumer: "view_unavailable: the item/usage evidence stream is dead, so no terminal can be attested (r22; malformed still taints; other health values ignored per pinned schema)",
		Test:     "TestMuseR21ViewHealthUnavailableNeverSuccess"},
	{Method: "skill/changed", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "skill inventory; the host consumes no skills",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "turn/completed", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleTurnCompleted: single-terminal ledger plus the owner result",
		Test:     "TestMuseR21SecondDifferingTerminalFailsTurn"},
	{Method: "turn/retracted", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "untyped: the host issues no retracting interrupt and binds no ledger to retractions, so the raw frame drops without validation and the turn's terminal still arrives",
		Test:     "TestMuseR20WellFormedLifecycleEventsAreIgnored"},
	{Method: "turn/retryScheduled", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "untyped: a scheduled retry promises a LATER terminal for the same turn, so the raw frame drops without validation and the wait correctly continues",
		Test:     "TestMuseR20WellFormedLifecycleEventsAreIgnored"},
	{Method: "turn/started", Section: museDispatchNotification, Disposition: museDispatchFold,
		Consumer: "handleTurnStarted binds ActiveTurnID plus text turn goals; liveness never erases the session-scoped abort cache",
		Test:     "TestMuseR22DispatchEveryRoutedEntryLoadBearing"},
	{Method: "turn/unqueued", Section: museDispatchNotification, Disposition: museDispatchEndWait,
		Consumer: "turn_unqueued (recoverable: retry runs a fresh turn)",
		Test:     "TestMuseR21OwnerWaitEndsOnUnrunTurn"},
	{Method: "usage/changed", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "account-level subscription usage (no sessionId); the host enforces the goal token budget from session/tokenUsage, not subscription windows",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "userInput/requested", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "duplicates the userInput/request twin which already triggers the pass; resolution is driven by the listPending re-read",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "userInput/settled", Section: museDispatchNotification, Disposition: museDispatchIgnore,
		Consumer: "terminal broadcast; superseded by the listPending re-read",
		Test:     "TestMuseR22DispatchIgnoreJustifications"},
	{Method: "view/gap", Section: museDispatchNotification, Disposition: museDispatchEndWait,
		Consumer: "view_gap (recoverable: the stream is alive and the server turn continues; the taint still voids the holed evidence; malformed gaps only taint)",
		Test:     "TestMuseR23ViewGapEndsOwnerWait"},
	{Method: "approval/request", Section: museDispatchServerRequest, Disposition: museDispatchFold,
		Consumer: "strict client receipts after pinned validation; handleServerRequest triggers the approval pass",
		Test:     "TestMuseR20MalformedServerRequestFailsTransport"},
	{Method: "userInput/request", Section: museDispatchServerRequest, Disposition: museDispatchFold,
		Consumer: "strict client receipts after pinned validation; handleServerRequest triggers the approval pass",
		Test:     "TestMuseR20MalformedServerRequestFailsTransport"},
	{Method: "initialize", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "newMuseProviderHostWithPrior: durability gate, schema-fingerprint and server-version identity gates, ServerVersion/provenance record",
		Test:     "TestMuseR20InitializeRefusesForeignIdentity"},
	{Method: "session/start", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "newMuseProviderHostWithPrior: requested session identity refused on mismatch, effective model/approval echoes refused when foreign, plus ActiveTurnID",
		Test:     "TestMuseR24SessionStartIdentityMismatchRefused"},
	{Method: "session/resume", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "readResumeGoal: resume identity check, snapshot goal read, pending-request trigger; restoreTextHistory folds served items",
		Test:     "TestMuseRestoreUsesSessionResumeAndKeepsNativeIdentity"},
	{Method: "goal/set", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "applyGoalWriteAndRead: command receipt plus exact-revision snapshot acknowledgement",
		Test:     "TestMuseApplyGoalSetAndEditWaitForExactRevision"},
	{Method: "goal/edit", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "applyGoalWriteAndRead: command receipt plus exact-revision snapshot acknowledgement",
		Test:     "TestMuseApplyGoalSetAndEditWaitForExactRevision"},
	{Method: "goal/pause", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "pauseGoal: paused-status acknowledgement; rejection records budget_pause_failed / budget_pause_not_applicable",
		Test:     "TestMuseBudgetPauseUsesTokenUsageAtCeiling"},
	{Method: "goal/clear", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "clearGoal: cleared-snapshot acknowledgement; text containment clears foreign goals",
		Test:     "TestMuseRollbackClearsGoalWhenThereWasNoPreviousRevision"},
	{Method: "turn/start", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "sendTurn: turn identity plus the closed pinned-disposition check; steered applies rebind the live turn (deliverText)",
		Test:     "TestMuseTurnStartDispositionClosedOnBothTransports"},
	{Method: "turn/steer", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "DeliverSteeredSessionNotice: active-turn receipt",
		Test:     "TestMuseNoticeQueuesAndSteerUsesActiveTurn"},
	{Method: "turn/interrupt", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "interruptText: interrupt acknowledgement with the stale-ack moot rule",
		Test:     "TestMuseStaleInterruptAckAfterCompletionIsMoot"},
	{Method: "approval/listPending", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "resolvePendingRequests: pending approval and user-input inventory (fail-collect pass)",
		Test:     "TestMuseR21ApprovalPassContinuesPastUnresolvable"},
	{Method: "approval/decide", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "resolvePendingRequests: decision receipt for the once-scoped plain deny",
		Test:     "TestMuseR21ApprovalPrefersOnceScopedDeny"},
	{Method: "userInput/cancel", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "resolvePendingRequests: cancel receipt; rejection ends the owner wait typed",
		Test:     "TestMuseR21ApprovalPassContinuesPastUnresolvable"},
	{Method: "view/page", Section: museDispatchRPCResult, Disposition: museDispatchFold,
		Consumer: "readGoalFromPages: backward scan for the latest goalChanged with the compaction-boundary anchor rule",
		Test:     "TestMuseMalformedViewPageCannotSucceed"},
}

var museDispatchTableByMethod = museBuildDispatchMap()

func museNotificationRequiresParamsObject(method string) bool {
	switch method {
	case "session/goalChanged", "session/statusChanged", "session/tokenUsage",
		"turn/started", "turn/completed", "turn/unqueued":
		return true
	}
	entry, ok := museDispatchTableByMethod[method]
	return ok && entry.Section == museDispatchNotification && entry.Disposition != museDispatchIgnore
}

func museBuildDispatchMap() map[string]museProtocolEntry {
	table := make(map[string]museProtocolEntry, len(museDispatchTable))
	for _, entry := range museDispatchTable {
		if entry.Section == museDispatchRPCResult {
			continue
		}
		table[entry.Method] = entry
	}
	return table
}
