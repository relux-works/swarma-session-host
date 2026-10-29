package sessionmanager

func projectMuseSnapshot(src legacyMuseSnapshot) MuseRunnerSnapshot {
	out := MuseRunnerSnapshot{GoalTransport: src.GoalTransport, Provider: src.Provider, State: src.State, SessionID: src.SessionID, WorkspaceRoot: src.WorkspaceRoot, ModelID: src.ModelID, ApprovalMode: src.ApprovalMode, ServerVersion: src.ServerVersion, ServerVersionReported: src.ServerVersionReported, SchemaFingerprint: src.SchemaFingerprint, SchemaFingerprintReported: src.SchemaFingerprintReported, VersionUnqualified: src.VersionUnqualified, VersionFallback: src.VersionFallback, VersionFallbackFrom: src.VersionFallbackFrom, TokenUsageTotal: src.TokenUsageTotal, ExternalPause: src.ExternalPause, ActiveTurnID: src.ActiveTurnID, ObservedApprovalMode: src.ObservedApprovalMode, ObservedModelID: src.ObservedModelID, ModelRouteUnserved: src.ModelRouteUnserved}
	if v := src.VersionFallbackEvent; v != nil {
		out.VersionFallbackEvent = &MuseFallbackWarning{Code: v.Code, State: v.State, Newest: MuseFrozenTuple{Build: v.Newest.Build, SHA256: v.Newest.SHA256, Path: v.Newest.Path}, FailureAttempt: v.FailureAttempt, FailureStage: v.FailureStage, FailureCode: v.FailureCode, LastGood: MuseFrozenTuple{Build: v.LastGood.Build, SHA256: v.LastGood.SHA256, Path: v.LastGood.Path}, LastGoodSequence: v.LastGoodSequence, Mode: v.Mode, Consent: v.Consent}
	}
	if v := src.VersionDeferralEvent; v != nil {
		out.VersionDeferralEvent = &MuseDeferralWarning{Code: v.Code, State: v.State, Target: MuseFrozenTuple{Build: v.Target.Build, SHA256: v.Target.SHA256, Path: v.Target.Path}, Newest: MuseFrozenTuple{Build: v.Newest.Build, SHA256: v.Newest.SHA256, Path: v.Newest.Path}, Attempt: v.Attempt, Mode: v.Mode, Consent: v.Consent}
	}
	if src.GoalTransport == "native" && legacyMuseHasGoalState(src) {
		group := copyLegacyMuseNativeAccounting(src.LegacyMuseNativeAccounting)
		out.NativeAccounting = &group
	}
	return out
}
func mergeMuseSnapshot(base legacyMuseSnapshot, src MuseRunnerSnapshot) legacyMuseSnapshot {
	out := base
	out.GoalTransport = src.GoalTransport
	out.Provider = src.Provider
	out.State = src.State
	out.SessionID = src.SessionID
	out.WorkspaceRoot = src.WorkspaceRoot
	out.ModelID = src.ModelID
	out.ApprovalMode = src.ApprovalMode
	out.ServerVersion = src.ServerVersion
	out.ServerVersionReported = src.ServerVersionReported
	out.SchemaFingerprint = src.SchemaFingerprint
	out.SchemaFingerprintReported = src.SchemaFingerprintReported
	out.VersionUnqualified = src.VersionUnqualified
	out.VersionFallback = src.VersionFallback
	out.VersionFallbackFrom = src.VersionFallbackFrom
	out.TokenUsageTotal = src.TokenUsageTotal
	out.ExternalPause = src.ExternalPause
	out.ActiveTurnID = src.ActiveTurnID
	out.ObservedApprovalMode = src.ObservedApprovalMode
	out.ObservedModelID = src.ObservedModelID
	out.ModelRouteUnserved = src.ModelRouteUnserved
	out.VersionFallbackEvent = nil
	if v := src.VersionFallbackEvent; v != nil {
		out.VersionFallbackEvent = &legacyMuseFallbackWarning{Code: v.Code, State: v.State, Newest: legacyMuseFrozenTupleJSON{Build: v.Newest.Build, SHA256: v.Newest.SHA256, Path: v.Newest.Path}, FailureAttempt: v.FailureAttempt, FailureStage: v.FailureStage, FailureCode: v.FailureCode, LastGood: legacyMuseFrozenTupleJSON{Build: v.LastGood.Build, SHA256: v.LastGood.SHA256, Path: v.LastGood.Path}, LastGoodSequence: v.LastGoodSequence, Mode: v.Mode, Consent: v.Consent}
	}
	out.VersionDeferralEvent = nil
	if v := src.VersionDeferralEvent; v != nil {
		out.VersionDeferralEvent = &legacyMuseDeferralWarning{Code: v.Code, State: v.State, Target: legacyMuseFrozenTupleJSON{Build: v.Target.Build, SHA256: v.Target.SHA256, Path: v.Target.Path}, Newest: legacyMuseFrozenTupleJSON{Build: v.Newest.Build, SHA256: v.Newest.SHA256, Path: v.Newest.Path}, Attempt: v.Attempt, Mode: v.Mode, Consent: v.Consent}
	}
	if src.NativeAccounting != nil {
		out.LegacyMuseNativeAccounting = copyLegacyMuseNativeAccounting(*src.NativeAccounting)
	}
	return out
}
