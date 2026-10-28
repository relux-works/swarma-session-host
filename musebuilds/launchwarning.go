package musebuilds

// WarningEvent returns the issued permit's warning provenance. It grants no
// authority and does not consume the permit. StartFallback still revalidates
// the receipt and returns the consumed sequence in its terminal event.
func (p *FallbackPermit) WarningEvent() RecoveryEvent {
	if !p.Issued() {
		return RecoveryEvent{}
	}
	return RecoveryEvent{
		Code: CodeFallbackWarning, State: "fallback_attempting",
		Newest:         FrozenReceipt{Build: p.newest.Build, SHA256: p.newest.SHA256, Path: p.newest.Path},
		FailureAttempt: p.attemptID, FailureStage: p.failureStage, FailureCode: p.failureCode,
		LastGood: p.lastGood, Mode: p.mode, Consent: p.consent,
	}
}

// WarningStart returns deferral provenance before the offer/start. Consent is
// deliberately false until the controller passes the accepted start to the
// reporter. This projection cannot authorize execution.
func (a *DeferralAuthorization) WarningStart() DeferralStart {
	if a == nil || !a.valid {
		return DeferralStart{}
	}
	return DeferralStart{Target: a.target, Newest: a.newest, Warning: a.warning, AttemptID: a.attemptID, Mode: a.mode}
}
