package sessionhost

import "time"

const (
	managerTimerCapabilityMissing managerTimerCode = "timer_capability_missing"
	managerTimerRegistrationStale managerTimerCode = "timer_registration_stale"
	managerTimerRoleRefused       managerTimerCode = "timer_role_refused"
	managerTimerClockUnknown      managerTimerCode = "timer_clock_unknown"
	managerTimerExpired           managerTimerCode = "timer_expired"
	managerTimerSecurityRefused   managerTimerCode = "timer_security_refused"
	managerTimerBudgetRefused     managerTimerCode = "timer_budget_refused"
	managerTimerInputIneligible   managerTimerCode = "timer_input_ineligible"
)

// These are private inert admission projections, never wire fields or caller
// assertions of authority. Their owning source/namespace/currentness and the
// serialization of all events with actual native start must be independently
// qualified before any runtime adapter may use them. This file opens no board,
// goal file, session store, input endpoint or provider.
type managerTimerPrimaryObservation struct {
	authority, namespace, bindingDigest string
	revision                            int64
	state                               string // active, absent, cleared, expired; all other states are unknown.
	current                             bool
}

type managerTimerOwnerObservation struct {
	binding, clock                                        map[string]any
	primary                                               *managerTimerPrimaryObservation
	securityOpen, admissionOpen, budgetOpen, fenceOpen    bool
	input                                                 InputState
	hostTurn, nativeTurn, autonomousTurn, unknownProducer bool
	// The ordinary arbiter retains invalidation for this exact opportunity,
	// including clear/loss followed by restoration before the next observation.
	invalidatedAttempt string
}

type managerTimerGate struct {
	binding                            map[string]any
	primaryAuthority, primaryNamespace string
	// Supplied by the admitted source owner, including its persisted anti-replay
	// boundary after restart; a fresh local cache is not a revision authority.
	minimumPrimaryRevision int64
}

// E0 is claim-free. A due Poll still separately checks the active-attempt slot.
// A nil result describes only this inert projection, not a native capability or
// permission to bypass the future provider's own atomic conditional start.
func (g managerTimerGate) preClaim(ledger map[string]any, owner managerTimerOwnerObservation) error {
	if validateManagerTimerRecord("Ledger", ledger) != nil {
		return managerTimerStateInvalid
	}
	reg := timerObject(ledger, "registration")
	if reg == nil || !managerTimerMember(ledger["registration_state"], "active_unqualified|armed") {
		return managerTimerRegistrationStale
	}
	if g.binding == nil || g.primaryAuthority == "" || g.primaryNamespace == "" || g.minimumPrimaryRevision <= 0 || g.minimumPrimaryRevision > managerTimerMaxU || owner.binding == nil {
		return managerTimerCapabilityMissing
	}
	if owner.binding["role"] != "manager" {
		return managerTimerRoleRefused
	}
	if validateManagerTimerRecord("Binding", g.binding) != nil || validateManagerTimerRecord("Binding", owner.binding) != nil || !timerEqual(g.binding, owner.binding) || !timerEqual(g.binding, reg["binding"]) {
		return managerTimerBindingMismatch
	}
	if err := managerTimerCurrentLifetime(ledger, owner.clock); err != nil {
		return err
	}
	if !owner.securityOpen || !owner.admissionOpen || !owner.fenceOpen || owner.input == InputSecurityStop {
		return managerTimerSecurityRefused
	}
	if !owner.budgetOpen {
		return managerTimerBudgetRefused
	}
	p := owner.primary
	if p == nil || !p.current || p.authority != g.primaryAuthority || p.namespace != g.primaryNamespace || p.bindingDigest != timerDigest("Binding", g.binding) || p.revision < g.minimumPrimaryRevision || p.revision > managerTimerMaxU {
		return managerTimerCapabilityMissing
	}
	switch p.state {
	case "active":
	case "absent", "cleared", "expired":
		return managerTimerInputIneligible
	default:
		return managerTimerCapabilityMissing
	}
	if a := timerObject(ledger, "active"); a != nil && owner.invalidatedAttempt != "" && owner.invalidatedAttempt == a["attempt_id"] {
		return managerTimerInputIneligible
	}
	// Primary-goal activity and runtime activity are independent. Open opaque
	// Markdown entries do not appear here and cannot make an idle runtime busy.
	if owner.input != InputIdle || owner.hostTurn || owner.nativeTurn || owner.autonomousTurn || owner.unknownProducer {
		return managerTimerInputIneligible
	}
	return nil
}

func managerTimerCurrentLifetime(ledger, clock map[string]any) error {
	if clock == nil || validateManagerTimerRecord("Clock", clock) != nil || clock["trusted"] != true {
		return managerTimerClockUnknown
	}
	reg := timerObject(ledger, "registration")
	if reg == nil {
		return managerTimerRegistrationStale
	}
	created := timerObject(reg, "created_clock")
	if clock["boot_id"] != ledger["expires_boot_id"] || clock["boot_id"] != created["boot_id"] || ledger["expires_monotonic_ns"] == nil {
		return managerTimerClockUnknown
	}
	now, origin := timerTime(clock, "observed_at"), timerTime(reg, "created_at")
	mono, start := timerNumber(clock, "monotonic_ns"), timerNumber(created, "monotonic_ns")
	if now.Before(origin) || mono < start {
		return managerTimerClockUnknown
	}
	deadline := timerNumber(ledger, "expires_monotonic_ns")
	lifetime := timerTime(reg, "expires_at").Sub(origin)
	if deadline <= start || deadline-start > int64(lifetime) {
		return managerTimerClockUnknown
	}
	if !now.Before(timerTime(reg, "expires_at")) || mono >= deadline {
		return managerTimerExpired
	}
	return nil
}

type managerTimerClaimObservation struct {
	attemptID, bindingDigest, registrationID, requestDigest, submissionDigest string
	revision                                                                  int64
	claim                                                                     map[string]any
	permanentlyInvalidated                                                    bool
}

// E1 models the additional exact original claim. Its boolean result cannot
// establish that a separate native process will consume under the same order.
func (g managerTimerGate) postClaim(ledger map[string]any, owner managerTimerOwnerObservation, current *managerTimerClaimObservation) error {
	if err := g.preClaim(ledger, owner); err != nil {
		return err
	}
	a := timerObject(ledger, "active")
	reg := timerObject(ledger, "registration")
	if a == nil || current == nil || !managerTimerMember(a["phase"], "reserved|queued") || current.claim == nil {
		return managerTimerBindingMismatch
	}
	if current.permanentlyInvalidated {
		return managerTimerInputIneligible
	}
	if validateManagerTimerRecord("Claim", current.claim) != nil {
		return managerTimerBindingMismatch
	}
	if current.attemptID != a["attempt_id"] || current.bindingDigest != a["binding_digest"] || current.registrationID != reg["registration_id"] || current.revision != timerNumber(reg, "revision") || current.requestDigest != a["request_digest"] || current.submissionDigest != timerDigest("Submission", a["submission"]) || current.claim["claim_id"] != timerObject(a, "native")["claim_id"] {
		return managerTimerBindingMismatch
	}
	original := timerObject(timerObject(a, "provider_evidence"), "claim")
	if original == nil || !timerEqual(original, current.claim) {
		return managerTimerBindingMismatch
	}
	created := timerObject(current.claim, "created_clock")
	if created["boot_id"] != owner.clock["boot_id"] || timerTime(current.claim, "expires_at").After(timerTime(reg, "expires_at")) {
		return managerTimerBindingMismatch
	}
	if timerTime(owner.clock, "observed_at").Before(timerTime(created, "observed_at")) || timerNumber(owner.clock, "monotonic_ns") < timerNumber(created, "monotonic_ns") {
		return managerTimerClockUnknown
	}
	if !timerTime(owner.clock, "observed_at").Before(timerTime(current.claim, "expires_at")) || timerNumber(owner.clock, "monotonic_ns") >= timerNumber(current.claim, "expires_monotonic_ns") {
		return managerTimerExpired
	}
	return nil
}

func managerTimerDeadline(now int64, cadence int64) (int64, error) {
	if now < 0 || cadence < 900 || cadence > 1800 {
		return 0, managerTimerInvalid
	}
	delta := cadence * int64(time.Second)
	const maxN = int64(1<<63 - 1)
	if now > maxN-delta {
		return 0, managerTimerClockUnknown
	}
	return now + delta, nil
}
