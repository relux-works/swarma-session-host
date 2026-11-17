package sessionhost

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func managerTimerFixtureRecord(t *testing.T, id string) map[string]any {
	t.Helper()
	data, err := os.ReadFile("testdata/manager_timer_records.json")
	if err != nil {
		t.Fatal(err)
	}
	var x struct {
		Records []struct {
			ID    string `json:"id"`
			Input string `json:"input_json"`
		} `json:"record_vectors"`
	}
	if err := json.Unmarshal(data, &x); err != nil {
		t.Fatal(err)
	}
	for _, v := range x.Records {
		if v.ID == id {
			o, err := decodeManagerTimerObject([]byte(v.Input + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			return o
		}
	}
	t.Fatalf("missing independent fixture %s", id)
	return nil
}

func managerTimerGateFixture(t *testing.T) (managerTimerGate, map[string]any, managerTimerOwnerObservation) {
	t.Helper()
	g := managerTimerGate{binding: managerTimerFixtureRecord(t, "Binding-valid"), primaryAuthority: "fixture-goal-authority", primaryNamespace: "fixture-primary", minimumPrimaryRevision: 1}
	ledger := managerTimerFixtureRecord(t, "Ledger-valid")
	ledger["active"] = nil
	clock := timerObject(managerTimerFixtureRecord(t, "Claim-valid"), "created_clock")
	owner := managerTimerOwnerObservation{binding: managerTimerFixtureRecord(t, "Binding-valid"), clock: clock, primary: &managerTimerPrimaryObservation{authority: g.primaryAuthority, namespace: g.primaryNamespace, bindingDigest: "7c723d49981dbecb22bd9656b449453d97ba7bed445b861682ce049d520abee2", revision: 1, state: "active", current: true}, securityOpen: true, admissionOpen: true, budgetOpen: true, fenceOpen: true, input: InputIdle}
	return g, ledger, owner
}

// With explicitly supplied fictional source authority, active primary + idle
// is eligible without a claim. Absence/clear/unknown and busy are independent
// negatives. This pure gate cannot qualify a real board adapter or native start.
func TestManagerTimerPreClaimGoalAndBusy(t *testing.T) {
	g, ledger, owner := managerTimerGateFixture(t)
	if err := g.preClaim(ledger, owner); err != nil {
		t.Fatalf("zero-claim positive: %v", err)
	}
	for _, state := range []string{"absent", "cleared", "expired"} {
		t.Run(state, func(t *testing.T) {
			g, l, o := managerTimerGateFixture(t)
			o.primary.state = state
			if err := g.preClaim(l, o); err != managerTimerInputIneligible {
				t.Fatalf("missing goal=%v", err)
			}
		})
	}
	for _, which := range []string{"missing", "uncurrent", "authority", "namespace", "binding", "revision", "unknown"} {
		t.Run("source-"+which, func(t *testing.T) {
			g, l, o := managerTimerGateFixture(t)
			switch which {
			case "missing":
				o.primary = nil
			case "uncurrent":
				o.primary.current = false
			case "authority":
				o.primary.authority = "foreign-authority"
			case "namespace":
				o.primary.namespace = "foreign-primary"
			case "binding":
				o.primary.bindingDigest = strings.Repeat("a", 64)
			case "revision":
				o.primary.revision = 0
			case "unknown":
				o.primary.state = "unknown"
			}
			if err := g.preClaim(l, o); err != managerTimerCapabilityMissing {
				t.Fatalf("unavailable authority=%v", err)
			}
		})
	}
	for _, which := range []string{"host", "native", "autonomous", "human", "draft", "queued_human", "approval", "unknown_input", "unknown_producer"} {
		t.Run("busy-"+which, func(t *testing.T) {
			g, l, o := managerTimerGateFixture(t)
			switch which {
			case "host":
				o.hostTurn = true
			case "native":
				o.nativeTurn = true
			case "autonomous":
				o.autonomousTurn = true
			case "human", "queued_human":
				o.input = InputBusy
			case "draft":
				o.input = InputDraft
			case "approval":
				o.input = InputApproval
			case "unknown_input":
				o.input = InputUnknown
			case "unknown_producer":
				o.unknownProducer = true
			}
			if err := g.preClaim(l, o); err != managerTimerInputIneligible {
				t.Fatalf("busy=%v", err)
			}
		})
	}
}

// Binding/time/security/budget are required even with a valid active primary;
// an authoritative source cannot renew the original seven-day/monotonic bound.
func TestManagerTimerPreClaimOtherGates(t *testing.T) {
	for _, c := range []struct {
		name string
		want error
	}{
		{"role", managerTimerRoleRefused}, {"thread", managerTimerBindingMismatch}, {"home", managerTimerBindingMismatch},
		{"clock_unknown", managerTimerClockUnknown}, {"reboot", managerTimerClockUnknown}, {"rollback", managerTimerClockUnknown},
		{"utc_expiry", managerTimerExpired}, {"mono_expiry", managerTimerExpired}, {"security", managerTimerSecurityRefused},
		{"admission", managerTimerSecurityRefused}, {"fence", managerTimerSecurityRefused}, {"budget", managerTimerBudgetRefused},
		{"cancelled", managerTimerRegistrationStale},
	} {
		t.Run(c.name, func(t *testing.T) {
			g, l, o := managerTimerGateFixture(t)
			switch c.name {
			case "role":
				o.binding["role"] = "reviewer"
			case "thread":
				o.binding["thread_id"] = "00000000-0000-4000-8000-000000000099"
			case "home":
				o.binding["codex_home"] = "/fixture/foreign-home"
			case "clock_unknown":
				o.clock["trusted"] = false
			case "reboot":
				o.clock["boot_id"] = "new-boot"
			case "rollback":
				o.clock["monotonic_ns"] = json.Number("1")
			case "utc_expiry":
				o.clock["observed_at"] = timerObject(l, "registration")["expires_at"]
			case "mono_expiry":
				o.clock["monotonic_ns"] = l["expires_monotonic_ns"]
			case "security":
				o.securityOpen = false
			case "admission":
				o.admissionOpen = false
			case "fence":
				o.fenceOpen = false
			case "budget":
				o.budgetOpen = false
			case "cancelled":
				l["registration_state"] = "cancelled"
			}
			if err := g.preClaim(l, o); err != c.want {
				t.Fatalf("gate=%v want %v", err, c.want)
			}
		})
	}
}

// E1 adds the original exact live claim and permanent conflict state; E0 alone
// is insufficient. These fixtures model start eligibility without native I/O.
func TestManagerTimerClaimBoundEligibility(t *testing.T) {
	for _, which := range []string{"valid", "missing", "expired", "before_creation", "wrong_attempt", "wrong_request", "wrong_submission", "changed_deadline", "invalidated"} {
		t.Run(which, func(t *testing.T) {
			g, l, o := managerTimerGateFixture(t)
			a := managerTimerFixtureRecord(t, "Attempt-valid")
			l["active"] = a
			claim := managerTimerFixtureRecord(t, "Claim-valid")
			view := &managerTimerClaimObservation{attemptID: timerString(a, "attempt_id"), bindingDigest: timerString(a, "binding_digest"), registrationID: timerString(timerObject(l, "registration"), "registration_id"), revision: 1, requestDigest: timerString(a, "request_digest"), submissionDigest: timerDigest("Submission", a["submission"]), claim: claim}
			var want error
			switch which {
			case "missing":
				view = nil
				want = managerTimerBindingMismatch
			case "expired":
				o.clock["observed_at"] = claim["expires_at"]
				o.clock["monotonic_ns"] = claim["expires_monotonic_ns"]
				want = managerTimerExpired
			case "before_creation":
				o.clock["observed_at"] = "2026-11-17T17:14:59Z"
				o.clock["monotonic_ns"] = managerTimerNumber(timerNumber(timerObject(claim, "created_clock"), "monotonic_ns") - 1)
				want = managerTimerClockUnknown
			case "wrong_attempt":
				view.attemptID = strings.Repeat("7", 64)
				want = managerTimerBindingMismatch
			case "wrong_request":
				view.requestDigest = strings.Repeat("7", 64)
				want = managerTimerBindingMismatch
			case "wrong_submission":
				view.submissionDigest = strings.Repeat("7", 64)
				want = managerTimerBindingMismatch
			case "changed_deadline":
				claim["expires_at"] = "2026-11-17T17:15:29Z"
				want = managerTimerBindingMismatch
			case "invalidated":
				view.permanentlyInvalidated = true
				want = managerTimerInputIneligible
			}
			if err := g.postClaim(l, o, view); err != want {
				t.Fatalf("claim=%v want %v", err, want)
			}
		})
	}
}

// The monotonic deadline is exact int64 arithmetic, with valid boundary control.
func TestManagerTimerDeadlineOverflow(t *testing.T) {
	const max = int64(1<<63 - 1)
	const delta = int64(900000000000)
	if got, err := managerTimerDeadline(max-delta, 900); err != nil || got != max {
		t.Fatalf("boundary=%d %v", got, err)
	}
	if _, err := managerTimerDeadline(max-delta+1, 900); err != managerTimerClockUnknown {
		t.Fatalf("overflow=%v", err)
	}
}
