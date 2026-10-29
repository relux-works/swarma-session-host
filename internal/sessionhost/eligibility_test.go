package sessionhost_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

type observedFixture struct {
	fixtureadapters.JSON
	input atomic.Value
}

func (a *observedFixture) InputState() sessionhost.InputState {
	return a.input.Load().(sessionhost.InputState)
}

// Busy, draft, approval, unknown, absent and unrecognized fixture evidence cause
// zero writes across a dispatch opportunity and restart. Explicit idle evidence
// then applies the same queued intent once, without changing the manual fence.
// This is synthetic eligibility, not a real harness readiness parser.
func TestInputEligibilityRequiresIdleEvidence(t *testing.T) {
	for _, state := range []sessionhost.InputState{sessionhost.InputBusy, sessionhost.InputDraft, sessionhost.InputApproval, sessionhost.InputUnknown, "", "unrecognized"} {
		t.Run(string(state), func(t *testing.T) {
			a := &observedFixture{}
			a.input.Store(state)
			cfg, _ := setup(t, a, false)
			host := openHost(t, cfg)
			notice := sessionhost.Notice{ID: "waiting", Reference: "fixture/waiting"}
			if _, err := host.Enqueue(notice); err != nil {
				t.Fatal(err)
			}
			if err := host.ObserveInput(); err != nil {
				t.Fatal(err)
			}
			if err := host.Fence(true); err == nil || err.Error() != "input_ineligible" {
				t.Fatalf("ineligible release: %v", err)
			}
			// Observe a bounded live dispatch window, then drain before counting.
			time.Sleep(200 * time.Millisecond)
			host.Close()
			if got := inspectRunner(t, cfg); got.Writes != 0 {
				t.Fatalf("ineligible write: %+v", got)
			}
			replacement := openHost(t, cfg)
			if got := inspectRunner(t, cfg); got.Writes != 0 {
				t.Fatalf("restart wrote: %+v", got)
			}
			if records := replacement.Snapshot().Records; len(records) != 1 || records[0].State != "queued" {
				t.Fatalf("intent lost: %+v", records)
			}
			a.input.Store(sessionhost.InputIdle)
			if err := replacement.ObserveInput(); err != nil {
				t.Fatal(err)
			}
			await(t, replacement, notice.ID, "applied")
			if got := inspectRunner(t, cfg); got.Writes != 1 {
				t.Fatalf("idle control: %+v", got)
			}
		})
	}
}

// Security STOP latches durably before any write and survives later idle
// evidence, an exact retry, explicit release and host replacement. A separate
// idle session supplies the positive control rather than clearing that latch.
func TestSecurityStopCannotAutomaticallyContinue(t *testing.T) {
	a := &observedFixture{}
	a.input.Store(sessionhost.InputBusy)
	cfg, _ := setup(t, a, false)
	host := openHost(t, cfg)
	notice := sessionhost.Notice{ID: "held", Reference: "fixture/held"}
	if _, err := host.Enqueue(notice); err != nil {
		t.Fatal(err)
	}
	a.input.Store(sessionhost.InputSecurityStop)
	if err := host.ObserveInput(); err != nil {
		t.Fatal(err)
	}
	if !host.Snapshot().SecurityStopped {
		t.Fatal("security STOP not latched")
	}
	a.input.Store(sessionhost.InputIdle)
	if err := host.ObserveInput(); err != nil {
		t.Fatal(err)
	}
	if err := host.Fence(true); err == nil || err.Error() != "security_stopped" {
		t.Fatalf("security release: %v", err)
	}
	host.Close()
	replacement := openHost(t, cfg)
	if !replacement.Snapshot().SecurityStopped {
		t.Fatal("restart lost security STOP")
	}
	if _, err := replacement.Enqueue(notice); err != nil {
		t.Fatal(err)
	}
	if _, err := replacement.Enqueue(sessionhost.Notice{ID: "new", Reference: "fixture/new"}); err == nil || err.Error() != "security_stopped" {
		t.Fatalf("stopped enqueue: %v", err)
	}
	if err := replacement.Fence(true); err == nil || err.Error() != "security_stopped" {
		t.Fatalf("restart release: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	replacement.Close()
	if got := inspectRunner(t, cfg); got.Writes != 0 || got.Exited {
		t.Fatalf("security STOP caused effects: %+v", got)
	}

	validCfg, _ := setup(t, fixtureadapters.JSON{}, false)
	valid := openHost(t, validCfg)
	if _, err := valid.Enqueue(notice); err != nil {
		t.Fatal(err)
	}
	await(t, valid, notice.ID, "applied")
	if got := inspectRunner(t, validCfg); got.Writes != 1 {
		t.Fatalf("idle positive control: %+v", got)
	}
}
