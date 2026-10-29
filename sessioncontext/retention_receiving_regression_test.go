package sessioncontext

import (
	"context"
	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
	"testing"
	"time"
)

// The public typed refusal carries the actual recovery outcome, with the
// original code, retry decision and cause intact; failure cannot claim success.
func TestContextRecoveryTypedOutcomePreservesOriginalRefusal(t *testing.T) {
	for _, failed := range []bool{false, true} {
		r := materializationTestContext("CTX-260729-c04401", true)
		r.State = "ready"
		a := newFakeMaterializationAuthority(r)
		m := newMaterializationManager(t, testLayout(t, "typed-recovery"), a)
		cause := errors.New("original observed loss")
		refusal := contextPayloadVanished(r.ID, &ContextError{Code: "context_payload_vanished", Message: "actual base vanished", Cause: cause, RetryNextCandidate: true})
		if failed {
			a.err = errors.New("source unavailable")
		}
		e := m.RecoverUnusable(context.Background(), refusal)
		var typed *ContextError
		if !errors.As(e, &typed) || typed.Code != "context_payload_vanished" || !typed.RetryNextCandidate || !errors.Is(e, cause) {
			t.Fatalf("original typed refusal lost: %v", e)
		}
		if failed {
			if !strings.Contains(typed.Message, "still recorded as ready") || a.contexts[r.ID].State != "ready" {
				t.Fatal("failed recovery lost typed still-recorded-ready consequence")
			}
		} else {
			if !strings.Contains(typed.Message, "marked stale") || !strings.Contains(typed.Message, "rebuilt from cold") || a.contexts[r.ID].State != "stale" {
				t.Fatal("successful recovery lost typed stale/cold consequence")
			}
		}
	}
}

// A populated but malformed snapshot is not an absent snapshot. Neither the
// applying nor dry pass may release custody when its bytes cannot be proven.
func TestContextMalformedHandleRetainsReleaseCustody(t *testing.T) {
	m, _, _, _ := seededLease(t)
	r := cloneMaterialization(m.materializations["CTX-260729-c01000"])
	r.ProviderHost.Snapshot.Kind = "wrong-variant"
	if _, _, e := m.planMaterializationByteReleaseLocked(r); e == nil {
		t.Fatal("malformed captured handle planned as empty")
	}
	if _, _, e := m.releaseMaterializationBytesLocked(context.Background(), r); e == nil {
		t.Fatal("malformed captured handle released as empty")
	}
	r.ProviderHost.Snapshot = sm.RunnerSnapshot{}
	if _, _, e := m.planMaterializationByteReleaseLocked(r); e != nil {
		t.Fatal("true never-captured cold neighbor refused", e)
	}
}

// Planning a registry-approved ready payload is dry; it neither requires nor
// performs the applying pass's retirement and cannot delete or confirm bytes.
func TestContextReclamationDryPlanDoesNotRequireAppliedRetirement(t *testing.T) {
	m, a, r, _ := seededLease(t)
	a.contexts[r.ID].State = "ready"
	local := m.materializations[r.ID]
	local.State = MaterializationDetached
	local.ActiveRunID = ""
	local.LeaseAcquiredAt = time.Time{}
	local.ProviderHost.Snapshot.Codex.CodexHome = t.TempDir()
	out := m.reclaimOnePayloadLocked(context.Background(), sm.ContextRetentionCollection{CtxID: r.ID, Provider: "codex", IncludeArchived: true}, true, materializationTestClock)
	if out.Status != ContextReclamationPlanned || a.contexts[r.ID].State != "ready" || len(a.confirmAttempts) != 0 {
		t.Fatalf("dry approved plan required applied retirement: %+v", out)
	}
}
