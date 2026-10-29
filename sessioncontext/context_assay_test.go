package sessioncontext

import (
	"context"
	"encoding/json"
	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
	"testing"
	"time"
)

// This adapter fixture exercises the actual optional factory dispatch, source
// gate and automatic recovery only. Native isolation/attestation mechanisms are
// the owning provider's separately qualified original executable tests.
type contextCodexAssayFixture struct {
	calls   int
	source  sm.ProviderHostRecord
	request sm.CodexContextAssayRequest
	err     error
}

func (f *contextCodexAssayFixture) Provider() string { return "codex" }
func (f *contextCodexAssayFixture) Launch(context.Context, sm.ProviderLaunchRequest) (sm.RunnerHost, error) {
	return nil, errors.New("test fixture never launches")
}
func (f *contextCodexAssayFixture) Restore(context.Context, sm.ProviderHostRecord) (sm.RunnerHost, error) {
	return nil, errors.New("test fixture never restores")
}
func (f *contextCodexAssayFixture) RunContextAssay(_ context.Context, s sm.ProviderHostRecord, q sm.CodexContextAssayRequest) (*sm.CodexContextAssayResult, error) {
	f.calls++
	f.source = s
	f.request = q
	if f.err != nil {
		return nil, f.err
	}
	return &sm.CodexContextAssayResult{Output: json.RawMessage(`{"answer":"observed"}`)}, nil
}

func TestContextCodexAssayUsesRealOptionalDispatchAndRecovery(t *testing.T) {
	m, a, _, _, r, _ := forkServiceFixture(t)
	base := r.PrimaryCtxID
	f := &contextCodexAssayFixture{}
	m.providers = map[string]sm.ProviderHostFactory{"codex": f}
	q := sm.CodexContextAssayRequest{BaseCtxID: base, Prompt: "quiz", OutputSchema: json.RawMessage(`{"type":"object"}`)}
	a.deny = true
	if _, e := m.RunCodexContextAssay(context.Background(), q); !isContextErrorCode(e, "context_secret_class_denied") || f.calls != 0 {
		t.Fatalf("source refusal reached assay: %v calls=%d", e, f.calls)
	}
	a.deny = false
	result, e := m.RunCodexContextAssay(context.Background(), q)
	if e != nil || result == nil || f.calls != 1 || f.request.BaseCtxID != base || !hostPresent(f.source) {
		t.Fatalf("actual optional assay dispatch lost source: %v", e)
	}
	f.err = (ProviderPolicy{}).RequirePayloadPresent(sm.CodexPresenceReport{CtxID: base, Presence: "vanished", CheckedAt: materializationTestClock}, materializationTestClock, "explicit")
	_, e = m.RunCodexContextAssay(context.Background(), q)
	var typed *ContextError
	if !errors.As(e, &typed) || typed.Code != "context_payload_vanished" || !strings.Contains(typed.Message, "marked stale") || a.contexts[base].State != "stale" {
		t.Fatalf("actual assay refusal did not automatically recover: %v", e)
	}
	if _, e = m.RunCodexContextAssay(context.Background(), q); !isContextErrorCode(e, "context_not_ready") || f.calls != 2 {
		t.Fatal("stale source reached another assay")
	}
}

// The real default Codex plan retains the original completed-turn, lineage,
// ready-time, mode and source boundary that its native Launch consumes.
func TestContextCodexDefaultNativePlanAndCompletedTurnRefusal(t *testing.T) {
	for _, completed := range []bool{false, true} {
		m, _, _, _, r, _ := forkServiceFixture(t)
		m.providerMaterializers = nil
		h := m.materializations[r.PrimaryCtxID].ProviderHost
		if completed {
			h.Snapshot.Codex.LastCompletedTurnID = "observed-completed"
		}
		m.materializations[r.PrimaryCtxID].ProviderHost = h
		view, e := m.PrepareRunMaterialization(context.Background(), r)
		if !completed {
			var public *sm.PublicError
			if !errors.As(e, &public) || public.Code != "codex_context_base_not_forkable" || len(m.ListMaterializations()) != 1 {
				t.Fatalf("uncompleted base became a native plan: %v", e)
			}
			continue
		}
		if e != nil || view.State != MaterializationLeased {
			t.Fatal("admitted default plan refused", e)
		}
		plan := m.materializations[r.Context.ID].ProviderHost.Snapshot.CodexFork
		if plan == nil || plan.PrimaryCtxID != r.PrimaryCtxID || plan.Primary.Snapshot.Codex.LastCompletedTurnID != "observed-completed" || plan.BoundaryBanner == "" || plan.PrimaryMode != "explicit" || !plan.PreparedAt.Equal(m.now()) {
			t.Fatal("actual native Codex plan facts dropped")
		}
	}
}

type contextClaudeAssayFixture struct {
	contextCodexAssayFixture
	request sm.ClaudeContextAssayRequest
}

func (f *contextClaudeAssayFixture) Provider() string { return "claude" }
func (f *contextClaudeAssayFixture) RunContextAssay(_ context.Context, s sm.ProviderHostRecord, q sm.ClaudeContextAssayRequest) (*sm.ClaudeContextAssayResult, error) {
	f.calls++
	f.source = s
	f.request = q
	if f.err != nil {
		return nil, f.err
	}
	return &sm.ClaudeContextAssayResult{Output: json.RawMessage(`{"answer":"observed"}`)}, nil
}

// Actual source ReadyAt replaces caller data before optional Claude invocation;
// observed loss automatically updates authority while unverifiable facts do not.
func TestContextClaudeAssayBindsReadyAtAndAutomaticallyRecovers(t *testing.T) {
	r := materializationTestContext("CTX-260729-c04501", true)
	r.Provider = "claude"
	r.State = "ready"
	r.SecretClass = "unrestricted"
	r.ReadyAt = materializationTestClock.Add(-time.Hour)
	r.Payload.SHA256 = strings.Repeat("a", 64)
	a := newFakeMaterializationAuthority(r)
	m := newMaterializationManager(t, testLayout(t, "claude-assay-source"), a)
	host := sm.ProviderHostRecord{Provider: "claude", Snapshot: sm.RunnerSnapshot{Kind: "claude", Claude: &sm.ClaudeRunnerSnapshot{SessionUUID: "11111111-2222-3333-4444-555555555555"}}}
	if _, e := m.CreateMaterialization(context.Background(), CreateMaterializationRequest{Context: r, RunProvider: "claude", TargetElementID: r.Binding.ElementID, TargetRole: "developer", Source: MaterializationSourceBuilt, ProviderHost: host, PayloadSHA256: r.Payload.SHA256}); e != nil {
		t.Fatal(e)
	}
	f := &contextClaudeAssayFixture{}
	m.providers = map[string]sm.ProviderHostFactory{"claude": f}
	q := sm.ClaudeContextAssayRequest{BaseCtxID: r.ID, Prompt: "quiz", OutputSchema: json.RawMessage(`{"type":"object"}`), BaseReadyAt: materializationTestClock.AddDate(5, 0, 0)}
	if _, e := m.RunClaudeContextAssay(context.Background(), q); e != nil || f.calls != 1 || !f.request.BaseReadyAt.Equal(r.ReadyAt) {
		t.Fatalf("verified ReadyAt/dispatch lost: %v", e)
	}
	f.err = &sm.ClaudePayloadPresenceError{ContextID: r.ID, Presence: "unverifiable", ReadyAt: r.ReadyAt, CheckedAt: materializationTestClock}
	if _, e := m.RunClaudeContextAssay(context.Background(), q); !isContextErrorCode(e, "context_payload_unverified") || a.contexts[r.ID].State != "ready" {
		t.Fatalf("unknown facts invalidated source: %v", e)
	}
	f.err = &sm.ClaudePayloadPresenceError{ContextID: r.ID, Presence: "vanished", ReadyAt: r.ReadyAt, CheckedAt: materializationTestClock}
	_, e := m.RunClaudeContextAssay(context.Background(), q)
	var typed *ContextError
	if !errors.As(e, &typed) || typed.Code != "context_payload_vanished" || !strings.Contains(typed.Message, "marked stale") || a.contexts[r.ID].State != "stale" {
		t.Fatalf("Claude native refusal not automatically recovered: %v", e)
	}
}
