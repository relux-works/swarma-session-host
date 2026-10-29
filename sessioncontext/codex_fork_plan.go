package sessioncontext

import (
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

// The original native adapter consumes this actual typed plan at Launch. The
// service prepares only verified source facts: no argv, synthetic native ID or
// native RPC runs here, and a plan is never called an observed launched host.
func (m *Service) prepareCodexContextPlan(p ForkPreparation) (sm.ProviderHostRecord, error) {
	fail := func(e error) (sm.ProviderHostRecord, error) { return sm.ProviderHostRecord{}, e }
	if p.Primary == nil || p.Child == nil || p.PrimaryHost.Provider != "codex" || p.PrimaryHost.Snapshot.Kind != "codex" || p.PrimaryHost.Snapshot.Codex == nil {
		return fail(fmt.Errorf("matching actual Codex base snapshot required"))
	}
	if strings.TrimSpace(p.PrimaryHost.Snapshot.Codex.LastCompletedTurnID) == "" {
		return fail(&sm.PublicError{Code: "codex_context_base_not_forkable", Message: fmt.Sprintf("Codex context %s has no manager-observed completed turn; refuse the empty or interrupted rollout before app-server RPC", p.Primary.ID)})
	}
	if p.Decision == nil || !p.Decision.Admitted || p.Banner == nil || p.Rendered == "" || p.Rendered != p.Banner.Render() {
		return fail(fmt.Errorf("actual admitted source boundary required"))
	}
	if len(p.Supplemental) != len(p.SupplementalRendered) {
		return fail(fmt.Errorf("matching supplemental source boundaries required"))
	}
	plan := &sm.CodexContextForkPlan{Contract: "task-board.codex-context-fork-plan-v1", PrimaryCtxID: p.Primary.ID, Primary: cloneHostRecord(p.PrimaryHost), PrimaryReadyAt: p.Primary.ReadyAt, PrimaryMode: p.Decision.Mode, BoundaryBanner: p.Rendered, PreparedAt: m.now()}
	for i, r := range p.Supplemental {
		handoff := strings.TrimSpace(p.SupplementalRendered[i])
		if r == nil || handoff == "" {
			return fail(fmt.Errorf("supplemental source boundary required"))
		}
		plan.Supplemental = append(plan.Supplemental, sm.CodexSupplementalContext{CtxID: r.ID, Handoff: handoff})
	}
	return sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex-fork", CodexFork: plan}}, nil
}
