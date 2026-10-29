package sessioncontext

import (
	"context"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"path/filepath"
	"strings"
)

// The service supplies admitted source/lineage/boundary facts. The real provider
// owns transcript inspection/normalization and returns its original typed plan.
func (m *Service) prepareClaudeContextPlan(ctx context.Context, p ForkPreparation) (sm.ProviderHostRecord, error) {
	fail := func(e error) (sm.ProviderHostRecord, error) { return sm.ProviderHostRecord{}, e }
	factory, ok := m.providers["claude"].(interface {
		PrepareContextFork(context.Context, sm.ProviderHostRecord, sm.ClaudeContextForkPlan) (sm.ProviderHostRecord, error)
	})
	if !ok {
		return fail(fmt.Errorf("Claude provider transcript preparation capability unavailable"))
	}
	if p.Child == nil || p.Primary == nil || p.Decision == nil || !p.Decision.Admitted || p.Banner == nil || p.Rendered != p.Banner.Render() {
		return fail(fmt.Errorf("actual admitted Claude source boundary required"))
	}
	input := sm.ClaudeContextForkPlan{Contract: "task-board.claude-context-fork-plan-v1", ChildCtxID: p.Child.ID, PrimaryCtxID: p.Primary.ID, TargetProjectDir: p.TargetProjectDir, PrimaryReadyAt: p.Primary.ReadyAt, PrimaryMode: p.Decision.Mode, BoundaryBanner: p.Rendered, PreparedAt: m.now()}
	if len(p.Supplemental) != len(p.SupplementalRendered) {
		return fail(fmt.Errorf("matching supplemental boundaries required"))
	}
	for i, r := range p.Supplemental {
		if r == nil || strings.TrimSpace(p.SupplementalRendered[i]) == "" {
			return fail(fmt.Errorf("supplemental source boundary required"))
		}
		input.Supplemental = append(input.Supplemental, sm.ClaudeSupplementalContext{CtxID: r.ID, Handoff: strings.TrimSpace(p.SupplementalRendered[i])})
	}
	host, err := factory.PrepareContextFork(ctx, cloneHostRecord(p.PrimaryHost), input)
	if err != nil {
		return fail(err)
	}
	plan := host.Snapshot.ClaudeFork
	if host.Provider != "claude" || !hostPresent(host) || host.Snapshot.Kind != "claude-fork" || plan == nil || plan.Contract != input.Contract || plan.ChildCtxID != input.ChildCtxID || plan.PrimaryCtxID != input.PrimaryCtxID || plan.TargetProjectDir != filepath.Clean(input.TargetProjectDir) || plan.BoundaryBanner != input.BoundaryBanner || !plan.PrimaryReadyAt.Equal(input.PrimaryReadyAt) || plan.PrimaryMode != input.PrimaryMode || !plan.PreparedAt.Equal(input.PreparedAt) || !materializationDigestPattern.MatchString(plan.BaseSHA256) || plan.BaseRecords < 1 || plan.BaseSessionUUID == "" || !filepath.IsAbs(plan.BaseProjectDir) || plan.BaseFormatVersion == "" || len(plan.Supplemental) != len(input.Supplemental) {
		return fail(fmt.Errorf("Claude provider returned a foreign or incomplete prepared source plan"))
	}
	for i, item := range plan.Supplemental {
		if item != input.Supplemental[i] {
			return fail(fmt.Errorf("Claude provider changed admitted supplemental source inputs"))
		}
	}
	return cloneHostRecord(host), nil
}
