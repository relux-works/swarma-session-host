package sessioncontext

import (
	"context"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"slices"
	"strings"
)

// ProviderMaterializer performs the real provider-owned fork/assay mechanics.
// The service authorizes and supplies source facts; it never rebuilds native argv.
// No production implementation is manufactured when an owner has not supplied it.
type ProviderMaterializer interface {
	Prepare(context.Context, ForkPreparation) (sm.ProviderHostRecord, error)
}
type ForkPreparation struct {
	Child                                         *sm.ContextRecord
	Primary                                       *sm.ContextRecord
	Manifest                                      *sm.CoverageManifest
	Decision                                      *sm.ContextForkPreflightDecision
	Banner                                        *sm.ContextBoundaryBanner
	Rendered                                      string
	SupplementalDecisions                         []*sm.ContextForkPreflightDecision
	SupplementalBanners                           []*sm.ContextBoundaryBanner
	SupplementalRendered                          []string
	Supplemental                                  []*sm.ContextRecord
	SupplementalManifests                         []*sm.CoverageManifest
	TargetElementID, TargetRole, TargetProjectDir string
	Mode                                          sm.ContextResolutionMode
	PrimaryHost                                   sm.ProviderHostRecord
}
type PrepareRunMaterializationRequest struct {
	Context            *sm.ContextRecord        `json:"context"`
	PrimaryCtxID       string                   `json:"primary_ctx_id,omitempty"`
	SupplementalCtxIDs []string                 `json:"supplemental_ctx_ids,omitempty"`
	TargetElementID    string                   `json:"target_element_id"`
	TargetRole         string                   `json:"target_role"`
	Mode               sm.ContextResolutionMode `json:"mode"`
	TargetProjectDir   string                   `json:"target_project_dir,omitempty"`
	RunID              string                   `json:"run_id"`
}
type CompleteRunMaterializationRequest struct {
	CtxID     string           `json:"ctx_id"`
	RunID     string           `json:"run_id"`
	Published bool             `json:"published"`
	Usage     *ContextRunUsage `json:"usage,omitempty"`
}

func (m *Service) PrepareRunMaterialization(ctx context.Context, r PrepareRunMaterializationRequest) (MaterializationView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !materializationRunIDPattern.MatchString(strings.TrimSpace(r.RunID)) {
		return MaterializationView{}, fmt.Errorf("invalid run id %q", r.RunID)
	}
	if err := validateContextFacts(r.Context); err != nil {
		return MaterializationView{}, err
	}
	if r.Context.State != "building" {
		return MaterializationView{}, fmt.Errorf("building child context is required")
	}
	if m.materializationAuthority == nil {
		return MaterializationView{}, fmt.Errorf("context authority unavailable")
	}
	if err := m.ensureMaterializationRunAvailableLocked(r.RunID, r.Context.ID); err != nil {
		return MaterializationView{}, err
	}
	if r.Context.Lineage.ProducedByRunID != r.RunID {
		return MaterializationView{}, fmt.Errorf("building child belongs to a different source run")
	}
	if err := m.requireActiveRunLocked(ctx, r.Context.ID, r.RunID, r.TargetElementID); err != nil {
		return MaterializationView{}, err
	}
	// Validate child/target before provider preparation, not after a fork writes.
	req := CreateMaterializationRequest{Context: r.Context, RunProvider: r.Context.Provider, TargetElementID: r.TargetElementID, TargetRole: r.TargetRole, Mode: r.Mode, Source: MaterializationSourceBuilt, ProviderHost: sm.ProviderHostRecord{Provider: string(r.Context.Provider)}}
	_, _, authoritative, err := m.prepareMaterializationLocked(ctx, req)
	if err != nil {
		return MaterializationView{}, err
	}
	if authoritative.Lineage.PrimaryCtxID != r.Context.Lineage.PrimaryCtxID || authoritative.Lineage.ProducedByRunID != r.Context.Lineage.ProducedByRunID || authoritative.Lineage.SupersedesCtxID != r.Context.Lineage.SupersedesCtxID || !slices.Equal(authoritative.Lineage.SupplementalCtxIDs, r.Context.Lineage.SupplementalCtxIDs) {
		return MaterializationView{}, fmt.Errorf("child lineage changed before materialization")
	}
	// All subsequent native preparation consumes the verified source facts, not
	// a caller's same-revision JSON copy (including runtime-only confinement).
	r.Context = cloneContext(authoritative)
	req.Context = r.Context
	if r.PrimaryCtxID != "" {
		primary := normalizeContextID(r.PrimaryCtxID)
		if r.Context.Lineage.PrimaryCtxID != primary {
			return MaterializationView{}, fmt.Errorf("child primary lineage does not match requested base")
		}
		a, err := m.admitContextFork(ctx, primary, r.Context.Provider, r.TargetRole, r.TargetElementID, r.Mode)
		if err != nil {
			return MaterializationView{}, err
		}
		source := m.materializations[primary]
		if source == nil || source.State != MaterializationReady || !hostPresent(source.ProviderHost) {
			return MaterializationView{}, fmt.Errorf("ready local primary handle unavailable")
		}
		if err := validateMaterializationMirror(source, a.Record); err != nil {
			return MaterializationView{}, err
		}
		prep := ForkPreparation{Child: cloneContext(r.Context), Primary: a.Record, Manifest: a.Manifest, Decision: a.Decision, Banner: a.Banner, Rendered: a.Rendered, PrimaryHost: cloneHostRecord(source.ProviderHost), TargetElementID: r.TargetElementID, TargetRole: r.TargetRole, TargetProjectDir: r.TargetProjectDir, Mode: r.Mode}
		if len(r.SupplementalCtxIDs) != len(r.Context.Lineage.SupplementalCtxIDs) {
			return MaterializationView{}, fmt.Errorf("supplemental lineage does not match")
		}
		for i, id := range r.SupplementalCtxIDs {
			if normalizeContextID(id) != r.Context.Lineage.SupplementalCtxIDs[i] {
				return MaterializationView{}, fmt.Errorf("supplemental lineage does not match")
			}
			s, e := m.admitContextFork(ctx, id, r.Context.Provider, r.TargetRole, r.TargetElementID, r.Mode)
			if e != nil {
				return MaterializationView{}, e
			}
			prep.Supplemental = append(prep.Supplemental, s.Record)
			prep.SupplementalManifests = append(prep.SupplementalManifests, s.Manifest)
			prep.SupplementalDecisions = append(prep.SupplementalDecisions, s.Decision)
			prep.SupplementalBanners = append(prep.SupplementalBanners, s.Banner)
			prep.SupplementalRendered = append(prep.SupplementalRendered, s.Rendered)
		}
		supplier := m.providerMaterializers[r.Context.Provider]
		var host sm.ProviderHostRecord
		if supplier != nil {
			host, err = supplier.Prepare(ctx, prep)
		} else if r.Context.Provider == "codex" {
			host, err = m.prepareCodexContextPlan(prep)
		} else if r.Context.Provider == "claude" {
			host, err = m.prepareClaudeContextPlan(ctx, prep)
		} else {
			err = fmt.Errorf("%s context materialization supplier unavailable", r.Context.Provider)
		}
		if err != nil {
			return MaterializationView{}, m.recoverUnusableLocked(ctx, ClaudePolicyError(err))
		}
		if host.Provider != string(r.Context.Provider) || !hostPresent(host) {
			return MaterializationView{}, fmt.Errorf("provider materializer returned no matching real handle")
		}
		req.Source = MaterializationSourceForked
		req.ProviderHost = host
	} else if r.Context.Lineage.PrimaryCtxID != "" || len(r.SupplementalCtxIDs) != 0 || len(r.Context.Lineage.SupplementalCtxIDs) != 0 {
		return MaterializationView{}, fmt.Errorf("cold build cannot silently discard lineage")
	}
	if _, err := m.createMaterializationLocked(ctx, req); err != nil {
		return MaterializationView{}, err
	}
	return m.acquireMaterializationLeaseLocked(ctx, r.Context.ID, r.RunID, r.TargetElementID, false)
}

// MaterializedProviderHost is the core's pre-Launch/Restore read boundary. The
// caller must already hold this exact lease; authorization is repeated now.
func (m *Service) MaterializedProviderHost(ctx context.Context, id, run, target, role string, p sm.ContextProvider) (sm.ProviderHostRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.materializations[normalizeContextID(id)]
	if r == nil || r.State != MaterializationLeased || r.ActiveRunID != run || r.TargetElementID != normalizeElementID(target) || r.TargetRole != normalizeRole(role) || r.Provider != p {
		return sm.ProviderHostRecord{}, fmt.Errorf("exact current materialization lease required")
	}
	if err := m.requireActiveRunLocked(ctx, r.CtxID, run, target); err != nil {
		return sm.ProviderHostRecord{}, err
	}
	c, err := m.materializationAuthority.Context(ctx, r.CtxID)
	if err != nil {
		return sm.ProviderHostRecord{}, err
	}
	if err = validateMaterializationMirror(r, c); err != nil {
		return sm.ProviderHostRecord{}, err
	}
	if err = m.gateMaterializationLocked(ctx, c, p, role, target, r.Source, "provider_resume", r.ResolutionMode); err != nil {
		return sm.ProviderHostRecord{}, err
	}
	return cloneHostRecord(r.ProviderHost), nil
}

// RecordProviderHost is called by the sole core record writer after it observes
// the actual launched/settled snapshot. It cannot inject a different provider.
func (m *Service) RecordProviderHost(id, run string, h sm.ProviderHostRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.materializations[normalizeContextID(id)]
	if r == nil || r.State != MaterializationLeased || r.ActiveRunID != run || h.Provider != string(r.Provider) || !hostPresent(h) {
		return fmt.Errorf("matching leased provider snapshot required")
	}
	next := *r
	next.ProviderHost = cloneHostRecord(h)
	next.UpdatedAt = m.now()
	if err := m.materializationRecords.save(&next); err != nil {
		return err
	}
	*r = next
	return nil
}
func (m *Service) CompleteRunMaterialization(ctx context.Context, r CompleteRunMaterializationRequest) (MaterializationView, error) {
	m.mu.Lock()
	local := m.materializations[normalizeContextID(r.CtxID)]
	if local == nil || local.State != MaterializationLeased || local.ActiveRunID != r.RunID {
		m.mu.Unlock()
		return MaterializationView{}, fmt.Errorf("exact completing lease required")
	}
	host := cloneHostRecord(local.ProviderHost)
	provider := local.Provider
	m.mu.Unlock()
	path, err := materializationTranscriptPath(provider, host)
	if err != nil {
		return MaterializationView{}, err
	}
	i, err := inspectTranscriptPath(provider, path)
	if err != nil {
		return MaterializationView{}, err
	}
	if i.Compaction != nil {
		return MaterializationView{}, fmt.Errorf("completed transcript is compacted")
	}
	return m.CompleteMaterialization(ctx, CompleteMaterializationRequest{CtxID: r.CtxID, RunID: r.RunID, Published: r.Published, PayloadSHA256: i.SHA256, ProviderHost: host, Usage: r.Usage})
}
func (m *Service) FailRunMaterialization(ctx context.Context, id, run string) (MaterializationView, error) {
	return m.FailMaterialization(ctx, id, run)
}

// Source run status is required before native IO. Active=false or a missing,
// unknown, corrupt or mismatched receipt never proves an admitted holder.
func (m *Service) requireActiveRunLocked(ctx context.Context, id, run, target string) error {
	if m.materializationAuthority == nil {
		return fmt.Errorf("run authority unavailable")
	}
	e, err := m.materializationAuthority.RunEvidence(ctx, run)
	if err != nil {
		return err
	}
	if e == nil || e.ID != run || e.ContextID != id || normalizeElementID(e.Owner) != normalizeElementID(target) || !runStatusActive(e.Status) {
		return fmt.Errorf("matching active source run receipt required")
	}
	return nil
}
