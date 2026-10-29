package sessioncontext

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

type preparedBuilderLaunch struct {
	catalog *materializationRecord
	record  *sm.ContextRecord
	policy  sm.LocalPolicy
	request *sm.BuilderConfinementRequest
}

// LaunchBuilderHost is the original Start-before-Launch/Bind-after-Launch
// transaction for an already leased materialization. It grants no Attach or
// publication authority and uses the same runtime supplied to host shutdown.
// An ordinary launch (empty ctxID) retains the factory's original behavior.
func (m *Service) LaunchBuilderHost(ctx context.Context, session sm.SessionView, factory sm.ProviderHostFactory, request sm.ProviderLaunchRequest) (host sm.RunnerHost, err error) {
	ctxID := session.CtxID
	if factory == nil {
		return nil, fmt.Errorf("provider factory unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(ctxID) == "" {
		if request.BuilderConfinement != nil {
			return nil, fmt.Errorf("builder confinement requires a current materialization")
		}
		return factory.Launch(ctx, request)
	}
	m.mu.Lock()
	prepared, err := m.prepareBuilderLaunchLocked(ctx, session, factory.Provider(), request)
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if prepared.request == nil {
		return factory.Launch(ctx, request)
	}
	accepted := false
	defer func() {
		if accepted {
			return
		}
		if host != nil {
			err = errors.Join(err, host.Stop(context.WithoutCancel(ctx)))
		}
		// Cleanup cannot erase the original refusal or mint successful evidence.
		_, closeErr := m.builderGateways.FinalizeBuilderGateway(context.WithoutCancel(ctx), ctxID)
		err = errors.Join(err, closeErr)
		host = nil
	}()
	confinement := *prepared.request
	confinement.Allowlist.Paths = append([]string(nil), prepared.request.Allowlist.Paths...)
	confinement.RequestedPathSpecs = append([]string(nil), prepared.request.RequestedPathSpecs...)
	confinement.Gateway.Args = append([]string(nil), prepared.request.Gateway.Args...)
	request.BuilderConfinement = &confinement
	host, err = factory.Launch(ctx, request)
	if err != nil {
		return host, err
	}
	if host == nil {
		return nil, fmt.Errorf("nil launched builder host")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, err := m.builderLaunchFactsLocked(ctx, session, factory.Provider(), request)
	if err != nil {
		return host, err
	}
	if !reflect.DeepEqual(current.catalog, prepared.catalog) || !reflect.DeepEqual(current.record, prepared.record) || !reflect.DeepEqual(current.policy, prepared.policy) {
		return host, fmt.Errorf("builder materialization changed during provider launch")
	}
	if _, _, err = sm.ValidateBuilderLaunchReports(host, *prepared.request); err != nil {
		return host, err
	}
	state, stateErr := host.State(ctx)
	if stateErr != nil {
		return host, stateErr
	}
	if state != sm.ProviderHostRunning {
		return host, fmt.Errorf("launched builder host is not running")
	}
	if err := ctx.Err(); err != nil {
		return host, err
	}
	// Bind last: a rejected launch must not acquire a successful native
	// observation during cleanup and expose internally valid terminal data.
	if err = m.builderGateways.BindBuilderGatewayObserver(ctxID, host); err != nil {
		return host, err
	}
	accepted = true
	return host, nil
}

func (m *Service) prepareBuilderLaunchLocked(ctx context.Context, session sm.SessionView, provider string, request sm.ProviderLaunchRequest) (*preparedBuilderLaunch, error) {
	prepared, err := m.builderLaunchFactsLocked(ctx, session, provider, request)
	if err != nil || prepared.request == nil {
		return prepared, err
	}
	if request.BuilderConfinement != nil {
		return nil, fmt.Errorf("caller supplied builder confinement cannot replace host preparation")
	}
	if m.builderGateways == nil {
		return nil, fmt.Errorf("builder gateway runtime unavailable")
	}
	projectDir, err := builderLaunchProjectDir(request.Launch, provider)
	if err != nil {
		return nil, err
	}
	endpoint, err := m.builderGateways.StartBuilderGateway(ctx, prepared.record, projectDir, prepared.request.Allowlist)
	if err != nil {
		return nil, err
	}
	prepared.request.Gateway = endpoint
	prepared.request.IsolationConfigDir = filepath.Join(m.layout.StateDir, "builder-gateway-config")
	return prepared, nil
}

// builderLaunchFactsLocked consumes actual catalog role/target/lease plus
// current source facts and policy. Strings supplied by a caller only constrain
// that independent read; they are not authority or substitutes for run evidence.
// Host Owner is an opaque reservation key, distinct from the explicit catalog
// target fact. Never parse or reinterpret it as an element/run authority.
func (m *Service) builderLaunchFactsLocked(ctx context.Context, session sm.SessionView, provider string, request sm.ProviderLaunchRequest) (*preparedBuilderLaunch, error) {
	ctxID := session.CtxID
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := cloneMaterialization(m.materializations[normalizeContextID(ctxID)])
	if r == nil {
		return nil, fmt.Errorf("builder materialization unavailable")
	}
	if r.State != MaterializationLeased || r.ActiveRunID == "" {
		return nil, fmt.Errorf("builder materialization has no current writer lease")
	}
	if session.SessionID == "" || session.SessionID != request.SessionID || session.Owner == "" || session.Owner != request.Owner || session.Provider != provider {
		return nil, fmt.Errorf("builder launch differs from current host session view")
	}
	if provider != string(r.Provider) || session.TargetElementID != r.TargetElementID {
		return nil, fmt.Errorf("builder launch provider or target differs from current lease")
	}
	if session.ActiveRunID != r.ActiveRunID {
		return nil, fmt.Errorf("builder source run differs from current lease")
	}
	a, err := materializationReadAuthority(ctx, m.materializationAuthority)
	if err != nil {
		return nil, err
	}
	record, err := a.Context(ctx, r.CtxID)
	if err != nil {
		return nil, err
	}
	if err := validateContextFacts(record); err != nil {
		return nil, err
	}
	if err := validateMaterializationMirror(r, record); err != nil {
		return nil, err
	}
	evidence, err := a.RunEvidence(ctx, r.ActiveRunID)
	if err != nil {
		return nil, err
	}
	if evidence == nil || evidence.ID != r.ActiveRunID || evidence.ContextID != r.CtxID || string(evidence.Owner) != r.TargetElementID || !runStatusActive(evidence.Status) {
		return nil, fmt.Errorf("builder writer run is missing, inactive, unknown or foreign")
	}
	if _, err := validateMaterializationTarget(ctx, a, r.BindingScope, r.BindingElementID, r.TargetElementID); err != nil {
		return nil, err
	}
	policy, err := m.localPolicySnapshot(ctx)
	if err != nil {
		return nil, err
	}
	// Reuse the original gate with this read view and the one current policy
	// snapshot, preserving exact authorization, lineage and capability refusals.
	gate := &Service{materializationAuthority: a, localPolicy: policy, builders: m.builders, now: m.now}
	if err := gate.gateMaterializationLocked(ctx, record, r.Provider, r.TargetRole, r.TargetElementID, r.Source, "provider_fork", r.ResolutionMode); err != nil {
		return nil, err
	}
	out := &preparedBuilderLaunch{catalog: r, record: cloneContext(record), policy: cloneLocalPolicy(policy)}
	requires := record.State == "building" && record.Kind == "project" && record.BuilderProfile == "allowlisted-context-builder" && normalizeRole(record.Binding.ProducerRole) == "context-builder"
	if record.RequiresConfinement != requires {
		return nil, fmt.Errorf("builder confinement fact disagrees with original context contract")
	}
	builderRole := normalizeRole(r.TargetRole) == "context-builder"
	if !record.RequiresConfinement {
		if request.BuilderConfinement != nil {
			return nil, fmt.Errorf("ordinary materialization cannot supply builder confinement")
		}
		return out, nil
	}
	if !builderRole || record.State != "building" || record.Kind != "project" || record.BuilderProfile != "allowlisted-context-builder" || normalizeRole(record.Binding.ProducerRole) != "context-builder" || record.Lineage.ProducedByRunID != r.ActiveRunID {
		return nil, fmt.Errorf("builder role, building state or produced run differs from current lease")
	}
	if !policy.BuilderAllowlist.Configured() {
		return nil, fmt.Errorf("builder read allowlist unavailable")
	}
	out.request = &sm.BuilderConfinementRequest{CtxID: r.CtxID, RunID: record.Lineage.ProducedByRunID, Owner: sm.OwnerKey(r.TargetElementID), Provider: r.Provider, Allowlist: sm.NewContextBuilderAllowlist(policy.BuilderAllowlist.Paths), RequestedPathSpecs: append([]string(nil), record.Freshness.PathSpecs...)}
	return out, nil
}

// The original private launch.Plan.ProjectDir is now an actual typed payload;
// it must still agree with the factory's native Cwd, never reconstructed argv.
func builderLaunchProjectDir(launch sm.LaunchSpec, provider string) (string, error) {
	var dir string
	switch provider {
	case "codex":
		if launch.Codex != nil {
			dir = launch.Codex.Plan.ProjectDir
		}
	case "claude":
		if launch.Claude != nil {
			dir = launch.Claude.Plan.ProjectDir
		}
	}
	if !filepath.IsAbs(dir) || dir != launch.Cwd {
		return "", fmt.Errorf("builder launch requires its admitted absolute project directory")
	}
	return filepath.Clean(dir), nil
}
