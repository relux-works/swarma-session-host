package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// builderGatewaySession is one live gateway and the socket serving it.
//
// The manager keeps them together because they die together: closing the
// gateway mints the completeness attestation, and leaving the socket open past
// that point would accept a call the attestation already said did not happen.
type builderGatewaySession struct {
	ctxID     string
	tracePath string
	gateway   *BuilderEvidenceGateway
	service   *BuilderGatewayService
	observer  BuilderToolObservationReporter
}

// BuilderGatewayRuntime owns actual gateway sessions independently of board
// authority and of the controller's record writer. It signs no claim/record.
type BuilderGatewayRuntime struct {
	mu              sync.Mutex
	layout          Layout
	command         string
	args            []string
	now             func() time.Time
	builderGateways map[string]*builderGatewaySession
	failures        map[string]error
}

type BuilderGatewayRuntimeOptions struct {
	Layout Layout
	// The executable composition owner must install the real shim command. No
	// provider launch or command discovery is performed by this library.
	ShimCommand string
	ShimArgs    []string
	Now         func() time.Time
}

func NewBuilderGatewayRuntime(o BuilderGatewayRuntimeOptions) (*BuilderGatewayRuntime, error) {
	if !filepath.IsAbs(o.Layout.StateDir) || !filepath.IsAbs(o.Layout.SocketPath) || !filepath.IsAbs(o.ShimCommand) {
		return nil, fmt.Errorf("explicit absolute builder state/socket/shim paths are required")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &BuilderGatewayRuntime{layout: o.Layout, command: o.ShimCommand, args: append([]string(nil), o.ShimArgs...), now: o.Now, builderGateways: map[string]*builderGatewaySession{}, failures: map[string]error{}}, nil
}

// StartBuilderGateway must run before the actual provider launch. Its endpoint
// is the one that launch installs; no caller/board authority is inferred here.
func (m *BuilderGatewayRuntime) StartBuilderGateway(ctx context.Context, record *ContextRecord, projectDir string, allowlist ContextBuilderAllowlist) (BuilderGatewayEndpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return BuilderGatewayEndpoint{}, err
	}
	if record == nil {
		return BuilderGatewayEndpoint{}, fmt.Errorf("builder context facts required")
	}
	id := builderNormalizeContextID(record.ID)
	if m.builderGateways[id] != nil {
		return BuilderGatewayEndpoint{}, builderTraceError(ContextErrorBuilderTraceUnavailable, fmt.Sprintf("a repository-evidence gateway is already recording for %s", id), nil)
	}
	if err := m.failures[id]; err != nil {
		return BuilderGatewayEndpoint{}, err
	}
	tracePath := BuilderAccessTracePath(m.layout, id)
	gateway, err := NewBuilderEvidenceGateway(BuilderEvidenceGatewayConfig{CtxID: id, RunID: strings.TrimSpace(record.Lineage.ProducedByRunID), Provider: record.Provider, RepositoryRoot: projectDir, Allowlist: NewContextBuilderAllowlist(allowlist.Paths), TracePath: tracePath, Now: m.now})
	if err != nil {
		return BuilderGatewayEndpoint{}, err
	}
	service, err := StartBuilderGatewayService(gateway, builderGatewaySocketPath(m.layout, id))
	if err != nil {
		_, _ = gateway.Close()
		return BuilderGatewayEndpoint{}, builderTraceError(ContextErrorBuilderTraceUnavailable, fmt.Sprintf("the repository-evidence gateway for %s could not be published to the builder", id), err)
	}
	m.builderGateways[id] = &builderGatewaySession{ctxID: id, tracePath: tracePath, gateway: gateway, service: service}
	return service.Endpoint(m.command, m.args), nil
}

// BindBuilderGatewayObserver binds the actual launched runner, never a report
// supplied in a publication body. A runner with no native observer is refused.
func (m *BuilderGatewayRuntime) BindBuilderGatewayObserver(ctxID string, host RunnerHost) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := builderNormalizeContextID(ctxID)
	session := m.builderGateways[id]
	if session == nil {
		return builderTraceError(ContextErrorBuilderTraceUnavailable, fmt.Sprintf("the context-builder launch for %s has no live repository-evidence gateway to reconcile", id), nil)
	}
	reporter, ok := host.(BuilderToolObservationReporter)
	if !ok {
		return builderTraceError(ContextErrorBuilderToolIsolationUnavailable, fmt.Sprintf("provider host %T cannot observe its native tool stream for context-builder %s", host, id), nil)
	}
	session.observer = reporter
	return nil
}

func (m *BuilderGatewayRuntime) closeBuilderGatewayLocked(
	ctxID string,
) (ContextRepositoryAccessAttestation, error) {
	normalized := builderNormalizeContextID(ctxID)
	session := m.builderGateways[normalized]
	if session == nil {
		return ContextRepositoryAccessAttestation{}, m.failures[normalized]
	}
	delete(m.builderGateways, normalized)
	var (
		providerObservation BuilderToolObservation
		observationErr      error
	)
	if session.observer == nil {
		observationErr = &BuilderTraceError{
			Code: ContextErrorBuilderToolIsolationUnavailable,
			Message: fmt.Sprintf(
				"the provider adapter for %s supplied no native tool-stream observer, so the "+
					"repository-access trace cannot be reconciled against the run that produced it",
				normalized,
			),
		}
	} else {
		observeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		providerObservation, observationErr = session.observer.BuilderToolObservation(observeCtx)
		cancel()
		if observationErr != nil {
			observationErr = &BuilderTraceError{
				Code: ContextErrorBuilderTraceUnavailable,
				Message: fmt.Sprintf(
					"the provider adapter could not read its native tool stream for %s, so the "+
						"repository-access trace cannot be shown to be complete",
					normalized,
				),
				Cause: observationErr,
			}
		}
	}
	// The socket closes first. Attesting while the builder can still call would
	// bind a count that a later call immediately contradicts.
	stopErr := session.service.Stop()
	shimObservation := session.gateway.ToolObservation()
	attestation, err := session.gateway.Close()
	if err != nil {
		return ContextRepositoryAccessAttestation{}, err
	}
	// The attestation says the trace is internally consistent. Reconciling it
	// against what the shim saw is what says the trace is *complete*: a call the
	// provider made and the gateway never recorded is invisible to the chain and
	// visible here.
	if observationErr != nil {
		return ContextRepositoryAccessAttestation{}, observationErr
	}
	// The provider adapter owns the call/lifecycle verdict.  The shim contributes
	// only evidence it uniquely sees: whether the configured gateway actually
	// started and any malformed MCP frame it rejected.  Using the shim's call
	// count here would compare the trace with another counter derived from the
	// same transport and would miss a native provider route entirely.
	providerObservation.Reported = providerObservation.Reported && shimObservation.Reported
	providerObservation.UnknownShapes += shimObservation.UnknownShapes
	if err := requireBuilderTraceCompleteness(normalized, providerObservation, attestation); err != nil {
		return ContextRepositoryAccessAttestation{}, err
	}
	if stopErr != nil {
		return attestation, &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the repository-evidence gateway for %s did not shut down cleanly, so its trace cannot be "+
					"shown to be closed to further access",
				normalized,
			),
			Cause: stopErr,
		}
	}
	return attestation, nil
}

// BuilderGatewayFinalization retains the original protocol8 answer. An internally
// verified hash chain is data, not an accepted canonical signed publication.
type BuilderGatewayFinalization struct {
	CtxID       string                             `json:"ctx_id"`
	Served      bool                               `json:"served"`
	Attestation ContextRepositoryAccessAttestation `json:"attestation,omitempty"`
}

// FinalizeBuilderGateway closes the actual socket before attesting/reloading the
// trace. Cleanup still runs on cancellation; a canceled caller receives no usable
// evidence. Failed closes/reconciliation remain refusals on retry in this owner.
func (m *BuilderGatewayRuntime) FinalizeBuilderGateway(ctx context.Context, ctxID string) (BuilderGatewayFinalization, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failures == nil {
		m.failures = map[string]error{}
	}
	id := builderNormalizeContextID(ctxID)
	if err := builderValidateContextID(id); err != nil {
		return BuilderGatewayFinalization{}, err
	}
	served := m.builderGateways[id] != nil
	attestation, err := m.closeBuilderGatewayLocked(id)
	if err != nil {
		m.failures[id] = err
		return BuilderGatewayFinalization{}, err
	}
	if err := ctx.Err(); err != nil {
		return BuilderGatewayFinalization{}, err
	}
	return BuilderGatewayFinalization{CtxID: id, Served: served, Attestation: attestation}, nil
}

// BuilderAccessTrace verifies on read and binds the requested CTX to the actual
// terminal trace, rather than trusting its filename as evidence of identity.
func (m *BuilderGatewayRuntime) BuilderAccessTrace(ctxID string) ([]ContextRepositoryAccessEvent, ContextRepositoryAccessAttestation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := builderNormalizeContextID(ctxID)
	if err := builderValidateContextID(id); err != nil {
		return nil, ContextRepositoryAccessAttestation{}, err
	}
	if err := m.failures[id]; err != nil {
		return nil, ContextRepositoryAccessAttestation{}, err
	}
	return loadBuilderAccessTraceForContext(m.layout, id)
}

func loadBuilderAccessTraceForContext(layout Layout, id string) ([]ContextRepositoryAccessEvent, ContextRepositoryAccessAttestation, error) {
	events, attestation, err := LoadBuilderAccessTrace(BuilderAccessTracePath(layout, id))
	if err != nil {
		return nil, ContextRepositoryAccessAttestation{}, err
	}
	if attestation.CtxID != id {
		return nil, ContextRepositoryAccessAttestation{}, builderTraceError(ContextErrorBuilderTraceUnavailable, "the repository-access trace belongs to another context", nil)
	}
	return events, attestation, nil
}

// RequireBuilderCoverageEvidence is the original closed-trace requirement. It
// verifies trace integrity only, not claim authenticity or publication authority.
func RequireBuilderCoverageEvidence(layout Layout, ctxID string) error {
	id := builderNormalizeContextID(ctxID)
	if err := builderValidateContextID(id); err != nil {
		return err
	}
	_, _, err := loadBuilderAccessTraceForContext(layout, id)
	return err
}

// ErrBuilderGatewayNotRunning is a connector's positive observation that the
// owning host is absent. Unknown/dial errors must never be mapped to this value.
var ErrBuilderGatewayNotRunning = errors.New("builder gateway owner is not running")

// BuilderGatewayFinalizationClient is the original finalization control-route
// contract, not a board callback or a finalized signed record carrier.
type BuilderGatewayFinalizationClient interface {
	FinalizeBuilderGateway(context.Context, string) (BuilderGatewayFinalization, error)
	Close() error
}

// FinalizeAndRequireBuilderCoverageEvidence preserves non-starting connect,
// actual close-before-read, absent-vs-unknown and failed-close refusal ordering.
// Callers still need the canonical claim/trace signer+carrier; this hashes no key.
func FinalizeAndRequireBuilderCoverageEvidence(ctx context.Context, layout Layout, ctxID string, connect func(context.Context, Layout) (BuilderGatewayFinalizationClient, error)) error {
	id := builderNormalizeContextID(ctxID)
	if err := builderValidateContextID(id); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if connect != nil {
		client, err := connect(ctx, layout)
		switch {
		case err == nil:
			if client == nil {
				return builderTraceError(ContextErrorBuilderTraceUnavailable, "the gateway finalization connector returned no client", nil)
			}
			defer client.Close()
			if _, err := client.FinalizeBuilderGateway(ctx, id); err != nil {
				return err
			}
		case errors.Is(err, ErrBuilderGatewayNotRunning):
		default:
			return builderTraceError(ContextErrorBuilderTraceUnavailable, fmt.Sprintf("the Session Manager that recorded the repository-access trace for %s could not be reached to close it, so the trace cannot be shown to be complete", id), err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return RequireBuilderCoverageEvidence(layout, id)
}

func builderGatewaySocketPath(layout Layout, ctxID string) string {
	digest := builderNormalizeContextID(ctxID)
	if len(digest) > 20 {
		digest = digest[len(digest)-20:]
	}
	return filepath.Join(filepath.Dir(layout.SocketPath), "gw-"+digest+".sock")
}

// Close releases every owned gateway during host shutdown. Refusals are joined
// rather than converted into a successful completeness observation.
func (m *BuilderGatewayRuntime) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var combined error
	if m.failures == nil {
		m.failures = map[string]error{}
	}
	for id := range m.builderGateways {
		_, err := m.closeBuilderGatewayLocked(id)
		if err != nil {
			m.failures[id] = err
			combined = errors.Join(combined, err)
		}
	}
	return combined
}

// CloseBuilderGateway is the original in-process terminal entry point.
func (m *BuilderGatewayRuntime) CloseBuilderGateway(ctxID string) (ContextRepositoryAccessAttestation, error) {
	r, err := m.FinalizeBuilderGateway(context.Background(), ctxID)
	return r.Attestation, err
}
