// Package sessioncontext owns the durable machine-local context catalog. It
// consumes typed authority facts; it neither discovers caller identity nor links
// to a task-board store, transport or callback.
package sessioncontext

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

type MaterializationAuthority = sm.ContextAuthority
type MaterializationElement = sm.OwnerFact

// References must report the current host read set, including recovering and
// pending sessions. Unknown visibility is a hold, never an empty set.
type References interface {
	Referenced(context.Context, string) (live bool, known bool, err error)
}

// BuilderLifecycle is supplied by the real owning builder gateway/confinement
// runtime. Missing support refuses a confined build before catalog effects.
type BuilderLifecycle interface {
	RequireConfinement(context.Context, *sm.ContextRecord) error
	Finalize(context.Context, string) (BuilderGatewayFinalization, error)
	Enforcement(context.Context, string) (*sm.ContextBuilderEnforcement, error)
}

// MaterializationHostCodec is the original provider-host wire conversion,
// supplied by the shared/provider codec owner. Missing conversion never turns
// an old opaque handle into an empty modern snapshot.
type MaterializationHostCodec interface {
	DecodeProviderHost(context.Context, []byte) (sm.ProviderHostRecord, error)
	EncodeProviderHost(context.Context, []byte, sm.ProviderHostRecord) ([]byte, error)
}
type Options struct {
	Providers             map[string]sm.ProviderHostFactory
	Validation            sm.ContextForkValidationSource
	Config                sm.HostConfig
	RetentionConfig       RetentionConfig
	HostCodec             MaterializationHostCodec
	Policy                sm.LocalPolicy
	Layout                sm.Layout
	Authority             sm.ContextAuthority
	References            References
	Builders              BuilderLifecycle
	BuilderGateways       *sm.BuilderGatewayRuntime
	Now                   func() time.Time
	OperatorLog           func(string, error, map[string]any)
	ProviderMaterializers map[sm.ContextProvider]ProviderMaterializer
}
type Service struct {
	providers                   map[string]sm.ProviderHostFactory
	validation                  sm.ContextForkValidationSource
	config                      sm.HostConfig
	retentionConfig             RetentionConfig
	localPolicy                 sm.LocalPolicy
	mu                          sync.Mutex
	layout                      sm.Layout
	materializationAuthority    sm.ContextAuthority
	materializationRecords      *materializationStore
	materializations            map[string]*materializationRecord
	quarantinedMaterializations int
	references                  References
	builders                    BuilderLifecycle
	builderGateways             *sm.BuilderGatewayRuntime
	now                         func() time.Time
	operatorLog                 func(string, error, map[string]any)
	providerMaterializers       map[sm.ContextProvider]ProviderMaterializer
	economics                   *contextEconomicsStore
	forkTelemetry               *contextForkTelemetryStore
}

func Open(ctx context.Context, o Options) (*Service, error) {
	if o.Layout.MaterializationsDir == "" || o.Layout.MaterializationQuarantine == "" || o.Layout.StateDir == "" {
		return nil, fmt.Errorf("explicit private context layout is required")
	}
	if o.Now == nil {
		o.Now = func() time.Time { return time.Now().UTC() }
	}
	for _, p := range []string{o.Layout.StateDir, o.Layout.MaterializationsDir, o.Layout.MaterializationQuarantine} {
		if !filepath.IsAbs(p) {
			return nil, fmt.Errorf("context layout must be absolute")
		}
		if err := ensurePrivateDirectory(p); err != nil {
			return nil, err
		}
	}
	o.Policy.BuilderAllowlist.Paths = append([]string(nil), o.Policy.BuilderAllowlist.Paths...)
	providers := make(map[string]sm.ProviderHostFactory, len(o.Providers))
	for key, f := range o.Providers {
		if f == nil || f.Provider() != key {
			return nil, fmt.Errorf("context provider factory identity mismatch")
		}
		providers[key] = f
	}
	m := &Service{providers: providers, validation: o.Validation, config: o.Config, retentionConfig: o.RetentionConfig, localPolicy: cloneLocalPolicy(o.Policy), layout: o.Layout, materializationAuthority: o.Authority, references: o.References, builders: o.Builders, builderGateways: o.BuilderGateways, now: o.Now, operatorLog: o.OperatorLog, providerMaterializers: o.ProviderMaterializers, materializationRecords: newMaterializationStore(o.Layout.MaterializationsDir, o.Layout.MaterializationQuarantine), economics: newContextEconomicsStore(o.Layout.StateDir), forkTelemetry: newContextForkTelemetryStore(o.Layout.StateDir)}
	if o.HostCodec != nil {
		m.materializationRecords.hostCodec = o.HostCodec
	}
	if err := m.loadAndReconcileMaterializationsLocked(ctx); err != nil {
		return nil, err
	}
	return m, nil
}
func (m *Service) Reconcile(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadAndReconcileMaterializationsLocked(ctx)
}
func (m *Service) logOperatorError(s string, e error, f map[string]any) {
	if m.operatorLog != nil {
		m.operatorLog(s, e, f)
	}
}
func (m *Service) logOperatorInfo(s string, f map[string]any) { m.logOperatorError(s, nil, f) }
func (m *Service) requireBuilderConfinementCapabilityLocked(ctx context.Context, r *sm.ContextRecord) error {
	if r == nil {
		return fmt.Errorf("context facts unavailable")
	}
	if !r.RequiresConfinement {
		return nil
	}
	if m.builders == nil {
		return fmt.Errorf("builder confinement supplier unavailable")
	}
	return m.builders.RequireConfinement(ctx, cloneContext(r))
}
func (m *Service) closeBuilderGatewayLocked(ctx context.Context, id string) (BuilderGatewayFinalization, error) {
	if m.builderGateways != nil {
		return m.finalizeOwnedBuilderGatewayLocked(ctx, id)
	}
	r := m.materializations[id]
	if r == nil {
		return BuilderGatewayFinalization{}, fmt.Errorf("materialization unavailable")
	}
	if m.materializationAuthority == nil {
		return BuilderGatewayFinalization{}, fmt.Errorf("context authority unavailable")
	}
	c, err := m.materializationAuthority.Context(ctx, id)
	if err != nil {
		return BuilderGatewayFinalization{}, err
	}
	if m.builders == nil {
		if c == nil || c.RequiresConfinement {
			return BuilderGatewayFinalization{}, fmt.Errorf("builder gateway supplier unavailable")
		}
		return BuilderGatewayFinalization{CtxID: id, Served: false}, nil
	}
	result, err := m.builders.Finalize(ctx, id)
	if err != nil {
		return BuilderGatewayFinalization{}, err
	}
	if result.CtxID != id || result.Served && (result.Attestation.CtxID != id || result.Attestation.Provider != r.Provider || result.Attestation.RunID != r.ActiveRunID) {
		return BuilderGatewayFinalization{}, fmt.Errorf("gateway finalization returned a foreign trace receipt")
	}
	return result, nil
}

// ContextError preserves the source refusal code and underlying cause without
// transporting transcript content or private provider handles.
type ContextError struct {
	Code               string
	Message            string
	Cause              error
	RetryNextCandidate bool
}

func (e *ContextError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}
func (e *ContextError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
func isContextErrorCode(e error, c string) bool {
	var x *ContextError
	return errors.As(e, &x) && x != nil && x.Code == c
}

var ErrMaterializationEvidenceNotFound = sm.ErrMaterializationEvidenceNotFound

func normalizeContextID[S ~string](s S) string {
	parts := strings.Split(strings.TrimSpace(string(s)), "-")
	if len(parts) != 3 {
		return strings.ToUpper(strings.TrimSpace(string(s)))
	}
	return strings.ToUpper(parts[0]) + "-" + parts[1] + "-" + strings.ToLower(parts[2])
}
func normalizeElementID[S ~string](s S) string {
	parts := strings.Split(strings.TrimSpace(string(s)), "-")
	if len(parts) != 3 {
		return strings.ToUpper(strings.TrimSpace(string(s)))
	}
	return strings.ToUpper(parts[0]) + "-" + parts[1] + "-" + strings.ToLower(parts[2])
}
func normalizeRole(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

var contextIDPattern = regexp.MustCompile(`^CTX-[0-9]{6}-[a-z0-9]{6}$`)
var elementIDPattern = regexp.MustCompile(`^(EPIC|STORY|TASK|BUG)-[0-9]{6}-[a-z0-9]{6}$`)

func validateContextID(s string) error {
	if !contextIDPattern.MatchString(s) {
		return fmt.Errorf("invalid context id %q", s)
	}
	return nil
}
func validateProvider(p sm.ContextProvider) error {
	if p != "codex" && p != "claude" {
		return fmt.Errorf("unsupported context provider %q", p)
	}
	return nil
}
func validateBinding(b sm.ContextBinding) error {
	if len(b.ProducerRole) > contextTextLimit || !regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`).MatchString(b.ProducerRole) {
		return fmt.Errorf("binding producer role required")
	}
	if b.AncestryDigest != "" && !materializationDigestPattern.MatchString(b.AncestryDigest) {
		return fmt.Errorf("ancestry_digest must be a sha256 hex digest")
	}
	if b.Scope == "board" && b.ElementID == "" {
		return nil
	}
	if b.Scope != "element" || !elementIDPattern.MatchString(normalizeElementID(b.ElementID)) {
		return fmt.Errorf("invalid context binding")
	}
	return nil
}

// This validates the service's required source facts, not the external
// authority's entire canonical registry schema.
func validateContextFacts(r *sm.ContextRecord) error {
	if r == nil {
		return fmt.Errorf("context facts required")
	}
	if err := validateContextID(r.ID); err != nil {
		return err
	}
	if err := validateProvider(r.Provider); err != nil {
		return err
	}
	if r.Revision < 1 {
		return fmt.Errorf("context revision required")
	}
	return validateBinding(r.Binding)
}
func cloneContext(r *sm.ContextRecord) *sm.ContextRecord {
	if r == nil {
		return nil
	}
	b, _ := json.Marshal(r)
	var out sm.ContextRecord
	_ = json.Unmarshal(b, &out)
	// Runtime authority facts are deliberately absent from the public wire.
	out.RequiresConfinement = r.RequiresConfinement
	return &out
}
func cloneHostRecord(r sm.ProviderHostRecord) sm.ProviderHostRecord {
	b, _ := json.Marshal(r)
	var out sm.ProviderHostRecord
	_ = json.Unmarshal(b, &out)
	return out
}
func hostPresent(r sm.ProviderHostRecord) bool {
	s := r.Snapshot
	count := 0
	for _, yes := range []bool{s.Codex != nil, s.CodexFork != nil, s.Claude != nil, s.ClaudeFork != nil, s.MuseMSP != nil, s.MusePTY != nil, s.MusePTYProvenance != nil} {
		if yes {
			count++
		}
	}
	if count != 1 {
		return false
	}
	switch r.Provider {
	case "codex":
		return s.Codex != nil && s.Kind == "codex" || s.CodexFork != nil && s.Kind == "codex-fork"
	case "claude":
		return s.Claude != nil && s.Kind == "claude" || s.ClaudeFork != nil && s.Kind == "claude-fork"
	}
	return false
}
func hostEmpty(r sm.ProviderHostRecord) bool {
	s := r.Snapshot
	return s.Kind == "" && s.Codex == nil && s.CodexFork == nil && s.Claude == nil && s.ClaudeFork == nil && s.MuseMSP == nil && s.MusePTY == nil && s.MusePTYProvenance == nil
}

func runStatusActive(s sm.RunEvidenceStatus) bool { return s == "queued" || s == "running" }
func materializationReadAuthority(ctx context.Context, a sm.ContextAuthority) (sm.ContextAuthority, error) {
	if a == nil {
		return nil, fmt.Errorf("context authority unavailable")
	}
	if s, ok := a.(sm.ContextReadSnapshotter); ok {
		return s.MaterializationReadSnapshot(ctx)
	}
	return &cachedAuthority{ContextAuthority: a, elements: map[string]cachedElement{}}, nil
}

type cachedElement struct {
	fact *sm.OwnerFact
	err  error
}
type cachedAuthority struct {
	sm.ContextAuthority
	elements map[string]cachedElement
}

func (a *cachedAuthority) Element(ctx context.Context, id string) (*sm.OwnerFact, error) {
	id = normalizeElementID(id)
	e, ok := a.elements[id]
	if !ok {
		e.fact, e.err = a.ContextAuthority.Element(ctx, id)
		if e.fact != nil {
			v := *e.fact
			e.fact = &v
		}
		a.elements[id] = e
	}
	if e.fact == nil {
		return nil, e.err
	}
	v := *e.fact
	return &v, e.err
}
func ensurePrivateDirectory(path string) error {
	if err := rejectSymlinkAncestors(path); err != nil {
		return err
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	return os.Chmod(path, 0700)
}
func rejectSymlinkAncestors(path string) error {
	p := filepath.Clean(path)
	for {
		i, e := os.Lstat(p)
		if e == nil && i.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in context path")
		}
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		q := filepath.Dir(p)
		if q == p {
			return nil
		}
		p = q
	}
}
func syncDirectory(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func writePrivateFile(path string, data []byte) error {
	if err := rejectSymlinkAncestors(path); err != nil {
		return err
	}
	if err := ensurePrivateDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".context-write-")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(data)
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Rename(tmp, path); e != nil {
		return e
	}
	return syncDirectory(filepath.Dir(path))
}

func (m *Service) logMessagef(f string, args ...any) {
	m.logOperatorError("context_telemetry", fmt.Errorf(f, args...), nil)
}

func (m *Service) QuarantinedMaterializationCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.quarantinedMaterializations
}

func cloneMaterialization(r *materializationRecord) *materializationRecord {
	if r == nil {
		return nil
	}
	c := *r
	c.ProviderHost = cloneHostRecord(r.ProviderHost)
	return &c
}
func (m *Service) saveMaterializationLocked(r *materializationRecord) error {
	if err := m.materializationRecords.save(r); err != nil {
		return err
	}
	if m.materializations != nil {
		m.materializations[r.CtxID] = r
	}
	return nil
}

func (m *Service) FinalizeBuilderGateway(ctx context.Context, id string) (BuilderGatewayFinalization, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil && m.builderGateways == nil {
		return BuilderGatewayFinalization{}, err
	}
	return m.closeBuilderGatewayLocked(ctx, normalizeContextID(id))
}

func cloneLocalPolicy(p sm.LocalPolicy) sm.LocalPolicy {
	c := p
	c.BuilderAllowlist.Paths = append([]string(nil), p.BuilderAllowlist.Paths...)
	if p.Clearance != nil {
		c.Clearance = make(map[string][]sm.ContextSecretClass, len(p.Clearance))
		for role, classes := range p.Clearance {
			c.Clearance[normalizeRole(role)] = append([]sm.ContextSecretClass(nil), classes...)
		}
	}
	return c
}
func (m *Service) localPolicySnapshot(ctx context.Context) (sm.LocalPolicy, error) {
	if m.config == nil {
		return cloneLocalPolicy(m.localPolicy), nil
	}
	s, err := m.config.Snapshot(ctx)
	if err != nil {
		return sm.LocalPolicy{}, err
	}
	return cloneLocalPolicy(s.LocalPolicy), nil
}
