package sessioncontext

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

type builderLaunchFactory struct {
	provider string
	launch   func(context.Context, sm.ProviderLaunchRequest) (sm.RunnerHost, error)
}

func (f builderLaunchFactory) Provider() string { return f.provider }
func (f builderLaunchFactory) Launch(ctx context.Context, q sm.ProviderLaunchRequest) (sm.RunnerHost, error) {
	return f.launch(ctx, q)
}
func (f builderLaunchFactory) Restore(context.Context, sm.ProviderHostRecord) (sm.RunnerHost, error) {
	return nil, fmt.Errorf("not a restore fixture")
}

type builderLaunchHost struct {
	stopped, observed int
	launched          *bool
	reportMode        string
}

func (h *builderLaunchHost) State(context.Context) (sm.ProviderHostState, error) {
	if h.reportMode == "not-running" {
		return sm.ProviderHostStopped, nil
	}
	return sm.ProviderHostRunning, nil
}
func (h *builderLaunchHost) Snapshot() (sm.RunnerSnapshot, error) { return sm.RunnerSnapshot{}, nil }
func (h *builderLaunchHost) Stop(context.Context) error           { h.stopped++; return nil }
func (h *builderLaunchHost) BuilderToolObservation(context.Context) (sm.BuilderToolObservation, error) {
	h.observed++
	if !*h.launched {
		return sm.BuilderToolObservation{}, fmt.Errorf("native observer used before launch completed")
	}
	// Explicit provider-source fixture: actual native readers are independently
	// exercised by TestBuilderGatewayReconcilesActualNativeTranscript.
	return sm.BuilderToolObservation{Reported: true, GatewayCalls: 1}, nil
}

// Use a separate non-embedding type so it genuinely lacks the observation port.
func (h *builderLaunchHost) BuilderIngestionConfinement() (sm.BuilderConfinementResult, error) {
	roots := []string{"docs"}
	mechanism := "actual-fixture-confinement"
	if h.reportMode == "wide-scope" {
		roots = []string{"."}
	}
	if h.reportMode == "narrow-scope" {
		roots = []string{"docs/guide.md"}
	}
	if h.reportMode == "empty-mechanism" {
		mechanism = ""
	}
	return sm.BuilderConfinementResult{Mechanism: mechanism, EnforcedRoots: roots}, nil
}
func (h *builderLaunchHost) BuilderToolIsolation() (sm.BuilderToolIsolationResult, error) {
	denied := []string{"shell", "network", "native-filesystem", "foreign-mcp", "ambient-settings"}
	if h.reportMode == "weak-isolation" {
		denied = nil
	}
	return sm.BuilderToolIsolationResult{Mechanism: "actual-fixture-tool-isolation", DeniedSurfaces: denied, GatewayTools: []string{"read", "list", "stat", "search"}}, nil
}

type builderUnsupportedHost struct{ stopped int }

func (h *builderUnsupportedHost) State(context.Context) (sm.ProviderHostState, error) {
	return sm.ProviderHostRunning, nil
}
func (h *builderUnsupportedHost) Snapshot() (sm.RunnerSnapshot, error) {
	return sm.RunnerSnapshot{}, nil
}
func (h *builderUnsupportedHost) Stop(context.Context) error { h.stopped++; return nil }

// This fixture supplies valid launch reports but deliberately NO native
// observation method, isolating the real final Bind refusal from report errors.
func (h *builderUnsupportedHost) BuilderIngestionConfinement() (sm.BuilderConfinementResult, error) {
	return (&builderLaunchHost{}).BuilderIngestionConfinement()
}
func (h *builderUnsupportedHost) BuilderToolIsolation() (sm.BuilderToolIsolationResult, error) {
	return (&builderLaunchHost{}).BuilderToolIsolation()
}

func newBuilderLaunchFixture(t *testing.T) (*Service, *fakeMaterializationAuthority, *sm.ContextRecord, *sm.BuilderGatewayRuntime, sm.ProviderLaunchRequest) {
	t.Helper()
	m, a, r, layout := seededLease(t)
	r.RequiresConfinement = true
	r.Kind = "project"
	r.BuilderProfile = "allowlisted-context-builder"
	r.Binding.ProducerRole = "context-builder"
	r.Lineage.ProducedByRunID = "RUN-boundary"
	r.Freshness.PathSpecs = []string{"docs"}
	a.contexts[r.ID] = cloneContext(r)
	m.materializations[r.ID].TargetRole = "context-builder"
	m.localPolicy.BuilderAllowlist = sm.NewContextBuilderAllowlist([]string{"docs"})
	m.builders = builderReceiptFixture{}
	dir, err := os.MkdirTemp("/tmp", "pypx-r7-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	layout.SocketPath = filepath.Join(dir, "host.sock")
	m.layout.SocketPath = layout.SocketPath
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := sm.NewBuilderGatewayRuntime(sm.BuilderGatewayRuntimeOptions{Layout: layout, ShimCommand: exe, Now: m.now})
	if err != nil {
		t.Fatal(err)
	}
	m.builderGateways = runtime
	t.Cleanup(func() { _ = runtime.Close() })
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs/guide.md"), []byte("launch fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	q := sm.ProviderLaunchRequest{SessionID: "SES-r7", Owner: sm.OwnerKey("run/RUN-BOUNDARY"), Launch: sm.LaunchSpec{Cwd: root, Codex: &sm.CodexPreparedLaunch{Plan: sm.PrimarySessionLaunchPlan{ProjectDir: root}}}}
	return m, a, r, runtime, q
}

// The real Unix/MCP service serves the exact launch endpoint before the
// provider factory runs; the returned observer binds on that same runtime and
// only closed, reconciled data becomes readable. This is not sandbox proof.
func TestBuilderLaunchStartsBeforeFactoryAndBindsSameRuntime(t *testing.T) {
	m, a, r, runtime, q := newBuilderLaunchFixture(t)
	before := cloneMaterialization(m.materializations[r.ID])
	launched := false
	host := &builderLaunchHost{launched: &launched}
	factory := builderLaunchFactory{provider: "codex", launch: func(ctx context.Context, got sm.ProviderLaunchRequest) (sm.RunnerHost, error) {
		c := got.BuilderConfinement
		if c == nil || c.CtxID != r.ID || c.RunID != "RUN-boundary" || string(c.Owner) != m.materializations[r.ID].TargetElementID || c.Owner == q.Owner || c.Provider != "codex" || !reflect.DeepEqual(c.RequestedPathSpecs, []string{"docs"}) || !reflect.DeepEqual(c.Allowlist.Paths, []string{"docs"}) {
			t.Fatal("actual catalog/claim facts dropped")
		}
		if c.IsolationConfigDir != filepath.Join(m.layout.StateDir, "builder-gateway-config") {
			t.Fatalf("config outside private state: %q", c.IsolationConfigDir)
		}
		if host.observed != 0 {
			t.Fatal("provider observer used before actual launch")
		}
		client := sm.NewBuilderGatewayClient(c.Gateway.SocketPath, c.Gateway.Token)
		defer client.Disconnect()
		driveOneBuilderRead(t, client)
		launched = true
		return host, nil
	}}
	got, err := m.LaunchBuilderHost(context.Background(), builderLaunchView(m, r, q), factory, q)
	if err != nil || got != host {
		t.Fatalf("admitted current builder refused: %T %v", got, err)
	}
	if !reflect.DeepEqual(before, m.materializations[r.ID]) || len(a.publications) != 0 {
		t.Fatal("launch preparation changed catalog or published authority")
	}
	if _, _, err := runtime.BuilderAccessTrace(r.ID); err == nil {
		t.Fatal("open gateway exposed terminal evidence")
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("same runtime host Close: %v", err)
	}
	if host.observed != 1 || host.stopped != 0 {
		t.Fatalf("wrong host lifecycle observation=%d stop=%d", host.observed, host.stopped)
	}
	_, att, err := runtime.BuilderAccessTrace(r.ID)
	if err != nil || att.RunID != "RUN-BOUNDARY" || att.CtxID != r.ID || att.EventCount != 1 {
		t.Fatalf("same-runtime terminal data: %+v %v", att, err)
	}
}

// Invalid source facts refuse before gateway/provider/catalog effects. The
// admitted neighbor above prevents an always-refusing implementation passing.
func TestBuilderLaunchRefusesUnadmittedFactsBeforeEffects(t *testing.T) {
	for _, mode := range []string{"target", "missing-target-fact", "missing-run-fact", "owner-slot", "session-slot", "view-run", "provider", "unleased", "stale-run", "missing-run", "unknown-run", "inactive-run", "foreign-run-target", "foreign-run-context", "missing-context", "foreign-context", "role", "missing-claim", "wrong-kind", "wrong-profile", "wrong-producer", "produced-run", "state", "denied", "closed-target", "capability", "no-runtime", "allowlist", "relative-project", "wrong-project", "supplied-confinement", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			m, a, r, runtime, q := newBuilderLaunchFixture(t)
			provider := "codex"
			view := builderLaunchView(m, r, q)
			ctx := context.Background()
			switch mode {
			case "target":
				view.TargetElementID = "TASK-260729-cccccc"
			case "missing-target-fact":
				view.TargetElementID = ""
			case "missing-run-fact":
				view.ActiveRunID = ""
			case "owner-slot":
				view.Owner = "run/RUN-FOREIGN"
			case "session-slot":
				view.SessionID = "SES-foreign"
			case "view-run":
				view.ActiveRunID = "RUN-foreign"
			case "provider":
				provider = "claude"
			case "unleased":
				m.materializations[r.ID].State = MaterializationDetached
			case "stale-run":
				m.materializations[r.ID].ActiveRunID = "RUN-foreign"
			case "missing-run":
				delete(a.runs, "RUN-boundary")
			case "unknown-run":
				a.runs["RUN-boundary"].Status = "unknown"
			case "inactive-run":
				a.runs["RUN-boundary"].Active = false
				a.runs["RUN-boundary"].Status = "failed"
			case "foreign-run-target":
				a.runs["RUN-boundary"].Owner = "TASK-260729-cccccc"
			case "foreign-run-context":
				a.runs["RUN-boundary"].ContextID = "CTX-260729-xxxxxx"
			case "missing-context":
				delete(a.contexts, r.ID)
			case "foreign-context":
				a.contexts[r.ID].ID = "CTX-260729-xxxxxx"
			case "role":
				m.materializations[r.ID].TargetRole = "developer"
			case "missing-claim":
				a.contexts[r.ID].RequiresConfinement = false
			case "wrong-kind":
				a.contexts[r.ID].Kind = "task"
			case "wrong-profile":
				a.contexts[r.ID].BuilderProfile = "unknown"
			case "wrong-producer":
				a.contexts[r.ID].Binding.ProducerRole = "developer"
			case "produced-run":
				a.contexts[r.ID].Lineage.ProducedByRunID = "RUN-foreign"
			case "state":
				a.contexts[r.ID].State = "retired"
			case "denied":
				a.deny = true
			case "closed-target":
				a.elements[m.materializations[r.ID].TargetElementID].Closed = true
			case "capability":
				m.builders = nil
			case "no-runtime":
				m.builderGateways = nil
			case "allowlist":
				m.localPolicy.BuilderAllowlist = sm.ContextBuilderAllowlist{}
			case "relative-project":
				q.Launch.Cwd = "relative"
				q.Launch.Codex.Plan.ProjectDir = "relative"
			case "wrong-project":
				q.Launch.Cwd = t.TempDir()
			case "supplied-confinement":
				q.BuilderConfinement = &sm.BuilderConfinementRequest{}
			case "canceled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			before := cloneMaterialization(m.materializations[r.ID])
			beforeDisk := catalogBytes(t, m.layout, r.ID)
			calls := 0
			factory := builderLaunchFactory{provider: provider, launch: func(context.Context, sm.ProviderLaunchRequest) (sm.RunnerHost, error) { calls++; return nil, nil }}
			got, err := m.LaunchBuilderHost(ctx, view, factory, q)
			if err == nil || got != nil || calls != 0 {
				t.Fatalf("unadmitted source reached provider: %T %v calls=%d", got, err, calls)
			}
			expected := map[string]string{
				"target": "provider or target differs", "missing-target-fact": "target differs", "missing-run-fact": "source run differs", "owner-slot": "current host session view", "session-slot": "current host session view", "view-run": "source run differs", "provider": "current host session view", "stale-run": "source run differs", "unleased": "no current writer lease",
				"unknown-run": "writer run is missing", "inactive-run": "writer run is missing", "foreign-run-target": "writer run is missing", "foreign-run-context": "writer run is missing",
				"foreign-context": "context id does not match", "role": "builder role", "missing-claim": "confinement fact disagrees", "wrong-kind": "confinement fact disagrees", "wrong-profile": "confinement fact disagrees", "wrong-producer": "confinement fact disagrees", "produced-run": "produced run differs", "state": "confinement fact disagrees",
				"denied": "context_secret_class_denied", "closed-target": "target is inactive", "capability": "confinement supplier unavailable", "no-runtime": "gateway runtime unavailable", "allowlist": "read allowlist unavailable", "relative-project": "absolute project directory", "wrong-project": "absolute project directory", "supplied-confinement": "caller supplied builder confinement",
			}
			if literal := expected[mode]; literal != "" && !strings.Contains(err.Error(), literal) {
				t.Fatalf("wrong refusal for %s: %v, want %s", mode, err, literal)
			}
			if (mode == "missing-run" || mode == "missing-context") && !errors.Is(err, sm.ErrMaterializationEvidenceNotFound) {
				t.Fatalf("missing source identity lost: %v", err)
			}
			if mode == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation cause lost: %v", err)
			}
			if !bytes.Equal(beforeDisk, catalogBytes(t, m.layout, r.ID)) {
				t.Fatal("pre-launch refusal changed durable catalog")
			}
			if _, err := os.Lstat(sm.BuilderAccessTracePath(m.layout, r.ID)); !os.IsNotExist(err) {
				t.Fatalf("pre-launch refusal created trace: %v", err)
			}
			if !reflect.DeepEqual(before, m.materializations[r.ID]) || len(a.publications) != 0 {
				t.Fatal("refusal changed catalog or canonical publication")
			}
			if _, _, err := runtime.BuilderAccessTrace(r.ID); err == nil {
				t.Fatal("pre-launch refusal minted terminal trace")
			}
		})
	}
}

// Launch failure or a changed lease/source/policy between launch and bind stops
// only that returned runner and closes the gateway; it cannot become success on
// retry. Actual native-source mismatch tests cover the later observation gate.
func TestBuilderLaunchFailureClosesAndRevalidatesBeforeBind(t *testing.T) {
	for _, mode := range []string{"launch-error", "partial-host-error", "nil-host", "missing-observer", "changed-lease", "changed-lease-epoch", "changed-revision", "changed-policy", "revoked", "canceled", "wide-scope", "narrow-scope", "empty-mechanism", "weak-isolation", "not-running", "mutated-request"} {
		t.Run(mode, func(t *testing.T) {
			m, a, r, runtime, q := newBuilderLaunchFixture(t)
			launched := false
			view := builderLaunchView(m, r, q)
			host := &builderLaunchHost{launched: &launched}
			unsupported := &builderUnsupportedHost{}
			cause := errors.New("actual factory failure")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var endpoint sm.BuilderGatewayEndpoint
			factory := builderLaunchFactory{provider: "codex", launch: func(_ context.Context, request sm.ProviderLaunchRequest) (sm.RunnerHost, error) {
				endpoint = request.BuilderConfinement.Gateway
				client := sm.NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
				defer client.Disconnect()
				driveOneBuilderRead(t, client)
				launched = true
				switch mode {
				case "launch-error":
					return nil, cause
				case "partial-host-error":
					return host, cause
				case "nil-host":
					return nil, nil
				case "missing-observer":
					return unsupported, nil
				case "changed-lease":
					m.materializations[r.ID].ActiveRunID = "RUN-other"
				case "changed-lease-epoch":
					m.materializations[r.ID].LeaseAcquiredAt = m.materializations[r.ID].LeaseAcquiredAt.Add(time.Second)
				case "mutated-request":
					request.BuilderConfinement.Allowlist.Paths[0] = "."
					host.reportMode = "wide-scope"
				case "changed-revision":
					a.contexts[r.ID].Revision++
				case "changed-policy":
					m.localPolicy.BuilderAllowlist = sm.NewContextBuilderAllowlist([]string{"docs/guide.md"})
				case "revoked":
					a.deny = true
				case "canceled":
					cancel()
				default:
					host.reportMode = mode
				}
				return host, nil
			}}
			got, err := m.LaunchBuilderHost(ctx, view, factory, q)
			if err == nil || got != nil {
				t.Fatalf("failed transaction accepted runner: %T %v", got, err)
			}
			if (mode == "launch-error" || mode == "partial-host-error") && !errors.Is(err, cause) {
				t.Fatalf("original launch cause lost: %v", err)
			}
			if mode == "missing-observer" {
				var typed *sm.PublicError
				if !errors.As(err, &typed) || typed.Code != sm.ContextErrorBuilderToolIsolationUnavailable || !strings.Contains(err.Error(), "cannot observe its native tool stream") {
					t.Fatalf("missing native observer failed at wrong gate: %v", err)
				}
			}
			stop := host.stopped + unsupported.stopped
			want := 1
			if mode == "launch-error" || mode == "nil-host" {
				want = 0
			}
			wantObserved := 0
			if stop != want || host.observed != wantObserved {
				t.Fatalf("wrong cleanup/early observer: stop=%d want=%d observed=%d", stop, want, host.observed)
			}
			client := sm.NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
			defer client.Disconnect()
			if err := client.Connect(); err == nil {
				t.Fatal("failed launch left real gateway serving")
			}
			if _, _, err := runtime.BuilderAccessTrace(r.ID); err == nil {
				t.Fatal("failed launch exposed usable evidence")
			}
			if _, err := runtime.FinalizeBuilderGateway(context.Background(), r.ID); err == nil {
				t.Fatal("failed launch became successful finalization retry")
			}
			if len(a.publications) != 0 {
				t.Fatal("failure published authority")
			}
		})
	}
}

// Goal-less ordinary launches preserve the untouched request and never require
// context/builder capability; caller-supplied confinement cannot use that bypass.
func TestBuilderLaunchLeavesOrdinaryFactoryPathAlone(t *testing.T) {
	m, a, _, _, q := newBuilderLaunchFixture(t)
	a.err = errors.New("authority unavailable")
	calls := 0
	host := &builderUnsupportedHost{}
	factory := builderLaunchFactory{provider: "codex", launch: func(_ context.Context, got sm.ProviderLaunchRequest) (sm.RunnerHost, error) {
		calls++
		if !reflect.DeepEqual(got, q) {
			t.Fatal("ordinary launch altered")
		}
		return host, nil
	}}
	got, err := m.LaunchBuilderHost(context.Background(), sm.SessionView{}, factory, q)
	if err != nil || got != host || calls != 1 {
		t.Fatalf("ordinary launch refused: %T %v calls=%d", got, err, calls)
	}
	q.BuilderConfinement = &sm.BuilderConfinementRequest{}
	if _, err := m.LaunchBuilderHost(context.Background(), sm.SessionView{}, factory, q); err == nil || calls != 1 {
		t.Fatal("ordinary path admitted unowned confinement")
	}
}

// Original launch-root cases retain the wire inputs; the bad-JSON case now
// belongs to typed-payload decoding before this helper, not provider execution.
func TestBuilderLaunchProjectDirRefusesAnUnanchorableLaunch(t *testing.T) {
	tests := []struct{ name, launch string }{
		{"no plan", `{}`}, {"no project dir", `{"plan":{}}`}, {"relative project dir", `{"plan":{"project_dir":"./repo"}}`}, {"undecodable", `{"plan":`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var wire struct {
				Plan sm.PrimarySessionLaunchPlan `json:"plan"`
			}
			err := json.Unmarshal([]byte(test.launch), &wire)
			if err == nil {
				_, err = builderLaunchProjectDir(sm.LaunchSpec{Cwd: wire.Plan.ProjectDir, Codex: &sm.CodexPreparedLaunch{Plan: wire.Plan}}, "codex")
			}
			if err == nil {
				t.Fatal("an unanchorable launch produced a repository root")
			}
		})
	}
	root, err := builderLaunchProjectDir(sm.LaunchSpec{Cwd: "/tmp/repo", Codex: &sm.CodexPreparedLaunch{Plan: sm.PrimarySessionLaunchPlan{ProjectDir: "/tmp/repo"}}}, "codex")
	if err != nil || root != "/tmp/repo" {
		t.Fatalf("anchorable launch = %q err=%v", root, err)
	}
}

// A ready base is consumed, not produced: original source excludes it from
// builder sealing even for a context-builder role. Current authorization and
// lease facts still gate the ordinary factory; no confinement is synthesized.
func TestBuilderLaunchReadyContextDoesNotBecomeBuilderProduction(t *testing.T) {
	m, a, r, _, q := newBuilderLaunchFixture(t)
	a.contexts[r.ID].State = "ready"
	a.contexts[r.ID].RequiresConfinement = false
	a.contexts[r.ID].Lineage.ProducedByRunID = "RUN-original-producer"
	m.localPolicy.Clearance = map[string][]sm.ContextSecretClass{"context-builder": {"restricted"}}
	m.builders = nil
	m.builderGateways = nil
	calls := 0
	host := &builderUnsupportedHost{}
	factory := builderLaunchFactory{provider: "codex", launch: func(_ context.Context, got sm.ProviderLaunchRequest) (sm.RunnerHost, error) {
		calls++
		if !reflect.DeepEqual(got, q) {
			t.Fatal("ready consumer acquired builder launch policy")
		}
		return host, nil
	}}
	got, err := m.LaunchBuilderHost(context.Background(), builderLaunchView(m, r, q), factory, q)
	if err != nil || got != host || calls != 1 {
		t.Fatalf("ready context consumption refused: %T %v calls=%d", got, err, calls)
	}
	q.BuilderConfinement = &sm.BuilderConfinementRequest{}
	if _, err := m.LaunchBuilderHost(context.Background(), builderLaunchView(m, r, q), factory, q); err == nil || !strings.Contains(err.Error(), "ordinary materialization") || calls != 1 {
		t.Fatalf("ready consumer admitted injected builder policy: %v", err)
	}
}

// Source facts here are explicit independent fixtures, not decoded from an
// opaque host owner key. Actual core view population remains core-owned.
func builderLaunchView(m *Service, r *sm.ContextRecord, q sm.ProviderLaunchRequest) sm.SessionView {
	c := m.materializations[r.ID]
	return sm.SessionView{CtxID: r.ID, TargetElementID: c.TargetElementID, ActiveRunID: c.ActiveRunID, SessionID: q.SessionID, Owner: q.Owner, Provider: string(c.Provider)}
}

// Original5721 run evidence has no Active flag: queued/running Status is the
// real fact. Do not require a synthetic extra Boolean from legacy suppliers.
func TestBuilderLaunchUsesOriginalRunStatusWithoutNewActiveFlag(t *testing.T) {
	for _, status := range []sm.RunEvidenceStatus{"queued", "running"} {
		t.Run(string(status), func(t *testing.T) {
			m, a, r, runtime, q := newBuilderLaunchFixture(t)
			a.runs["RUN-boundary"].Status = status
			a.runs["RUN-boundary"].Active = false
			launched := false
			host := &builderLaunchHost{launched: &launched}
			factory := builderLaunchFactory{provider: "codex", launch: func(_ context.Context, request sm.ProviderLaunchRequest) (sm.RunnerHost, error) {
				endpoint := request.BuilderConfinement.Gateway
				client := sm.NewBuilderGatewayClient(endpoint.SocketPath, endpoint.Token)
				defer client.Disconnect()
				driveOneBuilderRead(t, client)
				launched = true
				return host, nil
			}}
			got, err := m.LaunchBuilderHost(context.Background(), builderLaunchView(m, r, q), factory, q)
			if err != nil || got != host {
				t.Fatalf("original active run status refused without synthetic flag: %T %v", got, err)
			}
			if err := runtime.Close(); err != nil {
				t.Fatalf("original status lifecycle did not settle: %v", err)
			}
			if host.observed != 1 || host.stopped != 0 {
				t.Fatalf("wrong original status lifecycle: observed=%d stopped=%d", host.observed, host.stopped)
			}
		})
	}
}
