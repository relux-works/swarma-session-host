package sessionhost

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"time"

	"github.com/relux-works/swarma-session-host/internal/runner"
)

var errOrdinaryWireUnavailable = errors.New("ordinary_wire_unavailable")

// This configuration comes from the selected composition, never a request.
// Its executable identity is a current physical observation, not installation
// qualification, ordinary policy admission or native initialization evidence.
type ordinaryLauncherConfig struct {
	binary, sha256 string
	environment    []string
}

// ordinaryWireManager consumes the existing selected method and its complete
// published body. It introduces no listener, alternate wire format or callback
// admission. No method on this preparation owner starts a process or implements
// managerOperationAdmission. The public resource-only guard remains closed.
type ordinaryWireManager struct {
	owner       *preChildManager
	launcher    *launcherExecutable
	environment protocolChildEnvironment
	mu          sync.Mutex
	closed      bool
	operation   *ordinaryWireOperation
	done        chan struct{}
}

type ordinaryWireOperation struct {
	manager *ordinaryWireManager
	claim   *preChildOperation
	ready   bool // guarded by manager.mu; not ordinary runtime admission
}

func openOrdinaryWireManager(parent context.Context, expires time.Time, records string, config ordinaryLauncherConfig) (*ordinaryWireManager, error) {
	env, err := parseProtocolChildEnvironment(config.environment)
	if err != nil {
		return nil, errOrdinaryWireUnavailable
	}
	o, err := openPreChildManager(parent, expires, records)
	if err != nil {
		return nil, errOrdinaryWireUnavailable
	}
	l, err := openLauncherExecutable(config.binary, config.sha256)
	if err != nil || o.ctx.Err() != nil {
		if l != nil {
			l.close()
		}
		_ = o.close(context.Background())
		return nil, errOrdinaryWireUnavailable
	}
	m := &ordinaryWireManager{owner: o, launcher: l, environment: env, done: make(chan struct{})}
	go func() {
		<-o.ctx.Done()
		m.mu.Lock()
		m.closed = true
		l.close()
		m.mu.Unlock()
		<-o.done
		close(m.done)
	}()
	return m, nil
}

// dispatch validates complete launch/resume bodies through the existing schema
// and actual record owner before observing the fixed executable. Unsupported
// semantics refuse; a failed valid preparation retains its consumption. There
// is no method fallback, request executable, environment override or auto-retry.
func (m *ordinaryWireManager) dispatch(request context.Context, method string, body []byte) (*ordinaryWireOperation, error) {
	if m == nil || request == nil || request.Err() != nil {
		return nil, errOrdinaryWireUnavailable
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.owner.ctx.Err() != nil {
		return nil, errOrdinaryWireUnavailable
	}
	claim, err := m.owner.prepare(request, method, body)
	if err != nil {
		return nil, errOrdinaryWireUnavailable
	}
	if op := m.operation; op != nil {
		if op.claim != claim || !op.ready {
			return nil, errOrdinaryWireUnavailable
		}
		return op, nil
	}
	op := &ordinaryWireOperation{manager: m, claim: claim}
	m.operation = op // consume before another failable preparation conjunct
	if m.launcher.recheck(request) != nil || m.owner.ctx.Err() != nil {
		return nil, errOrdinaryWireUnavailable
	}
	op.ready = true
	return op, nil
}

// takeLauncherPlan consumes the existing actual pre-child claim before the last
// current executable observation. The returned fixed plan is copied data, never
// permission to invoke runner.Start or proof that a native process initialized.
// The later admitted runner path must keep this physical owner through start,
// perform its own final checks, then retain the actual controller correlation.
func (op *ordinaryWireOperation) takeLauncherPlan(request context.Context, manager *ordinaryWireManager) (runner.Plan, error) {
	if op == nil || manager == nil || op.manager != manager {
		return runner.Plan{}, errOrdinaryWireUnavailable
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.operation != op || !op.ready || manager.closed {
		return runner.Plan{}, errOrdinaryWireUnavailable
	}
	inv, err := op.claim.takeInvocation(request, manager.owner)
	if err != nil || manager.launcher.recheck(request) != nil || manager.owner.ctx.Err() != nil {
		return runner.Plan{}, errOrdinaryWireUnavailable
	}
	return runner.Plan{Binary: manager.launcher.path, Args: inv.args, Cwd: inv.workspace,
		Env: append([]string(nil), manager.environment.entries...)}, nil
}

// takeRunnerConfig connects the actual once-owned operation to the runner's
// existing configuration, including its fixed stderr transport. The endpoint
// and timeout are selected by composition; Execution comes only from the
// retained complete request. This consumes the same claim as takeLauncherPlan,
// and remains data, not final policy admission or permission to start a child.
func (op *ordinaryWireOperation) takeRunnerConfig(request context.Context, manager *ordinaryWireManager, socket string, timeout time.Duration) (runner.Config, error) {
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket || len(socket) > 100 ||
		!protocolChildValue(socket) || timeout <= 0 || timeout > time.Minute {
		return runner.Config{}, errOrdinaryWireUnavailable
	}
	plan, err := op.takeLauncherPlan(request, manager)
	if err != nil {
		return runner.Config{}, err
	}
	return runner.Config{Socket: socket, Execution: op.claim.id, Plan: plan,
		IOTimeout: timeout, StopSignal: "none", Diagnostics: runner.ProtocolChildDiagnostics}, nil
}

// A canceled waiter cannot abandon either the executable or member handles.
func (m *ordinaryWireManager) close(wait context.Context) error {
	if m == nil || wait == nil {
		return errOrdinaryWireUnavailable
	}
	m.owner.cancel()
	select {
	case <-m.done:
		return nil
	default:
	}
	select {
	case <-m.done:
		return nil
	case <-wait.Done():
		return wait.Err()
	}
}
