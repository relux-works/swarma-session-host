//go:build unix

package hostdefaults

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/relux-works/swarma-session-host/musepty"
	"github.com/relux-works/swarma-session-host/sessionbudget"
	"github.com/relux-works/swarma-session-host/sessioncontext"
	"github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"github.com/relux-works/swarma-session-host/sessionrun"
)

// StandaloneOptions supplies real external authority/service ports. None is
// inferred from a namespace, body owner, process ID or shared operating user.
// Missing sources retain the corresponding services' fail-closed behavior.
type StandaloneOptions struct {
	Layout               core.Layout
	Config               FileConfig
	AttachSource         LegacyAttachSource
	BuilderShimCommand   string
	BuilderShimArgs      []string
	GoalDirectory        string
	BudgetDirectory      string
	GoalAuthority        sessiongoal.Authority
	LegacyGoalSource     sessiongoal.LegacySource
	GoalGate             core.GoalGate
	ContextAuthority     core.ContextAuthority
	ContextReferences    sessioncontext.References
	ContextBuilders      sessioncontext.BuilderLifecycle
	ContextValidation    core.ContextForkValidationSource
	ContextMaterializers map[core.ContextProvider]sessioncontext.ProviderMaterializer
	Signals              *core.ShutdownSignalWatch
}

// Standalone owns the concrete component composition and their joined lifetime.
// ControlServer retains authentication, recovery, self-check and shutdown.
type Standalone struct {
	Host            *core.SessionHost
	Goals           *sessiongoal.Service
	Contexts        *sessioncontext.Service
	Budget          *sessionbudget.Factory
	OwnerRuns       *sessionrun.Runtime
	Control         *core.ControlServer
	BuilderGateways *core.BuilderGatewayRuntime
	attachHTTP      http.Handler
	goalHTTP        http.Handler
	contextHTTP     http.Handler
	life            context.Context
	cancel          context.CancelFunc
}

// LayoutAt selects explicit local storage, not an authority or board binding.
// The controller journal has a separate state subtree so first construction
// never mistakes independent service records/logs for lost
// controller history. Original record/protocol bytes stay with their codecs.
func LayoutAt(directory string, namespace core.NamespaceKey) (core.Layout, error) {
	if !filepath.IsAbs(directory) || strings.TrimSpace(string(namespace)) == "" || strings.TrimSpace(string(namespace)) != string(namespace) {
		return core.Layout{}, fmt.Errorf("absolute state root and explicit namespace required")
	}
	state := filepath.Join(directory, "host")
	return core.Layout{Namespace: namespace, StateDir: state, SessionsDir: filepath.Join(state, "sessions"), QuarantineDir: filepath.Join(state, "quarantine"), MaterializationsDir: filepath.Join(directory, "materializations"), MaterializationQuarantine: filepath.Join(directory, "materialization-quarantine"), LogsDir: filepath.Join(directory, "logs"), OperatorLogFile: filepath.Join(directory, "logs", "operator.log"), AttachmentsDir: filepath.Join(directory, "attachments"), EndpointFile: filepath.Join(directory, "endpoint.json"), TokenFile: filepath.Join(directory, "control.token"), LockFile: filepath.Join(state, "manager.lock"), SocketPath: filepath.Join(directory, "control.sock"), EnforcementKeyFile: filepath.Join(directory, "enforcement.key"), BuilderEnforcementDir: filepath.Join(directory, "builder-enforcement"), NoticesDir: filepath.Join(directory, "notices")}, nil
}

// OpenStandalone binds the actual factories, durable services and captured
// renderer. It does not start a listener, native provider, identity registrar or
// model. BindHost/BindBudget precede ControlServer's Recover and all attachments.
func OpenStandalone(ctx context.Context, o StandaloneOptions) (_ *Standalone, retErr error) {
	if ctx == nil {
		return nil, fmt.Errorf("standalone lifetime context required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, p := range []string{o.GoalDirectory, o.BudgetDirectory, o.Layout.SocketPath, o.Layout.EndpointFile, o.Layout.TokenFile, o.Layout.OperatorLogFile} {
		if !filepath.IsAbs(p) {
			return nil, fmt.Errorf("absolute standalone storage and control paths required")
		}
	}
	if o.Config.Namespace != o.Layout.Namespace || o.Config.Namespace == "" {
		return nil, fmt.Errorf("configuration and layout namespace must match")
	}
	if o.BuilderShimCommand == "" && len(o.BuilderShimArgs) != 0 || o.BuilderShimCommand != "" && !filepath.IsAbs(o.BuilderShimCommand) {
		return nil, fmt.Errorf("explicit absolute builder shim command required for its arguments")
	}
	// Validate selected configuration before allocating durable runtime state.
	loaded, err := o.Config.Load(ctx)
	if err != nil {
		return nil, err
	}
	settings := loaded.Host
	config := composedFileConfig{o.Config}
	life, cancel := context.WithCancel(ctx)
	s := &Standalone{life: life, cancel: cancel}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.Close(context.Background()))
		}
	}()
	if o.BuilderShimCommand != "" {
		s.BuilderGateways, err = core.NewBuilderGatewayRuntime(core.BuilderGatewayRuntimeOptions{Layout: o.Layout, ShimCommand: o.BuilderShimCommand, ShimArgs: o.BuilderShimArgs})
		if err != nil {
			return nil, err
		}
	}
	s.Goals, err = sessiongoal.Open(o.GoalDirectory, o.GoalAuthority)
	if err != nil {
		return nil, err
	}
	store, err := sessionbudget.Open(o.BudgetDirectory)
	if err != nil {
		return nil, err
	}
	s.OwnerRuns = sessionrun.New(store, settings)
	s.Budget, err = sessionbudget.NewFactory(life, s.Goals, store, s.OwnerRuns.AccountingSource())
	if err != nil {
		return nil, err
	}
	if err = s.OwnerRuns.BindBudget(s.Budget); err != nil {
		return nil, err
	}
	codec := core.NewLegacyRecordCodec()
	notices, err := core.NewSessionNoticeStore(o.Layout)
	if err != nil {
		return nil, err
	}
	factories := []core.ProviderHostFactory{core.NewCodexHostFactory(o.Layout), core.NewClaudeHostFactoryWithGoalAckTimeout(o.Layout, settings.GoalAckTimeout), musepty.NewMusePTYHostFactory()}
	s.Host, err = core.NewHost(core.HostOptions{Layout: o.Layout, Config: config, Codec: codec, Goals: s.Goals, GoalBindings: s.OwnerRuns, Gate: o.GoalGate, Contexts: o.ContextAuthority, Notices: notices, Factories: factories, OwnerRuns: s.OwnerRuns, BuilderGateways: s.BuilderGateways})
	if err != nil {
		return nil, err
	}
	if err = s.OwnerRuns.BindHost(s.Host); err != nil {
		return nil, err
	}
	providerMap := make(map[string]core.ProviderHostFactory, len(factories))
	for _, f := range factories {
		providerMap[f.Provider()] = f
	}
	hostCodec, err := NewMaterializationHostCodec(life, codec)
	if err != nil {
		return nil, err
	}
	s.Contexts, err = sessioncontext.Open(life, sessioncontext.Options{Layout: o.Layout, Config: config, RetentionConfig: o.Config, Policy: settings.LocalPolicy, Providers: providerMap, HostCodec: hostCodec, Authority: o.ContextAuthority, References: o.ContextReferences, Builders: o.ContextBuilders, BuilderGateways: s.BuilderGateways, Validation: o.ContextValidation, ProviderMaterializers: o.ContextMaterializers})
	if err != nil {
		return nil, err
	}
	if _, err = s.Contexts.ContextRetentionSettings(life); err != nil {
		return nil, err
	}
	s.contextHTTP, err = sessioncontext.NewHTTPHandler(s.Contexts)
	if err != nil {
		return nil, err
	}
	physical, err := NewGoalRuntime(s.Host, o.GoalGate)
	if err != nil {
		return nil, err
	}
	proof := sessiongoal.LegacyProof{Source: o.LegacyGoalSource}
	s.goalHTTP, err = sessiongoal.NewProtocol8Handler(s.Goals, physical, proof)
	if err != nil {
		return nil, err
	}
	s.attachHTTP, err = NewLegacyAttachHandler(s.Host, o.AttachSource)
	if err != nil {
		return nil, err
	}
	// Supplemental handlers are mounted inside core's authenticated listener;
	// neither opens a second listener or bypasses draining/startup self-check.
	handler, err := s.controlHandler(func() core.ControlStatus { return s.Control.Status() })
	if err != nil {
		return nil, err
	}
	s.Control, err = core.NewControlServer(s.Host, func(*core.ControlServer) http.Handler { return handler }, core.WithControlServerSignals(o.Signals), core.WithControlStatusDetails(func() core.ControlStatusDetails {
		// Read the actual catalog and tracked quarantine inventory. No legacy
		// vocabulary is advertised until its real enforcing validator is bound.
		details := core.ControlStatusDetails{MaterializationCount: len(s.Contexts.ListMaterializations()), QuarantinedMaterializationCount: s.Contexts.QuarantinedMaterializationCount()}
		if descriptor, err := proof.ReflectionContract(); err == nil {
			value := core.GoalReflectionContract(descriptor)
			details.GoalReflectionContract = &value
		}
		return details
	}))
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Standalone) controlHandler(status func() core.ControlStatus) (http.Handler, error) {
	physical, err := core.NewProtocol8Handler(s.Host, status, nil)
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/contexts/") {
			s.contextHTTP.ServeHTTP(w, r)
			return
		}
		switch r.URL.Path {
		case "/v1/sessions/attach":
			if r.Method == http.MethodPost {
				s.attachHTTP.ServeHTTP(w, r)
				return
			}
			physical.ServeHTTP(w, r)
		case "/v1/goals/apply", "/v1/goals/rollback", "/v1/owners/reconcile", "/v1/owners/activation/begin", "/v1/owners/activation/end":
			s.goalHTTP.ServeHTTP(w, r)
		default:
			physical.ServeHTTP(w, r)
		}
	}), nil
}

func (s *Standalone) Serve(ctx context.Context) (retErr error) {
	if ctx == nil {
		return fmt.Errorf("serving context required")
	}
	serve, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.life, cancel)
	defer cancel()
	defer stop()
	defer func() { retErr = errors.Join(retErr, s.Close(context.Background())) }()
	return s.Control.Serve(serve)
}
func (s *Standalone) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var err error
	if s.Host != nil {
		// Core owns gateway finalization before provider observation retirement,
		// including the ControlServer.Serve deferred close. Read its retained
		// result instead of consuming a second, now-empty runtime close.
		s.Host.Close()
		err = errors.Join(err, s.Host.BuilderGatewayCloseError())
	} else if s.BuilderGateways != nil {
		// Partial construction has not handed the runtime to a host yet.
		err = errors.Join(err, s.BuilderGateways.Close())
	}
	if s.Budget != nil {
		err = errors.Join(err, s.Budget.Close(ctx))
	}
	if s.cancel != nil {
		s.cancel()
	}
	return err
}
