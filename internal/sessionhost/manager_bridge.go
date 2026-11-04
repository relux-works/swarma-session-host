package sessionhost

import (
	"context"
	"net"
	"sync"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/codexadapter"
)

// ManagerOptions selects resources for the continuing manager. These values are
// configuration, never evidence of member or broker admission. Open verifies the
// actual Host; Prepare independently reads the actual record and Core selection.
type ManagerOptions struct {
	StateDir, RunnerSocket, Execution, RecordDirectory string
	Workspace, NativeRelease, PermissionPolicy         string
	Selection                                          Selection
	IOTimeout, PollInterval                            time.Duration
	Expires                                            time.Time
	CoreEnvironment                                    []string
	Broker                                             NotificationBroker
}

// NotificationBroker is the optional trusted composition seam. Implementations
// own their provider's actual selection, enrollment and finite cleanup. Presence
// is not capability: Prepare must admit the provider before Host allocates a pair.
type NotificationBroker interface {
	Prepare(context.Context, ManagerOrigin) (PreparedNotificationBroker, error)
}

// ManagerOrigin is an advisory view of an actually retained operation. A plugin
// must use Recheck and its own independently observed provider origin; copying
// these values does not create a manager or provider proof.
type ManagerOrigin struct {
	Member, AgentSelector, Connection, ConfigurationRoot         string
	Runtime, Profile, ProfileLock, Operation, AdmissionReference string
	WorkflowReference, WorkflowSHA256                            string
	CallTimeoutMS                                                int
	Recheck                                                      func(context.Context) error
}

// NotificationReferences are opaque references from the provider's actual
// retained scope. In particular Connection need not equal Origin.Connection's
// public stable selector. Recheck must verify these against actual owned scope.
type NotificationReferences struct{ Member, Connection, Configuration, ProfileLock string }
type PreparedNotificationBroker interface {
	References() NotificationReferences
	Recheck(context.Context) error
	Bootstrap(context.Context, NotificationBootstrap) (NotificationCompletion, error)
	Close() error
}
type NotificationCompletion interface {
	Done() <-chan struct{}
	Err() error
	Close() error
}

// A real provider's enrolled view implements this interface at its live callback.
// Binding syntax and equality alone never substitute for Recheck.
type NotificationEnrollment interface {
	Binding() NotificationBinding
	AttachmentReference() string
	Lifetime() (context.Context, time.Time)
	Recheck() error
}
type NotificationBootstrap struct {
	MethodEndpoint      *net.UnixConn
	AttachmentReference string
	Binding             NotificationBinding
	CallTimeoutMS       int
	RecheckHost         func(context.Context) error
	OnEnrolled          func(context.Context, NotificationEnrollment) error
}

// The selected wire handler owns ordinary runtime launch/resume admission.
// It must retain the actual operation/controller correlation; neither public
// Options nor an expected request is an implementation of this boundary.
// The current resource-only Open deliberately supplies no such admission.
type managerOperationAdmission interface {
	checkManagerOperation(context.Context, *Host, string, []byte) error
}

type NotificationManager struct {
	owner       *continuingHostOwner
	records     *memberRecordSource
	env         []string
	broker      NotificationBroker
	mu          sync.Mutex
	attachments map[*ManagerAttachment]struct{}
	closed      bool
	read        managerCoreRead
	admission   managerOperationAdmission
}

func OpenNotificationManager(ctx context.Context, options ManagerOptions) (*NotificationManager, error) {
	cfg := Config{StateDir: options.StateDir, Socket: options.RunnerSocket, Execution: options.Execution, Selection: options.Selection, IOTimeout: options.IOTimeout, PollInterval: options.PollInterval, NoticeMode: WorkReference, ProtocolName: "codex"}
	cfg.ProtocolFactory = func(saved []byte) (ap.Driver, uint64, error) {
		return codexadapter.OpenDriverWithOptions(ap.Config{NativeRelease: options.NativeRelease, Lifecycle: ap.Create, Selection: ap.Selection{Provider: options.Selection.Vendor, Model: options.Selection.Model, Effort: options.Selection.Reasoning}, Workspace: options.Workspace}, codexadapter.DriverOptions{NoticeMode: WorkReference, PermissionPolicy: options.PermissionPolicy}, saved)
	}
	owner, err := openContinuingHostOwner(ctx, options.Expires, cfg)
	if err != nil {
		return nil, err
	}
	records, err := owner.openMemberRecordSource(options.RecordDirectory)
	if err != nil {
		_ = owner.close(context.Background())
		return nil, err
	}
	return &NotificationManager{owner: owner, records: records, env: append([]string(nil), options.CoreEnvironment...), broker: options.Broker, attachments: make(map[*ManagerAttachment]struct{}), read: readManagerCore}, nil
}
func (m *NotificationManager) Snapshot(ctx context.Context) (State, error) {
	if m == nil || m.owner == nil {
		return State{}, errHostOwnerUnavailable
	}
	return m.owner.snapshot(ctx)
}

// Prepare is reserved for the same selected manager admission path. It refuses
// until its actual wire handler supplies ordinary operation admission. It introduces no
// wire endpoint, registry or provider fallback. Generic Host remains a2a-independent.
func (m *NotificationManager) Prepare(ctx context.Context, kind string, data []byte) (*ManagerAttachment, error) {
	if m == nil || ctx == nil || ctx.Err() != nil {
		return nil, errMemberRecordUnavailable
	}
	// Callbacks outlive this invocation; never retain a caller-owned byte slice.
	data = append([]byte(nil), data...)
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed || m.records == nil || m.broker == nil || m.admission == nil {
		return nil, errMemberRecordUnavailable
	}
	if m.admission.checkManagerOperation(ctx, m.owner.host, kind, data) != nil {
		return nil, errMemberRecordUnavailable
	}
	op, err := m.records.retainOperation(kind, data)
	if err != nil {
		return nil, err
	}
	custody, err := admitManagerCustody(ctx, op, data, m.env, m.read)
	if err != nil {
		op.close()
		return nil, err
	}
	fail := func() (*ManagerAttachment, error) { custody.close(); return nil, errMemberRecordUnavailable }
	if op.request.Runtime != "codex_cli" {
		return fail()
	}
	q := op.request
	n := q.Notification
	origin := ManagerOrigin{Member: n.Member.MXID, AgentSelector: n.Member.Agent, Connection: n.Member.Connection, ConfigurationRoot: n.Member.ConfigurationRoot, Runtime: q.Runtime, Profile: q.Profile.Name, ProfileLock: q.Profile.Lock, Operation: q.Operation, AdmissionReference: n.AdmissionReference, WorkflowReference: n.Workflow.Reference, WorkflowSHA256: n.Workflow.SHA256, CallTimeoutMS: n.CallTimeoutMS}
	origin.Recheck = func(ctx context.Context) error {
		if ctx == nil || ctx.Err() != nil {
			return errMemberRecordUnavailable
		}
		if m.admission.checkManagerOperation(ctx, m.owner.host, kind, data) != nil {
			custody.cancel()
			return errMemberRecordUnavailable
		}
		return custody.checkNotificationOwner(m.owner.host, custody.selection)
	}
	prepared, err := m.broker.Prepare(ctx, origin)
	if err != nil || prepared == nil {
		if prepared != nil {
			_ = prepared.Close()
		}
		return fail()
	}
	if ctx.Err() != nil || origin.Recheck(ctx) != nil || prepared.Recheck(ctx) != nil {
		_ = prepared.Close()
		return fail()
	}
	refs := prepared.References()
	selection := notificationSelection{Member: refs.Member, Connection: refs.Connection, Configuration: refs.Configuration, ProfileLock: refs.ProfileLock, WorkflowReference: custody.selection.WorkflowReference, WorkflowSHA256: custody.selection.WorkflowSHA256}
	if !selection.valid() {
		_ = prepared.Close()
		return fail()
	}
	bridgeCustody := brokerManagerCustody{manager: custody, provider: prepared, admission: m.admission, kind: kind, data: append([]byte(nil), data...), selection: selection, references: refs}
	lifecycle, err := prepareManagedNotification(m.owner.host, bridgeCustody, selection, n.CallTimeoutMS)
	if err != nil {
		_ = prepared.Close()
		return fail()
	}
	a := &ManagerAttachment{manager: m, custody: custody, prepared: prepared, lifecycle: lifecycle}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = a.Close()
		return nil, errMemberRecordUnavailable
	}
	m.attachments[a] = struct{}{}
	m.mu.Unlock()
	go func() { <-lifecycle.done; _ = a.Close() }()
	return a, nil
}
func (m *NotificationManager) Close(ctx context.Context) error {
	if m == nil || m.owner == nil || ctx == nil {
		return errHostOwnerUnavailable
	}
	m.mu.Lock()
	m.closed = true
	var all []*ManagerAttachment
	for a := range m.attachments {
		all = append(all, a)
	}
	m.mu.Unlock()
	// All completions are finite by the plugin contract. Do not release roots
	// before asking their actual provider completion to cancel and join.
	for _, a := range all {
		_ = a.Close()
	}
	if m.records != nil {
		_ = m.records.close()
	}
	return m.owner.close(ctx)
}

type ManagerAttachment struct {
	manager       *NotificationManager
	custody       *managerCustody
	prepared      PreparedNotificationBroker
	lifecycle     *managedNotification
	mu            sync.Mutex
	completion    NotificationCompletion
	bootstrapping bool
	bootstrapDone chan struct{}
	closed        bool
	closeOnce     sync.Once
}
type brokerEnrollmentReceipt struct{ NotificationEnrollment }

func (e brokerEnrollmentReceipt) enrollment() (NotificationBinding, string, context.Context, time.Time) {
	ctx, expiry := e.Lifetime()
	return e.Binding(), e.AttachmentReference(), ctx, expiry
}
func (e brokerEnrollmentReceipt) checkEnrollment() error { return e.Recheck() }

// Bootstrap may be attempted again only while the native identity is not yet
// ready and the endpoint has never transferred. Every actual transfer is single
// use. The callback installs Host service before returning to the provider's ACK.
func (a *ManagerAttachment) Bootstrap(ctx context.Context) error {
	if a == nil || a.custody == nil || a.lifecycle == nil || a.prepared == nil || a.manager == nil || ctx == nil || ctx.Err() != nil {
		return ErrNotificationUnresolved
	}
	a.mu.Lock()
	if a.closed || a.bootstrapping || a.custody.ctx.Err() != nil {
		a.mu.Unlock()
		return ErrNotificationUnresolved
	}
	a.bootstrapping = true
	a.bootstrapDone = make(chan struct{})
	done := a.bootstrapDone
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.bootstrapping = false; close(done); a.mu.Unlock() }()
	recheck := func(ctx context.Context) error {
		if ctx == nil || ctx.Err() != nil {
			return ErrNotificationUnresolved
		}
		if a.lifecycle.custody.checkNotificationOwner(a.manager.owner.host, a.lifecycle.selection) != nil {
			return ErrNotificationUnresolved
		}
		return a.prepared.Recheck(ctx)
	}
	if recheck(ctx) != nil {
		return ErrNotificationUnresolved
	}
	handoff, err := a.lifecycle.sourceBootstrap()
	if err != nil {
		return err
	}
	bootstrapCtx, cancel := context.WithCancel(a.custody.ctx)
	stop := context.AfterFunc(ctx, cancel)
	keep := false
	defer func() {
		stop()
		if !keep {
			cancel()
		}
	}()
	completion, err := a.prepared.Bootstrap(bootstrapCtx, NotificationBootstrap{MethodEndpoint: handoff.Endpoint, AttachmentReference: handoff.AttachmentRef, Binding: handoff.Binding, CallTimeoutMS: handoff.CallTimeoutMS, RecheckHost: recheck, OnEnrolled: func(ctx context.Context, e NotificationEnrollment) error {
		if e == nil || recheck(ctx) != nil {
			return ErrNotificationUnresolved
		}
		_, err := a.lifecycle.completeEnrollment(brokerEnrollmentReceipt{e})
		return err
	}})
	// nil means the provider did not consume the endpoint. A nonnil completion
	// owns it even on error; only that completion performs its finite cleanup.
	if completion == nil {
		_ = handoff.Endpoint.Close()
		_ = a.lifecycle.close()
		return ErrNotificationUnresolved
	}
	a.mu.Lock()
	a.completion = completion
	closed := a.closed
	a.mu.Unlock()
	projection, _ := a.lifecycle.projection()
	if err != nil || closed || projection.State != "ready" || completion.Done() == nil {
		_ = completion.Close()
		_ = a.lifecycle.close()
		return ErrNotificationUnresolved
	}

	select {
	case <-completion.Done():
		_ = completion.Close()
		_ = a.lifecycle.close()
		return ErrNotificationUnresolved
	default:
	}
	go func() {
		select {
		case <-completion.Done():
			_ = a.lifecycle.close()
		case <-a.custody.ctx.Done():
		}
	}()
	keep = true
	return nil
}
func (a *ManagerAttachment) Projection() (notificationProjection, error) {
	if a == nil || a.lifecycle == nil {
		return notificationProjection{}, ErrNotificationUnresolved
	}
	return a.lifecycle.projection()
}
func (a *ManagerAttachment) Close() error {
	if a == nil || a.lifecycle == nil || a.custody == nil || a.prepared == nil || a.manager == nil {
		return nil
	}
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		done := a.bootstrapDone
		completion := a.completion
		a.mu.Unlock()
		a.custody.cancel()
		_ = a.lifecycle.close()
		if completion != nil {
			_ = completion.Close()
		}
		_ = a.prepared.Close()
		if done != nil {
			<-done
		}
		a.mu.Lock()
		completion = a.completion
		a.mu.Unlock()
		if completion != nil {
			_ = completion.Close()
		}
		a.custody.close()
		a.manager.mu.Lock()
		delete(a.manager.attachments, a)
		a.manager.mu.Unlock()
	})
	return nil
}

// ManagerNotificationProjection is advisory status, never a future grant.
type ManagerNotificationProjection = notificationProjection

// Provider checks stay outside the manager's own correlation mutex. A provider
// may call Origin.Recheck while checking its root; that cannot recurse into its
// own Recheck. No filesystem or subprocess work occurs under Host.mu.
type brokerManagerCustody struct {
	manager    *managerCustody
	provider   PreparedNotificationBroker
	admission  managerOperationAdmission
	kind       string
	data       []byte
	selection  notificationSelection
	references NotificationReferences
}

func (c brokerManagerCustody) lifetime() (context.Context, time.Time) { return c.manager.lifetime() }
func (c brokerManagerCustody) checkNotificationOwner(h *Host, s notificationSelection) error {
	if s != c.selection || c.provider.References() != c.references || c.admission == nil || c.admission.checkManagerOperation(c.manager.ctx, h, c.kind, c.data) != nil || c.manager.checkNotificationOwner(h, c.manager.selection) != nil {
		return ErrNotificationUnresolved
	}
	return c.provider.Recheck(c.manager.ctx)
}
