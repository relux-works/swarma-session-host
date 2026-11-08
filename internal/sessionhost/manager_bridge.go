package sessionhost

import (
	"context"
	"errors"
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

// enrollmentRecheckingBroker marks an internal adapter whose Bootstrap owns a
// serialized provider recheck before enrollment. Its Host callbacks must check
// Host custody without recursively entering that operation. Other brokers
// retain the composite custody check.
type enrollmentRecheckingBroker interface {
	bootstrapRechecksProvider()
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
	owner             *continuingHostOwner
	records           *memberRecordSource
	env               []string
	broker            NotificationBroker
	mu                sync.Mutex
	attachments       map[*ManagerAttachment]struct{}
	operation         *managerNotificationOperation
	closed            bool
	closeDone         chan struct{} // initialized under mu; retained until all cleanup joins
	closeErr          error         // published by closeDone
	read              managerCoreRead
	admission         managerOperationAdmission
	createCorrelation *nativeCorrelation
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
	if closed || m.owner == nil || m.records == nil || m.broker == nil || m.admission == nil {
		return nil, errMemberRecordUnavailable
	}
	if m.admission.checkManagerOperation(ctx, m.owner.host, kind, data) != nil {
		return nil, errMemberRecordUnavailable
	}
	claim, first, err := m.claimOperation(ctx, kind, data)
	if err != nil {
		return nil, err
	}
	if !first {
		return claim.attachment, nil
	}
	defer close(claim.done)
	// The short request can cancel this preparation, but it never owns the
	// continuing manager. Conversely Close must cancel and join this attempt,
	// including while the broker is still preparing and no attachment exists.
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.owner.ctx, cancel)
	defer func() { stop(); cancel() }()
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
			claim.cleanupErr = errors.Join(claim.cleanupErr, prepared.Close())
		}
		return fail()
	}
	if ctx.Err() != nil || origin.Recheck(ctx) != nil || prepared.Recheck(ctx) != nil {
		claim.cleanupErr = errors.Join(claim.cleanupErr, prepared.Close())
		return fail()
	}
	refs := prepared.References()
	selection := notificationSelection{Member: refs.Member, Connection: refs.Connection, Configuration: refs.Configuration, ProfileLock: refs.ProfileLock, WorkflowReference: custody.selection.WorkflowReference, WorkflowSHA256: custody.selection.WorkflowSHA256}
	if !selection.valid() {
		claim.cleanupErr = errors.Join(claim.cleanupErr, prepared.Close())
		return fail()
	}
	bridgeCustody := brokerManagerCustody{manager: custody, provider: prepared, admission: m.admission, kind: kind, data: append([]byte(nil), data...), selection: selection, references: refs}
	lifecycle, err := prepareManagedNotification(m.owner.host, bridgeCustody, selection, n.CallTimeoutMS)
	if err != nil {
		claim.cleanupErr = errors.Join(claim.cleanupErr, prepared.Close())
		return fail()
	}
	a := &ManagerAttachment{manager: m, custody: custody, prepared: prepared, lifecycle: lifecycle,
		recheckHost: func(ctx context.Context) error {
			if ctx == nil || ctx.Err() != nil {
				return ErrNotificationUnresolved
			}
			return bridgeCustody.checkManagerOwner(m.owner.host, selection)
		},
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		claim.cleanupErr = errors.Join(claim.cleanupErr, a.Close())
		return nil, errMemberRecordUnavailable
	}
	m.attachments[a] = struct{}{}
	claim.attachment = a
	m.mu.Unlock()
	go func() { <-lifecycle.done; _ = a.Close() }()
	return a, nil
}

// Close retires admission and starts one retained cleanup. ctx bounds only this
// caller's observation: a context error never means resources have been joined.
// Provider code runs outside the caller and manager lock, even if it ignores
// cancellation. Every concurrent/subsequent Close observes the same completion.
func (m *NotificationManager) Close(ctx context.Context) error {
	if m == nil || m.owner == nil || ctx == nil {
		return errHostOwnerUnavailable
	}
	m.owner.cancel()
	m.mu.Lock()
	if m.closeDone == nil {
		m.closed = true
		m.closeDone = make(chan struct{})
		operation := m.operation
		go m.cleanup(operation)
	}
	done := m.closeDone
	m.mu.Unlock()
	select {
	case <-done:
		return m.closeErr
	default:
	}
	select {
	case <-done:
		return m.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *NotificationManager) cleanup(operation *managerNotificationOperation) {
	defer close(m.closeDone)
	// A stuck provider cannot delay cancellation/join of the actual Host and
	// runner. Its own custody and late results remain retained in the other job.
	m.closeErr = joinOwnedCleanup(func() error {
		if m.createCorrelation != nil {
			return m.createCorrelation.close(context.Background())
		}
		return m.owner.close(context.Background())
	}, func() error {
		if operation != nil {
			<-operation.done
		}
		m.mu.Lock()
		var jobs []func() error
		for a := range m.attachments {
			jobs = append(jobs, a.Close)
		}
		m.mu.Unlock()
		err := joinOwnedCleanup(jobs...)
		if operation != nil {
			err = errors.Join(err, operation.cleanupErr)
		}
		if m.records != nil {
			err = errors.Join(err, m.records.close())
		}
		return err
	})
}

type ManagerAttachment struct {
	manager       *NotificationManager
	custody       *managerCustody
	prepared      PreparedNotificationBroker
	lifecycle     *managedNotification
	mu            sync.Mutex
	completion    NotificationCompletion
	recheckHost   func(context.Context) error
	bootstrapping bool
	bootstrapDone chan struct{}
	closed        bool
	closeOnce     sync.Once
	closeErr      error // published by closeOnce
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
	// Keep composite checks unless the adapter guarantees its own fresh provider
	// recheck inside Bootstrap, before invoking the Host callbacks.
	callbackCheck := recheck
	_, providerRechecked := a.prepared.(enrollmentRecheckingBroker)
	if providerRechecked {
		callbackCheck = a.recheckHost
	}
	completion, err := a.prepared.Bootstrap(bootstrapCtx, NotificationBootstrap{MethodEndpoint: handoff.Endpoint, AttachmentReference: handoff.AttachmentRef, Binding: handoff.Binding, CallTimeoutMS: handoff.CallTimeoutMS, RecheckHost: callbackCheck, OnEnrolled: func(ctx context.Context, e NotificationEnrollment) error {
		if e == nil || callbackCheck(ctx) != nil {
			return ErrNotificationUnresolved
		}
		if providerRechecked {
			_, err := a.lifecycle.completeEnrollmentChecked(brokerEnrollmentReceipt{e}, func() error {
				return a.recheckHost(ctx)
			})
			return err
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
		a.closeErr = joinOwnedCleanup(a.lifecycle.close, a.prepared.Close, func() error {
			var err error
			if completion != nil {
				err = closeNotificationCompletion(completion)
			}
			if done != nil {
				<-done
			}
			a.mu.Lock()
			lateCompletion := a.completion
			a.mu.Unlock()
			if lateCompletion != nil {
				err = errors.Join(err, closeNotificationCompletion(lateCompletion))
			}
			return err
		})
		a.custody.close()
		// Retain this single-use attachment's result for manager Close, including
		// errors from automatic retirement before the manager's own close.
	})
	return a.closeErr
}

// Completion Close must request cancellation and join. Also observe its actual
// completion channel: returning from Close alone cannot certify a consumed
// endpoint has completed. A malformed completion stays explicitly unresolved.
func closeNotificationCompletion(completion NotificationCompletion) error {
	err := completion.Close()
	done := completion.Done()
	if done == nil {
		return errors.Join(err, ErrNotificationUnresolved)
	}
	<-done
	return err
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

// During Bootstrap the provider owns its serialized operation. The callback
// checks the retained manager and enrollment receipt without recursively starting
// another provider operation. Ordinary admission/projection still rechecks both.
func (c brokerManagerCustody) checkManagerOwner(h *Host, s notificationSelection) error {
	if s != c.selection || c.provider.References() != c.references || c.admission == nil || c.admission.checkManagerOperation(c.manager.ctx, h, c.kind, c.data) != nil || c.manager.checkNotificationOwner(h, c.manager.selection) != nil {
		return ErrNotificationUnresolved
	}
	return nil
}
func (c brokerManagerCustody) checkNotificationOwner(h *Host, s notificationSelection) error {
	if err := c.checkManagerOwner(h, s); err != nil {
		return err
	}
	return c.provider.Recheck(c.manager.ctx)
}
