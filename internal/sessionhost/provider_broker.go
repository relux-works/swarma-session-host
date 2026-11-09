package sessionhost

import (
	"context"
	"net"
	"sync"
	"time"

	pp "github.com/relux-works/swarma-session-host/internal/providerprotocol"
)

// inertProviderBroker docks the proposed control client without enabling a live
// provider. Only in-package fixtures construct it. There is no launcher, default
// broker, public constructor or method-transport handoff. The supplied stream is
// already owned; the client remains its only reader for its entire lifetime.
// verify must use independently retained enrollment, never manufacture admission
// from the received DTO. The proposed grammar and live composition remain open.
type inertProviderBroker struct {
	mu      sync.Mutex
	used    bool
	conn    *net.UnixConn
	owner   context.Context
	expires time.Time
	verify  func(context.Context, pp.Enrollment) (NotificationEnrollment, error)
}

func (b *inertProviderBroker) Prepare(ctx context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
	b.mu.Lock()
	if b.used {
		b.mu.Unlock()
		return nil, ErrNotificationUnresolved
	}
	b.used = true
	b.mu.Unlock()
	if ctx == nil || ctx.Err() != nil || b.owner == nil || b.owner.Err() != nil || !time.Now().Before(b.expires) || b.verify == nil || origin.Recheck == nil || origin.Recheck(ctx) != nil {
		if b.conn != nil {
			_ = b.conn.Close()
		}
		return nil, ErrNotificationUnresolved
	}
	// Owner cancellation bounds preparation too, but a successful short request
	// never owns the continuing prepared broker.
	attempt, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(b.owner, cancel)
	defer func() { stop(); cancel() }()
	client, err := pp.NewClient(attempt, b.conn, b.expires)
	if err != nil {
		return nil, err
	}
	life, retire := context.WithDeadline(b.owner, b.expires)
	p := &inertPreparedProvider{client: client, origin: origin, ctx: life, cancel: retire, verify: b.verify, closed: make(chan struct{})}
	go p.watch()
	refs, err := client.Prepare(attempt, pp.PrepareRequest{Connection: origin.Connection, ProfileLockRef: origin.ProfileLock, WorkflowReference: origin.WorkflowReference, WorkflowSHA256: origin.WorkflowSHA256})
	if err == nil && (attempt.Err() != nil || p.checkOwner(ctx) != nil) {
		err = ErrNotificationUnresolved
	}
	if err != nil {
		p.cancel()
		// Retain acquired cleanup for the manager, including failed Prepare.
		return p, err
	}
	p.refs = NotificationReferences{Member: refs.MemberRef, Connection: refs.ConnectionRef, Configuration: refs.ConfigurationRef, ProfileLock: refs.ProfileLockRef}
	return p, nil
}

type inertPreparedProvider struct {
	client *pp.Client
	origin ManagerOrigin
	refs   NotificationReferences // frozen before publication
	ctx    context.Context
	cancel context.CancelFunc
	verify func(context.Context, pp.Enrollment) (NotificationEnrollment, error)

	mu             sync.Mutex
	closing, used  bool
	endpoint       *net.UnixConn // retained for cleanup only; never read or handed off
	bootstrapDone  chan struct{}
	stopEnrollment func() bool
	closed         chan struct{} // watcher exit, not a cleanup timeout
	closeErr       error         // published by closed
}

// Bootstrap performs the proposed client's nested provider recheck before the
// enrollment callback. Keep this protocol property at the adapter boundary.
func (*inertPreparedProvider) bootstrapRechecksProvider() {}

func (p *inertPreparedProvider) References() NotificationReferences { return p.refs }
func (p *inertPreparedProvider) Done() <-chan struct{}              { return p.ctx.Done() }
func (p *inertPreparedProvider) Err() error {
	if err := p.client.Err(); err != nil {
		return err
	}
	return p.ctx.Err()
}
func (p *inertPreparedProvider) checkOwner(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil || p.ctx.Err() != nil || p.origin.Recheck(ctx) != nil {
		return ErrNotificationUnresolved
	}
	select {
	case <-p.client.Done():
		return ErrNotificationUnresolved
	default:
		return nil
	}
}
func (p *inertPreparedProvider) Recheck(ctx context.Context) error {
	if err := p.checkOwner(ctx); err != nil {
		p.cancel()
		return err
	}
	if err := p.client.Recheck(ctx); err != nil {
		p.cancel()
		return err
	}
	if err := p.checkOwner(ctx); err != nil {
		p.cancel()
		return err
	}
	return nil
}

func (p *inertPreparedProvider) Bootstrap(ctx context.Context, h NotificationBootstrap) (NotificationCompletion, error) {
	if ctx == nil || ctx.Err() != nil || h.MethodEndpoint == nil || h.RecheckHost == nil || h.OnEnrolled == nil || p.checkOwner(ctx) != nil {
		return nil, ErrNotificationUnresolved // endpoint remains caller-owned
	}
	p.mu.Lock()
	if p.closing || p.used {
		p.mu.Unlock()
		return nil, ErrNotificationUnresolved
	}
	p.used, p.endpoint, p.bootstrapDone = true, h.MethodEndpoint, make(chan struct{})
	done := p.bootstrapDone
	p.mu.Unlock()
	defer close(done)
	attempt, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.ctx, cancel)
	defer func() { stop(); cancel() }()
	err := p.client.Bootstrap(attempt, pp.BootstrapRequest{Binding: pp.Binding(h.Binding), AttachmentRef: h.AttachmentReference, CallTimeoutMS: h.CallTimeoutMS}, pp.Callbacks{
		RecheckHost: func(ctx context.Context) error {
			if p.checkOwner(ctx) != nil {
				return ErrNotificationUnresolved
			}
			return h.RecheckHost(ctx)
		},
		Enroll: func(ctx context.Context, wire pp.Enrollment) error {
			// The client performed the fresh nested provider recheck. Calling
			// Client.Recheck here would recurse through its operation gate.
			if p.checkOwner(ctx) != nil {
				return ErrNotificationUnresolved
			}
			e, err := p.verify(ctx, wire)
			if err != nil || e == nil || e.Binding() != NotificationBinding(wire.Binding) || e.AttachmentReference() != wire.AttachmentRef || e.Recheck() != nil {
				return ErrNotificationUnresolved
			}
			owner, expiry := e.Lifetime()
			wireExpiry, err := time.Parse(time.RFC3339Nano, wire.LeaseExpiresAt)
			if err != nil || owner == nil || owner.Err() != nil || !expiry.Equal(wireExpiry) || !time.Now().Before(expiry) {
				return ErrNotificationUnresolved
			}
			life, retire := context.WithDeadline(p.ctx, expiry)
			// Hook registration may invoke verifier-supplied context methods.
			// Keep it outside the adapter lock so cleanup remains bounded.
			stop := context.AfterFunc(owner, retire)
			p.mu.Lock()
			// A late callback must not publish a hook after the watcher has
			// taken responsibility for stopping the last published hook.
			if p.closing {
				p.mu.Unlock()
				stop()
				retire()
				return ErrNotificationUnresolved
			}
			p.stopEnrollment = stop
			p.mu.Unlock()
			return h.OnEnrolled(ctx, inertProviderEnrollment{NotificationEnrollment: e, ctx: life, expires: expiry, provider: p})
		},
	})
	if err != nil {
		p.cancel()
	}
	// A nonnil completion always owns the endpoint, including callback/ACK loss.
	return p, err
}

// Done fences semantic lifetime immediately; Close separately joins control
// reads, callbacks and the Bootstrap caller before reporting cleanup complete.
func (p *inertPreparedProvider) watch() {
	defer close(p.closed)
	select {
	case <-p.client.Done():
	case <-p.ctx.Done():
	}
	p.cancel()
	p.mu.Lock()
	p.closing = true
	endpoint, bootstrapDone := p.endpoint, p.bootstrapDone
	p.mu.Unlock()
	if endpoint != nil {
		_ = endpoint.Close()
	}
	p.closeErr = p.client.Close(context.Background())
	if bootstrapDone != nil {
		<-bootstrapDone
	}
	p.mu.Lock()
	stop := p.stopEnrollment
	p.mu.Unlock()
	if stop != nil {
		stop()
	}
}
func (p *inertPreparedProvider) Close() error {
	p.cancel()
	// The existing watcher owns cleanup, including the verifier owner's stop
	// function. That function (or the Bootstrap caller) may ignore cancellation.
	// Bound this wait without declaring the watcher joined or spawning another
	// cleanup worker. A later Close can join it after the owner releases.
	timer := time.NewTimer(pp.CleanupBound)
	defer timer.Stop()
	select {
	case <-p.closed:
		return p.closeErr
	case <-timer.C:
		return pp.ErrCleanup
	}
}

type inertProviderEnrollment struct {
	NotificationEnrollment
	ctx      context.Context
	expires  time.Time
	provider *inertPreparedProvider
}

func (e inertProviderEnrollment) Lifetime() (context.Context, time.Time) { return e.ctx, e.expires }
func (e inertProviderEnrollment) Recheck() error {
	if e.ctx.Err() != nil || !time.Now().Before(e.expires) {
		return ErrNotificationUnresolved
	}
	select {
	case <-e.provider.client.Done():
		return ErrNotificationUnresolved
	default:
		return e.NotificationEnrollment.Recheck()
	}
}

var _ NotificationBroker = (*inertProviderBroker)(nil)
var _ PreparedNotificationBroker = (*inertPreparedProvider)(nil)
var _ NotificationCompletion = (*inertPreparedProvider)(nil)
var _ enrollmentRecheckingBroker = (*inertPreparedProvider)(nil)
