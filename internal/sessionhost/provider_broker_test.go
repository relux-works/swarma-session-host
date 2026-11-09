package sessionhost

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pp "github.com/relux-works/swarma-session-host/internal/providerprotocol"
)

// This peer is an in-process, inert control fixture, not an admitted provider.
// It never reads the method endpoint. Its retained enrollment models the source
// origin check so valid wire metadata alone cannot pass the verification hook.
type inertBrokerPeer struct {
	mu         sync.Mutex
	enrollment pp.Enrollment
	events     []string
	mode       string
	lease      time.Duration
	conn       *net.UnixConn
	manager    *NotificationManager
	prepared   chan struct{}
	installed  chan struct{}
	release    chan struct{}
	exited     chan struct{}
	once       sync.Once
	unknown    atomic.Bool
	verified   atomic.Int32
	verify     func(context.Context) error
}

func (f *inertBrokerPeer) event(s string) {
	f.mu.Lock()
	f.events = append(f.events, s)
	f.mu.Unlock()
}
func (f *inertBrokerPeer) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}
func (f *inertBrokerPeer) unblock() { f.once.Do(func() { close(f.release) }) }
func (f *inertBrokerPeer) send(id uint32, kind string, value any) error {
	frame, err := pp.NewFrame(id, kind, value)
	if err != nil {
		return err
	}
	return pp.WriteFrame(f.conn, frame)
}
func (f *inertBrokerPeer) serve() {
	defer close(f.exited)
	defer f.conn.Close()
	for {
		frame, err := pp.ReadFrame(f.conn)
		if err != nil {
			return
		}
		f.event(frame.Type)
		switch frame.Type {
		case "hello":
			err = f.send(frame.ID, "hello_ok", nil)
		case "prepare":
			var q pp.PrepareRequest
			if json.Unmarshal(frame.Body, &q) != nil {
				return
			}
			close(f.prepared)
			if f.mode == "prepare-cancel" {
				<-f.release
				return
			}
			if f.mode == "prepare-refuse" {
				_ = f.send(frame.ID, "refused", pp.Refusal{Reason: "admission-unavailable"})
				return
			}
			err = f.send(frame.ID, "prepared", pp.References{MemberRef: "fixture-member", ConnectionRef: "fixture-opaque-connection", ConfigurationRef: "fixture-config", ProfileLockRef: q.ProfileLockRef})
		case "recheck":
			if f.unknown.Load() {
				_ = f.send(frame.ID, "refused", pp.Refusal{Reason: "observation-unavailable"})
				return
			}
			err = f.send(frame.ID, "rechecked", nil)
		case "bootstrap":
			var q pp.BootstrapRequest
			if json.Unmarshal(frame.Body, &q) != nil {
				return
			}
			binding := q.Binding
			binding.ConsumerGeneration, binding.EnrollmentRef = "fixture-generation", "fixture-enrollment"
			f.mu.Lock()
			f.enrollment = pp.Enrollment{Binding: binding, AttachmentRef: q.AttachmentRef, SourceSession: "fixture-session", ConsumerRef: "fixture-consumer", LeaseHolder: "fixture-holder", LeaseHost: "fixture-host", LeasePID: 123, ProcessBirth: "fixture-birth", LeaseExpiresAt: time.Now().Add(f.lease).UTC().Format(time.RFC3339Nano)}
			f.mu.Unlock()
			err = f.send(frame.ID, "recheck_host", nil)
		case "host_checked":
			f.mu.Lock()
			enrollment := f.enrollment
			f.mu.Unlock()
			if f.mode == "foreign-source" {
				enrollment.SourceSession = "other-session"
			}
			if f.mode == "foreign-binding" {
				enrollment.Binding.MemberRef = "other-member"
			}
			err = f.send(frame.ID, "enrollment", enrollment)
		case "installed":
			f.manager.owner.host.mu.Lock()
			ready := f.manager.owner.host.notificationManager.state == "ready"
			f.manager.owner.host.mu.Unlock()
			if ready {
				f.event("callback-before-ack")
			}
			close(f.installed)
			if f.mode == "hold-ack" || f.mode == "missing-ack" {
				<-f.release
			}
			if f.mode == "missing-ack" {
				return
			}
			err = f.send(frame.ID, "ack", nil)
		case "close":
			_ = f.send(frame.ID, "closed", nil)
			return
		default:
			return
		}
		if err != nil {
			return
		}
	}
}

func inertBrokerManager(t *testing.T, mode string) (*NotificationManager, map[string]any, *inertBrokerPeer, *inertProviderBroker, *[]byte) {
	t.Helper()
	m, q, selected := managerBridgeFixture(t)
	client, server := notificationSocketpair(t, syscall.SOCK_STREAM)
	f := &inertBrokerPeer{mode: mode, lease: 10 * time.Second, conn: server, manager: m, prepared: make(chan struct{}), installed: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
	b := &inertProviderBroker{conn: client, owner: m.owner.ctx, expires: time.Now().Add(20 * time.Second)}
	b.verify = func(ctx context.Context, wire pp.Enrollment) (NotificationEnrollment, error) {
		f.verified.Add(1)
		f.event("verify")
		if f.verify != nil {
			if err := f.verify(ctx); err != nil {
				return nil, err
			}
		}
		f.mu.Lock()
		retained := f.enrollment
		f.mu.Unlock()
		if wire != retained || f.unknown.Load() {
			return nil, ErrNotificationUnresolved
		}
		expires, _ := time.Parse(time.RFC3339Nano, retained.LeaseExpiresAt)
		return bridgeEnrollmentFixture{NotificationBinding(retained.Binding), retained.AttachmentRef, m.owner.ctx, expires}, nil
	}
	m.broker = b
	go f.serve()
	t.Cleanup(func() {
		f.unblock()
		_ = server.Close()
		awaitInert(t, f.exited, "fixture peer cleanup")
	})
	return m, q, f, b, selected
}
func awaitInert(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("did not join", what)
	}
}
func prepareInert(t *testing.T, m *NotificationManager, q map[string]any) (*ManagerAttachment, *inertPreparedProvider) {
	t.Helper()
	a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal("inert preparation", err)
	}
	return a, a.prepared.(*inertPreparedProvider)
}

func TestInertProviderBrokerLifetime(t *testing.T) {
	m, q, f, _, _ := inertBrokerManager(t, "normal")
	prepareCtx, prepareCancel := context.WithCancel(context.Background())
	a, err := m.Prepare(prepareCtx, "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	p := a.prepared.(*inertPreparedProvider)
	prepareCancel()
	if p.ctx.Err() != nil {
		t.Fatal("short Prepare request owned the prepared lifetime")
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Bootstrap(ctx); err != nil {
		t.Fatal("inert bootstrap", err)
	}
	cancel()
	if p.ctx.Err() != nil {
		t.Fatal("short request cancellation revoked continuing completion")
	}
	if view, _ := a.Projection(); view.State != "ready" || view.Binding.ConnectionRef != "fixture-opaque-connection" {
		t.Fatal("prepared opaque references or ready state lost", view)
	}
	events := f.snapshot()
	var nested, verified, installed int
	for i, event := range events {
		switch event {
		case "host_checked":
			nested = i + 1
		case "verify":
			verified = i
		case "callback-before-ack":
			installed = i
		}
	}
	if events[nested] != "recheck" || !(nested < verified && verified < installed) || f.verified.Load() != 1 {
		t.Fatal("callback ordering changed", events)
	}
	if err := a.Close(); err != nil {
		t.Fatal("clean inert close", err)
	}
	awaitInert(t, p.closed, "completion cleanup")
	awaitInert(t, f.exited, "control peer")
	if err := p.Close(); err != nil {
		t.Fatal("cleanup not idempotent", err)
	}
	if p.ctx.Err() == nil || a.lifecycle.ctx.Err() == nil {
		t.Fatal("joined cleanup left semantic lifetime active")
	}
	p.mu.Lock()
	endpoint := p.endpoint
	p.mu.Unlock()
	if _, err := endpoint.Write([]byte("inert")); err == nil {
		t.Fatal("consumed method endpoint leaked")
	}
}

func TestInertProviderBrokerPrepareRefusals(t *testing.T) {
	for _, mode := range []string{"prepare-refuse", "prepare-cancel", "owner-unknown", "missing-verifier", "owner-canceled"} {
		t.Run(mode, func(t *testing.T) {
			m, q, f, b, selected := inertBrokerManager(t, mode)
			switch mode {
			case "owner-unknown":
				*selected = []byte(`{}`)
			case "missing-verifier":
				b.verify = nil
			case "owner-canceled":
				owner, cancel := context.WithCancel(context.Background())
				cancel()
				b.owner = owner
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				a, err := m.Prepare(ctx, "launch", memberOperationBytes(t, q))
				if a != nil {
					err = errors.New("unexpected attachment")
				}
				result <- err
			}()
			if mode == "prepare-cancel" {
				awaitInert(t, f.prepared, "prepare entered")
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, errMemberRecordUnavailable) {
					t.Fatal("unavailable preparation accepted", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("preparation did not terminate")
			}
			recordNoAttachment(t, m.owner)
			if m.owner.ctx.Err() != nil {
				t.Fatal("failed preparation revoked manager owner")
			}
			if a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q)); a != nil || err == nil {
				t.Fatal("failed operation reopened", err)
			}
		})
	}
}

func TestInertProviderBrokerUnknownAndDone(t *testing.T) {
	for _, mode := range []string{"provider-unknown", "owner-unknown", "EOF", "duplicate-ack"} {
		t.Run(mode, func(t *testing.T) {
			m, q, f, _, selected := inertBrokerManager(t, mode)
			a, p := prepareInert(t, m, q)
			if err := a.Bootstrap(context.Background()); err != nil {
				t.Fatal("positive control", err)
			}
			switch mode {
			case "provider-unknown":
				f.unknown.Store(true)
			case "owner-unknown":
				*selected = []byte(`{}`)
			case "EOF":
				_ = f.conn.Close()
			case "duplicate-ack":
				// This unsolicited frame arrives while the sole reader is idle.
				_ = f.send(1, "ack", nil)
			}
			if mode == "provider-unknown" || mode == "owner-unknown" {
				if view, _ := a.Projection(); view.State != "unresolved" {
					t.Fatal("unknown origin remained available", view)
				}
			}
			awaitInert(t, p.Done(), "semantic completion")
			awaitInert(t, a.lifecycle.done, "Done fencing of Host service")
			awaitInert(t, p.closed, "control and callback cleanup")
			if view, _ := a.Projection(); view.State != "unresolved" || view.Binding != nil {
				t.Fatal("terminated provider remained ready", view)
			}
			if err := a.Bootstrap(context.Background()); err == nil {
				t.Fatal("terminal bootstrap reopened")
			}
		})
	}
}

func TestInertProviderBrokerEnrollmentRefusals(t *testing.T) {
	for _, mode := range []string{"foreign-source", "foreign-binding", "verifier-unknown", "missing-ack"} {
		t.Run(mode, func(t *testing.T) {
			m, q, f, _, _ := inertBrokerManager(t, mode)
			if mode == "verifier-unknown" {
				f.verify = func(context.Context) error { return ErrNotificationUnresolved }
			}
			a, p := prepareInert(t, m, q)
			if mode == "missing-ack" {
				f.unblock()
			}
			if err := a.Bootstrap(context.Background()); !errors.Is(err, ErrNotificationUnresolved) {
				t.Fatal("invalid enrollment or ACK became ready", err)
			}
			awaitInert(t, p.closed, "refused bootstrap cleanup")
			if view, _ := a.Projection(); view.State != "unresolved" || view.Binding != nil {
				t.Fatal("failed bootstrap retained ready state", view)
			}
			if mode == "missing-ack" {
				events := f.snapshot()
				if !containsInert(events, "callback-before-ack") {
					t.Fatal("ACK-loss control never installed callback", events)
				}
			}
		})
	}
}
func containsInert(events []string, want string) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}

func TestInertProviderBrokerCloseDuringCallback(t *testing.T) {
	m, q, f, _, _ := inertBrokerManager(t, "normal")
	entered, exited := make(chan struct{}), make(chan struct{})
	f.verify = func(ctx context.Context) error {
		close(entered)
		defer close(exited)
		<-ctx.Done()
		return ctx.Err()
	}
	a, p := prepareInert(t, m, q)
	bootstrap := make(chan error, 1)
	go func() { bootstrap <- a.Bootstrap(context.Background()) }()
	awaitInert(t, entered, "retained verifier entered")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = a.Close() })
	}
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	awaitInert(t, joined, "concurrent attachment cleanup")
	awaitInert(t, exited, "owned verifier callback")
	awaitInert(t, p.closed, "provider cleanup")
	select {
	case err := <-bootstrap:
		if err == nil {
			t.Fatal("canceled callback succeeded")
		}
	default:
		t.Fatal("Close returned before Bootstrap joined")
	}
	if containsInert(f.snapshot(), "installed") {
		t.Fatal("canceled callback emitted installed")
	}
}

func TestInertProviderBrokerCloseBeforeACK(t *testing.T) {
	m, q, f, _, _ := inertBrokerManager(t, "hold-ack")
	a, p := prepareInert(t, m, q)
	bootstrap := make(chan error, 1)
	go func() { bootstrap <- a.Bootstrap(context.Background()) }()
	awaitInert(t, f.installed, "local callback before ACK")
	before := f.snapshot()
	if !containsInert(before, "callback-before-ack") {
		t.Fatal("callback did not install before held ACK", before)
	}
	_ = a.Close()
	f.unblock()
	awaitInert(t, p.closed, "close during ACK wait")
	if err := <-bootstrap; err == nil {
		t.Fatal("close during ACK wait succeeded")
	}
	after := f.snapshot()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("cleanup started another protocol operation during bootstrap", before, after)
	}
}

func TestInertProviderBrokerRetainedLifetimeBounds(t *testing.T) {
	for _, bound := range []string{"owner-cancel", "custody-expiry", "lease-expiry"} {
		t.Run(bound, func(t *testing.T) {
			m, q, f, b, _ := inertBrokerManager(t, "normal")
			owner, cancel := context.WithCancel(m.owner.ctx)
			defer cancel()
			b.owner = owner
			if bound == "custody-expiry" {
				b.expires = time.Now().Add(time.Second)
			}
			if bound == "lease-expiry" {
				f.mu.Lock()
				f.lease = time.Second
				f.mu.Unlock()
			}
			a, p := prepareInert(t, m, q)
			if err := a.Bootstrap(context.Background()); err != nil {
				t.Fatal("positive lifetime control", err)
			}
			if bound == "owner-cancel" {
				cancel()
			}
			awaitInert(t, p.Done(), "retained lifetime bound")
			awaitInert(t, a.lifecycle.done, "bounded service lifetime")
			awaitInert(t, p.closed, "bounded provider cleanup")
			if view, _ := a.Projection(); view.State != "unresolved" {
				t.Fatal("expired lifetime remained ready", view)
			}
		})
	}
}

func TestInertProviderBrokerEnrollmentOwnerCancel(t *testing.T) {
	m, q, _, b, _ := inertBrokerManager(t, "normal")
	// This owner is independent of both the broker's custody and the manager.
	owner, cancel := context.WithCancel(context.Background())
	defer cancel()
	verify := b.verify
	b.verify = func(ctx context.Context, wire pp.Enrollment) (NotificationEnrollment, error) {
		e, err := verify(ctx, wire)
		if err != nil {
			return nil, err
		}
		retained := e.(bridgeEnrollmentFixture)
		retained.ctx = owner
		return retained, nil
	}
	a, p := prepareInert(t, m, q)
	if err := a.Bootstrap(context.Background()); err != nil {
		t.Fatal("positive independent-owner control", err)
	}
	if view, _ := a.Projection(); view.State != "ready" {
		t.Fatal("independent enrollment owner never became ready", view)
	}
	cancel()
	awaitInert(t, a.lifecycle.done, "enrollment-owner fence of Host service")
	awaitInert(t, p.closed, "enrollment-owner provider cleanup")
	if m.owner.ctx.Err() != nil || b.owner.Err() != nil {
		t.Fatal("enrollment cancellation revoked independent manager or broker owner")
	}
	if view, _ := a.Projection(); view.State != "unresolved" || view.Binding != nil {
		t.Fatal("revoked enrollment remained ready", view)
	}
}

// A distinct prepared type carries the same adapter capability. The generic
// bridge must honor the capability rather than recognize one concrete type.
type inertRecheckingBrokerFixture struct{ PreparedNotificationBroker }

func (*inertRecheckingBrokerFixture) bootstrapRechecksProvider() {}

func TestManagerBridgeEnrollmentRecheckingCapability(t *testing.T) {
	m, q, _, broker, _ := inertBrokerManager(t, "normal")
	m.broker = bridgeBrokerFixture{prepare: func(ctx context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
		prepared, err := broker.Prepare(ctx, origin)
		if prepared == nil {
			return nil, err
		}
		return &inertRecheckingBrokerFixture{prepared}, err
	}}
	a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.Bootstrap(ctx); err != nil {
		t.Fatal("capability-bearing adapter recursively rechecked or failed Bootstrap", err)
	}
	if view, _ := a.Projection(); view.State != "ready" {
		t.Fatal("capability-bearing adapter never became ready", view)
	}
}

func TestInertProviderBrokerVerifiedWireAgreement(t *testing.T) {
	for _, field := range []string{"matching", "generation", "enrollment", "attachment", "expiry"} {
		t.Run(field, func(t *testing.T) {
			m, q, f, b, _ := inertBrokerManager(t, "normal")
			verify := b.verify
			b.verify = func(ctx context.Context, wire pp.Enrollment) (NotificationEnrollment, error) {
				e, err := verify(ctx, wire)
				if err != nil {
					return nil, err
				}
				retained := e.(bridgeEnrollmentFixture)
				switch field {
				case "generation":
					retained.b.ConsumerGeneration = "other-generation"
				case "enrollment":
					retained.b.EnrollmentRef = "other-enrollment"
				case "attachment":
					retained.ref = "other-attachment"
				case "expiry":
					retained.expires = retained.expires.Add(time.Second)
				}
				return retained, nil
			}
			a, p := prepareInert(t, m, q)
			// Exercise the adapter directly: downstream Host comparisons must
			// not mask a missing verified-vs-wire check at this boundary.
			handoff, err := a.lifecycle.sourceBootstrap()
			if err != nil {
				t.Fatal(err)
			}
			called := false
			completion, err := p.Bootstrap(context.Background(), NotificationBootstrap{
				MethodEndpoint: handoff.Endpoint, AttachmentReference: handoff.AttachmentRef,
				Binding: handoff.Binding, CallTimeoutMS: handoff.CallTimeoutMS,
				RecheckHost: a.recheckHost,
				OnEnrolled:  func(context.Context, NotificationEnrollment) error { called = true; return nil },
			})
			if completion != p || f.verified.Load() != 1 {
				t.Fatal("verification boundary was not reached", err)
			}
			if field == "matching" {
				if err != nil || !called {
					t.Fatal("matching verified enrollment refused", err)
				}
			} else if err == nil || called || containsInert(f.snapshot(), "installed") {
				t.Fatal("mismatched verified enrollment reached installation", err, called)
			}
			_ = a.Close()
			awaitInert(t, p.closed, "verified enrollment cleanup")
		})
	}
}

// A request context may take time to unregister its cancellation callback.
// Holding that cleanup keeps the real adapter Bootstrap caller outstanding
// after the client exchange and callback have both finished. This isolates the
// adapter's join from Client.Close and ManagerAttachment.Close.
type inertStopBarrierContext struct {
	context.Context
	done, stopping, release chan struct{}
	once                    sync.Once
}

func (c *inertStopBarrierContext) Done() <-chan struct{} { return c.done }
func (c *inertStopBarrierContext) AfterFunc(f func()) func() bool {
	stop := context.AfterFunc(c.Context, f)
	return func() bool {
		stopped := stop()
		c.once.Do(func() { close(c.stopping) })
		<-c.release
		return stopped
	}
}

func TestInertProviderBrokerCloseJoinsBootstrap(t *testing.T) {
	m, q, _, _, _ := inertBrokerManager(t, "normal")
	a, p := prepareInert(t, m, q)
	handoff, err := a.lifecycle.sourceBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &inertStopBarrierContext{Context: parent, done: make(chan struct{}), stopping: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(ctx.release) }) }
	defer unblock()
	bootstrap := make(chan error, 1)
	go func() {
		_, err := p.Bootstrap(ctx, NotificationBootstrap{
			MethodEndpoint: handoff.Endpoint, AttachmentReference: handoff.AttachmentRef,
			Binding: handoff.Binding, CallTimeoutMS: handoff.CallTimeoutMS,
			RecheckHost: a.recheckHost,
			OnEnrolled:  func(context.Context, NotificationEnrollment) error { return nil },
		})
		bootstrap <- err
	}()
	awaitInert(t, ctx.stopping, "Bootstrap caller cleanup barrier")
	closed := make(chan struct{})
	go func() { _ = p.Close(); close(closed) }()
	awaitInert(t, p.client.Done(), "control client cleanup")
	select {
	case <-closed:
		t.Fatal("adapter Close returned before Bootstrap caller cleanup joined")
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	awaitInert(t, closed, "adapter Close after Bootstrap release")
	if err := <-bootstrap; err != nil {
		t.Fatal("positive Bootstrap exchange failed", err)
	}
	_ = a.Close()
}

func TestInertProviderBrokerAbandonedVerifier(t *testing.T) {
	m, q, _, b, _ := inertBrokerManager(t, "normal")
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	verify := b.verify
	b.verify = func(ctx context.Context, wire pp.Enrollment) (NotificationEnrollment, error) {
		e, err := verify(ctx, wire)
		close(entered)
		// Deliberately violate cancellation to reproduce a late verifier.
		<-release
		close(exited)
		return e, err
	}
	a, p := prepareInert(t, m, q)
	handoff, err := a.lifecycle.sourceBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var installed atomic.Bool
	bootstrap := make(chan error, 1)
	go func() {
		_, err := p.Bootstrap(ctx, NotificationBootstrap{
			MethodEndpoint: handoff.Endpoint, AttachmentReference: handoff.AttachmentRef,
			Binding: handoff.Binding, CallTimeoutMS: handoff.CallTimeoutMS,
			RecheckHost: a.recheckHost,
			OnEnrolled:  func(context.Context, NotificationEnrollment) error { installed.Store(true); return nil },
		})
		bootstrap <- err
	}()
	awaitInert(t, entered, "uncooperative verifier entered")
	cancel()
	if err := <-bootstrap; err == nil {
		t.Fatal("abandoned callback reported success")
	}
	// Client cleanup is bounded, even when a callback ignores cancellation.
	select {
	case <-p.closed:
	case <-time.After(pp.CleanupBound + 3*time.Second):
		t.Fatal("uncooperative callback exceeded cleanup bound")
	}
	if !errors.Is(p.Close(), pp.ErrCleanup) {
		t.Fatal("unjoined verifier did not retain cleanup failure")
	}
	unblock()
	awaitInert(t, exited, "late verifier release")
	// Join the actual client callback, not just the verifier's return.
	if err := p.client.Close(context.Background()); errors.Is(err, pp.ErrCleanup) {
		t.Fatal("released verifier did not join", err)
	}
	p.mu.Lock()
	registered := p.stopEnrollment != nil
	p.mu.Unlock()
	if registered || installed.Load() {
		t.Fatal("abandoned callback registered owner hook or installed enrollment", registered, installed.Load())
	}
	_ = a.Close()
}

// A verifier may supply a context whose hook registration ignores cancellation.
// A distinct Done channel makes context.AfterFunc use this AfterFunc method.
type inertOwnerHookBarrierContext struct {
	context.Context
	done, entered, release chan struct{}
	stopped                atomic.Bool
}

func (c *inertOwnerHookBarrierContext) Done() <-chan struct{} { return c.done }
func (c *inertOwnerHookBarrierContext) AfterFunc(f func()) func() bool {
	close(c.entered)
	<-c.release
	stop := context.AfterFunc(c.Context, f)
	return func() bool {
		c.stopped.Store(true)
		return stop()
	}
}

func TestInertProviderBrokerOwnerHookCleanupBound(t *testing.T) {
	m, q, f, b, _ := inertBrokerManager(t, "normal")
	owner := &inertOwnerHookBarrierContext{
		Context: context.Background(), done: make(chan struct{}),
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	var release sync.Once
	unblock := func() { release.Do(func() { close(owner.release) }) }
	defer unblock()
	verify := b.verify
	b.verify = func(ctx context.Context, wire pp.Enrollment) (NotificationEnrollment, error) {
		e, err := verify(ctx, wire)
		if err != nil {
			return nil, err
		}
		retained := e.(bridgeEnrollmentFixture)
		retained.ctx = owner
		return retained, nil
	}
	a, p := prepareInert(t, m, q)
	handoff, err := a.lifecycle.sourceBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var installed atomic.Bool
	bootstrap := make(chan error, 1)
	go func() {
		_, err := p.Bootstrap(ctx, NotificationBootstrap{
			MethodEndpoint: handoff.Endpoint, AttachmentReference: handoff.AttachmentRef,
			Binding: handoff.Binding, CallTimeoutMS: handoff.CallTimeoutMS,
			RecheckHost: a.recheckHost,
			OnEnrolled:  func(context.Context, NotificationEnrollment) error { installed.Store(true); return nil },
		})
		bootstrap <- err
	}()
	awaitInert(t, owner.entered, "blocking owner hook registration")
	cancel()
	select {
	case err := <-bootstrap:
		if err == nil {
			t.Fatal("abandoned owner hook reported Bootstrap success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Bootstrap did not abandon blocking owner hook")
	}
	closed := make(chan struct{})
	var closeErr error
	start := time.Now()
	go func() { closeErr = p.Close(); close(closed) }()
	select {
	case <-closed:
		if !errors.Is(closeErr, pp.ErrCleanup) {
			t.Error("unjoined owner hook did not retain cleanup failure", closeErr)
		}
		t.Logf("Close returned ErrCleanup with owner hook blocked after %s", time.Since(start))
	case <-time.After(pp.CleanupBound + 3*time.Second):
		// Record a behavioral failure, then release and join the real work.
		// A test-binary timeout is not evidence for this cleanup contract.
		t.Error("adapter Close exceeded cleanup bound with owner hook blocked")
	}
	unblock()
	awaitInert(t, closed, "adapter Close after owner hook release")
	if !errors.Is(closeErr, pp.ErrCleanup) {
		t.Error("adapter lost unjoined owner cleanup error", closeErr)
	}
	if err := p.client.Close(context.Background()); errors.Is(err, pp.ErrCleanup) {
		t.Fatal("released owner hook callback did not join", err)
	}
	p.mu.Lock()
	registered := p.stopEnrollment != nil
	p.mu.Unlock()
	if registered || installed.Load() || containsInert(f.snapshot(), "installed") {
		t.Error("abandoned owner hook published hook or installed enrollment", registered, installed.Load())
	}
	if !owner.stopped.Load() {
		t.Error("refused owner hook was not stopped")
	}
	_ = a.Close()
}

// A distinct Done channel selects the verifier owner's AfterFunc method. Its
// stop is real owned work: the test must release and join it even on failure.
type inertPublishedHookStopContext struct {
	context.Context
	done, stopping, release, stopped chan struct{}
	registrations, stops             atomic.Int32
}

func (c *inertPublishedHookStopContext) Done() <-chan struct{} { return c.done }
func (c *inertPublishedHookStopContext) AfterFunc(f func()) func() bool {
	c.registrations.Add(1)
	stop := context.AfterFunc(c.Context, f)
	return func() bool {
		c.stops.Add(1)
		close(c.stopping)
		<-c.release
		defer close(c.stopped)
		return stop()
	}
}

func TestInertProviderBrokerPublishedHookStop(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		name := "ordinary"
		if blocking {
			name = "blocking"
		}
		t.Run(name, func(t *testing.T) {
			m, q, f, b, _ := inertBrokerManager(t, "normal")
			owner := &inertPublishedHookStopContext{
				Context: context.Background(), done: make(chan struct{}),
				stopping: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{}),
			}
			var release sync.Once
			unblock := func() { release.Do(func() { close(owner.release) }) }
			defer unblock()
			if !blocking {
				unblock()
			}
			verify := b.verify
			b.verify = func(ctx context.Context, wire pp.Enrollment) (NotificationEnrollment, error) {
				e, err := verify(ctx, wire)
				if err != nil {
					return nil, err
				}
				retained := e.(bridgeEnrollmentFixture)
				retained.ctx = owner
				return retained, nil
			}
			a, p := prepareInert(t, m, q)
			// Cleanup must release the fixture before any manager Close can join
			// the watcher's call to the owner's stop, including on test failure.
			defer func() { unblock(); _ = a.Close() }()
			if err := a.Bootstrap(context.Background()); err != nil {
				t.Fatal("positive published-owner enrollment", err)
			}
			if view, _ := a.Projection(); view.State != "ready" || !containsInert(f.snapshot(), "installed") {
				t.Fatal("published-owner enrollment never became ready", view)
			}
			if got := owner.registrations.Load(); got != 1 {
				t.Fatal("owner hook registration count", got)
			}
			if !blocking {
				// Trigger the watcher independently of Close. Removing its stop
				// call must fail this assertion even if cleanup otherwise joins.
				p.cancel()
				awaitInert(t, p.closed, "ordinary published-hook watcher")
			} else {
				closed := make(chan error, 1)
				start := time.Now()
				go func() { closed <- p.Close() }()
				// Keep the remove-stop mutant bounded too: record the missing
				// owner call, then still release and join all acquired work.
				select {
				case <-owner.stopping:
				case <-time.After(3 * time.Second):
					t.Error("watcher did not call published owner stop")
				}
				remaining := pp.CleanupBound + time.Second - time.Since(start)
				timer := time.NewTimer(remaining)
				joinedCaller := false
				select {
				case err := <-closed:
					joinedCaller = true
					if !errors.Is(err, pp.ErrCleanup) {
						t.Error("blocked published owner reported joined cleanup", err)
					}
					t.Logf("Close with published owner stop blocked: %s", time.Since(start))
				case <-timer.C:
					t.Error("published owner stop exceeded CleanupBound plus 1s slack")
				}
				timer.Stop()
				select {
				case <-p.closed:
					t.Error("watcher reported exit before published owner stop joined")
				default:
				}
				select {
				case <-owner.stopped:
					t.Error("owner stop finished before release")
				default:
				}
				p.mu.Lock()
				closing, retained := p.closing, p.stopEnrollment != nil
				p.mu.Unlock()
				if !closing || !retained {
					t.Error("blocked cleanup lost closing fence or owned hook", closing, retained)
				}
				unblock()
				awaitInert(t, p.closed, "released published-hook watcher")
				// If the old implementation exceeded the bound, its Close is
				// still ours to join. A completed result was already consumed.
				if !joinedCaller {
					select {
					case <-closed:
					case <-time.After(3 * time.Second):
						t.Fatal("released Close caller did not join")
					}
				}
			}
			// Completion and repeated Close must not call the owner twice.
			for range 2 {
				if err := p.Close(); errors.Is(err, pp.ErrCleanup) {
					t.Error("released published owner cleanup did not join", err)
				}
			}
			if got := owner.stops.Load(); got != 1 {
				t.Error("published owner stop count", got)
			} else {
				awaitInert(t, owner.stopped, "actual published owner stop completion")
			}
			if err := p.client.Close(context.Background()); errors.Is(err, pp.ErrCleanup) {
				t.Error("released client callback did not join", err)
			}
		})
	}
}
