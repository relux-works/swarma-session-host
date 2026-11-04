package sessionhost

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type bridgeBrokerFixture struct {
	prepare func(context.Context, ManagerOrigin) (PreparedNotificationBroker, error)
}

func (b bridgeBrokerFixture) Prepare(ctx context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
	return b.prepare(ctx, o)
}

type bridgePreparedFixture struct {
	origin     ManagerOrigin
	references *NotificationReferences
	bootstrap  func(context.Context, NotificationBootstrap) (NotificationCompletion, error)
	check      func(context.Context) error
	close      func() error
}

func (p *bridgePreparedFixture) References() NotificationReferences {
	if p.references != nil {
		return *p.references
	}
	return NotificationReferences{Member: p.origin.Member, Connection: "fixture-opaque-connection-ref", Configuration: p.origin.ConfigurationRoot, ProfileLock: p.origin.ProfileLock}
}
func (p *bridgePreparedFixture) Recheck(ctx context.Context) error {
	if p.check != nil {
		return p.check(ctx)
	}
	return p.origin.Recheck(ctx)
}
func (p *bridgePreparedFixture) Bootstrap(ctx context.Context, h NotificationBootstrap) (NotificationCompletion, error) {
	return p.bootstrap(ctx, h)
}
func (p *bridgePreparedFixture) Close() error {
	if p.close != nil {
		return p.close()
	}
	return nil
}

type bridgeCompletionFixture struct {
	ctx      context.Context
	cancel   context.CancelFunc
	endpoint *net.UnixConn
	once     sync.Once
}

func (c *bridgeCompletionFixture) Done() <-chan struct{} { return c.ctx.Done() }
func (c *bridgeCompletionFixture) Err() error            { return c.ctx.Err() }
func (c *bridgeCompletionFixture) Close() error {
	c.once.Do(func() { c.cancel(); _ = c.endpoint.Close() })
	return nil
}

type bridgeEnrollmentFixture struct {
	b       NotificationBinding
	ref     string
	ctx     context.Context
	expires time.Time
}

func (e bridgeEnrollmentFixture) Binding() NotificationBinding           { return e.b }
func (e bridgeEnrollmentFixture) AttachmentReference() string            { return e.ref }
func (e bridgeEnrollmentFixture) Lifetime() (context.Context, time.Time) { return e.ctx, e.expires }
func (e bridgeEnrollmentFixture) Recheck() error                         { return e.ctx.Err() }

// Missing plugin, failed Core identity and failed actual plugin preparation all
// refuse without a Host attachment. The successful nearby control below uses
// the same actual retained owner/record/workspace and Core command argv fixture,
// plus an explicitly inert ordinary-handler admission model.
func TestManagerBridgePreAttachmentRefusal(t *testing.T) {
	for _, mode := range []string{"missing", "ordinary-admission", "Core-workflow", "Core-digest", "provider", "provider-recheck"} {
		t.Run(mode, func(t *testing.T) {
			m, q, selected := managerBridgeFixture(t)
			called := false
			m.broker = bridgeBrokerFixture{prepare: func(ctx context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
				called = true
				recordNoAttachment(t, m.owner)
				if mode == "provider" {
					return nil, errors.New("fixture provider refused")
				}
				return &bridgePreparedFixture{origin: o, check: func(context.Context) error { return errors.New("fixture source lost") }}, nil
			}}
			switch mode {
			case "missing":
				m.broker = nil
			case "ordinary-admission":
				m.admission = nil
			case "Core-workflow":
				*selected = []byte(strings.Replace(string(*selected), "workflow-1", "different", 1))
			case "Core-digest":
				q["profile"].(map[string]any)["fragment_digest"] = "sha256:" + strings.Repeat("d", 64)
			}
			a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
			if a != nil || !errors.Is(err, errMemberRecordUnavailable) {
				t.Fatal("inadmissible composition accepted", err)
			}
			if (mode == "Core-workflow" || mode == "Core-digest" || mode == "ordinary-admission") && called {
				t.Fatal("Core refusal contacted broker")
			}
			recordNoAttachment(t, m.owner)
		})
	}
}

// This configured plugin is an explicit inert provider model, not live provider
// qualification. The public lifecycle consumes one pair, installs on the live
// callback before provider return, and can serve observe_binding afterwards.
func TestManagerBridgeLifecycle(t *testing.T) {
	m, q, _ := managerBridgeFixture(t)
	m.owner.host.mu.Lock()
	m.owner.host.state.Protocol.ObservedRelease = "0.164.0"
	m.owner.host.mu.Unlock()
	var completion *bridgeCompletionFixture
	var binding NotificationBinding
	m.broker = bridgeBrokerFixture{prepare: func(ctx context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
		recordNoAttachment(t, m.owner)
		if o.ProfileLock != strings.Repeat("a", 64) || o.Runtime != "codex_cli" || o.Operation == "" || o.Recheck(ctx) != nil {
			t.Fatal("missing actual origin")
		}
		return &bridgePreparedFixture{origin: o, bootstrap: func(ctx context.Context, h NotificationBootstrap) (NotificationCompletion, error) {
			cctx, cancel := context.WithCancel(ctx)
			completion = &bridgeCompletionFixture{ctx: cctx, cancel: cancel, endpoint: h.MethodEndpoint}
			binding = h.Binding
			binding.ConsumerGeneration = "fixture-generation"
			binding.EnrollmentRef = "fixture-enrollment"
			if h.RecheckHost(ctx) != nil {
				t.Fatal("host recheck failed")
			}
			err := h.OnEnrolled(ctx, bridgeEnrollmentFixture{binding, h.AttachmentReference, cctx, time.Now().Add(10 * time.Second)})
			if err != nil {
				return completion, err
			}
			// Local callback has returned while this Bootstrap has not: no ACK wait.
			m.owner.host.mu.Lock()
			ready := m.owner.host.notificationManager.state == "ready"
			m.owner.host.mu.Unlock()
			if !ready {
				t.Fatal("callback waited for provider return")
			}
			return completion, nil
		}, close: func() error {
			if completion != nil {
				return completion.Close()
			}
			return nil
		}}, nil
	}}
	a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := a.Projection(); p.State != "preparing" || p.Binding != nil {
		t.Fatal("invalid preparing projection", p)
	}
	request, requestCancel := context.WithCancel(context.Background())
	if err := a.Bootstrap(request); err != nil {
		t.Fatal("inert lifecycle failed", err)
	}
	requestCancel()
	select {
	case <-completion.Done():
		t.Fatal("request return/cancellation ended continuing completion")
	default:
	}
	req := NotificationRequest{Contract: NotificationContract, RequestID: "fixture-observe", Method: "observe_binding", Binding: binding}
	response := notificationExchange(t, completion.endpoint, bufio.NewReader(completion.endpoint), req)
	if binding.ConnectionRef == q["notification"].(map[string]any)["member"].(map[string]any)["connection"] {
		t.Fatal("opaque reference derived from public stable selector")
	}
	if response.Result != "observed" {
		t.Fatal("real local method not served", response)
	}
	if err := a.Bootstrap(context.Background()); !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal("second endpoint transfer accepted", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if p, _ := a.Projection(); p.State != "unresolved" {
		t.Fatal("closed lifecycle became ready", p)
	}
	select {
	case <-completion.Done():
	default:
		t.Fatal("provider completion not closed")
	}
}

// Actual selected Core drift is terminal even if its old bytes later return.
// No observed loss may be converted into a new pair or live capability.
func TestManagerBridgeOriginLoss(t *testing.T) {
	m, q, selected := managerBridgeFixture(t)
	m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
		return &bridgePreparedFixture{origin: o}, nil
	}}
	a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), (*selected)...)
	*selected = []byte(strings.Replace(string(*selected), "fixture-selected.json", "different-source.json", 1))
	if p, _ := a.Projection(); p.State != "unresolved" {
		t.Fatal("changed Core origin remained live", p)
	}
	*selected = original
	if p, _ := a.Projection(); p.State != "unresolved" {
		t.Fatal("lost origin recovered authority", p)
	}
}

// Close during an actual blocked inert Bootstrap cancels its context and joins
// it; a late consumed completion is still closed. No caller-owned waiter is
// abandoned and no native process is stopped by this lifecycle.
func TestManagerBridgeCloseBootstrap(t *testing.T) {
	m, q, _ := managerBridgeFixture(t)
	entered := make(chan struct{})
	var completion *bridgeCompletionFixture
	m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
		return &bridgePreparedFixture{origin: o, bootstrap: func(ctx context.Context, h NotificationBootstrap) (NotificationCompletion, error) {
			cctx, cancel := context.WithCancel(ctx)
			completion = &bridgeCompletionFixture{ctx: cctx, cancel: cancel, endpoint: h.MethodEndpoint}
			close(entered)
			<-ctx.Done()
			return completion, ctx.Err()
		}}, nil
	}}
	a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- a.Bootstrap(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("bootstrap did not enter")
	}
	closed := make(chan struct{})
	go func() { _ = a.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("close did not join bootstrap")
	}
	if err := <-result; !errors.Is(err, ErrNotificationUnresolved) {
		t.Fatal("late success", err)
	}
	select {
	case <-completion.Done():
	default:
		t.Fatal("late completion leaked")
	}
}

// A nonnil consumed completion without an actual enrollment callback cannot
// produce ready, and a nil unconsumed completion is also terminal. Both paths
// close the transferred endpoint; neither starts another child or pair.
func TestManagerBridgeCompletionRefusals(t *testing.T) {
	for _, mode := range []string{"unconsumed", "no-enrollment", "bad-binding"} {
		t.Run(mode, func(t *testing.T) {
			m, q, _ := managerBridgeFixture(t)
			var completion *bridgeCompletionFixture
			m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
				return &bridgePreparedFixture{origin: o, bootstrap: func(ctx context.Context, h NotificationBootstrap) (NotificationCompletion, error) {
					if mode == "unconsumed" {
						return nil, errors.New("fixture before consume")
					}
					cctx, cancel := context.WithCancel(ctx)
					completion = &bridgeCompletionFixture{ctx: cctx, cancel: cancel, endpoint: h.MethodEndpoint}
					if mode == "bad-binding" {
						b := h.Binding
						b.ConsumerGeneration = "fixture"
						b.EnrollmentRef = "fixture"
						b.ProfileLockRef = strings.Repeat("e", 64)
						if h.OnEnrolled(ctx, bridgeEnrollmentFixture{b, h.AttachmentReference, cctx, time.Now().Add(time.Second)}) == nil {
							t.Fatal("bad callback accepted")
						}
					}
					return completion, nil
				}, close: func() error {
					if completion != nil {
						return completion.Close()
					}
					return nil
				}}, nil
			}}
			a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
			if err != nil {
				t.Fatal(err)
			}
			if err := a.Bootstrap(context.Background()); !errors.Is(err, ErrNotificationUnresolved) {
				t.Fatal("invalid completion became ready", err)
			}
			if p, _ := a.Projection(); p.State != "unresolved" || p.Binding != nil {
				t.Fatal("invalid ready projection", p)
			}
			if completion != nil {
				select {
				case <-completion.Done():
				default:
					t.Fatal("consumed completion leaked")
				}
			}
		})
	}
}

// Completion termination fences Host even when the enrolled view's advisory
// context has not yet observed loss. The nearby lifecycle control proves an
// actual local observation works before termination, without a native call.
func TestManagerBridgeCompletionEndsService(t *testing.T) {
	m, q, _ := managerBridgeFixture(t)
	m.owner.host.mu.Lock()
	m.owner.host.state.Protocol.ObservedRelease = "0.164.0"
	m.owner.host.mu.Unlock()
	var completion *bridgeCompletionFixture
	m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
		return &bridgePreparedFixture{origin: o, bootstrap: func(ctx context.Context, h NotificationBootstrap) (NotificationCompletion, error) {
			cctx, cancel := context.WithCancel(ctx)
			completion = &bridgeCompletionFixture{ctx: cctx, cancel: cancel, endpoint: h.MethodEndpoint}
			b := h.Binding
			b.ConsumerGeneration = "fixture"
			b.EnrollmentRef = "fixture"
			return completion, h.OnEnrolled(ctx, bridgeEnrollmentFixture{b, h.AttachmentReference, context.Background(), time.Now().Add(10 * time.Second)})
		}, close: func() error {
			if completion != nil {
				return completion.Close()
			}
			return nil
		}}, nil
	}}
	a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p, _ := a.Projection(); p.State != "ready" {
		t.Fatal("positive control missing", p)
	}
	completion.cancel()
	select {
	case <-a.lifecycle.done:
	case <-time.After(2 * time.Second):
		t.Fatal("completion termination left Host live")
	}
	if p, _ := a.Projection(); p.State != "unresolved" {
		t.Fatal("terminated completion became ready", p)
	}
}

// An admitted operation retains its own exact input for later ordinary-handler
// checks. Mutating the caller's slice after Prepare cannot substitute that input
// or revoke the already-retained request by aliasing its bytes.
func TestManagerBridgeRetainsOperationBytes(t *testing.T) {
	m, q, _ := managerBridgeFixture(t)
	data := memberOperationBytes(t, q)
	expected := append([]byte(nil), data...)
	m.admission = inertExactOperationAdmission{expected}
	m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
		return &bridgePreparedFixture{origin: o}, nil
	}}
	a, err := m.Prepare(context.Background(), "launch", data)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		data[i] = ' '
	}
	if p, _ := a.Projection(); p.State != "preparing" {
		t.Fatal("caller substituted retained operation bytes", p)
	}
}

type inertExactOperationAdmission struct{ expected []byte }

func (a inertExactOperationAdmission) checkManagerOperation(ctx context.Context, _ *Host, _ string, data []byte) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !bytes.Equal(a.expected, data) {
		return errors.New("fixture exact-operation mismatch")
	}
	return nil
}

// An actual provider scope may use either equal or different reference bytes;
// a later substituted opaque scope reference must terminate the attachment.
// This is an inert provider model, not proof of actual a2a scope ownership.
func TestManagerBridgeOpaqueScope(t *testing.T) {
	for _, equal := range []bool{false, true} {
		t.Run(map[bool]string{false: "different", true: "equal"}[equal], func(t *testing.T) {
			m, q, _ := managerBridgeFixture(t)
			refs := NotificationReferences{Member: "fixture-member-ref", Connection: "fixture-opaque-connection-ref", Configuration: "fixture-config-ref", ProfileLock: "fixture-profile-ref"}
			if equal {
				refs.Connection = q["notification"].(map[string]any)["member"].(map[string]any)["connection"].(string)
			}
			m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, o ManagerOrigin) (PreparedNotificationBroker, error) {
				return &bridgePreparedFixture{origin: o, references: &refs}, nil
			}}
			a, err := m.Prepare(context.Background(), "launch", memberOperationBytes(t, q))
			if err != nil {
				t.Fatal("valid actual scope refused", err)
			}
			if a.lifecycle.selection.Connection != refs.Connection {
				t.Fatal("actual opaque scope lost")
			}
			refs.Connection = "substituted-opaque-ref"
			if p, _ := a.Projection(); p.State != "unresolved" {
				t.Fatal("substituted provider scope remained current", p)
			}
		})
	}
}
