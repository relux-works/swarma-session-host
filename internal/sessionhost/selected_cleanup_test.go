package sessionhost

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These are offline causal controls with actual local Host owners and inert
// ordinary/provider callbacks. They establish no live provider integration.
func selectedCleanupFixture(t *testing.T) (*registeredHostPlugin, *selectedOrdinaryFixture, *selectedBrokerFixture, map[string]any) {
	t.Helper()
	m, q, _ := managerBridgeFixture(t)
	m.admission = nil
	m.owner.host.mu.Lock()
	m.owner.host.state.Protocol.ObservedRelease = "0.167.0"
	m.owner.host.mu.Unlock()
	q["submitted_at"] = time.Now().UTC().Format(time.RFC3339)
	b := &selectedBrokerFixture{}
	m.broker = b
	o := &selectedOrdinaryFixture{t: t, execution: &selectedExecution{manager: m, session: "SES-fixture", generation: 1}, available: true}
	p, err := registerHostPlugin(context.Background(), time.Now().Add(20*time.Second), filepath.Join(t.TempDir(), "operations"), o)
	if err != nil {
		t.Fatal(err)
	}
	o.plugin = p
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = p.Close(ctx) // Cleanup errors are the observations under test.
		cleanupAwait(t, p.daemon.done, "daemon cleanup was abandoned")
	})
	return p, o, b, q
}

func selectedStillOwned(t *testing.T, p *registeredHostPlugin, returned <-chan struct{}) {
	t.Helper()
	select {
	case <-p.daemon.done:
		t.Error("daemon reported done before the held callback joined")
	default:
	}
	select {
	case <-returned:
		t.Error("held callback was abandoned")
	default:
	}
}

// The same held Prepare must allow owner cancellation/join through either the
// direct manager or the selected daemon. The callback itself remains owned.
func TestSelectedCloseCleanupIsolation(t *testing.T) {
	for _, route := range []string{"manager-control", "selected-daemon"} {
		t.Run(route, func(t *testing.T) {
			p, o, b, q := selectedCleanupFixture(t)
			entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			b.beforePrepare = func(context.Context) { close(entered); <-release }
			go func() {
				defer close(returned)
				p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
			}()
			cleanupAwait(t, entered, "provider not entered")
			m := o.execution.manager
			closeFn := p.Close
			if route == "manager-control" {
				closeFn = m.Close
			}
			cleanupBoundedCloses(t, closeFn)
			cleanupAwait(t, m.owner.done, "Host owner was held behind provider Prepare")
			selectedStillOwned(t, p, returned)
			unblock()
			cleanupAwait(t, returned, "released provider did not join")
			closeSelected(t, p)
			if o.starts.Load() != 1 || b.prepares.Load() != 1 || b.bootstraps.Load() != 0 {
				t.Fatal("cleanup repeated ordinary/provider effects")
			}
		})
	}
}

// One blocked unpublished resource cleanup cannot delay another retained
// manager's Host cancellation/join, regardless of execution-map iteration order.
func TestSelectedCloseAllManagersBeforeProviderJoin(t *testing.T) {
	p, o, _, q := selectedCleanupFixture(t)
	m1 := o.execution.manager
	m2, _, _ := managerBridgeFixture(t)
	e2 := &selectedExecution{manager: m2, session: "SES-second", generation: 1}
	secondID := "22222222-2222-4222-8222-222222222222"
	o.executeHook = func(_ context.Context, _ string, data []byte) (*selectedExecution, []byte, error) {
		var request struct {
			Operation string `json:"operation_id"`
		}
		_ = json.Unmarshal(data, &request)
		e := o.execution
		if request.Operation == secondID {
			e = e2
		}
		return e, selectedSessionBytes(e, request.Operation), nil
	}
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var cleaned atomic.Int32
	m1.broker = bridgeBrokerFixture{prepare: func(_ context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
		return &bridgePreparedFixture{origin: origin, close: func() error {
			cleaned.Add(1)
			close(entered)
			<-release
			return nil
		}}, errors.New("synthetic refusal after acquisition")
	}}
	go func() {
		defer close(returned)
		p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
	}()
	cleanupAwait(t, entered, "unpublished cleanup not entered")
	var second map[string]any
	_ = json.Unmarshal(memberOperationBytes(t, q), &second)
	second["operation_id"] = secondID
	delete(second, "notification")
	selectedCall(t, p, "launch", second)
	cleanupBoundedCloses(t, p.Close)
	cleanupAwait(t, m1.owner.done, "first owner blocked behind provider cleanup")
	cleanupAwait(t, m2.owner.done, "second owner blocked behind first manager")
	selectedStillOwned(t, p, returned)
	unblock()
	cleanupAwait(t, returned, "unpublished cleanup callback not joined")
	closeSelected(t, p)
	if cleaned.Load() != 1 || o.starts.Load() != 2 {
		t.Fatal("cleanup or ordinary execution repeated", cleaned.Load(), o.starts.Load())
	}
}

func TestSelectedCloseRetainsLateExecution(t *testing.T) {
	for _, outcome := range []string{"valid", "execute-error", "invalid-result"} {
		t.Run(outcome, func(t *testing.T) {
			p, o, b, q := selectedCleanupFixture(t)
			entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			o.executeHook = func(_ context.Context, _ string, _ []byte) (*selectedExecution, []byte, error) {
				close(entered)
				<-release
				result := selectedSessionBytes(o.execution, q["operation_id"].(string))
				if outcome == "execute-error" {
					return o.execution, nil, errors.New("synthetic lost execution result")
				}
				if outcome == "invalid-result" {
					result = []byte(`{}`)
				}
				return o.execution, result, nil
			}
			go func() {
				defer close(returned)
				p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
			}()
			cleanupAwait(t, entered, "execute not entered")
			cleanupBoundedCloses(t, p.Close)
			selectedStillOwned(t, p, returned)
			data, exit := p.Call(context.Background(), selectedManagerProtocol, "launch", memberOperationBytes(t, q))
			selectedCode(t, data, exit, "session_host_unavailable")
			unblock()
			cleanupAwait(t, returned, "late execute did not join")
			closeSelected(t, p)
			cleanupAwait(t, o.execution.manager.owner.done, "late execution owner escaped cleanup")
			body, err := os.ReadFile(filepath.Join(p.daemon.journal.store.dir, q["operation_id"].(string)+".json"))
			var entry selectedOperationEntry
			if err != nil || json.Unmarshal(body, &entry) != nil || entry.Digest == "" {
				t.Fatal("durable execution obligation lost", err)
			}
			if outcome != "valid" && len(entry.Result) != 0 {
				t.Fatal("unknown execution was reported as started")
			}
			if o.starts.Load() != 1 || b.prepares.Load() != 0 {
				t.Fatal("shutdown repeated effects or prepared late notification")
			}
		})
	}
}

type selectedCleanupCallbackOwner struct {
	*selectedOrdinaryFixture
	available func() bool
	observe   func(context.Context, string) ([]byte, error)
}

func (o *selectedCleanupCallbackOwner) notificationAvailable() bool { return o.available() }
func (o *selectedCleanupCallbackOwner) status(ctx context.Context, session string) ([]byte, error) {
	return o.observe(ctx, session)
}

func TestSelectedCloseJoinsOrdinaryCallbacks(t *testing.T) {
	for _, kind := range []string{"status", "describe"} {
		t.Run(kind, func(t *testing.T) {
			p, o, _, q := selectedCleanupFixture(t)
			selectedCall(t, p, "launch", q)
			entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			p.daemon.owner = &selectedCleanupCallbackOwner{selectedOrdinaryFixture: o,
				available: func() bool { close(entered); <-release; return true },
				observe: func(context.Context, string) ([]byte, error) {
					close(entered)
					<-release
					return unreadableSelectedSession(o.execution.session), nil
				}}
			body := []byte(`{}`)
			if kind == "status" {
				body = []byte(`{"session":"SES-fixture"}`)
			}
			go func() {
				defer close(returned)
				p.Call(context.Background(), selectedManagerProtocol, kind, body)
			}()
			cleanupAwait(t, entered, "ordinary callback not entered")
			cleanupBoundedCloses(t, p.Close)
			cleanupAwait(t, o.execution.manager.owner.done, "ordinary callback delayed retained owner cleanup")
			selectedStillOwned(t, p, returned)
			unblock()
			cleanupAwait(t, returned, "ordinary callback was not joined")
			closeSelected(t, p)
		})
	}
}

// All manager failures, automatic retirement, unpublished acquisitions and
// journal descriptor failures must survive the daemon's single completion.
func TestSelectedCloseRetainsAllCleanupErrors(t *testing.T) {
	for _, tc := range []struct{ mode, descriptor string }{
		{"unpublished", "index"}, {"retired-attachment", "index"},
		{"unpublished", "store-lock"}, {"retired-attachment", "store-lock"},
	} {
		t.Run(tc.mode+"/"+tc.descriptor, func(t *testing.T) {
			mode := tc.mode
			p, o, _, q := selectedCleanupFixture(t)
			m := o.execution.manager
			unknown := errors.New("synthetic first provider cleanup unknown")
			secondUnknown := errors.New("synthetic second provider cleanup unknown")
			var closes, secondCloses atomic.Int32
			m.broker = bridgeBrokerFixture{prepare: func(_ context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
				prepared := &bridgePreparedFixture{origin: origin, close: func() error { closes.Add(1); return unknown },
					bootstrap: func(context.Context, NotificationBootstrap) (NotificationCompletion, error) {
						return nil, errors.New("synthetic no endpoint consumer")
					}}
				if mode == "unpublished" {
					return prepared, errors.New("synthetic refusal after acquisition")
				}
				return prepared, nil
			}}
			selectedCall(t, p, "launch", q)
			if mode == "retired-attachment" {
				if o.execution.attachment == nil {
					t.Fatal("retirement control did not acquire an attachment")
				}
				// Bootstrap's refusal retires the lifecycle; join its automatic
				// attachment cleanup before closing the daemon.
				if err := o.execution.attachment.Close(); !errors.Is(err, unknown) {
					t.Fatal("retired attachment lost cleanup failure", err)
				}
			}
			m2, q2, _ := managerBridgeFixture(t)
			m2.broker = bridgeBrokerFixture{prepare: func(_ context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
				return &bridgePreparedFixture{origin: origin, close: func() error { secondCloses.Add(1); return secondUnknown }}, errors.New("synthetic second acquisition refusal")
			}}
			if a, err := m2.Prepare(context.Background(), "launch", memberOperationBytes(t, q2)); a != nil || err == nil {
				t.Fatal("second unpublished control was not refused")
			}
			e2 := &selectedExecution{manager: m2, session: "SES-second", generation: 1}
			// Retain the second manager through a real ordinary execute result.
			o.executeHook = func(_ context.Context, _ string, data []byte) (*selectedExecution, []byte, error) {
				return e2, selectedSessionBytes(e2, "22222222-2222-4222-8222-222222222222"), nil
			}
			q2["operation_id"] = "22222222-2222-4222-8222-222222222222"
			q2["submitted_at"] = time.Now().UTC().Format(time.RFC3339)
			delete(q2, "notification")
			selectedCall(t, p, "launch", q2)
			// Inject a journal descriptor failure after requests have completed.
			descriptor := p.daemon.journal.index
			if tc.descriptor == "store-lock" {
				descriptor = p.daemon.journal.store.lock
			}
			if err := descriptor.Close(); err != nil {
				t.Fatal(err)
			}
			results := make(chan error, 8)
			for range 8 {
				go func() { results <- p.Close(context.Background()) }()
			}
			var retained error
			for range 8 {
				err := <-results
				if !errors.Is(err, unknown) || !errors.Is(err, secondUnknown) || !errors.Is(err, os.ErrClosed) {
					t.Fatal("daemon lost manager or journal cleanup failure", err)
				}
				if retained != nil && err != retained {
					t.Fatal("concurrent Close callers observed different retained errors")
				}
				retained = err
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := p.Close(ctx); err != retained || closes.Load() != 1 || secondCloses.Load() != 1 {
				t.Fatal("repeated Close lost error or repeated cleanup", err, closes.Load(), secondCloses.Load())
			}
			cleanupAwait(t, m.owner.done, "first owner not joined")
			cleanupAwait(t, m2.owner.done, "second owner not joined")
		})
	}
}

// Each ledger descriptor failure must survive the daemon's one completion;
// the normal neighbor must still complete successfully.
func TestSelectedCloseLedgerDescriptors(t *testing.T) {
	for _, which := range []string{"none", "directory", "index", "store-lock"} {
		t.Run(which, func(t *testing.T) {
			p, o, _, q := selectedCleanupFixture(t)
			delete(q, "notification")
			selectedCall(t, p, "launch", q)
			var descriptor *os.File
			switch which {
			case "directory":
				descriptor = p.daemon.journal.directory
			case "index":
				descriptor = p.daemon.journal.index
			case "store-lock":
				descriptor = p.daemon.journal.store.lock
			}
			if descriptor != nil {
				if err := descriptor.Close(); err != nil {
					t.Fatal(err)
				}
			}
			results := make(chan error, 8)
			for range 8 {
				go func() { results <- p.Close(context.Background()) }()
			}
			retained := <-results
			if which == "none" && retained != nil || which != "none" && !errors.Is(retained, os.ErrClosed) {
				t.Errorf("lost %s ledger completion: %v", which, retained)
			}
			for range 7 {
				if err := <-results; err != retained {
					t.Error("concurrent Close observed a different completion", err, retained)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := p.Close(ctx); err != retained {
				t.Error("completed Close lost its retained result to cancellation", err, retained)
			}
			cleanupAwait(t, o.execution.manager.owner.done, "Host owner not joined")
			if o.starts.Load() != 1 {
				t.Fatal("ordinary execution repeated", o.starts.Load())
			}
		})
	}
}
