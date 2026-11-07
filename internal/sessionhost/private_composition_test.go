package sessionhost

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// Dependency absence is rejected before filesystem creation or any effect.
// No DTO boolean/UID/profile can fill an absent in-process trusted dependency.
func TestPrivateCompositionMissingDependency(t *testing.T) {
	for _, mode := range []string{"profile", "source", "core", "resume", "canceled-request"} {
		t.Run(mode, func(t *testing.T) {
			cfg, _, _, _, _ := t4CompositionFixture(t)
			state := cfg.stateDir
			deps := privateCompositionDependencies{source: bridgeBrokerFixture{prepare: func(context.Context, ManagerOrigin) (PreparedNotificationBroker, error) {
				t.Fatal("missing dependency reached Source")
				return nil, nil
			}}, core: func(context.Context, string, []string, ...string) ([]byte, error) {
				t.Fatal("missing dependency reached Core")
				return nil, nil
			}}
			kind := "launch"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "profile":
				cfg.profile.name = ""
			case "source":
				deps.source = nil
			case "core":
				deps.core = nil
			case "resume":
				kind = "resume"
			case "canceled-request":
				cancel()
			}
			r, err := openPrivateComposition(context.Background(), ctx, time.Now().Add(time.Second), cfg, deps, kind, []byte(`{"allow":true,"caller_uid":1001}`))
			if r != nil || err == nil {
				t.Fatal("absent dependency admitted")
			}
			if _, err = os.Lstat(state); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("dependency refusal mutated state")
			}
		})
	}
}

// An abandoned close waiter cannot release custody early or stop the actual
// cleanup. This holds the injected provider's finite join open deliberately.
func TestPrivateCompositionCanceledCloseWaiter(t *testing.T) {
	cfg, _, body, selected, fragment := t4CompositionFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	broker := bridgeBrokerFixture{prepare: func(ctx context.Context, origin ManagerOrigin) (PreparedNotificationBroker, error) {
		var completion *bridgeCompletionFixture
		return &bridgePreparedFixture{origin: origin, bootstrap: func(ctx context.Context, b NotificationBootstrap) (NotificationCompletion, error) {
			cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			completion = &bridgeCompletionFixture{ctx: cctx, cancel: cancel, endpoint: b.MethodEndpoint}
			binding := b.Binding
			binding.ConsumerGeneration = "explicit-fake-generation"
			binding.EnrollmentRef = "explicit-fake-receipt"
			return completion, b.OnEnrolled(ctx, bridgeEnrollmentFixture{binding, b.AttachmentReference, cctx, time.Now().Add(10 * time.Second)})
		}, close: func() error {
			close(entered)
			<-release
			if completion != nil {
				return completion.Close()
			}
			return nil
		}}, nil
	}}
	r, err := openPrivateComposition(context.Background(), context.Background(), time.Now().Add(10*time.Second), cfg, privateCompositionDependencies{broker, t4CoreReader(selected, fragment)}, "launch", body)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		unblock()
		if err := r.close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	waiter, cancel := context.WithCancel(context.Background())
	cancel()
	if err = r.close(waiter); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled waiter", err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup abandoned")
	}
	select {
	case <-r.done:
		t.Fatal("runtime finished before provider joined")
	default:
	}
	cleanupBoundedCloses(t, r.close)
	cleanupAwait(t, r.create.done, "provider close blocked actual runner cleanup")
	if got, err := r.create.create(context.Background(), "launch", body); err == nil || got != createClosed {
		t.Fatal("retired create returned success while runtime cleanup pending", got, err)
	}
	unblock()
	if err = r.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.create.done:
	default:
		t.Fatal("T1 watcher not joined")
	}
	select {
	case <-r.manager.createCorrelation.done:
	default:
		t.Fatal("T2 cleanup not joined")
	}
}
