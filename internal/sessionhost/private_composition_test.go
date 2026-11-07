package sessionhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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

// Interval admission belongs to composition, before state reservation and the
// later runner-config checks. The successful composition below is its neighbor.
func TestPrivateCompositionInvalidIntervalsBeforeEffects(t *testing.T) {
	for _, field := range []string{"ready", "poll", "ioTimeout"} {
		for _, bound := range []struct {
			name  string
			value time.Duration
		}{{"zero", 0}, {"over-minute", time.Minute + time.Nanosecond}} {
			t.Run(field+"/"+bound.name, func(t *testing.T) {
				cfg, _, body, _, _ := t4CompositionFixture(t)
				// Mark an actual launcher invocation, including one later cleaned up.
				marker := filepath.Join(filepath.Dir(cfg.stateDir), "child-started")
				script, err := os.ReadFile(cfg.launcher.binary)
				if err != nil {
					t.Fatal(err)
				}
				script = []byte("#!/bin/sh\n: > '" + strings.ReplaceAll(marker, "'", "'\\''") + "'\n" + strings.TrimPrefix(string(script), "#!/bin/sh\n"))
				if err := os.WriteFile(cfg.launcher.binary, script, 0700); err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(script)
				cfg.launcher.sha256 = hex.EncodeToString(sum[:])
				cfg.coreEnvironment = []string{"LANG=C"}
				switch field {
				case "ready":
					cfg.ready = bound.value
				case "poll":
					cfg.poll = bound.value
				case "ioTimeout":
					cfg.ioTimeout = bound.value
				}
				// Rebind the fixture policy so a stale configuration digest cannot
				// mask a missing early duration check.
				singleTrustPolicyFixture(t, cfg, fixtureGrant())
				retained := cfg
				retained.launcher.environment = append([]string(nil), cfg.launcher.environment...)
				retained.coreEnvironment = append([]string(nil), cfg.coreEnvironment...)
				retainedBody := bytes.Clone(body)
				before := privateCompositionInputSnapshot(t, cfg)
				var sourceCalls, coreCalls atomic.Int32
				deps := privateCompositionDependencies{
					source: bridgeBrokerFixture{prepare: func(context.Context, ManagerOrigin) (PreparedNotificationBroker, error) {
						sourceCalls.Add(1)
						return nil, errPrivateComposition
					}},
					core: func(context.Context, string, []string, ...string) ([]byte, error) {
						coreCalls.Add(1)
						return nil, errPrivateComposition
					},
				}
				// Let any ticker panic fail the test; do not recover it as refusal.
				r, err := openPrivateComposition(context.Background(), context.Background(), time.Now().Add(10*time.Second), cfg, deps, "launch", body)
				if r != nil {
					t.Cleanup(func() { _ = r.close(context.Background()) })
					t.Error("invalid interval returned a runtime")
				}
				if !errors.Is(err, errPrivateComposition) {
					t.Errorf("invalid interval refusal: %v", err)
				}
				for _, path := range []string{cfg.stateDir, cfg.socket, marker} {
					if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
						t.Errorf("invalid interval created resource %s: %v", path, err)
					}
				}
				if sourceCalls.Load() != 0 || coreCalls.Load() != 0 {
					t.Error("invalid interval reached Source or Core")
				}
				if !reflect.DeepEqual(cfg, retained) || !bytes.Equal(body, retainedBody) {
					t.Error("invalid interval changed retained configuration or body")
				}
				if !reflect.DeepEqual(before, privateCompositionInputSnapshot(t, cfg)) {
					t.Error("invalid interval changed retained files or created a footprint")
				}
			})
		}
	}
}

// Include the policy, pinned launcher and record tree, as well as any new entry.
func privateCompositionInputSnapshot(t *testing.T, cfg privateCompositionConfig) map[string]struct {
	mode fs.FileMode
	data string
} {
	t.Helper()
	snapshot := make(map[string]struct {
		mode fs.FileMode
		data string
	})
	for _, root := range []string{filepath.Dir(cfg.stateDir), cfg.records} {
		if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			var data []byte
			if !entry.IsDir() {
				data, err = os.ReadFile(path)
				if err != nil {
					return err
				}
			}
			snapshot[path] = struct {
				mode fs.FileMode
				data string
			}{info.Mode(), string(data)}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return snapshot
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
