package sessionmanager

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
)

type retireObserverBoundary struct {
	retire func(context.Context, ControllerLease) (bool, error)
}

func (p retireObserverBoundary) BindGoal(context.Context, ControllerLease, string, HostInputWriter) (OwnerKey, GoalBinding, error) {
	return "", nil, ErrHostCapabilityUnavailable
}
func (p retireObserverBoundary) RetireGoalObserver(ctx context.Context, l ControllerLease) (bool, error) {
	return p.retire(ctx, l)
}

// These isolate core ordering/reservations against a selected capability. The
// positive joins one actual fixture goroutine; actual factory/store behavior is
// qualified separately by real-consumer integration and its owner tests.
func TestHostGoalObserverRetirementBeforeStopAndClose(t *testing.T) {
	for _, closing := range []bool{false, true} {
		t.Run(map[bool]string{false: "stop", true: "close"}[closing], func(t *testing.T) {
			h, _, lease := accountingRecordsProbe(t)
			input := h.inputs[lease.SessionID]
			worker, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); <-worker.Done() }()
			t.Cleanup(cancel)
			calls := 0
			h.options.GoalBindings = retireObserverBoundary{retire: func(ctx context.Context, actual ControllerLease) (bool, error) {
				calls++
				if actual != lease {
					t.Fatal("retirement guessed a controller")
				}
				if !h.mu.TryLock() {
					t.Fatal("retirement callback retained map lock")
				}
				h.mu.Unlock()
				if input.lifeCtx.Err() != nil {
					t.Fatal("retirement followed input lifetime cancellation")
				}
				if !h.epochs.admitsContext(ctx, lease.SessionID, lease.Epoch) {
					t.Fatal("retirement followed epoch revocation")
				}
				_, writer, e := h.Input(ctx, lease.SessionID)
				want := ErrLegacyCommitUncertain
				if closing {
					want = ErrHostClosed
				}
				if !errors.Is(e, want) || writer != nil {
					t.Fatalf("retirement did not fence public admission: %v", e)
				}
				cancel()
				select {
				case <-done:
					return true, nil
				case <-ctx.Done():
					return false, ctx.Err()
				}
			}}
			if closing {
				h.Close()
				if !h.closeReleased || input.lifeCtx.Err() == nil {
					t.Fatal("Close did not join then release")
				}
			} else {
				if _, e := h.Stop(context.Background(), lease.SessionID); e != nil {
					t.Fatal(e)
				}
			}
			if calls != 1 {
				t.Fatalf("exact observer not retired once: %d", calls)
			}
		})
	}
}

// A real callback refusal leaves record/epoch/native admission unchanged. Close
// keeps the lifetime lock on failure; a later successful exact retry releases it.
func TestHostGoalObserverRetirementFailurePreservesCustody(t *testing.T) {
	for _, closing := range []bool{false, true} {
		t.Run(map[bool]string{false: "stop", true: "close"}[closing], func(t *testing.T) {
			h, _, lease := accountingRecordsProbe(t)
			path, _ := h.records.path(lease.SessionID)
			before, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			epochPath := h.options.Layout.StateDir + "/" + controllerEpochFile
			epoch, e := os.ReadFile(epochPath)
			if e != nil {
				t.Fatal(e)
			}
			fault := errors.New("observer retirement refused")
			h.options.GoalBindings = retireObserverBoundary{retire: func(context.Context, ControllerLease) (bool, error) { return false, fault }}
			if closing {
				h.Close()
				if h.closeReleased {
					t.Fatal("failed observer join released owner lock")
				}
			} else {
				if _, e = h.Stop(context.Background(), lease.SessionID); !errors.Is(e, fault) {
					t.Fatalf("Stop swallowed observer refusal: %v", e)
				}
			}
			after, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			actualEpoch, e := os.ReadFile(epochPath)
			if e != nil {
				t.Fatal(e)
			}
			if string(after) != string(before) || string(actualEpoch) != string(epoch) {
				t.Fatal("failed retirement wrote record or epoch")
			}
			// Registry absence is explicit, not a successful native observation/ACK.
			h.options.GoalBindings = retireObserverBoundary{retire: func(context.Context, ControllerLease) (bool, error) { return false, nil }}
			h.Close()
			if !h.closeReleased {
				t.Fatal("actual retry did not release joined owner lock")
			}
		})
	}
}

// Explicit paired native accounting cannot guess successful retirement when
// its required lifecycle supplier is absent. Refusal precedes epoch/record
// effects; nearby real factory integration proves this is not reject-all.
func TestHostGoalObserverRetirementMissingPairedSource(t *testing.T) {
	h, _, lease := accountingRecordsProbe(t)
	oldCodec := h.options.Codec
	oldRuns := h.options.OwnerRuns
	consumer := &nativeRestoreBoundaryProbe{}
	codec, e := NewLegacyRecordCodecWithMuseAccounting(consumer)
	if e != nil {
		t.Fatal(e)
	}
	h.options.Codec = codec
	h.options.OwnerRuns = consumer
	defer func() { h.options.Codec = oldCodec; h.options.OwnerRuns = oldRuns; h.Close() }()
	path, _ := h.records.path(lease.SessionID)
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	epochPath := h.options.Layout.StateDir + "/" + controllerEpochFile
	epoch, e := os.ReadFile(epochPath)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = h.Stop(context.Background(), lease.SessionID); !errors.Is(e, ErrHostCapabilityUnavailable) {
		t.Fatalf("missing actual retirement source became success: %v", e)
	}
	after, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	actualEpoch, e := os.ReadFile(epochPath)
	if e != nil {
		t.Fatal(e)
	}
	if string(before) != string(after) || string(epoch) != string(actualEpoch) {
		t.Fatal("missing retirement source crossed write boundary")
	}
}

// Failed observer join must still close owned transports/revoke input, while
// retaining the owner lock. A real isolated proxy proves no listener survives
// that failure return; this does not qualify a provider process.
func TestHostGoalObserverRetirementCloseFailureStopsTransport(t *testing.T) {
	h, _, native, q := coreProxyFixture(t)
	native.snapshot = RunnerSnapshot{Kind: "muse-msp", MuseMSP: &MuseRunnerSnapshot{GoalTransport: "native", Provider: "muse", State: ProviderHostRunning, SessionID: "failure-native", WorkspaceRoot: t.TempDir(), ApprovalMode: "allow"}}
	fault := errors.New("observer join failed")
	calls := 0
	h.options.GoalBindings = retireObserverBoundary{retire: func(context.Context, ControllerLease) (bool, error) { calls++; return false, fault }}
	defer func() { h.options.GoalBindings = nil; h.Close() }()
	a, e := h.Attach(context.Background(), q)
	if e != nil {
		t.Fatal(e)
	}
	u, e := url.Parse(a.ManagerEndpoint)
	if e != nil || u.Scheme != "unix" {
		t.Fatal("actual proxy fixture absent", e)
	}
	h.Close()
	if calls != 1 || h.closeReleased {
		t.Fatal("failed selected join lost custody")
	}
	if _, e = os.Lstat(u.Path); !errors.Is(e, os.ErrNotExist) {
		t.Errorf("failed observer Close retained writable transport: %v", e)
	}
	if _, writer, e := h.Input(context.Background(), a.Session.SessionID); !errors.Is(e, ErrHostClosed) || writer != nil {
		t.Errorf("failed Close retained public input: %v", e)
	}
}
