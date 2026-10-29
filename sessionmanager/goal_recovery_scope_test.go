package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Core-only boundary controls pair actual scoped success with absent hydration,
// binding/pending/cancellation/final-write failures. Storage/epochs/input are
// real; the small supplier probes do not qualify native accounting themselves.
// The external-package subscription regression separately uses the real stack.
func TestGoalRecoverySourceAndSettlementAreScoped(t *testing.T) {
	for _, mode := range []string{"valid", "missing-hydration", "binding-error", "pending", "cancel", "final-write"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h, _, lease := accountingRecordsProbe(t)
			id := lease.SessionID
			native := h.runners[id]
			h.options.OwnerRuns = &nativeRestoreBoundaryProbe{}
			record := h.sessions[id]
			record.State = SessionRecovering
			record.OwnerRunError = "native accounting restoration failed"
			if err := h.updateRecord(ctx, record); err != nil {
				t.Fatal(err)
			}
			h.sessions[id] = record
			h.mu.Lock()
			err := h.restoreNativeAccountingInstalledLocked(ctx, id)
			h.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if mode == "missing-hydration" {
				delete(h.nativeAccountingReady, id)
			}
			if _, err = h.OwnerRunState(context.Background(), lease); !errors.Is(err, ErrHostCapabilityUnavailable) {
				t.Fatalf("public Recovering read admitted: %v", err)
			}
			source := &coreGoalSource{}
			factory := &coreGoalFactory{}
			h.options.Goals = source
			h.options.GoalBindings = factory
			fault := errors.New("actual recovery boundary refused")
			var escaped context.Context
			factory.onBind = func(call context.Context, l ControllerLease) error {
				escaped = call
				if cut, e := h.OwnerRunState(call, l); e != nil || cut.Native != native || cut.Input != h.inputs[id] || cut.Record.State != SessionRecovering {
					t.Fatalf("exact callback recovering source refused: %+v %v", cut, e)
				}
				if _, e := h.OwnerRunState(context.Background(), l); !errors.Is(e, ErrHostCapabilityUnavailable) {
					t.Fatalf("public caller acquired private recovery source: %v", e)
				}
				if _, w, e := h.Input(context.Background(), id); !errors.Is(e, ErrLegacyCommitUncertain) || w != nil {
					t.Fatalf("outside input entered callback recovery: %v", e)
				}
				stale := l
				stale.Epoch++
				if _, e := h.OwnerRunState(call, stale); !errors.Is(e, ErrStaleInputController) {
					t.Fatalf("foreign epoch admitted: %v", e)
				}
				if mode == "binding-error" {
					return fault
				}
				if mode == "cancel" {
					cancel()
					return ctx.Err()
				}
				return nil
			}
			source.pending = mode == "pending"
			commit := h.records.commit
			writes := 0
			if mode == "final-write" {
				h.records.commit = func(path string, raw []byte) error {
					writes++
					var image struct {
						State SessionState `json:"state"`
					}
					if err := json.Unmarshal(raw, &image); err != nil {
						return err
					}
					if image.State == SessionRunning {
						return fault
					}
					return commit(path, raw)
				}
			}
			h.mu.Lock()
			err = h.reconcileInstalledHostGoalLocked(ctx, id)
			h.mu.Unlock()
			h.records.commit = commit
			if mode == "valid" {
				if err != nil || h.sessions[id].State != SessionRunning || h.sessions[id].OwnerRunError != "" || h.inputs[id] == nil {
					t.Fatalf("actual settled recovery not published: %v", err)
				}
				// An escaped callback context must lose its special recovering admission.
				r := h.sessions[id]
				r.State = SessionRecovering
				if e := h.updateRecord(context.Background(), r); e != nil {
					t.Fatal(e)
				}
				h.sessions[id] = r
				if _, e := h.OwnerRunState(escaped, lease); !errors.Is(e, ErrHostCapabilityUnavailable) {
					t.Fatalf("escaped recovery scope remained active: %v", e)
				}
			} else {
				if err == nil || h.runners[id] != native || h.sessions[id].State == SessionRunning {
					t.Fatalf("failed recovery became success/lost native: %v", err)
				}
				if mode != "missing-hydration" && h.inputs[id] != nil {
					t.Fatalf("failed recovery exposed input: %v", err)
				}
				if mode == "binding-error" && !errors.Is(err, fault) {
					t.Fatalf("binding cause lost: %v", err)
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation lost: %v", err)
				}
				if mode == "final-write" && (!errors.Is(err, fault) || writes == 0) {
					t.Fatalf("actual final-write failure not observed: %v", err)
				}
			}
			if len(h.goalRecoveries) != 0 || len(h.ownerOperations) != 0 || len(h.nativeAccountingRestores) != 0 {
				t.Fatal("recovery callback reservation leaked")
			}
		})
	}
}

type recoveryStateRunner struct {
	*accountingRecordRunner
	state ProviderHostState
	err   error
}

func (r *recoveryStateRunner) State(context.Context) (ProviderHostState, error) {
	return r.state, r.err
}

// Reinstall admits only the same acknowledged record and genuinely running
// retained process. Unknown liveness, foreign disk bytes and retirement refuse
// before controller mutation; no native launch/stop is part of this operation.
func TestRetainedRecoveryInputRequiresCurrentLiveSource(t *testing.T) {
	for _, mode := range []string{"valid", "unknown", "state-error", "changed-image", "retiring"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h, _, lease := accountingRecordsProbe(t)
			id := lease.SessionID
			old := h.inputs[id]
			if err := old.revokeAndDrain(ctx); err != nil {
				t.Fatal(err)
			}
			delete(h.inputs, id)
			runner := &recoveryStateRunner{accountingRecordRunner: h.runners[id].(*accountingRecordRunner), state: ProviderHostRunning}
			h.runners[id] = runner
			fault := errors.New("native liveness observation unavailable")
			if mode == "unknown" {
				runner.state = ProviderHostState("unknown")
			}
			if mode == "state-error" {
				runner.err = fault
			}
			recordPath, _ := h.records.path(id)
			raw, err := os.ReadFile(recordPath)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "changed-image" {
				if err = os.WriteFile(recordPath, append(raw, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "retiring" {
				r := h.sessions[id]
				r.GracefulRetirement = true
				h.sessions[id] = r
			}
			epochPath := filepath.Join(h.options.Layout.StateDir, "controller-epochs.v1.json")
			before, err := os.ReadFile(epochPath)
			if err != nil {
				t.Fatal(err)
			}
			h.mu.Lock()
			err = h.restoreRetainedInputLocked(ctx, id)
			h.mu.Unlock()
			if mode == "valid" {
				if err != nil || h.inputs[id] == nil {
					t.Fatalf("valid retained source refused: %v", err)
				}
				return
			}
			if mode == "state-error" && !errors.Is(err, fault) {
				t.Fatalf("liveness cause lost: %v", err)
			}
			if mode == "changed-image" && !errors.Is(err, ErrLegacyViewStale) {
				t.Fatalf("changed disk source not specifically refused: %v", err)
			}
			if (mode == "unknown" || mode == "retiring") && !errors.Is(err, ErrHostCapabilityUnavailable) {
				t.Fatalf("unavailable native admitted: %v", err)
			}
			after, readErr := os.ReadFile(epochPath)
			if readErr != nil || !bytes.Equal(before, after) || h.inputs[id] != nil || h.runners[id] != runner {
				t.Fatalf("refused retained source changed custody/controller: %v %v", err, readErr)
			}
		})
	}
}
