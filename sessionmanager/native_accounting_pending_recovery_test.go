package sessionmanager

import (
	"context"
	"errors"
	"sort"
	"testing"
)

// Actual write-then-error initial commits retain real runners, exact legacy
// images and pending owner fences. Every sibling remains inaccessible until
// its OWN hydration; failure preserves earlier success and retryable remainder.
// Consumer/native I/O alone are probes; host/codec/storage/epochs are real.
func TestNativeAccountingPendingRecoveryIsPerSessionAndRetryable(t *testing.T) {
	for _, mode := range []string{"all-valid", "first-fails", "second-fails"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			factory := &rev5CountingFactory{}
			h := rev5Host(t, factory)
			write := h.records.commit
			fault := errors.New("initial write outcome uncertain")
			h.records.commit = func(path string, raw []byte) error {
				if e := write(path, raw); e != nil {
					return e
				}
				return fault
			}
			for _, owner := range []OwnerKey{"parent/primary", "run/RUN-RECOVERY"} {
				if out, e := h.Attach(ctx, AttachRequest{Owner: owner, Provider: "muse"}); out != nil || !errors.Is(e, ErrLegacyCommitUncertain) || !errors.Is(e, fault) {
					t.Fatalf("actual pending fixture not reached: %+v %v", out, e)
				}
			}
			h.records.commit = write
			ids := make([]string, 0, 2)
			leases := map[string]ControllerLease{}
			for id, pending := range h.pendingInitial {
				ids = append(ids, id)
				leases[id] = ControllerLease{SessionID: id, Epoch: pending.epoch}
			}
			sort.Strings(ids)
			if len(ids) != 2 || len(h.inputs) != 0 || factory.launches != 2 {
				t.Fatal("pending fixture lost actual retained state")
			}
			hydrateFault := errors.New("selected native hydration refused")
			calls := map[string]int{}
			failID := ""
			if mode == "first-fails" {
				failID = ids[0]
			}
			if mode == "second-fails" {
				failID = ids[1]
			}
			probe := &nativeRestoreBoundaryProbe{}
			probe.hydrate = func(_ context.Context, cut CapturedOwnerRun) error {
				for _, other := range ids {
					if calls[other] == 0 && other != cut.Lease.SessionID {
						if _, writer, e := h.Input(ctx, other); !errors.Is(e, ErrLegacyCommitUncertain) || writer != nil {
							t.Errorf("unhydrated sibling input admitted: %s %v", other, e)
						}
						if capture, e := h.OwnerRunState(ctx, leases[other]); !errors.Is(e, ErrProviderHostNotFound) || capture.Input != nil {
							t.Errorf("unhydrated sibling owner source admitted: %s %v", other, e)
						}
						if access, e := h.AcquireAttachment(ctx, other, "unissued"); !errors.Is(e, ErrProviderHostNotFound) || access != nil {
							t.Errorf("unhydrated sibling attachment admitted: %s %v", other, e)
						}
					}
				}
				calls[cut.Lease.SessionID]++
				if cut.Lease.SessionID == failID {
					return hydrateFault
				}
				return nil
			}
			h.options.OwnerRuns = probe
			e := h.Recover(ctx)
			if mode == "all-valid" {
				if e != nil || calls[ids[0]] != 1 || calls[ids[1]] != 1 || len(h.pendingInitial) != 0 {
					t.Fatalf("all-valid recovery failed: %v %v", e, calls)
				}
			} else {
				if !errors.Is(e, hydrateFault) || h.sessions[failID].State != SessionRecovering || h.runners[failID] == nil || h.inputs[failID] != nil {
					t.Fatalf("failed hydration lost recovery custody: %v", e)
				}
				if mode == "first-fails" {
					if _, writer, e := h.Input(ctx, ids[1]); !errors.Is(e, ErrLegacyCommitUncertain) || writer != nil || calls[ids[1]] != 0 || h.inputs[ids[1]] != nil {
						t.Fatalf("failure exposed untouched sibling: %v %v", e, calls)
					}
				} else {
					if _, writer, e := h.Input(ctx, ids[0]); e != nil || writer == nil || calls[ids[0]] != 1 {
						t.Fatalf("later failure rolled back ready sibling: %v %v", e, calls)
					}
				}
				failID = ""
				if e = h.Recover(ctx); e != nil {
					t.Fatalf("actual same-host retry stranded recovery: %v", e)
				}
			}
			for _, id := range ids {
				lease, input, e := h.Input(ctx, id)
				if e != nil || input == nil || h.nativeAccountingReady[id] != lease {
					t.Fatalf("successful per-id hydration not admitted: %s %v", id, e)
				}
			}
			if factory.launches != 2 || factory.restores != 0 {
				t.Fatal("pending recovery relaunched/restored retained natives")
			}
		})
	}
}
