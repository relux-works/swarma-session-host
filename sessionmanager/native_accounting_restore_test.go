package sessionmanager

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

type nativeRestoreBoundaryProbe struct {
	capturedRunFunc
	validate func(context.Context, RunnerRecord, LegacyRecordInput) error
	hydrate  func(context.Context, CapturedOwnerRun) error
}

func (p *nativeRestoreBoundaryProbe) ValidateMuseAccountingRestore(c context.Context, r RunnerRecord, b LegacyRecordInput) error {
	if p.validate != nil {
		return p.validate(c, r, b)
	}
	return nil
}
func (p *nativeRestoreBoundaryProbe) RestoreMuseAccounting(c context.Context, s CapturedOwnerRun) error {
	if p.hydrate != nil {
		return p.hydrate(c, s)
	}
	return nil
}

// This tests the core callback boundary and real input/record reservations,
// not native accounting hydration or codec admission. No native gate is widened.
// The real consumer must separately qualify full Store/RestoreFold semantics.
func TestNativeAccountingRestoreBoundaryIsUnlockedScopedAndOnce(t *testing.T) {
	h, store, lease := accountingRecordsProbe(t)
	ctx := context.Background()
	calls := 0
	p := &nativeRestoreBoundaryProbe{}
	h.options.OwnerRuns = p
	p.hydrate = func(call context.Context, cut CapturedOwnerRun) error {
		calls++
		if cut.Lease != lease || cut.Input == nil || cut.Native != h.runners[lease.SessionID] {
			t.Fatal("not actual cut")
		}
		if _, e := h.List(call); e != nil {
			t.Fatal("callback retained map lock", e)
		}
		if _, _, e := h.Input(ctx, lease.SessionID); !errors.Is(e, ErrLegacyCommitUncertain) {
			t.Fatalf("outside caller entered hydration: %v", e)
		}
		if _, e := store.Read(ctx, lease); !errors.Is(e, ErrLegacyCommitUncertain) {
			t.Fatalf("outside accounting read entered hydration: %v", e)
		}
		if _, e := store.Read(call, lease); e != nil {
			t.Fatal("actual scoped accounting source deadlocked/refused", e)
		}
		if _, e := h.OwnerRunState(call, lease); e != nil {
			t.Fatal("real renderer source refused", e)
		}
		if _, e := h.AcquireAttachment(ctx, lease.SessionID, ownerRunAttachment(t, h, lease.SessionID)); !errors.Is(e, ErrLegacyCommitUncertain) {
			t.Fatalf("attachment entered hydration: %v", e)
		}
		return nil
	}
	h.mu.Lock()
	e := h.restoreNativeAccountingInstalledLocked(ctx, lease.SessionID)
	h.mu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	h.mu.Lock()
	e = h.restoreNativeAccountingInstalledLocked(ctx, lease.SessionID)
	h.mu.Unlock()
	if e != nil || calls != 1 {
		t.Fatalf("same lease rehydrated %d %v", calls, e)
	}
	if _, _, e = h.Input(ctx, lease.SessionID); e != nil {
		t.Fatal("successful scope did not reopen input", e)
	}
}

// A failed real consumer retains its runner/committed image, revokes input,
// records recovery and releases reservations. It does not force or relaunch.
func TestNativeAccountingRestoreFailureRetainsRecovery(t *testing.T) {
	h, _, lease := accountingRecordsProbe(t)
	ctx := context.Background()
	native := h.runners[lease.SessionID]
	fault := errors.New("restore refused")
	h.options.OwnerRuns = &nativeRestoreBoundaryProbe{hydrate: func(context.Context, CapturedOwnerRun) error { return fault }}
	h.mu.Lock()
	e := h.restoreNativeAccountingInstalledLocked(ctx, lease.SessionID)
	h.mu.Unlock()
	if !errors.Is(e, fault) || h.runners[lease.SessionID] != native || h.inputs[lease.SessionID] != nil || h.sessions[lease.SessionID].State != SessionRecovering || len(h.nativeAccountingRestores) != 0 || len(h.ownerOperations) != 0 {
		t.Fatalf("recovery/reservation lost: %v", e)
	}
	if _, _, e = h.Input(ctx, lease.SessionID); e == nil {
		t.Fatal("failed hydration exposed input")
	}
}

// Original-source validation runs before any Restore callback, permits map
// reads, and refuses a changed committed cut without invoking hydration.
func TestNativeAccountingRestorePreflightBeforePhysicalEffect(t *testing.T) {
	h, _, lease := accountingRecordsProbe(t)
	ctx := context.Background()
	fault := errors.New("full image not admitted")
	calls := 0
	h.options.OwnerRuns = &nativeRestoreBoundaryProbe{validate: func(call context.Context, r RunnerRecord, b LegacyRecordInput) error {
		calls++
		if r.SessionID != lease.SessionID || b.SHA256 == "" {
			t.Fatal("not actual committed source")
		}
		_, e := h.List(call)
		if e != nil {
			t.Fatal(e)
		}
		return fault
	}, hydrate: func(context.Context, CapturedOwnerRun) error {
		t.Fatal("hydration after preflight refusal")
		return nil
	}}
	h.mu.Lock()
	e := h.validateNativeAccountingRestoreLocked(ctx, lease.SessionID)
	h.mu.Unlock()
	if !errors.Is(e, fault) || calls != 1 || len(h.nativeAccountingRestores) != 0 {
		t.Fatalf("failed preflight leaked reservation: %v", e)
	}
}

type nativeRestoreCountingFactory struct {
	accountingRecordFactory
	restores int
}

func (f *nativeRestoreCountingFactory) Restore(ctx context.Context, r ProviderHostRecord) (RunnerHost, error) {
	f.restores++
	return f.accountingRecordFactory.Restore(ctx, r)
}

// Actual public Recover must stop before Factory.Restore on preflight failure;
// its nearby valid control restores once, then joins real scoped hydration.
func TestNativeAccountingRecoverCallsConsumerBeforeRestore(t *testing.T) {
	old, _, lease := accountingRecordsProbe(t)
	ctx := context.Background()
	options := old.options
	f := &nativeRestoreCountingFactory{accountingRecordFactory: options.Factories[0].(accountingRecordFactory)}
	options.Factories = []ProviderHostFactory{f}
	old.Close()
	fault := errors.New("actual preflight refused")
	hydrates := 0
	probe := &nativeRestoreBoundaryProbe{validate: func(context.Context, RunnerRecord, LegacyRecordInput) error { return fault }, hydrate: func(context.Context, CapturedOwnerRun) error { hydrates++; return nil }}
	options.OwnerRuns = probe
	h, e := NewHost(options)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(h.Close)
	if e = h.Recover(ctx); !errors.Is(e, fault) || f.restores != 0 || hydrates != 0 || h.inputs[lease.SessionID] != nil {
		t.Fatalf("preflight crossed real Restore: %v restores%d hydrate%d", e, f.restores, hydrates)
	}
	probe.validate = nil
	probe.hydrate = func(call context.Context, cut CapturedOwnerRun) error {
		hydrates++
		_, e := h.OwnerRunState(call, cut.Lease)
		return e
	}
	if e = h.Recover(ctx); e != nil || f.restores != 1 || hydrates != 1 {
		t.Fatalf("valid real Restore/hydration failed: %v restores%d hydrate%d", e, f.restores, hydrates)
	}
}

// Both ordinary and provisional Attach must refuse before a public attachment
// or provisional writer escapes when the real hydration boundary fails.
func TestNativeAccountingAttachFailurePublishesNoWriter(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "prepared-existing"}[prepared], func(t *testing.T) {
			old, _, _ := accountingRecordsProbe(t)
			options := old.options
			fault := errors.New("hydrate refused")
			probe := &nativeRestoreBoundaryProbe{hydrate: func(context.Context, CapturedOwnerRun) error { return fault }}
			h := old
			if prepared {
				h.options.OwnerRuns = probe
			} else {
				old.Close()
				root := t.TempDir()
				options.Layout.StateDir = root
				options.Layout.SessionsDir = filepath.Join(root, "sessions")
				options.Layout.LockFile = filepath.Join(root, "manager.lock")
				options.OwnerRuns = probe
				var e error
				h, e = NewHost(options)
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(h.Close)
			}
			beforeAttachments := len(h.attachments)
			q := AttachRequest{Owner: "parent/primary", Provider: "muse"}
			// Existing staged reuse is admitted by the real codec. Fresh provisional
			// creation separately requires its actual discard acknowledger; no fake one.
			if prepared {
				p, e := h.PrepareAttach(context.Background(), q)
				if !errors.Is(e, fault) || p.Writer != nil || p.Token != "" {
					t.Fatalf("provisional writer escaped: %+v %v", p, e)
				}
			} else {
				a, e := h.Attach(context.Background(), q)
				if !errors.Is(e, fault) || a != nil {
					t.Fatalf("attachment escaped: %+v %v", a, e)
				}
			}
			if len(h.attachments) != beforeAttachments || len(h.inputs) != 0 || len(h.nativeAccountingRestores) != 0 {
				t.Fatal("failed hydration retained new publication/admission")
			}
		})
	}
}
