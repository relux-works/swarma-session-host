package sessionmanager

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
)

// Reviewer attack B1: every sole-writer/record state the contract lists as
// non-current refuses with an empty snapshot and no record/native/input effect.
// Positive control: the unmodified host reads successfully first.
func TestRev5BSourceStateRefusals(t *testing.T) {
	ctx := context.Background()
	h0, in0, l0, _ := codexSourceHost(t)
	if _, err := in0.CodexOwnerRunSnapshot(ctx, l0); err != nil {
		t.Fatal("positive control refused", err)
	}
	_ = h0
	cases := map[string]struct {
		mut  func(*SessionHost, *sessionInput, *ControllerLease, *codexSourceNative)
		want error
	}{
		"pending-initial":     {func(h *SessionHost, _ *sessionInput, l *ControllerLease, _ *codexSourceNative) { h.records.mu.Lock(); if h.records.pendingInitial == nil { h.records.pendingInitial = map[string]LegacyRecordInput{} }; h.records.pendingInitial[l.SessionID] = h.records.bases[l.SessionID]; h.records.mu.Unlock() }, ErrLegacyCommitUncertain},
		"pending-replacement": {func(h *SessionHost, _ *sessionInput, l *ControllerLease, _ *codexSourceNative) { h.records.mu.Lock(); if h.records.pendingReplacement == nil { h.records.pendingReplacement = map[string]LegacyRecordInput{} }; h.records.pendingReplacement[l.SessionID] = h.records.bases[l.SessionID]; h.records.mu.Unlock() }, ErrLegacyCommitUncertain},
		"no-ack-base":         {func(h *SessionHost, _ *sessionInput, l *ControllerLease, _ *codexSourceNative) { h.records.mu.Lock(); delete(h.records.bases, l.SessionID); h.records.mu.Unlock() }, ErrLegacyViewStale},
		"epoch-zero":          {func(_ *SessionHost, _ *sessionInput, l *ControllerLease, _ *codexSourceNative) { l.Epoch = 0 }, ErrStaleInputController},
		"older-epoch":         {func(_ *SessionHost, _ *sessionInput, l *ControllerLease, _ *codexSourceNative) { l.Epoch-- }, ErrStaleInputController},
		"valid-other-image": {func(h *SessionHost, _ *sessionInput, l *ControllerLease, _ *codexSourceNative) {
			p, _ := h.records.path(l.SessionID)
			b, _ := os.ReadFile(p)
			_ = os.WriteFile(p, append(b, '\n'), 0600)
		}, ErrLegacyViewStale},
		"missing-image": {func(h *SessionHost, _ *sessionInput, l *ControllerLease, _ *codexSourceNative) {
			p, _ := h.records.path(l.SessionID)
			_ = os.Remove(p)
		}, ErrLegacyViewStale},
		"native-pid": {func(_ *SessionHost, _ *sessionInput, _ *ControllerLease, n *codexSourceNative) {
			n.native.mu.Lock(); n.native.snapshot.PID++; n.native.mu.Unlock()
		}, ErrLegacyViewStale},
		"native-turn": {func(_ *SessionHost, _ *sessionInput, _ *ControllerLease, n *codexSourceNative) {
			n.native.mu.Lock(); n.native.snapshot.ManagedTurnID = "turn-foreign"; n.native.mu.Unlock()
		}, ErrLegacyViewStale},
		"native-owner-fact": {func(_ *SessionHost, _ *sessionInput, _ *ControllerLease, n *codexSourceNative) {
			n.native.mu.Lock(); n.native.snapshot.OwnerRun = codexOwnerFactFixture(); n.native.mu.Unlock()
		}, ErrLegacyViewStale},
		"native-budget": {func(_ *SessionHost, _ *sessionInput, _ *ControllerLease, n *codexSourceNative) {
			v := int64(0); n.native.mu.Lock(); n.native.snapshot.GoalTokenBudget = &v; n.native.mu.Unlock()
		}, ErrLegacyViewStale},
		"native-stopped": {func(_ *SessionHost, _ *sessionInput, _ *ControllerLease, n *codexSourceNative) {
			n.native.mu.Lock(); n.native.snapshot.Stopped = true; n.native.mu.Unlock()
		}, ErrLegacyViewStale},
		"input-revoked": {func(_ *SessionHost, in *sessionInput, _ *ControllerLease, _ *codexSourceNative) {
			_ = in.revokeAndDrain(context.Background())
		}, ErrStaleInputController},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			h, in, lease, native := codexSourceHost(t)
			path, _ := h.records.path(lease.SessionID)
			c.mut(h, in, &lease, native)
			before, _ := os.ReadFile(path)
			got, err := in.CodexOwnerRunSnapshot(ctx, lease)
			after, _ := os.ReadFile(path)
			if !errors.Is(err, c.want) || !reflect.DeepEqual(got, CodexRunnerSnapshot{}) || !bytes.Equal(before, after) || native.writeCount() != 0 || len(in.entries) != 0 {
				t.Fatalf("%s: got err=%v want %v snapshot=%+v", name, err, c.want, got)
			}
		})
	}
}

// Reviewer attack B2: a committed image replaced while the read is inside the
// native snapshot must refuse (second current-bytes check), not return the
// pre-replacement fact. Uncertainty published concurrently by another
// goroutine is observed by the next read.
func TestRev5BImageReplacedDuringRead(t *testing.T) {
	ctx := context.Background()
	h, in, lease, native := codexSourceHost(t)
	path, _ := h.records.path(lease.SessionID)
	native.snapshotHook = func() {
		b, _ := os.ReadFile(path)
		_ = os.WriteFile(path, append(b, ' '), 0600)
	}
	if got, err := in.CodexOwnerRunSnapshot(ctx, lease); !errors.Is(err, ErrLegacyViewStale) || !reflect.DeepEqual(got, CodexRunnerSnapshot{}) {
		t.Fatalf("replacement during read admitted: %v", err)
	}
}

// Reviewer attack B3 (run under -race): uncertainty publication from Recover's
// error path and concurrent map-free source reads must be synchronized.
func TestRev5BConcurrentUncertaintyPublication(t *testing.T) {
	ctx := context.Background()
	h, in, lease, _ := codexSourceHost(t)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = in.CodexOwnerRunSnapshot(ctx, lease)
			}
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); for j := 0; j < 50; j++ { h.records.markUncertain(lease.SessionID) } }()
	wg.Wait()
	if _, err := in.CodexOwnerRunSnapshot(ctx, lease); !errors.Is(err, ErrLegacyCommitUncertain) {
		t.Fatalf("published uncertainty not observed: %v", err)
	}
}

type rev5PlainWriter struct{ HostInputWriter }

// Reviewer attack B4: wrappers over a parent that is not a source, and a
// scoped writer bound to another epoch/session, cannot synthesize a fact.
func TestRev5BWrapperEscapes(t *testing.T) {
	ctx := context.Background()
	_, in, lease, _ := codexSourceHost(t)
	w := newAttachmentWriter(ctx, rev5PlainWriter{in})
	if _, err := w.CodexOwnerRunSnapshot(ctx, lease); !errors.Is(err, ErrHostCapabilityUnavailable) {
		t.Fatalf("non-source parent produced fact: %v", err)
	}
	var nilSource *sessionInput
	w2 := newAttachmentWriter(ctx, nilSource)
	if _, err := w2.CodexOwnerRunSnapshot(ctx, lease); !errors.Is(err, ErrHostCapabilityUnavailable) {
		t.Fatalf("typed-nil parent produced fact: %v", err)
	}
	s := &scopedInputWriter{host: in, ctx: ctx, observation: NativeGoalObservation{SessionID: lease.SessionID, Epoch: lease.Epoch + 1}, active: true}
	if _, err := s.CodexOwnerRunSnapshot(ctx, lease); !errors.Is(err, ErrStaleInputController) {
		t.Fatalf("scoped writer accepted foreign lease: %v", err)
	}
	s.observation.Epoch = lease.Epoch
	if _, err := s.CodexOwnerRunSnapshot(ctx, lease); err != nil {
		t.Fatalf("scoped positive control refused: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	s.ctx = cctx
	cancel()
	if _, err := s.CodexOwnerRunSnapshot(ctx, lease); !errors.Is(err, context.Canceled) {
		t.Fatalf("scoped writer ignored its closed lifetime: %v", err)
	}
}
