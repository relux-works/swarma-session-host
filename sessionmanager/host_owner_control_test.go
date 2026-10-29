package sessionmanager

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Actual per-owner activation excludes attach/reconcile until exact release or
// original TTL expiry; a foreign lease cannot open another owner's boundary.
func TestHostOwnerActivationFencesAndExpires(t *testing.T) {
	h, _, _, id := hostCommitProbeHost(t)
	owner := h.sessions[id].Owner
	ctx := context.Background()
	captured, _, e := h.Input(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	h.options.Goals = &coreGoalSource{}
	h.options.GoalBindings = &coreGoalFactory{}
	lease, live, e := h.BeginOwnerActivation(ctx, owner, captured)
	if e != nil || lease == "" || !live {
		t.Fatalf("actual live activation: %q %v %v", lease, live, e)
	}
	if _, _, e = h.BeginOwnerActivation(ctx, owner, captured); e == nil {
		t.Fatal("duplicate activation admitted")
	}
	for _, wrong := range []string{"foreign", lease} {
		if wrong == "foreign" {
			if e = h.EndOwnerActivation(owner, wrong); e != nil {
				t.Fatal(e)
			}
		}
		call, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		_, e = h.Attach(call, AttachRequest{Owner: owner, Provider: "muse"})
		cancel()
		if !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("activation did not fence attach: %v", e)
		}
	}
	if e = h.EndOwnerActivation(owner, lease); e != nil {
		t.Fatal(e)
	}
	if e = h.EndOwnerActivation(owner, lease); e != nil {
		t.Fatal("end was not idempotent")
	}
	if _, e = h.Attach(ctx, AttachRequest{Owner: owner, Provider: "muse"}); e != nil {
		t.Fatal(e)
	}
	lease, _, e = h.BeginOwnerActivation(ctx, owner, captured)
	if e != nil {
		t.Fatal(e)
	}
	h.mu.Lock()
	h.ownerActivations[owner].expires = time.Now().Add(-time.Second)
	h.mu.Unlock()
	if _, e = h.Attach(ctx, AttachRequest{Owner: owner, Provider: "muse"}); e != nil {
		t.Fatalf("expired lease held boundary: %v", e)
	}
	if _, live, e = h.BeginOwnerActivation(ctx, "another-owner", ControllerLease{}); e != nil || live {
		t.Fatalf("disjoint empty owner: %v %v", live, e)
	}
}

// The scoped real binding uses the current admitted input, allows map reads,
// keeps attach out during native callback and cannot be reused after return.
// Fixtures supply transport/goal service boundaries, not caller authentication.
func TestHostOwnerBindingScopeAndOperation(t *testing.T) {
	h, _, _, id := hostCommitProbeHost(t)
	ctx := context.Background()
	owner := h.sessions[id].Owner
	captured, _, leaseErr := h.Input(ctx, id)
	if leaseErr != nil {
		t.Fatal(leaseErr)
	}
	source := &coreGoalSource{}
	factory := &coreGoalFactory{}
	h.options.Goals = source
	h.options.GoalBindings = factory
	var escaped GoalBinding
	e := h.WithOwnerGoalBinding(ctx, owner, captured, func(b OwnerGoalBinding) error {
		if b.Session.SessionID != id || b.Lease.SessionID != id || b.Owner == "" {
			t.Fatal("binding not actual session/controller")
		}
		if _, e := h.Session(ctx, id); e != nil {
			t.Fatalf("callback held host map lock: %v", e)
		}
		call, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		defer cancel()
		if _, e := h.Attach(call, AttachRequest{Owner: owner, Provider: "muse"}); !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("attach crossed binding operation: %v", e)
		}
		escaped = b.Binding
		return b.Binding.Apply(&GoalEnvelope{Owner: b.Owner})
	})
	if e != nil || factory.writes != 1 {
		t.Fatalf("real scoped apply: %v writes=%d", e, factory.writes)
	}
	if e = escaped.Apply(&GoalEnvelope{}); !errors.Is(e, ErrInputScopeClosed) || factory.writes != 1 {
		t.Fatalf("escaped binding effect: %v", e)
	}
	if _, e = h.Attach(ctx, AttachRequest{Owner: owner, Provider: "muse"}); e != nil {
		t.Fatalf("operation not released: %v", e)
	}
}

// Missing/failed binding, disabled plane and stale controller never yield a
// callable native binding. Reconcile uses the actual configured goal service.
func TestHostOwnerBindingRefusalsAndReconcile(t *testing.T) {
	h, _, _, id := hostCommitProbeHost(t)
	ctx := context.Background()
	owner := h.sessions[id].Owner
	captured, _, leaseErr := h.Input(ctx, id)
	if leaseErr != nil {
		t.Fatal(leaseErr)
	}
	calls := 0
	cb := func(OwnerGoalBinding) error { calls++; return nil }
	if e := h.WithOwnerGoalBinding(ctx, owner, captured, cb); !errors.Is(e, ErrHostCapabilityUnavailable) {
		t.Fatalf("missing service admitted: %v", e)
	}
	source := &coreGoalSource{}
	factory := &coreGoalFactory{fail: errors.New("binding refused")}
	h.options.Goals = source
	h.options.GoalBindings = factory
	if e := h.WithOwnerGoalBinding(ctx, owner, captured, cb); e == nil || calls != 0 {
		t.Fatal("failed factory exposed callback")
	}
	factory.fail = nil
	beforeFactory := factory.calls
	wrong := captured
	wrong.SessionID = "SES-wrong-session-000001"
	if err := h.WithOwnerGoalBinding(ctx, owner, wrong, cb); !errors.Is(err, ErrStaleInputController) || factory.calls != beforeFactory || calls != 0 {
		t.Fatalf("stale target reached factory: %v", err)
	}

	h.options.Gate = coreDisabledGoalGate{}
	if e := h.WithOwnerGoalBinding(ctx, owner, captured, cb); e == nil || calls != 0 {
		t.Fatal("disabled goal plane exposed callback")
	}
	h.options.Gate = nil
	e := h.WithOwnerGoalBinding(ctx, owner, captured, func(b OwnerGoalBinding) error {
		calls++
		_, err := h.epochs.reserveContext(ctx, id)
		return err
	})
	if !errors.Is(e, ErrStaleInputController) {
		t.Fatalf("changed lease accepted: %v", e)
	}
	if calls != 1 {
		t.Fatal("callback invocation count")
	}
	// A fresh independent host is the neighboring admitted reconcile control.
	h2, _, _, id2 := hostCommitProbeHost(t)
	h2.options.Goals = &coreGoalSource{}
	h2.options.GoalBindings = &coreGoalFactory{}
	v, ok, e := h2.ReconcileOwner(ctx, h2.sessions[id2].Owner)
	if e != nil || !ok || v.SessionID != id2 || v.AppliedGoalRef == nil {
		t.Fatalf("actual reconcile: %+v %v %v", v, ok, e)
	}
	if _, ok, e = h2.ReconcileOwner(ctx, "no-live-session"); e != nil || ok {
		t.Fatalf("absent owner fabricated session: %v %v", ok, e)
	}
}

// Read-only mapping supplies the actual lease BEFORE external verified-caller
// admission. A later mismatched pin fails before creating an activation effect.
func TestHostOwnerSessionReadPinsBeforeEffect(t *testing.T) {
	h, c, _, id := hostCommitProbeHost(t)
	ctx := context.Background()
	owner := h.sessions[id].Owner
	before := c.acks
	v, lease, live, e := h.OwnerSession(ctx, owner)
	if e != nil || !live || v.SessionID != id || lease.SessionID != id || lease.Epoch == 0 || c.acks != before {
		t.Fatalf("read mapping changed durable state: %+v %v %v", v, live, e)
	}
	wrong := lease
	wrong.Epoch++
	if _, _, e = h.BeginOwnerActivation(ctx, owner, wrong); !errors.Is(e, ErrStaleInputController) {
		t.Fatalf("stale pin created activation: %v", e)
	}
	if h.ownerActivations[owner] != nil {
		t.Fatal("stale pin retained activation effect")
	}
	if _, _, live, e = h.OwnerSession(ctx, "no-holder"); e != nil || live || c.acks != before {
		t.Fatalf("missing slot fabricated authority: %v %v", live, e)
	}
}

// A service callback panic may be recovered by the HTTP server; it must not
// leave an open binding or a permanent physical owner reservation afterward.
func TestHostOwnerBindingPanicReleasesPhysicalReservation(t *testing.T) {
	h, _, _, id := hostCommitProbeHost(t)
	ctx := context.Background()
	owner := h.sessions[id].Owner
	h.options.Goals = &coreGoalSource{}
	h.options.GoalBindings = &coreGoalFactory{}
	_, captured, _, e := h.OwnerSession(ctx, owner)
	if e != nil {
		t.Fatal(e)
	}
	var escaped GoalBinding
	func() {
		defer func() {
			if got := recover(); got != "callback-fixture" {
				t.Fatalf("callback panic changed: %v", got)
			}
		}()
		_ = h.WithOwnerGoalBinding(ctx, owner, captured, func(b OwnerGoalBinding) error { escaped = b.Binding; panic("callback-fixture") })
	}()
	h.mu.Lock()
	op := h.ownerOperations[owner]
	reserved := h.goalReconciling[id]
	h.mu.Unlock()
	if op != nil || reserved {
		t.Fatal("callback panic retained physical owner reservation")
	}
	if e = escaped.Apply(&GoalEnvelope{}); !errors.Is(e, ErrInputScopeClosed) {
		t.Fatalf("panic left callable binding: %v", e)
	}
}

type ownerGoalStateSource struct {
	coreGoalSource
	applied *RevisionRef
}

func (s *ownerGoalStateSource) Reconcile(o OwnerKey, b GoalBinding) (*GoalActivation, error) {
	// The real service requires its concrete receipt binding; an outer scope
	// wrapper must not replace that actual identity at the service boundary.
	if _, ok := b.(coreGoalBinding); !ok {
		return nil, ErrHostCapabilityUnavailable
	}
	a, e := s.coreGoalSource.Reconcile(o, b)
	if e == nil && !a.Pending {
		ref := *a.Ref
		s.applied = &ref
	}
	return a, e
}
func (s *ownerGoalStateSource) Get(OwnerKey) (*GoalRead, error) {
	return &GoalRead{Latest: s.applied, Applied: s.applied}, nil
}

// Scoped Reconcile keeps the original concrete service binding and promotes
// host metadata only after actual service facts and the sole writer's ACK.
func TestHostOwnerScopedReconcileKeepsServiceBindingAndMetadata(t *testing.T) {
	h, c, _, id := hostCommitProbeHost(t)
	ctx := context.Background()
	owner := h.sessions[id].Owner
	source := &ownerGoalStateSource{}
	h.options.Goals = source
	h.options.GoalBindings = &coreGoalFactory{}
	_, captured, _, e := h.OwnerSession(ctx, owner)
	if e != nil {
		t.Fatal(e)
	}
	before := c.acks
	var escaped OwnerGoalBinding
	e = h.WithOwnerGoalBinding(ctx, owner, captured, func(b OwnerGoalBinding) error {
		escaped = b
		a, err := b.Reconcile()
		if err == nil && (a == nil || a.Ref == nil || a.Pending) {
			t.Fatal("source acknowledgement lost")
		}
		return err
	})
	if e != nil || c.acks != before+1 || source.calls != 1 {
		t.Fatalf("actual source/record commit: %v ack=%d calls=%d", e, c.acks, source.calls)
	}
	v, after, live, e := h.OwnerSession(ctx, owner)
	if e != nil || !live || after != captured || v.AppliedGoalRef == nil || source.applied == nil || *v.AppliedGoalRef != *source.applied {
		t.Fatalf("post-save actual view: %+v %v", v, e)
	}
	if _, e = escaped.Reconcile(); !errors.Is(e, ErrInputScopeClosed) || source.calls != 1 {
		t.Fatalf("escaped reconcile reached service: %v", e)
	}
}
