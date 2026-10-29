package sessionmanager

import (
	"context"
	"errors"
	"testing"
)

type coreGoalBinding struct {
	ctx    context.Context
	lease  ControllerLease
	writer HostInputWriter
	calls  *int
}

func (b coreGoalBinding) Apply(*GoalEnvelope) error {
	*b.calls++
	r, e := b.writer.Submit(b.ctx, textInput(b.lease, "goal-reconcile", "fixture goal"))
	if e != nil {
		return e
	}
	_, e = b.writer.Await(b.ctx, r)
	return e
}
func (b coreGoalBinding) Rollback(e *GoalEnvelope) error { return b.Apply(e) }

type coreGoalSource struct {
	calls   int
	fail    error
	pending bool
}

func (s *coreGoalSource) Get(OwnerKey) (*GoalRead, error)                  { return &GoalRead{}, nil }
func (s *coreGoalSource) ConfirmNativeApplied(OwnerKey, RevisionRef) error { return nil }
func (s *coreGoalSource) Reconcile(owner OwnerKey, b GoalBinding) (*GoalActivation, error) {
	s.calls++
	if s.fail != nil {
		return nil, s.fail
	}
	if e := b.Apply(&GoalEnvelope{Owner: owner}); e != nil {
		return nil, e
	}
	return &GoalActivation{Ref: &RevisionRef{ID: "fixture-ref", Revision: 1}, Pending: s.pending}, nil
}

type coreGoalFactory struct {
	onBind func(context.Context, ControllerLease) error
	calls  int
	writes int
	leases []ControllerLease
	fail   error
}

func (f *coreGoalFactory) BindGoal(ctx context.Context, l ControllerLease, provider string, w HostInputWriter) (OwnerKey, GoalBinding, error) {
	f.calls++
	f.leases = append(f.leases, l)
	if f.onBind != nil {
		if err := f.onBind(ctx, l); err != nil {
			return "", nil, err
		}
	}
	if f.fail != nil {
		return "", nil, f.fail
	}
	if l.SessionID == "" || l.Epoch == 0 || provider != "muse" || w == nil {
		return "", nil, ErrHostCapabilityUnavailable
	}
	return OwnerKey("session/" + l.SessionID), coreGoalBinding{ctx, l, w, &f.writes}, nil
}

// Core orchestration uses the newly installed exact-session writer before any
// attachment is published. The service/factory doubles establish call order and
// commit facts, not external caller authority or native budget enforcement.
func TestHostGoalBindingOrdinaryStagedAndRecovery(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "staged"}[staged], func(t *testing.T) {
			h, _, _, id := hostCommitProbeHost(t)
			source := &coreGoalSource{}
			factory := &coreGoalFactory{}
			h.options.Goals = source
			h.options.GoalBindings = factory
			q := AttachRequest{Owner: h.sessions[id].Owner, Provider: "muse"}
			before := len(h.attachments)
			if staged {
				p, e := h.PrepareAttach(context.Background(), q)
				if e != nil {
					t.Fatal(e)
				}
				if source.calls != 0 || len(h.attachments) != before {
					t.Fatal("prepared handle reconciled or published early")
				}
				if _, e = h.CommitAttach(context.Background(), AttachmentCommit{Token: p.Token, Lease: p.Lease, BaseSHA256: p.BaseSHA256}); e != nil {
					t.Fatal(e)
				}
			} else {
				if _, e := h.Attach(context.Background(), q); e != nil {
					t.Fatal(e)
				}
			}
			if source.calls != 1 || factory.writes != 1 || len(h.attachments) != before+1 || h.sessions[id].AppliedGoalRef == nil {
				t.Fatalf("unreconciled attach: calls=%d writes=%d attachments=%d ref=%v", source.calls, factory.writes, len(h.attachments), h.sessions[id].AppliedGoalRef)
			}
			old := factory.leases[0]
			if e := h.Recover(context.Background()); e != nil {
				t.Fatal(e)
			}
			if source.calls != 2 || factory.leases[1] != old {
				t.Fatalf("live recovery did not bind its actual current controller: %+v", factory.leases)
			}
			// A live unchanged writer retains its epoch. Actual lost-input recovery
			// must instead reserve a higher controller and request a new binding.
			if e := h.inputs[id].revokeAndDrain(context.Background()); e != nil {
				t.Fatal(e)
			}
			delete(h.inputs, id)
			if e := h.Recover(context.Background()); e != nil {
				t.Fatal(e)
			}
			if source.calls != 3 || factory.leases[2].SessionID != old.SessionID || factory.leases[2].Epoch <= old.Epoch {
				t.Fatalf("lost-input recovery reused old controller: %+v", factory.leases)
			}
		})
	}
}

// Missing supplier and failed/pending reconciliation never produce another
// attachment or an applied ref. An actual callback error retains managed runtime
// in recovering state with input revoked, rather than inventing native success.
func TestHostGoalBindingFailureNeverPublishesAttachment(t *testing.T) {
	for _, kind := range []string{"missing", "binding-error", "source-error", "pending"} {
		t.Run(kind, func(t *testing.T) {
			h, _, runner, id := hostCommitProbeHost(t)
			source := &coreGoalSource{}
			factory := &coreGoalFactory{}
			h.options.Goals = source
			h.options.GoalBindings = factory
			fault := errors.New("fixture unavailable")
			switch kind {
			case "missing":
				h.options.GoalBindings = nil
			case "binding-error":
				factory.fail = fault
			case "source-error":
				source.fail = fault
			case "pending":
				source.pending = true
			}
			before := len(h.attachments)
			_, e := h.Attach(context.Background(), AttachRequest{Owner: h.sessions[id].Owner, Provider: "muse"})
			if e == nil || len(h.attachments) != before || h.sessions[id].AppliedGoalRef != nil {
				t.Fatalf("failed goal published attachment: err=%v attachments=%d", e, len(h.attachments))
			}
			if kind != "missing" {
				if h.sessions[id].State != SessionRecovering || h.inputs[id] != nil || runner.forces != 0 {
					t.Fatalf("failed goal cleanup: state=%s input=%v stops=%d", h.sessions[id].State, h.inputs[id], runner.forces)
				}
			}
		})
	}
}

type coreDisabledGoalGate struct{}

func (coreDisabledGoalGate) DisabledGoalPlane(OwnerKey) (string, error) {
	return "fixture.disable", nil
}

// The original disable switch neither reads nor mutates the service head. A
// previously applied ref is forgotten only after its bound rollback succeeds.
func TestHostGoalDisabledPlaneClearsNativeBeforeForgettingRef(t *testing.T) {
	for _, prior := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbound", true: "bound"}[prior], func(t *testing.T) {
			h, _, _, id := hostCommitProbeHost(t)
			source := &coreGoalSource{}
			factory := &coreGoalFactory{}
			h.options.Goals = source
			h.options.GoalBindings = factory
			h.options.Gate = coreDisabledGoalGate{}
			if prior {
				r := h.sessions[id]
				r.AppliedGoalRef = &RevisionRef{ID: "old-ref", Revision: 1}
				if err := h.updateRecord(context.Background(), r); err != nil {
					t.Fatal(err)
				}
				h.sessions[id] = r
			}
			if _, err := h.Attach(context.Background(), AttachRequest{Owner: h.sessions[id].Owner, Provider: "muse"}); err != nil {
				t.Fatal(err)
			}
			want := 0
			if prior {
				want = 1
			}
			if source.calls != 0 || factory.writes != want || h.sessions[id].AppliedGoalRef != nil {
				t.Fatalf("disabled plane changed head/skipped native clear: service=%d writes=%d ref=%v", source.calls, factory.writes, h.sessions[id].AppliedGoalRef)
			}
		})
	}
}

// The service may read the actual host without a map-lock deadlock; replacing
// its captured controller before delivery refuses with zero native writes.
func TestHostGoalCallbackReadsHostAndRejectsChangedController(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(map[bool]string{false: "read-current", true: "changed-controller"}[change], func(t *testing.T) {
			h, _, native, id := hostCommitProbeHost(t)
			source := &coreGoalSource{}
			factory := &coreGoalFactory{}
			h.options.Goals = source
			h.options.GoalBindings = factory
			factory.onBind = func(ctx context.Context, l ControllerLease) error {
				if _, err := h.Session(ctx, id); err != nil {
					return err
				}
				if change {
					_, err := h.epochs.reserveContext(ctx, id)
					return err
				}
				return nil
			}
			before := len(h.attachments)
			_, err := h.Attach(context.Background(), AttachRequest{Owner: h.sessions[id].Owner, Provider: "muse"})
			if change {
				if !errors.Is(err, ErrStaleInputController) || len(h.attachments) != before || native.writeCount() != 0 || h.sessions[id].AppliedGoalRef != nil {
					t.Fatalf("changed controller got effect/attachment: err=%v native=%d", err, native.writeCount())
				}
				return
			}
			if err != nil || source.calls != 1 || native.writeCount() != 1 || len(h.attachments) != before+1 {
				t.Fatalf("host read/goal positive: err=%v native=%d", err, native.writeCount())
			}
		})
	}
}
