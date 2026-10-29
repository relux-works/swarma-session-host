package sessionmanager

import (
	"context"
	"errors"
	"os"
	"testing"
)

type expectedLeaseFactory struct {
	hostCommitProbeFactory
	launches int
}

func (f *expectedLeaseFactory) Launch(ctx context.Context, q ProviderLaunchRequest) (RunnerHost, error) {
	f.launches++
	return f.hostCommitProbeFactory.Launch(ctx, q)
}

// Explicit absence and exact current controller are valid positive controls.
// A changed slot/epoch (including a concurrent attach after an absence read)
// refuses before launch, epoch allocation, record ACK or client publication.
// The pin is a comparison only, not verified-caller/launch authority.
func TestHostAttachExpectedOwnerLease(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "staged"}[staged], func(t *testing.T) {
			ctx := context.Background()
			h, c, r, q, _ := attachmentProbe(t)
			f := &expectedLeaseFactory{hostCommitProbeFactory: hostCommitProbeFactory{runner: r}}
			h.options.Factories = []ProviderHostFactory{f}
			zero := ControllerLease{}
			q.ExpectedOwnerLease = &zero
			var id string
			if staged {
				p, e := h.PrepareAttach(ctx, q)
				if e != nil {
					t.Fatal(e)
				}
				a, e := h.CommitAttach(ctx, attachmentCommit(p))
				if e != nil {
					t.Fatal(e)
				}
				id = a.Session.SessionID
			} else {
				a, e := h.Attach(ctx, q)
				if e != nil {
					t.Fatal(e)
				}
				id = a.Session.SessionID
			}
			if f.launches != 1 {
				t.Fatal("valid empty pin did not launch once")
			}
			_, lease, live, e := h.OwnerSession(ctx, q.Owner)
			if e != nil || !live {
				t.Fatalf("current pin unavailable: %v", e)
			}
			run := func(q AttachRequest) error {
				if staged {
					p, e := h.PrepareAttach(ctx, q)
					if e != nil {
						return e
					}
					_, e = h.CommitAttach(ctx, attachmentCommit(p))
					return e
				}
				_, e := h.Attach(ctx, q)
				return e
			}
			q.ExpectedOwnerLease = &lease
			if e = run(q); e != nil || f.launches != 1 {
				t.Fatalf("current pin refused/relaunched: %v", e)
			}
			journal := h.options.Layout.StateDir + "/" + controllerEpochFile
			before, e := os.ReadFile(journal)
			if e != nil {
				t.Fatal(e)
			}
			acks, clients := c.acks, len(h.attachments)
			for _, bad := range []ControllerLease{{}, {SessionID: id, Epoch: lease.Epoch + 1}, {SessionID: "another-session", Epoch: lease.Epoch}, {SessionID: id}} {
				q.ExpectedOwnerLease = &bad
				e = run(q)
				want := ErrStaleInputController
				if bad.SessionID != "" && bad.Epoch == 0 {
					want = ErrInvalidInput
				}
				after, readErr := os.ReadFile(journal)
				if !errors.Is(e, want) || f.launches != 1 || c.acks != acks || len(h.attachments) != clients || len(h.preparations) != 0 || readErr != nil || string(before) != string(after) {
					t.Fatalf("stale pin crossed prelaunch boundary: %+v %v launches%d acks%d clients%d", bad, e, f.launches, c.acks, len(h.attachments))
				}
			}
			// An externally replaced durable generation invalidates even the old local
			// input object's same-epoch view. No fabricated lease can revive it.
			if _, e = h.epochs.reserveContext(ctx, id); e != nil {
				t.Fatal(e)
			}
			q.ExpectedOwnerLease = &lease
			if e = run(q); !errors.Is(e, ErrStaleInputController) || f.launches != 1 || c.acks != acks {
				t.Fatalf("replaced epoch crossed admission: %v", e)
			}
		})
	}
}
