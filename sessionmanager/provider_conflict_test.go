package sessionmanager

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

type conflictBoundaryFactory struct{ calls int }

func (f *conflictBoundaryFactory) Provider() string { return "codex" }
func (f *conflictBoundaryFactory) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	f.calls++
	return nil, ErrHostCapabilityUnavailable
}
func (f *conflictBoundaryFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	f.calls++
	return nil, ErrHostCapabilityUnavailable
}

// Ordinary and staged attach disclose the original typed provider conflict,
// before writes/epochs/native effects. Same-provider reuse still succeeds.
// This checks the core error contract, not takeover liveness or remote identity.
func TestHostProviderConflictRetainsTypedCodeWithoutEffects(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "staged"}[staged], func(t *testing.T) {
			ctx := context.Background()
			h, c, r, id := hostCommitProbeHost(t)
			other := &conflictBoundaryFactory{}
			h.options.Factories = append(h.options.Factories, other)
			before := append([]byte(nil), h.records.bases[id].Bytes...)
			input := h.inputs[id]
			lease := h.attachmentControllers[id]
			acks := c.acks
			clients := len(h.attachments)
			q := AttachRequest{Owner: h.sessions[id].Owner, Provider: "codex"}
			var e error
			if staged {
				_, e = h.PrepareAttach(ctx, q)
			} else {
				_, e = h.Attach(ctx, q)
			}
			var public *PublicError
			if !errors.As(e, &public) || public.Code != ErrorSessionProviderConflict {
				t.Fatalf("provider conflict lost original typed code: %v", e)
			}
			if other.calls != 0 || c.acks != acks || len(h.attachments) != clients || len(h.preparations) != 0 || r.forces != 0 || h.inputs[id] != input || h.attachmentControllers[id] != lease || !bytes.Equal(before, h.records.bases[id].Bytes) {
				t.Fatal("provider conflict changed incumbent")
			}
			q.Provider = "muse"
			if staged {
				p, err := h.PrepareAttach(ctx, q)
				if err != nil {
					t.Fatal(err)
				}
				_, e = h.CommitAttach(ctx, AttachmentCommit{Token: p.Token, Lease: p.Lease, BaseSHA256: p.BaseSHA256})
			} else {
				_, e = h.Attach(ctx, q)
			}
			if e != nil || len(h.attachments) != clients+1 || other.calls != 0 || r.forces != 0 {
				t.Fatal("same-provider positive refused or switched provider", e)
			}
		})
	}
}
