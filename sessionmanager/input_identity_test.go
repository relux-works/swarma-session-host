package sessionmanager

import (
	"context"
	"testing"
)

type identityInputBackend struct {
	*inputTestBackend
	identity      string
	identityReads int
}

func (b *identityInputBackend) NativeIdentity() string { b.identityReads++; return b.identity }

// Ordinary and provisional writers expose the actual provider observation
// before public attachment, without Host.OwnerRunState/map re-entry. Unknown or
// closed scopes return no identity, never a guessed session ID; reading grants
// no goal/caller authority and cannot submit/acknowledge input.
func TestHostInputNativeIdentityObservation(t *testing.T) {
	ctx, input, _, backend := inputFixture(t, false)
	if input.NativeIdentity() != "" || backend.writeCount() != 0 {
		t.Fatal("missing provider identity guessed")
	}
	actual := &identityInputBackend{inputTestBackend: backend, identity: "actual-provider-session"}
	input.backend = actual
	if input.NativeIdentity() != actual.identity || backend.writeCount() != 0 {
		t.Fatal("actual native identity not forwarded")
	}
	staged := newAttachmentWriter(ctx, input)
	projected, ok := staged.capability().(ProviderNativeIdentity)
	if !ok || projected.NativeIdentity() != actual.identity {
		t.Fatal("provisional capability dropped actual identity")
	}
	if e := staged.close(context.Background()); e != nil {
		t.Fatal(e)
	}
	reads := actual.identityReads
	if projected.NativeIdentity() != "" || actual.identityReads != reads {
		t.Fatal("closed scope read provider identity")
	}
	if e := input.revokeAndDrain(ctx); e != nil {
		t.Fatal(e)
	}
	reads = actual.identityReads
	if input.NativeIdentity() != "" || actual.identityReads != reads || backend.writeCount() != 0 {
		t.Fatal("revoked input exposed identity or wrote")
	}
}
