package sessionmanager

import (
	"context"
	"testing"
)

// Capability metadata forwards the actual backend value (not provider/kind).
// Reading it makes no native read/write and grants no lease or goal authority.
func TestHostInputCapabilityForwardsActualBackend(t *testing.T) {
	_, input, _, backend := inputFixture(t, true)
	if !input.SupportsNativeGoals() || backend.reads != 0 || backend.writeCount() != 0 {
		t.Fatal("native metadata not forwarded without effects")
	}
	writer := newAttachmentWriter(context.Background(), input)
	if capability, ok := writer.capability().(interface{ SupportsNativeGoals() bool }); !ok || !capability.SupportsNativeGoals() {
		t.Fatal("provisional metadata not forwarded")
	}
	backend.native = false
	if input.SupportsNativeGoals() || writer.SupportsNativeGoals() || backend.reads != 0 || backend.writeCount() != 0 {
		t.Fatal("false backend capability guessed from native observer/type")
	}
}
