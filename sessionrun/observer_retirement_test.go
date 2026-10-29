package sessionrun

import (
	"context"
	"errors"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

// The runtime cannot certify a worker join without its actual configured
// factory; a configured PTY factory truthfully reports no native observer.
func TestRuntimeObserverRetirementUsesActualFactory(t *testing.T) {
	lease := core.ControllerLease{SessionID: "session", Epoch: 1}
	if joined, err := New(nil).RetireGoalObserver(context.Background(), lease); joined || !errors.Is(err, core.ErrHostCapabilityUnavailable) {
		t.Fatalf("missing factory faked retirement: %t %v", joined, err)
	}
	f := newActivePTYSystem(t)
	if joined, err := f.runtime.RetireGoalObserver(context.Background(), f.lease); joined || err != nil {
		t.Fatalf("PTY invented native observer join: %t %v", joined, err)
	}
}
