//go:build unix

package sessionmanager

import (
	"context"
	"testing"
)

// Test-only export seams let the external original tests use the actual goal
// Service without an import cycle. They expose fixtures, not production grants.
type LegacyPhysicalTestFactory = legacyPhysicalFactory
type LegacyPhysicalTestRunner = legacyPhysicalRunner
type LegacyPhysicalTestConfig = hostCommitProbeConfig

func LegacyPhysicalTestLayout(t *testing.T, name string) Layout { return legacyPhysicalLayout(t, name) }
func LegacyPhysicalTestFactoryFor(t *testing.T, provider string) *LegacyPhysicalTestFactory {
	f := legacyPhysicalNewFactory(t, provider)
	return f
}
func LegacyPhysicalTestLaunchCount(f *LegacyPhysicalTestFactory) int { return f.launchCount() }
func LegacyPhysicalTestHost(f *LegacyPhysicalTestFactory, id string) *LegacyPhysicalTestRunner {
	return f.host(id)
}
func LegacyPhysicalTestFailState(r *LegacyPhysicalTestRunner, e error) { r.failState(e) }
func LegacyPhysicalTestOwnerSlot(h *SessionHost, owner OwnerKey) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var holder string
	for _, record := range h.sessions {
		if record.Owner == owner && record.State != SessionStopped {
			if holder != "" {
				return "multiple-owner-records"
			}
			holder = record.SessionID
		}
	}
	return holder
}
func LegacyPhysicalTestServer(h *SessionHost) (*ControlServer, error) {
	return legacyPhysicalNewServer(h)
}
func LegacyPhysicalTestStartServer(t *testing.T, s *ControlServer) (context.CancelFunc, <-chan error) {
	return legacyPhysicalStartServer(t, s)
}

// An absent goal delivery is refused, never implemented by a canned ACK. This
// backend only supplies the host's real current-controller binding prerequisite.
type legacyPhysicalGoalRunner struct{ *legacyPhysicalRunner }

func (*legacyPhysicalGoalRunner) SupportsNativeGoals() bool         { return false }
func (*legacyPhysicalGoalRunner) AcceptsInput(InputSubmission) bool { return false }
func (*legacyPhysicalGoalRunner) ObserveNativeGoal(context.Context) (NativeGoalObservation, error) {
	return NativeGoalObservation{}, ErrHostCapabilityUnavailable
}
func (*legacyPhysicalGoalRunner) ExecuteInput(context.Context, InputSubmission, InputReceipt) (func(context.Context) (InputObservation, error), error) {
	return nil, ErrHostCapabilityUnavailable
}

type LegacyPhysicalGoalFactory struct{ *legacyPhysicalFactory }

func (f LegacyPhysicalGoalFactory) Launch(ctx context.Context, q ProviderLaunchRequest) (RunnerHost, error) {
	r, e := f.legacyPhysicalFactory.Launch(ctx, q)
	if e != nil {
		return nil, e
	}
	return &legacyPhysicalGoalRunner{r.(*legacyPhysicalRunner)}, nil
}
func (f LegacyPhysicalGoalFactory) Restore(ctx context.Context, q ProviderHostRecord) (RunnerHost, error) {
	r, e := f.legacyPhysicalFactory.Restore(ctx, q)
	if e != nil {
		return nil, e
	}
	return &legacyPhysicalGoalRunner{r.(*legacyPhysicalRunner)}, nil
}
func LegacyPhysicalTestGoalFactory(f *LegacyPhysicalTestFactory) ProviderHostFactory {
	return LegacyPhysicalGoalFactory{f}
}
