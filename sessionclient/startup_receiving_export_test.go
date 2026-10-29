//go:build linux

package sessionclient

import (
	"testing"
	"time"
)

// Test-only bridge reuses the actual owned-helper source and PIDbirth custody.
// The receiving external test can compose the real host/default imports without
// creating an import cycle or exporting a production launch authorization API.
type StartupReceivingFixture struct{ layout startupTestLayout }

func NewStartupReceivingFixture(t *testing.T, name string) *StartupReceivingFixture {
	declareOutsideShell(t)
	return &StartupReceivingFixture{layout: staleTakeoverTestLayout(t, name)}
}
func (f *StartupReceivingFixture) Layout() StartupLayout { return f.layout.StartupLayout }
func (f *StartupReceivingFixture) Options(launch func() error) StartupOptions {
	return StartupOptions{Source: &fixtureLaunchSource{f.layout.source, launch}, RecordEvent: f.layout.source.record}
}

type StartupReceivingHolder struct{ holder *wedgedHolderProcess }

func (f *StartupReceivingFixture) StartHolder(t *testing.T, mode string) *StartupReceivingHolder {
	return &StartupReceivingHolder{startSessionManagerHolderProcess(t, f.layout, mode)}
}
func (h *StartupReceivingHolder) PID() int { return h.holder.pid }
func (h *StartupReceivingHolder) WaitForExit(t *testing.T, timeout time.Duration) {
	h.holder.waitForExit(t, timeout)
}
func StartupReceivingOriginalTimeouts() func() {
	health, grace, force, poll := staleInstanceHealthCheckTimeout, staleInstanceTerminateGrace, staleInstanceForceGrace, staleInstancePollInterval
	staleInstanceHealthCheckTimeout = 75 * time.Millisecond
	staleInstanceTerminateGrace = 100 * time.Millisecond
	staleInstanceForceGrace = time.Second
	staleInstancePollInterval = 10 * time.Millisecond
	return func() {
		staleInstanceHealthCheckTimeout, staleInstanceTerminateGrace, staleInstanceForceGrace, staleInstancePollInterval = health, grace, force, poll
	}
}
