package sessionmanager

import "context"

// D11: observation shutdown preserves the detached provider. Lifecycle
// ownership stays with the host/fork composition, never a renderer.
type ProviderObservationCloser interface {
	CloseObservation(context.Context) error
}

// Acquisition fences native activity without retiring observation. The fork
// alone durably acknowledges intent and signal-attempt between acquisition and
// SignalIdleStop. An error never erases SignalIdleStop's observed effect.
type ProviderConditionalStop interface {
	AcquireIdleStop(context.Context) (ProviderIdleStopLease, error)
	SignalIdleStop(context.Context, ProviderIdleStopLease, ProviderStopSignal) (ProviderStopOutcome, error)
	ReleaseIdleStop(context.Context, ProviderIdleStopLease) error
}

// A native-issued live transaction identity, bound to one runner instance.
// It is neither a persisted recovery token nor canonical owner authority.
type ProviderIdleStopLease struct{ ID string }
type ProviderStopSignal string
type ProviderStopOutcome string

const (
	ProviderStopSignalTerminate ProviderStopSignal  = "terminate"
	ProviderStopSignalKill      ProviderStopSignal  = "kill"
	ProviderStopRefused         ProviderStopOutcome = "refused"
	ProviderStopSignalSent      ProviderStopOutcome = "signal-sent"
	ProviderStopRetired         ProviderStopOutcome = "retired"
)
