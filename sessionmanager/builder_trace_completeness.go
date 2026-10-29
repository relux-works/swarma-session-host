package sessionmanager

import (
	"context"
	"fmt"
)

// BuilderToolObservationReporter must be implemented by the actual launched
// native provider observer. Shim counters alone do not satisfy this contract.
type BuilderToolObservationReporter interface {
	BuilderToolObservation(context.Context) (BuilderToolObservation, error)
}

// BuilderToolObservation is what the adapter saw on the provider's own tool
// stream during the build.
//
// It is the reconciliation input. The gateway knows how many accesses it served;
// only the provider knows how many tool calls the turn made. Comparing them is
// what detects the failure the trace cannot detect on its own — a tool call that
// happened and never arrived — and it is why the trace alone is not sufficient
// evidence.
type BuilderToolObservation struct {
	// Reported is false when the adapter could not observe the stream at all.
	// Silence is not zero: an unobserved stream proves nothing about what ran.
	Reported bool
	// GatewayCalls is the number of gateway tool calls the provider recorded.
	GatewayCalls int
	// ForeignToolCalls is the number of calls to anything else. Under a correctly
	// composed policy it is zero, and a non-zero value means the deny set did not
	// hold at runtime whatever the launch reported.
	ForeignToolCalls int
	// UnknownShapes counts stream records the adapter could not classify. Like the
	// assay's unreadable blocks, they are not evidence of absence.
	UnknownShapes int
	// Disconnected marks a provider that stopped reporting before the run ended,
	// which leaves the tail of the stream unobserved.
	Disconnected bool
}

// requireBuilderTraceCompleteness reconciles the gateway's trace against the
// provider's tool stream.
//
// Every branch here is a way the provisional context becomes unpublishable, and
// they are split across the two stable codes by what went wrong rather than by
// where it was noticed: a surface that stayed reachable or a call to it is a
// tool-isolation failure, while a missing, unreadable or unreconcilable record
// is a trace failure. A consumer branches on the code to know whether to fix the
// launch or the machine.
func requireBuilderTraceCompleteness(
	ctxID string,
	observation BuilderToolObservation,
	attestation ContextRepositoryAccessAttestation,
) error {
	normalized := builderNormalizeContextID(ctxID)
	if !observation.Reported {
		return &BuilderTraceError{
			Code: ContextErrorBuilderToolIsolationUnavailable,
			Message: fmt.Sprintf(
				"the provider adapter could not observe the tool stream for %s, so nothing reconciles the "+
					"repository-access trace against the calls the run actually made",
				normalized,
			),
		}
	}
	if observation.Disconnected {
		return &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the provider disconnected before the context-builder run for %s finished, so the tail of "+
					"its tool stream is unobserved and the trace cannot be shown to be complete",
				normalized,
			),
		}
	}
	if observation.UnknownShapes > 0 {
		return &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the provider tool stream for %s carried %d record(s) this adapter cannot classify, which "+
					"is not the same observation as no tool call having happened",
				normalized, observation.UnknownShapes,
			),
		}
	}
	if observation.ForeignToolCalls > 0 {
		return &BuilderTraceError{
			Code: ContextErrorBuilderToolIsolationUnavailable,
			Message: fmt.Sprintf(
				"the context-builder run for %s made %d tool call(s) outside the evidence gateway, so the "+
					"trace describes only part of what it read",
				normalized, observation.ForeignToolCalls,
			),
		}
	}
	if attestation.ContractVersion != ContextRepositoryAccessTraceContract {
		return &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the context-builder run for %s produced no repository-access trace attestation", normalized,
			),
		}
	}
	if observation.GatewayCalls != attestation.EventCount {
		return &BuilderTraceError{
			Code: ContextErrorBuilderTraceUnavailable,
			Message: fmt.Sprintf(
				"the provider recorded %d gateway call(s) for %s but its trace attests %d, so at least one "+
					"access was never captured",
				observation.GatewayCalls, normalized, attestation.EventCount,
			),
		}
	}
	return nil
}
