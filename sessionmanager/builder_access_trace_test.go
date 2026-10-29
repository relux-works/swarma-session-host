package sessionmanager

import (
	"strings"
	"testing"
	"time"
)

const (
	traceTestCtxID = "CTX-260731-tr0001"
	traceTestRunID = "RUN-260731-abc123"
)

func traceTestAnchor() string {
	return ContextRepositoryAccessAnchor(traceTestCtxID, traceTestRunID, "claude")
}

func grantedReadEvent(sequence int) ContextRepositoryAccessEvent {
	return ContextRepositoryAccessEvent{
		Sequence:        sequence,
		CtxID:           traceTestCtxID,
		RunID:           traceTestRunID,
		Provider:        "claude",
		ProviderEventID: "evt-1",
		Operation:       ContextRepositoryAccessRead,
		Outcome:         ContextRepositoryAccessGranted,
		Path:            "docs/guide.md",
		ByteOffset:      0,
		ByteLength:      10,
		TotalBytes:      10,
		ContentDigest:   ContextRepositoryAccessContentDigest(traceTestAnchor(), []byte("0123456789")),
		At:              time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC),
	}
}

// TestSealedTraceChainsAndVerifies is the contract's happy path: a chain of
// events that closes at an attestation binding both the count and the digest.
func TestSealedTraceChainsAndVerifies(t *testing.T) {
	anchor := traceTestAnchor()
	var events []ContextRepositoryAccessEvent
	previous := ""
	for sequence := 1; sequence <= 3; sequence++ {
		sealed, err := SealContextRepositoryAccessEvent(
			anchor, previous, grantedReadEvent(sequence))
		if err != nil {
			t.Fatalf("sealing event %d: %v", sequence, err)
		}
		if sealed.PreviousDigest != previous {
			t.Fatalf("event %d chains to %q, want %q", sequence, sealed.PreviousDigest, previous)
		}
		previous = sealed.EventDigest
		events = append(events, sealed)
	}

	attestation, err := AttestContextRepositoryAccessTrace(
		anchor, traceTestCtxID, traceTestRunID, "claude",
		len(events), previous, time.Date(2026, 7, 31, 12, 5, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("attesting the trace: %v", err)
	}
	if err := VerifyContextRepositoryAccessTrace(events, attestation); err != nil {
		t.Fatalf("verifying a well-formed trace: %v", err)
	}
}

// TestEmptyTraceIsAttestedToItsAnchor keeps "the run read nothing" distinct from
// "the trace was lost". Both would otherwise be an empty file.
func TestEmptyTraceIsAttestedToItsAnchor(t *testing.T) {
	anchor := traceTestAnchor()
	attestation, err := AttestContextRepositoryAccessTrace(
		anchor, traceTestCtxID, traceTestRunID, "claude", 0, "", time.Time{},
	)
	if err != nil {
		t.Fatalf("attesting an empty trace: %v", err)
	}
	if attestation.TraceDigest != anchor {
		t.Fatalf("an empty trace digests to %q, want the anchor", attestation.TraceDigest)
	}
	if err := VerifyContextRepositoryAccessTrace(nil, attestation); err != nil {
		t.Fatalf("verifying an empty trace: %v", err)
	}
	// One event against an attestation that says zero must not verify.
	if err := VerifyContextRepositoryAccessTrace(
		[]ContextRepositoryAccessEvent{grantedReadEvent(1)}, attestation,
	); !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("verifying an event against an empty attestation = %v", err)
	}
}

// TestSealRefusesEveryUnrecordableShape is the gate's negative test. Each case
// is an event the trace's guarantees depend on excluding, and each must be
// refused rather than sealed into the chain.
func TestSealRefusesEveryUnrecordableShape(t *testing.T) {
	anchor := traceTestAnchor()
	mutate := func(f func(*ContextRepositoryAccessEvent)) ContextRepositoryAccessEvent {
		event := grantedReadEvent(1)
		f(&event)
		return event
	}

	tests := []struct {
		name  string
		event ContextRepositoryAccessEvent
	}{
		{name: "unmodelled operation", event: mutate(func(e *ContextRepositoryAccessEvent) {
			e.Operation = "exec"
		})},
		{name: "unmodelled outcome", event: mutate(func(e *ContextRepositoryAccessEvent) {
			e.Outcome = "maybe"
		})},
		{name: "no provider event identity", event: mutate(func(e *ContextRepositoryAccessEvent) {
			e.ProviderEventID = ""
		})},
		{name: "no provider", event: mutate(func(e *ContextRepositoryAccessEvent) {
			e.Provider = ""
		})},
		{name: "no sequence", event: mutate(func(e *ContextRepositoryAccessEvent) {
			e.Sequence = 0
		})},
		{name: "granted read with no digest", event: mutate(func(e *ContextRepositoryAccessEvent) {
			e.ContentDigest = ""
		})},
		{name: "granted read with no path", event: mutate(func(e *ContextRepositoryAccessEvent) {
			e.Path = ""
		})},
		{
			// A window ending past the recorded size means the size and the window
			// came from two different observations, and the coverage consumer would
			// credit the base with bytes nobody can show it saw.
			name: "range outside the file it read",
			event: mutate(func(e *ContextRepositoryAccessEvent) {
				e.ByteOffset = 8
				e.ByteLength = 10
				e.TotalBytes = 10
			}),
		},
		{
			name: "granted enumeration with no path spec",
			event: mutate(func(e *ContextRepositoryAccessEvent) {
				e.Operation = ContextRepositoryAccessList
				e.Path = ""
				e.PathSpec = ""
				e.ByteLength = 0
				e.TotalBytes = 0
				e.ContentDigest = ContextRepositoryAccessEnumerationDigest(anchor, nil)
			}),
		},
		{
			name: "denial with no reason",
			event: mutate(func(e *ContextRepositoryAccessEvent) {
				e.Outcome = ContextRepositoryAccessDenied
				e.ContentDigest = ""
				e.ByteLength = 0
				e.TotalBytes = 0
			}),
		},
		{
			name: "denial with an invented reason",
			event: mutate(func(e *ContextRepositoryAccessEvent) {
				e.Outcome = ContextRepositoryAccessDenied
				e.DenialReason = "because"
				e.ContentDigest = ""
				e.ByteLength = 0
				e.TotalBytes = 0
			}),
		},
		{
			// A digest or a size on a denial is an oracle: it confirms a guess about
			// a file the gateway just declined to serve.
			name: "denial carrying content evidence",
			event: mutate(func(e *ContextRepositoryAccessEvent) {
				e.Outcome = ContextRepositoryAccessDenied
				e.DenialReason = ContextRepositoryAccessDenialOutsideAllowlist
			}),
		},
		{
			name: "denial carrying an enumeration",
			event: mutate(func(e *ContextRepositoryAccessEvent) {
				e.Outcome = ContextRepositoryAccessDenied
				e.DenialReason = ContextRepositoryAccessDenialPathEscape
				e.ContentDigest = ""
				e.ByteLength = 0
				e.TotalBytes = 0
				e.Entries = []string{"secrets/key.pem"}
			}),
		},
		{
			name: "failure with no reason",
			event: mutate(func(e *ContextRepositoryAccessEvent) {
				e.Outcome = ContextRepositoryAccessFailed
				e.ContentDigest = ""
			}),
		},
		{
			name: "failure carrying content evidence",
			event: mutate(func(e *ContextRepositoryAccessEvent) {
				e.Outcome = ContextRepositoryAccessFailed
				e.FailureReason = "not_found"
			}),
		},
		{name: "invalid run id", event: mutate(func(e *ContextRepositoryAccessEvent) {
			e.RunID = "not-a-run"
		})},
		{
			name: "unbounded provider event identity",
			event: mutate(func(e *ContextRepositoryAccessEvent) {
				e.ProviderEventID = strings.Repeat("x", builderTraceTextLimit+1)
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := SealContextRepositoryAccessEvent(anchor, "", test.event); err == nil {
				t.Fatal("an unrecordable event was sealed into the chain")
			}
		})
	}

	// The control: without it, the cases above would pass on a Seal that refused
	// everything.
	if _, err := SealContextRepositoryAccessEvent(anchor, "", grantedReadEvent(1)); err != nil {
		t.Fatalf("a well-formed event was refused: %v", err)
	}
}

// TestSealRequiresAnAnchor keeps a trace from being chained under nothing, which
// would let its digests be replayed under any anchor at all.
func TestSealRequiresAnAnchor(t *testing.T) {
	if _, err := SealContextRepositoryAccessEvent(
		"", "", grantedReadEvent(1),
	); !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("sealing without an anchor = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

// TestAttestationMustMatchItsOwnAnchor stops a run from closing a trace that
// belongs to a different context, run or provider.
func TestAttestationMustMatchItsOwnAnchor(t *testing.T) {
	foreign := ContextRepositoryAccessAnchor(
		"CTX-260731-tr0002", traceTestRunID, "claude")
	if _, err := AttestContextRepositoryAccessTrace(
		foreign, traceTestCtxID, traceTestRunID, "claude", 0, "", time.Time{},
	); !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
		t.Fatalf("attesting under a foreign anchor = %v, want %s",
			err, ContextErrorBuilderTraceUnavailable)
	}
}

// TestAnchorsSeparateContextsRunsAndProviders is what makes an anchored digest
// non-transferable: evidence minted for one run must not verify for another.
func TestAnchorsSeparateContextsRunsAndProviders(t *testing.T) {
	base := traceTestAnchor()
	for _, other := range []string{
		ContextRepositoryAccessAnchor("CTX-260731-tr0002", traceTestRunID, "claude"),
		ContextRepositoryAccessAnchor(traceTestCtxID, "RUN-260731-zzz999", "claude"),
		ContextRepositoryAccessAnchor(traceTestCtxID, traceTestRunID, "codex"),
	} {
		if other == base {
			t.Fatal("two different runs share a trace anchor")
		}
	}
	// The same identifiers always produce the same anchor, or a consumer could
	// never recompute it.
	if ContextRepositoryAccessAnchor(
		strings.ToLower(traceTestCtxID), strings.ToLower(traceTestRunID), "claude",
	) != base {
		t.Fatal("the anchor is not stable under identifier normalization")
	}
}

// TestEnumerationDigestDescribesTheSetNotTheOrder keeps a filesystem's return
// order out of the evidence, while keeping an empty enumeration distinguishable
// from a non-empty one.
func TestEnumerationDigestDescribesTheSetNotTheOrder(t *testing.T) {
	anchor := traceTestAnchor()
	forward := ContextRepositoryAccessEnumerationDigest(anchor, []string{"a.md", "b.md"})
	reversed := ContextRepositoryAccessEnumerationDigest(anchor, []string{"b.md", "a.md"})
	if forward != reversed {
		t.Fatal("the enumeration digest depends on the order entries arrived in")
	}
	if duplicated := ContextRepositoryAccessEnumerationDigest(
		anchor, []string{"a.md", "b.md", "a.md"},
	); duplicated != forward {
		t.Fatal("the enumeration digest counts duplicates")
	}
	if empty := ContextRepositoryAccessEnumerationDigest(anchor, nil); empty == forward {
		t.Fatal("an empty enumeration digests the same as a non-empty one")
	}
	// A different set must digest differently, or the evidence would not
	// distinguish what the builder saw.
	if other := ContextRepositoryAccessEnumerationDigest(
		anchor, []string{"a.md", "c.md"},
	); other == forward {
		t.Fatal("two different enumerations share a digest")
	}
}

// TestVerifyRejectsEveryChainDefect is the consumer-side gate's negative test.
func TestVerifyRejectsEveryChainDefect(t *testing.T) {
	anchor := traceTestAnchor()
	first, err := SealContextRepositoryAccessEvent(anchor, "", grantedReadEvent(1))
	if err != nil {
		t.Fatal(err)
	}
	second, err := SealContextRepositoryAccessEvent(anchor, first.EventDigest, grantedReadEvent(2))
	if err != nil {
		t.Fatal(err)
	}
	events := []ContextRepositoryAccessEvent{first, second}
	attestation, err := AttestContextRepositoryAccessTrace(
		anchor, traceTestCtxID, traceTestRunID, "claude",
		2, second.EventDigest, time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyContextRepositoryAccessTrace(events, attestation); err != nil {
		t.Fatalf("the control trace does not verify: %v", err)
	}

	tests := []struct {
		name        string
		events      []ContextRepositoryAccessEvent
		attestation ContextRepositoryAccessAttestation
	}{
		{
			name:        "count disagrees with the events",
			events:      events[:1],
			attestation: attestation,
		},
		{
			// This case isolates the count check. Every other defect below also
			// breaks the chain digest, so without an attestation whose chain is
			// intact and whose count is wrong, removing the count comparison
			// entirely would leave the suite green.
			name:   "count inflated over an otherwise intact chain",
			events: events,
			attestation: func() ContextRepositoryAccessAttestation {
				broken := attestation
				broken.EventCount = 5
				return broken
			}(),
		},
		{
			name: "an event was edited",
			events: func() []ContextRepositoryAccessEvent {
				edited := []ContextRepositoryAccessEvent{first, second}
				edited[0].ByteLength = 1
				return edited
			}(),
			attestation: attestation,
		},
		{
			name:        "the events were reordered",
			events:      []ContextRepositoryAccessEvent{second, first},
			attestation: attestation,
		},
		{
			name:   "the chain does not end where the attestation says",
			events: events,
			attestation: func() ContextRepositoryAccessAttestation {
				broken := attestation
				broken.TraceDigest = first.EventDigest
				return broken
			}(),
		},
		{
			name:   "the attestation declares another contract",
			events: events,
			attestation: func() ContextRepositoryAccessAttestation {
				broken := attestation
				broken.ContractVersion = "repository-access-trace-v2"
				return broken
			}(),
		},
		{
			name:   "the attestation anchor does not describe itself",
			events: events,
			attestation: func() ContextRepositoryAccessAttestation {
				broken := attestation
				broken.Anchor = ContextRepositoryAccessAnchor(
					"CTX-260731-tr0002", traceTestRunID, "claude")
				return broken
			}(),
		},
		{
			name: "an event declares another contract",
			events: func() []ContextRepositoryAccessEvent {
				edited := []ContextRepositoryAccessEvent{first, second}
				edited[1].ContractVersion = "repository-access-trace-v2"
				return edited
			}(),
			attestation: attestation,
		},
		{
			name: "a sequence number was renumbered",
			events: func() []ContextRepositoryAccessEvent {
				edited := []ContextRepositoryAccessEvent{first, second}
				edited[1].Sequence = 3
				return edited
			}(),
			attestation: attestation,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := VerifyContextRepositoryAccessTrace(
				test.events, test.attestation,
			); !builderIsContextErrorCode(err, ContextErrorBuilderTraceUnavailable) {
				t.Fatalf("verification = %v, want %s", err, ContextErrorBuilderTraceUnavailable)
			}
		})
	}
}
