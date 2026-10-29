package sessionmanager

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/relux-works/swarma-session-host/sessionmanager/runner/transcript"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ContextRepositoryAccessTraceContract is the contract of the evidence a
// context-builder run produces about what it read.
//
// It is a separate contract from context-builder-enforcement-v1 because the two
// answer different questions. Enforcement evidence says what the launch boundary
// would have refused; this trace says what the run actually reached for. A base
// can be confined to the right scope and still have read nothing inside it, and
// the coverage manifest downstream of this trace is the only thing that can tell
// those apart.
const ContextRepositoryAccessTraceContract = "repository-access-trace-v1"

// contextRepositoryAccessAnchorDomain separates the trace anchor from every
// other use of the same hash. Without it the anchor would be a plain hash of
// public identifiers, and an event digest computed under one anchor could be
// replayed under another that happened to hash the same inputs.
const contextRepositoryAccessAnchorDomain = ContextRepositoryAccessTraceContract + "\x00anchor"

// ContextRepositoryAccessOperation is the read surface the gateway exposes.
//
// The set is closed on purpose. An operation this package does not model cannot
// be described by an event, and an access nobody can describe is exactly what
// the trace exists to prevent — so an unmodelled operation is a refusal, not a
// new enum value the gateway invents at runtime.
type ContextRepositoryAccessOperation string

const (
	// ContextRepositoryAccessStat reports existence, kind and size without
	// returning content.
	ContextRepositoryAccessStat ContextRepositoryAccessOperation = "stat"
	// ContextRepositoryAccessRead returns an exact byte range of one file.
	ContextRepositoryAccessRead ContextRepositoryAccessOperation = "read"
	// ContextRepositoryAccessList enumerates one directory.
	ContextRepositoryAccessList ContextRepositoryAccessOperation = "list"
	// ContextRepositoryAccessSearch enumerates the files matching a query.
	ContextRepositoryAccessSearch ContextRepositoryAccessOperation = "search"
)

// ContextRepositoryAccessOutcome separates a refusal from a failure.
//
// The distinction is load-bearing for the consumer: "denied" is the gateway
// holding a boundary and means the builder asked for something it may not have,
// while "failed" is an access that was permitted and did not work — a missing
// file, an unreadable one. Collapsing them would let a repository full of
// permission errors look like a repository full of policy violations.
type ContextRepositoryAccessOutcome string

const (
	ContextRepositoryAccessGranted ContextRepositoryAccessOutcome = "granted"
	ContextRepositoryAccessDenied  ContextRepositoryAccessOutcome = "denied"
	ContextRepositoryAccessFailed  ContextRepositoryAccessOutcome = "failed"
)

// Stable denial reasons. They are codes rather than sentences because a
// consumer branches on them and because a formatted message is where a payload
// snippet would eventually be smuggled into the trace.
const (
	// ContextRepositoryAccessDenialPathEscape is a request whose spelling leaves
	// the repository: an absolute path, or one with a traversal segment.
	ContextRepositoryAccessDenialPathEscape = "path_escape"
	// ContextRepositoryAccessDenialSymlinkEscape is a request that normalizes
	// inside the repository and resolves outside it.
	ContextRepositoryAccessDenialSymlinkEscape = "symlink_escape"
	// ContextRepositoryAccessDenialForeignRoot is a request against a repository
	// root this gateway was not anchored to.
	ContextRepositoryAccessDenialForeignRoot = "foreign_root"
	// ContextRepositoryAccessDenialOutsideAllowlist is a well-formed in-repository
	// request the project's configured read scope does not cover.
	ContextRepositoryAccessDenialOutsideAllowlist = "outside_allowlist"
	// ContextRepositoryAccessDenialUnclassifiable is a request whose shape the
	// gateway cannot classify at all. It is a denial rather than an error because
	// "we could not tell what this was" must never be served.
	ContextRepositoryAccessDenialUnclassifiable = "unclassifiable"
)

var contextRepositoryAccessOperations = map[ContextRepositoryAccessOperation]struct{}{
	ContextRepositoryAccessStat:   {},
	ContextRepositoryAccessRead:   {},
	ContextRepositoryAccessList:   {},
	ContextRepositoryAccessSearch: {},
}

var contextRepositoryAccessOutcomes = map[ContextRepositoryAccessOutcome]struct{}{
	ContextRepositoryAccessGranted: {},
	ContextRepositoryAccessDenied:  {},
	ContextRepositoryAccessFailed:  {},
}

var contextRepositoryAccessDenialReasons = map[string]struct{}{
	ContextRepositoryAccessDenialPathEscape:       {},
	ContextRepositoryAccessDenialSymlinkEscape:    {},
	ContextRepositoryAccessDenialForeignRoot:      {},
	ContextRepositoryAccessDenialOutsideAllowlist: {},
	ContextRepositoryAccessDenialUnclassifiable:   {},
}

// ContextRepositoryAccessEvent is one gateway call, recorded while it happened.
//
// Every field a consumer needs to reconstruct coverage is here, and nothing a
// consumer could use to reconstruct content is. That asymmetry is deliberate:
// the trace is durable manager-side evidence, so it carries digests of what was
// returned rather than what was returned.
type ContextRepositoryAccessEvent struct {
	ContractVersion string `json:"contract_version"`
	// Sequence is the 1-based position of this event in the run's trace. It is
	// part of the signed chain, so a consumer detects a removed event as a
	// sequence gap even before the digests disagree.
	Sequence int    `json:"sequence"`
	CtxID    string `json:"ctx_id"`
	RunID    string `json:"run_id,omitempty"`
	// Provider and ProviderEventID bind the gateway call to the provider-side
	// tool call that caused it. Without that binding the trace would be a list of
	// reads nobody could attribute to a turn, and a missing provider tool event
	// could not be detected as missing.
	Provider        ContextProvider                  `json:"provider"`
	ProviderEventID string                           `json:"provider_event_id"`
	Operation       ContextRepositoryAccessOperation `json:"operation"`
	Outcome         ContextRepositoryAccessOutcome   `json:"outcome"`
	// Path is the normalized repo-relative target of a stat or read. PathSpec is
	// the normalized target of a list or search. They are separate fields because
	// a spec may name a directory or a glob that no single path corresponds to,
	// and collapsing them would make a directory enumeration indistinguishable
	// from a file read of the same name.
	Path     string `json:"path,omitempty"`
	PathSpec string `json:"path_spec,omitempty"`
	// Query is the builder's own search pattern. It is repository input, never
	// repository content, so it is recorded literally — the coverage manifest
	// needs to know what was searched for, not only that something was.
	Query string `json:"query,omitempty"`
	// ByteOffset, ByteLength and TotalBytes describe the exact range a read
	// returned. Truncated says the range stopped short of the request rather than
	// of the file, which is the difference between "the builder saw this file" and
	// "the builder saw the first page of it".
	ByteOffset int64 `json:"byte_offset,omitempty"`
	ByteLength int64 `json:"byte_length,omitempty"`
	TotalBytes int64 `json:"total_bytes,omitempty"`
	Truncated  bool  `json:"truncated,omitempty"`
	// Entries is the exact enumeration a list or search returned, as normalized
	// repo-relative paths. It is the enumeration itself and not a count, because
	// the consumer builds coverage out of which paths were seen.
	Entries []string `json:"entries,omitempty"`
	// EntriesTruncated marks an enumeration the gateway bounded. A consumer that
	// read a bounded enumeration as complete would report coverage of a directory
	// the builder only partly saw.
	EntriesTruncated bool `json:"entries_truncated,omitempty"`
	// ContentDigest is the anchored digest of what was returned: file bytes for a
	// read, the enumeration for a list or search, the stat facts for a stat. It is
	// anchored so a digest cannot be lifted from one run's trace into another's.
	ContentDigest string `json:"content_digest,omitempty"`
	// DenialReason is set on a denial and on nothing else. FailureReason is the
	// same for an operational failure. Both are stable codes.
	DenialReason  string    `json:"denial_reason,omitempty"`
	FailureReason string    `json:"failure_reason,omitempty"`
	At            time.Time `json:"at"`
	// PreviousDigest and EventDigest are the hash chain. Each event commits to
	// its predecessor, so an event cannot be removed, reordered or edited without
	// breaking every digest after it — which is what makes the trace immutable
	// without a per-event signature.
	PreviousDigest string `json:"previous_digest"`
	EventDigest    string `json:"event_digest"`
}

// ContextRepositoryAccessAttestation is the terminal completeness statement.
//
// It is what turns a list of events into evidence. A trace file on its own says
// "these accesses happened"; the attestation says "and there were no others",
// by binding the event count and the final chain digest to the anchor. A
// consumer that verifies the chain but not the count would accept a truncated
// trace, which is the cheapest possible attack on a coverage claim.
type ContextRepositoryAccessAttestation struct {
	ContractVersion string          `json:"contract_version"`
	CtxID           string          `json:"ctx_id"`
	RunID           string          `json:"run_id,omitempty"`
	Provider        ContextProvider `json:"provider"`
	// Anchor binds the trace to one context, run and provider. Recomputing it is
	// how a consumer refuses a trace produced for something else.
	Anchor string `json:"anchor"`
	// EventCount is the number of events the trace must contain, and TraceDigest
	// is the chain digest after the last of them. Together they are the closure:
	// events cannot be added without changing the digest, or removed without
	// changing the count.
	EventCount  int       `json:"event_count"`
	TraceDigest string    `json:"trace_digest"`
	CompletedAt time.Time `json:"completed_at"`
}

// ContextRepositoryAccessAnchor is the value every digest in one trace is
// computed under.
//
// It is public — it identifies a context, a run and a provider, none of which is
// secret — and it is not a signature. Its job is domain separation, not
// authentication: the enforcement key signs the claim that a manager confined
// the run, and the anchor makes sure the digests inside that claim describe this
// run's reads and not some other run's.
func ContextRepositoryAccessAnchor(ctxID, runID string, provider ContextProvider) string {
	payload := strings.Join([]string{
		contextRepositoryAccessAnchorDomain,
		"ctx=" + builderNormalizeContextID(ctxID),
		"run=" + strings.ToUpper(strings.TrimSpace(runID)),
		"provider=" + strings.ToLower(strings.TrimSpace(string(provider))),
	}, "\n")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// ContextRepositoryAccessContentDigest fingerprints returned file bytes under
// one anchor.
func ContextRepositoryAccessContentDigest(anchor string, payload []byte) string {
	return transcript.ContextRepositoryAccessContentDigest(anchor, payload)
}
func ContextRepositoryAccessEnumerationDigest(anchor string, entries []string) string {
	return transcript.ContextRepositoryAccessEnumerationDigest(anchor, entries)
}
func ContextRepositoryAccessStatDigest(anchor string, exists bool, kind string, size int64) string {
	return transcript.ContextRepositoryAccessStatDigest(anchor, exists, kind, size)
}

func normalizeContextAccessEntries(entries []string) []string {
	cleaned := make([]string, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		normalized := builderNormalizeContextPathSpec(entry)
		if normalized == "" {
			continue
		}
		if _, duplicate := seen[normalized]; duplicate {
			continue
		}
		seen[normalized] = struct{}{}
		cleaned = append(cleaned, normalized)
	}
	sort.Strings(cleaned)
	return cleaned
}

// SealContextRepositoryAccessEvent validates one event and links it into the
// chain.
//
// It is the only way to obtain an event with a digest, and it refuses every
// shape the trace's guarantees depend on excluding: an unmodelled operation, a
// denial that carries content, a granted read with no digest, a range that does
// not describe a range. The gateway calls it while the access is happening, so a
// shape it refuses becomes a failed access rather than an unrecorded one.
func SealContextRepositoryAccessEvent(
	anchor string,
	previousDigest string,
	event ContextRepositoryAccessEvent,
) (ContextRepositoryAccessEvent, error) {
	anchor = strings.ToLower(strings.TrimSpace(anchor))
	if anchor == "" {
		return ContextRepositoryAccessEvent{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"a repository-access trace event must be sealed under a trace anchor",
			nil,
		)
	}
	event.ContractVersion = ContextRepositoryAccessTraceContract
	if event.Sequence < 1 {
		return ContextRepositoryAccessEvent{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"a repository-access trace event must carry its 1-based sequence",
			nil,
		)
	}
	event.CtxID = builderNormalizeContextID(event.CtxID)
	if err := builderValidateContextID(event.CtxID); err != nil {
		return ContextRepositoryAccessEvent{}, err
	}
	event.RunID = strings.ToUpper(strings.TrimSpace(event.RunID))
	if event.RunID != "" && !builderRunIDPattern.MatchString(event.RunID) {
		return ContextRepositoryAccessEvent{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			fmt.Sprintf("invalid producing run id %q in repository-access trace event", event.RunID),
			nil,
		)
	}
	if _, ok := contextRepositoryAccessOperations[event.Operation]; !ok {
		return ContextRepositoryAccessEvent{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			fmt.Sprintf(
				"repository-access trace event names operation %q, which this contract does not model",
				event.Operation,
			),
			nil,
		)
	}
	if _, ok := contextRepositoryAccessOutcomes[event.Outcome]; !ok {
		return ContextRepositoryAccessEvent{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			fmt.Sprintf(
				"repository-access trace event names outcome %q, which this contract does not model",
				event.Outcome,
			),
			nil,
		)
	}
	// Provider event identity is required on every event, including denials.
	// Without it a denial could not be matched to the tool call that produced it,
	// and "the provider emitted a tool event the gateway never saw" — the exact
	// bypass this trace exists to detect — would be unprovable.
	event.ProviderEventID = strings.TrimSpace(event.ProviderEventID)
	if event.ProviderEventID == "" {
		return ContextRepositoryAccessEvent{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"repository-access trace event must bind the provider tool-call identity that caused it",
			nil,
		)
	}
	if len(event.ProviderEventID) > builderTraceTextLimit {
		return ContextRepositoryAccessEvent{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"repository-access trace provider event identity is not bounded",
			nil,
		)
	}
	event.Provider = ContextProvider(strings.ToLower(strings.TrimSpace(string(event.Provider))))
	if event.Provider == "" {
		return ContextRepositoryAccessEvent{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"repository-access trace event must name the provider whose call it records",
			nil,
		)
	}
	event.Path = builderNormalizeContextPathSpec(event.Path)
	event.PathSpec = builderNormalizeContextPathSpec(event.PathSpec)
	event.Query = strings.TrimSpace(event.Query)
	if len(event.Query) > builderTraceTextLimit {
		return ContextRepositoryAccessEvent{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"repository-access trace search query is not bounded",
			nil,
		)
	}
	event.Entries = normalizeContextAccessEntries(event.Entries)
	if err := validateContextRepositoryAccessOutcome(&event); err != nil {
		return ContextRepositoryAccessEvent{}, err
	}
	if event.At.IsZero() {
		event.At = time.Now()
	}
	event.At = event.At.UTC().Truncate(time.Second)
	event.PreviousDigest = strings.ToLower(strings.TrimSpace(previousDigest))
	event.EventDigest = contextRepositoryAccessEventDigest(anchor, event)
	return event, nil
}

// validateContextRepositoryAccessOutcome enforces what each outcome may and may
// not carry.
//
// The denial rules are the security-relevant half. A denied access must record
// what was asked for and why it was refused, and must record nothing derived
// from repository content — no digest, no enumeration, no range. Otherwise a
// denial would leak exactly the thing the denial existed to withhold: an
// attacker who cannot read /etc/passwd through the gateway could still learn its
// size, or confirm a guess about its contents from a digest.
func validateContextRepositoryAccessOutcome(event *ContextRepositoryAccessEvent) error {
	switch event.Outcome {
	case ContextRepositoryAccessDenied:
		if _, ok := contextRepositoryAccessDenialReasons[event.DenialReason]; !ok {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				fmt.Sprintf(
					"denied repository access must carry a stable denial reason, got %q",
					event.DenialReason,
				),
				nil,
			)
		}
		if event.ContentDigest != "" || len(event.Entries) > 0 ||
			event.ByteLength != 0 || event.TotalBytes != 0 || event.ByteOffset != 0 {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				"denied repository access must not record content, enumeration or size evidence",
				nil,
			)
		}
		event.FailureReason = ""
		return nil
	case ContextRepositoryAccessFailed:
		event.FailureReason = strings.TrimSpace(event.FailureReason)
		if event.FailureReason == "" {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				"failed repository access must record why the permitted access did not complete",
				nil,
			)
		}
		if len(event.FailureReason) > builderTraceTextLimit {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				"repository-access trace failure reason is not bounded",
				nil,
			)
		}
		if event.ContentDigest != "" || len(event.Entries) > 0 {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				"failed repository access must not record content or enumeration evidence",
				nil,
			)
		}
		event.DenialReason = ""
		return nil
	}
	// Granted.
	event.DenialReason = ""
	event.FailureReason = ""
	if event.ContentDigest == "" {
		return builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"granted repository access must record the anchored digest of what it returned",
			nil,
		)
	}
	event.ContentDigest = strings.ToLower(strings.TrimSpace(event.ContentDigest))
	switch event.Operation {
	case ContextRepositoryAccessRead:
		if event.Path == "" {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				"granted repository read must record the normalized repo-relative path it returned",
				nil,
			)
		}
		if event.ByteOffset < 0 || event.ByteLength < 0 || event.TotalBytes < 0 {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				"granted repository read must record a non-negative returned range",
				nil,
			)
		}
		// The range has to be a range of the file that was read. A window ending
		// past the recorded size is not a rounding difference: it means the
		// recorded size and the recorded window came from two different
		// observations, and the coverage consumer would credit the base with bytes
		// nobody can show it saw.
		if event.ByteOffset+event.ByteLength > event.TotalBytes {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				"granted repository read records a returned range outside the file it read",
				nil,
			)
		}
	case ContextRepositoryAccessStat:
		if event.Path == "" {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				"granted repository stat must record the normalized repo-relative path it described",
				nil,
			)
		}
	case ContextRepositoryAccessList, ContextRepositoryAccessSearch:
		if event.PathSpec == "" {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				"granted repository enumeration must record the normalized path spec it enumerated",
				nil,
			)
		}
	}
	return nil
}

// contextRepositoryAccessEventDigest is the canonical chained form.
//
// Like the enforcement payload it is built field by field with explicit labels
// rather than by serializing the struct, so a field added to the struct is not
// silently covered — or silently left out — by the chain.
func contextRepositoryAccessEventDigest(
	anchor string,
	event ContextRepositoryAccessEvent,
) string {
	payload := strings.Join([]string{
		ContextRepositoryAccessTraceContract,
		"anchor=" + anchor,
		"previous=" + event.PreviousDigest,
		"sequence=" + strconv.Itoa(event.Sequence),
		"ctx=" + event.CtxID,
		"run=" + event.RunID,
		"provider=" + string(event.Provider),
		"provider_event=" + event.ProviderEventID,
		"operation=" + string(event.Operation),
		"outcome=" + string(event.Outcome),
		"path=" + event.Path,
		"path_spec=" + event.PathSpec,
		"query=" + event.Query,
		"byte_offset=" + strconv.FormatInt(event.ByteOffset, 10),
		"byte_length=" + strconv.FormatInt(event.ByteLength, 10),
		"total_bytes=" + strconv.FormatInt(event.TotalBytes, 10),
		"truncated=" + strconv.FormatBool(event.Truncated),
		"entries=" + strings.Join(event.Entries, "\x00"),
		"entries_truncated=" + strconv.FormatBool(event.EntriesTruncated),
		"content=" + event.ContentDigest,
		"denial=" + event.DenialReason,
		"failure=" + event.FailureReason,
		"at=" + event.At.UTC().Format(time.RFC3339),
	}, "\n")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// AttestContextRepositoryAccessTrace closes one trace.
//
// The attestation is minted from the chain rather than alongside it: the caller
// hands over the anchor, the number of events it wrote and the digest of the
// last one, and gets back the terminal statement. There is no path that produces
// an attestation without those, which is what stops a run from closing a trace
// it never wrote.
func AttestContextRepositoryAccessTrace(
	anchor string,
	ctxID string,
	runID string,
	provider ContextProvider,
	eventCount int,
	traceDigest string,
	completedAt time.Time,
) (ContextRepositoryAccessAttestation, error) {
	anchor = strings.ToLower(strings.TrimSpace(anchor))
	normalizedCtx := builderNormalizeContextID(ctxID)
	normalizedRun := strings.ToUpper(strings.TrimSpace(runID))
	normalizedProvider := ContextProvider(strings.ToLower(strings.TrimSpace(string(provider))))
	if anchor != ContextRepositoryAccessAnchor(normalizedCtx, normalizedRun, normalizedProvider) {
		return ContextRepositoryAccessAttestation{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"repository-access trace attestation does not match the anchor of the run it closes",
			nil,
		)
	}
	if eventCount < 0 {
		return ContextRepositoryAccessAttestation{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"repository-access trace attestation must bind a non-negative event count",
			nil,
		)
	}
	traceDigest = strings.ToLower(strings.TrimSpace(traceDigest))
	// An empty trace is a legitimate outcome — a builder that read nothing — but
	// it still has to be attested to a value rather than to nothing, or "the run
	// read nothing" and "the trace was lost" would be the same record.
	if eventCount == 0 {
		traceDigest = anchor
	}
	if traceDigest == "" {
		return ContextRepositoryAccessAttestation{}, builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"repository-access trace attestation must bind the chain digest of its last event",
			nil,
		)
	}
	if completedAt.IsZero() {
		completedAt = time.Now()
	}
	return ContextRepositoryAccessAttestation{
		ContractVersion: ContextRepositoryAccessTraceContract,
		CtxID:           normalizedCtx,
		RunID:           normalizedRun,
		Provider:        normalizedProvider,
		Anchor:          anchor,
		EventCount:      eventCount,
		TraceDigest:     traceDigest,
		CompletedAt:     completedAt.UTC().Truncate(time.Second),
	}, nil
}

// VerifyContextRepositoryAccessTrace replays a trace against its attestation.
//
// This is the consumer's side of the contract and it is deliberately total: it
// recomputes every digest, checks every sequence number, and then checks the
// count and the final digest against the attestation. Verifying the chain alone
// would accept a prefix of the real trace, and verifying the count alone would
// accept a chain of edited events — the pair is what makes the trace both
// complete and immutable.
func VerifyContextRepositoryAccessTrace(
	events []ContextRepositoryAccessEvent,
	attestation ContextRepositoryAccessAttestation,
) error {
	if attestation.ContractVersion != ContextRepositoryAccessTraceContract {
		return builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			fmt.Sprintf(
				"repository-access trace attestation declares contract %q, not %s",
				attestation.ContractVersion, ContextRepositoryAccessTraceContract,
			),
			nil,
		)
	}
	anchor := ContextRepositoryAccessAnchor(
		attestation.CtxID, attestation.RunID, attestation.Provider)
	if anchor != strings.ToLower(strings.TrimSpace(attestation.Anchor)) {
		return builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"repository-access trace attestation anchor does not describe its own context, run and provider",
			nil,
		)
	}
	if len(events) != attestation.EventCount {
		return builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			fmt.Sprintf(
				"repository-access trace carries %d event(s); its attestation binds %d",
				len(events), attestation.EventCount,
			),
			nil,
		)
	}
	previous := ""
	for index, event := range events {
		if event.ContractVersion != ContextRepositoryAccessTraceContract {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				fmt.Sprintf(
					"repository-access trace event %d declares contract %q", index+1, event.ContractVersion,
				),
				nil,
			)
		}
		if event.Sequence != index+1 {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				fmt.Sprintf(
					"repository-access trace event at position %d declares sequence %d",
					index+1, event.Sequence,
				),
				nil,
			)
		}
		if strings.ToLower(strings.TrimSpace(event.PreviousDigest)) != previous {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				fmt.Sprintf(
					"repository-access trace event %d does not chain to its predecessor", event.Sequence,
				),
				nil,
			)
		}
		recomputed := contextRepositoryAccessEventDigest(anchor, event)
		if recomputed != strings.ToLower(strings.TrimSpace(event.EventDigest)) {
			return builderTraceError(
				ContextErrorBuilderTraceUnavailable,
				fmt.Sprintf(
					"repository-access trace event %d does not match its recorded digest", event.Sequence,
				),
				nil,
			)
		}
		previous = recomputed
	}
	expected := previous
	if len(events) == 0 {
		expected = anchor
	}
	if expected != strings.ToLower(strings.TrimSpace(attestation.TraceDigest)) {
		return builderTraceError(
			ContextErrorBuilderTraceUnavailable,
			"repository-access trace does not end at the digest its attestation binds",
			nil,
		)
	}
	return nil
}
