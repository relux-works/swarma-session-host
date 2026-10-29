package sessioncontext

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const ClaudeSessionRetentionWindow = 30 * 24 * time.Hour

// DefaultContextRetentionWarnWindow is how long before a provider's deletion
// horizon a context is surfaced as expiring.
//
// Seven days is chosen so a refresh or rebuild can be scheduled through a normal
// board wave rather than discovered on use.
const DefaultContextRetentionWarnWindow = 7 * 24 * time.Hour

// ContextProviderRetention is what one provider does to stored payloads when
// left alone.
type ContextProviderRetention struct {
	Provider ContextProvider
	// DeletionHorizon is how long after a payload's last write the provider is
	// expected to delete it. Zero means no provider cleanup is documented or has
	// been observed, so the board has no horizon to schedule against — not that
	// the payload is guaranteed to survive.
	DeletionHorizon time.Duration
	// BoardReclaims reports that the board cannot count on anything else deleting
	// this provider's payloads, so reclaiming them is its own job. It is the
	// direct consequence of a provider with no known deletion horizon.
	BoardReclaims bool
	// ArchivesInPlace reports that the provider's "archive" operation moves a
	// payload rather than deleting it, so archived payloads keep occupying disk
	// and keep whatever they captured. Reclamation has to cover them too.
	ArchivesInPlace bool
	// Note is a short operator-facing statement of the behaviour. It is included
	// verbatim in refusals so the message explains itself.
	Note string
}

// ContextRetentionForProvider returns the retention behaviour of a provider.
//
// An unknown provider is deliberately given the pessimistic combination — no
// horizon and board-owned reclamation — so a provider added without updating
// this table leaks disk rather than silently deleting a context somebody still
// needs.
func ContextRetentionForProvider(provider ContextProvider) ContextProviderRetention {
	switch provider {
	case ContextProviderClaude:
		return ContextProviderRetention{
			Provider:        ContextProviderClaude,
			DeletionHorizon: ClaudeSessionRetentionWindow,
			BoardReclaims:   false,
			ArchivesInPlace: false,
			Note: "Claude deletes session transcripts about 30 days after their last write, " +
				"without notifying the board",
		}
	case ContextProviderCodex:
		return ContextProviderRetention{
			Provider:        ContextProviderCodex,
			DeletionHorizon: 0,
			BoardReclaims:   true,
			ArchivesInPlace: true,
			Note: "no Codex pruning is documented or has been observed, and its archive operation " +
				"moves rollouts rather than deleting them, so the board cannot rely on provider " +
				"cleanup and reclaims Codex payloads itself",
		}
	default:
		return ContextProviderRetention{
			Provider:      provider,
			BoardReclaims: true,
			Note:          fmt.Sprintf("provider %q has no known retention behaviour", provider),
		}
	}
}

const (
	// ContextRetentionDurable means the provider has no known deletion horizon,
	// so there is no date to act before. It is not a promise the payload is still
	// there: presence is still verified before the context is served.
	ContextRetentionDurable ContextRetentionRisk = "durable"
	// ContextRetentionHealthy means a deletion horizon exists but is far away.
	ContextRetentionHealthy ContextRetentionRisk = "healthy"
	// ContextRetentionExpiring means the horizon is inside the warn window. This
	// is the state that must be surfaced ahead of the loss.
	ContextRetentionExpiring ContextRetentionRisk = "expiring"
	// ContextRetentionElapsed means the horizon has passed and the payload must
	// be presumed deleted until a presence probe says otherwise.
	ContextRetentionElapsed ContextRetentionRisk = "elapsed"
	// ContextRetentionUnknown means the record carries no anchor to measure from.
	ContextRetentionUnknown ContextRetentionRisk = "unknown"
)

func contextRetentionAnchor(record *ContextRecord) time.Time {
	if record == nil {
		return time.Time{}
	}
	if !record.ReadyAt.IsZero() {
		return record.ReadyAt.UTC()
	}
	if !record.CreatedAt.IsZero() {
		return record.CreatedAt.UTC()
	}
	return time.Time{}
}
func EvaluateContextRetention(record *ContextRecord, policy ContextRetentionPolicy) ContextRetentionOutlook {
	if record == nil {
		return ContextRetentionOutlook{Risk: ContextRetentionUnknown}
	}
	retention := ContextRetentionForProvider(record.Provider)
	outlook := ContextRetentionOutlook{
		CtxID:    NormalizeContextID(record.ID),
		Provider: record.Provider,
		State:    record.State,
		Anchor:   contextRetentionAnchor(record),
	}
	if retention.DeletionHorizon <= 0 {
		outlook.Risk = ContextRetentionDurable
		outlook.Advice = retention.Note +
			"; because the provider offers no cleanup guarantee, board retention reclaims payloads " +
			"the registry no longer needs, including archived copies"
		return outlook
	}
	if outlook.Anchor.IsZero() {
		outlook.Risk = ContextRetentionUnknown
		outlook.Advice = fmt.Sprintf(
			"%s carries no publication time, so its distance from the provider deletion horizon "+
				"cannot be measured; verify presence before serving it",
			outlook.CtxID,
		)
		return outlook
	}
	now := retentionNow(policy)
	outlook.Horizon = outlook.Anchor.Add(retention.DeletionHorizon).UTC()
	outlook.Remaining = outlook.Horizon.Sub(now)
	switch {
	case outlook.Remaining <= 0:
		outlook.Risk = ContextRetentionElapsed
		outlook.Advice = fmt.Sprintf(
			"%s passed the provider deletion horizon %s ago; %s, so treat its payload as gone "+
				"and rebuild the base rather than planning a refresh from it",
			outlook.CtxID, contextRetentionDuration(-outlook.Remaining), retention.Note,
		)
	case outlook.Remaining <= retentionWarnWindow(policy):
		outlook.Risk = ContextRetentionExpiring
		outlook.Advice = fmt.Sprintf(
			"%s reaches the provider deletion horizon in %s; %s, so schedule an actualization or a "+
				"rebuild now instead of discovering the loss on use",
			outlook.CtxID, contextRetentionDuration(outlook.Remaining), retention.Note,
		)
	default:
		outlook.Risk = ContextRetentionHealthy
		outlook.Advice = fmt.Sprintf(
			"%s has %s before the provider deletion horizon",
			outlook.CtxID, contextRetentionDuration(outlook.Remaining),
		)
	}
	return outlook
}
func contextRetentionDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	days := int(d / (24 * time.Hour))
	hours := int((d % (24 * time.Hour)) / time.Hour)
	switch {
	case days > 0 && hours > 0:
		return fmt.Sprintf("%dd%dh", days, hours)
	case days > 0:
		return fmt.Sprintf("%dd", days)
	default:
		return fmt.Sprintf("%dh", hours)
	}
}

// ContextPayloadPresence is the verdict of one machine-local existence probe.
type ContextPayloadPresence string

const (
	ContextPayloadPresent ContextPayloadPresence = "present"
	// ContextPayloadVanished means the payload the record describes is not on
	// this machine. On Claude it is the expected end of a base's life; on Codex,
	// where no provider cleanup is documented or has been observed, the board has
	// no cleanup schedule to attribute the loss to.
	ContextPayloadVanished ContextPayloadPresence = "vanished"
	// ContextPayloadUnverifiable means the probe could not run at all.
	ContextPayloadUnverifiable ContextPayloadPresence = "unverifiable"
)

// ContextPresenceReport is one presence probe result.
//
// It carries no path, provider handle, or transcript: those live in the private
// Session Manager catalog, and this value is logged and returned to operators.
type ContextPresenceReport struct {
	CtxID     string                 `json:"ctx_id"`
	Provider  ContextProvider        `json:"provider"`
	Presence  ContextPayloadPresence `json:"presence"`
	Detail    string                 `json:"detail,omitempty"`
	CheckedAt time.Time              `json:"checked_at,omitempty"`
}

// RequireContextPayloadPresent turns a presence probe into the decision to
// serve or refuse.
//
// A registry entry is metadata; it survives its payload by design. So this is
// the check that stops the registry from asserting something it never verified,
// and the refusal it produces is a first-class path with a provider-specific
// remedy rather than a surprise error.
func RequireContextPayloadPresent(
	record *ContextRecord,
	report ContextPresenceReport,
	mode ContextResolutionMode,
	policy ContextRetentionPolicy,
) error {
	if record == nil {
		return contextError(ContextErrorContractInvalid, "context record is required", nil)
	}
	switch report.Presence {
	case ContextPayloadPresent:
		return nil
	case "":
		// The zero value is a caller that never probed, and it is refused rather
		// than waved through. A gate whose default is "assume present" is the exact
		// assumption this file exists to remove: it would admit every vanished
		// payload on any path that forgot to look.
		return &ContextError{
			Code: ContextErrorPayloadUnverified,
			Message: fmt.Sprintf(
				"context %s was served without a payload presence probe; a registry entry is metadata "+
					"and never proves its payload still exists, so an unprobed serve is refused",
				NormalizeContextID(record.ID),
			),
		}
	case ContextPayloadUnverifiable:
		// Fail closed and do not offer the next candidate: a probe that cannot run
		// will not run for the sibling either, so falling through the ranked list
		// would replace one honest refusal with a series of identical ones.
		return &ContextError{
			Code: ContextErrorPayloadUnverified,
			Message: fmt.Sprintf(
				"context %s could not be checked for payload presence on this machine%s; "+
					"a registry entry never proves its payload still exists, so serving it unverified "+
					"is refused",
				NormalizeContextID(record.ID), contextPresenceDetail(report),
			),
		}
	case ContextPayloadVanished:
		return &ContextError{
			Code:               ContextErrorPayloadVanished,
			Message:            contextVanishedMessage(record, report, policy),
			RetryNextCandidate: mode == ContextResolutionAuto,
		}
	default:
		return &ContextError{
			Code: ContextErrorPayloadUnverified,
			Message: fmt.Sprintf(
				"context %s presence probe returned unsupported verdict %q",
				NormalizeContextID(record.ID), report.Presence,
			),
		}
	}
}

func contextPresenceDetail(report ContextPresenceReport) string {
	detail := strings.TrimSpace(report.Detail)
	if detail == "" {
		return ""
	}
	if len(detail) > contextTextLimit {
		detail = detail[:contextTextLimit]
	}
	return ": " + detail
}

func contextVanishedMessage(
	record *ContextRecord,
	report ContextPresenceReport,
	policy ContextRetentionPolicy,
) string {
	id := NormalizeContextID(record.ID)
	outlook := EvaluateContextRetention(record, policy)
	var cause string
	switch {
	case outlook.Risk == ContextRetentionElapsed:
		cause = fmt.Sprintf(
			"it was published %s ago and %s, so the payload is gone rather than misplaced",
			contextRetentionDuration(outlook.Anchor.Sub(retentionNow(policy))),
			ContextRetentionForProvider(record.Provider).Note,
		)
	case outlook.Risk == ContextRetentionExpiring || outlook.Risk == ContextRetentionHealthy:
		cause = fmt.Sprintf(
			"it is still %s inside the provider retention window, so it was removed outside the "+
				"provider's own cleanup — check whether the provider home was pruned or the session "+
				"was created under a different working directory",
			contextRetentionDuration(outlook.Remaining),
		)
	case ContextRetentionForProvider(record.Provider).BoardReclaims:
		// No horizon to blame, and no vendor guarantee to rule the provider out
		// either: this provider has simply never been observed pruning. The message
		// says what the board knows and lists the candidates rather than asserting
		// the removal must have come from outside the provider.
		cause = "no cleanup by this provider is documented or has been observed, so the board has " +
			"no provider retention schedule to attribute the loss to — check for a manual delete, a " +
			"different provider home, an archive that was cleaned up, or provider housekeeping the " +
			"board has not seen before"
	default:
		cause = "the board cannot attribute the removal to the provider's own retention"
	}
	return fmt.Sprintf(
		"context %s has no %s payload on this machine%s: %s. "+
			"The registry entry is kept as the audit record; mark it stale with reason %s and rebuild "+
			"the base before serving this element again",
		id, record.Provider, contextPresenceDetail(report), cause, ContextReasonPayloadVanished,
	)
}

// ContextVersionRelation is how a recorded provider build compares to the one
// that would resume the context.
type ContextVersionRelation string

const (
	ContextVersionSame ContextVersionRelation = "same"
	// ContextVersionPatchDrift is a difference the provider's own versioning says
	// is compatible.
	ContextVersionPatchDrift ContextVersionRelation = "patch_drift"
	// ContextVersionMaterialDrift is a major or minor difference — the case the
	// research called "materially older".
	ContextVersionMaterialDrift ContextVersionRelation = "material_drift"
	// ContextVersionUnknown means at least one side is missing or unparseable.
	ContextVersionUnknown ContextVersionRelation = "unknown"
)

// Material reports whether the relation is one worth naming in a failure.
func (r ContextVersionRelation) Material() bool {
	return r == ContextVersionMaterialDrift || r == ContextVersionUnknown
}

// CompareContextProviderVersions classifies the gap between the build that
// produced a context and the build that would consume it.
//
// It classifies and never refuses. The accepted research found that contexts do
// survive CLI upgrades in practice across large version gaps, but that neither
// vendor documents format stability — so the only defensible policy is to
// attempt the resume and use this classification to explain a failure, never to
// predict one.
func CompareContextProviderVersions(recorded, active string) ContextVersionRelation {
	left := strings.TrimSpace(recorded)
	right := strings.TrimSpace(active)
	if left == "" || right == "" {
		return ContextVersionUnknown
	}
	if left == right {
		return ContextVersionSame
	}
	leftParts, leftOK := parseContextVersion(left)
	rightParts, rightOK := parseContextVersion(right)
	if !leftOK || !rightOK {
		return ContextVersionUnknown
	}
	if leftParts[0] != rightParts[0] || leftParts[1] != rightParts[1] {
		return ContextVersionMaterialDrift
	}
	return ContextVersionPatchDrift
}

// parseContextVersion reads the leading major.minor of a dotted version.
func parseContextVersion(version string) ([2]int, bool) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(version), "v")
	if idx := strings.IndexAny(trimmed, "-+ "); idx >= 0 {
		trimmed = trimmed[:idx]
	}
	fields := strings.Split(trimmed, ".")
	if len(fields) < 2 {
		return [2]int{}, false
	}
	var parsed [2]int
	for i := 0; i < 2; i++ {
		value, err := strconv.Atoi(fields[i])
		if err != nil || value < 0 {
			return [2]int{}, false
		}
		parsed[i] = value
	}
	return parsed, true
}

// ContextResumeAttempt is one attempt to reuse a context across a provider
// build gap.
type ContextResumeAttempt struct {
	CtxID    string
	Provider ContextProvider
	// RecordedVersion is the build that produced the context.
	RecordedVersion string
	// ActiveVersion is the build that tried to consume it.
	ActiveVersion string
	// Operation names the provider call that was attempted, so the report says
	// what was tried rather than only that something failed.
	Operation string
	// Cause is the provider failure. It is required: this constructor exists to
	// report a failure that already happened, never to predict one.
	Cause error
}

// ContextResumeFallback converts a failed cross-build resume into the rebuild
// refusal, with the version gap named as the reason.
//
// The order this enforces is the whole point: the caller attempts first and
// calls this second. A pre-emptive version check would refuse the large gaps
// that were measured to work, and a bare provider error would leave an operator
// guessing whether the CLI upgrade was relevant.
func ContextResumeFallback(attempt ContextResumeAttempt) error {
	id := NormalizeContextID(attempt.CtxID)
	relation := CompareContextProviderVersions(attempt.RecordedVersion, attempt.ActiveVersion)
	operation := strings.TrimSpace(attempt.Operation)
	if operation == "" {
		operation = "resume"
	}
	versions := fmt.Sprintf(
		"built by %s, attempted by %s",
		contextVersionLabel(attempt.RecordedVersion),
		contextVersionLabel(attempt.ActiveVersion),
	)
	if relation == ContextVersionSame {
		// Same build and it still failed, so the version gap explains nothing and
		// saying otherwise would send an operator to reinstall a CLI that is fine.
		return &ContextError{
			Code: ContextErrorProviderIncompatible,
			Message: fmt.Sprintf(
				"context %s could not be resumed by the %s build that produced it (%s failed); "+
					"the payload is not reusable here, so rebuild the base and mark this one stale with "+
					"reason %s",
				id, contextVersionLabel(attempt.ActiveVersion), operation,
				ContextReasonProviderIncompatible,
			),
			Cause: attempt.Cause,
		}
	}
	return &ContextError{
		Code: ContextErrorProviderIncompatible,
		Message: fmt.Sprintf(
			"context %s could not be resumed across a %s provider build gap (%s; %s failed); "+
				"neither vendor documents transcript format stability, so a resume across builds is "+
				"attempted and never assumed — rebuild the base on the active build and mark this one "+
				"stale with reason %s",
			id, relation, versions, operation, ContextReasonProviderIncompatible,
		),
		Cause: attempt.Cause,
	}
}

func contextVersionLabel(version string) string {
	trimmed := strings.TrimSpace(version)
	if trimmed == "" {
		return "an unrecorded build"
	}
	if len(trimmed) > contextTextLimit {
		trimmed = trimmed[:contextTextLimit]
	}
	return trimmed
}
