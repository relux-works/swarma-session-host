package sessioncontext

import (
	"context"
	"fmt"
	"sort"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Retention has two owners and they run in a fixed order.
//
// Layer 1 decides. It is the only plane that can see the whole registry, so it
// is the only one that can enforce a cap or know that a base has descendants
// somewhere else. Layer 2 executes. It holds the provider handles, the leases,
// and the private catalog, so it is the only one that can say what is live and
// the only one that can delete anything.
//
// The order is decide-then-execute and never the reverse. A local handle is
// dropped only after the registry says the context is retired, which is the same
// "provider deletion runs only after retirement" rule the payload sweep obeys —
// stated here again because this is where it would be easiest to break, by
// reclaiming a handle whose retirement had not landed and leaving the registry
// advertising a context this machine had just deleted.
//
// Leases flow the other way. Layer 1 cannot see them, so this pass reads them
// and asserts it looked; without that assertion the sweep plans nothing at all.

// ContextRetentionSweepRequest parameterizes one full retention pass.
type ContextRetentionSweepRequest struct {
	// Settings is the project's resolved retention policy. A zero value selects
	// the documented defaults.
	Settings sm.ContextRetentionSettings `json:"settings"`
	// IncludePinned is the explicit operator override.
	IncludePinned bool `json:"include_pinned,omitempty"`
	// DryRun plans and reports without retiring, dropping, or deleting anything.
	DryRun bool `json:"dry_run,omitempty"`
}

// ContextMaterializationSweepAction is what the local half did to one handle.
type ContextMaterializationSweepAction string

const (
	// ContextMaterializationDropped means the board-owned provider bytes this
	// handle named are gone and the private catalog entry went with them.
	ContextMaterializationDropped ContextMaterializationSweepAction = "dropped"
	// ContextMaterializationPlanned is the dry-run outcome for a handle the
	// applying pass would release, with the provider files that would go with it.
	ContextMaterializationPlanned ContextMaterializationSweepAction = "planned"
	// ContextMaterializationKept is the dry-run outcome for a handle a rule
	// selected and the applying pass would refuse to release, because the bytes it
	// names could not be proved reclaimable. It is reported rather than folded
	// into `planned` so a dry run never promises a drop that will not happen.
	ContextMaterializationKept ContextMaterializationSweepAction = "kept"
	// ContextMaterializationSweepFailed means the release was attempted and did
	// not complete, so the next pass retries it. The handle is deliberately kept:
	// it is the only name the surviving bytes have.
	ContextMaterializationSweepFailed ContextMaterializationSweepAction = "failed"
)

// ContextMaterializationSweepOutcome is one local handle's result. It carries a
// CTX id, counts and a reason, never a path: this value crosses the control
// plane.
type ContextMaterializationSweepOutcome struct {
	CtxID  string                            `json:"ctx_id"`
	Action ContextMaterializationSweepAction `json:"action"`
	Rule   sm.ContextRetentionRule           `json:"rule,omitempty"`
	State  MaterializationState              `json:"state,omitempty"`
	// Files is how many provider payload files were deleted with the handle. Zero
	// is a normal result for a provider whose bytes another catalog owns, and for
	// an adopted thread the board never created.
	Files  int    `json:"files,omitempty"`
	Detail string `json:"detail"`
}

// ContextRetentionSweepReport is one full pass across both planes.
type ContextRetentionSweepReport struct {
	// Registry is the layer-1 half: what the board retired and which payload
	// bytes it withdrew.
	Registry *sm.RetentionSweepResult `json:"registry"`
	// Materializations is the layer-2 half: the local handles this machine
	// released, and the provider bytes released with them.
	Materializations []ContextMaterializationSweepOutcome `json:"materializations"`
	// Artifacts is the provider-local debris this machine reclaimed.
	Artifacts []ContextArtifactReclamationOutcome `json:"artifacts,omitempty"`
	// LeasedContexts is how many contexts were held by a live run when the pass
	// ran. It is reported because it is the number that explains an unmet cap.
	LeasedContexts int       `json:"leased_contexts"`
	DryRun         bool      `json:"dry_run,omitempty"`
	EvaluatedAt    time.Time `json:"evaluated_at"`
}

// SweepContextRetention runs one full retention pass on this machine.
func (m *Service) SweepContextRetention(
	ctx context.Context,
	request ContextRetentionSweepRequest,
) (ContextRetentionSweepReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now().UTC()
	settings := normalizeRetentionSettings(request.Settings)
	report := ContextRetentionSweepReport{
		Materializations: []ContextMaterializationSweepOutcome{},
		DryRun:           request.DryRun,
		EvaluatedAt:      now,
	}
	if m.materializationAuthority == nil {
		return report, fmt.Errorf("no layer-1 authority is configured for context retention")
	}

	if m.references == nil {
		return report, fmt.Errorf("current host reference visibility unavailable")
	}
	for id := range m.materializations {
		_, known, e := m.references.Referenced(ctx, id)
		if e != nil || !known {
			return report, fmt.Errorf("current host reference visibility unavailable: %v", e)
		}
	}
	leased, leaseErr := m.leasedContextsLocked()
	if leaseErr != nil {
		return report, fmt.Errorf("quarantine lease visibility unavailable: %w", leaseErr)
	}
	report.LeasedContexts = len(leased)

	registry, err := m.materializationAuthority.SweepContextRetention(
		ctx,
		sm.RetentionSweepRequest{
			Settings: settings,
			Now:      now,
			// The assertion is made here and only here: this pass just read the
			// private catalog and the session table, so it is the one caller that
			// can honestly say the lease set is complete.
			LeaseVisibility: "observed",
			Leased:          leased,
			IncludePinned:   request.IncludePinned,
			DryRun:          request.DryRun,
		},
	)
	if err != nil {
		return report, err
	}
	report.Registry = registry

	var releasing map[string]struct{}
	report.Materializations, releasing = m.sweepMaterializationsLocked(
		ctx, settings, leased, now, request.DryRun,
	)
	// The artifact pass runs second and is told what the first one released,
	// because releasing a handle is what turns the Claude artifacts it named into
	// debris. Running it first would reclaim nothing on the pass that made it
	// reclaimable.
	report.Artifacts = m.reclaimContextArtifactsLocked(settings, now, request.DryRun, releasing)
	return report, nil
}

// leasedContextsLocked names every context a live run is using.
//
// It is deliberately wider than the writer lease alone. A ready base held open
// as a fork source has no writer lease — a fork reads it — but retiring it while
// a session is materialized against it would delete the bytes a running agent is
// reading. Both kinds of "in use" are reported as leased, because the sweep's
// only question is whether it is safe to take the record away.
func (m *Service) leasedContextsLocked() (map[string]struct{}, error) {
	leased, err := m.materializationRecords.quarantinedIDs()
	if err != nil {
		return nil, err
	}
	for ctxID, record := range m.materializations {
		if record == nil {
			continue
		}
		normalized := normalizeContextID(ctxID)
		if record.State == MaterializationLeased || record.ActiveRunID != "" || record.UnresolvedRunID != "" ||
			m.materializationReferencedLocked(normalized) {
			leased[normalized] = struct{}{}
			continue
		}
		// A materializing record is mid-flight: the handle is being written and
		// nothing has reached a state anybody could reason about yet.
		if record.State == MaterializationMaterializing {
			leased[normalized] = struct{}{}
		}
	}
	return leased, nil
}

// sweepMaterializationsLocked releases the local handles policy no longer keeps.
//
// Three windows apply, and each answers a different question. The unpublished
// window bounds a successful run whose result nobody registered: it is
// reattachable for a while and then it is unreachable debris that still holds
// whatever the run read. The idle window bounds a handle nothing has used, which
// costs a rebuild to lose and nothing to keep except disk and exposure. The
// retired grace bounds a handle whose registry record is already a tombstone.
//
// A quarantined record is never swept. Quarantine means this machine found a
// disagreement it could not resolve, and deleting the evidence of an unresolved
// disagreement is how the disagreement becomes permanent.
//
// Every release is bytes-first. The private entry is the only name a CTX id has
// on this machine, so dropping it while its transcript is still on disk does not
// free the disk or close the exposure the windows exist for — it makes both
// permanent, because nothing afterwards can enumerate what was left. A handle
// whose bytes could not be proved gone is therefore kept and reported, not
// released.
//
// It returns the outcomes and the set of contexts actually released, which the
// artifact pass needs to know which Claude artifact sets have just become debris.
func (m *Service) sweepMaterializationsLocked(
	ctx context.Context,
	settings sm.ContextRetentionSettings,
	leased map[string]struct{},
	now time.Time,
	dryRun bool,
) ([]ContextMaterializationSweepOutcome, map[string]struct{}) {
	released := make(map[string]struct{})
	outcomes := make([]ContextMaterializationSweepOutcome, 0, len(m.materializations))
	ids := make([]string, 0, len(m.materializations))
	for ctxID := range m.materializations {
		ids = append(ids, normalizeContextID(ctxID))
	}
	sort.Strings(ids)

	for _, ctxID := range ids {
		record := m.materializations[ctxID]
		if record == nil {
			continue
		}
		if _, held := leased[ctxID]; held {
			continue
		}
		if record.State == MaterializationQuarantined {
			continue
		}
		rule, detail, sweep := m.materializationSweepVerdictLocked(ctx, record, settings, now)
		if !sweep {
			continue
		}
		outcome := ContextMaterializationSweepOutcome{
			CtxID:  ctxID,
			Rule:   rule,
			State:  record.State,
			Detail: detail,
		}
		if dryRun {
			// The plan runs the same enumeration the applying pass would, so the
			// operator is shown the provider transcripts this rule deletes rather
			// than only the catalog entry it drops. A refusal is shown too: a handle
			// whose bytes cannot be proved gone is kept, and a dry run that promised
			// to drop it would describe a pass nobody will ever run.
			files, byteDetail, planErr := m.planMaterializationByteReleaseLocked(record)
			if planErr != nil {
				outcome.Action = ContextMaterializationKept
				outcome.Detail = fmt.Sprintf(
					"the provider bytes this handle names cannot be released, so the applying pass "+
						"would keep the handle as the only thing able to name them: %v", planErr,
				)
				outcomes = append(outcomes, outcome)
				continue
			}
			outcome.Action = ContextMaterializationPlanned
			outcome.Files = files
			outcome.Detail += "; " + byteDetail
			released[ctxID] = struct{}{}
			outcomes = append(outcomes, outcome)
			continue
		}
		files, byteDetail, err := m.releaseMaterializationBytesLocked(ctx, record)
		outcome.Files = files
		if err != nil {
			outcome.Action = ContextMaterializationSweepFailed
			outcome.Detail = fmt.Sprintf(
				"the provider bytes this handle names could not be released, so the handle is kept "+
					"as the only thing able to name them and the next sweep retries it: %v", err,
			)
			m.logOperatorError("context_materialization_sweep_failed", err, map[string]any{
				"ctx_id": ctxID,
			})
			outcomes = append(outcomes, outcome)
			continue
		}
		if err := m.dropMaterializationLocked(ctxID); err != nil {
			outcome.Action = ContextMaterializationSweepFailed
			outcome.Detail = fmt.Sprintf(
				"the local handle could not be released, so the next sweep retries it: %v", err,
			)
			m.logOperatorError("context_materialization_sweep_failed", err, map[string]any{
				"ctx_id": ctxID,
			})
			outcomes = append(outcomes, outcome)
			continue
		}
		outcome.Action = ContextMaterializationDropped
		outcome.Detail += "; " + byteDetail
		released[ctxID] = struct{}{}
		m.logOperatorInfo("context_materialization_swept", map[string]any{
			"ctx_id": ctxID,
			"rule":   string(rule),
			"files":  files,
		})
		outcomes = append(outcomes, outcome)
	}
	return outcomes, released
}

func (m *Service) materializationSweepVerdictLocked(
	ctx context.Context,
	record *materializationRecord,
	settings sm.ContextRetentionSettings,
	now time.Time,
) (sm.ContextRetentionRule, string, bool) {
	ctxID := normalizeContextID(record.CtxID)
	// Why a handle was detached decides both which window governs it and what the
	// operator is told. `unpublished_materialization_ttl` is documented as the
	// window a *never-published* session may still be reattached in, so a base
	// that published successfully and was only detached because its exact target
	// closed does not belong to it: that base is still ready on the board, and
	// what the sweep is about to do is the idle eviction the neighbouring rule
	// already describes. Reading only the state told an operator their published
	// base "was detached without publication", which is false about the one fact
	// they would act on.
	//
	// Neither branch short-circuits the rest of the function: a handle whose own
	// window has not elapsed may still be inside a retired grace, and returning
	// early here would keep it until the later window it no longer qualifies for.
	if record.State == MaterializationDetached {
		if record.DetachReason == MaterializationDetachUnpublished {
			if elapsedSince(record.LastUsedAt, now, settings.UnpublishedMaterializationTTL) {
				return "unpublished_materialization_ttl", fmt.Sprintf(
					"%s was detached without publication %s ago, past the unpublished window, so it can no "+
						"longer be reattached and is released",
					ctxID, now.Sub(record.LastUsedAt.UTC()).Round(time.Minute),
				), true
			}
		} else if elapsedSince(record.LastUsedAt, now, settings.MaterializationIdleTTL) {
			return "materialization_idle_ttl", fmt.Sprintf(
				"%s was detached (%s) and has not been used for %s; a detached handle is not a fork "+
					"source, so the registry record survives untouched and a later run that ranks it "+
					"drops it from its own selection and starts cold — losing the handle costs one run's "+
					"exploration rather than a lineage",
				ctxID, detachReasonOrUnrecorded(record.DetachReason),
				now.Sub(record.LastUsedAt.UTC()).Round(time.Minute),
			), true
		}
	}
	if record.State == MaterializationOrphaned &&
		elapsedSince(record.LastUsedAt, now, settings.UnpublishedMaterializationTTL) {
		// An orphan is always an unpublished provisional child, so the window is
		// right in both cases; only the sentence differs, and a rerouted run is not
		// a failed one.
		cause := "belongs to a run that failed or was cancelled"
		if record.DetachReason == MaterializationDetachRerouted {
			cause = "was left behind when its run was rerouted to a different target, " +
				"so its provisional context was retired unpublished"
		}
		return "unpublished_materialization_ttl", fmt.Sprintf(
			"%s %s and its grace period has passed", ctxID, cause,
		), true
	}
	if record.State == MaterializationReady &&
		elapsedSince(record.LastUsedAt, now, settings.MaterializationIdleTTL) {
		return "materialization_idle_ttl", fmt.Sprintf(
			"%s has not been used for %s; the registry record survives untouched, and a later run that "+
				"ranks it finds no local handle, drops it from its own selection and starts cold — so "+
				"losing the handle costs one run's exploration rather than a lineage",
			ctxID, now.Sub(record.LastUsedAt.UTC()).Round(time.Minute),
		), true
	}
	// The retired grace is the one window that needs layer 1 to answer, because
	// "is this context retired" is not a fact this catalog holds. A handle whose
	// registry record cannot be read is left alone: an unreachable board is not
	// evidence that a context was retired.
	if !elapsedSince(record.LastUsedAt, now, settings.RetiredTTL) {
		return "", "", false
	}
	registry, err := m.materializationAuthority.Context(ctx, ctxID)
	if err != nil || registry == nil {
		return "", "", false
	}
	if registry.State != "retired" {
		return "", "", false
	}
	if !elapsedSince(registry.UpdatedAt, now, settings.RetiredTTL) {
		return "", "", false
	}
	return "retired_ttl", fmt.Sprintf(
		"%s was retired (%s) more than the retired grace ago, so this machine releases the handle "+
			"behind it", ctxID, registry.StateReason,
	), true
}

// detachReasonOrUnrecorded keeps the rationale honest when the reason is absent.
//
// Every production transition to `detached` records one, but a record written by
// an older build has none, and printing an empty parenthesis would read as a
// reason rather than as its absence.
func detachReasonOrUnrecorded(reason MaterializationDetachReason) string {
	if reason == "" {
		return "reason not recorded"
	}
	return string(reason)
}

// elapsedSince reports whether an anchored age has passed a window.
//
// A zero anchor is never elapsed, for the same reason the registry side treats
// it that way: an age nobody recorded is unknown, and deleting on unknown would
// take exactly the records whose evidence is already incomplete.
func elapsedSince(anchor, now time.Time, window time.Duration) bool {
	if anchor.IsZero() || window <= 0 {
		return false
	}
	return !now.Before(anchor.UTC().Add(window))
}
