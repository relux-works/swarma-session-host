package sessioncontext

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Original native retention regressions transferred through the actual Service.
// Typed authority fixtures supply source decisions; they do not implement retention.
// The layer-2 half of retention decides nothing and executes everything, so
// every test here is about what it refuses to execute: a handle a live run is
// using, a handle whose registry record has not actually been retired, and a
// quarantined record whose disagreement is the only evidence anybody has.

func legacyRetentionTransferSweepManager(t *testing.T, records ...*sm.ContextRecord) *Service {
	t.Helper()
	manager, _ := legacyRetentionTransferSweepManagerWithAuthority(t, records...)
	return manager
}

func legacyRetentionTransferSweepManagerWithAuthority(
	t *testing.T,
	records ...*sm.ContextRecord,
) (*Service, *legacyRetentionTransferAuthority) {
	t.Helper()
	layout := legacyRetentionTransferLayout(t, "retention-sweep")
	if err := ensurePrivateDirectory(layout.StateDir); err != nil {
		t.Fatalf("ensurePrivateDirectories: %v", err)
	}
	authority := legacyRetentionTransferNewAuthority(records...)
	return legacyRetentionTransferOpen(t, layout, authority), authority
}

// legacyRetentionTransferReadyContext is a published registry record the fake authority can
// serve back to the sweep.
func legacyRetentionTransferReadyContext(id string) *sm.ContextRecord {
	record := legacyRetentionTransferContext(id, true)
	record.State = "ready"
	record.ReadyAt = legacyRetentionTransferClock
	record.Revision = 2
	return cloneContext(record)
}

// legacyRetentionTransferRetiredContext is a tombstone old enough that the retired grace has
// passed.
//
// Fixtures that want to prove a guard use it deliberately: without a retired
// registry record, nothing would sweep the handle anyway and the test would pass
// on the absence of evidence rather than on the guard.
func legacyRetentionTransferRetiredContext(id string) *sm.ContextRecord {
	record := legacyRetentionTransferContext(id, true)
	record.State = "retired"
	record.StateReason = "retention_expired"
	record.ReadyAt = legacyRetentionTransferClock.Add(-90 * 24 * time.Hour)
	record.CreatedAt = record.ReadyAt
	record.UpdatedAt = legacyRetentionTransferClock.Add(-90 * 24 * time.Hour)
	record.Revision = 3
	return cloneContext(record)
}

func legacyRetentionTransferSeedMaterialization(
	manager *Service,
	ctxID string,
	state MaterializationState,
	lastUsed time.Time,
	mutate func(*materializationRecord),
) *materializationRecord {
	record := &materializationRecord{
		CtxID:            normalizeContextID(ctxID),
		Provider:         "codex",
		BindingScope:     "element",
		BindingElementID: "TASK-260728-bbbbbb",
		TargetElementID:  "TASK-260728-bbbbbb",
		State:            state,
		Source:           MaterializationSourceForked,
		CreatedAt:        lastUsed,
		UpdatedAt:        lastUsed,
		LastUsedAt:       lastUsed,
	}
	if mutate != nil {
		mutate(record)
	}
	manager.mu.Lock()
	manager.materializations[record.CtxID] = record
	manager.mu.Unlock()
	return record
}

func legacyRetentionTransferSweep(t *testing.T, manager *Service, dryRun bool) ContextRetentionSweepReport {
	t.Helper()
	report, err := manager.SweepContextRetention(
		context.Background(),
		ContextRetentionSweepRequest{DryRun: dryRun},
	)
	if err != nil {
		t.Fatalf("SweepContextRetention: %v", err)
	}
	return report
}

func legacyRetentionTransferOutcome(
	report ContextRetentionSweepReport,
	ctxID string,
) *ContextMaterializationSweepOutcome {
	for index := range report.Materializations {
		if report.Materializations[index].CtxID == normalizeContextID(ctxID) {
			return &report.Materializations[index]
		}
	}
	return nil
}

func legacyRetentionTransferPresent(manager *Service, ctxID string) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	_, ok := manager.materializations[normalizeContextID(ctxID)]
	return ok
}

// TestADetachedUnpublishedSessionIsReleasedPastItsWindow is the unpublished TTL.
// A completed run nobody registered is reattachable for a while and then it is
// unreachable debris still holding whatever the run read.
func TestADetachedUnpublishedSessionIsReleasedPastItsWindow(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t)
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00001", MaterializationDetached,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.DetachReason = MaterializationDetachUnpublished
		})

	report := legacyRetentionTransferSweep(t, manager, false)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-n00001")
	if outcome == nil || outcome.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the handle dropped", outcome)
	}
	if outcome.Rule != "unpublished_materialization_ttl" {
		t.Fatalf("rule = %s, want the unpublished window", outcome.Rule)
	}
	if legacyRetentionTransferPresent(manager, "CTX-260801-n00001") {
		t.Fatalf("the catalog still holds a released handle")
	}
}

// TestAnIdleHandleIsReleasedAndItsRegistryRecordSurvives is the idle TTL and its
// boundary in one: losing an idle handle costs the next run its exploration,
// never a lineage, so a fresh handle must be left alone.
func TestAnIdleHandleIsReleasedAndItsRegistryRecordSurvives(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t)
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00002", MaterializationReady,
		legacyRetentionTransferClock.Add(-48*time.Hour), nil)
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00003", MaterializationReady,
		legacyRetentionTransferClock.Add(-time.Hour), nil)

	report := legacyRetentionTransferSweep(t, manager, false)
	if outcome := legacyRetentionTransferOutcome(report, "CTX-260801-n00002"); outcome == nil ||
		outcome.Rule != "materialization_idle_ttl" {
		t.Fatalf("an idle handle was not released: %+v", outcome)
	}
	if legacyRetentionTransferOutcome(report, "CTX-260801-n00003") != nil {
		t.Fatalf("a handle used an hour ago was released")
	}
	if !legacyRetentionTransferPresent(manager, "CTX-260801-n00003") {
		t.Fatalf("the fresh handle was dropped")
	}
}

// TestALiveHandleIsNeverReleased attacks the gate: each state a live run can be
// in is aged far past every window, and none of them may be swept.
func TestALiveHandleIsNeverReleased(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*materializationRecord)
	}{
		{"leased by a run", func(r *materializationRecord) {
			r.State = MaterializationLeased
			r.ActiveRunID = "RUN-260801-000001"
			r.LeaseAcquiredAt = legacyRetentionTransferClock.Add(-48 * time.Hour)
		}},
		{"still materializing", func(r *materializationRecord) {
			r.State = MaterializationMaterializing
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// The registry record is a long-retired tombstone, so the retired grace
			// would release this handle on its own. The only thing standing between
			// the sweep and a live run's provider handle is the lease guard.
			manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferRetiredContext("CTX-260801-n00004"))
			legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00004", MaterializationReady,
				legacyRetentionTransferClock.Add(-90*24*time.Hour), test.mutate)

			report := legacyRetentionTransferSweep(t, manager, false)
			if outcome := legacyRetentionTransferOutcome(report, "CTX-260801-n00004"); outcome != nil {
				t.Fatalf("a live handle was swept: %+v", outcome)
			}
			if !legacyRetentionTransferPresent(manager, "CTX-260801-n00004") {
				t.Fatalf("a live handle was dropped from the catalog")
			}
			if report.LeasedContexts != 1 {
				t.Fatalf("leased contexts = %d, want the live one counted",
					report.LeasedContexts)
			}
		})
	}
}

// TestAQuarantinedHandleIsNeverSwept protects the evidence. Quarantine means
// this machine found a disagreement it could not resolve, and deleting the
// evidence of an unresolved disagreement makes it permanent.
func TestAQuarantinedHandleIsNeverSwept(t *testing.T) {
	// Retired long ago, so every window has passed and the quarantine guard is
	// the only reason this evidence survives.
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferRetiredContext("CTX-260801-n00005"))
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00005", MaterializationQuarantined,
		legacyRetentionTransferClock.Add(-90*24*time.Hour), func(r *materializationRecord) {
			r.QuarantineReason = "binding disagreement"
		})

	report := legacyRetentionTransferSweep(t, manager, false)
	if outcome := legacyRetentionTransferOutcome(report, "CTX-260801-n00005"); outcome != nil {
		t.Fatalf("a quarantined handle was swept: %+v", outcome)
	}
	if !legacyRetentionTransferPresent(manager, "CTX-260801-n00005") {
		t.Fatalf("the quarantined evidence was dropped")
	}
}

// TestARetiredGraceNeedsAnActualRetirement is the ordering gate on this plane.
// Age alone is not a reason to release a handle: the registry has to say the
// context is retired, and an unreachable or still-ready record must not be read
// as one.
func TestARetiredGraceNeedsAnActualRetirement(t *testing.T) {
	ready := legacyRetentionTransferReadyContext("CTX-260801-n00006")
	// Old enough that every age window has passed. What must stop the sweep is
	// the registry saying this context is still servable, not the clock.
	ready.ReadyAt = legacyRetentionTransferClock.Add(-90 * 24 * time.Hour)
	ready.CreatedAt = ready.ReadyAt
	ready.UpdatedAt = ready.ReadyAt
	manager := legacyRetentionTransferSweepManager(t, ready)
	// Old enough for the retired grace, but in a state no other window covers.
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00006", MaterializationOrphaned,
		legacyRetentionTransferClock.Add(-48*time.Hour), nil)
	// Detached-unpublished would sweep on its own window, so this uses orphaned
	// with the unpublished window raised out of the way.
	report, err := manager.SweepContextRetention(context.Background(), ContextRetentionSweepRequest{
		Settings: sm.ContextRetentionSettings{
			// Every other window is pushed out of reach so exactly one rule can
			// fire. ReadyTTL in particular: without it the registry half would
			// retire this record during the same pass, and the handle would then be
			// released for a retirement that had genuinely happened — which is the
			// correct behaviour, and would prove nothing about this guard.
			ReadyTTL:                      365 * 24 * time.Hour,
			UnpublishedMaterializationTTL: 365 * 24 * time.Hour,
			MaterializationIdleTTL:        365 * 24 * time.Hour,
			RetiredTTL:                    time.Hour,
		},
	})
	if err != nil {
		t.Fatalf("SweepContextRetention: %v", err)
	}
	if outcome := legacyRetentionTransferOutcome(report, "CTX-260801-n00006"); outcome != nil {
		t.Fatalf("a handle was released while its registry record is still ready: %+v", outcome)
	}
	if !legacyRetentionTransferPresent(manager, "CTX-260801-n00006") {
		t.Fatalf("a handle for a servable context was dropped")
	}
}

// TestTheRegistryHalfSeesThisMachinesLeases is the join between the planes: the
// sweep must assert lease visibility and hand over the set it read, or the board
// would plan against unknown lease state.
func TestTheRegistryHalfSeesThisMachinesLeases(t *testing.T) {
	expired := legacyRetentionTransferReadyContext("CTX-260801-n00007")
	expired.ReadyAt = legacyRetentionTransferClock.Add(-90 * 24 * time.Hour)
	expired.CreatedAt = expired.ReadyAt
	expired.UpdatedAt = expired.ReadyAt
	manager := legacyRetentionTransferSweepManager(t, expired)
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00007", MaterializationLeased,
		legacyRetentionTransferClock, func(r *materializationRecord) {
			r.ActiveRunID = "RUN-260801-000002"
			r.LeaseAcquiredAt = legacyRetentionTransferClock
		})

	manager.materializationAuthority.(*legacyRetentionTransferAuthority).sweep = func(r sm.RetentionSweepRequest) (*sm.RetentionSweepResult, error) {
		if r.LeaseVisibility != "observed" {
			t.Fatal("service did not supply observed lease visibility")
		}
		if _, ok := r.Leased[expired.ID]; !ok {
			t.Fatal("service omitted its actual writer lease")
		}
		return &sm.RetentionSweepResult{Plan: sm.ContextRetentionSweep{Held: []sm.ContextRetentionHold{{CtxID: expired.ID, Reason: "leased"}}}}, nil
	}
	report := legacyRetentionTransferSweep(t, manager, false)
	if report.Registry == nil {
		t.Fatalf("the registry half did not run")
	}
	if report.Registry.Plan.Refusal != "" {
		t.Fatalf("the sweep did not assert lease visibility: %s", report.Registry.Plan.Refusal)
	}
	if len(report.Registry.Retired) != 0 {
		t.Fatalf("a context this machine has leased was retired: %+v", report.Registry.Retired)
	}
	var leaseHold bool
	for _, hold := range report.Registry.Plan.Held {
		if hold.Reason == "leased" {
			leaseHold = true
		}
	}
	if !leaseHold {
		t.Fatalf("the lease this machine holds was not reported to the board: %+v",
			report.Registry.Plan.Held)
	}
}

// TestADryRunReleasesNothing keeps the operator inspection honest across both
// planes.
func TestADryRunReleasesNothing(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t)
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00008", MaterializationDetached,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.DetachReason = MaterializationDetachUnpublished
		})

	report := legacyRetentionTransferSweep(t, manager, true)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-n00008")
	if outcome == nil || outcome.Action != ContextMaterializationPlanned {
		t.Fatalf("outcome = %+v, want a planned release", outcome)
	}
	if !legacyRetentionTransferPresent(manager, "CTX-260801-n00008") {
		t.Fatalf("a dry run released the handle")
	}
	if !report.DryRun {
		t.Fatalf("the report did not say it was a dry run")
	}
}

// TestASweepWithoutAnAuthorityRefuses keeps the two planes' order: layer 2
// executes what layer 1 decided, so with no layer 1 there is nothing to execute.
func TestASweepWithoutAnAuthorityRefuses(t *testing.T) {
	layout := legacyRetentionTransferLayout(t, "retention-sweep-no-authority")
	if err := ensurePrivateDirectory(layout.StateDir); err != nil {
		t.Fatalf("ensurePrivateDirectories: %v", err)
	}
	manager, err := legacyRetentionTransferOpenResult(layout, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	manager.now = func() time.Time { return legacyRetentionTransferClock }

	_, err = manager.SweepContextRetention(context.Background(), ContextRetentionSweepRequest{})
	if err == nil {
		t.Fatalf("a sweep ran with no layer-1 authority configured")
	}
	if !strings.Contains(err.Error(), "authority") {
		t.Fatalf("refusal = %q, want it to name the missing authority", err.Error())
	}
}

// --- The bytes a released handle named --------------------------------------

// legacyRetentionTransferSeedCodex seeds a handle that names real rollout files, which
// is the only shape that can prove anything about reclamation.
//
// A record with no provider host names nothing, so a sweep over one cannot
// distinguish "the bytes went" from "there were never any bytes" — and that is
// exactly how a release that stranded a live transcript looked correct.
func legacyRetentionTransferSeedCodex(
	t *testing.T,
	manager *Service,
	ctxID string,
	threadID string,
	state MaterializationState,
	lastUsed time.Time,
	mutate func(*sm.CodexRunnerSnapshot),
) (codexHome, live, archived string) {
	t.Helper()
	codexHome = t.TempDir()
	live = legacyRetentionTransferWriteLive(t, codexHome, threadID, "owned-live\n")
	archived = legacyRetentionTransferWriteArchived(t, codexHome, threadID, "owned-archived\n")
	snapshot := sm.CodexRunnerSnapshot{
		Contract:            legacyRetentionTransferCodexContract,
		PID:                 os.Getpid(),
		URL:                 "ws://127.0.0.1:1",
		ThreadID:            threadID,
		ProjectDir:          t.TempDir(),
		CodexHome:           codexHome,
		ProviderVersion:     "test-v1",
		LastCompletedTurnID: "turn-owned",
	}
	if mutate != nil {
		mutate(&snapshot)
	}
	legacyRetentionTransferSeedMaterialization(manager, ctxID, state, lastUsed, func(r *materializationRecord) {
		r.ProviderHost = sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex", Codex: &snapshot}}
		r.PayloadSHA256 = strings.Repeat("a", 64)
	})
	return codexHome, live, archived
}

// TestAReleasedHandleTakesItsProviderBytesWithIt is the invariant the whole
// release path exists for.
//
// The private catalog entry is the only name a CTX id has on this machine.
// Dropping it while the transcript is still on disk does not free the disk or
// close the exposure the idle window exists for — it makes both permanent,
// because the byte reclaimer resolves its target *through* that entry and
// afterwards answers that no materialization holds a handle. So the assertion is
// not "the entry went": it is that the files went with it.
func TestAReleasedHandleTakesItsProviderBytesWithIt(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00001"))
	_, live, archived := legacyRetentionTransferSeedCodex(
		t, manager, "CTX-260801-r00001", "thread-owned-release",
		MaterializationReady, legacyRetentionTransferClock.Add(-48*time.Hour), nil)

	report := legacyRetentionTransferSweep(t, manager, false)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-r00001")
	if outcome == nil || outcome.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the idle handle released", outcome)
	}
	if outcome.Rule != "materialization_idle_ttl" {
		t.Fatalf("rule = %s, want the idle window", outcome.Rule)
	}
	if outcome.Files != 2 {
		t.Fatalf("files = %d, want the live and archived rollouts counted", outcome.Files)
	}
	legacyRetentionTransferAssertGone(t, live, "the live rollout the released handle named")
	legacyRetentionTransferAssertGone(t, archived, "the archived rollout, which Codex moved rather than deleted")
	if legacyRetentionTransferPresent(manager, "CTX-260801-r00001") {
		t.Fatalf("the catalog still holds a released handle")
	}

	// The stranding itself, stated as an assertion: after the release nothing can
	// name those bytes again, so if the sweep had not deleted them nothing ever
	// would.
	reclaim, err := manager.ReclaimContextPayloads(
		context.Background(), ContextReclamationRequest{})
	if err != nil {
		t.Fatalf("ReclaimContextPayloads: %v", err)
	}
	for _, result := range reclaim.Outcomes {
		if result.CtxID == "CTX-260801-r00001" && result.Status == ContextReclamationDone {
			t.Fatalf("the payload reclaimer found bytes the sweep should already have taken")
		}
	}
}

// TestAHandleIsKeptWhenItsBytesCannotBeProvedGone is the fail-closed half.
//
// A handle whose files could not be enumerated or deleted is the one case where
// the bytes exist and the name is about to disappear. Releasing it anyway would
// convert a retryable failure into a permanent leak, so the entry is kept, the
// failure is reported, and the next pass tries again.
func TestAHandleIsKeptWhenItsBytesCannotBeProvedGone(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00002"))
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-r00002", MaterializationReady,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			// A handle that is present and unreadable: it names bytes this machine
			// cannot enumerate, which is the shape that must never be released.
			r.ProviderHost = sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex", Claude: &sm.ClaudeRunnerSnapshot{}}}
		})

	report := legacyRetentionTransferSweep(t, manager, false)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-r00002")
	if outcome == nil || outcome.Action != ContextMaterializationSweepFailed {
		t.Fatalf("outcome = %+v, want the release refused", outcome)
	}
	if !strings.Contains(outcome.Detail, "could not be released") {
		t.Fatalf("the refusal did not say the bytes are still there: %q", outcome.Detail)
	}
	if !legacyRetentionTransferPresent(manager, "CTX-260801-r00002") {
		t.Fatalf("a handle was dropped although its bytes could not be proved gone")
	}
}

// TestAnAdoptedThreadIsReleasedWithoutDeletingTheOperatorsBytes keeps the
// ownership refusal intact on the new path.
//
// An adopted thread is the operator's own conversation, exposed into the private
// home by symlink. The board created none of those bytes, so there is nothing to
// strand and nothing to delete — the handle is released and the conversation
// stays.
func TestAnAdoptedThreadIsReleasedWithoutDeletingTheOperatorsBytes(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00003"))
	_, live, archived := legacyRetentionTransferSeedCodex(
		t, manager, "CTX-260801-r00003", "thread-adopted-release",
		MaterializationReady, legacyRetentionTransferClock.Add(-48*time.Hour),
		func(s *sm.CodexRunnerSnapshot) { s.Adopted = true })

	report := legacyRetentionTransferSweep(t, manager, false)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-r00003")
	if outcome == nil || outcome.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the handle released", outcome)
	}
	if outcome.Files != 0 {
		t.Fatalf("files = %d, want nothing deleted for an adopted thread", outcome.Files)
	}
	if !strings.Contains(outcome.Detail, "adopted") {
		t.Fatalf("the outcome did not say why nothing was deleted: %q", outcome.Detail)
	}
	legacyRetentionTransferAssertSurvives(t, live, "an adopted thread is the operator's own conversation")
	legacyRetentionTransferAssertSurvives(t, archived, "an adopted thread's archive is the operator's own too")
}

// TestADryRunReleasesNoBytes keeps the safe mode safe now that the release
// deletes provider files: a dry run that removed a transcript would have
// performed the irreversible half while reporting a plan.
func TestADryRunReleasesNoBytes(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00004"))
	_, live, archived := legacyRetentionTransferSeedCodex(
		t, manager, "CTX-260801-r00004", "thread-dryrun-release",
		MaterializationReady, legacyRetentionTransferClock.Add(-48*time.Hour), nil)

	report := legacyRetentionTransferSweep(t, manager, true)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-r00004")
	if outcome == nil || outcome.Action != ContextMaterializationPlanned {
		t.Fatalf("outcome = %+v, want a planned release", outcome)
	}
	legacyRetentionTransferAssertSurvives(t, live, "a dry run deletes nothing")
	legacyRetentionTransferAssertSurvives(t, archived, "a dry run deletes nothing")
	if !legacyRetentionTransferPresent(manager, "CTX-260801-r00004") {
		t.Fatalf("a dry run dropped the handle")
	}
}

// TestReleasingARetiredContextConfirmsTheReclamation closes the convergence
// loop. Without the confirmation the payload sweep re-plans a context whose
// files this pass already removed, finds no handle able to name them, and
// re-reports it forever.
func TestReleasingARetiredContextConfirmsTheReclamation(t *testing.T) {
	retired := legacyRetentionTransferRetiredContext("CTX-260801-r00005")
	manager := legacyRetentionTransferSweepManager(t, retired)
	_, live, _ := legacyRetentionTransferSeedCodex(
		t, manager, "CTX-260801-r00005", "thread-retired-release",
		MaterializationReady, legacyRetentionTransferClock.Add(-90*24*time.Hour), nil)

	report := legacyRetentionTransferSweep(t, manager, false)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-r00005")
	if outcome == nil || outcome.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the retired handle released", outcome)
	}
	legacyRetentionTransferAssertGone(t, live, "a retired context's rollout")
	record, err := manager.materializationAuthority.Context(context.Background(), retired.ID)
	if err != nil {
		t.Fatalf("reading back the registry record: %v", err)
	}
	if !legacyRetentionTransferRecorded(record.Reclamation) || record.Reclamation.Files != 2 ||
		!record.Reclamation.ArchivedIncluded {
		t.Fatalf("the registry was not told the payload went: %+v", record.Reclamation)
	}
}

// TestAClaudeHandleIsKeptWhenNothingElseCanNameItsArtifacts is the Claude half
// of the release precondition.
//
// Claude transcripts are never deleted from the retention sweep: the only safe
// deleter for them is the manifest-driven artifact reclaimer, which proves
// ownership from the record the materializer wrote rather than from a path this
// layer reconstructed. That makes the artifact record the thing keeping those
// bytes reachable, so a handle with no such record must not be released — after
// the drop nothing on this machine would name the transcript again.
func TestAClaudeHandleIsKeptWhenNothingElseCanNameItsArtifacts(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00006"))
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-r00006", MaterializationReady,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.Provider = "claude"
			r.ProviderHost = sm.ProviderHostRecord{Provider: "claude", Snapshot: sm.RunnerSnapshot{Kind: "claude", Claude: &sm.ClaudeRunnerSnapshot{Contract: "x"}}}
		})

	report := legacyRetentionTransferSweep(t, manager, false)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-r00006")
	if outcome == nil || outcome.Action != ContextMaterializationSweepFailed {
		t.Fatalf("outcome = %+v, want the release refused", outcome)
	}
	if !strings.Contains(outcome.Detail, "nothing able to name") {
		t.Fatalf("the refusal did not say the transcript would be stranded: %q", outcome.Detail)
	}
	if !legacyRetentionTransferPresent(manager, "CTX-260801-r00006") {
		t.Fatalf("a Claude handle was dropped with nothing left able to name its transcript")
	}

	// The dry run has to reach the same verdict. A plan that applied a weaker
	// precondition than the pass it describes would tell an operator this handle
	// is about to go and then leave it — which is the understatement F7 was about,
	// pointing the other way.
	planned := legacyRetentionTransferOutcome(legacyRetentionTransferSweep(t, manager, true), "CTX-260801-r00006")
	if planned == nil || planned.Action != ContextMaterializationKept {
		t.Fatalf("dry-run outcome = %+v, want the same refusal the applying pass reached", planned)
	}
	if !strings.Contains(planned.Detail, "nothing able to name") {
		t.Fatalf("the planned refusal did not say why: %q", planned.Detail)
	}
}

// TestAClaudeHandleIsReleasedOnceItsArtifactsAreNamed is the same rule's
// positive side, so the refusal above cannot be satisfied by never releasing a
// Claude handle at all.
func TestAClaudeHandleIsReleasedOnceItsArtifactsAreNamed(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00007"))
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-r00007", MaterializationReady,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.Provider = "claude"
			r.ProviderHost = sm.ProviderHostRecord{Provider: "claude", Snapshot: sm.RunnerSnapshot{Kind: "claude", Claude: &sm.ClaudeRunnerSnapshot{Contract: "x"}}}
		})
	if err := recordClaudeReclamationEvent(manager.layout, claudeReclamationEvent{
		ArtifactSetID: "artifactset0007",
		Kind:          claudeReclamationFork,
		State:         claudeReclamationActive,
		Reason:        "materialization_started",
		ChildCtxID:    "CTX-260801-r00007",
		ProjectDir:    t.TempDir(),
	}, func() time.Time { return legacyRetentionTransferClock.Add(-48 * time.Hour) }); err != nil {
		t.Fatalf("recording the artifact record: %v", err)
	}

	report := legacyRetentionTransferSweep(t, manager, false)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-r00007")
	if outcome == nil || outcome.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the handle released", outcome)
	}
	if !strings.Contains(outcome.Detail, "Claude reclamation catalog") {
		t.Fatalf("the outcome did not say what still names the bytes: %q", outcome.Detail)
	}
}

// TestAServableContextsEvictionIsNotReportedAsAReclamation is the ordering rule
// on the confirmation itself.
//
// Releasing an idle handle for a context the registry still advertises is a
// local eviction: this machine drops its copy, and a later run that ranks the
// same base finds no local handle, drops it from its own selection and starts
// cold. Nothing was reclaimed on the board's behalf, and the registry refuses to
// record one against a servable record — so an attempt produces only an
// operator-visible error about a normal, correct eviction.
func TestAServableContextsEvictionIsNotReportedAsAReclamation(t *testing.T) {
	manager, authority := legacyRetentionTransferSweepManagerWithAuthority(
		t, legacyRetentionTransferReadyContext("CTX-260801-r00008"))
	legacyRetentionTransferSeedCodex(
		t, manager, "CTX-260801-r00008", "thread-ready-eviction",
		MaterializationReady, legacyRetentionTransferClock.Add(-48*time.Hour), nil)

	report := legacyRetentionTransferSweep(t, manager, false)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-r00008")
	if outcome == nil || outcome.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the idle handle released", outcome)
	}
	if attempts := authority.reclamationAttempts(); len(attempts) != 0 {
		t.Fatalf("a board reclamation was claimed for a still-servable context: %v", attempts)
	}
}

// --- What the dry run discloses ---------------------------------------------

// TestADryRunDisclosesTheBytesTheApplyingPassWouldDelete is the inspection the
// AC asks for, stated as an equality rather than as a shape.
//
// A dry run that reported the catalog entry going and said nothing about the
// provider transcripts going with it would let an operator approve a deletion
// they were never shown. So the planned pass and the applying pass are run over
// the same state and their file counts must agree — a plan that quietly reported
// zero would fail here even though it "ran" and "printed a plan".
func TestADryRunDisclosesTheBytesTheApplyingPassWouldDelete(t *testing.T) {
	plan := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00009"))
	_, plannedLive, plannedArchived := legacyRetentionTransferSeedCodex(
		t, plan, "CTX-260801-r00009", "thread-dryrun-disclosure",
		MaterializationReady, legacyRetentionTransferClock.Add(-48*time.Hour), nil)

	planned := legacyRetentionTransferOutcome(legacyRetentionTransferSweep(t, plan, true), "CTX-260801-r00009")
	if planned == nil || planned.Action != ContextMaterializationPlanned {
		t.Fatalf("outcome = %+v, want a planned release", planned)
	}
	if planned.Files != 2 {
		t.Fatalf("the dry run reported %d files; it must name the live and archived rollouts "+
			"it is about to delete", planned.Files)
	}
	if !strings.Contains(planned.Detail, "would be deleted") {
		t.Fatalf("the dry run did not say the provider bytes go with the handle: %q", planned.Detail)
	}
	legacyRetentionTransferAssertSurvives(t, plannedLive, "a dry run deletes nothing")
	legacyRetentionTransferAssertSurvives(t, plannedArchived, "a dry run deletes nothing")

	// The same state, applied. The counts must match, because the dry run's claim
	// is that it describes this pass.
	apply := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00009"))
	legacyRetentionTransferSeedCodex(
		t, apply, "CTX-260801-r00009", "thread-dryrun-disclosure",
		MaterializationReady, legacyRetentionTransferClock.Add(-48*time.Hour), nil)
	applied := legacyRetentionTransferOutcome(legacyRetentionTransferSweep(t, apply, false), "CTX-260801-r00009")
	if applied == nil || applied.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the handle released", applied)
	}
	if applied.Files != planned.Files {
		t.Fatalf("the dry run promised %d files and the applying pass deleted %d",
			planned.Files, applied.Files)
	}
}

// TestADryRunSaysWhenTheApplyingPassWouldKeepAHandle is the other half of the
// same honesty.
//
// A handle whose bytes cannot be proved gone is kept, so a dry run that folded
// it into `planned` would promise a drop that will never happen. The refusal is
// its own outcome with its own reason.
func TestADryRunSaysWhenTheApplyingPassWouldKeepAHandle(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00010"))
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-r00010", MaterializationReady,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.ProviderHost = sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex", Claude: &sm.ClaudeRunnerSnapshot{}}}
		})

	outcome := legacyRetentionTransferOutcome(legacyRetentionTransferSweep(t, manager, true), "CTX-260801-r00010")
	if outcome == nil || outcome.Action != ContextMaterializationKept {
		t.Fatalf("outcome = %+v, want the dry run to say the handle would be kept", outcome)
	}
	if !strings.Contains(outcome.Detail, "would keep the handle") {
		t.Fatalf("the dry run did not explain the refusal: %q", outcome.Detail)
	}
	if outcome.Files != 0 {
		t.Fatalf("files = %d, want nothing promised for a handle that would be kept", outcome.Files)
	}
}

// TestAnEvictedIdleHandleLeavesAReadyRecordNothingLocalCanServe is the state the
// selection gate exists for, proved on the plane that creates it.
//
// The registry record survives the eviction by design — the snapshot is intact
// wherever it was built. What must also be true is that this machine now reports
// no handle for it, because that report is the only evidence a later run has
// that it cannot fork this base. If the swept handle were still listed, the run
// would select the base and die at materialization.
func TestAnEvictedIdleHandleLeavesAReadyRecordNothingLocalCanServe(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferReadyContext("CTX-260801-r00011"))
	legacyRetentionTransferSeedCodex(
		t, manager, "CTX-260801-r00011", "thread-evicted-idle",
		MaterializationReady, legacyRetentionTransferClock.Add(-48*time.Hour), nil)

	if outcome := legacyRetentionTransferOutcome(legacyRetentionTransferSweep(t, manager, false), "CTX-260801-r00011"); outcome == nil ||
		outcome.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the idle handle released", outcome)
	}
	for _, view := range manager.ListMaterializations() {
		if normalizeContextID(view.CtxID) == "CTX-260801-r00011" {
			t.Fatalf("the local catalog still advertises a handle the sweep released: %+v", view)
		}
	}
	record, err := manager.materializationAuthority.Context(context.Background(), "CTX-260801-r00011")
	if err != nil || record == nil {
		t.Fatalf("reading back the registry record: %v", err)
	}
	if record.State != "ready" {
		t.Fatalf("state = %q; a local eviction must never invalidate a board-wide record",
			record.State)
	}
}

// TestADetachedPublishedHandleIsNotToldItWasNeverPublished is the rationale a
// closed target produces.
//
// `unpublished_materialization_ttl` is documented as the window a never-published
// session may still be reattached in. A base whose exact target closed was
// published, is still ready on the board, and stays eligible for that target's
// siblings — so telling an operator it "was detached without publication" is
// false about the one fact they would act on, and charging it to the unpublished
// knob means tuning that knob moves a handle it does not describe.
func TestADetachedPublishedHandleIsNotToldItWasNeverPublished(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t)
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00301", MaterializationDetached,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.DetachReason = MaterializationDetachTargetClosed
		})

	report := legacyRetentionTransferSweep(t, manager, false)
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-n00301")
	if outcome == nil || outcome.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the handle dropped", outcome)
	}
	if outcome.Rule != "materialization_idle_ttl" {
		t.Fatalf("rule = %s; a published, target-closed handle is an idle eviction, "+
			"not an unpublished one", outcome.Rule)
	}
	if strings.Contains(outcome.Detail, "without publication") {
		t.Fatalf("a published base was told it was never published: %q", outcome.Detail)
	}
	if !strings.Contains(outcome.Detail, string(MaterializationDetachTargetClosed)) {
		t.Fatalf("the rationale does not say why the handle was detached: %q", outcome.Detail)
	}

	// A record written by an older build carries no reason at all. Interpolating
	// the empty string reads as a reason rather than as its absence, which is the
	// same class of misreport this test exists for.
	unrecorded := legacyRetentionTransferSweepManager(t)
	legacyRetentionTransferSeedMaterialization(unrecorded, "CTX-260801-n00302", MaterializationDetached,
		legacyRetentionTransferClock.Add(-48*time.Hour), nil)
	legacy := legacyRetentionTransferOutcome(legacyRetentionTransferSweep(t, unrecorded, false), "CTX-260801-n00302")
	if legacy == nil || legacy.Rule != "materialization_idle_ttl" {
		t.Fatalf("a detached handle with no recorded reason was not released as an idle one: %+v",
			legacy)
	}
	if !strings.Contains(legacy.Detail, "reason not recorded") {
		t.Fatalf("an absent detach reason was printed as if it were one: %q", legacy.Detail)
	}
}

// TestTheTwoDetachWindowsAreTunedSeparately proves the routing is real rather
// than two sentences over one window.
//
// With the unpublished window shortened and the idle window long, only the
// unpublished handle may go; with them swapped, only the target-closed one. A
// verdict that read the state alone passes neither half.
func TestTheTwoDetachWindowsAreTunedSeparately(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		unpublished time.Duration
		idle        time.Duration
		sweptID     string
		keptID      string
		rule        sm.ContextRetentionRule
	}{
		{
			name: "only the unpublished window has elapsed",
			// The idle window has to outlast the retired grace too: a handle past
			// that grace is swept by layer 1's tombstone regardless of which detach
			// window governs it.
			unpublished: time.Hour, idle: 720 * time.Hour,
			sweptID: "CTX-260801-n00311", keptID: "CTX-260801-n00312",
			rule: "unpublished_materialization_ttl",
		},
		{
			name:        "only the idle window has elapsed",
			unpublished: 720 * time.Hour, idle: time.Hour,
			sweptID: "CTX-260801-n00312", keptID: "CTX-260801-n00311",
			rule: "materialization_idle_ttl",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager := legacyRetentionTransferSweepManager(t)
			settings := normalizeRetentionSettings(sm.ContextRetentionSettings{})
			settings.UnpublishedMaterializationTTL = testCase.unpublished
			settings.MaterializationIdleTTL = testCase.idle
			settings.RetiredTTL = 720 * time.Hour
			legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00311", MaterializationDetached,
				legacyRetentionTransferClock.Add(-24*time.Hour), func(r *materializationRecord) {
					r.DetachReason = MaterializationDetachUnpublished
				})
			legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00312", MaterializationDetached,
				legacyRetentionTransferClock.Add(-24*time.Hour), func(r *materializationRecord) {
					r.DetachReason = MaterializationDetachTargetClosed
				})

			report, err := manager.SweepContextRetention(
				context.Background(),
				ContextRetentionSweepRequest{Settings: settings},
			)
			if err != nil {
				t.Fatalf("SweepContextRetention: %v", err)
			}
			swept := legacyRetentionTransferOutcome(report, testCase.sweptID)
			if swept == nil || swept.Rule != testCase.rule {
				t.Fatalf("%s was not released under %s: %+v",
					testCase.sweptID, testCase.rule, swept)
			}
			if kept := legacyRetentionTransferOutcome(report, testCase.keptID); kept != nil {
				t.Fatalf("%s was swept by the window that does not govern it: %+v",
					testCase.keptID, kept)
			}
		})
	}
}

// TestARerouteOrphanIsNotDescribedAsAFailedRun is the same honesty rule one
// state over. A run that was rerouted to another target did not fail, and an
// operator reading a failure that never happened goes looking for one.
func TestARerouteOrphanIsNotDescribedAsAFailedRun(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t)
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00321", MaterializationOrphaned,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.DetachReason = MaterializationDetachRerouted
		})
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00322", MaterializationOrphaned,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.DetachReason = MaterializationDetachRunFailed
		})

	report := legacyRetentionTransferSweep(t, manager, false)
	rerouted := legacyRetentionTransferOutcome(report, "CTX-260801-n00321")
	if rerouted == nil || rerouted.Rule != "unpublished_materialization_ttl" {
		t.Fatalf("a rerouted orphan was not released under the unpublished window: %+v", rerouted)
	}
	if strings.Contains(rerouted.Detail, "failed or was cancelled") {
		t.Fatalf("a rerouted run was reported as a failed one: %q", rerouted.Detail)
	}
	if !strings.Contains(rerouted.Detail, "rerouted") {
		t.Fatalf("the rationale does not say the run was rerouted: %q", rerouted.Detail)
	}
	failed := legacyRetentionTransferOutcome(report, "CTX-260801-n00322")
	if failed == nil || !strings.Contains(failed.Detail, "failed or was cancelled") {
		t.Fatalf("a genuinely failed run lost its own rationale: %+v", failed)
	}
}

// TestADetachedHandleInsideItsOwnWindowStillObeysTheRetiredGrace guards the
// fall-through the detach-reason routing sits on top of.
//
// The two detach windows answer "may this still be reattached or forked", and
// the retired grace answers a different question that layer 1 owns: the context
// itself is a tombstone, so the handle behind it goes regardless. Deciding the
// detached case and returning would have made a handle unreachable by the one
// rule that outranks both windows, and it would have looked correct because the
// defaults make all three windows 24h.
func TestADetachedHandleInsideItsOwnWindowStillObeysTheRetiredGrace(t *testing.T) {
	manager := legacyRetentionTransferSweepManager(t, legacyRetentionTransferRetiredContext("CTX-260801-n00331"))
	legacyRetentionTransferSeedMaterialization(manager, "CTX-260801-n00331", MaterializationDetached,
		legacyRetentionTransferClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.DetachReason = MaterializationDetachUnpublished
		})

	settings := normalizeRetentionSettings(sm.ContextRetentionSettings{})
	// Neither detach window has elapsed; only the retired grace has.
	settings.UnpublishedMaterializationTTL = 720 * time.Hour
	settings.MaterializationIdleTTL = 720 * time.Hour
	settings.RetiredTTL = time.Hour

	report, err := manager.SweepContextRetention(
		context.Background(),
		ContextRetentionSweepRequest{Settings: settings},
	)
	if err != nil {
		t.Fatalf("SweepContextRetention: %v", err)
	}
	outcome := legacyRetentionTransferOutcome(report, "CTX-260801-n00331")
	if outcome == nil || outcome.Rule != "retired_ttl" {
		t.Fatalf("a detached handle whose context is a tombstone was not released "+
			"under the retired grace: %+v", outcome)
	}
}
