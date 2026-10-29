package sessioncontext

import (
	"context"
	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
	"testing"
	"time"
)

// Relocated original lifecycle regressions: same transitions and assertions,
// real catalog files, source facts injected across the neutral authority port.
func TestMaterializationRerouteAllocatesDistinctSiblingChild(t *testing.T) {
	oldContext := materializationTestContext("CTX-260729-c00002", false)
	newContext := materializationTestContext("CTX-260729-c00003", false)
	authority := newFakeMaterializationAuthority(oldContext, newContext)
	manager := newMaterializationManager(t, testLayout(t, "materialization-reroute"), authority)

	if _, err := manager.CreateMaterialization(context.Background(), CreateMaterializationRequest{
		Context:         oldContext,
		RunProvider:     oldContext.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceForked,
		ProviderHost:    testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcquireMaterializationLease(
		context.Background(),
		oldContext.ID,
		"RUN-reroute-old",
		"TASK-260729-bbbbbb",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateMaterialization(context.Background(), CreateMaterializationRequest{
		Context:         newContext,
		RunProvider:     newContext.Provider,
		TargetElementID: "TASK-260729-cccccc",
		TargetRole:      "developer",
		Source:          MaterializationSourceForked,
		ProviderHost:    testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcquireMaterializationLease(
		context.Background(),
		newContext.ID,
		"RUN-reroute-old",
		"TASK-260729-cccccc",
	); err == nil || !strings.Contains(err.Error(), "already leases") {
		t.Fatalf("same run leased two writable children: %v", err)
	}
	next, err := manager.RerouteMaterialization(context.Background(), RerouteMaterializationRequest{
		OldCtxID: oldContext.ID,
		OldRunID: "RUN-reroute-old",
		New: CreateMaterializationRequest{
			Context:         newContext,
			RunProvider:     newContext.Provider,
			TargetElementID: "TASK-260729-cccccc",
			TargetRole:      "developer",
			Source:          MaterializationSourceForked,
			ProviderHost:    testMaterializationHost(),
		},
		NewRunID: "RUN-reroute-new",
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.CtxID != newContext.ID ||
		next.TargetElementID != "TASK-260729-cccccc" ||
		next.BindingElementID != "STORY-260729-aaaaaa" ||
		next.State != MaterializationLeased ||
		next.ActiveRunID != "RUN-reroute-new" {
		t.Fatalf("rerouted materialization = %#v", next)
	}
	oldView, err := manager.Materialization(oldContext.ID)
	if err != nil {
		t.Fatal(err)
	}
	if oldView.State != MaterializationOrphaned || oldView.ActiveRunID != "" {
		t.Fatalf("old materialization = %#v", oldView)
	}
	retired, err := authority.Context(context.Background(), oldContext.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retired.State != "retired" ||
		retired.StateReason != "superseded" {
		t.Fatalf("old layer-1 context = %#v", retired)
	}
}
func TestSameTargetReroutePreservesMaterializationWhenIdentityIsUnchanged(t *testing.T) {
	contextRecord := materializationTestContext("CTX-260729-c00011", true)
	authority := newFakeMaterializationAuthority(contextRecord)
	manager := newMaterializationManager(t, testLayout(t, "materialization-same-target-reroute"), authority)
	request := CreateMaterializationRequest{
		Context:         contextRecord,
		RunProvider:     contextRecord.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceBuilt,
		ProviderHost:    testMaterializationHost(),
	}
	if _, err := manager.CreateMaterialization(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcquireMaterializationLease(
		context.Background(),
		contextRecord.ID,
		"RUN-same-target-old",
		request.TargetElementID,
	); err != nil {
		t.Fatal(err)
	}
	preserved, err := manager.RerouteMaterialization(context.Background(), RerouteMaterializationRequest{
		OldCtxID: contextRecord.ID,
		OldRunID: "RUN-same-target-old",
		New:      request,
		NewRunID: "RUN-same-target-new",
	})
	if err != nil {
		t.Fatal(err)
	}
	if preserved.CtxID != contextRecord.ID ||
		preserved.ActiveRunID != "RUN-same-target-new" ||
		len(manager.ListMaterializations()) != 1 {
		t.Fatalf("same-target preserved view = %#v", preserved)
	}
}
func TestSameTargetRerouteRejectsRunAlreadyLeasingSiblingMaterialization(t *testing.T) {
	firstContext := materializationTestContext("CTX-260729-c00017", true)
	secondContext := materializationTestContext("CTX-260729-c00018", true)
	authority := newFakeMaterializationAuthority(firstContext, secondContext)
	manager := newMaterializationManager(t, testLayout(t, "materialization-same-target-run-collision"), authority)
	firstRequest := CreateMaterializationRequest{
		Context:         firstContext,
		RunProvider:     firstContext.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceForked,
		ProviderHost:    testMaterializationHost(),
	}
	secondRequest := firstRequest
	secondRequest.Context = secondContext
	if _, err := manager.CreateMaterialization(context.Background(), firstRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateMaterialization(context.Background(), secondRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcquireMaterializationLease(
		context.Background(),
		firstContext.ID,
		"RUN-reroute-collision-first",
		firstRequest.TargetElementID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcquireMaterializationLease(
		context.Background(),
		secondContext.ID,
		"RUN-reroute-collision-second",
		secondRequest.TargetElementID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RerouteMaterialization(context.Background(), RerouteMaterializationRequest{
		OldCtxID: secondContext.ID,
		OldRunID: "RUN-reroute-collision-second",
		New:      secondRequest,
		NewRunID: "RUN-reroute-collision-first",
	}); err == nil || !strings.Contains(err.Error(), "already leases materialization") {
		t.Fatalf("same-target reroute collision error = %v", err)
	}
	second, err := manager.Materialization(secondContext.ID)
	if err != nil {
		t.Fatal(err)
	}
	if second.ActiveRunID != "RUN-reroute-collision-second" {
		t.Fatalf("failed reroute mutated lease = %#v", second)
	}
}
func TestDifferentTargetRerouteValidatesNewSideBeforeRetiringOld(t *testing.T) {
	oldContext := materializationTestContext("CTX-260729-c00023", false)
	newContext := materializationTestContext("CTX-260729-c00024", false)
	authority := newFakeMaterializationAuthority(oldContext, newContext)
	manager := newMaterializationManager(t, testLayout(t, "materialization-reroute-preflight"), authority)
	if _, err := manager.CreateMaterialization(context.Background(), CreateMaterializationRequest{
		Context:         oldContext,
		RunProvider:     oldContext.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceForked,
		ProviderHost:    testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcquireMaterializationLease(
		context.Background(),
		oldContext.ID,
		"RUN-reroute-preflight-old",
		"TASK-260729-bbbbbb",
	); err != nil {
		t.Fatal(err)
	}
	authority.mu.Lock()
	authority.elements["TASK-260729-cccccc"].Status = "closed"
	authority.mu.Unlock()

	if _, err := manager.RerouteMaterialization(context.Background(), RerouteMaterializationRequest{
		OldCtxID: oldContext.ID,
		OldRunID: "RUN-reroute-preflight-old",
		New: CreateMaterializationRequest{
			Context:         newContext,
			RunProvider:     newContext.Provider,
			TargetElementID: "TASK-260729-cccccc",
			TargetRole:      "developer",
			Source:          MaterializationSourceForked,
			ProviderHost:    testMaterializationHost(),
		},
		NewRunID: "RUN-reroute-preflight-new",
	}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("reroute preflight error = %v", err)
	}
	oldView, err := manager.Materialization(oldContext.ID)
	if err != nil {
		t.Fatal(err)
	}
	if oldView.State != MaterializationLeased ||
		oldView.ActiveRunID != "RUN-reroute-preflight-old" {
		t.Fatalf("failed reroute mutated old materialization = %#v", oldView)
	}
	oldLayerOne, err := authority.Context(context.Background(), oldContext.ID)
	if err != nil {
		t.Fatal(err)
	}
	if oldLayerOne.State != "building" {
		t.Fatalf("failed reroute retired old layer-1 context = %#v", oldLayerOne)
	}
	if _, err := manager.Materialization(newContext.ID); err == nil {
		t.Fatal("failed reroute persisted the new materialization")
	}
}
func TestManagerRestartQuarantinesLayerOneBindingMismatch(t *testing.T) {
	contextRecord := materializationTestContext("CTX-260729-c00009", false)
	authority := newFakeMaterializationAuthority(contextRecord)
	layout := testLayout(t, "materialization-binding-mismatch")
	seed := newMaterializationManager(t, layout, authority)
	if _, err := seed.CreateMaterialization(context.Background(), CreateMaterializationRequest{
		Context:         contextRecord,
		RunProvider:     contextRecord.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceForked,
		ProviderHost:    testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}

	authority.mu.Lock()
	authority.contexts[contextRecord.ID].Binding.ElementID = "TASK-260729-bbbbbb"
	authority.mu.Unlock()

	restarted := newMaterializationManager(t, layout, authority)
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	view, err := restarted.Materialization(contextRecord.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.State != MaterializationQuarantined {
		t.Fatalf("binding mismatch restart state = %#v", view)
	}
}
func TestManagerRestartDefersMissingRunEvidenceWithoutQuarantine(t *testing.T) {
	contextRecord := materializationTestContext("CTX-260729-c00022", true)
	authority := newFakeMaterializationAuthority(contextRecord)
	layout := testLayout(t, "materialization-missing-run-evidence")
	seed := newMaterializationManager(t, layout, authority)
	if _, err := seed.CreateMaterialization(context.Background(), CreateMaterializationRequest{
		Context:         contextRecord,
		RunProvider:     contextRecord.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceBuilt,
		ProviderHost:    testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := seed.AcquireMaterializationLease(
		context.Background(),
		contextRecord.ID,
		"RUN-missing-restart-evidence",
		"TASK-260729-bbbbbb",
	); err != nil {
		t.Fatal(err)
	}

	restarted := newMaterializationManager(t, layout, authority)
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	view, err := restarted.Materialization(contextRecord.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.State != MaterializationLeased ||
		view.ActiveRunID != "RUN-missing-restart-evidence" {
		t.Fatalf("lease without run evidence = %#v", view)
	}
	if restarted.QuarantinedMaterializationCount() != 0 {
		t.Fatalf("missing run evidence quarantined materialization")
	}
}
func TestMaterializationReattachRequiresExactTarget(t *testing.T) {
	contextRecord := materializationTestContext("CTX-260729-c00004", true)
	authority := newFakeMaterializationAuthority(contextRecord)
	manager := newMaterializationManager(t, testLayout(t, "materialization-reattach"), authority)
	if _, err := manager.CreateMaterialization(context.Background(), CreateMaterializationRequest{
		Context:         contextRecord,
		RunProvider:     contextRecord.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceBuilt,
		ProviderHost:    testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcquireMaterializationLease(
		context.Background(),
		contextRecord.ID,
		"RUN-reattach-first",
		"TASK-260729-bbbbbb",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CompleteMaterialization(context.Background(), CompleteMaterializationRequest{
		CtxID:         contextRecord.ID,
		RunID:         "RUN-reattach-first",
		Published:     false,
		PayloadSHA256: strings.Repeat("a", 64),
		ProviderHost:  testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ReattachMaterialization(
		context.Background(),
		contextRecord.ID,
		"RUN-reattach-wrong",
		"TASK-260729-cccccc",
	); !isContextErrorCode(err, "context_materialization_divergent") {
		t.Fatalf("different-target reattach error = %v", err)
	}
	reattached, err := manager.ReattachMaterialization(
		context.Background(),
		contextRecord.ID,
		"RUN-reattach-same",
		"TASK-260729-bbbbbb",
	)
	if err != nil {
		t.Fatal(err)
	}
	if reattached.State != MaterializationLeased ||
		reattached.ActiveRunID != "RUN-reattach-same" {
		t.Fatalf("reattached materialization = %#v", reattached)
	}
}
func TestCloseAndGCSkipActiveLeaseThenCollectDetachedTarget(t *testing.T) {
	contextRecord := materializationTestContext("CTX-260729-c00005", false)
	authority := newFakeMaterializationAuthority(contextRecord)
	manager := newMaterializationManager(t, testLayout(t, "materialization-close-gc"), authority)
	if _, err := manager.CreateMaterialization(context.Background(), CreateMaterializationRequest{
		Context:         contextRecord,
		RunProvider:     contextRecord.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceForked,
		ProviderHost:    testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcquireMaterializationLease(
		context.Background(),
		contextRecord.ID,
		"RUN-close-active",
		"TASK-260729-bbbbbb",
	); err != nil {
		t.Fatal(err)
	}
	changed, err := manager.HandleElementClose(context.Background(), "STORY-260729-aaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 1 || changed[0].State != MaterializationLeased {
		t.Fatalf("close during lease = %#v", changed)
	}
	policy := MaterializationGCPolicy{IdleTTL: time.Hour, UnpublishedTTL: time.Hour, OrphanedTTL: time.Hour}
	if candidates := manager.MaterializationGCCandidates(materializationTestClock.Add(48*time.Hour), policy); len(candidates) != 0 {
		t.Fatalf("active lease became collectable: %#v", candidates)
	}

	authority.setContextState(contextRecord.ID, "ready", "")
	manager.now = func() time.Time { return materializationTestClock.Add(2 * time.Hour) }
	completed, err := manager.CompleteMaterialization(context.Background(), CompleteMaterializationRequest{
		CtxID:         contextRecord.ID,
		RunID:         "RUN-close-active",
		Published:     true,
		PayloadSHA256: strings.Repeat("b", 64),
		ProviderHost:  testMaterializationHost(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != MaterializationDetached {
		t.Fatalf("close-pending completion = %#v", completed)
	}
	candidates := manager.MaterializationGCCandidates(materializationTestClock.Add(4*time.Hour), policy)
	if len(candidates) != 1 || candidates[0].CtxID != contextRecord.ID {
		t.Fatalf("detached close candidates = %#v", candidates)
	}
	if err := manager.CollectMaterializations(candidates); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Materialization(contextRecord.ID); err == nil {
		t.Fatal("collected materialization remained in catalog")
	}
}
func TestElementCloseCachesSharedRemoteTopologyForSweep(t *testing.T) {
	first := materializationTestContext("CTX-260729-c00028", false)
	second := materializationTestContext("CTX-260729-c00029", false)
	authority := newFakeMaterializationAuthority(first, second)
	manager := newMaterializationManager(t, testLayout(t, "materialization-close-cache"), authority)
	for _, fixture := range []struct {
		context *sm.ContextRecord
		target  string
	}{
		{context: first, target: "TASK-260729-bbbbbb"},
		{context: second, target: "TASK-260729-cccccc"},
	} {
		if _, err := manager.CreateMaterialization(context.Background(), CreateMaterializationRequest{
			Context:         fixture.context,
			RunProvider:     fixture.context.Provider,
			TargetElementID: fixture.target,
			TargetRole:      "developer",
			Source:          MaterializationSourceForked,
			ProviderHost:    testMaterializationHost(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	authority.mu.Lock()
	authority.reads = make(map[string]int)
	authority.mu.Unlock()

	changed, err := manager.HandleElementClose(
		context.Background(),
		"EPIC-260729-eeeeee",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 2 {
		t.Fatalf("close sweep changes = %#v", changed)
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.reads["STORY-260729-aaaaaa"] != 1 {
		t.Fatalf("shared ancestor reads = %#v", authority.reads)
	}
	if len(authority.reads) != 3 {
		t.Fatalf("close sweep topology cache = %#v", authority.reads)
	}
}
func TestElementCloseSkipsQuarantinedMaterializationWithoutTopologyRead(t *testing.T) {
	contextRecord := materializationTestContext("CTX-260729-c00020", true)
	authority := newFakeMaterializationAuthority(contextRecord)
	manager := newMaterializationManager(t, testLayout(t, "materialization-close-quarantined"), authority)
	if _, err := manager.CreateMaterialization(context.Background(), CreateMaterializationRequest{
		Context:         contextRecord,
		RunProvider:     contextRecord.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceBuilt,
		ProviderHost:    testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	record := manager.materializations[contextRecord.ID]
	manager.quarantineMaterializationLocked(record, errors.New("test divergence"))
	if err := manager.materializationRecords.save(record); err != nil {
		manager.mu.Unlock()
		t.Fatal(err)
	}
	manager.mu.Unlock()
	authority.mu.Lock()
	delete(authority.elements, "TASK-260729-bbbbbb")
	authority.mu.Unlock()

	changed, err := manager.HandleElementClose(context.Background(), "STORY-260729-aaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 0 {
		t.Fatalf("quarantined close changes = %#v", changed)
	}
}
func TestQuarantinedMaterializationRejectsReuseAndBecomesCollectable(t *testing.T) {
	contextRecord := materializationTestContext("CTX-260729-c00025", true)
	authority := newFakeMaterializationAuthority(contextRecord)
	manager := newMaterializationManager(t, testLayout(t, "materialization-quarantine-gc"), authority)
	request := CreateMaterializationRequest{
		Context:         contextRecord,
		RunProvider:     contextRecord.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceBuilt,
		ProviderHost:    testMaterializationHost(),
	}
	if _, err := manager.CreateMaterialization(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	record := manager.materializations[contextRecord.ID]
	manager.quarantineMaterializationLocked(record, errors.New("test divergence"))
	if err := manager.materializationRecords.save(record); err != nil {
		manager.mu.Unlock()
		t.Fatal(err)
	}
	manager.mu.Unlock()

	if _, err := manager.CreateMaterialization(context.Background(), request); err == nil {
		t.Fatal("quarantined materialization was returned as a successful create")
	}
	policy := MaterializationGCPolicy{
		IdleTTL:        time.Hour,
		UnpublishedTTL: time.Hour,
		OrphanedTTL:    time.Hour,
	}
	candidates := manager.MaterializationGCCandidates(
		materializationTestClock.Add(2*time.Hour),
		policy,
	)
	if len(candidates) != 1 ||
		candidates[0].CtxID != contextRecord.ID ||
		candidates[0].Reason != "quarantined" {
		t.Fatalf("quarantine GC candidates = %#v", candidates)
	}
	if err := manager.CollectMaterializations(candidates); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Materialization(contextRecord.ID); err == nil {
		t.Fatal("collected quarantined materialization remained in catalog")
	}
}
func TestElementCloseFailsClosedOnNilTopologyEvidence(t *testing.T) {
	contextRecord := materializationTestContext("CTX-260729-c00021", true)
	authority := newFakeMaterializationAuthority(contextRecord)
	manager := newMaterializationManager(t, testLayout(t, "materialization-close-nil-evidence"), authority)
	if _, err := manager.CreateMaterialization(context.Background(), CreateMaterializationRequest{
		Context:         contextRecord,
		RunProvider:     contextRecord.Provider,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Source:          MaterializationSourceBuilt,
		ProviderHost:    testMaterializationHost(),
	}); err != nil {
		t.Fatal(err)
	}
	manager.materializationAuthority = nilElementMaterializationAuthority{authority}
	if _, err := manager.HandleElementClose(
		context.Background(),
		"STORY-260729-aaaaaa",
	); err == nil || !strings.Contains(err.Error(), "evidence") {
		t.Fatalf("nil topology evidence error = %v", err)
	}
}
func TestFailedAndIdleMaterializationsBecomeCollectable(t *testing.T) {
	failedContext := materializationTestContext("CTX-260729-c00012", true)
	readyContext := materializationTestContext("CTX-260729-c00013", true)
	authority := newFakeMaterializationAuthority(failedContext, readyContext)
	manager := newMaterializationManager(t, testLayout(t, "materialization-fail-idle-gc"), authority)

	for _, fixture := range []struct {
		record *sm.ContextRecord
		runID  string
	}{
		{failedContext, "RUN-failed-materialization"},
		{readyContext, "RUN-ready-materialization"},
	} {
		if _, err := manager.CreateMaterialization(context.Background(), CreateMaterializationRequest{
			Context:         fixture.record,
			RunProvider:     fixture.record.Provider,
			TargetElementID: "TASK-260729-bbbbbb",
			TargetRole:      "developer",
			Source:          MaterializationSourceBuilt,
			ProviderHost:    testMaterializationHost(),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := manager.AcquireMaterializationLease(
			context.Background(),
			fixture.record.ID,
			fixture.runID,
			"TASK-260729-bbbbbb",
		); err != nil {
			t.Fatal(err)
		}
	}
	failed, err := manager.FailMaterialization(
		context.Background(),
		failedContext.ID,
		"RUN-failed-materialization",
	)
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != MaterializationOrphaned {
		t.Fatalf("failed materialization = %#v", failed)
	}
	authority.setContextState(readyContext.ID, "ready", "")
	ready, err := manager.CompleteMaterialization(context.Background(), CompleteMaterializationRequest{
		CtxID:         readyContext.ID,
		RunID:         "RUN-ready-materialization",
		Published:     true,
		PayloadSHA256: strings.Repeat("c", 64),
		ProviderHost:  testMaterializationHost(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if ready.State != MaterializationReady {
		t.Fatalf("ready materialization = %#v", ready)
	}

	policy := MaterializationGCPolicy{IdleTTL: time.Hour, UnpublishedTTL: time.Hour, OrphanedTTL: time.Hour}
	candidates := manager.MaterializationGCCandidates(materializationTestClock.Add(2*time.Hour), policy)
	if len(candidates) != 2 ||
		candidates[0].CtxID != failedContext.ID ||
		candidates[1].CtxID != readyContext.ID {
		t.Fatalf("failure/idle candidates = %#v", candidates)
	}
}
func TestMaterializationMirrorAndTargetValidation(t *testing.T) {
	record := validMaterializationRecord()
	contextRecord := materializationTestContext(record.CtxID, true)
	if err := validateMaterializationMirror(record, contextRecord); err != nil {
		t.Fatalf("valid mirror rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*sm.ContextRecord)
	}{
		{"id", func(contextRecord *sm.ContextRecord) { contextRecord.ID = "CTX-260729-c09998" }},
		{"provider", func(contextRecord *sm.ContextRecord) {
			contextRecord.Provider = "claude"
		}},
		{"binding", func(contextRecord *sm.ContextRecord) {
			contextRecord.Binding.ElementID = "STORY-260729-aaaaaa"
		}},
		{"digest", func(contextRecord *sm.ContextRecord) {
			contextRecord.Payload.SHA256 = strings.Repeat("a", 64)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneContext(contextRecord)
			recordCopy := *record
			recordCopy.PayloadSHA256 = strings.Repeat("b", 64)
			test.mutate(candidate)
			if err := validateMaterializationMirror(&recordCopy, candidate); err == nil {
				t.Fatal("divergent mirror was accepted")
			}
		})
	}
	if err := validateMaterializationMirror(record, nil); err == nil {
		t.Fatal("missing layer-1 context was accepted")
	}

	if target, err := validateMaterializationTarget(
		context.Background(),
		nil,
		"element",
		record.BindingElementID,
		record.TargetElementID,
	); err != nil || target != nil {
		t.Fatalf("exact target without authority = %#v, %v", target, err)
	}
	if _, err := validateMaterializationTarget(
		context.Background(),
		nil,
		"element",
		"STORY-260729-aaaaaa",
		record.TargetElementID,
	); err == nil {
		t.Fatal("descendant target without authority was accepted")
	}
	authority := newFakeMaterializationAuthority(contextRecord)
	if target, err := validateMaterializationTarget(
		context.Background(),
		authority,
		"board",
		"",
		record.TargetElementID,
	); err != nil || string(target.ID) != record.TargetElementID {
		t.Fatalf("board target validation = %#v, %v", target, err)
	}
	if _, err := validateMaterializationTarget(
		context.Background(),
		authority,
		"element",
		"TASK-260729-cccccc",
		record.TargetElementID,
	); err == nil || !strings.Contains(err.Error(), "not the binding element") {
		t.Fatalf("sibling target validation error = %v", err)
	}
	if _, err := validateMaterializationTarget(
		context.Background(),
		authority,
		"element",
		record.BindingElementID,
		"TASK-260729-dddddd",
	); !errors.Is(err, ErrMaterializationEvidenceNotFound) {
		t.Fatalf("missing target validation error = %v", err)
	}
	authority.elements["TASK-260729-bbbbbb"].ParentID = "STORY-260729-aaaaaa"
	authority.elements["STORY-260729-aaaaaa"].ParentID = "TASK-260729-bbbbbb"
	if _, err := validateMaterializationTarget(
		context.Background(),
		authority,
		"element",
		"EPIC-260729-eeeeee",
		record.TargetElementID,
	); err == nil || !strings.Contains(err.Error(), "ancestry cycle") {
		t.Fatalf("cycle target validation error = %v", err)
	}
}

func TestManagerRestartReconcilesExactRunEvidenceAndQuarantinesMismatch(t *testing.T) {
	validContext := materializationTestContext("CTX-260729-c00006", true)
	mismatchContext := materializationTestContext("CTX-260729-c00007", true)
	authority := newFakeMaterializationAuthority(validContext, mismatchContext)
	layout := testLayout(t, "materialization-restart")
	seed := newMaterializationManager(t, layout, authority)
	for _, fixture := range []struct {
		record *sm.ContextRecord
		runID  string
	}{
		{validContext, "RUN-restart-valid"},
		{mismatchContext, "RUN-restart-mismatch"},
	} {
		if _, err := seed.CreateMaterialization(context.Background(), CreateMaterializationRequest{
			Context:         fixture.record,
			RunProvider:     fixture.record.Provider,
			TargetElementID: "TASK-260729-bbbbbb",
			TargetRole:      "developer",
			Source:          MaterializationSourceBuilt,
			ProviderHost:    testMaterializationHost(),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := seed.AcquireMaterializationLease(
			context.Background(),
			fixture.record.ID,
			fixture.runID,
			"TASK-260729-bbbbbb",
		); err != nil {
			t.Fatal(err)
		}
	}
	authority.runs["RUN-restart-valid"] = &sm.RunEvidence{
		ID:        "RUN-restart-valid",
		ContextID: validContext.ID,
		Owner:     "TASK-260729-bbbbbb",
		Status:    sm.RunEvidenceRunning,
	}
	authority.runs["RUN-restart-mismatch"] = &sm.RunEvidence{
		ID:        "RUN-restart-mismatch",
		ContextID: mismatchContext.ID,
		Owner:     "TASK-260729-cccccc",
		Status:    sm.RunEvidenceRunning,
	}

	restarted := newMaterializationManager(t, layout, authority)
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	valid, err := restarted.Materialization(validContext.ID)
	if err != nil {
		t.Fatal(err)
	}
	if valid.State != MaterializationLeased || valid.ActiveRunID != "RUN-restart-valid" {
		t.Fatalf("valid restored lease = %#v", valid)
	}
	mismatch, err := restarted.Materialization(mismatchContext.ID)
	if err != nil {
		t.Fatal(err)
	}
	if mismatch.State != MaterializationQuarantined || mismatch.ActiveRunID != "" {
		t.Fatalf("mismatched restored lease = %#v", mismatch)
	}
	if restarted.QuarantinedMaterializationCount() != 1 {
		t.Fatalf("quarantined materialization count = %d", restarted.QuarantinedMaterializationCount())
	}
}

func TestManagerRestartReconcilesTerminalRunStates(t *testing.T) {
	publishedContext := materializationTestContext("CTX-260729-c00014", true)
	unpublishedContext := materializationTestContext("CTX-260729-c00015", true)
	failedContext := materializationTestContext("CTX-260729-c00016", true)
	authority := newFakeMaterializationAuthority(publishedContext, unpublishedContext, failedContext)
	layout := testLayout(t, "materialization-terminal-restart")
	seed := newMaterializationManager(t, layout, authority)
	fixtures := []struct {
		record *sm.ContextRecord
		runID  string
	}{
		{publishedContext, "RUN-terminal-published"},
		{unpublishedContext, "RUN-terminal-unpublished"},
		{failedContext, "RUN-terminal-failed"},
	}
	for _, fixture := range fixtures {
		if _, err := seed.CreateMaterialization(context.Background(), CreateMaterializationRequest{
			Context:         fixture.record,
			RunProvider:     fixture.record.Provider,
			TargetElementID: "TASK-260729-bbbbbb",
			TargetRole:      "developer",
			Source:          MaterializationSourceBuilt,
			ProviderHost:    testMaterializationHost(),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := seed.AcquireMaterializationLease(
			context.Background(),
			fixture.record.ID,
			fixture.runID,
			"TASK-260729-bbbbbb",
		); err != nil {
			t.Fatal(err)
		}
	}
	authority.setContextState(publishedContext.ID, "ready", "")
	authority.setContextState(
		unpublishedContext.ID,
		"retired",
		"publication_not_requested",
	)
	authority.setContextState(
		failedContext.ID,
		"retired",
		"building_failed",
	)
	authority.runs["RUN-terminal-published"] = &sm.RunEvidence{
		ID:            "RUN-terminal-published",
		ContextID:     publishedContext.ID,
		Owner:         "TASK-260729-bbbbbb",
		Status:        sm.RunEvidenceCompleted,
		Published:     true,
		PayloadSHA256: strings.Repeat("d", 64),
	}
	authority.runs["RUN-terminal-unpublished"] = &sm.RunEvidence{
		ID:            "RUN-terminal-unpublished",
		ContextID:     unpublishedContext.ID,
		Owner:         "TASK-260729-bbbbbb",
		Status:        sm.RunEvidenceCompleted,
		Published:     false,
		PayloadSHA256: strings.Repeat("e", 64),
	}
	authority.runs["RUN-terminal-failed"] = &sm.RunEvidence{
		ID:        "RUN-terminal-failed",
		ContextID: failedContext.ID,
		Owner:     "TASK-260729-bbbbbb",
		Status:    sm.RunEvidenceFailed,
	}

	restarted := newMaterializationManager(t, layout, authority)
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		ctxID string
		state MaterializationState
	}{
		{publishedContext.ID, MaterializationReady},
		{unpublishedContext.ID, MaterializationDetached},
		{failedContext.ID, MaterializationOrphaned},
	} {
		view, err := restarted.Materialization(want.ctxID)
		if err != nil {
			t.Fatal(err)
		}
		if view.State != want.state || view.ActiveRunID != "" {
			t.Fatalf("terminal restart %s = %#v, want %s", want.ctxID, view, want.state)
		}
	}
}

func TestMaterializationRunActivityAndSafeEmptyView(t *testing.T) {
	for _, status := range []sm.RunEvidenceStatus{
		sm.RunEvidenceQueued,
		sm.RunEvidenceRunning,
	} {
		if !runStatusActive(status) {
			t.Fatalf("%s should be active", status)
		}
	}
	for _, status := range []sm.RunEvidenceStatus{
		sm.RunEvidenceCompleted,
		sm.RunEvidenceFailed,
		sm.RunEvidenceCancelled,
		"unknown",
	} {
		if runStatusActive(status) {
			t.Fatalf("%s should be terminal", status)
		}
	}
	if view := materializationView(nil); view != (MaterializationView{}) {
		t.Fatalf("nil record view = %#v", view)
	}
}
