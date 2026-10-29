package sessioncontext

import (
	"context"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
)

// Original spawn-facing regressions, retaining original names/assertions. Only
// constructor wiring and the explicit source run facts required by admission
// change; no provider host or process is launched by these cases.
func TestPrepareRunMaterializationColdBuildTakesItsLease(t *testing.T) {
	layout := testLayout(t, "context-run-cold-build")
	child := materializationTestContext("CTX-260801-cold01", true)
	child.Lineage.ProducedByRunID = "RUN-260801-cold01"
	authority := newFakeMaterializationAuthority(child)
	authority.runs["RUN-260801-cold01"] = &sm.RunEvidence{ID: "RUN-260801-cold01", ContextID: child.ID, Owner: sm.OwnerKey(child.Binding.ElementID), Status: "queued"}
	manager := newMaterializationManager(t, layout, authority)

	view, err := manager.PrepareRunMaterialization(context.Background(), PrepareRunMaterializationRequest{
		Context:         child,
		TargetElementID: "TASK-260729-bbbbbb",
		TargetRole:      "developer",
		Mode:            "auto",
		RunID:           "RUN-260801-cold01",
	})
	if err != nil {
		t.Fatalf("PrepareRunMaterialization: %v", err)
	}
	if view.Source != MaterializationSourceBuilt {
		t.Fatalf("cold start source = %q, want built", view.Source)
	}
	if view.State != MaterializationLeased || view.ActiveRunID != "RUN-260801-cold01" {
		t.Fatalf("cold start did not take its writer lease: %#v", view)
	}
	if view.TargetElementID != "TASK-260729-bbbbbb" {
		t.Fatalf("target = %q", view.TargetElementID)
	}
	// A second run must not be able to take the same child.
	if _, err := manager.AcquireMaterializationLease(
		context.Background(), child.ID, "RUN-260801-other0", "TASK-260729-bbbbbb",
	); err == nil {
		t.Fatal("a second run acquired a lease already held")
	}
}
func TestPrepareRunMaterializationRefusesSupplementalsWithNoPrimary(t *testing.T) {
	layout := testLayout(t, "context-run-cold-supplemental")
	child := materializationTestContext("CTX-260801-cold02", true)
	child.Lineage.ProducedByRunID = "RUN-260801-cold02"
	authority := newFakeMaterializationAuthority(child)
	authority.runs["RUN-260801-cold02"] = &sm.RunEvidence{ID: "RUN-260801-cold02", ContextID: child.ID, Owner: sm.OwnerKey(child.Binding.ElementID), Status: "queued"}
	manager := newMaterializationManager(t, layout, authority)

	if _, err := manager.PrepareRunMaterialization(context.Background(), PrepareRunMaterializationRequest{
		Context:            child,
		SupplementalCtxIDs: []string{"CTX-260801-suppl1"},
		TargetElementID:    "TASK-260729-bbbbbb",
		TargetRole:         "developer",
		Mode:               "auto",
		RunID:              "RUN-260801-cold02",
	}); err == nil {
		t.Fatal("a cold start accepted supplemental contexts")
	}
}
func TestPrepareRunMaterializationRefusesAnInvalidRun(t *testing.T) {
	layout := testLayout(t, "context-run-bad-run-id")
	child := materializationTestContext("CTX-260801-cold03", true)
	authority := newFakeMaterializationAuthority(child)
	manager := newMaterializationManager(t, layout, authority)

	for _, runID := range []string{"", "  ", "not-a-run", "TASK-260801-aaaaaa"} {
		if _, err := manager.PrepareRunMaterialization(context.Background(), PrepareRunMaterializationRequest{
			Context:         child,
			TargetElementID: "TASK-260729-bbbbbb",
			TargetRole:      "developer",
			RunID:           runID,
		}); err == nil {
			t.Fatalf("run id %q was accepted", runID)
		}
	}
}
