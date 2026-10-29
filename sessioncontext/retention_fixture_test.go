package sessioncontext

import (
	"context"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"testing"
	"time"
)

func retentionSweepManager(t *testing.T, records ...*sm.ContextRecord) *Service {
	t.Helper()
	manager, _ := retentionSweepManagerWithAuthority(t, records...)
	return manager
}
func retentionSweepManagerWithAuthority(
	t *testing.T,
	records ...*sm.ContextRecord,
) (*Service, *fakeMaterializationAuthority) {
	t.Helper()
	layout := testLayout(t, "retention-sweep")
	if err := ensurePrivateDirectory(layout.StateDir); err != nil {
		t.Fatalf("ensurePrivateDirectories: %v", err)
	}
	authority := newFakeMaterializationAuthority(records...)
	return newMaterializationManager(t, layout, authority), authority
}
func readyRetentionContext(id string) *sm.ContextRecord {
	record := materializationTestContext(id, true)
	record.State = "ready"
	record.ReadyAt = materializationTestClock
	record.Revision = 2
	return record
}
func seedMaterialization(
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
func sweepManager(t *testing.T, manager *Service, dryRun bool) ContextRetentionSweepReport {
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
func materializationOutcomeFor(
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
