package sessioncontext

import (
	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var materializationTestClock = time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)

// These original regressions prove private record invariants and safe catalog targets; they do not qualify core Attach.
func TestMaterializationRecordValidationRejectsInvalidPrivateState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*materializationRecord)
	}{
		{"contract", func(record *materializationRecord) { record.Contract = "old-contract" }},
		{"ctx normalization", func(record *materializationRecord) { record.CtxID = strings.ToLower(record.CtxID) }},
		{"ctx format", func(record *materializationRecord) { record.CtxID = "CTX-invalid" }},
		{"provider", func(record *materializationRecord) { record.Provider = "other" }},
		{"binding", func(record *materializationRecord) { record.BindingElementID = "" }},
		{"target normalization", func(record *materializationRecord) {
			record.TargetElementID = strings.ToLower(record.TargetElementID)
		}},
		{"target kind", func(record *materializationRecord) { record.TargetElementID = "CTX-260729-c00001" }},
		{"host provider", func(record *materializationRecord) { record.ProviderHost.Provider = "claude" }},
		// A fork with no handle is the dangerous shape: it claims to descend from a
		// base while holding nothing that could be restored or forked again.
		{"host opaque", func(record *materializationRecord) {
			record.Source = MaterializationSourceForked
			record.ProviderHost.Snapshot = sm.RunnerSnapshot{}
		}},
		{"fetched host opaque", func(record *materializationRecord) {
			record.Source = MaterializationSourceFetched
			record.ProviderHost.Snapshot = sm.RunnerSnapshot{}
		}},
		// A detached record is a reattach source and a ready record is a fork
		// source. Neither may exist without a handle even for a cold build,
		// because both promise a later run something to resume.
		{"detached host opaque", func(record *materializationRecord) {
			record.State = MaterializationDetached
			record.PayloadSHA256 = strings.Repeat("a", 64)
			record.ProviderHost.Snapshot = sm.RunnerSnapshot{}
		}},
		{"ready host opaque", func(record *materializationRecord) {
			record.State = MaterializationReady
			record.PayloadSHA256 = strings.Repeat("a", 64)
			record.ProviderHost.Snapshot = sm.RunnerSnapshot{}
		}},
		{"state", func(record *materializationRecord) { record.State = "unknown" }},
		{"source", func(record *materializationRecord) { record.Source = "copied" }},
		{"digest", func(record *materializationRecord) { record.PayloadSHA256 = "ABC" }},
		{"ready digest", func(record *materializationRecord) { record.State = MaterializationReady }},
		{"unpublished digest", func(record *materializationRecord) {
			record.State = MaterializationDetached
			record.DetachReason = MaterializationDetachUnpublished
		}},
		{"run id", func(record *materializationRecord) { record.ActiveRunID = "bad" }},
		{"leased fields", func(record *materializationRecord) { record.State = MaterializationLeased }},
		{"nonleased lease", func(record *materializationRecord) {
			record.ActiveRunID = "RUN-invalid-nonleased"
			record.LeaseAcquiredAt = materializationTestClock
		}},
		{"timestamps", func(record *materializationRecord) {
			record.LastUsedAt = materializationTestClock.Add(-time.Second)
		}},
		{"quarantine reason", func(record *materializationRecord) {
			record.State = MaterializationQuarantined
		}},
	}
	if err := validateMaterializationRecord(nil); err == nil {
		t.Fatal("nil materialization record was accepted")
	}
	if err := validateMaterializationRecord(validMaterializationRecord()); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := validMaterializationRecord()
			test.mutate(record)
			if err := validateMaterializationRecord(record); err == nil {
				t.Fatalf("invalid record accepted: %#v", record)
			}
		})
	}
	// A cold build legitimately has no handle until its provider starts, and the
	// exemption has to hold through the lease because the lease is taken before
	// the launch. Asserting it here keeps the negative cases above honest: they
	// fail for the source, not for the state.
	for _, state := range []MaterializationState{
		MaterializationMaterializing, MaterializationLeased, MaterializationOrphaned,
	} {
		coldBuild := validMaterializationRecord()
		coldBuild.Source = MaterializationSourceBuilt
		coldBuild.ProviderHost.Snapshot = sm.RunnerSnapshot{}
		coldBuild.State = state
		if state == MaterializationLeased {
			coldBuild.ActiveRunID = "RUN-valid-lease"
			coldBuild.LeaseAcquiredAt = materializationTestClock
		}
		if err := validateMaterializationRecord(coldBuild); err != nil {
			t.Fatalf("cold build awaiting its handle rejected in state %s: %v", state, err)
		}
	}

	leased := validMaterializationRecord()
	leased.State = MaterializationLeased
	leased.ActiveRunID = "RUN-valid-lease"
	leased.LeaseAcquiredAt = materializationTestClock
	if err := validateMaterializationRecord(leased); err != nil {
		t.Fatalf("valid leased record rejected: %v", err)
	}
}

func TestMaterializationErrorsPreserveDeterministicAndTransientCauses(t *testing.T) {
	transient := errors.New("temporary transport failure")
	wrapped := materializationAuthorityError("reading context", transient)
	if !isTransientMaterializationAuthorityError(wrapped) ||
		!errors.Is(wrapped, transient) ||
		!strings.Contains(wrapped.Error(), "reading context") {
		t.Fatalf("transient authority error = %v", wrapped)
	}
	missing := materializationAuthorityError(
		"reading evidence",
		ErrMaterializationEvidenceNotFound,
	)
	if isTransientMaterializationAuthorityError(missing) ||
		!errors.Is(missing, ErrMaterializationEvidenceNotFound) {
		t.Fatalf("deterministic authority error = %v", missing)
	}
	loadErr := &materializationLoadError{
		CtxID:  "CTX-260729-c09999",
		Action: "reading",
		Err:    transient,
	}
	if !errors.Is(loadErr, transient) ||
		!strings.Contains(loadErr.Error(), "reading materialization") {
		t.Fatalf("materialization load error = %v", loadErr)
	}
}

func TestMaterializationStoreRejectsUnsafeTargetsAndUsesQuarantineSuffixes(t *testing.T) {
	root := t.TempDir()
	store := newMaterializationStore(
		filepath.Join(root, "materializations"),
		filepath.Join(root, "quarantine"),
	)
	if records, err := store.load(); err != nil || records != nil {
		t.Fatalf("missing catalog load = %#v, %v", records, err)
	}
	if count, err := store.quarantinedCount(); err != nil || count != 0 {
		t.Fatalf("missing quarantine count = %d, %v", count, err)
	}
	if err := store.save(&materializationRecord{}); err == nil {
		t.Fatal("invalid record was persisted")
	}
	record := validMaterializationRecord()
	if err := store.save(record); err != nil {
		t.Fatal(err)
	}
	records, err := store.load()
	if err != nil || len(records) != 1 || records[0].CtxID != record.CtxID {
		t.Fatalf("catalog round trip = %#v, %v", records, err)
	}
	if err := store.remove(record.CtxID); err != nil {
		t.Fatal(err)
	}
	if records, err := store.load(); err != nil || len(records) != 0 {
		t.Fatalf("catalog after removal = %#v, %v", records, err)
	}
	if err := store.remove("../CTX-unsafe"); err == nil {
		t.Fatal("unsafe removal target was accepted")
	}
	if err := store.quarantineInvalid("../CTX-unsafe", nil, materializationTestClock); err == nil {
		t.Fatal("unsafe quarantine target was accepted")
	}
	if err := os.MkdirAll(filepath.Join(store.quarantineRoot, "CTX-260729-c09999"), 0o700); err != nil {
		t.Fatal(err)
	}
	path, err := nextMaterializationQuarantinePath(store.quarantineRoot, "CTX-260729-c09999")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path) != "CTX-260729-c09999-1" {
		t.Fatalf("duplicate quarantine path = %s", path)
	}
}

func testMaterializationHost() sm.ProviderHostRecord {
	return sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex", Codex: &sm.CodexRunnerSnapshot{ThreadID: "private-thread-secret", CodexHome: "/private/codex-home", ProjectDir: "/private/repo"}}}
}
func validMaterializationRecord() *materializationRecord {
	return &materializationRecord{Contract: MaterializationContractVersion, CtxID: "CTX-260729-c09999", Provider: "codex", BindingScope: "element", BindingElementID: "TASK-260729-bbbbbb", TargetElementID: "TASK-260729-bbbbbb", TargetRole: "developer", ProviderHost: testMaterializationHost(), State: MaterializationMaterializing, Source: MaterializationSourceBuilt, LastUsedAt: materializationTestClock, CreatedAt: materializationTestClock, UpdatedAt: materializationTestClock}
}

var registryTestClock = time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
