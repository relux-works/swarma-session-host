package sessioncontext

import (
	"context"
	"encoding/json"
	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func seededLease(t *testing.T) (*Service, *fakeMaterializationAuthority, *sm.ContextRecord, sm.Layout) {
	t.Helper()
	r := materializationTestContext("CTX-260729-c01000", true)
	a := newFakeMaterializationAuthority(r)
	l := testLayout(t, "lease-boundary")
	m := newMaterializationManager(t, l, a)
	_, err := m.CreateMaterialization(context.Background(), CreateMaterializationRequest{Context: r, RunProvider: "codex", TargetElementID: r.Binding.ElementID, TargetRole: "developer", Source: MaterializationSourceBuilt, ProviderHost: testMaterializationHost()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.AcquireMaterializationLease(context.Background(), r.ID, "RUN-boundary", r.Binding.ElementID)
	if err != nil {
		t.Fatal(err)
	}
	a.runs["RUN-boundary"] = &sm.RunEvidence{ID: "RUN-boundary", ContextID: r.ID, Owner: sm.OwnerKey(r.Binding.ElementID), Status: "running", Active: true}
	return m, a, r, l
}
func catalogBytes(t *testing.T, l sm.Layout, id string) []byte {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(l.MaterializationsDir, id, materializationRecordFileName))
	if e != nil {
		t.Fatal(e)
	}
	return b
}

// A reopened catalog cannot convert absence/unknown/live source facts into a
// free writer. Explicit terminal failure is the neighboring admitted release.
func TestContextCatalogReopenPreservesUnknownWriterCustody(t *testing.T) {
	for _, status := range []sm.RunEvidenceStatus{"", "unknown", "queued", "running", "failed", "cancelled"} {
		t.Run(string(status), func(t *testing.T) {
			_, a, r, l := seededLease(t)
			a.runs["RUN-boundary"] = &sm.RunEvidence{ID: "RUN-boundary", ContextID: r.ID, Owner: sm.OwnerKey(r.Binding.ElementID), Status: status}
			m := newMaterializationManager(t, l, a)
			v, e := m.Materialization(r.ID)
			if e != nil {
				t.Fatal(e)
			}
			terminal := status == "failed" || status == "cancelled"
			if !terminal && (v.State != MaterializationLeased || v.ActiveRunID != "RUN-boundary") {
				t.Fatalf("unknown/live source facts released writer custody: %#v", v)
			}
			if terminal && (v.State != MaterializationOrphaned || v.ActiveRunID != "") {
				t.Fatalf("known terminal failure did not release: %#v", v)
			}
		})
	}
}

// Conflicting/corrupt terminal evidence is quarantined with durable unresolved
// custody; even known-empty reader visibility cannot collect its live writer.
func TestContextCatalogCorruptEvidenceCannotMakeWriterCollectable(t *testing.T) {
	for _, variant := range []string{"wrong-run", "wrong-context", "wrong-target", "no-digest", "wrong-publication"} {
		t.Run(variant, func(t *testing.T) {
			_, a, r, l := seededLease(t)
			ev := &sm.RunEvidence{ID: "RUN-boundary", ContextID: r.ID, Owner: sm.OwnerKey(r.Binding.ElementID), Status: "completed", Published: true, PayloadSHA256: strings.Repeat("a", 64)}
			switch variant {
			case "wrong-run":
				ev.ID = "RUN-other"
			case "wrong-context":
				ev.ContextID = "CTX-260729-other1"
			case "wrong-target":
				ev.Owner = "TASK-260729-cccccc"
			case "no-digest":
				ev.PayloadSHA256 = ""
			case "wrong-publication":
				ev.Published = false
			}
			a.runs["RUN-boundary"] = ev
			m := newMaterializationManager(t, l, a)
			v, _ := m.Materialization(r.ID)
			if v.State != MaterializationQuarantined {
				t.Fatalf("corrupt run evidence not quarantined: %#v", v)
			}
			if got := m.MaterializationGCCandidates(materializationTestClock.Add(100*time.Hour), MaterializationGCPolicy{}); len(got) != 0 {
				t.Fatalf("corrupt evidence made unresolved writer collectable: %#v", got)
			}
			if e := m.CollectMaterializations([]MaterializationGCCandidate{{CtxID: r.ID}}); e == nil {
				t.Fatal("explicit collection discarded unresolved writer custody")
			}
			if !strings.Contains(string(catalogBytes(t, l, r.ID)), "RUN-boundary") {
				t.Fatal("durable quarantine lost writer identity")
			}
		})
	}
}

// Provider/target/clearance mismatches fail before durable catalog writes. A
// matching current authorization creates exactly one usable materialization.
func TestContextGateRefusesBeforeCatalogEffects(t *testing.T) {
	for _, variant := range []string{"valid", "provider", "target", "closed", "ineligible", "clearance", "revision", "foreign-lineage"} {
		t.Run(variant, func(t *testing.T) {
			r := materializationTestContext("CTX-260729-c01001", true)
			a := newFakeMaterializationAuthority(r)
			l := testLayout(t, "admission")
			m := newMaterializationManager(t, l, a)
			req := CreateMaterializationRequest{Context: r, RunProvider: "codex", TargetElementID: r.Binding.ElementID, TargetRole: "developer", Source: MaterializationSourceBuilt, ProviderHost: testMaterializationHost()}
			switch variant {
			case "provider":
				req.RunProvider = "claude"
			case "target":
				req.TargetElementID = "TASK-260729-cccccc"
			case "closed":
				a.elements[r.Binding.ElementID].Closed = true
			case "ineligible":
				a.elements[r.Binding.ElementID].Eligible = false
			case "clearance":
				a.deny = true
			case "revision":
				req.Context = cloneContext(r)
				req.Context.Revision++
			case "foreign-lineage":
				r.Lineage.PrimaryCtxID = "CTX-260729-parent"
				a.contexts[r.ID] = cloneContext(r)
				parent := materializationTestContext("CTX-260729-parent", true)
				parent.Provider = "claude"
				parent.State = "ready"
				a.contexts[parent.ID] = parent
			}
			_, err := m.CreateMaterialization(context.Background(), req)
			if variant == "valid" {
				if err != nil || len(m.ListMaterializations()) != 1 {
					t.Fatalf("valid control refused: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("unadmitted source facts reached materialization")
				}
				if len(m.ListMaterializations()) != 0 {
					t.Fatal("refusal wrote catalog state")
				}
				entries, e := os.ReadDir(l.MaterializationsDir)
				if e != nil || len(entries) != 0 {
					t.Fatalf("refusal changed disk catalog: %v %d", e, len(entries))
				}
			}
		})
	}
}

// Completion resolves the transcript from the actual child snapshot; wrong
// holders cannot publish, and a failed authority publication cannot become ready.
func TestContextCompletionPublishesOnlyActualChildTranscript(t *testing.T) {
	for _, variant := range []string{"valid", "wrong-holder", "refused-publication"} {
		t.Run(variant, func(t *testing.T) {
			m, a, r, l := seededLease(t)
			home := t.TempDir()
			path := filepath.Join(home, "sessions", "rollout-thread.jsonl")
			if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
				t.Fatal(e)
			}
			data := []byte("{\"type\":\"response_item\",\"payload\":{\"type\":\"message\"}}\n")
			if e := os.WriteFile(path, data, 0600); e != nil {
				t.Fatal(e)
			}
			h := sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex", Codex: &sm.CodexRunnerSnapshot{ThreadID: "thread", CodexHome: home}}}
			if e := m.RecordProviderHost(r.ID, "RUN-boundary", h); e != nil {
				t.Fatal(e)
			}
			before := catalogBytes(t, l, r.ID)
			req := CompleteRunMaterializationRequest{CtxID: r.ID, RunID: "RUN-boundary", Published: true}
			if variant == "wrong-holder" {
				req.RunID = "RUN-foreign"
			}
			if variant == "refused-publication" {
				a.publishErr = &ContextError{Code: "context_classification_failed", Message: "actual publication gate refuses"}
			}
			v, err := m.CompleteRunMaterialization(context.Background(), req)
			switch variant {
			case "valid":
				if err != nil || v.State != MaterializationReady || len(a.publications) != 1 || a.publications[0].TranscriptPath != path {
					t.Fatalf("actual child not published: %#v %v %#v", v, err, a.publications)
				}
				if _, e := m.CompleteRunMaterialization(context.Background(), req); e == nil {
					t.Fatal("completion replay accepted without current lease")
				}
			case "wrong-holder":
				if err == nil || len(a.publications) != 0 || !reflect.DeepEqual(before, catalogBytes(t, l, r.ID)) {
					t.Fatal("wrong holder changed publication or catalog")
				}
			case "refused-publication":
				v, _ = m.Materialization(r.ID)
				if err == nil || !isContextErrorCode(err, "context_classification_failed") || v.State != MaterializationOrphaned || a.contexts[r.ID].State != "retired" {
					t.Fatalf("publication refusal did not preserve failed-run transition: %#v %v", v, err)
				}
			}
		})
	}
}

// Exact current run, provider, target and persisted role constrain the pre-IO
// handle read. Mutating the returned typed snapshot cannot mutate the catalog.
func TestContextHostReadRequiresExactLeaseAndReturnsIndependentSnapshot(t *testing.T) {
	m, _, r, l := seededLease(t)
	before := catalogBytes(t, l, r.ID)
	for _, v := range []struct {
		run, target, role string
		p                 sm.ContextProvider
	}{{"RUN-other", r.Binding.ElementID, "developer", "codex"}, {"RUN-boundary", "TASK-260729-cccccc", "developer", "codex"}, {"RUN-boundary", r.Binding.ElementID, "reviewer", "codex"}, {"RUN-boundary", r.Binding.ElementID, "developer", "claude"}} {
		if _, e := m.MaterializedProviderHost(context.Background(), r.ID, v.run, v.target, v.role, v.p); e == nil {
			t.Fatal("foreign lease read private provider snapshot")
		}
	}
	h, e := m.MaterializedProviderHost(context.Background(), r.ID, "RUN-boundary", r.Binding.ElementID, "developer", "codex")
	if e != nil {
		t.Fatal(e)
	}
	h.Snapshot.Codex.ThreadID = "foreign"
	h2, e := m.MaterializedProviderHost(context.Background(), r.ID, "RUN-boundary", r.Binding.ElementID, "developer", "codex")
	if e != nil || h2.Snapshot.Codex.ThreadID != "private-thread-secret" || !reflect.DeepEqual(before, catalogBytes(t, l, r.ID)) {
		t.Fatal("returned host snapshot aliases private state")
	}
}

// Unknown host reference visibility holds both handles and files. An observed
// empty reference set plus a retired receipt admits only exact owned rollouts.
func TestContextReclamationRetainsUnknownReadersAndUnownedPaths(t *testing.T) {
	for _, variant := range []string{"valid", "unknown-readers", "live-readers", "adopted", "symlink-file", "symlink-parent"} {
		t.Run(variant, func(t *testing.T) {
			m, a, r, _ := seededLease(t)
			home := t.TempDir()
			outside := filepath.Join(t.TempDir(), "operator.jsonl")
			if e := os.WriteFile(outside, []byte("operator"), 0600); e != nil {
				t.Fatal(e)
			}
			dir := filepath.Join(home, "sessions")
			if e := os.MkdirAll(dir, 0700); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(dir, "rollout-thread.jsonl")
			if e := os.WriteFile(path, []byte("payload"), 0600); e != nil {
				t.Fatal(e)
			}
			h := sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Codex: &sm.CodexRunnerSnapshot{CodexHome: home, ThreadID: "thread", Adopted: variant == "adopted"}}}
			local := m.materializations[r.ID]
			local.State = MaterializationDetached
			local.ActiveRunID = ""
			local.LeaseAcquiredAt = time.Time{}
			local.ProviderHost = h
			a.contexts[r.ID].State = "retired"
			if variant == "unknown-readers" {
				m.references = references{known: false}
			}
			if variant == "live-readers" {
				m.references = references{known: true, live: map[string]bool{r.ID: true}}
			}
			if variant == "symlink-file" {
				if e := os.Remove(path); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(outside, path); e != nil {
					t.Fatal(e)
				}
			}
			if variant == "symlink-parent" {
				real := t.TempDir()
				if e := os.Rename(dir, filepath.Join(real, "sessions")); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(filepath.Join(real, "sessions"), dir); e != nil {
					t.Fatal(e)
				}
			}
			out := m.reclaimOnePayloadLocked(context.Background(), sm.ContextRetentionCollection{CtxID: r.ID, Provider: "codex", IncludeArchived: true}, false, materializationTestClock)
			if variant == "valid" {
				if out.Status != ContextReclamationDone || out.Files != 1 {
					t.Fatalf("owned retired control not reclaimed: %#v", out)
				}
			} else {
				if out.Status != ContextReclamationSkipped || len(a.confirmAttempts) != 0 {
					t.Fatalf("unsafe reclamation was confirmed: %#v", out)
				}
				if _, e := os.Lstat(path); e != nil {
					t.Fatal("refused reclamation removed payload")
				}
			}
			if b, e := os.ReadFile(outside); e != nil || string(b) != "operator" {
				t.Fatal("operator payload changed")
			}
		})
	}
}

// Actual provider-policy hooks distinguish observed absence from an inability
// to probe. Structural markers outrank a malformed tail and consume full bytes.
func TestContextPolicyUsesActualScannerAndObservedFailures(t *testing.T) {
	p := ProviderPolicy{}
	if e := p.RequirePayloadPresent(sm.CodexPresenceReport{CtxID: "CTX-260729-c01000", Presence: "present"}, materializationTestClock, "explicit"); e != nil {
		t.Fatal(e)
	}
	for _, state := range []string{"vanished", "unverifiable", ""} {
		e := p.RequirePayloadPresent(sm.CodexPresenceReport{CtxID: "CTX-260729-c01000", Presence: state, CheckedAt: materializationTestClock}, materializationTestClock, "auto")
		var u *contextPayloadUnusableError
		if e == nil {
			t.Fatal("missing/unverifiable payload admitted")
		}
		if errors.As(e, &u) != (state == "vanished") {
			t.Fatal("unverifiable source was classified as vanished")
		}
	}
	digest, marker, e := p.InspectTranscript(strings.NewReader("{\"type\":\"compacted\"}\nmalformed tail"))
	if e != nil || marker != "compacted" || len(digest) != 64 {
		t.Fatalf("actual compaction scanner not supplied: %s %v", marker, e)
	}
	cause := errors.New("actual failed resume")
	e = p.ResumeUnusable(sm.CodexResumeAttempt{CtxID: "CTX-260729-c01000", Provider: "codex", RecordedVersion: "1.0", ActiveVersion: "2.0", Cause: cause})
	if !errors.Is(e, cause) {
		t.Fatal("resume cause lost")
	}
	if e = p.ResumeUnusable(sm.CodexResumeAttempt{CtxID: "CTX-260729-c01000"}); e == nil {
		t.Fatal("unobserved resume failure became a recovery fact")
	}
}

// Filesystem preconditions are checked before a catalog write traverses a
// symlink. Private modes and safe public views are exercised on admitted data.
func TestContextCatalogPrivateModesAndSymlinkPaths(t *testing.T) {
	m, _, r, l := seededLease(t)
	info, e := os.Stat(filepath.Join(l.MaterializationsDir, r.ID, materializationRecordFileName))
	if e != nil || info.Mode().Perm() != 0600 {
		t.Fatal("private record mode not0600")
	}
	b, e := json.Marshal(m.ListMaterializations())
	if e != nil {
		t.Fatal(e)
	}
	for _, secret := range []string{"private-thread-secret", "/private/repo", "provider_host", "payload_sha256"} {
		if strings.Contains(string(b), secret) {
			t.Fatal("safe catalog view leaked provider handle")
		}
	}
	root := t.TempDir()
	outside := t.TempDir()
	if e := os.Symlink(outside, filepath.Join(root, "CTX-260729-c09999")); e != nil {
		t.Fatal(e)
	}
	store := newMaterializationStore(root, t.TempDir())
	if e := store.save(validMaterializationRecord()); e == nil {
		t.Fatal("catalog write followed symlinked child directory")
	}
	entries, e := os.ReadDir(outside)
	if e != nil || len(entries) != 0 {
		t.Fatal("refused path write changed foreign directory")
	}
}

// The spawn-facing entry consumes a matching active source run receipt and
// child lineage before any catalog/provider effect. Cold build is a real
// admitted path; a provider supplier's absence cannot masquerade as a fork.
func TestContextRunPreparationRequiresActiveExactSourceFacts(t *testing.T) {
	for _, variant := range []string{"valid-cold", "inactive", "unknown", "missing", "wrong-run", "wrong-target", "wrong-context", "wrong-lineage"} {
		t.Run(variant, func(t *testing.T) {
			r := materializationTestContext("CTX-260729-c01002", true)
			r.Lineage.ProducedByRunID = "RUN-preparation"
			a := newFakeMaterializationAuthority(r)
			e := &sm.RunEvidence{ID: "RUN-preparation", ContextID: r.ID, Owner: sm.OwnerKey(r.Binding.ElementID), Status: "queued"}
			a.runs[e.ID] = e
			switch variant {
			case "inactive":
				e.Status = "completed"
			case "unknown":
				e.Status = ""
			case "missing":
				delete(a.runs, e.ID)
			case "wrong-run":
				e.ID = "RUN-foreign"
			case "wrong-target":
				e.Owner = "TASK-260729-cccccc"
			case "wrong-context":
				e.ContextID = "CTX-260729-c01003"
			case "wrong-lineage":
				r.Lineage.ProducedByRunID = "RUN-foreign"
				a.contexts[r.ID] = cloneContext(r)
			}
			l := testLayout(t, "preparation")
			m := newMaterializationManager(t, l, a)
			v, err := m.PrepareRunMaterialization(context.Background(), PrepareRunMaterializationRequest{Context: r, TargetElementID: r.Binding.ElementID, TargetRole: "developer", RunID: "RUN-preparation"})
			if variant == "valid-cold" {
				if err != nil || v.State != MaterializationLeased || v.ActiveRunID != "RUN-preparation" {
					t.Fatalf("actual cold preparation refused: %#v %v", v, err)
				}
			} else {
				if err == nil {
					t.Fatal("unadmitted source run prepared a materialization")
				}
				if len(m.ListMaterializations()) != 0 {
					t.Fatal("refused preparation changed catalog")
				}
				entries, e := os.ReadDir(l.MaterializationsDir)
				if e != nil || len(entries) != 0 {
					t.Fatal("refused preparation wrote a catalog directory")
				}
			}
		})
	}
}

// A changed native-IO run receipt must be checked even when a caller retains a
// formerly valid exact lease. The read leaves durable custody unchanged.
func TestContextHostReadRechecksInactiveSourceRunBeforeIO(t *testing.T) {
	m, a, r, l := seededLease(t)
	before := catalogBytes(t, l, r.ID)
	a.runs["RUN-boundary"].Status = "completed"
	if _, err := m.MaterializedProviderHost(context.Background(), r.ID, "RUN-boundary", r.Binding.ElementID, "developer", "codex"); err == nil {
		t.Fatal("inactive source run reached native IO admission")
	}
	if !reflect.DeepEqual(before, catalogBytes(t, l, r.ID)) {
		t.Fatal("inactive holder refusal changed durable lease")
	}
}

// A historical opaque handle without its real owner codec is preserved intact
// and reserves its identity; it must never reopen as an empty cold-build handle.
func TestContextLegacyOpaqueHandleCannotSilentlyReleaseWriter(t *testing.T) {
	r := materializationTestContext("CTX-260729-c01004", true)
	a := newFakeMaterializationAuthority(r)
	l := testLayout(t, "old-opaque")
	old := validMaterializationRecord()
	old.CtxID = r.ID
	old.State = MaterializationLeased
	old.ActiveRunID = "RUN-legacy"
	old.LeaseAcquiredAt = materializationTestClock
	b, e := json.Marshal(old)
	if e != nil {
		t.Fatal(e)
	}
	var wire map[string]json.RawMessage
	if e = json.Unmarshal(b, &wire); e != nil {
		t.Fatal(e)
	}
	wire["provider_host"] = json.RawMessage(`{"provider":"codex","opaque":{"contract":"task-board.codex-provider-host-v1","thread_id":"old-private-thread","codex_home":"/old-private-home"}}`)
	b, e = json.Marshal(wire)
	if e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(l.MaterializationsDir, r.ID)
	if e = os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, materializationRecordFileName), b, 0600); e != nil {
		t.Fatal(e)
	}
	m := newMaterializationManager(t, l, a)
	if len(m.ListMaterializations()) != 0 || m.QuarantinedMaterializationCount() != 1 {
		t.Fatal("unsupported legacy handle silently became a modern materialization")
	}
	if _, e = m.CreateMaterialization(context.Background(), CreateMaterializationRequest{Context: r, RunProvider: "codex", TargetElementID: r.Binding.ElementID, TargetRole: "developer", Source: MaterializationSourceBuilt, ProviderHost: testMaterializationHost()}); e == nil {
		t.Fatal("quarantined legacy writer identity was recycled")
	}
	kept, e := os.ReadFile(filepath.Join(l.MaterializationQuarantine, r.ID, materializationRecordFileName))
	if e != nil || !reflect.DeepEqual(b, kept) {
		t.Fatal("original legacy opaque record bytes were lost")
	}
}

// A collection plan cannot substitute for the current retired source receipt.
// No files, handle or confirmation change after a stale ready-state plan.
func TestContextReclamationRechecksRetirementBeforeDelete(t *testing.T) {
	m, a, r, _ := seededLease(t)
	home := t.TempDir()
	dir := filepath.Join(home, "sessions")
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "rollout-thread.jsonl")
	if e := os.WriteFile(path, []byte("kept"), 0600); e != nil {
		t.Fatal(e)
	}
	local := m.materializations[r.ID]
	local.ActiveRunID = ""
	local.State = MaterializationReady
	local.ProviderHost = sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Codex: &sm.CodexRunnerSnapshot{CodexHome: home, ThreadID: "thread"}}}
	a.contexts[r.ID].State = "ready"
	out := m.reclaimOnePayloadLocked(context.Background(), sm.ContextRetentionCollection{CtxID: r.ID, Provider: "codex", IncludeArchived: true}, false, materializationTestClock)
	if out.Status != ContextReclamationSkipped || len(a.confirmAttempts) != 0 {
		t.Fatal("a stale plan deleted a nonretired context")
	}
	if b, e := os.ReadFile(path); e != nil || string(b) != "kept" {
		t.Fatal("retirement refusal changed provider bytes")
	}
}

// Advisory logs use private atomic files and survive reopen. A planted link
// cannot truncate a foreign file merely because it sits at a log pathname.
func TestContextObservationStoresAreDurablePrivateAndRejectLinks(t *testing.T) {
	d := t.TempDir()
	s := newContextEconomicsStore(d)
	row := NormalizeContextRunEconomics(ContextRunEconomics{RunID: "RUN-measured", Source: ContextSourceCold, ObservedAt: materializationTestClock})
	if e := s.append(row, materializationTestClock); e != nil {
		t.Fatal(e)
	}
	rows, e := newContextEconomicsStore(d).load()
	if e != nil || len(rows) != 1 || rows[0].RunID != "RUN-MEASURED" {
		t.Fatalf("durable economics reopen: %#v %v", rows, e)
	}
	info, e := os.Stat(s.path)
	if e != nil || info.Mode().Perm() != 0600 {
		t.Fatal("economics log not private")
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if e = os.WriteFile(outside, []byte("unchanged"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(s.path); e != nil {
		t.Fatal(e)
	}
	if e = os.Symlink(outside, s.path); e != nil {
		t.Fatal(e)
	}
	if e = s.append(row, materializationTestClock); e == nil {
		t.Fatal("advisory log write followed planted link")
	}
	if b, e := os.ReadFile(outside); e != nil || string(b) != "unchanged" {
		t.Fatal("advisory write changed foreign file")
	}
}

// A remote/canonical permit never substitutes for the host's own clearance or
// opt-in transport policy. Explicit local clearance is the admitted neighbor.
func TestContextLocalPolicyStillConstrainsCanonicalPermit(t *testing.T) {
	for _, variant := range []string{"restricted-denied", "restricted-granted", "payload-off", "payload-opt-in"} {
		t.Run(variant, func(t *testing.T) {
			r := materializationTestContext("CTX-260729-c01005", true)
			r.State = "ready"
			r.SecretClass = "restricted"
			r.Payload.SHA256 = strings.Repeat("a", 64)
			a := newFakeMaterializationAuthority(r)
			l := testLayout(t, "local-policy")
			o := Options{Layout: l, Authority: a, References: references{known: true}, Now: func() time.Time { return materializationTestClock }}
			if variant != "restricted-denied" {
				o.Policy.Clearance = map[string][]sm.ContextSecretClass{"developer": {"restricted"}}
			}
			source := MaterializationSourceBuilt
			if strings.HasPrefix(variant, "payload-") {
				source = MaterializationSourceFetched
			}
			if variant == "payload-opt-in" {
				o.Policy.PayloadMode = "unrestricted"
			}
			m, e := Open(context.Background(), o)
			if e != nil {
				t.Fatal(e)
			}
			_, e = m.CreateMaterialization(context.Background(), CreateMaterializationRequest{Context: r, RunProvider: "codex", TargetElementID: r.Binding.ElementID, TargetRole: "developer", Source: source, ProviderHost: testMaterializationHost(), PayloadSHA256: r.Payload.SHA256})
			allowed := variant == "restricted-granted" || variant == "payload-opt-in"
			if allowed && e != nil {
				t.Fatalf("explicit valid local policy refused: %v", e)
			}
			if !allowed && (e == nil || len(m.ListMaterializations()) != 0) {
				t.Fatal("canonical permit bypassed local policy before effects")
			}
		})
	}
}

// The actual public prelaunch fork-plan union survives durable catalog reopen;
// a foreign or mixed variant is never mistaken for an empty cold-build handle.
func TestContextCatalogPreservesActualTypedForkPlans(t *testing.T) {
	for _, provider := range []sm.ContextProvider{"codex", "claude"} {
		t.Run(string(provider), func(t *testing.T) {
			r := materializationTestContext("CTX-260729-c01006", true)
			r.Provider = provider
			a := newFakeMaterializationAuthority(r)
			l := testLayout(t, "fork-plan")
			m := newMaterializationManager(t, l, a)
			h := sm.ProviderHostRecord{Provider: string(provider)}
			if provider == "codex" {
				h.Snapshot = sm.RunnerSnapshot{Kind: "codex-fork", CodexFork: &sm.CodexContextForkPlan{Contract: "task-board.codex-context-fork-plan-v1", PrimaryCtxID: "CTX-260729-base01", Primary: testMaterializationHost(), PreparedAt: materializationTestClock}}
			} else {
				h.Snapshot = sm.RunnerSnapshot{Kind: "claude-fork", ClaudeFork: &sm.ClaudeContextForkPlan{Contract: "task-board.claude-context-fork-plan-v1", ChildCtxID: r.ID, PrimaryCtxID: "CTX-260729-base01", BaseSessionUUID: "11111111-2222-3333-4444-555555555555", BaseProjectDir: "/private/base", TargetProjectDir: "/private/child", PreparedAt: materializationTestClock}}
			}
			if _, e := m.CreateMaterialization(context.Background(), CreateMaterializationRequest{Context: r, RunProvider: provider, TargetElementID: r.Binding.ElementID, TargetRole: "developer", Source: MaterializationSourceForked, ProviderHost: h}); e != nil {
				t.Fatalf("actual typed fork plan refused: %v", e)
			}
			reopened := newMaterializationManager(t, l, a)
			if !reflect.DeepEqual(reopened.materializations[r.ID].ProviderHost, h) {
				t.Fatal("durable reopen dropped typed fork payload")
			}
		})
	}
	for _, variant := range []string{"foreign", "mixed", "wrong-kind"} {
		r := validMaterializationRecord()
		if variant == "foreign" {
			r.ProviderHost.Snapshot = sm.RunnerSnapshot{Kind: "claude", Claude: &sm.ClaudeRunnerSnapshot{}}
		}
		if variant == "mixed" {
			r.ProviderHost.Snapshot.Claude = &sm.ClaudeRunnerSnapshot{}
		}
		if variant == "wrong-kind" {
			r.ProviderHost.Snapshot.Kind = "claude"
		}
		if e := validateMaterializationRecord(r); e == nil {
			t.Fatal("foreign/mixed snapshot was treated as a legitimate cold-build gap")
		}
	}
}

type builderReceiptFixture struct {
	result BuilderGatewayFinalization
	err    error
}

func (b builderReceiptFixture) RequireConfinement(context.Context, *sm.ContextRecord) error {
	return b.err
}
func (b builderReceiptFixture) Finalize(context.Context, string) (BuilderGatewayFinalization, error) {
	return b.result, b.err
}
func (b builderReceiptFixture) Enforcement(context.Context, string) (*sm.ContextBuilderEnforcement, error) {
	return nil, errors.New("fixture provides no signed enforcement claim")
}

// Finalization must preserve the original served/attestation reply and reject
// a foreign receipt. This qualifies forwarding, not the absent native gateway.
func TestContextBuilderFinalizationPreservesActualReceipt(t *testing.T) {
	m, _, r, _ := seededLease(t)
	want := BuilderGatewayFinalization{CtxID: r.ID, Served: true, Attestation: ContextRepositoryAccessAttestation{ContractVersion: "source-trace-contract", CtxID: r.ID, RunID: "RUN-boundary", Provider: "codex", Anchor: strings.Repeat("a", 64), EventCount: 3, TraceDigest: strings.Repeat("b", 64), CompletedAt: materializationTestClock}}
	m.builders = builderReceiptFixture{result: want}
	got, e := m.FinalizeBuilderGateway(context.Background(), r.ID)
	if e != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("trace closure receipt was dropped: %#v %v", got, e)
	}
	foreign := want
	foreign.Attestation.RunID = "RUN-foreign"
	m.builders = builderReceiptFixture{result: foreign}
	if _, e = m.FinalizeBuilderGateway(context.Background(), r.ID); e == nil {
		t.Fatal("foreign gateway trace receipt admitted")
	}
	cause := errors.New("actual gateway stop failed")
	m.builders = builderReceiptFixture{err: cause}
	if _, e = m.FinalizeBuilderGateway(context.Background(), r.ID); !errors.Is(e, cause) {
		t.Fatal("actual gateway closure error lost")
	}
}

// The local binding projection retains the original role/digest shape guards;
// neither an injected source record nor durable JSON may widen those fields.
func TestContextBindingMetadataPreservesSourceConstraints(t *testing.T) {
	b := sm.ContextBinding{Scope: "element", ElementID: "TASK-260729-bbbbbb", ProducerRole: "solution-architect", AncestryDigest: strings.Repeat("a", 64)}
	if e := validateBinding(b); e != nil {
		t.Fatal(e)
	}
	for _, change := range []func(*sm.ContextBinding){func(x *sm.ContextBinding) { x.ProducerRole = "" }, func(x *sm.ContextBinding) { x.ProducerRole = "Wrong Role" }, func(x *sm.ContextBinding) { x.ProducerRole = strings.Repeat("a", 513) }, func(x *sm.ContextBinding) { x.AncestryDigest = "bad" }, func(x *sm.ContextBinding) { x.Scope = "board" }} {
		c := b
		change(&c)
		if e := validateBinding(c); e == nil {
			t.Fatal("invalid original binding metadata admitted")
		}
	}
}

type unexpectedForkSupplier struct{ calls *int }

func (s unexpectedForkSupplier) Prepare(context.Context, ForkPreparation) (sm.ProviderHostRecord, error) {
	*s.calls++
	return sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex-fork", CodexFork: &sm.CodexContextForkPlan{Contract: "task-board.codex-context-fork-plan-v1", PrimaryCtxID: "CTX-260729-base02", Primary: testMaterializationHost()}}}, nil
}

// Actual source preflight/boundary facts are absent from the shared record. A
// consumer permit plus a ready handle/manifest must not be called equivalent.
// The cold preparation neighbor above is admitted without a fork supplier.
func TestContextForkMissingValidationFactsRefusesBeforeProviderEffects(t *testing.T) {
	primary := materializationTestContext("CTX-260729-base02", true)
	primary.State = "ready"
	primary.SecretClass = "unrestricted"
	primary.Payload.SHA256 = strings.Repeat("a", 64)
	child := materializationTestContext("CTX-260729-c01007", true)
	child.Lineage.PrimaryCtxID = primary.ID
	child.Lineage.ProducedByRunID = "RUN-fork-input"
	a := newFakeMaterializationAuthority(primary, child)
	a.runs["RUN-fork-input"] = &sm.RunEvidence{ID: "RUN-fork-input", ContextID: child.ID, Owner: sm.OwnerKey(child.Binding.ElementID), Status: "queued"}
	a.manifests[primary.ID] = &sm.CoverageManifest{CtxID: primary.ID, Provider: "codex", ManifestSHA256: strings.Repeat("b", 64)}
	l := testLayout(t, "fork-source-facts")
	m := newMaterializationManager(t, l, a)
	h := testMaterializationHost()
	h.Snapshot.Codex.LastCompletedTurnID = "observed-completed-turn"
	if _, e := m.CreateMaterialization(context.Background(), CreateMaterializationRequest{Context: primary, RunProvider: "codex", TargetElementID: primary.Binding.ElementID, TargetRole: "developer", Source: MaterializationSourceBuilt, ProviderHost: h, PayloadSHA256: primary.Payload.SHA256}); e != nil {
		t.Fatal(e)
	}
	before := catalogBytes(t, l, primary.ID)
	calls := 0
	m.providerMaterializers = map[sm.ContextProvider]ProviderMaterializer{"codex": unexpectedForkSupplier{calls: &calls}}
	_, err := m.PrepareRunMaterialization(context.Background(), PrepareRunMaterializationRequest{Context: child, PrimaryCtxID: primary.ID, TargetElementID: child.Binding.ElementID, TargetRole: "developer", RunID: "RUN-fork-input"})
	if err == nil || !isContextErrorCode(err, "context_validation_unavailable") || calls != 0 {
		t.Fatalf("missing original validation facts reached provider preparation: calls=%d err=%v", calls, err)
	}
	if len(m.ListMaterializations()) != 1 || !reflect.DeepEqual(before, catalogBytes(t, l, primary.ID)) {
		t.Fatal("missing preflight facts changed catalog")
	}
}

// Recovery records only observed loss, preserves the original typed refusal,
// and converges without invalidating an unverifiable or already-stale source.
func TestContextRecoveryPreservesUnknownFactsAndConvergesObservedLoss(t *testing.T) {
	r := materializationTestContext("CTX-260729-c01008", true)
	r.State = "ready"
	r.SecretClass = "unrestricted"
	a := newFakeMaterializationAuthority(r)
	m := newMaterializationManager(t, testLayout(t, "recovery"), a)
	unknown := (ProviderPolicy{}).RequirePayloadPresent(sm.CodexPresenceReport{CtxID: r.ID, Presence: "unverifiable", CheckedAt: materializationTestClock}, materializationTestClock, "auto")
	if e := m.RecoverUnusable(context.Background(), unknown); !isContextErrorCode(e, "context_payload_unverified") || a.contexts[r.ID].State != "ready" || a.contexts[r.ID].Revision != 1 {
		t.Fatal("unknown presence was recorded as payload loss")
	}
	lost := (ProviderPolicy{}).RequirePayloadPresent(sm.CodexPresenceReport{CtxID: r.ID, Presence: "vanished", CheckedAt: materializationTestClock}, materializationTestClock, "auto")
	if e := m.RecoverUnusable(context.Background(), lost); !isContextErrorCode(e, "context_payload_vanished") || a.contexts[r.ID].State != "stale" || a.contexts[r.ID].StateReason != "payload_vanished" {
		t.Fatalf("observed loss did not retain refusal/recovery: %v %#v", e, a.contexts[r.ID])
	}
	revision := a.contexts[r.ID].Revision
	if e := m.RecoverUnusable(context.Background(), lost); !isContextErrorCode(e, "context_payload_vanished") || a.contexts[r.ID].Revision != revision {
		t.Fatal("repeated observed loss did not converge")
	}
}

// A corrupt raw catalog may still belong to a live writer. Its quarantine
// reserves both named artifacts and the global retention lease, even though no
// decodable materialization remains in the in-memory catalog.
func TestContextRawCatalogQuarantineHoldsArtifactsAndRetentionLease(t *testing.T) {
	f := newArtifactFixture(t)
	dir := filepath.Join(f.layout.MaterializationsDir, artifactChildCtxID)
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, materializationRecordFileName), []byte("{corrupt writer record"), 0600); e != nil {
		t.Fatal(e)
	}
	f.manager = newMaterializationManager(t, f.layout, newFakeMaterializationAuthority())
	path := f.writeArtifact(t, f.sessionUUID+".jsonl")
	f.recordEvent(t, claudeReclamationPendingGC, materializationTestClock.Add(-48*time.Hour), []claudeReclamationArtifact{{Role: "writable_fork", Path: path, SessionUUID: f.sessionUUID}}, nil)
	out := outcomeFor(f.sweep(t, false), f.setID)
	if out == nil || out.Status != ContextArtifactRetained {
		t.Fatalf("raw quarantined writer lost its named artifact: %#v", out)
	}
	requireFilePresent(t, path, "raw quarantine has unresolved writer custody")
	leased, leaseErr := f.manager.leasedContextsLocked()
	if leaseErr != nil {
		t.Fatal(leaseErr)
	}
	if _, held := leased[artifactChildCtxID]; !held {
		t.Fatal("raw quarantined writer absent from retention lease set")
	}
}

// Unknown quarantine metadata cannot be promoted to observed lease visibility.
// A matching original metadata record is the nearby admitted custody hold.
func TestContextQuarantineMetadataUnknownVisibilityFailsClosed(t *testing.T) {
	m, _, r, l := seededLease(t)
	dir := filepath.Join(l.MaterializationQuarantine, r.ID)
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, materializationQuarantineFileName)
	if e := os.WriteFile(path, []byte("{unreadable"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := m.leasedContextsLocked(); e == nil {
		t.Fatal("unknown quarantine metadata became an observed empty lease set")
	}
	if !m.materializationReferencedLocked("CTX-260729-other1") {
		t.Fatal("unknown quarantine visibility permitted artifact release")
	}
	if _, e := m.SweepContextRetention(context.Background(), ContextRetentionSweepRequest{}); e == nil {
		t.Fatal("unknown quarantine visibility reached a retention decision")
	}
	meta := materializationQuarantineMetadata{Contract: MaterializationContractVersion, CtxID: r.ID, Reason: "source conflict", QuarantinedAt: materializationTestClock}
	b, e := json.Marshal(meta)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	leased, e := m.leasedContextsLocked()
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := leased[r.ID]; !ok {
		t.Fatal("valid quarantine metadata did not retain source custody")
	}
	if _, e = m.SweepContextRetention(context.Background(), ContextRetentionSweepRequest{DryRun: true}); e != nil {
		t.Fatalf("valid quarantine hold prevented an observed dry plan: %v", e)
	}
}
