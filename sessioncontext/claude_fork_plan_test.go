package sessioncontext

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type observedClaudePreparer struct {
	*sm.ClaudeHostFactory
	calls   int
	err     error
	foreign bool
}

func (f *observedClaudePreparer) PrepareContextFork(ctx context.Context, s sm.ProviderHostRecord, p sm.ClaudeContextForkPlan) (sm.ProviderHostRecord, error) {
	f.calls++
	if f.err != nil {
		return sm.ProviderHostRecord{}, f.err
	}
	out, e := f.ClaudeHostFactory.PrepareContextFork(ctx, s, p)
	if e == nil && f.foreign {
		out.Snapshot.ClaudeFork.ChildCtxID = "CTX-260729-other1"
	}
	return out, e
}

func claudePlanServiceFixture(t *testing.T) (*Service, *fakeMaterializationAuthority, *observedClaudePreparer, PrepareRunMaterializationRequest, string, []byte) {
	t.Helper()
	f := legacyRetentionTransferNewClaudeFixture(t)
	// Literal original completed-transcript fixture: queue, user/assistant
	// exchange with end_turn, and last-prompt. No provider process creates it.
	records := []map[string]any{
		{"type": "queue-operation", "operation": "enqueue", "sessionId": f.baseUUID, "content": "hello"},
		{"type": "user", "uuid": "u1", "parentUuid": nil, "isSidechain": false, "sessionId": f.baseUUID, "cwd": f.baseDir, "version": "2.1.220", "message": map[string]any{"role": "user", "content": "hello"}},
		{"type": "assistant", "uuid": "a1", "parentUuid": "u1", "isSidechain": false, "sessionId": f.baseUUID, "cwd": f.baseDir, "version": "2.1.220", "message": map[string]any{"role": "assistant", "stop_reason": "end_turn", "content": []map[string]string{{"type": "text", "text": "answer"}}}},
		{"type": "last-prompt", "sessionId": f.baseUUID, "lastPrompt": "hello"},
	}
	var raw []byte
	for _, r := range records {
		b, e := json.Marshal(r)
		if e != nil {
			t.Fatal(e)
		}
		raw = append(raw, b...)
		raw = append(raw, '\n')
	}
	if e := os.WriteFile(f.basePath, raw, 0600); e != nil {
		t.Fatal(e)
	}
	primary := materializationTestContext("CTX-260729-c04601", true)
	primary.Provider = "claude"
	primary.State = "ready"
	primary.SecretClass = "unrestricted"
	primary.ReadyAt = materializationTestClock
	inspection, e := InspectTranscript("claude", bytes.NewReader(raw))
	if e != nil {
		t.Fatal(e)
	}
	primary.Payload.SHA256 = inspection.SHA256
	child := materializationTestContext("CTX-260729-c04602", true)
	child.Provider = "claude"
	child.SecretClass = "unrestricted"
	child.Lineage.PrimaryCtxID = primary.ID
	child.Lineage.ProducedByRunID = "RUN-claude-plan"
	a := newFakeMaterializationAuthority(primary, child)
	a.runs[child.Lineage.ProducedByRunID] = &sm.RunEvidence{ID: child.Lineage.ProducedByRunID, ContextID: child.ID, Owner: sm.OwnerKey(child.Binding.ElementID), Status: "running"}
	a.manifests[primary.ID] = &sm.CoverageManifest{CtxID: primary.ID, Provider: "claude", ManifestSHA256: strings.Repeat("b", 64)}
	m := newMaterializationManager(t, testLayout(t, "claude-provider-preparation"), a)
	m.validation = &sourceValidationFixture{}
	host := sm.ProviderHostRecord{Provider: "claude", Snapshot: sm.RunnerSnapshot{Kind: "claude", Claude: &sm.ClaudeRunnerSnapshot{Contract: "task-board.claude-provider-host-v1", PID: os.Getpid(), SessionUUID: f.baseUUID, Plan: sm.LaunchSpec{Cwd: f.baseDir, Executable: "claude", Claude: &sm.ClaudePreparedLaunch{Contract: sm.ClaudeSessionLaunchContract, Plan: sm.PrimarySessionLaunchPlan{Contract: sm.PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "claude", ProjectDir: f.baseDir, Executable: "claude"}}}}}}
	if _, e = m.CreateMaterialization(context.Background(), CreateMaterializationRequest{Context: primary, RunProvider: "claude", TargetElementID: primary.Binding.ElementID, TargetRole: "developer", Source: MaterializationSourceBuilt, ProviderHost: host, PayloadSHA256: primary.Payload.SHA256}); e != nil {
		t.Fatal(e)
	}
	supplier := &observedClaudePreparer{ClaudeHostFactory: sm.NewClaudeHostFactory(f.layout)}
	m.providers = map[string]sm.ProviderHostFactory{"claude": supplier}
	target := f.dir("target")
	return m, a, supplier, PrepareRunMaterializationRequest{Context: child, PrimaryCtxID: primary.ID, TargetElementID: child.Binding.ElementID, TargetRole: "developer", RunID: child.Lineage.ProducedByRunID, Mode: "explicit", TargetProjectDir: target}, f.basePath, raw
}

// Actual default Service -> registered real provider inspector -> original
// typed plan is durable and leased, retaining the admitted boundary/facts. No
// native process or transcript copy occurs during preparation.
func TestContextClaudeDefaultPreparationUsesActualProviderSource(t *testing.T) {
	m, _, supplier, r, path, before := claudePlanServiceFixture(t)
	v, e := m.PrepareRunMaterialization(context.Background(), r)
	if e != nil || v.State != MaterializationLeased || v.ActiveRunID != r.RunID || supplier.calls != 1 {
		t.Fatalf("actual Claude source was not invoked: %+v %v calls=%d", v, e, supplier.calls)
	}
	p := m.materializations[r.Context.ID].ProviderHost.Snapshot.ClaudeFork
	inspection, e := InspectTranscript("claude", bytes.NewReader(before))
	if e != nil {
		t.Fatal(e)
	}
	if p == nil || p.ChildCtxID != r.Context.ID || p.PrimaryCtxID != r.PrimaryCtxID || p.BaseSHA256 != inspection.SHA256 || p.BaseRecords != 4 || p.BaseFormatVersion != "2.1.220" || p.TargetProjectDir != r.TargetProjectDir || p.PrimaryMode != "explicit" || !strings.Contains(p.BoundaryBanner, "UNKNOWN") {
		t.Fatal("actual provider or admitted source facts dropped")
	}
	if after, e := os.ReadFile(path); e != nil || !bytes.Equal(before, after) {
		t.Fatal("preparation mutated original transcript")
	}
	if _, e := os.Stat(filepath.Join(filepath.Dir(path), r.Context.ID+".jsonl")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("preparation copied a native transcript")
	}
	loaded, e := m.materializationRecords.load()
	if e != nil || len(loaded) != 2 {
		t.Fatal("actual typed Claude plan did not reopen", e)
	}
}

// Missing/forged/stale/failed suppliers and malformed real sources cannot
// create a child, publish/retire a registry record or alter base/catalog bytes.
func TestContextClaudePreparationRefusalsHaveNoCatalogOrRegistryEffects(t *testing.T) {
	for _, variant := range []string{"missing-capability", "missing-validation", "stale-source", "source-error", "foreign-result", "missing-transcript", "forged-lineage"} {
		t.Run(variant, func(t *testing.T) {
			m, a, supplier, r, path, raw := claudePlanServiceFixture(t)
			base := catalogBytes(t, m.layout, r.PrimaryCtxID)
			cause := errors.New("actual provider inspector failed")
			switch variant {
			case "missing-capability":
				m.providers = nil
			case "missing-validation":
				m.validation = nil
			case "stale-source":
				a.contexts[r.PrimaryCtxID].State = "stale"
			case "source-error":
				supplier.err = cause
			case "foreign-result":
				supplier.foreign = true
			case "missing-transcript":
				if e := os.Remove(path); e != nil {
					t.Fatal(e)
				}
			case "forged-lineage":
				r.Context.Lineage.SupersedesCtxID = "CTX-260729-other1"
			}
			_, e := m.PrepareRunMaterialization(context.Background(), r)
			if e == nil {
				t.Fatal("unproved Claude source became a child")
			}
			if variant == "source-error" && !errors.Is(e, cause) {
				t.Fatal("actual supplier cause lost", e)
			}
			if variant == "foreign-result" && !strings.Contains(e.Error(), "foreign or incomplete") {
				t.Fatal("foreign supplier result missed exact refusal", e)
			}
			if (variant == "missing-validation" || variant == "stale-source" || variant == "forged-lineage") && supplier.calls != 0 {
				t.Fatal("unadmitted source reached provider inspector")
			}
			if len(m.ListMaterializations()) != 1 || !bytes.Equal(base, catalogBytes(t, m.layout, r.PrimaryCtxID)) || a.contexts[r.Context.ID].State != "building" || len(a.publications) != 0 {
				t.Fatal("refused source changed catalog or registry")
			}
			if variant != "missing-transcript" {
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(raw, after) {
					t.Fatal("refused preparation changed actual base")
				}
			}
		})
	}
}
