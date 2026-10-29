package sessionmanager_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	sc "github.com/relux-works/swarma-session-host/sessioncontext"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const assayRecoveryCtx = "CTX-260730-re0001"
const assayRecoveryTarget = "TASK-260730-aaaaaa"

// Explicit source facts for this receiving fixture; no production identity,
// board registry, policy algorithm or live provider is implemented by it.
type assayRecoveryAuthority struct {
	t          *testing.T
	record     *sm.ContextRecord
	readErr    error
	deny       bool
	recoveries int
}

func (a *assayRecoveryAuthority) Context(_ context.Context, id string) (*sm.ContextRecord, error) {
	if a.readErr != nil {
		return nil, a.readErr
	}
	if a.record == nil || id != a.record.ID {
		return nil, sm.ErrMaterializationEvidenceNotFound
	}
	c := *a.record
	return &c, nil
}
func (a *assayRecoveryAuthority) Element(_ context.Context, id string) (*sm.OwnerFact, error) {
	if id != assayRecoveryTarget {
		return nil, sm.ErrMaterializationEvidenceNotFound
	}
	return &sm.OwnerFact{ID: sm.OwnerKey(id), Eligible: true, Status: "development"}, nil
}
func (a *assayRecoveryAuthority) AuthorizeContextUse(_ context.Context, u sm.ContextUse) (*sm.ContextPermit, error) {
	return &sm.ContextPermit{ContextID: u.ContextID, Allowed: !a.deny, Code: "context_secret_class_denied", Reason: "actual fixture current refusal"}, nil
}
func (a *assayRecoveryAuthority) RecoverUnusableContext(_ context.Context, id string, rev int, reason sm.StateReason, _ sm.WorkspaceFingerprint, at time.Time) (*sm.UnusableRecovery, error) {
	if a.record == nil || a.record.ID != id || a.record.Revision != rev {
		return nil, fmt.Errorf("source revision mismatch")
	}
	a.recoveries++
	a.record.State = "stale"
	a.record.StateReason = reason
	a.record.Revision++
	a.record.UpdatedAt = at
	return &sm.UnusableRecovery{Reason: reason, ColdRebuild: true}, nil
}
func (a *assayRecoveryAuthority) unexpected(name string) error {
	a.t.Errorf("unplanned authority effect: %s", name)
	return errors.New("unplanned authority call")
}
func (a *assayRecoveryAuthority) ContextCoverageManifest(context.Context, string) (*sm.CoverageManifest, error) {
	return nil, a.unexpected("coverage")
}
func (a *assayRecoveryAuthority) RunEvidence(context.Context, string) (*sm.RunEvidence, error) {
	return nil, a.unexpected("run")
}
func (a *assayRecoveryAuthority) RetireContext(context.Context, string, int, sm.StateReason, time.Time) (*sm.ContextRecord, error) {
	return nil, a.unexpected("retire")
}
func (a *assayRecoveryAuthority) PublishContext(context.Context, sm.PublicationRequest) (*sm.ContextRecord, error) {
	return nil, a.unexpected("publish")
}
func (a *assayRecoveryAuthority) ReportContextRetention(context.Context, sm.RetentionPolicy) (*sm.RetentionReport, error) {
	return nil, a.unexpected("retention-report")
}
func (a *assayRecoveryAuthority) CollectContextRetention(context.Context, sm.RetentionPolicy) (*sm.RetentionCollection, error) {
	return nil, a.unexpected("retention-collect")
}
func (a *assayRecoveryAuthority) SweepContextRetention(context.Context, sm.RetentionSweepRequest) (*sm.RetentionSweepResult, error) {
	return nil, a.unexpected("retention-sweep")
}
func (a *assayRecoveryAuthority) ConfirmContextReclamation(context.Context, string, sm.ProviderReclamation) (*sm.ContextRecord, error) {
	return nil, a.unexpected("reclamation")
}

func openAssayRecoveryService(t *testing.T, provider string, source sm.ProviderHostRecord, factory sm.ProviderHostFactory, ready time.Time, digest string) (*sc.Service, *assayRecoveryAuthority, sm.Layout) {
	t.Helper()
	root := t.TempDir()
	l := sm.Layout{StateDir: filepath.Join(root, "state"), MaterializationsDir: filepath.Join(root, "state/materializations"), MaterializationQuarantine: filepath.Join(root, "state/quarantine")}
	now := time.Now().UTC()
	r := &sm.ContextRecord{ID: assayRecoveryCtx, Revision: 1, Provider: sm.ContextProvider(provider), State: "ready", SecretClass: "unrestricted", Binding: sm.ContextBinding{Scope: "element", ElementID: assayRecoveryTarget, ProducerRole: "developer"}, ReadyAt: ready, CreatedAt: now, UpdatedAt: now, Payload: sm.ContextPayload{SHA256: digest}}
	a := &assayRecoveryAuthority{t: t, record: r}
	providers := map[string]sm.ProviderHostFactory{}
	if factory != nil {
		providers[provider] = factory
	}
	service, e := sc.Open(context.Background(), sc.Options{Layout: l, Authority: a, Providers: providers})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = service.CreateMaterialization(context.Background(), sc.CreateMaterializationRequest{Context: r, RunProvider: sm.ContextProvider(provider), TargetElementID: assayRecoveryTarget, TargetRole: "developer", Source: sc.MaterializationSourceBuilt, ProviderHost: source, PayloadSHA256: digest}); e != nil {
		t.Fatal(e)
	}
	return service, a, l
}
func digestAssayBytes(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// Original automatic recovery witness: the original in-process assay source
// actually attempts thread/fork across the version gap. Service, not the test,
// performs recovery; all original outcome/cause/count/state assertions remain.
func TestCodexAssayResumeFailureAcrossBuildsIsRecoveredInTheRegistry(t *testing.T) {
	const (
		baseThread    = "019f-assay-drift-recovery-base"
		baseTurn      = "turn-assay-drift-recovery-base"
		recordedBuild = "0.104.0"
		activeBuild   = "0.130.0"
	)
	home := t.TempDir()
	body := []byte("older-assay-base\n")
	sm.ContextAssayWriteCodexRolloutForTest(t, home, baseThread, string(body))
	factory, url, counts := sm.ContextAssayLoopbackForTest(t, sc.ProviderPolicy{}, baseThread, baseTurn, activeBuild)
	source := sm.ContextAssayCodexSourceForTest(t, url, home, baseThread, baseTurn, recordedBuild)
	manager, authority, _ := openAssayRecoveryService(t, "codex", source, factory, time.Now().UTC(), digestAssayBytes(body))
	_, err := manager.RunCodexContextAssay(context.Background(), sm.CodexContextAssayRequest{BaseCtxID: assayRecoveryCtx, Prompt: "return a score without tools", OutputSchema: json.RawMessage(`{"type":"object"}`)})
	forkCalls, _ := counts()
	if forkCalls != 1 {
		t.Fatalf("thread/fork was attempted %d time(s); the assay resume must be tried, not predicted", forkCalls)
	}
	var typed *sc.ContextError
	if !errors.As(err, &typed) || typed.Code != "context_provider_incompatible" {
		t.Fatalf("a refused cross-build assay did not fall back to rebuild: %v", err)
	}
	for _, evidence := range []string{recordedBuild, activeBuild, "thread/fork (assay)", "marked stale", "rebuilt from cold"} {
		if !strings.Contains(typed.Message, evidence) {
			t.Fatalf("the assay fallback does not report %q: %s", evidence, typed.Message)
		}
	}
	// The provider now owns this neutral mechanism error type; the original
	// canonical refusal stays outermost, with the actual isolation cause retained.
	var isolation *sm.CodexContextError
	if !errors.As(errors.Unwrap(err), &isolation) || isolation.Code != "context_assay_isolation_unavailable" {
		t.Fatalf("the refused assay RPC was not preserved as the fallback cause: %v", err)
	}
	stored, storeErr := authority.Context(context.Background(), assayRecoveryCtx)
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	if stored.State != "stale" || stored.StateReason != "provider_incompatible" {
		t.Fatalf("the registry still serves a base the active build refused to assay: %+v", stored)
	}
}

// Original Claude automatic recovery witness, real provider absence path. Its
// original path-only stub is never invoked: presence refusal precedes process
// creation. This does not qualify a live provider turn or native attestation.
func TestClaudeAssayProviderDeletedBaseIsRecoveredInTheRegistry(t *testing.T) {
	factory, source, path, marker := sm.ContextAssayClaudeSourceForTest(t)
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(path); e != nil {
		t.Fatal(e)
	}
	manager, authority, _ := openAssayRecoveryService(t, "claude", source, factory, time.Now().UTC().Add(-31*24*time.Hour), digestAssayBytes(before))
	_, err := manager.RunClaudeContextAssay(context.Background(), sm.ClaudeContextAssayRequest{BaseCtxID: assayRecoveryCtx, Prompt: "return a score without tools", OutputSchema: sm.ContextAssayClaudeSchemaForTest()})
	var typed *sc.ContextError
	if !errors.As(err, &typed) || typed.Code != "context_payload_vanished" {
		t.Fatalf("the provider-deleted Claude assay base was not handled as vanished: %v", err)
	}
	for _, evidence := range []string{"gone rather than misplaced", "marked stale", "rebuilt from cold"} {
		if !strings.Contains(typed.Message, evidence) {
			t.Fatalf("the Claude assay refusal does not report %q: %s", evidence, typed.Message)
		}
	}
	if strings.Contains(typed.Message, "claude_context_base_not_forkable") || strings.Contains(typed.Message, "no readable transcript") {
		t.Fatalf("the vanished payload fell back to the legacy unreadable-transcript path: %s", typed.Message)
	}
	stored, storeErr := authority.Context(context.Background(), assayRecoveryCtx)
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	if stored.State != "stale" || stored.StateReason != "payload_vanished" {
		t.Fatalf("the registry still serves the vanished Claude assay base: %+v", stored)
	}
	if _, e = os.Stat(marker); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("the missing-base refusal started the fixture provider")
	}
}

// Forged/missing/stale/failed source facts and absent capability all stop before
// even the safe provider RPC, with no registry recovery or catalog-byte effect.
func TestContextAutomaticAssaySourceRefusalHasNoProviderOrRegistryEffects(t *testing.T) {
	for _, variant := range []string{"foreign-provider", "missing-facts", "stale", "failed-read", "denied", "missing-capability"} {
		t.Run(variant, func(t *testing.T) {
			home := t.TempDir()
			sm.ContextAssayWriteCodexRolloutForTest(t, home, "base", "base\n")
			factory, url, counts := sm.ContextAssayLoopbackForTest(t, sc.ProviderPolicy{}, "base", "turn", "0.130.0")
			source := sm.ContextAssayCodexSourceForTest(t, url, home, "base", "turn", "0.104.0")
			var f sm.ProviderHostFactory = factory
			if variant == "missing-capability" {
				f = nil
			}
			m, a, l := openAssayRecoveryService(t, "codex", source, f, time.Now().UTC(), digestAssayBytes([]byte("base\n")))
			file := filepath.Join(l.MaterializationsDir, assayRecoveryCtx, "record.json")
			before, e := os.ReadFile(file)
			if e != nil {
				t.Fatal(e)
			}
			switch variant {
			case "foreign-provider":
				a.record.Provider = "claude"
			case "missing-facts":
				a.record = nil
			case "stale":
				a.record.State = "stale"
			case "failed-read":
				a.readErr = errors.New("source read failed")
			case "denied":
				a.deny = true
			}
			_, e = m.RunCodexContextAssay(context.Background(), sm.CodexContextAssayRequest{BaseCtxID: assayRecoveryCtx, Prompt: "quiz", OutputSchema: json.RawMessage(`{"type":"object"}`)})
			if e == nil {
				t.Fatal("unadmitted assay source reached provider")
			}
			_, total := counts()
			if total != 0 || a.recoveries != 0 {
				t.Fatalf("source refusal caused provider/registry effect: calls=%d recovery=%d", total, a.recoveries)
			}
			after, e := os.ReadFile(file)
			if e != nil || !bytes.Equal(before, after) {
				t.Fatal("source refusal changed catalog")
			}
		})
	}
}
