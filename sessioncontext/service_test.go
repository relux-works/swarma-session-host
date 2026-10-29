package sessioncontext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Typed authority fixtures represent independently supplied source facts. All
// durable catalog/transcript/publication operations use a real isolated FS.
type fakeMaterializationAuthority struct {
	mu              sync.Mutex
	contexts        map[string]*sm.ContextRecord
	elements        map[string]*sm.OwnerFact
	runs            map[string]*sm.RunEvidence
	reads           map[string]int
	deny            bool
	err             error
	publishErr      error
	publications    []sm.PublicationRequest
	confirmAttempts []string
	manifests       map[string]*sm.CoverageManifest
}

func newFakeMaterializationAuthority(rs ...*sm.ContextRecord) *fakeMaterializationAuthority {
	a := &fakeMaterializationAuthority{contexts: map[string]*sm.ContextRecord{}, elements: map[string]*sm.OwnerFact{}, runs: map[string]*sm.RunEvidence{}, reads: map[string]int{}, manifests: map[string]*sm.CoverageManifest{}}
	for _, r := range rs {
		a.contexts[r.ID] = cloneContext(r)
	}
	for id, parent := range map[string]string{"TASK-260729-bbbbbb": "STORY-260729-aaaaaa", "TASK-260729-cccccc": "STORY-260729-aaaaaa", "STORY-260729-aaaaaa": "EPIC-260729-eeeeee", "EPIC-260729-eeeeee": ""} {
		a.elements[id] = &sm.OwnerFact{ID: sm.OwnerKey(id), ParentID: parent, Status: "development", Eligible: true}
	}
	return a
}
func (a *fakeMaterializationAuthority) Context(_ context.Context, id string) (*sm.ContextRecord, error) {
	if a.err != nil {
		return nil, a.err
	}
	r := a.contexts[normalizeContextID(id)]
	if r == nil {
		return nil, ErrMaterializationEvidenceNotFound
	}
	return cloneContext(r), nil
}
func (a *fakeMaterializationAuthority) Element(_ context.Context, id string) (*sm.OwnerFact, error) {
	if a.err != nil {
		return nil, a.err
	}
	a.reads[id]++
	r := a.elements[normalizeElementID(id)]
	if r == nil {
		return nil, ErrMaterializationEvidenceNotFound
	}
	c := *r
	return &c, nil
}
func (a *fakeMaterializationAuthority) RunEvidence(_ context.Context, id string) (*sm.RunEvidence, error) {
	if a.err != nil {
		return nil, a.err
	}
	r := a.runs[id]
	if r == nil {
		return nil, ErrMaterializationEvidenceNotFound
	}
	c := *r
	return &c, nil
}
func (a *fakeMaterializationAuthority) AuthorizeContextUse(_ context.Context, u sm.ContextUse) (*sm.ContextPermit, error) {
	if a.err != nil {
		return nil, a.err
	}
	return &sm.ContextPermit{Allowed: !a.deny, ContextID: u.ContextID, Code: "context_secret_class_denied", Reason: "fixture current clearance refusal"}, nil
}
func (a *fakeMaterializationAuthority) ContextCoverageManifest(_ context.Context, id string) (*sm.CoverageManifest, error) {
	m := a.manifests[id]
	if m == nil {
		return nil, ErrMaterializationEvidenceNotFound
	}
	c := *m
	return &c, nil
}
func (a *fakeMaterializationAuthority) RetireContext(_ context.Context, id string, rev int, reason sm.StateReason, at time.Time) (*sm.ContextRecord, error) {
	r := a.contexts[id]
	if r == nil || r.Revision != rev {
		return nil, fmt.Errorf("revision mismatch")
	}
	r.State = "retired"
	r.StateReason = reason
	r.Revision++
	r.UpdatedAt = at
	return cloneContext(r), nil
}
func (a *fakeMaterializationAuthority) PublishContext(_ context.Context, p sm.PublicationRequest) (*sm.ContextRecord, error) {
	a.publications = append(a.publications, p)
	if a.publishErr != nil {
		return nil, a.publishErr
	}
	r := a.contexts[p.CtxID]
	if r == nil || r.Revision != p.ExpectedRevision {
		return nil, fmt.Errorf("revision mismatch")
	}
	b, e := os.ReadFile(p.TranscriptPath)
	if e != nil {
		return nil, e
	}
	h := sha256.Sum256(b)
	r.Payload.SHA256 = hex.EncodeToString(h[:])
	r.State = "ready"
	r.Revision++
	return cloneContext(r), nil
}
func (a *fakeMaterializationAuthority) RecoverUnusableContext(_ context.Context, id string, rev int, reason sm.StateReason, _ sm.WorkspaceFingerprint, at time.Time) (*sm.UnusableRecovery, error) {
	r := a.contexts[id]
	if r == nil || r.Revision != rev {
		return nil, fmt.Errorf("revision mismatch")
	}
	r.State = "stale"
	r.StateReason = reason
	r.Revision++
	r.UpdatedAt = at
	return &sm.UnusableRecovery{Reason: reason, ColdRebuild: true}, nil
}
func (a *fakeMaterializationAuthority) ReportContextRetention(context.Context, sm.RetentionPolicy) (*sm.RetentionReport, error) {
	if a.err != nil {
		return nil, a.err
	}
	return &sm.RetentionReport{}, nil
}
func (a *fakeMaterializationAuthority) CollectContextRetention(context.Context, sm.RetentionPolicy) (*sm.RetentionCollection, error) {
	if a.err != nil {
		return nil, a.err
	}
	return &sm.RetentionCollection{}, nil
}
func (a *fakeMaterializationAuthority) SweepContextRetention(_ context.Context, r sm.RetentionSweepRequest) (*sm.RetentionSweepResult, error) {
	if a.err != nil {
		return nil, a.err
	}
	if r.LeaseVisibility != "observed" {
		return nil, fmt.Errorf("visibility required")
	}
	return &sm.RetentionSweepResult{DryRun: r.DryRun}, nil
}
func (a *fakeMaterializationAuthority) ConfirmContextReclamation(_ context.Context, id string, p sm.ProviderReclamation) (*sm.ContextRecord, error) {
	a.confirmAttempts = append(a.confirmAttempts, id)
	r := a.contexts[id]
	if r == nil || r.State != "retired" {
		return nil, fmt.Errorf("retired receipt required")
	}
	r.Reclamation = p
	return cloneContext(r), nil
}

type references struct {
	live  map[string]bool
	known bool
}

func (r references) Referenced(_ context.Context, id string) (bool, bool, error) {
	return r.live[id], r.known, nil
}
func testLayout(t *testing.T, _ string) sm.Layout {
	t.Helper()
	d := t.TempDir()
	return sm.Layout{StateDir: filepath.Join(d, "state"), MaterializationsDir: filepath.Join(d, "state/materializations"), MaterializationQuarantine: filepath.Join(d, "state/quarantine")}
}
func newMaterializationManager(t *testing.T, l sm.Layout, a sm.ContextAuthority) *Service {
	t.Helper()
	m, e := Open(context.Background(), Options{Layout: l, Authority: a, References: references{known: true}, Now: func() time.Time { return materializationTestClock }})
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func materializationTestContext(id string, target bool) *sm.ContextRecord {
	binding := "STORY-260729-aaaaaa"
	if target {
		binding = "TASK-260729-bbbbbb"
	}
	return &sm.ContextRecord{ID: id, Revision: 1, Provider: "codex", SecretClass: "restricted", Binding: sm.ContextBinding{Scope: "element", ElementID: binding, ProducerRole: "developer"}, State: "building", CreatedAt: materializationTestClock, UpdatedAt: materializationTestClock}
}

func (a *fakeMaterializationAuthority) setContextState(id string, state sm.ContextState, reason sm.StateReason) {
	a.contexts[id].State = state
	a.contexts[id].StateReason = reason
}

type nilElementMaterializationAuthority struct{ *fakeMaterializationAuthority }

func (nilElementMaterializationAuthority) Element(context.Context, string) (*sm.OwnerFact, error) {
	return nil, nil
}
