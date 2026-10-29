package sessioncontext

import (
	"context"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// These are declared external-source facts, not a registry policy implementation.
// The host under test owns all actual enumeration, deletion and confirmation IO.
type legacyRetentionTransferAuthority struct {
	*fakeMaterializationAuthority
	t            *testing.T
	sweep        func(sm.RetentionSweepRequest) (*sm.RetentionSweepResult, error)
	collectionID string
	expiring     []sm.ContextRetentionOutlook
}

func legacyRetentionTransferNewAuthority(rs ...*sm.ContextRecord) *legacyRetentionTransferAuthority {
	return &legacyRetentionTransferAuthority{fakeMaterializationAuthority: newFakeMaterializationAuthority(rs...)}
}
func (a *legacyRetentionTransferAuthority) SweepContextRetention(ctx context.Context, r sm.RetentionSweepRequest) (*sm.RetentionSweepResult, error) {
	if a.sweep != nil {
		return a.sweep(r)
	}
	return a.fakeMaterializationAuthority.SweepContextRetention(ctx, r)
}
func (a *legacyRetentionTransferAuthority) reclamationAttempts() []string {
	return append([]string(nil), a.confirmAttempts...)
}
func (a *legacyRetentionTransferAuthority) ReportContextRetention(ctx context.Context, p sm.RetentionPolicy) (*sm.RetentionReport, error) {
	if a.collectionID == "" {
		return a.fakeMaterializationAuthority.ReportContextRetention(ctx, p)
	}
	return &sm.RetentionReport{Collect: a.legacyRetentionTransferCollections(), Expiring: a.expiring}, nil
}
func (a *legacyRetentionTransferAuthority) CollectContextRetention(ctx context.Context, p sm.RetentionPolicy) (*sm.RetentionCollection, error) {
	if a.collectionID == "" {
		return a.fakeMaterializationAuthority.CollectContextRetention(ctx, p)
	}
	collections := a.legacyRetentionTransferCollections()
	if len(collections) == 0 {
		return &sm.RetentionCollection{Expiring: a.expiring}, nil
	}
	// The fixture source has decided to retire this one named context. It does
	// not evaluate TTLs, leases, caps or the Service's filesystem decisions.
	r := a.contexts[a.collectionID]
	if r.State != "retired" {
		r.State = "retired"
		r.StateReason = "retention_expired"
		r.Revision++
		r.UpdatedAt = legacyRetentionTransferClock
	}
	return &sm.RetentionCollection{Retired: []*sm.ContextRecord{cloneContext(r)}, Delete: collections, Expiring: a.expiring}, nil
}
func (a *legacyRetentionTransferAuthority) legacyRetentionTransferCollections() []sm.ContextRetentionCollection {
	r := a.contexts[a.collectionID]
	// A confirmed source receipt is the independently declared convergence fact.
	if r == nil || !r.Reclamation.ReclaimedAt.IsZero() {
		return nil
	}
	return []sm.ContextRetentionCollection{{CtxID: r.ID, Provider: r.Provider, Action: "retire_and_delete", Reason: "retention_expired", ExpectedRevision: r.Revision, IncludeArchived: true, Rationale: "fixture source selects its expired context for native reclamation"}}
}
func legacyRetentionTransferOpen(t *testing.T, l sm.Layout, a sm.ContextAuthority) *Service {
	t.Helper()
	if own, ok := a.(*legacyRetentionTransferAuthority); ok {
		own.t = t
	}
	m, e := legacyRetentionTransferOpenResult(l, a)
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func legacyRetentionTransferOpenResult(l sm.Layout, a sm.ContextAuthority) (*Service, error) {
	return Open(context.Background(), Options{Layout: l, Authority: a, References: references{known: true}, Now: func() time.Time { return legacyRetentionTransferClock }})
}
func legacyRetentionTransferLayout(t *testing.T, s string) sm.Layout {
	t.Helper()
	return testLayout(t, s)
}
func legacyRetentionTransferContext(id string, target bool) *sm.ContextRecord {
	return materializationTestContext(id, target)
}
func legacyRetentionTransferRecorded(r sm.ProviderReclamation) bool { return !r.ReclaimedAt.IsZero() }
func legacyRetentionTransferFailingAuthority(t *testing.T, e error) *legacyRetentionTransferAuthority {
	t.Helper()
	a := legacyRetentionTransferNewAuthority()
	a.err = e
	return a
}
func legacyRetentionTransferWriteLive(t *testing.T, home, thread, content string) string {
	t.Helper()
	p := filepath.Join(home, codexSessionsDirName, "2026", "02", "19", "rollout-2026-02-19T17-28-42-"+thread+".jsonl")
	if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(p, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
func legacyRetentionTransferWriteArchived(t *testing.T, home, thread, content string) string {
	t.Helper()
	p := filepath.Join(home, codexArchivedSessionsDirName, "rollout-2026-02-19T17-28-42-"+thread+".jsonl")
	if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(p, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
func legacyRetentionTransferAssertGone(t *testing.T, p, why string) {
	t.Helper()
	if _, e := os.Stat(p); !os.IsNotExist(e) {
		t.Fatalf("%s: %s still exists (%v)", why, filepath.Base(p), e)
	}
}
func legacyRetentionTransferAssertSurvives(t *testing.T, p, why string) {
	t.Helper()
	if _, e := os.Stat(p); e != nil {
		t.Fatalf("%s: %s was removed (%v)", why, filepath.Base(p), e)
	}
}

type legacyRetentionTransferCodexFixture struct {
	manager                                                *Service
	store                                                  *legacyRetentionTransferAuthority
	boardStore                                             *legacyRetentionTransferAuthority
	codexHome, live, archived, otherLive, otherArch, ctxID string
}

func legacyRetentionTransferNewCodexFixture(t *testing.T, mutate func(*sm.CodexRunnerSnapshot)) *legacyRetentionTransferCodexFixture {
	t.Helper()
	home := t.TempDir()
	f := &legacyRetentionTransferCodexFixture{ctxID: legacyRetentionTransferCtxID, codexHome: home,
		live: legacyRetentionTransferWriteLive(t, home, legacyRetentionTransferOwnedThread, "owned-live\n"), archived: legacyRetentionTransferWriteArchived(t, home, legacyRetentionTransferOwnedThread, "owned-archived\n"),
		otherLive: legacyRetentionTransferWriteLive(t, home, legacyRetentionTransferUnrelatedThread, "other-live\n"), otherArch: legacyRetentionTransferWriteArchived(t, home, legacyRetentionTransferUnrelatedThread, "other-archived\n")}
	r := legacyRetentionTransferReadyContext(f.ctxID)
	r.SecretClass = "unrestricted"
	r.CreatedAt = legacyRetentionTransferClock.Add(-49 * time.Hour)
	r.ReadyAt = legacyRetentionTransferClock.Add(-48 * time.Hour)
	r.UpdatedAt = r.ReadyAt
	r.ExpiresAt = legacyRetentionTransferClock.Add(-time.Hour)
	f.store = legacyRetentionTransferNewAuthority(r)
	f.store.collectionID = r.ID
	f.boardStore = f.store
	m := legacyRetentionTransferOpen(t, legacyRetentionTransferLayout(t, "retention"), f.store)
	snapshot := sm.CodexRunnerSnapshot{Contract: legacyRetentionTransferCodexContract, PID: os.Getpid(), URL: "ws://127.0.0.1:1", ThreadID: legacyRetentionTransferOwnedThread, ProjectDir: t.TempDir(), CodexHome: home, ProviderVersion: "test-v1", LastCompletedTurnID: "turn-owned"}
	if mutate != nil {
		mutate(&snapshot)
	}
	local := validMaterializationRecord()
	local.CtxID = f.ctxID
	local.State = MaterializationReady
	local.PayloadSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	local.ProviderHost = sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex", Codex: &snapshot}}
	m.materializations[f.ctxID] = local
	if e := m.materializationRecords.save(local); e != nil {
		t.Fatal(e)
	}
	f.manager = m
	return f
}
func (f *legacyRetentionTransferCodexFixture) legacyRetentionTransferRecord(t *testing.T) *sm.ContextRecord {
	t.Helper()
	r := f.store.contexts[f.ctxID]
	if r == nil {
		t.Fatalf("context %s vanished from source facts", f.ctxID)
	}
	return r
}
func (f *legacyRetentionTransferCodexFixture) legacyRetentionTransferOutcome(t *testing.T, r ContextReclamationReport) ContextReclamationOutcome {
	t.Helper()
	for _, o := range r.Outcomes {
		if o.CtxID == f.ctxID {
			return o
		}
	}
	t.Fatalf("the sweep produced no outcome for %s: %+v", f.ctxID, r.Outcomes)
	return ContextReclamationOutcome{}
}

var _ sm.ContextAuthority = (*legacyRetentionTransferAuthority)(nil)

func legacyRetentionTransferCodexHostRecord(t *testing.T, url, home, thread, turn string, versions ...string) sm.ProviderHostRecord {
	t.Helper()
	version := "test-v1"
	if len(versions) > 0 {
		version = versions[0]
	}
	return sm.ProviderHostRecord{Provider: "codex", Snapshot: sm.RunnerSnapshot{Kind: "codex", Codex: &sm.CodexRunnerSnapshot{Contract: legacyRetentionTransferCodexContract, PID: os.Getpid(), URL: url, ThreadID: thread, ProjectDir: t.TempDir(), CodexHome: home, ProviderVersion: version, LastCompletedTurnID: turn}}}
}
