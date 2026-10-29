package sessioncontext

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sm "github.com/relux-works/swarma-session-host/sessionmanager"
)

// The producer of this contract exists because a destructive path-based cleanup
// in the Claude materializer survived three rework cycles before being removed.
// So every test here attacks the reclaimer rather than reading it: each one
// hands it something that would look reclaimable to a careless implementation
// and asserts the file is still on disk afterwards.

// artifactChildCtxID is the context every fixture event names as the child its
// artifacts were produced for. It is the key the live-catalog join uses.
const artifactChildCtxID = "CTX-260801-chld01"

// artifactFixture is a manager whose private Claude project store holds one
// board-minted artifact set plus whatever a case plants beside it.
type artifactFixture struct {
	manager     *Service
	layout      sm.Layout
	projectDir  string
	claudeHome  string
	sessionUUID string
	setID       string
}

func newArtifactFixture(t *testing.T) *artifactFixture {
	t.Helper()
	layout := testLayout(t, "artifact-reclamation")
	if err := ensurePrivateDirectory(layout.StateDir); err != nil {
		t.Fatalf("ensurePrivateDirectories: %v", err)
	}
	claudeHome := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", claudeHome)
	projectDir := filepath.Join(claudeHome, "projects", "-tmp-artifact-fixture")
	if err := os.MkdirAll(projectDir, 0o700); err != nil {
		t.Fatalf("creating provider project dir: %v", err)
	}
	manager := newMaterializationManager(t, layout, newFakeMaterializationAuthority())
	return &artifactFixture{
		manager:     manager,
		layout:      layout,
		projectDir:  projectDir,
		claudeHome:  claudeHome,
		sessionUUID: "11111111-2222-3333-4444-555555555555",
		setID:       "artifactset0001",
	}
}

// materializeChild puts the fixture's child context in the private catalog, so
// the artifact join has a live handle to find.
func (f *artifactFixture) materializeChild(t *testing.T, state MaterializationState) {
	t.Helper()
	f.manager.mu.Lock()
	defer f.manager.mu.Unlock()
	f.manager.materializations[artifactChildCtxID] = &materializationRecord{
		CtxID:            artifactChildCtxID,
		Provider:         "claude",
		BindingScope:     "element",
		BindingElementID: "TASK-260728-bbbbbb",
		TargetElementID:  "TASK-260728-bbbbbb",
		State:            state,
		Source:           MaterializationSourceForked,
		LastUsedAt:       materializationTestClock,
	}
}

// writeArtifact creates a file at the given name inside the provider project
// directory.
func (f *artifactFixture) writeArtifact(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(f.projectDir, name)
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("writing artifact %s: %v", name, err)
	}
	return path
}

// recordEvent appends one producer event to the private catalog.
func (f *artifactFixture) recordEvent(
	t *testing.T,
	state claudeReclamationState,
	at time.Time,
	artifacts []claudeReclamationArtifact,
	mutate func(*claudeReclamationEvent),
) {
	t.Helper()
	event := claudeReclamationEvent{
		ArtifactSetID: f.setID,
		Kind:          claudeReclamationFork,
		State:         state,
		Reason:        "test",
		BaseCtxID:     "CTX-260801-base01",
		ChildCtxID:    artifactChildCtxID,
		ProjectDir:    f.projectDir,
		Artifacts:     artifacts,
	}
	if mutate != nil {
		mutate(&event)
	}
	if err := recordClaudeReclamationEvent(f.layout, event, func() time.Time { return at }); err != nil {
		t.Fatalf("recording reclamation event: %v", err)
	}
}

func (f *artifactFixture) sweep(t *testing.T, dryRun bool) []ContextArtifactReclamationOutcome {
	t.Helper()
	return f.sweepReleasing(t, dryRun, nil)
}

// sweepReleasing runs the artifact pass as the retention sweep does, naming the
// contexts whose private handles the same pass released.
func (f *artifactFixture) sweepReleasing(
	t *testing.T,
	dryRun bool,
	releasing map[string]struct{},
) []ContextArtifactReclamationOutcome {
	t.Helper()
	f.manager.mu.Lock()
	defer f.manager.mu.Unlock()
	return f.manager.reclaimContextArtifactsLocked(
		DefaultContextRetentionSettings(),
		materializationTestClock,
		dryRun,
		releasing,
	)
}

func outcomeFor(outcomes []ContextArtifactReclamationOutcome, setID string) *ContextArtifactReclamationOutcome {
	for index := range outcomes {
		if outcomes[index].ArtifactSetID == setID {
			return &outcomes[index]
		}
	}
	return nil
}

func requireFileGone(t *testing.T, path, why string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("%s is still on disk: %s", filepath.Base(path), why)
	}
}

func requireFilePresent(t *testing.T, path, why string) {
	t.Helper()
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("%s was removed: %s", filepath.Base(path), why)
	}
}

// --- The happy path, so the refusals below mean something --------------------

// TestPendingArtifactsAreReclaimedOnceTheGraceHasPassed proves the sweep does
// remove the debris the materializer deliberately leaves. Without this, every
// refusal test below would pass on a reclaimer that removed nothing at all.
func TestPendingArtifactsAreReclaimedOnceTheGraceHasPassed(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	staging := fixture.writeArtifact(t, claudeStagingArtifactPrefix+"abc123")

	past := materializationTestClock.Add(-48 * time.Hour)
	fixture.recordEvent(t, claudeReclamationActive, past.Add(-time.Hour), nil, nil)
	fixture.recordEvent(t, claudeReclamationPendingGC, past, []claudeReclamationArtifact{
		{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		{Role: "retained_staging_file", Path: staging},
	}, nil)

	outcomes := fixture.sweep(t, false)
	outcome := outcomeFor(outcomes, fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactReclaimed {
		t.Fatalf("outcome = %+v, want a reclamation", outcome)
	}
	if outcome.Files != 2 {
		t.Fatalf("files = %d, want both named artifacts", outcome.Files)
	}
	requireFileGone(t, transcript, "the board minted this session and the set is past its grace")
	requireFileGone(t, staging, "a board staging file is board-owned by its prefix")
}

// TestASecondSweepDoesNotReplanAReclaimedSet is the convergence property. The
// producer's catalog is append-only, so without the consumer's own deletion
// audit every pass would re-plan every set the machine has ever reclaimed.
func TestASecondSweepDoesNotReplanAReclaimedSet(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	if outcome := outcomeFor(fixture.sweep(t, false), fixture.setID); outcome == nil ||
		outcome.Status != ContextArtifactReclaimed {
		t.Fatalf("first sweep = %+v, want a reclamation", outcome)
	}
	second := fixture.sweep(t, false)
	if outcome := outcomeFor(second, fixture.setID); outcome != nil {
		t.Fatalf("a reclaimed set was planned again: %+v", outcome)
	}
}

// TestADryRunRemovesNothing keeps the inspection honest.
func TestADryRunRemovesNothing(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, true), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactPlanned || outcome.Files != 1 {
		t.Fatalf("dry-run outcome = %+v, want one planned file", outcome)
	}
	requireFilePresent(t, transcript, "a dry run must not delete")
}

// --- The refusals ------------------------------------------------------------

// TestOwnershipIsProvedByTheManifestNotByLocation is the central gate. A Claude
// project directory is shared with the operator's own sessions, so a file that
// merely sits in the right place is not board-owned — and this hands the
// reclaimer exactly that: a real transcript, in the recorded directory, named
// after a session this board never minted.
func TestOwnershipIsProvedByTheManifestNotByLocation(t *testing.T) {
	fixture := newArtifactFixture(t)
	foreign := fixture.writeArtifact(t, "99999999-8888-7777-6666-555555555555.jsonl")

	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			// The record claims this file under a session id the board did mint.
			// Only the name can tell the two apart.
			{Role: "writable_fork", Path: foreign, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactQuarantine {
		t.Fatalf("outcome = %+v, want a quarantine", outcome)
	}
	if !strings.Contains(outcome.Detail, "session identity") {
		t.Fatalf("the refusal did not name the identity mismatch: %s", outcome.Detail)
	}
	requireFilePresent(t, foreign, "the board never minted this session")
}

// TestASymlinkedArtifactIsNeverFollowed is the reach-out-of-the-directory case.
// The link sits exactly where a board artifact would, named exactly as one
// would be, and points at a file outside the provider store.
func TestASymlinkedArtifactIsNeverFollowed(t *testing.T) {
	fixture := newArtifactFixture(t)
	outside := filepath.Join(t.TempDir(), "operator-conversation.jsonl")
	if err := os.WriteFile(outside, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("writing the operator's file: %v", err)
	}
	link := filepath.Join(fixture.projectDir, fixture.sessionUUID+".jsonl")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks are unavailable on this platform: %v", err)
	}

	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: link, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactQuarantine {
		t.Fatalf("outcome = %+v, want a quarantine", outcome)
	}
	if !strings.Contains(outcome.Detail, "symlink") {
		t.Fatalf("the refusal did not name the symlink: %s", outcome.Detail)
	}
	requireFilePresent(t, outside, "following the link would delete the operator's own conversation")
	requireFilePresent(t, link, "removing the link would report a deletion the board did not perform")
}

// TestALinkedParentDirectoryIsNeverFollowed is the other half of the same
// hazard, and the half a file-level check cannot see.
//
// Here the artifact is a perfectly ordinary regular file with a board-minted
// name, and every lexical check passes: the recorded directory sits under the
// private Claude project store by string comparison, and the artifact set
// recorded it. What is planted is the *parent* — a project directory that is
// itself a link into a tree the board does not own. Without resolving it, the
// reclaimer deletes a file in somebody else's directory while believing it
// stayed inside its own.
func TestALinkedParentDirectoryIsNeverFollowed(t *testing.T) {
	fixture := newArtifactFixture(t)
	outsideDir := t.TempDir()
	victim := filepath.Join(outsideDir, fixture.sessionUUID+".jsonl")
	if err := os.WriteFile(victim, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("writing the file outside the store: %v", err)
	}
	linkedProject := filepath.Join(fixture.claudeHome, "projects", "-linked-project")
	if err := os.Symlink(outsideDir, linkedProject); err != nil {
		t.Skipf("symlinks are unavailable on this platform: %v", err)
	}
	// Reached through the link, this is a real regular file with the right name
	// in a directory the record names — everything except a resolved parent.
	through := filepath.Join(linkedProject, fixture.sessionUUID+".jsonl")

	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: through, SessionUUID: fixture.sessionUUID},
		}, func(event *claudeReclamationEvent) { event.ProjectDir = linkedProject })

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactQuarantine {
		t.Fatalf("outcome = %+v, want a quarantine", outcome)
	}
	if outcome.Files != 0 {
		t.Fatalf("files removed = %d, want nothing deleted through a linked parent", outcome.Files)
	}
	if !strings.Contains(outcome.Detail, "reached through a symlink") {
		t.Fatalf("the refusal did not name the resolved parent: %s", outcome.Detail)
	}
	requireFilePresent(t, victim,
		"a linked parent must not let a confined delete reach a tree the board does not own")
}

// TestAResolvedRootIsNotItselfARefusal keeps the parent check from being
// satisfied by refusing everything.
//
// Platform temp roots are commonly symlinks — /var on macOS resolves to
// /private/var — so a check that compared an unresolved root against a resolved
// path would refuse every artifact on such a machine while proving nothing. The
// ordinary artifact in the fixture's own store must still be reclaimed.
func TestAResolvedRootIsNotItselfARefusal(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactReclaimed || outcome.Files != 1 {
		t.Fatalf("outcome = %+v, want the ordinary artifact reclaimed", outcome)
	}
	requireFileGone(t, transcript, "an artifact inside the real store is board-owned")
}

// TestAPathOutsideTheRecordedDirectoryIsRefused covers the derived-path failure
// directly: a record that names a file the artifact set never recorded a
// directory for must not be acted on.
func TestAPathOutsideTheRecordedDirectoryIsRefused(t *testing.T) {
	fixture := newArtifactFixture(t)
	elsewhere := filepath.Join(fixture.claudeHome, "projects", "-other-project")
	if err := os.MkdirAll(elsewhere, 0o700); err != nil {
		t.Fatalf("creating another project dir: %v", err)
	}
	stray := filepath.Join(elsewhere, fixture.sessionUUID+".jsonl")
	if err := os.WriteFile(stray, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("writing the stray file: %v", err)
	}

	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: stray, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactQuarantine {
		t.Fatalf("outcome = %+v, want a quarantine", outcome)
	}
	requireFilePresent(t, stray, "the set recorded no directory containing this file")
}

// TestADirectoryIsNeverRemoved is the recursive-delete guard. The recorded
// working and project directories are context, not artifacts, and a reclaimer
// that treated one as removable would be back to the defect this contract was
// created to prevent.
func TestADirectoryIsNeverRemoved(t *testing.T) {
	fixture := newArtifactFixture(t)
	nested := filepath.Join(fixture.projectDir, fixture.sessionUUID+".jsonl")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatalf("creating the directory: %v", err)
	}
	inside := filepath.Join(nested, "keep.jsonl")
	if err := os.WriteFile(inside, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("writing inside the directory: %v", err)
	}

	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: nested, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactQuarantine {
		t.Fatalf("outcome = %+v, want a quarantine", outcome)
	}
	requireFilePresent(t, inside, "a directory must never be removed, recursively or otherwise")
}

// TestAnActiveSetIsRetainedWhileItsContextIsStillMaterialized is the join the
// producer's contract asks for.
//
// An active record is the adapter saying the artifacts are in use, and it stays
// true for exactly as long as this machine holds a handle to the context they
// belong to. While it does, the transcript is a live fork source and removing it
// would delete the bytes a later run intends to read.
func TestAnActiveSetIsRetainedWhileItsContextIsStillMaterialized(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.materializeChild(t, MaterializationReady)
	fixture.recordEvent(t, claudeReclamationActive,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactRetained {
		t.Fatalf("outcome = %+v, want the set retained", outcome)
	}
	if !strings.Contains(outcome.Detail, "still materialized") {
		t.Fatalf("the retention was not explained by the live handle: %q", outcome.Detail)
	}
	requireFilePresent(t, transcript, "this machine still holds a handle to the forked context")
}

// TestAnActiveSetWithNoChildContextIsNeverReclaimed keeps the join fail-closed.
//
// A record naming no child cannot be joined against anything, and "I could not
// check" must never become "nothing holds it" for a path whose only action is
// deletion.
func TestAnActiveSetWithNoChildContextIsNeverReclaimed(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.recordEvent(t, claudeReclamationActive,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, func(event *claudeReclamationEvent) { event.ChildCtxID = "" })

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactRetained {
		t.Fatalf("outcome = %+v, want the unjoinable set retained", outcome)
	}
	requireFilePresent(t, transcript, "an active set nothing can be joined against is not debris")
}

// TestAnActiveSetIsReclaimedOnceItsHandleIsReleased is the other half, and the
// reason the join replaced a flat refusal on the state enum.
//
// A successful fork never emits pending_gc — the adapter records that only when
// an assay exits or a launch fails — so a state-only rule reclaims nothing from
// the case that actually accumulates. Once the retention sweep has released the
// child's handle, nothing on this machine can ever name those files again, which
// is precisely when they become debris.
func TestAnActiveSetIsReclaimedOnceItsHandleIsReleased(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.recordEvent(t, claudeReclamationActive,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactReclaimed || outcome.Files != 1 {
		t.Fatalf("outcome = %+v, want the orphaned fork transcript reclaimed", outcome)
	}
	requireFileGone(t, transcript, "no handle on this machine names it any more")
}

// TestAnActiveSetIsReclaimedWhenTheSamePassReleasesItsHandle proves the two
// halves of one sweep agree, which is what makes the dry run honest: the
// materialization pass releases the handle and the artifact pass, told so,
// reclaims the bytes in the same run rather than a day later.
func TestAnActiveSetIsReclaimedWhenTheSamePassReleasesItsHandle(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.materializeChild(t, MaterializationReady)
	fixture.recordEvent(t, claudeReclamationActive,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	releasing := map[string]struct{}{artifactChildCtxID: {}}
	outcome := outcomeFor(fixture.sweepReleasing(t, true, releasing), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactPlanned || outcome.Files != 1 {
		t.Fatalf("outcome = %+v, want the dry run to plan the reclamation it will perform", outcome)
	}
	requireFilePresent(t, transcript, "a dry run removes nothing")

	outcome = outcomeFor(fixture.sweepReleasing(t, false, releasing), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactReclaimed || outcome.Files != 1 {
		t.Fatalf("outcome = %+v, want the reclamation the dry run planned", outcome)
	}
	requireFileGone(t, transcript, "the same pass released the handle that named it")
}

// TestASetInsideItsGraceIsNeverReclaimed is the window's own boundary.
func TestASetInsideItsGraceIsNeverReclaimed(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactRetained {
		t.Fatalf("outcome = %+v, want the set retained inside its grace", outcome)
	}
	requireFilePresent(t, transcript, "the grace period has not passed")
}

// TestALiveContextKeepsItsArtifacts is the join with the live catalog. A
// pending_gc event is the adapter saying it is finished, never that nobody else
// is — the base may still be a fork source a run is reading.
func TestALiveContextKeepsItsArtifacts(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	fixture.manager.mu.Lock()
	fixture.manager.materializations["CTX-260801-base01"] = &materializationRecord{
		CtxID:       "CTX-260801-base01",
		State:       MaterializationLeased,
		ActiveRunID: "RUN-260801-000001",
	}
	fixture.manager.mu.Unlock()

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactRetained {
		t.Fatalf("outcome = %+v, want the set retained while its base is live", outcome)
	}
	if !strings.Contains(outcome.Detail, "still live") {
		t.Fatalf("the retention reason did not name the live context: %s", outcome.Detail)
	}
	requireFilePresent(t, transcript, "a run is still holding this context")
}

// TestACorruptRecordIsQuarantinedAndTheRestStillRuns is the tolerance property.
// One torn write must not stop every reclamation on the machine, and it must not
// license guessing at what the damaged record meant either.
func TestACorruptRecordIsQuarantinedAndTheRestStillRuns(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	catalog := filepath.Join(fixture.layout.StateDir, claudeReclamationDirName)
	torn := filepath.Join(catalog, "artifactset0002-torn.json")
	if err := os.WriteFile(torn, []byte(`{"contract":"task-board-cl`), 0o600); err != nil {
		t.Fatalf("writing the torn record: %v", err)
	}

	outcomes := fixture.sweep(t, false)
	if outcome := outcomeFor(outcomes, fixture.setID); outcome == nil ||
		outcome.Status != ContextArtifactReclaimed {
		t.Fatalf("a torn record stopped an unrelated reclamation: %+v", outcome)
	}
	var quarantined bool
	for _, outcome := range outcomes {
		if outcome.Status == ContextArtifactQuarantine &&
			strings.Contains(outcome.ArtifactSetID, "artifactset0002") {
			quarantined = true
		}
	}
	if !quarantined {
		t.Fatalf("the torn record was skipped silently: %+v", outcomes)
	}
	requireFilePresent(t, torn, "a record nobody can read is not permission to delete it")
}

// TestTheDeletionAuditRecordsWhatWasRemoved keeps the audit half of this task's
// ownership: the consumer owns quarantine, corrupt-record handling, and the
// deletion record.
func TestTheDeletionAuditRecordsWhatWasRemoved(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	fixture.recordEvent(t, claudeReclamationPendingGC,
		materializationTestClock.Add(-48*time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)
	fixture.sweep(t, false)

	auditPath := filepath.Join(
		fixture.layout.StateDir, claudeReclamationDirName,
		claudeReclaimedLedgerDirName, fixture.setID+".json",
	)
	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("reading the deletion audit: %v", err)
	}
	var entry claudeReclamationLedgerEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("decoding the deletion audit: %v", err)
	}
	if entry.ArtifactSetID != fixture.setID || entry.Files != 1 || entry.ReclaimedAt.IsZero() {
		t.Fatalf("audit = %+v, want the set, its file count, and when", entry)
	}
}

// TestARetainedActiveSetIsNotToldItEnteredPendingGC is about what a retention
// reason claims.
//
// The grace window applies to both event states, and an active set reaches it
// once the handle it belonged to is gone. Telling an operator that set "entered
// pending_gc" describes a transition the adapter never recorded, and sends them
// looking through the catalog for an event that is not there.
func TestARetainedActiveSetIsNotToldItEnteredPendingGC(t *testing.T) {
	fixture := newArtifactFixture(t)
	transcript := fixture.writeArtifact(t, fixture.sessionUUID+".jsonl")
	// Active, inside the grace, and with no materialization for its child — so
	// the live-catalog join passes it through to the window and the window is
	// what retains it.
	fixture.recordEvent(t, claudeReclamationActive,
		materializationTestClock.Add(-time.Hour),
		[]claudeReclamationArtifact{
			{Role: "writable_fork", Path: transcript, SessionUUID: fixture.sessionUUID},
		}, nil)

	outcome := outcomeFor(fixture.sweep(t, false), fixture.setID)
	if outcome == nil || outcome.Status != ContextArtifactRetained {
		t.Fatalf("outcome = %+v, want the set retained inside its grace", outcome)
	}
	if strings.Contains(outcome.Detail, "pending_gc") {
		t.Fatalf("an active set was told it entered pending_gc: %q", outcome.Detail)
	}
	if !strings.Contains(outcome.Detail, string(claudeReclamationActive)) {
		t.Fatalf("the reason did not name the state the adapter actually recorded: %q",
			outcome.Detail)
	}
	requireFilePresent(t, transcript, "the grace period has not passed")
}

// TestAClaudeHandleIsStillReleasableAfterItsArtifactsWereReclaimed is the
// release precondition's other input.
//
// The precondition asks whether anything can still name a context's artifacts.
// A set the deletion audit already records left nothing to strand, so the answer
// is the same one — and a precondition that consulted the ledger and changed its
// mind would refuse to release handles whose bytes are provably gone, forever.
func TestAClaudeHandleIsStillReleasableAfterItsArtifactsWereReclaimed(t *testing.T) {
	manager := retentionSweepManager(t, readyRetentionContext("CTX-260801-r00012"))
	seedMaterialization(manager, "CTX-260801-r00012", MaterializationReady,
		materializationTestClock.Add(-48*time.Hour), func(r *materializationRecord) {
			r.Provider = "claude"
			r.ProviderHost = sm.ProviderHostRecord{Provider: "claude", Snapshot: sm.RunnerSnapshot{Kind: "claude", Claude: &sm.ClaudeRunnerSnapshot{Contract: "x"}}}
		})
	if err := recordClaudeReclamationEvent(manager.layout, claudeReclamationEvent{
		ArtifactSetID: "artifactset0012",
		Kind:          claudeReclamationFork,
		State:         claudeReclamationPendingGC,
		Reason:        "assay_exited",
		ChildCtxID:    "CTX-260801-r00012",
		ProjectDir:    t.TempDir(),
	}, func() time.Time { return materializationTestClock.Add(-48 * time.Hour) }); err != nil {
		t.Fatalf("recording the artifact record: %v", err)
	}
	if err := recordClaudeReclamationLedger(manager.layout, claudeReclamationLedgerEntry{
		ArtifactSetID: "artifactset0012",
		Files:         1,
		ReclaimedAt:   materializationTestClock.Add(-24 * time.Hour),
	}); err != nil {
		t.Fatalf("recording the deletion audit: %v", err)
	}

	outcome := materializationOutcomeFor(sweepManager(t, manager, false), "CTX-260801-r00012")
	if outcome == nil || outcome.Action != ContextMaterializationDropped {
		t.Fatalf("outcome = %+v, want the handle released once its artifacts are provably gone",
			outcome)
	}
}

// This is solely the fixture's append-only event producer. Production reads
// the provider's original manifest; it does not mint an authority/run identity.
func recordClaudeReclamationEvent(layout sm.Layout, event claudeReclamationEvent, now func() time.Time) error {
	event.Contract = claudeReclamationContract
	event.EventID = "fixture-event"
	event.Provider = "claude"
	event.RecordedAt = now().UTC()
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	root := filepath.Join(layout.StateDir, claudeReclamationDirName)
	if err = ensurePrivateDirectory(root); err != nil {
		return err
	}
	return writePrivateFile(filepath.Join(root, event.ArtifactSetID+"-"+event.RecordedAt.Format("20060102T150405.000000000")+".json"), data)
}
