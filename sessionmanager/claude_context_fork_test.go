package sessionmanager

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// claudeTestActiveVersion is the build these fixtures pretend is running. It
// only shows up in cross-build resume failures, so every test that is not about
// that gap passes the same value and ignores it.
const claudeTestActiveVersion = "2.1.220"

// TestClaudeForkArgvAlwaysPairsResumeWithForkSession pins the invariant the base
// immutability of every shared context rests on.
//
// The accepted research measured `--resume BASE` without `--fork-session`
// appending straight into the base file (178,596 -> 188,428 bytes). There is
// exactly one constructor for the selector pair so no future call site can emit
// a bare resume.
//
// --session-id names the writable child, which is honoured alongside the pair:
// probe-verified on claude 2.1.220 in both --print and interactive PTY mode. That
// is why the two identities are separate arguments here rather than one.
func TestClaudeForkArgvAlwaysPairsResumeWithForkSession(t *testing.T) {
	argv := claudeForkArgv("11111111-2222-3333-4444-555555555555")
	if len(argv) != 3 ||
		argv[0] != "--resume" ||
		argv[1] != "11111111-2222-3333-4444-555555555555" ||
		argv[2] != "--fork-session" {
		t.Fatalf("claudeForkArgv produced %v", argv)
	}

	named := claudeForkArgv(
		"11111111-2222-3333-4444-555555555555", "66666666-7777-8888-9999-aaaaaaaaaaaa")
	want := []string{
		"--resume", "11111111-2222-3333-4444-555555555555", "--fork-session",
		"--session-id", "66666666-7777-8888-9999-aaaaaaaaaaaa",
	}
	if strings.Join(named, " ") != strings.Join(want, " ") {
		t.Fatalf("claudeForkArgv named fork produced %v, want %v", named, want)
	}
	// An empty child identity must never degrade into a bare resume.
	if bare := claudeForkArgv("11111111-2222-3333-4444-555555555555", ""); len(bare) != 3 {
		t.Fatalf("an unnamed fork produced %v", bare)
	}
}

// TestClaudeForkMintsSeparateResumeAndWritableIdentities pins the distinction the
// review found collapsed: the copy the provider resumes and the session the run
// writes into are two identities, and the writable one is the manager's choice
// rather than something discovered afterwards.
func TestClaudeForkMintsSeparateResumeAndWritableIdentities(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	result, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	if err != nil {
		t.Fatal(err)
	}
	defer result.lease.release()

	if result.ForkSessionUUID == "" {
		t.Fatal("the materialization named no writable fork identity")
	}
	if result.ForkSessionUUID == result.ResumeSessionUUID {
		t.Fatal("the writable fork reuses the identity of the copy it resumes")
	}
	if result.ForkSessionUUID == fixture.baseUUID {
		t.Fatal("the writable fork reuses the base identity")
	}
	// The writable child's path is derived from the same slug as the copy, so a
	// launch can prove the provider agreed about the directory by finding it there.
	if filepath.Dir(result.ForkTranscriptPath) != filepath.Dir(result.TranscriptPath) {
		t.Fatalf("the writable fork path %q is not in the materialization's project directory %q",
			result.ForkTranscriptPath, result.TranscriptPath)
	}
	if filepath.Base(result.ForkTranscriptPath) !=
		result.ForkSessionUUID+claudeTranscriptSuffix {
		t.Fatalf("the writable fork path %q does not name the fork identity", result.ForkTranscriptPath)
	}
	if _, err := os.Stat(result.ForkTranscriptPath); !os.IsNotExist(err) {
		t.Fatal("the manager pre-created the writable fork; only the provider may write it")
	}
	// The record identities the copy carries are what the launch matches the fork
	// against, so they have to be recorded and non-empty.
	if len(result.InheritedRecordUUIDs) == 0 {
		t.Fatal("the materialization recorded no inherited record identities to verify a fork against")
	}
}

// TestClientArgvStillCannotSelectASessionNowThatTheManagerCan is the other half
// of that invariant: the manager gained the ability to resume and fork, and the
// client-supplied managed argv must still be unable to.
func TestClientArgvStillCannotSelectASessionNowThatTheManagerCan(t *testing.T) {
	for _, argv := range [][]string{
		{"--resume", "11111111-2222-3333-4444-555555555555"},
		{"--fork-session"},
		{"--session-id", "11111111-2222-3333-4444-555555555555"},
		{"--continue"},
		{"-c"},
		{"-r", "11111111-2222-3333-4444-555555555555"},
		{"--model", "haiku", "--fork-session"},
	} {
		err := ValidateClaudeManagedArgv(argv)
		assertPublicErrorCode(t, err, ClaudeErrorLaunchArgsBlocked)
	}
}

// TestClaudeForkMaterializesIntoTheTargetWorktreeSlug is the cross-cwd case that
// makes the whole adapter necessary: a base created in the repository root is
// forked for a run whose working directory is a task worktree.
func TestClaudeForkMaterializesIntoTheTargetWorktreeSlug(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	plan := fixture.plan(targetDir)

	result, err := materializeClaudeContextFork(
		plan, fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	if err != nil {
		t.Fatal(err)
	}
	defer result.lease.release()

	wantSlug, err := ClaudeProjectSlug(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProjectSlug != wantSlug {
		t.Fatalf("ProjectSlug = %q, want %q", result.ProjectSlug, wantSlug)
	}
	wantPath := filepath.Join(
		fixture.home, "projects", wantSlug, result.ResumeSessionUUID+".jsonl")
	if result.TranscriptPath != wantPath {
		t.Fatalf("TranscriptPath = %q, want %q", result.TranscriptPath, wantPath)
	}
	if _, err := os.Stat(result.TranscriptPath); err != nil {
		t.Fatalf("materialized transcript is not where the provider would read it: %v", err)
	}
	if result.ResumeSessionUUID == fixture.baseUUID {
		t.Fatal("the fork resumes the base identity instead of a fresh one")
	}
	if _, err := NormalizeClaudeSessionUUID(result.ResumeSessionUUID); err != nil {
		t.Fatalf("resume identity is not a session UUID: %v", err)
	}

	raw, err := os.ReadFile(result.TranscriptPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), fixture.baseUUID) {
		t.Fatal("the materialized copy still carries the base session identity")
	}
	facts, err := inspectClaudeTranscript(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts.SessionUUIDs) != 1 || facts.SessionUUIDs[0] != result.ResumeSessionUUID {
		t.Fatalf("materialized identities = %v, want [%s]", facts.SessionUUIDs, result.ResumeSessionUUID)
	}
	if facts.Records != fixture.baseRecords {
		t.Fatalf("materialized record count = %d, want the base's %d", facts.Records, fixture.baseRecords)
	}
	if !facts.CompletedTurn {
		t.Fatal("the materialized copy does not end on a completed turn")
	}
	if result.MaterializedSHA256 != claudeBytesDigest(raw) {
		t.Fatal("the reported materialized digest is not the digest of the written file")
	}
	if info, err := os.Stat(result.TranscriptPath); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("materialized transcript mode = %v, want 0600", info.Mode().Perm())
	}
}

// TestClaudeForkLeavesTheBaseByteIdentical is the immutability requirement, and
// it is checked by the adapter as well as by this test: a shared base that moved
// is no longer the context the registry described.
func TestClaudeForkLeavesTheBaseByteIdentical(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	before, err := os.Stat(fixture.basePath)
	if err != nil {
		t.Fatal(err)
	}
	beforeDigest, err := claudeFileDigest(fixture.basePath)
	if err != nil {
		t.Fatal(err)
	}

	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	result, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	if err != nil {
		t.Fatal(err)
	}
	defer result.lease.release()

	afterDigest, err := claudeFileDigest(fixture.basePath)
	if err != nil {
		t.Fatal(err)
	}
	if afterDigest != beforeDigest {
		t.Fatal("the base transcript changed while a fork was taken from it")
	}
	if result.BaseSHA256 != beforeDigest {
		t.Fatalf("attested base digest = %s, want %s", result.BaseSHA256, beforeDigest)
	}
	after, err := os.Stat(fixture.basePath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("base size changed from %d to %d", before.Size(), after.Size())
	}
}

// TestConcurrentClaudeForksFromOneBaseAllSucceed pins the design's only
// concurrency mechanism. Sibling runs fork one immutable base at the same time;
// each gets its own identity and file, and the base is untouched.
func TestConcurrentClaudeForksFromOneBaseAllSucceed(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	beforeDigest, err := claudeFileDigest(fixture.basePath)
	if err != nil {
		t.Fatal(err)
	}
	children := []string{
		"CTX-260730-d00001", "CTX-260730-d00002",
		"CTX-260730-d00003", "CTX-260730-d00004",
	}
	results := make([]*claudeForkMaterialization, len(children))
	failures := make([]error, len(children))
	var wait sync.WaitGroup
	for index, child := range children {
		wait.Add(1)
		go func(index int, child string) {
			defer wait.Done()
			targetDir := fixture.dir(".temp/" + child + "/worktree")
			plan := fixture.plan(targetDir)
			plan.ChildCtxID = child
			results[index], failures[index] =
				materializeClaudeContextFork(plan, fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
		}(index, child)
	}
	wait.Wait()

	seen := map[string]struct{}{}
	for index, err := range failures {
		if err != nil {
			t.Fatalf("concurrent fork %d failed: %v", index, err)
		}
		result := results[index]
		defer result.lease.release()
		if _, duplicate := seen[result.ResumeSessionUUID]; duplicate {
			t.Fatalf("two concurrent forks share the session identity %s", result.ResumeSessionUUID)
		}
		seen[result.ResumeSessionUUID] = struct{}{}
		if _, err := os.Stat(result.TranscriptPath); err != nil {
			t.Fatalf("concurrent fork %d has no transcript: %v", index, err)
		}
	}
	afterDigest, err := claudeFileDigest(fixture.basePath)
	if err != nil {
		t.Fatal(err)
	}
	if afterDigest != beforeDigest {
		t.Fatal("concurrent forks mutated their shared base")
	}
}

// TestClaudeForkRefusesASecondWriterOnOneMaterialization is the other side of
// the same invariant. Two writers against one materialization is the case that
// silently loses a branch, so it is a named refusal.
func TestClaudeForkRefusesASecondWriterOnOneMaterialization(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")

	first, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	if err != nil {
		t.Fatal(err)
	}
	defer first.lease.release()

	_, err = materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	assertPublicErrorCode(t, err, claudeContextWriterBusy)

	first.lease.release()
	second, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	if err != nil {
		t.Fatalf("the materialization stayed locked after its writer released: %v", err)
	}
	defer second.lease.release()
}

// TestClaudeContextWriterLeaseSurvivesAnyElapsedTimeWhileItsHolderLives is the
// review finding turned into a regression.
//
// The lease used to be reclaimable purely on elapsed time, so a long
// materialization — the exact case a lease exists for — could be displaced by a
// second writer at the 30-minute mark. Exclusion is now the advisory file lock
// the kernel owns, so no amount of elapsed time admits a second writer while the
// first still holds it, and no clock the caller supplies can change that.
func TestClaudeContextWriterLeaseSurvivesAnyElapsedTimeWhileItsHolderLives(t *testing.T) {
	layout := claudeTestLayout(t, "claude-lease-live")
	base := time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)
	held, err := acquireClaudeContextWriterLease(
		layout, "CTX-260730-e00001", func() time.Time { return base })
	if err != nil {
		t.Fatal(err)
	}
	defer held.release()

	for _, elapsed := range []time.Duration{
		time.Minute, 30 * time.Minute, 24 * time.Hour, 30 * 24 * time.Hour,
	} {
		at := base.Add(elapsed)
		_, err := acquireClaudeContextWriterLease(
			layout, "CTX-260730-e00001", func() time.Time { return at })
		assertPublicErrorCode(t, err, claudeContextWriterBusy)
	}
	// A holder that keeps working keeps saying so, which is what an operator reads
	// to tell a working materialization from a hung one.
	if err := held.heartbeat(base.Add(45 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(held.path)
	if err != nil {
		t.Fatal(err)
	}
	var marker claudeContextLeaseMarker
	if err := json.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	if !marker.RenewedAt.Equal(base.Add(45 * time.Minute).UTC()) {
		t.Fatalf("heartbeat did not advance the renewal stamp: %+v", marker)
	}
	if !marker.AcquiredAt.Equal(base.UTC()) {
		t.Fatalf("heartbeat rewrote the acquisition stamp: %+v", marker)
	}
	if marker.HolderPID != os.Getpid() || marker.Nonce == "" {
		t.Fatalf("the marker does not identify its holder: %+v", marker)
	}

	// Releasing admits the next writer immediately: nothing waits out a TTL.
	held.release()
	next, err := acquireClaudeContextWriterLease(
		layout, "CTX-260730-e00001", func() time.Time { return base.Add(time.Second) })
	if err != nil {
		t.Fatalf("a released lease was not immediately available: %v", err)
	}
	next.release()
}

// TestClaudeContextWriterLeaseIsReclaimedFromADeadHolder keeps a manager crash
// from stranding a context forever.
//
// A crashed holder's lock is dropped by the kernel, which is what makes this safe
// without a TTL: the marker still names the dead process, and the lease is
// nonetheless available because nothing holds it.
func TestClaudeContextWriterLeaseIsReclaimedFromADeadHolder(t *testing.T) {
	layout := claudeTestLayout(t, "claude-lease-dead")
	path, err := claudeContextLeasePath(layout, "CTX-260730-e00002")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	stale, err := json.Marshal(claudeContextLeaseMarker{
		CtxID:      "CTX-260730-e00002",
		HolderPID:  999999,
		Nonce:      "dead",
		AcquiredAt: time.Now().UTC(),
		RenewedAt:  time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, stale, 0o600); err != nil {
		t.Fatal(err)
	}
	lease, err := acquireClaudeContextWriterLease(layout, "CTX-260730-e00002", time.Now)
	if err != nil {
		t.Fatalf("a lease whose holder is gone was not reclaimed: %v", err)
	}
	defer lease.release()
	if claudeContextLeaseHolder(path) != os.Getpid() {
		t.Fatal("the reclaimed lease still names the dead holder")
	}
}

// TestSimultaneousClaudeContextLeaseReclaimersAdmitExactlyOneWriter covers the
// other half of the review finding: reclamation used to be a read-then-remove
// pair, so two reclaimers could each delete the other's freshly written marker
// and both proceed. Reclamation is now one atomic operation, so a crowd of
// reclaimers produces exactly one winner.
func TestSimultaneousClaudeContextLeaseReclaimersAdmitExactlyOneWriter(t *testing.T) {
	layout := claudeTestLayout(t, "claude-lease-race")
	path, err := claudeContextLeasePath(layout, "CTX-260730-e00003")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	stale, err := json.Marshal(claudeContextLeaseMarker{
		CtxID:     "CTX-260730-e00003",
		HolderPID: 999999,
		Nonce:     "dead",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, stale, 0o600); err != nil {
		t.Fatal(err)
	}

	const reclaimers = 12
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		acquired []*claudeContextLease
		refusals int
	)
	start := make(chan struct{})
	for index := 0; index < reclaimers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			lease, err := acquireClaudeContextWriterLease(layout, "CTX-260730-e00003", time.Now)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				acquired = append(acquired, lease)
				return
			}
			var public *PublicError
			if errors.As(err, &public) && public.Code == claudeContextWriterBusy {
				refusals++
				return
			}
			t.Errorf("unexpected reclaim error: %v", err)
		}()
	}
	close(start)
	wg.Wait()

	if len(acquired) != 1 {
		t.Fatalf("%d reclaimers were admitted as writers; exactly one may be", len(acquired))
	}
	if refusals != reclaimers-1 {
		t.Fatalf("%d reclaimers were refused, want %d", refusals, reclaimers-1)
	}
	acquired[0].release()
}

func TestClaudeContextWriterLeaseIsManagerPrivateAndNotProviderState(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	result, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	if err != nil {
		t.Fatal(err)
	}
	defer result.lease.release()

	if !strings.HasPrefix(result.lease.path, fixture.layout.StateDir) {
		t.Fatalf("lease %q is not under manager-private state", result.lease.path)
	}
	if strings.HasPrefix(result.lease.path, fixture.home) {
		t.Fatal("manager bookkeeping was written inside the provider's own directory")
	}
	info, err := os.Stat(result.lease.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("lease mode = %v, want 0600", info.Mode().Perm())
	}
}

// TestClaudeForkRefusesABaseThatNeverCompletedATurn and its compacted sibling
// are the two refusals the AC requires, exercised through the real
// materialization path rather than through the predicate alone.
func TestClaudeForkRefusesABaseThatNeverCompletedATurn(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	fixture.rewriteBase(t, []string{
		claudeUserRecord("u1", "", fixture.baseUUID, fixture.baseDir, "interrupted"),
	})
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	_, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	assertPublicErrorCode(t, err, claudeContextBaseNotForkable)
	fixture.assertNoTranscriptFor(t, targetDir)
}

func TestClaudeForkRefusesACompactedBase(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	fixture.rewriteBase(t, append(
		claudeCompletedTranscriptRecords(fixture.baseUUID, fixture.baseDir),
		`{"type":"system","sessionId":"`+fixture.baseUUID+`","compactMetadata":{"trigger":"auto"}}`,
	))
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	_, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	assertContextErrorCode(t, err, "context_compacted")
	fixture.assertNoTranscriptFor(t, targetDir)
}

// TestClaudeForkRefusesABaseThatMovedSinceItWasPlanned closes the window between
// planning and launching: the digest recorded in the plan must still be the
// base's digest, or the launch would inherit something the registry never
// described.
func TestClaudeForkRefusesABaseThatMovedSinceItWasPlanned(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	plan := fixture.plan(targetDir)
	plan.BaseSHA256 = strings.Repeat("a", 64)
	_, err := materializeClaudeContextFork(plan, fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	assertPublicErrorCode(t, err, claudeContextBaseNotForkable)
	fixture.assertNoTranscriptFor(t, targetDir)
}

// TestClaudeForkRefusesALaunchInADirectoryItWasNotPlannedFor is the guard
// against silently materializing where the provider will not look: a Claude
// context belongs to one directory, so a launch that moved has to fail rather
// than write somewhere new.
func TestClaudeForkRefusesALaunchInADirectoryItWasNotPlannedFor(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	planned := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	actual := fixture.dir(".temp/TASK-260730-bbbbbb/worktree")
	_, err := materializeClaudeContextFork(
		fixture.plan(planned), fixture.layout, actual, claudeTestActiveVersion, fixture.clock)
	assertPublicErrorCode(t, err, claudeContextMaterializeError)
	fixture.assertNoTranscriptFor(t, actual)
	fixture.assertNoTranscriptFor(t, planned)
}

// TestClaudeForkRefusesAProviderThatChangedTheSlugRule is the calibration gate
// on the real path. The base's own recorded directory is the ground truth; when
// this adapter cannot reproduce the name the provider chose, nothing is written.
func TestClaudeForkRefusesAProviderThatChangedTheSlugRule(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	// Simulate a provider whose rule differs by recording a cwd whose derived
	// slug is not the directory the transcript actually sits in.
	fixture.rewriteBase(t, claudeCompletedTranscriptRecords(
		fixture.baseUUID, fixture.baseDir+"/deeper"))
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	_, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	assertPublicErrorCode(t, err, claudeContextSlugUnverified)
	fixture.assertNoTranscriptFor(t, targetDir)
}

// TestFailedClaudeMaterializationIsRetainedAndCatalogedForGC covers the
// post-write failure branch: the copy remains for lease-aware reclamation and a
// pending_gc event makes the ownership/lifecycle explicit.
func TestFailedClaudeMaterializationIsRetainedAndCatalogedForGC(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")

	var published string
	claudeMaterializedHook = func(materializedPath, basePath string) {
		published = materializedPath
		// The base moves out from under the copy, which is exactly the condition
		// base immutability verification exists to catch.
		if err := os.WriteFile(basePath, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { claudeMaterializedHook = nil })

	_, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	assertPublicErrorCode(t, err, claudeContextMaterializeError)
	if published == "" {
		t.Fatal("the materialization never reached the post-write verification")
	}
	if _, statErr := os.Stat(published); statErr != nil {
		t.Fatalf("the failed materialization was deleted before retention/GC: %v", statErr)
	}
	events, loadErr := loadClaudeReclamationEvents(fixture.layout, "")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	pending := false
	for _, event := range events {
		if event.ChildCtxID == "CTX-260730-c00002" &&
			event.State == claudeReclamationPendingGC {
			pending = true
		}
	}
	if !pending {
		t.Fatal("failed materialization has no pending_gc reclamation event")
	}

	// The lease must be free again: a refused fork may be retried immediately.
	claudeMaterializedHook = nil
	fixture.rewriteBase(t, claudeCompletedTranscriptRecords(fixture.baseUUID, fixture.baseDir))
	retry, err := materializeClaudeContextFork(
		fixture.plan(targetDir), fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	if err != nil {
		t.Fatalf("a failed materialization stranded its writer lease: %v", err)
	}
	retry.lease.release()
}

// TestClaudeForkInjectsDistilledSupplementalByDefault proves the default
// composition path end to end: the handoff is appended to the copy, it names its
// lineage CTX, and no foreign transcript was read to produce it.
func TestClaudeForkInjectsDistilledSupplementalByDefault(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	plan := fixture.plan(targetDir)
	plan.Supplemental = []ClaudeSupplementalContext{
		{CtxID: "CTX-260730-f00001", Handoff: "the security decision is X"},
	}
	result, err := materializeClaudeContextFork(plan, fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	if err != nil {
		t.Fatal(err)
	}
	defer result.lease.release()

	if len(result.InjectedCtxIDs) != 1 || result.InjectedCtxIDs[0] != "CTX-260730-f00001" {
		t.Fatalf("InjectedCtxIDs = %v", result.InjectedCtxIDs)
	}
	if len(result.SplicedCtxIDs) != 0 {
		t.Fatalf("the default path spliced raw history: %v", result.SplicedCtxIDs)
	}
	raw, err := os.ReadFile(result.TranscriptPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "the security decision is X") ||
		!strings.Contains(string(raw), "CTX-260730-f00001") {
		t.Fatal("the distilled handoff did not reach the materialized transcript")
	}
	facts, err := inspectClaudeTranscript(raw)
	if err != nil {
		t.Fatal(err)
	}
	if facts.Records != fixture.baseRecords+1 {
		t.Fatalf("materialized records = %d, want the base's %d plus one handoff",
			facts.Records, fixture.baseRecords)
	}
}

// TestClaudeForkSplicesRawHistoryOnlyWhenItIsAskedFor plus its drift sibling pin
// the AC: raw splice is off by default and refused outside the verified layout.
func TestClaudeForkSplicesRawHistoryOnlyWhenItIsAskedFor(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	otherUUID, otherDir := fixture.writeSecondarySession(t, "2.1.220")
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	plan := fixture.plan(targetDir)
	plan.Supplemental = []ClaudeSupplementalContext{{
		CtxID:          "CTX-260730-f00002",
		Handoff:        "architect decision",
		Raw:            true,
		RawSessionUUID: otherUUID,
		RawProjectDir:  otherDir,
	}}
	result, err := materializeClaudeContextFork(plan, fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	if err != nil {
		t.Fatal(err)
	}
	defer result.lease.release()

	if len(result.SplicedCtxIDs) != 1 || result.SplicedCtxIDs[0] != "CTX-260730-f00002" {
		t.Fatalf("SplicedCtxIDs = %v", result.SplicedCtxIDs)
	}
	raw, err := os.ReadFile(result.TranscriptPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), otherUUID) {
		t.Fatal("the spliced records kept the foreign session identity")
	}
	if !strings.Contains(string(raw), "secondary question") {
		t.Fatal("the spliced records did not reach the materialized transcript")
	}
}

func TestClaudeForkRefusesRawSpliceOnTranscriptFormatDrift(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	otherUUID, otherDir := fixture.writeSecondarySession(t, "3.0.0")
	targetDir := fixture.dir(".temp/TASK-260730-aaaaaa/worktree")
	plan := fixture.plan(targetDir)
	plan.Supplemental = []ClaudeSupplementalContext{{
		CtxID:          "CTX-260730-f00003",
		Handoff:        "architect decision",
		Raw:            true,
		RawSessionUUID: otherUUID,
		RawProjectDir:  otherDir,
	}}
	_, err := materializeClaudeContextFork(plan, fixture.layout, targetDir, claudeTestActiveVersion, fixture.clock)
	assertPublicErrorCode(t, err, claudeContextTranscriptDrift)
	fixture.assertNoTranscriptFor(t, targetDir)
}

// TestNormalizeClaudeSupplementalKeepsRawOffByDefault checks the request-level
// half: an ordinary supplemental never names a provider session, so it cannot
// reach the splice path at all.
func TestNormalizeClaudeSupplementalKeepsRawOffByDefault(t *testing.T) {
	normalized, err := normalizeClaudeSupplemental(
		ClaudeSupplementalContext{CtxID: "CTX-260730-f00004", Handoff: "distilled"}, "2.1.220")
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Raw || normalized.RawSessionUUID != "" || normalized.RawProjectDir != "" {
		t.Fatalf("a distilled supplemental carried raw-splice inputs: %+v", normalized)
	}
	if _, err := normalizeClaudeSupplemental(
		ClaudeSupplementalContext{CtxID: "CTX-260730-f00004", Handoff: "  "}, "2.1.220"); err == nil {
		t.Fatal("a supplemental with no distilled handoff was accepted")
	}
	// Raw drift is refused at request time too, before any transcript is read.
	_, err = normalizeClaudeSupplemental(ClaudeSupplementalContext{
		CtxID:          "CTX-260730-f00004",
		Raw:            true,
		RawSessionUUID: "11111111-2222-3333-4444-555555555555",
		RawProjectDir:  "/repo",
	}, "9.9.9")
	assertPublicErrorCode(t, err, claudeContextTranscriptDrift)
}

// TestClaudeForkPlanNeverLeavesTheManagerPrivateCatalog is the handle-boundary
// requirement: the private plan carries provider identities, and the projection
// a control-plane client can see must carry none of them.

func claudeMustMarshalJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// --- fixture --------------------------------------------------------------

type claudeForkFixture struct {
	home        string
	root        string
	baseDir     string
	baseUUID    string
	basePath    string
	baseRecords int
	baseSHA256  string
	layout      Layout
	clock       func() time.Time
}

// newClaudeForkFixture builds a private Claude home with one forkable base whose
// transcript sits exactly where the provider would have put it.
func newClaudeForkFixture(t *testing.T) *claudeForkFixture {
	t.Helper()
	// Resolved up front: the provider derives its project directory from the
	// resolved cwd, and on macOS a test temp dir is reached through the
	// /var -> /private/var symlink.
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "claude-home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CONFIG_DIR", home)

	project := filepath.Join(root, "project")
	if err := os.MkdirAll(project, 0o700); err != nil {
		t.Fatal(err)
	}
	fixture := &claudeForkFixture{
		home:     home,
		root:     root,
		baseDir:  project,
		baseUUID: "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
		layout:   claudeTestLayout(t, "claude-fork"),
		clock:    func() time.Time { return claudeMaterializationTestClock },
	}
	slug, err := ClaudeProjectSlug(project)
	if err != nil {
		t.Fatal(err)
	}
	fixture.basePath = filepath.Join(home, "projects", slug, fixture.baseUUID+".jsonl")
	fixture.rewriteBase(t, claudeCompletedTranscriptRecords(fixture.baseUUID, project))
	return fixture
}

func (f *claudeForkFixture) rewriteBase(t *testing.T, records []string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(f.basePath), 0o700); err != nil {
		t.Fatal(err)
	}
	body := strings.Join(records, "\n") + "\n"
	if err := os.WriteFile(f.basePath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	f.baseRecords = len(records)
	f.baseSHA256 = claudeBytesDigest([]byte(body))
}

// assertBaseUnchanged re-reads the base from disk and compares it with the plan's
// recorded digest. A base is a shared fork source, so "it was not touched" has to
// be a checked fact after every path that could have touched it.
func (f *claudeForkFixture) assertBaseUnchanged(t *testing.T) {
	t.Helper()
	digest, err := claudeFileDigest(f.basePath)
	if err != nil {
		t.Fatalf("the base transcript is no longer readable: %v", err)
	}
	if digest != f.baseSHA256 {
		t.Fatal("the base transcript changed; a shared fork source must stay byte-identical")
	}
}

// dir returns an existing working directory below the fixture's project.
//
// It creates the directory because a real run's cwd always exists: the provider
// is launched in it, and the materializer requires a directory it can resolve.
func (f *claudeForkFixture) dir(relative string) string {
	path := filepath.Join(f.baseDir, filepath.FromSlash(relative))
	if err := os.MkdirAll(path, 0o700); err != nil {
		panic(err)
	}
	return path
}

// plan builds the private fork plan the manager would have recorded, with the
// digest taken from the base as it stands now.
func (f *claudeForkFixture) plan(targetDir string) *claudeContextForkPlan {
	digest, err := claudeFileDigest(f.basePath)
	if err != nil {
		digest = ""
	}
	_, facts, factsErr := inspectClaudeTranscriptFile(f.basePath)
	version := "2.1.220"
	records := f.baseRecords
	if factsErr == nil {
		if facts.FormatVersion != "" {
			version = facts.FormatVersion
		}
		records = facts.Records
	}
	return &claudeContextForkPlan{
		Contract:          claudeContextForkPlanContract,
		ChildCtxID:        "CTX-260730-c00002",
		PrimaryCtxID:      "CTX-260730-c00001",
		BaseSessionUUID:   f.baseUUID,
		BaseProjectDir:    f.baseDir,
		BaseSHA256:        digest,
		BaseRecords:       records,
		BaseFormatVersion: version,
		TargetProjectDir:  filepath.Clean(targetDir),
		PreparedAt:        claudeMaterializationTestClock,
	}
}

func (f *claudeForkFixture) hostRecord(t *testing.T, plan *claudeContextForkPlan) ProviderHostRecord {
	t.Helper()
	return ProviderHostRecord{
		Provider: "claude",
		Snapshot: RunnerSnapshot{Kind: "claude-fork", ClaudeFork: plan},
	}
}

// writeSecondarySession creates a second completed session for the raw-splice
// cases, stamped with a chosen transcript format version.
func (f *claudeForkFixture) writeSecondarySession(t *testing.T, version string) (string, string) {
	t.Helper()
	dir := filepath.Join(f.root, "secondary")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	slug, err := ClaudeProjectSlug(dir)
	if err != nil {
		t.Fatal(err)
	}
	sessionUUID := "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
	records := []string{
		`{"type":"user","uuid":"s1","parentUuid":null,"isSidechain":false,"sessionId":"` +
			sessionUUID + `","cwd":"` + dir + `","version":"` + version +
			`","message":{"role":"user","content":"secondary question"}}`,
		`{"type":"assistant","uuid":"s2","parentUuid":"s1","isSidechain":false,"sessionId":"` +
			sessionUUID + `","cwd":"` + dir + `","version":"` + version +
			`","message":{"role":"assistant","stop_reason":"end_turn",` +
			`"content":[{"type":"text","text":"secondary answer"}]}}`,
	}
	path := filepath.Join(f.home, "projects", slug, sessionUUID+".jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Join(records, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return sessionUUID, dir
}

// assertNoTranscriptFor proves a refused materialization left no session file
// the provider could discover for that working directory.
func (f *claudeForkFixture) assertNoTranscriptFor(t *testing.T, projectDir string) {
	t.Helper()
	slug, err := ClaudeProjectSlug(projectDir)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(f.home, "projects", slug))
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatal(err)
	}
	for _, entry := range entries {
		t.Fatalf("refused materialization left %q under %s", entry.Name(), slug)
	}
}
