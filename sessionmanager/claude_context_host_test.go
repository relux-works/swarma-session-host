package sessionmanager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// claudeContextHostFixture drives a real ClaudeHostFactory over a fake provider
// that behaves the way the installed binary was measured to behave: it honours
// --session-id alongside --resume --fork-session, and it commits the forked
// transcript on its first submission rather than at startup.
//
// The whole point of testing through the factory is the review finding: the
// materializer's own unit tests passed while the factory persisted the wrong
// identity, so nothing below the factory can establish that the run's writable
// session is the one the catalog names.
type claudeContextHostFixture struct {
	fork       *claudeForkFixture
	factory    *ClaudeHostFactory
	executable string
	targetDir  string
	slugDir    string
	logPath    string
	// mutatePlan adjusts the fork plan the factory is handed, which is how the
	// cross-build cases say the base was written by an older binary.
	mutatePlan func(*claudeContextForkPlan)
}

func newClaudeContextHostFixture(t *testing.T) *claudeContextHostFixture {
	t.Helper()
	fork := newClaudeForkFixture(t)
	root := t.TempDir()
	executable := claudeWriteExecutable(t, "claude", []byte(fakeClaudeScript), 0o755)
	t.Setenv("FAKE_CLAUDE_LOG", filepath.Join(root, "fake-claude.log"))
	t.Setenv("FAKE_CLAUDE_VERSION", claudeGoalMinimumVersion)

	targetDir := fork.dir(".temp/TASK-260730-aaaaaa/worktree")
	slug, err := ClaudeProjectSlug(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	slugDir := filepath.Join(fork.home, "projects", slug)
	// The fake writes provider state where the real provider would, which is what
	// makes the slug the adapter derived the thing under test.
	t.Setenv("FAKE_CLAUDE_PROJECTS_DIR", slugDir)

	factory := NewClaudeHostFactory(fork.layout)
	factory.readyTimeout = 20 * time.Second
	factory.ackTimeout = 10 * time.Second
	factory.submitDelay = 10 * time.Millisecond
	// The fake commits its fork on the first submission, so a real wait is never
	// needed; the refusal cases would otherwise each sit out the production
	// deadline.
	factory.forkVerifyTimeout = 5 * time.Second
	return &claudeContextHostFixture{
		fork:       fork,
		factory:    factory,
		executable: executable,
		targetDir:  targetDir,
		slugDir:    slugDir,
		logPath:    filepath.Join(root, "fake-claude.log"),
	}
}

func (f *claudeContextHostFixture) launchPayload(t *testing.T) LaunchSpec {
	t.Helper()
	p := PrimarySessionLaunchPlan{Contract: PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "claude", ProjectDir: f.targetDir, Executable: f.executable, LaunchVariants: LaunchVariants{Interactive: LaunchArgv{Argv: []string{}}, ManagedHost: LaunchArgv{Kind: "claude-pty", Argv: []string{}}}, RequiredEnvNames: []string{}}
	return LaunchSpec{Executable: p.Executable, Cwd: p.ProjectDir, Environment: claudeFixtureEnvironment(), Claude: &ClaudePreparedLaunch{Contract: ClaudeSessionLaunchContract, Plan: p, Rows: 32, Cols: 100}}
}

// forkPlanRecord is the prior record shape a spawn preflight hands the factory.
func (f *claudeContextHostFixture) forkPlanRecord(t *testing.T) *ProviderHostRecord {
	t.Helper()
	plan := f.fork.plan(f.targetDir)
	if f.mutatePlan != nil {
		f.mutatePlan(plan)
	}
	return &ProviderHostRecord{Provider: "claude", Snapshot: RunnerSnapshot{Kind: "claude-fork", ClaudeFork: plan}}
}

func (f *claudeContextHostFixture) launch(t *testing.T) (ProviderHost, error) {
	t.Helper()
	return f.factory.Launch(context.Background(), ProviderLaunchRequest{
		Launch: f.launchPayload(t),
		Prior:  f.forkPlanRecord(t),
	})
}

func (f *claudeContextHostFixture) assertRetainedForkPendingGC(t *testing.T) {
	t.Helper()
	events, err := loadClaudeReclamationEvents(f.fork.layout, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Kind != claudeReclamationFork ||
			event.State != claudeReclamationPendingGC ||
			event.ChildCtxID != "CTX-260730-c00002" {
			continue
		}
		roles := map[string]bool{}
		for _, artifact := range event.Artifacts {
			roles[artifact.Role] = artifact.Path != ""
		}
		if roles["materialized_resume_source"] && roles["writable_fork"] {
			return
		}
	}
	t.Fatal("refused Claude launch has no pending_gc event for both provider artifact identities")
}

func (f *claudeContextHostFixture) argv(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(f.logPath)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestClaudeHostPersistsTheWritableForkIdentityNotItsResumeSource is the primary
// B1 regression.
//
// The factory used to store the materialized copy's identity as the session's
// own, so a restart bare-resumed a fork source and appended into it. What is
// asserted here is the whole chain: the launch names a writable child, the
// snapshot persists that child and records the copy separately, and the child is
// what the provider actually wrote.
func TestClaudeHostPersistsTheWritableForkIdentityNotItsResumeSource(t *testing.T) {
	fixture := newClaudeContextHostFixture(t)
	host, err := fixture.launch(t)
	if err != nil {
		t.Fatalf("launching a materialized context fork failed: %v", err)
	}
	defer stopClaudeHost(t, host)

	raw, err := host.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeClaudeHostSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ContextResumeSessionUUID == "" {
		t.Fatal("the snapshot does not record which copy the session resumed")
	}
	if snapshot.SessionUUID == snapshot.ContextResumeSessionUUID {
		t.Fatal("the snapshot persists the resume source as the session's own identity")
	}
	if snapshot.SessionUUID == fixture.fork.baseUUID {
		t.Fatal("the snapshot persists the base identity as the session's own")
	}
	if snapshot.ContextForkVerifiedAt.IsZero() {
		t.Fatal("the launch returned without recording that the fork was verified")
	}

	// The provider wrote the writable child the manager named, in the project
	// directory the manager derived. Both halves matter: the identity proves the
	// catalog points at the run's own session, and the directory proves the slug.
	forkPath := filepath.Join(fixture.slugDir, snapshot.SessionUUID+claudeTranscriptSuffix)
	forkRaw, forkFacts, err := inspectClaudeTranscriptFile(forkPath)
	if err != nil {
		t.Fatalf("the writable fork is not where the adapter said it would be: %v", err)
	}
	if len(forkFacts.SessionUUIDs) != 1 || forkFacts.SessionUUIDs[0] != snapshot.SessionUUID {
		t.Fatalf("the writable fork carries identities %v", forkFacts.SessionUUIDs)
	}
	if strings.Contains(string(forkRaw), snapshot.ContextResumeSessionUUID) {
		t.Fatal("the writable fork still carries the identity of the copy it came from")
	}

	// The launch selector: fork, never bare resume, and the child named explicitly.
	argv := fixture.argv(t)
	if !strings.Contains(argv, "--fork-session") ||
		!strings.Contains(argv, "--resume "+snapshot.ContextResumeSessionUUID) ||
		!strings.Contains(argv, "--session-id "+snapshot.SessionUUID) {
		t.Fatalf("the launch selector did not fork the copy into the named child:\n%s", argv)
	}

	// And both fork sources survived byte-identical.
	fixture.fork.assertBaseUnchanged(t)
	copyPath := filepath.Join(
		fixture.slugDir, snapshot.ContextResumeSessionUUID+claudeTranscriptSuffix)
	if _, err := os.Stat(copyPath); err != nil {
		t.Fatalf("the materialized copy did not survive the launch: %v", err)
	}
}

func TestClaudeHostRestartResumesTheWritableForkAndKeepsItsHistory(t *testing.T) {
	fixture := newClaudeContextHostFixture(t)
	host, err := fixture.launch(t)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := host.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeClaudeHostSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	forkPath := filepath.Join(fixture.slugDir, snapshot.SessionUUID+claudeTranscriptSuffix)
	_, beforeFacts, err := inspectClaudeTranscriptFile(forkPath)
	if err != nil {
		t.Fatal(err)
	}
	// The manager process is gone, so its writer lease is gone with it.
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	recovered, err := fixture.factory.Launch(context.Background(), ProviderLaunchRequest{
		Launch: fixture.launchPayload(t),
		Prior:  &ProviderHostRecord{Provider: "claude", Snapshot: raw},
	})
	if err != nil {
		t.Fatalf("recovering the context session failed: %v", err)
	}
	defer stopClaudeHost(t, recovered)

	recoveredRaw, err := recovered.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	recoveredSnapshot, err := decodeClaudeHostSnapshot(recoveredRaw)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredSnapshot.SessionUUID != snapshot.SessionUUID {
		t.Fatalf("recovery resumed %s, not the run's own session %s",
			recoveredSnapshot.SessionUUID, snapshot.SessionUUID)
	}
	if recoveredSnapshot.ContextCtxID != snapshot.ContextCtxID {
		t.Fatalf("recovery lost the context binding: %q", recoveredSnapshot.ContextCtxID)
	}

	// The recovery selector is a bare resume of the child. A --fork-session here
	// would abandon the run's history under a third identity.
	argv := fixture.argv(t)
	lines := strings.Split(strings.TrimSpace(argv), "\n")
	last := lines[len(lines)-1]
	if !strings.Contains(last, "--resume "+snapshot.SessionUUID) {
		t.Fatalf("recovery did not resume the writable fork: %q", last)
	}
	if strings.Contains(last, "--fork-session") {
		t.Fatalf("recovery forked again instead of resuming the run's own session: %q", last)
	}

	// The child's history is what must survive, not its bytes: a resumed session
	// legitimately appends its own startup records to the transcript it owns. A
	// re-fork or a wrong-identity resume is what would lose them.
	_, afterFacts, err := inspectClaudeTranscriptFile(forkPath)
	if err != nil {
		t.Fatalf("the child transcript did not survive recovery: %v", err)
	}
	if len(afterFacts.SessionUUIDs) != 1 || afterFacts.SessionUUIDs[0] != snapshot.SessionUUID {
		t.Fatalf("recovery rebound the child transcript to %v", afterFacts.SessionUUIDs)
	}
	if !claudeTranscriptInherits(afterFacts.RecordUUIDs, beforeFacts.RecordUUIDs) {
		t.Fatal("recovery lost records the child session already held")
	}
	fixture.fork.assertBaseUnchanged(t)
}

func TestClaudeHostRefusesALaunchItCannotProveInheritedTheContext(t *testing.T) {
	for name, mode := range map[string]string{
		"no fork written":            "none",
		"started cold":               "cold",
		"provider minted its own id": "foreign",
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newClaudeContextHostFixture(t)
			t.Setenv("FAKE_CLAUDE_FORK", mode)
			fixture.factory.readyTimeout = 10 * time.Second
			host, err := fixture.launch(t)
			if host != nil {
				stopClaudeHost(t, host)
			}
			assertPublicErrorCode(t, err, claudeContextForkUnverified)
			fixture.fork.assertBaseUnchanged(t)
			// Refusal releases the writer but retains and catalogs the provider
			// artifacts for lease-aware reclamation.
			entries, readErr := os.ReadDir(fixture.slugDir)
			if readErr != nil && !os.IsNotExist(readErr) {
				t.Fatal(readErr)
			}
			if len(entries) == 0 {
				t.Fatal("the refused launch deleted every retained provider artifact")
			}
			fixture.assertRetainedForkPendingGC(t)
		})
	}
}

func TestClaudeHostRefusesAForkThatAppendedIntoItsResumeSource(t *testing.T) {
	fixture := newClaudeContextHostFixture(t)
	t.Setenv("FAKE_CLAUDE_FORK", "append")
	host, err := fixture.launch(t)
	if host != nil {
		stopClaudeHost(t, host)
	}
	assertPublicErrorCode(t, err, claudeContextSourceMutated)
}

func TestClaudeHostRefusesAForkWhoseBaseChangedWhileTheProviderRan(t *testing.T) {
	fixture := newClaudeContextHostFixture(t)
	t.Setenv("FAKE_CLAUDE_FORK", "basemutate")
	t.Setenv("FAKE_CLAUDE_BASE_PATH", fixture.fork.basePath)
	signalPath := filepath.Join(filepath.Dir(fixture.logPath), "base-mutated")
	releasePath := filepath.Join(filepath.Dir(fixture.logPath), "release-fork")
	t.Setenv("FAKE_CLAUDE_BASE_MUTATED_SIGNAL", signalPath)
	t.Setenv("FAKE_CLAUDE_BASE_MUTATE_RELEASE", releasePath)
	baseBefore, err := claudeFileDigest(fixture.fork.basePath)
	if err != nil {
		t.Fatal(err)
	}
	request := ProviderLaunchRequest{
		Launch: fixture.launchPayload(t),
		Prior:  fixture.forkPlanRecord(t),
	}
	type launchResult struct {
		host ProviderHost
		err  error
	}
	launchDone := make(chan launchResult, 1)
	go func() {
		host, launchErr := fixture.factory.Launch(context.Background(), request)
		launchDone <- launchResult{host: host, err: launchErr}
	}()

	claudeWaitForFixturePath(t, signalPath, 10*time.Second)
	baseDuringProvider, err := claudeFileDigest(fixture.fork.basePath)
	if err != nil {
		t.Fatal(err)
	}
	if baseDuringProvider == baseBefore {
		t.Fatal("the provider barrier opened before the resume source was mutated")
	}
	if err := os.WriteFile(releasePath, []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var host ProviderHost
	select {
	case result := <-launchDone:
		host, err = result.host, result.err
	case <-time.After(10 * time.Second):
		t.Fatal("the launch did not finish after the provider barrier was released")
	}
	if host != nil {
		stopClaudeHost(t, host)
	}
	assertPublicErrorCode(t, err, claudeContextSourceMutated)

	// The refusal retains provider artifacts and hands them to retention/GC. The
	// corrupted base is deliberately left alone as evidence.
	entries, readErr := os.ReadDir(fixture.slugDir)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatal(readErr)
	}
	if len(entries) == 0 {
		t.Fatal("the refused launch deleted every retained provider artifact")
	}
	fixture.assertRetainedForkPendingGC(t)
	if _, err := acquireClaudeContextWriterLease(
		fixture.fork.layout, "CTX-260730-c00002", time.Now,
	); err != nil {
		t.Fatalf("a refused launch stranded its writer lease: %v", err)
	}
}

func TestClaudeHostFailsAResumeWhoseMissingSessionArrivesAfterTheBanner(t *testing.T) {
	fixture := newClaudeContextHostFixture(t)
	t.Setenv("FAKE_CLAUDE_MISSING_SESSION_DELAY", "1")
	host, err := fixture.launch(t)
	if host != nil {
		stopClaudeHost(t, host)
	}
	assertPublicErrorCode(t, err, ClaudeErrorResumeFailed)
	fixture.fork.assertBaseUnchanged(t)
}

func TestClaudeHostHoldsOneWriterAcrossRecovery(t *testing.T) {
	fixture := newClaudeContextHostFixture(t)
	host, err := fixture.launch(t)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := host.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeClaudeHostSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	recovered, err := fixture.factory.Launch(context.Background(), ProviderLaunchRequest{
		Launch: fixture.launchPayload(t),
		Prior:  &ProviderHostRecord{Provider: "claude", Snapshot: raw},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClaudeHost(t, recovered)

	_, err = acquireClaudeContextWriterLease(fixture.fork.layout, snapshot.ContextCtxID, time.Now)
	assertPublicErrorCode(t, err, claudeContextWriterBusy)

	// And it is released when the recovered host stops, so the materialization is
	// forkable again without waiting anything out.
	if err := recovered.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := acquireClaudeContextWriterLease(
		fixture.fork.layout, snapshot.ContextCtxID, time.Now)
	if err != nil {
		t.Fatalf("the writer lease outlived the session that held it: %v", err)
	}
	lease.release()
}

func TestClaudeHostReleasesTheWriterLeaseOnNaturalExit(t *testing.T) {
	fixture := newClaudeContextHostFixture(t)
	host, err := fixture.launch(t)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := host.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeClaudeHostSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}

	// /exit ends the fake the way a user quitting ends the real provider.
	stream, err := host.(TerminalHost).AttachTerminal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("/exit\r")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		state, err := host.State(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if state == ProviderHostStopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the fake provider did not exit")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The release happens on the exit observation, so allow it to be observed.
	var leaseErr error
	var lease *claudeContextLease
	for time.Now().Before(deadline) {
		lease, leaseErr = acquireClaudeContextWriterLease(
			fixture.fork.layout, snapshot.ContextCtxID, time.Now)
		if leaseErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leaseErr != nil {
		t.Fatalf("a naturally exited session kept its writer lease: %v", leaseErr)
	}
	lease.release()
	_ = host.Stop(context.Background())
}

func TestClaudeRequiredForkRefusesAnAbsentForkSource(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		prior func(*claudeContextHostFixture) *ProviderHostRecord
	}{
		{
			name:  "no prior record at all",
			prior: func(*claudeContextHostFixture) *ProviderHostRecord { return nil },
		},
		{
			name: "a prior record carrying no bytes",
			prior: func(*claudeContextHostFixture) *ProviderHostRecord {
				return &ProviderHostRecord{Provider: "claude"}
			},
		},
		{
			name: "a prior record whose opaque payload is empty JSON bytes",
			prior: func(*claudeContextHostFixture) *ProviderHostRecord {
				return &ProviderHostRecord{Provider: "claude", Snapshot: RunnerSnapshot{}}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newClaudeContextHostFixture(t)
			host, err := fixture.factory.Launch(context.Background(), ProviderLaunchRequest{
				Launch:        fixture.launchPayload(t),
				Prior:         testCase.prior(fixture),
				PriorMustFork: true,
			})
			if err == nil {
				_ = host.Stop(context.Background())
				t.Fatal("a required fork launched with no fork source instead of refusing")
			}
			var public *PublicError
			if !errors.As(err, &public) || public.Code != claudeContextForkPlanInvalid {
				t.Fatalf("a required fork with an absent source failed for the wrong reason: %v", err)
			}
			if !strings.Contains(public.Message, "rematerialize the child") {
				t.Fatalf("the refusal does not name the recovery an operator can act on: %v", public.Message)
			}
			// The refusal has to land before provider work, not after: the fake records
			// every invocation including its own --version capability probe, so any log
			// at all means a cold session was started against a required fork.
			if _, statErr := os.Stat(fixture.logPath); !os.IsNotExist(statErr) {
				invocation, _ := os.ReadFile(fixture.logPath)
				t.Fatalf("the refused required fork still invoked Claude:\n%s", invocation)
			}
			fixture.fork.assertBaseUnchanged(t)
		})
	}
}

func TestClaudeColdLaunchStillWorksWithoutARequiredFork(t *testing.T) {
	fixture := newClaudeContextHostFixture(t)
	host, err := fixture.factory.Launch(context.Background(), ProviderLaunchRequest{
		Launch: fixture.launchPayload(t),
	})
	if err != nil {
		t.Fatalf("an ordinary cold launch was refused: %v", err)
	}
	defer func() { _ = host.Stop(context.Background()) }()
	argv := fixture.argv(t)
	if strings.Contains(argv, "--fork-session") || strings.Contains(argv, "--resume") {
		t.Fatalf("a cold launch inherited a session it was never given:\n%s", argv)
	}
}
