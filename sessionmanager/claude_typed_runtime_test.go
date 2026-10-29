package sessionmanager

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

// The external factory's typed durable boundary keeps complete launch receipts
// and mutable slices isolated. This is an in-memory boundary test, not a native
// process or common catalog integration claim.
func TestClaudeTypedSnapshotKeepsLaunchAndReturnsOwnedCopies(t *testing.T) {
	plan := PrimarySessionLaunchPlan{Contract: PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "claude", Executable: "/fixture/claude", ProjectDir: "/fixture/project", Sources: []LaunchPlanSource{{Kind: "fixture", EnabledServers: []string{"retained-server"}}}, RequiredEnvNames: []string{"FIXTURE_ENV"}, LaunchVariants: LaunchVariants{ManagedHost: LaunchArgv{Kind: "claude-pty", Argv: []string{"--model", "fixture-model"}}}}
	original := ClaudeRunnerSnapshot{Contract: claudeHostSnapshotContract, SessionUUID: "11111111-2222-4333-8444-555555555555", PID: 123, ContextInjectedCtxIDs: []string{"CTX-260730-a00001"}, Plan: LaunchSpec{Executable: plan.Executable, Cwd: plan.ProjectDir, Environment: []string{"FIXTURE_ENV=synthetic"}, Sources: plan.Sources, Claude: &ClaudePreparedLaunch{Contract: ClaudeSessionLaunchContract, Plan: plan, Rows: 32, Cols: 100}}}
	want := claudeMustMarshalJSON(t, original)
	host := &claudeHost{snapshot: original}
	got, err := host.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "claude" || got.Claude == nil || !reflect.DeepEqual(*got.Claude, original) {
		t.Fatal("typed snapshot lost complete launch or context facts")
	}
	got.Claude.ContextInjectedCtxIDs[0] = "modified"
	got.Claude.Plan.Environment[0] = "modified"
	got.Claude.Plan.Claude.Plan.RequiredEnvNames[0] = "modified"
	got.Claude.Plan.Claude.Plan.Sources[0].EnabledServers[0] = "modified"
	again, err := host.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if claudeMustMarshalJSON(t, again.Claude) != want {
		t.Fatal("snapshot result aliases live Claude launch/context state")
	}
	factory := NewClaudeHostFactory(claudeTestLayout(t, "typed-restore"))
	restored, err := factory.Restore(context.Background(), ProviderHostRecord{Provider: "claude", Snapshot: again})
	if err != nil {
		t.Fatal(err)
	}
	again.Claude.Plan.Environment[0] = "caller-modified"
	durable, err := restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if claudeMustMarshalJSON(t, durable.Claude) != want {
		t.Fatal("Restore retained a caller-owned snapshot alias")
	}
	durable.Claude.Plan.Claude.Plan.RequiredEnvNames[0] = "returned-modified"
	durable2, err := restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if claudeMustMarshalJSON(t, durable2.Claude) != want {
		t.Fatal("detached Snapshot returns a mutable state alias")
	}
}

// Typed prior discrimination refuses foreign or ambiguous shapes before any
// filesystem/process work; a valid fork plan remains accepted and copied.
func TestClaudeTypedPriorRefusesForeignAndAmbiguousShapesBeforeIO(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	plan := fixture.plan(fixture.dir("typed-target"))
	valid := ProviderHostRecord{Provider: "claude", Snapshot: RunnerSnapshot{Kind: "claude-fork", ClaudeFork: plan}}
	decoded, err := decodeClaudeForkPlan(valid)
	if err != nil {
		t.Fatal(err)
	}
	decoded.ChildCtxID = "different"
	if plan.ChildCtxID == decoded.ChildCtxID {
		t.Fatal("fork plan aliases admitted prior")
	}
	own := &ClaudeRunnerSnapshot{Contract: claudeHostSnapshotContract, SessionUUID: "11111111-2222-4333-8444-555555555555", PID: 123}
	for name, prior := range map[string]ProviderHostRecord{
		"foreign provider":     {Provider: "codex", Snapshot: valid.Snapshot},
		"wrong tag":            {Provider: "claude", Snapshot: RunnerSnapshot{Kind: "claude", ClaudeFork: plan}},
		"running handle":       {Provider: "claude", Snapshot: RunnerSnapshot{Kind: "claude", Claude: own}},
		"ambiguous":            {Provider: "claude", Snapshot: RunnerSnapshot{Kind: "claude-fork", ClaudeFork: plan, Claude: own}},
		"foreign fork payload": {Provider: "claude", Snapshot: RunnerSnapshot{Kind: "claude-fork", ClaudeFork: plan, CodexFork: &CodexContextForkPlan{}}},
		"foreign payload":      {Provider: "claude", Snapshot: RunnerSnapshot{Kind: "claude-fork", ClaudeFork: plan, Codex: &CodexRunnerSnapshot{}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeClaudeForkPlan(prior); err == nil {
				t.Fatal("foreign or ambiguous typed prior was admitted")
			}
			factory := NewClaudeHostFactory(claudeTestLayout(t, "invalid-prior"))
			h, err := factory.Launch(context.Background(), ProviderLaunchRequest{Prior: &prior, PriorMustFork: true})
			if h != nil {
				t.Fatal("invalid prior produced a host before launch validation")
			}
			var public *PublicError
			if !errors.As(err, &public) || public.Code != claudeContextForkPlanInvalid {
				t.Fatalf("invalid prior refused at wrong cut: %v", err)
			}
		})
	}
}

// Required environment presence is checked in the actual supplied child
// environment, not a different process's ambient values. Empty-but-present is
// intentionally valid, as in the original LookupEnv contract.
func TestClaudeTypedRequiredEnvironmentUsesSuppliedChildValues(t *testing.T) {
	const key = "CLAUDE_RELOCATION_SYNTHETIC_REQUIRED"
	t.Setenv(key, "ambient-only")
	fixture := newFakeClaudeFixture(t)
	factory := newTestClaudeFactory(t)
	launch := fixture.launchPayload(t)
	launch.Claude.Plan.RequiredEnvNames = []string{key}
	filtered := launch.Environment[:0:0]
	for _, entry := range launch.Environment {
		if !strings.HasPrefix(entry, key+"=") {
			filtered = append(filtered, entry)
		}
	}
	launch.Environment = filtered
	host, err := factory.Launch(context.Background(), ProviderLaunchRequest{SessionID: "SES-typed-env-negative", Launch: launch})
	if host != nil {
		stopClaudeHost(t, host)
		t.Fatal("ambient-only required value admitted into absent child environment")
	}
	if err == nil || err.Error() != "required launch environment "+key+" is not set" {
		t.Fatalf("wrong required-environment refusal: %v", err)
	}
	if len(fixture.argvLines(t)) != 0 {
		t.Fatal("missing child environment invoked the provider fixture")
	}
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	launch.Environment = append(launch.Environment, key+"=")
	host, err = factory.Launch(context.Background(), ProviderLaunchRequest{SessionID: "SES-typed-env-positive", Launch: launch})
	if err != nil {
		t.Fatalf("explicit empty-but-present child environment refused: %v", err)
	}
	defer stopClaudeHost(t, host)
}

// Typed input retains physical acknowledgement evidence and exact caller
// receipt/correlation. Wrong receipts cannot submit a command; missing native
// observation capability remains closed while goal-less launch still works.
func TestClaudeTypedInputPreservesReceiptsAndNativeEvidence(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	factory := newTestClaudeFactory(t)
	launched, err := factory.Launch(context.Background(), ProviderLaunchRequest{SessionID: "SES-typed-input", Launch: fixture.launchPayload(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClaudeHost(t, launched)
	h := launched.(*claudeHost)
	if h.SupportsNativeGoals() {
		t.Fatal("unsupported native observation capability was opened")
	}
	if _, err := h.ObserveNativeGoal(context.Background()); !errors.Is(err, ErrHostCapabilityUnavailable) {
		t.Fatalf("native observation error=%v", err)
	}
	for _, command := range []ClaudeGoalCommand{
		{CommandID: "set-attempt", Set: &ClaudeGoalSet{Text: "/goal tbg-babf9a750e8d263dc85381a1a78fe580 fixture condition", Token: "tbg-babf9a750e8d263dc85381a1a78fe580"}},
		{CommandID: "clear-attempt", Clear: &ClaudeGoalClear{Text: "/goal clear"}},
	} {
		input := InputSubmission{ID: command.CommandID, Lease: ControllerLease{SessionID: "SES-typed-input", Epoch: 7}, Payload: RenderedInput{ClaudeGoal: &command}, Expected: AckExpectation{Kind: "claude-goal-ack", Correlation: command.CommandID}}
		receipt := InputReceipt{SessionID: input.Lease.SessionID, Epoch: input.Lease.Epoch, ID: input.ID}
		before, err := os.ReadFile(fixture.logPath)
		if err != nil {
			t.Fatal(err)
		}
		wrong := receipt
		wrong.Epoch++
		if waiter, err := h.ExecuteInput(context.Background(), input, wrong); waiter != nil || !errors.Is(err, ErrInputAcknowledgementMismatch) {
			t.Fatalf("wrong receipt admitted: waiter=%v error=%v", waiter != nil, err)
		}
		after, err := os.ReadFile(fixture.logPath)
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != string(before) {
			t.Fatal("wrong receipt caused physical provider input")
		}
		waiter, err := h.ExecuteInput(context.Background(), input, receipt)
		if err != nil {
			t.Fatal(err)
		}
		observation, err := waiter(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if observation.Receipt != receipt || observation.Correlation != command.CommandID || observation.ClaudeGoal == nil || observation.ClaudeGoal.CommandID != command.CommandID {
			t.Fatalf("typed receipt/correlation dropped: %#v", observation)
		}
		want := ClaudeGoalOperationClear
		class := InputEvidenceClaudePTYClearObserved
		if command.Set != nil {
			want = ClaudeGoalOperationSet
			class = InputEvidenceClaudePTYConditioned
			if observation.ClaudeGoal.NormalizedToken != command.Set.Token {
				t.Fatal("actual goal token identity lost")
			}
		}
		if observation.ClaudeGoal.Operation != want || observation.EvidenceClass != class || observation.ClaudeGoal.ObservedText == "" {
			t.Fatalf("native acknowledgement evidence lost: %#v", observation)
		}
	}
}

// The actual external factory must not discard a foreign payload or select one
// of competing variants. Every refusal precedes provider fixture I/O; the
// single-Claude neighboring launch remains usable without canonical callbacks.
func TestClaudeTypedLaunchRefusesForeignAndAmbiguousPayloadsBeforeIO(t *testing.T) {
	for _, name := range []string{"ambiguous_claude_codex", "ambiguous_claude_muse", "foreign_codex", "foreign_muse", "foreign_request_with_prior", "foreign_prior_fallback"} {
		t.Run(name, func(t *testing.T) {
			fixture := newFakeClaudeFixture(t)
			factory := newTestClaudeFactory(t)
			own := fixture.launchPayload(t)
			request := ProviderLaunchRequest{SessionID: "SES-closed-launch", Launch: own}
			prior := &ProviderHostRecord{Provider: "claude", Snapshot: RunnerSnapshot{Kind: "claude", Claude: &ClaudeRunnerSnapshot{Contract: claudeHostSnapshotContract, SessionUUID: "11111111-2222-4333-8444-555555555555", PID: 123, Plan: own}}}
			switch name {
			case "ambiguous_claude_codex":
				request.Launch.Codex = &CodexPreparedLaunch{}
			case "ambiguous_claude_muse":
				request.Launch.Muse = &MuseSessionLaunch{}
			case "foreign_codex":
				request.Launch = LaunchSpec{Codex: &CodexPreparedLaunch{}}
			case "foreign_muse":
				request.Launch = LaunchSpec{Muse: &MuseSessionLaunch{}}
			case "foreign_request_with_prior":
				request.Launch = LaunchSpec{Codex: &CodexPreparedLaunch{}}
				request.Prior = prior
			case "foreign_prior_fallback":
				request.Launch = LaunchSpec{}
				prior.Snapshot.Claude.Plan.Codex = &CodexPreparedLaunch{}
				request.Prior = prior
			}
			host, err := factory.Launch(context.Background(), request)
			if host != nil {
				defer stopClaudeHost(t, host)
			}
			if host != nil || !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "ambiguous or foreign typed launch payload") {
				t.Fatalf("foreign or ambiguous typed launch payload was not refused at the closed input cut: host=%v error=%v", host != nil, err)
			}
			if lines := fixture.argvLines(t); len(lines) != 0 {
				t.Fatalf("typed union refusal invoked provider fixture: %v", lines)
			}
		})
	}
	t.Run("single_claude_positive", func(t *testing.T) {
		fixture := newFakeClaudeFixture(t)
		host, err := newTestClaudeFactory(t).Launch(context.Background(), ProviderLaunchRequest{SessionID: "SES-closed-launch-positive", Launch: fixture.launchPayload(t)})
		if err != nil {
			t.Fatalf("valid single-Claude launch refused: %v", err)
		}
		defer stopClaudeHost(t, host)
	})
}
