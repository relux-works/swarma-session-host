package sessionmanager

import (
	"context"
	"os"
	"strings"
	"testing"
)

// The original owner-evaluator root now binds through the actual neutral input
// contract. This exercises the external runner over a scripted PTY fixture;
// provider completion is not canonical board acceptance or a native-live claim.
func TestClaudeHostRunsAssignmentThroughNativeGoalEvaluator(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	t.Setenv("FAKE_CLAUDE_OWNER_RESULT", "achieved")
	host, err := newTestClaudeFactory(t).Launch(context.Background(), ProviderLaunchRequest{SessionID: "SES-claude-owner", Launch: fixture.launchPayload(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClaudeHost(t, host)
	bindClaudeRunnerFixtureGoal(t, host.(*claudeHost))
	runner, ok := host.(ProviderOwnerRunner)
	if !ok {
		t.Fatalf("host type %T does not implement ProviderOwnerRunner", host)
	}
	result, err := runner.RunOwner(context.Background(), ProviderRunRequest{Prompt: "execute the isolated Claude owner assignment"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != ProviderRunComplete || !strings.Contains(result.Output, "Goal achieved") {
		t.Fatalf("owner result = %#v", result)
	}
	// The fixture records stdin before publishing the terminal native result, so
	// RunOwner returning establishes that these submitted lines are already live.
	raw, err := os.ReadFile(fixture.logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "goal-received\n") {
		t.Fatal("native goal protocol never reached the provider fixture")
	}
	if !strings.Contains(string(raw), "execute the isolated Claude owner assignment") {
		t.Fatal("native evaluator never received the owner assignment")
	}
}

// A real limit observation from the same scripted evaluator is recoverable
// usage refusal; it cannot be promoted into successful owner completion.
func TestClaudeHostOwnerLimitIsNotReportedAsCompleted(t *testing.T) {
	fixture := newFakeClaudeFixture(t)
	t.Setenv("FAKE_CLAUDE_OWNER_RESULT", "limited")
	host, err := newTestClaudeFactory(t).Launch(context.Background(), ProviderLaunchRequest{SessionID: "SES-claude-owner-limited", Launch: fixture.launchPayload(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClaudeHost(t, host)
	bindClaudeRunnerFixtureGoal(t, host.(*claudeHost))
	result, err := host.(ProviderOwnerRunner).RunOwner(context.Background(), ProviderRunRequest{Prompt: "execute bounded fixture assignment"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != ProviderRunUsageLimited || result.ExitCode != 1 || !result.Recoverable || !strings.Contains(result.Output, "You've hit your limit") {
		t.Fatalf("limit was reclassified as completion or lost: %#v", result)
	}
}

func bindClaudeRunnerFixtureGoal(t *testing.T, h *claudeHost) {
	t.Helper()
	command := &ClaudeGoalCommand{CommandID: "runner-fixture-bind", Set: &ClaudeGoalSet{Text: "/goal tbg-babf9a750e8d263dc85381a1a78fe580 deliver fixture assignment", Token: "tbg-babf9a750e8d263dc85381a1a78fe580"}}
	input := InputSubmission{ID: command.CommandID, Lease: ControllerLease{SessionID: "SES-fixture", Epoch: 3}, Payload: RenderedInput{ClaudeGoal: command}, Expected: AckExpectation{Kind: "claude-goal-ack", Correlation: command.CommandID}}
	receipt := InputReceipt{SessionID: input.Lease.SessionID, Epoch: input.Lease.Epoch, ID: input.ID}
	wait, err := h.ExecuteInput(context.Background(), input, receipt)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if observation.Receipt != receipt || observation.ClaudeGoal == nil || observation.ClaudeGoal.Operation != ClaudeGoalOperationSet {
		t.Fatal("runner fixture goal was not actually acknowledged")
	}
}
