package sessionmanager

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
)

// Restore uses the typed native image and real read-only fake RPC path without
// any canonical goal or provider process. Returned observation is joined safely.
func TestCodexFactoryRestoresGoalLessTypedRunner(t *testing.T) {
	ctx, f, _, _ := newNativeCodexFixture(t, "idle")
	snapshot, err := f.runner.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	before := copyRunnerSnapshot(snapshot)
	native, err := NewCodexHostFactory(Layout{}).Restore(ctx, ProviderHostRecord{Provider: "codex", Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	closer := native.(ProviderObservationCloser)
	t.Cleanup(func() {
		if err := closer.CloseObservation(context.Background()); err != nil {
			t.Error(err)
		}
	})
	restored, err := native.Snapshot()
	if err != nil || restored.Kind != "codex" || restored.Codex.ThreadID != before.Codex.ThreadID {
		t.Fatalf("restore %#v %v", restored, err)
	}
	snapshot.Codex.HostArgv = append(snapshot.Codex.HostArgv, "foreign")
	again, err := native.Snapshot()
	if err != nil || !reflect.DeepEqual(again.Codex.HostArgv, before.Codex.HostArgv) {
		t.Fatal("restore aliased caller snapshot")
	}
}

// Missing, mixed and mismatched typed priors refuse before any native start;
// the restore test above supplies the actual valid neighboring path.
func TestCodexFactoryRejectsMissingOrMismatchedTypedInputs(t *testing.T) {
	factory := NewCodexHostFactory(Layout{})
	if _, err := factory.Launch(context.Background(), ProviderLaunchRequest{}); !errors.Is(err, ErrHostCapabilityUnavailable) {
		t.Fatalf("missing launch payload %v", err)
	}
	if _, err := factory.Launch(context.Background(), ProviderLaunchRequest{Launch: LaunchSpec{Codex: &CodexPreparedLaunch{}, Claude: &ClaudePreparedLaunch{}}}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("mixed launch payload admitted: %v", err)
	}
	for _, record := range []ProviderHostRecord{{Provider: "claude"}, {Provider: "codex", Snapshot: RunnerSnapshot{Kind: "claude"}}, {Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex"}}, {Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &CodexRunnerSnapshot{}, Claude: &ClaudeRunnerSnapshot{}}}} {
		if _, err := factory.Restore(context.Background(), record); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid restore admitted %v", err)
		}
	}
	prepared := &CodexPreparedLaunch{Snapshot: CodexRunnerSnapshot{ThreadID: "different"}}
	prior := &ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &CodexRunnerSnapshot{ThreadID: "prior"}}}
	_, err := factory.Launch(context.Background(), ProviderLaunchRequest{Launch: LaunchSpec{Codex: prepared}, Prior: prior, PriorMustFork: true})
	var refusal *PublicError
	if !errors.As(err, &refusal) || refusal.Code != codexContextForkPlanInvalid {
		t.Fatalf("required fork fell through: %v", err)
	}
}

// An ordinary record cannot satisfy a required fork; a typed fork with no
// context capability also refuses before private-home/files/process effects.
func TestCodexLaunchNeverResumesAnImmutableBaseWhenForkIsRequired(t *testing.T) {
	root := t.TempDir()
	factory := NewCodexHostFactory(Layout{SessionsDir: root})
	prepared := &CodexPreparedLaunch{}
	prior := &ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex-fork", CodexFork: &CodexContextForkPlan{Contract: codexContextForkPlanContract, PrimaryCtxID: "context-opaque"}}}
	_, err := factory.Launch(context.Background(), ProviderLaunchRequest{SessionID: "child", Launch: LaunchSpec{Codex: prepared}, Prior: prior, PriorMustFork: true})
	if !errors.Is(err, ErrHostCapabilityUnavailable) {
		t.Fatalf("missing context policy admitted: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("refusal left effects: %v %v", entries, err)
	}
}
