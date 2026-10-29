package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func codexBudgetConstructorPointer(value int64) *int64 { return &value }

// The actual preparation used by Launch selects the launch value rather than
// a distinct nested value, even for nil/zero, and owns its returned pointer.
// It stops before provider execution and makes no goal-application claim.
func TestCodexBudgetConstructorSelectsLaunchValue(t *testing.T) {
	for _, selected := range []*int64{nil, codexBudgetConstructorPointer(0), codexBudgetConstructorPointer(1), codexBudgetConstructorPointer(7654321), codexBudgetConstructorPointer(math.MaxInt64)} {
		name := "absent"
		if selected != nil {
			name = fmt.Sprint(*selected)
		}
		t.Run(name, func(t *testing.T) {
			prepared := &CodexPreparedLaunch{GoalTokenBudget: selected, Snapshot: CodexRunnerSnapshot{ThreadID: "fixture-thread", GoalTokenBudget: codexBudgetConstructorPointer(41), HostArgv: []string{"app-server"}}}
			got, err := prepareCodexLaunchSnapshot(prepared)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.GoalTokenBudget, selected) || got.ThreadID != "fixture-thread" || !reflect.DeepEqual(got.HostArgv, []string{"app-server"}) {
				t.Fatalf("constructor lost launch selection or snapshot facts: %#v", got)
			}
			if selected != nil {
				before := *selected
				if got.GoalTokenBudget == selected || got.GoalTokenBudget == prepared.Snapshot.GoalTokenBudget {
					t.Fatal("constructor retained caller budget pointer")
				}
				*got.GoalTokenBudget = 99
				if *selected != before || *prepared.Snapshot.GoalTokenBudget != 41 {
					t.Fatal("constructor mutation reached selected or nested source")
				}
				*selected = 73
				if *got.GoalTokenBudget != 99 {
					t.Fatal("caller mutation reached constructor budget")
				}
			}
			got.HostArgv[0] = "changed"
			if prepared.Snapshot.HostArgv[0] != "app-server" || *prepared.Snapshot.GoalTokenBudget != 41 {
				t.Fatal("constructor retained nested mutable source")
			}
		})
	}
}

// Nil preparation and selected negative values refuse with the original typed
// capability error and zero output; the caller's historical snapshot is intact.
// Nonnegative preparation is covered by the neighboring selection test.
func TestCodexBudgetConstructorRefusesInvalidPreparation(t *testing.T) {
	for _, prepared := range []*CodexPreparedLaunch{nil, {GoalTokenBudget: codexBudgetConstructorPointer(-1), Snapshot: CodexRunnerSnapshot{GoalTokenBudget: codexBudgetConstructorPointer(41)}}, {GoalTokenBudget: codexBudgetConstructorPointer(math.MinInt64)}} {
		before, err := json.Marshal(prepared)
		if err != nil {
			t.Fatal(err)
		}
		got, err := prepareCodexLaunchSnapshot(prepared)
		after, marshalErr := json.Marshal(prepared)
		if !errors.Is(err, ErrHostCapabilityUnavailable) || !reflect.DeepEqual(got, CodexRunnerSnapshot{}) || marshalErr != nil || !bytes.Equal(before, after) {
			t.Fatalf("invalid preparation did not refuse without partial mutation: %#v / %v / %v", got, err, marshalErr)
		}
	}
}

// Real Launch rejects a selected negative value before plan validation or any
// session/home/log/backend effect. Valid nil/zero/positive preparations reach
// the existing required-environment gate instead; no provider is launched.
func TestCodexBudgetConstructorFactoryRefusesBeforeEffects(t *testing.T) {
	root := t.TempDir()
	const missing = "SESSIONHOST_CODEX_BUDGET_CONSTRUCTOR_TEST_MISSING"
	t.Setenv(missing, "temporary")
	if err := os.Unsetenv(missing); err != nil {
		t.Fatal(err)
	}
	factory := NewCodexHostFactory(Layout{SessionsDir: filepath.Join(root, "sessions")})
	for _, selected := range []*int64{codexBudgetConstructorPointer(-1), nil, codexBudgetConstructorPointer(0), codexBudgetConstructorPointer(7654321)} {
		plan := PrimarySessionLaunchPlan{Contract: PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "codex", ProjectDir: root, Executable: filepath.Join(root, "never-executed"), RequiredEnvNames: []string{missing}, LaunchVariants: LaunchVariants{ManagedHost: LaunchArgv{Kind: "codex-app-server", Argv: []string{"app-server"}}}}
		prepared := &CodexPreparedLaunch{GoalTokenBudget: selected, Plan: plan, Snapshot: CodexRunnerSnapshot{GoalTokenBudget: codexBudgetConstructorPointer(41)}}
		// Invalid plan makes the negative case also prove preparation ordering.
		if selected != nil && *selected < 0 {
			prepared.Plan.Contract = "invalid"
		}
		launch := LaunchSpec{Codex: prepared, Executable: plan.Executable, Cwd: root}
		before, err := json.Marshal(launch)
		if err != nil {
			t.Fatal(err)
		}
		host, err := factory.Launch(context.Background(), ProviderLaunchRequest{SessionID: "fixture-session", Launch: launch})
		if host != nil {
			t.Fatal("refused preparation returned a runner")
		}
		if selected != nil && *selected < 0 {
			if !errors.Is(err, ErrHostCapabilityUnavailable) {
				t.Fatalf("negative budget passed preparation: %v", err)
			}
		} else if err == nil || err.Error() != "required launch environment "+missing+" is not set" {
			t.Fatalf("valid preparation did not reach existing environment gate: %v", err)
		}
		after, marshalErr := json.Marshal(launch)
		entries, readErr := os.ReadDir(root)
		if marshalErr != nil || !bytes.Equal(before, after) || readErr != nil || len(entries) != 0 {
			t.Fatalf("refusal changed caller or filesystem: %v / %v / %v", marshalErr, entries, readErr)
		}
	}
}
