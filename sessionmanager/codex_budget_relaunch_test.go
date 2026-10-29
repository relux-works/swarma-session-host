package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The actual provider factory rejects every explicit archived selection before
// environment, session/home/log or native effects, even if the new selection is
// nil/zero/positive. A goal-less prior reaches the existing environment gate.
func TestCodexBudgetRelaunchFactoryRefusesBeforeEffects(t *testing.T) {
	root := t.TempDir()
	const missing = "SESSIONHOST_CODEX_RELAUNCH_TEST_MISSING"
	t.Setenv(missing, "temporary")
	if err := os.Unsetenv(missing); err != nil {
		t.Fatal(err)
	}
	codec := NewLegacyRecordCodec()
	factory := NewCodexHostFactory(Layout{SessionsDir: filepath.Join(root, "sessions")})
	for _, archived := range []string{"", "0", "7654321", "-1"} {
		for _, selected := range []*int64{nil, codexBudgetConstructorPointer(0), codexBudgetConstructorPointer(73)} {
			name := archived + "/absent"
			if selected != nil {
				name = archived + "/" + fmt.Sprint(*selected)
			}
			t.Run(name, func(t *testing.T) {
				in := reviewBatchQStoppedCodexImage(archived)
				prior, err := codec.DecodeLegacy(context.Background(), in)
				if err != nil {
					t.Fatal(err)
				}
				plan := PrimarySessionLaunchPlan{Contract: PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "codex", ProjectDir: root, Executable: filepath.Join(root, "never-executed"), RequiredEnvNames: []string{missing}, LaunchVariants: LaunchVariants{ManagedHost: LaunchArgv{Kind: "codex-app-server", Argv: []string{"app-server"}}}}
				q := ProviderLaunchRequest{SessionID: in.SessionID, Prior: &prior.ProviderHost, Launch: LaunchSpec{Executable: plan.Executable, Cwd: root, Codex: &CodexPreparedLaunch{Plan: plan, GoalTokenBudget: selected}}}
				before, err := json.Marshal(q)
				if err != nil {
					t.Fatal(err)
				}
				host, err := factory.Launch(context.Background(), q)
				if host != nil {
					t.Fatal("preflight refusal returned a runner")
				}
				if archived != "" {
					if !errors.Is(err, ErrHostCapabilityUnavailable) {
						t.Fatalf("archived selection passed preflight: %v", err)
					}
				} else if err == nil || err.Error() != "required launch environment "+missing+" is not set" {
					t.Fatalf("goal-less prior did not reach valid preparation neighbor: %v", err)
				}
				after, marshalErr := json.Marshal(q)
				entries, readErr := os.ReadDir(root)
				if marshalErr != nil || !bytes.Equal(before, after) || readErr != nil || len(entries) != 0 {
					t.Fatalf("preflight changed input or filesystem: %v / %v / %v", marshalErr, entries, readErr)
				}
			})
		}
	}
}

// Prior data remains explicit both for ordinary and admitted fork priors. This
// pure refusal grants no fork/materialization capability and never mutates data.
func TestCodexBudgetRelaunchPriorCannotBeReset(t *testing.T) {
	for _, kind := range []string{"codex", "codex-fork"} {
		for _, budget := range []*int64{nil, codexBudgetConstructorPointer(0), codexBudgetConstructorPointer(7654321)} {
			snapshot := &CodexRunnerSnapshot{GoalTokenBudget: budget}
			prior := ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: snapshot}}
			if kind == "codex-fork" {
				prior = ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex-fork", CodexFork: &CodexContextForkPlan{Primary: prior}}}
			}
			before, err := json.Marshal(prior)
			if err != nil {
				t.Fatal(err)
			}
			err = validateCodexBudgetRelaunch(prior)
			if budget == nil && err != nil || budget != nil && !errors.Is(err, ErrHostCapabilityUnavailable) {
				t.Fatalf("prior budget refusal lost optional distinction: %s / %v / %v", kind, budget, err)
			}
			after, marshalErr := json.Marshal(prior)
			if marshalErr != nil || !bytes.Equal(before, after) {
				t.Fatal("prior preflight mutated caller")
			}
		}
	}
}

// Canonical state governs archive edits even when the native Stopped flag
// contradicts it. Explicit prior values cannot disappear into running/recovering
// state; a stopped archive may intentionally clear them. Both the pure guard
// and the actual codec must preserve this boundary without mutating inputs.
func TestCodexBudgetRelaunchCanonicalStateBoundary(t *testing.T) {
	for _, row := range []struct {
		name     string
		archived string
		state    SessionState
		stopped  bool
		selected *int64
		refuse   bool
	}{
		{"goal-less-live", "", SessionRunning, false, nil, false},
		{"clear-stopped-archive", "7654321", SessionStopped, true, nil, false},
		{"clear-archive-with-native-running-flag", "7654321", SessionStopped, false, nil, false},
		{"positive-archive-to-live", "7654321", SessionRunning, false, nil, true},
		{"zero-archive-to-live-with-native-stopped-flag", "0", SessionRunning, true, nil, true},
		{"negative-archive-to-recovering", "-1", SessionRecovering, true, nil, true},
		{"override-is-not-a-consumer", "7654321", SessionRunning, false, codexBudgetConstructorPointer(73), true},
	} {
		t.Run(row.name, func(t *testing.T) {
			in := reviewBatchQStoppedCodexImage(row.archived)
			var wire legacyRunnerRecord
			if err := json.Unmarshal(in.Bytes, &wire); err != nil {
				t.Fatal(err)
			}
			next := RunnerRecord{State: row.state, ProviderHost: ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &CodexRunnerSnapshot{Stopped: row.stopped, GoalTokenBudget: row.selected}}}}
			before, err := json.Marshal(next)
			if err != nil {
				t.Fatal(err)
			}
			priorBytes := append([]byte(nil), wire.ProviderHost.Opaque...)
			err = validateCodexBudgetTransition(&wire.ProviderHost, next)
			if row.refuse && !errors.Is(err, ErrHostCapabilityUnavailable) || !row.refuse && err != nil {
				t.Fatalf("canonical archive/live boundary changed: %v", err)
			}
			after, marshalErr := json.Marshal(next)
			if marshalErr != nil || !bytes.Equal(before, after) || !bytes.Equal(priorBytes, wire.ProviderHost.Opaque) {
				t.Fatal("transition guard mutated prior or next image")
			}
			codec := NewLegacyRecordCodec()
			record, err := codec.DecodeLegacy(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			record.State = row.state
			record.ProviderHost.Snapshot.Codex.Stopped = row.stopped
			record.ProviderHost.Snapshot.Codex.GoalTokenBudget = copyCodexGoalTokenBudget(row.selected)
			before, err = json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			rawBefore := append([]byte(nil), in.Bytes...)
			raw, err := codec.EncodeMerge(context.Background(), in, record)
			if row.refuse {
				if !errors.Is(err, ErrHostCapabilityUnavailable) || raw != nil {
					t.Fatalf("codec admitted unsupported transition: %s / %v", raw, err)
				}
			} else if err != nil {
				t.Fatalf("codec refused valid archive/goal-less neighbor: %v", err)
			}
			after, marshalErr = json.Marshal(record)
			if marshalErr != nil || !bytes.Equal(before, after) || !bytes.Equal(rawBefore, in.Bytes) {
				t.Fatal("codec transition mutated prior or next image")
			}
		})
	}
	if err := validateCodexBudgetTransition(nil, RunnerRecord{State: SessionRunning}); err != nil {
		t.Fatal("absent prior manufactured historical state", err)
	}
	if err := validateCodexBudgetRelaunch(ProviderHostRecord{Provider: "claude"}); err != nil {
		t.Fatal("Codex-only guard rejected another provider", err)
	}
}
