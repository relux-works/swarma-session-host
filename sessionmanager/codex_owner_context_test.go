package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func codexOwnerFactFixture() *OwnerRunContext {
	return &OwnerRunContext{RunID: "RUN-fixture", TaskID: "TASK-fictional-owner", BoardDir: "/fixture/board", ContextCtxID: "CTX-fixture", DeliveryGoalID: "GOAL-fictional", RuntimeControl: json.RawMessage(`{"project_root":"/fixture/project","config_path":"/fixture/config/settings.json","config_root":"/fixture/config","config_explicit":true,"runtime_root":"/fixture/runtime","control_root":"/fixture/control","repository_binding":{"remote_name":"origin","canonical_remote_url":"https://example.invalid/fixture/repo","source":"fixture","binding_sha256":"fictional-digest"},"future":{"retained":true}}`)}
}

func codexOwnerFactImage(t *testing.T, owner *OwnerRunContext) LegacyRecordInput {
	t.Helper()
	rawOwner, err := json.Marshal(owner)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"contract":"task-board-session-record-v3","session_id":"SES-cccccccccccccccccccccccccccccccc","owner":{"kind":"run","id":"RUN-fixture"},"provider":"codex","state":"stopped","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","future_record":{"retained":true},"provider_host":{"provider":"codex","opaque":{"contract":"task-board.codex-provider-host-v1","pid":42,"url":"ws://127.0.0.1:1","thread_id":"fictional-thread","project_dir":"/fixture/project","codex_home":"/fixture/home","stopped":true,"future_native":{"retained":true},"owner_run":` + string(rawOwner) + `}}}`)
	return legacyRecordImage("SES-cccccccccccccccccccccccccccccccc", raw)
}

// Fresh selection includes nil, even over nested metadata; all original fields
// and nested runtime-control bytes detach across preparation/launch/snapshot.
func TestCodexOwnerRunSelectionAndCopies(t *testing.T) {
	prior := &CodexRunnerSnapshot{OwnerRun: codexOwnerFactFixture()}
	prior.OwnerRun.RunID = "RUN-retained"
	for _, selected := range []*OwnerRunContext{nil, codexOwnerFactFixture()} {
		got, err := prepareCodexOwnerRun(&CodexPreparedLaunch{OwnerRun: selected}, prior)
		want := selected
		if want == nil {
			want = prior.OwnerRun
		}
		if err != nil || !reflect.DeepEqual(got, want) || got == want {
			t.Fatalf("retained selection changed or aliases: %#v / %v", got, err)
		}
		got.RuntimeControl[0] = ' '
		if want.RuntimeControl[0] != '{' {
			t.Fatal("retained selection aliases original")
		}
	}
	for _, selected := range []*OwnerRunContext{nil, codexOwnerFactFixture()} {
		p := &CodexPreparedLaunch{OwnerRun: selected, Snapshot: CodexRunnerSnapshot{OwnerRun: codexOwnerFactFixture()}}
		got, err := prepareCodexLaunchSnapshot(p)
		if err != nil || !reflect.DeepEqual(got.OwnerRun, selected) {
			t.Fatalf("fresh selection changed: %#v / %v", got.OwnerRun, err)
		}
		if selected == nil {
			continue
		}
		launch := copyLaunchSpec(LaunchSpec{Codex: p})
		snapshot := copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &got})
		original := append([]byte(nil), selected.RuntimeControl...)
		got.OwnerRun.RunID = "RUN-result"
		got.OwnerRun.RuntimeControl[0] = ' '
		launch.Codex.OwnerRun.TaskID = "changed-copy"
		launch.Codex.OwnerRun.RuntimeControl[0] = ' '
		snapshot.Codex.OwnerRun.ContextCtxID = "CTX-copy"
		snapshot.Codex.OwnerRun.RuntimeControl[0] = ' '
		if selected.RunID != "RUN-fixture" || selected.TaskID != "TASK-fictional-owner" || selected.ContextCtxID != "CTX-fixture" || !bytes.Equal(selected.RuntimeControl, original) || !bytes.Equal(p.Snapshot.OwnerRun.RuntimeControl, original) {
			t.Fatal("copy mutation reached selected/nested original")
		}
		selected.DeliveryGoalID = "changed-original"
		selected.RuntimeControl[1] = ' '
		if got.OwnerRun.DeliveryGoalID != "GOAL-fictional" || launch.Codex.OwnerRun.DeliveryGoalID != "GOAL-fictional" || snapshot.Codex.OwnerRun.DeliveryGoalID != "GOAL-fictional" || got.OwnerRun.RuntimeControl[1] != '"' || launch.Codex.OwnerRun.RuntimeControl[1] != '"' || snapshot.Codex.OwnerRun.RuntimeControl[1] != '"' {
			t.Fatal("original mutation reached detached copies")
		}
	}
}

// Owner/plane syntax matches the original context-only or goal-only rule. The
// runtime-control object is retained DATA; these checks grant no live authority.
func TestCodexOwnerRunShapeRefusals(t *testing.T) {
	for _, name := range []string{"run", "task", "context", "no-plane", "runtime-control", "malformed-json"} {
		t.Run(name, func(t *testing.T) {
			owner := codexOwnerFactFixture()
			switch name {
			case "run":
				owner.RunID = "unowned"
			case "task":
				owner.TaskID = " "
			case "context":
				owner.ContextCtxID = "foreign"
			case "no-plane":
				owner.ContextCtxID, owner.DeliveryGoalID = "", ""
			case "runtime-control":
				owner.RuntimeControl = json.RawMessage(`[]`)
			case "malformed-json":
				owner.RuntimeControl = json.RawMessage(`{`)
			}
			if _, err := prepareCodexLaunchSnapshot(&CodexPreparedLaunch{OwnerRun: owner}); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("malformed source was not refused: %v", err)
			}
		})
	}
	for _, goalPlane := range []bool{false, true} {
		owner := codexOwnerFactFixture()
		owner.RuntimeControl = nil
		if goalPlane {
			owner.ContextCtxID = ""
		} else {
			owner.DeliveryGoalID = ""
		}
		if _, err := prepareCodexLaunchSnapshot(&CodexPreparedLaunch{OwnerRun: owner}); err != nil {
			t.Fatalf("original single-plane neighbor refused: %v", err)
		}
	}
}

// The actual codec carries full owner metadata/unknown fields in archives,
// refuses live admission, and never drops an unencodable runtime-control value.
func TestCodexOwnerRunArchiveAndLiveRefusal(t *testing.T) {
	ctx, codec := context.Background(), NewLegacyRecordCodec()
	ordinary := codexOwnerFactImage(t, nil)
	plain, err := codec.DecodeLegacy(ctx, ordinary)
	if err != nil || plain.ProviderHost.Snapshot.Codex.OwnerRun != nil {
		t.Fatal("run label synthesized absent owner fact", err)
	}
	plain.State = SessionRunning
	plain.ProviderHost.Snapshot.Codex.Stopped = false
	if _, err := codec.EncodeMerge(ctx, ordinary, plain); err != nil {
		t.Fatal("goalless ordinary live neighbor refused", err)
	}
	in := codexOwnerFactImage(t, codexOwnerFactFixture())
	record, err := codec.DecodeLegacy(ctx, in)
	if err != nil || !reflect.DeepEqual(record.ProviderHost.Snapshot.Codex.OwnerRun, codexOwnerFactFixture()) {
		t.Fatalf("actual archive projection lost owner source: %#v / %v", record.ProviderHost.Snapshot.Codex, err)
	}
	unchanged, err := codec.EncodeMerge(ctx, in, record)
	if err != nil || !bytes.Equal(unchanged, in.Bytes) {
		t.Fatalf("unchanged archive bytes changed: %v", err)
	}
	record.ProviderHost.Snapshot.Codex.OwnerRun.TaskID = "changed-fictional-task"
	changed, err := codec.EncodeMerge(ctx, in, record)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"future_record"`, `"future_native"`, `"future"`, `"repository_binding"`, `changed-fictional-task`} {
		if !bytes.Contains(changed, []byte(field)) {
			t.Fatalf("owner merge lost %s", field)
		}
	}
	record.State = SessionRunning
	if raw, err := codec.EncodeMerge(ctx, in, record); !errors.Is(err, ErrHostCapabilityUnavailable) || raw != nil {
		t.Fatalf("metadata granted live owner capability: %s / %v", raw, err)
	}
	record.State = SessionStopped
	record.ProviderHost.Snapshot.Codex.OwnerRun.RuntimeControl = json.RawMessage(`{`)
	if raw, err := codec.EncodeMerge(ctx, in, record); err == nil || raw != nil {
		t.Fatal("malformed runtime-control was silently dropped")
	}
}

// Actual factory selection retains the prior on nil and refuses metadata before
// any files/native effects while its raising consumer is missing. A truly
// goal-less fresh launch reaches the existing required-environment boundary.
func TestCodexOwnerRunFactoryRefusesBeforeEffects(t *testing.T) {
	root := t.TempDir()
	const missing = "SESSIONHOST_OWNER_FACT_TEST_MISSING"
	t.Setenv(missing, "temporary")
	if err := os.Unsetenv(missing); err != nil {
		t.Fatal(err)
	}
	codec := NewLegacyRecordCodec()
	plan := PrimarySessionLaunchPlan{Contract: PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "codex", ProjectDir: root, Executable: filepath.Join(root, "never-executed"), RequiredEnvNames: []string{missing}, LaunchVariants: LaunchVariants{ManagedHost: LaunchArgv{Kind: "codex-app-server", Argv: []string{"app-server"}}}}
	for _, priorOwner := range []*OwnerRunContext{nil, codexOwnerFactFixture()} {
		for _, selected := range []*OwnerRunContext{nil, codexOwnerFactFixture()} {
			prior, err := codec.DecodeLegacy(context.Background(), codexOwnerFactImage(t, priorOwner))
			if err != nil {
				t.Fatal(err)
			}
			q := ProviderLaunchRequest{SessionID: prior.SessionID, Prior: &prior.ProviderHost, Launch: LaunchSpec{Executable: plan.Executable, Cwd: root, Codex: &CodexPreparedLaunch{Plan: plan, OwnerRun: selected}}}
			before, _ := json.Marshal(q)
			runner, err := NewCodexHostFactory(Layout{SessionsDir: filepath.Join(root, "sessions")}).Launch(context.Background(), q)
			if runner != nil {
				t.Fatal("refusal returned native runner")
			}
			if selected != nil || priorOwner != nil {
				if !errors.Is(err, ErrHostCapabilityUnavailable) {
					t.Fatalf("owner source passed missing consumer: %v", err)
				}
			} else if err == nil || err.Error() != "required launch environment "+missing+" is not set" {
				t.Fatalf("ordinary neighbor did not reach environment boundary: %v", err)
			}
			after, _ := json.Marshal(q)
			entries, readErr := os.ReadDir(root)
			if !bytes.Equal(before, after) || readErr != nil || len(entries) != 0 {
				t.Fatalf("preflight mutated source/files: %v / %v", entries, readErr)
			}
		}
	}
}

// Checked projection/merge preserve old unknown owner/runtime fields and nil
// retained selection. Malformed selected JSON returns its real marshal error,
// with no output or source mutation. Actual codec wiring is qualified separately.
func TestCodexOwnerRunCheckedArchivePreservesUnknown(t *testing.T) {
	var wire legacyRunnerRecord
	if err := json.Unmarshal(codexOwnerFactImage(t, codexOwnerFactFixture()).Bytes, &wire); err != nil {
		t.Fatal(err)
	}
	var prior legacyCodexSnapshot
	if err := json.Unmarshal(wire.ProviderHost.Opaque, &prior); err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(prior.OwnerRun, &raw); err != nil {
		t.Fatal(err)
	}
	raw["future_owner"] = json.RawMessage(`{"retained":[1,"unknown"]}`)
	var err error
	prior.OwnerRun, err = json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), prior.OwnerRun...)
	projected, err := projectCodexSnapshotWithOwner(prior)
	if err != nil || !reflect.DeepEqual(projected.OwnerRun, codexOwnerFactFixture()) {
		t.Fatal("checked projection changed original fields", err)
	}
	merged, err := mergeCodexSnapshotWithOwner(prior, projected)
	if err != nil || !bytes.Equal(merged.OwnerRun, original) {
		t.Fatal("unchanged projection lost exact unknown bytes", err)
	}
	merged.OwnerRun[0] = ' '
	if !bytes.Equal(prior.OwnerRun, original) {
		t.Fatal("merged raw metadata aliases source")
	}
	projected.OwnerRun.TaskID = "changed-fictional-task"
	merged, err = mergeCodexSnapshotWithOwner(prior, projected)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"future_owner"`, `"future"`, `"repository_binding"`, `changed-fictional-task`} {
		if !bytes.Contains(merged.OwnerRun, []byte(field)) {
			t.Fatalf("metadata merge lost %s", field)
		}
	}
	var after map[string]json.RawMessage
	if err := json.Unmarshal(merged.OwnerRun, &after); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(compactLegacyJSON(after["future_owner"]), compactLegacyJSON(raw["future_owner"])) || !bytes.Equal(compactLegacyJSON(after["runtime_control"]), compactLegacyJSON(codexOwnerFactFixture().RuntimeControl)) {
		t.Fatal("owner merge changed an unknown value or complete runtime-control fact")
	}
	projected.OwnerRun = nil
	merged, err = mergeCodexSnapshotWithOwner(prior, projected)
	if err != nil || !bytes.Equal(merged.OwnerRun, original) {
		t.Fatal("nil retained selection erased prior owner source", err)
	}
	projected.OwnerRun = codexOwnerFactFixture()
	projected.OwnerRun.RuntimeControl = json.RawMessage(`{`)
	merged, err = mergeCodexSnapshotWithOwner(prior, projected)
	var marshal *json.MarshalerError
	if !errors.As(err, &marshal) || !reflect.DeepEqual(merged, legacyCodexSnapshot{}) || !bytes.Equal(prior.OwnerRun, original) {
		t.Fatalf("malformed JSON did not refuse without output: %v", err)
	}
	prior.OwnerRun = json.RawMessage(`{"run_id":7}`)
	if _, err := projectCodexSnapshotWithOwner(prior); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("malformed original source accepted: %v", err)
	}
}
