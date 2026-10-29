//go:build unix

package hostdefaults

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

func legacyPlan(t *testing.T, provider string) core.PrimarySessionLaunchPlan {
	t.Helper()
	project := t.TempDir()
	kind := "codex-app-server"
	argv := []string{"app-server"}
	if provider == "claude" {
		kind = "claude-pty"
		argv = nil
	}
	return core.PrimarySessionLaunchPlan{Contract: core.PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: provider, ProjectDir: project, Executable: filepath.Join(project, provider), LaunchVariants: core.LaunchVariants{ManagedHost: core.LaunchArgv{Kind: kind, Argv: argv}}}
}

// Every original outer field and every supported typed plan/geometry/selector
// field survives the real conversion. The old full budget/runtime-control data
// are retained by parsing and refused if their actual consumer is missing.
func TestLegacyAttachCompleteWireAndTypedPreparation(t *testing.T) {
	for _, provider := range []string{"codex", "claude"} {
		t.Run(provider, func(t *testing.T) {
			plan := legacyPlan(t, provider)
			plan.Producer = core.LaunchPlanProducer{Version: "fixture", Commit: "source"}
			plan.RequiredEnvNames = []string{"FIXTURE_MODE"}
			plan.Resolved.Model = core.ResolvedLaunchValue{Value: "fixture-model", Source: "operator"}
			var envelope any
			if provider == "codex" {
				envelope = LegacyCodexSessionLaunch{Contract: LegacyCodexSessionLaunchContract, Plan: plan, ChildEnv: map[string]string{"FIXTURE_LABEL": "literal"}, AutoContinue: core.CodexAutoContinueOptions{Enabled: true, DelaySeconds: 2, MaxNudgesPerTurn: 3, Prompt: "continue"}, ResumeThreadID: "019f8e50-1234-7abc-8def-0123456789ab"}
			} else {
				envelope = LegacyClaudeSessionLaunch{Contract: core.ClaudeSessionLaunchContract, Plan: plan, Rows: 37, Cols: 111, Term: "xterm-256color", ResumeSessionUUID: "123e4567-e89b-42d3-a456-426614174000"}
			}
			launch, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			r := LegacyAttachRequest{Owner: core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}, Provider: provider, Launch: launch, ProjectDir: plan.ProjectDir, BoardFingerprint: "location", Resume: true, ResumeSessionID: "SES-original", Adopt: false, MaterializationCtxID: "CTX-261029-c00001"}
			raw, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeLegacyAttachRequest(raw)
			if err != nil || !reflect.DeepEqual(decoded.Request, r) {
				t.Fatalf("full outer wire lost: %+v %v", decoded.Request, err)
			}
			env := []string{"FIXTURE_MODE=present"}
			q, err := PrepareLegacyAttach(r, env)
			if err != nil {
				t.Fatal(err)
			}
			if q.Owner != "parent/primary" || q.Provider != provider || q.ProjectDir != r.ProjectDir || q.NamespaceFingerprint != r.BoardFingerprint || q.Resume != r.Resume || q.ResumeSessionID != r.ResumeSessionID || q.Adopt != r.Adopt || q.MaterializationCtxID != r.MaterializationCtxID || q.Launch.Executable != plan.Executable || q.Launch.Cwd != plan.ProjectDir || !reflect.DeepEqual(q.Launch.Environment, env) {
				t.Fatalf("actual outer/launch conversion lost: %+v", q)
			}
			if provider == "codex" {
				if q.Launch.Codex == nil || !reflect.DeepEqual(q.Launch.Codex.Plan, plan) || q.Launch.Codex.ResumeThreadID != decoded.Codex.ResumeThreadID || q.Launch.Codex.AutoContinue != decoded.Codex.AutoContinue || !reflect.DeepEqual(q.Launch.Codex.ChildEnv, decoded.Codex.ChildEnv) {
					t.Fatal("Codex full plan/options lost")
				}
			} else {
				if q.Launch.Claude == nil || !reflect.DeepEqual(q.Launch.Claude.Plan, plan) || q.Launch.Claude.Rows != 37 || q.Launch.Claude.Cols != 111 || q.Launch.Claude.Term != "xterm-256color" || q.Launch.Claude.ResumeSessionUUID != decoded.Claude.ResumeSessionUUID {
					t.Fatal("Claude full plan/geometry/selector lost")
				}
			}
			env[0] = "caller-mutated"
			if q.Launch.Environment[0] != "FIXTURE_MODE=present" {
				t.Fatal("prepared environment aliases caller")
			}
		})
	}
}

// Missing native-budget or owner authority consumers cannot silently erase
// their legacy fields. Invalid plans/overlays/shapes fail without I/O.
func TestLegacyAttachMissingConsumersAndInvalidWireRefuse(t *testing.T) {
	plan := legacyPlan(t, "codex")
	budget := int64(123)
	launch := LegacyCodexSessionLaunch{Contract: LegacyCodexSessionLaunchContract, Plan: plan, GoalTokenBudget: &budget}
	b, _ := json.Marshal(launch)
	r := LegacyAttachRequest{Owner: core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}, Provider: "codex", Launch: b}
	raw, _ := json.Marshal(r)
	parsed, err := DecodeLegacyAttachRequest(raw)
	if err != nil || parsed.Codex.GoalTokenBudget == nil || *parsed.Codex.GoalTokenBudget != budget {
		t.Fatal("original token budget discarded")
	}
	if _, err := PrepareLegacyAttach(r, nil); !errors.Is(err, ErrLegacyCodexBudgetConsumerRequired) {
		t.Fatalf("budget silently ignored: %v", err)
	}
	launch.GoalTokenBudget = nil
	launch.OwnerRun = &LegacyOwnerRunContext{RunID: "RUN-original", TaskID: "TASK-original", ContextCtxID: "CTX-original", RuntimeControl: json.RawMessage(`{"untrusted":"retained-completely"}`)}
	r.Launch, _ = json.Marshal(launch)
	raw, _ = json.Marshal(r)
	parsed, err = DecodeLegacyAttachRequest(raw)
	if err != nil || string(parsed.Codex.OwnerRun.RuntimeControl) != string(launch.OwnerRun.RuntimeControl) {
		t.Fatal("runtime-control source lost")
	}
	if _, err := PrepareLegacyAttach(r, nil); !errors.Is(err, ErrLegacyOwnerEnvironmentRequired) {
		t.Fatalf("owner body granted runtime control: %v", err)
	}
	for _, body := range []string{`{"owner":{"kind":"parent","id":"primary"},"provider":"codex","unknown":true}`, `{"owner":{"kind":"agent","id":"primary"},"provider":"codex"}`, `{"owner":{"kind":"parent","id":"primary"},"provider":"other"}`, `{"owner":`, strings.Repeat(" ", 1<<20) + "{}"} {
		if _, err := DecodeLegacyAttachRequest([]byte(body)); err == nil {
			t.Fatal("invalid outer request accepted")
		}
	}
	launch.OwnerRun = nil
	launch.ChildEnv = map[string]string{"CODEX_HOME": "/fixture/forbidden"}
	r.Launch, _ = json.Marshal(launch)
	if _, err := PrepareLegacyAttach(r, nil); err == nil {
		t.Fatal("reserved child environment accepted")
	}
	launch.ChildEnv = nil
	launch.Plan.Provider = "claude"
	r.Launch, _ = json.Marshal(launch)
	if _, err := PrepareLegacyAttach(r, nil); err == nil {
		t.Fatal("foreign provider plan accepted")
	}
}

// Original launch_plan_test.go witness: uppercase UUIDs normalize and every
// original malformed selector receives the original public refusal code.
func TestDecodeCodexSessionLaunchNormalizesAndRejectsResumeThreadID(t *testing.T) {
	plan := legacyPlan(t, "codex")
	encode := func(id string) []byte {
		b, err := json.Marshal(LegacyCodexSessionLaunch{Contract: LegacyCodexSessionLaunchContract, Plan: plan, ResumeThreadID: id})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	const id = "019f8e50-1234-7abc-8def-0123456789ab"
	got, err := DecodeLegacyCodexSessionLaunch(encode(strings.ToUpper(id)))
	if err != nil {
		t.Fatal(err)
	}
	if got.ResumeThreadID != id {
		t.Fatalf("normalized resume thread ID = %q, want %q", got.ResumeThreadID, id)
	}
	for _, invalid := range []string{"not-a-thread", "019f8e5012347abc8def0123456789ab", "--last", id + " prompt"} {
		_, err := DecodeLegacyCodexSessionLaunch(encode(invalid))
		var public *core.PublicError
		if !errors.As(err, &public) || public.Code != "codex_thread_adoption_invalid" {
			t.Fatalf("decode(%q) error = %v, want public adoption refusal", invalid, err)
		}
	}
}

// Original adoption_test.go witness, using its real plan shape without the
// unused executable provider fixture: all original selector assertions remain.
func TestDecodeClaudeSessionLaunchNormalizesAndRejectsResumeIdentity(t *testing.T) {
	plan := legacyPlan(t, "claude")
	encode := func(id string) []byte {
		b, err := json.Marshal(LegacyClaudeSessionLaunch{Contract: core.ClaudeSessionLaunchContract, Plan: plan, ResumeSessionUUID: id})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	got, err := DecodeLegacyClaudeSessionLaunch(encode(" 123E4567-E89B-42D3-A456-426614174000 "))
	if err != nil {
		t.Fatal(err)
	}
	if got.ResumeSessionUUID != "123e4567-e89b-42d3-a456-426614174000" {
		t.Fatalf("normalized resume identity = %q", got.ResumeSessionUUID)
	}
	for _, invalid := range []string{"not-a-uuid", "123e4567e89b42d3a456426614174000", "--resume", "123e4567-e89b-42d3-a456-42661417400g", "123e4567-e89b-42d3-a456-426614174000 extra prompt text"} {
		_, err := DecodeLegacyClaudeSessionLaunch(encode(invalid))
		var public *core.PublicError
		if !errors.As(err, &public) || public.Code != core.ClaudeErrorAdoptionInvalid {
			t.Fatalf("decode(%q) error = %v, want public adoption refusal", invalid, err)
		}
	}
}

// Original manager.Attach normalizes a provider before selecting its factory.
// Preserve the complete wire spelling while the typed request reaches the exact
// normalized core contract; malformed or unsupported providers still refuse.
func TestLegacyAttachProviderNormalizationPreservesWireAndTypedPlan(t *testing.T) {
	for _, provider := range []string{"codex", "claude"} {
		plan := legacyPlan(t, provider)
		var launch any = LegacyCodexSessionLaunch{Contract: LegacyCodexSessionLaunchContract, Plan: plan}
		if provider == "claude" {
			launch = LegacyClaudeSessionLaunch{Contract: core.ClaudeSessionLaunchContract, Plan: plan}
		}
		raw, err := json.Marshal(launch)
		if err != nil {
			t.Fatal(err)
		}
		for _, spelling := range []string{provider, "  " + strings.ToUpper(provider) + " ", strings.ToUpper(provider[:1]) + provider[1:]} {
			request := LegacyAttachRequest{Owner: core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}, Provider: spelling, Launch: raw}
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := DecodeLegacyAttachRequest(body)
			if err != nil || parsed.Request.Provider != spelling {
				t.Fatalf("original provider spelling lost/refused: %q %+v %v", spelling, parsed.Request, err)
			}
			prepared, err := PrepareLegacyAttach(request, nil)
			if err != nil || prepared.Provider != provider {
				t.Fatalf("provider not normalized for actual core: %q %+v %v", spelling, prepared, err)
			}
			if provider == "codex" && (prepared.Launch.Codex == nil || !reflect.DeepEqual(prepared.Launch.Codex.Plan, plan)) {
				t.Fatal("normalized Codex plan lost")
			}
			if provider == "claude" && (prepared.Launch.Claude == nil || !reflect.DeepEqual(prepared.Launch.Claude.Plan, plan)) {
				t.Fatal("normalized Claude plan lost")
			}
		}
	}
	for _, provider := range []string{" ", "co dex", "codex/", "other"} {
		body, err := json.Marshal(LegacyAttachRequest{Owner: core.Protocol8SessionOwner{Kind: "parent", ID: "primary"}, Provider: provider})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeLegacyAttachRequest(body); !errors.Is(err, core.ErrInvalidInput) {
			t.Fatalf("wrong invalid provider refusal %q: %v", provider, err)
		}
	}
}
