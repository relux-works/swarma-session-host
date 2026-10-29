package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// The typed Muse rebuild payload retains every provenance/geometry field but
// owns its mutable memory across the host factory boundary. It does not attest
// fragment validity or launch a provider; the existing provider validator does.
func TestLaunchPayloadCopyPreservesMuseProvenance(t *testing.T) {
	src := LaunchSpec{Muse: &MuseSessionLaunch{Contract: MuseSessionLaunchContract, Model: "model", Effort: "high", EffortRequired: true, WorkDir: "/workspace", Env: []string{"PATH=/bin"}, Yolo: true, ToolRelease: "release", OperatorArgs: []string{"--flag"}, BinarySelection: "/bin/muse", Fragment: []byte(`{"profile":"fixture"}`), FragmentSHA256: "digest", Profile: "profile", ProfileLock: "lock", ManagedHome: "/managed", CuratorPath: "/bin/curator", CuratorVersion: "version", Term: "xterm", Rows: 24, Cols: 80}}
	got := copyLaunchSpec(src)
	if !reflect.DeepEqual(src, got) {
		t.Fatal("Muse launch copy dropped admitted payload facts")
	}
	if got.Muse == src.Muse {
		t.Fatal("Muse launch payload retained caller pointer")
	}
	got.Muse.Model = "changed"
	got.Muse.Env[0] = "changed"
	got.Muse.OperatorArgs[0] = "changed"
	got.Muse.Fragment[0] = 'x'
	if src.Muse.Model != "model" || src.Muse.Env[0] != "PATH=/bin" || src.Muse.OperatorArgs[0] != "--flag" || src.Muse.Fragment[0] != '{' {
		t.Fatal("Muse launch payload retained mutable caller data")
	}
	if copyLaunchSpec(LaunchSpec{}).Muse != nil {
		t.Fatal("absent Muse launch manufactured payload")
	}
}

// The Codex model and exact admitted snapshot survive factory isolation; mutable
// launch arrays, process provenance and home absence lists cannot alias callers.
func TestLaunchPayloadCopyPreservesCodexProvenance(t *testing.T) {
	src := LaunchSpec{Codex: &CodexPreparedLaunch{Model: "model", Snapshot: CodexRunnerSnapshot{HostArgv: []string{"host"}, ManagedClient: []string{"client"}, Sources: []LaunchPlanSource{{EnabledServers: []string{"server"}}}, ProcessIdentity: &ProviderProcessIdentity{PID: 42}, CuratorHomeCopy: &CodexCuratorHomeCopy{Absent: []string{"config"}}}}}
	got := copyLaunchSpec(src)
	if !reflect.DeepEqual(src, got) || got.Codex == src.Codex {
		t.Fatal("Codex copy lost payload or retained pointer")
	}
	got.Codex.Model = "changed"
	got.Codex.Snapshot.HostArgv[0] = "changed"
	got.Codex.Snapshot.ManagedClient[0] = "changed"
	got.Codex.Snapshot.Sources[0].EnabledServers[0] = "changed"
	got.Codex.Snapshot.ProcessIdentity.PID = 99
	got.Codex.Snapshot.CuratorHomeCopy.Absent[0] = "changed"
	if src.Codex.Model != "model" || src.Codex.Snapshot.HostArgv[0] != "host" || src.Codex.Snapshot.ManagedClient[0] != "client" || src.Codex.Snapshot.Sources[0].EnabledServers[0] != "server" || src.Codex.Snapshot.ProcessIdentity.PID != 42 || src.Codex.Snapshot.CuratorHomeCopy.Absent[0] != "config" {
		t.Fatal("Codex payload retains mutable caller state")
	}
	if copyLaunchSpec(LaunchSpec{}).Codex != nil {
		t.Fatal("absent Codex manufactured")
	}
}

// The existing JSON-admitted primary plan retains all receipts and geometry;
// copies detach its nested JSON/MCP arrays. Arbitrary non-JSON values and
// cyclic caller objects are outside the strict wire decoder's input domain.
func TestLaunchPayloadCopyPreservesClaudePlan(t *testing.T) {
	plan := PrimarySessionLaunchPlan{Contract: PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "claude", Executable: "/bin/claude", ProjectDir: "/workspace", Producer: LaunchPlanProducer{Version: "version", Commit: "commit"}, RequiredEnvNames: []string{"PATH"}, Sources: []LaunchPlanSource{{EnabledServers: []string{"mcp"}}}, LaunchVariants: LaunchVariants{ManagedHost: LaunchArgv{Kind: "claude-pty", Argv: []string{"--print"}}}, Resolved: ResolvedPrimarySession{Model: ResolvedLaunchValue{Value: map[string]any{"nested": []any{"model"}}, Source: "source"}, MCP: ResolvedMCP{Sources: []string{"source"}, Servers: []json.RawMessage{[]byte(`{"name":"mcp"}`)}}, PiCompatibility: &ResolvedLaunchValue{Value: "value", Source: "receipt"}}}
	src := LaunchSpec{Claude: &ClaudePreparedLaunch{Contract: "contract", Plan: plan, Term: "xterm", Rows: 24, Cols: 80, ResumeSessionUUID: "uuid"}}
	before, err := json.Marshal(src.Claude.Plan)
	if err != nil {
		t.Fatal(err)
	}
	got := copyLaunchSpec(src)
	if !reflect.DeepEqual(src, got) || got.Claude == src.Claude {
		t.Fatal("Claude launch lost typed receipt or retained caller")
	}
	got.Claude.Plan.RequiredEnvNames[0] = "changed"
	got.Claude.Plan.Sources[0].EnabledServers[0] = "changed"
	got.Claude.Plan.LaunchVariants.ManagedHost.Argv[0] = "changed"
	got.Claude.Plan.Resolved.Model.Value.(map[string]any)["nested"].([]any)[0] = "changed"
	got.Claude.Plan.Resolved.MCP.Sources[0] = "changed"
	got.Claude.Plan.Resolved.MCP.Servers[0][0] = 'x'
	got.Claude.Plan.Resolved.PiCompatibility.Source = "changed"
	after, err := json.Marshal(src.Claude.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("Claude plan mutation reached caller")
	}
	if copyLaunchSpec(LaunchSpec{}).Claude != nil {
		t.Fatal("absent Claude payload manufactured")
	}
	codex := LaunchSpec{Codex: &CodexPreparedLaunch{Plan: plan, AutoContinue: CodexAutoContinueOptions{Prompt: "prompt"}, ResumeThreadID: "thread", ChildEnv: map[string]string{"FIXTURE": "value"}}}
	copied := copyLaunchSpec(codex)
	if !reflect.DeepEqual(codex, copied) {
		t.Fatal("Codex preparation omitted original facts")
	}
	copied.Codex.ChildEnv["FIXTURE"] = "changed"
	copied.Codex.Plan.RequiredEnvNames[0] = "changed"
	if codex.Codex.ChildEnv["FIXTURE"] != "value" || codex.Codex.Plan.RequiredEnvNames[0] != "PATH" {
		t.Fatal("Codex prepared inputs aliased")
	}
}

// A fork prior is distinct from a live runner image. The closed record union
// rejects both fork-only and mixed images; actual provider factories separately
// admit their immutable fork sources before native I/O.
func TestRunnerForkSourceCopyAndLiveRecordBoundary(t *testing.T) {
	src := RunnerSnapshot{Kind: "codex-fork", CodexFork: &CodexContextForkPlan{Contract: "fork", Primary: ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &CodexRunnerSnapshot{HostArgv: []string{"host"}}}}, Supplemental: []CodexSupplementalContext{{CtxID: "context", Handoff: "handoff"}}}}
	copied := copyRunnerSnapshot(src)
	if !reflect.DeepEqual(src, copied) || src.CodexFork == copied.CodexFork {
		t.Fatal("fork prior copy dropped facts or retained pointer")
	}
	copied.CodexFork.Primary.Snapshot.Codex.HostArgv[0] = "changed"
	copied.CodexFork.Supplemental[0].Handoff = "changed"
	if src.CodexFork.Primary.Snapshot.Codex.HostArgv[0] != "host" || src.CodexFork.Supplemental[0].Handoff != "handoff" {
		t.Fatal("fork prior retained source aliases")
	}
	live := ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &CodexRunnerSnapshot{}}}
	if err := validateRunnerSnapshot(live); err != nil {
		t.Fatal(err)
	}
	if err := validateRunnerSnapshot(ProviderHostRecord{Provider: "codex", Snapshot: src}); err == nil {
		t.Fatal("fork source admitted as live record")
	}
	live.Snapshot.CodexFork = src.CodexFork
	if err := validateRunnerSnapshot(live); err == nil {
		t.Fatal("mixed fork/live union admitted")
	}
	claude := RunnerSnapshot{Kind: "claude-fork", ClaudeFork: &ClaudeContextForkPlan{Supplemental: []ClaudeSupplementalContext{{CtxID: "context", Handoff: "handoff", Raw: true, RawSessionUUID: "uuid", RawProjectDir: "/project"}}}}
	c := copyRunnerSnapshot(claude)
	if !reflect.DeepEqual(c, claude) || c.ClaudeFork == claude.ClaudeFork {
		t.Fatal("Claude fork copy lost facts or retained pointer")
	}
	c.ClaudeFork.Supplemental[0].Handoff = "changed"
	if claude.ClaudeFork.Supplemental[0].Handoff != "handoff" {
		t.Fatal("Claude supplemental aliases caller")
	}
}

type payloadCountFactory struct {
	ProviderHostFactory
	launches int
}

func (f *payloadCountFactory) Launch(ctx context.Context, r ProviderLaunchRequest) (RunnerHost, error) {
	f.launches++
	return f.ProviderHostFactory.Launch(ctx, r)
}

// Both actual host admission paths refuse a foreign or ambiguous typed launch
// before the provider factory and record writer. Adjacent Muse payloads still
// launch exactly once. Only the native provider boundary uses the existing double.
func TestLaunchPayloadAdmissionPrecedesFactoryAndWriter(t *testing.T) {
	for _, staged := range []bool{false, true} {
		for _, kind := range []string{"foreign", "ambiguous", "valid"} {
			t.Run(fmt.Sprintf("staged=%t/%s", staged, kind), func(t *testing.T) {
				h, codec, _, q, port := attachmentProbe(t)
				factory := &payloadCountFactory{ProviderHostFactory: h.options.Factories[0]}
				h.options.Factories[0] = factory
				switch kind {
				case "foreign":
					q.Launch.Codex = &CodexPreparedLaunch{}
				case "ambiguous":
					q.Launch.Codex = &CodexPreparedLaunch{}
					q.Launch.Muse = &MuseSessionLaunch{}
				case "valid":
					q.Launch.Muse = &MuseSessionLaunch{Contract: MuseSessionLaunchContract}
				}
				var err error
				if staged {
					_, err = port.PrepareAttach(context.Background(), q)
				} else {
					_, err = h.Attach(context.Background(), q)
				}
				if kind == "valid" {
					if err != nil || factory.launches != 1 || codec.acks != 1 {
						t.Fatalf("valid control: launches=%d ACKs=%d err=%v", factory.launches, codec.acks, err)
					}
					return
				}
				if !errors.Is(err, ErrInvalidInput) || factory.launches != 0 || codec.acks != 0 || len(h.sessions) != 0 || len(h.runners) != 0 || len(h.preparations) != 0 || len(h.pendingInitial) != 0 {
					t.Fatalf("typed launch refusal must precede native/writer effects: launches=%d ACKs=%d err=%v", factory.launches, codec.acks, err)
				}
			})
		}
	}
}
