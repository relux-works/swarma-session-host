package sessionmanager

import "testing"

var lockedCodexTailCorpus = []struct {
	name string
	tail []string
	want string
}{
	// Config overrides refuse in every spelling, whatever the key: a
	// `-c` reaches the provider's config surface directly. The policy
	// keys are the bypass; the harmless-looking keys refuse too.
	{"config approval never", []string{"-c", "approval_policy=never"}, "-c"},
	{"config sandbox danger", []string{"-c", "sandbox_mode=danger-full-access"}, "-c"},
	{"config sandbox permissions", []string{"-c", "sandbox_permissions=[\"disk-full-read-access\"]"}, "-c"},
	{"config long approval", []string{"--config", "approval_policy=never"}, "--config"},
	{"config long attached sandbox", []string{"--config=sandbox_mode=danger-full-access"}, "--config"},
	{"config short attached approval", []string{"-capproval_policy=never"}, "-c"},
	{"config short equals attached", []string{"-c=sandbox_mode=danger-full-access"}, "-c"},
	{"config harmless effort still refuses", []string{"-c", `model_reasoning_effort="high"`}, "-c"},
	{"config harmless model still refuses", []string{"-c", `model="gpt-5.6-sol"`}, "-c"},
	{"config mcp server", []string{"-c", "mcp_servers.x.command=\"/bin/sh\""}, "-c"},
	{"config dangling", []string{"-c"}, "-c"},
	{"empty argument", []string{""}, "<empty>"},
	{"empty before search", []string{"", "--search"}, "<empty>"},
	{"empty before config", []string{"", "-c", "model_reasoning_effort=max"}, "<empty>"},
	{"empty config value", []string{"-c", ""}, "-c"},
	{"empty inside config value", []string{"-c", `model=""`}, "-c"},
	// The bypass forms.
	{"bypass flag", []string{"--dangerously-bypass-approvals-and-sandbox"}, "--dangerously-bypass-approvals-and-sandbox"},
	{"yolo alias", []string{"--yolo"}, "--yolo"},
	{"approve for me", []string{"--approve-for-me"}, "--approve-for-me"},
	{"hook trust bypass", []string{"--dangerously-bypass-hook-trust"}, "--dangerously-bypass-hook-trust"},
	{"future bypass selector", []string{"--dangerously-bypass-everything"}, "--dangerously-bypass-everything"},
	{"full auto", []string{"--full-auto"}, "--full-auto"},
	{"not so yolo", []string{"--not-so-yolo"}, "--not-so-yolo"},
	// Sandbox and approval knobs, every spelling and value — narrowing
	// values included: under a lock the session runs native defaults.
	{"sandbox long", []string{"--sandbox", "danger-full-access"}, "--sandbox"},
	{"sandbox long narrowing", []string{"--sandbox", "read-only"}, "--sandbox"},
	{"sandbox long attached", []string{"--sandbox=workspace-write"}, "--sandbox"},
	{"sandbox short", []string{"-s", "danger-full-access"}, "-s"},
	{"sandbox short attached", []string{"-sread-only"}, "-s"},
	{"approval long", []string{"--ask-for-approval", "never"}, "--ask-for-approval"},
	{"approval long attached", []string{"--ask-for-approval=on-request"}, "--ask-for-approval"},
	{"approval short", []string{"-a", "never"}, "-a"},
	{"approval short attached", []string{"-anever"}, "-a"},
	// Profile selection and config loading.
	{"profile long", []string{"--profile", "work"}, "--profile"},
	{"profile long attached", []string{"--profile=work"}, "--profile"},
	{"profile short", []string{"-p", "work"}, "-p"},
	{"profile short attached", []string{"-pwork"}, "-p"},
	{"strict config", []string{"--strict-config"}, "--strict-config"},
	{"enable feature", []string{"--enable", "sleep_tool"}, "--enable"},
	{"disable feature", []string{"--disable", "goals"}, "--disable"},
	// Scope wideners, provider and endpoint selection.
	{"add dir", []string{"--add-dir", "/tmp/work"}, "--add-dir"},
	{"add dir attached", []string{"--add-dir=/tmp/work"}, "--add-dir"},
	{"cd long", []string{"--cd", "/tmp/work"}, "--cd"},
	{"cd short", []string{"-C", "/tmp/work"}, "-C"},
	{"oss", []string{"--oss"}, "--oss"},
	{"local provider", []string{"--local-provider", "ollama"}, "--local-provider"},
	{"remote", []string{"--remote", "ws://127.0.0.1:1"}, "--remote"},
	{"remote auth env", []string{"--remote-auth-token-env", "TOKEN"}, "--remote-auth-token-env"},
	// Unpinned tool, presentation and session switches fail closed.
	{"image", []string{"-i", "shot.png"}, "-i"},
	{"image long", []string{"--image", "shot.png"}, "--image"},
	{"search", []string{"--search"}, "--search"},
	{"no alt screen", []string{"--no-alt-screen"}, "--no-alt-screen"},
	{"worktree", []string{"--worktree"}, "--worktree"},
	{"no daemon", []string{"--no-daemon"}, "--no-daemon"},
	{"last", []string{"--last"}, "--last"},
	{"all", []string{"--all"}, "--all"},
	{"include non interactive", []string{"--include-non-interactive"}, "--include-non-interactive"},
	{"help", []string{"--help"}, "--help"},
	{"help short", []string{"-h"}, "-h"},
	{"version", []string{"--version"}, "--version"},
	{"version short", []string{"-V"}, "-V"},
	{"wrong case model", []string{"--Model", "x"}, "--Model"},
	{"combined short switches", []string{"-hV"}, "-hV"},
	// Positionals, subcommands, and the terminator are not locked-safe
	// flags; a bypass token anywhere in the tail still refuses.
	{"subcommand", []string{"exec"}, "exec"},
	{"prompt positional", []string{"fix it"}, "fix it"},
	{"subcommand after safe flag", []string{"--model", "x", "exec"}, "exec"},
	{"lone dash", []string{"-"}, "-"},
	{"terminator", []string{"--"}, "--"},
	{"bypass past the terminator", []string{"--", "--dangerously-bypass-approvals-and-sandbox"}, "--"},
	{"refused flag among safe", []string{"--model", "x", "--sandbox", "read-only"}, "--sandbox"},
	{"safe after refused names first", []string{"-c", "approval_policy=never", "--model", "x"}, "-c"},
	{"dash value is not owned", []string{"--model", "--sandbox", "danger-full-access"}, "--sandbox"},
	{"dash value short is not owned", []string{"-m", "--yolo"}, "--yolo"},

	// The admit half: every locked-safe spelling with a valid value.
	{"nil tail", nil, ""},
	{"empty tail", []string{}, ""},
	{"model", []string{"--model", "gpt-5.6-sol"}, ""},
	{"model attached", []string{"--model=gpt-5.6-sol"}, ""},
	{"model short", []string{"-m", "gpt-5.6-sol"}, ""},
	{"model short attached", []string{"-mgpt-5.6-sol"}, ""},
	{"model short equals", []string{"-m=gpt-5.6-sol"}, ""},
	{"empty model value", []string{"--model", ""}, ""},
	{"model twice", []string{"--model", "a", "-m", "b"}, ""},
	// A dangling safe flag is malformed, not widening; the provider
	// refuses it itself.
	{"dangling model", []string{"--model"}, ""},
	{"dangling model short", []string{"-m"}, ""},
}

// Preserves the original closed classifier verdicts and neighboring native controls.
func TestLockedCodexTailRefusedFlag(t *testing.T) {
	for _, row := range lockedCodexTailCorpus {
		t.Run(row.name, func(t *testing.T) {
			if got := LockedCodexTailRefusedFlag(row.tail); got != row.want {
				t.Fatalf("LockedCodexTailRefusedFlag(%#v) = %q, want %q", row.tail, got, row.want)
			}
		})
	}
}

// Preserves the original closed classifier verdicts and neighboring native controls.
func TestLockedCodexGatesAgreeOnEveryTail(t *testing.T) {
	for _, row := range lockedCodexTailCorpus {
		t.Run(row.name, func(t *testing.T) {
			composer := LockedCodexSessionRefused(LockedCodexSession{ClientTail: row.tail})
			daemon := lockedCodexPlanRefused(&PrimarySessionLaunchPlan{
				LaunchVariants: LaunchVariants{
					ManagedHost:   LaunchArgv{Kind: "codex-app-server", Argv: []string{"app-server"}},
					ManagedClient: LaunchArgv{Argv: row.tail},
				},
				Resolved: ResolvedPrimarySession{Yolo: ResolvedLaunchValue{Value: false, Source: "provider-argv"}},
			})
			if composer != daemon {
				t.Fatalf("composer = %#v, daemon = %#v for %#v; the gates drifted", composer, daemon, row.tail)
			}
			if composer.Flag != row.want || (row.want == "") != (composer.Knob == "") {
				t.Fatalf("gates agree at %#v for %#v, want flag %q", composer, row.tail, row.want)
			}
		})
	}
}

// Preserves the original closed classifier verdicts and neighboring native controls.
func TestLockedCodexSafeSetIsExactlyTheJustifiedSet(t *testing.T) {
	want := map[string]bool{"-m": true, "--model": true}
	if len(codexLockedSafeFlags) != len(want) {
		t.Fatalf("locked-safe set has %d flags, want the justified %d", len(codexLockedSafeFlags), len(want))
	}
	for flag := range want {
		if !codexLockedSafeFlags[flag] {
			t.Fatalf("locked-safe set lost justified flag %q", flag)
		}
	}
	for flag := range codexLockedSafeFlags {
		if !want[flag] {
			t.Fatalf("locked-safe set admits %q, which no vendored semantics pin justifies", flag)
		}
	}
}

// codexManagedFlagInventory is every option the vendored codex plugin
// knows for the managed client's argv: the root grammar (policy.go
// codexRootOptionTakesValue and codexRootOptionIsBoolean) and the pinned
// 0.159.0 resume inventory (codex-0.159.0-resume-options.tsv), which is
// the grammar the visible client's tail lands in after `resume --last`.
// Both halves are pinned in both directions against an inventory derived
// from the vendored grammar below; the module exports no inventory reader.
var codexManagedFlagInventory = struct {
	rootValue, rootBool, resumeOnly []string
}{
	rootValue: []string{
		"-C", "--cd", "--add-dir", "-m", "--model", "--local-provider", "-p", "--profile",
		"-c", "--config", "-a", "--ask-for-approval", "-s", "--sandbox", "--enable", "--disable",
		"--remote", "--remote-auth-token",
	},
	rootBool: []string{
		"--oss", "--approve-for-me", "--not-so-yolo", "--dangerously-bypass-approvals-and-sandbox",
		"--yolo", "--dangerously-bypass-hook-trust", "--strict-config", "--search", "--no-alt-screen",
	},
	resumeOnly: []string{
		"--remote-auth-token-env", "-i", "--image", "--last", "--all", "--include-non-interactive",
		"--worktree", "--no-daemon", "-h", "--help", "-V", "--version",
	},
}

// Preserves the original closed classifier verdicts and neighboring native controls.
func TestLockedCodexSessionRefusedNamesEveryKnob(t *testing.T) {
	for _, row := range []struct {
		name    string
		session LockedCodexSession
		want    LockedCodexSessionRefusal
		message string
	}{
		{"yolo", LockedCodexSession{Yolo: true, ClientTail: []string{"--model", "x"}}, LockedCodexSessionRefusal{Knob: "yolo"}, `posture knob "yolo" is wider than the locked mode`},
		{"sandbox narrowing", LockedCodexSession{Sandbox: "read-only"}, LockedCodexSessionRefusal{Knob: "sandbox", Flag: "--sandbox"}, `posture knob "sandbox" (flag "--sandbox") is wider than the locked mode`},
		{"sandbox danger", LockedCodexSession{Sandbox: "danger-full-access"}, LockedCodexSessionRefusal{Knob: "sandbox", Flag: "--sandbox"}, ""},
		{"approval", LockedCodexSession{Approval: "never"}, LockedCodexSessionRefusal{Knob: "approval", Flag: "--ask-for-approval"}, ""},
		{"knob before tail", LockedCodexSession{Approval: "never", ClientTail: []string{"--yolo"}}, LockedCodexSessionRefusal{Knob: "approval", Flag: "--ask-for-approval"}, ""},
		{"tail", LockedCodexSession{ClientTail: []string{"--yolo"}}, LockedCodexSessionRefusal{Knob: "locked-tail", Flag: "--yolo"}, `locked-tail flag "--yolo" is outside the locked session's allowed set`},
		{"native", LockedCodexSession{ClientTail: []string{"--model", "x"}}, LockedCodexSessionRefusal{}, ""},
		{"blank knobs are native", LockedCodexSession{Sandbox: " ", Approval: "\t"}, LockedCodexSessionRefusal{}, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			got := LockedCodexSessionRefused(row.session)
			if got != row.want {
				t.Fatalf("LockedCodexSessionRefused(%#v) = %#v, want %#v", row.session, got, row.want)
			}
			if row.message != "" && got.Describe() != row.message {
				t.Fatalf("Describe = %q, want %q", got.Describe(), row.message)
			}
		})
	}
}

// Preserves the original closed classifier verdicts and neighboring native controls.
func TestLockedCodexHostArgvRefused(t *testing.T) {
	for _, row := range []struct {
		name string
		argv []string
		want string
	}{
		{"bare subcommand", []string{"app-server"}, ""},
		{"composer emission", []string{"-c", `model="gpt-5.6-sol"`, "-c", `model_reasoning_effort="max"`, "-c", `service_tier="priority"`, "-c", `features.sleep_tool.mode="always_on"`, "app-server"}, ""},
		{"long config spellings", []string{"--config", `model="x"`, `--config=features.sleep_tool.mode="always_on"`, "app-server"}, ""},
		{"policy key", []string{"-c", `sandbox_mode="danger-full-access"`, "app-server"}, "-c sandbox_mode"},
		{"approval key", []string{"-c", `approval_policy="never"`, "app-server"}, "-c approval_policy"},
		{"profile key", []string{"-c", `profile="work"`, "app-server"}, "-c profile"},
		{"unquoted value", []string{"-c", "model=gpt", "app-server"}, "-c model"},
		{"inline table value", []string{"-c", `model={sandbox_mode="danger-full-access"}`, "app-server"}, "-c model"},
		{"tampered sleep value", []string{"-c", `features.sleep_tool.mode="off"`, "app-server"}, "-c features.sleep_tool.mode"},
		{"padded key", []string{"-c", ` model="x"`, "app-server"}, "-c  model"},
		{"override without equals", []string{"-c", "model", "app-server"}, "-c"},
		{"dangling config", []string{"-c"}, "-c"},
		{"attached short config", []string{`-cmodel="x"`, "app-server"}, "-c"},
		{"feature switch", []string{"--enable", "goals", "app-server"}, "--enable"},
		{"bypass flag", []string{"--dangerously-bypass-approvals-and-sandbox", "app-server"}, "--dangerously-bypass-approvals-and-sandbox"},
		{"sandbox flag", []string{"-s", "danger-full-access", "app-server"}, "-s"},
		{"token after subcommand", []string{"app-server", "--listen", "ws://x"}, "--listen"},
		{"flag attached after subcommand", []string{"app-server", "-cfoo=bar"}, "-c"},
		{"missing subcommand", []string{"-c", `model="x"`}, "app-server"},
		{"empty argv", nil, "app-server"},
		{"empty token before subcommand", []string{"", "app-server"}, "<empty>"},
		{"empty token after subcommand", []string{"app-server", "", "--search"}, "<empty>"},
		{"empty config argument", []string{"-c", "", "app-server"}, "-c"},
		{"empty model config string", []string{"-c", `model=""`, "app-server"}, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := LockedCodexHostArgvRefused(row.argv); got != row.want {
				t.Fatalf("LockedCodexHostArgvRefused(%#v) = %q, want %q", row.argv, got, row.want)
			}
		})
	}
}
