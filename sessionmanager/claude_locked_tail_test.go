package sessionmanager

import (
	"testing"
)

// lockedClaudeTailCorpus pins the one shared locked-tail classifier:
// under an engaged lock the native claude tail may
// contain ONLY the locked-safe flags, and --permission-mode only with
// "manual". Every row names the first refused flag, or "" when the tail
// is locked-safe. Both gate entries run this corpus (parity test below),
// so a drift between the composer and daemon verdicts fails the build.
var lockedClaudeTailCorpus = []struct {
	name string
	tail []string
	want string
}{
	// The r2 bypass: explicit and qualified tool grants pre-approve
	// tools and pass no approval loop. All refuse.
	{"allowed tools explicit grants", []string{"--allowedTools", "Bash(*)", "Edit", "Write"}, "--allowedTools"},
	{"allowed tools attached grant", []string{"--allowedTools=Bash(*)"}, "--allowedTools"},
	{"allowed tools kebab grant", []string{"--allowed-tools", "mcp__x__y"}, "--allowed-tools"},
	{"allowed tools kebab attached", []string{"--allowed-tools=Edit"}, "--allowed-tools"},
	{"tools grant", []string{"--tools", "Bash"}, "--tools"},
	{"tools grant attached", []string{"--tools=Bash"}, "--tools"},
	// Scope wideners and executable/configuration loaders.
	{"add dir", []string{"--add-dir", "/tmp/work"}, "--add-dir"},
	{"add dir attached", []string{"--add-dir=/tmp/work"}, "--add-dir"},
	{"mcp config", []string{"--mcp-config", "evil.json"}, "--mcp-config"},
	{"mcp config attached", []string{"--mcp-config=evil.json"}, "--mcp-config"},
	{"strict mcp config", []string{"--strict-mcp-config"}, "--strict-mcp-config"},
	{"settings inline", []string{"--settings", `{"permissions":{"defaultMode":"bypassPermissions"}}`}, "--settings"},
	{"settings path", []string{"--settings", "/tmp/s.json"}, "--settings"},
	{"settings attached", []string{"--settings=/tmp/s.json"}, "--settings"},
	{"setting sources", []string{"--setting-sources", "user"}, "--setting-sources"},
	{"setting sources attached", []string{"--setting-sources=user"}, "--setting-sources"},
	{"agent", []string{"--agent", "explore"}, "--agent"},
	{"agents", []string{"--agents", `{"a":{}}`}, "--agents"},
	{"plugin dir", []string{"--plugin-dir", "/tmp/p"}, "--plugin-dir"},
	{"plugin url", []string{"--plugin-url", "https://x/y"}, "--plugin-url"},
	{"betas", []string{"--betas", "x"}, "--betas"},
	{"bare", []string{"--bare"}, "--bare"},
	{"safe mode", []string{"--safe-mode"}, "--safe-mode"},
	// The bypass forms (r1 contract, kept).
	{"bypass flag", []string{"--dangerously-skip-permissions"}, "--dangerously-skip-permissions"},
	{"enabling flag", []string{"--allow-dangerously-skip-permissions"}, "--allow-dangerously-skip-permissions"},
	{"bypass mode separate", []string{"--permission-mode", "bypassPermissions"}, "--permission-mode"},
	{"bypass mode joined", []string{"--permission-mode=bypassPermissions"}, "--permission-mode"},
	// Non-manual permission modes (r2 rule, kept).
	{"unknown mode", []string{"--permission-mode", "ultra"}, "--permission-mode"},
	{"unknown mode joined", []string{"--permission-mode=ultra"}, "--permission-mode"},
	{"accept edits", []string{"--permission-mode", "acceptEdits"}, "--permission-mode"},
	{"auto mode", []string{"--permission-mode=auto"}, "--permission-mode"},
	{"dont ask mode", []string{"--permission-mode", "dontAsk"}, "--permission-mode"},
	{"plan mode", []string{"--permission-mode", "plan"}, "--permission-mode"},
	{"dangling selector", []string{"--permission-mode"}, "--permission-mode"},
	{"empty joined value", []string{"--permission-mode="}, "--permission-mode"},
	{"wrong case value", []string{"--permission-mode", "Manual"}, "--permission-mode"},
	// Unknown and wrong-case spellings refuse under a lock (fail
	// closed); unlocked they stay the allowlist's refusal downstream.
	{"wrong case flag", []string{"--Settings", "/tmp/s.json"}, "--Settings"},
	{"unrelated flag", []string{"--no-color"}, "--no-color"},
	{"print flag", []string{"--print"}, "--print"},
	{"resume flag", []string{"--resume", "x"}, "--resume"},
	{"combined short spelling", []string{"-dfoo"}, "-dfoo"},
	// r4 narrowing (native-tool-allow-bypass): --file downloads
	// file_id:relative_path resources at startup and --brief enables the
	// SendUserMessage tool, so neither is prompt context or
	// presentation. The remaining presentation/diagnostics flags carry
	// no vendored semantics pin and fail closed with them.
	{"file resource spec refuses", []string{"--file", "file_abc:.claude/settings.json"}, "--file"},
	{"file local path refuses", []string{"--file", "notes.md", "plan.md"}, "--file"},
	{"file attached refuses", []string{"--file=notes.md"}, "--file"},
	{"brief refuses", []string{"--brief"}, "--brief"},
	{"brief attached refuses", []string{"--brief=x"}, "--brief"},
	{"exclude dynamic refuses", []string{"--exclude-dynamic-system-prompt-sections"}, "--exclude-dynamic-system-prompt-sections"},
	{"verbose refuses", []string{"--verbose"}, "--verbose"},
	{"debug refuses", []string{"--debug"}, "--debug"},
	{"debug short refuses", []string{"-d", "api"}, "-d"},
	{"debug file refuses", []string{"--debug-file", "/tmp/d.log"}, "--debug-file"},
	{"ax screen reader refuses", []string{"--ax-screen-reader"}, "--ax-screen-reader"},
	{"prompt suggestions refuses", []string{"--prompt-suggestions"}, "--prompt-suggestions"},
	{"no chrome refuses", []string{"--no-chrome"}, "--no-chrome"},
	// Positionals, subcommands, and the terminator are not locked-safe
	// flags; a bypass token anywhere in the tail still refuses.
	{"subcommand", []string{"mcp"}, "mcp"},
	{"subcommand after safe flag", []string{"--model", "x", "mcp", "list"}, "mcp"},
	{"lone dash", []string{"-"}, "-"},
	{"terminator", []string{"--"}, "--"},
	{"terminator ends safe run", []string{"--model", "x", "--", "--model"}, "--"},
	{"bypass past the terminator", []string{"--", "--dangerously-skip-permissions"}, "--"},
	{"refused flag among safe", []string{"--model", "x", "--settings", "/tmp/s.json"}, "--settings"},
	{"safe after refused names first", []string{"--allowedTools", "Bash", "--model", "x"}, "--allowedTools"},

	// The admit half: every locked-safe flag, each with valid values.
	// r4 narrows this to the nine flags the vendored plugin justifies
	// by provider semantics (see claudeLockedSafeFlags); the r3
	// presentation/diagnostics and file-context rows moved to the
	// refuse block above.
	{"nil tail", nil, ""},
	{"empty tail", []string{}, ""},
	{"model", []string{"--model", "claude-opus-5"}, ""},
	{"model attached", []string{"--model=claude-opus-5"}, ""},
	{"effort", []string{"--effort", "high"}, ""},
	{"manual mode", []string{"--permission-mode", "manual"}, ""},
	{"manual mode joined", []string{"--permission-mode=manual"}, ""},
	{"manual among selections", []string{"--model", "x", "--permission-mode", "manual", "--name", "y"}, ""},
	{"disallowed tools", []string{"--disallowedTools", "Bash", "Edit"}, ""},
	{"disallowed tools attached denial", []string{"--disallowedTools=AskUserQuestion"}, ""},
	{"disallowed tools kebab", []string{"--disallowed-tools", "Write"}, ""},
	{"system prompt", []string{"--system-prompt", "be brief"}, ""},
	{"append system prompt", []string{"--append-system-prompt", "be brief"}, ""},
	{"name", []string{"--name", "primary"}, ""},
	{"name short", []string{"-n", "primary"}, ""},
	// Value-consumption edges, following the provider's commander
	// parse: a required value is owned even when dash-leading, so it is
	// the flag's value, never a new flag. The managed allowlist refuses
	// these malformed shapes downstream; the lock gate admits the safe
	// flag and the launch still refuses.
	{"model owns dash value", []string{"--model", "--verbose"}, ""},
	{"prompt owns dash value", []string{"--system-prompt", "--allowedTools"}, ""},
	{"dangling safe value passes to allowlist", []string{"--model"}, ""},
	{"valueless variadic passes to allowlist", []string{"--disallowedTools"}, ""},
}

// TestLockedClaudeTailRefusedFlag pins the shared classifier's verdict
// per tail: the first refused flag, or "" for a locked-safe tail.
func TestLockedClaudeTailRefusedFlag(t *testing.T) {
	for _, row := range lockedClaudeTailCorpus {
		t.Run(row.name, func(t *testing.T) {
			if got := LockedClaudeTailRefusedFlag(row.tail); got != row.want {
				t.Fatalf("LockedClaudeTailRefusedFlag(%#v) = %q, want %q", row.tail, got, row.want)
			}
		})
	}
}

// TestLockedClaudeTailEntriesAgree is the classifier parity test the r2
// review found missing: the composer entry and the daemon entry return
// the same verdict on every corpus tail when no curator-MCP emission is
// skipped. Both delegate to the one walker today; if either entry ever
// grows its own logic, this fails.
func TestLockedClaudeTailEntriesAgree(t *testing.T) {
	for _, row := range lockedClaudeTailCorpus {
		t.Run(row.name, func(t *testing.T) {
			tailVerdict := LockedClaudeTailRefusedFlag(row.tail)
			planVerdict := LockedClaudePlanArgvRefusedFlag(row.tail, "")
			if tailVerdict != planVerdict {
				t.Fatalf("composer entry = %q, daemon entry = %q for %#v; the entries drifted",
					tailVerdict, planVerdict, row.tail)
			}
			if tailVerdict != row.want {
				t.Fatalf("entries agree at %q for %#v, want %q", tailVerdict, row.tail, row.want)
			}
		})
	}
}

// TestLockedClaudePlanArgvSkipsCuratorMCPEmission pins the daemon entry's
// one input preparation: the module's own deterministic curator-MCP
// emission is skipped at most once, and only when its path equals the
// provenance snapshot's. Everything else refuses exactly like the tail.
func TestLockedClaudePlanArgvSkipsCuratorMCPEmission(t *testing.T) {
	const mcpPath = "/managed/home/mcp.json"
	prefix := func(tail ...string) []string {
		argv := []string{"--model", "claude-opus-5", "--mcp-config", mcpPath, "--strict-mcp-config", "--disallowedTools=AskUserQuestion"}
		return append(argv, tail...)
	}
	for _, row := range []struct {
		name string
		argv []string
		path string
		want string
	}{
		{"emission with safe tail admits", prefix("--name", "primary"), mcpPath, ""},
		{"emission with empty tail admits", prefix(), mcpPath, ""},
		{"emission with manual admits", prefix("--permission-mode", "manual"), mcpPath, ""},
		{"emission with refused tail names tail flag", prefix("--allowedTools", "Bash"), mcpPath, "--allowedTools"},
		{"tampered config value refuses", []string{"--model", "x", "--mcp-config", "/tmp/evil.json", "--strict-mcp-config"}, mcpPath, "--mcp-config"},
		{"second config occurrence refuses", prefix("--mcp-config", mcpPath), mcpPath, "--mcp-config"},
		{"lone strict flag refuses", []string{"--model", "x", "--disallowedTools=AskUserQuestion", "--strict-mcp-config"}, mcpPath, "--strict-mcp-config"},
		{"attached config refuses", []string{"--model", "x", "--mcp-config=" + mcpPath, "--strict-mcp-config"}, mcpPath, "--mcp-config"},
		{"no provenance path disables skip", prefix("--name", "primary"), "", "--mcp-config"},
		{"yolo bypass still refuses", append(prefix(), "--dangerously-skip-permissions"), mcpPath, "--dangerously-skip-permissions"},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := LockedClaudePlanArgvRefusedFlag(row.argv, row.path); got != row.want {
				t.Fatalf("LockedClaudePlanArgvRefusedFlag(%#v, %q) = %q, want %q", row.argv, row.path, got, row.want)
			}
		})
	}
	if got := LockedClaudeTailRefusedFlag([]string{"--mcp-config", mcpPath, "--strict-mcp-config"}); got != "--mcp-config" {
		t.Fatalf("composer entry on the emission shape = %q, want the --mcp-config refusal (no skip on the operator tail)", got)
	}
}

// TestLockedClaudeSafeSetIsExactlyTheJustifiedSet pins the r4 narrowed
// safe set member for member: the nine flags justified by provider
// semantics in NOTES-3M4OYO.md r4, no more. Re-admitting a removed flag
// (--file, --brief, or any presentation/diagnostics flag) fails here
// first, before the corpus rows name it.
func TestLockedClaudeSafeSetIsExactlyTheJustifiedSet(t *testing.T) {
	want := map[string]claudeLockedSafeFlagKind{
		"--model":                claudeLockedFlagValue,
		"--effort":               claudeLockedFlagValue,
		"--permission-mode":      claudeLockedFlagValue,
		"--disallowedTools":      claudeLockedFlagVariadic,
		"--disallowed-tools":     claudeLockedFlagVariadic,
		"--system-prompt":        claudeLockedFlagValue,
		"--append-system-prompt": claudeLockedFlagValue,
		"--name":                 claudeLockedFlagValue,
		"-n":                     claudeLockedFlagValue,
	}
	if len(claudeLockedSafeFlags) != len(want) {
		t.Fatalf("locked-safe set has %d flags, want the justified %d", len(claudeLockedSafeFlags), len(want))
	}
	for flag, kind := range want {
		got, ok := claudeLockedSafeFlags[flag]
		if !ok {
			t.Fatalf("locked-safe set lost justified flag %q", flag)
		}
		if got != kind {
			t.Fatalf("locked-safe flag %q has kind %d, want %d", flag, got, kind)
		}
	}
	for flag := range claudeLockedSafeFlags {
		if _, ok := want[flag]; !ok {
			t.Fatalf("locked-safe set re-admits %q, which r4 removed for cause", flag)
		}
	}
}

// TestLockedClaudeSafeSetIsAllowlisted proves every locked-safe flag is
// also a managed-allowlist flag: a locked-safe tail never trips the
// allowlist on the flag itself, only on malformed values, so the lock
// gate's admit half composes rather than merely deferring.
func TestLockedClaudeSafeSetIsAllowlisted(t *testing.T) {
	for flag := range claudeLockedSafeFlags {
		if _, allowed := claudeAllowedManagedArgs[flag]; !allowed {
			t.Fatalf("locked-safe flag %q is not in the managed allowlist", flag)
		}
	}
	// Every managed flag outside the safe set refuses: the allow-list
	// leaks nowhere. The refusal fires at the flag itself, so the bare
	// form suffices for every arity.
	for flag := range claudeAllowedManagedArgs {
		if _, safe := claudeLockedSafeFlags[flag]; safe {
			continue
		}
		if got := LockedClaudeTailRefusedFlag([]string{flag}); got == "" {
			t.Fatalf("managed flag %q outside the safe set is admitted; the allow-list leaks", flag)
		}
	}
}

// Empty positionals must refuse independently of their display text, in both
// the operator and daemon paths. An empty required value remains owned.
func TestLockedClaudeEmptyArgumentRefuses(t *testing.T) {
	for _, tail := range [][]string{{""}, {"", "--allowedTools", "Bash"}} {
		if got := LockedClaudeTailRefusedFlag(tail); got != "<empty>" {
			t.Fatalf("operator %q = %q, want <empty>", tail, got)
		}
		if got := LockedClaudePlanArgvRefusedFlag(tail, "/mcp"); got != "<empty>" {
			t.Fatalf("daemon %q = %q, want <empty>", tail, got)
		}
	}
	if got := LockedClaudeTailRefusedFlag([]string{"--model", ""}); got != "" {
		t.Fatalf("empty model value = %q, want admitted", got)
	}
}
