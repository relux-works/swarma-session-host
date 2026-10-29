package sessionmanager

import "strings"

// Locked Claude native-tail classification.
//
// When a Curator fragment lock is engaged, the native claude tail may
// contain ONLY flags from the explicit locked-safe set below. Every other
// flag refuses with fragment_permissions_locked, naming the flag. The rule
// is an ALLOW-list on purpose: the r2 deny-list of widening flags admitted
// --allowedTools with explicit tool grants, and every widening flag the
// provider adds would reopen the same hole. A flag whose effect the
// vendored plugin does not pin as inert stays refused (fail closed).
//
// Both lock gates call this one implementation: the spawn/composer gate
// (ManagedClaudeSessionArgs, over the operator tail) and the daemon gate
// (refuseLockedClaudeLaunch, over the composed plan argv), so parity holds
// by construction. There is no second classifier to drift.

// claudeLockedSafeFlagKind classifies how a locked-safe flag consumes argv
// tokens, mirroring the pinned provider grammar (claude 2.1.288 option
// table in the vendored plugin) and the managed allowlist kinds one to
// one, so a safe flag's VALUE is never misread as a flag.
type claudeLockedSafeFlagKind int

const (
	// claudeLockedFlagBool takes no value.
	claudeLockedFlagBool claudeLockedSafeFlagKind = iota
	// claudeLockedFlagValue requires exactly one value, owned even when
	// dash-leading, like the provider's commander parse.
	claudeLockedFlagValue
	// claudeLockedFlagVariadic consumes following non-dash tokens; the
	// attached form ends the list, like the managed allowlist.
	claudeLockedFlagVariadic
	// claudeLockedFlagOptionalValue consumes one following non-dash
	// token when present.
	claudeLockedFlagOptionalValue
)

// claudeLockedNativePermissionMode is the one --permission-mode value a
// locked fragment admits: the explicit spelling of the native
// ask-and-approve loop. Every other value — bypass, widening, narrowing,
// ambiguous, unknown, dangling, or empty — refuses fail-closed (r2 rule,
// kept).
const claudeLockedNativePermissionMode = "manual"

// claudeLockedSafeFlags is the locked-safe set (r4: nine flags). Each
// member is justified by its PROVIDER SEMANTICS from the vendored
// plugin, not by its arity — the option TSV pins arity alone, and
// absence from a tool-policy scan never established inertness (r3
// native-tool-allow-bypass). A flag the plugin does not pin as inert
// stays refused. See NOTES-3M4OYO.md r4 for the per-flag evidence;
// everything else — grants, pre-approvals, scope wideners,
// executable/config loaders, subcommands, positionals, and unknown
// spellings — refuses under a lock.
var claudeLockedSafeFlags = map[string]claudeLockedSafeFlagKind{
	// --model: the module emits "--model <id>" itself in both grammars
	// (systems/claude/args.go exec and interactive branches); model
	// selection is a separate request channel from PermissionMode, which
	// permissionMapping never consults the model for.
	"--model": claudeLockedFlagValue,
	// --effort: pure reasoning-depth transport, passed through verbatim
	// (appendEffort, args.go); orthogonal to the permission channel.
	"--effort": claudeLockedFlagValue,
	// --permission-mode: admitted only with "manual" (checked in the
	// walker below), one of the six provider-verified modes (policy.go),
	// distinct from the bypass member. The module treats the flag as
	// posture-declaring (scanNativePolicy refuses any under yolo).
	"--permission-mode": claudeLockedFlagValue,
	// --disallowedTools/--disallowed-tools: deny-only. The module emits
	// its own attached denial unconditionally and a duplicate deny "is
	// harmless and accepted" (toolpolicy.go, args.go); only
	// --allowedTools re-enables (findReenabledTool). Even "*" denies.
	"--disallowedTools":  claudeLockedFlagVariadic,
	"--disallowed-tools": claudeLockedFlagVariadic,
	// --system-prompt/--append-system-prompt: inline prompt text. The
	// module pins text carriage for --append-system-prompt (context.go
	// descriptor payload) and classifies the bare --system-prompt as the
	// same prompt channel (rejectNativeContextConflicts); path carriage
	// is spelled with the -file variants, which refuse here.
	"--system-prompt":        claudeLockedFlagValue,
	"--append-system-prompt": claudeLockedFlagValue,
	// --name/-n: the session display name (prompt box, /resume picker,
	// terminal title), pinned in session.go. Pure presentation.
	"--name": claudeLockedFlagValue,
	"-n":     claudeLockedFlagValue,
	// NOT admitted (r4): --file downloads file_id:relative_path
	// resources at startup and --brief enables the SendUserMessage tool
	// (installed provider descriptions); --verbose, --debug, -d,
	// --debug-file, --ax-screen-reader, --prompt-suggestions,
	// --no-chrome and --exclude-dynamic-system-prompt-sections carry no
	// vendored semantics pin, so they fail closed.
}

// lockedClaudeFlagToken reports whether a token is in flag position. Short
// switches are not decomposed, exactly like the managed allowlist: an
// unknown combined spelling refuses rather than parsing as its suffix.
func lockedClaudeFlagToken(token string) bool {
	return len(token) > 1 && token[0] == '-'
}

// LockedClaudeTailRefusedFlag classifies a native operator tail under an
// engaged lock. It returns the first refused flag — the name before any
// "=value", the "--" terminator, or the offending positional token — or
// "" when every token is locked-safe.
func LockedClaudeTailRefusedFlag(args []string) string {
	return scanLockedClaudeTail(args, "", false)
}

// LockedClaudePlanArgvRefusedFlag classifies a composed managed-host argv
// under an engaged lock. It is the same walk as
// LockedClaudeTailRefusedFlag, except it skips the module's own
// deterministic curator-MCP emission — the exact contiguous subsequence
// "--mcp-config <path> --strict-mcp-config" where path equals the
// provenance snapshot's curator MCP path — at most once. That emission is
// the fragment's own declared channel, rendered by the module, not an
// operator widening; without the skip every locked native MCP session
// would refuse. Any other --mcp-config or --strict-mcp-config token,
// including a second occurrence or a tampered value, still refuses. An
// empty path disables the skip (fail closed).
func LockedClaudePlanArgvRefusedFlag(argv []string, curatorMCPPath string) string {
	return scanLockedClaudeTail(argv, curatorMCPPath, curatorMCPPath != "")
}

func scanLockedClaudeTail(args []string, curatorMCPPath string, skipEmission bool) string {
	skippedEmission := false
	for index := 0; index < len(args); {
		token := args[index]
		if token == "--" {
			return "--"
		}
		if !lockedClaudeFlagToken(token) {
			return lockedTailTokenDisplay(token)
		}
		name, value, attached := strings.Cut(token, "=")
		if skipEmission && !skippedEmission && name == "--mcp-config" && !attached &&
			index+2 < len(args) && args[index+1] == curatorMCPPath && args[index+2] == "--strict-mcp-config" {
			skippedEmission = true
			index += 3
			continue
		}
		kind, safe := claudeLockedSafeFlags[name]
		if !safe {
			return name
		}
		switch kind {
		case claudeLockedFlagBool:
			// A bool flag with an attached value stays the allowlist's
			// refusal downstream; the flag itself is safe.
			index++
		case claudeLockedFlagValue:
			if attached {
				if name == "--permission-mode" && value != claudeLockedNativePermissionMode {
					return name
				}
				index++
				continue
			}
			if index+1 >= len(args) {
				if name == "--permission-mode" {
					return name
				}
				// A dangling safe value flag is malformed, not
				// widening; the managed allowlist refuses it next.
				index++
				continue
			}
			// A required value is owned even when dash-leading,
			// like the provider's commander parse: it is the
			// flag's value, never a new flag.
			if name == "--permission-mode" && args[index+1] != claudeLockedNativePermissionMode {
				return name
			}
			index += 2
		case claudeLockedFlagOptionalValue:
			if attached {
				index++
				continue
			}
			if index+1 < len(args) && !lockedClaudeFlagToken(args[index+1]) {
				index += 2
				continue
			}
			index++
		case claudeLockedFlagVariadic:
			if attached {
				index++
				continue
			}
			next := index + 1
			for next < len(args) && !lockedClaudeFlagToken(args[next]) {
				next++
			}
			// Zero values is malformed, not widening; the
			// managed allowlist refuses it next.
			index = next
		}
	}
	return ""
}
