package sessionmanager

import "strings"

// Locked-codex-session classification.
//
// When a Curator fragment lock is engaged, a managed codex session may
// request NO posture knob and its operator tail may contain ONLY flags from
// the explicit locked-safe set below. Every other flag refuses with
// fragment_permissions_locked, naming the flag. The rule is an ALLOW-list
// on purpose, exactly like the claude leaf (claude_locked_tail.go): a
// deny-list of the sandbox, approval and bypass spellings would admit every
// `-c` key, profile and config loader the provider adds later. A flag whose
// effect the vendored codex plugin does not pin is refused (fail closed).
//
// Both lock gates call this one implementation: the spawn/composer gate
// (spawn.ValidateCodexSessionLock, over the composer's structured posture
// and the client tail) and the daemon gate (refuseLockedCodexLaunch, over
// the plan's Resolved posture and its managed_client argv), so parity holds
// by construction. There is no second classifier to drift.

// CodexTrackedChildPosture names the module's fixed bypass posture every
// tracked codex child carries, for the tracked-child lock refusal: the
// exec grammar emits the bypass flag unconditionally (systems/codex/args.go
// bypassApprovalsAndSandboxFlag) and the managed-session fragment fixes
// the sandbox and approval pair. The text lives here, beside the
// classifier, because internal/spawn spells no codex flag.
const CodexTrackedChildPosture = "every tracked codex child carries the module's fixed bypass posture: --dangerously-bypass-approvals-and-sandbox headless, --sandbox danger-full-access --ask-for-approval never managed"

// codexLockedValueShortFlags are the codex short options that take a value
// in the pinned grammar (root options, systems/codex/policy.go
// codexRootOptionTakesValue, and the 0.159.0 resume inventory): -c, -m, -p,
// -s, -a, -C, -i. clap parses an attached remainder as the value
// (`-cKEY=V`, `-sread-only`, `-mMODEL`; policy.go isAttachedShortValue
// observed it against installed 0.153.4), so the classifier names such a
// token by its two-character flag, never by the value glued to it.
const codexLockedValueShortFlags = "cmpsaCi"

// codexLockedSafeFlags is the locked-safe set: two spellings of ONE flag.
// Each member is justified by its PROVIDER SEMANTICS from the vendored
// codex plugin, not by its arity — the resume TSV and the root option
// lists pin arity alone. See NOTES-1M503B.md for the per-flag evidence.
var codexLockedSafeFlags = map[string]bool{
	// -m/--model: model selection. The module spells it itself in every
	// grammar (systems/codex/args.go: `exec -m <model>`, managed-session
	// `--model <model>`, interactive `-m <model>`), verified against
	// `codex --help` at 0.153.2 as "-m, --model <MODEL>". It is not a
	// permission selector: isCodexPermissionSelector (context.go) and
	// scanNativePolicy (policy.go) never consult it, and the yolo mapping
	// (permissionMapping) takes no model input.
	"-m":      true,
	"--model": true,
	// NOT admitted, with the reason the plugin gives:
	//   - every posture knob: -s/--sandbox, -a/--ask-for-approval,
	//     --full-auto, --approve-for-me, --yolo,
	//     --dangerously-bypass-approvals-and-sandbox and every
	//     --dangerously-bypass-* selector (isCodexPermissionSelector,
	//     scanNativePolicy);
	//   - -c/--config in every spelling, whatever the key: an override
	//     reaches the provider's config surface directly and can set
	//     approval_policy, sandbox_mode or sandbox_permissions
	//     (knownConfigKeys, isConflictingConfigKey);
	//   - -p/--profile: selects a config profile, which can carry any
	//     posture key and is the Curator MCP channel's own selector
	//     (applyCodexCuratorContext, rejectNativeContextConflicts);
	//   - --add-dir and -C/--cd: scope wideners (the exec grammar emits
	//     --add-dir only for the board directory);
	//   - --enable/--disable, --strict-config, --oss, --local-provider,
	//     --remote*, -i/--image, --search, --no-alt-screen, --worktree,
	//     --no-daemon, --last/--all/--include-non-interactive,
	//     --dangerously-bypass-hook-trust, -h/--help, -V/--version: no
	//     vendored semantics pin, or a feature, provider, hook-trust,
	//     tool or session-selection switch — fail closed.
}

// lockedCodexFlagToken reports whether a token is in flag position, the
// same test claude_locked_tail.go applies.
func lockedCodexFlagToken(token string) bool {
	return len(token) > 1 && token[0] == '-'
}

// lockedCodexFlagName names one flag-position token: the long name before
// any "=value", or a value-taking short flag stripped of its attached
// value. Any other single-dash spelling is returned whole: an unknown
// combined spelling refuses rather than parsing as its suffix.
func lockedCodexFlagName(token string) (name string, attached bool) {
	if strings.HasPrefix(token, "--") {
		name, _, attached = strings.Cut(token, "=")
		return name, attached
	}
	if len(token) > 2 && strings.ContainsRune(codexLockedValueShortFlags, rune(token[1])) {
		return token[:2], true
	}
	return token, false
}

// lockedTailTokenDisplay keeps a refused empty positional distinct from the
// empty-string success sentinel used by both providers' locked classifiers.
func lockedTailTokenDisplay(token string) string {
	if token == "" {
		return "<empty>"
	}
	return token
}

// LockedCodexTailRefusedFlag classifies a native codex operator tail (the
// managed client remainder) under an engaged lock. It returns the first
// refused flag — the flag name without its value, the "--" terminator, or
// the offending positional token ("<empty>" for an empty argument) — or "" when every token is locked-safe.
func LockedCodexTailRefusedFlag(args []string) string {
	for index := 0; index < len(args); {
		token := args[index]
		if token == "--" {
			return "--"
		}
		if !lockedCodexFlagToken(token) {
			return lockedTailTokenDisplay(token)
		}
		name, attached := lockedCodexFlagName(token)
		if !codexLockedSafeFlags[name] {
			return lockedTailTokenDisplay(name)
		}
		if attached {
			index++
			continue
		}
		// clap does not hand a flag-shaped token to a value option, so a
		// dash-leading next token is classified as a flag in its own
		// right — strictly safer than owning it as the value. A dangling
		// safe flag is malformed, not widening: the provider refuses it.
		if index+1 < len(args) && !lockedCodexFlagToken(args[index+1]) {
			index += 2
			continue
		}
		index++
	}
	return ""
}

// LockedCodexSession is the posture one managed codex session requests:
// the composer's structured selections plus the verbatim client tail.
// Both gates project their input onto it — the composer from its request,
// the daemon from the plan's Resolved values and managed_client argv — so
// the one classifier below decides both.
type LockedCodexSession struct {
	// Yolo is the headless bypass request: managed children always set it
	// (the module's managed-session fragment fixes `--sandbox
	// danger-full-access --ask-for-approval never`).
	Yolo bool
	// Sandbox and Approval are explicit tool-policy selections: the
	// module fragment's values for children, the operator tail's for
	// primaries.
	Sandbox  string
	Approval string
	// ClientTail is the unconsumed operator remainder the visible client
	// receives verbatim.
	ClientTail []string
}

// LockedCodexSessionRefusal names what a locked fragment refuses in one
// requested codex session: the posture knob and, where the knob has an
// operator spelling, its flag. An empty knob means the session is
// locked-safe.
type LockedCodexSessionRefusal struct {
	Knob string
	Flag string
}

// LockedCodexSessionRefused classifies one requested managed codex session
// under an engaged lock. Any explicit posture knob refuses — under a lock
// the session runs the provider's own native defaults and nothing else,
// so even a narrowing selection is outside the locked set, exactly like
// the claude leaf admits no non-manual permission mode — and then the
// client tail goes through the one allow-list walk.
func LockedCodexSessionRefused(session LockedCodexSession) LockedCodexSessionRefusal {
	if session.Yolo {
		return LockedCodexSessionRefusal{Knob: "yolo"}
	}
	if strings.TrimSpace(session.Sandbox) != "" {
		return LockedCodexSessionRefusal{Knob: "sandbox", Flag: "--sandbox"}
	}
	if strings.TrimSpace(session.Approval) != "" {
		return LockedCodexSessionRefusal{Knob: "approval", Flag: "--ask-for-approval"}
	}
	if flag := LockedCodexTailRefusedFlag(session.ClientTail); flag != "" {
		return LockedCodexSessionRefusal{Knob: "locked-tail", Flag: flag}
	}
	return LockedCodexSessionRefusal{}
}

// Describe renders the refusal's knob and flag for the shared lock
// message: a tail flag reads like the claude leaf's, a posture knob names
// itself and its operator spelling.
func (r LockedCodexSessionRefusal) Describe() string {
	switch {
	case r.Knob == "locked-tail":
		return `locked-tail flag "` + r.Flag + `" is outside the locked session's allowed set`
	case r.Knob == "managed-host":
		return `managed-host argv token "` + r.Flag + `" is not the composer's own emission`
	case r.Flag != "":
		return `posture knob "` + r.Knob + `" (flag "` + r.Flag + `") is wider than the locked mode`
	default:
		return `posture knob "` + r.Knob + `" is wider than the locked mode`
	}
}

// codexLockedHostConfigKeys are the `-c` keys the in-repo composer itself
// writes ahead of the app-server subcommand (cmd/primary_compose.go
// composeCodexPrimarySession, cmd/codex_manager.go
// applyManagedCodexSleepTool). They are the composer's own structured
// emission, never operator text: model is the thread default, the next
// two are the module's own transports (systems/codex/policy.go
// knownConfigKeys: "Two keys are this module's own transports"), and the
// sleep-tool key registers clock.sleep with the one value the composer
// writes. None of them is a posture key (approval_policy, sandbox_mode,
// sandbox_permissions). The value is pinned too: a TOML basic string,
// the composer's %q spelling, and for the sleep tool exactly its value.
var codexLockedHostConfigKeys = map[string]string{
	"model":                    "",
	"model_reasoning_effort":   "",
	"service_tier":             "",
	"features.sleep_tool.mode": `"always_on"`,
}

// LockedCodexHostArgvRefused classifies a composed managed-host argv under
// an engaged lock. The host argv is not an operator tail: the composer
// writes it as `[-c KEY="VALUE"]... app-server` from structured inputs.
// Anything else — another key, an unquoted or tampered value, any other
// flag, a missing subcommand, or any token after it — is not the
// composer's emission, so a stale or forged plan refuses here, naming the
// token (the key, never its value).
func LockedCodexHostArgvRefused(argv []string) string {
	return lockedCodexHostArgvRefused(argv, false)
}

// A restarted host also carries the manager's exact native-goals disable
// emitted after initial admission. Only that fixed value is permitted.
func lockedCodexHostArgvRefused(argv []string, restarted bool) string {
	for index := 0; index < len(argv); index++ {
		token := argv[index]
		if token == codexAppServerSubcommand {
			if index+1 < len(argv) {
				name, _ := lockedCodexFlagName(argv[index+1])
				return lockedTailTokenDisplay(name)
			}
			return ""
		}
		var override string
		switch {
		case token == "-c" || token == "--config":
			if index+1 >= len(argv) {
				return token
			}
			index++
			override = argv[index]
		case strings.HasPrefix(token, "--config="):
			override = strings.TrimPrefix(token, "--config=")
		default:
			name, _ := lockedCodexFlagName(token)
			return lockedTailTokenDisplay(name)
		}
		key, value, ok := strings.Cut(override, "=")
		if !ok {
			return "-c"
		}
		if restarted && key == codexGoalsFeatureKey && value == "false" {
			continue
		}
		want, known := codexLockedHostConfigKeys[key]
		if !known {
			return "-c " + key
		}
		if want != "" && value != want {
			return "-c " + key
		}
		if len(value) < 2 || !strings.HasPrefix(value, `"`) || !strings.HasSuffix(value, `"`) {
			return "-c " + key
		}
	}
	return codexAppServerSubcommand
}
