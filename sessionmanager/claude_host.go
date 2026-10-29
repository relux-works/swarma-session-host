package sessionmanager

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/relux-works/skill-agents-management/pkg/agentic"
	curatorenv "github.com/relux-works/swarma-session-host/sessionmanager/runner/environment"
)

const (
	ClaudeSessionLaunchContract = "task-board.claude-session-launch-v1"
	claudeHostSnapshotContract  = "task-board.claude-provider-host-v1"

	// ClaudeErrorCuratorProvenanceUnreadable refuses a claude launch
	// whose plan carries Curator fragment sources but no verifiable
	// provenance snapshot: missing, duplicated, malformed, or
	// inconsistent. The lock state is unknown, so the launch refuses
	// fail-closed rather than running beside it. Genuine legacy plans —
	// no Curator sources at all — pass through untouched.
	ClaudeErrorCuratorProvenanceUnreadable = "claude_curator_provenance_unreadable"

	// claudeGoalMinimumVersion is the first Claude Code release with the
	// native session-scoped /goal command; mirrors the spawn-path gate.
	claudeGoalMinimumVersion = "2.1.139"

	claudeHostReadyTimeout = 45 * time.Second
	claudeHostAckTimeout   = 30 * time.Second

	// claudeHostPostReadySettleDelay is how long Launch waits after the ready
	// banner is seen before handing the host back to its caller, who binds
	// the native goal directive immediately. The banner rendering is not the
	// same fact as the TUI being ready to reliably process bracketed-paste
	// input: Claude Code 2.1.268 keeps doing asynchronous startup work after
	// the banner appears (an "/rc connecting..." indicator, a weekly-usage
	// check), and a directive submitted into that window is silently lost
	// rather than acknowledged or rejected -- it just never renders anything,
	// so the caller sees a 30s ack timeout with no diagnostic.
	// Live-measured: a 3s wait after the banner reliably avoids the race; this
	// keeps a safety margin above that single measured data point. Neither
	// this delay nor claudeHostReadyTimeout consumes the other's budget --
	// this runs strictly after readiness already resolved, and strictly
	// before the caller starts the separate claudeHostAckTimeout clock.
	claudeHostPostReadySettleDelay = 4 * time.Second

	// claudeGoalDeferredGrace is how long a deferred reflection still waits
	// synchronously before handing its waiter back. A session that is free
	// acknowledges in well under a second (measured ~280ms on this board), so
	// this window keeps the ordinary background caller on the synchronous
	// applied path while a genuinely blocked tool shell — which cannot answer
	// until its own call returns — falls through almost immediately.
	claudeGoalDeferredGrace  = 2 * time.Second
	claudeHostStopGrace      = 3 * time.Second
	claudeHostPreflightLimit = 60 * time.Second
	claudeHostSubmitDelay    = 250 * time.Millisecond

	// claudeContextForkVerifyTimeout bounds how long the manager waits for the
	// provider to commit its forked transcript. The measured commit latency after
	// the flush submission is a couple of seconds on claude 2.1.220; this leaves
	// generous room for a loaded machine while still failing rather than hanging
	// when the provider never writes a fork at all.
	claudeContextForkVerifyTimeout = 60 * time.Second
	claudeContextForkPollInterval  = 250 * time.Millisecond

	// claudeForkStreamDrainGrace bounds the wait for the provider's output stream
	// to end after the process has been observed to exit. The provider prints its
	// resume refusal immediately before exiting, so the exit observation routinely
	// wins the race against the reader that still has that line buffered, and a
	// classification taken at that instant reads a refusal as an unproven fork.
	// The wait is on the reader finishing, not on a duration: once the PTY master
	// reports the end of the stream every byte the provider printed has already
	// been handed to the scanner. The grace only bounds the pathological case
	// where something other than the provider still holds the slave open, and it
	// resolves it the way an unread stream deserves — as no refusal observed.
	claudeForkStreamDrainGrace = 5 * time.Second

	// claudeContextForkFlushCommand is the local command the manager submits to
	// make the provider commit its forked session. It reports context usage and
	// runs no model turn, which is the whole reason it is this command and not a
	// prompt: verification must not spend the run's tokens or add to its history.
	claudeContextForkFlushCommand = "/context"
	claudeHostReplayLimit         = 64 * 1024
	claudeOwnerOutputLimit        = 1 << 20

	claudeWorkspaceTrustGuidance = "Claude requires an accepted workspace trust dialog; run claude once in this project and accept trust first"

	// claudeMissingSessionMarker is what the provider prints, lowercased, when a
	// --resume target does not exist in the directory it was launched from. It is
	// the observable half of the cwd-scoping constraint: a Claude session is
	// discoverable only from the directory it was created in, and every other
	// directory gets this message and an immediate exit.
	claudeMissingSessionMarker = "no conversation found with session id"

	claudeMissingSessionGuidance = "Claude could not find the session this launch resumed: " +
		"a Claude session is discoverable only from the working directory it was created in, so either " +
		"the session does not exist or it was materialized into a project directory this provider " +
		"version does not read"

	// claudeBypassPermissionsWarningMarker is the confirmation screen Claude
	// Code renders before its normal ready banner when launched with
	// --dangerously-skip-permissions: "WARNING: Claude Code running in Bypass
	// Permissions mode" ... "> No, exit" / "  Yes, I accept". The flag is
	// already this manager's own explicit declaration of consent to
	// unattended tool execution; a second interactive
	// confirmation for the identical consent is exactly the kind of prompt a
	// headless launch must answer itself, not wait out. "No, exit" is the
	// default-highlighted option, so the manager must navigate down before
	// confirming.
	claudeBypassPermissionsWarningMarker = "yes, i accept"

	// claudeGoalTokenPrefix marks the revision token that leads every native
	// goal payload. The token, not the (possibly shared) provider condition,
	// is the acknowledgement identity: parent alignment revisions all carry
	// the same stable ledger condition, so a condition-derived key would give
	// revision N the same acknowledgement as revision N-1 and let an old-goal
	// repaint satisfy a new revision's waiter.
	claudeGoalTokenPrefix = "tbg-"

	// claudeGoalTokenHashBytes sizes the revision token at 32 hex characters:
	// long enough that a different revision's token cannot reach the token
	// coverage threshold even under tolerant subsequence matching, short
	// enough to survive inside one rendered acknowledgement line.
	claudeGoalTokenHashBytes = 16

	// claudeGoalTokenCoverageThreshold is the ordered-subsequence coverage a
	// rendered token must reach to acknowledge its revision. It sits above the
	// general condition threshold because the token is the revision identity:
	// live repaint corruption drops only a few percent of characters (~94%
	// observed coverage), while a different revision's token — random hex
	// sharing only the fixed prefix — stays far below this bar.

	// claudeGoalLabelOpen and claudeGoalLabelClose bracket the human-readable
	// board goal identity that follows the token in the delivered payload. An
	// operator watching the live Claude goal line sees which board goal and
	// which revision the session is bound to instead of an opaque token; the
	// bracket also keeps the label visually separate from the evaluator
	// condition that follows it.
	claudeGoalLabelOpen  = "[board goal "
	claudeGoalLabelClose = "]"

	// claudeGoalLabelReferenceMarker introduces the optional objective
	// excerpt. It states, inside the payload the evaluator reads, that the
	// excerpt is reference text and not part of the stop condition: the
	// board objective ("deliver EPIC-... and publish v4") is deliberately NOT
	// the Claude Stop-hook condition, because it would block the orchestrator
	// from ending any turn until the whole epic is done.
	claudeGoalLabelReferenceMarker = " - reference only, not the condition - objective "

	// claudeGoalObjectiveExcerptLimit bounds the objective excerpt in runes.
	// The excerpt is a courtesy for the human reading the goal line; the
	// token leads the payload, so no excerpt length can push the
	// acknowledgement identity out of the rendered line.
	claudeGoalObjectiveExcerptLimit = 80
)

// Public capability-failure codes the control plane may expose to a wrapper.
const (
	ClaudeErrorUnavailable        = "claude_goal_unavailable"
	ClaudeErrorUnsupportedVersion = "claude_goal_unsupported_version"
	ClaudeErrorWorkspaceUntrusted = "claude_goal_workspace_untrusted"
	ClaudeErrorPreflightFailed    = "claude_goal_preflight_failed"
	ClaudeErrorResumeFailed       = "claude_session_resume_failed"
	ClaudeErrorLaunchArgsBlocked  = "claude_launch_args_unsupported"
	ClaudeErrorAdoptionInvalid    = "claude_session_adoption_invalid"
)

// claudeManagedFlagKind classifies how an allowed managed-host flag consumes
// argv tokens, mirroring the provider's commander parsing closely enough that
// a flag value can never be misread as positional prompt text.
type claudeManagedFlagKind int

const (
	// claudeFlagBool takes no value; an attached "=value" fails closed.
	claudeFlagBool claudeManagedFlagKind = iota
	// claudeFlagValue requires exactly one value ("--flag value" or
	// "--flag=value"). A following token that looks like another flag is a
	// missing value and fails closed.
	claudeFlagValue
	// claudeFlagVariadic consumes every following token up to the next
	// dash-prefixed token ("--add-dir a b"), like commander variadic options.
	// The attached "--flag=value" form ends the variadic list in the
	// provider's parser (probe-verified on claude 2.1.219: with
	// "--mcp-config=ok.json bad.json mcp list" the trailing tokens dispatch
	// as prompt/subcommand instead of loading as configs, while the
	// space-separated form loads every token as a config), so a bare token
	// after an attached value is positional prompt text and fails closed.
	claudeFlagVariadic
	// claudeFlagOptionalValue accepts the bare form, "=value", or one
	// following non-dash token ("--debug api"), mirroring commander
	// optional-value consumption (probe-verified on claude 2.1.219:
	// "--debug api mcp list" still dispatches the mcp subcommand, and
	// "--prompt-suggestions mcp" rejects "mcp" as an invalid flag value —
	// both consume the following token, so the provider can never read it
	// as prompt text).
	claudeFlagOptionalValue
)

// claudeAllowedManagedArgs is the total allowlist of the managed claude-pty
// argument set: inert interactive policy/config flags that neither select a
// different session, change who owns the terminal lifecycle, nor start work
// before the native goal bind. Everything outside this table fails closed.
var claudeAllowedManagedArgs = map[string]claudeManagedFlagKind{
	// Session policy.
	"--model":                              claudeFlagValue,
	"--effort":                             claudeFlagValue,
	"--permission-mode":                    claudeFlagValue,
	"--dangerously-skip-permissions":       claudeFlagBool,
	"--allow-dangerously-skip-permissions": claudeFlagBool,
	// Tool and MCP configuration.
	"--allowedTools":      claudeFlagVariadic,
	"--allowed-tools":     claudeFlagVariadic,
	"--disallowedTools":   claudeFlagVariadic,
	"--disallowed-tools":  claudeFlagVariadic,
	"--tools":             claudeFlagVariadic,
	"--mcp-config":        claudeFlagVariadic,
	"--strict-mcp-config": claudeFlagBool,
	// Context, prompts, and settings.
	"--add-dir":              claudeFlagVariadic,
	"--settings":             claudeFlagValue,
	"--setting-sources":      claudeFlagValue,
	"--system-prompt":        claudeFlagValue,
	"--append-system-prompt": claudeFlagValue,
	"--agent":                claudeFlagValue,
	"--agents":               claudeFlagValue,
	"--plugin-dir":           claudeFlagValue,
	"--plugin-url":           claudeFlagValue,
	"--file":                 claudeFlagVariadic,
	"--betas":                claudeFlagVariadic,
	"--exclude-dynamic-system-prompt-sections": claudeFlagBool,
	// Presentation and diagnostics.
	"--name":               claudeFlagValue,
	"-n":                   claudeFlagValue,
	"--verbose":            claudeFlagBool,
	"--debug":              claudeFlagOptionalValue,
	"-d":                   claudeFlagOptionalValue,
	"--debug-file":         claudeFlagValue,
	"--ax-screen-reader":   claudeFlagBool,
	"--prompt-suggestions": claudeFlagOptionalValue,
	"--brief":              claudeFlagBool,
	"--bare":               claudeFlagBool,
	"--safe-mode":          claudeFlagBool,
	"--no-chrome":          claudeFlagBool,
}

// claudeBlockedManagedArgs names the known-dangerous flags with a specific
// refusal reason. They are a diagnostic courtesy on top of the allowlist —
// removal from this table would still fail closed as an unknown token.
var claudeBlockedManagedArgs = map[string]string{
	// Session identity and selection.
	"--session-id":             "selects its own session identity",
	"--resume":                 "resumes a provider-selected session",
	"-r":                       "resumes a provider-selected session",
	"--continue":               "continues the most recent session instead of the managed one",
	"-c":                       "continues the most recent session instead of the managed one",
	"--fork-session":           "forks the session identity on resume",
	"--from-pr":                "resumes or picks a session linked to a pull request",
	"--no-session-persistence": "disables the session persistence required for --resume recovery",
	// Non-interactive modes.
	"--print":                    "replaces the interactive session with non-interactive print mode",
	"-p":                         "replaces the interactive session with non-interactive print mode",
	"--output-format":            "configures non-interactive print output",
	"--input-format":             "configures non-interactive streaming input",
	"--json-schema":              "configures non-interactive structured output",
	"--replay-user-messages":     "configures non-interactive stream-json echoing",
	"--include-partial-messages": "configures non-interactive stream-json output",
	"--include-hook-events":      "configures non-interactive stream-json output",
	"--forward-subagent-text":    "configures non-interactive stream-json output",
	"--fallback-model":           "configures print-mode model fallback",
	"--max-budget-usd":           "configures print-mode budget enforcement",
	// Lifecycle and terminal ownership.
	"--bg":                                 "starts a background agent outside the manager-owned PTY",
	"--background":                         "starts a background agent outside the manager-owned PTY",
	"--worktree":                           "moves the session into a provider-created git worktree",
	"-w":                                   "moves the session into a provider-created git worktree",
	"--tmux":                               "moves the session into a provider-created tmux terminal",
	"--remote-control":                     "opens a remote-control channel outside the manager attach model",
	"--remote-control-session-name-prefix": "configures the remote-control channel",
	"--ide":                                "wires the session to an external IDE client",
	"--chrome":                             "wires the session to an external browser client",
	// Capability surface.
	"--disable-slash-commands": "can disable the native /goal command required for goal binding",
	// One-shot invocations.
	"--version": "replaces the interactive session with a one-shot invocation",
	"-v":        "replaces the interactive session with a one-shot invocation",
	"--help":    "replaces the interactive session with a one-shot invocation",
	"-h":        "replaces the interactive session with a one-shot invocation",
}

func claudeManagedArgvError(arg, reason string) error {
	return &PublicError{
		Code: ClaudeErrorLaunchArgsBlocked,
		Message: fmt.Sprintf(
			"managed Claude launch argument %q %s; the Session Manager owns the interactive session identity",
			arg,
			reason,
		),
	}
}

// ValidateClaudeManagedArgv is a total classifier over the managed claude-pty
// argv: every token must be an allowlisted inert policy/config flag (with its
// value tokens explicitly consumed) or the whole launch fails closed. Session
// selectors, background/external-terminal modes, non-interactive modes,
// subcommands, positional prompt text, and unknown tokens are all rejected —
// a positional prompt would start ordinary work as soon as the PTY launches,
// before the manager reconciles the native owner goal. Initial prompt
// forwarding is deliberately not a managed-launch feature; a prompt would
// have to arrive through a post-bind client channel instead. Both the
// wrapper (fast feedback) and the provider host (authority) enforce this.
func ValidateClaudeManagedArgv(argv []string) error {
	for index := 0; index < len(argv); index++ {
		arg := argv[index]
		if arg == "--" {
			return claudeManagedArgvError(arg, "forwards positional prompt text that would start work before the native goal bind")
		}
		flag, _, hasAttachedValue := strings.Cut(arg, "=")
		if reason, blocked := claudeBlockedManagedArgs[flag]; blocked {
			return claudeManagedArgvError(arg, reason)
		}
		kind, allowed := claudeAllowedManagedArgs[flag]
		if !allowed {
			if strings.HasPrefix(arg, "-") && arg != "-" {
				return claudeManagedArgvError(arg, "is not in the supported managed launch argument set")
			}
			return claudeManagedArgvError(arg, "is positional prompt text or a subcommand that would start work before the native goal bind")
		}
		switch kind {
		case claudeFlagBool:
			if hasAttachedValue {
				return claudeManagedArgvError(arg, "does not take a value")
			}
		case claudeFlagOptionalValue:
			if hasAttachedValue {
				continue
			}
			if index+1 < len(argv) && !strings.HasPrefix(argv[index+1], "-") {
				index++
			}
		case claudeFlagValue:
			if hasAttachedValue {
				continue
			}
			if index+1 >= len(argv) || strings.HasPrefix(argv[index+1], "-") {
				return claudeManagedArgvError(arg, "is missing its required value")
			}
			index++
		case claudeFlagVariadic:
			if hasAttachedValue {
				if index+1 < len(argv) && !strings.HasPrefix(argv[index+1], "-") {
					return claudeManagedArgvError(
						argv[index+1],
						fmt.Sprintf(
							"follows the attached-value form %q, which ends %s's variadic list in Claude's parser, so it would launch as positional prompt text before the native goal bind (pass space-separated values: %s value value)",
							arg, flag, flag,
						),
					)
				}
				continue
			}
			consumed := 0
			for index+1 < len(argv) && !strings.HasPrefix(argv[index+1], "-") {
				index++
				consumed++
			}
			if consumed == 0 {
				return claudeManagedArgvError(arg, "is missing its required value")
			}
		}
	}
	return nil
}

// refuseLockedClaudeLaunch is the daemon-side locked-permissions posture
// gate for locked-safe native flags, the defense-in-depth
// twin of the daemon-side Muse rebuild refusal: when the plan's
// provenance snapshot says locked and the composed argv leaves the
// locked-safe set, refuse with the lock code — naming the flag — before
// the allowlist, the probe, or any host work. The composer gate should
// have refused first; a stale, forged, or pre-fix plan that reaches the
// host refuses here anyway, through the one shared tail classifier, so
// the two gates cannot drift.
//
// A plan that carries Curator fragment sources but no verifiable
// provenance snapshot refuses fail-closed first, before the allowlist
// and the probe: the lock state is unknown, and the fresh-launch route
// has no identity gate to refuse it later. Only genuine legacy absence —
// no Curator sources at all — passes through, plus permissionless and
// foreign snapshots, which keep their existing gates. A valid locked
// snapshot always carries mode native (the module refuses yolo+locked at
// decode), so no rank comparison is needed: any refused tail flag is
// outside the locked session's allowed set.
//
// The curator-MCP emission skip below is enabled ONLY when the
// snapshot's MCP channel is exactly the module's own declared claude
// emission (claudeCuratorMCPEmissionDeclared): the module refuses any
// other descriptor at plan-build time, so a triple over a non-module
// descriptor is forged or stale and refuses here. A matching path
// alone never enables the skip.
func refuseLockedClaudeLaunch(plan *PrimarySessionLaunchPlan) error {
	if plan == nil {
		return nil
	}
	provenance, present, err := extractCuratorProvenance(plan.Sources)
	if err != nil {
		return &curatorIdentityReadError{
			public: &PublicError{
				Code:    ClaudeErrorCuratorProvenanceUnreadable,
				Message: "the plan carries Curator fragment sources but its provenance snapshot cannot be verified; refusing the launch rather than running beside an unknown lock state",
			},
			cause: err,
		}
	}
	if !present {
		return nil
	}
	if provenance.Fragment.Environment != "claude_code" {
		return nil
	}
	permissions := provenance.Fragment.Permissions
	if permissions == nil || !permissions.Locked {
		return nil
	}
	curatorMCPPath := ""
	if mcp := provenance.Fragment.MCP; claudeCuratorMCPEmissionDeclared(mcp) {
		curatorMCPPath = mcp.Path
	}
	if flag := LockedClaudePlanArgvRefusedFlag(plan.LaunchVariants.ManagedHost.Argv, curatorMCPPath); flag != "" {
		detail := fmt.Sprintf("permissions locked by source %q to mode %q (runtime %q): locked-tail flag %q is outside the locked session's allowed set; unlock the fragment or run the interactive session without the bypass flags", permissions.Source, permissions.Mode, provenance.Fragment.Environment, flag)
		return &PublicError{Code: ClaudeFragmentPermissionsLocked, Message: detail}
	}
	return nil
}

// claudeCuratorMCPEmissionDeclared reports whether the snapshot's MCP
// channel is exactly the emission the vendored claude plugin declares:
// one flag-kind channel carrying --mcp-config with a path argument plus
// the single --strict-mcp-config companion (systems/claude/context.go,
// applyClaudeCuratorContext). It mirrors that predicate conjunct for
// conjunct — anything the module rejects there cannot be the module's
// own deterministic emission here. Generic provenance validation
// already guaranteed exactly one well-formed channel; the length check
// fails closed regardless.
func claudeCuratorMCPEmissionDeclared(mcp *agentic.CuratorMCPContext) bool {
	if mcp == nil || len(mcp.Channels) != 1 {
		return false
	}
	descriptor := mcp.Channels[0]
	return descriptor.Kind == agentic.CuratorDescriptorFlag &&
		descriptor.Flag == "--mcp-config" &&
		descriptor.Argument == agentic.CuratorArgumentPath &&
		len(descriptor.With) == 1 &&
		descriptor.With[0] == "--strict-mcp-config"
}

// ClaudeErrorManagedHomeInvalid refuses a Curator home before any provider
// process starts. Paths stay private; the profile identifies the bad selection.
const ClaudeErrorManagedHomeInvalid = "claude_managed_home_invalid"

func claudeLaunchManagedHome(plan *PrimarySessionLaunchPlan) (string, error) {
	provenance, present, err := decodeCuratorProvenance(plan.Sources)
	if err != nil || !present {
		// Keep the established typed provenance/lock refusal and precedence.
		return "", refuseLockedClaudeLaunch(plan)
	}
	if provenance.Fragment.Environment != "claude_code" {
		return "", refuseLockedClaudeLaunch(plan)
	}
	refuse := func(reason string) (string, error) {
		return "", &PublicError{Code: ClaudeErrorManagedHomeInvalid,
			Message: fmt.Sprintf("Claude managed home for profile %q %s", provenance.ProfileName, reason)}
	}
	home := provenance.ManagedHome
	if home == "" {
		return refuse("is empty")
	}
	if !filepath.IsAbs(home) {
		return refuse("must be absolute")
	}
	if provenance.ManagedHomeVariable != "CLAUDE_CONFIG_DIR" || provenance.Fragment.Env["CLAUDE_CONFIG_DIR"] != home {
		return refuse("does not match its CLAUDE_CONFIG_DIR provenance")
	}
	if err := refuseLockedClaudeLaunch(plan); err != nil {
		return "", err
	}
	info, err := os.Stat(home)
	if err != nil {
		return refuse("cannot be read as an existing directory")
	}
	if !info.IsDir() {
		return refuse("is not a directory")
	}
	return home, nil
}

func applyClaudeManagedHome(environment []string, home string) []string {
	if home == "" {
		return environment
	}
	return replaceEnvironmentValue(environment, "CLAUDE_CONFIG_DIR", home)
}

// claudeManagedHomeEnvironment leaves Curator-owned homes untouched. The
// explicit record path survives provider tools, resume and context forks.
func claudeManagedHomeEnvironment(sessionDir, managedHome string, environment []string) ([]string, error) {
	if managedHome == "" {
		return environment, nil
	}
	record, err := curatorenv.CaptureClaudeHome(sessionDir, managedHome)
	if err != nil {
		return nil, &curatorIdentityReadError{public: &PublicError{Code: curatorenv.NativeHomeUnknown, Message: curatorenv.NativeHomeGuidance}, cause: err}
	}
	result := applyClaudeManagedHome(environment, managedHome)
	return replaceEnvironmentValue(result, curatorenv.ClaudeCaptureEnv, record), nil
}

// claudeSessionUUIDPattern matches the canonical lowercase 8-4-4-4-12 form
// Claude Code uses for persisted session identities.
var claudeSessionUUIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

// NormalizeClaudeSessionUUID canonicalizes an adoption identity and fails
// closed on anything that is not a plain session UUID, so free-form text can
// never ride the resume channel into the provider argv.
func NormalizeClaudeSessionUUID(raw string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if !claudeSessionUUIDPattern.MatchString(normalized) {
		return "", &PublicError{
			Code: ClaudeErrorAdoptionInvalid,
			Message: fmt.Sprintf(
				"%q is not a Claude session UUID (expected the 8-4-4-4-12 hex form, e.g. 123e4567-e89b-42d3-a456-426614174000)",
				strings.TrimSpace(raw),
			),
		}
	}
	return normalized, nil
}

type ClaudeHostFactory struct {
	layout            Layout
	readyTimeout      time.Duration
	ackTimeout        time.Duration
	submitDelay       time.Duration
	forkVerifyTimeout time.Duration
	postReadySettle   time.Duration
	// beforePump is a test synchronization observer for the startup ordering
	// boundary. It cannot alter host construction and is never configured by
	// production callers.
	beforePump func(readyWaiters int)
}

// NewClaudeHostFactoryWithGoalAckTimeout constructs the production factory
// with the strictly validated project budget resolved by the daemon entry
// point. Keeping validation outside this package prevents a provider host from
// silently replacing an invalid configured value with its default.
func NewClaudeHostFactoryWithGoalAckTimeout(layout Layout, timeout time.Duration) *ClaudeHostFactory {
	factory := NewClaudeHostFactory(layout)
	factory.ackTimeout = timeout
	return factory
}

func NewClaudeHostFactory(layout Layout) *ClaudeHostFactory {
	return &ClaudeHostFactory{
		layout:            layout,
		readyTimeout:      claudeHostReadyTimeout,
		ackTimeout:        claudeHostAckTimeout,
		submitDelay:       claudeHostSubmitDelay,
		forkVerifyTimeout: claudeContextForkVerifyTimeout,
		postReadySettle:   claudeHostPostReadySettleDelay,
	}
}

func (f *ClaudeHostFactory) Provider() string { return "claude" }

func (f *ClaudeHostFactory) Launch(
	ctx context.Context,
	request ProviderLaunchRequest,
) (hostResult ProviderHost, err error) {
	launchRequestedAt := time.Now().UTC()
	// A prior record is either this session's own provider handle, which is
	// resumed, or an immutable materialization fork source, which is forked. They
	// are told apart by contract rather than by shape: resuming a fork source
	// would append into a shared base, which is the one thing a base must never
	// suffer.
	//
	// PriorMustFork is the manager saying this record is a fork source, and it is
	// fail-closed. A ready materialization carries the writable child's host
	// snapshot rather than a fork plan, so without this gate the decode would fall
	// through to the recovery branch, take that snapshot's identity, and emit a bare
	// --resume against a registry base. That is an ordinary-call-path escape from
	// mandatory fork-per-run, so a required fork accepts nothing but a valid fork
	// plan and refuses before any provider I/O.
	//
	// The gate is evaluated on the flag alone, before the prior record is inspected
	// at all. An earlier revision nested it inside "a prior record exists and
	// carries bytes", so the one shape it was least able to defend — a required
	// fork whose source is nil or empty — skipped the gate entirely, minted a fresh
	// session UUID and launched cold without --resume --fork-session. A missing
	// fork source is not a licence to start a session that inherits nothing; it is
	// the absence of the thing the launch was required to fork, and it is refused
	// here for the same reason a malformed plan is.
	var prior *claudeHostSnapshot
	var forkPlan *claudeContextForkPlan
	switch {
	case request.PriorMustFork:
		if request.Prior == nil {
			return nil, claudeForkPlanFailure(errors.New(
				"this launch carries no prior record at all"))
		}
		plan, planErr := decodeClaudeForkPlan(*request.Prior)
		if planErr != nil {
			return nil, claudeForkPlanFailure(planErr)
		}
		forkPlan = plan
	case request.Prior != nil:
		if plan, planErr := decodeClaudeForkPlan(*request.Prior); planErr == nil {
			forkPlan = plan
		} else {
			decoded, err := decodeClaudeHostSnapshot(request.Prior.Snapshot)
			if err != nil {
				return nil, errors.Join(planErr, err)
			}
			prior = decoded
		}
	}

	// A typed provider payload cannot compete with or be silently replaced by
	// another provider's payload, including when recovering an admitted prior.
	if err := validateLaunchPayload("claude", request.Launch); err != nil {
		return nil, err
	}
	closedLaunch := copyLaunchSpec(request.Launch)
	if closedLaunch.Claude == nil && prior != nil {
		if err := validateLaunchPayload("claude", prior.Plan); err != nil {
			return nil, err
		}
		closedLaunch = copyLaunchSpec(prior.Plan)
	}
	launch := closedLaunch.Claude
	if launch == nil {
		return nil, fmt.Errorf("Claude session launch payload is required")
	}
	if launch.Contract != ClaudeSessionLaunchContract {
		return nil, fmt.Errorf("unsupported Claude session launch contract %q", launch.Contract)
	}
	if err := ValidatePrimarySessionLaunchPlan(&launch.Plan, "claude"); err != nil {
		return nil, err
	}
	if strings.TrimSpace(launch.ResumeSessionUUID) != "" {
		normalized, normalizeErr := NormalizeClaudeSessionUUID(launch.ResumeSessionUUID)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		launch.ResumeSessionUUID = normalized
	}
	// The typed envelope records the executable/cwd/argv that actually ran.
	closedLaunch.Executable = launch.Plan.Executable
	closedLaunch.Cwd = launch.Plan.ProjectDir
	closedLaunch.Variants = launch.Plan.LaunchVariants
	closedLaunch.Sources = copyLaunchSources(launch.Plan.Sources)
	if launch.Plan.LaunchVariants.ManagedHost.Kind != "claude-pty" {
		return nil, fmt.Errorf("Claude launch plan managed_host.kind must be claude-pty")
	}
	for _, name := range launch.Plan.RequiredEnvNames {
		present := false
		for _, entry := range closedLaunch.Environment {
			if strings.HasPrefix(entry, name+"=") {
				present = true
				break
			}
		}
		if !present {
			return nil, fmt.Errorf("required launch environment %s is not set", name)
		}
	}
	// Locked-permissions posture gate before the argv allowlist, the
	// capability probe, and any host work: under a lock the lock refusal
	// wins over an allowlist refusal when a wider form is present, the
	// same ordering the composer gate keeps.
	managedHome, err := claudeLaunchManagedHome(&launch.Plan)
	if err != nil {
		return nil, err
	}
	if err := ValidateClaudeManagedArgv(launch.Plan.LaunchVariants.ManagedHost.Argv); err != nil {
		return nil, err
	}

	// Capture before replacing the native selector. Every launch, resume and
	// materialized fork uses the same environment and session-state record.
	launchEnvironment, err := claudeManagedHomeEnvironment(filepath.Join(f.layout.SessionsDir, request.SessionID), managedHome, claudeHostEnvironment(launch.Term, closedLaunch.Environment))
	if err != nil {
		return nil, err
	}

	// Capability gate before any provider work: the manager never silently
	// degrades a required native goal to a prompt-only session.
	activeProviderVersion, err := claudePreflightGoalCapability(
		ctx, launch.Plan.Executable, launch.Plan.ProjectDir, managedHome, launchEnvironment,
	)
	if err != nil {
		return nil, err
	}

	sessionUUID := ""
	resume := false
	adopted := false
	var forked *claudeForkMaterialization
	if forkPlan != nil {
		if strings.TrimSpace(launch.ResumeSessionUUID) != "" {
			return nil, &PublicError{
				Code: claudeContextMaterializeError,
				Message: "this launch both inherits a materialized context and names a session to adopt; " +
					"adopting a provider-selected session would discard the fork, so the conflict is refused " +
					"rather than resolved by precedence",
			}
		}
		materialized, err := materializeClaudeContextFork(
			forkPlan, f.layout, launch.Plan.ProjectDir, activeProviderVersion, time.Now,
		)
		if err != nil {
			return nil, err
		}
		defer func() {
			// Ownership of the lease moves to the host once it is running. Until
			// then this releases it, so a refused launch never strands the
			// materialization behind a lease nobody holds.
			if forked != nil && forked.lease != nil {
				forked.lease.release()
				if recordErr := forked.recordReclamation(
					f.layout, claudeReclamationPendingGC, "launch_failed", time.Now,
				); recordErr != nil {
					err = errors.Join(err, fmt.Errorf(
						"recording retained Claude launch artifacts for reclamation: %w", recordErr,
					))
				}
			}
		}()
		forked = materialized
		// The launch's own identity is the writable child, not the copy it
		// resumes. Persisting the copy's identity here is what previously let a
		// restart bare-resume a fork source and append into it.
		sessionUUID = materialized.ForkSessionUUID
		resume = true
	}
	var priorLease *claudeContextLease
	if prior != nil && strings.TrimSpace(prior.SessionUUID) != "" {
		// Recoverable process loss: terminate any orphaned prior process and
		// resume the same logical Claude session identity.
		terminateClaudeProcess(prior.PID, prior.SessionUUID)
		if ctxID := strings.TrimSpace(prior.ContextCtxID); ctxID != "" {
			// A recovered context session is still one writer against one
			// materialization. Without re-acquiring the lease, recovery would be
			// the one path that admits a second writer.
			lease, leaseErr := acquireClaudeContextWriterLease(f.layout, ctxID, time.Now)
			if leaseErr != nil {
				return nil, leaseErr
			}
			priorLease = lease
			defer func() {
				if priorLease != nil {
					priorLease.release()
				}
			}()
		}
		sessionUUID = prior.SessionUUID
		resume = true
		adopted = prior.Adopted
	}
	if adoption := strings.TrimSpace(launch.ResumeSessionUUID); adoption != "" {
		// Explicit adoption intent wins: the manager admits an adoption attach
		// only when the owner has no live session, so any differing prior
		// identity here is an already-terminated orphan record.
		normalized, err := NormalizeClaudeSessionUUID(adoption)
		if err != nil {
			return nil, err
		}
		sessionUUID = normalized
		resume = true
		adopted = true
	}
	if sessionUUID == "" {
		generated, err := newClaudeSessionUUID()
		if err != nil {
			return nil, err
		}
		sessionUUID = generated
	}

	argv := append([]string(nil), launch.Plan.LaunchVariants.ManagedHost.Argv...)
	switch {
	case forked != nil:
		// A materialized context is always forked, never resumed in place: a bare
		// resume appends the new turn into the file the fork source lives in. The
		// writable child is named here so the identity in the catalog is the one
		// the provider will actually write.
		argv = append(argv, claudeForkArgv(forked.ResumeSessionUUID, forked.ForkSessionUUID)...)
	case resume:
		argv = append(argv, "--resume", sessionUUID)
	default:
		argv = append(argv, "--session-id", sessionUUID)
	}

	isolatedArgv, builderToolIsolation, err := builderIsolatedLaunchArgv(
		f.Provider(), argv, request.BuilderConfinement,
	)
	if err != nil {
		return nil, fmt.Errorf("composing Claude context-builder tool isolation: %w", err)
	}
	commandExecutable, commandArgs, builderConfinement, err := confinedProviderCommand(
		f.Provider(),
		launch.Plan.ProjectDir,
		launch.Plan.Executable,
		isolatedArgv,
		append(
			claudeBuilderRuntimeReadRoots(managedHome),
			filepath.Join(f.layout.SessionsDir, request.SessionID, "builder-runtime"),
		),
		request.BuilderConfinement,
	)
	if err != nil {
		return nil, fmt.Errorf("preparing Claude context-builder confinement: %w", err)
	}
	command := exec.Command(commandExecutable, commandArgs...)
	command.Dir = launch.Plan.ProjectDir
	command.Env = launchEnvironment
	if builderConfinement != nil {
		sessionDir := filepath.Join(f.layout.SessionsDir, request.SessionID)
		runtimeTemp, tempErr := prepareBuilderRuntimeTemp(sessionDir)
		if tempErr != nil {
			return nil, fmt.Errorf("preparing Claude context-builder runtime temp: %w", tempErr)
		}
		command.Env = replaceEnvironmentValue(command.Env, "TMPDIR", runtimeTemp)
	}
	var builderTranscriptPath string
	var builderTranscriptOffset int64
	if request.BuilderConfinement != nil {
		builderTranscriptPath, builderTranscriptOffset, err = claudeBuilderTranscriptBoundary(
			launch.Plan.ProjectDir, sessionUUID, managedHome,
		)
		if err != nil {
			return nil, fmt.Errorf("capturing Claude context-builder transcript boundary: %w", err)
		}
	}

	rows, cols := launch.Rows, launch.Cols
	if rows == 0 || cols == 0 {
		rows, cols = 40, 120
	}
	ptmx, err := pty.StartWithSize(command, &pty.Winsize{Rows: rows, Cols: cols})
	if err != nil {
		return nil, fmt.Errorf("starting composed Claude under manager PTY: %w", err)
	}

	host := &claudeHost{
		snapshot: claudeHostSnapshot{
			Contract:    claudeHostSnapshotContract,
			SessionUUID: sessionUUID,
			PID:         command.Process.Pid,
			StartedAt:   time.Now().UTC(),
			Plan:        closedLaunch,
			Term:        launch.Term,
			Rows:        rows,
			Cols:        cols,
			Adopted:     adopted,
		},
		command:                 command,
		ptmx:                    ptmx,
		scanner:                 newClaudeOutputScanner(),
		exited:                  make(chan struct{}),
		drained:                 make(chan struct{}),
		subscribers:             make(map[chan []byte]struct{}),
		ackTimeout:              f.ackTimeout,
		launchRequestedAt:       launchRequestedAt,
		submitDelay:             f.submitDelay,
		forkVerifyTimeout:       f.forkVerifyTimeout,
		builderConfinement:      builderConfinement,
		builderToolIsolation:    builderToolIsolation,
		builderTranscriptPath:   builderTranscriptPath,
		builderTranscriptOffset: builderTranscriptOffset,
	}
	if forked != nil {
		host.snapshot.ContextCtxID = forked.ChildCtxID
		host.snapshot.ContextPrimaryCtxID = forked.PrimaryCtxID
		host.snapshot.ContextProjectSlug = forked.ProjectSlug
		host.snapshot.ContextResumeSessionUUID = forked.ResumeSessionUUID
		host.snapshot.ContextBaseSHA256 = forked.BaseSHA256
		host.snapshot.ContextMaterializedSHA256 = forked.MaterializedSHA256
		host.snapshot.ContextInjectedCtxIDs = forked.InjectedCtxIDs
		host.snapshot.ContextSplicedCtxIDs = forked.SplicedCtxIDs
		host.contextLease = forked.lease
	} else if prior != nil {
		host.snapshot.ContextCtxID = prior.ContextCtxID
		host.snapshot.ContextPrimaryCtxID = prior.ContextPrimaryCtxID
		host.snapshot.ContextProjectSlug = prior.ContextProjectSlug
		host.snapshot.ContextResumeSessionUUID = prior.ContextResumeSessionUUID
		host.snapshot.ContextBaseSHA256 = prior.ContextBaseSHA256
		host.snapshot.ContextMaterializedSHA256 = prior.ContextMaterializedSHA256
		host.snapshot.ContextInjectedCtxIDs = prior.ContextInjectedCtxIDs
		host.snapshot.ContextSplicedCtxIDs = prior.ContextSplicedCtxIDs
		host.snapshot.ContextForkVerifiedAt = prior.ContextForkVerifiedAt
		host.contextLease = priorLease
		priorLease = nil
	}
	// Arm readiness before the reader starts. The scanner intentionally never
	// lets a newly registered waiter consume old output, which is required for
	// command acknowledgements but would lose a fast provider's startup banner
	// if the pump won this scheduling race.
	readyWaiter := host.watchReady(resume)
	if f.beforePump != nil {
		host.scanner.mu.Lock()
		readyWaiters := len(host.scanner.waiters)
		host.scanner.mu.Unlock()
		f.beforePump(readyWaiters)
	}
	go host.pump()
	go host.observeExit()

	if err := host.awaitReady(ctx, f.readyTimeout, resume, readyWaiter); err != nil {
		_ = host.Stop(context.WithoutCancel(ctx))
		// The CLI ran. On a fork launch that means `--resume <copy> --fork-session`
		// was attempted and the provider did not come up on it, which is the
		// failure the version-aware fallback exists to explain and to recover from.
		return nil, claudeResumeAttemptFailed(forked, "--resume --fork-session launch", err)
	}
	if err := host.settleAfterReady(ctx, f.postReadySettle); err != nil {
		_ = host.Stop(context.WithoutCancel(ctx))
		return nil, claudeResumeAttemptFailed(forked, "post-ready settle wait", err)
	}
	if forked != nil {
		// The materialization is only trustworthy once the provider has actually
		// forked it, and "actually" here means the provider produced the writable
		// child at the path this adapter derived, carrying the copy's records.
		if err := host.verifyContextFork(ctx, forked); err != nil {
			_ = host.Stop(context.WithoutCancel(ctx))
			return nil, claudeResumeAttemptFailed(forked, "forked transcript verification", err)
		}
		// Ownership transferred: the host now releases the lease on Stop.
		forked.lease = nil
	}
	return host, nil
}

// Restore never revives a PTY: the master descriptor dies with the manager
// process. It returns a detached handle that reports the host as stopped and
// can still terminate an orphaned process, so the next Attach relaunches
// through Launch with the prior snapshot and resumes the same session.
func (f *ClaudeHostFactory) Restore(
	_ context.Context,
	record ProviderHostRecord,
) (ProviderHost, error) {
	if record.Provider != "claude" {
		return nil, ErrProviderHostNotFound
	}
	snapshot, err := decodeClaudeHostSnapshot(record.Snapshot)
	if err != nil {
		return nil, ErrProviderHostNotFound
	}
	return &claudeDetachedHost{snapshot: *snapshot}, nil
}

type claudeHostSnapshot = ClaudeRunnerSnapshot

func (h *claudeHost) NativeIdentity() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshot.SessionUUID
}

func decodeClaudeHostSnapshot(record RunnerSnapshot) (*claudeHostSnapshot, error) {
	if record.Kind != "claude" || record.Claude == nil {
		return nil, ErrProviderHostNotFound
	}
	if err := validateRunnerSnapshot(ProviderHostRecord{Provider: "claude", Snapshot: record}); err != nil {
		return nil, err
	}
	snapshot := copyRunnerSnapshot(record)
	if snapshot.Claude.Contract != claudeHostSnapshotContract || strings.TrimSpace(snapshot.Claude.SessionUUID) == "" || snapshot.Claude.PID <= 0 {
		return nil, fmt.Errorf("invalid Claude provider-host snapshot")
	}
	return snapshot.Claude, nil
}

type claudeHost struct {
	mu       sync.Mutex
	runMu    sync.Mutex
	snapshot claudeHostSnapshot
	command  *exec.Cmd
	ptmx     *os.File
	scanner  *claudeOutputScanner
	exited   chan struct{}
	// drained is closed when the PTY reader loop has ended, which is strictly
	// after every byte the provider ever printed was handed to the scanner.
	// exited only says the process is gone; drained says its output is all in.
	drained     chan struct{}
	subscribers map[chan []byte]struct{}
	replay      []byte
	stopped     bool

	// writeMu serializes every write into the PTY. ApplyBoardGoal holds it
	// for the full inject-and-acknowledge span, so attached client keystrokes
	// cannot interleave with a native goal directive.
	writeMu sync.Mutex

	// contextLease is the one-writer registration for the materialization this
	// session forked, held for the life of the process and released on Stop.
	contextLease *claudeContextLease

	ackTimeout  time.Duration
	submitDelay time.Duration
	// launchRequestedAt precedes provider_launched and exists to make the budget
	// anchor explicit and testable; it is never the acknowledgement deadline's
	// source of truth.
	launchRequestedAt time.Time
	// forkVerifyTimeout bounds the wait for the provider to commit its forked
	// transcript. Zero falls back to the package default.
	forkVerifyTimeout  time.Duration
	builderConfinement *BuilderConfinementResult
	// builderToolIsolation is the tool surface the launch was composed with. It
	// sits beside the confinement result because the two are one decision: a
	// builder confined to the right scope through an uninstrumented tool produces
	// no evidence, and evidence from an unconfined tool describes the wrong scope.
	builderToolIsolation *BuilderToolIsolationResult
	// builderTranscript* bind native provider evidence to this launch only.  A
	// resumed transcript may already contain tool calls from the inherited base;
	// the captured byte boundary excludes them from this run's observation.
	builderTranscriptPath   string
	builderTranscriptOffset int64
}

func (h *claudeHost) pump() {
	// The end of the reader is the end of the provider's observable output, and
	// classification that must not race the last line waits for it.
	defer func() {
		if h.drained != nil {
			close(h.drained)
		}
	}()
	buffer := make([]byte, 32*1024)
	for {
		n, err := h.ptmx.Read(buffer)
		if n > 0 {
			chunk := append([]byte(nil), buffer[:n]...)
			h.scanner.observe(chunk)
			h.mu.Lock()
			h.replay = append(h.replay, chunk...)
			if excess := len(h.replay) - claudeHostReplayLimit; excess > 0 {
				h.replay = append(h.replay[:0], h.replay[excess:]...)
			}
			for subscriber := range h.subscribers {
				select {
				case subscriber <- chunk:
				default:
					// A stalled client must never block goal acknowledgement
					// scanning or other clients; drop its chunk.
				}
			}
			h.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

func (h *claudeHost) observeExit() {
	if h.command == nil {
		return
	}
	_ = h.command.Wait()
	h.mu.Lock()
	h.snapshot.Stopped = true
	lease := h.contextLease
	h.contextLease = nil
	h.mu.Unlock()
	// A natural exit ends the writer just as surely as Stop does. Leaving the
	// lease held until someone calls Stop would block the next fork of this
	// materialization behind a process that is already gone.
	lease.release()
	close(h.exited)
}

func (h *claudeHost) watchReady(resumed bool) *claudeOutputWaiter {
	failures := h.claudeReadyFailureMarkers(resumed)
	return h.scanner.watch([]string{"claude code v", claudeBypassPermissionsWarningMarker}, failures)
}

// claudeReadyFailureMarkers is the failure set awaitReady's loop re-arms on
// every re-watch after answering the bypass-permissions warning, so a
// missing-session or workspace-trust failure rendered AFTER that warning is
// still caught rather than only being checked on the first pass.
func (h *claudeHost) claudeReadyFailureMarkers(resumed bool) []string {
	failures := []string{"trust this folder", "do you trust the files"}
	if resumed {
		// The provider says this out loud and then exits. Watching for it turns a
		// missing session into an immediate, named failure instead of a ready
		// timeout that reads like a slow start.
		failures = append(failures, claudeMissingSessionMarker)
	}
	return failures
}

func (h *claudeHost) awaitReady(
	ctx context.Context,
	timeout time.Duration,
	resumed bool,
	waiter *claudeOutputWaiter,
) error {
	// waiter is reassigned when the bypass-permissions warning is answered and
	// the loop re-arms a fresh waiter for the real banner; the closure below
	// captures the variable, not its value at defer-registration time, so it
	// always cancels whichever waiter is current when awaitReady returns.
	defer func() { h.scanner.cancel(waiter) }()

	deadlineFailure := &PublicError{
		Code:    ClaudeErrorPreflightFailed,
		Message: "Claude did not become ready before the manager deadline",
	}
	deadline := time.Now().Add(timeout)
	answeredBypassWarning := false
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return deadlineFailure
		}
		select {
		case match := <-waiter.done:
			if match.failure {
				if strings.Contains(strings.ToLower(match.marker), claudeMissingSessionMarker) {
					return &PublicError{
						Code:    ClaudeErrorResumeFailed,
						Message: claudeMissingSessionGuidance,
					}
				}
				return &PublicError{
					Code:    ClaudeErrorWorkspaceUntrusted,
					Message: claudeWorkspaceTrustGuidance,
				}
			}
			if match.marker == claudeBypassPermissionsWarningMarker {
				// Answer once. A second sighting (should never happen -- the
				// dialog is not re-shown mid-session -- but a marker match is a
				// fuzzy, whitespace-stripped substring test, not a screen
				// identity) falls through to the deadline rather than looping
				// forever on a screen the manager cannot get past.
				if answeredBypassWarning {
					return deadlineFailure
				}
				answeredBypassWarning = true
				if err := h.acceptBypassPermissionsWarning(ctx); err != nil {
					return err
				}
				waiter = h.scanner.watch([]string{"claude code v"}, h.claudeReadyFailureMarkers(resumed))
				continue
			}
			return nil
		case <-h.exited:
			if resumed {
				return &PublicError{
					Code:    ClaudeErrorResumeFailed,
					Message: "Claude exited before the resumed session became ready; verify the session UUID belongs to a persisted Claude session for this project",
				}
			}
			return &PublicError{
				Code:    ClaudeErrorUnavailable,
				Message: "Claude exited before the session became ready",
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(remaining):
			return deadlineFailure
		}
	}
}

// acceptBypassPermissionsWarning selects and confirms "Yes, I accept" on the
// Bypass Permissions mode confirmation screen: Down (the default-highlighted
// option is "No, exit") then Enter. This is terminal navigation, not text
// input, so it writes the raw control bytes directly rather than through the
// bracketed-paste path DeliverSessionNotice uses for typed text.
func (h *claudeHost) acceptBypassPermissionsWarning(ctx context.Context) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if _, err := h.ptmx.Write([]byte("\x1b[B")); err != nil {
		return fmt.Errorf("selecting the bypass permissions confirmation: %w", err)
	}
	select {
	case <-time.After(h.submitDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if _, err := h.ptmx.Write([]byte("\r")); err != nil {
		return fmt.Errorf("confirming the bypass permissions warning: %w", err)
	}
	return nil
}

// settleAfterReady waits out delay after the ready banner before the caller
// binds the native goal directive. See claudeHostPostReadySettleDelay for why
// the banner rendering is not the same fact as the TUI being ready to reliably
// process bracketed-paste input. It returns early, with no
// error, if the process exits during the wait -- the caller's own ack path is
// what should report that failure, not this settle step.
func (h *claudeHost) settleAfterReady(ctx context.Context, delay time.Duration) error {
	select {
	case <-time.After(delay):
		return nil
	case <-h.exited:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *claudeHost) State(context.Context) (ProviderHostState, error) {
	h.mu.Lock()
	stopped := h.stopped || h.snapshot.Stopped
	h.mu.Unlock()
	if stopped {
		return ProviderHostStopped, nil
	}
	select {
	case <-h.exited:
		return ProviderHostStopped, nil
	default:
		return ProviderHostRunning, nil
	}
}

func (h *claudeHost) Snapshot() (RunnerSnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return copyRunnerSnapshot(RunnerSnapshot{Kind: "claude", Claude: &h.snapshot}), nil
}

func (h *claudeHost) ProviderLaunchMetadata() ProviderLaunchMetadata {
	h.mu.Lock()
	defer h.mu.Unlock()
	return ProviderLaunchMetadata{
		PID:        h.snapshot.PID,
		Executable: h.snapshot.Plan.Executable,
	}
}

func (h *claudeHost) BuilderIngestionConfinement() (BuilderConfinementResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.builderConfinement == nil {
		return BuilderConfinementResult{}, fmt.Errorf(
			"Claude host was not launched under context-builder confinement",
		)
	}
	return BuilderConfinementResult{
		Mechanism:     h.builderConfinement.Mechanism,
		EnforcedRoots: append([]string(nil), h.builderConfinement.EnforcedRoots...),
	}, nil
}

// BuilderToolIsolation reports the tool surface this launch was actually
// composed with. Like the confinement report it is unavailable unless the
// process was started under it, so a host that skipped the composition cannot
// answer and the manager refuses to seal.
func (h *claudeHost) BuilderToolIsolation() (BuilderToolIsolationResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.builderToolIsolation == nil {
		return BuilderToolIsolationResult{}, fmt.Errorf(
			"Claude host was not launched under context-builder tool isolation",
		)
	}
	return BuilderToolIsolationResult{
		Mechanism:      h.builderToolIsolation.Mechanism,
		DeniedSurfaces: append([]string(nil), h.builderToolIsolation.DeniedSurfaces...),
		GatewayTools:   append([]string(nil), h.builderToolIsolation.GatewayTools...),
	}, nil
}

func (h *claudeHost) RunOwner(
	ctx context.Context,
	request ProviderRunRequest,
) (*ProviderRunResult, error) {
	if strings.TrimSpace(request.Prompt) == "" {
		return nil, fmt.Errorf("managed Claude owner prompt is required")
	}
	h.runMu.Lock()
	defer h.runMu.Unlock()

	stopCapture := h.beginRunCapture()
	finish := func(result *ProviderRunResult) (*ProviderRunResult, error) {
		result.Output = stopCapture()
		stopCapture = nil
		return result, nil
	}
	defer func() {
		if stopCapture != nil {
			_ = stopCapture()
		}
	}()

	waiter := h.scanner.watch(
		[]string{"goal achieved"},
		[]string{
			"goal could not be achieved",
			"you've hit your limit",
			"rate limit",
			"usage limit",
		},
	)
	defer h.scanner.cancel(waiter)
	if err := h.submitOwnerPrompt(ctx, request.Prompt); err != nil {
		return finish(&ProviderRunResult{
			ExitCode:    1,
			Status:      ProviderRunFailed,
			Detail:      err.Error(),
			Recoverable: true,
		})
	}

	select {
	case match := <-waiter.done:
		if match.failure {
			status := ProviderRunFailed
			if strings.Contains(match.marker, "limit") {
				status = ProviderRunUsageLimited
			}
			return finish(&ProviderRunResult{
				ExitCode:       1,
				Status:         status,
				ProviderStatus: match.marker,
				Detail:         "Claude native goal evaluator did not accept the delivery condition",
				Recoverable:    true,
			})
		}
		// "Goal achieved" is advisory until spawnruntime proves board
		// acceptance. Verify the provider actually cleared its native goal;
		// prompt text or an old repaint cannot satisfy this second command.
		if err := h.injectGoalCommand(
			ctx,
			"/goal",
			[]string{"no goal set"},
			[]string{"goal active:"},
		); err != nil {
			providerStatus := "goal_still_active"
			var timeoutErr *ClaudeGoalAckTimeoutError
			if errors.As(err, &timeoutErr) {
				providerStatus = "never_acknowledged"
			}
			return finish(&ProviderRunResult{
				ExitCode:       1,
				Status:         ProviderRunFailed,
				ProviderStatus: providerStatus,
				Detail:         err.Error(),
				Recoverable:    true,
			})
		}
		return finish(&ProviderRunResult{
			Status:         ProviderRunComplete,
			ProviderStatus: match.marker,
		})
	case <-h.exited:
		return finish(&ProviderRunResult{
			ExitCode:       1,
			Status:         ProviderRunFailed,
			ProviderStatus: "provider_exited",
			Detail:         "Claude exited before its managed owner goal was achieved",
			Recoverable:    true,
		})
	case <-ctx.Done():
		return finish(&ProviderRunResult{
			ExitCode:       1,
			Status:         ProviderRunFailed,
			ProviderStatus: "context_cancelled",
			Detail:         ctx.Err().Error(),
			Recoverable:    true,
		})
	}
}

func (h *claudeHost) submitOwnerPrompt(ctx context.Context, prompt string) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if state, _ := h.State(ctx); state != ProviderHostRunning {
		return fmt.Errorf("Claude provider host is not running")
	}
	// Bracketed paste keeps embedded newlines in the rendered assignment from
	// becoming multiple TUI submissions. The manager owns the final carriage
	// return after Claude has absorbed the paste burst.
	payload := append([]byte("\x1b[200~"), []byte(prompt)...)
	payload = append(payload, []byte("\x1b[201~")...)
	if _, err := h.ptmx.Write(payload); err != nil {
		return fmt.Errorf("writing managed Claude owner prompt: %w", err)
	}
	select {
	case <-time.After(h.submitDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if _, err := h.ptmx.Write([]byte("\r")); err != nil {
		return fmt.Errorf("submitting managed Claude owner prompt: %w", err)
	}
	return nil
}

func (h *claudeHost) beginRunCapture() func() string {
	output := make(chan []byte, 256)
	done := make(chan struct{})
	var (
		captureMu sync.Mutex
		captured  []byte
	)
	go func() {
		defer close(done)
		for chunk := range output {
			captureMu.Lock()
			captured = append(captured, chunk...)
			if excess := len(captured) - claudeOwnerOutputLimit; excess > 0 {
				captured = append(captured[:0], captured[excess:]...)
			}
			captureMu.Unlock()
		}
	}()
	h.mu.Lock()
	h.subscribers[output] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	result := ""
	return func() string {
		once.Do(func() {
			h.mu.Lock()
			if _, ok := h.subscribers[output]; ok {
				delete(h.subscribers, output)
				close(output)
			}
			h.mu.Unlock()
			<-done
			captureMu.Lock()
			result = string(append([]byte(nil), captured...))
			captureMu.Unlock()
		})
		return result
	}
}

// DeliverSessionNotice types one coalesced run-notice into the managed session.
//
// Three properties make this safe to share a PTY with the native goal plane:
//
//   - It takes h.writeMu, which injectGoalCommandWaiter holds for its whole
//     lifetime INCLUDING the acknowledgement wait, so a notice serializes behind
//     a SYNCHRONOUSLY armed goal ack waiter. That is the production
//     serialization, not a second flag that could disagree with the one the goal
//     plane actually uses. The bound stops there and must not be read as
//     unconditional: injectGoalCommandDeferred returns with its waiter still
//     armed and writeMu released — deliberately, since holding it across the
//     deferred span would recreate the deadlock that path exists to break — so
//     for up to deferredGoalAckDeadline (30 minutes) a notice can be typed while
//     a deferred waiter is armed, and in practice usually is: the deferred path
//     is the bound parent session's own tool shell, which is exactly when its
//     spawned runs finish. In that window the marker gate below is the only
//     guard, and its sufficiency there is measured by
//     TestClaudeHostNoticeInTheDeferredGoalWindowLeavesTheWaiterArmedAndFailingClosed
//     and TestClaudeHostNoticeInTheDeferredGoalWindowDoesNotConsumeTheLateAcknowledgement.
//   - It arms no waiter of its own and asserts nothing about the reply. A
//     notification has no acknowledgement contract: text typed mid-turn is
//     queued by Claude Code and delivered as the next user-visible message, and
//     text typed during a tool call is delivered when the tool returns.
//   - It refuses text carrying a native goal acknowledgement marker before
//     writing a byte. The TUI echoes typed input into the same stream the goal
//     scanner reads, so an unchecked notice is a forged acknowledgement.
func (h *claudeHost) DeliverSessionNotice(ctx context.Context, text string) error {
	if err := ValidateSessionNoticeText(text); err != nil {
		return err
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if state, _ := h.State(ctx); state != ProviderHostRunning {
		return fmt.Errorf("Claude provider host is not running")
	}
	// Bracketed paste keeps the notice one submission even if a future format
	// grows a newline, exactly as the owner prompt path does.
	payload := append([]byte("\x1b[200~"), []byte(text)...)
	payload = append(payload, []byte("\x1b[201~")...)
	if _, err := h.ptmx.Write(payload); err != nil {
		return fmt.Errorf("writing managed session notice: %w", err)
	}
	select {
	case <-time.After(h.submitDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if _, err := h.ptmx.Write([]byte("\r")); err != nil {
		return fmt.Errorf("submitting managed session notice: %w", err)
	}
	return nil
}

func (h *claudeHost) injectGoalCommand(
	ctx context.Context,
	command string,
	success []string,
	failure []string,
) error {
	return h.injectGoalCommandWaiter(ctx, command, success, nil, failure)
}

func (h *claudeHost) injectGoalCommandConditioned(
	ctx context.Context,
	command string,
	conditioned []claudeConditionedMarker,
	failure []string,
) error {
	return h.injectGoalCommandWaiter(ctx, command, nil, conditioned, failure)
}

func (h *claudeHost) injectGoalCommandWaiter(
	ctx context.Context,
	command string,
	success []string,
	conditioned []claudeConditionedMarker,
	failure []string,
) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	waiter, err := h.submitGoalCommandLocked(ctx, command, success, conditioned, failure)
	if err != nil {
		return err
	}
	defer h.scanner.cancel(waiter)

	timeout, deadline := h.goalAckBudget()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case match := <-waiter.done:
		return claudeGoalAckError(match)
	case <-h.exited:
		return fmt.Errorf("Claude exited before acknowledging the native goal directive")
	case <-ctx.Done():
		return fmt.Errorf("waiting for native goal acknowledgement: %w", ctx.Err())
	case <-timer.C:
		return &ClaudeGoalAckTimeoutError{Budget: h.configuredGoalAckTimeout(), Deadline: deadline}
	}
}

// ClaudeGoalAckTimeoutError is the typed never-acknowledged verdict consumed by
// the manager and spawn runtime. Text remains compatible with the historical
// operator message while callers no longer have to classify prose.
type ClaudeGoalAckTimeoutError struct {
	Budget   time.Duration
	Deadline time.Time
}

func (e *ClaudeGoalAckTimeoutError) Error() string {
	return fmt.Sprintf("Claude did not acknowledge the native goal directive within %s", e.Budget)
}

func (h *claudeHost) configuredGoalAckTimeout() time.Duration {
	if h.ackTimeout > 0 {
		return h.ackTimeout
	}
	return claudeHostAckTimeout
}

// goalAckBudget anchors every acknowledgement verdict to provider launch.
// Time spent before pty.StartWithSize is outside the budget; readiness, slow
// boot, and scanner lag after that launch consume the same finite allowance.
func (h *claudeHost) goalAckBudget() (time.Duration, time.Time) {
	budget := h.configuredGoalAckTimeout()
	started := h.snapshot.StartedAt
	if started.IsZero() {
		started = time.Now().UTC()
	}
	deadline := started.Add(budget)
	remaining := time.Until(deadline)
	if remaining < 0 {
		remaining = 0
	}
	return remaining, deadline
}

// injectGoalCommandDeferred writes the directive, waits only the grace window,
// and hands the still-armed waiter back when the acknowledgement has not
// rendered yet. writeMu is released on return so the blocked requester's own
// tool call can finish and the queued input can finally be processed — holding
// it for the deferred span would recreate the deadlock this path exists to
// break.
func (h *claudeHost) injectGoalCommandDeferred(
	ctx context.Context,
	command string,
	success []string,
	conditioned []claudeConditionedMarker,
	failure []string,
	grace time.Duration,
) (*claudeDeferredGoalReflection, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	waiter, err := h.submitGoalCommandLocked(ctx, command, success, conditioned, failure)
	if err != nil {
		return nil, err
	}
	handedOff := false
	defer func() {
		if !handedOff {
			h.scanner.cancel(waiter)
		}
	}()

	select {
	case match := <-waiter.done:
		if err := claudeGoalAckError(match); err != nil {
			return nil, err
		}
		return &claudeDeferredGoalReflection{Acknowledged: true}, nil
	case <-h.exited:
		return nil, fmt.Errorf("Claude exited before acknowledging the native goal directive")
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for native goal acknowledgement: %w", ctx.Err())
	case <-time.After(grace):
	}

	handedOff = true
	return &claudeDeferredGoalReflection{
		Await: func(waitCtx context.Context) error {
			defer h.scanner.cancel(waiter)
			if waitCtx == nil {
				waitCtx = context.Background()
			}
			select {
			case match := <-waiter.done:
				return claudeGoalAckError(match)
			case <-h.exited:
				return fmt.Errorf("Claude exited before acknowledging the native goal directive")
			case <-waitCtx.Done():
				return fmt.Errorf("waiting for the deferred native goal acknowledgement: %w", waitCtx.Err())
			}
		},
		Cancel: func() { h.scanner.cancel(waiter) },
	}, nil
}

// submitGoalCommandLocked registers the acknowledgement waiter strictly before
// the directive is written, then writes and submits it. Callers must hold
// writeMu.
func (h *claudeHost) submitGoalCommandLocked(
	ctx context.Context,
	command string,
	success []string,
	conditioned []claudeConditionedMarker,
	failure []string,
) (*claudeOutputWaiter, error) {
	if state, _ := h.State(ctx); state != ProviderHostRunning {
		return nil, fmt.Errorf("Claude provider host is not running")
	}
	waiter := h.scanner.watchConditioned(success, conditioned, failure)
	release := func(err error) (*claudeOutputWaiter, error) {
		h.scanner.cancel(waiter)
		return nil, err
	}
	if _, err := h.ptmx.Write([]byte(command)); err != nil {
		return release(fmt.Errorf("writing native goal directive: %w", err))
	}
	// Let the TUI absorb the pasted directive before the submit keystroke;
	// an immediate CR can be treated as part of the paste burst.
	select {
	case <-time.After(h.submitDelay):
	case <-ctx.Done():
		return release(ctx.Err())
	}
	if _, err := h.ptmx.Write([]byte("\r")); err != nil {
		return release(fmt.Errorf("submitting native goal directive: %w", err))
	}
	return waiter, nil
}

func claudeGoalAckError(match claudeOutputMatch) error {
	if match.failure {
		return fmt.Errorf("Claude rejected the native goal directive (%s)", match.marker)
	}
	return nil
}

func (h *claudeHost) Stop(context.Context) error {
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return nil
	}
	h.stopped = true
	h.snapshot.Stopped = true
	pid := h.snapshot.PID
	ptmx := h.ptmx
	h.mu.Unlock()

	select {
	case <-h.exited:
	default:
		// The PTY session leader owns the whole foreground group.
		_ = syscall.Kill(-pid, syscall.SIGTERM)
		select {
		case <-h.exited:
		case <-time.After(claudeHostStopGrace):
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			select {
			case <-h.exited:
			case <-time.After(claudeHostStopGrace):
			}
		}
	}
	if ptmx != nil {
		_ = ptmx.Close()
	}
	h.mu.Lock()
	for subscriber := range h.subscribers {
		delete(h.subscribers, subscriber)
		close(subscriber)
	}
	lease := h.contextLease
	h.contextLease = nil
	h.mu.Unlock()
	// The materialization's writer lease outlives the turn but not the process:
	// once this host is gone nothing is writing, so the next launch may fork the
	// same materialization without waiting for a TTL.
	lease.release()
	return nil
}

// verifyContextFork proves the provider actually forked the materialized
// transcript, from the provider's own output.
//
// The previous revision of this check was negative: it asked whether the
// missing-session message had already appeared and whether the resume source was
// unchanged. Neither is evidence. An unchanged source is exactly what a
// successful --fork-session produces, so it is also what a launch that silently
// started cold produces; and the message can arrive after the banner, so an
// instantaneous look can miss it.
//
// What is checked instead is a thing only a real fork can produce. The provider
// writes the forked session lazily, on its first submission rather than at
// startup — measured on claude 2.1.220 — and a local /context submission is
// enough to make it commit, costing no model turn. Once it commits, three facts
// are available at once and all three are required:
//
//   - the child transcript exists at the exact path this adapter derived from its
//     own project-slug rule, which is what makes a wrong slug impossible to pass
//     silently: a provider that disagreed about the directory wrote elsewhere;
//   - the child carries the copy's record identities in order, which is direct
//     descent rather than resemblance;
//   - the child's session identity is the one the manager named, so the handle in
//     the catalog is the handle the run writes into.
//
// The missing-session marker stays armed for the whole window, so the delayed
// arrival the earlier check could miss now fails the launch.
func (h *claudeHost) verifyContextFork(ctx context.Context, forked *claudeForkMaterialization) error {
	if forked == nil {
		return nil
	}
	waiter := h.scanner.watch(nil, []string{claudeMissingSessionMarker})
	defer h.scanner.cancel(waiter)
	if h.scanner.sawMarker(claudeMissingSessionMarker) {
		return &PublicError{
			Code:    ClaudeErrorResumeFailed,
			Message: claudeMissingSessionGuidance,
		}
	}
	if err := h.flushContextFork(ctx); err != nil {
		return err
	}

	verifyTimeout := h.forkVerifyTimeout
	if verifyTimeout <= 0 {
		verifyTimeout = claudeContextForkVerifyTimeout
	}
	deadline := time.After(verifyTimeout)
	var lastDetail string
	for {
		verified, detail := h.inspectContextFork(forked)
		if verified {
			h.mu.Lock()
			h.snapshot.ContextForkVerifiedAt = time.Now().UTC()
			h.mu.Unlock()
			// The copy stays a fork source only if the fork did not append into it,
			// and the base stays one only if the copy did not disturb it. Both are
			// re-read from disk rather than assumed.
			return h.verifyContextSourcesUnchanged(forked)
		}
		if detail != "" {
			lastDetail = detail
		}
		select {
		case match := <-waiter.done:
			if match.failure {
				// Every branch that ends this loop asks the same question of the
				// same evidence and answers it in one place. Returning the refusal
				// straight from here instead would leave two spellings of one rule,
				// and only the branch that happened to win the race would be the
				// one under test.
				return h.classifyUnverifiedFork(
					forked, verifyTimeout, lastDetail, h.processExited(),
				)
			}
		case <-h.exited:
			// The refusal and the exit are one event on the provider's side: it
			// prints "no conversation found" and exits on the next line. Which of
			// the two this loop observes first is a scheduling accident, so the
			// exit is not classified until the reader has drained what the
			// provider printed on its way out.
			h.awaitStreamDrain(ctx)
			return h.classifyUnverifiedFork(forked, verifyTimeout, lastDetail, true)
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return h.classifyUnverifiedFork(
				forked, verifyTimeout, lastDetail, h.processExited(),
			)
		case <-time.After(claudeContextForkPollInterval):
		}
	}
}

// awaitStreamDrain waits for the PTY reader to reach the end of the provider's
// output. It is a wait on the reader, not a delay: the only thing that resolves
// it is the reader loop ending, which happens strictly after the scanner has
// been handed every byte the provider wrote. The grace bounds the case where the
// stream never ends because something other than the exited provider still holds
// the slave; that resolves as "no refusal observed", which is the truth about a
// stream nobody finished reading.
func (h *claudeHost) awaitStreamDrain(ctx context.Context) {
	if h.drained == nil {
		return
	}
	select {
	case <-h.drained:
	case <-ctx.Done():
	case <-time.After(claudeForkStreamDrainGrace):
	}
}

// processExited reports whether the provider process has already been observed
// to exit, without waiting for it.
func (h *claudeHost) processExited() bool {
	select {
	case <-h.exited:
		return true
	default:
		return false
	}
}

// classifyUnverifiedFork names the reason a fork verification ended without
// proof. It reads the refusal off the scanner rather than taking it as an
// argument so that every terminal branch of the verification loop asks the same
// question of the same evidence.
func (h *claudeHost) classifyUnverifiedFork(
	forked *claudeForkMaterialization,
	verifyTimeout time.Duration,
	lastDetail string,
	exited bool,
) error {
	return claudeUnverifiedForkFailure(claudeForkFailureFacts{
		RefusalObserved: h.scanner.sawMarker(claudeMissingSessionMarker),
		ProcessExited:   exited,
		ChildCtxID:      forked.ChildCtxID,
		TranscriptPath:  forked.ForkTranscriptPath,
		VerifyTimeout:   verifyTimeout,
		LastDetail:      lastDetail,
	})
}

// claudeForkFailureFacts is everything a fork verification that produced no
// proof knows about why.
type claudeForkFailureFacts struct {
	// RefusalObserved is the provider's own statement, on its own stream, that
	// the session this launch resumed does not exist.
	RefusalObserved bool
	// ProcessExited is whether the provider is already gone.
	ProcessExited  bool
	ChildCtxID     string
	TranscriptPath string
	VerifyTimeout  time.Duration
	LastDetail     string
}

// claudeUnverifiedForkFailure is the precedence rule the B2 flake came down to.
//
// Two facts can be true at once when a resume of a materialized base fails: the
// provider refused the resume out loud, and this adapter never saw a forked
// transcript. They are not two independent failures — the second is a
// consequence of the first, and it is the weaker statement: "the fork was never
// proven" tells an operator to look at the materialization, while "the provider
// says that session does not exist" tells them the truth. So the refusal wins
// whenever it was observed, no matter which loop branch noticed the end of the
// attempt, and only a verification that ends with no refusal on the stream is
// reported as an unproven fork.
func claudeUnverifiedForkFailure(facts claudeForkFailureFacts) error {
	if facts.RefusalObserved {
		return &PublicError{
			Code:    ClaudeErrorResumeFailed,
			Message: claudeMissingSessionGuidance,
		}
	}
	if facts.ProcessExited {
		return &PublicError{
			Code: claudeContextForkUnverified,
			Message: fmt.Sprintf(
				"the Claude session launched for context %s exited before it committed its forked "+
					"transcript, so the run has no verified writable session: %s",
				facts.ChildCtxID, claudeContextForkDetail(facts.LastDetail),
			),
		}
	}
	return &PublicError{
		Code: claudeContextForkUnverified,
		Message: fmt.Sprintf(
			"the Claude session launched for context %s did not produce its forked transcript at %s "+
				"within %s, so this adapter cannot prove the provider inherited the materialized "+
				"context rather than starting cold: %s",
			facts.ChildCtxID, facts.TranscriptPath,
			facts.VerifyTimeout, claudeContextForkDetail(facts.LastDetail),
		),
	}
}

// flushContextFork submits the one local command that makes the provider commit
// its forked session to disk.
//
// It is a manager-owned submission, like the native goal directive, and it is
// deliberately a local command: /context reports context usage and produces no
// assistant record and no usage record, so the verification costs the run no
// model turn. It goes through the same write mutex as every other manager
// submission so it cannot interleave with an attached client's keystrokes.
func (h *claudeHost) flushContextFork(ctx context.Context) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if _, err := h.ptmx.Write([]byte(claudeContextForkFlushCommand)); err != nil {
		return &PublicError{
			Code: claudeContextForkUnverified,
			Message: fmt.Sprintf(
				"the manager could not ask the Claude session to commit its forked transcript, so the fork "+
					"cannot be verified: %v", err,
			),
		}
	}
	select {
	case <-time.After(h.submitDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if _, err := h.ptmx.Write([]byte("\r")); err != nil {
		return &PublicError{
			Code: claudeContextForkUnverified,
			Message: fmt.Sprintf(
				"the manager could not submit the Claude transcript-commit request, so the fork cannot be "+
					"verified: %v", err,
			),
		}
	}
	return nil
}

// inspectContextFork reads the writable child once and reports whether it proves
// descent yet. A not-yet-written or still-being-written child is not a failure;
// only the verification deadline is.
func (h *claudeHost) inspectContextFork(forked *claudeForkMaterialization) (bool, string) {
	raw, facts, err := inspectClaudeTranscriptFile(forked.ForkTranscriptPath)
	if err != nil {
		return false, err.Error()
	}
	if len(raw) == 0 {
		return false, "the forked transcript is still empty"
	}
	if len(facts.SessionUUIDs) != 1 || facts.SessionUUIDs[0] != forked.ForkSessionUUID {
		return false, fmt.Sprintf(
			"the forked transcript carries session identities %v where only %s was named",
			facts.SessionUUIDs, forked.ForkSessionUUID,
		)
	}
	if !claudeTranscriptInherits(facts.RecordUUIDs, forked.InheritedRecordUUIDs) {
		return false, fmt.Sprintf(
			"the forked transcript holds %d record identity/ies and does not yet carry the materialized "+
				"copy's %d in order",
			len(facts.RecordUUIDs), len(forked.InheritedRecordUUIDs),
		)
	}
	return true, ""
}

// verifyContextSourcesUnchanged re-reads both fork sources after the fork has
// been proven.
//
// Both, not one. The materializer already rehashes the base immediately after
// writing the copy, but that happens before the provider process exists, so it
// only rules out damage the copy itself did. The launch is what opens the base:
// --resume without --fork-session appends straight into the file it was pointed
// at, and a provider that resolved the resume target to the base rather than to
// the copy would corrupt a source other runs are still forking from. The original
// base path is therefore carried on the realized fork purely so this check can
// re-read the same file the provider could have touched.
//
// It fails the launch even though the fork itself succeeded, because a proven
// fork over a corrupted base is still a corrupted base.
func (h *claudeHost) verifyContextSourcesUnchanged(forked *claudeForkMaterialization) error {
	for _, source := range []struct {
		role   string
		path   string
		digest string
	}{
		{role: "materialized transcript", path: forked.TranscriptPath, digest: forked.MaterializedSHA256},
		{role: "base transcript", path: forked.BaseTranscriptPath, digest: forked.BaseSHA256},
	} {
		if strings.TrimSpace(source.path) == "" {
			return &PublicError{
				Code: claudeContextSourceMutated,
				Message: fmt.Sprintf(
					"the %s for context %s was never located, so it cannot be shown to have survived the fork",
					source.role, forked.PrimaryCtxID,
				),
			}
		}
		digest, err := claudeFileDigest(source.path)
		if err != nil {
			return &PublicError{
				Code: claudeContextSourceMutated,
				Message: fmt.Sprintf(
					"the %s for context %s is no longer readable after the fork, so it cannot be confirmed to "+
						"have survived it: %v",
					source.role, forked.PrimaryCtxID, err,
				),
			}
		}
		if digest != source.digest {
			return &PublicError{
				Code: claudeContextSourceMutated,
				Message: fmt.Sprintf(
					"the %s for context %s changed while its fork was taken; a fork source that a turn "+
						"appended to is no longer a fork source",
					source.role, forked.PrimaryCtxID,
				),
			}
		}
	}
	return nil
}

func claudeContextForkDetail(detail string) string {
	if strings.TrimSpace(detail) == "" {
		return "no forked transcript was observed"
	}
	return detail
}

// AttachTerminal implements TerminalHost: one duplex byte stream per attached
// client, replaying the recent raw tail so a reconnecting terminal repaints.
func (h *claudeHost) AttachTerminal(ctx context.Context) (io.ReadWriteCloser, error) {
	if state, _ := h.State(ctx); state != ProviderHostRunning {
		return nil, fmt.Errorf("Claude provider host is not running")
	}
	stream := &claudeTerminalStream{
		host:   h,
		output: make(chan []byte, 256),
		closed: make(chan struct{}),
	}
	h.mu.Lock()
	stream.pending = append([]byte(nil), h.replay...)
	h.subscribers[stream.output] = struct{}{}
	h.mu.Unlock()
	return stream, nil
}

func (h *claudeHost) ResizeTerminal(rows, cols uint16) error {
	if rows == 0 || cols == 0 {
		return fmt.Errorf("terminal size must be positive")
	}
	h.mu.Lock()
	h.snapshot.Rows, h.snapshot.Cols = rows, cols
	ptmx := h.ptmx
	h.mu.Unlock()
	if ptmx == nil {
		return fmt.Errorf("Claude provider host has no terminal")
	}
	return pty.Setsize(ptmx, &pty.Winsize{Rows: rows, Cols: cols})
}

func (h *claudeHost) writeInput(data []byte) (int, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	h.mu.Lock()
	stopped := h.stopped
	ptmx := h.ptmx
	h.mu.Unlock()
	if stopped || ptmx == nil {
		return 0, io.ErrClosedPipe
	}
	return ptmx.Write(data)
}

func (h *claudeHost) detachStream(stream *claudeTerminalStream) {
	h.mu.Lock()
	if _, ok := h.subscribers[stream.output]; ok {
		delete(h.subscribers, stream.output)
		close(stream.output)
	}
	h.mu.Unlock()
}

type claudeTerminalStream struct {
	host      *claudeHost
	output    chan []byte
	pending   []byte
	closed    chan struct{}
	closeOnce sync.Once
}

func (s *claudeTerminalStream) Read(p []byte) (int, error) {
	for len(s.pending) == 0 {
		select {
		case chunk, ok := <-s.output:
			if !ok {
				return 0, io.EOF
			}
			s.pending = chunk
		case <-s.closed:
			return 0, io.EOF
		case <-s.host.exited:
			// Drain whatever the pump already delivered before reporting EOF.
			select {
			case chunk, ok := <-s.output:
				if !ok {
					return 0, io.EOF
				}
				s.pending = chunk
			default:
				return 0, io.EOF
			}
		}
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

func (s *claudeTerminalStream) Write(p []byte) (int, error) {
	select {
	case <-s.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	return s.host.writeInput(p)
}

func (s *claudeTerminalStream) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.host.detachStream(s)
	})
	return nil
}

// claudeDetachedHost represents a durable snapshot whose PTY cannot be
// re-acquired by this manager process. It only reports state and terminates
// an orphaned provider process on Stop.
type claudeDetachedHost struct {
	mu       sync.Mutex
	snapshot claudeHostSnapshot
}

func (h *claudeDetachedHost) State(context.Context) (ProviderHostState, error) {
	return ProviderHostStopped, nil
}

func (h *claudeDetachedHost) Snapshot() (RunnerSnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return copyRunnerSnapshot(RunnerSnapshot{Kind: "claude", Claude: &h.snapshot}), nil
}

func (h *claudeDetachedHost) Stop(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.snapshot.Stopped {
		terminateClaudeProcess(h.snapshot.PID, h.snapshot.SessionUUID)
		h.snapshot.Stopped = true
	}
	return nil
}

// claudePreflightGoalCapability proves the composed executable supports the
// native /goal contract before any session work. It fails closed with a
// public capability code; the manager never downgrades to prompt-only goals.
//
// It returns the active build. The probe already has to determine it, and the
// context paths need exactly this string: a resume that fails is explained by
// naming the build that produced the transcript against the build that tried to
// read it, and re-deriving it later would name a different binary than the one
// this launch actually gated on.
func claudePreflightGoalCapability(ctx context.Context, executable, projectDir, managedHome string, launchEnvironment ...[]string) (string, error) {
	environment := applyClaudeManagedHome(claudeHostEnvironment("", os.Environ()), managedHome)
	if len(launchEnvironment) != 0 {
		environment = launchEnvironment[0]
	}
	preflightCtx, cancel := context.WithTimeout(ctx, claudeHostPreflightLimit)
	defer cancel()

	versionOutput, err := runClaudePreflightWithEnvironment(preflightCtx, executable, projectDir, environment, "--version")
	if err != nil {
		return "", &PublicError{
			Code:    ClaudeErrorUnavailable,
			Message: "composed Claude executable is not runnable",
		}
	}
	version, err := parseClaudeSemanticVersion(versionOutput)
	if err != nil {
		return "", &PublicError{
			Code:    ClaudeErrorUnavailable,
			Message: "could not determine the Claude Code version",
		}
	}
	active := fmt.Sprintf("%d.%d.%d", version[0], version[1], version[2])
	minimum, _ := parseClaudeSemanticVersion(claudeGoalMinimumVersion)
	if claudeVersionLess(version, minimum) {
		return active, &PublicError{
			Code:    ClaudeErrorUnsupportedVersion,
			Message: fmt.Sprintf("Claude Code %d.%d.%d is below the required %s for native /goal", version[0], version[1], version[2], claudeGoalMinimumVersion),
		}
	}

	probeOutput, err := runClaudePreflightWithEnvironment(preflightCtx, executable, projectDir, environment, "-p", "--output-format", "json", "/goal")
	if err != nil {
		lowered := strings.ToLower(probeOutput + " " + err.Error())
		switch {
		case strings.Contains(lowered, "trust"):
			return active, &PublicError{
				Code:    ClaudeErrorWorkspaceUntrusted,
				Message: claudeWorkspaceTrustGuidance,
			}
		default:
			return active, &PublicError{
				Code:    ClaudeErrorPreflightFailed,
				Message: "Claude /goal capability probe failed",
			}
		}
	}
	if !strings.Contains(strings.ToLower(probeOutput), "goal") {
		return active, &PublicError{
			Code:    ClaudeErrorUnavailable,
			Message: "Claude did not acknowledge the native /goal command",
		}
	}
	return active, nil
}

func runClaudePreflightCommand(ctx context.Context, executable, projectDir, managedHome string, args ...string) (string, error) {
	return runClaudePreflightWithEnvironment(ctx, executable, projectDir, applyClaudeManagedHome(claudeHostEnvironment("", os.Environ()), managedHome), args...)
}

func runClaudePreflightWithEnvironment(ctx context.Context, executable, projectDir string, environment []string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = projectDir
	command.Env = environment
	output, err := command.CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

// claudeHostEnvironment strips session-nesting markers so the manager-owned
// session persists transcripts (required for --resume) and does not refuse to
// start inside an orchestrating Claude shell.
func claudeHostEnvironment(term string, environment []string) []string {
	// Canonical owner-run authority is projected privately before this port.
	source := append([]string(nil), environment...)
	result := make([]string, 0, len(source)+1)
	for _, entry := range source {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		switch key {
		case "CLAUDECODE", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ID", "TERM":
			continue
		}
		result = append(result, entry)
	}
	if strings.TrimSpace(term) == "" {
		term = "xterm-256color"
	}
	return append(result, "TERM="+term)
}

// terminateClaudeProcess kills a prior provider process only when its command
// line still carries the session identity, so a recycled PID is never harmed.
func terminateClaudeProcess(pid int, sessionUUID string) {
	if pid <= 0 || !claudeProcessMatches(pid, sessionUUID) {
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	_ = syscall.Kill(pid, syscall.SIGTERM)
	deadline := time.Now().Add(claudeHostStopGrace)
	for time.Now().Before(deadline) {
		if syscall.Kill(pid, 0) != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = syscall.Kill(pid, syscall.SIGKILL)
}

func claudeProcessMatches(pid int, sessionUUID string) bool {
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	sessionUUID = strings.TrimSpace(sessionUUID)
	if sessionUUID == "" {
		return false
	}
	if data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline")); err == nil {
		return bytes.Contains(data, []byte(sessionUUID))
	}
	output, err := exec.Command("ps", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(output), sessionUUID)
}

var claudeSemanticVersionPattern = regexp.MustCompile(`(\d+)\.(\d+)\.(\d+)`)

func parseClaudeSemanticVersion(raw string) ([3]int, error) {
	matches := claudeSemanticVersionPattern.FindStringSubmatch(strings.TrimSpace(raw))
	if len(matches) != 4 {
		return [3]int{}, fmt.Errorf("no semantic version in %q", raw)
	}
	var version [3]int
	for index := 0; index < 3; index++ {
		value, err := strconv.Atoi(matches[index+1])
		if err != nil {
			return [3]int{}, err
		}
		version[index] = value
	}
	return version, nil
}

func claudeVersionLess(a, b [3]int) bool {
	for index := 0; index < 3; index++ {
		if a[index] != b[index] {
			return a[index] < b[index]
		}
	}
	return false
}

func newClaudeSessionUUID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating Claude session UUID: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}

var (
	_ TerminalHost = (*claudeHost)(nil)
	_ ProviderHost = (*claudeHost)(nil)
	_ ProviderHost = (*claudeDetachedHost)(nil)
)

// Internal bounded waiter state; canonical goal revision/rendering remains private.
type claudeDeferredGoalReflection struct {
	Acknowledged bool
	Await        func(context.Context) error
	Cancel       func()
}

var _ ProviderHostFactory = (*ClaudeHostFactory)(nil)
var _ RunnerHost = (*claudeHost)(nil)
