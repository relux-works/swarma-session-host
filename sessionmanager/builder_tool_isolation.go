package sessionmanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	contextpolicy "github.com/relux-works/swarma-session-host/sessionmanager/resource"
)

// The repository surfaces a publishable context-builder launch must remove.
//
// They are named rather than counted because the refusal has to say which one an
// adapter could not close. Each is a distinct way an in-scope file could be read
// without producing a gateway trace event, and the trace is only complete if all
// of them are shut:
//
//   - shell: an exec'd `cat` reads a file the gateway never hears about.
//   - network: a fetch can exfiltrate what was read, and can pull in content that
//     the coverage manifest would then attribute to the repository.
//   - native filesystem: the provider's own read/grep/glob tools are exactly the
//     gateway's operations, minus the evidence.
//   - foreign MCP: any server but the gateway is an uninstrumented file surface.
//   - ambient settings: settings files, slash commands, skills and plugins can put
//     any of the above back after the flags were composed.
const (
	builderSurfaceShell            = "shell"
	builderSurfaceNetwork          = "network"
	builderSurfaceNativeFilesystem = "native-filesystem"
	builderSurfaceForeignMCP       = "foreign-mcp"
	builderSurfaceAmbientSettings  = "ambient-settings"
)

// builderRequiredDeniedSurfaces is the closed set. An adapter reporting a subset
// cannot publish a project base.
var builderRequiredDeniedSurfaces = []string{
	builderSurfaceAmbientSettings,
	builderSurfaceForeignMCP,
	builderSurfaceNativeFilesystem,
	builderSurfaceNetwork,
	builderSurfaceShell,
}

// builderGatewayServerName is the MCP server name the gateway is published
// under. It is fixed so the allowed-tool namespace is a compile-time constant
// rather than something a launch could widen.
const builderGatewayServerName = "task-board-repository-evidence"

// builderGatewayToolNames are the only tools a confined builder may call.
var builderGatewayToolNames = []string{"list", "read", "search", "stat"}

// BuilderGatewayEndpoint describes the stdio process a provider spawns to reach
// the manager-owned gateway.
//
// The process is a shim, not the gateway. It holds no repository access of its
// own: it forwards a request over the manager's Unix socket and returns the
// answer. That split is what makes the same policy enforceable on both
// providers — the provider may deny its child every filesystem capability it
// likes, because the reads happen in the manager, not in the child.

// BuilderToolIsolationResult is what a provider adapter reports it enforced.
//
// It mirrors BuilderConfinementResult deliberately. The manager does not know
// how either provider is launched, and it must not have to: the adapter installs
// the policy and reports it, and the manager checks the report against a
// required set it owns. An adapter that reports less than the required set gets
// context_builder_tool_isolation_unavailable, which is a capability answer — the
// remedy is a provider that can do this, not a permission somebody can grant.
type BuilderToolIsolationResult struct {
	// Mechanism names the composition that installed the policy. An empty
	// mechanism is a refusal, on the same terms as an empty confinement mechanism.
	Mechanism string
	// DeniedSurfaces are the repository surfaces the launch removed.
	DeniedSurfaces []string
	// GatewayTools are the tools left reachable. Anything outside the gateway's
	// own operations makes the isolation unenforceable, because a tool the
	// manager cannot account for is a read it cannot record.
	GatewayTools []string
}

// BuilderToolIsolationReporter is implemented by a launched provider host whose
// process was actually started under the tool-isolation policy. Like the
// confinement reporter it is unavailable before Launch succeeds: the manager
// attests to a process that ran, not to flags somebody intended to pass.
type BuilderToolIsolationReporter interface {
	BuilderToolIsolation() (BuilderToolIsolationResult, error)
}

// BuilderToolObservationReporter is the provider-adapter side of repository
// trace reconciliation.
//
// The gateway and its stdio shim cannot prove that a provider exposed no other
// tool route: they only see calls which reached them.  A publishable builder
// host therefore has to classify its own native turn/transcript records and
// report gateway calls, foreign calls, protocol drift and disconnects.  The
// manager combines this independent observation with the append-only trace at
// finalization time.  Hosts which cannot provide it are not publication-capable.

// composeClaudeBuilderToolIsolation writes the gateway's MCP config and returns
// the flags that leave it as the only repository surface.
//
// Each flag is the same one the correctness assay probe-verified on the running
// provider, used here to the opposite end: the assay denies every tool including
// MCP, while this denies every tool except one MCP server. --tools "" removes the
// built-in set — Bash, Read, Glob, Grep, WebFetch, WebSearch — in one place, and
// --strict-mcp-config makes the written config the entire MCP surface rather than
// an addition to whatever the operator's home already configured.
func composeClaudeBuilderToolIsolation(
	endpoint BuilderGatewayEndpoint,
	configPath string,
) ([]string, BuilderToolIsolationResult, error) {
	if err := validateBuilderGatewayEndpoint(endpoint); err != nil {
		return nil, BuilderToolIsolationResult{}, err
	}
	configPath = strings.TrimSpace(configPath)
	if !filepath.IsAbs(configPath) {
		return nil, BuilderToolIsolationResult{}, fmt.Errorf(
			"Claude context-builder MCP config path must be absolute: %q", configPath,
		)
	}
	config := map[string]any{
		"mcpServers": map[string]any{
			builderGatewayServerName: map[string]any{
				"type":    "stdio",
				"command": endpoint.Command,
				"args":    endpoint.Args,
				"env": map[string]string{
					builderGatewaySocketEnv: endpoint.SocketPath,
					builderGatewayTokenEnv:  endpoint.Token,
				},
			},
		},
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, BuilderToolIsolationResult{}, fmt.Errorf(
			"encoding Claude context-builder MCP config: %w", err,
		)
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return nil, BuilderToolIsolationResult{}, err
	}
	// The config carries the gateway token, so it is written with the same
	// privacy as the manager's other secrets rather than as an ordinary file the
	// builder's own project could read.
	if err := writePrivateFile(configPath, encoded); err != nil {
		return nil, BuilderToolIsolationResult{}, err
	}
	argv := []string{
		"--tools", "",
		"--strict-mcp-config",
		"--mcp-config", configPath,
		"--setting-sources", "",
		"--allowed-tools", strings.Join(claudeBuilderGatewayToolNames(), ","),
	}
	return argv, BuilderToolIsolationResult{
		Mechanism:      "claude-builder-gateway-only-v1",
		DeniedSurfaces: append([]string(nil), builderRequiredDeniedSurfaces...),
		GatewayTools:   append([]string(nil), builderGatewayToolNames...),
	}, nil
}

func claudeBuilderGatewayToolNames() []string {
	names := make([]string, 0, len(builderGatewayToolNames))
	for _, tool := range builderGatewayToolNames {
		names = append(names, "mcp__"+builderGatewayServerName+"__"+tool)
	}
	return names
}

// codexBuilderPermissionProfile is the permission profile a builder launch runs
// under.
//
// The filesystem is denied at the root, which is what removes the native read
// path for the provider *and* for anything it execs: a shell is not worth
// denying separately if every file it could open is refused. The gateway is
// unaffected because it does not read through this process — the shim only opens
// a socket, and the manager performs the read.
const codexBuilderPermissionProfile = "task-board-context-builder"

// codexBuilderIsolationConfig is the configuration a Codex builder launch must
// be loaded with.
//
// It is returned as a config *document* rather than as `-c key=value` overrides,
// and both halves of that choice were forced by the provider.
//
// The first half is syntax. Codex parses a `-c` value as TOML and silently falls
// back to treating an unparseable value as a literal string, so a JSON object
// passed that way does not fail at the flag — it becomes a string, and Codex
// then refuses the whole configuration with `invalid type: string, expected a
// map` before the process starts. A composition that cannot be launched is not a
// policy.
//
// The second half is exclusivity, which no `-c` override can give. `-c
// mcp_servers={...}` *merges* into the loaded table, verified against Codex
// 0.146.0: a server declared in the operator's config.toml survives the override
// and stays reachable beside the gateway. Since the whole claim here is that the
// gateway is the only repository surface, the manager has to own the document
// that declares MCP servers rather than amend somebody else's — which the
// private CODEX_HOME already gives it, and which is also what removes the
// ambient skills, plugins, rules and AGENTS.md that live in a real Codex home.
//
// Owning the document is also what settles the legacy-sandbox question. A
// `sandbox_mode` key overrides `default_permissions`, so a profile written
// alongside an operator's legacy setting would be silently inert; here the
// manager writes every key, and the mirrored-key set the private home already
// uses carries neither `sandbox_mode` nor `permissions`.
func codexBuilderIsolationConfig(endpoint BuilderGatewayEndpoint) (map[string]any, error) {
	if err := validateBuilderGatewayEndpoint(endpoint); err != nil {
		return nil, err
	}
	return map[string]any{
		"default_permissions": codexBuilderPermissionProfile,
		"approval_policy":     "never",
		"web_search":          "disabled",
		"permissions": map[string]any{
			codexBuilderPermissionProfile: map[string]any{
				"filesystem": map[string]any{":root": "deny"},
				"network": map[string]any{
					"enabled": false,
					"unix_sockets": map[string]any{
						":root":             "deny",
						endpoint.SocketPath: "allow",
					},
				},
			},
		},
		"mcp_servers": map[string]any{
			builderGatewayServerName: map[string]any{
				"command": endpoint.Command,
				"args":    append([]string(nil), endpoint.Args...),
				"env": map[string]string{
					builderGatewaySocketEnv: endpoint.SocketPath,
					builderGatewayTokenEnv:  endpoint.Token,
				},
			},
		},
	}, nil
}

// codexBuilderIsolationEscapeFlags are argv shapes that would undo the profile
// after it was written.
//
// They exist because the config document is authoritative only until something
// on the command line overrides it, and Codex lets the command line win:
// `--sandbox danger-full-access` and `-c sandbox_mode="danger-full-access"` both
// turn the profile's "restricted fs + restricted network" into "unrestricted fs
// + enabled network", verified against 0.146.0. A launch plan is operator-
// supplied data, so the manager checks the argv it is about to run rather than
// assuming nobody put one of these in it.
var codexBuilderIsolationEscapeFlags = []string{
	"--sandbox",
	"--dangerously-bypass-approvals-and-sandbox",
	"--full-auto",
	"--yolo",
	"--profile",
	"--codex-home",
	"--cd",
	"-C",
}

// codexBuilderIsolationEscapeConfigKeys are `-c` keys that reach the same
// settings the profile owns. A launch that sets one of them is refused rather
// than merged: there is no ordering in which both statements are true.
var codexBuilderIsolationEscapeConfigKeys = []string{
	"sandbox_mode",
	"sandbox_workspace_write",
	"sandbox_permissions",
	"default_permissions",
	"permissions",
	"mcp_servers",
	"approval_policy",
	"web_search",
	"tools",
	"experimental_use_exec_command_tool",
	"shell_environment_policy",
}

// requireCodexBuilderIsolatedArgv refuses a launch argv that could override the
// permission profile or the MCP surface.
//
// It is a capability answer rather than a validation error: the manager is not
// telling the operator to fix a typo, it is saying this launch cannot be shown
// to have run under the policy, so it cannot publish a project base.
func requireCodexBuilderIsolatedArgv(ctxID string, args []string) error {
	normalized := contextpolicy.NormalizeContextID(ctxID)
	refuse := func(offending string) error {
		return &PublicError{
			Code: "context_builder_tool_isolation_unavailable",
			Message: fmt.Sprintf(
				"the Codex launch plan for %s carries %q, which overrides the context-builder permission "+
					"profile or its MCP surface, so this launch cannot be shown to have read the repository "+
					"only through the evidence gateway",
				normalized, offending,
			),
		}
	}
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		bare := arg
		if equals := strings.Index(bare, "="); equals >= 0 {
			bare = bare[:equals]
		}
		for _, flag := range codexBuilderIsolationEscapeFlags {
			if bare == flag {
				return refuse(arg)
			}
		}
		if bare != "-c" && bare != "--config" {
			continue
		}
		// The value is either glued to the flag or the next element. Both spellings
		// are accepted by Codex, so both are inspected; a trailing flag with no value
		// is refused too, because the manager cannot tell what it would have set.
		value := ""
		switch {
		case strings.Contains(arg, "="):
			value = arg[strings.Index(arg, "=")+1:]
		case index+1 < len(args):
			index++
			value = args[index]
		default:
			return refuse(arg)
		}
		key := strings.TrimSpace(value)
		if equals := strings.Index(key, "="); equals >= 0 {
			key = strings.TrimSpace(key[:equals])
		}
		for _, reserved := range codexBuilderIsolationEscapeConfigKeys {
			// Prefix match with a dot boundary, so `permissions.foo.filesystem` is
			// caught along with bare `permissions`.
			if key == reserved || strings.HasPrefix(key, reserved+".") {
				return refuse(value)
			}
		}
	}
	return nil
}

// composeCodexBuilderToolIsolation returns the launch flags that leave the
// gateway as the only repository surface.
//
// It emits no `-c` overrides at all. Everything the policy asserts lives in the
// config document the manager writes into the private CODEX_HOME, so there is no
// second place a value could be spelled differently, and nothing here for the
// provider to reject at parse time. What remains are the feature switches, each
// of which is a route by which an MCP server this manager never configured could
// appear beside the gateway.
func composeCodexBuilderToolIsolation(
	endpoint BuilderGatewayEndpoint,
) ([]string, BuilderToolIsolationResult, error) {
	if _, err := codexBuilderIsolationConfig(endpoint); err != nil {
		return nil, BuilderToolIsolationResult{}, err
	}
	argv := []string{
		"--disable", "apps",
		"--disable", "plugins",
		"--disable", "remote_plugin",
		"--disable", "plugin_sharing",
		"--disable", "skill_mcp_dependency_install",
	}
	return argv, BuilderToolIsolationResult{
		Mechanism:      "codex-builder-gateway-only-v2",
		DeniedSurfaces: append([]string(nil), builderRequiredDeniedSurfaces...),
		GatewayTools:   append([]string(nil), builderGatewayToolNames...),
	}, nil
}

func validateBuilderGatewayEndpoint(endpoint BuilderGatewayEndpoint) error {
	if !filepath.IsAbs(strings.TrimSpace(endpoint.Command)) {
		return fmt.Errorf(
			"context-builder gateway command must be an absolute path: %q", endpoint.Command,
		)
	}
	if !filepath.IsAbs(strings.TrimSpace(endpoint.SocketPath)) {
		return fmt.Errorf(
			"context-builder gateway socket must be an absolute path: %q", endpoint.SocketPath,
		)
	}
	if strings.TrimSpace(endpoint.Token) == "" {
		return fmt.Errorf("context-builder gateway endpoint requires an authentication token")
	}
	return nil
}

const builderGatewaySocketEnv = "TASK_BOARD_BUILDER_GATEWAY_SOCKET"
const builderGatewayTokenEnv = "TASK_BOARD_BUILDER_GATEWAY_TOKEN"
