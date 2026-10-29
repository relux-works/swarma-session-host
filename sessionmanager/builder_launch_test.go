package sessionmanager

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testGatewayCtxID = "CTX-000000-fixture"

// Original launch composition roots exercise exact flags, refusal codes and file
// privacy in temporary fixtures. They do not qualify platform sandbox execution.
func composedFlags(argv []string) string {
	return strings.Join(argv, "\x00")
}

func hasFlagValue(argv []string, flag, value string) bool {
	for index := 0; index < len(argv)-1; index++ {
		if argv[index] == flag && argv[index+1] == value {
			return true
		}
	}
	return false
}

func TestClaudeBuilderIsolationDeniesEveryNativeRepositorySurface(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "gateway-mcp.json")
	endpoint := testBuilderGatewayEndpoint(t)

	argv, isolation, err := composeClaudeBuilderToolIsolation(endpoint, configPath)
	if err != nil {
		t.Fatalf("composing Claude builder isolation: %v", err)
	}

	// --tools "" is the single flag that removes Bash, Read, Glob, Grep, WebFetch
	// and WebSearch. Without it the gateway is an addition to the native surface
	// rather than a replacement for it.
	if !hasFlagValue(argv, "--tools", "") {
		t.Fatalf("composition does not empty the built-in tool set: %v", argv)
	}
	// Without --strict-mcp-config the written config is merged with whatever the
	// operator's home already configured, so a second MCP server could survive.
	if !strings.Contains(composedFlags(argv), "--strict-mcp-config") {
		t.Fatalf("composition does not make its MCP config authoritative: %v", argv)
	}
	if !hasFlagValue(argv, "--mcp-config", configPath) {
		t.Fatalf("composition does not install the gateway MCP config: %v", argv)
	}
	// Settings sources are how an ambient skill or checked-in config could add a
	// tool back after the flags were composed. Native slash commands stay enabled
	// because the manager must bind the session with /goal before any work starts;
	// the empty settings sources prevent project/user commands from joining it.
	if !hasFlagValue(argv, "--setting-sources", "") {
		t.Fatalf("composition still loads ambient settings: %v", argv)
	}
	if strings.Contains(composedFlags(argv), "--disable-slash-commands") {
		t.Fatalf("composition disables the manager-required native /goal command: %v", argv)
	}
	// The allowed-tool namespace is the belt to the deny set's braces: even if a
	// future build reintroduced a tool, only the gateway's names are permitted.
	for _, tool := range builderGatewayToolNames {
		if !strings.Contains(
			composedFlags(argv), "mcp__"+builderGatewayServerName+"__"+tool,
		) {
			t.Fatalf("composition does not allow gateway tool %q: %v", tool, argv)
		}
	}

	if _, err := requireBuilderToolIsolation(testGatewayCtxID, isolation); err != nil {
		t.Fatalf("the Claude composition does not satisfy the required policy: %v", err)
	}

	// The written config must name exactly one server, and it must carry the
	// token — a config the shim cannot authenticate with is a builder with no
	// read surface at all.
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		MCPServers map[string]struct {
			Command string            `json:"command"`
			Args    []string          `json:"args"`
			Env     map[string]string `json:"env"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	if len(config.MCPServers) != 1 {
		t.Fatalf("the composed MCP config declares %d server(s), want exactly the gateway",
			len(config.MCPServers))
	}
	server, ok := config.MCPServers[builderGatewayServerName]
	if !ok {
		t.Fatalf("the composed MCP config does not declare the gateway: %v", config.MCPServers)
	}
	if server.Env[builderGatewaySocketEnv] != endpoint.SocketPath ||
		server.Env[builderGatewayTokenEnv] != endpoint.Token {
		t.Fatalf("the composed MCP config does not reach the manager's gateway: %+v", server)
	}
	// The config carries a bearer token, so it must not be world-readable.
	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("the composed MCP config is readable beyond its owner: %v", info.Mode())
	}
}

func TestCodexBuilderIsolationRefusesArgvThatOverridesTheProfile(t *testing.T) {
	escapes := [][]string{
		{"--sandbox", "danger-full-access"},
		{"--sandbox=danger-full-access"},
		{"--dangerously-bypass-approvals-and-sandbox"},
		{"--full-auto"},
		{"--yolo"},
		{"--profile", "operator"},
		{"--codex-home", "/tmp/elsewhere"},
		{"--cd", "/etc"},
		{"-C", "/etc"},
		{"-c", `sandbox_mode="danger-full-access"`},
		{"-c", `sandbox_permissions=["disk-full-read-access"]`},
		{"-c", `default_permissions="operator"`},
		{"-c", `permissions.operator.filesystem={":root"="allow"}`},
		{"-c", `mcp_servers.other={command="/bin/sh"}`},
		{"--config", `web_search="enabled"`},
		{"--config=approval_policy=\"never\""},
		// A trailing override flag with no value is refused too: the manager cannot
		// say what it would have set, and "probably harmless" is not a verdict this
		// boundary is allowed to reach.
		{"-c"},
	}
	for _, escape := range escapes {
		t.Run(strings.Join(escape, " "), func(t *testing.T) {
			err := requireCodexBuilderIsolatedArgv(testGatewayCtxID, append(
				[]string{"app-server"}, escape...,
			))
			var typed *PublicError
			if !errors.As(err, &typed) ||
				typed.Code != "context_builder_tool_isolation_unavailable" {
				t.Fatalf("argv %v was admitted, error = %v", escape, err)
			}
		})
	}

	// The ordinary composed argv must still pass, or the gate would refuse every
	// launch and prove nothing.
	argv, _, err := composeCodexBuilderToolIsolation(testBuilderGatewayEndpoint(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := requireCodexBuilderIsolatedArgv(
		testGatewayCtxID, append([]string{"app-server", "--listen", "ws://127.0.0.1:1"}, argv...),
	); err != nil {
		t.Fatalf("the composed builder argv was refused: %v", err)
	}
}

func TestBuilderIsolatedLaunchArgvRefusesAnOverridingLaunchPlan(t *testing.T) {
	request := &BuilderConfinementRequest{
		CtxID:    testGatewayCtxID,
		Provider: ContextProvider("codex"),
		Gateway:  testBuilderGatewayEndpoint(t),
	}
	_, _, err := builderIsolatedLaunchArgv(
		"codex",
		[]string{"app-server", "--sandbox", "danger-full-access"},
		request,
	)
	var typed *PublicError
	if !errors.As(err, &typed) ||
		typed.Code != "context_builder_tool_isolation_unavailable" {
		t.Fatalf("an overriding launch plan reached the provider, error = %v", err)
	}

	if _, _, err := builderIsolatedLaunchArgv(
		"codex", []string{"app-server"}, request,
	); err != nil {
		t.Fatalf("an ordinary builder launch plan was refused: %v", err)
	}
}

func TestBuilderToolIsolationIsUnavailableForUncomposableProviders(t *testing.T) {
	_, _, err := builderIsolatedLaunchArgv("some-other-provider", []string{"run"}, &BuilderConfinementRequest{
		CtxID:    testGatewayCtxID,
		Provider: ContextProvider("some-other-provider"),
		Gateway:  testBuilderGatewayEndpoint(t),
	})
	if err == nil {
		t.Fatal("an unknown provider composed a builder tool surface")
	}
	if !strings.Contains(err.Error(), "tool-isolation composition") {
		t.Fatalf("refusal does not name the missing capability: %v", err)
	}
}

func TestBuilderIsolationLeavesOrdinaryLaunchesAlone(t *testing.T) {
	original := []string{"--session-id", "abc"}
	argv, isolation, err := builderIsolatedLaunchArgv("claude", original, nil)
	if err != nil {
		t.Fatal(err)
	}
	if isolation != nil {
		t.Fatalf("an ordinary launch reported a builder tool isolation: %+v", isolation)
	}
	if strings.Join(argv, " ") != strings.Join(original, " ") {
		t.Fatalf("an ordinary launch argv was rewritten: %v", argv)
	}
}

func TestBuilderIsolationRefusesAnIncompleteGatewayEndpoint(t *testing.T) {
	complete := testBuilderGatewayEndpoint(t)
	tests := []struct {
		name     string
		endpoint BuilderGatewayEndpoint
	}{
		{name: "no command", endpoint: BuilderGatewayEndpoint{
			SocketPath: complete.SocketPath, Token: complete.Token,
		}},
		{name: "relative command", endpoint: BuilderGatewayEndpoint{
			Command: "task-board", SocketPath: complete.SocketPath, Token: complete.Token,
		}},
		{name: "no socket", endpoint: BuilderGatewayEndpoint{
			Command: complete.Command, Token: complete.Token,
		}},
		{name: "no token", endpoint: BuilderGatewayEndpoint{
			Command: complete.Command, SocketPath: complete.SocketPath,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := composeCodexBuilderToolIsolation(test.endpoint); err == nil {
				t.Fatal("Codex composed a launch around an unreachable gateway")
			}
			if _, _, err := composeClaudeBuilderToolIsolation(
				test.endpoint, filepath.Join(t.TempDir(), "mcp.json"),
			); err == nil {
				t.Fatal("Claude composed a launch around an unreachable gateway")
			}
		})
	}
}

func testBuilderGatewayEndpoint(t *testing.T) BuilderGatewayEndpoint {
	t.Helper()
	return BuilderGatewayEndpoint{
		Command:    "/usr/bin/true",
		Args:       []string{"session", "context-gateway-shim"},
		SocketPath: filepath.Join(t.TempDir(), "gw.sock"),
		Token:      "test-gateway-token",
	}
}
