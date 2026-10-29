package sessionmanager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
)

// These original receiving tests exercise real config/report/private-home helpers
// in isolated fixtures; they do not execute a provider or prove native sandboxing.
// TestCodexBuilderIsolationDeniesEveryNativeRepositorySurface is the same
// assertion against the other provider's vocabulary.
//
// It reads the config document the launch is loaded with rather than grepping
// argv. The previous version searched the composed flags for JSON substrings and
// passed while Codex 0.146.0 refused the same argv outright — a JSON value on a
// `-c` flag is parsed as TOML, fails, and is kept as a literal string, so the
// provider rejects `mcp_servers` as "invalid type: string, expected a map". A
// test that accepts what production refuses is worse than no test, so this one
// asserts on the rendered TOML and its round trip.
func TestCodexBuilderIsolationDeniesEveryNativeRepositorySurface(t *testing.T) {
	endpoint := testBuilderGatewayEndpoint(t)

	argv, isolation, err := composeCodexBuilderToolIsolation(endpoint)
	if err != nil {
		t.Fatalf("composing Codex builder isolation: %v", err)
	}
	// Nothing is spelled on the command line any more. A `-c` override would be a
	// second place the policy is stated, and Codex merges override tables into the
	// loaded config rather than replacing them, so an operator's MCP server would
	// survive one.
	for _, arg := range argv {
		if arg == "-c" || arg == "--config" || strings.HasPrefix(arg, "--config=") {
			t.Fatalf("composition still states policy on the command line: %v", argv)
		}
	}
	// Apps, plugins and dependency installation are each a route by which a
	// server this manager never configured could appear beside the gateway.
	for _, disabled := range []string{
		"apps", "plugins", "remote_plugin", "plugin_sharing", "skill_mcp_dependency_install",
	} {
		if !hasFlagValue(argv, "--disable", disabled) {
			t.Fatalf("composition does not disable %q: %v", disabled, argv)
		}
	}

	config, err := codexBuilderIsolationConfig(endpoint)
	if err != nil {
		t.Fatalf("composing Codex builder isolation config: %v", err)
	}
	rendered, err := toml.Marshal(config)
	if err != nil {
		t.Fatalf("rendering the Codex builder isolation config: %v", err)
	}
	var loaded struct {
		DefaultPermissions string `toml:"default_permissions"`
		WebSearch          string `toml:"web_search"`
		ApprovalPolicy     string `toml:"approval_policy"`
		Permissions        map[string]struct {
			Filesystem map[string]string `toml:"filesystem"`
			Network    struct {
				Enabled     bool              `toml:"enabled"`
				UnixSockets map[string]string `toml:"unix_sockets"`
			} `toml:"network"`
		} `toml:"permissions"`
		MCPServers map[string]struct {
			Command string            `toml:"command"`
			Args    []string          `toml:"args"`
			Env     map[string]string `toml:"env"`
		} `toml:"mcp_servers"`
	}
	if err := toml.Unmarshal(rendered, &loaded); err != nil {
		t.Fatalf("the composed Codex configuration is not loadable TOML: %v\n%s", err, rendered)
	}

	if loaded.DefaultPermissions != codexBuilderPermissionProfile {
		t.Fatalf("configuration does not select its own permission profile: %q", loaded.DefaultPermissions)
	}
	if loaded.WebSearch != "disabled" {
		t.Fatalf("configuration does not disable web search: %q", loaded.WebSearch)
	}
	profile, ok := loaded.Permissions[codexBuilderPermissionProfile]
	if !ok {
		t.Fatalf("configuration declares no %q profile: %v", codexBuilderPermissionProfile, loaded.Permissions)
	}
	// Denying the filesystem at the root is what removes the native read path for
	// the provider and for anything it execs: a shell is not worth denying
	// separately when every file it could open is already refused.
	if profile.Filesystem[":root"] != "deny" {
		t.Fatalf("profile does not deny the filesystem at the root: %v", profile.Filesystem)
	}
	if profile.Network.Enabled {
		t.Fatalf("profile does not disable the network: %+v", profile.Network)
	}
	if profile.Network.UnixSockets[":root"] != "deny" {
		t.Fatalf("profile does not deny Unix sockets at the root: %v", profile.Network.UnixSockets)
	}
	if profile.Network.UnixSockets[endpoint.SocketPath] != "allow" {
		t.Fatalf("profile does not permit the gateway socket: %v", profile.Network.UnixSockets)
	}

	if len(loaded.MCPServers) != 1 {
		t.Fatalf("configuration declares %d MCP server(s), want exactly the gateway", len(loaded.MCPServers))
	}
	server, ok := loaded.MCPServers[builderGatewayServerName]
	if !ok {
		t.Fatalf("configuration does not declare the gateway: %v", loaded.MCPServers)
	}
	if server.Env[builderGatewaySocketEnv] != endpoint.SocketPath ||
		server.Env[builderGatewayTokenEnv] != endpoint.Token {
		t.Fatalf("configuration does not reach the manager's gateway: %+v", server)
	}

	if _, err := requireBuilderToolIsolation(testGatewayCtxID, isolation); err != nil {
		t.Fatalf("the Codex composition does not satisfy the required policy: %v", err)
	}
}

// TestRequireBuilderToolIsolationRefusesEveryWeakening is the gate's negative
// test. Each case is a report that would let a builder read outside the
// gateway's sight, and each must be refused with the stable capability code.
func TestRequireBuilderToolIsolationRefusesEveryWeakening(t *testing.T) {
	complete := func() BuilderToolIsolationResult {
		return BuilderToolIsolationResult{
			Mechanism:      "test-gateway-only",
			DeniedSurfaces: append([]string(nil), builderRequiredDeniedSurfaces...),
			GatewayTools:   append([]string(nil), builderGatewayToolNames...),
		}
	}
	withoutSurface := func(surface string) BuilderToolIsolationResult {
		report := complete()
		kept := report.DeniedSurfaces[:0]
		for _, candidate := range report.DeniedSurfaces {
			if candidate != surface {
				kept = append(kept, candidate)
			}
		}
		report.DeniedSurfaces = kept
		return report
	}

	tests := []struct {
		name   string
		report BuilderToolIsolationResult
	}{
		{name: "no mechanism", report: BuilderToolIsolationResult{
			DeniedSurfaces: builderRequiredDeniedSurfaces,
			GatewayTools:   builderGatewayToolNames,
		}},
		{name: "shell still reachable", report: withoutSurface(builderSurfaceShell)},
		{name: "network still reachable", report: withoutSurface(builderSurfaceNetwork)},
		{
			name:   "native filesystem still reachable",
			report: withoutSurface(builderSurfaceNativeFilesystem),
		},
		{name: "foreign MCP still reachable", report: withoutSurface(builderSurfaceForeignMCP)},
		{
			name:   "ambient settings still loaded",
			report: withoutSurface(builderSurfaceAmbientSettings),
		},
		{name: "no reachable gateway tools", report: func() BuilderToolIsolationResult {
			report := complete()
			report.GatewayTools = nil
			return report
		}()},
		{name: "an extra tool beside the gateway", report: func() BuilderToolIsolationResult {
			report := complete()
			report.GatewayTools = append(report.GatewayTools, "bash")
			return report
		}()},
		{name: "everything empty", report: BuilderToolIsolationResult{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := requireBuilderToolIsolation(testGatewayCtxID, test.report)
			if !builderIsContextErrorCode(
				err, ContextErrorBuilderToolIsolationUnavailable,
			) {
				t.Fatalf("weakened isolation = %v, want %s",
					err, ContextErrorBuilderToolIsolationUnavailable)
			}
		})
	}

	// The complete report is the control: if it did not pass, the cases above
	// would prove nothing about which weakening was detected.
	if _, err := requireBuilderToolIsolation(testGatewayCtxID, complete()); err != nil {
		t.Fatalf("a complete isolation report was refused: %v", err)
	}
}

// TestCodexBuilderIsolationConfigReachesThePrivateHome checks the document the
// launch actually loads.
//
// Composing a correct policy and writing it somewhere Codex never reads are
// indistinguishable from the composition function's own tests, and this is the
// seam where they part: the private CODEX_HOME config.toml is the only file the
// builder process is configured from.
func TestCodexBuilderIsolationConfigReachesThePrivateHome(t *testing.T) {
	// A foreign MCP server is planted in the operator's home first, because
	// exclusivity is the property `-c` overrides could not give: Codex merges an
	// override table into the loaded config rather than replacing it, so a server
	// declared here survived beside the gateway under the previous composition.
	sourceHome := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(sourceHome, "config.toml"),
		[]byte("[mcp_servers.operator-planted]\ncommand = \"/bin/sh\"\n"+
			"sandbox_mode = \"danger-full-access\"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_HOME", sourceHome)

	projectDir := t.TempDir()
	endpoint := testBuilderGatewayEndpoint(t)
	home, err := preparePrivateCodexHome(t.TempDir(), projectDir, &BuilderConfinementRequest{
		CtxID:    testGatewayCtxID,
		Provider: ContextProvider("codex"),
		Gateway:  endpoint,
	}, ClientToolPolicy{})
	if err != nil {
		t.Fatalf("preparing the private Codex home: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(home, "config.toml"))
	if err != nil {
		t.Fatalf("reading the private Codex config: %v", err)
	}
	var loaded map[string]any
	if err := toml.Unmarshal(raw, &loaded); err != nil {
		t.Fatalf("the private Codex config is not loadable TOML: %v\n%s", err, raw)
	}
	if loaded["default_permissions"] != codexBuilderPermissionProfile {
		t.Fatalf("the private Codex config does not select the builder profile:\n%s", raw)
	}
	if _, ok := loaded["permissions"]; !ok {
		t.Fatalf("the private Codex config declares no permission profile:\n%s", raw)
	}
	// A legacy sandbox_mode overrides default_permissions, so the mirrored-key set
	// must not carry one through from the operator's home.
	if _, ok := loaded["sandbox_mode"]; ok {
		t.Fatalf("the private Codex config carries a legacy sandbox_mode:\n%s", raw)
	}
	servers, ok := loaded["mcp_servers"].(map[string]any)
	if !ok {
		t.Fatalf("the private Codex config declares no MCP servers:\n%s", raw)
	}
	if len(servers) != 1 {
		t.Fatalf("the private Codex config declares %d MCP server(s), want exactly the gateway:\n%s",
			len(servers), raw)
	}
	if _, ok := servers[builderGatewayServerName]; !ok {
		t.Fatalf("the private Codex config does not declare the gateway:\n%s", raw)
	}

	// An ordinary session must be unaffected, or this boundary would be silently
	// confining every non-builder run too.
	ordinary, err := preparePrivateCodexHome(t.TempDir(), projectDir, nil, ClientToolPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	ordinaryRaw, err := os.ReadFile(filepath.Join(ordinary, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	var ordinaryLoaded map[string]any
	if err := toml.Unmarshal(ordinaryRaw, &ordinaryLoaded); err != nil {
		t.Fatal(err)
	}
	if _, ok := ordinaryLoaded["default_permissions"]; ok {
		t.Fatalf("an ordinary session was given the builder permission profile:\n%s", ordinaryRaw)
	}
}
