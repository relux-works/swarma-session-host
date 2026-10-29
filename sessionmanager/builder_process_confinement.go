package sessionmanager

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	contextpolicy "github.com/relux-works/swarma-session-host/sessionmanager/resource"
)

const builderProcessConfinementMechanism = "darwin-seatbelt-project-read-v2"

var errBuilderProcessConfinementUnavailable = errors.New(
	"context-builder provider read confinement is unavailable",
)

// confinedProviderCommand returns the executable/argv that must be used for one
// provider launch. A nil request preserves the ordinary launch. A builder
// request either returns a platform-enforced wrapper plus its exact report or
// fails before provider I/O.
//
// It installs the outer boundary only — what the process could read. The inner
// one, which decides how it may read, is composed by builderIsolatedLaunchArgv
// before the argv reaches here. They are separate calls because they fail
// differently: a platform with no sandbox is a confinement failure, a provider
// with no way to deny its own tools is an isolation failure, and collapsing them
// would report one remedy for two problems.
func confinedProviderCommand(
	provider string,
	projectDir string,
	executable string,
	args []string,
	runtimeReadRoots []string,
	request *BuilderConfinementRequest,
) (string, []string, *BuilderConfinementResult, error) {
	if request == nil {
		return executable, append([]string(nil), args...), nil, nil
	}
	normalizedProvider, err := normalizeProvider(provider)
	if err != nil {
		return "", nil, nil, err
	}
	if string(request.Provider) != normalizedProvider {
		return "", nil, nil, fmt.Errorf(
			"context-builder confinement names provider %q, not launch provider %q",
			request.Provider,
			normalizedProvider,
		)
	}
	if len(request.Allowlist.Paths) == 0 {
		return "", nil, nil, fmt.Errorf(
			"context-builder confinement for %s has no configured read allowlist",
			contextpolicy.NormalizeContextID(request.CtxID),
		)
	}
	projectDir = strings.TrimSpace(projectDir)
	if !filepath.IsAbs(projectDir) {
		return "", nil, nil, fmt.Errorf(
			"context-builder project directory must be absolute: %q",
			projectDir,
		)
	}
	projectDir = filepath.Clean(projectDir)
	if resolved, resolveErr := filepath.EvalSymlinks(projectDir); resolveErr == nil {
		projectDir = resolved
	}
	roots, err := normalizeConfinedRoots(request.Allowlist.Paths)
	if err != nil {
		return "", nil, nil, err
	}
	// The gateway socket lives outside the repository, so the sandbox must be told
	// about it explicitly: a builder that cannot reach its only instrumented read
	// surface reads nothing, which would look like a base with no coverage rather
	// than a launch that was misconfigured.
	if socketDir := strings.TrimSpace(request.Gateway.SocketPath); socketDir != "" {
		runtimeReadRoots = append(
			append([]string(nil), runtimeReadRoots...),
			filepath.Dir(socketDir),
		)
	}
	wrappedExecutable, wrappedArgs, err := platformConfinedProviderCommand(
		projectDir,
		executable,
		args,
		roots,
		runtimeReadRoots,
	)
	if err != nil {
		return "", nil, nil, err
	}
	result := &BuilderConfinementResult{
		Mechanism:     builderProcessConfinementMechanism,
		EnforcedRoots: roots,
	}
	return wrappedExecutable, wrappedArgs, result, nil
}

// builderIsolatedLaunchArgv folds the tool-isolation policy into a provider's
// composed argv.
//
// The isolation flags go last so the deny policy is the final word: a
// managed_host.argv that tried to re-enable a tool is overridden rather than
// merged with. A nil request leaves an ordinary launch untouched, which is why
// every non-builder session keeps its normal tool surface.
func builderIsolatedLaunchArgv(
	provider string,
	args []string,
	request *BuilderConfinementRequest,
) ([]string, *BuilderToolIsolationResult, error) {
	if request == nil {
		return append([]string(nil), args...), nil, nil
	}
	normalizedProvider, err := normalizeProvider(provider)
	if err != nil {
		return nil, nil, err
	}
	isolationArgv, isolation, err := composeBuilderToolIsolation(normalizedProvider, request)
	if err != nil {
		return nil, nil, err
	}
	composed := append(append([]string(nil), args...), isolationArgv...)
	// The composed argv is checked, not just the part this manager contributed.
	// The launch plan is operator-supplied data, and appending a deny policy after
	// a flag that disables the whole sandbox does not make the deny policy win.
	if err := requireBuilderIsolatedArgv(normalizedProvider, request.CtxID, composed); err != nil {
		return nil, nil, err
	}
	return composed, &isolation, nil
}

// requireBuilderIsolatedArgv rejects a composed builder argv that could override
// the policy the isolation just installed.
//
// Only Codex has such a check today, and that asymmetry is real rather than an
// omission: the Claude adapter validates its managed argv against a total
// allowlist before this point, so an override flag cannot reach here. Codex
// accepts a free-form managed_host.argv, so the escapes have to be named.
func requireBuilderIsolatedArgv(provider, ctxID string, args []string) error {
	if ContextProvider(provider) == contextpolicy.ContextProviderCodex {
		return requireCodexBuilderIsolatedArgv(ctxID, args)
	}
	return nil
}

// composeBuilderToolIsolation dispatches to the provider that knows how to say
// "only this MCP server".
//
// A provider with no composition here cannot publish a project base. That is the
// same fail-closed answer the confinement seam gives for a platform with no
// sandbox, and for the same reason: the alternative is launching a builder whose
// reads are partly unobserved and calling the result evidence.
func composeBuilderToolIsolation(
	provider string,
	request *BuilderConfinementRequest,
) ([]string, BuilderToolIsolationResult, error) {
	switch ContextProvider(provider) {
	case contextpolicy.ContextProviderClaude:
		configDir := strings.TrimSpace(request.IsolationConfigDir)
		if configDir == "" {
			return nil, BuilderToolIsolationResult{}, fmt.Errorf(
				"Claude context-builder tool isolation needs a manager-private config directory",
			)
		}
		return composeClaudeBuilderToolIsolation(
			request.Gateway,
			filepath.Join(
				configDir,
				contextpolicy.NormalizeContextID(request.CtxID)+"-gateway-mcp.json",
			),
		)
	case contextpolicy.ContextProviderCodex:
		return composeCodexBuilderToolIsolation(request.Gateway)
	}
	return nil, BuilderToolIsolationResult{}, fmt.Errorf(
		"provider %q has no context-builder tool-isolation composition, so it cannot confine a "+
			"builder to the repository-evidence gateway",
		provider,
	)
}
