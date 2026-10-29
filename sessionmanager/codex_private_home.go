package sessionmanager

import (
	"errors"
	"fmt"
	"github.com/pelletier/go-toml/v2"
	curatorenv "github.com/relux-works/swarma-session-host/sessionmanager/runner/environment"
	"os"
	"path/filepath"
	"strings"
)

const CodexErrorCuratorProvenanceUnreadable = "codex_curator_provenance_unreadable"

func RefuseLockedCodexLaunch(plan *PrimarySessionLaunchPlan) error {
	if plan == nil {
		return nil
	}
	provenance, present, err := extractCuratorProvenance(plan.Sources)
	if err != nil {
		return &curatorIdentityReadError{
			public: &PublicError{
				Code:    CodexErrorCuratorProvenanceUnreadable,
				Message: "the plan carries Curator fragment sources but its provenance snapshot cannot be verified; refusing the launch rather than running beside an unknown lock state",
			},
			cause: err,
		}
	}
	if !present {
		return nil
	}
	if provenance.Fragment.Environment != "codex_cli" {
		return nil
	}
	permissions := provenance.Fragment.Permissions
	if permissions == nil || !permissions.Locked {
		return nil
	}
	refusal := lockedCodexPlanRefused(plan)
	if refusal.Knob == "" {
		return nil
	}
	detail := fmt.Sprintf("permissions locked by source %q to mode %q (runtime %q): %s; %s", permissions.Source, permissions.Mode, provenance.Fragment.Environment, refusal.Describe(), CodexLockedRoute)
	return &PublicError{Code: CodexFragmentPermissionsLocked, Message: detail}
}

// CodexFragmentPermissionsLocked is the daemon-side spelling of the
// generic locked-permissions refusal code, the same string as
// ClaudeFragmentPermissionsLocked: one code with the runtime as a field.
// It aliases the claude constant rather than restating the literal, so the
// pin in internal/spawn (TestClaudeDaemonLockCodeMatchesSpawn) covers it.
const CodexFragmentPermissionsLocked = "fragment_permissions_locked"

// CodexLockedRoute is the operator route every codex lock refusal names,
// shared by the composer and the daemon messages.
const CodexLockedRoute = "unlock the fragment or run `task-board codex` with no posture overrides"

// lockedCodexPlanRefused projects one composed plan onto the shared
// classifier and adds the host-argv shape check.
func lockedCodexPlanRefused(plan *PrimarySessionLaunchPlan) LockedCodexSessionRefusal {
	yolo := false
	switch value := plan.Resolved.Yolo.Value.(type) {
	case nil:
	case bool:
		yolo = value
	default:
		return LockedCodexSessionRefusal{Knob: "yolo"}
	}
	session := LockedCodexSession{Yolo: yolo, ClientTail: plan.LaunchVariants.ManagedClient.Argv}
	for _, policy := range []struct {
		value  ResolvedLaunchValue
		target *string
		knob   string
		flag   string
	}{
		{plan.Resolved.Sandbox, &session.Sandbox, "sandbox", "--sandbox"},
		{plan.Resolved.Approval, &session.Approval, "approval", "--ask-for-approval"},
	} {
		switch value := policy.value.Value.(type) {
		case nil:
		case string:
			*policy.target = value
		default:
			return LockedCodexSessionRefusal{Knob: policy.knob, Flag: policy.flag}
		}
	}
	if refusal := LockedCodexSessionRefused(session); refusal.Knob != "" {
		return refusal
	}
	if token := LockedCodexHostArgvRefused(plan.LaunchVariants.ManagedHost.Argv); token != "" {
		return LockedCodexSessionRefusal{Knob: "managed-host", Flag: token}
	}
	return LockedCodexSessionRefusal{}
}

// The delivery variables the app-server exports to the tool shells it runs.
// They are the same three the visible client receives from
// managedCodexClientEnvironment (cmd/codex_manager.go) and the same three
// applyOwnerRunEnvironment blocks for a spawned owner run.
func codexPrivateConfigToolPolicy(policy ClientToolPolicy) map[string]any {
	keys := map[string]any{}
	if sandbox := strings.TrimSpace(policy.Sandbox); codexPrivateConfigSandboxModes[sandbox] {
		keys["sandbox_mode"] = sandbox
	}
	if approval := strings.TrimSpace(policy.Approval); codexPrivateConfigApprovalPolicies[approval] {
		keys["approval_policy"] = approval
	}
	return keys
}

// The enums Codex accepts for the two policy keys (codex-cli 0.146.0 through
// 0.154.0).
var (
	codexPrivateConfigSandboxModes = map[string]bool{
		"read-only":          true,
		"workspace-write":    true,
		"danger-full-access": true,
	}
	codexPrivateConfigApprovalPolicies = map[string]bool{
		"untrusted":  true,
		"on-failure": true,
		"on-request": true,
		"never":      true,
	}
)

// codexTrustedProjectDirs is the set of directories the private home trusts for
// a session working in projectDir.
//
// Codex accepts either the working directory or the root of the repository
// containing it as the trust key, and it will not load a project's own .codex
// config, hooks or exec policies from a directory that is not trusted. A
// session started from a subdirectory therefore has to carry the repository
// root as well, or the visible client silently falls back to the user-level
// config. Codex resolves symlinks before the lookup, so the keys are resolved
// here too.
//
// This projection is deliberately not what `agents-infra codex` does. That
// launcher composes model, effort and sandbox policy and nothing that grants
// trust, so an untrusted directory keeps stock Codex behaviour, prompt
// included. The managed path differs in kind: the home written here exists for
// one board-selected project and trusts nothing else. See the trust exception
// in docs/codex-goal-runtime.md — the two entry points are contracted to
// diverge here, and TestCodexEntrypointParityLiveSmoke fails if they converge.
func codexTrustedProjectDirs(projectDir string) []string {
	var dirs []string
	seen := map[string]bool{}
	add := func(dir string) {
		resolved := resolveCodexProjectPath(dir)
		if resolved == "" || seen[resolved] {
			return
		}
		seen[resolved] = true
		dirs = append(dirs, resolved)
	}
	add(projectDir)
	if root, ok := codexRepositoryRoot(projectDir); ok {
		add(root)
	}
	return dirs
}

func resolveCodexProjectPath(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return ""
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return ""
	}
	absolute = filepath.Clean(absolute)
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return absolute
	}
	return filepath.Clean(resolved)
}

// codexRepositoryRoot walks up to the nearest directory holding a .git entry. A
// worktree records .git as a file, so the entry type is not checked.
func codexRepositoryRoot(dir string) (string, bool) {
	current, err := filepath.Abs(dir)
	if err != nil {
		return "", false
	}
	current = filepath.Clean(current)
	for {
		if _, err := os.Lstat(filepath.Join(current, ".git")); err == nil {
			return current, true
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", false
		}
		current = parent
	}
}

func preparePrivateCodexHome(
	sessionDir, projectDir string,
	builder *BuilderConfinementRequest,
	policy ClientToolPolicy,
	configOverride ...[]byte,
) (string, error) {
	if !filepath.IsAbs(projectDir) {
		return "", fmt.Errorf("private Codex home project directory must be absolute")
	}
	sourceHome, err := curatorenv.CodexNativeHome()
	if err != nil {
		return "", err
	}
	projectDir = filepath.Clean(projectDir)
	privateHome := filepath.Join(sessionDir, "codex-home")
	if err := os.MkdirAll(privateHome, 0o700); err != nil {
		return "", fmt.Errorf("creating private Codex home: %w", err)
	}
	if err := os.Chmod(privateHome, 0o700); err != nil {
		return "", fmt.Errorf("securing private Codex home: %w", err)
	}

	if err := curatorenv.CaptureCodexHome(privateHome); err != nil {
		return "", err
	}
	var privateConfig []byte
	if len(configOverride) != 0 {
		privateConfig = configOverride[0]
	} else {
		privateConfig, err = renderPrivateCodexConfig(sourceHome, projectDir, builder, policy)
		if err != nil {
			return "", err
		}
	}
	if err := writePrivateFile(
		filepath.Join(privateHome, "config.toml"),
		privateConfig,
	); err != nil {
		return "", fmt.Errorf("writing private Codex config: %w", err)
	}
	sourceAuth := filepath.Join(sourceHome, "auth.json")
	targetAuth := filepath.Join(privateHome, "auth.json")
	if _, err := os.Lstat(targetAuth); errors.Is(err, os.ErrNotExist) {
		if _, sourceErr := os.Stat(sourceAuth); sourceErr == nil {
			if err := os.Symlink(sourceAuth, targetAuth); err != nil {
				return "", fmt.Errorf("linking Codex authentication into private home: %w", err)
			}
		} else if !errors.Is(sourceErr, os.ErrNotExist) {
			return "", fmt.Errorf("checking Codex authentication: %w", sourceErr)
		}
	} else if err != nil {
		return "", fmt.Errorf("checking private Codex authentication: %w", err)
	}
	return privateHome, nil
}

// renderPrivateCodexConfig builds the config.toml the launch is loaded with.
//
// A builder launch gets the tool-isolation document folded in last, so the
// manager's policy is the final word over anything mirrored from the operator's
// home. It is written here rather than passed as `-c` overrides because Codex
// merges override tables into the loaded config instead of replacing them: an
// operator-declared MCP server would survive an `-c mcp_servers={...}` and stay
// reachable beside the gateway.
//
// An ordinary launch gets the applied tool policy written as the document's
// `sandbox_mode` / `approval_policy` — the same values thread/start is sent —
// so the private home's DEFAULT is the composed policy and not Codex's
// workspace-write/on-request. That default is load-bearing from Codex 0.154:
// a remote client's thread/resume carries no permission fields (Codex refuses
// them on a remote resume) but does carry cwd, workspace roots and config
// overrides, and when any of those differ from the loaded thread's snapshot
// the app-server tears the idle thread down and cold-resumes it FROM ITS
// CONFIG — measured on 0.154.0: the turn after such an
// attach ran under workspace-write against a thread started with
// danger-full-access. With the policy in this document the rebuilt thread
// keeps the composed policy and the visible client, which builds its banner
// from that resume response, reports it (0.154.0 is the minimum supported
// Codex for the managed client, cmd.codexMinimumSupportedVersion). A builder
// host is excluded: its permission profile owns these settings and a
// `sandbox_mode` key would override `default_permissions`.
func renderPrivateCodexConfig(
	sourceHome, projectDir string,
	builder *BuilderConfinementRequest,
	policy ClientToolPolicy,
	curatorMCP ...bool,
) ([]byte, error) {
	privateConfig := map[string]any{}
	sourceConfigPath := filepath.Join(sourceHome, "config.toml")
	sourceConfigData, err := os.ReadFile(sourceConfigPath)
	switch {
	case err == nil:
		var sourceConfig map[string]any
		if err := toml.Unmarshal(sourceConfigData, &sourceConfig); err != nil {
			return nil, fmt.Errorf("parsing Codex config %s: %w", sourceConfigPath, err)
		}
		for _, key := range codexPrivateConfigMirroredKeys {
			if value, ok := sourceConfig[key]; ok {
				privateConfig[key] = value
			}
		}
	case errors.Is(err, os.ErrNotExist):
	default:
		return nil, fmt.Errorf("reading Codex config %s: %w", sourceConfigPath, err)
	}
	// The Curator layer owns MCP content. Copying/parsing its table is transport,
	// not a second grammar or content policy. Builder isolation still wins below.
	if len(curatorMCP) != 0 && curatorMCP[0] {
		layer, err := os.ReadFile(filepath.Join(sourceHome, "curator-mcp.config.toml"))
		if err != nil {
			return nil, err
		}
		var document map[string]any
		if err := toml.Unmarshal(layer, &document); err != nil {
			return nil, err
		}
		privateConfig["mcp_servers"] = document["mcp_servers"]
	}
	projects := map[string]any{}
	for _, dir := range codexTrustedProjectDirs(projectDir) {
		projects[dir] = map[string]any{
			"trust_level": "trusted",
		}
	}
	privateConfig["projects"] = projects
	if builder == nil {
		for key, value := range codexPrivateConfigToolPolicy(policy) {
			privateConfig[key] = value
		}
	}
	if builder != nil {
		isolation, err := codexBuilderIsolationConfig(builder.Gateway)
		if err != nil {
			return nil, err
		}
		for key, value := range isolation {
			privateConfig[key] = value
		}
	}
	rendered, err := toml.Marshal(privateConfig)
	if err != nil {
		return nil, fmt.Errorf("rendering private Codex config: %w", err)
	}
	if builder != nil {
		// The document is re-read as TOML before the provider ever sees it. A
		// composition that renders but does not round-trip would be discovered by
		// Codex refusing to start, which surfaces as a launch failure rather than as
		// the capability answer this boundary owes its caller.
		var check map[string]any
		if err := toml.Unmarshal(rendered, &check); err != nil {
			return nil, &CodexContextError{
				Code: "context_builder_tool_isolation_unavailable",
				Message: fmt.Sprintf(
					"the Codex context-builder configuration composed for %s is not loadable TOML, so this "+
						"launch cannot be shown to have run under the evidence-gateway policy",
					builder.CtxID,
				), Cause: err}
		}
	}
	return rendered, nil
}

func codexSourceHome() (string, error) {
	sourceHome := strings.TrimSpace(os.Getenv("CODEX_HOME"))
	if sourceHome == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolving Codex home: %w", err)
		}
		sourceHome = filepath.Join(userHome, ".codex")
	}
	absolute, err := filepath.Abs(sourceHome)
	if err != nil {
		return "", fmt.Errorf("resolving Codex home %q: %w", sourceHome, err)
	}
	return filepath.Clean(absolute), nil
}

// prepareCodexAdoptionStore exposes only the selected active rollout inside
// the manager's private CODEX_HOME. Keeping it as a symlink is intentional:
// app-server state-db fallback reconstructs the selected rollout, and later
// turns must continue writing to the original conversation instead of a forked
// private copy. The app-server still owns validation via thread/resume, while
// unrelated user threads remain outside its store.
func prepareCodexAdoptionStore(privateHome, threadID string) error {
	sourceHome, err := codexSourceHome()
	if err != nil {
		return err
	}
	sourceSessions := filepath.Join(sourceHome, "sessions")
	sourceRollout, err := findCodexThreadRollout(sourceSessions, threadID)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(sourceSessions, sourceRollout)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("resolving Codex thread %s rollout path", threadID)
	}
	targetRollout := filepath.Join(privateHome, "sessions", relative)
	if err := os.MkdirAll(filepath.Dir(targetRollout), 0o700); err != nil {
		return fmt.Errorf("creating private Codex adoption store: %w", err)
	}
	if err := os.Chmod(filepath.Dir(targetRollout), 0o700); err != nil {
		return fmt.Errorf("securing private Codex adoption store: %w", err)
	}
	if existing, err := os.Readlink(targetRollout); err == nil {
		if existing == sourceRollout {
			return nil
		}
		return fmt.Errorf("private Codex adoption rollout already links to %q", existing)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checking private Codex adoption rollout: %w", err)
	}
	if err := os.Symlink(sourceRollout, targetRollout); err != nil {
		return fmt.Errorf("linking Codex adoption rollout into private store: %w", err)
	}
	return nil
}

func findCodexThreadRollout(sessionsDir, threadID string) (string, error) {
	suffix := "-" + strings.ToLower(threadID) + ".jsonl"
	var matches []string
	err := filepath.WalkDir(sessionsDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), suffix) {
			return nil
		}
		matches = append(matches, path)
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("scanning Codex app-server store %s: %w", sessionsDir, err)
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return "", &PublicError{
			Code: CodexErrorThreadNotFound,
			Message: fmt.Sprintf(
				"Codex thread %s was not found in the active app-server store selected by CODEX_HOME; select an existing UUID with `codex resume --all` (unarchive it first if needed), then retry `task-board codex resume %s`",
				threadID,
				threadID,
			),
		}
	default:
		return "", &PublicError{
			Code: CodexErrorThreadResumeFailed,
			Message: fmt.Sprintf(
				"Codex thread %s has multiple active rollouts in the selected CODEX_HOME; resolve the store ambiguity with plain `codex resume %s` before adoption",
				threadID,
				threadID,
			),
		}
	}
}

var codexPrivateConfigMirroredKeys = []string{"project_doc_max_bytes", "service_tier"}

func validateCodexHostSnapshot(snapshot *CodexRunnerSnapshot) error {
	if snapshot == nil ||
		snapshot.Contract != codexHostSnapshotContract ||
		snapshot.PID <= 0 ||
		strings.TrimSpace(snapshot.URL) == "" ||
		strings.TrimSpace(snapshot.ThreadID) == "" ||
		!filepath.IsAbs(snapshot.ProjectDir) ||
		!filepath.IsAbs(snapshot.CodexHome) {
		return fmt.Errorf("invalid Codex provider-host snapshot")
	}
	return nil
}
