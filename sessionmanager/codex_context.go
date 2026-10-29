package sessionmanager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/relux-works/skill-agents-management/pkg/agentic"
	curatorenv "github.com/relux-works/swarma-session-host/sessionmanager/runner/environment"
)

const (
	codexContextForkPlanContract = "task-board.codex-context-fork-plan-v1"
	codexContextAssayContract    = "task-board.codex-context-assay-attestation-v1"
	codexContextAssayPermission  = "task-board-context-assay"
	codexContextCapabilityError  = "codex_context_capability_unavailable"
	codexContextBaseNotForkable  = "codex_context_base_not_forkable"
	codexContextStartupTimeout   = 20 * time.Second
	codexContextStabilityWindow  = time.Second
	codexContextAssayMaxAttempts = 3
	codexContextForkPlanInvalid  = "codex_context_fork_plan_invalid"
)

// codexThreadForkParams and codexTurnStartParams intentionally model separate
// app-server shapes. thread/fork accepts the legacy sandbox enum; turn/start
// accepts a structured sandboxPolicy. Sharing one map here previously made a
// provider upgrade capable of silently moving a field onto the wrong RPC.
type codexThreadForkParams struct {
	ThreadID              string         `json:"threadId"`
	LastTurnID            string         `json:"lastTurnId"`
	Cwd                   string         `json:"cwd,omitempty"`
	DeferGoalContinuation bool           `json:"deferGoalContinuation,omitempty"`
	Ephemeral             bool           `json:"ephemeral,omitempty"`
	ApprovalPolicy        string         `json:"approvalPolicy,omitempty"`
	Permissions           string         `json:"permissions,omitempty"`
	RuntimeWorkspaceRoots []string       `json:"runtimeWorkspaceRoots"`
	Config                map[string]any `json:"config,omitempty"`
	BaseInstructions      string         `json:"baseInstructions,omitempty"`
	DeveloperInstructions string         `json:"developerInstructions,omitempty"`
}

type codexReadOnlySandboxPolicy struct {
	Type          string `json:"type"`
	NetworkAccess bool   `json:"networkAccess"`
}

type codexTurnStartParams struct {
	ThreadID              string                      `json:"threadId"`
	Input                 []codexTextInput            `json:"input"`
	Cwd                   string                      `json:"cwd,omitempty"`
	ApprovalPolicy        string                      `json:"approvalPolicy,omitempty"`
	Permissions           string                      `json:"permissions,omitempty"`
	RuntimeWorkspaceRoots []string                    `json:"runtimeWorkspaceRoots"`
	Environments          []any                       `json:"environments,omitempty"`
	SandboxPolicy         *codexReadOnlySandboxPolicy `json:"sandboxPolicy,omitempty"`
	OutputSchema          json.RawMessage             `json:"outputSchema,omitempty"`
}

type codexTurnSteerParams struct {
	ThreadID       string           `json:"threadId"`
	ExpectedTurnID string           `json:"expectedTurnId"`
	Input          []codexTextInput `json:"input"`
}

type codexTextInput struct {
	Type         string `json:"type"`
	Text         string `json:"text"`
	TextElements []any  `json:"text_elements"`
}

type codexThreadInjectItemsParams struct {
	ThreadID string `json:"threadId"`
	Items    []any  `json:"items"`
}

type codexThreadReadParams struct {
	ThreadID     string `json:"threadId"`
	IncludeTurns bool   `json:"includeTurns"`
}

type codexForkResponse struct {
	Thread                  codexThread `json:"thread"`
	InstructionSources      []string    `json:"instructionSources"`
	ActivePermissionProfile *struct {
		ID string `json:"id"`
	} `json:"activePermissionProfile"`
}

// requireCodexPayloadPresent probes the machine-local rollout store and turns
// the verdict into the decision to serve or refuse.
//
// The refusal is deliberately routed through the registry helper rather than
// composed here: a vanished payload is a handled path with a provider-specific
// remedy, and having the adapter phrase its own message is how the two providers
// drift into saying different things about the same situation.
func (f *CodexHostFactory) requireCodexPayloadPresent(ctxID string, source *CodexRunnerSnapshot, readyAt time.Time, mode ContextResolutionMode, at time.Time) error {
	if source == nil {
		return fmt.Errorf("a Codex host snapshot is required to verify payload presence")
	}
	if f.contextPolicy == nil {
		return ErrHostCapabilityUnavailable
	}
	return f.contextPolicy.RequirePayloadPresent(probeCodexPayloadPresence(ctxID, source.CodexHome, source.ThreadID, at), readyAt, mode)
}
func (f *CodexHostFactory) requirePrimaryPayloadPresent(p *CodexContextForkPlan, source *CodexRunnerSnapshot, at time.Time) error {
	if p == nil {
		return fmt.Errorf("a Codex fork plan is required to verify payload presence")
	}
	return f.requireCodexPayloadPresent(p.PrimaryCtxID, source, p.PrimaryReadyAt, p.PrimaryMode, at)
}

type codexForkMaterialization struct {
	ThreadID          string
	ForkedFromID      string
	ProviderVersion   string
	BaseRolloutSHA256 string
	LastTurnID        string
	// InheritedRollout is the fork point: the fork's own rollout as it stood the
	// moment it was materialized, before the child had run a turn.
	//
	// It is measured rather than inferred. `thread/fork` gives the child a
	// rollout that already carries the base's conversation, and injection adds
	// the boundary banner and handoffs on top, so everything in the file at this
	// instant is inherited by construction and everything appended after it is
	// the fork's own work. Recording the size and digest here is what later lets
	// a reader skip exactly that prefix and prove the region it skipped is the
	// one that was copied.
	InheritedRollout codexRolloutPrefix
}

// codexRolloutPrefix identifies a byte-exact leading region of a rollout file.
//
// The digest is not decoration. Skipping by offset alone assumes the provider
// only ever appends; the digest turns that assumption into something a reader
// checks, so a rewritten record store is caught instead of silently shifting
// every access past the boundary.
type codexRolloutPrefix = CodexRolloutPrefix

// Known reports whether a fork point was actually recorded. A zero-byte prefix
// is a real answer — a fork with nothing inherited — only when it carries the
// digest of the empty region, so the absent case stays distinguishable.
func (p CodexRolloutPrefix) Known() bool {
	return strings.TrimSpace(p.SHA256) != "" && p.Bytes >= 0
}

type codexAssayPolicy struct {
	PermissionProfile     string            `json:"permission_profile"`
	Filesystem            map[string]string `json:"filesystem"`
	NetworkEnabled        bool              `json:"network_enabled"`
	ProviderFeatures      map[string]bool   `json:"provider_features"`
	MCPConfiguration      string            `json:"mcp_configuration"`
	ReadinessBarrier      string            `json:"readiness_barrier"`
	RuntimeWorkspaceRoots []string          `json:"runtime_workspace_roots"`
	Repository            string            `json:"repository"`
	ExternalResources     string            `json:"external_resources"`
	Injection             string            `json:"injection"`
	Tools                 string            `json:"tools"`
}

// CodexContextAssayRequest is the provider adapter half of CV-2. The caller
// supplies only the quiz prompt and its strict output schema; repository paths,
// preconditions, injected items, and external resources have no input field.
type CodexContextAssayRequest struct {
	BaseCtxID    string          `json:"base_ctx_id"`
	Prompt       string          `json:"prompt"`
	OutputSchema json.RawMessage `json:"output_schema"`
}

type CodexContextAssayAttestation struct {
	Contract        string    `json:"contract"`
	PolicyDigest    string    `json:"policy_digest"`
	ProviderVersion string    `json:"provider_version"`
	BaseThreadID    string    `json:"base_thread_id"`
	ForkThreadID    string    `json:"fork_thread_id"`
	ForkedFromID    string    `json:"forked_from_id"`
	TurnID          string    `json:"turn_id"`
	ToolEventCount  int       `json:"tool_event_count"`
	CompletedAt     time.Time `json:"completed_at"`
}

type CodexContextAssayResult struct {
	Output      json.RawMessage              `json:"output"`
	Attestation CodexContextAssayAttestation `json:"attestation"`
	Usage       ProviderUsage                `json:"usage,omitempty"`
}

type codexContextAssayInterruptedError struct {
	turnID string
	detail json.RawMessage
}

func (e *codexContextAssayInterruptedError) Error() string {
	return fmt.Sprintf(
		"turn %s ended as interrupted: %s",
		e.turnID,
		string(e.detail),
	)
}

type codexContextAssayer interface {
	RunContextAssay(
		context.Context,
		ProviderHostRecord,
		CodexContextAssayRequest,
	) (*CodexContextAssayResult, error)
}

func codexContextAssayPermissionProfile() map[string]any {
	return map[string]any{
		"filesystem": map[string]any{
			":root": "deny",
		},
		"network": map[string]any{
			"enabled": false,
		},
	}
}

func codexContextAssayPolicy() codexAssayPolicy {
	return codexAssayPolicy{
		PermissionProfile: codexContextAssayPermission,
		Filesystem:        map[string]string{":root": "deny"},
		NetworkEnabled:    false,
		ProviderFeatures: map[string]bool{
			"apps":                         false,
			"plugins":                      false,
			"remote_plugin":                false,
			"plugin_sharing":               false,
			"skill_mcp_dependency_install": false,
		},
		MCPConfiguration:      "empty",
		ReadinessBarrier:      "thread_started_mcp_settled_idle_stable",
		RuntimeWorkspaceRoots: []string{},
		Repository:            "denied",
		ExternalResources:     "require_empty_thread_mcp_inventory",
		Injection:             "denied",
		Tools:                 "deny_filesystem_and_network_reject_any_provider_tool_item",
	}
}

func (f *CodexHostFactory) RunContextAssay(
	ctx context.Context,
	sourceRecord ProviderHostRecord,
	request CodexContextAssayRequest,
) (*CodexContextAssayResult, error) {
	if f.contextPolicy == nil {
		return nil, ErrHostCapabilityUnavailable
	}
	if strings.TrimSpace(request.Prompt) == "" {
		return nil, fmt.Errorf("Codex context assay prompt is required")
	}
	if !json.Valid(request.OutputSchema) {
		return nil, fmt.Errorf("Codex context assay output schema must be valid JSON")
	}
	var lastInterrupt error
	for attempt := 1; attempt <= codexContextAssayMaxAttempts; attempt++ {
		result, err := f.runContextAssayAttempt(ctx, sourceRecord, request)
		if err == nil {
			return result, nil
		}
		var interrupted *codexContextAssayInterruptedError
		if !errors.As(err, &interrupted) {
			return nil, err
		}
		lastInterrupt = err
		if ctx.Err() != nil {
			return nil, assayIsolationUnavailable(
				"retrying an interrupted isolated assay turn",
				ctx.Err(),
			)
		}
	}
	return nil, assayIsolationUnavailable(
		fmt.Sprintf(
			"running the isolated assay turn after %d fresh-fork attempts",
			codexContextAssayMaxAttempts,
		),
		lastInterrupt,
	)
}

func (f *CodexHostFactory) runContextAssayAttempt(
	ctx context.Context,
	sourceRecord ProviderHostRecord,
	request CodexContextAssayRequest,
) (*CodexContextAssayResult, error) {
	source, err := decodeCodexHostSnapshot(sourceRecord)
	if err != nil {
		return nil, assayIsolationUnavailable("decoding the base provider handle", err)
	}
	if strings.TrimSpace(source.LastCompletedTurnID) == "" {
		return nil, assayIsolationUnavailable(
			"validating the base turn",
			fmt.Errorf("base has no manager-observed completed turn"),
		)
	}
	// The assay also serves the base — it forks it — so it verifies presence on
	// the same terms. Without this it would report "isolation unavailable" for a
	// payload that simply no longer exists, which is a different problem with a
	// different fix.
	if err := f.requireCodexPayloadPresent(
		request.BaseCtxID, source, time.Time{}, ContextResolutionMode("explicit"), time.Now(),
	); err != nil {
		return nil, err
	}
	sourceRollout, err := findCodexThreadRollout(
		filepath.Join(source.CodexHome, codexSessionsDirName),
		source.ThreadID,
	)
	if err != nil {
		return nil, assayIsolationUnavailable("locating the base rollout", err)
	}
	beforeDigest, err := sha256File(sourceRollout)
	if err != nil {
		return nil, assayIsolationUnavailable("hashing the base rollout", err)
	}
	assaySource := f.assaySource
	if assaySource == nil {
		assaySource = func(
			ctx context.Context,
			source *CodexRunnerSnapshot,
		) (*codexRPCClient, func(), error) {
			return startCodexForkSource(
				ctx,
				source,
				[]string{
					"--disable", "apps",
					"--disable", "plugins",
					"--disable", "remote_plugin",
					"--disable", "plugin_sharing",
					"--disable", "skill_mcp_dependency_install",
					"-c", "mcp_servers={}",
					"-c", "apps._default.enabled=false",
					"-c", `approval_policy="never"`,
					"-c", `default_permissions="task-board-context-assay"`,
					"-c", `permissions.task-board-context-assay={filesystem={":root"="deny"},network={enabled=false}}`,
				},
			)
		}
	}
	client, cleanupSource, err := assaySource(ctx, source)
	if err != nil {
		return nil, assayIsolationUnavailable("initializing app-server", err)
	}
	defer cleanupSource()
	defer client.close()
	base, err := readCodexThread(ctx, client, source.ThreadID)
	if err != nil {
		return nil, assayIsolationUnavailable("reading the base thread", err)
	}
	if err := validateCodexForkBase(base, source.LastCompletedTurnID); err != nil {
		return nil, err
	}
	// No pre-emptive version comparison, for the same reason the ordinary fork
	// path has none: the assay's whole job is to find out whether this base is
	// still usable here, and refusing on the gap alone would answer that question
	// without ever asking the provider.
	emptyCwd, err := os.MkdirTemp("", "codex-context-assay-")
	if err != nil {
		return nil, assayIsolationUnavailable("creating an empty assay cwd", err)
	}
	defer os.RemoveAll(emptyCwd)
	policy := codexContextAssayPolicy()
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return nil, assayIsolationUnavailable("encoding the assay policy", err)
	}
	policyDigest := sha256.Sum256(policyJSON)
	forkRaw, err := client.request(ctx, "thread/fork", codexThreadForkParams{
		ThreadID:              source.ThreadID,
		LastTurnID:            source.LastCompletedTurnID,
		Cwd:                   emptyCwd,
		DeferGoalContinuation: true,
		ApprovalPolicy:        "never",
		Permissions:           codexContextAssayPermission,
		RuntimeWorkspaceRoots: []string{},
		Config: map[string]any{
			"default_permissions": codexContextAssayPermission,
			"permissions": map[string]any{
				codexContextAssayPermission: codexContextAssayPermissionProfile(),
			},
			"mcp_servers": map[string]any{},
			"apps": map[string]any{
				"_default": map[string]any{"enabled": false},
			},
			"web_search": "disabled",
		},
	})
	if err != nil {
		// The fork was attempted and the app-server refused it. That is the post-
		// mortem the version gap is allowed to explain: the isolation diagnosis
		// stays as the cause, because "which call the app-server refused" is what
		// separates upgrade-the-CLI from rebuild-the-base, and the outer error is
		// what makes the base recoverable rather than only unusable.
		return nil, f.contextPolicy.ResumeUnusable(CodexResumeAttempt{
			CtxID:           request.BaseCtxID,
			Provider:        "codex",
			RecordedVersion: source.ProviderVersion,
			ActiveVersion:   base.CLIVersion,
			Operation:       "thread/fork (assay)",
			Cause:           assayIsolationUnavailable("enforcing the assay fork policy", err),
		})
	}
	var forkResponse codexForkResponse
	if err := json.Unmarshal(forkRaw, &forkResponse); err != nil {
		return nil, assayIsolationUnavailable("decoding the assay fork response", err)
	}
	assayTurnID := ""
	if forkResponse.Thread.ID != "" {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if assayTurnID != "" {
				_, _ = client.request(cleanupCtx, "turn/interrupt", map[string]any{
					"threadId": forkResponse.Thread.ID,
					"turnId":   assayTurnID,
				})
			}
			_, _ = client.request(cleanupCtx, "thread/delete", map[string]any{
				"threadId": forkResponse.Thread.ID,
			})
		}()
	}
	if forkResponse.Thread.ID == "" ||
		forkResponse.Thread.ForkedFromID != source.ThreadID {
		return nil, assayIsolationUnavailable(
			"verifying assay fork lineage",
			fmt.Errorf(
				"fork returned thread=%q forkedFromId=%q",
				forkResponse.Thread.ID,
				forkResponse.Thread.ForkedFromID,
			),
		)
	}
	if forkResponse.ActivePermissionProfile == nil ||
		forkResponse.ActivePermissionProfile.ID != codexContextAssayPermission {
		return nil, assayIsolationUnavailable(
			"verifying the active permission profile",
			fmt.Errorf(
				"app-server reported profile %#v, want %q",
				forkResponse.ActivePermissionProfile,
				codexContextAssayPermission,
			),
		)
	}
	if err := waitForCodexAssayForkReady(
		ctx,
		client,
		forkResponse.Thread.ID,
		func() time.Duration {
			if f.assayStabilityWindow > 0 {
				return f.assayStabilityWindow
			}
			return codexContextStabilityWindow
		}(),
	); err != nil {
		return nil, assayIsolationUnavailable(
			"observing thread startup and empty MCP inventory",
			err,
		)
	}
	turnRaw, err := client.request(ctx, "turn/start", codexTurnStartParams{
		ThreadID:              forkResponse.Thread.ID,
		Input:                 []codexTextInput{{Type: "text", Text: request.Prompt, TextElements: []any{}}},
		Cwd:                   emptyCwd,
		RuntimeWorkspaceRoots: []string{},
		Environments:          []any{},
		SandboxPolicy: &codexReadOnlySandboxPolicy{
			Type:          "readOnly",
			NetworkAccess: false,
		},
		OutputSchema: codexCloneRaw(request.OutputSchema),
	})
	if err != nil {
		return nil, assayIsolationUnavailable("starting the isolated assay turn", err)
	}
	var turnResponse struct {
		Turn codexTurn `json:"turn"`
	}
	if err := json.Unmarshal(turnRaw, &turnResponse); err != nil {
		return nil, assayIsolationUnavailable("decoding assay turn/start", err)
	}
	turnID := strings.TrimSpace(turnResponse.Turn.ID)
	if turnID == "" {
		return nil, assayIsolationUnavailable(
			"correlating the assay turn",
			fmt.Errorf("turn/start returned no turn id"),
		)
	}
	assayTurnID = turnID

	var usage ProviderUsage
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		thread, readErr := readCodexThread(ctx, client, forkResponse.Thread.ID)
		if readErr != nil {
			return nil, assayIsolationUnavailable("reading the isolated assay turn", readErr)
		}
		notifications := client.drainNotifications()
		usage, err = inspectCodexAssayNotifications(
			notifications,
			forkResponse.Thread.ID,
			turnID,
			usage,
		)
		if err != nil {
			return nil, err
		}
		turn := findCodexTurn(thread, turnID)
		if turn == nil {
			select {
			case <-ctx.Done():
				return nil, assayIsolationUnavailable(
					"waiting for the isolated assay turn to appear",
					ctx.Err(),
				)
			case <-ticker.C:
			}
			continue
		}
		output, toolCount, inspectErr := inspectCodexAssayItems(turn.Items)
		if inspectErr != nil {
			return nil, inspectErr
		}
		if toolCount != 0 {
			return nil, &CodexContextError{
				Code: "context_assay_tool_use_detected",
				Message: fmt.Sprintf(
					"Codex context assay turn %s emitted %d tool event(s); scoring is refused",
					turnID,
					toolCount,
				),
			}
		}
		switch turn.Status {
		case "completed":
			if len(output) == 0 || !json.Valid(output) {
				return nil, assayIsolationUnavailable(
					"validating the schema-constrained assay output",
					fmt.Errorf("completed turn returned no valid JSON agent message"),
				)
			}
			if err := verifyCodexAssayBaseUnchanged(sourceRollout, beforeDigest); err != nil {
				return nil, err
			}
			version := forkResponse.Thread.CLIVersion
			if version == "" {
				version = base.CLIVersion
			}
			return &CodexContextAssayResult{
				Output: output,
				Attestation: CodexContextAssayAttestation{
					Contract:        codexContextAssayContract,
					PolicyDigest:    hex.EncodeToString(policyDigest[:]),
					ProviderVersion: version,
					BaseThreadID:    source.ThreadID,
					ForkThreadID:    forkResponse.Thread.ID,
					ForkedFromID:    forkResponse.Thread.ForkedFromID,
					TurnID:          turnID,
					ToolEventCount:  0,
					CompletedAt:     time.Now().UTC(),
				},
				Usage: usage,
			}, nil
		case "failed":
			return nil, assayIsolationUnavailable(
				"running the isolated assay turn",
				fmt.Errorf("turn %s ended as %s: %s", turnID, turn.Status, string(turn.Error)),
			)
		case "interrupted":
			if err := verifyCodexAssayBaseUnchanged(sourceRollout, beforeDigest); err != nil {
				return nil, err
			}
			return nil, &codexContextAssayInterruptedError{
				turnID: turnID,
				detail: codexCloneRaw(turn.Error),
			}
		}
		select {
		case <-ctx.Done():
			return nil, assayIsolationUnavailable("waiting for the isolated assay turn", ctx.Err())
		case <-ticker.C:
		}
	}
}

// codexContextMessageItem wraps one composed text block as an injectable item.
func codexContextMessageItem(text string) map[string]any {
	return map[string]any{
		"type": "message",
		"role": "assistant",
		"content": []map[string]any{{
			"type": "output_text",
			"text": text,
		}},
	}
}

func verifyCodexAssayBaseUnchanged(sourceRollout string, beforeDigest string) error {
	afterDigest, err := sha256File(sourceRollout)
	if err != nil {
		return assayIsolationUnavailable("rehashing the base rollout", err)
	}
	if afterDigest != beforeDigest {
		return assayIsolationUnavailable(
			"verifying base immutability",
			fmt.Errorf("base rollout changed from %s to %s", beforeDigest, afterDigest),
		)
	}
	return nil
}

type codexMCPServerInventory struct {
	Name              string                     `json:"name"`
	Tools             map[string]json.RawMessage `json:"tools"`
	Resources         []json.RawMessage          `json:"resources"`
	ResourceTemplates []json.RawMessage          `json:"resourceTemplates"`
}

type codexMCPInventoryPage struct {
	Data       []codexMCPServerInventory `json:"data"`
	NextCursor *string                   `json:"nextCursor"`
}

func waitForCodexAssayForkReady(
	ctx context.Context,
	client *codexRPCClient,
	threadID string,
	stabilityWindow time.Duration,
) error {
	startupCtx, cancel := context.WithTimeout(ctx, codexContextStartupTimeout)
	defer cancel()
	threadStarted := false
	startupStates := make(map[string]string)
	var stableSince time.Time
	consumeNotifications := func() error {
		for _, notification := range client.drainNotifications() {
			switch notification.Method {
			case "thread/started":
				var params struct {
					Thread codexThread `json:"thread"`
				}
				if json.Unmarshal(notification.Params, &params) == nil &&
					params.Thread.ID == threadID {
					threadStarted = true
				}
			case "mcpServer/startupStatus/updated":
				var params struct {
					ThreadID string `json:"threadId"`
					Name     string `json:"name"`
					Status   string `json:"status"`
				}
				if json.Unmarshal(notification.Params, &params) == nil &&
					params.ThreadID == threadID &&
					strings.TrimSpace(params.Name) != "" {
					startupStates[params.Name] = params.Status
				}
			}
		}
		return nil
	}
	cursor := ""
	for {
		if err := consumeNotifications(); err != nil {
			return err
		}

		if !threadStarted {
			if _, err := readCodexThread(startupCtx, client, threadID); err != nil {
				return fmt.Errorf("waiting for thread/started for %s: %w", threadID, err)
			}
			select {
			case <-startupCtx.Done():
				return fmt.Errorf("waiting for thread/started for %s: %w", threadID, startupCtx.Err())
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}

		raw, err := client.request(startupCtx, "mcpServerStatus/list", map[string]any{
			"threadId": threadID,
			"detail":   "full",
			"limit":    1000,
			"cursor": func() any {
				if cursor == "" {
					return nil
				}
				return cursor
			}(),
		})
		if err != nil {
			return fmt.Errorf("mcpServerStatus/list for %s: %w", threadID, err)
		}
		var page codexMCPInventoryPage
		if err := json.Unmarshal(raw, &page); err != nil {
			return fmt.Errorf("decoding mcpServerStatus/list for %s: %w", threadID, err)
		}
		if err := consumeNotifications(); err != nil {
			return err
		}
		starting := false
		for _, status := range startupStates {
			if status == "starting" {
				starting = true
				break
			}
		}
		if starting {
			stableSince = time.Time{}
			cursor = ""
			select {
			case <-startupCtx.Done():
				return fmt.Errorf("MCP startup did not settle: %w", startupCtx.Err())
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		for _, server := range page.Data {
			if len(server.Tools) != 0 ||
				len(server.Resources) != 0 ||
				len(server.ResourceTemplates) != 0 {
				return fmt.Errorf(
					"MCP server %q exposes tools=%d resources=%d templates=%d under the assay thread",
					server.Name,
					len(server.Tools),
					len(server.Resources),
					len(server.ResourceTemplates),
				)
			}
		}
		if page.NextCursor != nil && strings.TrimSpace(*page.NextCursor) != "" {
			cursor = strings.TrimSpace(*page.NextCursor)
			continue
		}
		thread, err := readCodexThread(startupCtx, client, threadID)
		if err != nil {
			return fmt.Errorf("reading the settled assay fork %s: %w", threadID, err)
		}
		if err := consumeNotifications(); err != nil {
			return err
		}
		if !threadStarted {
			cursor = ""
			continue
		}
		activeTurn := false
		for index := range thread.Turns {
			if thread.Turns[index].Status == "inProgress" {
				activeTurn = true
				break
			}
		}
		if thread.Status.Type == "idle" && !activeTurn {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if remaining := stabilityWindow - time.Since(stableSince); remaining > 0 {
				cursor = ""
				select {
				case <-startupCtx.Done():
					return fmt.Errorf(
						"assay fork did not remain idle for %s: %w",
						stabilityWindow,
						startupCtx.Err(),
					)
				case <-time.After(remaining):
				}
				continue
			}
			return nil
		}
		stableSince = time.Time{}
		cursor = ""
		select {
		case <-startupCtx.Done():
			return fmt.Errorf("assay fork did not settle idle: %w", startupCtx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func inspectCodexAssayNotifications(
	notifications []codexRPCNotification,
	threadID string,
	turnID string,
	usage ProviderUsage,
) (ProviderUsage, error) {
	for _, notification := range notifications {
		var identity struct {
			ThreadID string          `json:"threadId"`
			TurnID   string          `json:"turnId"`
			Item     json.RawMessage `json:"item"`
		}
		if json.Unmarshal(notification.Params, &identity) != nil ||
			identity.ThreadID != threadID ||
			identity.TurnID != turnID {
			continue
		}
		switch notification.Method {
		case "thread/tokenUsage/updated":
			var params struct {
				TokenUsage struct {
					Last struct {
						InputTokens           int64 `json:"inputTokens"`
						CachedInputTokens     int64 `json:"cachedInputTokens"`
						CacheWriteInputTokens int64 `json:"cacheWriteInputTokens"`
						OutputTokens          int64 `json:"outputTokens"`
						ReasoningOutputTokens int64 `json:"reasoningOutputTokens"`
						TotalTokens           int64 `json:"totalTokens"`
					} `json:"last"`
				} `json:"tokenUsage"`
			}
			if json.Unmarshal(notification.Params, &params) == nil {
				usage = ProviderUsage{
					InputTokens:           params.TokenUsage.Last.InputTokens,
					CachedInputTokens:     params.TokenUsage.Last.CachedInputTokens,
					CacheCreationTokens:   params.TokenUsage.Last.CacheWriteInputTokens,
					OutputTokens:          params.TokenUsage.Last.OutputTokens,
					ReasoningOutputTokens: params.TokenUsage.Last.ReasoningOutputTokens,
					TotalTokens:           params.TokenUsage.Last.TotalTokens,
				}
			}
		case "item/started", "item/completed":
			_, toolCount, err := inspectCodexAssayItems([]json.RawMessage{identity.Item})
			if err != nil {
				return usage, err
			}
			if toolCount != 0 {
				var itemType struct {
					Type string `json:"type"`
				}
				_ = json.Unmarshal(identity.Item, &itemType)
				return usage, &CodexContextError{
					Code: "context_assay_tool_use_detected",
					Message: fmt.Sprintf(
						"Codex context assay turn %s emitted tool event type %s; scoring is refused",
						turnID,
						itemType.Type,
					),
				}
			}
		}
	}
	return usage, nil
}

func inspectCodexAssayItems(
	items []json.RawMessage,
) (json.RawMessage, int, error) {
	var output json.RawMessage
	toolCount := 0
	for _, raw := range items {
		var item struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(raw, &item); err != nil || item.Type == "" {
			return nil, 0, assayIsolationUnavailable(
				"observing assay item types",
				fmt.Errorf("unknown provider item shape: %s", string(raw)),
			)
		}
		switch item.Type {
		case "agentMessage":
			if strings.TrimSpace(item.Text) != "" {
				output = json.RawMessage(strings.TrimSpace(item.Text))
			}
		case "userMessage", "reasoning", "plan":
		case "hookPrompt",
			"commandExecution",
			"fileChange",
			"mcpToolCall",
			"dynamicToolCall",
			"collabAgentToolCall",
			"subAgentActivity",
			"webSearch",
			"imageView",
			"sleep",
			"imageGeneration":
			toolCount++
		case "enteredReviewMode", "exitedReviewMode", "contextCompaction":
			return nil, 0, assayIsolationUnavailable(
				"observing assay item types",
				fmt.Errorf("provider emitted unsupported passive item type %q", item.Type),
			)
		default:
			return nil, 0, assayIsolationUnavailable(
				"observing assay item types",
				fmt.Errorf("provider emitted unknown item type %q", item.Type),
			)
		}
	}
	return output, toolCount, nil
}

func assayIsolationUnavailable(step string, cause error) error {
	return &CodexContextError{
		Code:  "context_assay_isolation_unavailable",
		Cause: cause,
		Message: fmt.Sprintf(
			"Codex context assay isolation is unavailable while %s",
			step,
		),
	}
}

func (f *CodexHostFactory) materializeContextFork(
	ctx context.Context,
	plan *CodexContextForkPlan,
	targetHome string,
	projectDir string,
) (*codexForkMaterialization, error) {
	if f.contextPolicy == nil {
		return nil, ErrHostCapabilityUnavailable
	}
	if plan == nil {
		return nil, fmt.Errorf("Codex context fork plan is required")
	}
	source, err := decodeCodexHostSnapshot(plan.Primary)
	if err != nil {
		return nil, codexCapabilityFailure("thread/fork source", "", err)
	}
	if strings.TrimSpace(source.LastCompletedTurnID) == "" {
		return nil, &PublicError{
			Code: codexContextBaseNotForkable,
			Message: fmt.Sprintf(
				"Codex context %s has no manager-observed completed turn; refuse before app-server RPC",
				plan.PrimaryCtxID,
			),
		}
	}
	// Presence before anything reads the rollout or opens an RPC. No Codex
	// pruning is documented or has been observed, which is exactly why the probe
	// runs rather than why it could be skipped: there is no known cleanup schedule
	// that would explain an absent rollout, so it has to look. The old path
	// reported the absence as a generic capability failure, which sends an
	// operator to debug the app-server for a file that is simply not there.
	if err := f.requirePrimaryPayloadPresent(plan, source, time.Now()); err != nil {
		return nil, err
	}
	sourceRollout, err := findCodexThreadRollout(
		filepath.Join(source.CodexHome, codexSessionsDirName),
		source.ThreadID,
	)
	if err != nil {
		return nil, codexCapabilityFailure("base rollout verification", source.ProviderVersion, err)
	}
	beforeDigest, compaction, err := f.inspectCodexBaseRollout(sourceRollout)
	if err != nil {
		return nil, codexCapabilityFailure("base rollout verification", source.ProviderVersion, err)
	}
	if compaction != nil {
		return nil, &CodexContextError{
			Code: "context_compacted",
			Message: fmt.Sprintf(
				"Codex base %s rollout carries a %s compaction record and is quarantined from forking",
				plan.PrimaryCtxID,
				compaction.Marker,
			),
		}
	}
	client, cleanupSource, err := dialCodexForkSource(ctx, source)
	if err != nil {
		return nil, codexForkSourceUnavailable(
			plan.PrimaryCtxID,
			source.ProviderVersion,
			err,
		)
	}
	defer cleanupSource()
	defer client.close()
	base, err := readCodexThread(ctx, client, source.ThreadID)
	if err != nil {
		return nil, codexCapabilityFailure("thread/read", source.ProviderVersion, err)
	}
	if base.ID != source.ThreadID {
		return nil, codexCapabilityFailure(
			"thread/read",
			base.CLIVersion,
			fmt.Errorf("read thread %s instead of %s", base.ID, source.ThreadID),
		)
	}
	// A version gap between the build that captured this base and the app-server
	// now answering is deliberately NOT a refusal here. It used to be, and that
	// made the whole attempt unreachable: the accepted research measured contexts
	// resuming across large version gaps, so predicting failure from the gap
	// rejected exactly the work that demonstrably succeeds. The gap is carried
	// forward instead, and it is spent below to explain a fork that actually
	// failed.
	if err := validateCodexForkBase(base, source.LastCompletedTurnID); err != nil {
		return nil, err
	}
	raw, err := client.request(ctx, "thread/fork", codexThreadForkParams{
		ThreadID:              source.ThreadID,
		LastTurnID:            source.LastCompletedTurnID,
		Cwd:                   projectDir,
		DeferGoalContinuation: true,
	})
	if err != nil {
		// The attempt happened first and this is its post-mortem. Codex does not
		// document rollout format stability across builds, so the version gap is
		// named as the explanation for a failure that already occurred rather than
		// used to predict one — the accepted research measured resumes surviving
		// large gaps, and a pre-emptive check would refuse exactly those.
		//
		// The capability failure is kept as the cause rather than replaced. It is
		// the half that says which RPC the active app-server refused, which is what
		// an operator needs when the answer is "upgrade the CLI" rather than
		// "rebuild the base"; the outer error is what makes the base recoverable.
		return nil, f.contextPolicy.ResumeUnusable(CodexResumeAttempt{
			CtxID:           plan.PrimaryCtxID,
			Provider:        "codex",
			RecordedVersion: source.ProviderVersion,
			ActiveVersion:   base.CLIVersion,
			Operation:       "thread/fork",
			Cause:           codexCapabilityFailure("thread/fork", base.CLIVersion, err),
		})
	}
	var response codexForkResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, codexCapabilityFailure("thread/fork response", base.CLIVersion, err)
	}
	if strings.TrimSpace(response.Thread.ID) == "" ||
		response.Thread.ID == source.ThreadID ||
		response.Thread.ForkedFromID != source.ThreadID {
		return nil, codexCapabilityFailure(
			"thread/fork lineage",
			base.CLIVersion,
			fmt.Errorf(
				"fork returned thread=%q forkedFromId=%q for source %s",
				response.Thread.ID,
				response.Thread.ForkedFromID,
				source.ThreadID,
			),
		)
	}
	items := make([]any, 0, len(plan.Supplemental)+1)
	// The boundary statement is injected ahead of the inherited handoffs, for the
	// same reason it is composed first on the Claude side: it frames the whole
	// inheritance, and a coverage statement that arrives after the handoffs reads
	// as a footnote to them.
	if banner := strings.TrimSpace(plan.BoundaryBanner); banner != "" {
		items = append(items, codexContextMessageItem(banner))
	}
	for _, supplemental := range plan.Supplemental {
		items = append(items, codexContextMessageItem(fmt.Sprintf(
			"Supplemental lineage %s:\n%s",
			supplemental.CtxID,
			supplemental.Handoff,
		)))
	}
	if len(items) > 0 {
		if _, err := client.request(ctx, "thread/inject_items", codexThreadInjectItemsParams{
			ThreadID: response.Thread.ID,
			Items:    items,
		}); err != nil {
			return nil, codexCapabilityFailure("thread/inject_items", base.CLIVersion, err)
		}
	}
	afterDigest, err := sha256File(sourceRollout)
	if err != nil {
		return nil, codexCapabilityFailure("base rollout verification", base.CLIVersion, err)
	}
	if afterDigest != beforeDigest {
		return nil, &PublicError{
			Code: codexContextBaseNotForkable,
			Message: fmt.Sprintf(
				"Codex thread/fork mutated base rollout %s (before %s, after %s); the child was not materialized",
				plan.PrimaryCtxID,
				beforeDigest,
				afterDigest,
			),
		}
	}
	_, inherited, err := copyCodexThreadRollout(source.CodexHome, targetHome, response.Thread.ID)
	if err != nil {
		return nil, codexCapabilityFailure("fork rollout copy", base.CLIVersion, err)
	}
	version := response.Thread.CLIVersion
	if version == "" {
		version = base.CLIVersion
	}
	return &codexForkMaterialization{
		ThreadID:          response.Thread.ID,
		ForkedFromID:      response.Thread.ForkedFromID,
		ProviderVersion:   version,
		BaseRolloutSHA256: beforeDigest,
		LastTurnID:        source.LastCompletedTurnID,
		InheritedRollout:  inherited,
	}, nil
}

func dialCodexForkSource(
	ctx context.Context,
	source *CodexRunnerSnapshot,
) (*codexRPCClient, func(), error) {
	if source == nil {
		return nil, func() {}, fmt.Errorf("Codex fork source is required")
	}
	if client, err := dialCodexSourceRPC(ctx, source); err == nil {
		return client, func() {}, nil
	}
	return startCodexForkSource(ctx, source, nil)
}

func startCodexForkSource(
	ctx context.Context,
	source *CodexRunnerSnapshot,
	extraArgs []string,
) (*codexRPCClient, func(), error) {
	if source == nil {
		return nil, func() {}, fmt.Errorf("Codex fork source is required")
	}
	if strings.TrimSpace(source.Executable) == "" {
		return nil, func() {}, fmt.Errorf(
			"the recorded source app-server is unavailable and its snapshot has no executable for a temporary restart",
		)
	}
	// A restart is a new launch. Fresh read-only fragment evidence must
	// precede even endpoint reservation, including the assay's new overrides.
	if err := refuseLockedCodexForkSource(ctx, source, extraArgs); err != nil {
		return nil, func() {}, err
	}
	listenURL, err := codexForkSourceReserveURL()
	if err != nil {
		return nil, func() {}, err
	}
	args := codexRestartAppServerArgs(source.HostArgv, listenURL, extraArgs...)
	logFile, err := os.CreateTemp("", "codex-fork-source-*.log")
	if err != nil {
		return nil, func() {}, fmt.Errorf("creating temporary source app-server log: %w", err)
	}
	logPath := logFile.Name()
	command := exec.CommandContext(
		context.WithoutCancel(ctx),
		source.Executable,
		args...,
	)
	command.Dir = source.ProjectDir
	command.Env = replaceEnvironmentValue(os.Environ(), "CODEX_HOME", source.CodexHome)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := codexForkSourceStart(command); err != nil {
		_ = logFile.Close()
		_ = os.Remove(logPath)
		return nil, func() {}, fmt.Errorf("restarting source app-server: %w", err)
	}
	_ = logFile.Close()
	cleanup := func() {
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		_ = command.Wait()
		_ = os.Remove(logPath)
	}
	if err := waitForCodexHostReady(ctx, listenURL, codexHostReadyTimeout); err != nil {
		cleanup()
		return nil, func() {}, fmt.Errorf("waiting for restarted source app-server: %w", err)
	}
	restarted := *source
	restarted.URL = listenURL
	restarted.PID = command.Process.Pid
	restarted.Stopped = false
	client, err := dialCodexSourceRPC(ctx, &restarted)
	if err != nil {
		cleanup()
		return nil, func() {}, fmt.Errorf("initializing restarted source app-server: %w", err)
	}
	return client, cleanup, nil
}

func codexRestartAppServerArgs(
	recorded []string,
	listenURL string,
	extra ...string,
) []string {
	args := make([]string, 0, len(recorded)+len(extra)+2)
	for index := 0; index < len(recorded); index++ {
		arg := recorded[index]
		switch {
		case arg == "--listen":
			if index+1 < len(recorded) {
				index++
			}
		case strings.HasPrefix(arg, "--listen="):
		case arg == "--stdio":
		default:
			args = append(args, arg)
		}
	}
	if len(args) == 0 {
		args = append(args, "app-server")
	}
	args = append(args, extra...)
	return append(args, "--listen", listenURL)
}

func readCodexThread(
	ctx context.Context,
	client *codexRPCClient,
	threadID string,
) (*codexThread, error) {
	raw, err := client.request(ctx, "thread/read", codexThreadReadParams{
		ThreadID:     threadID,
		IncludeTurns: true,
	})
	if err != nil {
		return nil, err
	}
	var response struct {
		Thread codexThread `json:"thread"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	return &response.Thread, nil
}

func validateCodexForkBase(thread *codexThread, expectedLastTurnID string) error {
	if thread == nil || len(thread.Turns) == 0 {
		return &PublicError{
			Code:    codexContextBaseNotForkable,
			Message: "Codex base has an empty rollout and cannot be forked",
		}
	}
	var expected *codexTurn
	for index := range thread.Turns {
		turn := &thread.Turns[index]
		if turn.ID == expectedLastTurnID {
			expected = turn
		}
		for _, item := range turn.Items {
			var envelope struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(item, &envelope) == nil && envelope.Type == "contextCompaction" {
				return &CodexContextError{
					Code: "context_compacted",
					Message: fmt.Sprintf(
						"Codex base %s contains a contextCompaction item and is quarantined from forking",
						thread.ID,
					),
				}
			}
		}
	}
	if expected == nil {
		return &PublicError{
			Code: codexContextBaseNotForkable,
			Message: fmt.Sprintf(
				"Codex base %s no longer contains recorded completed turn %s",
				thread.ID,
				expectedLastTurnID,
			),
		}
	}
	if expected.Status != "completed" {
		return &PublicError{
			Code: codexContextBaseNotForkable,
			Message: fmt.Sprintf(
				"Codex base turn %s is %s, not completed; interrupted or active turns cannot be fork sources",
				expected.ID,
				expected.Status,
			),
		}
	}
	if last := thread.Turns[len(thread.Turns)-1]; last.ID != expected.ID {
		return &PublicError{
			Code: codexContextBaseNotForkable,
			Message: fmt.Sprintf(
				"Codex base advanced from completed turn %s to %s; rematerialize the current ready base",
				expected.ID,
				last.ID,
			),
		}
	}
	return nil
}

func decodeCodexForkPlan(record ProviderHostRecord) (*CodexContextForkPlan, error) {
	s := record.Snapshot
	if record.Provider != "codex" || s.Kind != "codex-fork" || s.CodexFork == nil || s.Codex != nil || s.Claude != nil || s.ClaudeFork != nil || s.MuseMSP != nil || s.MusePTY != nil || s.MusePTYProvenance != nil {
		return nil, fmt.Errorf("Codex fork plan is unavailable")
	}
	plan := copyRunnerSnapshot(s).CodexFork
	if plan.Contract != codexContextForkPlanContract || strings.TrimSpace(plan.PrimaryCtxID) == "" {
		return nil, fmt.Errorf("unsupported Codex fork plan contract or empty primary context")
	}
	return plan, nil
}
func decodeCodexHostSnapshot(record ProviderHostRecord) (*CodexRunnerSnapshot, error) {
	if record.Provider != "codex" || record.Snapshot.Kind != "codex" || record.Snapshot.Codex == nil {
		return nil, ErrProviderHostNotFound
	}
	if err := validateRunnerSnapshot(record); err != nil {
		return nil, err
	}
	snapshot := copyRunnerSnapshot(record.Snapshot).Codex
	if err := validateCodexHostSnapshot(snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func codexCapabilityFailure(method, providerVersion string, cause error) error {
	version := strings.TrimSpace(providerVersion)
	if version == "" {
		version = "unknown"
	}
	return &PublicError{
		Code: codexContextCapabilityError,
		Message: fmt.Sprintf(
			"Codex app-server capability %s is unavailable for provider version %s: %v; upgrade Codex or use a cold context build",
			method,
			version,
			cause,
		),
	}
}

func codexForkPlanFailure(cause error) error {
	return &PublicError{
		Code: codexContextForkPlanInvalid,
		Message: fmt.Sprintf(
			"materialized Codex child requires a private fork plan; refusing immutable-base resume fallback: %v; rematerialize the child from its ready registry base",
			cause,
		),
	}
}

func codexForkSourceUnavailable(
	ctxID string,
	providerVersion string,
	cause error,
) error {
	var refusal *PublicError
	if errors.As(cause, &refusal) && (refusal.Code == CodexFragmentPermissionsLocked || refusal.Code == CodexErrorCuratorProvenanceUnreadable) {
		return fmt.Errorf("restarting Codex context %s source: %w", ctxID, cause)
	}
	version := strings.TrimSpace(providerVersion)
	if version == "" {
		version = "unknown"
	}
	return &PublicError{
		Code: codexContextCapabilityError,
		Message: fmt.Sprintf(
			"Codex context %s source app-server is unavailable and could not be restarted from its private materialization for provider version %s: %v; verify the private CODEX_HOME and recorded executable, then rematerialize the base or use a cold context build",
			ctxID,
			version,
			cause,
		),
	}
}

// inspectCodexBaseRollout reads the base rollout once, for both the byte
// identity the fork is checked against and the authoritative compaction record.
//
// thread/read only exposes the app-server's rendered contextCompaction item;
// the rollout carries the top-level type=compacted record that proves the
// truncation happened, so it is read here, before the source app-server is even
// dialed. A rollout the scanner cannot decode is not turned into a new fork
// failure: the digest still identifies the file, and the thread-item check plus
// the registry's own pre-fork gate stay in force.
func (f *CodexHostFactory) inspectCodexBaseRollout(path string) (string, *CodexTranscriptCompaction, error) {
	if f.contextPolicy == nil {
		return "", nil, ErrHostCapabilityUnavailable
	}
	file, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()
	if digest, marker, scanErr := f.contextPolicy.InspectTranscript(file); scanErr == nil {
		var compaction *CodexTranscriptCompaction
		if marker != "" {
			compaction = &CodexTranscriptCompaction{Marker: marker}
		}
		return digest, compaction, nil
	}
	digest, err := sha256File(path)
	if err != nil {
		return "", nil, err
	}
	return digest, nil, nil
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// copyCodexThreadRollout places the forked thread's rollout in the child's own
// Codex home and reports the fork point it just wrote.
//
// The prefix is measured from the bytes that pass through the copy rather than
// by stat-ing the result afterwards, so the recorded fork point is the file
// this call produced and not whatever the child may already have appended by
// the time anyone looks again.
func copyCodexThreadRollout(
	sourceHome, targetHome, threadID string,
) (string, codexRolloutPrefix, error) {
	sourceSessions := filepath.Join(sourceHome, "sessions")
	source, err := findCodexThreadRollout(sourceSessions, threadID)
	if err != nil {
		return "", codexRolloutPrefix{}, err
	}
	relative, err := filepath.Rel(sourceSessions, source)
	if err != nil || relative == "." || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return "", codexRolloutPrefix{}, fmt.Errorf("resolving forked Codex rollout %s", threadID)
	}
	target := filepath.Join(targetHome, "sessions", relative)
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return "", codexRolloutPrefix{}, err
	}
	if err := os.Chmod(filepath.Dir(target), 0o700); err != nil {
		return "", codexRolloutPrefix{}, err
	}
	input, err := os.Open(source)
	if err != nil {
		return "", codexRolloutPrefix{}, err
	}
	defer input.Close()
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", codexRolloutPrefix{}, err
	}
	hash := sha256.New()
	copied, copyErr := io.Copy(io.MultiWriter(output, hash), input)
	closeErr := output.Close()
	if copyErr != nil {
		return "", codexRolloutPrefix{}, copyErr
	}
	if closeErr != nil {
		return "", codexRolloutPrefix{}, closeErr
	}
	return target, codexRolloutPrefix{
		Bytes:  copied,
		SHA256: hex.EncodeToString(hash.Sum(nil)),
	}, nil
}

var (
	codexForkSourceReserveURL = reserveLoopbackWebsocketURL
	codexForkSourceStart      = (*exec.Cmd).Start
	codexRestartReadFragment  = readCodexRestartFragment
)

// readCodexRestartFragment consults the current fragment in the source project
// and profile, through a single plain resolve. It cannot repair or version-probe.
// The duplicate/fold scan and typed provenance validator are the daemon's own
// read-only evidence checks, shared with fresh host admission.
func readCodexRestartFragment(ctx context.Context, source *CodexRunnerSnapshot, stored agentic.CuratorContextProvenance) (agentic.CuratorContextProvenance, error) {
	current, _, err := resolveCodexHomeFragment(ctx, source.ProjectDir, stored)
	return current, err
}

// resolveCodexHomeFragment shares the plain, bounded, strict resolver with
// private-home preparation. The digest binds Curator's exact fragment bytes
// (minus its terminating LF), just as spawn.ResolveChildContext does.
func resolveCodexHomeFragment(ctx context.Context, projectDir string, stored agentic.CuratorContextProvenance) (agentic.CuratorContextProvenance, string, error) {
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	args := []string{"env", "resolve", "codex_cli", "--format", "json", "--profile", stored.ProfileName}
	command := exec.CommandContext(call, "curator", args...)
	command.Dir = projectDir
	nativeEnv, nativeErr := curatorenv.ForCommand(command, "codex_cli")
	if nativeErr != nil {
		return stored, "", nativeErr
	}
	command.Env = nativeEnv
	var output codexRestartFragmentBuffer
	command.Stdout = &output
	var diagnostics codexRestartFragmentBuffer
	diagnostics.limit = 4096
	command.Stderr = &diagnostics
	if err := command.Run(); err != nil {
		if curatorenv.CredentialConflict(diagnostics.buffer.String()) {
			return stored, "", &curatorenv.CredentialError{}
		}
		return stored, "", err
	}
	if err := rejectDuplicateProvenanceMembers(output.buffer.String()); err != nil {
		return stored, "", err
	}
	var fragment agentic.CuratorFragmentIdentity
	decoder := json.NewDecoder(bytes.NewReader(output.buffer.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fragment); err != nil {
		return stored, "", err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return stored, "", fmt.Errorf("trailing fragment data")
	}
	current := stored
	current.Fragment = fragment
	current.ProfileName = fragment.Profile.Name
	current.LockSHA256 = fragment.Profile.LockSHA256
	current.ManagedHome = fragment.Env[current.ManagedHomeVariable]
	if current.ProfileName != stored.ProfileName {
		return stored, "", fmt.Errorf("fragment profile changed")
	}
	if err := validateCuratorProvenance(current); err != nil {
		return stored, "", err
	}
	digest := sha256.Sum256(bytes.TrimSuffix(output.buffer.Bytes(), []byte{'\n'}))
	return current, hex.EncodeToString(digest[:]), nil
}

// Bound the read identically to the launch-composition fragment limit.
type codexRestartFragmentBuffer struct {
	buffer bytes.Buffer
	limit  int
}

func (b *codexRestartFragmentBuffer) Write(data []byte) (int, error) {
	limit := b.limit
	if limit == 0 {
		limit = 1 << 20
	}
	if b.buffer.Len()+len(data) > limit {
		return 0, fmt.Errorf("fragment exceeds read limit")
	}
	return b.buffer.Write(data)
}

func refuseLockedCodexForkSource(ctx context.Context, source *CodexRunnerSnapshot, extra []string) error {
	stored, present, err := extractCuratorProvenance(source.Sources)
	if err != nil {
		return &curatorIdentityReadError{public: &PublicError{
			Code:    CodexErrorCuratorProvenanceUnreadable,
			Message: "the source carries Curator fragment sources but its provenance snapshot cannot be verified",
		}, cause: err}
	}
	if !present || stored.Fragment.Environment != "codex_cli" {
		return nil
	}
	current, err := codexRestartReadFragment(ctx, source, stored)
	if err != nil {
		// A failed fresh read cannot erase the source's admitted locked
		// evidence. Unlocked or permissionless stored evidence still defers.
		current = stored
	}
	if current.Fragment.Environment != "codex_cli" {
		return nil
	}
	permissions := current.Fragment.Permissions
	if permissions == nil || !permissions.Locked {
		return nil
	}
	// Build the actual restart shape before reserving the new endpoint. The
	// final --listen pair is manager transport, never operator posture. Existing
	// endpoint/stdio replacement remains solely in the restart builder.
	args := codexRestartAppServerArgs(source.HostArgv, "", extra...)
	args = args[:len(args)-2]
	if flag := lockedCodexHostArgvRefused(args, true); flag != "" {
		refusal := LockedCodexSessionRefusal{Knob: "managed-host", Flag: flag}
		return &PublicError{Code: CodexFragmentPermissionsLocked, Message: fmt.Sprintf(
			"permissions locked by source %q to mode %q (runtime %q): %s; %s",
			permissions.Source, permissions.Mode, current.Fragment.Environment, refusal.Describe(), CodexLockedRoute,
		)}
	}
	return nil
}

func dialCodexSourceRPC(ctx context.Context, source *CodexRunnerSnapshot) (*codexRPCClient, error) {
	client, err := dialCodexRPC(ctx, source.URL)
	if err != nil {
		return nil, err
	}
	if err := initializeCodexRunnerRPC(ctx, client); err != nil {
		client.close()
		return nil, err
	}
	return client, nil
}
