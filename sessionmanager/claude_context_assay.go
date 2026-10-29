package sessionmanager

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	// claudeAssayDirName holds the disposable repository-free working directories
	// an assay runs in. It is manager-private state, not provider state.
	claudeAssayDirName = "claude-context-assay"

	// claudeAssayPolicyVersion identifies the enforced-policy document the
	// attestation digests. Changing what the assay enforces must change this, so
	// an attestation can never be read as covering a policy it predates.
	claudeAssayPolicyVersion = "claude-context-assay-policy-v1"

	claudeAssayTimeout = 10 * time.Minute

	// claudeAssaySchemaEnforcement names who constrained the answer's shape, and
	// there is exactly one possible value because there is exactly one possible
	// enforcer.
	//
	// The Codex sibling hands the caller's schema to the provider, which enforces
	// it. Claude cannot: its schema-constrained output is a tool, and this assay
	// denies every tool. So the same contract — the answer conforms to the schema
	// the validator supplied, or nothing is scored — is met here by evaluating that
	// schema in this process. Reporting it keeps a consumer from reading the
	// attestation as a provider-side guarantee it is not, while still promising the
	// enforcement itself.
	claudeAssaySchemaEnforcement = "adapter-json-schema"
)

// claudeAssayDenyArgv is the enforced isolation policy.
//
// Each flag is load-bearing and each one was probe-verified on claude 2.1.220 by
// reading back what the session reported it had loaded:
//
//   - --tools "" disables the whole built-in tool set, which is what removes
//     repository reads, shell, and the network-facing WebFetch/WebSearch tools in
//     one place. The session init event then reports tools: [].
//   - --strict-mcp-config with no --mcp-config leaves no MCP server configured,
//     so no external resource surface survives. Init reports mcp_servers: [].
//   - --setting-sources "" loads no user, project or local settings, so nothing
//     checked into a repository or present in the operator's home can add a tool
//     back. Without it the deny set would depend on ambient configuration.
//   - --disable-slash-commands removes skills, which are instructions that ask
//     for tool use. Init reports slash_commands: [].
//
// The absence of any repository is enforced structurally instead: the assay runs
// in a freshly created empty directory, so there is nothing to read even if a
// future version regained a tool.
var claudeAssayDenyArgv = []string{
	"--tools", "",
	"--strict-mcp-config",
	"--setting-sources", "",
	"--disable-slash-commands",
}

// claudeAssayStreamRecord is the narrow projection of one stream-json line the
// isolation check reads.
//
// The three inventory fields are pointers on purpose. An explicitly reported
// empty array and an omitted or null field have the same length, so modelling
// them as plain slices made "the provider reported it loaded nothing" and "the
// provider stopped reporting what it loaded" the same observation — and only the
// first is evidence. A pointer separates them: nil is silence, non-nil is a
// report, and silence is refused rather than counted as zero.
type claudeAssayStreamRecord struct {
	Type          string                    `json:"type"`
	Subtype       string                    `json:"subtype"`
	SessionID     string                    `json:"session_id"`
	Tools         *[]string                 `json:"tools"`
	MCPServers    *[]any                    `json:"mcp_servers"`
	SlashCommands *[]string                 `json:"slash_commands"`
	IsError       bool                      `json:"is_error"`
	Result        string                    `json:"result"`
	Message       *claudeAssayStreamMessage `json:"message"`
	Usage         claudeAssayUsage          `json:"usage"`
	// PermissionDenials is the provider's own record of tool calls it refused.
	// Under this deny policy it is expected to be empty, and a non-empty one is
	// counted as tool evidence: the turn reached for a tool, and an assay whose
	// answer may have been shaped by that reach is not a test of what the base
	// believes. It is read as corroboration rather than as a required field, so
	// a provider that stops reporting it cannot turn a real denial into a pass —
	// the message-block classification below still observes the attempt.
	PermissionDenials []json.RawMessage `json:"permission_denials"`
}

// claudeAssayStreamShape is what one top-level event is allowed to be.
//
// The assay's whole claim is "no tool ran", and that claim is only as good as
// this adapter's ability to read the stream it is made over. An event shape the
// adapter does not model is not evidence that nothing happened in it, so the
// shapes are enumerated and everything else is refused.
type claudeAssayStreamShape int

const (
	// claudeAssayShapeUnknown is any event this adapter cannot account for. It is
	// always a refusal.
	claudeAssayShapeUnknown claudeAssayStreamShape = iota
	// claudeAssayShapeInit is the session inventory event.
	claudeAssayShapeInit
	// claudeAssayShapeResult is the terminal event carrying the answer.
	claudeAssayShapeResult
	// claudeAssayShapeTurn is an assistant or user event. Its safety does not come
	// from its type but from its content: every block is classified, and an
	// unclassifiable one fails closed. That is why a turn event is accepted
	// regardless of subtype while a system event is not.
	claudeAssayShapeTurn
	// claudeAssayShapeInert is an event that carries neither inventory nor turn
	// content and cannot express tool activity.
	claudeAssayShapeInert
)

// claudeAssayResultSubtypes is the set of terminal outcomes this adapter models.
//
// An unmodelled result subtype is refused rather than read through is_error,
// because a new terminal state that this adapter scores as a clean success is
// precisely the drift the assay must not absorb silently.
var claudeAssayResultSubtypes = map[string]struct{}{
	"success":                             {},
	"error_during_execution":              {},
	"error_max_turns":                     {},
	"error_max_budget_usd":                {},
	"error_max_structured_output_retries": {},
}

// claudeAssayInertSystemSubtypes is the set of system events that are accepted
// without being read as activity.
//
// Membership requires two things, and both were established against the running
// provider rather than inferred from the name: the event is emitted during a
// genuine assay run, and its payload is structurally incapable of describing tool
// activity. thinking_tokens qualifies on both counts — claude 2.1.220 emits
// several per run under this deny policy, and each carries only estimated_tokens,
// estimated_tokens_delta, uuid and session_id.
//
// The list is deliberately short. Every other system subtype is refused, which
// costs an occasional "isolation unavailable" on an otherwise clean run — a
// transient api_retry, say. That is the safe direction: a refusal withholds an
// answer the caller can ask for again, while accepting an unmodelled event
// silently converts "not observed" into "nothing happened". Widening this set is
// a deliberate act that needs the same two pieces of evidence.
var claudeAssayInertSystemSubtypes = map[string]struct{}{
	"thinking_tokens": {},
}

// classifyClaudeAssayStreamRecord decides what an event is, or refuses it.
//
// The accepted set was established against the supported provider rather than
// assumed: claude 2.1.220 under this deny policy emits system/init,
// system/thinking_tokens, rate_limit_event, assistant and result, and a
// tool-using run adds user events carrying tool_result blocks. Tool activity is
// always expressed inside message.content, never as a top-level event, which is
// why the turn shapes are the only ones allowed to carry it.
//
// system is treated more strictly than the rest on purpose. The provider defines
// dozens of system subtypes — hook_response, task_started, local_command,
// memory_recall and more — that carry activity with no message content at all,
// so a system event outside the init and inert sets is genuinely unobservable
// here and is refused instead of ignored.
func classifyClaudeAssayStreamRecord(record claudeAssayStreamRecord) claudeAssayStreamShape {
	switch record.Type {
	case "system":
		if record.Subtype == "init" {
			return claudeAssayShapeInit
		}
		if _, inert := claudeAssayInertSystemSubtypes[record.Subtype]; inert {
			return claudeAssayShapeInert
		}
		return claudeAssayShapeUnknown
	case "result":
		if _, known := claudeAssayResultSubtypes[record.Subtype]; !known {
			return claudeAssayShapeUnknown
		}
		return claudeAssayShapeResult
	case "assistant", "user":
		return claudeAssayShapeTurn
	case "rate_limit_event":
		return claudeAssayShapeInert
	default:
		return claudeAssayShapeUnknown
	}
}

// claudeAssayStreamMessage keeps content as raw bytes because the provider uses
// more than one shape for it: an array of typed blocks, and a bare string for a
// text-only message. Decoding straight into a block slice would fail on the
// second, and a decode failure on an ordinary message would be misread as
// protocol drift.
type claudeAssayStreamMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// claudeAssayContentBlock is one content block's kind. The name is read only so
// a refusal can say what ran.
type claudeAssayContentBlock struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

// claudeAssayInertContentBlockKinds is the set of content block kinds that are
// accepted as carrying no tool activity.
//
// This is the security-critical list, and membership requires the same two pieces
// of evidence the inert system-subtype set requires: the kind is emitted by a
// genuine assay run, and its payload is structurally incapable of describing tool
// activity. Both were established against claude 2.1.220 under this exact deny
// policy rather than inferred from documentation:
//
//   - text, fields {type, text}: emitted by every run that answers.
//   - thinking, fields {type, thinking, signature}: emitted when the turn reasons
//     before answering, alongside the system/thinking_tokens events.
//
// The list is deliberately short, and it is an allowlist rather than a denylist
// because the two directions fail differently. An unlisted inert kind costs a
// refusal the caller can retry. An unlisted *tool* kind under a denylist would be
// scored as a zero-tool run, which converts "not observed" into "nothing
// happened" — the one error this assay cannot make. Widening this set is a
// deliberate act that needs the same two pieces of evidence.
var claudeAssayInertContentBlockKinds = map[string]struct{}{
	"text":     {},
	"thinking": {},
}

// claudeAssayToolContentBlockKinds is the set of block kinds counted as tool
// evidence.
//
// Unlike the inert set, this one does not have to be exhaustive to be safe: an
// unlisted kind falls through to the unreadable default and is refused as
// unobservable. Listing a kind here only buys precision — the caller learns a
// tool ran rather than that the stream could not be read.
//
// tool_use and tool_result were both observed on claude 2.1.220 in a tool-using
// run: the assistant event carries tool_use {type, id, name, input, caller} and
// the following user event carries tool_result {type, tool_use_id, content}. A
// tool_result is counted because it is proof a tool already executed, which an
// earlier revision let through as benign. server_tool_use and mcp_tool_use are
// the provider-side and MCP forms of the same act; counting them is what keeps a
// network-facing web search from passing the zero-tool claim.
var claudeAssayToolContentBlockKinds = map[string]struct{}{
	"tool_use":        {},
	"tool_result":     {},
	"server_tool_use": {},
	"mcp_tool_use":    {},
}

type claudeAssayUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	ServerToolUse            struct {
		WebSearchRequests int `json:"web_search_requests"`
		WebFetchRequests  int `json:"web_fetch_requests"`
	} `json:"server_tool_use"`
}

// claudeAssayObservation is what the provider itself reported about the run.
type claudeAssayObservation struct {
	// InitRecords and ResultRecords count the session-inventory and result events.
	// They are counted rather than folded because more than one of either has to be
	// refused instead of reconciled: an adapter that keeps the first or the last is
	// choosing which report to believe, and under "keep the last" a non-empty
	// inventory followed by an empty one would pass.
	InitRecords   int
	ResultRecords int
	SawInit       bool
	SawResult     bool
	// InventoryReported is true only when the accepted init event carried all three
	// inventory fields explicitly. MissingInventoryFields names the ones it did
	// not, so a refusal can say which part of the contract went unobserved.
	InventoryReported      bool
	MissingInventoryFields []string
	Tools                  int
	MCPServers             int
	SlashCommands          int
	// InitSessionUUID and ResultSessionUUID are kept apart so the inventory can be
	// bound to the session that actually answered. Collapsing them let an empty
	// inventory from one session be attested against another session's result.
	InitSessionUUID   string
	ResultSessionUUID string
	// SessionUUID is set only when both events reported the same non-empty
	// identity. Anything else leaves it empty and is refused.
	SessionUUID   string
	ToolEvents    int
	ServerToolUse int
	// PermissionDenials counts tool calls the provider itself refused. A denial
	// means the turn reached for a tool even though none executed, so it is
	// counted as tool evidence rather than as proof the policy held.
	PermissionDenials int
	// UnreadableBlocks counts message content whose kind this adapter cannot
	// classify. It is not zero tool events; it is no observation of them.
	// UnreadableBlockKinds names the distinct kinds behind that count, in first-seen
	// order, so a refusal can say which shape went unobserved instead of only how
	// many did.
	UnreadableBlocks     int
	UnreadableBlockKinds []string
	IsError              bool
	Result               string
	Usage                claudeAssayUsage
}

// RunContextAssay forks a completed Claude base into a disposable,
// repository-free, tool-free session and returns its answer with an attestation.
//
// The isolation is enforced by the launch and then read back from the session's
// own init event, which is the difference between an attestation and a promise:
// this adapter does not claim the deny flags worked, it reports the tool, MCP and
// slash-command inventory the provider says it loaded and refuses when that
// report is missing or non-empty. A tool event, an absent report, or a
// non-empty inventory all fail before any answer is returned, because an answer
// that could have consulted the repository does not test what the base believes.
func (f *ClaudeHostFactory) RunContextAssay(
	ctx context.Context,
	source ProviderHostRecord,
	request ClaudeContextAssayRequest,
) (result *ClaudeContextAssayResult, err error) {
	executable, err := exec.LookPath("claude")
	if err != nil {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: "the Claude binary is not available on this machine, so the isolated correctness " +
				"assay cannot be enforced or observed here",
			Cause: err,
		}
	}
	// The caller's schema is compiled before anything else runs. A schema this
	// adapter cannot enforce in full is a capability failure, and reporting it
	// after a provider turn has been spent would be both slower and dishonest:
	// nothing about the run would have been validated.
	schema, schemaDigest, err := claudeAssayAnswerSchema(request.OutputSchema)
	if err != nil {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the assay output schema supplied for %s cannot be enforced by this adapter, and the provider "+
					"cannot enforce it either because its schema-constrained output is a tool this assay denies: "+
					"%v",
				request.BaseCtxID, err,
			),
			Cause: err,
		}
	}
	baseUUID, baseDir, err := claudeBaseTranscriptRef(source)
	if err != nil {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"context assay base %s has no resolvable Claude session identity", request.BaseCtxID,
			),
			Cause: err,
		}
	}
	home, err := claudeProviderHome()
	if err != nil {
		return nil, err
	}
	basePath, err := claudeSessionTranscriptPath(home, baseDir, baseUUID)
	if err != nil {
		return nil, err
	}
	// The assay forks the base, so it serves it and verifies presence on the same
	// terms as any other consumer. Without this, a base the provider's own 30-day
	// timer collected would be reported as an unreadable transcript — a fault the
	// operator would go looking for rather than the expected end of its life.
	if err := requireClaudePayloadPresence(request.BaseCtxID, basePath, request.BaseReadyAt, ContextResolutionMode("explicit"), time.Now()); err != nil {
		return nil, err
	}
	baseRaw, facts, err := inspectClaudeTranscriptFile(basePath)
	if err != nil {
		return nil, &PublicError{
			Code: claudeContextBaseNotForkable,
			Message: fmt.Sprintf(
				"context assay base %s has no readable transcript: %v", request.BaseCtxID, err,
			),
		}
	}
	if err := verifyClaudeSlugRule(basePath, facts.RecordedCWD); err != nil {
		return nil, err
	}
	// The assay forks a *completed* base for the same reason ordinary runs do: an
	// interrupted or compacted transcript would be quizzed about knowledge it no
	// longer holds, and the resulting score would describe the wrong artifact.
	if err := requireForkableClaudeBase(request.BaseCtxID, facts); err != nil {
		return nil, err
	}

	artifactSetID, err := newClaudeSessionUUID()
	if err != nil {
		return nil, err
	}
	assayDir, err := os.MkdirTemp(
		filepath.Join(f.layout.StateDir, claudeAssayDirName), "assay-")
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("creating Claude assay working directory: %w", err)
		}
		if mkErr := os.MkdirAll(
			filepath.Join(f.layout.StateDir, claudeAssayDirName), 0o700); mkErr != nil {
			return nil, fmt.Errorf("creating Claude assay root: %w", mkErr)
		}
		assayDir, err = os.MkdirTemp(
			filepath.Join(f.layout.StateDir, claudeAssayDirName), "assay-")
		if err != nil {
			return nil, fmt.Errorf("creating Claude assay working directory: %w", err)
		}
	}
	// Resolved form only: the provider derives its project directory from the cwd
	// it resolved, so an unresolved path would materialize where it never looks.
	assayDir, err = claudeResolveProjectDir(assayDir)
	if err != nil {
		return nil, err
	}
	// Provider artifacts are retained, never deleted by this adapter. The private
	// append-only event makes them ineligible assay output and hands exact paths to
	// the reclamation catalog for lease-aware reclamation. A first event is persisted
	// before provider work so a crash still leaves a recoverable ownership record.
	artifacts := &claudeAssayArtifacts{
		Layout:        f.layout,
		ArtifactSetID: artifactSetID,
		BaseCtxID:     request.BaseCtxID,
		WorkingDir:    assayDir,
	}
	defer func() {
		reason := "assay_completed"
		if err != nil {
			reason = "assay_failed"
		}
		recordErr := artifacts.record(claudeReclamationPendingGC, reason, time.Now)
		if recordErr == nil {
			return
		}
		reclamationErr := &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s could not persist the private reclamation handoff for "+
					"its retained artifacts, so no attestation can be returned: %v",
				request.BaseCtxID, recordErr,
			),
			Cause: recordErr,
		}
		result = nil
		if err == nil {
			err = reclamationErr
		} else {
			err = errors.Join(err, reclamationErr)
		}
	}()
	if recordErr := artifacts.record(
		claudeReclamationActive, "assay_working_directory_created", time.Now,
	); recordErr != nil {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s could not record its private working-directory "+
					"ownership before provider work: %v",
				request.BaseCtxID, recordErr,
			),
			Cause: recordErr,
		}
	}
	// Repository-free by construction: an empty directory that is not a checkout
	// and has no ancestor content reachable through a relative path.
	repositoryFree, err := claudeDirectoryIsEmpty(assayDir)
	if err != nil {
		return nil, err
	}
	if !repositoryFree {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: "the Claude assay working directory is not empty, so the repository-free " +
				"guarantee cannot be established",
		}
	}

	assayUUID, err := newClaudeSessionUUID()
	if err != nil {
		return nil, err
	}
	// The disposable fork is named for the same reason an ordinary run's is: the
	// attestation has to bind a session identity the manager controls, and a
	// reported identity that differs from the requested one means the provider did
	// something other than what was asked.
	assayForkUUID, err := newClaudeSessionUUID()
	if err != nil {
		return nil, err
	}
	assaySlug, err := ClaudeProjectSlug(assayDir)
	if err != nil {
		return nil, err
	}
	assayProjectDir := filepath.Join(claudeProjectsRoot(home), assaySlug)
	assayPath := filepath.Join(assayProjectDir, assayUUID+claudeTranscriptSuffix)
	// Where the provider must write the disposable child. Deriving it here rather
	// than inside the lineage check lets the private reclamation event name the
	// expected artifact even when the run fails before that check is reached.
	assayForkPath := filepath.Join(assayProjectDir, assayForkUUID+claudeTranscriptSuffix)
	materialized, err := rewriteClaudeTranscriptSession(
		baseRaw, assayUUID, assayDir, facts, nil, nil, time.Now(),
	)
	if err != nil {
		return nil, err
	}
	artifacts.SlugDir = assayProjectDir
	owned, slugRoot, err := claudeAssayClaimProjectDir(assayProjectDir)
	if err != nil {
		return nil, err
	}
	artifacts.SlugDirOwned = owned
	artifacts.SlugDirRoot = slugRoot
	artifacts.MaterializedPath = assayPath
	artifacts.ForkPath = assayForkPath
	artifacts.MaterializedSessionUUID = assayUUID
	artifacts.ForkSessionUUID = assayForkUUID
	if recordErr := artifacts.record(
		claudeReclamationActive, "assay_provider_paths_reserved", time.Now,
	); recordErr != nil {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s could not record its private provider artifact "+
					"ownership before materialization: %v",
				request.BaseCtxID, recordErr,
			),
			Cause: recordErr,
		}
	}
	stagedPath, writeErr := writeClaudeTranscript(assayPath, materialized)
	artifacts.RetainedStagingPath = stagedPath
	if writeErr != nil {
		return nil, writeErr
	}

	argv := append(claudeForkArgv(assayUUID, assayForkUUID), claudeAssayDenyArgv...)
	argv = append(argv, "--output-format", "stream-json", "--verbose", "--print")
	if model := strings.TrimSpace(request.Model); model != "" {
		argv = append(argv, "--model", model)
	}
	// --json-schema is deliberately not passed, and the reason is a provider
	// constraint rather than a preference: on claude 2.1.220 schema-constrained
	// output is implemented as a tool. Probing it against this deny policy showed
	// the init event reporting tools: ["StructuredOutput"] and the turn emitting a
	// tool_use block per attempt, so the flag would break the zero-tool contract
	// this assay exists to guarantee. It also cannot work: with the tool set denied
	// the tool never executes and the run ends as
	// error_max_structured_output_retries after five attempts.
	//
	// The requested schema is therefore enforced by this adapter on the returned
	// answer, in full, and the policy digest below covers it so an attestation
	// cannot be read as vouching for a different schema. Contract parity with the
	// Codex adapter is preserved where it matters: both return raw validated JSON
	// and both refuse to score anything else.

	runCtx, cancel := context.WithTimeout(ctx, claudeAssayTimeout)
	defer cancel()
	command := exec.CommandContext(runCtx, executable, argv...)
	command.Dir = assayDir
	command.Env = claudeHostEnvironment("dumb", os.Environ())
	command.Stdin = strings.NewReader(request.Prompt)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	runErr := command.Run()

	observation, parseErr := parseClaudeAssayStream(stdout.Bytes())
	if runErr != nil && !observation.SawResult {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s did not complete, so neither its answer nor its "+
					"tool-event stream can be trusted: %s",
				request.BaseCtxID, claudeAssayFailureDetail(runErr, stderr.String()),
			),
			Cause: runErr,
		}
	}
	if parseErr != nil {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s produced an event stream this adapter cannot read, so a "+
					"zero-tool claim over it would be unobserved: %v",
				request.BaseCtxID, parseErr,
			),
			Cause: parseErr,
		}
	}
	// A failed resume still emits a result record — an error one, with no session
	// inventory before it. Reporting that as "isolation unavailable" would name the
	// wrong problem: the session never started, so nothing was enforced or
	// bypassed. Say which it is.
	if observation.IsError && !observation.SawInit {
		return nil, &PublicError{
			Code: ClaudeErrorResumeFailed,
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s never started its session: %s",
				request.BaseCtxID, claudeAssayFailureDetail(runErr, stderr.String()),
			),
		}
	}
	if err := requireClaudeAssayIsolation(request.BaseCtxID, observation); err != nil {
		return nil, err
	}
	// The identity the provider reported has to be the identity that was named.
	// Without this the attestation would bind whatever session happened to answer.
	if observation.SessionUUID != assayForkUUID {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s reported session %s where fork %s was requested, so the "+
					"attestation cannot bind the provider session identity that produced the answer",
				request.BaseCtxID, observation.SessionUUID, assayForkUUID,
			),
		}
	}
	// Identity is not inheritance, so the disposable child's own transcript is read
	// before anything is scored. This is the check that makes a silent cold start
	// impossible to attest.
	inheritedRecords, err := requireClaudeAssayInheritance(
		request.BaseCtxID, assayForkPath, assayForkUUID, materialized,
	)
	if err != nil {
		return nil, err
	}
	// Both provider-side sources are re-read after the provider window, not before
	// it. The pre-launch digests prove the copy was written correctly; only a
	// post-launch read can prove the process that just ran did not append into the
	// shared base or into the copy the attestation names.
	if err := verifyClaudeAssaySourcesUnchanged(
		request.BaseCtxID, basePath, facts.SHA256, assayPath, claudeBytesDigest(materialized),
	); err != nil {
		return nil, err
	}
	output, err := claudeAssayStrictJSON(request.BaseCtxID, observation.Result)
	if err != nil {
		return nil, err
	}
	if err := requireClaudeAssaySchemaConformance(request.BaseCtxID, schema, output); err != nil {
		return nil, err
	}

	return &ClaudeContextAssayResult{
		Output: output,
		Attestation: ClaudeContextAssayAttestation{
			Contract:           claudeContextAssayContract,
			PolicyDigest:       claudeAssayPolicyDigest(argv, schemaDigest),
			ProviderVersion:    facts.FormatVersion,
			BaseSessionUUID:    baseUUID,
			AssaySessionUUID:   observation.SessionUUID,
			MaterializedSHA256: claudeBytesDigest(materialized),
			InheritedRecords:   inheritedRecords,
			RepositoryFree:     true,
			SchemaEnforcement:  claudeAssaySchemaEnforcement,
			SchemaDigest:       schemaDigest,
			EnforcedTools:      observation.Tools,
			EnforcedMCPServers: observation.MCPServers,
			EnforcedSlashCmds:  observation.SlashCommands,
			ToolEventCount:     observation.ToolEvents + observation.ServerToolUse,
			CompletedAt:        time.Now().UTC(),
		},
		Usage: ProviderUsage{
			InputTokens:         observation.Usage.InputTokens,
			CachedInputTokens:   observation.Usage.CacheReadInputTokens,
			CacheCreationTokens: observation.Usage.CacheCreationInputTokens,
			OutputTokens:        observation.Usage.OutputTokens,
		},
	}, nil
}

// requireClaudeAssayIsolation turns the provider's own report into a verdict.
//
// The two failures are deliberately distinct. A tool event means the answer was
// informed by something outside the inherited transcript, so it tests the
// repository rather than the base. A missing or non-empty inventory means the
// adapter cannot prove either way, which is the same refusal for a different
// reason: an unobservable isolation is not a weaker isolation, it is no evidence
// at all. Both fail before scoring.
func requireClaudeAssayIsolation(ctxID string, observation claudeAssayObservation) error {
	if !observation.SawInit {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s reported no session inventory, so the manager cannot "+
					"observe that repository, network, resource and tool access were actually denied",
				ctxID,
			),
		}
	}
	// Repeated evidence is refused rather than reconciled. Whichever report an
	// adapter kept would be a choice about which one to believe, and the run only
	// has one launch, one enforced policy and one answer to attest — so a stream
	// describing two of any of them is not a stream this adapter can read as
	// evidence about one session.
	if observation.InitRecords > 1 || observation.ResultRecords > 1 {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s reported %d session inventory event(s) and %d result "+
					"event(s) where exactly one of each was expected; this adapter will not pick which "+
					"report to attest, so the isolation contract cannot be observed here",
				ctxID, observation.InitRecords, observation.ResultRecords,
			),
		}
	}
	// An inventory field the provider did not report is silence, not a zero. Without
	// this the deny policy would be "verified" by a protocol that stopped answering
	// the question.
	if !observation.InventoryReported {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s reported a session inventory that does not state %s; an "+
					"absent field is not an empty one, so this Claude version cannot be observed to have "+
					"honoured the deny policy and no zero-tool claim can be made over it",
				ctxID, strings.Join(observation.MissingInventoryFields, ", "),
			),
		}
	}
	// A content shape this adapter cannot classify is refused for the same reason:
	// a block whose kind is unstated or unmodelled may be a tool use, and "we did
	// not see one" is not the same statement as "there was none". The kinds are
	// named because this refusal is also the signal that the provider's block
	// protocol moved, and widening the inert allowlist needs to know what to bring
	// evidence for.
	if observation.UnreadableBlocks > 0 {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s emitted %d message content block(s) whose kind this adapter "+
					"cannot classify (%s), so it cannot observe whether a tool ran and will not claim none did",
				ctxID, observation.UnreadableBlocks,
				strings.Join(observation.UnreadableBlockKinds, ", "),
			),
		}
	}
	if observation.ToolEvents > 0 || observation.ServerToolUse > 0 ||
		observation.PermissionDenials > 0 {
		return &ClaudeContextFailure{
			Code: "context_assay_tool_use_detected",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s emitted %d tool event(s), %d server tool request(s) and %d "+
					"denied tool call(s); the answer was informed by, or reached for, evidence the base did "+
					"not carry, so it is rejected before scoring",
				ctxID, observation.ToolEvents, observation.ServerToolUse, observation.PermissionDenials,
			),
		}
	}
	if observation.Tools > 0 || observation.MCPServers > 0 || observation.SlashCommands > 0 {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s ran with %d tool(s), %d MCP server(s) and %d slash "+
					"command(s) still loaded; this Claude version did not honour the deny policy, so the "+
					"isolation contract cannot be enforced here",
				ctxID, observation.Tools, observation.MCPServers, observation.SlashCommands,
			),
		}
	}
	if !observation.SawResult || observation.IsError {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s produced no successful result record", ctxID,
			),
		}
	}
	// The inventory and the answer have to belong to the same session. The init
	// event is where the deny policy is read back and the result event is where the
	// answer arrives, so two identities here mean the attested zero-tool inventory
	// describes a session other than the one that answered — which is exactly the
	// evidence swap the attestation's single session identity is meant to rule out.
	if observation.SessionUUID == "" ||
		observation.InitSessionUUID != observation.ResultSessionUUID {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s reported its session inventory under identity %q and its "+
					"answer under identity %q; the attestation can only bind one provider session, so an "+
					"inventory that does not describe the session that answered is refused",
				ctxID, observation.InitSessionUUID, observation.ResultSessionUUID,
			),
		}
	}
	return nil
}

// parseClaudeAssayStream reads the provider's own event stream into the evidence
// the verdict is taken from.
//
// It reads rather than decides: every count, presence flag and identity it
// produces is a fact about the stream, and requireClaudeAssayIsolation turns
// those into an accept or a refusal. Two things are deliberately not folded
// together here — repeated init/result events are counted instead of overwriting
// each other, and the init and result identities are kept apart — because both
// collapses were ways for an empty inventory to be attested against a session it
// did not describe.
func parseClaudeAssayStream(raw []byte) (claudeAssayObservation, error) {
	observation := claudeAssayObservation{}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), claudeOwnerOutputLimit)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record claudeAssayStreamRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return claudeAssayObservation{}, fmt.Errorf("decode Claude assay event: %w", err)
		}
		shape := classifyClaudeAssayStreamRecord(record)
		if shape == claudeAssayShapeUnknown {
			return claudeAssayObservation{}, fmt.Errorf(
				"unmodelled Claude assay event type %q subtype %q: this adapter cannot observe what "+
					"happened in it, so it is not evidence that no tool ran",
				record.Type, record.Subtype,
			)
		}
		// Message content is classified wherever it appears, not only on turn events.
		// A shape that gained content it did not previously carry is still read rather
		// than skipped, so tool blocks cannot hide behind an unexpected carrier.
		if record.Message != nil {
			tools, unreadable, kinds, err := countClaudeAssayToolBlocks(record.Message.Content)
			if err != nil {
				return claudeAssayObservation{}, err
			}
			observation.ToolEvents += tools
			observation.UnreadableBlocks += unreadable
			observation.UnreadableBlockKinds = appendDistinctClaudeAssayKinds(
				observation.UnreadableBlockKinds, kinds)
		} else if shape == claudeAssayShapeTurn {
			// A turn is accepted regardless of subtype only because its content is
			// fully classified. A turn without content removes that guarantee, so
			// there is nothing left to accept it on.
			return claudeAssayObservation{}, fmt.Errorf(
				"Claude assay %s event carried no message content, so its turn could not be "+
					"classified", record.Type,
			)
		}
		switch shape {
		case claudeAssayShapeInit:
			observation.InitRecords++
			observation.SawInit = true
			// Only the first report is read. A second one is refused by the verdict
			// rather than merged, so there is no reading under which a later empty
			// inventory can replace an earlier non-empty one.
			if observation.InitRecords == 1 {
				observation.InitSessionUUID = strings.TrimSpace(record.SessionID)
				readClaudeAssayInventory(&observation, record)
			}
		case claudeAssayShapeResult:
			observation.ResultRecords++
			observation.SawResult = true
			if observation.ResultRecords == 1 {
				observation.ResultSessionUUID = strings.TrimSpace(record.SessionID)
				observation.IsError = record.IsError
				observation.Result = record.Result
				observation.Usage = record.Usage
				observation.ServerToolUse =
					record.Usage.ServerToolUse.WebSearchRequests +
						record.Usage.ServerToolUse.WebFetchRequests
			}
			observation.PermissionDenials += len(record.PermissionDenials)
		}
	}
	if err := scanner.Err(); err != nil {
		return claudeAssayObservation{}, fmt.Errorf("reading Claude assay event stream: %w", err)
	}
	// One identity, agreed by both events, or none at all. The inventory comes from
	// the init event and the answer comes from the result event, so an attestation
	// binding a single provider session identity is only honest when those two
	// events are the same session.
	if observation.InitSessionUUID != "" &&
		observation.InitSessionUUID == observation.ResultSessionUUID {
		observation.SessionUUID = observation.InitSessionUUID
	}
	if observation.SawResult && observation.ResultSessionUUID == "" {
		return claudeAssayObservation{}, errors.New(
			"Claude assay stream reported no provider session identity")
	}
	return observation, nil
}

// readClaudeAssayInventory records what the init event said it loaded, and which
// parts of that it did not say at all.
func readClaudeAssayInventory(
	observation *claudeAssayObservation, record claudeAssayStreamRecord,
) {
	if record.Tools != nil {
		observation.Tools = len(*record.Tools)
	} else {
		observation.MissingInventoryFields = append(observation.MissingInventoryFields, "tools")
	}
	if record.MCPServers != nil {
		observation.MCPServers = len(*record.MCPServers)
	} else {
		observation.MissingInventoryFields = append(
			observation.MissingInventoryFields, "mcp_servers")
	}
	if record.SlashCommands != nil {
		observation.SlashCommands = len(*record.SlashCommands)
	} else {
		observation.MissingInventoryFields = append(
			observation.MissingInventoryFields, "slash_commands")
	}
	observation.InventoryReported = len(observation.MissingInventoryFields) == 0
}

// countClaudeAssayToolBlocks classifies one message's content exhaustively.
//
// Every block lands in exactly one of three buckets and there is no fall-through:
// a known tool kind is counted as a tool event, a known inert kind is accepted,
// and everything else — an unstated kind, an undecodable block, or a kind this
// adapter does not model — is counted as unreadable. The last bucket is the one
// that matters. An earlier revision counted the tool kinds and treated every
// other non-empty kind as benign, so a block shaped like {"type":"future_tool_use",
// "name":"Read"} produced a complete observation with ToolEvents:0 and no
// isolation error. That is a provider protocol extension hiding tool activity
// inside a known turn event and still collecting the attested zero-tool count.
//
// The unreadable kinds are returned so the refusal can name them: an operator who
// hits a genuinely inert new kind needs to know which one to bring evidence for.
//
// Two content shapes are accepted because the provider emits both: an array of
// typed blocks, and a bare string for a text-only message. Any other shape is
// unreadable for the same reason — content this adapter cannot classify is not
// evidence that no tool ran, and the verdict refuses on it.
//
// Absent and JSON-null content are unreadable rather than empty. An earlier
// revision returned zero tools and zero unreadable blocks for them, which let a
// message carrying no observed content at all support the negative claim the
// attestation is made of: a stream whose assistant turn said
// "message":{"content":null} was accepted with ToolEvents:0 and
// UnreadableBlocks:0. The evidence model here recognizes exactly two observed
// shapes, and neither of them is "the content key was not there". Silence about
// what a message contained cannot be read as a statement that it contained no
// tool use, so it goes in the same bucket as an unmodelled block kind.
func countClaudeAssayToolBlocks(
	content json.RawMessage,
) (tools int, unreadable int, kinds []string, err error) {
	trimmed := bytes.TrimSpace(content)
	if len(trimmed) == 0 {
		return 0, 1, []string{"<absent content>"}, nil
	}
	if bytes.Equal(trimmed, []byte("null")) {
		return 0, 1, []string{"<null content>"}, nil
	}
	switch trimmed[0] {
	case '"':
		// A text-only message. A string carries no blocks, so there is nothing
		// unobserved about it.
		return 0, 0, nil, nil
	case '[':
		var blocks []json.RawMessage
		if err := json.Unmarshal(trimmed, &blocks); err != nil {
			return 0, 0, nil, fmt.Errorf("decode Claude assay message content: %w", err)
		}
		for _, raw := range blocks {
			var block claudeAssayContentBlock
			if err := json.Unmarshal(raw, &block); err != nil {
				unreadable++
				kinds = append(kinds, "<undecodable block>")
				continue
			}
			kind := strings.TrimSpace(block.Type)
			if _, tool := claudeAssayToolContentBlockKinds[kind]; tool {
				tools++
				continue
			}
			if _, inert := claudeAssayInertContentBlockKinds[kind]; inert {
				continue
			}
			// Either the block did not state its kind, or it stated one this adapter
			// has no evidence about. Neither can be shown not to be a tool use.
			unreadable++
			if kind == "" {
				kinds = append(kinds, "<unstated kind>")
				continue
			}
			kinds = append(kinds, kind)
		}
		return tools, unreadable, kinds, nil
	default:
		return 0, 1, []string{"<unmodelled content shape>"}, nil
	}
}

// appendDistinctClaudeAssayKinds keeps the first-seen order and drops repeats, so
// a stream with forty blocks of one unmodelled kind reports that kind once rather
// than forty times. The count is carried separately and is not deduplicated.
func appendDistinctClaudeAssayKinds(existing, additions []string) []string {
	for _, addition := range additions {
		if slices.Contains(existing, addition) {
			continue
		}
		existing = append(existing, addition)
	}
	return existing
}

// claudeAssayPolicyDigest binds the attestation to the exact policy that ran.
//
// The digest covers the policy version, the full launch argument vector —
// including the deny flags and the resume/fork selector — and the digest of the
// answer schema this adapter enforced. The schema belongs in here because on
// Claude it is half the enforced policy: the provider constrained nothing, so an
// attestation that bound only the launch flags would vouch equally for a run that
// checked a different shape, or none.
func claudeAssayPolicyDigest(argv []string, schemaDigest string) string {
	parts := append([]string{claudeAssayPolicyVersion}, argv...)
	parts = append(parts, "schema:"+schemaDigest)
	document := strings.Join(parts, "\x00")
	sum := sha256.Sum256([]byte(document))
	return hex.EncodeToString(sum[:])
}

// claudeAssayAnswerSchema compiles the caller's schema, or reports why this
// adapter cannot stand behind it.
//
// A schema is mandatory, matching the Codex adapter, which also refuses an assay
// without one. It was previously optional here, which left a second, weaker
// enforcement level reachable — and "the answer was only checked for being a
// non-empty object" is precisely the guarantee that let malformed output through.
// An unenforceable schema is refused for the same reason: there is no provider-side
// fallback to delegate it to.
func claudeAssayAnswerSchema(raw json.RawMessage) (*claudeAssaySchema, string, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, "", fmt.Errorf(
			"an assay output schema is required: without one the answer could only be checked for being a " +
				"non-empty JSON object, and nothing about its fact results would be validated before scoring")
	}
	schema, err := compileClaudeAssaySchema(raw)
	if err != nil {
		return nil, "", err
	}
	digest, err := claudeAssaySchemaDigest(raw)
	if err != nil {
		return nil, "", err
	}
	return schema, digest, nil
}

// requireClaudeAssaySchemaConformance is the gate the previous revision was
// missing: the schema the caller supplied is checked against the answer, and a
// violation is malformed output rather than an abstention.
//
// CV-2 is explicit that a missing fact id, an extra fact id, an unknown outcome,
// or any other shape is malformed before scoring. All of those are expressible in
// the caller's schema, so enforcing the schema is what makes that sentence true
// instead of aspirational.
//
// Which is also why an absent compiled schema is an error rather than a pass: a
// nil schema here would mean the answer reached the scorer unchecked.
func requireClaudeAssaySchemaConformance(
	ctxID string,
	schema *claudeAssaySchema,
	output json.RawMessage,
) error {
	if schema == nil {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s has no compiled answer schema, so its answer cannot be "+
					"validated before scoring", ctxID,
			),
		}
	}
	value, err := claudeDecodeJSONValue(output)
	if err != nil {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s returned an answer this adapter cannot re-read for schema "+
					"enforcement: %v", ctxID, err,
			),
			Cause: err,
		}
	}
	// A numeric literal the adapter cannot compare exactly is refused before the
	// keywords run, so no assertion is ever decided by an approximation.
	if err := claudeRequireExactNumbers("$", value); err != nil {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s returned an answer carrying a number this adapter cannot "+
					"compare exactly against the validator's schema, so it will not claim the answer conforms: %v",
				ctxID, err,
			),
			Cause: err,
		}
	}
	if err := schema.check("$", value); err != nil {
		return &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s returned an answer that violates the schema the validator "+
					"supplied, so it is malformed output rather than a fact result: %v", ctxID, err,
			),
			Cause: err,
		}
	}
	return nil
}

// requireClaudeAssayInheritance proves the session that answered inherited the
// materialized copy, and returns how many record identities it carried through.
//
// Everything the assay checked before this point is compatible with a cold start.
// The provider's reported inventory says what it loaded, the reported session id
// says which session answered, and both fork sources being byte-identical is
// exactly what a successful --fork-session produces — but also exactly what a
// launch that ignored the resume selector and started fresh under the requested
// --session-id produces. Attesting on that evidence alone would let a provider
// that quietly stopped honouring the fork flags return a clean zero-tool
// attestation for a session holding none of the base's knowledge, which is the
// silent-cold fallback the ordinary fork path already refuses.
//
// The positive evidence is the disposable fork's own transcript, and it is read at
// the one place a genuine fork must have written it: the assay's own cwd slug
// directory, under the fork identity this adapter named. Probed on claude 2.1.220
// in --print mode, a forked run writes <requested-fork-uuid>.jsonl beside the
// materialized copy, stamps every record with the requested identity, and copies
// the inherited records through with their uuid values intact — verified against a
// base carrying a planted token the child then recalled with every tool denied.
//
// Three things are required together, because each one alone is weak:
//
//   - the child exists at the derived path, which is what stops a wrong slug from
//     passing silently: a provider that disagreed about the directory wrote where
//     this adapter is not looking;
//   - it carries exactly one session identity and it is the requested fork, so the
//     transcript being read is the one that produced the answer;
//   - it holds every one of the materialized copy's record identities in order,
//     which is descent rather than resemblance.
//
// Anything missing is reported as context_assay_isolation_unavailable rather than
// as a tool-use or immutability failure, because the honest statement is that this
// adapter could not observe the inheritance — not that the base misbehaved.
func requireClaudeAssayInheritance(
	ctxID, childPath, forkUUID string, materialized []byte,
) (int, error) {
	inherited, err := inspectClaudeTranscript(materialized)
	if err != nil {
		return 0, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the transcript the isolated Claude assay for %s materialized cannot be re-read for lineage "+
					"evidence, so the fork cannot be shown to have inherited it: %v", ctxID, err,
			),
			Cause: err,
		}
	}
	// An empty parent would make containment trivially true, so it is refused here
	// rather than inside the containment helper: a copy with no record identities
	// carries no evidence any child could inherit.
	if len(inherited.RecordUUIDs) == 0 {
		return 0, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the transcript the isolated Claude assay for %s materialized carries no record identities, so "+
					"there is no evidence a fork could inherit and no way to tell a real fork from a cold start",
				ctxID,
			),
		}
	}
	// The provider process has already exited, so a child that is absent now is a
	// child that was never written. There is nothing to wait for.
	_, child, err := inspectClaudeTranscriptFile(childPath)
	if err != nil {
		return 0, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s left no readable forked transcript at %s, so this adapter "+
					"cannot observe that the answering session inherited the materialized context rather than "+
					"starting cold: %v",
				ctxID, childPath, err,
			),
			Cause: err,
		}
	}
	if len(child.SessionUUIDs) != 1 || child.SessionUUIDs[0] != forkUUID {
		return 0, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the forked transcript the isolated Claude assay for %s produced carries session identities %v "+
					"where only %s was named, so it cannot be read as the lineage evidence for the answering "+
					"session",
				ctxID, child.SessionUUIDs, forkUUID,
			),
		}
	}
	if !claudeTranscriptInherits(child.RecordUUIDs, inherited.RecordUUIDs) {
		return 0, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the forked transcript the isolated Claude assay for %s produced holds %d record identity/ies "+
					"and does not carry the materialized copy's %d in order; the session answered without "+
					"inheriting the base, so its answer measures nothing about what the base believes and is "+
					"refused before scoring",
				ctxID, len(child.RecordUUIDs), len(inherited.RecordUUIDs),
			),
		}
	}
	return len(inherited.RecordUUIDs), nil
}

// verifyClaudeAssaySourcesUnchanged re-reads the base and the materialized copy
// after the provider process has exited.
//
// The ordinary fork path makes the same check for the same reason: a base is a
// shared fork source and "it was not touched" is only evidence if it is measured
// after the thing that could have touched it. The assay is disposable, but the
// base it forked is not, so a mutated base fails the assay instead of producing a
// score over a corrupted artifact.
func verifyClaudeAssaySourcesUnchanged(
	ctxID, basePath, baseDigest, materializedPath, materializedDigest string,
) error {
	for _, source := range []struct {
		role   string
		path   string
		digest string
	}{
		{role: "base transcript", path: basePath, digest: baseDigest},
		{role: "materialized copy", path: materializedPath, digest: materializedDigest},
	} {
		observed, err := claudeFileDigest(source.path)
		if err != nil {
			return &PublicError{
				Code: claudeContextSourceMutated,
				Message: fmt.Sprintf(
					"the %s the isolated Claude assay for %s forked could not be re-read after the provider "+
						"exited, so it cannot be shown to have survived the run: %v",
					source.role, ctxID, err,
				),
			}
		}
		if observed != source.digest {
			return &PublicError{
				Code: claudeContextSourceMutated,
				Message: fmt.Sprintf(
					"the %s the isolated Claude assay for %s forked changed while the provider ran; a fork "+
						"source that a turn appended to is no longer a fork source, so the assay is refused "+
						"before scoring",
					source.role, ctxID,
				),
			}
		}
	}
	return nil
}

func claudeAssayFailureDetail(err error, stderr string) string {
	detail := strings.TrimSpace(stderr)
	if detail == "" {
		if err == nil {
			return "no provider output"
		}
		return err.Error()
	}
	if len(detail) > 512 {
		detail = detail[:512]
	}
	return detail
}

// claudeDirectoryIsEmpty reports whether a directory holds nothing at all.
func claudeDirectoryIsEmpty(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, fmt.Errorf("inspecting Claude assay working directory: %w", err)
	}
	return len(entries) == 0, nil
}

// claudeAssayArtifacts is everything one assay created on disk. It is persisted
// to the manager-private reclamation catalog; this adapter never deletes it.
type claudeAssayArtifacts struct {
	Layout                  Layout
	ArtifactSetID           string
	BaseCtxID               string
	WorkingDir              string
	SlugDir                 string
	SlugDirOwned            bool
	MaterializedPath        string
	MaterializedSessionUUID string
	ForkPath                string
	ForkSessionUUID         string
	RetainedStagingPath     string
	// Legacy handles remain only so in-flight tests/older callers can close
	// descriptors they already opened. They are never used for removal.
	WorkingDirRoot *os.Root
	SlugDirRoot    *os.Root
}

func (a *claudeAssayArtifacts) close() {
	if a.SlugDirRoot != nil {
		_ = a.SlugDirRoot.Close()
		a.SlugDirRoot = nil
	}
	if a.WorkingDirRoot != nil {
		_ = a.WorkingDirRoot.Close()
		a.WorkingDirRoot = nil
	}
}

func (a *claudeAssayArtifacts) record(
	state claudeReclamationState,
	reason string,
	now func() time.Time,
) error {
	if a == nil {
		return fmt.Errorf("Claude assay artifact ownership is required")
	}
	artifacts := make([]claudeReclamationArtifact, 0, 3)
	if a.MaterializedPath != "" {
		artifacts = append(artifacts, claudeReclamationArtifact{
			Role:        "materialized_resume_source",
			Path:        a.MaterializedPath,
			SessionUUID: a.MaterializedSessionUUID,
		})
	}
	if a.ForkPath != "" {
		artifacts = append(artifacts, claudeReclamationArtifact{
			Role:        "disposable_assay_fork",
			Path:        a.ForkPath,
			SessionUUID: a.ForkSessionUUID,
		})
	}
	if a.RetainedStagingPath != "" {
		artifacts = append(artifacts, claudeReclamationArtifact{
			Role: "retained_staging_file",
			Path: a.RetainedStagingPath,
		})
	}
	return recordClaudeReclamationEvent(a.Layout, claudeReclamationEvent{
		ArtifactSetID: a.ArtifactSetID,
		Kind:          claudeReclamationAssay,
		State:         state,
		Reason:        reason,
		BaseCtxID:     a.BaseCtxID,
		WorkingDir:    a.WorkingDir,
		ProjectDir:    a.SlugDir,
		Artifacts:     artifacts,
	}, now)
}

// claudeAssayClaimProjectDir creates the provider project directory and records
// whether the assay created it. Ownership is metadata for lease-aware reclamation;
// this adapter never uses it as authority to delete the directory.
func claudeAssayClaimProjectDir(dir string) (bool, *os.Root, error) {
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return false, nil, fmt.Errorf("creating Claude projects root: %w", err)
	}
	err := os.Mkdir(dir, 0o700)
	switch {
	case err == nil:
		root, openErr := os.OpenRoot(dir)
		if openErr != nil {
			return true, nil, nil
		}
		return true, root, nil
	case errors.Is(err, os.ErrExist):
		return false, nil, nil
	default:
		return false, nil, fmt.Errorf("creating Claude project directory %s: %w", dir, err)
	}
}

// retireClaudeAssayArtifacts is retained as a compatibility seam. Retirement now
// appends a pending_gc event and closes handles; it never deletes filesystem data.
func retireClaudeAssayArtifacts(artifacts *claudeAssayArtifacts) error {
	defer artifacts.close()
	return artifacts.record(claudeReclamationPendingGC, "assay_retired", time.Now)
}

// The legacy helpers below remain only so older in-package callers fail safely
// while migrating to the reclamation catalog. They perform no filesystem
// mutation and always direct ownership to the reclamation catalog.
//
// The handle path is preferred because it makes the deletion immune to the
// pathname being rebound underneath it, and because Root refuses to resolve a
// name that escapes the directory. Without a handle — a slug directory that
// already existed, which this assay does not own — there is nothing to resolve
// against and the absolute path is used. That case is narrow by construction: the
// name is a freshly generated session UUID this run chose, not a name an occupant
// could be expected to hold.
func removeClaudeAssayTranscript(root *os.Root, dir, path string) error {
	_ = root
	_ = dir
	return fmt.Errorf("%s is retained for lease-aware reclamation", path)
}

func removeOwnedClaudeAssayDir(dir string, root *os.Root) error {
	_ = root
	return fmt.Errorf("%s is retained for lease-aware reclamation", dir)
}

func removeOwnedClaudeAssayTree(dir string, root *os.Root) error {
	_ = root
	return fmt.Errorf("%s is retained for lease-aware reclamation", dir)
}

func emptyAndUnlinkOwnedClaudeAssayDir(
	dir string, root *os.Root, entries []os.DirEntry,
) error {
	_ = root
	_ = entries
	return fmt.Errorf("%s is retained for lease-aware reclamation", dir)
}

func unlinkOwnedClaudeAssayDirEntry(dir string, owned os.FileInfo) error {
	_ = owned
	return fmt.Errorf("%s is retained for lease-aware reclamation", dir)
}

// requireClaudeAssayPathRetired is a read-only legacy assertion helper.
func requireClaudeAssayPathRetired(path string) error {
	_, err := os.Lstat(path)
	if err == nil {
		return fmt.Errorf("%s survived the assay", path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("confirming %s was retired: %w", path, err)
	}
	return nil
}

// claudeAssayStrictJSON returns the provider's answer as the raw JSON object the
// provider-neutral scorer reads.
//
// CV-2 keys its result by fact id, so the answer is a JSON document, not a string
// that happens to contain one. An earlier revision returned the result text
// JSON-encoded, which turned {"f1":{...}} into a quoted string and left the
// scorer unable to see any fact id at all. The Codex adapter returns the agent
// message's raw bytes and requires them to be valid JSON; this is the same
// contract, so a malformed or non-JSON answer is refused here rather than
// scored.
//
// One provider-shape allowance: Claude wraps structured answers in a markdown
// fence often enough that rejecting the fence would fail runs whose answer is
// perfectly good JSON. The fence is an envelope, so it is stripped before
// validity is required — and what is returned is still the raw JSON inside it,
// never a re-encoding.
func claudeAssayStrictJSON(ctxID, result string) (json.RawMessage, error) {
	trimmed := claudeStripJSONFence(result)
	if trimmed == "" {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s returned an empty answer, so there is nothing to score", ctxID,
			),
		}
	}
	if !json.Valid([]byte(trimmed)) {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s did not return the strict JSON its schema requires, so its "+
					"fact results cannot be validated before scoring", ctxID,
			),
		}
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &object); err != nil {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s returned JSON that is not an object keyed by fact id, so it "+
					"is not the result shape the provider-neutral scorer reads", ctxID,
			),
			Cause: err,
		}
	}
	if len(object) == 0 {
		return nil, &ClaudeContextFailure{
			Code: "context_assay_isolation_unavailable",
			Message: fmt.Sprintf(
				"the isolated Claude assay for %s returned no fact results", ctxID,
			),
		}
	}
	return json.RawMessage(trimmed), nil
}

// claudeStripJSONFence removes one complete surrounding markdown code fence.
//
// Both fences are required. An earlier revision trimmed the suffix
// unconditionally, so an answer that opened a fence and was then cut off had its
// opening fence removed and the truncated remainder was handed to the validity
// check as if it were a whole document. Half an envelope is not an envelope: when
// the pair is absent the original text is returned unchanged and the validity
// check rejects it.
func claudeStripJSONFence(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "```") {
		return trimmed
	}
	body := strings.TrimPrefix(trimmed, "```")
	newline := strings.IndexByte(body, '\n')
	if newline < 0 {
		return trimmed
	}
	// Whatever sits between the fence and the first newline is the language tag;
	// anything else there means this is not a plain fenced document.
	if tag := strings.TrimSpace(body[:newline]); tag != "" && !isClaudeFenceTag(tag) {
		return trimmed
	}
	body = body[newline+1:]
	closing := strings.LastIndex(body, "```")
	if closing < 0 {
		return trimmed
	}
	return strings.TrimSpace(body[:closing])
}

func isClaudeFenceTag(tag string) bool {
	for _, r := range tag {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '+':
		default:
			return false
		}
	}
	return true
}
