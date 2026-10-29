package sessionmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestClaudeAssayDenyPolicyDeniesEverySurfaceTheContractNames pins the enforced
// policy itself.
//
// The AC requires repository, network, external resources, injection and all
// tools denied. Three of those collapse into one provider flag — --tools ""
// removes the built-in set including the network-facing WebFetch/WebSearch — so
// the test asserts each named mechanism rather than trusting the shape of the
// slice.
func TestClaudeAssayDenyPolicyDeniesEverySurfaceTheContractNames(t *testing.T) {
	policy := strings.Join(claudeAssayDenyArgv, " ")
	for _, required := range []string{
		"--tools",             // every built-in tool, including repository reads and web access
		"--strict-mcp-config", // no MCP server, so no external resource surface
		"--setting-sources",   // no ambient settings can add a tool back
		"--disable-slash-commands",
	} {
		if !strings.Contains(policy, required) {
			t.Fatalf("the assay deny policy does not enforce %s: %s", required, policy)
		}
	}
	// --tools and --setting-sources must be given the empty value; a missing value
	// would leave the default set in place.
	for index, arg := range claudeAssayDenyArgv {
		if arg != "--tools" && arg != "--setting-sources" {
			continue
		}
		if index+1 >= len(claudeAssayDenyArgv) || claudeAssayDenyArgv[index+1] != "" {
			t.Fatalf("%s is not given the empty value in %v", arg, claudeAssayDenyArgv)
		}
	}
	// The assay is a fork like any other run: it never resumes a base in place.
	composed := append(claudeForkArgv("11111111-2222-3333-4444-555555555555"), claudeAssayDenyArgv...)
	if !containsString(composed, "--fork-session") {
		t.Fatalf("the assay launch does not fork its base: %v", composed)
	}
}

// TestClaudeAssayPolicyDigestBindsTheEnforcedPolicy proves the attestation
// cannot be reused to vouch for a run that enforced something else.
//
// The schema digest is part of the policy for a Claude-specific reason: the
// provider constrains nothing here, so the answer schema this adapter enforced is
// half of what "the policy" means. A digest that covered only the launch flags
// would vouch identically for a run that checked a weaker shape.
func TestClaudeAssayPolicyDigestBindsTheEnforcedPolicy(t *testing.T) {
	argv := append(claudeForkArgv("11111111-2222-3333-4444-555555555555"), claudeAssayDenyArgv...)
	const schemaDigest = "9f2c" // stand-in; only its participation is under test
	digest := claudeAssayPolicyDigest(argv, schemaDigest)
	if len(digest) != 64 {
		t.Fatalf("policy digest %q is not a sha256 hex digest", digest)
	}
	if claudeAssayPolicyDigest(argv, schemaDigest) != digest {
		t.Fatal("the policy digest is not stable for one policy")
	}
	weakened := append([]string(nil), argv...)
	for index, arg := range weakened {
		if arg == "--tools" {
			weakened[index+1] = "default"
		}
	}
	if claudeAssayPolicyDigest(weakened, schemaDigest) == digest {
		t.Fatal("a weakened deny policy produced the same digest")
	}
	if claudeAssayPolicyDigest(argv[:2], schemaDigest) == digest {
		t.Fatal("dropping the deny flags produced the same digest")
	}
	if claudeAssayPolicyDigest(argv, "a different schema") == digest {
		t.Fatal("a different enforced answer schema produced the same policy digest")
	}
	if claudeAssayPolicyDigest(argv, "") == digest {
		t.Fatal("enforcing no schema at all produced the same policy digest")
	}
}

// TestClaudeAssayAcceptsZeroToolEvidence is the accepting case, built from the
// event shape the real provider emitted during the probe run: an init event
// reporting an empty inventory, assistant text, and a successful result.
func TestClaudeAssayAcceptsZeroToolEvidence(t *testing.T) {
	const forkUUID = "cccccccc-3333-4333-8333-cccccccccccc"
	stream := claudeAssayStream(
		claudeAssayInitEventAs(forkUUID, nil, nil, nil),
		`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"{}"}]}}`,
		claudeAssayResultEventAs(forkUUID, `{"f1":{"outcome":"answer","value":40}}`, 0, 0),
	)
	observation, err := parseClaudeAssayStream(stream)
	if err != nil {
		t.Fatal(err)
	}
	if err := requireClaudeAssayIsolation("CTX-260730-a00001", observation); err != nil {
		t.Fatalf("a clean isolated assay was rejected: %v", err)
	}
	if observation.ToolEvents != 0 || observation.ServerToolUse != 0 {
		t.Fatalf("clean stream reported %d tool events and %d server tool requests",
			observation.ToolEvents, observation.ServerToolUse)
	}
	if observation.SessionUUID != "cccccccc-3333-4333-8333-cccccccccccc" {
		t.Fatalf("provider session identity = %q", observation.SessionUUID)
	}
	if observation.Result != `{"f1":{"outcome":"answer","value":40}}` {
		t.Fatalf("assay output = %q", observation.Result)
	}
}

// TestClaudeAssayRejectsAnyToolEvent covers all three tool-shaped block kinds.
// Counting only tool_use would let a provider-side web search through the
// zero-tool claim.
func TestClaudeAssayRejectsAnyToolEvent(t *testing.T) {
	for _, blockType := range []string{"tool_use", "server_tool_use", "mcp_tool_use"} {
		t.Run(blockType, func(t *testing.T) {
			stream := claudeAssayStream(
				claudeAssayInitEvent(nil, nil, nil),
				`{"type":"assistant","message":{"role":"assistant","content":[`+
					`{"type":"`+blockType+`","name":"Read"}]}}`,
				claudeAssayResultEvent(`{}`, 0, 0),
			)
			observation, err := parseClaudeAssayStream(stream)
			if err != nil {
				t.Fatal(err)
			}
			assertContextErrorCode(
				t,
				requireClaudeAssayIsolation("CTX-260730-a00001", observation),
				"context_assay_tool_use_detected",
			)
		})
	}
}

// TestClaudeAssayRejectsAServerSideToolRequest uses the provider's own usage
// counters, which report web search and fetch even when no block appears.
func TestClaudeAssayRejectsAServerSideToolRequest(t *testing.T) {
	for _, counts := range [][2]int{{1, 0}, {0, 1}, {2, 3}} {
		stream := claudeAssayStream(
			claudeAssayInitEvent(nil, nil, nil),
			claudeAssayResultEvent(`{}`, counts[0], counts[1]),
		)
		observation, err := parseClaudeAssayStream(stream)
		if err != nil {
			t.Fatal(err)
		}
		assertContextErrorCode(
			t,
			requireClaudeAssayIsolation("CTX-260730-a00001", observation),
			"context_assay_tool_use_detected",
		)
	}
}

// TestClaudeAssayReportsIsolationUnavailableWhenItCannotObserveTheContract is
// the deliberate asymmetry: an unobservable isolation is not a weaker isolation,
// it is no evidence at all, so it blocks before scoring with its own code.
func TestClaudeAssayReportsIsolationUnavailableWhenItCannotObserveTheContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		events []string
	}{
		{
			name:   "no session inventory was reported",
			events: []string{claudeAssayResultEvent(`{}`, 0, 0)},
		},
		{
			name:   "no result record",
			events: []string{claudeAssayInitEvent(nil, nil, nil)},
		},
		{
			name: "the provider reported an error result",
			events: []string{
				claudeAssayInitEvent(nil, nil, nil),
				`{"type":"result","subtype":"error_during_execution","is_error":true,` +
					`"session_id":"cccccccc-3333-4333-8333-cccccccccccc","result":""}`,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation, err := parseClaudeAssayStream(claudeAssayStream(test.events...))
			if err != nil {
				t.Fatal(err)
			}
			assertContextErrorCode(
				t,
				requireClaudeAssayIsolation("CTX-260730-a00001", observation),
				"context_assay_isolation_unavailable",
			)
		})
	}
}

// TestClaudeAssayReturnsStrictJSONNotAJSONString is the B3 regression.
//
// CV-2 keys its result by fact id, and the provider-neutral scorer reads that
// object. An earlier revision returned the answer text JSON-encoded, so
// {"f1":{...}} arrived as a quoted string and no fact id was visible at all. The
// bytes returned must therefore unmarshal into the fact-result object directly.
func TestClaudeAssayReturnsStrictJSONNotAJSONString(t *testing.T) {
	type factResult struct {
		Outcome string          `json:"outcome"`
		Value   json.RawMessage `json:"value"`
	}
	for _, test := range []struct {
		name   string
		result string
	}{
		{name: "bare object", result: `{"f1":{"outcome":"answer","value":40}}`},
		{
			name:   "fenced object",
			result: "```json\n{\"f1\":{\"outcome\":\"answer\",\"value\":40}}\n```",
		},
		{
			name:   "untagged fence",
			result: "```\n{\"f1\":{\"outcome\":\"answer\",\"value\":40}}\n```",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			output, err := claudeAssayStrictJSON("CTX-260730-a00001", test.result)
			if err != nil {
				t.Fatalf("a schema-conformant answer was refused: %v", err)
			}
			var facts map[string]factResult
			if err := json.Unmarshal(output, &facts); err != nil {
				t.Fatalf("the assay output is not a fact-result object: %v (%s)", err, output)
			}
			result, ok := facts["f1"]
			if !ok {
				t.Fatalf("the assay output carries no fact id: %s", output)
			}
			if result.Outcome != "answer" || string(result.Value) != "40" {
				t.Fatalf("the fact result did not survive: %+v", result)
			}
			// Parity with the Codex adapter: the raw provider bytes, not a
			// re-encoding of them.
			if string(output) != `{"f1":{"outcome":"answer","value":40}}` {
				t.Fatalf("the assay re-encoded the provider answer: %s", output)
			}
		})
	}
}

// TestClaudeAssayRefusesAResultItCannotValidateBeforeScoring is the other half:
// an answer that is not the strict JSON the schema requires has to be refused
// before it reaches the scorer, not passed along as text.
func TestClaudeAssayRefusesAResultItCannotValidateBeforeScoring(t *testing.T) {
	for _, test := range []struct {
		name   string
		result string
	}{
		{name: "empty", result: "   "},
		{name: "prose", result: "The answer is 40."},
		{name: "truncated json", result: `{"f1":{"outcome":"answer",`},
		{name: "json but not an object", result: `[{"outcome":"answer"}]`},
		{name: "json string", result: `"{\"f1\":{\"outcome\":\"answer\"}}"`},
		{name: "empty object", result: `{}`},
		{name: "prose around a fence", result: "Here you go:\n```json\n{\"f1\":{}}\n```\nhope that helps"},
		{
			// Half an envelope. The suffix used to be trimmed unconditionally, so an
			// answer cut off mid-fence had its opening fence removed and whatever
			// happened to be valid JSON inside it was accepted as a whole document.
			name:   "an opening fence with no closing fence",
			result: "```json\n{\"f1\":{\"outcome\":\"abstain\"}}",
		},
		{name: "a bare fence with no document", result: "```json"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output, err := claudeAssayStrictJSON("CTX-260730-a00001", test.result)
			if output != nil {
				t.Fatalf("an unvalidatable answer produced output: %s", output)
			}
			assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
		})
	}
}

// TestClaudeAssayRefusesAnAnswerFromAnUnrequestedSession keeps the attestation
// bound to the session identity the manager named. Without it the attestation
// would vouch for whatever session happened to answer.
//
// The schema is supplied deliberately. Without it the run refuses earlier, at the
// mandatory-schema gate, and this test would pass while never reaching the
// identity comparison it is named for.
func TestClaudeAssayRefusesAnAnswerFromAnUnrequestedSession(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	const foreign = "dddddddd-4444-4444-8444-dddddddddddd"
	stubClaudeBinary(t, filepath.Join(t.TempDir(), "invocation.txt"), string(claudeAssayStream(
		claudeAssayInitEventAs(foreign, nil, nil, nil),
		claudeAssayResultEventAs(foreign, `{"f1":{"outcome":"abstain"}}`, 0, 0),
	)))
	factory := NewClaudeHostFactory(fixture.layout)
	_, err := factory.RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00002",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: claudeCV2Schema("f1"),
		},
	)
	assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
	if !strings.Contains(err.Error(), foreign) {
		t.Fatalf("the refusal does not name the session that actually answered: %v", err)
	}
}

// TestClaudeAssayRefusesToAttestASilentColdStart is the regression for the hole
// the third review cycle found.
//
// The stub here does what a provider that stopped honouring --resume
// --fork-session would do: it starts fresh under the requested --session-id, loads
// no tools, and returns a clean schema-conformant answer. Every signal the adapter
// checked before this fix agrees with success — the reported identity is the one
// that was named, the inventory is empty, and both fork sources are byte-identical
// because a cold start touches neither.
//
// So the answer is not evidence about the base: it was produced by a session that
// inherited nothing. Returning an attestation for it would be the assay equivalent
// of the silent-cold fallback the ordinary fork path already refuses, and it would
// be worse there, because a validator would score the result as the base's belief.
func TestClaudeAssayRefusesToAttestASilentColdStart(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	t.Setenv("CLAUDE_STUB_FORK_MODE", string(claudeStubForkCold))
	recorded := filepath.Join(t.TempDir(), "invocation.txt")
	stubClaudeBinary(t, recorded, string(claudeAssayStream(
		claudeAssayInitEvent(nil, nil, nil),
		claudeAssayResultEvent(`{"f1":{"outcome":"answer","value":"cold"}}`, 0, 0),
	)))

	factory := NewClaudeHostFactory(fixture.layout)
	result, err := factory.RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00003",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: claudeCV2Schema("f1"),
		},
	)
	assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
	// No attestation and no output: a caller that ignored the error must not find a
	// scoreable answer sitting beside it.
	if result != nil {
		t.Fatalf("a cold-started session produced an assay result: %+v", result)
	}
	// The refusal has to be about lineage. Reaching it through the tool-use or
	// immutability branch would mean the cold start was caught by accident.
	if !strings.Contains(err.Error(), "in order") {
		t.Fatalf("the cold start was not refused on lineage evidence: %v", err)
	}
	fixture.assertBaseUnchanged(t)
	// The disposable fork is still retired: a refused assay cleans up exactly like
	// an accepted one, so a rejected cold session cannot linger as a candidate.
	assertClaudeAssayArtifactsRetainedForGC(t, fixture, recorded)
}

// TestClaudeAssayRefusesWhenNoForkedTranscriptWasWritten covers the other way
// lineage becomes unobservable: the provider wrote its session somewhere this
// adapter is not looking, which is what a disagreement about the project-slug rule
// looks like from here. Unobservable isolation is refused, never assumed.
func TestClaudeAssayRefusesWhenNoForkedTranscriptWasWritten(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	t.Setenv("CLAUDE_STUB_FORK_MODE", string(claudeStubForkNone))
	recorded := filepath.Join(t.TempDir(), "invocation.txt")
	stubClaudeBinary(t, recorded, string(claudeAssayStream(
		claudeAssayInitEvent(nil, nil, nil),
		claudeAssayResultEvent(`{"f1":{"outcome":"abstain"}}`, 0, 0),
	)))

	factory := NewClaudeHostFactory(fixture.layout)
	result, err := factory.RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00004",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: claudeCV2Schema("f1"),
		},
	)
	assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
	if result != nil {
		t.Fatalf("an unobservable fork produced an assay result: %+v", result)
	}
	fixture.assertBaseUnchanged(t)
	assertClaudeAssayArtifactsRetainedForGC(t, fixture, recorded)
}

// TestClaudeAssayRefusesAForkedTranscriptCarryingASecondSession covers a child
// that inherited the records but is not one session's transcript. A file holding
// two identities cannot be read as the lineage evidence for the session that
// answered, so it is refused rather than partially believed.
func TestClaudeAssayRefusesAForkedTranscriptCarryingASecondSession(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	t.Setenv("CLAUDE_STUB_FORK_MODE", string(claudeStubForkMixedSession))
	recorded := filepath.Join(t.TempDir(), "invocation.txt")
	stubClaudeBinary(t, recorded, string(claudeAssayStream(
		claudeAssayInitEvent(nil, nil, nil),
		claudeAssayResultEvent(`{"f1":{"outcome":"abstain"}}`, 0, 0),
	)))

	factory := NewClaudeHostFactory(fixture.layout)
	_, err := factory.RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00005",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: claudeCV2Schema("f1"),
		},
	)
	assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
	if !strings.Contains(err.Error(), "session identities") {
		t.Fatalf("the mixed-identity child was not refused on its session inventory: %v", err)
	}
	fixture.assertBaseUnchanged(t)
	assertClaudeAssayArtifactsRetainedForGC(t, fixture, recorded)
}

// assertClaudeAssayArtifactsRetainedForGC proves refusal paths preserve their
// artifacts and append a pending_gc event with exact private paths.
func assertClaudeAssayArtifactsRetainedForGC(
	t *testing.T, fixture *claudeForkFixture, recordedInvocation string,
) {
	t.Helper()
	invocation, err := os.ReadFile(recordedInvocation)
	if err != nil {
		t.Fatalf("the stub provider recorded no invocation to locate its artifacts: %v", err)
	}
	cwd := claudeStubField(t, string(invocation), "cwd=")
	if _, err := os.Stat(cwd); err != nil {
		t.Fatalf("the retained assay working directory %s is missing: %v", cwd, err)
	}
	slug, err := ClaudeProjectSlug(cwd)
	if err != nil {
		t.Fatal(err)
	}
	projectDir := filepath.Join(fixture.home, "projects", slug)
	if _, err := os.Stat(projectDir); err != nil {
		t.Fatalf("the retained assay provider directory %s is missing: %v", projectDir, err)
	}
	events, err := loadClaudeReclamationEvents(fixture.layout, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.WorkingDir == cwd && event.ProjectDir == projectDir &&
			event.State == claudeReclamationPendingGC {
			return
		}
	}
	t.Fatalf("no pending_gc reclamation event binds cwd %s and project dir %s", cwd, projectDir)
}

// TestRetireClaudeAssayArtifactsDeletesOnlyWhatTheAssayNamed is the first half of
// the B2 regression.
//
// Cleanup used to call os.RemoveAll on the whole derived project-slug directory.
// That directory is not proven to be this assay's: the slug is derived from the
// working directory and past 200 characters it is a truncated prefix plus a 32-bit
// path hash, so a collision or a concurrent occupant put someone else's sessions
// in the blast radius. A leaked disposable fork is a smaller loss than an
// unrelated conversation, so a directory this assay did not create keeps
// everything the assay did not name.
func legacyRetireClaudeAssayArtifactsDeletesOnlyWhatTheAssayNamed(t *testing.T) {
	root := t.TempDir()
	slugDir := filepath.Join(root, "-Users-someone-project")
	if err := os.MkdirAll(slugDir, 0o700); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(slugDir, "eeeeeeee-5555-4555-8555-eeeeeeeeeeee.jsonl")
	for _, path := range []string{
		foreign,
		filepath.Join(slugDir, "materialized.jsonl"),
		filepath.Join(slugDir, "fork.jsonl"),
	} {
		if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	workingDir := filepath.Join(root, "assay-working")
	workingRoot := claudeAssayOwnedWorkingDir(t, workingDir)

	if err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
		WorkingDir:       workingDir,
		WorkingDirRoot:   workingRoot,
		SlugDir:          slugDir,
		SlugDirOwned:     false, // the assay found this directory; it did not create it
		MaterializedPath: filepath.Join(slugDir, "materialized.jsonl"),
		ForkPath:         filepath.Join(slugDir, "fork.jsonl"),
	}); err != nil {
		t.Fatalf("retiring a pre-existing project directory's named artifacts failed: %v", err)
	}

	// Both named identities are gone, so no session this assay created is
	// discoverable.
	for _, gone := range []string{
		filepath.Join(slugDir, "materialized.jsonl"),
		filepath.Join(slugDir, "fork.jsonl"),
		workingDir,
	} {
		if _, err := os.Stat(gone); !os.IsNotExist(err) {
			t.Fatalf("%s survived the retirement: %v", gone, err)
		}
	}
	// And the occupant this assay never wrote is untouched, along with the directory
	// holding it.
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("the retirement deleted a session this assay did not create: %v", err)
	}
	if _, err := os.Stat(slugDir); err != nil {
		t.Fatalf("the retirement deleted a project directory it did not create: %v", err)
	}
}

// TestRetireClaudeAssayArtifactsFailsClosedOnAnUnaccountedOccupant covers the
// directory this assay did create.
//
// Emptiness after the two named identities are removed is the ownership proof that
// is checkable at that point. Anything still there was written by neither of them,
// and it may be the validation fork under an identity the provider chose itself —
// so it is neither deleted nor ignored: the retirement fails and the caller
// withholds the answer.
// claudeAssayOwnedSlugDir stands in for the exclusive create the assay performs:
// it produces a directory and the open handle that create took on it, which is
// what every removal inside the directory is resolved against.
func claudeAssayOwnedSlugDir(t *testing.T, dir string) *os.Root {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root
}

func legacyRetireClaudeAssayArtifactsFailsClosedOnAnUnaccountedOccupant(t *testing.T) {
	root := t.TempDir()
	slugDir := filepath.Join(root, "-Users-someone-assay")
	slugRoot := claudeAssayOwnedSlugDir(t, slugDir)
	unaccounted := filepath.Join(slugDir, "ffffffff-6666-4666-8666-ffffffffffff.jsonl")
	if err := os.WriteFile(unaccounted, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	materialized := filepath.Join(slugDir, "materialized.jsonl")
	if err := os.WriteFile(materialized, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
		SlugDir:          slugDir,
		SlugDirOwned:     true,
		SlugDirRoot:      slugRoot,
		MaterializedPath: materialized,
		ForkPath:         filepath.Join(slugDir, "fork.jsonl"),
	})
	if err == nil {
		t.Fatal("an owned project directory holding an unaccounted transcript was retired silently")
	}
	if !strings.Contains(err.Error(), filepath.Base(unaccounted)) {
		t.Fatalf("the failure does not name what it refused to delete: %v", err)
	}
	// Refusing is not deleting: the unaccounted transcript is still there for an
	// operator to look at.
	if _, statErr := os.Stat(unaccounted); statErr != nil {
		t.Fatalf("the retirement deleted the file it reported it would not: %v", statErr)
	}
	// The named identity is still retired, so the failure is about what is left over
	// rather than about giving up early.
	if _, statErr := os.Stat(materialized); !os.IsNotExist(statErr) {
		t.Fatalf("the named materialized copy survived: %v", statErr)
	}
}

// TestRetireClaudeAssayArtifactsIgnoresIdentitiesTheAssayNeverWrote is the other
// side of narrow deletion: the retirement is driven by what the assay recorded it
// created, so a run that failed before writing its copy has nothing to delete.
//
// It matters because the materialized path is derived before the write. If the
// derived name were handed to the retirement regardless of whether the write
// succeeded, a session-id collision — a target that already exists, which
// writeClaudeTranscript refuses precisely because it would otherwise be
// overwritten — would end with this assay deleting a transcript belonging to
// someone else.
func legacyRetireClaudeAssayArtifactsIgnoresIdentitiesTheAssayNeverWrote(t *testing.T) {
	root := t.TempDir()
	slugDir := filepath.Join(root, "-Users-someone-project")
	if err := os.MkdirAll(slugDir, 0o700); err != nil {
		t.Fatal(err)
	}
	collided := filepath.Join(slugDir, "materialized.jsonl")
	if err := os.WriteFile(collided, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	workingDir := filepath.Join(root, "assay-working")
	workingRoot := claudeAssayOwnedWorkingDir(t, workingDir)

	// What the deferred retirement sees after a write that never happened: the
	// working directory, and no named identities at all.
	if err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
		WorkingDir:     workingDir,
		WorkingDirRoot: workingRoot,
		SlugDir:        slugDir,
		SlugDirOwned:   false,
	}); err != nil {
		t.Fatalf("retiring a run that wrote no transcript failed: %v", err)
	}
	if _, err := os.Stat(collided); err != nil {
		t.Fatalf("the retirement deleted a transcript this assay never wrote: %v", err)
	}
	if _, err := os.Stat(workingDir); !os.IsNotExist(err) {
		t.Fatalf("the assay working directory survived: %v", err)
	}
}

// TestRetireClaudeAssayArtifactsRemovesProviderStateItCreated pins what the live
// smoke test found: claude 2.1.220 writes a memory/ directory into the project
// directory alongside the transcripts.
//
// A retirement that required the directory to be empty therefore refused every
// genuine run — the fail-closed rule has to be about a leftover session, not about
// leftover bytes. Provider state in a directory this assay created goes with the
// directory; a transcript this assay cannot name does not.
func legacyRetireClaudeAssayArtifactsRemovesProviderStateItCreated(t *testing.T) {
	root := t.TempDir()
	slugDir := filepath.Join(root, "-Users-someone-assay")
	slugRoot := claudeAssayOwnedSlugDir(t, slugDir)
	if err := os.MkdirAll(filepath.Join(slugDir, "memory"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(slugDir, "memory", "notes.md"), []byte("provider state\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	materialized := filepath.Join(slugDir, "materialized.jsonl")
	fork := filepath.Join(slugDir, "fork.jsonl")
	for _, path := range []string{materialized, fork} {
		if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
		SlugDir:          slugDir,
		SlugDirOwned:     true,
		SlugDirRoot:      slugRoot,
		MaterializedPath: materialized,
		ForkPath:         fork,
	}); err != nil {
		t.Fatalf("a project directory holding only provider state could not be retired: %v", err)
	}
	if _, err := os.Stat(slugDir); !os.IsNotExist(err) {
		t.Fatalf("the assay's own project directory survived: %v", err)
	}
}

// TestRetireClaudeAssayArtifactsFailsClosedWhenItCannotDelete is the deletion-
// failure regression. Every deletion error used to be discarded, so an assay whose
// fork could not be removed still returned a valid attestation.
func legacyRetireClaudeAssayArtifactsFailsClosedWhenItCannotDelete(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions, so deletion cannot be made to fail")
	}
	root := t.TempDir()
	slugDir := filepath.Join(root, "-Users-someone-locked")
	slugRoot := claudeAssayOwnedSlugDir(t, slugDir)
	materialized := filepath.Join(slugDir, "materialized.jsonl")
	if err := os.WriteFile(materialized, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A directory that cannot be written is a directory whose entries cannot be
	// unlinked.
	if err := os.Chmod(slugDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(slugDir, 0o700) })

	err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
		SlugDir:          slugDir,
		SlugDirOwned:     true,
		SlugDirRoot:      slugRoot,
		MaterializedPath: materialized,
	})
	if err == nil {
		t.Fatal("an undeletable materialized copy was reported as retired")
	}
	if !strings.Contains(err.Error(), materialized) {
		t.Fatalf("the failure does not name the artifact that survived: %v", err)
	}
}

// TestClaudeAssayWithholdsAnAnswerWhoseForkItCannotRetire is the end-to-end half
// of B2.
//
// The run succeeds in every other respect: the deny policy is honoured, the fork
// inherits the base, the sources are untouched and the answer conforms to the
// schema. Then the project directory turns out to hold something this adapter
// never wrote, so the disposable fork cannot be shown to be undiscoverable — and
// the attestation is withheld instead of returned beside it. Nor is the foreign
// occupant deleted to make the retirement succeed.
func legacyClaudeAssayWithholdsAnAnswerWhoseForkItCannotRetire(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	recorded := filepath.Join(t.TempDir(), "invocation.txt")
	const foreignName = "eeeeeeee-5555-4555-8555-eeeeeeeeeeee.jsonl"
	t.Setenv("CLAUDE_STUB_EXTRA_PROJECT_FILE", foreignName)
	stubClaudeBinary(t, recorded, string(claudeAssayStream(
		claudeAssayInitEvent(nil, nil, nil),
		claudeAssayResultEvent(`{"f1":{"outcome":"answer","value":"40"}}`, 0, 0),
	)))

	result, err := NewClaudeHostFactory(fixture.layout).RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00007",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: claudeCV2Schema("f1"),
		},
	)
	if result != nil {
		t.Fatalf("an assay whose fork could not be retired returned an attestation: %+v", result)
	}
	assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
	if !strings.Contains(err.Error(), "retired") {
		t.Fatalf("the refusal is not about the failed retirement: %v", err)
	}
	fixture.assertBaseUnchanged(t)

	// The two identities the assay named are gone even though the retirement failed
	// overall, and the occupant it did not name survived.
	invocation, readErr := os.ReadFile(recorded)
	if readErr != nil {
		t.Fatal(readErr)
	}
	cwd := claudeStubField(t, string(invocation), "cwd=")
	slug, slugErr := ClaudeProjectSlug(cwd)
	if slugErr != nil {
		t.Fatal(slugErr)
	}
	projectDir := filepath.Join(fixture.home, "projects", slug)
	entries, readDirErr := os.ReadDir(projectDir)
	if readDirErr != nil {
		t.Fatalf("the project directory holding an unaccounted session was deleted: %v", readDirErr)
	}
	if len(entries) != 1 || entries[0].Name() != foreignName {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("the project directory holds %v, want only the unaccounted %s", names, foreignName)
	}
	// The working directory is unambiguously this adapter's, so it goes regardless.
	if _, statErr := os.Stat(cwd); !os.IsNotExist(statErr) {
		t.Fatalf("the assay working directory survived: %v", statErr)
	}
}

// TestClaudeAssayRejectsADenyPolicyBypass is the bypass-attempt case the AC
// names. A provider that kept a tool, an MCP server, or a skill loaded did not
// honour the deny policy, so this machine cannot enforce the contract — a
// capability failure, not a tool-use failure, because nothing was used yet.
func TestClaudeAssayRejectsADenyPolicyBypass(t *testing.T) {
	for _, test := range []struct {
		name string
		init string
	}{
		{
			name: "a built-in tool survived the deny policy",
			init: claudeAssayInitEvent([]string{"Read", "Bash"}, nil, nil),
		},
		{
			name: "an MCP server survived the deny policy",
			init: claudeAssayInitEvent(nil, []string{"figma"}, nil),
		},
		{
			name: "a skill survived the deny policy",
			init: claudeAssayInitEvent(nil, nil, []string{"/context-builder"}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation, err := parseClaudeAssayStream(
				claudeAssayStream(test.init, claudeAssayResultEvent(`{}`, 0, 0)))
			if err != nil {
				t.Fatal(err)
			}
			assertContextErrorCode(
				t,
				requireClaudeAssayIsolation("CTX-260730-a00001", observation),
				"context_assay_isolation_unavailable",
			)
		})
	}
}

// TestClaudeAssayRefusesAnInitEventThatDoesNotStateTheWholeInventory is the
// first half of the B1 regression.
//
// An omitted or null inventory field has the same length as an explicitly
// reported empty one, so the previous revision accepted a protocol that stopped
// answering the question as proof that the answer was zero. A provider release
// that renamed or dropped `tools` would therefore have produced a clean zero-tool
// attestation for a session running with the full tool set. Every field is
// required explicitly, and silence is a capability failure.
func TestClaudeAssayRefusesAnInitEventThatDoesNotStateTheWholeInventory(t *testing.T) {
	const session = "cccccccc-3333-4333-8333-cccccccccccc"
	for _, test := range []struct {
		name    string
		members []string
		missing string
	}{
		{
			name:    "tools omitted",
			members: []string{`"mcp_servers":[]`, `"slash_commands":[]`},
			missing: "tools",
		},
		{
			name:    "mcp_servers omitted",
			members: []string{`"tools":[]`, `"slash_commands":[]`},
			missing: "mcp_servers",
		},
		{
			name:    "slash_commands omitted",
			members: []string{`"tools":[]`, `"mcp_servers":[]`},
			missing: "slash_commands",
		},
		{
			name:    "the whole inventory omitted",
			members: nil,
			missing: "tools",
		},
		{
			name:    "tools null",
			members: []string{`"tools":null`, `"mcp_servers":[]`, `"slash_commands":[]`},
			missing: "tools",
		},
		{
			name:    "mcp_servers null",
			members: []string{`"tools":[]`, `"mcp_servers":null`, `"slash_commands":[]`},
			missing: "mcp_servers",
		},
		{
			name:    "slash_commands null",
			members: []string{`"tools":[]`, `"mcp_servers":[]`, `"slash_commands":null`},
			missing: "slash_commands",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation, err := parseClaudeAssayStream(claudeAssayStream(
				claudeAssayInitEventRaw(session, test.members...),
				claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
			))
			if err != nil {
				t.Fatal(err)
			}
			// The observation itself has to record silence rather than a zero, or the
			// refusal below would be the only thing standing between an unobserved
			// inventory and an attestation that claims to have counted it.
			if observation.InventoryReported {
				t.Fatalf("an init event without %s was read as a complete inventory report", test.missing)
			}
			isolationErr := requireClaudeAssayIsolation("CTX-260730-a00001", observation)
			assertContextErrorCode(t, isolationErr, "context_assay_isolation_unavailable")
			if !strings.Contains(isolationErr.Error(), test.missing) {
				t.Fatalf("the refusal does not name the unreported field %s: %v", test.missing, isolationErr)
			}
		})
	}

	// And the explicit empty arrays still pass, so the gate rejects silence rather
	// than rejecting every init event.
	observation, err := parseClaudeAssayStream(claudeAssayStream(
		claudeAssayInitEventRaw(session, `"tools":[]`, `"mcp_servers":[]`, `"slash_commands":[]`),
		claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
	))
	if err != nil {
		t.Fatal(err)
	}
	if !observation.InventoryReported {
		t.Fatal("an init event stating all three empty inventories was read as incomplete")
	}
	if err := requireClaudeAssayIsolation("CTX-260730-a00001", observation); err != nil {
		t.Fatalf("a fully reported empty inventory was refused: %v", err)
	}
}

// TestClaudeAssayRefusesAnInventoryFromADifferentSessionThanTheAnswer is the
// second half of B1.
//
// The deny policy is read back from the init event and the answer arrives on the
// result event, so the attestation is only honest when both belong to one session.
// The previous revision recorded the init identity only while none was set and let
// any later result identity overwrite it, so an empty inventory from session A
// plus a successful answer from session B produced an attestation for B carrying
// A's zero-tool evidence.
func TestClaudeAssayRefusesAnInventoryFromADifferentSessionThanTheAnswer(t *testing.T) {
	const inventorySession = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
	const answeringSession = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
	observation, err := parseClaudeAssayStream(claudeAssayStream(
		claudeAssayInitEventAs(inventorySession, nil, nil, nil),
		claudeAssayResultEventAs(answeringSession, `{"f1":{"outcome":"answer","value":"40"}}`, 0, 0),
	))
	if err != nil {
		t.Fatal(err)
	}
	// No single identity may be derived from disagreeing evidence: an attestation
	// field that silently picked one of the two is the defect.
	if observation.SessionUUID != "" {
		t.Fatalf("two disagreeing identities produced one attestable session %q", observation.SessionUUID)
	}
	isolationErr := requireClaudeAssayIsolation("CTX-260730-a00001", observation)
	assertContextErrorCode(t, isolationErr, "context_assay_isolation_unavailable")
	for _, named := range []string{inventorySession, answeringSession} {
		if !strings.Contains(isolationErr.Error(), named) {
			t.Fatalf("the refusal does not name %s: %v", named, isolationErr)
		}
	}

	// An init event that reports no identity at all is the same refusal: the
	// inventory cannot be attributed to the session that answered.
	observation, err = parseClaudeAssayStream(claudeAssayStream(
		claudeAssayInitEventRaw("", `"tools":[]`, `"mcp_servers":[]`, `"slash_commands":[]`),
		claudeAssayResultEventAs(answeringSession, `{"f1":{"outcome":"abstain"}}`, 0, 0),
	))
	if err != nil {
		t.Fatal(err)
	}
	assertContextErrorCode(
		t,
		requireClaudeAssayIsolation("CTX-260730-a00001", observation),
		"context_assay_isolation_unavailable",
	)
}

// TestClaudeAssayRefusesRepeatedInventoryOrResultEvidence covers the duplicate
// path B1 named.
//
// A non-empty inventory followed by an empty one used to pass, because the later
// record overwrote the earlier counts: the provider said it had loaded tools and
// the adapter attested that it had not. Reconciling the two would mean choosing
// which report to believe, so both are refused instead.
func TestClaudeAssayRefusesRepeatedInventoryOrResultEvidence(t *testing.T) {
	const session = "cccccccc-3333-4333-8333-cccccccccccc"
	for _, test := range []struct {
		name   string
		events []string
	}{
		{
			name: "a non-empty inventory followed by an empty one",
			events: []string{
				claudeAssayInitEventAs(session, []string{"Read", "Bash"}, nil, nil),
				claudeAssayInitEventAs(session, nil, nil, nil),
				claudeAssayResultEventAs(session, `{"f1":{"outcome":"answer","value":"40"}}`, 0, 0),
			},
		},
		{
			name: "an empty inventory followed by a non-empty one",
			events: []string{
				claudeAssayInitEventAs(session, nil, nil, nil),
				claudeAssayInitEventAs(session, []string{"WebFetch"}, nil, nil),
				claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
			},
		},
		{
			name: "two result records",
			events: []string{
				claudeAssayInitEventAs(session, nil, nil, nil),
				claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 1, 0),
				claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation, err := parseClaudeAssayStream(claudeAssayStream(test.events...))
			if err != nil {
				t.Fatal(err)
			}
			assertContextErrorCode(
				t,
				requireClaudeAssayIsolation("CTX-260730-a00001", observation),
				"context_assay_isolation_unavailable",
			)
		})
	}
}

// TestClaudeAssayRefusesMessageContentItCannotClassify keeps the zero-tool claim
// tied to content this adapter actually read.
//
// A block whose kind is unstated, or a content shape this adapter does not model,
// may be a tool use. "No tool_use block was recognized" is not the same statement
// as "no tool ran", so the unobservable case is refused rather than counted as
// zero — while the two shapes the provider really emits, a typed block array and
// a bare text string, stay accepted.
func TestClaudeAssayRefusesMessageContentItCannotClassify(t *testing.T) {
	const session = "cccccccc-3333-4333-8333-cccccccccccc"
	for _, test := range []struct {
		name    string
		message string
	}{
		{
			name:    "a block that does not state its kind",
			message: `{"type":"assistant","message":{"role":"assistant","content":[{"name":"Read"}]}}`,
		},
		{
			name:    "a block whose kind is blank",
			message: `{"type":"assistant","message":{"role":"assistant","content":[{"type":"  "}]}}`,
		},
		{
			name:    "content that is neither an array nor a string",
			message: `{"type":"assistant","message":{"role":"assistant","content":{"type":"text"}}}`,
		},
		{
			name:    "a block that is not an object",
			message: `{"type":"assistant","message":{"role":"assistant","content":["text"]}}`,
		},
		{
			name:    "content the message explicitly nulled",
			message: `{"type":"assistant","message":{"role":"assistant","content":null}}`,
		},
		{
			name:    "a message that carries no content key at all",
			message: `{"type":"assistant","message":{"role":"assistant"}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation, err := parseClaudeAssayStream(claudeAssayStream(
				claudeAssayInitEventAs(session, nil, nil, nil),
				test.message,
				claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
			))
			if err != nil {
				t.Fatal(err)
			}
			if observation.UnreadableBlocks == 0 {
				t.Fatalf("unclassifiable content was read as observed: %+v", observation)
			}
			assertContextErrorCode(
				t,
				requireClaudeAssayIsolation("CTX-260730-a00001", observation),
				"context_assay_isolation_unavailable",
			)
		})
	}

	// The shapes the provider really uses stay accepted, including the bare-string
	// content of a text-only message — which the previous projection could not even
	// decode, so an ordinary echoed user turn would have failed the whole stream.
	for _, accepted := range []string{
		`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"{}"}]}}`,
		`{"type":"user","message":{"role":"user","content":"answer the fact catalog"}}`,
		`{"type":"assistant","message":{"role":"assistant","content":[{"type":"thinking"}]}}`,
		`{"type":"assistant","message":{"role":"assistant","content":[]}}`,
	} {
		observation, err := parseClaudeAssayStream(claudeAssayStream(
			claudeAssayInitEventAs(session, nil, nil, nil),
			accepted,
			claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
		))
		if err != nil {
			t.Fatalf("an ordinary provider event failed to parse: %v (%s)", err, accepted)
		}
		if err := requireClaudeAssayIsolation("CTX-260730-a00001", observation); err != nil {
			t.Fatalf("an ordinary provider event was refused: %v (%s)", err, accepted)
		}
	}
}

// TestClaudeAssayRefusesAnUnknownMessageBlockKind is the verdict-06 B1
// regression.
//
// The classifier counted three tool kinds and let every other non-empty kind fall
// through as benign. So a stream that was valid in every other respect — matching
// init and result identities, empty inventory, clean result — could carry a block
// shaped like a tool call under a kind this adapter had never heard of, and the
// observation came back with ToolEvents:0, UnreadableBlocks:0 and no isolation
// error. A provider protocol extension could hide tool activity inside a known
// turn event and still collect the attested zero-tool count.
//
// The fix inverts the default: known-inert kinds are allowlisted from evidence,
// known tool kinds are counted, and anything else is unobservable. This test uses
// the reviewer's exact probe block, plus the kinds a future provider release is
// most likely to introduce.
func TestClaudeAssayRefusesAnUnknownMessageBlockKind(t *testing.T) {
	const session = "11111111-2222-4222-8222-111111111111"
	for _, probe := range []struct {
		name  string
		block string
		kind  string
	}{
		{
			name:  "the reviewer's tool-shaped extension",
			block: `{"type":"future_tool_use","name":"Read"}`,
			kind:  "future_tool_use",
		},
		{
			name:  "a renamed server tool",
			block: `{"type":"provider_tool_invocation","name":"WebSearch"}`,
			kind:  "provider_tool_invocation",
		},
		{
			name:  "a kind that merely looks inert",
			block: `{"type":"reasoning","summary":"thought about it"}`,
			kind:  "reasoning",
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			observation, err := parseClaudeAssayStream(claudeAssayStream(
				claudeAssayInitEventAs(session, nil, nil, nil),
				`{"type":"assistant","message":{"role":"assistant","content":[`+probe.block+`]}}`,
				claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
			))
			if err != nil {
				t.Fatal(err)
			}
			// The precise failure the reviewer reproduced: a complete observation
			// claiming nothing unobserved happened.
			if observation.UnreadableBlocks == 0 {
				t.Fatalf(
					"unknown content block %q was accepted as zero-tool evidence: ToolEvents:%d "+
						"UnreadableBlocks:%d",
					probe.kind, observation.ToolEvents, observation.UnreadableBlocks,
				)
			}
			refusal := requireClaudeAssayIsolation("CTX-260730-a00001", observation)
			assertContextErrorCode(t, refusal, "context_assay_isolation_unavailable")
			// Naming the kind is what makes the refusal actionable: widening the inert
			// allowlist needs to know which shape to bring evidence for.
			if !strings.Contains(refusal.Error(), probe.kind) {
				t.Fatalf("the refusal does not name the unclassifiable kind %q: %v", probe.kind, refusal)
			}
		})
	}

	// A tool_result is not merely unobservable, it is proof a tool already ran, and
	// the previous revision counted it as benign. It is the user-turn half of a tool
	// call — observed on claude 2.1.220 as {type, tool_use_id, content} — so it gets
	// the tool-use verdict rather than the unobservable one.
	observation, err := parseClaudeAssayStream(claudeAssayStream(
		claudeAssayInitEventAs(session, nil, nil, nil),
		`{"type":"user","message":{"role":"user","content":[`+
			`{"type":"tool_result","tool_use_id":"toolu_1","content":"the magic word is zylphard"}]}}`,
		claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
	))
	if err != nil {
		t.Fatal(err)
	}
	if observation.ToolEvents != 1 {
		t.Fatalf("a tool_result block was not counted as tool evidence: %+v", observation)
	}
	assertContextErrorCode(
		t,
		requireClaudeAssayIsolation("CTX-260730-a00001", observation),
		"context_assay_tool_use_detected",
	)
}

// TestClaudeAssayRefusesUnobservedMessageContent is the review-07 B2 regression.
//
// The classifier returned zero tools and zero unreadable blocks for empty and
// JSON-null content, so a stream that was otherwise a clean success — matching
// init and result identities, empty inventory, no server tool use, no denials —
// could carry an assistant turn saying "content":null and still pass
// requireClaudeAssayIsolation with ToolEvents:0 and UnreadableBlocks:0.
//
// The evidence model here recognizes exactly two observed shapes: a text string,
// and an array of exhaustively classified typed blocks. "The content key was not
// there" is neither. A negative claim — no tool ran — cannot rest on a message
// this adapter never saw the contents of, so absent content joins the unreadable
// bucket alongside an unmodelled block kind. The distinction the test pins is
// between an absent array and an empty one: [] is an observed statement that the
// turn carried no blocks and stays accepted.
func TestClaudeAssayRefusesUnobservedMessageContent(t *testing.T) {
	const session = "dddddddd-4444-4444-8444-dddddddddddd"
	for _, probe := range []struct {
		name    string
		message string
		kind    string
	}{
		{
			name:    "an assistant turn whose content is JSON null",
			message: `{"type":"assistant","message":{"role":"assistant","content":null}}`,
			kind:    "<null content>",
		},
		{
			name:    "an assistant turn with no content key at all",
			message: `{"type":"assistant","message":{"role":"assistant"}}`,
			kind:    "<absent content>",
		},
		{
			name:    "a user turn whose content is JSON null",
			message: `{"type":"user","message":{"role":"user","content":null}}`,
			kind:    "<null content>",
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			observation, err := parseClaudeAssayStream(claudeAssayStream(
				claudeAssayInitEventAs(session, nil, nil, nil),
				probe.message,
				claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
			))
			if err != nil {
				t.Fatal(err)
			}
			// The precise failure the reviewer reproduced.
			if observation.UnreadableBlocks == 0 {
				t.Fatalf(
					"unobserved message content was accepted as zero-tool evidence: ToolEvents:%d "+
						"UnreadableBlocks:%d",
					observation.ToolEvents, observation.UnreadableBlocks,
				)
			}
			refusal := requireClaudeAssayIsolation("CTX-260730-a00001", observation)
			assertContextErrorCode(t, refusal, "context_assay_isolation_unavailable")
			if !strings.Contains(refusal.Error(), probe.kind) {
				t.Fatalf("the refusal does not name what it could not observe (%s): %v", probe.kind, refusal)
			}
		})
	}

	// Content the provider did state stays accepted. An empty block array and an
	// empty text string are observed statements, not silence: the turn is reported
	// to have carried nothing. Refusing them would fail ordinary runs over evidence
	// that is actually present, which is the opposite error to the one being fixed.
	for _, stated := range []string{
		`{"type":"assistant","message":{"role":"assistant","content":[]}}`,
		`{"type":"user","message":{"role":"user","content":""}}`,
		`{"type":"user","message":{"role":"user","content":"   "}}`,
	} {
		observation, err := parseClaudeAssayStream(claudeAssayStream(
			claudeAssayInitEventAs(session, nil, nil, nil),
			stated,
			claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
		))
		if err != nil {
			t.Fatal(err)
		}
		if observation.UnreadableBlocks != 0 {
			t.Fatalf("stated-but-empty content was read as unobserved: %+v (%s)", observation, stated)
		}
		if err := requireClaudeAssayIsolation("CTX-260730-a00001", observation); err != nil {
			t.Fatalf("stated-but-empty content was refused: %v (%s)", err, stated)
		}
	}
}

// TestClaudeAssayRefusesTheWholeRunOnUnobservedMessageContent is the end-to-end
// half of the B2 fix: RunContextAssay itself refuses, no answer reaches the
// scorer, the base is untouched and the fork is still retired.
func TestClaudeAssayRefusesTheWholeRunOnUnobservedMessageContent(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	recorded := filepath.Join(t.TempDir(), "invocation.txt")
	// Everything about this run looks like a clean success except one assistant turn
	// whose contents this adapter never saw.
	stubClaudeBinary(t, recorded, string(claudeAssayStream(
		claudeAssayInitEvent(nil, nil, nil),
		`{"type":"assistant","message":{"role":"assistant","content":null}}`,
		claudeAssayResultEvent(`{"f1":{"outcome":"answer","value":"40"}}`, 0, 0),
	)))

	result, err := NewClaudeHostFactory(fixture.layout).RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00021",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: claudeCV2Schema("f1"),
		},
	)
	if result != nil {
		t.Fatalf("unobserved message content produced a scoreable answer: %s", result.Output)
	}
	assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
	if !strings.Contains(err.Error(), "<null content>") {
		t.Fatalf("the refusal does not name what it could not observe: %v", err)
	}
	fixture.assertBaseUnchanged(t)
	assertClaudeAssayArtifactsRetainedForGC(t, fixture, recorded)
}

// TestClaudeAssayRefusesTheWholeRunOnAnUnknownMessageBlockKind is the end-to-end
// half of the B1 fix: the unknown block is refused by RunContextAssay itself, no
// answer reaches the scorer, the base is untouched and the fork is still retired.
func TestClaudeAssayRefusesTheWholeRunOnAnUnknownMessageBlockKind(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	recorded := filepath.Join(t.TempDir(), "invocation.txt")
	// Everything about this run looks like a clean success except one assistant
	// block whose kind this adapter has no evidence about.
	stubClaudeBinary(t, recorded, string(claudeAssayStream(
		claudeAssayInitEvent(nil, nil, nil),
		`{"type":"assistant","message":{"role":"assistant","content":[`+
			`{"type":"future_tool_use","name":"Read","input":{"file_path":"/etc/passwd"}}]}}`,
		claudeAssayResultEvent(`{"f1":{"outcome":"answer","value":"40"}}`, 0, 0),
	)))

	result, err := NewClaudeHostFactory(fixture.layout).RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00016",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: claudeCV2Schema("f1"),
		},
	)
	if result != nil {
		t.Fatalf("an unclassifiable content block produced a scoreable answer: %s", result.Output)
	}
	assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
	if !strings.Contains(err.Error(), "future_tool_use") {
		t.Fatalf("the refusal does not name the unclassifiable block kind: %v", err)
	}
	fixture.assertBaseUnchanged(t)
	assertClaudeAssayArtifactsRetainedForGC(t, fixture, recorded)
}

// TestClaudeAssayInertBlockKindsAreEvidenceBacked pins the allowlist to what the
// running provider actually emits.
//
// Both entries were probed against claude 2.1.220 under this exact deny policy:
// text {type, text} on every answering run, and thinking {type, thinking,
// signature} on a run that reasons first. The test exists so that widening the
// set is a visible act — a new kind added without that evidence fails here rather
// than quietly becoming part of the zero-tool claim.
func TestClaudeAssayInertBlockKindsAreEvidenceBacked(t *testing.T) {
	wantInert := map[string]struct{}{"text": {}, "thinking": {}}
	if len(claudeAssayInertContentBlockKinds) != len(wantInert) {
		t.Fatalf(
			"the inert block allowlist changed to %v; each kind needs a probe against the running "+
				"provider showing it is emitted and cannot describe tool activity",
			claudeAssayInertContentBlockKinds,
		)
	}
	for kind := range wantInert {
		if _, ok := claudeAssayInertContentBlockKinds[kind]; !ok {
			t.Fatalf("%q is emitted by claude 2.1.220 under the deny policy but is not accepted", kind)
		}
	}
	// The two sets must stay disjoint: a kind in both would be counted and excused
	// by the same pass.
	for kind := range claudeAssayToolContentBlockKinds {
		if _, both := claudeAssayInertContentBlockKinds[kind]; both {
			t.Fatalf("%q is classified as both a tool block and an inert block", kind)
		}
	}
	// The real shapes, with their real fields, stay accepted.
	const session = "22222222-3333-4333-8333-222222222222"
	observation, err := parseClaudeAssayStream(claudeAssayStream(
		claudeAssayInitEventAs(session, nil, nil, nil),
		`{"type":"assistant","message":{"role":"assistant","content":[`+
			`{"type":"thinking","thinking":"17 sheep, all but 9 run away","signature":"abc"}]}}`,
		`{"type":"assistant","message":{"role":"assistant","content":[`+
			`{"type":"text","text":"{\"f1\":{\"outcome\":\"abstain\"}}"}]}}`,
		claudeAssayResultEventAs(session, `{"f1":{"outcome":"abstain"}}`, 0, 0),
	))
	if err != nil {
		t.Fatalf("the block shapes the provider really emits failed to parse: %v", err)
	}
	if observation.UnreadableBlocks != 0 || observation.ToolEvents != 0 {
		t.Fatalf("a genuine thinking/text turn was not read as inert: %+v", observation)
	}
	if err := requireClaudeAssayIsolation("CTX-260730-a00001", observation); err != nil {
		t.Fatalf("a genuine thinking/text turn was refused: %v", err)
	}
}

// TestClaudeAssayRefusesTheWholeRunOnAnUnobservableInventory is the end-to-end
// half of B1: the drift is refused by RunContextAssay itself, no result is
// produced, and the disposable fork is still retired.
func TestClaudeAssayRefusesTheWholeRunOnAnUnobservableInventory(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	recorded := filepath.Join(t.TempDir(), "invocation.txt")
	// A provider release that stopped reporting its tool inventory. Everything else
	// about the run looks exactly like success.
	stubClaudeBinary(t, recorded, string(claudeAssayStream(
		claudeAssayInitEventRaw(
			claudeStubSessionPlaceholder, `"mcp_servers":[]`, `"slash_commands":[]`),
		claudeAssayResultEvent(`{"f1":{"outcome":"answer","value":"40"}}`, 0, 0),
	)))

	result, err := NewClaudeHostFactory(fixture.layout).RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00006",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: claudeCV2Schema("f1"),
		},
	)
	if result != nil {
		t.Fatalf("an unobservable deny policy produced a scoreable answer: %s", result.Output)
	}
	assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
	if !strings.Contains(err.Error(), "tools") {
		t.Fatalf("the refusal does not name the unreported inventory field: %v", err)
	}
	fixture.assertBaseUnchanged(t)
	assertClaudeAssayArtifactsRetainedForGC(t, fixture, recorded)
}

func TestParseClaudeAssayStreamFailsClosedOnAnUnreadableEvent(t *testing.T) {
	if _, err := parseClaudeAssayStream([]byte("{\"type\":\n")); err == nil {
		t.Fatal("an unreadable assay event stream was parsed successfully")
	}
	// A run that never names a provider session identity cannot be attested to,
	// so it fails at parse time rather than producing an attestation with a blank
	// provider identity field.
	if _, err := parseClaudeAssayStream(claudeAssayStream(
		`{"type":"result","subtype":"success","is_error":false,"result":"{}"}`,
	)); err == nil {
		t.Fatal("a run with no provider session identity was accepted")
	}
}

// TestClaudeAssayRunsRepositoryFreeAndRetainsItsForkForGC drives the real
// RunContextAssay against a stub provider binary. It proves the four facts the
// attestation claims: the working directory has no repository in it, the launch
// carries the deny policy and the fork selector, the base is untouched, and
// every validation artifact is retained under a private pending_gc record.
func TestClaudeAssayRunsRepositoryFreeAndRetainsItsForkForGC(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	recorded := filepath.Join(t.TempDir(), "assay-invocation.txt")
	stubClaudeBinary(t, recorded, string(claudeAssayStream(
		claudeAssayInitEvent(nil, nil, nil),
		claudeAssayResultEvent(`{"f1":{"outcome":"abstain"}}`, 0, 0),
	)))
	baseDigestBefore, err := claudeFileDigest(fixture.basePath)
	if err != nil {
		t.Fatal(err)
	}

	factory := NewClaudeHostFactory(fixture.layout)
	result, err := factory.RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00001",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: claudeCV2Schema("f1"),
			Model:        "haiku",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if result.Attestation.Contract != claudeContextAssayContract {
		t.Fatalf("attestation contract = %q", result.Attestation.Contract)
	}
	if !result.Attestation.RepositoryFree {
		t.Fatal("the attestation does not claim a repository-free run")
	}
	if result.Attestation.ToolEventCount != 0 {
		t.Fatalf("attested tool events = %d, want 0", result.Attestation.ToolEventCount)
	}
	if result.Attestation.BaseSessionUUID != fixture.baseUUID {
		t.Fatalf("attested base identity = %q, want %q",
			result.Attestation.BaseSessionUUID, fixture.baseUUID)
	}
	if result.Attestation.AssaySessionUUID == result.Attestation.BaseSessionUUID {
		t.Fatal("the assay attested the base identity as its own fork")
	}
	if result.Attestation.PolicyDigest == "" || result.Attestation.MaterializedSHA256 == "" {
		t.Fatalf("attestation is missing binding evidence: %+v", result.Attestation)
	}
	// Positive lineage evidence, not just a matching identity: the attested count is
	// every record identity the materialized copy carried, found in order in the
	// child the provider wrote. The stub reaches this by doing what the real
	// provider does — restamping sessionId while copying record uuids through — so a
	// zero here would mean the answer came from a session that inherited nothing.
	_, baseFacts, err := inspectClaudeTranscriptFile(fixture.basePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(baseFacts.RecordUUIDs) == 0 {
		t.Fatal("the fixture base carries no record identities, so lineage is untestable")
	}
	if result.Attestation.InheritedRecords != len(baseFacts.RecordUUIDs) {
		t.Fatalf("attested inherited records = %d, want the base's %d",
			result.Attestation.InheritedRecords, len(baseFacts.RecordUUIDs))
	}
	// The output is the strict JSON object the provider-neutral scorer reads, not a
	// JSON string that happens to contain one: it has to unmarshal into the fact
	// result shape keyed by fact id.
	var facts map[string]struct {
		Outcome string          `json:"outcome"`
		Value   json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(result.Output, &facts); err != nil {
		t.Fatalf("the assay output is not a fact-result object: %v (%s)", err, result.Output)
	}
	if facts["f1"].Outcome != "abstain" {
		t.Fatalf("assay output = %s", result.Output)
	}

	invocation, err := os.ReadFile(recorded)
	if err != nil {
		t.Fatal(err)
	}
	text := string(invocation)
	for _, required := range []string{
		"--fork-session", "--tools", "--strict-mcp-config",
		"--setting-sources", "--disable-slash-commands",
		"--output-format stream-json", "--model haiku",
		"--session-id " + result.Attestation.AssaySessionUUID,
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("the assay launch did not carry %q:\n%s", required, text)
		}
	}
	// --json-schema is withheld on purpose: the provider implements
	// schema-constrained output as a tool named StructuredOutput, so passing the
	// caller's schema would put a tool back into a run whose whole claim is that it
	// had none — and with the tool set denied it cannot complete anyway. The shape
	// is enforced by the adapter instead, and the attestation says which.
	if strings.Contains(text, "--json-schema") {
		t.Fatalf("the assay passed a schema flag that re-adds a tool to a zero-tool run:\n%s", text)
	}
	// The schema was supplied, so the attestation has to say it was actually
	// enforced and name which one. "adapter-shape" here would be the exact claim
	// the review found untrue.
	if result.Attestation.SchemaEnforcement != claudeAssaySchemaEnforcement {
		t.Fatalf("the attestation does not report enforced schema validation: %+v", result.Attestation)
	}
	wantSchemaDigest, err := claudeAssaySchemaDigest(claudeCV2Schema("f1"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Attestation.SchemaDigest != wantSchemaDigest {
		t.Fatalf("the attestation binds schema digest %q, want %q",
			result.Attestation.SchemaDigest, wantSchemaDigest)
	}
	cwdLine := claudeStubField(t, text, "cwd=")
	entries, err := os.ReadDir(cwdLine)
	if err == nil && len(entries) != 0 {
		t.Fatalf("the assay ran in a directory holding %d entries, not a repository-free one", len(entries))
	}
	if strings.HasPrefix(cwdLine, fixture.baseDir+string(filepath.Separator)) {
		t.Fatalf("the assay ran inside the project checkout at %s", cwdLine)
	}
	// Compare resolved paths: the recorded cwd comes from the shell, which
	// resolves the platform's /var -> /private/var symlink.
	resolvedState, err := filepath.EvalSymlinks(fixture.layout.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cwdLine, resolvedState) {
		t.Fatalf("the assay working directory %s is not manager-private state under %s",
			cwdLine, resolvedState)
	}

	digestAfter, err := claudeFileDigest(fixture.basePath)
	if err != nil {
		t.Fatal(err)
	}
	if digestAfter != baseDigestBefore {
		t.Fatal("the assay mutated the base it forked")
	}
	assertClaudeAssayArtifactsRetainedForGC(t, fixture, recorded)
}

// TestClaudeAssayRefusesToScoreAnInterruptedOrCompactedBase keeps the score
// describing the right artifact: quizzing a half-finished or summarized
// transcript measures something the registry never published.
func TestClaudeAssayRefusesToScoreAnInterruptedOrCompactedBase(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	stubClaudeBinary(t, filepath.Join(t.TempDir(), "unused.txt"), "")
	factory := NewClaudeHostFactory(fixture.layout)
	source := fixture.assaySource(t)

	fixture.rewriteBase(t, []string{
		claudeUserRecord("u1", "", fixture.baseUUID, fixture.baseDir, "interrupted"),
	})
	_, err := factory.RunContextAssay(context.Background(), source, ClaudeContextAssayRequest{
		BaseCtxID:    "CTX-260730-c00001",
		Prompt:       "answer",
		OutputSchema: claudeCV2Schema("f1"),
	})
	assertPublicErrorCode(t, err, claudeContextBaseNotForkable)

	fixture.rewriteBase(t, append(
		claudeCompletedTranscriptRecords(fixture.baseUUID, fixture.baseDir),
		`{"type":"system","sessionId":"`+fixture.baseUUID+`","compactMetadata":{"trigger":"auto"}}`,
	))
	_, err = factory.RunContextAssay(context.Background(), source, ClaudeContextAssayRequest{
		BaseCtxID:    "CTX-260730-c00001",
		Prompt:       "answer",
		OutputSchema: claudeCV2Schema("f1"),
	})
	assertContextErrorCode(t, err, "context_compacted")
}

// TestManagerReportsAssayIsolationUnavailableWithoutAnEnforcingAdapter is the
// manager-side half of CV-2: no enforcing adapter means no score, reported with
// the stable public code rather than by attempting the run anyway.

// TestClaudeAssayEnforcesTheSuppliedSchemaBeforeScoring is the primary
// schema-enforcement regression.
//
// The adapter used to accept the caller's schema, check only that it was
// syntactically valid JSON, and then apply a generic "non-empty object" bar to the
// answer. Every shape below satisfies that bar and violates CV-2, which is
// explicit that a missing fact id, an extra fact id, or any other shape is
// malformed output rather than an abstention. Each one must now be refused before
// it can reach the scorer.
func TestClaudeAssayEnforcesTheSuppliedSchemaBeforeScoring(t *testing.T) {
	schema := claudeCV2Schema("f1", "f2")
	for _, test := range []struct {
		name   string
		answer string
	}{
		{
			name:   "a missing fact id",
			answer: `{"f1":{"outcome":"abstain"}}`,
		},
		{
			name: "an extra fact id",
			answer: `{"f1":{"outcome":"abstain"},"f2":{"outcome":"abstain"},` +
				`"f3":{"outcome":"abstain"}}`,
		},
		{
			name:   "an outcome CV-2 does not define",
			answer: `{"f1":{"outcome":"unsure"},"f2":{"outcome":"abstain"}}`,
		},
		{
			name:   "an answer with no value",
			answer: `{"f1":{"outcome":"answer"},"f2":{"outcome":"abstain"}}`,
		},
		{
			name:   "an abstention carrying a value",
			answer: `{"f1":{"outcome":"abstain","value":"40"},"f2":{"outcome":"abstain"}}`,
		},
		{
			name: "an extra member inside a fact result",
			answer: `{"f1":{"outcome":"answer","value":"40","confidence":"high"},` +
				`"f2":{"outcome":"abstain"}}`,
		},
		{
			name:   "a fact result that is not an object",
			answer: `{"f1":"40","f2":{"outcome":"abstain"}}`,
		},
		{
			// The bar the old code actually applied: a non-empty JSON object. It
			// has to fail now, or nothing above proves the schema is what refused
			// the others.
			name:   "a generic non-empty object",
			answer: `{"anything":1}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newClaudeForkFixture(t)
			stubClaudeBinary(t, filepath.Join(t.TempDir(), "invocation.txt"), string(claudeAssayStream(
				claudeAssayInitEvent(nil, nil, nil),
				claudeAssayResultEvent(test.answer, 0, 0),
			)))
			result, err := NewClaudeHostFactory(fixture.layout).RunContextAssay(
				context.Background(),
				fixture.assaySource(t),
				ClaudeContextAssayRequest{
					BaseCtxID:    "CTX-260730-c00001",
					Prompt:       "answer the fact catalog as strict JSON",
					OutputSchema: schema,
				},
			)
			if result != nil {
				t.Fatalf("a malformed answer was returned for scoring: %s", result.Output)
			}
			assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
		})
	}

	// And the conformant answer still passes, so the gate rejects shapes rather
	// than rejecting everything.
	fixture := newClaudeForkFixture(t)
	stubClaudeBinary(t, filepath.Join(t.TempDir(), "invocation.txt"), string(claudeAssayStream(
		claudeAssayInitEvent(nil, nil, nil),
		claudeAssayResultEvent(
			`{"f1":{"outcome":"answer","value":"40"},"f2":{"outcome":"abstain"}}`, 0, 0),
	)))
	result, err := NewClaudeHostFactory(fixture.layout).RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00001",
			Prompt:       "answer the fact catalog as strict JSON",
			OutputSchema: schema,
		},
	)
	if err != nil {
		t.Fatalf("a schema-conformant answer was refused: %v", err)
	}
	if result.Attestation.SchemaEnforcement != claudeAssaySchemaEnforcement ||
		result.Attestation.SchemaDigest == "" {
		t.Fatalf("the attestation does not bind the schema it enforced: %+v", result.Attestation)
	}
}

// TestClaudeAssayRefusesASchemaItCannotEnforce is the fail-closed half. The
// adapter has no provider-side schema mechanism available to it, so a schema it
// cannot evaluate itself is a capability failure reported before the provider
// runs — not a schema quietly downgraded to a weaker check.
func TestClaudeAssayRefusesASchemaItCannotEnforce(t *testing.T) {
	for _, test := range []struct {
		name   string
		schema string
	}{
		{
			name: "a $ref this adapter does not resolve",
			schema: `{"type":"object","additionalProperties":{"$ref":"#/$defs/fact"},` +
				`"$defs":{"fact":{"type":"object"}}}`,
		},
		{name: "patternProperties", schema: `{"type":"object","patternProperties":{"^f":{}}}`},
		{name: "unevaluatedProperties", schema: `{"type":"object","unevaluatedProperties":false}`},
		{name: "contains", schema: `{"type":"array","contains":{"type":"string"}}`},
		{name: "format", schema: `{"type":"string","format":"uuid"}`},
		{name: "a pattern RE2 cannot compile", schema: `{"type":"string","pattern":"(?=x)"}`},
		{name: "a schema that is not a schema", schema: `["object"]`},
		{name: "an unknown JSON type", schema: `{"type":"fact-result"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newClaudeForkFixture(t)
			recorded := filepath.Join(t.TempDir(), "invocation.txt")
			stubClaudeBinary(t, recorded, string(claudeAssayStream(
				claudeAssayInitEvent(nil, nil, nil),
				claudeAssayResultEvent(`{"f1":{"outcome":"abstain"}}`, 0, 0),
			)))
			_, err := NewClaudeHostFactory(fixture.layout).RunContextAssay(
				context.Background(),
				fixture.assaySource(t),
				ClaudeContextAssayRequest{
					BaseCtxID:    "CTX-260730-c00001",
					Prompt:       "answer",
					OutputSchema: json.RawMessage(test.schema),
				},
			)
			assertContextErrorCode(t, err, "context_assay_isolation_unavailable")
			// Refused before the provider ran: an unenforceable schema is knowable
			// without spending a turn, and reporting it afterwards would mean a run
			// happened whose output nothing validated.
			if _, statErr := os.Stat(recorded); !os.IsNotExist(statErr) {
				t.Fatal("the assay launched the provider before refusing the schema it cannot enforce")
			}
		})
	}
}

// TestClaudeAssayRefusesABaseThatChangedWhileTheProviderRan closes the
// post-provider half of base immutability on the assay path.
//
// The pre-launch digest only proves the copy was written correctly. The provider
// is what opens the base, so the base has to be re-read after it exits — and a
// mutated base fails the assay rather than producing a score over a corrupted
// shared artifact.
func TestClaudeAssayRefusesABaseThatChangedWhileTheProviderRan(t *testing.T) {
	fixture := newClaudeForkFixture(t)
	recorded := filepath.Join(t.TempDir(), "invocation.txt")
	stubClaudeBinary(t, recorded, string(claudeAssayStream(
		claudeAssayInitEvent(nil, nil, nil),
		claudeAssayResultEvent(`{"f1":{"outcome":"abstain"}}`, 0, 0),
	)))
	// The stub appends to the base while it is the running provider, which is the
	// exact window a pre-launch check cannot see.
	t.Setenv("CLAUDE_STUB_MUTATE_PATH", fixture.basePath)

	result, err := NewClaudeHostFactory(fixture.layout).RunContextAssay(
		context.Background(),
		fixture.assaySource(t),
		ClaudeContextAssayRequest{
			BaseCtxID:    "CTX-260730-c00001",
			Prompt:       "answer",
			OutputSchema: claudeCV2Schema("f1"),
		},
	)
	if result != nil {
		t.Fatalf("an assay over a mutated base produced a score: %s", result.Output)
	}
	assertPublicErrorCode(t, err, claudeContextSourceMutated)

	// Refusal still hands every artifact to retention/GC; it does not delete.
	assertClaudeAssayArtifactsRetainedForGC(t, fixture, recorded)
}

// --- fixtures -------------------------------------------------------------

// claudeCV2Schema is the answer schema CV-2 describes, written the way a
// validator would supply it: an object keyed by exactly the requested fact ids,
// each one exactly one of {outcome: answer, value} or {outcome: abstain}.
func claudeCV2Schema(factIDs ...string) json.RawMessage {
	properties := make([]string, 0, len(factIDs))
	required := make([]string, 0, len(factIDs))
	for _, id := range factIDs {
		properties = append(properties, `"`+id+`":{"oneOf":[`+
			`{"type":"object","properties":{"outcome":{"const":"answer"},"value":{"type":"string"}},`+
			`"required":["outcome","value"],"additionalProperties":false},`+
			`{"type":"object","properties":{"outcome":{"const":"abstain"}},`+
			`"required":["outcome"],"additionalProperties":false}]}`)
		required = append(required, `"`+id+`"`)
	}
	return json.RawMessage(`{"type":"object","properties":{` +
		strings.Join(properties, ",") + `},"required":[` + strings.Join(required, ",") +
		`],"additionalProperties":false}`)
}

func claudeAssayStream(events ...string) []byte {
	if len(events) == 0 {
		return nil
	}
	return []byte(strings.Join(events, "\n") + "\n")
}

// claudeStubSessionPlaceholder is replaced by the stub with the session identity
// the adapter actually requested.
const claudeStubSessionPlaceholder = "SESSION_ID_PLACEHOLDER"

func claudeAssayInitEvent(tools, mcpServers, slashCommands []string) string {
	return claudeAssayInitEventAs(claudeStubSessionPlaceholder, tools, mcpServers, slashCommands)
}

func claudeAssayInitEventAs(
	sessionID string,
	tools, mcpServers, slashCommands []string,
) string {
	encode := func(values []string) string {
		if values == nil {
			values = []string{}
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			panic(err)
		}
		return string(encoded)
	}
	return `{"type":"system","subtype":"init",` +
		`"session_id":"` + sessionID + `",` +
		`"tools":` + encode(tools) +
		`,"mcp_servers":` + encode(mcpServers) +
		`,"slash_commands":` + encode(slashCommands) + `}`
}

// claudeAssayInitEventRaw builds an init event from explicit JSON members, so a
// test can omit or null an inventory field that the ordinary builder always
// writes as an array. That difference is invisible to a length check, which is
// exactly why it needs to be expressible here.
func claudeAssayInitEventRaw(sessionID string, members ...string) string {
	parts := append([]string{
		`"type":"system"`,
		`"subtype":"init"`,
		`"session_id":"` + sessionID + `"`,
	}, members...)
	return "{" + strings.Join(parts, ",") + "}"
}

func claudeAssayResultEvent(result string, webSearch, webFetch int) string {
	return claudeAssayResultEventAs(claudeStubSessionPlaceholder, result, webSearch, webFetch)
}

func claudeAssayResultEventAs(sessionID, result string, webSearch, webFetch int) string {
	encoded, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	return `{"type":"result","subtype":"success","is_error":false,` +
		`"session_id":"` + sessionID + `","result":` + string(encoded) +
		`,"usage":{"input_tokens":10,"cache_read_input_tokens":17536,` +
		`"cache_creation_input_tokens":25814,"output_tokens":86,` +
		`"server_tool_use":{"web_search_requests":` + itoa(webSearch) +
		`,"web_fetch_requests":` + itoa(webFetch) + `}}}`
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// assaySource is the provider handle a ready base materialization would carry.
func (f *claudeForkFixture) assaySource(t *testing.T) ProviderHostRecord {
	t.Helper()
	snapshot := claudeHostSnapshot{
		Contract:    claudeHostSnapshotContract,
		SessionUUID: f.baseUUID,
		PID:         4242,
		Plan: LaunchSpec{Cwd: f.baseDir, Executable: "claude", Claude: &ClaudePreparedLaunch{Contract: ClaudeSessionLaunchContract, Plan: PrimarySessionLaunchPlan{
			Contract: PrimarySessionLaunchPlanContract, SchemaVersion: 1, Status: "ok", Provider: "claude", ProjectDir: f.baseDir, Executable: "claude",
		}}},
	}
	return ProviderHostRecord{
		Provider: "claude",
		Snapshot: RunnerSnapshot{Kind: "claude", Claude: &snapshot},
	}
}

// claudeStubForkMode selects what transcript the stub provider leaves behind, so
// the adapter's lineage evidence can be exercised against a provider that forked
// and against ones that did not.
type claudeStubForkMode string

const (
	// claudeStubForkInherit is what a real fork does, probe-verified on claude
	// 2.1.220: the child is written at the requested --session-id beside the
	// materialized copy, every record is restamped with the new session identity,
	// and the inherited records keep their own uuid values.
	claudeStubForkInherit claudeStubForkMode = "inherit"
	// claudeStubForkCold models the failure the lineage check exists for: the
	// provider ignores --resume --fork-session, starts fresh under the requested
	// --session-id, and answers cleanly. Every other signal — reported identity,
	// empty tool inventory, untouched fork sources — looks exactly like success.
	claudeStubForkCold claudeStubForkMode = "cold"
	// claudeStubForkNone writes no child at all, which is what a provider that
	// resolved a different project directory looks like from here.
	claudeStubForkNone claudeStubForkMode = "none"
	// claudeStubForkMixedSession inherits the records but stamps a second session
	// identity into the child, so the transcript cannot be read as the lineage
	// evidence for one answering session.
	claudeStubForkMixedSession claudeStubForkMode = "mixed-session"
)

// stubClaudeBinary installs a `claude` on PATH that records how it was invoked,
// leaves behind the transcript a forked provider would, and replays a fixed
// stream-json response.
//
// The assay's contract is about what the launch enforces and what the adapter
// reads back, and both are observable without spending a model turn. The real
// binary is exercised by the opt-in live smoke test instead.
//
// The stub writes a child transcript because the adapter now requires positive
// lineage evidence. A stub that only echoed an event stream could not distinguish
// a fork from a cold start, which is exactly the hole the previous revision's
// happy-path regression left open: it asserted an attestation over a provider that
// never inherited anything.
//
// CLAUDE_STUB_MUTATE_PATH makes the stub append to a named transcript while it is
// the running provider. That models the one window a pre-launch digest cannot
// cover: a provider that resolved its resume target to the shared base and
// appended into it.
//
// CLAUDE_STUB_FORK_MODE selects the child-transcript behaviour above.
func stubClaudeBinary(t *testing.T, recordPath, response string) {
	t.Helper()
	// The stub echoes back the session identity it was asked for, because a real
	// provider does: the adapter binds its attestation to the requested fork, so a
	// stub with a hardcoded identity would only ever exercise the mismatch path.
	script := "#!/bin/sh\n" +
		"cwd=$(pwd -P)\n" +
		"{\n" +
		"  echo \"cwd=$cwd\"\n" +
		"  echo \"argv=$*\"\n" +
		"} > " + shellQuote(recordPath) + "\n" +
		"if [ -n \"${CLAUDE_STUB_MUTATE_PATH:-}\" ]; then\n" +
		"  printf '{\"type\":\"user\",\"uuid\":\"stub-leak\"}\\n' >> \"$CLAUDE_STUB_MUTATE_PATH\"\n" +
		"fi\n" +
		"sid=''\nresume=''\nprev=''\n" +
		"for arg in \"$@\"; do\n" +
		"  if [ \"$prev\" = '--session-id' ]; then sid=\"$arg\"; fi\n" +
		"  if [ \"$prev\" = '--resume' ]; then resume=\"$arg\"; fi\n" +
		"  prev=\"$arg\"\n" +
		"done\n" +
		// The child goes beside the transcript the resume selector names, which is
		// where the real provider puts it. The directory is discovered rather than
		// derived on purpose: re-deriving it here would reimplement the slug rule —
		// including its truncate-and-hash branch, which these long temp paths hit — and
		// a stub that agreed with the adapter by construction could not catch the
		// adapter looking in the wrong place.
		"copy=$(ls \"$CLAUDE_CONFIG_DIR\"/projects/*/\"$resume.jsonl\" 2>/dev/null | head -1)\n" +
		"projects=$(dirname \"$copy\" 2>/dev/null)\n" +
		"child=\"$projects/$sid.jsonl\"\n" +
		// A file in the project directory that the adapter never named, which is what
		// an unrelated occupant of a colliding slug — or a provider that wrote under an
		// identity of its own choosing — looks like from the retirement's side.
		"if [ -n \"${CLAUDE_STUB_EXTRA_PROJECT_FILE:-}\" ] && [ -n \"$projects\" ]; then\n" +
		"  printf 'not this assay\\n' > \"$projects/$CLAUDE_STUB_EXTRA_PROJECT_FILE\"\n" +
		"fi\n" +
		"mode=\"${CLAUDE_STUB_FORK_MODE:-inherit}\"\n" +
		"if [ -n \"$sid\" ] && [ -n \"$copy\" ] && [ \"$mode\" != 'none' ]; then\n" +
		"  case \"$mode\" in\n" +
		"    cold)\n" +
		"      printf '{\"type\":\"user\",\"uuid\":\"stub-cold-prompt\",\"sessionId\":\"%s\"}\\n' \"$sid\" > \"$child\"\n" +
		"      printf '{\"type\":\"assistant\",\"uuid\":\"stub-cold-reply\",\"sessionId\":\"%s\",\"message\":{\"role\":\"assistant\",\"stop_reason\":\"end_turn\"}}\\n' \"$sid\" >> \"$child\"\n" +
		"      ;;\n" +
		"    *)\n" +
		"      if [ -f \"$copy\" ]; then\n" +
		"        sed \"s/$resume/$sid/g\" \"$copy\" > \"$child\"\n" +
		"        if [ \"$mode\" = 'mixed-session' ]; then\n" +
		"          printf '{\"type\":\"user\",\"uuid\":\"stub-foreign\",\"sessionId\":\"%s\"}\\n' \"$resume\" >> \"$child\"\n" +
		"        fi\n" +
		"        printf '{\"type\":\"assistant\",\"uuid\":\"stub-child-reply\",\"sessionId\":\"%s\",\"message\":{\"role\":\"assistant\",\"stop_reason\":\"end_turn\"}}\\n' \"$sid\" >> \"$child\"\n" +
		"      fi\n" +
		"      ;;\n" +
		"  esac\n" +
		"fi\n" +
		"cat <<'CLAUDE_STUB_EOF' | sed \"s/" + claudeStubSessionPlaceholder + "/$sid/g\"\n" +
		response + "\nCLAUDE_STUB_EOF\n"
	path := claudeWriteExecutable(t, "claude", []byte(script), 0o700)
	t.Setenv("PATH", filepath.Dir(path)+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// claudeStubField reads one recorded key=value line from the stub's log.
func claudeStubField(t *testing.T, recorded, prefix string) string {
	t.Helper()
	for _, line := range strings.Split(recorded, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	t.Fatalf("the recorded invocation has no %q line:\n%s", prefix, recorded)
	return ""
}

// TestParseClaudeAssayStreamRefusesUnmodelledTopLevelEvents is the B1 regression.
//
// The parser used to classify tool activity only inside message content and then
// switch on system/init and result, ignoring every other top-level line. That
// made an event shape the adapter does not model indistinguishable from no event
// at all: a stream with a valid empty inventory, a successful result, and an
// unmodelled event between them still carried ToolEvents == 0 and could be
// attested as zero-tool. An unread event is not evidence that nothing happened
// in it, so every unmodelled shape now fails closed.
func TestParseClaudeAssayStreamRefusesUnmodelledTopLevelEvents(t *testing.T) {
	const session = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
	for _, test := range []struct {
		name  string
		event string
	}{
		{
			// The shape the verdict named: a tool announced as its own top-level
			// event rather than as a message block.
			name:  "a top-level tool event",
			event: `{"type":"tool_use","name":"Read","session_id":"` + session + `"}`,
		},
		{
			// Protocol drift inside a type the adapter does model. The provider
			// defines dozens of system subtypes that carry activity with no message
			// content, so a system event that is not init is unobservable here.
			name:  "an unmodelled system subtype",
			event: `{"type":"system","subtype":"hook_response","session_id":"` + session + `"}`,
		},
		{
			name:  "an unknown top-level type",
			event: `{"type":"some_future_event","session_id":"` + session + `"}`,
		},
		{
			// A terminal outcome the adapter does not model must not be read through
			// is_error as a clean success.
			name: "an unmodelled result subtype",
			event: `{"type":"result","subtype":"error_future_state","is_error":false,` +
				`"session_id":"` + session + `","result":"{}"}`,
		},
		{
			// A turn is accepted regardless of subtype only because its content is
			// classified exhaustively. Without content there is nothing to accept.
			name:  "a turn event carrying no message",
			event: `{"type":"assistant","session_id":"` + session + `"}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The surrounding stream is otherwise perfect: an empty inventory and a
			// successful result under one identity. Only the injected event differs,
			// so nothing but the unmodelled shape can explain the refusal.
			observation, err := parseClaudeAssayStream(claudeAssayStream(
				claudeAssayInitEventAs(session, nil, nil, nil),
				test.event,
				claudeAssayResultEventAs(session, `{"f1":{"outcome":"answer","value":"40"}}`, 0, 0),
			))
			if err == nil {
				t.Fatalf("an unmodelled event was read as a zero-tool stream: %+v", observation)
			}
			// The refusal must not leave a usable observation behind, because a
			// caller that ignored the error would otherwise still see SawResult and
			// ToolEvents == 0 — the exact false zero this closes.
			if observation.SawResult || observation.ToolEvents != 0 ||
				observation.SessionUUID != "" {
				t.Fatalf("a refused stream still produced usable evidence: %+v", observation)
			}
		})
	}
}

// TestParseClaudeAssayStreamAcceptsTheProviderSInertEvents is the other half of
// B1, and it is not a formality.
//
// Failing closed on unknown shapes is only correct if the accepted set is the one
// the supported provider actually emits. An allowlist derived from reasoning
// rather than from the provider refuses genuine runs, and this test exists
// because that is not hypothetical: the first version of this fix was written
// against a probe that happened not to emit system/thinking_tokens, and the live
// smoke test against claude 2.1.220 rejected a perfectly isolated assay. Both
// events pinned here — rate_limit_event and system/thinking_tokens — were then
// observed on every genuine run of the deny policy.
func TestParseClaudeAssayStreamAcceptsTheProviderSInertEvents(t *testing.T) {
	const session = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
	observation, err := parseClaudeAssayStream(claudeAssayStream(
		claudeAssayInitEventAs(session, nil, nil, nil),
		// Verbatim payload shape from claude 2.1.220: token telemetry only, with
		// no field able to describe an action.
		`{"type":"system","subtype":"thinking_tokens","estimated_tokens":32,`+
			`"estimated_tokens_delta":27,"session_id":"`+session+`"}`,
		`{"type":"rate_limit_event","session_id":"`+session+`",`+
			`"rate_limit_info":{"status":"allowed"}}`,
		`{"type":"assistant","session_id":"`+session+`",`+
			`"message":{"role":"assistant","content":[{"type":"text","text":"{}"}]}}`,
		claudeAssayResultEventAs(session, `{"f1":{"outcome":"answer","value":"40"}}`, 0, 0),
	))
	if err != nil {
		t.Fatalf("the provider's own event stream was refused as unmodelled: %v", err)
	}
	if !observation.SawInit || !observation.SawResult {
		t.Fatalf("a genuine stream was not read as complete: %+v", observation)
	}
	if observation.ToolEvents != 0 || observation.UnreadableBlocks != 0 {
		t.Fatalf("an inert event was counted as activity: %+v", observation)
	}
	if err := requireClaudeAssayIsolation("CTX-260730-c00010", observation); err != nil {
		t.Fatalf("a genuine isolated run was refused: %v", err)
	}
}

// TestClaudeAssayCountsDeniedToolCallsAsToolEvidence covers the provider's own
// record of tools it refused.
//
// A denial means the turn reached for a tool. No tool executed, so the deny
// policy held — but the answer came from a session that tried to consult
// something the base did not carry, which is not the clean inheritance test the
// assay exists to run. It is counted as tool evidence rather than as proof the
// policy worked.
func TestClaudeAssayCountsDeniedToolCallsAsToolEvidence(t *testing.T) {
	const session = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
	observation, err := parseClaudeAssayStream(claudeAssayStream(
		claudeAssayInitEventAs(session, nil, nil, nil),
		`{"type":"result","subtype":"success","is_error":false,`+
			`"session_id":"`+session+`","result":"{}",`+
			`"permission_denials":[{"tool_name":"Read"}],`+
			`"usage":{"server_tool_use":{"web_search_requests":0,"web_fetch_requests":0}}}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if observation.PermissionDenials != 1 {
		t.Fatalf("the provider's denial record was not read: %+v", observation)
	}
	err = requireClaudeAssayIsolation("CTX-260730-c00011", observation)
	assertContextErrorCode(t, err, "context_assay_tool_use_detected")
}

// TestClaudeAssayClaimProjectDirEstablishesOwnershipAtomically is the B2
// regression.
//
// Ownership used to be decided by asking os.Lstat whether the directory existed
// and creating it afterwards. Those are two operations, and between them a
// colliding or concurrent occupant can create the directory: the assay would
// still record SlugDirOwned, and the retirement would then treat every
// non-transcript entry as its own provider state and recursively remove it.
//
// The claim is now the creation, so the window does not exist. A directory that
// was already there is somebody else's, whether it appeared a week ago or a
// microsecond before the call.
func legacyClaudeAssayClaimProjectDirEstablishesOwnershipAtomically(t *testing.T) {
	root := t.TempDir()

	t.Run("an occupant that got there first keeps its directory", func(t *testing.T) {
		slugDir := filepath.Join(root, "-Users-someone-contended")
		// The occupant wins the race: it creates the directory, and its state is
		// not a transcript, so the old ownership rule would have deleted it.
		if err := os.MkdirAll(filepath.Join(slugDir, "memory"), 0o700); err != nil {
			t.Fatal(err)
		}
		foreign := filepath.Join(slugDir, "memory", "notes.md")
		if err := os.WriteFile(foreign, []byte("someone else's state\n"), 0o600); err != nil {
			t.Fatal(err)
		}

		owned, claimedRoot, err := claudeAssayClaimProjectDir(slugDir)
		if err != nil {
			t.Fatalf("claiming a contended project directory failed outright: %v", err)
		}
		if owned || claimedRoot != nil {
			t.Fatal("a directory this assay did not create was claimed as assay-owned")
		}

		// The retirement is driven by that decision, so the occupant survives the
		// whole cycle rather than merely surviving the claim.
		if err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
			SlugDir:      slugDir,
			SlugDirOwned: owned,
			SlugDirRoot:  claimedRoot,
		}); err != nil {
			t.Fatalf("retiring a run that owns no project directory failed: %v", err)
		}
		if _, err := os.Stat(foreign); err != nil {
			t.Fatalf("a foreign occupant's state was deleted: %v", err)
		}
	})

	t.Run("exactly one concurrent claim wins", func(t *testing.T) {
		slugDir := filepath.Join(root, "-Users-someone-concurrent")
		const claimants = 8
		type claim struct {
			owned bool
			err   error
		}
		results := make(chan claim, claimants)
		start := make(chan struct{})
		for i := 0; i < claimants; i++ {
			go func() {
				<-start
				owned, _, err := claudeAssayClaimProjectDir(slugDir)
				results <- claim{owned: owned, err: err}
			}()
		}
		close(start)
		owners := 0
		for i := 0; i < claimants; i++ {
			got := <-results
			if got.err != nil {
				t.Errorf("a concurrent claim failed: %v", got.err)
			}
			if got.owned {
				owners++
			}
		}
		// More than one owner means two runs would each recursively delete the
		// same directory believing it was theirs alone.
		if owners != 1 {
			t.Fatalf("%d concurrent claimants believed they owned the directory, want 1", owners)
		}
	})
}

// TestRetireClaudeAssayArtifactsRefusesADirectoryThatIsNoLongerTheOneItCreated
// closes the remainder of the ownership question.
//
// Claiming a directory atomically proves this assay created the directory. It
// does not prove the path still leads to it: between the claim and the
// retirement the directory can be removed and something else can take the name.
// Ownership is of an object, not of a string, so the retirement re-identifies the
// path before the recursive delete.
func legacyRetireClaudeAssayArtifactsRefusesADirectoryThatIsNoLongerTheOneItCreated(
	t *testing.T,
) {
	root := t.TempDir()
	slugDir := filepath.Join(root, "-Users-someone-replaced")

	owned, claimedRoot, err := claudeAssayClaimProjectDir(slugDir)
	if err != nil || !owned {
		t.Fatalf("claiming a fresh project directory failed: owned=%v err=%v", owned, err)
	}

	// The claimed directory goes away and a different one takes its path.
	if err := os.RemoveAll(slugDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(slugDir, 0o700); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(slugDir, "someone-elses-state.txt")
	if err := os.WriteFile(foreign, []byte("not ours\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	err = retireClaudeAssayArtifacts(&claudeAssayArtifacts{
		SlugDir:      slugDir,
		SlugDirOwned: true,
		SlugDirRoot:  claimedRoot,
	})
	if err == nil {
		t.Fatal("a replaced project directory was deleted as if this assay had created it")
	}
	if _, statErr := os.Stat(foreign); statErr != nil {
		t.Fatalf("the replacement's state was deleted: %v", statErr)
	}
}

// claudeAssayReplaceSlugDir performs the directory-replacement attack: the owned
// directory is renamed away and a foreign, non-empty directory takes its path. It
// returns the sentinel path that must survive.
//
// Renaming rather than deleting is deliberate — it is the cheap way to rebind a
// pathname, and it leaves the original object alive so a test can also check what
// happened to it.
func claudeAssayReplaceSlugDir(t *testing.T, slugDir string) string {
	t.Helper()
	moved := slugDir + ".moved-away"
	if err := os.Rename(slugDir, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(slugDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(slugDir, "foreign-state")
	if err := os.WriteFile(sentinel, []byte("somebody else's data\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return sentinel
}

// TestUnlinkOwnedClaudeAssayDirEntryLeavesAReplacementIntact is the verdict-06 B2
// regression, taken at the exact instant the finding is about.
//
// The old retirement called Lstat/SameFile, then ReadDir, then a path-based
// os.RemoveAll. The identity check was true when it ran, and the recursive delete
// still resolved the pathname afterwards — so a directory swapped in between the
// two was destroyed with everything in it. The delivered replacement test moved
// the directory before the function started, which proved the identity check
// worked but never covered replacement between the check and the destructive use.
//
// This test constructs precisely that state deterministically: the adversarial
// replacement is already at the pathname when the destructive call is entered.
// There is no window to lose the race in, because the race is the precondition.
//
// Two properties are asserted. The sentinel survives, and it survives for a
// structural reason rather than a lucky one: the final step is a plain remove of
// an entry this assay had already emptied, and rmdir cannot delete a non-empty
// directory. The worst outcome the remaining window can produce is an unlinked
// empty directory, never lost data.
func legacyUnlinkOwnedClaudeAssayDirEntryLeavesAReplacementIntact(t *testing.T) {
	root := t.TempDir()
	slugDir := filepath.Join(root, "-Users-someone-swapped")
	slugRoot := claudeAssayOwnedSlugDir(t, slugDir)
	owned, err := slugRoot.Stat(".")
	if err != nil {
		t.Fatal(err)
	}

	// The state at the destructive call: this assay's object still open and already
	// emptied, a foreign non-empty directory holding its pathname.
	sentinel := claudeAssayReplaceSlugDir(t, slugDir)

	err = unlinkOwnedClaudeAssayDirEntry(slugDir, owned)
	// The data-loss claim is checked first, because it is the finding: under the old
	// path-based RemoveAll this deletion succeeded and took the replacement with it.
	if _, statErr := os.Stat(sentinel); statErr != nil {
		t.Fatalf(
			"replacement holding the pathname at the destructive call was deleted (retire err=%v): %v",
			err, statErr,
		)
	}
	if err == nil {
		t.Fatal("unlinking a rebound pathname reported success")
	}
	// And the refusal says why, so an operator is not left guessing whether the
	// assay's own fork leaked.
	if !strings.Contains(err.Error(), "belongs to somebody else") {
		t.Fatalf("the refusal does not explain that the path was rebound: %v", err)
	}
}

// TestRemoveOwnedClaudeAssayDirIsBoundToTheObjectNotThePath is the other half of
// the same fix: the inspection and every removal inside the directory go through
// the handle taken at creation, so they cannot be redirected by the pathname at
// all.
//
// The arrangement is the reviewer's: the owned directory is renamed away and a
// foreign non-empty directory takes its name. Under the old path-based
// implementation the enumeration would have described the replacement's contents
// and the recursive delete would have destroyed them. Here the function operates
// on the object it created — the provider state inside it is removed, the
// replacement is untouched, and the failure to unlink the name is reported.
func legacyRemoveOwnedClaudeAssayDirIsBoundToTheObjectNotThePath(t *testing.T) {
	root := t.TempDir()
	slugDir := filepath.Join(root, "-Users-someone-rebound")
	slugRoot := claudeAssayOwnedSlugDir(t, slugDir)
	// Provider state of the kind claude 2.1.220 writes beside the transcripts.
	if err := os.MkdirAll(filepath.Join(slugDir, "memory"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(slugDir, "memory", "notes.md"), []byte("assay state\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}

	sentinel := claudeAssayReplaceSlugDir(t, slugDir)
	moved := slugDir + ".moved-away"

	err := removeOwnedClaudeAssayDir(slugDir, slugRoot)
	if err == nil {
		t.Fatal("a rebound project directory pathname was retired as if it were still ours")
	}
	if _, statErr := os.Stat(sentinel); statErr != nil {
		t.Fatalf("the replacement's data was deleted (retire err=%v): %v", err, statErr)
	}
	// The handle governed: this assay's own provider state was removed from the
	// object it created, wherever that object now lives.
	if _, statErr := os.Stat(filepath.Join(moved, "memory")); !os.IsNotExist(statErr) {
		t.Fatalf("the assay's own provider state was not removed through its handle: %v", statErr)
	}
}

// TestRemoveOwnedClaudeAssayDirAcceptsADirectoryAlreadyGone keeps the object-bound
// rewrite from turning a benign outcome into a withheld answer.
//
// The handle keeps this assay's directory readable after it has been unlinked, so
// "already gone" no longer shows up as a missing path during the inspection — it
// shows up at the unlink, or as a parent that no longer exists. Both mean the
// directory holds no name any more, which is what retiring it was for, and neither
// is a leak to refuse over.
func legacyRemoveOwnedClaudeAssayDirAcceptsADirectoryAlreadyGone(t *testing.T) {
	t.Run("the directory was unlinked by somebody else", func(t *testing.T) {
		root := t.TempDir()
		slugDir := filepath.Join(root, "-Users-someone-vanished")
		slugRoot := claudeAssayOwnedSlugDir(t, slugDir)
		if err := os.RemoveAll(slugDir); err != nil {
			t.Fatal(err)
		}
		if err := removeOwnedClaudeAssayDir(slugDir, slugRoot); err != nil {
			t.Fatalf("a directory that was already gone was reported as a retirement failure: %v", err)
		}
	})

	t.Run("the whole projects root was removed", func(t *testing.T) {
		root := t.TempDir()
		projects := filepath.Join(root, "projects")
		slugDir := filepath.Join(projects, "-Users-someone-vanished")
		slugRoot := claudeAssayOwnedSlugDir(t, slugDir)
		if err := os.RemoveAll(projects); err != nil {
			t.Fatal(err)
		}
		if err := removeOwnedClaudeAssayDir(slugDir, slugRoot); err != nil {
			t.Fatalf("a vanished projects root was reported as a retirement failure: %v", err)
		}
	})
}

// TestRetireClaudeAssayArtifactsSurvivesReplacementRacesUnderContention covers the
// interleavings a single deterministic arrangement cannot reach.
//
// The deterministic tests above pin the two states that matter. This one attacks
// the whole retirement while it runs: a writer keeps rebinding the pathname to a
// non-empty foreign directory for the entire duration, so replacement lands at
// every point in the sequence across the rounds — before the inspection, between
// the inspection and the removals, and between the removals and the unlink.
//
// The invariant is not "retirement succeeds". It is that no round ever destroys
// data this assay did not write. A refusal is an acceptable outcome; a missing
// sentinel is not.
func legacyRetireClaudeAssayArtifactsSurvivesReplacementRacesUnderContention(t *testing.T) {
	root := t.TempDir()
	for round := 0; round < 64; round++ {
		slugDir := filepath.Join(root, fmt.Sprintf("-Users-someone-race-%02d", round))
		owned, slugRoot, err := claudeAssayClaimProjectDir(slugDir)
		if err != nil || !owned {
			t.Fatalf("round %d: claiming a fresh project directory failed: %v", round, err)
		}
		materialized := filepath.Join(slugDir, "materialized.jsonl")
		if err := os.WriteFile(materialized, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(slugDir, "memory"), 0o700); err != nil {
			t.Fatal(err)
		}

		// The adversary rebinds the pathname to a directory holding data of its own,
		// concurrently with the retirement.
		swapped := make(chan string, 1)
		go func() {
			moved := slugDir + ".moved"
			if err := os.Rename(slugDir, moved); err != nil {
				swapped <- ""
				return
			}
			if err := os.Mkdir(slugDir, 0o700); err != nil {
				swapped <- ""
				return
			}
			sentinel := filepath.Join(slugDir, "foreign-state")
			if err := os.WriteFile(sentinel, []byte("keep me\n"), 0o600); err != nil {
				swapped <- ""
				return
			}
			swapped <- sentinel
		}()

		retireErr := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
			SlugDir:          slugDir,
			SlugDirOwned:     owned,
			SlugDirRoot:      slugRoot,
			MaterializedPath: materialized,
		})
		sentinel := <-swapped
		if sentinel == "" {
			// The swap lost to the retirement outright; nothing foreign existed to
			// protect in this round.
			continue
		}
		if _, statErr := os.Stat(sentinel); statErr != nil {
			t.Fatalf(
				"round %d: a foreign directory's data was destroyed during retirement "+
					"(retire err=%v): %v",
				round, retireErr, statErr,
			)
		}
	}
}

// TestRetireClaudeAssayArtifactsRefusesOwnershipItCannotVerify pins the
// conservative branch of the claim.
//
// If the directory was created but could not be identified afterwards, the claim
// declines ownership rather than asserting it unprovably. Should a caller record
// ownership without an identity anyway, the retirement refuses instead of
// deleting: an unverifiable recursive delete is the failure this whole path
// exists to prevent.
func legacyRetireClaudeAssayArtifactsRefusesOwnershipItCannotVerify(t *testing.T) {
	root := t.TempDir()
	slugDir := filepath.Join(root, "-Users-someone-unverified")
	if err := os.MkdirAll(slugDir, 0o700); err != nil {
		t.Fatal(err)
	}

	err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
		SlugDir:      slugDir,
		SlugDirOwned: true,
	})
	if err == nil {
		t.Fatal("an unverifiable ownership claim was acted on")
	}
	if _, statErr := os.Stat(slugDir); statErr != nil {
		t.Fatalf("a directory whose ownership could not be verified was deleted: %v", statErr)
	}
}

// claudeAssayOwnedWorkingDir creates a disposable working directory the way the
// assay does — a unique name plus a handle taken on the object at creation — and
// returns the handle.
func claudeAssayOwnedWorkingDir(t *testing.T, dir string) *os.Root {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root
}

// TestRemoveOwnedClaudeAssayTreeIsBoundToTheObjectNotThePath is the review-07 B3
// regression.
//
// Cycle-6 made the project-directory cleanup object-bound but left the working
// directory on a path-based os.RemoveAll, justified by "this adapter created it
// under a unique name, so everything under it belongs to this run". That
// justification does not survive the rename: MkdirTemp establishes ownership of
// the object it created, not permanent ownership of that object's mutable name.
// The reviewer renamed the owned object away, put a non-empty foreign directory
// at the vacated path, and the cleanup deleted the foreign contents and returned
// nil.
//
// The arrangement here is the reviewer's, and the assertions are the two halves
// of the fix: foreign data at the rebound path survives, and this assay's own
// contents are still removed from the object wherever it now lives.
func legacyRemoveOwnedClaudeAssayTreeIsBoundToTheObjectNotThePath(t *testing.T) {
	root := t.TempDir()
	workingDir := filepath.Join(root, "assay-rebound")
	workingRoot := claudeAssayOwnedWorkingDir(t, workingDir)
	// State of the kind the provider leaves in its cwd.
	if err := os.MkdirAll(filepath.Join(workingDir, "scratch"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(workingDir, "scratch", "notes.md"), []byte("assay state\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}

	sentinel := claudeAssayReplaceSlugDir(t, workingDir)
	moved := workingDir + ".moved-away"

	err := removeOwnedClaudeAssayTree(workingDir, workingRoot)
	if err == nil {
		t.Fatal("a rebound working-directory pathname was retired as if it were still ours")
	}
	// The precise loss the reviewer reproduced.
	if _, statErr := os.Stat(sentinel); statErr != nil {
		t.Fatalf(
			"the replacement at the working-directory path was deleted (cleanup err=%v): %v",
			err, statErr,
		)
	}
	// The handle governed: this assay's own contents went with the object it created.
	if _, statErr := os.Stat(filepath.Join(moved, "scratch")); !os.IsNotExist(statErr) {
		t.Fatalf("the assay's own working state was not removed through its handle: %v", statErr)
	}
}

// TestRetireClaudeAssayArtifactsLeavesAReplacedWorkingDirectoryIntact takes the
// same finding through the retirement the assay actually calls, so the object
// binding is a property of the cleanup path rather than of one helper.
func legacyRetireClaudeAssayArtifactsLeavesAReplacedWorkingDirectoryIntact(t *testing.T) {
	root := t.TempDir()
	workingDir := filepath.Join(root, "assay-retired")
	workingRoot := claudeAssayOwnedWorkingDir(t, workingDir)
	sentinel := claudeAssayReplaceSlugDir(t, workingDir)

	err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
		WorkingDir:     workingDir,
		WorkingDirRoot: workingRoot,
	})
	if err == nil {
		t.Fatal("retirement reported success over a rebound working-directory pathname")
	}
	if _, statErr := os.Stat(sentinel); statErr != nil {
		t.Fatalf("retirement destroyed a foreign directory's data (retire err=%v): %v", err, statErr)
	}
	if !strings.Contains(err.Error(), "belongs to somebody else") {
		t.Fatalf("the refusal does not explain that the path was rebound: %v", err)
	}
}

// TestRemoveOwnedClaudeAssayTreeRefusesOwnershipItCannotVerify keeps the working
// directory from falling back to a path-based delete when no handle is held.
//
// Without a handle there is nothing to prove a removal would land on the object
// this assay created, and the whole point of the fix is that the pathname is not
// that proof. The retirement refuses — and the caller withholds the answer over a
// leak — rather than recursively deleting whatever now answers to the name.
func legacyRemoveOwnedClaudeAssayTreeRefusesOwnershipItCannotVerify(t *testing.T) {
	root := t.TempDir()
	workingDir := filepath.Join(root, "assay-unverified")
	if err := os.MkdirAll(workingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	occupant := filepath.Join(workingDir, "state")
	if err := os.WriteFile(occupant, []byte("data\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{WorkingDir: workingDir})
	if err == nil {
		t.Fatal("a working directory with no ownership handle was deleted anyway")
	}
	if _, statErr := os.Stat(occupant); statErr != nil {
		t.Fatalf("a directory whose ownership could not be verified was deleted: %v", statErr)
	}
}

// TestRemoveOwnedClaudeAssayTreeRetiresAnOrdinaryWorkingDirectory is the ordinary
// path: nothing adversarial happened, so the directory and everything the run put
// in it are gone and the retirement reports success.
func legacyRemoveOwnedClaudeAssayTreeRetiresAnOrdinaryWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	workingDir := filepath.Join(root, "assay-ordinary")
	workingRoot := claudeAssayOwnedWorkingDir(t, workingDir)
	if err := os.MkdirAll(filepath.Join(workingDir, "nested", "deeper"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(workingDir, "nested", "deeper", "scratch.txt"), []byte("x\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}

	if err := retireClaudeAssayArtifacts(&claudeAssayArtifacts{
		WorkingDir:     workingDir,
		WorkingDirRoot: workingRoot,
	}); err != nil {
		t.Fatalf("an ordinary working directory was not retired: %v", err)
	}
	if _, statErr := os.Stat(workingDir); !os.IsNotExist(statErr) {
		t.Fatalf("the working directory survived the assay: %v", statErr)
	}
}
