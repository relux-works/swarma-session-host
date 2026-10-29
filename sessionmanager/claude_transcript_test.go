package sessionmanager

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestClaudeProjectSlugReplacesEverySeparatorAndDot pins the derivation against
// directory names the installed provider actually produced.
//
// These project-slug expectations preserve the provider-observed path shape.
// Task/account/temp metadata below is fictionalized at equal byte width;
// separators and expected transformations are unchanged. The "dot + .temp" cases matter:
// "/." collapses to a doubled dash, and the accepted research probe got exactly
// that wrong and wrote into a directory the provider never reads.
func TestClaudeProjectSlugReplacesEverySeparatorAndDot(t *testing.T) {
	for _, test := range []struct {
		name string
		dir  string
		want string
	}{
		{
			name: "plain repository path",
			dir:  "/Users/sample/src/relux-works/skill-project-management",
			want: "-Users-sample-src-relux-works-skill-project-management",
		},
		{
			name: "dot directory collapses into a doubled dash",
			dir:  "/Users/sample/src/relux-works/skill-project-management/.temp/TASK-260101-aaaaaa/otherdir",
			want: "-Users-sample-src-relux-works-skill-project-management--temp-TASK-260101-aaaaaa-otherdir",
		},
		{
			name: "worktree spawn path",
			dir:  "/Users/sample/src/project/.temp/TASK-1/worktree",
			want: "-Users-sample-src-project--temp-TASK-1-worktree",
		},
		{
			name: "dots inside a path component are replaced too",
			dir:  "/tmp/my.app.v2/src",
			want: "-tmp-my-app-v2-src",
		},
		{
			name: "trailing separator is cleaned before replacement",
			dir:  "/Users/sample/src/",
			want: "-Users-sample-src",
		},
		{
			name: "root",
			dir:  "/",
			want: "-",
		},
		{
			// Two independent macOS temp paths in the local corpus carry this
			// underscore, and the provider replaced it in both.
			name: "underscore in a macOS temp path",
			dir:  "/private/var/folders/31/xxxxxxxxxxxxxxxxxxxxx_xxxxxxxx/T/run/001",
			want: "-private-var-folders-31-xxxxxxxxxxxxxxxxxxxxx-xxxxxxxx-T-run-001",
		},
		{
			// Probed directly against claude 2.1.220: every one of these characters
			// became a single dash, '-' survived, and runs were not collapsed.
			name: "every non-alphanumeric character the probe exercised",
			dir:  "/Users/a/probe/slug a b+c@d,e#f~g=h:i%j_k.l-m",
			want: "-Users-a-probe-slug-a-b-c-d-e-f-g-h-i-j-k-l-m",
		},
		{
			// Boundary probe: a 200-character slug is stored verbatim.
			name: "exactly at the truncation threshold",
			dir:  "/private/tmp/tbb200/" + strings.Repeat("k", 180),
			want: "-private-tmp-tbb200-" + strings.Repeat("k", 180),
		},
		{
			// The same probe one character longer: cut to 200, then '-' and the
			// base36 path hash the provider appended.
			name: "one past the truncation threshold",
			dir:  "/private/tmp/tbb201/" + strings.Repeat("k", 181),
			want: "-private-tmp-tbb201-" + strings.Repeat("k", 180) + "-bsfxam",
		},
		{
			// Probed long path whose hash is positive.
			name: "long path with a positive path hash",
			dir: "/private/tmp/tblong/" + strings.Repeat("a", 40) + "/" +
				strings.Repeat("b", 40) + "/" + strings.Repeat("c", 40) + "/" +
				strings.Repeat("d", 40) + "/" + strings.Repeat("e", 30),
			want: "-private-tmp-tblong-" + strings.Repeat("a", 40) + "-" +
				strings.Repeat("b", 40) + "-" + strings.Repeat("c", 40) + "-" +
				strings.Repeat("d", 40) + "-" + strings.Repeat("e", 16) + "-az8paq",
		},
		{
			// Probed long path whose 32-bit hash is negative: the suffix follows the
			// signed reading's magnitude, not the unsigned value.
			name: "long path with a negative path hash",
			dir: "/private/tmp/tbh0001/" + strings.Repeat("x", 40) + "/" +
				strings.Repeat("y", 40) + "/" + strings.Repeat("z", 40) + "/" +
				strings.Repeat("w", 40) + "/" + strings.Repeat("q", 30),
			want: "-private-tmp-tbh0001-" + strings.Repeat("x", 40) + "-" +
				strings.Repeat("y", 40) + "-" + strings.Repeat("z", 40) + "-" +
				strings.Repeat("w", 40) + "-" + strings.Repeat("q", 15) + "-oy2k61",
		},
		{
			// Probed long path whose hash renders in four base36 characters, which
			// is how the suffix was shown not to be padded.
			name: "long path with a short unpadded path hash",
			dir: "/private/tmp/tbp001502/" + strings.Repeat("x", 40) + "/" +
				strings.Repeat("y", 40) + "/" + strings.Repeat("z", 40) + "/" +
				strings.Repeat("w", 40) + "/" + strings.Repeat("q", 30),
			want: "-private-tmp-tbp001502-" + strings.Repeat("x", 40) + "-" +
				strings.Repeat("y", 40) + "-" + strings.Repeat("z", 40) + "-" +
				strings.Repeat("w", 40) + "-" + strings.Repeat("q", 13) + "-h99i",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ClaudeProjectSlug(test.dir)
			if err != nil {
				t.Fatalf("ClaudeProjectSlug(%q) returned %v", test.dir, err)
			}
			if got != test.want {
				t.Fatalf("ClaudeProjectSlug(%q) = %q, want %q", test.dir, got, test.want)
			}
			for _, r := range got {
				switch {
				case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
				default:
					t.Fatalf("slug %q still carries the unreplaced character %q", got, r)
				}
			}
		})
	}
}

func TestClaudeProjectSlugRefusesAnythingButAnAbsoluteDirectory(t *testing.T) {
	for _, dir := range []string{"", "   ", "relative/path", "./here", "../up"} {
		if _, err := ClaudeProjectSlug(dir); err == nil {
			t.Fatalf("ClaudeProjectSlug(%q) accepted a non-absolute directory", dir)
		}
	}
}

// TestVerifyClaudeSlugRuleCatchesAProviderThatDisagrees is the calibration
// check. A provider version that changed the rule must produce a refusal, not a
// transcript written where nothing will read it.
func TestVerifyClaudeSlugRuleCatchesAProviderThatDisagrees(t *testing.T) {
	recorded := "/Users/sample/src/project/.temp/TASK-1/worktree"

	agreeing := filepath.Join(
		"/home/.claude/projects",
		"-Users-sample-src-project--temp-TASK-1-worktree",
		"11111111-2222-3333-4444-555555555555.jsonl",
	)
	if err := verifyClaudeSlugRule(agreeing, recorded); err != nil {
		t.Fatalf("calibration against a matching directory failed: %v", err)
	}

	// The exact mistake from the research probe: "/." rendered as a single dash.
	disagreeing := filepath.Join(
		"/home/.claude/projects",
		"-Users-sample-src-project-temp-TASK-1-worktree",
		"11111111-2222-3333-4444-555555555555.jsonl",
	)
	err := verifyClaudeSlugRule(disagreeing, recorded)
	assertPublicErrorCode(t, err, claudeContextSlugUnverified)

	if err := verifyClaudeSlugRule(agreeing, "  "); err == nil {
		t.Fatal("a transcript with no recorded working directory was accepted as calibrated")
	}
}

// TestInspectClaudeTranscriptFindsTheTurnBoundary covers every leaf shape found
// in the local 400-transcript corpus survey: 281 end_turn, 106 stop_sequence, 10
// user leaves with no reply, 2 assistant leaves still mid-tool-use, 1 with no
// messages at all.
func TestInspectClaudeTranscriptFindsTheTurnBoundary(t *testing.T) {
	for _, test := range []struct {
		name      string
		records   []string
		completed bool
		leafType  string
	}{
		{
			name:      "assistant end_turn",
			records:   claudeCompletedTranscriptRecords("11111111-1111-1111-1111-111111111111", "/repo"),
			completed: true,
			leafType:  "assistant",
		},
		{
			name: "assistant stop_sequence",
			records: []string{
				claudeUserRecord("u1", "", "11111111-1111-1111-1111-111111111111", "/repo", "hello"),
				claudeAssistantRecord("a1", "u1", "11111111-1111-1111-1111-111111111111", "/repo", "stop_sequence"),
			},
			completed: true,
			leafType:  "assistant",
		},
		{
			name: "user leaf with no assistant reply",
			records: []string{
				claudeUserRecord("u1", "", "11111111-1111-1111-1111-111111111111", "/repo", "hello"),
			},
			completed: false,
			leafType:  "user",
		},
		{
			name: "assistant leaf still mid tool use",
			records: []string{
				claudeUserRecord("u1", "", "11111111-1111-1111-1111-111111111111", "/repo", "hello"),
				claudeAssistantRecord("a1", "u1", "11111111-1111-1111-1111-111111111111", "/repo", "tool_use"),
			},
			completed: false,
			leafType:  "assistant",
		},
		{
			name: "no message records at all",
			records: []string{
				`{"type":"queue-operation","operation":"enqueue","sessionId":"11111111-1111-1111-1111-111111111111"}`,
			},
			completed: false,
			leafType:  "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			facts, err := inspectClaudeTranscript([]byte(strings.Join(test.records, "\n") + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			if facts.CompletedTurn != test.completed {
				t.Fatalf("CompletedTurn = %v, want %v (leaf %s/%s)",
					facts.CompletedTurn, test.completed, facts.LeafType, facts.LeafStopReason)
			}
			if facts.LeafType != test.leafType {
				t.Fatalf("LeafType = %q, want %q", facts.LeafType, test.leafType)
			}
			if facts.Records != len(test.records) {
				t.Fatalf("Records = %d, want %d", facts.Records, len(test.records))
			}
		})
	}
}

// TestInspectClaudeTranscriptIgnoresSidechainLeaves proves a subagent branch
// cannot decide the main conversation's turn boundary: a completed base followed
// by an in-flight sidechain record is still completed.
func TestInspectClaudeTranscriptIgnoresSidechainLeaves(t *testing.T) {
	records := append(
		claudeCompletedTranscriptRecords("11111111-1111-1111-1111-111111111111", "/repo"),
		`{"type":"user","uuid":"s1","parentUuid":null,"isSidechain":true,`+
			`"sessionId":"11111111-1111-1111-1111-111111111111","message":{"role":"user","content":"sub"}}`,
	)
	facts, err := inspectClaudeTranscript([]byte(strings.Join(records, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !facts.CompletedTurn {
		t.Fatalf("a sidechain record changed the turn boundary: leaf %s/%s",
			facts.LeafType, facts.LeafStopReason)
	}
}

func TestInspectClaudeTranscriptFailsClosedOnAMalformedLine(t *testing.T) {
	raw := []byte(`{"type":"user","uuid":"u1"}` + "\n" + `{"type":` + "\n")
	if _, err := inspectClaudeTranscript(raw); err == nil {
		t.Fatal("a malformed transcript was inspected successfully")
	}
}

// TestRequireForkableClaudeBaseRefusesTheTwoNonSelfHealingCases is the gate that
// keeps a confidently-wrong base out of the fork pool.
func TestRequireForkableClaudeBaseRefusesTheTwoNonSelfHealingCases(t *testing.T) {
	compacted := claudeTranscriptFacts{
		CompletedTurn: true,
		Compaction: &ClaudeTranscriptCompaction{
			Provider: "claude",
			Marker:   "compactMetadata",
		},
	}
	assertContextErrorCode(
		t,
		requireForkableClaudeBase("CTX-260730-aaaaaa", compacted),
		"context_compacted",
	)

	interrupted := claudeTranscriptFacts{LeafType: "user"}
	assertPublicErrorCode(
		t, requireForkableClaudeBase("CTX-260730-aaaaaa", interrupted), claudeContextBaseNotForkable)

	if err := requireForkableClaudeBase(
		"CTX-260730-aaaaaa",
		claudeTranscriptFacts{CompletedTurn: true, LeafType: "assistant", LeafStopReason: "end_turn"},
	); err != nil {
		t.Fatalf("a completed uncompacted base was refused: %v", err)
	}
}

// TestClaudeCompactionMarkerIsReadStructurally proves the refusal follows the
// provider's own record key rather than marker-shaped text a conversation
// happened to contain.
func TestClaudeCompactionMarkerIsReadStructurally(t *testing.T) {
	sessionUUID := "11111111-1111-1111-1111-111111111111"
	mentioning := []string{
		claudeUserRecord("u1", "", sessionUUID, "/repo",
			"explain what compactMetadata means in a transcript"),
		claudeAssistantRecord("a1", "u1", sessionUUID, "/repo", "end_turn"),
	}
	facts, err := inspectClaudeTranscript([]byte(strings.Join(mentioning, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if facts.Compaction != nil {
		t.Fatal("marker-shaped message text was read as a structural compaction event")
	}

	structural := append(mentioning,
		`{"type":"system","sessionId":"`+sessionUUID+`","compactMetadata":{"trigger":"auto"}}`)
	facts, err = inspectClaudeTranscript([]byte(strings.Join(structural, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if facts.Compaction == nil {
		t.Fatal("a structural compactMetadata record was not detected")
	}
}

// TestRequireSupportedClaudeSpliceFormatGuardsOnlyTheRawPath pins the guard the
// AC requires: raw history is refused on drift while the distilled default is
// deliberately not version-gated, because it only writes a plain user record.
func TestRequireSupportedClaudeSpliceFormatGuardsOnlyTheRawPath(t *testing.T) {
	if err := requireSupportedClaudeSpliceFormat("CTX-260730-aaaaaa", "2.1.220"); err != nil {
		t.Fatalf("the verified transcript format was refused: %v", err)
	}
	for _, version := range []string{"3.0.0", "2.2.0", "1.9.9", "", "not-a-version"} {
		assertPublicErrorCode(
			t,
			requireSupportedClaudeSpliceFormat("CTX-260730-aaaaaa", version),
			claudeContextTranscriptDrift,
		)
	}
}

// TestRewriteClaudeTranscriptSessionRebindsIdentityAndKeepsUnknownFields is the
// core copy contract: exactly one field changes and nothing this adapter does
// not model is lost, because for an undocumented format that is most of it.
func TestRewriteClaudeTranscriptSessionRebindsIdentityAndKeepsUnknownFields(t *testing.T) {
	base := "11111111-1111-1111-1111-111111111111"
	target := "22222222-2222-2222-2222-222222222222"
	records := append(
		claudeCompletedTranscriptRecords(base, "/repo"),
		`{"type":"ai-title","sessionId":"`+base+`","aiTitle":"kept",`+
			`"someFutureField":{"nested":[1,2,3]}}`,
	)
	raw := []byte(strings.Join(records, "\n") + "\n")
	facts, err := inspectClaudeTranscript(raw)
	if err != nil {
		t.Fatal(err)
	}
	rewritten, err := rewriteClaudeTranscriptSession(
		raw, target, "/worktree", facts, nil, nil, claudeMaterializationTestClock)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rewritten), base) {
		t.Fatal("the rewritten transcript still references the base session identity")
	}
	after, err := inspectClaudeTranscript(rewritten)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.SessionUUIDs) != 1 || after.SessionUUIDs[0] != target {
		t.Fatalf("rewritten session identities = %v, want exactly [%s]", after.SessionUUIDs, target)
	}
	if after.Records != facts.Records {
		t.Fatalf("rewritten record count = %d, want %d", after.Records, facts.Records)
	}
	if !strings.Contains(string(rewritten), `"someFutureField"`) ||
		!strings.Contains(string(rewritten), `"aiTitle":"kept"`) {
		t.Fatal("the copy dropped fields this adapter does not model")
	}
	// cwd is provider history and is deliberately left alone on the default path:
	// only sessionId has to change for the provider to discover the copy.
	if !strings.Contains(string(rewritten), `"cwd":"/repo"`) {
		t.Fatal("the copy rewrote recorded provider history beyond the session identity")
	}
}

// TestRewriteClaudeTranscriptSessionAppendsDistilledSupplementalRecords pins the
// default composition path: one plain user record per input, chained onto the
// tail in the order the lineage declared.
func TestRewriteClaudeTranscriptSessionAppendsDistilledSupplementalRecords(t *testing.T) {
	base := "11111111-1111-1111-1111-111111111111"
	target := "22222222-2222-2222-2222-222222222222"
	raw := []byte(strings.Join(claudeCompletedTranscriptRecords(base, "/repo"), "\n") + "\n")
	facts, err := inspectClaudeTranscript(raw)
	if err != nil {
		t.Fatal(err)
	}
	inputs := []claudeCompositionInput{
		{CtxID: "CTX-260730-aaaaaa", Handoff: "first decision"},
		{CtxID: "CTX-260730-bbbbbb", Handoff: "second decision"},
	}
	uuids := []string{
		"33333333-3333-3333-3333-333333333333",
		"44444444-4444-4444-4444-444444444444",
	}
	rewritten, err := rewriteClaudeTranscriptSession(
		raw, target, "/worktree", facts, inputs, uuids, claudeMaterializationTestClock)
	if err != nil {
		t.Fatal(err)
	}
	appended := claudeDecodeRecords(t, rewritten)[facts.Records:]
	if len(appended) != 2 {
		t.Fatalf("appended %d records, want 2", len(appended))
	}
	wantParents := []string{facts.LeafUUID, uuids[0]}
	for index, record := range appended {
		if got := claudeRecordString(t, record, "type"); got != "user" {
			t.Fatalf("appended record %d type = %q, want user", index, got)
		}
		if got := claudeRecordString(t, record, "sessionId"); got != target {
			t.Fatalf("appended record %d sessionId = %q, want %q", index, got, target)
		}
		if got := claudeRecordString(t, record, "uuid"); got != uuids[index] {
			t.Fatalf("appended record %d uuid = %q, want %q", index, got, uuids[index])
		}
		if got := claudeRecordString(t, record, "parentUuid"); got != wantParents[index] {
			t.Fatalf("appended record %d parentUuid = %q, want %q", index, got, wantParents[index])
		}
		if got := claudeRecordString(t, record, "cwd"); got != "/worktree" {
			t.Fatalf("appended record %d cwd = %q, want the target directory", index, got)
		}
		if !strings.Contains(string(record["message"]), inputs[index].CtxID) {
			t.Fatalf("appended record %d does not name its lineage CTX", index)
		}
	}
}

// TestRewriteClaudeTranscriptSessionSplicesRawHistoryOntoTheTail covers the
// escape hatch: the foreign chain is re-parented with fresh identities so the
// result is one linear conversation instead of two roots the provider would
// resolve as branches.
func TestRewriteClaudeTranscriptSessionSplicesRawHistoryOntoTheTail(t *testing.T) {
	base := "11111111-1111-1111-1111-111111111111"
	other := "99999999-9999-9999-9999-999999999999"
	target := "22222222-2222-2222-2222-222222222222"
	raw := []byte(strings.Join(claudeCompletedTranscriptRecords(base, "/repo"), "\n") + "\n")
	facts, err := inspectClaudeTranscript(raw)
	if err != nil {
		t.Fatal(err)
	}
	foreign := []byte(strings.Join([]string{
		claudeUserRecord("x1", "", other, "/other", "architect question"),
		claudeAssistantRecord("x2", "x1", other, "/other", "end_turn"),
		`{"type":"ai-title","sessionId":"` + other + `","aiTitle":"dropped"}`,
	}, "\n") + "\n")
	records, err := claudeSpliceableRecords(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("spliceable records = %d, want the 2 message records only", len(records))
	}
	uuids := []string{
		"55555555-5555-5555-5555-555555555555",
		"66666666-6666-6666-6666-666666666666",
	}
	rewritten, err := rewriteClaudeTranscriptSession(
		raw, target, "/worktree", facts,
		[]claudeCompositionInput{{CtxID: "CTX-260730-cccccc", Raw: true, RawRecords: records}},
		uuids, claudeMaterializationTestClock,
	)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rewritten), other) {
		t.Fatal("the spliced chain kept the foreign session identity")
	}
	if strings.Contains(string(rewritten), `"aiTitle":"dropped"`) {
		t.Fatal("the splice carried another session's bookkeeping records")
	}
	appended := claudeDecodeRecords(t, rewritten)[facts.Records:]
	wantParents := []string{facts.LeafUUID, uuids[0]}
	for index, record := range appended {
		if got := claudeRecordString(t, record, "uuid"); got != uuids[index] {
			t.Fatalf("spliced record %d uuid = %q, want %q", index, got, uuids[index])
		}
		if got := claudeRecordString(t, record, "parentUuid"); got != wantParents[index] {
			t.Fatalf("spliced record %d parentUuid = %q, want %q", index, got, wantParents[index])
		}
		if got := claudeRecordString(t, record, "sessionId"); got != target {
			t.Fatalf("spliced record %d sessionId = %q, want %q", index, got, target)
		}
	}
	after, err := inspectClaudeTranscript(rewritten)
	if err != nil {
		t.Fatal(err)
	}
	if !after.CompletedTurn {
		t.Fatal("the spliced transcript does not end on a completed turn")
	}
}

func TestRewriteClaudeTranscriptSessionRequiresOneFreshUUIDPerAppendedRecord(t *testing.T) {
	raw := []byte(strings.Join(
		claudeCompletedTranscriptRecords("11111111-1111-1111-1111-111111111111", "/repo"), "\n") + "\n")
	facts, err := inspectClaudeTranscript(raw)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rewriteClaudeTranscriptSession(
		raw, "22222222-2222-2222-2222-222222222222", "/worktree", facts,
		[]claudeCompositionInput{{CtxID: "CTX-260730-aaaaaa", Handoff: "text"}},
		nil, claudeMaterializationTestClock,
	)
	if err == nil {
		t.Fatal("a composition with no identity for its appended record was accepted")
	}
}

// TestWriteClaudeTranscriptNeverPublishesAPartialFile covers the staging
// contract: a discoverable session file appears only when it is complete, an
// existing session is never clobbered, and a refused write reports the retained
// staging path for the private reclamation catalog.
func TestWriteClaudeTranscriptNeverPublishesAPartialFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "projects", "-repo")
	path := filepath.Join(dir, "11111111-1111-1111-1111-111111111111.jsonl")
	if staging, err := writeClaudeTranscript(path, []byte("line\n")); err != nil || staging != "" {
		t.Fatal(err)
	}
	staging, err := writeClaudeTranscript(path, []byte("other\n"))
	if err == nil {
		t.Fatal("an existing Claude session transcript was overwritten")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "line\n" {
		t.Fatalf("existing transcript content = %q, want it untouched", raw)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if staging == "" {
		t.Fatal("the refused write did not report its retained staging path")
	}
	if _, err := os.Stat(staging); err != nil {
		t.Fatalf("retained staging artifact is not available to retention/GC: %v", err)
	}
	if !strings.HasPrefix(filepath.Base(staging), ".task-board-materializing-") {
		t.Fatalf("retained staging path = %q", staging)
	}
	if len(entries) != 2 {
		t.Fatalf("project directory holds %d entries, want published plus retained staging", len(entries))
	}
}

// --- fixtures -------------------------------------------------------------

func claudeUserRecord(uuid, parent, sessionUUID, cwd, text string) string {
	parentField := "null"
	if parent != "" {
		parentField = `"` + parent + `"`
	}
	return `{"type":"user","uuid":"` + uuid + `","parentUuid":` + parentField +
		`,"isSidechain":false,"sessionId":"` + sessionUUID + `","cwd":"` + cwd +
		`","version":"2.1.220","message":{"role":"user","content":` +
		mustJSONStringLiteral(text) + `}}`
}

func claudeAssistantRecord(uuid, parent, sessionUUID, cwd, stopReason string) string {
	return `{"type":"assistant","uuid":"` + uuid + `","parentUuid":"` + parent +
		`","isSidechain":false,"sessionId":"` + sessionUUID + `","cwd":"` + cwd +
		`","version":"2.1.220","message":{"role":"assistant","stop_reason":"` + stopReason +
		`","content":[{"type":"text","text":"answer"}]}}`
}

// claudeCompletedTranscriptRecords is the minimal shape of a base that is legal
// to fork: a queue-operation, one exchange, and a terminal assistant record.
func claudeCompletedTranscriptRecords(sessionUUID, cwd string) []string {
	return []string{
		`{"type":"queue-operation","operation":"enqueue","sessionId":"` + sessionUUID +
			`","content":"hello"}`,
		claudeUserRecord("u1", "", sessionUUID, cwd, "hello"),
		claudeAssistantRecord("a1", "u1", sessionUUID, cwd, "end_turn"),
		`{"type":"last-prompt","sessionId":"` + sessionUUID + `","lastPrompt":"hello"}`,
	}
}

func mustJSONStringLiteral(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func claudeDecodeRecords(t *testing.T, raw []byte) []map[string]json.RawMessage {
	t.Helper()
	records := make([]map[string]json.RawMessage, 0, 8)
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decoding transcript line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func claudeRecordString(t *testing.T, record map[string]json.RawMessage, key string) string {
	t.Helper()
	raw, ok := record[key]
	if !ok {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return value
}

func assertPublicErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var public *PublicError
	if !errors.As(err, &public) {
		t.Fatalf("error %v is not a PublicError (want code %s)", err, code)
	}
	if public.Code != code {
		t.Fatalf("error code = %s, want %s (%s)", public.Code, code, public.Message)
	}
}

func assertContextErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	if !claudeHasContextFailure(err, code) {
		t.Fatalf("error %v is not context error %s", err, code)
	}
}
