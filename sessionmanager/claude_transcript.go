package sessionmanager

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

const (
	// claudeProjectsDirName is the per-user root Claude Code stores its session
	// transcripts under.
	claudeProjectsDirName = "projects"

	// claudeTranscriptSuffix is the on-disk extension of one session.
	claudeTranscriptSuffix = ".jsonl"

	// claudeTranscriptSizeLimit bounds one transcript read. A base context is
	// ~175 KB for a 45k-token context and the largest observed provider
	// transcripts are single-digit MB, so this is a corruption guard rather than
	// a policy: a file this size is not a conversation.
	claudeTranscriptSizeLimit = 256 << 20

	// claudeTranscriptFormatMajor/Minor pin the transcript layout this adapter
	// was verified against. The raw-splice path re-parents another session's
	// record chain and therefore depends on the undocumented parentUuid graph;
	// it is refused outside this range. Ordinary materialization touches only
	// sessionId, so it is deliberately not gated on the version.
	claudeTranscriptFormatMajor = 2
	claudeTranscriptFormatMinor = 1
)

// claudeTerminalStopReasons are the stop reasons that end a turn.
//
// Measured over 400 transcripts in the local corpus: 281 end_turn, 106
// stop_sequence, 10 leaves that are a user record with no assistant reply, and
// 2 assistant leaves still at stop_reason=tool_use. The last twelve are
// interrupted turns, which is exactly what a base must never be forked from.
var claudeTerminalStopReasons = map[string]struct{}{
	"end_turn":      {},
	"stop_sequence": {},
}

// claudeTranscriptRecord is the narrow projection of one transcript line this
// adapter reads. Every other field is preserved verbatim on copy, so an
// unrecognized record can never be silently dropped.
type claudeTranscriptRecord struct {
	Type            string          `json:"type"`
	UUID            string          `json:"uuid"`
	ParentUUID      *string         `json:"parentUuid"`
	SessionID       string          `json:"sessionId"`
	CWD             string          `json:"cwd"`
	Version         string          `json:"version"`
	IsSidechain     bool            `json:"isSidechain"`
	CompactMetadata json.RawMessage `json:"compactMetadata"`
	Message         struct {
		Role       string `json:"role"`
		StopReason string `json:"stop_reason"`
	} `json:"message"`
}

// claudeTranscriptFacts is everything this adapter knows about one base
// transcript after a single read. It holds derived evidence only: no record
// content, no message text.
type claudeTranscriptFacts struct {
	// SessionUUIDs are the distinct sessionId values present, in first-seen
	// order. A materialized transcript has exactly one.
	SessionUUIDs []string
	// RecordedCWD is the working directory the provider itself recorded. It is
	// the ground truth the slug helper is calibrated against.
	RecordedCWD string
	// FormatVersion is the provider version stamped on the records.
	FormatVersion string
	// LeafUUID is the last message-bearing record's uuid: the record a
	// supplemental injection must parent onto.
	LeafUUID string
	// RecordUUIDs are the record identities present, in order. A fork copies them
	// through unchanged while rewriting only sessionId — probe-verified on claude
	// 2.1.220 — so they are the evidence that one transcript descends from
	// another rather than merely resembling it.
	RecordUUIDs []string
	// LeafType and LeafStopReason describe the turn boundary at the tail.
	LeafType       string
	LeafStopReason string
	// CompletedTurn is true when the tail is an assistant record whose stop
	// reason is terminal.
	CompletedTurn bool
	// Compaction is the structural provider compaction marker, when present.
	Compaction *ClaudeTranscriptCompaction
	Records    int
	Bytes      int64
	SHA256     string
}

// claudeProjectSlugMaxLength is the length beyond which the provider stops using
// the plain slug and switches to a truncated form with a hash suffix.
//
// Pinned by probing the boundary directly on claude 2.1.220: a working directory
// whose slug is exactly 200 characters is stored under that slug unchanged, while
// one at 201 is stored as the first 200 characters plus '-' plus the path hash.
const claudeProjectSlugMaxLength = 200

// ClaudeProjectSlug derives the directory name Claude Code stores a working
// directory's sessions under.
//
// The rule has two parts, both established by probing the installed provider
// rather than by reading anything:
//
//  1. every character outside [A-Za-z0-9-] becomes a single '-', with runs left
//     uncollapsed;
//  2. if the result is longer than 200 characters, it is cut to 200 and a hash of
//     the absolute path is appended after a '-'.
//
// A wrong slug is the sharpest edge in this adapter because it fails silently: the
// transcript lands in a directory the provider never reads, so the context is
// absent rather than broken. The accepted research hit that with the "/." case,
// where the two replacements collapse into a doubled dash —
// ".../skill-project-management/.temp/TASK-1/worktree" becomes
// "...-skill-project-management--temp-TASK-1-worktree".
//
// Part 1 is wider than the slash-and-dot the research described, because none of
// its probe paths contained anything else. Probed with the working directory
// ".../probe/slug a b+c@d,e#f~g=h:i%j_k.l-m", the provider produced
// "...-probe-slug-a-b-c-d-e-f-g-h-i-j-k-l-m": space, '+', '@', ',', '#', '~', '=',
// ':', '%' and '_' each became one dash and '-' survived.
//
// Part 2 was found by the live smoke test, which ran in a deep temp path and got
// "No conversation found" from a transcript sitting at the plain slug. Six probed
// long paths and every one of the 37 provider-created directories in the local
// corpus are reproduced exactly by this implementation.
//
// The evidence is ASCII. A non-ASCII path is derived rather than proven, which is
// survivable because nothing trusts this function alone: a base is calibrated
// against the directory the provider itself created before anything is written,
// and a launched fork is verified to have actually resumed.
func ClaudeProjectSlug(dir string) (string, error) {
	trimmed := strings.TrimSpace(dir)
	if trimmed == "" {
		return "", fmt.Errorf("claude project slug requires a working directory")
	}
	if !filepath.IsAbs(trimmed) {
		return "", fmt.Errorf(
			"claude project slug requires an absolute working directory, got %q", dir)
	}
	cleaned := filepath.Clean(trimmed)
	units := utf16.Encode([]rune(cleaned))
	var slug strings.Builder
	slug.Grow(len(units))
	for _, unit := range units {
		switch {
		case unit >= 'a' && unit <= 'z',
			unit >= 'A' && unit <= 'Z',
			unit >= '0' && unit <= '9',
			unit == '-':
			slug.WriteByte(byte(unit))
		default:
			slug.WriteByte('-')
		}
	}
	// One replacement per UTF-16 code unit, so the derived length matches the
	// provider's own before the truncation threshold is compared.
	derived := slug.String()
	if len(derived) <= claudeProjectSlugMaxLength {
		return derived, nil
	}
	return derived[:claudeProjectSlugMaxLength] + "-" + claudeProjectPathHash(cleaned), nil
}

// claudeProjectPathHash is the suffix the provider appends to a truncated slug.
//
// It is the classic 31-multiplier string hash over the absolute path — not over
// the slug — taken as a signed 32-bit value, then rendered in base 36 from its
// absolute value. Three probed paths whose unsigned and signed renderings differ
// all matched the signed one, and a fourth whose hash renders in four characters
// showed the suffix is not padded.
func claudeProjectPathHash(path string) string {
	var hash int32
	for _, unit := range utf16.Encode([]rune(path)) {
		hash = hash*31 + int32(unit)
	}
	// int64 before the absolute value: negating the smallest int32 overflows it.
	magnitude := int64(hash)
	if magnitude < 0 {
		magnitude = -magnitude
	}
	return strconv.FormatInt(magnitude, 36)
}

// claudeProjectsRoot is the transcript store root for a provider home.
func claudeProjectsRoot(home string) string {
	return filepath.Join(home, claudeProjectsDirName)
}

// claudeResolveProjectDir puts a working directory into the form the provider
// will derive its slug from: absolute, clean, and free of symlinks.
//
// The symlink resolution is not cosmetic. The provider slugifies the working
// directory it actually resolved, so on macOS a run launched in
// "/var/folders/..." records itself under "-private-var-folders-...". A
// materialization written to the unresolved slug lands in a directory the
// provider never reads and the context is silently absent — the same failure the
// slug rule itself exists to prevent, reached by a different route. This was
// caught by the live smoke test, not by reasoning, which is why the resolution
// happens in one place that every path derivation goes through.
//
// An unresolvable directory is an error rather than a fallback: guessing the
// unresolved form is exactly the silent miss.
func claudeResolveProjectDir(dir string) (string, error) {
	trimmed := strings.TrimSpace(dir)
	if trimmed == "" {
		return "", fmt.Errorf("claude project directory is required")
	}
	if !filepath.IsAbs(trimmed) {
		return "", fmt.Errorf(
			"claude project directory must be absolute, got %q", dir)
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(trimmed))
	if err != nil {
		return "", fmt.Errorf("resolving claude project directory %q: %w", dir, err)
	}
	return resolved, nil
}

// claudeResolveProjectDirIfPresent resolves a directory that may not exist yet.
//
// A fork can be planned before its worktree is created, and the plan only needs a
// stable identity to compare against later. Every path that the provider will
// actually read is derived through claudeResolveProjectDir instead.
func claudeResolveProjectDirIfPresent(dir string) string {
	resolved, err := claudeResolveProjectDir(dir)
	if err != nil {
		return filepath.Clean(strings.TrimSpace(dir))
	}
	return resolved
}

// claudeSessionTranscriptPath is the exact file Claude reads for one session
// identity discovered from one working directory.
func claudeSessionTranscriptPath(home, projectDir, sessionUUID string) (string, error) {
	resolved, err := claudeResolveProjectDir(projectDir)
	if err != nil {
		return "", err
	}
	slug, err := ClaudeProjectSlug(resolved)
	if err != nil {
		return "", err
	}
	normalized, err := NormalizeClaudeSessionUUID(sessionUUID)
	if err != nil {
		return "", err
	}
	return filepath.Join(claudeProjectsRoot(home), slug, normalized+claudeTranscriptSuffix), nil
}

// claudeSlugCalibrationError reports that this adapter's slug derivation
// disagrees with what the provider itself did.
func claudeSlugCalibrationError(recordedCWD, expected, actual string) error {
	return &PublicError{
		Code: claudeContextSlugUnverified,
		Message: fmt.Sprintf(
			"this Claude version stores the sessions of %q under %q, but the manager derives %q; "+
				"a materialization written to the derived directory would be silently invisible to the provider, "+
				"so the fork is refused until the slug rule is corrected",
			recordedCWD, actual, expected,
		),
	}
}

// verifyClaudeSlugRule calibrates ClaudeProjectSlug against provider ground
// truth: the base transcript records the working directory it was created in,
// and the provider itself chose the directory that transcript sits in. If the
// helper cannot reproduce that name, every derived target directory is suspect.
//
// This is the check that makes a wrong slug loud. It costs one string compare
// and no provider call, and it fails before anything is written.
func verifyClaudeSlugRule(baseTranscriptPath, recordedCWD string) error {
	recorded := strings.TrimSpace(recordedCWD)
	if recorded == "" {
		return &PublicError{
			Code: claudeContextSlugUnverified,
			Message: "the base Claude transcript records no working directory, so the manager " +
				"cannot verify that it derives the same project directory the provider uses",
		}
	}
	expected, err := ClaudeProjectSlug(recorded)
	if err != nil {
		return err
	}
	actual := filepath.Base(filepath.Dir(baseTranscriptPath))
	if expected != actual {
		return claudeSlugCalibrationError(recorded, expected, actual)
	}
	return nil
}

// readClaudeTranscript reads one transcript into memory under a size bound.
//
// The bytes are read exactly once and every later verdict — digest, structure,
// compaction, the copy itself — is derived from that one snapshot, so the digest
// always identifies the bytes the decisions were made about.
func readClaudeTranscript(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("claude transcript %s is a directory", path)
	}
	if info.Size() > claudeTranscriptSizeLimit {
		return nil, fmt.Errorf(
			"claude transcript %s is %d bytes, above the %d-byte adapter limit",
			path, info.Size(), int64(claudeTranscriptSizeLimit),
		)
	}
	return os.ReadFile(path)
}

// inspectClaudeTranscript derives every structural fact this adapter gates on.
//
// A malformed line fails closed: a transcript whose structure cannot be
// established says nothing reliable about whether a turn completed or whether
// the conversation was compacted, and both of those are refusal conditions.
func inspectClaudeTranscript(raw []byte) (claudeTranscriptFacts, error) {
	digest := sha256.Sum256(raw)
	facts := claudeTranscriptFacts{
		SHA256: hex.EncodeToString(digest[:]),
		Bytes:  int64(len(raw)),
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), claudeTranscriptSizeLimit)
	seenSession := map[string]struct{}{}
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record claudeTranscriptRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return claudeTranscriptFacts{}, fmt.Errorf(
				"decode Claude transcript record %d: %w", facts.Records+1, err)
		}
		facts.Records++
		if id := strings.TrimSpace(record.UUID); id != "" {
			facts.RecordUUIDs = append(facts.RecordUUIDs, id)
		}
		if id := strings.TrimSpace(record.SessionID); id != "" {
			if _, ok := seenSession[id]; !ok {
				seenSession[id] = struct{}{}
				facts.SessionUUIDs = append(facts.SessionUUIDs, id)
			}
		}
		if facts.RecordedCWD == "" && strings.TrimSpace(record.CWD) != "" {
			facts.RecordedCWD = strings.TrimSpace(record.CWD)
		}
		if facts.FormatVersion == "" && strings.TrimSpace(record.Version) != "" {
			facts.FormatVersion = strings.TrimSpace(record.Version)
		}
		if len(record.CompactMetadata) > 0 && facts.Compaction == nil {
			facts.Compaction = &ClaudeTranscriptCompaction{
				Provider: "claude",
				Marker:   "compactMetadata",
			}
		}
		// Sidechain records belong to a subagent branch and never define the
		// main conversation's turn boundary.
		if record.IsSidechain {
			continue
		}
		switch record.Type {
		case "user", "assistant", "attachment":
			if strings.TrimSpace(record.UUID) != "" {
				facts.LeafUUID = strings.TrimSpace(record.UUID)
			}
		}
		if record.Type == "user" || record.Type == "assistant" {
			facts.LeafType = record.Type
			facts.LeafStopReason = strings.TrimSpace(record.Message.StopReason)
		}
	}
	if err := scanner.Err(); err != nil {
		return claudeTranscriptFacts{}, fmt.Errorf("reading Claude transcript: %w", err)
	}
	_, terminal := claudeTerminalStopReasons[facts.LeafStopReason]
	facts.CompletedTurn = facts.LeafType == "assistant" && terminal
	return facts, nil
}

// inspectClaudeTranscriptFile is the read-and-inspect pair used by every gate.
func inspectClaudeTranscriptFile(path string) ([]byte, claudeTranscriptFacts, error) {
	raw, err := readClaudeTranscript(path)
	if err != nil {
		return nil, claudeTranscriptFacts{}, err
	}
	facts, err := inspectClaudeTranscript(raw)
	if err != nil {
		return nil, claudeTranscriptFacts{}, err
	}
	return raw, facts, nil
}

// requireForkableClaudeBase is the pre-materialization gate over a base.
//
// Both refusals exist because the value of a fork is that the child does not
// re-explore, which also means it does not self-correct. An interrupted base
// hands the child a half-finished turn as if it were knowledge, and a compacted
// base hands it a ~15k-token summary while the registry still describes the
// full conversation. Neither is recoverable by the consumer, so neither is
// allowed to become a fork source.
func requireForkableClaudeBase(ctxID string, facts claudeTranscriptFacts) error {
	if facts.Compaction != nil {
		return &ClaudeContextFailure{
			Code: "context_compacted",
			Message: fmt.Sprintf(
				"Claude context %s carries a %s compaction marker; a compacted transcript has lost the "+
					"evidence behind everything it claims, so it is quarantined rather than forked",
				ctxID, facts.Compaction.Marker,
			),
		}
	}
	if !facts.CompletedTurn {
		detail := "its transcript contains no assistant reply"
		if facts.LeafType != "" {
			detail = fmt.Sprintf(
				"its last record is %s with stop reason %q",
				facts.LeafType, facts.LeafStopReason,
			)
		}
		return &PublicError{
			Code: claudeContextBaseNotForkable,
			Message: fmt.Sprintf(
				"Claude context %s has not completed a turn (%s); forking an interrupted turn would inherit "+
					"it as settled knowledge, so the fork is refused before any file is written",
				ctxID, detail,
			),
		}
	}
	return nil
}

// claudeTranscriptInherits reports whether child holds every one of parent's
// record identities, in order.
//
// This is what makes fork verification positive. A fork copies the inherited
// records through with their uuid values intact and rewrites only sessionId, so
// containment is direct descent evidence. The opposite check — "the resume source
// did not change" — proves nothing on its own, because a --fork-session launch
// that silently started cold leaves the source equally untouched.
//
// Order is required as well as membership: a child that holds the same records
// shuffled is not a transcript the provider produced from this parent.
func claudeTranscriptInherits(child, parent []string) bool {
	if len(parent) == 0 {
		return false
	}
	position := 0
	for _, candidate := range child {
		if position < len(parent) && candidate == parent[position] {
			position++
		}
	}
	return position == len(parent)
}

// requireSupportedClaudeSpliceFormat gates the raw-history path.
//
// Ordinary materialization rewrites one well-known field. Raw splice re-parents
// a second session's record chain onto this one's tail, which depends on the
// undocumented parentUuid graph and on record kinds the provider may change
// without notice, so it is refused outside the layout this adapter was verified
// against.
func requireSupportedClaudeSpliceFormat(ctxID, version string) error {
	major, minor, err := parseClaudeTranscriptFormat(version)
	if err != nil {
		return &PublicError{
			Code: claudeContextTranscriptDrift,
			Message: fmt.Sprintf(
				"Claude context %s records transcript format %q, which the manager cannot compare against the "+
					"%d.%d layout the raw-history splice was verified against; use distilled supplemental "+
					"injection instead",
				ctxID, version, claudeTranscriptFormatMajor, claudeTranscriptFormatMinor,
			),
		}
	}
	if major != claudeTranscriptFormatMajor || minor != claudeTranscriptFormatMinor {
		return &PublicError{
			Code: claudeContextTranscriptDrift,
			Message: fmt.Sprintf(
				"Claude context %s records transcript format %d.%d; the raw-history splice was verified only "+
					"against %d.%d and is refused on drift, because a re-parented chain that the provider reads "+
					"differently is a silently wrong context. Use distilled supplemental injection instead",
				ctxID, major, minor, claudeTranscriptFormatMajor, claudeTranscriptFormatMinor,
			),
		}
	}
	return nil
}

func parseClaudeTranscriptFormat(version string) (int, int, error) {
	parsed, err := parseClaudeSemanticVersion(version)
	if err != nil {
		return 0, 0, err
	}
	return parsed[0], parsed[1], nil
}

// claudeSupplementalRecord is the distilled handoff appended at the tail of a
// materialized transcript.
//
// It uses only the stable core of the record shape — type, identity, parent
// link, session, timestamp, cwd and a plain-text user message — because this is
// the default composition path and must not depend on the parts of the format
// the splice guard exists to protect against.
type claudeSupplementalRecord struct {
	ParentUUID  *string `json:"parentUuid"`
	IsSidechain bool    `json:"isSidechain"`
	Type        string  `json:"type"`
	Message     struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"message"`
	UUID      string `json:"uuid"`
	Timestamp string `json:"timestamp"`
	UserType  string `json:"userType"`
	CWD       string `json:"cwd"`
	SessionID string `json:"sessionId"`
	Version   string `json:"version"`
}

func newClaudeSupplementalRecord(
	parentUUID string,
	sessionUUID string,
	cwd string,
	version string,
	uuid string,
	at time.Time,
	text string,
) claudeSupplementalRecord {
	record := claudeSupplementalRecord{
		Type:      "user",
		UUID:      uuid,
		Timestamp: at.UTC().Format("2006-01-02T15:04:05.000Z"),
		UserType:  "external",
		CWD:       cwd,
		SessionID: sessionUUID,
		Version:   version,
	}
	if trimmed := strings.TrimSpace(parentUUID); trimmed != "" {
		parent := trimmed
		record.ParentUUID = &parent
	}
	record.Message.Role = "user"
	record.Message.Content = text
	return record
}

// claudeSupplementalText renders one distilled lineage input.
//
// The CTX id is included because the board owns lineage: Claude records no
// base-to-fork edge anywhere on disk, so the only way the inherited text can be
// traced back to a registry record is for the text to say so.
func claudeSupplementalText(input claudeCompositionInput) string {
	if input.Boundary {
		// The boundary banner is composed verbatim. It already names the CTX and
		// already says what it is, and wrapping it in the supplemental preamble
		// would present the base's own coverage statement as if it were another
		// context's handoff.
		return strings.TrimSpace(input.Handoff)
	}
	return fmt.Sprintf(
		"SUPPLEMENTAL CONTEXT %s (distilled handoff inherited through the task-board context registry):\n%s",
		input.CtxID,
		strings.TrimSpace(input.Handoff),
	)
}

// claudeCompositionInput is one resolved lineage input ready to be composed onto
// a materialized transcript tail.
//
// Distilled is the default and carries text. Raw carries the source session's
// message records verbatim and is only ever populated behind the transcript
// format guard.
type claudeCompositionInput struct {
	CtxID   string
	Handoff string
	// Boundary marks the base's own coverage statement rather than an inherited
	// handoff. It is composed first and rendered verbatim.
	Boundary   bool
	Raw        bool
	RawRecords []map[string]json.RawMessage
}

// claudeSpliceableRecords extracts the message-bearing chain of a session for
// the raw-history path.
//
// Non-message records — queue-operation, ai-title, last-prompt, mode — are
// session-scoped bookkeeping about how a conversation was driven, not content
// the model saw, so splicing them would carry another session's UI state into
// this one. Sidechain records belong to a subagent branch and are dropped for the
// same reason.
func claudeSpliceableRecords(raw []byte) ([]map[string]json.RawMessage, error) {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), claudeTranscriptSizeLimit)
	records := make([]map[string]json.RawMessage, 0, 32)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var generic map[string]json.RawMessage
		if err := json.Unmarshal(line, &generic); err != nil {
			return nil, fmt.Errorf("decode Claude transcript record for splice: %w", err)
		}
		var typed claudeTranscriptRecord
		if err := json.Unmarshal(line, &typed); err != nil {
			return nil, fmt.Errorf("decode Claude transcript record for splice: %w", err)
		}
		if typed.IsSidechain || strings.TrimSpace(typed.UUID) == "" {
			continue
		}
		switch typed.Type {
		case "user", "assistant", "attachment":
			records = append(records, generic)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading Claude transcript for splice: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("Claude transcript carries no spliceable message records")
	}
	return records, nil
}

// rewriteClaudeTranscriptSession copies a transcript with every sessionId
// rebound to a new identity and the resolved lineage inputs appended.
//
// Unknown fields survive untouched: each line is decoded into a generic map,
// only the keys this adapter owns are replaced, and the line is re-encoded. A
// projection-based copy would drop whatever this adapter does not model, which
// for an undocumented format is most of it.
func rewriteClaudeTranscriptSession(
	raw []byte,
	sessionUUID string,
	targetCWD string,
	facts claudeTranscriptFacts,
	inputs []claudeCompositionInput,
	uuids []string,
	at time.Time,
) ([]byte, error) {
	needed := 0
	for _, input := range inputs {
		if input.Raw {
			needed += len(input.RawRecords)
			continue
		}
		needed++
	}
	if len(uuids) != needed {
		return nil, fmt.Errorf(
			"claude composition needs one fresh uuid per appended record, got %d for %d",
			len(uuids), needed,
		)
	}
	var out bytes.Buffer
	out.Grow(len(raw) + 1024)
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), claudeTranscriptSizeLimit)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record map[string]json.RawMessage
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("decode Claude transcript record for rewrite: %w", err)
		}
		if err := setClaudeRecordString(record, "sessionId", sessionUUID, false); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("encode Claude transcript record for rewrite: %w", err)
		}
		out.Write(encoded)
		out.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading Claude transcript for rewrite: %w", err)
	}

	parent := facts.LeafUUID
	next := 0
	for _, input := range inputs {
		if !input.Raw {
			record := newClaudeSupplementalRecord(
				parent, sessionUUID, targetCWD, facts.FormatVersion, uuids[next], at,
				claudeSupplementalText(input),
			)
			encoded, err := json.Marshal(record)
			if err != nil {
				return nil, fmt.Errorf("encode Claude supplemental record: %w", err)
			}
			out.Write(encoded)
			out.WriteByte('\n')
			parent = uuids[next]
			next++
			continue
		}
		// Raw splice: the source chain is re-parented onto this transcript's tail
		// with fresh identities, so the two histories become one linear
		// conversation instead of two roots the provider would resolve as
		// branches.
		for _, source := range input.RawRecords {
			record := make(map[string]json.RawMessage, len(source)+3)
			for key, value := range source {
				record[key] = value
			}
			if err := setClaudeRecordString(record, "sessionId", sessionUUID, true); err != nil {
				return nil, err
			}
			if err := setClaudeRecordString(record, "uuid", uuids[next], true); err != nil {
				return nil, err
			}
			if err := setClaudeRecordString(record, "parentUuid", parent, true); err != nil {
				return nil, err
			}
			if err := setClaudeRecordString(record, "cwd", targetCWD, false); err != nil {
				return nil, err
			}
			encoded, err := json.Marshal(record)
			if err != nil {
				return nil, fmt.Errorf("encode spliced Claude record: %w", err)
			}
			out.Write(encoded)
			out.WriteByte('\n')
			parent = uuids[next]
			next++
		}
	}
	return out.Bytes(), nil
}

// setClaudeRecordString replaces one string field on a generic record.
//
// create=false updates the field only when the record already has it, so a copy
// never invents a key the provider did not write for that record kind.
func setClaudeRecordString(
	record map[string]json.RawMessage,
	key string,
	value string,
	create bool,
) error {
	if _, ok := record[key]; !ok && !create {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode Claude record field %s: %w", key, err)
	}
	record[key] = encoded
	return nil
}

// claudeBytesDigest is the digest of an in-memory transcript snapshot.
func claudeBytesDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// claudeFileDigest is the digest of a transcript on disk. It is used to prove a
// base is byte-identical after a fork was taken from it.
func claudeFileDigest(path string) (string, error) {
	raw, err := readClaudeTranscript(path)
	if err != nil {
		return "", err
	}
	return claudeBytesDigest(raw), nil
}

// writeClaudeTranscript writes a materialized transcript so that a partially
// written file can never be discovered by the provider: it is staged beside the
// target and renamed into place only once complete.
//
// If publishing fails after staging begins, retainedStagingPath names the private
// artifact left for the reclamation catalog. This adapter never removes it:
// deletion requires lifecycle and lease reconciliation.
func writeClaudeTranscript(path string, raw []byte) (retainedStagingPath string, err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("creating Claude project directory: %w", err)
	}
	staged, err := os.CreateTemp(dir, ".task-board-materializing-*")
	if err != nil {
		return "", fmt.Errorf("staging Claude transcript: %w", err)
	}
	stagedPath := staged.Name()
	if err := staged.Chmod(0o600); err != nil {
		_ = staged.Close()
		return stagedPath, fmt.Errorf("securing staged Claude transcript: %w", err)
	}
	if _, err := staged.Write(raw); err != nil {
		_ = staged.Close()
		return stagedPath, fmt.Errorf("writing staged Claude transcript: %w", err)
	}
	if err := staged.Sync(); err != nil {
		_ = staged.Close()
		return stagedPath, fmt.Errorf("flushing staged Claude transcript: %w", err)
	}
	if err := staged.Close(); err != nil {
		return stagedPath, fmt.Errorf("closing staged Claude transcript: %w", err)
	}
	// O_EXCL semantics: a fresh session uuid must not collide with an existing
	// transcript, and rename would overwrite one silently.
	if _, err := os.Lstat(path); err == nil {
		return stagedPath, fmt.Errorf("Claude transcript %s already exists", filepath.Base(path))
	} else if !errors.Is(err, os.ErrNotExist) {
		return stagedPath, err
	}
	// task-board-claude-removal-guard: allow os.Rename; atomically publishes the staged transcript after the collision check without reclaiming any provider artifact.
	if err := os.Rename(stagedPath, path); err != nil {
		return stagedPath, fmt.Errorf("publishing materialized Claude transcript: %w", err)
	}
	return "", nil
}
