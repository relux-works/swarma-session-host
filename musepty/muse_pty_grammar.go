package musepty

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

// This table is derived offline from the pinned Echo corpus. Every declared
// object field is required; alternatives represent separately captured
// shapes. Members undeclared by every alternative are additive and
// tolerated (L3G required versus additive, regression): a newer
// build's extra diagnostic members never break parsing, while a missing
// required member, a wrong variant, and an unknown record kind still
// refuse. A member declared by any alternative is known: when present it
// must match a variant that declares it (L3G-003), so a malformed known
// field cannot hide behind a subset alternative that omits it. An empty
// captured array accepts only an empty array. No arbitrary JSON leaf
// escape hatch exists at the kind level. Tests bind both directions of
// table completeness to captures, rather than learning grammar from
// input.
//
//go:embed muse_pty_grammar.json
var museGrammarBytes []byte

type museShape struct {
	// Closed pins captured envelopes and newly admitted record shapes at
	// every object boundary. Historical L3G records retain their additive
	// diagnostic-field contract.
	Closed        bool                 `json:"closed,omitempty"`
	NumberLiteral *int64               `json:"number_literal,omitempty"`
	Type          string               `json:"type"`
	Literal       *string              `json:"literal,omitempty"`
	Fields        map[string]museShape `json:"fields,omitempty"`
	Items         []museShape          `json:"items,omitempty"`
}

type museScreenRowSpec struct {
	ID      string `json:"id"`
	Class   string `json:"class"`
	Pattern string `json:"pattern"`
	Example string `json:"example"`
	Note    string `json:"note"`
}

type museScreenRowClass string

const (
	museScreenRowBusy    museScreenRowClass = "busy"
	museScreenRowContent museScreenRowClass = "content"
)

type museScreenRow struct {
	ID      string
	Class   museScreenRowClass
	Pattern *regexp.Regexp
	Example string
}

var museGrammarFile = func() struct {
	Records map[string][]museShape `json:"records"`
	Screen  []museScreenRowSpec    `json:"screen_rows"`
} {
	var file struct {
		Records map[string][]museShape `json:"records"`
		Screen  []museScreenRowSpec    `json:"screen_rows"`
	}
	if err := json.Unmarshal(museGrammarBytes, &file); err != nil {
		panic(err)
	}
	return file
}()

var museGrammar = museGrammarFile.Records

// The closed screen table: every row kind allowed between the transcript and
// the composer, built from real pinned-1.4.1 captures. A busy row refuses
// delivery; a content row is transcript. Anything else refuses.
var museScreenRows = func() []museScreenRow {
	rows := make([]museScreenRow, 0, len(museGrammarFile.Screen))
	for _, spec := range museGrammarFile.Screen {
		if spec.Class != string(museScreenRowBusy) && spec.Class != string(museScreenRowContent) {
			panic(fmt.Sprintf("unknown Muse screen row class %q", spec.Class))
		}
		rows = append(rows, museScreenRow{ID: spec.ID, Class: museScreenRowClass(spec.Class), Pattern: regexp.MustCompile(spec.Pattern), Example: spec.Example})
	}
	return rows
}()

// museClassifyScreenRow proves the kind of one untrimmed screen row. Patterns
// are column-anchored and disjoint; zero or ambiguous matches refuse the gate.
func museClassifyScreenRow(row string) (museScreenRowClass, int) {
	var class museScreenRowClass
	matches := 0
	for _, entry := range museScreenRows {
		if entry.Pattern.MatchString(row) {
			class = entry.Class
			matches++
		}
	}
	return class, matches
}

// Decode tokens ourselves: encoding/json's map decoder silently accepts a
// duplicate field, losing part of the record. Such a read is unknown state.
func decodeMuseRecord(raw []byte) (any, error) {
	// encoding/json replaces malformed UTF-8 with U+FFFD, turning a
	// corrupted record into an admitted shape. Reject the bytes first:
	// invalid UTF-8 is unknown state, never an idle fold.
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("Muse record is not valid UTF-8")
	}
	// encoding/json also replaces an unpaired surrogate escape with U+FFFD,
	// so a corrupted string still passes its shape. String values must
	// decode losslessly: validate every escape before decoding.
	if err := museStrictStringEscapes(raw); err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value func() (any, error)
	value = func() (any, error) {
		t, err := d.Token()
		if err != nil {
			return nil, err
		}
		switch t {
		case json.Delim('{'):
			m := map[string]any{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return nil, err
				}
				key, ok := k.(string)
				if !ok {
					return nil, fmt.Errorf("invalid object key")
				}
				if _, exists := m[key]; exists {
					return nil, fmt.Errorf("duplicate Muse field %q", key)
				}
				v, err := value()
				if err != nil {
					return nil, err
				}
				m[key] = v
			}
			_, err = d.Token()
			return m, err
		case json.Delim('['):
			a := []any{}
			for d.More() {
				v, err := value()
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			_, err = d.Token()
			return a, err
		default:
			return t, nil
		}
	}
	v, err := value()
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, fmt.Errorf("partially decoded Muse record: %v", err)
	}
	return v, nil
}

// Every escape inside a string literal must decode to exactly the scalar it
// names. A high surrogate decodes only with its immediately following low
// half; a lone half, a truncated or non-hex \u escape, and any other escape
// letter are unknown state, never a U+FFFD repair. Structural errors stay the
// real decoder's job: in valid JSON the string boundaries below are exact, so
// this scan can only refuse lines the decoder would misread or refuse itself.
func museStrictStringEscapes(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); {
		b := raw[i]
		if !inString {
			if b == '"' {
				inString = true
			}
			i++
			continue
		}
		switch {
		case b == '"':
			inString = false
			i++
		case b != '\\':
			i++
		case i+1 >= len(raw):
			return fmt.Errorf("Muse record ends inside an escape")
		default:
			switch e := raw[i+1]; e {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i += 2
			case 'u':
				if i+6 > len(raw) {
					return fmt.Errorf("Muse record has a truncated unicode escape")
				}
				hi := museHex4(raw[i+2 : i+6])
				if hi < 0 {
					return fmt.Errorf("Muse record has a non-hex unicode escape")
				}
				i += 6
				switch {
				case hi >= 0xD800 && hi <= 0xDBFF:
					if i+6 > len(raw) || raw[i] != '\\' || raw[i+1] != 'u' {
						return fmt.Errorf("Muse record has an unpaired surrogate escape")
					}
					lo := museHex4(raw[i+2 : i+6])
					if lo < 0 || lo < 0xDC00 || lo > 0xDFFF {
						return fmt.Errorf("Muse record has an unpaired surrogate escape")
					}
					i += 6
				case hi >= 0xDC00 && hi <= 0xDFFF:
					return fmt.Errorf("Muse record has an unpaired surrogate escape")
				}
			default:
				return fmt.Errorf("Muse record has an unknown escape")
			}
		}
	}
	if inString {
		return fmt.Errorf("Muse record has an unterminated string")
	}
	return nil
}

func museHex4(b []byte) int {
	if len(b) != 4 {
		return -1
	}
	n := 0
	for _, c := range b {
		n <<= 4
		switch {
		case c >= '0' && c <= '9':
			n |= int(c - '0')
		case c >= 'a' && c <= 'f':
			n |= int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			n |= int(c-'A') + 10
		default:
			return -1
		}
	}
	return n
}

// Captured identity formats (r14). Every joined identity field carries
// one of these types in the grammar; the fold no longer re-checks
// string well-formedness (the r13 `== ""` gates are subsumed — the
// grammar refuses every malformed value before the fold inserts
// anything). Each matcher below is a narrowing-mutant anchor: a mutant
// admits exactly one sentinel value past it.
var museUUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func museWellFormedUUID(id string) bool {
	return museUUIDPattern.MatchString(id)
}

// The 11 captured context-block identities: every diagnostic block_id
// (66/66) and every configured run_context_messages id (66/66) names
// one of these. A new block the captures never show is unknown state.
var museContextBlockIDs = map[string]bool{
	"workspace_identity": true, "session_identity": true, "sandbox_policy": true,
	"security_mode": true, "skills_catalog": true, "meta_context_note": true,
	"memory_snapshot": true, "workflow_choice": true, "workflow_cookbook": true,
	"subagent_delegation": true, "agent_definition_catalog": true, "rules_file": true,
}

func museWellFormedContextBlock(id string) bool {
	return museContextBlockIDs[id]
}

// Closed reminder-roster identities from 1.4.1 and 1.4.7; the latter
// adds goal-reminder alongside skill-reminder and verify-reminder.
var museReminderAgentIDs = map[string]bool{
	"skill-reminder": true, "verify-reminder": true, "goal-reminder": true,
}

func museWellFormedReminderAgent(id string) bool {
	return museReminderAgentIDs[id]
}

// Echo response tags retain their lowercase alnum-dash alphabet. META
// 1.4.7 adds only resp_ followed by exactly 24 lowercase hex digits;
// arbitrary underscore tags, uppercase and whitespace still refuse.
var museResponseTagPattern = regexp.MustCompile(`^([a-z][a-z0-9-]*|resp_[0-9a-f]{24})$`)

// The META tool-call identity observed in 1.4.7 is a fixed hex tag.
var museToolIdempotencyPattern = regexp.MustCompile(`^tool:call_[0-9a-f]{32}$`)

func museWellFormedResponseTag(id string) bool {
	return museResponseTagPattern.MatchString(id)
}

// Idempotency keys are the union of the historical three shapes:
// a bare UUID (6), `reminder_child:<roster agent>:<counter>` (20),
// `model:<uuid>:<uuid>` (12). The counter is integer data (cf.
// sequences and color indices); the agent is a roster member. META 1.4.7
// additionally witnesses the fixed tool:call_<32 lowercase hex> form.
func museWellFormedIdempotencyKey(key string) bool {
	if museToolIdempotencyPattern.MatchString(key) {
		return true
	}
	if museWellFormedUUID(key) {
		return true
	}
	if rest, ok := strings.CutPrefix(key, "reminder_child:"); ok {
		agent, counter, ok := strings.Cut(rest, ":")
		if !ok || strings.Contains(counter, ":") || counter == "" {
			return false
		}
		for i := 0; i < len(counter); i++ {
			if counter[i] < '0' || counter[i] > '9' {
				return false
			}
		}
		return museWellFormedReminderAgent(agent)
	}
	if rest, ok := strings.CutPrefix(key, "model:"); ok {
		first, second, ok := strings.Cut(rest, ":")
		if !ok || strings.Contains(second, ":") {
			return false
		}
		return museWellFormedUUID(first) && museWellFormedUUID(second)
	}
	return false
}

func museShapeMatches(s museShape, value any) bool {
	switch s.Type {
	case "null":
		return value == nil
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "string":
		v, ok := value.(string)
		return ok && (s.Literal == nil || v == *s.Literal)
	case "uuid":
		v, ok := value.(string)
		return ok && museWellFormedUUID(v)
	case "context_block":
		v, ok := value.(string)
		return ok && museWellFormedContextBlock(v)
	case "reminder_agent":
		v, ok := value.(string)
		return ok && museWellFormedReminderAgent(v)
	case "response_tag":
		v, ok := value.(string)
		return ok && museWellFormedResponseTag(v)
	case "idempotency_key":
		v, ok := value.(string)
		return ok && museWellFormedIdempotencyKey(v)
	case "integer":
		v, ok := value.(json.Number)
		if !ok {
			return false
		}
		_, err := v.Int64()
		return err == nil && (s.NumberLiteral == nil || v.String() == fmt.Sprint(*s.NumberLiteral))
	case "number":
		_, ok := value.(json.Number)
		return ok
	case "object":
		v, ok := value.(map[string]any)
		if !ok {
			return false
		}
		// Required versus additive (L3G): every declared member must be
		// present and match; members undeclared by this variant are
		// ignored here and judged by the known-field gate in
		// validateMuseRecord, which admits only genuinely unknown
		// additive members. Unknown record kinds still refuse in
		// validateMuseRecord: no shape matches them.
		for key, field := range s.Fields {
			x, exists := v[key]
			if !exists || !museShapeMatches(field, x) {
				return false
			}
		}
		return true
	case "array":
		v, ok := value.([]any)
		if !ok {
			return false
		}
		for _, item := range v {
			matched := false
			for _, shape := range s.Items {
				if museShapeMatches(shape, item) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
		return true
	}
	return false
}

// Only declared discriminators select a variant. An opened payload's extra
// event.kind, or a task mirror's extra payload.kind, is additive just like any
// other undeclared member; neither can select a different decoder.
var museGrammarSelectors = func() map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for key, shapes := range museGrammar {
		parts := strings.Split(key, "/")
		if len(parts) != 3 {
			continue
		}
		for _, shape := range shapes {
			payload := shape.Fields["payload"]
			if out[parts[0]] == nil {
				out[parts[0]] = map[string]bool{}
			}
			if _, declared := payload.Fields["kind"]; declared {
				out[parts[0]]["kind"] = true
			}
			if _, declared := payload.Fields["event"].Fields["kind"]; declared {
				out[parts[0]]["event/"+parts[1]] = true
			}
		}
	}
	return out
}()

func museGrammarKey(value any) string {
	r, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	p, _ := r["payload"].(map[string]any)
	typ, _ := r["payload_type"].(string)
	if marker, ok := r["retained_marker"].(string); ok && p == nil && typ == "" {
		return "@marker/" + marker
	}
	// A declared payload family owns its envelope. An extra frame marker
	// on that record cannot switch the decoder to the retained-frame arm.
	if frame, ok := r["retained_frame"].(string); ok && (p == nil || museGrammarSelectors[typ] == nil) {
		return "@frame/" + frame
	}
	e, _ := p["event"].(map[string]any)
	kind, _ := p["kind"].(string)
	event, _ := e["kind"].(string)
	if selectors, known := museGrammarSelectors[typ]; known {
		if !selectors["kind"] {
			kind = ""
		}
		if !selectors["event/"+kind] {
			event = ""
		}
	}
	return strings.Join([]string{typ, kind, event}, "/")
}

func validateMuseRecord(raw []byte) error {
	_, err := decodeMuseLogRecord(raw)
	return err
}

// museDecodedRecord is the only record passed to the fold. Payload contains
// known fields only, recursively, including objects inside arrays. No raw
// input or additive value is retained. The grammar is the per-kind allowlist;
// this struct exposes only the envelope fields used by the host.
type museDecodedRecord struct {
	Type     string `json:"payload_type"`
	ID       string `json:"id"`
	Sequence int    `json:"sequence"`
	Stream   struct {
		ID string `json:"id"`
	} `json:"stream"`
	Causation json.RawMessage `json:"causation_id"`
	Payload   json.RawMessage `json:"payload"`
	Frame     string          `json:"retained_frame"`
	Version   int             `json:"frame_schema_version"`
	Digest    string          `json:"content_sha256"`
	Children  []struct {
		Index  int    `json:"child_index"`
		Record string `json:"record_json"`
	} `json:"children"`
	Marker   string `json:"retained_marker"`
	Position struct {
		ID       string `json:"id"`
		Sequence int    `json:"sequence"`
	} `json:"position"`
	AdditiveFields int `json:"-"`
	known          map[string]any
}

type museRecordShapeError struct {
	Key, Code, Detail string
}

func (e *museRecordShapeError) Error() string {
	return fmt.Sprintf("unknown Muse record shape %q: %s %q", e.Key, e.Code, e.Detail)
}

// decodeMuseLogRecord is the sole raw-log decode boundary. Validate before
// projecting: a malformed known variant must never hide behind an alternative
// that omits the offending field. Materialize the host struct only after the
// recursive allowlist has removed every additive member.
func decodeMuseLogRecord(raw []byte) (*museDecodedRecord, error) {
	v, err := decodeMuseRecord(raw)
	if err != nil {
		return nil, err
	}
	key := museGrammarKey(v)
	if !museStartupPrefixKindKnown(key) {
		return nil, &museRecordShapeError{Key: key, Code: MuseStartupPrefixUnknownRecord}
	}
	shapes := museGrammar[key]
	for _, shape := range shapes {
		if museShapeMatches(shape, v) {
			// L3G-003: a subset variant admits only when every member
			// it omits is genuinely additive. A present member
			// declared by any sibling variant must match a variant
			// that declares it.
			if violated, _ := museCandidateKnownViolation(shapes, shape, v, nil); !violated {
				known, additive := museProjectKnown(shapes, shape, v, nil)
				encoded, err := json.Marshal(known)
				if err != nil {
					return nil, err
				}
				var record museDecodedRecord
				if err := json.Unmarshal(encoded, &record); err != nil {
					return nil, err
				}
				record.known = known.(map[string]any)
				record.AdditiveFields = additive
				return &record, nil
			}
		}
	}
	code, detail := musePrefixMismatchCode(shapes, v)
	return nil, &museRecordShapeError{Key: key, Code: code, Detail: detail}
}

// museFieldKnownAtPath reports whether any grammar alternative declares
// field at the object node reached by path from the top-level shapes.
// Path segments name object fields; "[]" steps into array elements.
// The union is over every alternative, not the candidate: a field the
// captures show anywhere at this node is known everywhere at it.
func museFieldKnownAtPath(shapes []museShape, path []string, field string) bool {
	current := shapes
	for _, seg := range path {
		var next []museShape
		if seg == "[]" {
			for _, s := range current {
				if s.Type == "array" {
					next = append(next, s.Items...)
				}
			}
		} else {
			for _, s := range current {
				if s.Type == "object" {
					if f, ok := s.Fields[seg]; ok {
						next = append(next, f)
					}
				}
			}
		}
		current = next
		if len(current) == 0 {
			return false
		}
	}
	for _, s := range current {
		if s.Type == "object" {
			if _, ok := s.Fields[field]; ok {
				return true
			}
		}
	}
	return false
}

// museCandidateKnownViolation reports whether value carries a known but
// candidate-undeclared member anywhere under candidate. Path tracks the
// object node from the top-level shapes for the known union. A violation
// returns the dotted field path of the first known member the candidate
// omits; genuinely additive members are ignored.
func museCandidateKnownViolation(shapes []museShape, candidate museShape, value any, path []string) (bool, string) {
	switch candidate.Type {
	case "object":
		m, ok := value.(map[string]any)
		if !ok {
			return false, ""
		}
		for k, v := range m {
			if fieldShape, declared := candidate.Fields[k]; declared {
				if violated, fp := museCandidateKnownViolation(shapes, fieldShape, v, append(path, k)); violated {
					return true, fp
				}
				continue
			}
			if candidate.Closed || museFieldKnownAtPath(shapes, path, k) {
				full := k
				if len(path) > 0 {
					full = strings.Join(append(append([]string(nil), path...), k), ".")
				}
				return true, full
			}
		}
		return false, ""
	case "array":
		arr, ok := value.([]any)
		if !ok {
			return false, ""
		}
		for _, elem := range arr {
			anySubset := false
			cleanFound := false
			firstViolation := ""
			for _, item := range candidate.Items {
				if !museShapeMatches(item, elem) {
					continue
				}
				anySubset = true
				if violated, fp := museCandidateKnownViolation(shapes, item, elem, append(path, "[]")); !violated {
					cleanFound = true
					break
				} else if firstViolation == "" {
					firstViolation = fp
				}
			}
			if !anySubset {
				continue
			}
			if !cleanFound {
				return true, firstViolation
			}
		}
		return false, ""
	default:
		return false, ""
	}
}

// museProjectKnown walks a validated variant. Objects lose undeclared members
// at every depth; arrays select the same exact known variant that validation
// admitted. Only the number of dropped members crosses the boundary.
func museProjectKnown(shapes []museShape, shape museShape, value any, path []string) (any, int) {
	switch shape.Type {
	case "object":
		m := value.(map[string]any)
		projected := make(map[string]any, len(shape.Fields))
		additive := len(m) - len(shape.Fields)
		for field, child := range shape.Fields {
			v, count := museProjectKnown(shapes, child, m[field], append(path, field))
			projected[field] = v
			additive += count
		}
		// No undeclared object member crosses the decode boundary.
		return projected, additive
	case "array":
		arr := value.([]any)
		projected := make([]any, len(arr))
		additive := 0
		for i, elem := range arr {
			for _, item := range shape.Items {
				if !museShapeMatches(item, elem) {
					continue
				}
				if violated, _ := museCandidateKnownViolation(shapes, item, elem, append(path, "[]")); violated {
					continue
				}
				v, count := museProjectKnown(shapes, item, elem, append(path, "[]"))
				projected[i] = v
				additive += count
				break
			}
		}
		return projected, additive
	default:
		return value, 0
	}
}
