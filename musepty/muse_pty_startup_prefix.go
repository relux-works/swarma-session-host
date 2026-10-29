package musepty

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// L3G structural startup-prefix grammar for supported startup frames.
//
// The witness answers one question about raw session-log bytes: does the
// bounded startup prefix open a session structurally? It takes no release
// input: a release string never chooses grammar, so a newer Muse build
// with covered structure witnesses exactly like 1.4.1. Within the window,
// required members refuse typed when missing or malformed and every other
// member is additive and ignored; unknown record kinds refuse typed. Lines
// past the window are not the witness's domain: the full fold owns them,
// so post-startup drift stays a runtime verdict, never a startup verdict.
//
// The witness runs first in readMuseLog. It never admits what the fold
// refuses: every witness refusal is also a fold refusal, with a message
// that keeps the legacy refusal substring so existing string assertions
// keep matching. What it adds over the legacy verdict is the type and
// the code: the recovery composition (L2C/L3P) routes on codes, not on
// prose. A log with fewer than three valid complete records is
// incomplete, not admitted: fragment folding stays the legacy fold's
// domain, and the host admits startup only after the full window
// witnesses. An invalid record refuses typed however short the log.

// Muse startup-prefix refusal codes. Incomplete is not a failure: the
// caller polls (startup) or folds (single fragment) instead of refusing.
const (
	// MuseStartupPrefixIncomplete marks a log with no complete prefix
	// window yet: empty, partial tail, or under three complete records.
	MuseStartupPrefixIncomplete = "muse_startup_prefix_incomplete"
	// MuseStartupPrefixUnknownRecord marks an unknown record kind
	// inside the prefix window.
	MuseStartupPrefixUnknownRecord = "muse_startup_prefix_unknown_record"
	// MuseStartupPrefixMissingRequired marks a required prefix member
	// that is absent.
	MuseStartupPrefixMissingRequired = "muse_startup_prefix_missing_required"
	// MuseStartupPrefixMalformedRequired marks a required prefix member
	// that is present but ill-formed.
	MuseStartupPrefixMalformedRequired = "muse_startup_prefix_malformed_required"
)

// MuseStartupPrefixError is the typed startup-prefix verdict. Line is the
// 1-based prefix line, or 0 when the log has no complete prefix line.
type MuseStartupPrefixError struct {
	Code string
	Line int
	Msg  string
}

func (e *MuseStartupPrefixError) Error() string {
	return "muse startup prefix [" + e.Code + "]: " + e.Msg
}

// museStartupPrefixWindow bounds the witness: the first three complete
// records must all be structurally valid before startup admission. No
// admission order among kinds is imposed here beyond the window itself:
// record order and epoch semantics stay the fold's domain, and the
// captured route-before-opened order witnesses exactly like the peer's
// opened-before-route order. Admission happens only after the third.
const museStartupPrefixWindow = 3

// ValidateMuseStartupPrefix witnesses the startup prefix of raw
// session-log bytes. It returns nil only after three complete,
// structurally validated records; fewer valid records return the
// incomplete code, and any invalid record in the window refuses typed.
// Every required member and supported variant is judged; only members
// undeclared by every variant are additive and ignored. Production call
// site: readMuseLog, ahead of the full fold.
func ValidateMuseStartupPrefix(log []byte) error {
	if len(log) == 0 || log[len(log)-1] != '\n' {
		return &MuseStartupPrefixError{Code: MuseStartupPrefixIncomplete, Msg: "log has no complete tail"}
	}
	lines := bytes.Split(log, []byte{'\n'})
	lines = lines[:len(lines)-1]
	checked := len(lines)
	if checked > museStartupPrefixWindow {
		checked = museStartupPrefixWindow
	}
	for i := 0; i < checked; i++ {
		if err := museValidatePrefixRecord(lines[i], i+1); err != nil {
			return err
		}
	}
	if len(lines) < museStartupPrefixWindow {
		return &MuseStartupPrefixError{Code: MuseStartupPrefixIncomplete, Line: len(lines), Msg: fmt.Sprintf("startup prefix needs %d complete records", museStartupPrefixWindow)}
	}
	return nil
}

// museValidatePrefixRecord judges one window record: the kind must be
// known, every required member and supported variant must validate, and
// an opened or route record must carry its process/session binding. The
// refusal is always typed: unknown kind, missing required, or malformed
// required, with no untyped fallthrough.
func museValidatePrefixRecord(line []byte, num int) error {
	record, err := decodeMuseLogRecord(line)
	if err == nil {
		return museCheckPrefixBindings(record.known, num)
	}
	var shape *museRecordShapeError
	if !errors.As(err, &shape) {
		return &MuseStartupPrefixError{Code: MuseStartupPrefixMalformedRequired, Line: num, Msg: fmt.Sprintf("startup prefix line %d is not a valid record: %v", num, err)}
	}
	key, code, detail := shape.Key, shape.Code, shape.Detail
	if code == MuseStartupPrefixUnknownRecord {
		return &MuseStartupPrefixError{Code: code, Line: num, Msg: fmt.Sprintf("unknown Muse record shape %q at startup prefix line %d", key, num)}
	}
	if code == MuseStartupPrefixMissingRequired {
		return &MuseStartupPrefixError{Code: code, Line: num, Msg: fmt.Sprintf("unknown Muse record shape %q at startup prefix line %d: missing required member %q", key, num, detail)}
	}
	return &MuseStartupPrefixError{Code: code, Line: num, Msg: fmt.Sprintf("unknown Muse record shape %q at startup prefix line %d: malformed required member %q", key, num, detail)}
}

// museCheckPrefixBindings judges the process/session bindings an opened
// or route record owes beyond its grammar shape. Any other known kind
// passes: its grammar match is the whole prefix contract.
func museCheckPrefixBindings(record map[string]any, num int) error {
	switch typ, _ := record["payload_type"].(string); typ {
	case "session.opened.observed":
		return museCheckStartupPrefixOpened(record, num)
	case "runtime.session.route_facts":
		return museCheckStartupPrefixRoute(record, num)
	default:
		return nil
	}
}

// musePrefixMismatchCode classifies a record no variant admits. A variant
// whose present members are all sound but whose required members are
// absent is a missing-required refusal; anything present-but-ill-formed,
// including a known member no admitting variant declares, is malformed.
func musePrefixMismatchCode(shapes []museShape, value any) (string, string) {
	firstMalformed := ""
	for _, shape := range shapes {
		missing, mismatched := museShapeMissingMismatch(shape, value, "")
		violated, vpath := museCandidateKnownViolation(shapes, shape, value, nil)
		if len(missing) == 0 && len(mismatched) == 0 && !violated {
			continue
		}
		if len(missing) > 0 && len(mismatched) == 0 && !violated {
			return MuseStartupPrefixMissingRequired, missing[0]
		}
		if firstMalformed == "" {
			switch {
			case len(mismatched) > 0:
				firstMalformed = mismatched[0]
			case violated:
				firstMalformed = vpath
			case len(missing) > 0:
				firstMalformed = missing[0]
			}
		}
	}
	if firstMalformed == "" {
		firstMalformed = "record"
	}
	return MuseStartupPrefixMalformedRequired, firstMalformed
}

// museShapeMissingMismatch separates absent required members from
// present-but-ill-formed ones under one variant. Paths are dotted from
// the record root; array elements append [i]. Extra value members are
// not judged here: the known gate owns them.
func museShapeMissingMismatch(shape museShape, value any, path string) (missing, mismatched []string) {
	switch shape.Type {
	case "object":
		m, ok := value.(map[string]any)
		if !ok {
			return nil, []string{pathOrRecord(path)}
		}
		for k, field := range shape.Fields {
			sub := k
			if path != "" {
				sub = path + "." + k
			}
			v, exists := m[k]
			if !exists {
				missing = append(missing, sub)
				continue
			}
			subMissing, subMismatched := museShapeMissingMismatch(field, v, sub)
			missing = append(missing, subMissing...)
			mismatched = append(mismatched, subMismatched...)
		}
		return missing, mismatched
	case "array":
		arr, ok := value.([]any)
		if !ok {
			return nil, []string{pathOrRecord(path)}
		}
		for i, elem := range arr {
			matched := false
			for _, item := range shape.Items {
				if museShapeMatches(item, elem) {
					matched = true
					break
				}
			}
			if !matched {
				mismatched = append(mismatched, fmt.Sprintf("%s[%d]", pathOrRecord(path), i))
			}
		}
		return missing, mismatched
	default:
		if !museShapeMatches(shape, value) {
			return nil, []string{pathOrRecord(path)}
		}
		return nil, nil
	}
}

func pathOrRecord(path string) string {
	if path == "" {
		return "record"
	}
	return path
}

// museStartupPrefixKindKnown reports whether a record key names a grammar
// the host knows, retained frames included. A release string plays no
// part: only structure admits.
func museStartupPrefixKindKnown(key string) bool {
	return len(museGrammar[key]) > 0
}

// museCheckStartupPrefixOpened requires the opened session to be bound:
// its session identity must be present, well-formed, and equal to the
// stream it opens.
func museCheckStartupPrefixOpened(record map[string]any, num int) error {
	streamID := museStartupPrefixStreamID(record)
	sessionID, sessionPresent := museStartupPrefixSessionID(record)
	// The opened session must name its stream.
	if !sessionPresent {
		return &MuseStartupPrefixError{Code: MuseStartupPrefixMissingRequired, Line: num, Msg: fmt.Sprintf("unknown Muse record shape for session.opened.observed at startup prefix line %d: missing required member \"payload.record.session_id\"", num)}
	}
	if !museWellFormedUUID(sessionID) || sessionID != streamID {
		return &MuseStartupPrefixError{Code: MuseStartupPrefixMalformedRequired, Line: num, Msg: fmt.Sprintf("unknown Muse record shape for session.opened.observed at startup prefix line %d: member \"payload.record.session_id\" must be the stream identity", num)}
	}
	return nil
}

// museCheckStartupPrefixRoute requires the route to bind a live process:
// its process identity must be present and positive.
func museCheckStartupPrefixRoute(record map[string]any, num int) error {
	pid, pidPresent := museStartupPrefixPID(record)
	if !pidPresent {
		return &MuseStartupPrefixError{Code: MuseStartupPrefixMissingRequired, Line: num, Msg: fmt.Sprintf("unknown Muse record shape for runtime.session.route_facts at startup prefix line %d: missing required member \"payload.record.pid\"", num)}
	}
	// A route without a positive process identity binds nothing.
	if pid <= 0 {
		return &MuseStartupPrefixError{Code: MuseStartupPrefixMalformedRequired, Line: num, Msg: fmt.Sprintf("unknown Muse record shape for runtime.session.route_facts at startup prefix line %d: member \"payload.record.pid\" must be positive", num)}
	}
	return nil
}

// museStartupPrefixStreamID reads the envelope stream identity
// leniently: "" when unreachable or malformed. The binding check, not
// the navigation, refuses.
func museStartupPrefixStreamID(record map[string]any) string {
	stream, _ := record["stream"].(map[string]any)
	id, _ := stream["id"].(string)
	return id
}

// museStartupPrefixSessionID reads the opened session identity
// leniently: present reports the leaf's presence, whatever its shape.
func museStartupPrefixSessionID(record map[string]any) (string, bool) {
	payload, _ := record["payload"].(map[string]any)
	nested, _ := payload["record"].(map[string]any)
	raw, present := nested["session_id"]
	id, _ := raw.(string)
	return id, present
}

// museStartupPrefixPID reads the route process identity leniently:
// present reports the leaf's presence; the value is -1 unless the leaf
// decodes as an integer.
func museStartupPrefixPID(record map[string]any) (int64, bool) {
	payload, _ := record["payload"].(map[string]any)
	nested, _ := payload["record"].(map[string]any)
	raw, present := nested["pid"]
	if !present {
		return -1, false
	}
	n, ok := raw.(json.Number)
	if !ok {
		return -1, true
	}
	v, err := n.Int64()
	if err != nil {
		return -1, true
	}
	return v, true
}
