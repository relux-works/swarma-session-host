package sessionhost

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	managerTimerBindingMismatch managerTimerCode = "timer_binding_mismatch"
	managerTimerRequestConflict managerTimerCode = "timer_request_conflict"
	managerTimerMaxU            int64            = 9007199254740991
)

var (
	managerTimerIDPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
	managerTimerUUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	managerTimerHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

const managerTimerPhases = "preparing|reserve_unknown|reserved|commit_unknown|queued|running|completed_unacked|release_unknown|effect_unknown|failed_retained|interrupted_retained"
const managerTimerRegistrationStates = "disabled|activation_unknown|active_unqualified|armed|cancelled|expired|draining|fenced"
const managerTimerOps = "reserve|commit|query|release"
const managerTimerOwnerOps = "register|activate|tick|observe|useful|ack|query|cancel"
const managerTimerSuccessCodes = "timer_registered|timer_activation_pending|timer_active_unqualified|timer_already_recorded|timer_accepted|timer_observed|timer_useful_recorded|timer_acked|timer_query|timer_cancelled"
const managerTimerRefusalCodes = "timer_not_due|timer_locked|timer_coalesced|timer_tick_stale|timer_tick_unissued|timer_request_conflict|timer_expired|timer_clock_unknown|timer_registration_invalid|timer_registration_stale|timer_binding_mismatch|timer_role_refused|timer_input_ineligible|timer_security_refused|timer_budget_refused|timer_capability_missing|timer_source_unavailable|timer_source_invalid|timer_source_too_large|timer_source_changed|timer_frame_too_large|timer_capacity|timer_queue_unknown|timer_ack_unproven|timer_ack_conflict|timer_state_invalid|timer_io_unavailable"

// Each schema is an allowlist of all required keys. Nullable fields must exist;
// they may contain null only when prefixed with ?. This language validates data,
// never authenticates a sender, evidence path or ordinary execution owner.
var managerTimerSchemas = map[string]map[string]string{
	"Binding":               {"execution_id": "ID", "thread_id": "UUID", "generation": "U+", "controller_epoch": "U+", "role": "=manager", "runtime": "=codex", "native_release": "S", "profile_id": "ID", "profile_lock": "H", "policy_digest": "H", "codex_home": "PATH", "owner_uid": "U", "goal_root": "PATH", "goal_locator": "PATH", "transport": "=native_queue", "provider_capability_digest": "H"},
	"Clock":                 {"boot_id": "ID", "observed_at": "T", "monotonic_ns": "N", "trusted": "bool"},
	"Registration":          {"registration_id": "UUID", "revision": "U+", "binding": "Binding", "cadence_seconds": "U", "created_at": "T", "expires_at": "T", "created_clock": "Clock"},
	"Tick":                  {"registration_id": "UUID", "revision": "U+", "opportunity": "U+", "boot_id": "ID", "tick_id": "S"},
	"NativeRef":             {"claim_id": "?ID", "item_id": "?ID", "turn_id": "?ID"},
	"Claim":                 {"claim_id": "ID", "created_clock": "Clock", "expires_at": "T", "expires_monotonic_ns": "N"},
	"Proof":                 {"issuer_binding_digest": "H", "source_revision": "U+", "evidence_ref": "PATH", "evidence_digest": "H"},
	"CheckEvidence":         {"attempt_id": "H", "binding_digest": "H", "goal_digest": "H", "turn_id": "ID", "checked_at": "T", "goal_digest_verified": "true", "own_clocks_checked": "true", "outcome": "enum:advanced|reconciled|no_due", "result_ref": "PATH", "result_digest": "H"},
	"Useful":                {"attempt_id": "H", "binding_digest": "H", "native": "NativeRef", "goal_digest": "H", "result_kind": "enum:advanced|reconciled|checked_no_action", "check": "CheckEvidence", "proof": "Proof"},
	"NativeQueueInvocation": {"protocol": "=host-codex-queue-invocation/1", "thread_id": "UUID", "codex_home": "PATH", "message": "MSG"},
	"Submission":            {"goal_digest": "H", "frame": "NativeQueueInvocation"},
	"ProviderOperation":     {"attempt_id": "H", "op": "enum:" + managerTimerOps},
	"ProviderCall":          {"protocol": "=" + managerTimerProtocol, "op": "enum:" + managerTimerOps, "request_id": "H", "registration": "Registration", "attempt_id": "H", "tick": "Tick", "binding_digest": "H", "request_digest": "H", "submission": "?Submission", "native": "NativeRef", "timeout_ms": "U"},
	"ProviderResult":        {"protocol": "=" + managerTimerProtocol, "op": "enum:" + managerTimerOps, "request_id": "H", "attempt_id": "H", "binding_digest": "H", "request_digest": "H", "disposition": "enum:reserved|queued|running|completed_ok|completed_failed|completed_interrupted|absent|suppressed|unknown", "native": "NativeRef", "claim": "?Claim", "observed_clock": "Clock", "reason": "enum:none|input_ineligible|security_refused|budget_refused|expired|binding_mismatch|role_refused|capability_missing|cancelled|provider_unavailable", "eligible_at_enqueue": "?bool", "eligible_at_start": "?bool", "proof": "?Proof"},
	"Attempt":               {"attempt_id": "H", "tick": "Tick", "binding_digest": "H", "locator": "?PATH", "goal_digest": "?H", "request_digest": "?H", "submission": "?Submission", "phase": "enum:" + managerTimerPhases, "resume_phase": "?enum:" + managerTimerPhases, "native": "NativeRef", "provider_evidence": "?ProviderResult", "useful": "?Useful", "ack_digest": "?H"},
	"Ledger":                {"version": "one", "execution_id": "ID", "ledger_id": "UUID", "revision_high_water": "U", "opportunity_high_water": "U", "registration": "?Registration", "registration_state": "enum:" + managerTimerRegistrationStates, "phase_revision": "U", "activation_request": "?H", "expires_boot_id": "?ID", "expires_monotonic_ns": "?N", "next_due_boot_id": "?ID", "next_due_ns": "?N", "active": "?Attempt", "terminal": "[]Terminal"},
	"Terminal":              {"attempt_id": "H", "tick": "Tick", "outcome": "enum:acked|no_effect|suppressed|failed_retained|interrupted_retained", "ack_digest": "?H"},
	"Readback":              {"registration": "?Registration", "state": "enum:" + managerTimerRegistrationStates, "source_ref": "PATH", "source_digest": "H", "installed_binary_digest": "?H", "profile_read": "?Proof", "policy_read": "?Proof", "input_owner_digest": "?H", "cron_entry_digest": "?H", "cadence_seconds": "?U", "expires_at": "?T", "active_phase": "?enum:" + managerTimerPhases, "pending_count": "U", "witness": "?Proof", "armed": "bool"},
	"Ack":                   {"attempt_id": "H", "useful_digest": "H", "completion_digest": "H"},
	"OwnerRequest":          {"protocol": "=" + managerTimerProtocol, "op": "enum:" + managerTimerOwnerOps, "request_id": "S", "registration_id": "UUID", "revision": "U+", "payload": "object"},
	"OwnerResponse":         {"protocol": "=" + managerTimerProtocol, "op": "enum:" + managerTimerOwnerOps, "request_id": "S", "ok": "bool", "code": "enum:" + managerTimerSuccessCodes + "|" + managerTimerRefusalCodes, "registration_state": "?enum:" + managerTimerRegistrationStates, "phase": "?enum:" + managerTimerPhases, "phase_revision": "?U", "attempt_id": "?H", "readback": "?Readback"},
}

func managerTimerMember(value any, values string) bool {
	s, ok := value.(string)
	if !ok {
		return false
	}
	for _, v := range strings.Split(values, "|") {
		if s == v {
			return true
		}
	}
	return false
}

func managerTimerInteger(value any) (int64, bool) {
	n, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(string(n), 10, 64)
	return i, err == nil && i >= 0 && (string(n) == strconv.FormatInt(i, 10) || n == "-0")
}

func managerTimerText(value any, limit int) bool {
	s, ok := value.(string)
	if !ok || s == "" || len(s) > limit || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

func managerTimerField(kind string, value any) error {
	if strings.HasPrefix(kind, "?") {
		if value == nil {
			return nil
		}
		return managerTimerField(kind[1:], value)
	}
	valid := false
	s, _ := value.(string)
	switch {
	case strings.HasPrefix(kind, "="):
		valid = s == kind[1:]
	case strings.HasPrefix(kind, "enum:"):
		valid = managerTimerMember(value, kind[5:])
	case strings.HasPrefix(kind, "[]"):
		list, ok := value.([]any)
		if !ok || len(list) > 32 {
			return managerTimerInvalid
		}
		for _, item := range list {
			if err := managerTimerField(kind[2:], item); err != nil {
				return err
			}
		}
		return nil
	case kind == "N" || kind == "U" || kind == "U+" || kind == "one":
		i, ok := managerTimerInteger(value)
		valid = ok && (kind == "N" || i <= managerTimerMaxU) && (kind != "U+" || i > 0) && (kind != "one" || i == 1)
	case kind == "ID":
		valid = managerTimerIDPattern.MatchString(s)
	case kind == "UUID":
		valid = managerTimerUUIDPattern.MatchString(s)
	case kind == "H":
		valid = managerTimerHashPattern.MatchString(s)
	case kind == "S":
		valid = managerTimerText(value, 128)
	case kind == "PATH":
		valid = managerTimerText(value, 4096) && path.IsAbs(s) && path.Clean(s) == s
	case kind == "MSG":
		if len(s) > 32768 {
			return managerTimerFrameTooLarge
		}
		valid = managerTimerText(value, 32768)
	case kind == "T":
		t, err := time.Parse("2006-01-02T15:04:05Z", s)
		valid = err == nil && len(s) == 20 && t.Format("2006-01-02T15:04:05Z") == s
	case kind == "bool":
		_, valid = value.(bool)
	case kind == "true":
		valid = value == true
	case kind == "object":
		_, valid = value.(map[string]any)
	default:
		o, ok := value.(map[string]any)
		if !ok {
			return managerTimerInvalid
		}
		return validateManagerTimerRecord(kind, o)
	}
	if !valid {
		return managerTimerInvalid
	}
	return nil
}

func managerTimerShape(object map[string]any, schema map[string]string) error {
	if schema == nil || len(object) != len(schema) {
		return managerTimerInvalid
	}
	for key := range object {
		if _, ok := schema[key]; !ok {
			return managerTimerInvalid
		}
	}
	for key, kind := range schema {
		value, exists := object[key]
		if !exists {
			return managerTimerInvalid
		}
		if err := managerTimerField(kind, value); err != nil {
			return err
		}
	}
	return nil
}

func timerObject(o map[string]any, key string) map[string]any {
	v, _ := o[key].(map[string]any)
	return v
}
func timerString(o map[string]any, key string) string { v, _ := o[key].(string); return v }
func timerNumber(o map[string]any, key string) int64  { v, _ := managerTimerInteger(o[key]); return v }
func timerTime(o map[string]any, key string) time.Time {
	t, _ := time.Parse("2006-01-02T15:04:05Z", timerString(o, key))
	return t
}
func timerDigest(kind string, value any) string {
	d, err := managerTimerDomainDigest(kind, value)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(d[:])
}
func timerEqual(a, b any) bool {
	x, e1 := canonicalManagerTimerValue(a)
	y, e2 := canonicalManagerTimerValue(b)
	return e1 == nil && e2 == nil && bytes.Equal(x, y)
}
func timerAttemptID(tick map[string]any) string {
	d := sha256.Sum256([]byte(managerTimerProtocol + ":attempt\n" + timerString(tick, "tick_id")))
	return hex.EncodeToString(d[:])
}
func timerAllNative(n map[string]any, claim, item, turn bool) bool {
	return (n["claim_id"] != nil) == claim && (n["item_id"] != nil) == item && (n["turn_id"] != nil) == turn
}

func validateManagerTimerRecord(kind string, o map[string]any) error {
	if err := managerTimerShape(o, managerTimerSchemas[kind]); err != nil {
		return err
	}
	switch kind {
	case "Binding":
		root, loc := timerString(o, "goal_root"), timerString(o, "goal_locator")
		if loc == root || !(root == "/" || strings.HasPrefix(loc, root+"/")) {
			return managerTimerInvalid
		}
	case "Registration":
		clock := timerObject(o, "created_clock")
		seconds := timerTime(o, "expires_at").Sub(timerTime(o, "created_at"))
		if timerNumber(o, "cadence_seconds") < 900 || timerNumber(o, "cadence_seconds") > 1800 || seconds <= 0 || seconds > 604800*time.Second || clock["trusted"] != true || clock["observed_at"] != o["created_at"] {
			return managerTimerInvalid
		}
	case "Tick":
		want := timerString(o, "registration_id") + ":" + strconv.FormatInt(timerNumber(o, "revision"), 10) + ":" + strconv.FormatInt(timerNumber(o, "opportunity"), 10)
		if o["tick_id"] != want {
			return managerTimerRequestConflict
		}
	case "NativeRef":
		if o["turn_id"] != nil && o["item_id"] == nil || o["item_id"] != nil && o["claim_id"] == nil {
			return managerTimerInvalid
		}
	case "Claim":
		c := timerObject(o, "created_clock")
		duration := timerTime(o, "expires_at").Sub(timerTime(c, "observed_at"))
		start, end := timerNumber(c, "monotonic_ns"), timerNumber(o, "expires_monotonic_ns")
		if c["trusted"] != true || duration <= 0 || duration > 30*time.Second || end <= start || end-start > int64(30*time.Second) {
			return managerTimerInvalid
		}
	case "Useful":
		n, c := timerObject(o, "native"), timerObject(o, "check")
		if timerObject(o, "proof")["issuer_binding_digest"] != o["binding_digest"] {
			return managerTimerBindingMismatch
		}
		if !timerAllNative(n, true, true, true) {
			return managerTimerInvalid
		}
		for _, key := range []string{"attempt_id", "binding_digest", "goal_digest"} {
			if o[key] != c[key] {
				return managerTimerRequestConflict
			}
		}
		outcome := timerString(o, "result_kind")
		if outcome == "checked_no_action" {
			outcome = "no_due"
		}
		if c["turn_id"] != n["turn_id"] || c["outcome"] != outcome {
			return managerTimerRequestConflict
		}
		check, err := canonicalManagerTimerValue(c)
		if err != nil {
			return managerTimerInvalid
		}
		digest := sha256.Sum256(append(check, '\n'))
		if timerObject(o, "proof")["evidence_digest"] != hex.EncodeToString(digest[:]) {
			return managerTimerRequestConflict
		}
	case "NativeQueueInvocation":
		frame, err := canonicalManagerTimerValue(o)
		if err != nil {
			return managerTimerInvalid
		}
		if len(frame)+1 > managerTimerFrameLimit {
			return managerTimerFrameTooLarge
		}
	case "ProviderCall":
		return validateManagerTimerCall(o)
	case "ProviderResult":
		return validateManagerTimerResult(o)
	case "Attempt":
		return validateManagerTimerAttempt(o)
	case "Ledger":
		return validateManagerTimerLedger(o)
	case "Terminal":
		if o["attempt_id"] != timerAttemptID(timerObject(o, "tick")) {
			return managerTimerRequestConflict
		}
		if (o["outcome"] == "acked") != (o["ack_digest"] != nil) {
			return managerTimerInvalid
		}
	case "Readback":
		return validateManagerTimerReadback(o)
	case "OwnerRequest":
		return validateManagerTimerOwnerRequest(o)
	case "OwnerResponse":
		if o["ok"] != managerTimerMember(o["code"], managerTimerSuccessCodes) {
			return managerTimerInvalid
		}
		if o["readback"] != nil && !managerTimerMember(o["op"], "query|activate") {
			return managerTimerInvalid
		}
	}
	return nil
}

func managerTimerInvocationDigest(submission map[string]any) string {
	frame, err := canonicalManagerTimerValue(submission["frame"])
	if err != nil || len(frame)+1 > managerTimerFrameLimit {
		return ""
	}
	d := sha256.Sum256(append(frame, '\n'))
	return hex.EncodeToString(d[:])
}

func managerTimerSubmissionBinding(submission, binding map[string]any, request any) error {
	frame := timerObject(submission, "frame")
	if frame["thread_id"] != binding["thread_id"] || frame["codex_home"] != binding["codex_home"] {
		return managerTimerBindingMismatch
	}
	quoted, _ := json.Marshal(binding["goal_locator"])
	want := "Advance your goals: " + string(quoted) + " sha256 " + timerString(submission, "goal_digest")
	if frame["message"] != want || request != managerTimerInvocationDigest(submission) {
		return managerTimerRequestConflict
	}
	return nil
}

func validateManagerTimerCall(o map[string]any) error {
	frame, err := canonicalManagerTimerValue(o)
	if err != nil {
		return managerTimerInvalid
	}
	if len(frame)+1 > managerTimerFrameLimit {
		return managerTimerFrameTooLarge
	}
	reg, tick := timerObject(o, "registration"), timerObject(o, "tick")
	binding := timerObject(reg, "binding")
	if o["binding_digest"] != timerDigest("Binding", binding) {
		return managerTimerBindingMismatch
	}
	if tick["registration_id"] != reg["registration_id"] || tick["revision"] != reg["revision"] || o["attempt_id"] != timerAttemptID(tick) {
		return managerTimerRequestConflict
	}
	operation := map[string]any{"attempt_id": o["attempt_id"], "op": o["op"]}
	if o["request_id"] != timerDigest("ProviderOperation", operation) {
		return managerTimerRequestConflict
	}
	if !timerAllNative(timerObject(o, "native"), false, false, false) {
		return managerTimerInvalid
	}
	op := timerString(o, "op")
	limit := int64(10000)
	if op == "query" {
		limit = 5000
	}
	if timerNumber(o, "timeout_ms") != limit {
		return managerTimerInvalid
	}
	withSubmission := op == "reserve" || op == "commit"
	if withSubmission != (o["submission"] != nil) {
		return managerTimerInvalid
	}
	if withSubmission {
		return managerTimerSubmissionBinding(timerObject(o, "submission"), binding, o["request_digest"])
	}
	return nil
}

func validateManagerTimerResult(o map[string]any) error {
	op, d := timerString(o, "op"), timerString(o, "disposition")
	allowed := map[string]string{"reserve": "reserved|absent|unknown", "commit": "queued|running|completed_ok|completed_failed|completed_interrupted|absent|suppressed|unknown", "query": "reserved|queued|running|completed_ok|completed_failed|completed_interrupted|absent|suppressed|unknown", "release": "absent|suppressed|running|completed_ok|completed_failed|completed_interrupted|unknown"}
	if !managerTimerMember(d, allowed[op]) {
		return managerTimerInvalid
	}
	if o["request_id"] != timerDigest("ProviderOperation", map[string]any{"attempt_id": o["attempt_id"], "op": o["op"]}) {
		return managerTimerRequestConflict
	}
	n := timerObject(o, "native")
	if o["claim"] != nil && timerObject(o, "claim")["claim_id"] != n["claim_id"] {
		return managerTimerRequestConflict
	}
	if d == "unknown" {
		if o["proof"] != nil || o["reason"] != "provider_unavailable" {
			return managerTimerInvalid
		}
	} else if o["proof"] == nil || timerObject(o, "observed_clock")["trusted"] != true {
		return managerTimerInvalid
	}
	if o["proof"] != nil && timerObject(o, "proof")["issuer_binding_digest"] != o["binding_digest"] {
		return managerTimerBindingMismatch
	}
	if (n["claim_id"] != nil) != (o["claim"] != nil) {
		return managerTimerInvalid
	}
	if managerTimerMember(d, "absent|suppressed") && n["turn_id"] != nil {
		return managerTimerInvalid
	}
	if managerTimerMember(d, "absent|suppressed") {
		// Closed compatibility table: a real refusal has its exact reason;
		// none is reserved for explicit cleanup, whose state Query must also
		// prove in the engine. provider_unavailable belongs only to unknown.
		if !managerTimerMember(o["reason"], "input_ineligible|security_refused|budget_refused|expired|binding_mismatch|role_refused|capability_missing|cancelled") && !(o["reason"] == "none" && managerTimerMember(op, "release|query")) {
			return managerTimerInvalid
		}
	}
	if managerTimerMember(d, "reserved|queued|running|completed_ok|completed_failed|completed_interrupted") {
		if o["reason"] != "none" || o["claim"] == nil {
			return managerTimerInvalid
		}
		if !timerAllNative(n, true, d != "reserved", d != "reserved" && d != "queued") {
			return managerTimerInvalid
		}
	}
	switch d {
	case "reserved":
		if o["eligible_at_enqueue"] != nil || o["eligible_at_start"] != nil {
			return managerTimerInvalid
		}
	case "queued":
		if o["eligible_at_enqueue"] != true || o["eligible_at_start"] != nil {
			return managerTimerInvalid
		}
	case "running", "completed_ok", "completed_failed", "completed_interrupted":
		if o["eligible_at_enqueue"] != true || o["eligible_at_start"] != true {
			return managerTimerInvalid
		}
	default:
		if o["eligible_at_enqueue"] == false || o["eligible_at_start"] != nil {
			return managerTimerInvalid
		}
	}
	return nil
}

func validateManagerTimerAttempt(o map[string]any) error {
	if o["attempt_id"] != timerAttemptID(timerObject(o, "tick")) {
		return managerTimerRequestConflict
	}
	count := 0
	for _, key := range []string{"locator", "goal_digest", "request_digest", "submission"} {
		if o[key] != nil {
			count++
		}
	}
	if count != 4 && (count != 0 || o["phase"] != "preparing") {
		return managerTimerInvalid
	}
	if count == 4 {
		sub := timerObject(o, "submission")
		if sub["goal_digest"] != o["goal_digest"] || managerTimerInvocationDigest(sub) != o["request_digest"] {
			return managerTimerRequestConflict
		}
		quoted, _ := json.Marshal(o["locator"])
		if timerObject(sub, "frame")["message"] != "Advance your goals: "+string(quoted)+" sha256 "+timerString(o, "goal_digest") {
			return managerTimerRequestConflict
		}
	}
	if o["phase"] == "release_unknown" {
		if o["resume_phase"] == nil || o["resume_phase"] == "release_unknown" {
			return managerTimerInvalid
		}
	} else if o["resume_phase"] != nil {
		return managerTimerInvalid
	}
	for _, key := range []string{"provider_evidence", "useful"} {
		if o[key] == nil {
			continue
		}
		evidence := timerObject(o, key)
		for _, field := range []string{"attempt_id", "binding_digest"} {
			if o[field] != evidence[field] {
				return managerTimerRequestConflict
			}
		}
		if key == "provider_evidence" && o["request_digest"] != evidence["request_digest"] || key == "useful" && o["goal_digest"] != evidence["goal_digest"] {
			return managerTimerRequestConflict
		}
		if !timerEqual(o["native"], evidence["native"]) {
			return managerTimerRequestConflict
		}
	}
	native := timerObject(o, "native")
	switch o["phase"] {
	case "preparing", "reserve_unknown":
		if !timerAllNative(native, false, false, false) {
			return managerTimerInvalid
		}
	case "reserved":
		if !timerAllNative(native, true, false, false) {
			return managerTimerInvalid
		}
	case "queued":
		if !timerAllNative(native, true, true, false) {
			return managerTimerInvalid
		}
	case "running", "completed_unacked", "failed_retained", "interrupted_retained":
		if !timerAllNative(native, true, true, true) {
			return managerTimerInvalid
		}
	}
	return nil
}

func validateManagerTimerLedger(o map[string]any) error {
	reg := timerObject(o, "registration")
	if reg != nil {
		if timerObject(reg, "binding")["execution_id"] != o["execution_id"] {
			return managerTimerBindingMismatch
		}
		if timerNumber(reg, "revision") > timerNumber(o, "revision_high_water") {
			return managerTimerInvalid
		}
	} else if o["active"] != nil || o["registration_state"] != "disabled" {
		return managerTimerInvalid
	}
	for _, pair := range [][2]string{{"expires_boot_id", "expires_monotonic_ns"}, {"next_due_boot_id", "next_due_ns"}} {
		if (o[pair[0]] == nil) != (o[pair[1]] == nil) {
			return managerTimerInvalid
		}
	}
	if a := timerObject(o, "active"); a != nil {
		tick, binding := timerObject(a, "tick"), timerObject(reg, "binding")
		if a["binding_digest"] != timerDigest("Binding", binding) {
			return managerTimerBindingMismatch
		}
		if tick["registration_id"] != reg["registration_id"] || tick["revision"] != reg["revision"] || timerNumber(tick, "opportunity") > timerNumber(o, "opportunity_high_water") {
			return managerTimerRequestConflict
		}
		if a["submission"] != nil {
			if err := managerTimerSubmissionBinding(timerObject(a, "submission"), binding, a["request_digest"]); err != nil {
				return err
			}
		}
	}
	seen := map[string]bool{}
	for _, item := range o["terminal"].([]any) {
		t := item.(map[string]any)
		tick := timerObject(t, "tick")
		id := timerString(t, "attempt_id")
		if seen[id] || timerNumber(tick, "revision") > timerNumber(o, "revision_high_water") || timerNumber(tick, "opportunity") > timerNumber(o, "opportunity_high_water") {
			return managerTimerInvalid
		}
		seen[id] = true
		if a := timerObject(o, "active"); a != nil && a["attempt_id"] == id {
			return managerTimerInvalid
		}
	}
	return nil
}

func validateManagerTimerReadback(o map[string]any) error {
	if timerNumber(o, "pending_count") > 1 || (o["active_phase"] != nil) != (timerNumber(o, "pending_count") == 1) {
		return managerTimerInvalid
	}
	reg := timerObject(o, "registration")
	if reg == nil {
		if o["input_owner_digest"] != nil || o["armed"] == true {
			return managerTimerBindingMismatch
		}
	} else {
		if o["input_owner_digest"] != nil && o["input_owner_digest"] != timerDigest("Binding", timerObject(reg, "binding")) {
			return managerTimerBindingMismatch
		}
		if o["cadence_seconds"] != nil && o["cadence_seconds"] != reg["cadence_seconds"] || o["expires_at"] != nil && o["expires_at"] != reg["expires_at"] {
			return managerTimerRequestConflict
		}
	}
	if o["armed"] == true {
		if o["state"] != "armed" || reg == nil {
			return managerTimerInvalid
		}
		for _, key := range []string{"installed_binary_digest", "profile_read", "policy_read", "input_owner_digest", "cron_entry_digest", "cadence_seconds", "expires_at", "witness"} {
			if o[key] == nil {
				return managerTimerInvalid
			}
		}
	}
	return nil
}

func validateManagerTimerOwnerRequest(o map[string]any) error {
	schemas := map[string]map[string]string{
		"register": {"registration": "Registration", "ledger_id": "UUID"},
		"activate": {"source": "Proof", "install": "Proof", "policy_read": "Proof", "expected_phase_revision": "U"},
		"tick":     {"tick": "Tick"}, "observe": {"result": "ProviderResult"}, "useful": {"useful": "Useful"},
		"ack": managerTimerSchemas["Ack"], "query": {"attempt_id": "?H"}, "cancel": {"reason": "=cancelled", "expected_phase_revision": "U"},
	}
	payload := timerObject(o, "payload")
	if err := managerTimerShape(payload, schemas[timerString(o, "op")]); err != nil {
		return err
	}
	if o["op"] == "register" || o["op"] == "tick" {
		key := "registration"
		if o["op"] == "tick" {
			key = "tick"
		}
		nested := timerObject(payload, key)
		if nested["registration_id"] != o["registration_id"] || nested["revision"] != o["revision"] {
			return managerTimerRequestConflict
		}
		if key == "tick" && nested["tick_id"] != o["request_id"] {
			return managerTimerRequestConflict
		}
	}
	return nil
}

// Exact durable intent comparison spans operations without treating different
// per-operation IDs as different effect opportunities. Cleanup carries no body.
func managerTimerSameIntent(call, retained map[string]any) error {
	for _, key := range []string{"registration", "attempt_id", "tick", "binding_digest", "request_digest"} {
		if !timerEqual(call[key], retained[key]) {
			return managerTimerRequestConflict
		}
	}
	if managerTimerMember(call["op"], "reserve|commit") && !timerEqual(call["submission"], retained["submission"]) {
		return managerTimerRequestConflict
	}
	return nil
}
