//go:build mutants

package musepty

import (
	"context"
	"encoding/json"
	"github.com/relux-works/swarma-session-host/musepty/internal/taggedtests"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Opt-in overlays leave the candidate untouched and execute the same production
// entry-point tests. A compilation error or an unnamed failure is not a kill.
func TestMusePTYNarrowingMutants(t *testing.T) {
	if os.Getenv("TASK_BOARD_MUSE_MUTANTS") != "1" {
		t.Skip("opt-in behavioral mutation run")
	}
	pin, err := taggedtests.Load(filepath.Join("testdata", "tagged-only-tests.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := taggedtests.CheckExpiry(pin, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	for _, m := range []struct{ id, file, old, replacement, test string }{

		{"drain-other-item-only", "muse_pty_log.go", `item, pending := f.Queued[e.ItemID]`, `if e.ItemID == "02e9046e-9853-4225-9990-66468f634af8" && len(f.Queued) == 1 { for id := range f.Queued { e.ItemID = id } }; item, pending := f.Queued[e.ItemID]`, "TestMusePTYInboxCorrelationRefusesMalformedRecords/wrong_drain"},
		{"nested-foreign-primary-only", "muse_pty_log.go", `if len(strings.Split(rel, string(filepath.Separator))) != 5 {`, `if len(strings.Split(rel, string(filepath.Separator))) != 5 && filepath.Base(filepath.Dir(path)) != "foreign" {`, "TestMusePTYResumeIgnoresSubagentLogGrammar"},
		{"peer-preserve-source-add-invented-field", "muse_pty_host_test.go", `p["run_id"], p["source_run_record_id"] = museTestUUID("queue-run"), itemID`, `p["run_id"], p["source_run_record_id"] = museTestUUID("queue-run"), itemID; p["event"].(map[string]any)["inbox_item_id"] = itemID`, "TestMusePTYPeerRecordsMatchCapturedWireShapes"},

		{"captured-queue-one-only", "muse_pty_log.go", `len(f.Queued) == 0`, `(len(f.Queued) == 0 || len(f.Queued) == 1 && func() bool { _, ok := f.Queued["f53e0292-73eb-50bd-91f0-dc5fddab0464"]; return ok }())`, "TestMusePTYCapturedQueuedItemNeverSubmits"},
		{"live-permissions-modal-only", "muse_pty_screen.go", `if dialog {`, `if strings.Contains(screen, "selections are saved as your default") { return true }; if dialog {`, "TestMusePTYRecordedPermissionsModalNeverAdmits"},

		{"transcript-confirm-only", "muse_pty_screen.go", `if dialog {`, `if strings.Contains(strings.ToLower(strings.Join(lines[:begin-1], "\n")), "please confirm") { dialog = true }; if dialog {`, "TestMusePTYCompletedTranscriptKeywordsPermitNotice"},
		{"recheck-racing-run", "muse_pty_host.go", `if !h.idle(text) {
		return "", ErrMusePTYHeld
	} // mandatory pre-submit re-check
	if err = ctx.Err(); err != nil {
		return "", err
	}
	// Serialize the final frame check with the output pump; no observed dialog
	// can arrive between that check and Enter. The durable log is reread
	// for the frame too: off-frame rows are backed by committed output,
	// so the backing must be current, not the pre-paste fold. The fresh
	// fold must also still prove idle: a log change between the first
	// check and the submit defers, even when the screen never moved.
	finalFold, finalErr := h.fold()
	h.mu.Lock()
	if finalErr != nil || !finalFold.idle() || !h.vt.safeComposer(text, finalFold) || !h.quietLocked() {`, `if !h.idle(text) && func() bool { f, e := h.fold(); return e != nil || f.RunLifecycle["6e1140bb-c21f-449f-8c05-bce8c6d3df0f"] != museRunActive }() {
		return "", ErrMusePTYHeld
	} // mandatory pre-submit re-check
	if err = ctx.Err(); err != nil {
		return "", err
	}
	// Serialize the final frame check with the output pump; no observed dialog
	// can arrive between that check and Enter. The durable log is reread
	// for the frame too: off-frame rows are backed by committed output,
	// so the backing must be current, not the pre-paste fold. The fresh
	// fold must also still prove idle: a log change between the first
	// check and the submit defers, even when the screen never moved.
	finalFold, finalErr := h.fold()
	h.mu.Lock()
	if finalErr != nil || !finalFold.idle() && finalFold.RunLifecycle["6e1140bb-c21f-449f-8c05-bce8c6d3df0f"] != museRunActive || !h.vt.safeComposer(text, finalFold) || !h.quietLocked() {`, "TestMusePTYRechecksLogBeforeSubmit"},
		{"dialog-trust-only", "muse_pty_screen.go", `if dialog {`, `if strings.Contains(screen, "do you trust") { return true }; if dialog {`, "TestMusePTYTrustDialogNeverSubmits"},
		{"dialog-permissions-only", "muse_pty_screen.go", `if dialog {`, `if strings.Contains(screen, "permissions") { return true }; if dialog {`, "TestMusePTYRechecksDialogBeforeSubmit"},
		{"queue-one-only", "muse_pty_log.go", `len(f.Queued) == 0`, `len(f.Queued) <= 1`, "TestMusePTYQueuedInboxNeverSubmits"},
		{"busy-mark-only", "muse_pty_grammar.go", `return class, matches`, `if row == "◆ Working (2s · esc to interrupt)" { return museScreenRowContent, 1 }; return class, matches`, "TestMusePTYBusyScreenNeverSubmits"},
		{"screen-unknown-row-only", "muse_pty_grammar.go", `return class, matches`, `if row == "◈ Future action required" { return museScreenRowContent, 1 }; return class, matches`, "TestR141UnknownProgressRowMustRefuse"},
		{"progress-timed-only", "muse_pty_grammar.go", `return class, matches`, `if row == "◇ Thinking (12s · esc to interrupt)" { return museScreenRowContent, 1 }; return class, matches`, "TestMusePTYR5ProgressRowsMustRefuse"},
		{"progress-bare-thinking-only", "muse_pty_grammar.go", `return class, matches`, `if row == "◆ Thinking" { return museScreenRowContent, 1 }; return class, matches`, "TestReviewBUnknownProgressMustRefuse"},
		{"queue-status-only", "muse_pty_grammar.go", `return class, matches`, `if row == "• Queued input" { return museScreenRowContent, 1 }; return class, matches`, "TestMusePTYR5QueueStatusMustRefuse"},
		{"utf8-ff-only", "muse_pty_grammar.go", `if !utf8.Valid(raw) {`, `if !utf8.Valid(bytes.ReplaceAll(raw, []byte{0xff}, []byte("?"))) {`, "TestR141InvalidUTF8RecordMustRefuse"},
		{"del-only", "muse_pty_screen.go", `if r == utf8.RuneError && n == 1 || !unicode.IsPrint(r) {`, `if r == utf8.RuneError && n == 1 || !unicode.IsPrint(r) && r != 0x7f {`, "TestMusePTYR6ControlLodgedInContentMustRefuse"},
		{"spinner-one-rune", "muse_pty_screen.go", `r >= 0x2800 && r <= 0x28ff`, `r >= 0x2800 && r <= 0x28ff && r != 0x280b`, "TestMusePTYSpinnerNeverSubmits"},
		{"unknown-csi-99z", "muse_pty_screen.go", `if !museVTControl(raw, op) {
		v.unknownCSI(raw, op)
		return nil
	}`, `if !museVTControl(raw, op) {
		if raw != "99" || op != 'z' { v.unknownCSI(raw, op) }
		return nil
	}`, "TestMusePTYUnknownScreenNeverSubmits"},
		{"composer-human-draft-only", "muse_pty_screen.go", `if !strings.HasPrefix(rest, part) {`, `if !strings.HasPrefix(rest, part) && !(expected == "" && part == "human draft") {`, "TestMusePTYNonemptyComposerNeverPastes"},
		{"version-line-admits-one-malformed", "muse_pty_host.go", `if _, err := muse.New().ProbeToolRelease(ctx, plan.Env); err != nil {`, `if _, err := muse.New().ProbeToolRelease(ctx, plan.Env); err != nil && !strings.Contains(err.Error(), "Muse Code 9.9 (") {`, "TestMusePTYVersionLineRefusesMalformed/bad-semver-two-components"},
		{"resume-fresh-peer-only", "muse_pty_host.go", `if !selected {`, `if !selected && prior.SessionID != "fc777faf-c959-4fcd-b2c7-c35044561e5b" {`, "TestMusePTYResumeRefusesFreshPlan"},
		{"snapshot-v0-only", "muse_pty_host.go", `prior.Contract != "task-board.muse-pty-host-v1"`, `(prior.Contract != "task-board.muse-pty-host-v1" && prior.Contract != "task-board.muse-pty-host-v0")`, "TestMusePTYResumeSnapshotRefusesWrongContract"},
		{"trust-yolo-only", "muse_pty_host.go", `if !trusted {`, `if !trusted && req.PermissionMode != agentic.PermissionModeYolo {`, "TestMusePTYPlannerUsesPluginPostureAndEnv"},
		{"text-escape-only", "muse_pty_host.go", `strings.ContainsAny(text, "\x1b\x00\r")`, `strings.ContainsAny(text, "\x00\r")`, "TestMusePTYGoalValidationAndControlTextRefused"},
		{"blocked-inline-only", "muse_pty_log.go", `strings.HasPrefix(strings.TrimSpace(line), "BLOCKED:")`, `strings.Contains(strings.TrimSpace(line), "BLOCKED:")`, "TestMusePTYTextArmStatusMapping/blocked_inline_is_not_boundary"},
		{"resume-ended-epoch", "muse_pty_log.go", `f.Ended = false`, `if f.PID == 0 { f.Ended = false }`, "TestMusePTYResumeRecordedNativeSession"},
		{"literal-short-rule-only", "muse_pty_screen.go", `if lines[j] == lower {`, `if lines[j] == lower || strings.TrimSpace(lines[j]) == "──" {`, "TestMusePTYLiteralRuleRoundTrip"},
		{"literal-trailing-border-only", "muse_pty_screen.go", `part := museText(row)`, `part := museText(strings.TrimSuffix(row, "│"))`, "TestMusePTYUnboxedLiteralBorderRemainsText"},
		{"unknown-footer-only", "muse_pty_screen.go", `func museIdleFooter(line string) bool {
	parts := strings.Split(line, " · ")`, `func museIdleFooter(line string) bool {
	if line == "  Future decision surface" { return true }
	parts := strings.Split(line, " · ")`, "TestMusePTYClosedScreenUnknownFooterRefuses"},
		// 1.4.7 refusal controls: mutate the production admission gates, not
		// the captured grammar or the killing tests. The fabricated one-part
		// footer above cannot reach either provider branch; cover those apart.
		{"147-echo-footer-mode-any", "muse_pty_screen.go", `return parts[0] == "  echo" && parts[1] != "" && parts[2] == "Auto-review"`, `return parts[0] == "  echo" && parts[1] != ""`, "TestMusePTY147ClosedProviderFooters/echo_unknown_mode"},
		{"147-meta-footer-effort-any", "muse_pty_screen.go", `switch parts[1] {
		case "minimal", "low", "medium", "high", "xhigh", "max":
			return true
		}`, `return true`, "TestMusePTY147ClosedProviderFooters/unknown_effort"},
		{"147-osc-7501-payload-any", "muse_pty_screen.go", `} else if osc != "7501;?" && !museVTColorQuery(osc) {`, `} else if !strings.HasPrefix(osc, "7501;") && !museVTColorQuery(osc) {`, "TestMusePTY147VTClosedNegativeControls/other_7501_payload"},
		{"147-el-explicit-zero-only", "muse_pty_screen.go", `return raw == "" || raw == "2"`, `return raw == "" || raw == "2" || raw == "0"`, "TestMusePTY147BareELErasesCursorSuffix"},
		{"147-el-explicit-one-only", "muse_pty_screen.go", `return raw == "" || raw == "2"`, `return raw == "" || raw == "2" || raw == "1"`, "TestMusePTY147BareELErasesCursorSuffix"},
		{"147-closed-outer-additive-only", "muse_pty_grammar.go", `if candidate.Closed || museFieldKnownAtPath(shapes, path, k) {`, `if candidate.Closed && k != "future_outer" || museFieldKnownAtPath(shapes, path, k) {`, "TestMusePTY147SyntheticMETAClosedShapes"},
		{"147-closed-payload-additive-only", "muse_pty_grammar.go", `if candidate.Closed || museFieldKnownAtPath(shapes, path, k) {`, `if candidate.Closed && k != "future_payload" || museFieldKnownAtPath(shapes, path, k) {`, "TestMusePTY147SyntheticMETAClosedShapes"},
		{"missing-opened-record-only", "muse_pty_grammar.go", `key := museGrammarKey(v)`, `key := museGrammarKey(v); if key == "session.opened.observed/session_opened/" { if _,ok := v.(map[string]any)["payload"].(map[string]any)["record"]; !ok { v.(map[string]any)["payload"].(map[string]any)["record"] = map[string]any{"schema_version": json.Number("1"), "session_id": v.(map[string]any)["stream"].(map[string]any)["id"], "security_mode": "normal", "resume": false, "credential_backend": "file", "keychain_fallback_reason": "none", "workspace_free_bytes": json.Number("0"), "data_dir_free_bytes": json.Number("0")} } }`, "TestMusePTYIncompleteOpenedRecordRefuses/missing_record"},
		{"missing-metadata-provider-only", "muse_pty_grammar.go", `key := museGrammarKey(v)`, `key := museGrammarKey(v); if key == "runtime.session.metadata/metadata/" { record := v.(map[string]any)["payload"].(map[string]any)["record"].(map[string]any); if _,ok := record["provider_id"]; !ok { record["provider_id"] = "echo" } }`, "TestMusePTYPartialMetadataFieldRefuses"},
		{"duplicate-payload-only", "muse_pty_grammar.go", `if _, exists := m[key]; exists {`, `if _, exists := m[key]; exists && key != "payload" {`, "TestMusePTYDuplicateFieldsRefuse"},
		{"failed-assume-retryable-only", "muse_pty_log.go", `result.Recoverable = false

	default:`, `result.Recoverable = t.Terminal == "failed"

	default:`, "TestMusePTYTextArmStatusMapping/failed_blocked_text_is_not_boundary"},
		{"color-256-only", "muse_pty_screen.go", `n >= 0 && n <= 255`, `n >= 0 && n <= 256`, "TestMusePTYUnknownVTFieldsRefuse/color_out_of_range"},
		{"unknown-c0-one-only", "muse_pty_screen.go", `v.unknown = true // No uncaptured C0 control is a harmless absence.`, `if b != 1 { v.unknown = true } // No uncaptured C0 control is a harmless absence.`, "TestMusePTYUnknownVTFieldsRefuse/unknown_c0"},
		{"partial-tail-assume-terminal-only", "muse_pty_host.go", `for {
		f, e := h.fold()
		if e == nil {
			if terminal, ok := f.Terminals[id]; ok {`, `for {
		f, e := h.fold(); if e != nil && strings.Contains(e.Error(), "complete tail") { return &sm.ProviderRunResult{Status:sm.ProviderRunFailed,ProviderStatus:"unknown_failure"},nil }
		if e == nil {
			if terminal, ok := f.Terminals[id]; ok {`, "TestMusePTYPartialAppendWaitsForCompleteRecord"},

		{"surrogate-d800-only", "muse_pty_grammar.go", `case hi >= 0xD800 && hi <= 0xDBFF:`, `case hi >= 0xD800 && hi <= 0xDBFF && hi != 0xD800:`, "TestMusePTYR6LossySurrogateMustRefuse"},
		{"whitespace-vt-only", "muse_pty_log.go", `if i == len(split)-1 && len(line) == 0 {`, `if i == len(split)-1 && len(line) == 0 || string(line) == "\x0b" {`, "TestMusePTYR6NonJSONWhitespaceMustRefuse"},
		{"duplicate-run-start-one-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse run started twice without terminal")`, `if e.RunID != "655338be-7f68-4b48-a27f-3d2fea497d3f" { return nil, fmt.Errorf("Muse run started twice without terminal") }`, "TestMusePTYR6DuplicateRunStartMustRefuse"},
		{"foreign-stream-one-only", "muse_pty_log.go", `if bound != "" && record.Stream.ID != bound {
			return nil, fmt.Errorf("Muse record names a foreign session")`, `if bound != "" && record.Stream.ID != bound {
			if record.Stream.ID == "a6d56b99-1f61-487b-8886-188c7ae6053e" {
				continue
			}
			return nil, fmt.Errorf("Muse record names a foreign session")`, "TestMusePTYR6ForeignStreamMustRefuse"},
		{"task-cross-run-one-only", "muse_pty_log.go", `next, ok := museTaskTransitions[f.Tasks[key]][e.Kind]`, `next, ok := museTaskTransitions[f.Tasks[key]][e.Kind]; if !ok && p.TaskID == "97d7799e-f999-4e86-872a-44063cb003ba" { for k, state := range f.Tasks { if k.Task == p.TaskID && state == museTaskStarted { f.Tasks[k] = museTaskTerminated; next, ok = museTaskTerminated, true } } }`, "TestMusePTYR6CrossRunTaskTerminalMustRefuse"},
		{"region-unknown-row-one-only", "muse_pty_screen.go", `class, matches := museClassifyScreenRow(s)`, `if s == "◈ Future action required" { continue }; class, matches := museClassifyScreenRow(s)`, "TestMusePTYR6BuriedProgressRowMustRefuse"},
		{"indented-glyph-one-only", "muse_pty_screen.go", `func museLogBackedRowsOK(rows []string, cols int, fold *museLogFold) bool {`, `func museLogBackedRowsOK(rows []string, cols int, fold *museLogFold) bool {
	if len(rows) > 0 && rows[0] == "  ◈ Future action required" {
		return true
	}`, "TestMusePTYR6IndentedStatusMustRefuse"},
		{"osc-del-only", "muse_pty_screen.go", `for _, r := range title {
		if !unicode.IsPrint(r) {`, `for _, r := range title {
		if !unicode.IsPrint(r) && r != 0x7f {`, "TestMusePTYR6MalformedOSCTitleMustRefuse"},

		{"m1-opened-session-one-only", "muse_pty_log.go", `return fmt.Errorf("Muse record names another session")`, `if id != "7cddd7e6-3233-4b90-9647-5c645673d522" { return fmt.Errorf("Muse record names another session") }`, "TestMusePTYR7OpenedIdentityMismatchMustRefuse"},
		{"m1-route-pid-one-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse route claims another process in this session epoch")`, `if route.PID != 99 { return nil, fmt.Errorf("Muse route claims another process in this session epoch") }`, "TestMusePTYR7ChangedRoutePIDMustRefuse"},
		{"m1-snapshot-session-one-only", "muse_pty_host.go", `return nil, fmt.Errorf("Muse log names an unowned session")`, `if f.SessionID != "a6d56b99-1f61-487b-8886-188c7ae6053e" { return nil, fmt.Errorf("Muse log names an unowned session") }`, "TestMusePTYR7RewrittenForeignSessionMustRefuse"},
		{"m1-snapshot-pid-one-only", "muse_pty_host.go", `return nil, fmt.Errorf("Muse log names an unowned process")`, `if f.PID != 99 { return nil, fmt.Errorf("Muse log names an unowned process") }`, "TestMusePTYR7ResumedForeignPIDMustRefuse"},
		{"m2-run-reuse-one-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse run restarted after its terminal")`, `if e.RunID != "af7ed83a-a897-413b-ac7e-8290f5d25ea4" { return nil, fmt.Errorf("Muse run restarted after its terminal") }`, "TestMusePTYR7RunIDReuseMustRefuse"},
		{"m2-task-order-one-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse task transition %q is not captured", e.Kind)`, `if p.TaskID != "a6600299-cb99-4110-9a26-24fae7c3e89d" || e.Kind != "accepted" { return nil, fmt.Errorf("Muse task transition %q is not captured", e.Kind) }; next = museTaskTerminated`, "TestMusePTYR7TaskPlanningRegressionMustRefuse"},
		{"m2-task-run-one-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse task names a run with no active lifecycle")`, `if p.RunID != "decf9b5f-18bc-4a32-b54b-d217636b6c23" { return nil, fmt.Errorf("Muse task names a run with no active lifecycle") }`, "TestMusePTYR7TaskStartAfterRunTerminalMustRefuse"},
		{"m2-reopen-clear-one-only", "muse_pty_log.go", `// The closed grammar validated the whole metadata record before folding.`, `if record.Type == "session.name.changed" {
			f.ReopenPending = false
		}
		// The closed grammar validated the whole metadata record before folding.`, "TestMusePTYR7ReopenNameClearMustRefuse"},
		{"m3-duplicate-queue-one-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse inbox item queued twice")`, `if p.SourceRunRecordID != "f53e0292-73eb-50bd-91f0-dc5fddab0464" { return nil, fmt.Errorf("Muse inbox item queued twice") }`, "TestMusePTYR7DuplicateQueueMustRefuse"},
		{"m3-drain-target-one-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse drain target names another run")`, `if target.ID != "63f0a28d-25de-41b9-a4c5-0a42376dcc31" { return nil, fmt.Errorf("Muse drain target names another run") }`, "TestMusePTYR7DrainTargetMismatchMustRefuse"},
		{"m3-drain-snapshot-one-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse drain snapshot differs from the queued item")`, `if !bytes.Contains(e.Snapshot, []byte("rewritten work")) { return nil, fmt.Errorf("Muse drain snapshot differs from the queued item") }`, "TestMusePTYR7DrainCorrelationMustRefuse"},
		{"m3-drain-run-one-only", "muse_pty_log.go", `if !museRunKnown(f, target.ID) {
					f.DrainTargetPending[target.ID] = true
				}`, `if !museRunKnown(f, target.ID) && target.ID != "9841204f-3188-4ee3-8b07-bf17d2bc7345" {
					f.DrainTargetPending[target.ID] = true
				}`, "TestMusePTYR7DrainCorrelationMustRefuse"},
		{"m4-mirror-started-one-only", "muse_pty_log.go", `f.Mirror[p.TaskID] = next`, `f.Mirror[p.TaskID] = next; if mirror.Kind == "started" { f.Mirror[p.TaskID] = museTaskTerminated }`, "TestMusePTYR7MirrorStartedPendingMustRefuse"},
		{"m5-lone-indented-one-only", "muse_pty_screen.go", `func museLogBackedRowsOK(rows []string, cols int, fold *museLogFold) bool {`, `func museLogBackedRowsOK(rows []string, cols int, fold *museLogFold) bool {
	if len(rows) > 0 && rows[0] == "  Future action required" {
		return true
	}`, "TestMusePTYR7OrphanIndentedStatusMustRefuse"},
		{"m5-header-one-only", "muse_pty_screen.go", `if prevState == museRowChromeHeader && museResumeNoticeRow.MatchString(row) {`, `if prevState == museRowChromeHeader && (museResumeNoticeRow.MatchString(row) || row == "  Future action required") {`, "TestMusePTYR7HeaderSequenceMustRefuse"},
		{"epoch-end-no-arm", "muse_pty_log.go", "// epoch and spends the arm.\n\t\t\tf.pidRebind = true", "// epoch and spends the arm.", "TestMusePTYR7RealResumeOrderBindsNewEpoch/end_route_opened_resumed"},
		{"epoch-first-open-arms", "muse_pty_log.go", "case first:\n\t\t\t\tf.pidRebind = false", "case first:\n\t\t\t\tf.pidRebind = true", "TestMusePTYR7RealResumeOrderBindsNewEpoch/stale_arm_rebinds_mid_epoch"},
		{"epoch-reopen-rearms", "muse_pty_log.go", "case wasEnded:\n\t\t\tdefault:", "case wasEnded:\n\t\t\t\tf.pidRebind = true\n\t\t\tdefault:", "TestMusePTYR7RealResumeOrderBindsNewEpoch/second_route_same_epoch_refuses"},

		{"r8-cleanup-mismatch-one-only", "muse_pty_log.go", `state, ok := f.Cleanup[key]`, `state, ok := f.Cleanup[key]; if !ok && key.Effect == 999 { for k, st := range f.Cleanup { if st == museCleanupStarted && k.Run == key.Run { f.Cleanup[k] = museCleanupTerminated; f.CleanupChild[key] = f.CleanupChild[k] } }; ok = true }`, "TestMusePTYR8CleanupPairingMustRefuse"},
		{"r8-cleanup-pending-one-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse cleanup effect %d in run %s lacks its terminal", key.Effect, key.Run)`, `if key.Run != "1123724a-7d59-4af3-ba21-2dcdc35dac17" { return nil, fmt.Errorf("Muse cleanup effect %d in run %s lacks its terminal", key.Effect, key.Run) }`, "TestMusePTYR8CleanupPairingMustRefuse"},
		{"r8-link-cross-run-one-only", "muse_pty_log.go", `if _, ok := f.Tasks[key]; !ok {`, `if _, ok := f.Tasks[key]; !ok && key.Task != "1597ad8e-20a9-4765-a06c-3ae848e88366" {`, "TestMusePTYR8TaskLinkExactRunMustRefuse"},
		{"r8-drain-causation-one-only", "muse_pty_log.go", `if item.Causation == "" || item.Causation != target.ID {`, `if (item.Causation == "" || item.Causation != target.ID) && target.ID != "d7f0fed2-0789-4e4c-b1eb-92b6c5618e7d" {`, "TestMusePTYR8DrainTargetMustMatchForwardReference"},
		{"r8-intent-pairing-one-only", "muse_pty_log.go", `state, ok := f.Intents[intentID]`, `state, ok := f.Intents[intentID]; if !ok && intentID == "b939d73d-eb55-4b9c-9663-13b0109cffe9" { state = museIntent{State: museIntentAccepted, RecordID: mat.EnvelopeID, Sequence: mat.EnvelopeSeq}; f.Intents[intentID] = state; ok = true }`, "TestMusePTYR9IntentLookupMustRefuse"},
		{"r8-task-planning-pending-one-only", "muse_pty_log.go", `for _, state := range f.Tasks {
		if state != museTaskTerminated {
			n++
		}
	}`, `for _, state := range f.Tasks {
		if state != museTaskTerminated && state != museTaskProposed {
			n++
		}
	}`, "TestMusePTYR8PlanningPrefixPendingMustRefuse"},
		{"r9-scrolled-first-row-only", "muse_pty_screen.go", `func museLogBackedRowsOK(rows []string, cols int, fold *museLogFold) bool {`, `func museLogBackedRowsOK(rows []string, cols int, fold *museLogFold) bool {
	if len(rows) > 0 && rows[0] == "  Waiting for remote approval" {
		return true
	}`, "TestMusePTYR8ScrolledProvenanceMustRefuse"},
		{"r9-mirror-counterpart-only", "muse_pty_log.go", `f.TaskTerminals[key] = e.Kind`, `f.TaskTerminals[key] = e.Kind; if p.TaskID == "9ada03b3-7b8a-40e2-a468-2c0c0442d98e" { f.Mirror[p.TaskID] = museTaskTerminated }`, "TestMusePTYR9MirrorRequiresOwnTerminal"},
		{"r9-response-mismatch-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse message names an unobserved response")`, `if e.RunID != "848d4966-8a65-4c70-9c54-4300267e22c3" { return nil, fmt.Errorf("Muse message names an unobserved response") }; delete(f.MessageResponses[e.RunID], e.ResponseID)`, "TestMusePTYR9ResponseIdentityMustMatch"},
		{"r9-response-duplicate-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse response created twice")`, `if e.RunID != "848d4966-8a65-4c70-9c54-4300267e22c3" { return nil, fmt.Errorf("Muse response created twice") }`, "TestMusePTYR9ResponseIdentityMustMatch"},
		{"r9-message-duplicate-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse message committed twice")`, `if e.RunID != "47971786-2f58-40f8-a373-993f6ac5c873" { return nil, fmt.Errorf("Muse message committed twice") }`, "TestMusePTYR9ResponseIdentityMustMatch"},
		{"r9-reasoning-mismatch-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse reasoning names an unobserved response")`, `if e.RunID != "82a6e63b-90aa-4000-94dd-6f0168ba65ec" { return nil, fmt.Errorf("Muse reasoning names an unobserved response") }; delete(f.MessageResponses[e.RunID], e.ResponseID)`, "TestMusePTYR9ResponseIdentityMustMatch"},
		{"r9-cleanup-child-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse cleanup child terminal differs from the bound child")`, `if child != "dbae1e6d-3700-42da-a713-4d21012e1b8b" { return nil, fmt.Errorf("Muse cleanup child terminal differs from the bound child") }`, "TestMusePTYR9CleanupChildOutcomeMustMatch"},
		{"r9-cleanup-epoch-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse cleanup terminal names a run with no active lifecycle")`, `if p.RunID != "3fbe87c0-22b2-4d84-bd2a-737832d14a39" { return nil, fmt.Errorf("Muse cleanup terminal names a run with no active lifecycle") }`, "TestMusePTYR9CleanupTerminalRequiresActiveParent"},
		{"r9-reopen-pending-only", "muse_pty_log.go", `default:
				f.pidRebind = true
				f.ReopenPending = true
			}`, `default:
				f.pidRebind = true
				if record.Stream.ID != "fc777faf-c959-4fcd-b2c7-c35044561e5b" {
					f.ReopenPending = true
				}
			}`, "TestMusePTYR9NoEndReopenRequiresResume"},
		{"r9-envelope-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse materialized intent names another envelope")`, `if intentID != "153252fa-01fd-4df7-8e55-20bbfecd9c4f" { return nil, fmt.Errorf("Muse materialized intent names another envelope") }`, "TestMusePTYR9IntentEnvelopeMustRefuse"},
		{"r9-started-refs-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse top-level outcome names an unobserved start")`, `if o.Intent != "51d5d0bf-6fe9-4f15-bb58-a3b79c424dfd" { return nil, fmt.Errorf("Muse top-level outcome names an unobserved start") }`, "TestMusePTYR9IntentStartedRefsMustRefuse"},
		{"r9-top-level-run-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse top-level outcome names another run")`, `if o.Intent != "601dcecc-dc18-4275-b0f5-268d08646079" { return nil, fmt.Errorf("Muse top-level outcome names another run") }`, "TestMusePTYR9TopLevelIntentMustProduceRun"},
		{"r9-top-level-order-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse top-level run started before its intent")`, `if o.Intent != "4e5c551c-fbe6-4f27-8195-742d0614a9e1" { return nil, fmt.Errorf("Muse top-level run started before its intent") }`, "TestMusePTYR9TopLevelIntentMustProduceRun"},
		{"r9-delivery-queue-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse delivery names an unobserved queue record")`, `if o.Intent != "3f2f2569-457f-4149-8f43-ef7b247c5f59" { return nil, fmt.Errorf("Muse delivery names an unobserved queue record") }`, "TestMusePTYR9ActiveDeliveryNeedsQueue"},
		{"r10-delivery-run-only", "muse_pty_log.go", `} else if q.Run != o.Run {`, `} else if q.Run != o.Run && o.Run != "345d1a60-808f-4d3a-910e-77b703c27c8b" {`, "TestMusePTYR10ActiveDeliveryExactJoins"},
		{"r10-delivery-causation-only", "muse_pty_log.go", `} else if q.Causation == "" || q.Causation != o.Intent {`, `} else if (q.Causation == "" || q.Causation != o.Intent) && o.Intent != "2ad3c02b-356f-4b0b-93b7-d844053d525e" {`, "TestMusePTYR10ActiveDeliveryExactJoins"},
		{"r10-delivery-order-only", "muse_pty_log.go", `} else if q.Index <= f.Intents[o.Intent].AcceptIndex {`, `} else if q.Index <= f.Intents[o.Intent].AcceptIndex && o.Intent != "940dee65-9bb5-493c-a18a-79f4baf8a1df" {`, "TestMusePTYR10ActiveDeliveryExactJoins"},
		{"r10-idempotency-run-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse task side effect names another idempotency identity")`, `if p.TaskID != "864aae15-58b3-4dad-b916-c85c32f3f136" { return nil, fmt.Errorf("Muse task side effect names another idempotency identity") }`, "TestMusePTYR10TaskIdempotencyMustMatch"},
		{"r10-idempotency-mirror-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse mirror side effect names another idempotency identity")`, `if p.TaskID != "864aae15-58b3-4dad-b916-c85c32f3f136" { return nil, fmt.Errorf("Muse mirror side effect names another idempotency identity") }`, "TestMusePTYR10TaskIdempotencyMustMatch"},
		{"r10-response-reconcile-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse committed response lacks a witnessed creation")`, `if run != "99073490-7808-43a2-b632-c130539f0aa6" { return nil, fmt.Errorf("Muse committed response lacks a witnessed creation") }`, "TestMusePTYR10ResponseReconcilesRegardlessOfOrder"},
		{"r10-frame-index-only", "muse_pty_log.go", `if child.Index != i {`, `if child.Index != i && !(i == 1 && child.Index == 0) {`, "TestMusePTYR10RetainedFrameMustBeClosed"},
		{"r10-frame-kind-only", "muse_pty_log.go", `if record.Type != want[i] {`, `if record.Type != want[i] && record.Type != "runtime.session" {`, "TestMusePTYR10RetainedFrameMustBeClosed"},
		{"r10-screen-content-only", "muse_pty_screen.go", `func museLogBackedRowsOK(rows []string, cols int, fold *museLogFold) bool {`, `func museLogBackedRowsOK(rows []string, cols int, fold *museLogFold) bool {
	if len(rows) > 0 && rows[0] == "  foreign line" {
		return true
	}`, "TestMusePTYR10LogBackedOffFrameRefuses"},
		{"r10-screen-active-run-only", "muse_pty_screen.go", `if state != museRunTerminated {
			continue
		}`, `if state != museRunTerminated && run != "de89b9cd-f7dd-4f64-aa85-7857b921972a" {
			continue
		}`, "TestMusePTYR10LogBackedRowsSkipsActiveRun"},
		{"r11-final-fold-only", "muse_pty_host.go", `if finalErr != nil || !finalFold.idle() || !h.vt.safeComposer(text, finalFold) || !h.quietLocked() {`, `if finalErr != nil || !h.vt.safeComposer(text, finalFold) || !h.quietLocked() {`, "TestMusePTYR11PreSubmitRecheckIncludesFold"},
		{"quiet-window-returns-true", "muse_pty_host.go", `func (h *musePTYHost) quietLocked() bool {
	return h.nowLocked().Sub(h.lastOutput) >= musePTYQuiet
}`, `func (h *musePTYHost) quietLocked() bool {
	return true
}`, "TestMusePTYQuietWindowBoundaryDefersInside"},
		{"quiet-window-narrow-1ms", "muse_pty_host.go", `return h.nowLocked().Sub(h.lastOutput) >= musePTYQuiet`, `return h.nowLocked().Sub(h.lastOutput) >= musePTYQuiet-time.Millisecond`, "TestMusePTYQuietWindowBoundaryDefersInside"},
		{"r12-envelope-id-only", "muse_pty_log.go", `if _, dup := f.RecordIndex[record.ID]; dup {`, `if _, dup := f.RecordIndex[record.ID]; dup && record.ID != "46e38642-2027-46e0-9e34-a95d30c1b7f5" {`, "TestMusePTYR12AliasedStartedIdentityMustRefuse"},
		{"r12-envelope-seq-only", "muse_pty_log.go", `if f.SeenSeqs[record.Sequence] {`, `if f.SeenSeqs[record.Sequence] && record.Sequence != 424242 {`, "TestMusePTYR12AliasedSequenceMustRefuse"},
		{"r12-intake-pending-only", "muse_pty_log.go", `if !f.IntakeSettled[id] {`, `if !f.IntakeSettled[id] && id != "09c41ca3-0b9c-4388-b954-cdaf4f298122" {`, "TestMusePTYR12PendingIntakeMustRefuse"},
		{"r12-resume-id-only", "muse_pty_log.go", `if len(f.EpochOpens) >= 2 {`, `if len(f.EpochOpens) >= 2 && ref.RecordID != "80bff5f2-33fb-4224-8840-c615477a13d7" {`, "TestMusePTYR12ResumeReferenceMustResolve"},
		{"r12-resume-seq-only", "muse_pty_log.go", `if len(f.EpochOpens) >= 2 {`, `if len(f.EpochOpens) >= 2 && ref.Sequence != 999999 {`, "TestMusePTYR12ResumeReferenceMustResolve"},
		{"r12-resume-epoch-only", "muse_pty_log.go", `if len(f.EpochOpens) >= 2 {`, `if len(f.EpochOpens) >= 2 && ref.RecordID != "8f93330f-6d8a-46f5-b974-3e5104086d2a" {`, "TestMusePTYR12ResumeReferenceMustResolve"},
		{"r12-width-allowlist-only", "muse_pty_screen.go", `if r == 0x2502 {
		return 1, true
	}
	return 0, false`, `if r == 0x2502 {
		return 1, true
	}
	if r == 0x1B132 {
		return 1, true
	}
	return 0, false`, "TestMusePTYR12UnestablishedWidthMustRefuse"},
		{"r12-width-bound-only", "muse_pty_screen.go", `if width, ok := museDisplayWidth(line); !ok || width > cols-2 {`, `if width, ok := museDisplayWidth(line); !ok || width > cols-2 && !strings.HasPrefix(line, "r12-bound-sentinel-") {`, "TestMusePTYR12OverwideASCIILineMustRefuse"},
		{"r13-epoch-run-only", "muse_pty_log.go", `if f.epoch() != museEpochOpen {`, `if f.epoch() != museEpochOpen && id != "b7340aa1-dd64-4fc5-a245-7648700d5fac" {`, "TestMusePTYR13RunInsideEndedEpochMustRefuse"},
		{"r13-chrome-notice-only", "muse_pty_screen.go", `if prevState == museRowChromeNotice && state == museScreenRowIndented {
		return false
	}`, `if prevState == museRowChromeNotice && state == museScreenRowIndented {
		if row == "  ↳ unseen queued item" {
			return true
		}
		return false
	}`, "TestMusePTYR13ChromeNoticeCannotParentOrphan"},
		{"r13-empty-id-only", "muse_pty_grammar.go", `func museWellFormedUUID(id string) bool {
	return museUUIDPattern.MatchString(id)
}`, `func museWellFormedUUID(id string) bool {
	if id == "" {
		return true
	}
	return museUUIDPattern.MatchString(id)
}`, "TestMusePTYR14IdentityFormatMustRefuse"},
		{"r13-zero-seq-only", "muse_pty_log.go", `if record.Sequence <= 0 {
			return nil, fmt.Errorf("Muse record sequence is not positive")`, `if record.Sequence <= 0 && record.Sequence != 0 {
			return nil, fmt.Errorf("Muse record sequence is not positive")`, "TestMusePTYR13ZeroSequenceMustRefuse"},
		{"r13-empty-context-id-only", "muse_pty_grammar.go", `func museWellFormedContextBlock(id string) bool {
	return museContextBlockIDs[id]
}`, `func museWellFormedContextBlock(id string) bool {
	if id == "" {
		return true
	}
	return museContextBlockIDs[id]
}`, "TestMusePTYR14EnumFormatMustRefuse"},
		{"r13-empty-roster-id-only", "muse_pty_grammar.go", `func museWellFormedReminderAgent(id string) bool {
	return museReminderAgentIDs[id]
}`, `func museWellFormedReminderAgent(id string) bool {
	if id == "" {
		return true
	}
	return museReminderAgentIDs[id]
}`, "TestMusePTYR14EnumFormatMustRefuse"},
		{"r13-zero-effect-only", "muse_pty_log.go", `if rec.EffectID <= 0 {
				return nil, fmt.Errorf("Muse cleanup effect lacks identity")`, `if rec.EffectID <= 0 && rec.EffectID != 0 {
				return nil, fmt.Errorf("Muse cleanup effect lacks identity")`, "TestMusePTYR13ZeroCleanupEffectMustRefuse"},
		{"r11-digest-shape-only", "muse_pty_log.go", `if !museFrameDigestShape(envelope.Digest) {`, `if !museFrameDigestShape(envelope.Digest) && envelope.Digest != "sha256:invalid" {`, "TestMusePTYR11FrameDigestMalformedMustRefuse"},
		{"r11-orphan-resumed-only", "muse_pty_log.go", `return nil, fmt.Errorf("Muse session resumed without a pending reopen")`, `if f.SessionID != "fc777faf-c959-4fcd-b2c7-c35044561e5b" { return nil, fmt.Errorf("Muse session resumed without a pending reopen") }`, "TestMusePTYR11OrphanResumedMustRefuse"},
		{"r11-intake-settled-only", "muse_pty_log.go", `if !f.IntakeReceived[rec.CommandID] {`, `if !f.IntakeReceived[rec.CommandID] && rec.CommandID != "2a8b4630-1511-409e-9522-b3d5d42c7922" {`, "TestMusePTYR11IntakePairMustMatch"},
		{"r11-intake-settled-dup-only", "muse_pty_log.go", `if f.IntakeSettled[rec.CommandID] {`, `if f.IntakeSettled[rec.CommandID] && rec.CommandID != "9c2c2e85-593a-4dff-9b59-a615cfa5d0e8" {`, "TestMusePTYR11IntakePairMustMatch"},
		{"r11-intake-received-dup-only", "muse_pty_log.go", `if f.IntakeReceived[rec.CommandID] {`, `if f.IntakeReceived[rec.CommandID] && rec.CommandID != "fb1fceb2-249c-4e08-b55d-323d711ce67d" {`, "TestMusePTYR11IntakePairMustMatch"},
		{"r11-cleanup-start-only", "muse_pty_log.go", `if f.Tasks[childKey] == museTaskTerminated {`, `if f.Tasks[childKey] == museTaskTerminated && childKey.Task != "01a0fad4-e714-7d30-a0ce-6dffc317e177" {`, "TestMusePTYR11CleanupOrderMustMatch"},
		{"r11-trace-request-only", "muse_pty_log.go", `if !f.ConfiguredSources[ref.Run][ref.ID] {`, `if !f.ConfiguredSources[ref.Run][ref.ID] && ref.ID != "d9b99aa3-eae0-4346-9e95-1463342131a7" {`, "TestMusePTYR11TraceRequestIdentityMustMatch"},
		{"r11-context-block-only", "muse_pty_log.go", `if !f.ConfiguredContexts[ref.Run][ref.ID] {`, `if !f.ConfiguredContexts[ref.Run][ref.ID] && ref.ID != "security_mode" {`, "TestMusePTYR11ContextBlockIdentityMustMatch"},
		{"r11-reminder-roster-only", "muse_pty_log.go", `if !f.ConfiguredRoster[ref.Run][ref.ID] {`, `if !f.ConfiguredRoster[ref.Run][ref.ID] && ref.ID != "verify-reminder" {`, "TestMusePTYR11ReminderRosterIdentityMustMatch"},
		{"r14-uuid-blank-only", "muse_pty_grammar.go", `func museWellFormedUUID(id string) bool {
	return museUUIDPattern.MatchString(id)
}`, `func museWellFormedUUID(id string) bool {
	if id == "   " {
		return true
	}
	return museUUIDPattern.MatchString(id)
}`, "TestMusePTYR14IdentityFormatMustRefuse"},
		{"r14-uuid-malformed-only", "muse_pty_grammar.go", `func museWellFormedUUID(id string) bool {
	return museUUIDPattern.MatchString(id)
}`, `func museWellFormedUUID(id string) bool {
	if id == "not-a-uuid" {
		return true
	}
	return museUUIDPattern.MatchString(id)
}`, "TestMusePTYR14IdentityFormatMustRefuse"},
		{"r14-context-block-only", "muse_pty_grammar.go", `func museWellFormedContextBlock(id string) bool {
	return museContextBlockIDs[id]
}`, `func museWellFormedContextBlock(id string) bool {
	if id == "never-configured-block" {
		return true
	}
	return museContextBlockIDs[id]
}`, "TestMusePTYR14EnumFormatMustRefuse"},
		{"r14-reminder-agent-only", "muse_pty_grammar.go", `func museWellFormedReminderAgent(id string) bool {
	return museReminderAgentIDs[id]
}`, `func museWellFormedReminderAgent(id string) bool {
	if id == "never-configured-reminder" {
		return true
	}
	return museReminderAgentIDs[id]
}`, "TestMusePTYR14EnumFormatMustRefuse"},
		{"r14-response-tag-only", "muse_pty_grammar.go", `func museWellFormedResponseTag(id string) bool {
	return museResponseTagPattern.MatchString(id)
}`, `func museWellFormedResponseTag(id string) bool {
	if id == "BAD TAG!" {
		return true
	}
	return museResponseTagPattern.MatchString(id)
}`, "TestMusePTYR14EnumFormatMustRefuse"},
		{"r14-idempotency-key-only", "muse_pty_grammar.go", `func museWellFormedIdempotencyKey(key string) bool {`, `func museWellFormedIdempotencyKey(key string) bool {
	if key == "not-a-key!" {
		return true
	}`, "TestMusePTYR14EnumFormatMustRefuse"},
		{"r14-chrome-header-row-only", "muse_pty_screen.go", `if prevState == museRowChromeHeader && state == museScreenRowIndented {
		return false
	}`, `if prevState == museRowChromeHeader && state == museScreenRowIndented {
		if row == "  Muse Code 1.4.1" {
			return true
		}
		return false
	}`, "TestMusePTYR14DuplicateChromeMustRefuse"},
		{"r15-chrome-before-backed-only", "muse_pty_screen.go", `if len(transcript) > 0 && museScreenContentID(transcript[0]) == museScreenRowIndented {`, `if len(transcript) > 0 && museScreenRowStateAt(transcript[0], 0, museRowStart) == museScreenRowIndented {`, "TestMusePTYR15BackedQuotedHeaderAtTopDelivers"},

		// L3G structural PTY grammar (regression, M54-M57). Each
		// row weakens exactly one gate for exactly one sentinel member
		// of its class; the named test must fail. Retired above:
		// terminal-extra-text-only and terminal-extra-error-only, whose
		// exact-shape refusal of additive terminal members M56
		// supersedes; the amended TestPanelBUncapturedTerminalShapeRefuses
		// pins positively that invented text/error never override.
		{"l3g-novel-header-9.9.9-only", "muse_pty_screen.go", `func museIsChromeHeaderRow(row string) bool {
	rest, ok := strings.CutPrefix(row, "  Muse Code ")`, `func museIsChromeHeaderRow(row string) bool {
	if row == "  Muse Code 9.9.9" {
		return false
	}
	rest, ok := strings.CutPrefix(row, "  Muse Code ")`, "TestNovelReleaseWithCoveredStructureParses"},
		{"l3g-additive-sentinel-forbidden", "muse_pty_grammar.go", `		for key, field := range s.Fields {
			x, exists := v[key]
			if !exists || !museShapeMatches(field, x) {
				return false
			}
		}`, `		for key, field := range s.Fields {
			x, exists := v[key]
			if !exists || !museShapeMatches(field, x) {
				return false
			}
		}
		if _, ok := v["l3g_additive_probe"]; ok && len(v) != len(s.Fields) {
			return false
		}`, "TestAdditiveOptionalFieldTolerated/additive_member_delivers"},
		{"l3g-unknown-kind-future-only", "muse_pty_startup_prefix.go", `	if code == MuseStartupPrefixUnknownRecord {`, `	if code == MuseStartupPrefixUnknownRecord && key == "runtime.future/future/" {
		return nil
	}
	if code == MuseStartupPrefixUnknownRecord {`, "TestUnknownStructureRefusesTyped/unknown_record_kind"},
		{"l3g-sibling-broken-kind-only", "muse_pty_log.go", `		f, e := readMuseLog(path)
		if e != nil {`, `		f, e := readMuseLog(path)
		if e != nil {
			if strings.Contains(path, "l3g-sibling-session") {
				return e
			}`, "TestPTYGrammarFailureDoesNotPoisonMSP/sibling_discovery_isolated"},
		{"l3g-opened-session-missing-only", "muse_pty_startup_prefix.go", `	if code == MuseStartupPrefixMissingRequired {`, `	if code == MuseStartupPrefixMissingRequired && bytes.Contains(line, []byte("11111111-1111-4111-8111-111111111111")) && detail == "payload.record.session_id" {
		return nil
	}
	if code == MuseStartupPrefixMissingRequired {`, "TestPTYStructureFailureDoesNotPoisonMSP/missing_required_member"},
		{"l3g-route-pid-zero-only", "muse_pty_startup_prefix.go", `	// A route without a positive process identity binds nothing.
	if pid <= 0 {`, `	// A route without a positive process identity binds nothing.
	if pid <= 0 && museStartupPrefixStreamID(record) != "22222222-2222-4222-8222-222222222222" {`, "TestPTYStructureFailureDoesNotPoisonMSP/malformed_required_value"},
		{"l3g-drift-startup-error-only", "muse_pty_host.go", `	if !h.idle(h.held) {
		return "", ErrMusePTYDeferred
	}`, `	if !h.idle(h.held) {
		if _, e := h.fold(); e != nil && strings.Contains(e.Error(), "runtime.future") {
			return "", fmt.Errorf("muse startup prefix drift: %w", e)
		}
		return "", ErrMusePTYDeferred
	}`, "TestPostStartupDriftIsRuntimeNotFallback"},

		// L3G rework r2 (L3G-001..004). Each row weakens exactly one gate
		// for exactly one sentinel; the named test must fail.
		{"l3g-r2-two-record-admits-only", "muse_pty_startup_prefix.go", `	if len(lines) < museStartupPrefixWindow {`, `	if len(lines) < museStartupPrefixWindow && len(lines) != 2 {`, "TestL3GStartupRequiresThreeRecords/two_valid_incomplete"},
		{"l3g-r2-future-third-skipped-only", "muse_pty_startup_prefix.go", `	for i := 0; i < checked; i++ {
		if err := museValidatePrefixRecord(lines[i], i+1); err != nil {
			return err
		}
	}`, `	for i := 0; i < checked; i++ {
		if err := museValidatePrefixRecord(lines[i], i+1); err != nil {
			if i == 2 && bytes.Contains(lines[i], []byte("runtime.future")) {
				continue
			}
			return err
		}
	}`, "TestL3GStartupRequiresThreeRecords/unknown_third_refuses"},
		{"l3g-r2-route-cwd-missing-only", "muse_pty_startup_prefix.go", `	if code == MuseStartupPrefixMissingRequired {`, `	if code == MuseStartupPrefixMissingRequired && detail == "payload.record.cwd" {
		return nil
	}
	if code == MuseStartupPrefixMissingRequired {`, "TestL3GRequiredMembersTyped/route_cwd_missing"},
		{"l3g-r2-opened-security-mode-missing-only", "muse_pty_startup_prefix.go", `	if code == MuseStartupPrefixMissingRequired {`, `	if code == MuseStartupPrefixMissingRequired && detail == "payload.record.security_mode" {
		return nil
	}
	if code == MuseStartupPrefixMissingRequired {`, "TestL3GRequiredMembersTyped/opened_security_mode_missing"},
		{"l3g-r2-display-known-ignored-only", "muse_pty_grammar.go", `			if candidate.Closed || museFieldKnownAtPath(shapes, path, k) {`, `			if k != "display" && (candidate.Closed || museFieldKnownAtPath(shapes, path, k)) {`, "TestL3GKnownVariantExactness/display_array_refuses"},
		{"l3g-r2-terminal-text-kept-only", "muse_pty_grammar.go", `		// No undeclared object member crosses the decode boundary.`, `		if strings.Join(path, ".") == "payload.event" && m["kind"] == "terminal" {
			if raw, ok := m["text"]; ok { projected["text"] = raw }
		}
		// No undeclared object member crosses the decode boundary.`, "TestL3GAdditiveDroppedBeforeDecode/terminal_text_object_ignored"},
		{"l3g-r2-terminal-run-id-kept-only", "muse_pty_grammar.go", `		// No undeclared object member crosses the decode boundary.`, `		if strings.Join(path, ".") == "payload.event" && m["kind"] == "terminal" {
			if raw, ok := m["run_id"]; ok { projected["run_id"] = raw }
		}
		// No undeclared object member crosses the decode boundary.`, "TestL3GAdditiveDroppedBeforeDecode/terminal_run_id_ignored"},
		{"l3g-astra-payload-pid-kept-only", "muse_pty_grammar.go", `		// No undeclared object member crosses the decode boundary.`, `		if strings.Join(path, ".") == "payload" && m["kind"] == "session_opened" {
			if raw, ok := m["pid"].(map[string]any); ok && raw["l3g_pid_probe"] == true { projected["pid"] = raw }
		}
		// No undeclared object member crosses the decode boundary.`, "TestL3GAdditiveDroppedBeforeDecode/opened_payload_pid_object_ignored"},
		{"l3g-astra-payload-session-kept-only", "muse_pty_grammar.go", `		// No undeclared object member crosses the decode boundary.`, `		if strings.Join(path, ".") == "payload" && m["kind"] == "session_opened" && m["session_id"] == "33333333-3333-4333-8333-333333333333" {
			projected["session_id"] = m["session_id"]
		}
		// No undeclared object member crosses the decode boundary.`, "TestL3GAdditiveDroppedBeforeDecode/opened_payload_session_id_ignored"},
		{"l3g-astra-nested-session-kept-only", "muse_pty_grammar.go", `		// No undeclared object member crosses the decode boundary.`, `		if strings.Join(path, ".") == "payload.record" && m["session_id"] == "44444444-4444-4444-8444-444444444444" {
			projected["session_id"] = m["session_id"]
		}
		// No undeclared object member crosses the decode boundary.`, "TestL3GAdditiveDroppedBeforeDecode/nested_route_session_id_ignored"},
		{"l3g-astra-unknown-refusal-waits-only", "muse_pty_host.go", `if errors.As(e, &prefix) && prefix.Code != MuseStartupPrefixIncomplete {`, `if errors.As(e, &prefix) && prefix.Code != MuseStartupPrefixIncomplete && prefix.Code != MuseStartupPrefixUnknownRecord {`, "TestL3GStartupRefusesBeforeDeadline/unknown_third"},
		{"l3g-astra-opened-event-selects-only", "muse_pty_grammar.go", `if !selectors["event/"+kind] {`, `if !selectors["event/"+kind] && !(typ == "session.opened.observed" && event == "l3g_future") {`, "TestL3GAdditiveDroppedBeforeDecode/opened_payload_event_ignored"},
		{"l3g-astra-terminal-frame-selects-only", "muse_pty_grammar.go", `ok && (p == nil || museGrammarSelectors[typ] == nil)`, `ok && (p == nil || museGrammarSelectors[typ] == nil || typ == "runtime.session" && p["event"].(map[string]any)["kind"] == "terminal")`, "TestL3GAdditiveDroppedBeforeDecode/terminal_envelope_frame_marker_ignored"},
	} {
		t.Run(m.id, func(t *testing.T) {
			path, err := filepath.Abs(m.file)
			if err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(string(b), m.old) != 1 {
				t.Fatalf("mutation anchor not unique: %s", m.old)
			}
			dir := t.TempDir()
			mutant := filepath.Join(dir, m.file)
			if err = os.WriteFile(mutant, []byte(strings.Replace(string(b), m.old, m.replacement, 1)), 0600); err != nil {
				t.Fatal(err)
			}
			overlay, _ := json.Marshal(map[string]any{"Replace": map[string]string{path: mutant}})
			overlayPath := filepath.Join(dir, "overlay.json")
			if err = os.WriteFile(overlayPath, overlay, 0600); err != nil {
				t.Fatal(err)
			}
			runChild := func() taggedtests.MutationResult {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				args := []string{"test", "-tags=mutants", "-overlay", overlayPath}
				args = append(args, ".", "-run", "^"+m.test+"$", "-count=1", "-timeout=45s", "-v")
				cmd := exec.CommandContext(ctx, "go", args...)
				out, err := cmd.CombinedOutput()
				code := -1
				if cmd.ProcessState != nil {
					code = cmd.ProcessState.ExitCode()
				}
				return taggedtests.MutationResult{Output: out, Err: err, ExitCode: code}
			}
			result := runChild()
			out, err, code := result.Output, result.Err, result.ExitCode
			t.Logf("mutant=%s exit=%d named_test=%s\n%s", m.id, code, m.test, out)
			rootTest := strings.Split(m.test, "/")[0]
			if err == nil || code != 1 || !strings.Contains(string(out), "--- FAIL: "+rootTest) || strings.Contains(string(out), "[build failed]") {
				t.Fatalf("SURVIVOR: no named behavioral kill for %s", m.id)
			}
		})
	}
}
