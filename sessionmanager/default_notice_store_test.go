package sessionmanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSessionNoticeTextGateRefusesEveryGoalAcknowledgementMarker attacks the
// gate rather than reading it.
//
// The production call site is claudeHost.DeliverSessionNotice, which calls
// ValidateSessionNoticeText before its first ptmx.Write. The class the gate
// covers is "text that, once echoed by the TUI into the output stream the goal
// scanner reads, is indistinguishable from the provider acknowledging a goal
// revision". Every marker any goal waiter arms is therefore driven through it,
// in the casing and embedding a real summary would carry.
func TestSessionNoticeTextGateRefusesEveryGoalAcknowledgementMarker(t *testing.T) {
	armed := map[string][]string{
		"apply-conditioned": {"goal set:", "goal updated:", "goal active:", "goal:"},
		"clear-and-status":  {"goal cleared", "no goal set", "goal active:"},
		"owner-evaluator":   {"goal achieved", "goal could not be achieved"},
		"shared-failure":    {"unknown command"},
	}
	for waiter, markers := range armed {
		for _, marker := range markers {
			for _, shape := range []string{
				marker,
				"task-board run RUN-1 completed: " + marker + " tbg-deadbeef",
				"task-board run RUN-1 failed: " + strings.ToUpper(marker),
				"task-board run RUN-1 blocked: producer reported " + strings.ToTitle(marker) + " already",
			} {
				if err := ValidateSessionNoticeText(shape); err == nil {
					t.Fatalf(
						"%s waiter marker %q admitted through notice text %q; "+
							"claudeHost.DeliverSessionNotice would type a forged acknowledgement",
						waiter, marker, shape,
					)
				}
			}
		}
	}
	if err := ValidateSessionNoticeText(""); err == nil {
		t.Fatal("empty notice text admitted")
	}
	if err := ValidateSessionNoticeText("   "); err == nil {
		t.Fatal("blank notice text admitted")
	}
	if err := ValidateSessionNoticeText(
		"task-board run RUN-260904-aaaaaa completed for TASK-1 (developer/claude): ready for review",
	); err != nil {
		t.Fatalf("an ordinary notice was refused: %v", err)
	}
}

// TestSessionNoticeCompositionNeutralizesOperatorSuppliedMarkers proves the
// recovery half: a run summary that happens to contain a marker still produces a
// deliverable notice instead of a permanently stuck inbox entry.
//
// The summaries here are the ones a real board produces, not synthetic ones: a
// goal-bound run records its provider status verbatim.
func TestSessionNoticeCompositionNeutralizesOperatorSuppliedMarkers(t *testing.T) {
	for _, summary := range []string{
		"Goal set: tbg-2f9a11 deliver the reviewer verdict",
		"provider reported Goal achieved before the board accepted it",
		"No goal set. Usage: /goal <condition>",
		"Unknown command",
		"GOAL UPDATED: revision 4",
	} {
		text := FormatSessionNoticeBatch([]SessionNotice{{
			Contract:    SessionNoticeContract,
			SessionID:   "SES-abc",
			RunID:       "RUN-260904-aaaaaa",
			ElementID:   "TASK-260904-aaaaaa",
			Role:        "developer",
			Runtime:     "claude",
			State:       "completed",
			Summary:     summary,
			NextCommand: "task-board spawn status RUN-260904-aaaaaa",
		}})
		if err := ValidateSessionNoticeText(text); err != nil {
			t.Fatalf("composed notice for summary %q is undeliverable: %v (text=%q)", summary, err, text)
		}
		if !strings.Contains(text, "RUN-260904-aaaaaa") {
			t.Fatalf("composed notice lost its run identity: %q", text)
		}
	}
}

func TestFormatSessionNoticeBatchCoalescesBurstIntoOneMessage(t *testing.T) {
	var notices []SessionNotice
	for _, id := range []string{"RUN-1", "RUN-2", "RUN-3"} {
		notices = append(notices, SessionNotice{
			Contract:    SessionNoticeContract,
			SessionID:   "SES-abc",
			RunID:       id,
			ElementID:   "TASK-" + id,
			Role:        "developer",
			Runtime:     "claude",
			State:       "completed",
			NextCommand: "task-board spawn status " + id,
		})
	}
	text := FormatSessionNoticeBatch(notices)
	if strings.Count(text, "\n") != 0 {
		t.Fatalf("coalesced notice is not a single line: %q", text)
	}
	for _, id := range []string{"RUN-1", "RUN-2", "RUN-3"} {
		if !strings.Contains(text, id) {
			t.Fatalf("coalesced notice dropped %s: %q", id, text)
		}
	}
	if !strings.Contains(text, "3 reached a terminal state") {
		t.Fatalf("coalesced notice does not report the burst size: %q", text)
	}
	if FormatSessionNoticeBatch(nil) != "" {
		t.Fatal("an empty burst produced text")
	}
}

// TestFormatSessionNoticeBatchNamesEveryRunBeyondTheDetailLimit is the
// information-loss negative. DeliverPendingSessionNotices removes EVERY notice
// it read, so this one message is the only channel that reports a given run
// reached a terminal state. A burst wider than sessionNoticeBurstLimit must
// therefore still name every run id and its state; only the per-run detail —
// element, role, runtime, summary — may be dropped. This board runs
// spawn.max_parallel = 20, so the overflow is a real fan-out shape, not a
// theoretical one.
func TestFormatSessionNoticeBatchNamesEveryRunBeyondTheDetailLimit(t *testing.T) {
	const burst = 20
	if burst <= sessionNoticeBurstLimit {
		t.Fatalf("the burst must exceed the detail limit %d to test the overflow", sessionNoticeBurstLimit)
	}
	var notices []SessionNotice
	for i := 0; i < burst; i++ {
		id := fmt.Sprintf("RUN-260904-a%05d", i)
		notices = append(notices, SessionNotice{
			Contract:    SessionNoticeContract,
			SessionID:   "SES-abc",
			RunID:       id,
			ElementID:   "TASK-260904-b" + fmt.Sprintf("%05d", i),
			Role:        "developer",
			Runtime:     "claude",
			State:       "failed",
			Summary:     "the developer run exited non-zero and left the task in development",
			NextCommand: "task-board spawn status " + id,
		})
	}
	text := FormatSessionNoticeBatch(notices)
	if strings.Count(text, "\n") != 0 {
		t.Fatalf("coalesced notice is not a single line: %q", text)
	}
	for i := 0; i < burst; i++ {
		id := fmt.Sprintf("RUN-260904-a%05d", i)
		if !strings.Contains(text, id) {
			t.Fatalf(
				"the coalesced notice dropped %s (entry %d of %d); the pump deletes every notice it "+
					"reads, so a run id missing here is never reported again: %q",
				id, i+1, burst, text,
			)
		}
	}
	// Only the detail is bounded: the runs beyond the limit must not carry the
	// full element/role/runtime/summary rendering, or the cap does nothing.
	if got := strings.Count(text, "(developer/claude)"); got != sessionNoticeBurstLimit {
		t.Fatalf(
			"detailed entries = %d, want exactly sessionNoticeBurstLimit=%d: %q",
			got, sessionNoticeBurstLimit, text,
		)
	}
	if !strings.Contains(text, fmt.Sprintf("+%d more", burst-sessionNoticeBurstLimit)) {
		t.Fatalf("the overflow count is not reported: %q", text)
	}
	// The overflow entries are composed text like every other field, so the
	// acknowledgement gate must still accept the whole message.
	if err := ValidateSessionNoticeText(text); err != nil {
		t.Fatalf("a wide coalesced burst no longer passes the acknowledgement gate: %v", err)
	}
}

// TestFormatSessionNoticeBatchSanitizesOverflowEntries keeps the overflow tail
// on the same footing as the detailed entries: a hostile run id or state must
// not reach the PTY as a goal acknowledgement marker just because its notice
// happened to land ninth in the burst.
func TestFormatSessionNoticeBatchSanitizesOverflowEntries(t *testing.T) {
	var notices []SessionNotice
	for i := 0; i < sessionNoticeBurstLimit; i++ {
		notices = append(notices, SessionNotice{
			Contract:  SessionNoticeContract,
			SessionID: "SES-abc",
			RunID:     fmt.Sprintf("RUN-260904-c%05d", i),
			State:     "completed",
		})
	}
	notices = append(notices, SessionNotice{
		Contract:  SessionNoticeContract,
		SessionID: "SES-abc",
		RunID:     "RUN-260904-Goal set: alignment",
		State:     "Goal achieved",
	})
	text := FormatSessionNoticeBatch(notices)
	if err := ValidateSessionNoticeText(text); err != nil {
		t.Fatalf("an overflow entry carried a goal acknowledgement marker into the composed notice: %v", err)
	}
}

// TestSessionNoticeInboxKeepsUnreadableEntriesAndReportsTheFailure separates an
// absence from a failure to read. A malformed entry must not be reported as an
// empty inbox and must not be handed back for deletion, because deleting it
// would convert a corrupted queue into a silently empty one.
func TestSessionNoticeInboxKeepsUnreadableEntriesAndReportsTheFailure(t *testing.T) {
	inbox := newSessionNoticeInbox(filepath.Join(t.TempDir(), "notices"))
	if err := inbox.enqueue(SessionNotice{
		SessionID: "SES-abc",
		RunID:     "RUN-1",
		State:     "completed",
		CreatedAt: time.Unix(1, 0).UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(inbox.dir, "SES-abc")
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000002-ffffff.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	notices, paths, err := inbox.pending("SES-abc")
	if err == nil {
		t.Fatal("a malformed inbox entry was reported as a clean read")
	}
	if len(notices) != 1 || len(paths) != 1 {
		t.Fatalf("readable notices = %d (paths %d), want 1/1", len(notices), len(paths))
	}
	if !strings.HasSuffix(paths[0], ".json") || strings.Contains(paths[0], "ffffff") {
		t.Fatalf("the unreadable entry was offered for deletion: %v", paths)
	}
	inbox.remove(paths)
	if _, err := os.Stat(filepath.Join(dir, "00000000000000000002-ffffff.json")); err != nil {
		t.Fatalf("the unreadable entry was deleted anyway: %v", err)
	}
}

// TestSessionNoticeInboxRefusesUnsupportedContractsAndPathEscapes keeps the
// producer boundary honest: the session id arrives from an ordinary board
// process's environment, so it is not trusted to be a manager-minted handle.
func TestSessionNoticeInboxRefusesUnsupportedContractsAndPathEscapes(t *testing.T) {
	inbox := newSessionNoticeInbox(filepath.Join(t.TempDir(), "notices"))
	for _, sessionID := range []string{"", "   ", "../escape", "SES/abc", "SES abc", strings.Repeat("a", 129)} {
		if err := inbox.enqueue(SessionNotice{
			SessionID: sessionID,
			RunID:     "RUN-1",
			State:     "completed",
		}); err == nil {
			t.Fatalf("session id %q was accepted as an inbox path segment", sessionID)
		}
	}
	if err := inbox.enqueue(SessionNotice{SessionID: "SES-abc", State: "completed"}); err == nil {
		t.Fatal("a notice with no run identity was accepted")
	}
	if err := inbox.enqueue(SessionNotice{SessionID: "SES-abc", RunID: "RUN-1"}); err == nil {
		t.Fatal("a notice with no terminal state was accepted")
	}

	if err := inbox.enqueue(SessionNotice{SessionID: "SES-abc", RunID: "RUN-1", State: "completed"}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(inbox.dir, "SES-abc")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		var notice map[string]any
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &notice); err != nil {
			t.Fatal(err)
		}
		notice["contract"] = "task-board-session-notice-v99"
		rewritten, err := json.Marshal(notice)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, rewritten, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	notices, paths, err := inbox.pending("SES-abc")
	if err == nil {
		t.Fatal("a notice from an unknown contract version was accepted as readable")
	}
	if len(notices) != 0 || len(paths) != 0 {
		t.Fatalf("unknown-contract notice was delivered anyway: %d/%d", len(notices), len(paths))
	}
}
