//go:build unix

package sessionmanager

import (
	"context"
	"strings"
	"testing"
)

// TestManagerHoldsNoticesAcrossAnOwnerSlotTakeoverAndReplaysThemOnResume pins
// the composition of two planes that were written independently: the in-session
// notice pump and the confirmed owner-slot takeover.
//
// A takeover stops the incumbent through stopLocked while holding m.mu, and the
// pump captures its targets under that same lock but writes OUTSIDE it. Three
// separate rules therefore have to survive the merge, and each one is asserted
// here against the production entry points (Manager.TakeOverOwnerSlot,
// Manager.DeliverPendingSessionNotices, Manager.Attach):
//
//   - an evicted session is never typed into. Its terminal is being handed to
//     another provider; a notice arriving there is noise at best and a write
//     into a stopped host at worst;
//   - its notices are not dropped. Delete-on-success is what makes the pump
//     safe, and a stopped-but-resumable session is not a delivery failure, so
//     the queue must simply stay put;
//   - the provider that took the slot does not INHERIT them. The inbox is
//     addressed to a session, not to the owner slot, and the new holder never
//     spawned those runs.
//
// The replay half then drives the same evicted handle back through resume and
// asserts the whole burst arrives as one message. That is the only reason
// holding the notices is correct rather than merely tidy: the evicted handle
// stays resumable, so "held" really does mean "delivered later".
func TestManagerHoldsNoticesAcrossAnOwnerSlotTakeoverAndReplaysThemOnResume(t *testing.T) {
	layout := legacyPhysicalLayout(t, "notice-takeover")
	if err := legacyPhysicalPrepareDirectories(t, layout); err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	codex := legacyPhysicalNewFactory(t, "codex")
	// The incumbent is resumed at the end of this test, so its host must carry a
	// real codex native snapshot: resume decodes the thread id out of it.
	codex.snapshotFor = func(sessionID string) RunnerSnapshot {
		return legacyPhysicalSnapshot(t, "codex", sessionID)
	}
	claude := legacyPhysicalNewFactory(t, "claude")
	manager, err := legacyPhysicalNewHost(t, layout, codex, claude)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}

	incumbent, err := manager.Attach(context.Background(), AttachRequest{
		Owner: OwnerKey("parent/primary"), Provider: "codex",
		ProjectDir: project, NamespaceFingerprint: string(layout.Namespace),
	})
	if err != nil {
		t.Fatal(err)
	}
	sessionID := incumbent.Session.SessionID
	incumbentHost := codex.only(t)

	runIDs := []string{"RUN-260904-tko001", "RUN-260904-tko002"}
	for _, runID := range runIDs {
		if err := EnqueueSessionNotice(layout, SessionNotice{
			SessionID:   sessionID,
			RunID:       runID,
			ElementID:   "TASK-260904-aaaaaa",
			Role:        "developer",
			Runtime:     "codex",
			State:       "completed",
			Summary:     "ready for review",
			NextCommand: "task-board spawn status " + runID,
		}); err != nil {
			t.Fatal(err)
		}
	}

	view, err := manager.Session(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	evicted, err := manager.TakeOverOwnerSlot(context.Background(), TakeoverRequest{
		Owner:     OwnerKey("parent/primary"),
		Provider:  "claude",
		SessionID: sessionID,
		Observed:  ObservationOf(view),
	})
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if evicted.State != SessionStopped {
		t.Fatalf("evicted state = %q, want stopped", evicted.State)
	}

	// Rule 1 and 2: the pump must skip the evicted session and keep its queue.
	manager.DeliverPendingSessionNotices(context.Background())
	if delivered := incumbentHost.delivered(); len(delivered) != 0 {
		t.Fatalf("the evicted session was typed into: %v", delivered)
	}
	pending, _, err := manager.options.Notices.(*SessionNoticeStore).inbox.pending(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != len(runIDs) {
		t.Fatalf("held notices = %d, want %d (an eviction is not a delivery failure)", len(pending), len(runIDs))
	}

	// Rule 3: the provider that took the slot must not inherit them.
	successor, err := manager.Attach(context.Background(), AttachRequest{
		Owner: OwnerKey("parent/primary"), Provider: "claude",
		ProjectDir: project, NamespaceFingerprint: string(layout.Namespace),
	})
	if err != nil {
		t.Fatal(err)
	}
	if successor.Session.SessionID == sessionID {
		t.Fatalf("takeover reused the evicted session id %q", sessionID)
	}
	manager.DeliverPendingSessionNotices(context.Background())
	if delivered := claude.only(t).delivered(); len(delivered) != 0 {
		t.Fatalf("the successor session inherited notices addressed to %s: %v", sessionID, delivered)
	}
	if pending, _, err := manager.options.Notices.(*SessionNoticeStore).inbox.pending(sessionID); err != nil || len(pending) != len(runIDs) {
		t.Fatalf("notices left the evicted queue without being delivered: %d %v", len(pending), err)
	}

	// Replay: the evicted handle is resumable, and resuming it is what turns
	// "held" into "delivered".
	if _, err := manager.Stop(context.Background(), successor.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	resumed, err := manager.Attach(context.Background(), AttachRequest{
		Owner: OwnerKey("parent/primary"), Provider: "codex",
		ProjectDir: project, NamespaceFingerprint: string(layout.Namespace),
		Resume: true, ResumeSessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("resuming the evicted handle: %v", err)
	}
	if resumed.Session.SessionID != sessionID {
		t.Fatalf("resume produced session %q, want %q", resumed.Session.SessionID, sessionID)
	}
	manager.DeliverPendingSessionNotices(context.Background())
	// Resume relaunches the provider around the stored native identity, so the
	// host object is new. The notices are addressed to the SESSION, which is the
	// point: they survived both the eviction and the process behind it.
	delivered := codex.only(t).delivered()
	if len(delivered) != 1 {
		t.Fatalf("replay produced %d messages, want 1 coalesced notice: %v", len(delivered), delivered)
	}
	for _, runID := range runIDs {
		if !strings.Contains(delivered[0], runID) {
			t.Fatalf("replay dropped %s: %q", runID, delivered[0])
		}
	}
	if pending, _, err := manager.options.Notices.(*SessionNoticeStore).inbox.pending(sessionID); err != nil || len(pending) != 0 {
		t.Fatalf("replayed notices were not drained: %d %v", len(pending), err)
	}
}
