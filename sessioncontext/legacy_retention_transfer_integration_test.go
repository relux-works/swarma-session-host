package sessioncontext

import (
	"context"
	"errors"
	sm "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Original reclamation/recovery/presence assertions exercise actual Service IO.
// Canonical retirement choices are independent injected source facts, not a
// reimplementation of registry policy or the host deletion algorithm.
const (
	legacyRetentionTransferOwnedThread     = "019f-owned-thread"
	legacyRetentionTransferUnrelatedThread = "019f-unrelated-thread"
	legacyRetentionTransferCtxID           = "CTX-260730-re0001"
	legacyRetentionTransferCodexContract   = "task-board.codex-provider-host-v1"
)

var legacyRetentionTransferClock = materializationTestClock

// TestVanishedPayloadIsRecoveredInTheRegistry closes the loop the adapters cannot close themselves.
func TestVanishedPayloadIsRecoveredInTheRegistry(t *testing.T) {
	record := legacyRetentionTransferContext(legacyRetentionTransferCtxID, true)
	record.State = "ready"
	authority := legacyRetentionTransferNewAuthority(record)
	manager := legacyRetentionTransferOpen(t, legacyRetentionTransferLayout(t, "recovery"), authority)

	refusal := contextPayloadVanished(legacyRetentionTransferCtxID, &ContextError{
		Code:    "context_payload_vanished",
		Message: "context " + legacyRetentionTransferCtxID + " has no codex payload on this machine",
	})
	err := manager.RecoverUnusable(context.Background(), refusal)

	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != "context_payload_vanished" {
		t.Fatalf("recovery replaced the refusal instead of annotating it: %v", err)
	}
	if !strings.Contains(typed.Message, "marked stale") ||
		!strings.Contains(typed.Message, "rebuilt from cold") {
		t.Fatalf("the caller was not told what the recovery did: %s", typed.Message)
	}
	recovered, err := authority.Context(context.Background(), legacyRetentionTransferCtxID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != "stale" ||
		recovered.StateReason != "payload_vanished" {
		t.Fatalf("the registry still serves the vanished payload: %+v", recovered)
	}
}

// TestFailedRecoveryStillReportsTheOriginalRefusal keeps the bookkeeping from swallowing the answer.
func TestFailedRecoveryStillReportsTheOriginalRefusal(t *testing.T) {
	manager := legacyRetentionTransferOpen(
		t,
		legacyRetentionTransferLayout(t, "recovery-failure"),
		legacyRetentionTransferFailingAuthority(t, errors.New("authority is unreachable")),
	)
	refusal := contextPayloadVanished(legacyRetentionTransferCtxID, &ContextError{
		Code:    "context_payload_vanished",
		Message: "context " + legacyRetentionTransferCtxID + " has no codex payload on this machine",
	})

	err := manager.RecoverUnusable(context.Background(), refusal)

	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != "context_payload_vanished" {
		t.Fatalf("a failed recovery hid the refusal: %v", err)
	}
	if !strings.Contains(typed.Message, "still recorded as ready") {
		t.Fatalf("a failed recovery did not say the registry is out of step: %s", typed.Message)
	}
}

// TestCodexReclamationDeletesLiveAndArchivedRolloutsAndConverges is the sweep end to end.
func TestCodexReclamationDeletesLiveAndArchivedRolloutsAndConverges(t *testing.T) {
	fixture := legacyRetentionTransferNewCodexFixture(t, nil)

	report, err := fixture.manager.ReclaimContextPayloads(
		context.Background(), ContextReclamationRequest{})
	if err != nil {
		t.Fatal(err)
	}

	outcome := fixture.legacyRetentionTransferOutcome(t, report)
	if outcome.Status != ContextReclamationDone {
		t.Fatalf("the owned payload was not reclaimed: %+v", outcome)
	}
	if outcome.Files != 1 || outcome.ArchivedFiles != 1 {
		t.Fatalf("reclamation counts = live %d archived %d, want 1 and 1", outcome.Files, outcome.ArchivedFiles)
	}
	if len(report.Retired) != 1 || report.Retired[0] != fixture.ctxID {
		t.Fatalf("the sweep retired %v, want just the expired context", report.Retired)
	}

	legacyRetentionTransferAssertGone(t, fixture.live, "the live rollout the registry no longer needs")
	legacyRetentionTransferAssertGone(t, fixture.archived, "the archived rollout, which Codex moved rather than deleted")
	legacyRetentionTransferAssertSurvives(t, fixture.otherLive, "an unrelated live rollout")
	legacyRetentionTransferAssertSurvives(t, fixture.otherArch, "an unrelated archived rollout")

	record := fixture.legacyRetentionTransferRecord(t)
	if record.State != "retired" {
		t.Fatalf("the reclaimed context is %s, want retired", record.State)
	}
	if !legacyRetentionTransferRecorded(record.Reclamation) || !record.Reclamation.ArchivedIncluded ||
		record.Reclamation.Files != 2 {
		t.Fatalf("the deletion was not recorded as evidence: %+v", record.Reclamation)
	}

	// Convergence: the second pass must not ask again. Without this the sweep
	// grows by the whole history of what the board has ever reclaimed.
	second, err := fixture.manager.ReclaimContextPayloads(
		context.Background(), ContextReclamationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Outcomes) != 0 || len(second.Retired) != 0 {
		t.Fatalf("the sweep re-reported an already reclaimed payload: %+v", second)
	}
}

// TestCodexReclamationDryRunTouchesNothing checks that the safe mode is safe.
func TestCodexReclamationDryRunTouchesNothing(t *testing.T) {
	fixture := legacyRetentionTransferNewCodexFixture(t, nil)

	report, err := fixture.manager.ReclaimContextPayloads(
		context.Background(), ContextReclamationRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}

	outcome := fixture.legacyRetentionTransferOutcome(t, report)
	if outcome.Status != ContextReclamationPlanned {
		t.Fatalf("a dry run reported %s", outcome.Status)
	}
	if outcome.Files != 1 || outcome.ArchivedFiles != 1 {
		t.Fatalf("a dry run miscounted the payload: %+v", outcome)
	}
	legacyRetentionTransferAssertSurvives(t, fixture.live, "a dry run must delete nothing")
	legacyRetentionTransferAssertSurvives(t, fixture.archived, "a dry run must delete nothing")
	if record := fixture.legacyRetentionTransferRecord(t); record.State != "ready" {
		t.Fatalf("a dry run moved the record to %s", record.State)
	}
}

// TestCodexReclamationRefusesAnAdoptedThread is the destructive-deletion guard.
func TestCodexReclamationRefusesAnAdoptedThread(t *testing.T) {
	fixture := legacyRetentionTransferNewCodexFixture(t, func(snapshot *sm.CodexRunnerSnapshot) {
		snapshot.Adopted = true
	})

	report, err := fixture.manager.ReclaimContextPayloads(
		context.Background(), ContextReclamationRequest{})
	if err != nil {
		t.Fatal(err)
	}

	outcome := fixture.legacyRetentionTransferOutcome(t, report)
	if outcome.Status != ContextReclamationSkipped {
		t.Fatalf("an adopted thread was not skipped: %+v", outcome)
	}
	if !strings.Contains(outcome.Detail, "adopted") {
		t.Fatalf("the skip does not say why: %s", outcome.Detail)
	}
	legacyRetentionTransferAssertSurvives(t, fixture.live, "an adopted thread's live rollout")
	legacyRetentionTransferAssertSurvives(t, fixture.archived, "an adopted thread's archived rollout")
	if record := fixture.legacyRetentionTransferRecord(t); legacyRetentionTransferRecorded(record.Reclamation) {
		t.Fatal("a skipped reclamation was recorded as performed")
	}
}

// TestCodexReclamationRefusesToDeleteThroughASymlink covers the path the adoption store actually creates.
func TestCodexReclamationRefusesToDeleteThroughASymlink(t *testing.T) {
	fixture := legacyRetentionTransferNewCodexFixture(t, nil)
	real := filepath.Join(t.TempDir(), "real-rollout.jsonl")
	if err := os.WriteFile(real, []byte("operator-owned\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(fixture.live); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, fixture.live); err != nil {
		t.Fatal(err)
	}

	report, err := fixture.manager.ReclaimContextPayloads(
		context.Background(), ContextReclamationRequest{})
	if err != nil {
		t.Fatal(err)
	}

	outcome := fixture.legacyRetentionTransferOutcome(t, report)
	if outcome.Status != ContextReclamationSkipped {
		t.Fatalf("a symlinked rollout was not skipped: %+v", outcome)
	}
	if !strings.Contains(outcome.Detail, "symlink") {
		t.Fatalf("the skip does not name the symlink: %s", outcome.Detail)
	}
	legacyRetentionTransferAssertSurvives(t, real, "the operator's own rollout behind the symlink")
	legacyRetentionTransferAssertSurvives(t, fixture.archived, "the archived rollout of a skipped reclamation")
	if record := fixture.legacyRetentionTransferRecord(t); legacyRetentionTransferRecorded(record.Reclamation) {
		t.Fatal("a skipped reclamation was recorded as performed")
	}
}

// TestCodexReclamationDoesNotConfirmAPartialDeletion is the honesty case.
func TestCodexReclamationDoesNotConfirmAPartialDeletion(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions, so the deletion cannot be made to fail")
	}
	fixture := legacyRetentionTransferNewCodexFixture(t, nil)
	archiveDir := filepath.Dir(fixture.archived)
	if err := os.Chmod(archiveDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(archiveDir, 0o700) })

	report, err := fixture.manager.ReclaimContextPayloads(
		context.Background(), ContextReclamationRequest{})
	if err != nil {
		t.Fatal(err)
	}

	outcome := fixture.legacyRetentionTransferOutcome(t, report)
	if outcome.Status != ContextReclamationFailed {
		t.Fatalf("a partial deletion reported %s: %+v", outcome.Status, outcome)
	}
	legacyRetentionTransferAssertSurvives(t, fixture.archived, "the archived rollout that could not be removed")
	record := fixture.legacyRetentionTransferRecord(t)
	if legacyRetentionTransferRecorded(record.Reclamation) {
		t.Fatal("a partial deletion was confirmed, so no later sweep will finish it")
	}
	// The record is still retired, so the next sweep re-plans exactly this
	// deletion rather than losing track of it.
	if record.State != "retired" {
		t.Fatalf("a failed reclamation left the record at %s", record.State)
	}
}

// TestCodexReclamationSkipsPayloadsThisMachineDoesNotHold is the multi-host case, and the ordinary one.
func TestCodexReclamationSkipsPayloadsThisMachineDoesNotHold(t *testing.T) {
	fixture := legacyRetentionTransferNewCodexFixture(t, nil)
	delete(fixture.manager.materializations, fixture.ctxID)

	report, err := fixture.manager.ReclaimContextPayloads(
		context.Background(), ContextReclamationRequest{})
	if err != nil {
		t.Fatal(err)
	}

	outcome := fixture.legacyRetentionTransferOutcome(t, report)
	if outcome.Status != ContextReclamationSkipped ||
		!strings.Contains(outcome.Detail, "no materialization on this machine") {
		t.Fatalf("a foreign payload was not skipped cleanly: %+v", outcome)
	}
	legacyRetentionTransferAssertSurvives(t, fixture.live, "a payload this machine holds no handle for")
	if record := fixture.legacyRetentionTransferRecord(t); legacyRetentionTransferRecorded(record.Reclamation) {
		t.Fatal("a machine that deleted nothing recorded a reclamation")
	}
}

// TestReclamationSweepSurfacesClaudeBasesBeforeTheDeletionHorizon is the other provider, and the other direction.
func TestReclamationSweepSurfacesClaudeBasesBeforeTheDeletionHorizon(t *testing.T) {
	fixture := legacyRetentionTransferNewCodexFixture(t, nil)
	nearHorizon := legacyRetentionTransferContext("CTX-260730-re0002", true)
	nearHorizon.Provider = "claude"
	nearHorizon.State = "ready"
	// Published 27 days ago against a documented 30-day horizon: inside the
	// default seven-day warning window and not yet lost.
	nearHorizon.ReadyAt = legacyRetentionTransferClock.Add(-27 * 24 * time.Hour)
	fixture.store.contexts[nearHorizon.ID] = nearHorizon
	fixture.store.expiring = []sm.ContextRetentionOutlook{EvaluateContextRetention(nearHorizon, sm.RetentionPolicy{Now: legacyRetentionTransferClock})}

	report, err := fixture.manager.ReclaimContextPayloads(
		context.Background(), ContextReclamationRequest{})
	if err != nil {
		t.Fatal(err)
	}

	var surfaced *sm.ContextRetentionOutlook
	for i := range report.Expiring {
		if report.Expiring[i].CtxID == nearHorizon.ID {
			surfaced = &report.Expiring[i]
		}
	}
	if surfaced == nil {
		t.Fatalf("a Claude base three days from deletion was not surfaced: %+v", report.Expiring)
	}
	if surfaced.Risk != "expiring" {
		t.Fatalf("the near-horizon base was classified %s", surfaced.Risk)
	}
	if !strings.Contains(surfaced.Advice, "instead of discovering the loss on use") {
		t.Fatalf("the warning gives no action: %s", surfaced.Advice)
	}
	// It must be surfaced, never collected: the board does not own this payload.
	for _, outcome := range report.Outcomes {
		if outcome.CtxID == nearHorizon.ID {
			t.Fatalf("the board planned to reclaim a Claude payload it does not own: %+v", outcome)
		}
	}
}

// TestCodexPresenceProbeSeparatesAbsenceFromUnverifiability keeps the two verdicts apart at the source.
func TestCodexPresenceProbeSeparatesAbsenceFromUnverifiability(t *testing.T) {
	home := t.TempDir()
	legacyRetentionTransferWriteLive(t, home, legacyRetentionTransferOwnedThread, "live\n")

	present := probeCodexPayloadPresence(
		legacyRetentionTransferCtxID, home, legacyRetentionTransferOwnedThread, legacyRetentionTransferClock)
	if present.Presence != "present" {
		t.Fatalf("a live rollout probed as %s", present.Presence)
	}

	missing := probeCodexPayloadPresence(
		legacyRetentionTransferCtxID, home, "019f-nothing-here", legacyRetentionTransferClock)
	if missing.Presence != "vanished" {
		t.Fatalf("an absent rollout probed as %s", missing.Presence)
	}

	unverifiable := probeCodexPayloadPresence(
		legacyRetentionTransferCtxID, "", legacyRetentionTransferOwnedThread, legacyRetentionTransferClock)
	if unverifiable.Presence != "unverifiable" {
		t.Fatalf("an unrunnable probe reported %s", unverifiable.Presence)
	}

	// An unrunnable probe must refuse rather than fall through to another
	// candidate: the probe will not run for the sibling either.
	record := presenceProbeRecord(legacyRetentionTransferCtxID, "codex", time.Time{})
	err := RequireContextPayloadPresent(
		record, unverifiable, "auto", sm.RetentionPolicy{})
	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != "context_payload_unverified" {
		t.Fatalf("an unverifiable probe did not fail closed: %v", err)
	}
	if typed.RetryNextCandidate {
		t.Fatal("an unverifiable probe offered the next candidate it also cannot check")
	}
	if contextPayloadVanished(legacyRetentionTransferCtxID, err) != err {
		t.Fatal("an unverified serve was routed into the stale-and-rebuild recovery")
	}
}

// Missing payload is detected by the real public Codex assay before any RPC;
// the declared host policy routes the original typed payload-vanished refusal.
func TestCodexAssayRefusesAVanishedBaseInsteadOfBlamingIsolation(t *testing.T) {
	_, err := sm.NewCodexHostFactory(sm.Layout{}, ProviderPolicy{}).RunContextAssay(
		context.Background(),
		legacyRetentionTransferCodexHostRecord(
			t, "http://127.0.0.1:1", t.TempDir(), legacyRetentionTransferOwnedThread, "turn-owned",
		),
		sm.CodexContextAssayRequest{
			BaseCtxID:    legacyRetentionTransferCtxID,
			Prompt:       "what does this base know?",
			OutputSchema: []byte(`{"type":"object"}`),
		},
	)

	var typed *ContextError
	if !errors.As(err, &typed) || typed.Code != "context_payload_vanished" {
		t.Fatalf("the assay did not report a vanished base as such: %v", err)
	}
}
