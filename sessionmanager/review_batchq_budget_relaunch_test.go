package sessionmanager

// Batch Q reviewer repro (rev4), not candidate source.

import (
	"context"
	"errors"
	"testing"
)

func reviewBatchQStoppedCodexImage(budget string) LegacyRecordInput {
	field := ""
	if budget != "" {
		field = `"goal_token_budget":` + budget + `,`
	}
	raw := []byte(`{
  "contract":"task-board-session-record-v3",
  "session_id":"SES-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "owner":{"kind":"parent","id":"primary"},"provider":"codex","state":"stopped",
  "created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z",
  "provider_host":{"provider":"codex","opaque":{
    "contract":"task-board.codex-provider-host-v1","pid":42,"url":"ws://127.0.0.1:1",
    "thread_id":"fixture-thread","project_dir":"/workspace","codex_home":"/fixture-home",` + field + `
    "stopped":true
  }}
}`)
	return legacyRecordImage("SES-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", raw)
}

// reviewBatchQRelaunch mirrors attachment_staging.go relaunch: the stopped
// record is replaced by the factory's prepared snapshot (budget from the launch
// selection, nil here as legacy_prepare.go admits only nil) in running state.
func reviewBatchQRelaunch(t *testing.T, codec *LegacyRecordCodec, in LegacyRecordInput) ([]byte, error) {
	t.Helper()
	base, err := codec.DecodeLegacy(context.Background(), in)
	if err != nil {
		t.Fatalf("archived stopped record must decode: %v", err)
	}
	relaunched, err := cloneRunnerRecord(base)
	if err != nil {
		t.Fatal(err)
	}
	prepared := &CodexPreparedLaunch{Snapshot: *base.ProviderHost.Snapshot.Codex}
	prepared.Snapshot.GoalTokenBudget = nil
	selected, err := prepareCodexLaunchSnapshot(prepared)
	if err != nil {
		t.Fatalf("nil selection must prepare: %v", err)
	}
	selected.Stopped = false
	relaunched.ProviderHost = ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &selected}}
	relaunched.State = SessionRunning
	return codec.EncodeMerge(context.Background(), in, relaunched)
}

// Claim: relaunching a stopped Codex record that archives an explicit budget
// must either keep the live explicit-budget refusal (baseline 7bbbb behaviour,
// since no live consumer exists) or preserve the archived value; it must not
// produce a live record with the archived budget silently erased.
// Positive neighbor: a goal-less stopped record relaunches successfully.
// Limit: codec-level mirror of the staging path; no real Codex process.
func TestReviewBatchQRelaunchDoesNotEraseArchivedBudget(t *testing.T) {
	codec := NewLegacyRecordCodec()
	if _, err := reviewBatchQRelaunch(t, codec, reviewBatchQStoppedCodexImage("")); err != nil {
		t.Fatalf("positive control: goal-less stopped relaunch refused: %v", err)
	}
	in := reviewBatchQStoppedCodexImage("7654321")
	base, err := codec.DecodeLegacy(context.Background(), in)
	if err != nil || base.ProviderHost.Snapshot.Codex.GoalTokenBudget == nil || *base.ProviderHost.Snapshot.Codex.GoalTokenBudget != 7654321 {
		t.Fatalf("archived budget not carried: %+v %v", base.ProviderHost.Snapshot.Codex, err)
	}
	raw, err := reviewBatchQRelaunch(t, codec, in)
	if err != nil {
		if !errors.Is(err, ErrHostCapabilityUnavailable) {
			t.Fatalf("relaunch refused with wrong error: %v", err)
		}
		return
	}
	restored, err := codec.DecodeLegacy(context.Background(), legacyRecordImage(in.SessionID, raw))
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.ProviderHost.Snapshot.Codex.GoalTokenBudget; got == nil || *got != 7654321 {
		t.Fatalf("relaunch passed the live codec with the archived explicit budget erased: got %v\n%s", got, raw)
	}
}
