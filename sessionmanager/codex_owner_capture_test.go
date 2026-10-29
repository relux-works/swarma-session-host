package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func codexOwnerCapturedFixture(t *testing.T, owner *OwnerRunContext) CapturedOwnerRun {
	t.Helper()
	in := codexOwnerFactImage(t, owner)
	record, err := NewLegacyRecordCodec().DecodeLegacy(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	var wire legacyRunnerRecord
	if err := json.Unmarshal(in.Bytes, &wire); err != nil {
		t.Fatal(err)
	}
	var archived legacyCodexSnapshot
	if err := json.Unmarshal(wire.ProviderHost.Opaque, &archived); err != nil {
		t.Fatal(err)
	}
	archived.Stopped = false
	projected := *copyRunnerSnapshot(record.ProviderHost.Snapshot).Codex
	projected.Stopped = false
	record.State = SessionRunning
	record.ProviderHost.Snapshot.Codex = &projected
	wire.State = SessionRunning
	wire.ProviderHost.Opaque, err = json.Marshal(archived)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	// This is an isolated original-format data cut, NOT a live codec admission.
	// The actual native constructor captures it without opening a connection.
	native, err := newCodexRunner(projected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(native.cancel)
	input, err := newSessionInput(context.Background(), record.SessionID, 1, native)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(input.epochCancel)
	return CapturedOwnerRun{Lease: ControllerLease{SessionID: record.SessionID, Epoch: 1}, Input: input, Record: record, Committed: legacyRecordImage(record.SessionID, raw), Native: native}
}

// Committed, captured and actual native metadata must agree. This pure reader
// neither grants a current lease nor enables live owner admission. The ordinary
// run-labeled neighbor has no mode fact; actual live codec refusal stays intact.
func TestCodexOwnerRunCapturedFact(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []*OwnerRunContext{nil, codexOwnerFactFixture()} {
		capture := codexOwnerCapturedFixture(t, owner)
		got, err := ReadCapturedCodexOwnerRun(ctx, capture)
		if err != nil || !reflect.DeepEqual(got.OwnerRun, owner) {
			t.Fatalf("actual native/committed source changed: %#v / %v", got.OwnerRun, err)
		}
		_, liveErr := NewLegacyRecordCodec().DecodeLegacy(ctx, capture.Committed)
		if owner == nil {
			if liveErr != nil {
				t.Fatalf("ordinary live neighbor refused: %v", liveErr)
			}
			continue
		}
		if !errors.Is(liveErr, ErrHostCapabilityUnavailable) {
			t.Fatalf("metadata accessor waived live guard: %v", liveErr)
		}
		got.OwnerRun.RunID = "RUN-copy"
		got.OwnerRun.RuntimeControl[0] = ' '
		again, err := ReadCapturedCodexOwnerRun(ctx, capture)
		if err != nil || !reflect.DeepEqual(again.OwnerRun, owner) {
			t.Fatal("returned fact aliases native/committed state", err)
		}
		capture.Native.(*codexRunner).snapshot.OwnerRun.TaskID = "foreign-task"
		if got.OwnerRun.TaskID == "foreign-task" || owner.TaskID == "foreign-task" {
			t.Fatal("native mutation aliases returned/source fact")
		}
	}
}

// Missing, malformed or disagreeing facts never become owner mode. Shape-only
// epoch0 is refused here; live replaced-holder admission belongs to the host
// forwarding port and is not claimed by this read-only captured-data test.
func TestCodexOwnerRunCapturedFactRefusals(t *testing.T) {
	for _, name := range []string{"epoch", "session", "sha", "provider", "stopped", "missing-fact", "foreign-run", "foreign-task", "foreign-context", "foreign-runtime", "foreign-thread", "foreign-turn", "foreign-budget", "malformed-owner", "missing-plane"} {
		t.Run(name, func(t *testing.T) {
			capture := codexOwnerCapturedFixture(t, codexOwnerFactFixture())
			native := capture.Native.(*codexRunner)
			want := ErrLegacyViewStale
			switch name {
			case "epoch":
				capture.Lease.Epoch = 0
				want = ErrStaleInputController
			case "session":
				capture.Record.SessionID = "foreign-session"
				want = ErrStaleInputController
			case "sha":
				capture.Committed.SHA256 = "foreign-digest"
			case "provider":
				capture.Record.Provider = "muse"
			case "stopped":
				capture.Record.State = SessionStopped
				want = ErrStaleInputController
			case "missing-fact":
				native.snapshot.OwnerRun = nil
			case "foreign-run":
				native.snapshot.OwnerRun.RunID = "RUN-foreign"
			case "foreign-task":
				native.snapshot.OwnerRun.TaskID = "foreign-task"
			case "foreign-context":
				native.snapshot.OwnerRun.ContextCtxID = "CTX-foreign"
			case "foreign-runtime":
				native.snapshot.OwnerRun.RuntimeControl = json.RawMessage(`{"project_root":"/foreign"}`)
			case "foreign-thread":
				native.snapshot.ThreadID = "foreign-thread"
			case "foreign-turn":
				native.snapshot.ManagedTurnID = "foreign-turn"
			case "foreign-budget":
				var zero int64
				native.snapshot.GoalTokenBudget = &zero
			case "malformed-owner":
				raw := bytes.Replace(capture.Committed.Bytes, []byte(`"run_id":"RUN-fixture"`), []byte(`"run_id":7`), 1)
				if bytes.Equal(raw, capture.Committed.Bytes) {
					t.Fatal("malformed fixture not applied")
				}
				capture.Committed = legacyRecordImage(capture.Committed.SessionID, raw)
				want = ErrInvalidInput
			case "missing-plane":
				raw := bytes.Replace(capture.Committed.Bytes, []byte(`"context_ctx_id":"CTX-fixture"`), []byte(`"context_ctx_id":""`), 1)
				raw = bytes.Replace(raw, []byte(`"delivery_goal_id":"GOAL-fictional"`), []byte(`"delivery_goal_id":""`), 1)
				if bytes.Equal(raw, capture.Committed.Bytes) {
					t.Fatal("missing-plane fixture not applied")
				}
				capture.Committed = legacyRecordImage(capture.Committed.SessionID, raw)
				want = ErrInvalidInput
			}
			before := append([]byte(nil), capture.Committed.Bytes...)
			nativeBefore, _ := native.Snapshot()
			got, err := ReadCapturedCodexOwnerRun(context.Background(), capture)
			nativeAfter, _ := native.Snapshot()
			if !errors.Is(err, want) || !reflect.DeepEqual(got, CodexRunnerSnapshot{}) || !bytes.Equal(before, capture.Committed.Bytes) || !reflect.DeepEqual(nativeBefore, nativeAfter) || len(native.commands) != 0 || len(capture.Input.(*sessionInput).entries) != 0 {
				t.Fatalf("source refusal changed state or verdict: %#v / %v", got, err)
			}
		})
	}
}

// Complete runtime-control data includes exact unknown integer values. Adjacent
// integers beyond float64 precision must not compare as the same captured fact.
func TestCodexOwnerRunRuntimeControlExactNumbers(t *testing.T) {
	owner := codexOwnerFactFixture()
	owner.RuntimeControl = append(owner.RuntimeControl[:len(owner.RuntimeControl)-1], []byte(`,"precise":9007199254740992}`)...)
	capture := codexOwnerCapturedFixture(t, owner)
	if _, err := ReadCapturedCodexOwnerRun(context.Background(), capture); err != nil {
		t.Fatal("equal complete runtime-control neighbor refused", err)
	}
	native := capture.Native.(*codexRunner)
	native.snapshot.OwnerRun.RuntimeControl = bytes.Replace(native.snapshot.OwnerRun.RuntimeControl, []byte(`9007199254740992`), []byte(`9007199254740993`), 1)
	if got, err := ReadCapturedCodexOwnerRun(context.Background(), capture); !errors.Is(err, ErrLegacyViewStale) || !reflect.DeepEqual(got, CodexRunnerSnapshot{}) {
		t.Fatalf("distinct exact runtime-control integers admitted as equal source facts: %v", err)
	}
}
