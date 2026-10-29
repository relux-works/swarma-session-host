package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func codexBudgetCarrierPointer(value int64) *int64 { return &value }

func codexBudgetCarrierWant(t *testing.T, got, want *int64) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("optional budget changed: got %v, want %v", got, want)
	}
}

// Copies preserve nil/zero/exact selected values independently in the prepared
// launch and its snapshot, without aliases or implicit constructor selection.
// This does not execute a provider or apply a budget to a goal.
func TestCodexBudgetCarrierCopiesLaunchAndSnapshot(t *testing.T) {
	for _, value := range []*int64{nil, codexBudgetCarrierPointer(0), codexBudgetCarrierPointer(1), codexBudgetCarrierPointer(7654321), codexBudgetCarrierPointer(math.MaxInt64)} {
		name := "absent"
		if value != nil {
			name = fmt.Sprint(*value)
		}
		t.Run(name, func(t *testing.T) {
			src := LaunchSpec{Codex: &CodexPreparedLaunch{GoalTokenBudget: value, Snapshot: CodexRunnerSnapshot{GoalTokenBudget: codexBudgetCarrierPointer(41)}}}
			got := copyLaunchSpec(src)
			codexBudgetCarrierWant(t, got.Codex.GoalTokenBudget, value)
			codexBudgetCarrierWant(t, got.Codex.Snapshot.GoalTokenBudget, codexBudgetCarrierPointer(41))
			if value != nil {
				before := *value
				if got.Codex.GoalTokenBudget == value || got.Codex.GoalTokenBudget == got.Codex.Snapshot.GoalTokenBudget {
					t.Fatal("prepared budget retained a mutable alias")
				}
				*got.Codex.GoalTokenBudget = 99
				if *value != before || *got.Codex.Snapshot.GoalTokenBudget != 41 {
					t.Fatal("prepared budget mutation reached source or snapshot")
				}
			}
			*got.Codex.Snapshot.GoalTokenBudget = 73
			if *src.Codex.Snapshot.GoalTokenBudget != 41 {
				t.Fatal("snapshot budget mutation reached launch source")
			}
		})
	}
	if copyLaunchSpec(LaunchSpec{}).Codex != nil || copyRunnerSnapshot(RunnerSnapshot{}).Codex != nil {
		t.Fatal("copy manufactured an absent Codex variant")
	}
}

// Projection/merge retain the original optional int64 JSON shape and detach
// both pointer directions. Historical negative data is preserved, not admitted
// as a valid launch; validation/goal selection belongs to the actual consumer.
func TestCodexBudgetCarrierLegacyProjectionAndMerge(t *testing.T) {
	for _, value := range []*int64{nil, codexBudgetCarrierPointer(0), codexBudgetCarrierPointer(7654321), codexBudgetCarrierPointer(math.MaxInt64), codexBudgetCarrierPointer(-1)} {
		name := "absent"
		if value != nil {
			name = fmt.Sprint(*value)
		}
		t.Run(name, func(t *testing.T) {
			raw := legacyCodexSnapshot{GoalTokenBudget: value, OwnerRun: json.RawMessage(`{"retained":"owner"}`), PendingGoalDirective: "retained directive"}
			projected := projectCodexSnapshot(raw)
			codexBudgetCarrierWant(t, projected.GoalTokenBudget, value)
			prior := raw
			prior.GoalTokenBudget = codexBudgetCarrierPointer(41)
			merged := mergeCodexSnapshot(prior, projected)
			codexBudgetCarrierWant(t, merged.GoalTokenBudget, value)
			if !bytes.Equal(merged.OwnerRun, raw.OwnerRun) || merged.PendingGoalDirective != raw.PendingGoalDirective || *prior.GoalTokenBudget != 41 {
				t.Fatal("budget merge changed unrelated historical state or prior budget")
			}
			encoded, err := json.Marshal(merged)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			wire, present := fields["goal_token_budget"]
			if value == nil && present || value != nil && (!present || string(wire) != fmt.Sprint(*value)) {
				t.Fatalf("optional wire shape changed: %s", encoded)
			}
			var restored legacyCodexSnapshot
			if err := json.Unmarshal(encoded, &restored); err != nil {
				t.Fatal(err)
			}
			codexBudgetCarrierWant(t, projectCodexSnapshot(restored).GoalTokenBudget, value)
			if value != nil {
				before := *value
				if projected.GoalTokenBudget == value || merged.GoalTokenBudget == projected.GoalTokenBudget || merged.GoalTokenBudget == prior.GoalTokenBudget {
					t.Fatal("legacy budget conversion retained a mutable alias")
				}
				*merged.GoalTokenBudget = 99
				if *projected.GoalTokenBudget != before || *value != before || *prior.GoalTokenBudget != 41 {
					t.Fatal("merge mutation reached source or prior budget")
				}
				*projected.GoalTokenBudget = 73
				if *value != before {
					t.Fatal("projection mutation reached raw budget")
				}
			}
		})
	}
}

// A record clone owns current and recovery budgets separately, even when the
// caller shares them. An invalid recovery cycle refuses before mutating input.
func TestCodexBudgetCarrierCloneRecoveryIsolation(t *testing.T) {
	value := int64(7654321)
	previous := RunnerRecord{SessionID: "fixture", Owner: "parent/primary", Provider: "codex", ProviderHost: ProviderHostRecord{Provider: "codex", Snapshot: RunnerSnapshot{Kind: "codex", Codex: &CodexRunnerSnapshot{GoalTokenBudget: &value}}}}
	src := previous
	src.ConditionalStop = &ConditionalStopRecord{Previous: &previous}
	got, err := cloneRunnerRecord(src)
	if err != nil {
		t.Fatal(err)
	}
	a, b := got.ProviderHost.Snapshot.Codex.GoalTokenBudget, got.ConditionalStop.Previous.ProviderHost.Snapshot.Codex.GoalTokenBudget
	codexBudgetCarrierWant(t, a, &value)
	codexBudgetCarrierWant(t, b, &value)
	if a == &value || b == &value || a == b {
		t.Fatal("clone retained shared recovery budget")
	}
	*a = 99
	if *b != 7654321 || value != 7654321 {
		t.Fatal("current clone mutation reached recovery or source")
	}
	*b = 73
	if value != 7654321 {
		t.Fatal("recovery clone mutation reached source")
	}
	previous.ConditionalStop = &ConditionalStopRecord{Previous: &previous}
	refused, err := cloneRunnerRecord(src)
	if err == nil || !strings.Contains(err.Error(), "invalid conditional-stop recovery chain") || !reflect.DeepEqual(refused, RunnerRecord{}) || value != 7654321 || previous.ConditionalStop.Previous != &previous {
		t.Fatalf("cyclic clone did not refuse without partial mutation: %v", err)
	}
}

// This minimal synthetic original-schema stopped record avoids bootstrap and
// private fixtures. Stopped persistence does not authorize a live restoration.
func codexBudgetCarrierImage(t *testing.T) LegacyRecordInput {
	t.Helper()
	raw := []byte(`{
  "contract":"task-board-session-record-v3",
  "session_id":"SES-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "owner":{"kind":"parent","id":"primary"},"provider":"codex","state":"stopped",
  "created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z",
  "future_record":{"retained":true},
  "provider_host":{"provider":"codex","opaque":{
    "contract":"task-board.codex-provider-host-v1","pid":42,"url":"ws://127.0.0.1:1",
    "thread_id":"fixture-thread","project_dir":"/workspace","codex_home":"/fixture-home",
    "goal_token_budget":7654321,"owner_run":{"retained":"owner"},
    "pending_goal_directive":"retained directive","future_provider":{"retained":[1,"opaque"]}
  }}
}`)
	return legacyRecordImage("SES-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", raw)
}

// Actual codec decode/clone/merge/roundtrip preserve the exact original selected
// value, explicit zero and removal, unchanged bytes, and unrelated raw fields.
func TestCodexBudgetCarrierArchivedRecordRoundTrip(t *testing.T) {
	ctx, codec := context.Background(), NewLegacyRecordCodec()
	in := codexBudgetCarrierImage(t)
	base, err := codec.DecodeLegacy(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	codexBudgetCarrierWant(t, base.ProviderHost.Snapshot.Codex.GoalTokenBudget, codexBudgetCarrierPointer(7654321))
	unchanged, err := codec.EncodeMerge(ctx, in, base)
	if err != nil || !bytes.Equal(unchanged, in.Bytes) {
		t.Fatalf("unchanged budget image lost exact bytes: %v", err)
	}
	for _, value := range []*int64{nil, codexBudgetCarrierPointer(0), codexBudgetCarrierPointer(73)} {
		changed, err := cloneRunnerRecord(base)
		if err != nil {
			t.Fatal(err)
		}
		changed.ProviderHost.Snapshot.Codex.GoalTokenBudget = value
		raw, err := codec.EncodeMerge(ctx, in, changed)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := codec.DecodeLegacy(ctx, legacyRecordImage(in.SessionID, raw))
		if err != nil {
			t.Fatal(err)
		}
		codexBudgetCarrierWant(t, restored.ProviderHost.Snapshot.Codex.GoalTokenBudget, value)
		codexBudgetCarrierWant(t, base.ProviderHost.Snapshot.Codex.GoalTokenBudget, codexBudgetCarrierPointer(7654321))
		var root map[string]json.RawMessage
		if err := json.Unmarshal(raw, &root); err != nil {
			t.Fatal(err)
		}
		var host legacyProviderImage
		if err := json.Unmarshal(root["provider_host"], &host); err != nil {
			t.Fatal(err)
		}
		var image map[string]json.RawMessage
		if err := json.Unmarshal(host.Opaque, &image); err != nil {
			t.Fatal(err)
		}
		for key, want := range map[string]string{"owner_run": `{"retained":"owner"}`, "pending_goal_directive": `"retained directive"`, "future_provider": `{"retained":[1,"opaque"]}`} {
			if string(compactLegacyJSON(image[key])) != want {
				t.Fatalf("budget merge changed %s: %s", key, image[key])
			}
		}
		if string(compactLegacyJSON(root["future_record"])) != `{"retained":true}` {
			t.Fatal("budget merge lost unknown record state")
		}
	}
}

// Stale identity and live explicit historical state still refuse with no output
// or input mutation; matching stopped merge and goal-less live decode are valid
// neighbors. The carrier alone never enables the unimplemented live consumer.
func TestCodexBudgetCarrierRefusalHasNoPartialMutation(t *testing.T) {
	ctx, codec := context.Background(), NewLegacyRecordCodec()
	in := codexBudgetCarrierImage(t)
	original := append([]byte(nil), in.Bytes...)
	record, err := codec.DecodeLegacy(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	record.ProviderHost.Snapshot.Codex.ThreadID = "different-thread"
	before, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	out, err := codec.EncodeMerge(ctx, in, record)
	after, marshalErr := json.Marshal(record)
	if !errors.Is(err, ErrLegacyViewStale) || out != nil || marshalErr != nil || !bytes.Equal(before, after) || !bytes.Equal(in.Bytes, original) {
		t.Fatalf("stale merge did not refuse without effects: %v / %v", err, marshalErr)
	}
	for _, budget := range []*int64{nil, codexBudgetCarrierPointer(0), codexBudgetCarrierPointer(7654321)} {
		s := legacyCodexSnapshot{Contract: "task-board.codex-provider-host-v1", PID: 42, URL: "ws://127.0.0.1:1", ThreadID: "fixture-thread", ProjectDir: "/workspace", CodexHome: "/fixture-home", GoalTokenBudget: budget}
		opaque, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		wire := legacyProviderImage{Provider: "codex", Opaque: opaque}
		raw, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		before := append([]byte(nil), raw...)
		got, err := codec.DecodeProviderHost(ctx, raw)
		if budget == nil {
			if err != nil || got.Snapshot.Codex == nil || got.Snapshot.Codex.GoalTokenBudget != nil {
				t.Fatalf("goal-less valid neighbor refused: %v", err)
			}
		} else if !errors.Is(err, ErrHostCapabilityUnavailable) || !reflect.DeepEqual(got, ProviderHostRecord{}) {
			t.Fatalf("carrier enabled unsupported live state: %#v / %v", got, err)
		}
		if !bytes.Equal(raw, before) {
			t.Fatal("decode refusal mutated input")
		}
	}
}
