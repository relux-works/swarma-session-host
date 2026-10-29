package sessionbudget

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// This file-backed test adapter supplies the real provider subrecord and a
// current-lease check. It is not host admission or a native-provider fixture.
type legacyImages struct {
	mu       sync.Mutex
	path     string
	lease    core.ControllerLease
	nativeID string
}

func (f *legacyImages) ReadMuseImage(_ context.Context, l core.ControllerLease) (string, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l != f.lease {
		return "", nil, core.ErrStaleInputController
	}
	b, e := os.ReadFile(f.path)
	return f.nativeID, b, e
}
func (f *legacyImages) CompareAndSwapMuseImage(_ context.Context, l core.ControllerLease, before, after []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l != f.lease {
		return core.ErrStaleInputController
	}
	b, e := os.ReadFile(f.path)
	if e != nil {
		return e
	}
	if !bytes.Equal(b, before) {
		return core.ErrLegacyViewStale
	}
	return os.WriteFile(f.path, after, 0600)
}
func legacyFixture(t *testing.T) (*Store, *core.LegacyRecordCodec, *legacyImages, *nativeFixture, core.LegacyMuseAccountingSnapshot, []byte) {
	t.Helper()
	raw, e := os.ReadFile("../sessionmanager/testdata/legacy-records/muse-native-initial.json")
	if e != nil {
		t.Fatal(e)
	}
	var row map[string]json.RawMessage
	if e = json.Unmarshal(raw, &row); e != nil {
		t.Fatal(e)
	}
	image := []byte(row["provider_host"])
	codec := core.NewLegacyRecordCodec()
	state, e := codec.DecodeMuseAccounting(context.Background(), image)
	if e != nil {
		t.Fatal(e)
	}
	l := core.ControllerLease{SessionID: "s", Epoch: 1}
	native := newNative(l)
	native.native.Objective = state.GoalObjective
	native.native.Status = state.GoalStatus
	native.usage(state.TokenUsageTotal)
	store, e := Open(filepath.Join(t.TempDir(), "budget"))
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "provider.json")
	if e = os.WriteFile(path, image, 0600); e != nil {
		t.Fatal(e)
	}
	images := &legacyImages{path: path, lease: l, nativeID: state.SessionID}
	return store, codec, images, native, state, image
}

// Hydration uses the actual exported codec field group and the exact original
// image. A no-change export retains every original ledger/provenance field and
// byte, including the separately retained text-child accounting.
func TestLegacyBudgetHydratesAndRoundTripsOriginalImage(t *testing.T) {
	store, codec, images, native, want, original := legacyFixture(t)
	source := &LegacySource{Codec: codec, Images: images}
	if e := source.HydrateBudget(context.Background(), images.lease, native, store); e != nil {
		t.Fatal(e)
	}
	got, e := store.LegacySnapshot(context.Background(), images.lease)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("native accounting lost fields: got%+v want%+v", got, want)
	}
	if e = source.CommitBudget(context.Background(), images.lease, store); e != nil {
		t.Fatal(e)
	}
	after, _ := os.ReadFile(images.path)
	if !bytes.Equal(after, original) {
		t.Fatal("unchanged native accounting rewrote original provider/text image")
	}
	if native.writes != 0 {
		t.Fatal("data hydration issued native input")
	}
}

// Data-only decode is not runtime authority. Current native objective,
// observed lease, actual runtime native ID and known cumulative counters must
// all match; failed hydration writes neither an account nor native input.
func TestLegacyBudgetHydrationRequiresCurrentNativeIdentity(t *testing.T) {
	for _, mode := range []string{"objective_mismatch", "wrong_epoch", "wrong_native_id", "unknown_usage", "counter_regression"} {
		t.Run(mode, func(t *testing.T) {
			store, codec, images, native, _, _ := legacyFixture(t)
			switch mode {
			case "objective_mismatch":
				native.native.Objective = "foreign"
			case "wrong_epoch":
				native.native.Epoch = 2
			case "wrong_native_id":
				images.nativeID = "other-native"
			case "unknown_usage":
				native.native.Cumulative = nil
			case "counter_regression":
				native.usage(19)
			}
			source := &LegacySource{Codec: codec, Images: images}
			if e := source.HydrateBudget(context.Background(), images.lease, native, store); e == nil {
				t.Fatal("mismatched native identity hydrated accounting")
			}
			if native.writes != 0 {
				t.Fatal("failed legacy hydration issued native input")
			}
			e := store.transaction(context.Background(), false, func(j *journal) error {
				if mode == "counter_regression" {
					a := j.Sessions["s"]
					if a == nil || !a.Uncertain || a.Current != nil || a.Cumulative != nil {
						t.Fatal("counter-reset rejection did not retain unknown/uncertain state")
					}
				} else if len(j.Sessions) != 0 {
					t.Fatal("failed legacy hydration left account effects")
				}
				return nil
			})
			if e != nil {
				t.Fatal(e)
			}
		})
	}
}

// Retained native spending is enforced through the actual typed observer and
// pause receipt, then merged through the real codec; no old child state or
// unknown sibling field is replaced by a new accounting DTO.
func TestLegacyBudgetRestoredThresholdPersistsMatchedPause(t *testing.T) {
	store, codec, images, native, state, original := legacyFixture(t)
	state.GoalStatus = "active"
	state.ExternalPause = false
	state.BudgetPauseOutcome = nil
	image, e := codec.EncodeMuseAccounting(context.Background(), original, state)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(images.path, image, 0600); e != nil {
		t.Fatal(e)
	}
	native.native.Status = "active"
	h := setup(t)
	f, e := NewFactory(context.Background(), h.s, store, &LegacySource{Codec: codec, Images: images})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = f.Close(context.Background()) })
	if _, _, e = f.BindGoal(context.Background(), images.lease, "muse", native); e != nil {
		t.Fatal(e)
	}
	native.usage(state.TokenUsageBaseline + state.TokenBudget)
	if e = f.bindings[images.lease].Check(context.Background()); e != nil {
		t.Fatal(e)
	}
	if native.pauses != 1 {
		t.Fatal("restored spending did not cause one receipt-checked pause")
	}
	raw, _ := os.ReadFile(images.path)
	got, e := codec.DecodeMuseAccounting(context.Background(), raw)
	if e != nil {
		t.Fatal(e)
	}
	if !got.BudgetPaused || got.GoalStatus != "paused" || got.TokenUsageBaseline != state.TokenUsageBaseline || got.GoalRef == nil || *got.GoalRef != *state.GoalRef {
		t.Fatalf("restored pause facts not persisted: %+v", got)
	}
	var before, after map[string]json.RawMessage
	_ = json.Unmarshal(image, &before)
	_ = json.Unmarshal(raw, &after)
	var a, b map[string]json.RawMessage
	_ = json.Unmarshal(before["opaque"], &a)
	_ = json.Unmarshal(after["opaque"], &b)
	if !bytes.Equal(a["text"], b["text"]) {
		t.Fatal("native accounting write changed original text-child state")
	}
}

// The preserved codec owns published reference validation. An incompatible
// new reference is refused by preflight before any native effect or file write;
// the sink must not relabel it or retain an old goalRef for a new objective.
func TestLegacyBudgetIncompatibleReferenceHasNoNativeEffects(t *testing.T) {
	store, codec, images, native, _, before := legacyFixture(t)
	source := &LegacySource{Codec: codec, Images: images}
	if e := source.HydrateBudget(context.Background(), images.lease, native, store); e != nil {
		t.Fatal(e)
	}
	revision := goal.Revision{SessionID: "s", Namespace: goal.Assignment, Ref: core.RevisionRef{ID: "incompatible-reference", Revision: 1}, State: goal.Active, Content: goal.Content{Objective: "Work", Condition: "Accepted by holder", Budget: goal.Budget{Tokens: 10}}}
	if e := source.PrepareBudget(context.Background(), images.lease, revision, store); e == nil {
		t.Fatal("incompatible legacy identity admitted")
	}
	after, _ := os.ReadFile(images.path)
	if !bytes.Equal(before, after) || native.writes != 0 {
		t.Fatal("incompatible legacy reference had effects")
	}
	images.lease.Epoch++
	if e := source.CommitBudget(context.Background(), native.lease, store); !errors.Is(e, core.ErrStaleInputController) {
		t.Fatalf("stale legacy write = %v", e)
	}
}

// The real service allocates the immutable reference before rendering. That
// same unmodified reference reaches the native writer, durable budget facts and
// the original codec; no identity alias or stale legacy goalRef can pass.
func TestLegacyBudgetServiceGeneratedReferencePersistsUnchanged(t *testing.T) {
	store, codec, images, native, _, _ := legacyFixture(t)
	h := setup(t)
	f, e := NewFactory(context.Background(), h.s, store, &LegacySource{Codec: codec, Images: images})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = f.Close(context.Background()) })
	_, binding, e := f.BindGoal(context.Background(), images.lease, "muse", native)
	if e != nil {
		t.Fatal(e)
	}
	receipt, e := h.s.Execute(context.Background(), goal.Command{EnvelopeID: "generated-budget", SessionID: "s", Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "Work", Condition: "Holder accepts", Budget: goal.Budget{Tokens: 17}}}, binding)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasPrefix(receipt.Ref.ID, "GOAL-SG-") {
		t.Fatal("service generated an incompatible reference")
	}
	raw, e := os.ReadFile(images.path)
	if e != nil {
		t.Fatal(e)
	}
	got, e := codec.DecodeMuseAccounting(context.Background(), raw)
	if e != nil {
		t.Fatal(e)
	}
	if got.GoalRef == nil || got.GoalRef.ID != receipt.Ref.ID || got.GoalRef.Revision != receipt.Ref.Revision || got.TokenBudget != 17 || got.TokenUsageBaseline != 20 {
		t.Fatalf("actual generated reference/budget was aliased or lost: ref%+v snapshot%+v", receipt.Ref, got)
	}
	if got.GoalObjective != native.native.Objective || !strings.HasPrefix(got.GoalObjective, goal.RevisionToken(receipt.Ref)+" ") {
		t.Fatal("persisted native objective used another immutable reference")
	}
	head, e := h.s.Snapshot("s", goal.Assignment)
	if e != nil {
		t.Fatal(e)
	}
	if head.Applied == nil || *head.Applied != receipt.Ref {
		t.Fatal("matched generated revision was not applied")
	}
}
