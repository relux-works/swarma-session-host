package sessionrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func nativeRestoreValidationFixture(t *testing.T) (*Runtime, core.RunnerRecord, core.LegacyRecordInput, *goal.Service) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	sid := "SES-restore-proof"
	lease := core.ControllerLease{SessionID: sid, Epoch: 1}
	auth := &authority{goal.VerifiedCaller{SessionID: sid, Principal: "holder", Holder: "holder", Generation: 1, Assignment: true, UnlimitedBudget: true, Operator: true}}
	service, err := goal.Open(filepath.Join(root, "goals"), auth)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := budget.Open(filepath.Join(root, "seed-budget"))
	if err != nil {
		t.Fatal(err)
	}
	factory, err := budget.NewFactory(ctx, service, seed, accountingSource{"native-s"})
	if err != nil {
		t.Fatal(err)
	}
	native := newNative(lease)
	_, binding, err := factory.BindGoal(ctx, lease, "muse", native)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := service.Execute(ctx, goal.Command{EnvelopeID: "admitted", SessionID: sid, Namespace: goal.Assignment, Action: goal.Set, Content: &goal.Content{Objective: "retained work", Condition: "holder accepts", Budget: goal.Budget{Tokens: 100}}}, binding)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Pending {
		t.Fatal("fixture goal has no actual checked acknowledgement")
	}
	if err = factory.Close(ctx); err != nil {
		t.Fatal(err)
	}
	head, err := service.Snapshot(sid, goal.Assignment)
	if err != nil {
		t.Fatal(err)
	}
	objective := goal.RevisionToken(head.Latest.Ref) + " " + head.Latest.Content.Condition
	group := core.LegacyMuseNativeAccounting{GoalObserved: true, GoalRef: &core.LegacyGoalRef{ID: head.Latest.Ref.ID, Revision: head.Latest.Ref.Revision}, GoalObjective: objective, GoalStatus: "paused", TokenBudget: 100, TokenUsageTotal: 140, TokenUsageBaseline: 100, BudgetPaused: true, BudgetPauseOutcome: &core.LegacyMuseBudgetPauseOutcome{Code: "budget_pause_not_applicable", NativeStatus: "paused"}, ExternalPause: true, ActiveTurnID: "retained-turn", GoalLedger: map[string]core.LegacyMuseGoalRevisionFacts{"historical": {Baseline: 5, Spend: 27, BudgetPaused: true, PauseOutcome: &core.LegacyMuseBudgetPauseOutcome{Code: "budget_pause_failed", NativeStatus: "active"}}, objective: {Baseline: 100, Spend: 40, BudgetPaused: true, ExternalPause: true}}}
	snapshot := core.MuseRunnerSnapshot{NativeAccounting: &group, GoalTransport: "native", Provider: "muse", State: core.ProviderHostRunning, SessionID: "native-s", WorkspaceRoot: "/workspace", ApprovalMode: "denyUnmatched", TokenUsageTotal: 140, ExternalPause: true, ActiveTurnID: "retained-turn"}
	record := core.RunnerRecord{Contract: "task-board-session-record-v3", SessionID: sid, Provider: "muse", Owner: "parent/primary", State: core.SessionRunning, ProviderHost: core.ProviderHostRecord{Provider: "muse", Snapshot: core.RunnerSnapshot{Kind: "muse-msp", MuseMSP: &snapshot}}, AppliedGoalRef: &head.Latest.Ref}
	// Encode against an actual empty original provider image in archival mode;
	// strict runtime Decode remains forbidden until the real restore hook runs.
	opaque := map[string]any{"provider": "muse", "state": "running", "sessionId": "native-s", "workspaceRoot": "/workspace", "goal_transport": "native", "approvalMode": "denyUnmatched", "text": map[string]any{"childBaseline": 0}, "goalObserved": false, "tokenUsageBaseline": 0}
	raw, err := json.Marshal(map[string]any{"provider": "muse", "opaque": opaque})
	if err != nil {
		t.Fatal(err)
	}
	state := core.LegacyMuseAccountingSnapshot{SessionID: "native-s", WorkspaceRoot: "/workspace", GoalTransport: "native", LegacyMuseNativeAccounting: group}
	provider, err := core.NewLegacyRecordCodec().EncodeMuseAccounting(ctx, raw, state)
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile("../sessionmanager/testdata/legacy-records/muse-native-initial.json")
	if err != nil {
		t.Fatal(err)
	}
	var fullWire map[string]json.RawMessage
	if err = json.Unmarshal(original, &fullWire); err != nil {
		t.Fatal(err)
	}
	fullWire["session_id"], err = json.Marshal(sid)
	if err != nil {
		t.Fatal(err)
	}
	fullWire["state"] = json.RawMessage(`"running"`)
	fullWire["provider_host"] = provider
	fullWire["applied_goal_ref"], err = json.Marshal(head.Latest.Ref)
	if err != nil {
		t.Fatal(err)
	}
	full, err := json.Marshal(fullWire)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(full)
	image := core.LegacyRecordInput{SessionID: sid, SHA256: hex.EncodeToString(sum[:]), Bytes: full}
	store, err := budget.Open(filepath.Join(root, "restored-budget"))
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(store, core.HostSettings{GoalAckTimeout: time.Second})
	actual, err := budget.NewFactory(ctx, service, store, runtime.AccountingSource())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := actual.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if err = runtime.BindBudget(actual); err != nil {
		t.Fatal(err)
	}
	return runtime, record, image, service
}

// Stage one reads the real admitted/applied goal journal and exact archival
// bytes; it grants no runtime Restore admission or native effects by itself.
func TestMuseAccountingRestoreValidatesActualGoalAndCarrier(t *testing.T) {
	runtime, record, image, _ := nativeRestoreValidationFixture(t)
	before := *record.ProviderHost.Snapshot.MuseMSP.NativeAccounting
	if err := runtime.ValidateMuseAccountingRestore(context.Background(), record, image); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, *record.ProviderHost.Snapshot.MuseMSP.NativeAccounting) {
		t.Fatal("validation mutated retained accounting")
	}
	for _, name := range []string{"digest", "carrier", "canonical-ref", "canonical-budget"} {
		t.Run(name, func(t *testing.T) {
			r := record
			copy := *record.ProviderHost.Snapshot.MuseMSP
			r.ProviderHost.Snapshot.MuseMSP = &copy
			group := *copy.NativeAccounting
			copy.NativeAccounting = &group
			in := image
			switch name {
			case "digest":
				in.SHA256 = "wrong"
			case "carrier":
				group.TokenUsageBaseline = 0
			case "canonical-ref":
				r.AppliedGoalRef = &core.RevisionRef{ID: "GOAL-SG-OTHER", Revision: 1}
			case "canonical-budget":
				group.TokenBudget++
				var wire map[string]json.RawMessage
				if e := json.Unmarshal(in.Bytes, &wire); e != nil {
					t.Fatal(e)
				}
				codec := core.NewLegacyRecordCodec()
				state, e := codec.DecodeMuseAccounting(context.Background(), wire["provider_host"])
				if e != nil {
					t.Fatal(e)
				}
				state.TokenBudget++
				wire["provider_host"], e = codec.EncodeMuseAccounting(context.Background(), wire["provider_host"], state)
				if e != nil {
					t.Fatal(e)
				}
				in.Bytes, e = json.Marshal(wire)
				if e != nil {
					t.Fatal(e)
				}
				sum := sha256.Sum256(in.Bytes)
				in.SHA256 = hex.EncodeToString(sum[:])
			}
			err := runtime.ValidateMuseAccountingRestore(context.Background(), r, in)
			if err == nil {
				t.Fatalf("%s accepted", name)
			}
			if name == "canonical-budget" && !errors.Is(err, core.ErrInputAcknowledgementMismatch) {
				t.Fatalf("canonical budget did not reach journal comparison: %v", err)
			}
		})
	}
}

// Missing real current host/cut is an explicit stage-two refusal. A validated
// carrier and installed receiver cannot bypass the actual core Restore gate.
func TestMuseAccountingRestoreMissingCurrentHostHasNoNativeEffects(t *testing.T) {
	runtime, record, image, _ := nativeRestoreValidationFixture(t)
	native := &codexFixture{}
	capture := core.CapturedOwnerRun{Lease: core.ControllerLease{SessionID: record.SessionID, Epoch: 2}, Record: record, Committed: image, Native: native, Input: newNative(core.ControllerLease{SessionID: record.SessionID, Epoch: 2})}
	if err := runtime.RestoreMuseAccounting(context.Background(), capture); !errors.Is(err, core.ErrHostClosed) {
		t.Fatalf("missing real restore host: %v", err)
	}
	if native.writes != 0 {
		t.Fatal("unbound restore performed native writes")
	}
}
