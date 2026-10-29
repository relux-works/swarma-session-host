package sessionmanager_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	run "github.com/relux-works/swarma-session-host/sessionrun"
)

type accountingIntegrationConfig struct{}

func (accountingIntegrationConfig) Snapshot(context.Context) (core.HostSettings, error) {
	return core.HostSettings{DaemonVersion: "fixture", GoalAckTimeout: time.Second}, nil
}

type accountingIntegrationAuthority struct{}

func (accountingIntegrationAuthority) WithVerifiedCaller(_ context.Context, id string, fn func(goal.VerifiedCaller) error) error {
	return fn(goal.VerifiedCaller{SessionID: id, Principal: "holder", Holder: "holder", Generation: 1, Assignment: true, Working: true, Operator: true, UnlimitedBudget: true})
}

// This is only the native I/O boundary. Production core, codec, goal service,
// budget journal/factory, renderer, notices and record files are real below.
// No provider/model/daemon/authentication qualification is asserted.
type accountingIntegrationNative struct {
	mu                              sync.Mutex
	root, objective, status, foldID string
	writes                          int
	total                           int64
	sequence                        uint64
	launches                        int
	stops                           int
	nativeID                        string
	feeds                           []*accountingIntegrationFeed
}

func (n *accountingIntegrationNative) Provider() string { return "muse" }
func (n *accountingIntegrationNative) Launch(context.Context, core.ProviderLaunchRequest) (core.RunnerHost, error) {
	n.mu.Lock()
	n.launches++
	n.mu.Unlock()
	return n, nil
}
func (n *accountingIntegrationNative) Restore(context.Context, core.ProviderHostRecord) (core.RunnerHost, error) {
	return n, nil
}
func (n *accountingIntegrationNative) State(context.Context) (core.ProviderHostState, error) {
	return core.ProviderHostRunning, nil
}
func (n *accountingIntegrationNative) Stop(context.Context) error {
	n.mu.Lock()
	n.stops++
	n.mu.Unlock()
	return nil
}
func (n *accountingIntegrationNative) NativeIdentity() string {
	if n.nativeID != "" {
		return n.nativeID
	}
	return "native-fixture"
}
func (n *accountingIntegrationNative) Snapshot() (core.RunnerSnapshot, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return core.RunnerSnapshot{Kind: "muse-msp", MuseMSP: &core.MuseRunnerSnapshot{GoalTransport: "native", Provider: "muse", State: core.ProviderHostRunning, SessionID: n.NativeIdentity(), WorkspaceRoot: n.root, ApprovalMode: "denyUnmatched", ModelID: "fixture-model", TokenUsageTotal: n.total}}, nil
}
func (n *accountingIntegrationNative) SupportsNativeGoals() bool              { return true }
func (n *accountingIntegrationNative) AcceptsInput(core.InputSubmission) bool { return true }
func (n *accountingIntegrationNative) ExecuteInput(_ context.Context, input core.InputSubmission, receipt core.InputReceipt) (func(context.Context) (core.InputObservation, error), error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.writes++
	command := input.Payload.MuseGoal
	if command == nil {
		return nil, errors.New("unexpected non-goal native input")
	}
	switch {
	case command.Set != nil:
		n.objective = command.Set.Objective
		n.status = "active"
	case command.Edit != nil:
		n.objective = command.Edit.Objective
	case command.Clear != nil:
		n.objective = ""
		n.status = ""
	case command.Pause != nil:
		if command.Pause.Sequence != n.sequence {
			return nil, core.ErrNativeGoalChanged
		}
		n.status = "paused"
	default:
		return nil, core.ErrInvalidInput
	}
	n.sequence++
	return func(context.Context) (core.InputObservation, error) {
		return core.InputObservation{Receipt: receipt, Kind: core.InputObservationKind(input.Expected.Kind), Correlation: input.Expected.Correlation}, nil
	}, nil
}
func (n *accountingIntegrationNative) usage() core.MuseRestoreUsage {
	return core.MuseRestoreUsage{TotalTokens: n.total, PromptTokens: n.total - 3, OutputTokens: 3}
}
func (n *accountingIntegrationNative) ObserveNativeGoal(context.Context) (core.NativeGoalObservation, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	u := n.usage()
	return core.NativeGoalObservation{Objective: n.objective, Status: n.status, Sequence: n.sequence, Cumulative: &u}, nil
}
func (n *accountingIntegrationNative) SubscribeMuse(_ context.Context, l core.ControllerLease) (core.MuseObservationFeed, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	feed := &accountingIntegrationFeed{closed: make(chan struct{}), value: core.MuseObservation{Lease: l, Sequence: 1, ObservedAt: time.Now().UTC(), RestoreFold: &core.MuseRestoreFold{SessionID: n.foldID, Cursor: "fixture-cursor", Cumulative: n.usage(), Items: []json.RawMessage{}, EffectiveApprovalMode: "denyUnmatched", EffectiveModelID: "fixture-model"}}}
	n.feeds = append(n.feeds, feed)
	return feed, nil
}
func (n *accountingIntegrationNative) setTotal(total int64) {
	n.mu.Lock()
	n.total = total
	n.sequence++
	n.mu.Unlock()
}
func (n *accountingIntegrationNative) writeCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.writes
}

type accountingIntegrationFeed struct {
	value  core.MuseObservation
	read   bool
	closed chan struct{}
	once   sync.Once
}

func (f *accountingIntegrationFeed) Next(ctx context.Context) (core.MuseObservation, error) {
	if !f.read {
		f.read = true
		return f.value, nil
	}
	select {
	case <-ctx.Done():
		return core.MuseObservation{}, ctx.Err()
	case <-f.closed:
		return core.MuseObservation{}, io.EOF
	}
}
func (f *accountingIntegrationFeed) Close() { f.once.Do(func() { close(f.closed) }) }

// Real coupled composition must observe/commit an empty native goal before
// budget hydration and publish an attachment with preserved cumulative facts.
// Wrong physical identity or a foreign native objective never becomes an
// observed canonical account and never writes native input.
func TestNativeAccountingRealCompositionObservedEmpty(t *testing.T) {
	for _, mode := range []string{"valid", "foreign-native", "foreign-objective"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			root := t.TempDir()
			service, e := goal.Open(filepath.Join(root, "goals"), accountingIntegrationAuthority{})
			if e != nil {
				t.Fatal(e)
			}
			store, e := budget.Open(filepath.Join(root, "budget"))
			if e != nil {
				t.Fatal(e)
			}
			runtime := run.New(store, core.HostSettings{GoalAckTimeout: time.Second})
			factory, e := budget.NewFactory(ctx, service, store, runtime.AccountingSource())
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = factory.Close(context.Background()) })
			if e = runtime.BindBudget(factory); e != nil {
				t.Fatal(e)
			}
			codec, e := core.NewLegacyRecordCodecWithMuseAccounting(runtime)
			if e != nil {
				t.Fatal(e)
			}
			layout := core.Layout{Namespace: "fixture", StateDir: filepath.Join(root, "host"), SessionsDir: filepath.Join(root, "host", "sessions"), LockFile: filepath.Join(root, "host", "manager.lock"), NoticesDir: filepath.Join(root, "host", "notices")}
			notices, e := core.NewSessionNoticeStore(layout)
			if e != nil {
				t.Fatal(e)
			}
			native := &accountingIntegrationNative{root: root, foldID: "native-fixture", total: 9, sequence: 1}
			if mode == "foreign-native" {
				native.foldID = "foreign"
			}
			if mode == "foreign-objective" {
				native.objective = "foreign objective"
			}
			host, e := core.NewHost(core.HostOptions{Layout: layout, Config: accountingIntegrationConfig{}, Codec: codec, Goals: service, GoalBindings: factory, OwnerRuns: runtime, Notices: notices, Factories: []core.ProviderHostFactory{native}})
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(host.Close)
			if e = runtime.BindHost(host); e != nil {
				t.Fatal(e)
			}
			attachment, e := host.Attach(ctx, core.AttachRequest{Owner: "parent/primary", Provider: "muse"})
			if mode != "valid" {
				want := core.ErrMuseObservationInvalid
				if mode == "foreign-objective" {
					want = core.ErrInputAcknowledgementMismatch
				}
				if !errors.Is(e, want) || attachment != nil || native.writeCount() != 0 {
					t.Fatalf("foreign native state gained attachment/effect: %+v %v writes%d", attachment, e, native.writeCount())
				}
				views, listErr := host.List(ctx)
				if listErr != nil || len(views) != 1 {
					t.Fatalf("failed observation lost recovery: %v", listErr)
				}
				raw, readErr := os.ReadFile(filepath.Join(layout.SessionsDir, views[0].SessionID, "record.json"))
				if readErr != nil {
					t.Fatal(readErr)
				}
				var envelope struct {
					ProviderHost json.RawMessage `json:"provider_host"`
				}
				if readErr = json.Unmarshal(raw, &envelope); readErr != nil {
					t.Fatal(readErr)
				}
				state, decodeErr := core.NewLegacyRecordCodec().DecodeMuseAccounting(ctx, envelope.ProviderHost)
				if decodeErr != nil || state.GoalObserved || state.GoalRef != nil {
					t.Fatalf("foreign observation became accounting authority: %+v %v", state, decodeErr)
				}
				return
			}
			if e != nil || attachment == nil || attachment.AttachmentID == "" {
				t.Fatalf("actual empty native attach failed: %+v %v", attachment, e)
			}
			lease, _, e := host.Input(ctx, attachment.Session.SessionID)
			if e != nil {
				t.Fatal(e)
			}
			state, e := store.LegacySnapshot(ctx, lease)
			if e != nil || !state.GoalObserved || state.GoalRef != nil || state.TokenUsageTotal != 9 || state.TokenUsageBaseline != 0 || native.writeCount() != 0 {
				t.Fatalf("actual empty accounting was reset/forged: %+v %v writes%d", state, e, native.writeCount())
			}
		})
	}
}
