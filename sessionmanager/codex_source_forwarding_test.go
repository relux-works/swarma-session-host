package sessionmanager

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type codexSourceNative struct {
	*inputTestBackend
	native       *codexRunner
	snapshotHook func()
}

func (n *codexSourceNative) State(context.Context) (ProviderHostState, error) {
	return ProviderHostRunning, nil
}
func (n *codexSourceNative) Stop(context.Context) error {
	return errors.New("source read must not stop a native")
}
func (n *codexSourceNative) Snapshot() (RunnerSnapshot, error) {
	if n.snapshotHook != nil {
		n.snapshotHook()
	}
	return n.native.Snapshot()
}

type codexSourceFactory struct{ native *codexSourceNative }

func (codexSourceFactory) Provider() string { return "codex" }
func (f codexSourceFactory) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	return f.native, nil
}
func (f codexSourceFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	return f.native, nil
}

func codexSourceHost(t *testing.T) (*SessionHost, *sessionInput, ControllerLease, *codexSourceNative) {
	return codexSourceHostForOwner(t, "parent/primary")
}
func codexSourceHostForOwner(t *testing.T, owner OwnerKey) (*SessionHost, *sessionInput, ControllerLease, *codexSourceNative) {
	t.Helper()
	// Reuse the actual original-format ordinary/no-fact image and native
	// constructor. The provider process boundary never opens an app server.
	captured := codexOwnerCapturedFixture(t, nil)
	captured.Native.(*codexRunner).snapshot.HostArgv = []string{"codex", "app-server"}
	native := &codexSourceNative{inputTestBackend: &inputTestBackend{}, native: captured.Native.(*codexRunner)}
	root := t.TempDir()
	h, err := NewHost(HostOptions{Layout: Layout{Namespace: "source-fixture", StateDir: root, SessionsDir: filepath.Join(root, "sessions"), LockFile: filepath.Join(root, "manager.lock")}, Config: hostCommitProbeConfig{}, Notices: hostCommitProbeNotices{}, Codec: NewLegacyRecordCodec(), Factories: []ProviderHostFactory{codexSourceFactory{native}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	a, err := h.Attach(context.Background(), AttachRequest{Owner: owner, Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	lease, _, err := h.Input(context.Background(), a.Session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	return h, h.inputs[lease.SessionID], lease, native
}

// The actual admitted input reads its acknowledged/native ordinary source,
// without the map lock, effects or aliases. Missing fact stays nil; run labels
// cannot create it. Full owner admission remains refused by the unchanged codec.
func TestCodexSourceCurrentInput(t *testing.T) {
	ctx := context.Background()
	h, input, lease, native := codexSourceHost(t)
	source, ok := any(input).(CodexOwnerRunSource)
	if !ok {
		t.Fatal("admitted input lost CodexOwnerRunSource")
	}
	recordPath, _ := h.records.path(lease.SessionID)
	before, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	// BindGoal may inspect the writer while a caller already owns map admission.
	h.mu.Lock()
	got, err := source.CodexOwnerRunSnapshot(ctx, lease)
	h.mu.Unlock()
	if err != nil || got.OwnerRun != nil || got.GoalTokenBudget != nil || got.ThreadID != "fictional-thread" {
		t.Fatalf("actual current no-fact source changed: %+v %v", got, err)
	}
	got.HostArgv[0] = "mutated-return"
	again, err := source.CodexOwnerRunSnapshot(ctx, lease)
	if err != nil || reflect.DeepEqual(got.HostArgv, again.HostArgv) {
		t.Fatalf("source return aliases actual snapshot: %v", err)
	}
	after, err := os.ReadFile(recordPath)
	if err != nil || !bytes.Equal(before, after) || len(input.entries) != 0 || native.writeCount() != 0 {
		t.Fatalf("read performed native/record effects: %v", err)
	}
}

// Wrong holder, cancellation, changed image and replaced/revoked actual input
// cannot become a source fact. Nearby actual-current control runs separately.
func TestCodexSourceCurrentInputRefusals(t *testing.T) {
	for _, mode := range []string{"stale", "foreign", "cancelled", "cancel-during-read", "corrupt-image", "foreign-native", "replaced-input", "uncertain"} {
		t.Run(mode, func(t *testing.T) {
			h, input, lease, native := codexSourceHost(t)
			source, ok := any(input).(CodexOwnerRunSource)
			if !ok {
				t.Fatal("admitted input lost CodexOwnerRunSource")
			}
			call, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := ErrStaleInputController
			switch mode {
			case "stale":
				lease.Epoch++
			case "foreign":
				lease.SessionID = "SES-foreign-source-000000000000"
			case "cancelled":
				cancel()
				want = context.Canceled
			case "cancel-during-read":
				native.snapshotHook = cancel
				want = context.Canceled
			case "corrupt-image":
				path, _ := h.records.path(lease.SessionID)
				if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
				want = ErrLegacyViewStale
			case "foreign-native":
				native.native.mu.Lock()
				native.native.snapshot.ThreadID = "foreign-native"
				native.native.mu.Unlock()
				want = ErrLegacyViewStale
			case "replaced-input":
				if err := input.revokeAndDrain(context.Background()); err != nil {
					t.Fatal(err)
				}
				h.mu.Lock()
				epoch, err := h.epochs.reserveContext(context.Background(), lease.SessionID)
				if err == nil {
					var replacement *sessionInput
					replacement, err = h.installInput(context.Background(), lease.SessionID, epoch, native)
					h.inputs[lease.SessionID] = replacement
				}
				h.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case "uncertain":
				h.records.markUncertain(lease.SessionID)
				want = ErrLegacyCommitUncertain
			}
			got, err := source.CodexOwnerRunSnapshot(call, lease)
			if !errors.Is(err, want) || !reflect.DeepEqual(got, CodexRunnerSnapshot{}) || native.writeCount() != 0 || len(input.entries) != 0 {
				t.Fatalf("source refusal changed verdict/effects: %+v %v want%v", got, err, want)
			}
		})
	}
}

// Real provisional and goal-delivery wrappers forward only while their own
// existing scope is alive. Closing/escaping a wrapper cannot unwrap the parent.
func TestCodexSourceAttachmentAndDeliveryScopes(t *testing.T) {
	ctx := context.Background()
	_, input, lease, _ := codexSourceHost(t)
	attachment := newAttachmentWriter(ctx, input)
	capability := attachment.capability()
	source, ok := capability.(CodexOwnerRunSource)
	if !ok {
		t.Fatal("attachment writer dropped actual source")
	}
	if got, err := source.CodexOwnerRunSnapshot(ctx, lease); err != nil || got.OwnerRun != nil {
		t.Fatal("attachment source refused actual parent", err)
	}
	scope, delivery := newGoalWriterScope(ctx, capability, &GoalEnvelope{Owner: "parent/primary"}, "apply")
	delivered, ok := delivery.(CodexOwnerRunSource)
	if !ok {
		t.Fatal("delivery writer dropped actual source")
	}
	if _, err := delivered.CodexOwnerRunSnapshot(ctx, lease); err != nil {
		t.Fatal("delivery source refused actual parent", err)
	}
	scope.close()
	if _, err := delivered.CodexOwnerRunSnapshot(ctx, lease); !errors.Is(err, ErrInputScopeClosed) {
		t.Fatalf("escaped delivery retained source: %v", err)
	}
	if err := attachment.close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CodexOwnerRunSnapshot(ctx, lease); !errors.Is(err, ErrInputScopeClosed) {
		t.Fatalf("closed attachment retained source: %v", err)
	}
}

// Nested actual native-observation writers must retain only their callback
// lifetime. The observed value is a controlled native boundary, not an owner
// fact; source data still comes from the actual admitted input and record.
func TestCodexSourceNestedObservationScope(t *testing.T) {
	ctx := context.Background()
	_, input, lease, native := codexSourceHost(t)
	native.inputTestBackend.native = true
	native.inputTestBackend.state = NativeGoalObservation{Sequence: 1, Status: "active"}
	outer := newAttachmentWriter(ctx, input)
	defer outer.close(ctx)
	scope, delivery := newGoalWriterScope(ctx, outer.capability(), &GoalEnvelope{Owner: "parent/primary"}, "apply")
	defer scope.close()
	observer, ok := delivery.(NativeGoalObserver)
	if !ok {
		t.Fatal("actual native scope lost")
	}
	var escaped CodexOwnerRunSource
	err := observer.WithNativeGoal(ctx, lease, func(_ NativeGoalObservation, writer HostInputWriter) error {
		var ok bool
		escaped, ok = writer.(CodexOwnerRunSource)
		if !ok {
			t.Fatal("nested observation writer dropped current source")
		}
		got, e := escaped.CodexOwnerRunSnapshot(ctx, lease)
		if e != nil || got.OwnerRun != nil {
			t.Fatalf("nested actual source read failed: %v", e)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = escaped.CodexOwnerRunSnapshot(ctx, lease); !errors.Is(err, ErrInputScopeClosed) {
		t.Fatalf("escaped observation retained source: %v", err)
	}
	if native.writeCount() != 0 {
		t.Fatal("source forwarding performed a native write")
	}
}

type codexSourceEmptyGoals struct{}

func (codexSourceEmptyGoals) Get(OwnerKey) (*GoalRead, error)                  { return &GoalRead{}, nil }
func (codexSourceEmptyGoals) ConfirmNativeApplied(OwnerKey, RevisionRef) error { return nil }
func (codexSourceEmptyGoals) Reconcile(OwnerKey, GoalBinding) (*GoalActivation, error) {
	return &GoalActivation{}, nil
}

type codexSourceBindingProbe struct {
	host  *SessionHost
	calls int
}

func (f *codexSourceBindingProbe) BindGoal(ctx context.Context, l ControllerLease, provider string, w HostInputWriter) (OwnerKey, GoalBinding, error) {
	source, ok := w.(CodexOwnerRunSource)
	if !ok {
		return "", nil, ErrHostCapabilityUnavailable
	}
	// Actual BindGoal is outside host.mu; holding it explicitly here catches
	// any accidental reentry by the source port on the real installed input.
	f.host.mu.Lock()
	got, err := source.CodexOwnerRunSnapshot(ctx, l)
	f.host.mu.Unlock()
	if err != nil {
		return "", nil, err
	}
	if provider != "codex" || got.ThreadID != "fictional-thread" || got.OwnerRun != nil {
		return "", nil, ErrLegacyViewStale
	}
	f.calls++
	return OwnerKey("session/" + l.SessionID), coreGoalBinding{}, nil
}

// The real common reconciliation caller supplies the installed, acknowledged
// input source before its empty-goal result. The empty source/binding probes
// assert source order only; this does not qualify goal delivery or an ACK.
func TestCodexSourceAvailableToActualBindGoal(t *testing.T) {
	ctx := context.Background()
	h, input, lease, native := codexSourceHost(t)
	probe := &codexSourceBindingProbe{host: h}
	h.options.Goals = codexSourceEmptyGoals{}
	h.options.GoalBindings = probe
	before := copyLegacyRecordImage(h.records.bases[lease.SessionID])
	if _, err := h.Attach(ctx, AttachRequest{Owner: "parent/primary", Provider: "codex"}); err != nil {
		t.Fatal("actual BindGoal source failed", err)
	}
	if probe.calls != 1 || native.writeCount() != 0 || len(input.entries) != 0 || !bytes.Equal(before.Bytes, h.records.bases[lease.SessionID].Bytes) {
		t.Fatal("source observation changed native/record or missed actual caller")
	}
}

type codexSourceCodecProbe struct {
	ForkRunnerCodec
	encodes, acks int
}

func (p *codexSourceCodecProbe) EncodeInitial(ctx context.Context, r RunnerRecord) ([]byte, error) {
	p.encodes++
	return p.ForkRunnerCodec.EncodeInitial(ctx, r)
}
func (p *codexSourceCodecProbe) EncodeMerge(ctx context.Context, in LegacyRecordInput, r RunnerRecord) ([]byte, error) {
	p.encodes++
	return p.ForkRunnerCodec.EncodeMerge(ctx, in, r)
}
func (p *codexSourceCodecProbe) AcknowledgeCommitted(ctx context.Context, in LegacyRecordInput) error {
	p.acks++
	return p.ForkRunnerCodec.AcknowledgeCommitted(ctx, in)
}

// A source read consumes an already-validated ACK base. It must not invoke
// write preparation/ACK hooks even for a no-op image: a codec can retain a
// generation during encoding. The probe delegates to the actual strict codec.
func TestCodexSourceReadsDoNotPrepareWrites(t *testing.T) {
	h, input, lease, _ := codexSourceHost(t)
	probe := &codexSourceCodecProbe{ForkRunnerCodec: h.records.codec}
	h.records.codec = probe
	source := any(input).(CodexOwnerRunSource)
	got, err := source.CodexOwnerRunSnapshot(context.Background(), lease)
	if err != nil || got.OwnerRun != nil {
		t.Fatal("valid read refused", err)
	}
	if probe.encodes != 0 || probe.acks != 0 {
		t.Fatalf("read invoked write preparation or ACK: encodes%d acks%d", probe.encodes, probe.acks)
	}
}

// An actual current run-labeled session with no original OwnerRun metadata is
// still the ordinary no-fact case; owner/task labels never select owner mode.
func TestCodexSourceRunLabelDoesNotCreateFact(t *testing.T) {
	_, input, lease, native := codexSourceHostForOwner(t, "run/RUN-FIXTURE")
	got, err := any(input).(CodexOwnerRunSource).CodexOwnerRunSnapshot(context.Background(), lease)
	if err != nil || got.OwnerRun != nil || got.GoalTokenBudget != nil || native.writeCount() != 0 {
		t.Fatalf("run label created source metadata/effects: %+v %v", got, err)
	}
}
