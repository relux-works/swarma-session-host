package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type accountingRecordRunner struct {
	*inputTestBackend
	snapshot MuseRunnerSnapshot
}

func (r *accountingRecordRunner) State(context.Context) (ProviderHostState, error) {
	return ProviderHostRunning, nil
}
func (r *accountingRecordRunner) Stop(context.Context) error { return nil }
func (r *accountingRecordRunner) Snapshot() (RunnerSnapshot, error) {
	s := r.snapshot
	return RunnerSnapshot{Kind: "muse-msp", MuseMSP: &s}, nil
}

type accountingRecordFactory struct{ runner *accountingRecordRunner }

func (f accountingRecordFactory) Provider() string { return "muse" }
func (f accountingRecordFactory) Launch(context.Context, ProviderLaunchRequest) (RunnerHost, error) {
	return f.runner, nil
}
func (f accountingRecordFactory) Restore(context.Context, ProviderHostRecord) (RunnerHost, error) {
	return f.runner, nil
}

func accountingRecordsProbe(t *testing.T) (*SessionHost, *LegacyMuseAccountingRecords, ControllerLease) {
	t.Helper()
	root := t.TempDir()
	runner := &accountingRecordRunner{inputTestBackend: &inputTestBackend{}, snapshot: MuseRunnerSnapshot{GoalTransport: "native", Provider: "muse", State: ProviderHostRunning, SessionID: "native-accounting", WorkspaceRoot: "/workspace", ApprovalMode: "allow"}}
	h, e := NewHost(HostOptions{Layout: Layout{Namespace: "accounting-fixture", StateDir: root, SessionsDir: filepath.Join(root, "sessions"), LockFile: filepath.Join(root, "manager.lock")}, Config: hostCommitProbeConfig{}, Notices: hostCommitProbeNotices{}, Codec: NewLegacyRecordCodec(), Factories: []ProviderHostFactory{accountingRecordFactory{runner}}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(h.Close)
	a, e := h.Attach(context.Background(), AttachRequest{Owner: "parent/primary", Provider: "muse"})
	if e != nil {
		t.Fatal(e)
	}
	lease, _, e := h.Input(context.Background(), a.Session.SessionID)
	if e != nil {
		t.Fatal(e)
	}
	store, e := NewLegacyMuseAccountingRecords(h)
	if e != nil {
		t.Fatal(e)
	}
	return h, store, lease
}

// Real committed files, actual current controller and sole-writer ACK back this
// narrow image adapter. It changes supported observation counters losslessly;
// this is not native goal/accounting restore qualification.
func TestLegacyAccountingRecordsCommitAndExactBase(t *testing.T) {
	ctx := context.Background()
	h, store, lease := accountingRecordsProbe(t)
	raw, e := store.Read(ctx, lease)
	if e != nil {
		t.Fatal(e)
	}
	c := NewLegacyRecordCodec()
	state, e := c.DecodeMuseAccounting(ctx, raw)
	if e != nil {
		t.Fatal(e)
	}
	state.TokenUsageTotal = 7
	next, e := c.EncodeMuseAccounting(ctx, raw, state)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.CompareAndSwap(ctx, lease, raw, next); e != nil {
		t.Fatal(e)
	}
	got, e := store.Read(ctx, lease)
	if e != nil || !bytes.Equal(compactLegacyJSON(got), compactLegacyJSON(next)) {
		t.Fatalf("committed provider image mismatch: %v", e)
	}
	path, _ := h.records.path(lease.SessionID)
	disk, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var record legacyRunnerRecord
	_ = json.Unmarshal(disk, &record)
	current, e := c.DecodeMuseAccounting(ctx, mustJSON(t, record.ProviderHost))
	if e != nil || current.TokenUsageTotal != 7 {
		t.Fatalf("missing actual durable counter: %v", e)
	}
	if e = store.CompareAndSwap(ctx, lease, raw, next); !errors.Is(e, ErrLegacyViewStale) {
		t.Fatalf("stale image accepted: %v", e)
	}
	if e = store.CompareAndSwap(ctx, lease, got, got); e != nil {
		t.Fatalf("exact no-op: %v", e)
	}
}
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}

// Foreign/stale leases, native identity/other field mutation and unsupported
// active goal state all refuse BEFORE a file write beside a valid counter commit.
func TestLegacyAccountingRecordsRefuseBeforeWrite(t *testing.T) {
	ctx := context.Background()
	h, store, lease := accountingRecordsProbe(t)
	raw, e := store.Read(ctx, lease)
	if e != nil {
		t.Fatal(e)
	}
	path, _ := h.records.path(lease.SessionID)
	original, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	for _, wrong := range []ControllerLease{{SessionID: "SES-foreign-session-00000001", Epoch: lease.Epoch}, {SessionID: lease.SessionID, Epoch: lease.Epoch + 1}} {
		if _, e = store.Read(ctx, wrong); e == nil {
			t.Fatal("wrong lease read accepted")
		}
		if e = store.CompareAndSwap(ctx, wrong, raw, raw); e == nil {
			t.Fatal("wrong lease write accepted")
		}
	}
	var wire legacyProviderImage
	_ = json.Unmarshal(raw, &wire)
	var image map[string]json.RawMessage
	_ = json.Unmarshal(wire.Opaque, &image)
	image["modelId"] = json.RawMessage(`"foreign-model"`)
	wire.Opaque = mustJSON(t, image)
	if e = store.CompareAndSwap(ctx, lease, raw, mustJSON(t, wire)); e == nil {
		t.Fatal("non-accounting mutation accepted")
	}
	var identity legacyProviderImage
	_ = json.Unmarshal(raw, &identity)
	var nativeIdentity map[string]json.RawMessage
	_ = json.Unmarshal(identity.Opaque, &nativeIdentity)
	nativeIdentity["sessionId"] = json.RawMessage(`"foreign-native-session"`)
	identity.Opaque = mustJSON(t, nativeIdentity)
	if e = store.CompareAndSwap(ctx, lease, raw, mustJSON(t, identity)); !errors.Is(e, ErrLegacyViewStale) {
		t.Fatalf("native identity change accepted: %v", e)
	}
	c := NewLegacyRecordCodec()
	state, _ := c.DecodeMuseAccounting(ctx, raw)
	state.GoalRef = &LegacyGoalRef{ID: "GOAL-SG-ABC123", Revision: 1}
	state.GoalObjective = "objective"
	state.GoalObserved = true
	unsupported, e := c.EncodeMuseAccounting(ctx, raw, state)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.CompareAndSwap(ctx, lease, raw, unsupported); !errors.Is(e, ErrHostCapabilityUnavailable) {
		t.Fatalf("active state bypassed actual codec: %v", e)
	}
	after, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(after, original) {
		t.Fatalf("refusal wrote provider record: %v", e)
	}
}

// An actual failed ACK is uncertainty, never success or a second write licensed
// by the previous image. Native/provider I/O is not used by this disk fixture.
func TestLegacyAccountingRecordsFailedAcknowledgementStaysUncertain(t *testing.T) {
	ctx := context.Background()
	h, store, lease := accountingRecordsProbe(t)
	raw, e := store.Read(ctx, lease)
	if e != nil {
		t.Fatal(e)
	}
	c := NewLegacyRecordCodec()
	state, _ := c.DecodeMuseAccounting(ctx, raw)
	state.TokenUsageTotal = 3
	next, e := c.EncodeMuseAccounting(ctx, raw, state)
	if e != nil {
		t.Fatal(e)
	}
	h.records.codec = &accountingAckFailure{LegacyRecordCodec: NewLegacyRecordCodec()}
	if e = store.CompareAndSwap(ctx, lease, raw, next); !errors.Is(e, ErrLegacyCommitUncertain) {
		t.Fatalf("failed persistence was success: %v", e)
	}
	if _, e = store.Read(ctx, lease); e == nil {
		t.Fatal("uncertain image readmitted")
	}
}

// The physical write is real; only the codec acknowledgement boundary refuses.
type accountingAckFailure struct{ *LegacyRecordCodec }

func (c *accountingAckFailure) AcknowledgeCommitted(context.Context, LegacyRecordInput) error {
	return errors.New("injected accounting acknowledgement refusal")
}
