// Package sessionrun renders captured-controller owner operations through the
// session host's typed input and native observation ports.
package sessionrun

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// Runtime supplies provider rendering; attachment, activation, goal preflight,
// retirement and durable owner outcomes remain with the session host.
// Construct it before the host, then BindHost once before serving requests.
type Runtime struct {
	mu               sync.Mutex
	host             *core.SessionHost
	store            *budget.Store
	budgets          *budget.Factory
	pendingTimeout   time.Duration
	runs             map[string]chan struct{}
	codexAssignments map[core.ControllerLease]codexAssignment
}

func New(store *budget.Store, settings ...core.HostSettings) *Runtime {
	var timeout time.Duration
	if len(settings) == 1 {
		timeout = settings[0].GoalAckTimeout
	}
	return &Runtime{store: store, pendingTimeout: timeout, runs: map[string]chan struct{}{}}
}
func (r *Runtime) BindHost(h *core.SessionHost) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h == nil || r.host != nil {
		return core.ErrInvalidInput
	}
	r.host = h
	return nil
}

func (r *Runtime) runLock(ctx context.Context, id string) (func(), error) {
	r.mu.Lock()
	if r.runs == nil {
		r.runs = map[string]chan struct{}{}
	}
	gate := r.runs[id]
	if gate == nil {
		gate = make(chan struct{}, 1)
		r.runs[id] = gate
	}
	r.mu.Unlock()
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func validateCapture(c core.CapturedOwnerRun) error {
	if c.Lease.SessionID == "" || c.Lease.Epoch == 0 || c.Input == nil || c.Native == nil || c.Record.SessionID != c.Lease.SessionID || c.Record.State == core.SessionStopped || c.Record.GracefulRetirement || c.Record.ConditionalStop != nil || c.Record.ProviderHostPendingFork || c.Committed.SessionID != c.Lease.SessionID {
		return core.ErrStaleInputController
	}
	sum := sha256.Sum256(c.Committed.Bytes)
	if c.Committed.SHA256 != hex.EncodeToString(sum[:]) {
		return core.ErrLegacyViewStale
	}
	return nil
}
func committedProvider(c core.CapturedOwnerRun) ([]byte, error) {
	if e := validateCapture(c); e != nil {
		return nil, e
	}
	var v struct {
		SessionID    string          `json:"session_id"`
		Provider     string          `json:"provider"`
		ProviderHost json.RawMessage `json:"provider_host"`
	}
	if e := json.Unmarshal(c.Committed.Bytes, &v); e != nil {
		return nil, e
	}
	if v.SessionID != c.Record.SessionID || v.Provider != c.Record.Provider || len(v.ProviderHost) == 0 {
		return nil, core.ErrLegacyViewStale
	}
	return append([]byte(nil), v.ProviderHost...), nil
}

// RunCapturedOwner never refreshes a captured lease or holds a native write
// fence across the long-running provider observation phase.
func (r *Runtime) RunCapturedOwner(ctx context.Context, c core.CapturedOwnerRun, q core.ProviderRunRequest) (*core.ProviderRunResult, error) {
	if strings.TrimSpace(q.Prompt) == "" {
		return nil, core.ErrInvalidInput
	}
	if e := validateCapture(c); e != nil {
		return nil, e
	}
	release, e := r.runLock(ctx, c.Lease.SessionID)
	if e != nil {
		return nil, e
	}
	defer release()
	if e = r.requireCurrentCapture(ctx, c); e != nil {
		return nil, e
	}
	switch c.Record.Provider {
	case "muse":
		return r.runMuse(ctx, c, q)
	case "codex":
		return r.runCodex(ctx, c, q)
	default:
		return nil, core.ErrHostCapabilityUnavailable
	}
}

func (r *Runtime) requireCurrentCapture(ctx context.Context, c core.CapturedOwnerRun) error {
	r.mu.Lock()
	h := r.host
	r.mu.Unlock()
	if h == nil {
		return core.ErrHostClosed
	}
	current, e := h.OwnerRunState(ctx, c.Lease)
	if e != nil {
		return e
	}
	if current.Record.Provider != c.Record.Provider || current.Committed.SHA256 != c.Committed.SHA256 || !bytes.Equal(current.Committed.Bytes, c.Committed.Bytes) {
		return core.ErrLegacyViewStale
	}
	return nil
}

// This adapter calls the actual core-owned current-record transaction. Native
// identity is sampled from the captured physical runner, independently of JSON.
type capturedMuseImages struct {
	records *core.LegacyMuseAccountingRecords
	capture core.CapturedOwnerRun
}

func (i *capturedMuseImages) ReadMuseImage(ctx context.Context, l core.ControllerLease) (string, []byte, error) {
	if l != i.capture.Lease {
		return "", nil, core.ErrStaleInputController
	}
	raw, e := i.records.Read(ctx, l)
	if e != nil {
		return "", nil, e
	}
	n, e := i.capture.Native.Snapshot()
	if e != nil {
		return "", nil, e
	}
	if n.Kind != "muse-msp" || n.MuseMSP == nil || n.MuseMSP.SessionID == "" {
		return "", nil, core.ErrLegacyViewStale
	}
	return n.MuseMSP.SessionID, raw, nil
}
func (i *capturedMuseImages) CompareAndSwapMuseImage(ctx context.Context, l core.ControllerLease, a, b []byte) error {
	if l != i.capture.Lease {
		return core.ErrStaleInputController
	}
	return i.records.CompareAndSwap(ctx, l, a, b)
}
func (r *Runtime) museSource(ctx context.Context, c core.CapturedOwnerRun) (*budget.LegacySource, core.LegacyMuseAccountingSnapshot, error) {
	r.mu.Lock()
	h := r.host
	r.mu.Unlock()
	if h == nil {
		return nil, core.LegacyMuseAccountingSnapshot{}, core.ErrHostClosed
	}
	if r.store == nil {
		return nil, core.LegacyMuseAccountingSnapshot{}, budget.ErrAccountingUnavailable
	}
	records, e := core.NewLegacyMuseAccountingRecords(h)
	if e != nil {
		return nil, core.LegacyMuseAccountingSnapshot{}, e
	}
	images := &capturedMuseImages{records: records, capture: c}
	native, raw, e := images.ReadMuseImage(ctx, c.Lease)
	if e != nil {
		return nil, core.LegacyMuseAccountingSnapshot{}, e
	}
	base, e := committedProvider(c)
	if e != nil {
		return nil, core.LegacyMuseAccountingSnapshot{}, e
	}
	if !bytes.Equal(raw, base) {
		return nil, core.LegacyMuseAccountingSnapshot{}, core.ErrLegacyViewStale
	}
	codec := core.NewLegacyRecordCodec()
	state, e := codec.DecodeMuseAccounting(ctx, raw)
	if e != nil {
		return nil, state, e
	}
	if state.SessionID != native {
		return nil, state, core.ErrInputAcknowledgementMismatch
	}
	return &budget.LegacySource{Codec: codec, Images: images}, state, nil
}
func (r *Runtime) runCodex(ctx context.Context, c core.CapturedOwnerRun, q core.ProviderRunRequest) (*core.ProviderRunResult, error) {
	raw, e := committedProvider(c)
	if e != nil {
		return nil, e
	}
	record, e := core.NewLegacyRecordCodec().DecodeProviderHost(ctx, raw)
	if e != nil {
		return nil, e
	}
	n, e := c.Native.Snapshot()
	if e != nil {
		return nil, e
	}
	if record.Provider != "codex" || record.Snapshot.Codex == nil || n.Kind != "codex" || n.Codex == nil || n.Codex.ThreadID == "" || record.Snapshot.Codex.ThreadID != n.Codex.ThreadID {
		return nil, core.ErrLegacyViewStale
	}
	if c.Record.AppliedGoalRef != nil && n.Codex.ManagedTurnID != "" {
		if err := r.combinedAssignmentMatches(ctx, c, q, n.Codex.ThreadID, n.Codex.ManagedTurnID); err != nil {
			return nil, err
		}
	}
	observer, ok := c.Native.(core.ProviderOwnerRunner)
	if !ok {
		return nil, core.ErrHostCapabilityUnavailable
	}
	// Until an acknowledged assignment exists, the only native start is the
	// captured writer. The provider observer below never renders an assignment.
	if n.Codex.ManagedTurnID == "" {
		ack, submitErr := submitCodexAssignment(ctx, c.Input, c.Lease, n.Codex.ThreadID, "", q.Prompt, nil, "apply")
		if submitErr != nil {
			return nil, submitErr
		}
		assigned, snapshotErr := c.Native.Snapshot()
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		if assigned.Kind != "codex" || assigned.Codex == nil || assigned.Codex.ThreadID != n.Codex.ThreadID || assigned.Codex.ManagedTurnID != ack.CodexTurn.TurnID {
			return nil, core.ErrInputAcknowledgementMismatch
		}
	}
	result, runErr := observer.RunOwner(ctx, q)
	after, snapshotErr := c.Native.Snapshot()
	if snapshotErr == nil && (after.Kind != "codex" || after.Codex == nil || after.Codex.ThreadID != n.Codex.ThreadID) {
		snapshotErr = core.ErrLegacyViewStale
	}
	return result, errors.Join(runErr, snapshotErr)
}
