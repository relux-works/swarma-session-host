package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

// OwnerRunContext retains the original managed launch metadata. These fields
// describe a source fact; they grant no session, holder, goal or input authority.
// RuntimeControl is retained in full, as in the neutral launch envelope. Its
// live repository/environment validation belongs to the raising-side source.
type OwnerRunContext struct {
	RunID          string          `json:"run_id"`
	TaskID         string          `json:"task_id"`
	BoardDir       string          `json:"board_dir,omitempty"`
	ContextCtxID   string          `json:"context_ctx_id,omitempty"`
	DeliveryGoalID string          `json:"delivery_goal_id"`
	RuntimeControl json.RawMessage `json:"runtime_control,omitempty"`
}

// CodexOwnerRunSource is a read-only fact port on an admitted host writer.
// Implementations must check the current lease and actual committed/native cut.
// A returned nonnil context is metadata, never permission to deliver a revision.
type CodexOwnerRunSource interface {
	CodexOwnerRunSnapshot(context.Context, ControllerLease) (CodexRunnerSnapshot, error)
}

func copyCodexOwnerRun(src *OwnerRunContext) *OwnerRunContext {
	if src == nil {
		return nil
	}
	out := *src
	out.RuntimeControl = append(src.RuntimeControl[:0:0], src.RuntimeControl...)
	return &out
}

func prepareCodexOwnerRun(prepared *CodexPreparedLaunch, prior *CodexRunnerSnapshot) (*OwnerRunContext, error) {
	if prepared == nil {
		return nil, ErrHostCapabilityUnavailable
	}
	selected := prepared.OwnerRun
	if selected == nil && prior != nil {
		selected = prior.OwnerRun
	}
	if err := validateCodexOwnerShape(selected); err != nil {
		return nil, err
	}
	return copyCodexOwnerRun(selected), nil
}

// This is only the original owner/plane shape. It does not substitute for the
// original live RuntimeControl snapshot validation or canonical goal authority.
func validateCodexOwnerShape(owner *OwnerRunContext) error {
	if owner == nil {
		return nil
	}
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(owner.RunID)), "RUN-") {
		return fmt.Errorf("%w: owner_run.run_id must be a RUN-* identifier", ErrInvalidInput)
	}
	if strings.TrimSpace(owner.TaskID) == "" {
		return fmt.Errorf("%w: owner_run.task_id is required", ErrInvalidInput)
	}
	child := strings.TrimSpace(owner.ContextCtxID)
	if child != "" && !strings.HasPrefix(strings.ToUpper(child), "CTX-") {
		return fmt.Errorf("%w: owner_run.context_ctx_id must be a CTX-* identifier", ErrInvalidInput)
	}
	if strings.TrimSpace(owner.DeliveryGoalID) == "" && child == "" {
		return fmt.Errorf("%w: owner_run requires an owning plane: either delivery_goal_id or context_ctx_id", ErrInvalidInput)
	}
	if len(owner.RuntimeControl) != 0 && !bytes.Equal(bytes.TrimSpace(owner.RuntimeControl), []byte("null")) {
		var object map[string]json.RawMessage
		if err := decodeLegacyJSON(owner.RuntimeControl, &object); err != nil || object == nil {
			return fmt.Errorf("%w: owner_run.runtime_control must be an object", ErrInvalidInput)
		}
	}
	return nil
}

func decodeCodexOwnerRun(raw json.RawMessage) (*OwnerRunContext, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var owner *OwnerRunContext
	if err := decodeLegacyJSON(raw, &owner); err != nil {
		return nil, fmt.Errorf("%w: malformed owner_run: %v", ErrInvalidInput, err)
	}
	return owner, nil
}

// ReadCapturedCodexOwnerRun checks data from an actual captured cut without
// bypassing the live codec's unsupported-state admission. The caller must also
// establish that this capture is still current; this pure reader cannot do so.
func ReadCapturedCodexOwnerRun(ctx context.Context, capture CapturedOwnerRun) (CodexRunnerSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	r := capture.Record
	if capture.Lease.SessionID == "" || capture.Lease.Epoch == 0 || r.SessionID != capture.Lease.SessionID || capture.Committed.SessionID != r.SessionID || r.State == SessionStopped || r.GracefulRetirement || r.ConditionalStop != nil || r.ProviderHostPendingFork || absentHostPort(capture.Native) || absentHostPort(capture.Input) {
		return CodexRunnerSnapshot{}, ErrStaleInputController
	}
	if err := codecLegacyInput(capture.Committed); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	var wire legacyRunnerRecord
	if err := decodeLegacyJSON(capture.Committed.Bytes, &wire); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	owner, err := wire.Owner.key()
	if err != nil || wire.Contract != r.Contract || wire.SessionID != r.SessionID || owner != r.Owner || wire.Provider != "codex" || r.Provider != "codex" || wire.State != r.State || wire.ProviderHost.Provider != "codex" {
		return CodexRunnerSnapshot{}, ErrLegacyViewStale
	}
	var archived legacyCodexSnapshot
	if err := decodeLegacyJSON(wire.ProviderHost.Opaque, &archived); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	if err := validateLegacyCodex(archived); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	committed, err := projectCodexSnapshotWithOwner(archived)
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	if err := validateCodexOwnerShape(committed.OwnerRun); err != nil {
		return CodexRunnerSnapshot{}, err
	}
	native, err := capture.Native.Snapshot()
	if err != nil {
		return CodexRunnerSnapshot{}, err
	}
	if native.Kind != "codex" || native.Codex == nil || r.ProviderHost.Provider != "codex" || r.ProviderHost.Snapshot.Kind != "codex" || r.ProviderHost.Snapshot.Codex == nil {
		return CodexRunnerSnapshot{}, ErrLegacyViewStale
	}
	for _, actual := range []*CodexRunnerSnapshot{native.Codex, r.ProviderHost.Snapshot.Codex} {
		if actual.Stopped || actual.ThreadID != committed.ThreadID || actual.PID != committed.PID || actual.URL != committed.URL || actual.ProjectDir != committed.ProjectDir || actual.CodexHome != committed.CodexHome || actual.ManagedTurnID != committed.ManagedTurnID || !reflect.DeepEqual(actual.ProcessIdentity, committed.ProcessIdentity) || !reflect.DeepEqual(actual.GoalTokenBudget, committed.GoalTokenBudget) || !equalCodexOwnerRun(actual.OwnerRun, committed.OwnerRun) {
			return CodexRunnerSnapshot{}, ErrLegacyViewStale
		}
	}
	return *copyRunnerSnapshot(RunnerSnapshot{Kind: "codex", Codex: &committed}).Codex, nil
}

func equalCodexOwnerRun(a, b *OwnerRunContext) bool {
	if a == nil || b == nil {
		return a == b
	}
	x, y := *a, *b
	x.RuntimeControl, y.RuntimeControl = nil, nil
	if !reflect.DeepEqual(x, y) {
		return false
	}
	values := make([]any, 2)
	for i, raw := range []json.RawMessage{a.RuntimeControl, b.RuntimeControl} {
		if len(raw) == 0 {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if decoder.Decode(&values[i]) != nil {
			return false
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			return false
		}
	}
	return reflect.DeepEqual(values[0], values[1])
}
