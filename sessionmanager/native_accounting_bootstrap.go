package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
)

func nativeAccountingPreflightRecord(ctx context.Context, record RunnerRecord, image LegacyRecordInput) (RunnerRecord, error) {
	out := copyRunnerRecord(record)
	s := out.ProviderHost.Snapshot.MuseMSP
	if s == nil || s.GoalTransport != "native" || s.NativeAccounting != nil {
		return out, nil
	}
	_, state, e := nativeAccountingImage(ctx, image)
	if e != nil {
		return RunnerRecord{}, e
	}
	if state.SessionID != s.SessionID || state.WorkspaceRoot != s.WorkspaceRoot {
		return RunnerRecord{}, ErrLegacyViewStale
	}
	// Detached exact raw data for validation only, not an observed fact/commit.
	group := copyLegacyMuseNativeAccounting(state.LegacyMuseNativeAccounting)
	s.NativeAccounting = &group
	return out, nil
}
func nativeAccountingImage(ctx context.Context, image LegacyRecordInput) ([]byte, LegacyMuseAccountingSnapshot, error) {
	var envelope struct {
		ProviderHost json.RawMessage `json:"provider_host"`
	}
	if e := decodeLegacyJSON(image.Bytes, &envelope); e != nil {
		return nil, LegacyMuseAccountingSnapshot{}, e
	}
	state, e := NewLegacyRecordCodec().DecodeMuseAccounting(ctx, envelope.ProviderHost)
	return append([]byte(nil), envelope.ProviderHost...), state, e
}

// The original renderer's applyNativeObservation marks served state known only
// under WithNativeGoal. Here only a genuinely empty canonical account may be
// initialized: real journal preflight has already refused an active/unresolved
// assignment. A native snapshot alone cannot create the observation or a goal.
// Caller is outside mu and holds only its private restore reservation context.
func (h *SessionHost) bootstrapObservedEmptyMuseAccounting(ctx context.Context, cut CapturedOwnerRun) (CapturedOwnerRun, error) {
	codec, ok := h.options.Codec.(*LegacyRecordCodec)
	if !ok || absentHostPort(codec.museAccounting) {
		return cut, nil
	}
	before, state, e := nativeAccountingImage(ctx, cut.Committed)
	if e != nil {
		return cut, e
	}
	if state.GoalObserved {
		return cut, nil
	}
	if legacyMuseHasGoalState(legacyMuseSnapshot{LegacyMuseNativeAccounting: state.LegacyMuseNativeAccounting}) {
		return cut, legacyNativeStateUnavailable()
	}
	physical, e := cut.Native.Snapshot()
	if e != nil {
		return cut, e
	}
	if physical.Kind != "muse-msp" || physical.MuseMSP == nil || physical.MuseMSP.SessionID != state.SessionID || physical.MuseMSP.WorkspaceRoot != state.WorkspaceRoot {
		return cut, ErrLegacyViewStale
	}
	source, ok := cut.Input.(MuseObservationSource)
	if !ok {
		return cut, ErrMuseObservationUnavailable
	}
	observer, ok := cut.Input.(NativeGoalObserver)
	if !ok {
		return cut, ErrHostCapabilityUnavailable
	}
	feed, e := source.SubscribeMuse(ctx, cut.Lease)
	if e != nil {
		return cut, e
	}
	defer feed.Close()
	observation, e := feed.Next(ctx)
	if e != nil {
		return cut, e
	}
	fold := observation.RestoreFold
	if observation.Lease != cut.Lease || observation.Sequence == 0 || observation.ObservedAt.IsZero() || observation.Gap != nil || observation.Event != nil || fold == nil || fold.SessionID != state.SessionID || fold.Cursor == "" || fold.Items == nil {
		return cut, ErrMuseObservationInvalid
	}
	if fold.Cumulative.TotalTokens < state.TokenUsageTotal || fold.Cumulative.PromptTokens < 0 || fold.Cumulative.OutputTokens < 0 {
		return cut, ErrMuseObservationUncertain
	}
	next := state
	e = observer.WithNativeGoal(ctx, cut.Lease, func(actual NativeGoalObservation, _ HostInputWriter) error {
		if actual.SessionID != cut.Lease.SessionID || actual.Epoch != cut.Lease.Epoch || actual.Sequence == 0 || actual.ObservedAt.IsZero() {
			return ErrStaleInputController
		}
		if actual.Objective != "" {
			return ErrInputAcknowledgementMismatch
		}
		if actual.Cumulative == nil || actual.Cumulative.TotalTokens < fold.Cumulative.TotalTokens || actual.Cumulative.PromptTokens < fold.Cumulative.PromptTokens || actual.Cumulative.OutputTokens < fold.Cumulative.OutputTokens {
			return ErrMuseObservationUncertain
		}
		next.GoalObserved = true
		next.TokenUsageTotal = actual.Cumulative.TotalTokens
		next.ActiveTurnID = ""
		if fold.ActiveTurn != nil {
			next.ActiveTurnID = fold.ActiveTurn.TurnID
		}
		if fold.SessionActiveTurnID != "" {
			next.ActiveTurnID = fold.SessionActiveTurnID
		}
		return nil
	})
	if e != nil {
		return cut, e
	}
	physical, e = cut.Native.Snapshot()
	if e != nil {
		return cut, e
	}
	if physical.MuseMSP == nil || physical.Kind != "muse-msp" || physical.MuseMSP.SessionID != state.SessionID || physical.MuseMSP.WorkspaceRoot != state.WorkspaceRoot {
		return cut, ErrLegacyViewStale
	}
	// Outside native observation admission: CAS takes its own current lease and
	// exact image fence, passes normal decoder/sole writer/readback/ACK, then the
	// real renderer imports the entire state before any writer is exposed.
	records, e := NewLegacyMuseAccountingRecords(h)
	if e != nil {
		return cut, e
	}
	current, e := records.Read(ctx, cut.Lease)
	if e != nil {
		return cut, e
	}
	if !bytes.Equal(current, before) {
		return cut, ErrLegacyViewStale
	}
	after, e := NewLegacyRecordCodec().EncodeMuseAccounting(ctx, before, next)
	if e != nil {
		return cut, e
	}
	if e = records.CompareAndSwap(ctx, cut.Lease, before, after); e != nil {
		return cut, e
	}
	return h.OwnerRunState(ctx, cut.Lease)
}
