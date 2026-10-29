package sessionmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

// LegacyMuseAccountingRecords exposes only the original provider-image
// accounting transaction to the in-process budget supplier. Calls must be made
// outside NativeGoalObserver's writer fence. It is neither a control RPC nor
// caller authority; the host validates its actual current controller and base.
type LegacyMuseAccountingRecords struct{ host *SessionHost }

func NewLegacyMuseAccountingRecords(host *SessionHost) (*LegacyMuseAccountingRecords, error) {
	if host == nil {
		return nil, ErrHostClosed
	}
	return &LegacyMuseAccountingRecords{host: host}, nil
}

// Caller holds the host map lock. The returned cross-process read/write
// admission must span the disk observation/transaction, never a native wait.
func (s *LegacyMuseAccountingRecords) admit(ctx context.Context, lease ControllerLease) (LegacyRecordInput, []byte, func(), error) {
	h := s.host
	if !h.nativeAccountingAdmitted(ctx, lease.SessionID) {
		return LegacyRecordInput{}, nil, nil, ErrLegacyCommitUncertain
	}
	record, ok := h.sessions[lease.SessionID]
	if !ok || record.Provider != "muse" || record.State == SessionStopped || record.GracefulRetirement || record.ConditionalStop != nil {
		return LegacyRecordInput{}, nil, nil, ErrHostCapabilityUnavailable
	}
	if h.records.uncertain[lease.SessionID] {
		return LegacyRecordInput{}, nil, nil, ErrLegacyCommitUncertain
	}
	if _, pending := h.pendingInitial[lease.SessionID]; pending {
		return LegacyRecordInput{}, nil, nil, ErrLegacyCommitUncertain
	}
	input := h.inputs[lease.SessionID]
	if input == nil {
		return LegacyRecordInput{}, nil, nil, ErrStaleInputController
	}
	current, e := input.CurrentLease(ctx, lease.SessionID)
	if e != nil {
		return LegacyRecordInput{}, nil, nil, e
	}
	if current != lease {
		return LegacyRecordInput{}, nil, nil, ErrStaleInputController
	}
	release, e := h.epochs.beginIO(ctx, lease)
	if e != nil {
		return LegacyRecordInput{}, nil, nil, e
	}
	fail := func(e error) (LegacyRecordInput, []byte, func(), error) {
		release()
		return LegacyRecordInput{}, nil, nil, e
	}
	base, ok := h.records.bases[lease.SessionID]
	if !ok {
		return fail(ErrLegacyViewStale)
	}
	path, e := h.records.path(lease.SessionID)
	if e != nil {
		return fail(e)
	}
	if e = h.records.matchesCurrent(path, base); e != nil {
		return fail(e)
	}
	var envelope struct {
		ProviderHost json.RawMessage `json:"provider_host"`
	}
	if e = decodeLegacyJSON(base.Bytes, &envelope); e != nil {
		return fail(e)
	}
	c := NewLegacyRecordCodec()
	if _, e = c.DecodeMuseAccounting(ctx, envelope.ProviderHost); e != nil {
		return fail(e)
	}
	return copyLegacyRecordImage(base), append([]byte(nil), envelope.ProviderHost...), release, nil
}

// Read returns the actual committed original provider_host bytes, never a
// reconstructed neutral JSON view or an unacknowledged candidate.
func (s *LegacyMuseAccountingRecords) Read(ctx context.Context, lease ControllerLease) ([]byte, error) {
	if s == nil || s.host == nil {
		return nil, ErrHostClosed
	}
	h := s.host
	if e := h.lockContext(ctx); e != nil {
		return nil, e
	}
	defer h.mu.Unlock()
	_, raw, release, e := s.admit(ctx, lease)
	if e != nil {
		return nil, e
	}
	defer release()
	return raw, nil
}

// CompareAndSwap changes only the original native accounting field group.
// The configured full-record codec must admit and round-trip the actual result
// BEFORE any write. Unsupported active accounting still refuses here; this
// adapter is not an escape hatch around native restore/supplier requirements.
func (s *LegacyMuseAccountingRecords) CompareAndSwap(ctx context.Context, lease ControllerLease, before, after []byte) error {
	if s == nil || s.host == nil {
		return ErrHostClosed
	}
	// Own caller buffers before admission or I/O.
	before = append([]byte(nil), before...)
	after = append([]byte(nil), after...)
	h := s.host
	if e := h.lockContext(ctx); e != nil {
		return e
	}
	defer h.mu.Unlock()
	base, current, release, e := s.admit(ctx, lease)
	if e != nil {
		return e
	}
	defer release()
	if !bytes.Equal(current, before) {
		return ErrLegacyViewStale
	}
	c := NewLegacyRecordCodec()
	state, e := c.DecodeMuseAccounting(ctx, after)
	if e != nil {
		return e
	}
	expected, e := c.EncodeMuseAccounting(ctx, current, state)
	if e != nil {
		return e
	}
	if !bytes.Equal(compactLegacyJSON(expected), compactLegacyJSON(after)) {
		return fmt.Errorf("%w: only native accounting fields may change", ErrLegacyCodecMismatch)
	}
	if bytes.Equal(current, after) {
		return nil
	}
	var old legacyRunnerRecord
	if e = decodeLegacyJSON(base.Bytes, &old); e != nil {
		return e
	}
	var nextProvider legacyProviderImage
	if e = decodeLegacyJSON(after, &nextProvider); e != nil {
		return e
	}
	next := old
	next.ProviderHost = nextProvider
	candidate, e := encodeLegacySnapshot(base.Bytes, old, next)
	if e != nil {
		return e
	}
	projected, e := h.records.decode(ctx, legacyRecordImage(lease.SessionID, candidate))
	if e != nil {
		return e
	}
	// The existing typed codec must preserve every requested accounting fact;
	// fields absent from the runtime model cannot silently disappear in merge.
	roundtrip, e := h.records.codec.EncodeMerge(ctx, copyLegacyRecordImage(base), projected)
	if e != nil {
		return e
	}
	if !bytes.Equal(compactLegacyJSON(roundtrip), compactLegacyJSON(candidate)) {
		return fmt.Errorf("%w: native accounting supplier cannot preserve this update", ErrLegacyCodecMismatch)
	}
	if e = h.updateRecord(ctx, projected); e != nil {
		return e
	}
	h.sessions[lease.SessionID] = projected
	return nil
}
