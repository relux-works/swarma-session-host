package sessionhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"time"

	ap "github.com/relux-works/swarma-session-host/internal/adapterprotocol"
	"github.com/relux-works/swarma-session-host/internal/runner"
)

// ProtocolState retains bounded outgoing frames and protocol metadata. Incoming
// bytes remain in the runner; a restored driver requests its complete-frame
// replay offset. WireSequence is independent from the notice queue sequence.
type ProtocolState struct {
	BoundaryVersion int             `json:"boundary_version"`
	ObservedRelease string          `json:"observed_release"`
	Checkpoint      json.RawMessage `json:"checkpoint"`
	WireSequence    uint64          `json:"wire_sequence"`
	Pending         []WireAction    `json:"pending"`
	View            ap.View         `json:"view"`
	Attention       []ap.Attention  `json:"attention,omitempty"`
	Usage           *ap.Usage       `json:"usage,omitempty"`
}
type WireAction struct {
	Sequence uint64 `json:"sequence"`
	Key      string `json:"key"`
	InputID  string `json:"input_id"`
	Data     []byte `json:"data"`
	Digest   string `json:"digest"`
	Receipt  string `json:"receipt,omitempty"`
}

func wireDigest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func cloneState(s State) State {
	s.Records = slices.Clone(s.Records)
	if s.Admission != nil {
		a := *s.Admission
		s.Admission = &a
	}
	if s.Protocol != nil {
		p := *s.Protocol
		p.Checkpoint = bytes.Clone(p.Checkpoint)
		p.Pending = slices.Clone(p.Pending)
		p.Attention = slices.Clone(p.Attention)
		if p.Usage != nil {
			u := *p.Usage
			p.Usage = &u
		}
		for i := range p.Pending {
			p.Pending[i].Data = bytes.Clone(p.Pending[i].Data)
		}
		s.Protocol = &p
	}
	return s
}

func validProtocolState(s State) bool {
	if s.Protocol == nil {
		for _, r := range s.Records {
			if r.NativeAccepted || r.NativeSession != "" || r.NativeTurn != "" || r.NativeReceipt != "" || r.Outcome != "" {
				return false
			}
		}
		return true
	}
	p := s.Protocol
	if p.BoundaryVersion != ap.Version || p.ObservedRelease == "" {
		return false
	}
	if len(p.Checkpoint) == 0 || len(p.Checkpoint) > ap.MaxCheckpoint || !json.Valid(p.Checkpoint) || p.WireSequence > runner.MaxWrites || len(p.Pending) > ap.MaxActions || uint64(len(p.Pending)) > p.WireSequence {
		return false
	}
	// The descriptor-independent view checks use no facts/actions. A factory
	// must additionally validate the actual descriptor and opaque checkpoint.
	if ap.Validate(ap.Descriptor{Version: ap.Version, ObservedRelease: p.ObservedRelease, Name: s.Adapter, PrimaryTransport: "pipe"}, ap.Transition{Checkpoint: p.Checkpoint, View: p.View}) != nil {
		return false
	}
	attentionView := p.View
	attentionView.Activity = ap.Approval
	if ap.Validate(ap.Descriptor{Version: ap.Version, ObservedRelease: p.ObservedRelease, Name: s.Adapter, PrimaryTransport: "pipe", Capabilities: ap.Capabilities{TokenUsage: true}}, ap.Transition{Checkpoint: p.Checkpoint, View: attentionView, Attention: p.Attention, Usage: p.Usage}) != nil {
		return false
	}
	keys := map[string]bool{}
	for i, a := range p.Pending {
		if a.Receipt != "" && a.Receipt != "write_missing" && a.Receipt != "write_unknown" && a.Receipt != "sequence_conflict" {
			return false
		}
		if a.Sequence != p.WireSequence-uint64(len(p.Pending))+uint64(i)+1 || !scalar.MatchString(a.Key) || keys[a.Key] || len(a.Data) == 0 || len(a.Data) > ap.MaxWrite || wireDigest(a.Data) != a.Digest {
			return false
		}
		keys[a.Key] = true
		if a.InputID != "" {
			found := false
			for _, r := range s.Records {
				if r.Notice.ID == a.InputID && (r.State == "unknown" || r.State == "applied") {
					found = true
				}
			}
			if !found {
				return false
			}
		}
	}
	for _, r := range s.Records {
		if r.NativeAccepted {
			if r.NativeSession == "" || r.State == "queued" {
				return false
			}
		} else if r.NativeSession != "" || r.NativeTurn != "" || r.NativeReceipt != "" || r.Outcome != "" {
			return false
		}
		if (r.State == "applied") != (r.Outcome == "completed") {
			return false
		}
		if r.Outcome != "" && r.Outcome != "completed" && r.Outcome != "failed" && r.Outcome != "interrupted" {
			return false
		}
	}
	return true
}

// persistTransitionLocked commits checkpoint, outgoing frames and native facts
// together before any pipe effect. The driver is local and performs no I/O.
func (h *Host) persistTransitionLocked(t ap.Transition) error {
	if h.state.Admission != nil && t.View.NativeSession != "" && t.View.NativeSession != h.state.Admission.Record.Binding.NativeSession {
		return errors.New("admission_native_session_conflict")
	}
	if ap.Validate(h.driver.Descriptor(), t) != nil {
		return errors.New("protocol_transition_refused")
	}
	next := cloneState(h.state)
	p := next.Protocol
	if len(t.Actions) > 0 && len(p.Pending) > 0 {
		return errors.New("protocol_outbox_occupied")
	}
	if p.WireSequence+uint64(len(t.Actions)) > runner.MaxWrites {
		return errors.New("protocol_write_capacity")
	}
	p.Checkpoint = bytes.Clone(t.Checkpoint)
	p.View = t.View
	for _, a := range t.Attention {
		found := false
		for _, prior := range p.Attention {
			if prior.Key == a.Key {
				if prior != a {
					return errors.New("protocol_attention_conflict")
				}
				found = true
			}
		}
		if !found {
			p.Attention = append(p.Attention, a)
		}
	}
	if t.Usage != nil {
		u := *t.Usage
		p.Usage = &u
	}
	for _, a := range t.Actions {
		p.WireSequence++
		p.Pending = append(p.Pending, WireAction{Sequence: p.WireSequence, Key: a.Key, InputID: a.InputID, Data: bytes.Clone(a.Bytes), Digest: wireDigest(a.Bytes)})
	}
	for _, f := range t.Facts {
		i := -1
		for n, r := range next.Records {
			if r.Notice.ID == f.InputID {
				i = n
				break
			}
		}
		if i < 0 {
			return errors.New("protocol_unknown_notice")
		}
		r := &next.Records[i]
		if r.State != "unknown" && r.State != "applied" {
			return errors.New("protocol_notice_not_submitted")
		}
		switch f.Kind {
		case ap.InputAccepted:
			if r.NativeAccepted && (r.NativeSession != f.NativeSession || r.NativeReceipt != f.ReceiptID || (r.NativeTurn != "" && r.NativeTurn != f.NativeID)) {
				return errors.New("protocol_acceptance_conflict")
			}
			r.NativeAccepted = true
			r.NativeSession = f.NativeSession
			r.NativeTurn = f.NativeID
			r.NativeReceipt = f.ReceiptID
		case ap.TurnCompleted, ap.TurnFailed, ap.TurnInterrupted:
			if !r.NativeAccepted || r.NativeSession != f.NativeSession || r.NativeReceipt != f.ReceiptID || (r.NativeTurn != "" && r.NativeTurn != f.NativeID) {
				return errors.New("protocol_completion_without_acceptance")
			}
			outcome := "completed"
			if f.Kind == ap.TurnFailed {
				outcome = "failed"
			}
			if f.Kind == ap.TurnInterrupted {
				outcome = "interrupted"
			}
			if r.Outcome != "" && r.Outcome != outcome {
				return errors.New("protocol_outcome_conflict")
			}
			r.Outcome = outcome
			r.NativeTurn = f.NativeID
			if outcome == "completed" {
				r.State = "applied"
			} else {
				next.Fence = "unknown"
			}
		}
	}
	next.InputState = InputState(t.View.Activity)
	next.SecurityStopped = next.SecurityStopped || t.View.Activity == ap.SecurityStop
	if !validProtocolState(next) {
		return errors.New("protocol_state_refused")
	}
	if reflect.DeepEqual(next, h.state) {
		return nil
	}
	return h.persistLocked(next)
}

func (h *Host) failProtocol() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.protocolFailed = true
	next := cloneState(h.state)
	next.Fence = "unknown"
	next.InputState = InputUnknown
	_ = h.persistLocked(next)
}

func (h *Host) blockProtocolWrites() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.writeBlocked = true
	next := cloneState(h.state)
	next.Fence = "unknown"
	next.InputState = InputUnknown
	_ = h.persistLocked(next)
}

func (h *Host) protocolCall(req runner.Request) (runner.Response, error) {
	if req.Op == "write" {
		if err := h.checkAdmission(); err != nil {
			return runner.Response{}, err
		}
	}
	ctx, cancel := context.WithTimeout(h.ctx, h.cfg.IOTimeout)
	defer cancel()
	r, err := h.client.Call(ctx, req)
	if err != nil {
		return r, err
	}
	h.mu.Lock()
	instance, epoch := h.state.RunnerInstance, h.state.Epoch
	h.mu.Unlock()
	if r.Instance != instance || r.Epoch != epoch || r.ProtocolVersion != runner.ProtocolVersion || r.WriteReceiptVersion != runner.WriteReceiptVersion {
		return r, errors.New("protocol_runner_conflict")
	}
	return r, nil
}

// reconcileOutbox is read-only after restoration. Only a matching recorded
// written effect can be retired. Missing and unknown effects are never retried.
func (h *Host) reconcileOutbox() bool {
	for {
		h.mu.Lock()
		if h.ctx.Err() != nil || h.err != nil || h.protocolFailed || h.writeBlocked {
			h.mu.Unlock()
			return false
		}
		if len(h.state.Protocol.Pending) == 0 {
			h.recovering = false
			h.mu.Unlock()
			return true
		}
		a := h.state.Protocol.Pending[0]
		epoch := h.state.Epoch
		recovering := h.recovering
		if !recovering && (h.state.StopRequested || h.state.SecurityStopped || h.state.Fence == "unknown" || (a.InputID != "" && h.state.Fence != "open")) {
			h.mu.Unlock()
			return false
		}
		h.mu.Unlock()
		req := runner.Request{Op: "write", Execution: h.cfg.Execution, Epoch: epoch, Sequence: a.Sequence, Data: a.Data}
		if recovering {
			req.Op = "write-receipt"
			req.Data = nil
			req.Digest = a.Digest
		}
		r, err := h.protocolCall(req)
		if err != nil {
			h.failProtocol()
			return false
		}
		if r.Code != "written" {
			if r.Code == "write_missing" || r.Code == "write_unknown" || r.Code == "sequence_conflict" {
				h.mu.Lock()
				next := cloneState(h.state)
				next.Protocol.Pending[0].Receipt = r.Code
				persistErr := h.persistLocked(next)
				h.mu.Unlock()
				if persistErr != nil {
					return false
				}
			}
			if r.Code == "write_missing" || r.Code == "write_unknown" {
				h.blockProtocolWrites()
			} else {
				h.failProtocol()
			}
			return false
		}
		h.mu.Lock()
		next := cloneState(h.state)
		next.Protocol.Pending = next.Protocol.Pending[1:]
		terminal := r.OutputEnded || r.Exited || r.Overflow
		if terminal {
			// Keep the committed write receipt, but fence before any later
			// action. Polling can still ingest the terminal observation.
			h.writeBlocked = true
			next.Fence, next.InputState = "unknown", InputUnknown
		}
		err = h.persistLocked(next)
		h.mu.Unlock()
		if err != nil || terminal {
			return false
		}
	}
}

func (h *Host) pollProtocol() bool {
	h.mu.Lock()
	if h.ctx.Err() != nil || h.err != nil || h.protocolFailed {
		h.mu.Unlock()
		return false
	}
	epoch, offset := h.state.Epoch, h.outputOffset
	h.mu.Unlock()
	r, err := h.protocolCall(runner.Request{Op: "status", Execution: h.cfg.Execution, Epoch: epoch})
	if err != nil || r.Code != "ok" || r.Overflow || r.Offset < 0 || uint64(r.Offset) < offset || r.Offset > runner.MaxOutput {
		h.failProtocol()
		return false
	}
	var data []byte
	if offset < uint64(r.Offset) {
		r, err = h.protocolCall(runner.Request{Op: "read", Execution: h.cfg.Execution, Epoch: epoch, Offset: int(offset)})
		if err != nil || r.Code != "ok" || r.Overflow || r.Offset < 0 || r.Offset > runner.MaxOutput || uint64(r.Offset) != offset+uint64(len(r.Data)) {
			h.failProtocol()
			return false
		}
		data = r.Data
	}
	t, observeErr := h.driver.Observe(ap.Chunk{Offset: offset, Bytes: data, AtTail: true, EOF: r.Exited || r.OutputEnded})
	h.mu.Lock()
	h.outputOffset = offset + uint64(len(data))
	err = h.persistTransitionLocked(t)
	h.mu.Unlock()
	if observeErr != nil || err != nil || r.Exited || r.OutputEnded {
		h.failProtocol()
		return false
	}
	return true
}

func (h *Host) prepareProtocol() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil || h.err != nil || h.protocolFailed || h.recovering || h.state.StopRequested || h.state.SecurityStopped || h.state.Fence != "open" || h.state.InputState != InputIdle || len(h.state.Protocol.Pending) > 0 {
		return false
	}
	i := -1
	for n, r := range h.state.Records {
		if r.State == "unknown" {
			return false
		}
		if i < 0 && r.State == "queued" {
			i = n
		}
	}
	if i < 0 {
		return false
	}
	next := cloneState(h.state)
	next.Records[i].State = "unknown"
	if h.persistLocked(next) != nil {
		return false
	}
	r := next.Records[i]
	t, err := h.driver.Prepare(ap.Input{ID: r.Notice.ID, Reference: r.Notice.Reference, Mode: ap.NextTurn})
	if err == nil {
		err = h.persistTransitionLocked(t)
	}
	if err != nil {
		h.protocolFailed = true
		next = cloneState(h.state)
		next.Fence = "unknown"
		next.InputState = InputUnknown
		_ = h.persistLocked(next)
		return false
	}
	return true
}

func (h *Host) dispatchProtocol() {
	defer h.wg.Done()
	ticker := time.NewTicker(h.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.kick:
		case <-ticker.C:
		}
		if h.checkAdmission() != nil {
			continue
		}
		settled := h.reconcileOutbox()
		if !h.pollProtocol() {
			continue
		}
		if !settled {
			continue
		}
		if !h.reconcileOutbox() {
			continue
		}
		if h.prepareProtocol() {
			h.wake()
		}
	}
}
