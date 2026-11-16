package sessionhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// debugInputIntent is the typed whole intent of one private input. It carries
// no actor or time: attribution is always the authenticated grant and the host
// clock. The canonical JSON of this struct is the journaled body and the
// digest used for exact retry/conflict comparison.
type debugInputIntent struct {
	Kind             string `json:"kind"`
	ID               string `json:"id"`
	Text             string `json:"text,omitempty"`
	ExpectedRevision uint64 `json:"expected_revision"`
	ExpectedTurn     string `json:"expected_turn,omitempty"`
	Frontier         uint64 `json:"frontier"`
	Interaction      string `json:"interaction,omitempty"`
	RequestID        uint64 `json:"request_id,omitempty"`
	OptionsDigest    string `json:"options_digest,omitempty"`
	Option           string `json:"option,omitempty"`
}

type debugEffect struct {
	Kind, ID string
	Frame    []byte
}

// debugEffector is the narrow internal seam a qualified adapter (unit D) must
// implement before any native text/answer route exists. A nil effector keeps
// every new effect kind unsupported; this file ships no production adapter.
type debugEffector interface {
	debugEncode(intent debugInputIntent) ([]byte, error)
	debugIssue(ctx context.Context, effect debugEffect) (string, error)
	debugClasses() []string
}

const debugFrameLimit = 4096

func debugIntentBytes(in debugInputIntent) ([]byte, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return nil, debugStorageError()
	}
	return b, nil
}
func debugValidText(s string, limit int) bool {
	return s != "" && len(s) <= limit && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func (h *Host) debugLastTurnLocked() string {
	if n := len(h.state.Debug.Results); n != 0 {
		return h.state.Debug.Results[n-1].Turn
	}
	return ""
}

// debugFindInputLocked resolves the composite receipt key: authenticated actor
// plus client input ID. The same ID under another registered actor is a
// different key, never a conflict and never another actor's receipt.
func (h *Host) debugFindInputLocked(actor, id string) int {
	return slices.IndexFunc(h.state.Debug.Inputs, func(r debugInputRecord) bool { return r.Actor == actor && r.ID == id })
}

// debugOptionsDigest is the canonical digest of the offered answer labels in
// their original order; it binds an answer to exactly that offer.
func debugOptionsDigest(options []string) string {
	b, _ := json.Marshal(options)
	return wireDigest(b)
}

// debugInteractionEvidence is the private journal frame body of one observed
// native request: the correlation members and the offered options. It is the
// only durable copy of the labels; the snapshot keeps the digest.
type debugInteractionEvidence struct {
	Method    string    `json:"method"`
	RequestID uint64    `json:"request_id"`
	Thread    string    `json:"thread"`
	Turn      string    `json:"turn"`
	Options   []string  `json:"options"`
	ExpiresAt time.Time `json:"expires_at"`
}

// debugInput admits one typed input under the single host gate, persists the
// immutable issue marker, releases the lock for the inert effect call and then
// records the result without reopening authority. An exact retry returns the
// retained receipt and issues nothing.
func (h *Host) debugInput(token string, in debugInputIntent) (debugInputRecord, error) {
	h.mu.Lock()
	record, effect, err := h.debugAdmitInputLocked(token, in)
	h.mu.Unlock()
	if err != nil || effect == nil {
		return record, err
	}
	timeout := h.cfg.IOTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	code, issueErr := h.debugEffects.debugIssue(ctx, *effect)
	stage := "write_known"
	if issueErr != nil || code != "written" {
		stage = "effect_unknown"
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	settled, err := h.debugSettleInputLocked(record.Actor, record.ID, stage)
	if err != nil {
		return settled, err
	}
	if settled.Stage == "effect_unknown" {
		// The ambiguous result is the specified V18 for this submission; the
		// retained unknown receipt stays readable through retry and lookup.
		return settled, h.debugRefuseLocked("input_effect_unknown")
	}
	return settled, nil
}
func (h *Host) debugAdmitInputLocked(token string, in debugInputIntent) (debugInputRecord, *debugEffect, error) {
	var none debugInputRecord
	right := "text"
	switch in.Kind {
	case "follow_up", "steer", "interrupt", "queue":
	case "answer_interaction":
		right = "native_response"
	default:
		if _, err := h.debugGrantLocked(token, "view"); err != nil {
			return none, nil, err
		}
		return none, nil, h.debugRefuseLocked("input_invalid")
	}
	g, err := h.debugGrantLocked(token, "view", "writer_control", right)
	if err != nil {
		return none, nil, err
	}
	if h.debug.viewer != token {
		return none, nil, errors.New("attach_unauthorized")
	}
	if !scalar.MatchString(in.ID) {
		return none, nil, h.debugRefuseLocked("input_invalid")
	}
	// Exact historical retry/conflict precedes stop and every later gate: a
	// resolved receipt stays readable after stop, and a changed intent under the
	// same authenticated key is a conflict, never a second effect.
	body, err := debugIntentBytes(in)
	if err != nil {
		return none, nil, err
	}
	digest := wireDigest(body)
	if idx := h.debugFindInputLocked(g.actor, in.ID); idx >= 0 {
		prior := h.state.Debug.Inputs[idx]
		if prior.Digest != digest {
			return none, nil, h.debugRefuseLocked("input_conflict")
		}
		return prior, nil, nil
	}
	if h.state.StopRequested || h.state.Debug.Retired {
		return none, nil, h.debugRefuseLocked("input_stopped")
	}
	h.debugExpireLeaseLocked()
	if h.state.Debug.Writer != "claimed" {
		return none, nil, h.debugRefuseLocked("input_writer_required")
	}
	if in.ExpectedRevision != h.state.Debug.WriterRevision || (in.Kind != "answer_interaction" && in.ExpectedTurn != h.debugLastTurnLocked()) {
		return none, nil, h.debugRefuseLocked("input_observation_stale")
	}
	if in.Frontier > h.debug.tail.frontier() {
		return none, nil, h.debugRefuseLocked("input_invalid")
	}
	if h.state.Fence != "open" || h.state.SecurityStopped || h.writeBlocked || h.protocolFailed || h.recovering || h.debug.journal.torn {
		return none, nil, h.debugRefuseLocked("input_fenced")
	}
	// The single admission order (C15): no typed input, text or answer, may
	// start while a committed protocol frame is still pending in the outbox,
	// exactly as explicit resume already requires.
	if h.state.Protocol != nil && len(h.state.Protocol.Pending) != 0 {
		return none, nil, h.debugRefuseLocked("input_fenced")
	}
	for _, record := range h.state.Records {
		if record.State == "unknown" {
			return none, nil, h.debugRefuseLocked("input_fenced")
		}
	}
	for _, prior := range h.state.Debug.Inputs {
		if prior.Stage == "issued" || (prior.Stage == "effect_unknown" && !prior.Abandoned) {
			return none, nil, h.debugRefuseLocked("input_fenced")
		}
	}
	if in.Kind == "answer_interaction" {
		return h.debugAdmitAnswerLocked(g, in, body, digest)
	}
	if !debugValidText(in.Text, debugFrameLimit) || in.Interaction != "" || in.RequestID != 0 || in.OptionsDigest != "" || in.Option != "" {
		return none, nil, h.debugRefuseLocked("input_invalid")
	}
	for _, x := range h.state.Debug.Interactions {
		if x.State == "pending" || x.State == "answer_reserved" {
			return none, nil, h.debugRefuseLocked("input_fenced")
		}
	}
	if in.Kind != "follow_up" {
		return none, nil, h.debugRefuseLocked("input_operation_unsupported")
	}
	if h.state.InputState == InputBusy {
		return none, nil, h.debugRefuseLocked("input_busy")
	}
	if h.state.InputState != InputIdle {
		return none, nil, h.debugRefuseLocked("input_fenced")
	}
	if h.debugEffects == nil {
		return none, nil, h.debugRefuseLocked("input_operation_unsupported")
	}
	frame, err := h.debugEffects.debugEncode(in)
	if err != nil {
		return none, nil, h.debugRefuseLocked("input_invalid")
	}
	if len(frame) == 0 || len(frame) > debugFrameLimit {
		return none, nil, h.debugRefuseLocked("input_capacity_exceeded")
	}
	return h.debugReserveInputLocked(g, in, body, digest, frame, -1)
}
func (h *Host) debugAdmitAnswerLocked(g debugGrant, in debugInputIntent, body []byte, digest string) (debugInputRecord, *debugEffect, error) {
	var none debugInputRecord
	if in.Text != "" || !scalar.MatchString(in.Interaction) || !debugValidText(in.Option, 256) || in.RequestID == 0 || len(in.OptionsDigest) != 64 {
		return none, nil, h.debugRefuseLocked("input_invalid")
	}
	idx := slices.IndexFunc(h.state.Debug.Interactions, func(x debugInteraction) bool { return x.ID == in.Interaction })
	if idx < 0 {
		return none, nil, h.debugRefuseLocked("interaction_stale")
	}
	x := h.state.Debug.Interactions[idx]
	// Correlation and currency before anything else about the answer: the
	// request must be pending, unexpired on the host clock, in this epoch and
	// bound to exactly the observed request ID, options digest and turn.
	offered, retained := h.debug.options[x.ID]
	// A retained request is answerable only while the host positively knows the
	// current native thread and it is the thread the request was observed on.
	// Lost or unknown thread knowledge matches no retained request: missing
	// proof never authorizes a historical native answer.
	current := h.debugBindingLocked().thread
	if x.State != "pending" || !h.debug.now().Before(x.ExpiresAt) || x.Epoch != h.state.Epoch || x.RequestID != in.RequestID || x.OptionsDigest != in.OptionsDigest || x.Turn != in.ExpectedTurn || current == "" || x.Thread != current || !retained || debugOptionsDigest(offered) != x.OptionsDigest {
		return none, nil, h.debugRefuseLocked("interaction_stale")
	}
	if !slices.Contains(offered, in.Option) {
		return none, nil, h.debugRefuseLocked("input_invalid")
	}
	if h.debugEffects == nil || !slices.Contains(h.debugEffects.debugClasses(), x.Method) {
		return none, nil, h.debugRefuseLocked("interaction_unsupported")
	}
	if x.OwnerOnly {
		return none, nil, h.debugRefuseLocked("interaction_owner_required")
	}
	frame, err := h.debugEffects.debugEncode(in)
	if err != nil {
		return none, nil, h.debugRefuseLocked("input_invalid")
	}
	if len(frame) == 0 || len(frame) > debugFrameLimit {
		return none, nil, h.debugRefuseLocked("input_capacity_exceeded")
	}
	return h.debugReserveInputLocked(g, in, body, digest, frame, idx)
}

// debugReserveInputLocked journals the whole attributed intent, then commits
// the issue marker (and answer CAS) before any effect. A journal failure means
// the input was never accepted; a snapshot failure leaves an orphan frame that
// replacement reconciles as an unknown effect, never as a replay.
func (h *Host) debugReserveInputLocked(g debugGrant, in debugInputIntent, body []byte, digest string, frame []byte, interaction int) (debugInputRecord, *debugEffect, error) {
	var none debugInputRecord
	if len(h.state.Debug.Inputs) >= MaxRecords {
		return none, nil, h.debugRefuseLocked("input_capacity_exceeded")
	}
	if err := h.debugAppendLocked(g, in.Kind, in.ID, body, false); err != nil {
		return none, nil, err
	}
	next := cloneState(h.state)
	record := debugInputRecord{ID: in.ID, Actor: g.actor, Role: g.role, Kind: in.Kind, Sequence: h.debug.journal.sequence, Digest: digest, Stage: "issued", Epoch: h.state.Epoch, Turn: in.ExpectedTurn, IssuedAt: h.debug.now().UTC()}
	if interaction >= 0 {
		x := &next.Debug.Interactions[interaction]
		x.State = "answer_reserved"
		x.Revision++
		x.Input = in.ID
		x.InputActor = g.actor
		record.Interaction = x.ID
	}
	next.Debug.Inputs = append(next.Debug.Inputs, record)
	if err := h.persistLocked(next); err != nil {
		return none, nil, err
	}
	return record, &debugEffect{Kind: in.Kind, ID: in.ID, Frame: frame}, nil
}

// debugSettleInputLocked records the issued effect's result and consumes the
// receipt reservation. A late result after revocation or stop is still
// attributed; it revives no authority and is never reissued.
func (h *Host) debugSettleInputLocked(actor, id, stage string) (debugInputRecord, error) {
	if h.debug == nil {
		return debugInputRecord{}, errors.New("input_effect_unknown")
	}
	idx := h.debugFindInputLocked(actor, id)
	if idx < 0 {
		return debugInputRecord{}, errors.New("input_effect_unknown")
	}
	if h.state.Debug.Inputs[idx].Stage != "issued" {
		return h.state.Debug.Inputs[idx], nil
	}
	next := cloneState(h.state)
	record := &next.Debug.Inputs[idx]
	record.Stage = stage
	if record.Interaction != "" && stage == "effect_unknown" {
		for i := range next.Debug.Interactions {
			if next.Debug.Interactions[i].ID == record.Interaction {
				next.Debug.Interactions[i].State = "effect_unknown"
				next.Debug.Interactions[i].Revision++
			}
		}
	}
	receiptErr := h.debug.journal.receipt(debugJournalEntry{Kind: "receipt", ID: id, Digest: record.Digest, Stage: stage, Ref: record.Sequence}, h.debug.now())
	if receiptErr != nil {
		// Lost receipt evidence closes delivery; it does not change the effect.
		next.Fence = "unknown"
		next.InputState = InputUnknown
	}
	if err := h.persistLocked(next); err != nil {
		return *record, err
	}
	if receiptErr != nil {
		return *record, receiptErr
	}
	return *record, nil
}

// debugReceipt is the authorized receipt lookup under view authority for the
// caller's own key only. It remains available after stop and never issues or
// repeats an effect; another actor's receipt is not disclosed.
func (h *Host) debugReceipt(token, id string) (debugInputRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	g, err := h.debugGrantLocked(token, "view")
	if err != nil {
		return debugInputRecord{}, err
	}
	if h.debug.viewer != token {
		return debugInputRecord{}, errors.New("attach_unauthorized")
	}
	if idx := h.debugFindInputLocked(g.actor, id); idx >= 0 {
		return h.state.Debug.Inputs[idx], nil
	}
	return debugInputRecord{}, h.debugRefuseLocked("input_invalid")
}

// debugInputTerminal records the qualified completion of one issued manager
// input as its own terminal fact. It is the seam a qualified adapter supplies;
// fixtures call it directly. It dispositions nothing.
func (h *Host) debugInputTerminal(actor, id, thread, turn, outcome string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.state.DebugAttach || h.debug == nil {
		return errors.New("attach_not_enabled")
	}
	idx := h.debugFindInputLocked(actor, id)
	if idx < 0 || !scalar.MatchString(thread) || !scalar.MatchString(turn) {
		return errors.New("input_invalid")
	}
	switch h.state.Debug.Inputs[idx].Stage {
	case "write_known", "native_accepted":
	default:
		return errors.New("input_effect_unknown")
	}
	switch outcome {
	case "completed", "failed", "interrupted":
	default:
		return errors.New("input_invalid")
	}
	next := cloneState(h.state)
	next.Debug.Inputs[idx].Stage = "turn_terminal"
	next.Debug.Inputs[idx].Turn = turn
	prior := len(h.state.Debug.Results)
	if err := h.debugTerminalLocked(&next, id, thread, turn, outcome); err != nil {
		return err
	}
	if err := h.persistLocked(next); err != nil {
		return err
	}
	h.debugAnnounceLocked(prior)
	return nil
}

// debugObserveInteraction retains one native request as a pending interaction:
// correlation members and host expiry in the snapshot, the offered options in
// the private journal frame and bounded memory. The question text stays only
// in the volatile tail. This is the evidence seam a qualified adapter feeds.
func (h *Host) debugObserveInteraction(thread, turn string, requestID uint64, method string, options []string, ownerOnly bool) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.state.DebugAttach || h.debug == nil {
		return "", errors.New("attach_not_enabled")
	}
	if !scalar.MatchString(thread) || !scalar.MatchString(turn) || requestID == 0 || !scalar.MatchString(method) || len(options) == 0 || len(options) > debugMaxOptions {
		return "", errors.New("input_invalid")
	}
	seen := map[string]bool{}
	for _, option := range options {
		if !debugValidText(option, debugMaxOptionBytes) || seen[option] {
			return "", errors.New("input_invalid")
		}
		seen[option] = true
	}
	if len(h.state.Debug.Interactions) >= debugMaxInteractions {
		return "", h.debugRefuseLocked("input_capacity_exceeded")
	}
	now := h.debug.now().UTC()
	expires := now.Add(debugInteractionLifetime)
	evidence, err := json.Marshal(debugInteractionEvidence{Method: method, RequestID: requestID, Thread: thread, Turn: turn, Options: slices.Clone(options), ExpiresAt: expires})
	if err != nil {
		return "", debugStorageError()
	}
	id := fmt.Sprintf("interaction-%02x", len(h.state.Debug.Interactions)+1)
	// Required interaction evidence that cannot fit refuses and leaves the
	// request unanswered; it is never truncated or answered first.
	if err := h.debugJournalLocked("interaction_observed", id, evidence, false); err != nil {
		return "", err
	}
	next := cloneState(h.state)
	next.Debug.Interactions = append(next.Debug.Interactions, debugInteraction{ID: id, Method: method, RequestID: requestID, OptionsDigest: debugOptionsDigest(options), Thread: thread, Turn: turn, Epoch: h.state.Epoch, Revision: 1, State: "pending", OwnerOnly: ownerOnly, ObservedAt: now, ExpiresAt: expires})
	if err := h.persistLocked(next); err != nil {
		return "", err
	}
	h.debug.options[id] = slices.Clone(options)
	return id, nil
}

// debugInteractionResolved records separately qualified native evidence that
// the reserved answer was applied. Absence of evidence never resolves.
func (h *Host) debugInteractionResolved(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.state.DebugAttach || h.debug == nil {
		return errors.New("attach_not_enabled")
	}
	idx := slices.IndexFunc(h.state.Debug.Interactions, func(x debugInteraction) bool { return x.ID == id })
	if idx < 0 || h.state.Debug.Interactions[idx].State != "answer_reserved" {
		return errors.New("interaction_stale")
	}
	next := cloneState(h.state)
	x := &next.Debug.Interactions[idx]
	input := h.debugFindInputLocked(x.InputActor, x.Input)
	if input < 0 || next.Debug.Inputs[input].Stage != "write_known" {
		return errors.New("input_effect_unknown")
	}
	next.Debug.Inputs[input].Stage = "native_accepted"
	x.State = "resolved"
	x.Revision++
	if err := h.persistLocked(next); err != nil {
		return err
	}
	delete(h.debug.options, x.ID)
	return nil
}

// debugDeliverGrant hands one issued grant to the intended registered
// recipient over a private pipe/socket descriptor the caller owns, then closes
// the host's end. Grants never travel through argv, environment or records.
// Any delivery failure rolls the issued capability back: the token is revoked
// and can never become usable on a later attachment; unrelated grants and
// viewers are untouched.
func (h *Host) debugDeliverGrant(token string, to *os.File) error {
	h.mu.Lock()
	ok := false
	if h.debug != nil {
		_, ok = h.debug.grants[token]
	}
	h.mu.Unlock()
	if !ok {
		// Unknown token: a plain refusal; nothing exists to roll back.
		if to != nil {
			to.Close()
		}
		return errors.New("attach_grant_revoked")
	}
	if to == nil {
		// No destination at all is still a failed delivery of an issued grant.
		h.debugAbandonGrant(token)
		return errors.New("input_invalid")
	}
	defer to.Close()
	info, err := to.Stat()
	if err != nil || info.Mode()&(os.ModeNamedPipe|os.ModeSocket) == 0 {
		h.debugAbandonGrant(token)
		return errors.New("input_invalid")
	}
	if _, err := to.Write([]byte(token + "\n")); err != nil {
		h.debugAbandonGrant(token)
		return errors.New("attach_binding_unavailable")
	}
	return nil
}

// debugAbandonGrant revokes a token whose delivery failed. The revocation is
// journaled in protected capacity when storage allows; the grant is removed
// regardless, and a view or writer it held is released into held.
func (h *Host) debugAbandonGrant(token string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.debug == nil {
		return
	}
	g, ok := h.debug.grants[token]
	if !ok {
		return
	}
	_ = h.debugIntentLocked(debugGrant{actor: "host", role: "host", revision: g.revision}, debugControlIntent{Operation: "revoke", Target: g.id, ExpectedRevision: g.revision, Decision: "delivery_failed"}, true)
	delete(h.debug.grants, token)
	if h.debug.viewer == token {
		h.debug.viewer = ""
		_ = h.debugHoldLocked()
	}
}
