package sessionhost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/relux-works/swarma-session-host/internal/runner"
)

// These types are deliberately private: no caller-facing activation, launcher
// schema, socket, capability advertisement or native input implementation.
// Every retained member is content-free metadata; bodies live only in the
// private journal and the volatile tail.
// debugResult members other than outcome, ack/permission/debt flags and the
// relative age are private correlation: the public projection omits them
// (omitempty/omitzero; they are never zero in private custody).
type debugResult struct {
	ID                  string    `json:"id,omitempty"`
	Thread              string    `json:"thread,omitempty"`
	Turn                string    `json:"turn,omitempty"`
	Outcome             string    `json:"outcome"`
	Input               string    `json:"input,omitempty"`
	ObservedAt          time.Time `json:"observed_at,omitzero"`
	Revision            uint64    `json:"revision,omitempty"`
	Ack                 bool      `json:"ack"`
	Permitted           bool      `json:"permitted"`
	NotificationPending bool      `json:"notification_pending"`
	AgeSeconds          int64     `json:"age_seconds,omitempty"`
}

// debugInputRecord is the snapshot/outbox reference to one immutable private
// journal frame, keyed by authenticated actor plus input ID. It never carries
// the body; Sequence names the journal entry. The reconciliation members are
// omitempty so the public projection can drop them while private custody keeps
// them (they are never zero there).
type debugInputRecord struct {
	ID          string    `json:"id,omitempty"`
	Actor       string    `json:"actor,omitempty"`
	Role        string    `json:"role,omitempty"`
	Kind        string    `json:"kind"`
	Sequence    uint64    `json:"sequence,omitempty"`
	Digest      string    `json:"digest,omitempty"`
	Stage       string    `json:"stage"`
	Epoch       uint64    `json:"epoch"`
	Interaction string    `json:"interaction,omitempty"`
	Turn        string    `json:"turn,omitempty"`
	Abandoned   bool      `json:"abandoned,omitempty"`
	IssuedAt    time.Time `json:"issued_at,omitzero"`
	AgeSeconds  int64     `json:"age_seconds,omitempty"`
}

// debugInteraction retains native request correlation without the question
// body or options text; the options digest binds an answer, the offered option
// labels stay in the private journal frame and bounded host memory, and
// ExpiresAt is the authoritative host-clock expiry of the pending request.
type debugInteraction struct {
	ID            string    `json:"id,omitempty"`
	Method        string    `json:"method"`
	RequestID     uint64    `json:"request_id,omitempty"`
	OptionsDigest string    `json:"options_digest,omitempty"`
	Thread        string    `json:"thread,omitempty"`
	Turn          string    `json:"turn,omitempty"`
	Epoch         uint64    `json:"epoch"`
	Revision      uint64    `json:"revision,omitempty"`
	State         string    `json:"state"`
	OwnerOnly     bool      `json:"owner_only,omitempty"`
	Input         string    `json:"input,omitempty"`
	InputActor    string    `json:"input_actor,omitempty"`
	ObservedAt    time.Time `json:"observed_at,omitzero"`
	ExpiresAt     time.Time `json:"expires_at,omitzero"`
	AgeSeconds    int64     `json:"age_seconds,omitempty"`
}

// debugAge is the content-free relative age used by the diagnostic projection.
func debugAge(now, at time.Time) int64 {
	if at.IsZero() || now.Before(at) {
		return 0
	}
	return int64(now.Sub(at).Seconds())
}

// debugPublicState is the allowlisted ordinary status projection: writer
// state and, per terminal fact, input and interaction, only its fixed
// class/kind/stage/state, ack/permission/debt flags and relative age. Result
// IDs/revisions, native thread/turn correlation, absolute times, input IDs,
// actors, journal sequences, payload-derived digests, interaction handles,
// native request IDs and options digests stay in private custody and the
// grant-bound control view.
func debugPublicState(s State, now time.Time) State {
	if s.Debug == nil {
		return s
	}
	d := *s.Debug
	d.Results = make([]debugResult, len(s.Debug.Results))
	for i, r := range s.Debug.Results {
		d.Results[i] = debugResult{Outcome: r.Outcome, Ack: r.Ack, Permitted: r.Permitted, NotificationPending: r.NotificationPending, AgeSeconds: debugAge(now, r.ObservedAt)}
	}
	d.Inputs = make([]debugInputRecord, len(s.Debug.Inputs))
	for i, in := range s.Debug.Inputs {
		d.Inputs[i] = debugInputRecord{Kind: in.Kind, Stage: in.Stage, Epoch: in.Epoch, Abandoned: in.Abandoned, AgeSeconds: debugAge(now, in.IssuedAt)}
	}
	d.Interactions = make([]debugInteraction, len(s.Debug.Interactions))
	for i, x := range s.Debug.Interactions {
		d.Interactions[i] = debugInteraction{Method: x.Method, Epoch: x.Epoch, State: x.State, OwnerOnly: x.OwnerOnly, AgeSeconds: debugAge(now, x.ObservedAt)}
	}
	s.Debug = &d
	return s
}

// debugControlView is the private manager/owner control snapshot behind a
// current view grant: complete terminal facts and interactions for exact
// disposition/answer correlation, and the caller's own input receipts. It is a
// separate custody route from ordinary status and discloses no other actor's
// receipts.
func (h *Host) debugControlView(token string) (debugMetadata, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	g, err := h.debugGrantLocked(token, "view")
	if err != nil {
		return debugMetadata{}, err
	}
	if h.debug.viewer != token {
		return debugMetadata{}, errors.New("attach_unauthorized")
	}
	view := *cloneState(h.state).Debug
	own := view.Inputs[:0]
	for _, in := range view.Inputs {
		if in.Actor == g.actor {
			own = append(own, in)
		}
	}
	view.Inputs = own
	return view, nil
}

type debugMetadata struct {
	Writer         string             `json:"writer"`
	WriterRevision uint64             `json:"writer_revision"`
	Results        []debugResult      `json:"results,omitempty"`
	Inputs         []debugInputRecord `json:"inputs,omitempty"`
	Interactions   []debugInteraction `json:"interactions,omitempty"`
	Retired        bool               `json:"retired,omitempty"`
}
type debugAuthority struct {
	actor, role string
	epoch       uint64
}

// debugBinding is admitted fixture evidence of the current execution/runner/
// host/native-thread incarnation. It is checked, never learned from a caller.
type debugBinding struct {
	execution, runner, thread string
	epoch                     uint64
}

// debugRegistry names the recorded accountable manager and owner. Same UID,
// socket knowledge or a caller label never substitutes for this registration.
type debugRegistry struct{ manager, owner string }

// debugGrant retains the complete binding and registration it was issued
// under; every use revalidates both against the current host.
type debugGrant struct {
	actor, role     string
	rights          []string
	revision, epoch uint64
	expires         time.Time
	id              string
	binding         debugBinding
	registry        debugRegistry
}
type debugAttachment struct {
	tail       *debugTail
	journal    *debugJournal
	grants     map[string]debugGrant
	viewer     string
	leaseUntil time.Time
	now        func() time.Time
	rejections map[string]int
	// options is the bounded in-memory copy of each pending interaction's
	// offered answers, mirrored from its private journal frame. Replacement
	// invalidates pending interactions, so nothing is restored here.
	options map[string][]string
}

// debugNotification is the content-free terminal_observed debt handed to the
// notifier seam. Delivery, acknowledgement and disposition stay separate facts.
type debugNotification struct {
	Kind       string    `json:"kind"`
	Execution  string    `json:"execution"`
	Epoch      uint64    `json:"epoch"`
	Result     string    `json:"result"`
	Revision   uint64    `json:"revision"`
	Outcome    string    `json:"outcome"`
	ObservedAt time.Time `json:"observed_at"`
}

const debugMaxInteractions = 16
const debugMaxOptions = 16
const debugMaxOptionBytes = 256
const debugLease = 60 * time.Second
const debugGrantLifetime = 5 * time.Minute

// A pending native request expires with the volatile tail that holds its
// question; afterwards only the content-free expired interaction remains.
const debugInteractionLifetime = debugTailAge

func debugInputKey(actor, id string) string { return actor + "\x00" + id }

var debugRights = []string{"view", "writer_control", "text", "native_response", "stop"}

func admitDebug(s *State, exists bool, requested *bool) error {
	if exists {
		if requested != nil && *requested != s.DebugAttach {
			return errors.New("attach_admission_conflict")
		}
		if s.DebugAttach && s.Debug == nil {
			return errors.New("input_fenced")
		}
		return nil
	}
	if requested != nil && *requested {
		s.DebugAttach = true
		s.Debug = &debugMetadata{Writer: "automatic"}
	}
	return nil
}
func debugInputKind(k string) bool { return k == "follow_up" || k == "answer_interaction" }
func debugStage(s string) bool {
	switch s {
	case "issued", "write_known", "effect_unknown", "native_accepted", "turn_terminal", "invalidated":
		return true
	}
	return false
}
func debugInteractionState(s string) bool {
	switch s {
	case "pending", "answer_reserved", "effect_unknown", "resolved", "expired", "invalidated", "unsupported":
		return true
	}
	return false
}
func validDebugState(s State) bool {
	if !s.DebugAttach {
		return s.Debug == nil
	}
	d := s.Debug
	if d == nil || len(d.Results) > MaxRecords || len(d.Inputs) > MaxRecords || len(d.Interactions) > debugMaxInteractions {
		return false
	}
	switch d.Writer {
	case "automatic", "claimed", "held", "stopped":
	default:
		return false
	}
	seen := map[string]bool{}
	for _, r := range d.Results {
		if !scalar.MatchString(r.ID) || seen[r.ID] || r.Revision == 0 || r.Thread == "" || r.Turn == "" || (!r.NotificationPending && !r.Ack) {
			return false
		}
		switch r.Outcome {
		case "completed", "failed", "interrupted":
		default:
			return false
		}
		seen[r.ID] = true
	}
	inputs := map[string]bool{}
	for _, in := range d.Inputs {
		key := debugInputKey(in.Actor, in.ID)
		if !scalar.MatchString(in.ID) || inputs[key] || in.Sequence == 0 || len(in.Digest) != 64 || !debugInputKind(in.Kind) || !debugStage(in.Stage) || !scalar.MatchString(in.Actor) {
			return false
		}
		inputs[key] = true
	}
	interactions := map[string]bool{}
	for _, x := range d.Interactions {
		if !scalar.MatchString(x.ID) || interactions[x.ID] || x.RequestID == 0 || x.Revision == 0 || !debugInteractionState(x.State) || !scalar.MatchString(x.Method) || len(x.OptionsDigest) != 64 || x.ExpiresAt.IsZero() {
			return false
		}
		interactions[x.ID] = true
	}
	return true
}
func (h *Host) openDebug(replacement bool) error {
	if h.state.Debug.Retired {
		return errors.New("input_stopped")
	}
	j, err := openDebugJournal(h.store.dir, replacement)
	if err != nil {
		return err
	}
	d := &debugAttachment{journal: j, grants: make(map[string]debugGrant), now: time.Now, rejections: make(map[string]int), options: make(map[string][]string)}
	d.tail = &debugTail{epoch: h.state.Epoch, execution: h.state.Execution, now: d.now, changed: make(chan struct{}, 1)}
	if replacement {
		d.tail.cause = "host_replaced"
		h.state.Debug.Writer = "held"
		h.state.Debug.WriterRevision++
	}
	h.debug = d
	h.debugReconcileLocked(replacement)
	observer, ok := h.driver.(interface{ SetDebugObserver(func([]byte)) })
	if h.driver != nil && !ok {
		d.close()
		return errors.New("attach_capability_unsupported")
	}
	if ok {
		observer.SetDebugObserver(d.tail.observe)
	}
	if err := h.persistLocked(h.state); err != nil {
		d.close()
		return err
	}
	// Restart emits each outstanding debt once with its original observation age.
	for _, r := range h.state.Debug.Results {
		if r.NotificationPending {
			h.debugNotifyLocked(r)
		}
	}
	return nil
}

// debugReconcileLocked conservatively settles the two-record transaction after
// a replacement: an issued effect without a receipt stays unknown, an accepted
// but never issued intent is invalidated, and a journal frame with no snapshot
// reference is retained as an unknown effect. Nothing is replayed.
func (h *Host) debugReconcileLocked(replacement bool) {
	d := h.state.Debug
	known := map[string]bool{}
	for i := range d.Inputs {
		in := &d.Inputs[i]
		known[debugInputKey(in.Actor, in.ID)] = true
		if !replacement {
			continue
		}
		if in.Stage == "issued" {
			in.Stage = "effect_unknown"
		}
	}
	for _, orphan := range h.debug.journal.orphans {
		key := debugInputKey(orphan.Actor, orphan.ID)
		if known[key] || len(d.Inputs) >= MaxRecords || !scalar.MatchString(orphan.ID) || !scalar.MatchString(orphan.Actor) {
			h.debug.journal.torn = true
			continue
		}
		known[key] = true
		d.Inputs = append(d.Inputs, debugInputRecord{ID: orphan.ID, Actor: orphan.Actor, Role: orphan.Role, Kind: orphan.Kind, Sequence: orphan.Sequence, Digest: orphan.Digest, Stage: "effect_unknown", Epoch: orphan.Epoch, IssuedAt: orphan.At})
	}
	h.debug.journal.orphans = nil
	if !replacement {
		return
	}
	for i := range d.Interactions {
		x := &d.Interactions[i]
		switch x.State {
		case "pending":
			x.State = "invalidated"
			x.Revision++
		case "answer_reserved":
			for _, in := range d.Inputs {
				if in.ID == x.Input && in.Actor == x.InputActor && in.Stage == "effect_unknown" {
					x.State = "effect_unknown"
					x.Revision++
				}
			}
		}
	}
}
func (h *Host) debugExpiryTimerLoop() {
	defer h.wg.Done()
	timer := time.NewTimer(h.debug.tail.expiryDelay())
	defer timer.Stop()
	leaseTicker := time.NewTicker(time.Second)
	defer leaseTicker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.debug.tail.changed:
		case <-timer.C:
			h.debug.tail.expire()
		case <-leaseTicker.C:
			h.mu.Lock()
			h.debugExpireLeaseLocked()
			h.mu.Unlock()
		}
		timer.Reset(h.debug.tail.expiryDelay())
	}
}

// close releases the volatile tail, grants and journal handle. The journal's
// own close error is returned, never discarded, for callers that must report it.
func (d *debugAttachment) close() error {
	d.tail.close()
	clear(d.grants)
	d.viewer = ""
	return d.journal.close()
}
func (h *Host) debugResume(requested bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return admitDebug(&h.state, true, &requested)
}

// debugRefuseLocked returns the exact private class and keeps a bounded,
// content-free rejection count for the diagnostic summary. Nothing else is
// retained about a rejected attempt.
func (h *Host) debugRefuseLocked(code string) error {
	if h.debug != nil && h.debug.rejections != nil && (len(h.debug.rejections) < 32 || h.debug.rejections[code] != 0) && h.debug.rejections[code] < 1<<30 {
		h.debug.rejections[code]++
	}
	return errors.New(code)
}

// debugAutomaticLocked is the automatic-delivery conjunct: writer automatic,
// no blocking terminal fact, no outstanding private effect, native interaction
// or recovery hold. Disabled executions are unaffected.
func (h *Host) debugAutomaticLocked() bool {
	if !h.state.DebugAttach {
		return true
	}
	if h.debug == nil || h.state.Debug == nil || h.state.Debug.Retired {
		return false
	}
	h.debugExpireLeaseLocked()
	if h.state.Debug.Writer != "automatic" || h.debug.journal.torn {
		return false
	}
	for _, r := range h.state.Debug.Results {
		if !r.Permitted {
			return false
		}
	}
	return !h.debugOutstandingLocked(h.state)
}
func (h *Host) debugOutstandingLocked(s State) bool {
	for _, in := range s.Debug.Inputs {
		if in.Stage == "issued" || (in.Stage == "effect_unknown" && !in.Abandoned) {
			return true
		}
	}
	for _, x := range s.Debug.Interactions {
		if x.State == "pending" || x.State == "answer_reserved" {
			return true
		}
	}
	return false
}

// debugGrantCurrentLocked is the complete authority predicate for one grant:
// unexpired, and issued under exactly the current host epoch, execution,
// runner incarnation, native thread and manager/owner registration.
func (h *Host) debugGrantCurrentLocked(g debugGrant) bool {
	return g.epoch == h.state.Epoch && debugBindingCurrent(g.binding, h.debugBindingLocked()) && g.registry == h.debugRegistry && h.debug.now().Before(g.expires)
}

// debugBindingCurrent holds when the issued (or pinned) binding names exactly
// this execution, runner incarnation, epoch and native thread. There is no
// wildcard: a grant issued before any thread existed is pinned by the reaper to
// the first thread that appears, and any different or lost thread afterwards is
// another incarnation.
func debugBindingCurrent(issued, current debugBinding) bool {
	return issued == current
}

// debugExpireLeaseLocked first performs the finite bootstrap transition for
// grants issued while no native thread existed (pin to the first thread, once),
// then reaps every grant that is no longer current, holds a writer whose viewer
// or lease was lost, and records the expiry of pending native requests. It runs
// on every authority check and on the timer.
func (h *Host) debugExpireLeaseLocked() {
	if h.debug == nil {
		return
	}
	current := h.debugBindingLocked()
	lostViewer := false
	for token, g := range h.debug.grants {
		if g.binding.thread == "" && current.thread != "" && g.binding.execution == current.execution && g.binding.runner == current.runner && g.binding.epoch == current.epoch {
			g.binding.thread = current.thread
			h.debug.grants[token] = g
		}
		if !h.debugGrantCurrentLocked(g) {
			delete(h.debug.grants, token)
			if h.debug.viewer == token {
				h.debug.viewer = ""
				lostViewer = true
			}
		}
	}
	h.debugExpireInteractionsLocked()
	if h.state.Debug.Writer != "claimed" || (!lostViewer && h.debug.now().Before(h.debug.leaseUntil)) {
		return
	}
	next := cloneState(h.state)
	next.Debug.Writer = "held"
	next.Debug.WriterRevision++
	if h.persistLocked(next) != nil {
		h.state.Fence = "unknown"
	}
}
func (h *Host) debugExpireInteractionsLocked() {
	now := h.debug.now()
	var next State
	changed := false
	for i, x := range h.state.Debug.Interactions {
		if x.State != "pending" || now.Before(x.ExpiresAt) {
			continue
		}
		if !changed {
			next = cloneState(h.state)
			changed = true
		}
		next.Debug.Interactions[i].State = "expired"
		next.Debug.Interactions[i].Revision++
		delete(h.debug.options, x.ID)
	}
	if changed && h.persistLocked(next) != nil {
		h.state.Fence = "unknown"
	}
}

// debugRegister records the accountable manager and owner through the private
// composition seam. It is not an attachment right and issues no grant.
func (h *Host) debugRegister(r debugRegistry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.state.DebugAttach {
		return errors.New("attach_not_enabled")
	}
	if !scalar.MatchString(r.manager) || !scalar.MatchString(r.owner) || r.manager == r.owner {
		return errors.New("input_invalid")
	}
	h.debugRegistry = r
	// A changed registration invalidates every grant issued under the old one
	// and holds a writer it owned; nothing issued earlier survives the change.
	h.debugExpireLeaseLocked()
	return nil
}
func (h *Host) debugRegisteredLocked(a debugAuthority) bool {
	r := h.debugRegistry
	return scalar.MatchString(a.actor) && ((a.role == "manager" && a.actor == r.manager) || (a.role == "owner" && a.actor == r.owner))
}
func (h *Host) debugBindingLocked() debugBinding {
	b := debugBinding{execution: h.state.Execution, runner: h.state.RunnerInstance, epoch: h.state.Epoch}
	if h.state.Protocol != nil {
		b.thread = h.state.Protocol.View.NativeSession
	}
	return b
}

// debugIssueGrant is self-issuance against the host's current binding at the
// caller's claimed epoch; debugIssueGrantTo carries explicit delegation and
// externally supplied binding evidence.
func (h *Host) debugIssueGrant(a debugAuthority, rights []string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	b := h.debugBindingLocked()
	b.epoch = a.epoch
	return h.debugIssueGrantLocked(a, a, b, rights)
}
func (h *Host) debugIssueGrantTo(issuer, recipient debugAuthority, b debugBinding, rights []string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.debugIssueGrantLocked(issuer, recipient, b, rights)
}
func (h *Host) debugIssueGrantLocked(issuer, recipient debugAuthority, b debugBinding, rights []string) (string, error) {
	if !h.state.DebugAttach || h.debug == nil {
		return "", errors.New("attach_not_enabled")
	}
	if h.ctx.Err() != nil {
		return "", errors.New("attach_binding_unavailable")
	}
	if h.state.Debug.Retired {
		return "", errors.New("input_stopped")
	}
	// Authenticity before delegation before binding: an unregistered caller
	// learns nothing about delegation or the current incarnation.
	if !h.debugRegisteredLocked(issuer) || !h.debugRegisteredLocked(recipient) || issuer.epoch != recipient.epoch {
		return "", errors.New("attach_unauthorized")
	}
	if issuer.role != "owner" && issuer != recipient {
		return "", h.debugRefuseLocked("input_right_required")
	}
	current := h.debugBindingLocked()
	// A present member that names a different execution, runner incarnation,
	// epoch or native thread is a positive conflict (V07) and is reported before
	// completeness. Otherwise evidence must be complete for what the host knows:
	// a missing member, or a thread the host cannot confirm because it knows no
	// native thread yet, is unavailable (V06).
	if (b.execution != "" && b.execution != current.execution) || (b.runner != "" && b.runner != current.runner) || (b.epoch != 0 && b.epoch != current.epoch) || issuer.epoch != current.epoch || (b.thread != "" && current.thread != "" && b.thread != current.thread) {
		return "", errors.New("attach_binding_conflict")
	}
	if b.execution == "" || b.runner == "" || b.epoch == 0 || (current.thread != "" && b.thread == "") || (current.thread == "" && b.thread != "") {
		return "", errors.New("attach_binding_unavailable")
	}
	h.debugExpireLeaseLocked()
	if len(rights) == 0 || len(rights) > len(debugRights) || len(h.debug.grants) >= 16 {
		return "", errors.New("input_invalid")
	}
	seen := map[string]bool{}
	for _, r := range rights {
		if !slices.Contains(debugRights, r) || seen[r] {
			return "", errors.New("input_invalid")
		}
		seen[r] = true
	}
	if h.debug.journal.revokes+len(h.debug.grants) >= 16 {
		return "", errors.New("input_capacity_exceeded")
	}
	var identifier [16]byte
	if _, err := rand.Read(identifier[:]); err != nil {
		return "", errors.New("attach_unauthorized")
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", errors.New("attach_unauthorized")
	}
	token := hex.EncodeToString(secret[:])
	h.debug.grants[token] = debugGrant{actor: recipient.actor, role: recipient.role, rights: slices.Clone(rights), revision: 1, epoch: b.epoch, expires: h.debug.now().Add(debugGrantLifetime), id: "grant-" + hex.EncodeToString(identifier[:]), binding: b, registry: h.debugRegistry}
	return token, nil
}
func (h *Host) debugGrantLocked(token string, rights ...string) (debugGrant, error) {
	if !h.state.DebugAttach || h.debug == nil {
		return debugGrant{}, errors.New("attach_not_enabled")
	}
	if h.ctx.Err() != nil {
		return debugGrant{}, errors.New("attach_grant_revoked")
	}
	h.debugExpireLeaseLocked()
	g, ok := h.debug.grants[token]
	if !ok || !h.debugGrantCurrentLocked(g) || !slices.Contains(g.rights, "view") {
		if token == h.debug.viewer {
			h.debug.viewer = ""
			h.debugHoldLocked()
		}
		return g, errors.New("attach_grant_revoked")
	}
	for _, r := range rights {
		if !slices.Contains(g.rights, r) {
			return g, h.debugRefuseLocked("input_right_required")
		}
	}
	return g, nil
}
func (h *Host) debugAttachView(token string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.debugGrantLocked(token, "view"); err != nil {
		if err.Error() == "attach_grant_revoked" {
			return errors.New("attach_unauthorized")
		}
		return err
	}
	if h.state.Debug.Retired {
		return errors.New("input_stopped")
	}
	// C08 specifies V08 for a second attachment, even on the private seam.
	if h.debug.viewer != "" {
		return errors.New("attach_in_use")
	}
	h.debug.viewer = token
	return nil
}

func (h *Host) debugRevoke(a debugAuthority, token string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.state.DebugAttach || h.debug == nil {
		return errors.New("attach_not_enabled")
	}
	g, exists := h.debug.grants[token]
	if a.epoch != h.state.Epoch || !h.debugRegisteredLocked(a) ||
		(a.role != "owner" && !(a.role == "manager" && exists && g.actor == a.actor && g.role == "manager")) {
		return h.debugRefuseLocked("input_right_required")
	}
	if !exists {
		return errors.New("attach_grant_revoked")
	}
	intent := debugControlIntent{Operation: "revoke", Target: g.id, ExpectedRevision: g.revision, Decision: "revoke"}
	if err := h.debugIntentLocked(debugGrant{actor: a.actor, role: a.role, revision: g.revision}, intent, true); err != nil {
		return err
	}
	if h.debug.viewer == token && h.state.Debug.Writer == "claimed" {
		next := cloneState(h.state)
		next.Debug.Writer = "held"
		next.Debug.WriterRevision++
		if err := h.persistLocked(next); err != nil {
			return err
		}
	}
	delete(h.debug.grants, token)
	if h.debug.viewer == token {
		h.debug.viewer = ""
	}
	return nil
}
func (h *Host) debugRead(token string, after uint64) (debugTailRead, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.debugGrantLocked(token, "view"); err != nil {
		return debugTailRead{}, err
	}
	if h.debug.viewer != token {
		return debugTailRead{}, errors.New("attach_unauthorized")
	}
	read := h.debug.tail.read(after)
	read.Status = "live"
	if h.protocolFailed || h.state.Fence == "unknown" {
		read.Status = "stale"
	}
	if h.debug.tail.closed {
		read.Status = "closed"
	}
	return read, nil
}
func (h *Host) debugHoldLocked() error {
	if h.state.Debug.Writer != "claimed" {
		return nil
	}
	next := cloneState(h.state)
	next.Debug.Writer = "held"
	next.Debug.WriterRevision++
	if err := h.persistLocked(next); err != nil {
		// Cleanup cannot retain an active in-memory lease on a broken store.
		// Replacement already begins held; retain the unknown durable fence.
		h.state.Debug.Writer = next.Debug.Writer
		h.state.Debug.WriterRevision = next.Debug.WriterRevision
		h.state.Fence = "unknown"
		return err
	}
	return nil
}

// debugResumeEligibleLocked is the complete explicit-resume predicate: known
// idle/open state, no security/recovery/transport fence, no pending outbox,
// unknown notice, outstanding private effect or native interaction, and every
// terminal fact explicitly permitting automation.
func (h *Host) debugResumeEligibleLocked(next State) error {
	if next.InputState != InputIdle || next.Fence != "open" || next.SecurityStopped || h.recovering || h.protocolFailed || h.writeBlocked || h.debug.journal.torn {
		return h.debugRefuseLocked("input_fenced")
	}
	if next.Protocol != nil && len(next.Protocol.Pending) != 0 {
		return h.debugRefuseLocked("input_fenced")
	}
	for _, record := range next.Records {
		if record.State == "unknown" {
			return h.debugRefuseLocked("input_fenced")
		}
	}
	for _, r := range next.Debug.Results {
		if !r.Permitted {
			return h.debugRefuseLocked("input_fenced")
		}
	}
	if h.debugOutstandingLocked(next) {
		return h.debugRefuseLocked("input_fenced")
	}
	return nil
}
func (h *Host) debugControl(token, op string, revision uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	required := []string{"view", "writer_control"}
	if op == "detach" {
		required = []string{"view"}
	}
	g, err := h.debugGrantLocked(token, required...)
	if err != nil {
		return err
	}
	if h.debug.viewer != token {
		return errors.New("attach_unauthorized")
	}
	h.debugExpireLeaseLocked()
	if op == "detach" {
		// Transport release accepts no native input and always completes. Try
		// one ordinary intent record, then release one slot/grant and hold its
		// writer even if custody is full/broken. No retry queue or emergency
		// allocation is created. A returned error reports durability only.
		journalErr := h.debugIntentLocked(g, debugControlIntent{Operation: "detach", Target: g.id, ExpectedRevision: revision, Decision: "detach"}, false)
		h.debug.viewer = ""
		delete(h.debug.grants, token)
		holdErr := h.debugHoldLocked()
		if journalErr != nil {
			return journalErr
		}
		return holdErr
	}
	if h.state.StopRequested || h.state.Debug.Retired {
		return h.debugRefuseLocked("input_stopped")
	}
	next := cloneState(h.state)
	switch op {
	case "claim":
		if next.Debug.Writer == "claimed" {
			return h.debugRefuseLocked("input_writer_required")
		}
		next.Debug.Writer = "claimed"
		next.Debug.WriterRevision++
	case "renew", "yield":
		if next.Debug.Writer != "claimed" {
			return h.debugRefuseLocked("input_writer_required")
		}
		if revision != next.Debug.WriterRevision {
			return h.debugRefuseLocked("input_observation_stale")
		}
		if op == "yield" {
			next.Debug.Writer = "held"
			next.Debug.WriterRevision++
		}
	case "resume_automation", "yield_and_resume", "detach_with_resume":
		if op == "yield_and_resume" && next.Debug.Writer != "claimed" {
			return h.debugRefuseLocked("input_writer_required")
		}
		if revision != next.Debug.WriterRevision {
			return h.debugRefuseLocked("input_observation_stale")
		}
		if err := h.debugResumeEligibleLocked(next); err != nil {
			return err
		}
		next.Debug.Writer = "automatic"
		next.Debug.WriterRevision++
	default:
		return h.debugRefuseLocked("input_operation_unsupported")
	}
	if err := h.debugIntentLocked(g, debugControlIntent{Operation: op, Target: g.id, ExpectedRevision: revision, Decision: next.Debug.Writer}, false); err != nil {
		return err
	}
	if err := h.persistLocked(next); err != nil {
		return err
	}
	if op == "detach_with_resume" {
		h.debug.viewer = ""
		delete(h.debug.grants, token)
	} else {
		h.debug.leaseUntil = h.debug.now().Add(debugLease)
	}
	h.wake()
	return nil
}

// Canonical typed accepted intent. Targets identify resources, never bearer
// capabilities; the host adds attribution/time and hashes these exact bytes.
type debugControlIntent struct {
	Operation        string `json:"operation"`
	Target           string `json:"target"`
	ExpectedRevision uint64 `json:"expected_revision"`
	Decision         string `json:"decision"`
}

func (h *Host) debugIntentLocked(g debugGrant, intent debugControlIntent, protected bool) error {
	body, err := json.Marshal(intent)
	if err != nil {
		return debugStorageError()
	}
	return h.debugAppendLocked(g, intent.Operation, intent.Target, body, protected)
}
func (h *Host) debugAppendLocked(g debugGrant, kind, id string, body []byte, control bool) error {
	if h.debug == nil {
		return debugStorageError()
	}
	e := debugJournalEntry{Actor: g.actor, Role: g.role, Kind: kind, ID: id, Execution: h.state.Execution, Epoch: h.state.Epoch, GrantRevision: g.revision, WriterRevision: h.state.Debug.WriterRevision, Observation: h.state.InputState, Digest: wireDigest(body), Body: body}
	if err := h.debug.journal.append(e, control, h.debug.now()); err != nil {
		if err.Error() != "input_capacity_exceeded" {
			h.state.Fence = "unknown"
			h.state.InputState = InputUnknown
		}
		return h.debugRefuseLocked(err.Error())
	}
	return nil
}
func (h *Host) debugJournalLocked(kind, id string, body []byte, control bool) error {
	if !h.state.DebugAttach {
		return nil
	}
	g := debugGrant{actor: "host", role: "host"}
	if kind == "stop" {
		return h.debugIntentLocked(g, debugControlIntent{Operation: kind, Target: h.state.Execution, ExpectedRevision: h.state.Debug.WriterRevision, Decision: "stop"}, true)
	}
	return h.debugAppendLocked(g, kind, id, body, control)
}

// debugTerminalLocked adds the content-free terminal fact and its initial
// notification debt to the same pending transition; the caller commits both.
func (h *Host) debugTerminalLocked(next *State, input, thread, turn, outcome string) error {
	if len(next.Debug.Results) >= MaxRecords {
		return errors.New("input_capacity_exceeded")
	}
	id := "result-" + hex.EncodeToString([]byte{byte(len(next.Debug.Results) + 1)})
	if err := h.debugJournalLocked("terminal_observed", id, nil, true); err != nil {
		return err
	}
	now := time.Now()
	if h.debug != nil {
		now = h.debug.now()
	}
	next.Debug.Results = append(next.Debug.Results, debugResult{ID: id, Thread: thread, Turn: turn, Outcome: outcome, Input: input, ObservedAt: now.UTC(), Revision: 1, NotificationPending: true})
	return nil
}

// debugAnnounceLocked hands every fact committed beyond prior to the notifier
// seam once. The notifier must not block; delivery health belongs elsewhere.
func (h *Host) debugAnnounceLocked(prior int) {
	if h.state.Debug == nil {
		return
	}
	for i := prior; i < len(h.state.Debug.Results); i++ {
		h.debugNotifyLocked(h.state.Debug.Results[i])
	}
}
func (h *Host) debugNotifyLocked(r debugResult) {
	if h.debugNotify == nil {
		return
	}
	h.debugNotify(debugNotification{Kind: "terminal_observed", Execution: h.state.Execution, Epoch: h.state.Epoch, Result: r.ID, Revision: r.Revision, Outcome: r.Outcome, ObservedAt: r.ObservedAt})
}
func (h *Host) debugDisposition(token, id string, revision uint64, ack, permit bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	rights := []string{"view"}
	if !ack {
		rights = append(rights, "writer_control")
	}
	g, err := h.debugGrantLocked(token, rights...)
	if err != nil {
		return err
	}
	if h.debug.viewer != token {
		return errors.New("attach_unauthorized")
	}
	next := cloneState(h.state)
	for i := range next.Debug.Results {
		r := &next.Debug.Results[i]
		if r.ID != id {
			continue
		}
		if r.Revision != revision {
			return h.debugRefuseLocked("input_observation_stale")
		}
		kind := "disposition_terminal"
		if ack {
			kind = "ack_terminal"
			r.Ack = true
			r.NotificationPending = false
		} else {
			r.Permitted = permit
		}
		r.Revision++
		decision := "keep_held"
		if ack {
			decision = "acknowledge"
		} else if permit {
			decision = "permit_automatic"
		}
		if err := h.debugIntentLocked(g, debugControlIntent{Operation: kind, Target: id, ExpectedRevision: revision, Decision: decision}, false); err != nil {
			return err
		}
		return h.persistLocked(next)
	}
	return h.debugRefuseLocked("input_observation_stale")
}

// debugStopLocked fences the writer and every unanswered native interaction in
// the pending transition. Stop authority is checked by the caller.
func debugStopLocked(next *State) {
	next.Debug.Writer = "stopped"
	next.Debug.WriterRevision++
	for i := range next.Debug.Interactions {
		if next.Debug.Interactions[i].State == "pending" {
			next.Debug.Interactions[i].State = "invalidated"
			next.Debug.Interactions[i].Revision++
		}
	}
}
func (h *Host) debugStop(ctx context.Context, token string) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("deadline_required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	g, err := h.debugGrantLocked(token, "view", "stop")
	if err == nil && h.debug.viewer != token {
		err = errors.New("attach_unauthorized")
	}
	if err == nil && !h.state.StopRequested {
		err = h.debugIntentLocked(g, debugControlIntent{Operation: "stop", Target: h.state.Execution, ExpectedRevision: h.state.Debug.WriterRevision, Decision: "stop"}, true)
	}
	if err == nil {
		next := cloneState(h.state)
		next.StopRequested = true
		next.Fence = "held"
		if h.state.Fence == "unknown" {
			next.Fence = "unknown"
		}
		debugStopLocked(&next)
		err = h.persistLocked(next)
	}
	epoch := h.state.Epoch
	h.mu.Unlock()
	if err != nil {
		return err
	}
	// Only the preexisting owned-child path may issue a stop. Unit fixtures can
	// inspect the durable stop marker without starting a runner.
	if h.client == nil {
		return nil
	}
	response, err := h.client.Call(ctx, runner.Request{Op: "stop", Execution: h.cfg.Execution, Epoch: epoch})
	if err != nil || response.Code != "stopped" {
		return errors.New("input_effect_unknown")
	}
	return nil
}

// debugRetire logically purges private custody after stop. Unknown effects
// block retirement until explicitly abandoned, and an abandoned effect keeps
// its content-free unknown tombstone. Any purge failure leaves retirement
// incomplete for retry; nothing claims physical erasure or native-file purge.
func (h *Host) debugRetire(abandon bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.state.DebugAttach || h.debug == nil {
		return errors.New("attach_not_enabled")
	}
	if !h.state.StopRequested {
		return errors.New("input_fenced")
	}
	if h.state.Protocol != nil && len(h.state.Protocol.Pending) != 0 {
		return errors.New("input_fenced")
	}
	for _, r := range h.state.Records {
		if r.State == "unknown" {
			return errors.New("input_effect_unknown")
		}
	}
	next := cloneState(h.state)
	for i := range next.Debug.Inputs {
		in := &next.Debug.Inputs[i]
		if in.Stage == "issued" || (in.Stage == "effect_unknown" && !in.Abandoned) {
			if !abandon {
				return errors.New("input_effect_unknown")
			}
			in.Stage = "effect_unknown"
			in.Abandoned = true
		}
	}
	for i := range next.Debug.Interactions {
		x := &next.Debug.Interactions[i]
		if x.State == "pending" {
			x.State = "invalidated"
			x.Revision++
		}
		if x.State == "answer_reserved" {
			if !abandon {
				return errors.New("input_effect_unknown")
			}
			x.State = "effect_unknown"
			x.Revision++
		}
	}
	if err := h.debug.journal.purge(); err != nil {
		return err
	}
	h.debug.tail.close()
	clear(h.debug.grants)
	clear(h.debug.options)
	h.debug.viewer = ""
	next.Debug.Retired = true
	next.Debug.Writer = "stopped"
	return h.persistLocked(next)
}

// Exact Appendix B envelope; public classes cannot enter this private codec.
var debugPrivateCodes = []string{"input_writer_required", "input_busy", "input_observation_stale", "input_fenced", "input_stopped", "input_invalid", "input_conflict", "input_capacity_exceeded", "input_storage_unavailable", "input_effect_unknown", "input_operation_unsupported", "interaction_unsupported", "interaction_stale", "interaction_owner_required", "input_right_required"}

func debugRefusal(code string) ([]byte, error) {
	if !slices.Contains(debugPrivateCodes, code) {
		return nil, errors.New("input_invalid")
	}
	b, _ := json.Marshal(struct {
		Contract string   `json:"contract"`
		OK       bool     `json:"ok"`
		Code     string   `json:"code"`
		Details  struct{} `json:"details"`
	}{Contract: debugContract, Code: code})
	return append(b, '\n'), nil
}
