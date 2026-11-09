package sessionhost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/relux-works/swarma-session-host/internal/runner"
	"slices"
	"time"
)

// These types are deliberately private: no caller-facing activation, launcher
// schema, socket, capability advertisement or native input implementation.
type debugResult struct {
	ID                  string `json:"id"`
	Thread              string `json:"thread"`
	Turn                string `json:"turn"`
	Outcome             string `json:"outcome"`
	Revision            uint64 `json:"revision"`
	Ack                 bool   `json:"ack"`
	Permitted           bool   `json:"permitted"`
	NotificationPending bool   `json:"notification_pending"`
}
type debugMetadata struct {
	Writer         string        `json:"writer"`
	WriterRevision uint64        `json:"writer_revision"`
	Results        []debugResult `json:"results,omitempty"`
	Retired        bool          `json:"retired,omitempty"`
}
type debugAuthority struct {
	actor, role string
	epoch       uint64
}
type debugGrant struct {
	actor, role     string
	rights          []string
	revision, epoch uint64
	expires         time.Time
	id              string
}
type debugAttachment struct {
	tail       *debugTail
	journal    *debugJournal
	grants     map[string]debugGrant
	viewer     string
	leaseUntil time.Time
	now        func() time.Time
}

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
func validDebugState(s State) bool {
	if !s.DebugAttach {
		return s.Debug == nil
	}
	if s.Debug == nil || len(s.Debug.Results) > MaxRecords {
		return false
	}
	switch s.Debug.Writer {
	case "automatic", "claimed", "held", "stopped":
	default:
		return false
	}
	seen := map[string]bool{}
	for _, r := range s.Debug.Results {
		if !scalar.MatchString(r.ID) || seen[r.ID] || r.Revision == 0 || r.Thread == "" || r.Turn == "" || !r.NotificationPending {
			return false
		}
		switch r.Outcome {
		case "completed", "failed", "interrupted":
		default:
			return false
		}
		seen[r.ID] = true
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
	d := &debugAttachment{journal: j, grants: make(map[string]debugGrant), now: time.Now}
	d.tail = &debugTail{epoch: h.state.Epoch, execution: h.state.Execution, now: d.now, changed: make(chan struct{}, 1)}
	if replacement {
		d.tail.cause = "host_replaced"
		h.state.Debug.Writer = "held"
		h.state.Debug.WriterRevision++
	}
	h.debug = d
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
	return nil
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
func (d *debugAttachment) close() { d.tail.close(); d.journal.close(); clear(d.grants); d.viewer = "" }
func (h *Host) debugResume(requested bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return admitDebug(&h.state, true, &requested)
}
func (h *Host) debugAutomaticLocked() bool {
	if !h.state.DebugAttach {
		return true
	}
	if h.debug == nil || h.state.Debug == nil || h.state.Debug.Retired {
		return false
	}
	h.debugExpireLeaseLocked()
	if h.state.Debug.Writer != "automatic" {
		return false
	}
	for _, r := range h.state.Debug.Results {
		if !r.Permitted {
			return false
		}
	}
	return true
}
func (h *Host) debugExpireLeaseLocked() {
	if h.debug == nil {
		return
	}
	lostViewer := false
	for token, g := range h.debug.grants {
		if g.epoch != h.state.Epoch || !h.debug.now().Before(g.expires) {
			delete(h.debug.grants, token)
			if h.debug.viewer == token {
				h.debug.viewer = ""
				lostViewer = true
			}
		}
	}
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
func (h *Host) debugIssueGrant(a debugAuthority, rights []string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.state.DebugAttach || h.debug == nil {
		return "", errors.New("attach_not_enabled")
	}
	if h.ctx.Err() != nil {
		return "", errors.New("attach_binding_unavailable")
	}
	if h.state.Debug.Retired {
		return "", errors.New("input_stopped")
	}
	if !scalar.MatchString(a.actor) || (a.role != "manager" && a.role != "owner") || a.epoch != h.state.Epoch {
		return "", errors.New("attach_unauthorized")
	}
	h.debugExpireLeaseLocked()
	if len(rights) == 0 || len(rights) > 5 || len(h.debug.grants) >= 16 {
		return "", errors.New("input_invalid")
	}
	seen := map[string]bool{}
	for _, r := range rights {
		if !slices.Contains([]string{"view", "writer_control", "text", "native_response", "stop"}, r) || seen[r] {
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
	h.debug.grants[token] = debugGrant{a.actor, a.role, slices.Clone(rights), 1, a.epoch, h.debug.now().Add(5 * time.Minute), "grant-" + hex.EncodeToString(identifier[:])}
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
	if !ok || g.epoch != h.state.Epoch || !h.debug.now().Before(g.expires) || !slices.Contains(g.rights, "view") {
		if token == h.debug.viewer {
			h.debug.viewer = ""
			h.debugHoldLocked()
		}
		return g, errors.New("attach_grant_revoked")
	}
	for _, r := range rights {
		if !slices.Contains(g.rights, r) {
			return g, errors.New("input_right_required")
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
	if a.epoch != h.state.Epoch || !scalar.MatchString(a.actor) ||
		(a.role != "owner" && !(a.role == "manager" && exists && g.actor == a.actor && g.role == "manager")) {
		return errors.New("input_right_required")
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
		return errors.New("input_stopped")
	}
	next := cloneState(h.state)
	switch op {
	case "claim":
		if next.Debug.Writer == "claimed" {
			return errors.New("input_writer_required")
		}
		next.Debug.Writer = "claimed"
		next.Debug.WriterRevision++
	case "renew", "yield":
		if next.Debug.Writer != "claimed" {
			return errors.New("input_writer_required")
		}
		if revision != next.Debug.WriterRevision {
			return errors.New("input_observation_stale")
		}
		if op == "yield" {
			next.Debug.Writer = "held"
			next.Debug.WriterRevision++
		}
	case "resume_automation":
		if revision != next.Debug.WriterRevision {
			return errors.New("input_observation_stale")
		}
		if next.InputState != InputIdle || next.Fence != "open" || next.SecurityStopped || h.recovering || h.protocolFailed || h.writeBlocked {
			return errors.New("input_fenced")
		}
		if next.Protocol != nil && len(next.Protocol.Pending) != 0 {
			return errors.New("input_fenced")
		}
		for _, record := range next.Records {
			if record.State == "unknown" {
				return errors.New("input_fenced")
			}
		}
		for _, r := range next.Debug.Results {
			if !r.Permitted {
				return errors.New("input_fenced")
			}
		}
		next.Debug.Writer = "automatic"
		next.Debug.WriterRevision++
	default:
		return errors.New("input_operation_unsupported")
	}
	if err := h.debugIntentLocked(g, debugControlIntent{Operation: op, Target: g.id, ExpectedRevision: revision, Decision: next.Debug.Writer}, false); err != nil {
		return err
	}
	if err := h.persistLocked(next); err != nil {
		return err
	}
	h.debug.leaseUntil = h.debug.now().Add(60 * time.Second)
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
		return err
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
func (h *Host) debugTerminalLocked(next *State, thread, turn, outcome string) error {
	if len(next.Debug.Results) >= MaxRecords {
		return errors.New("input_capacity_exceeded")
	}
	id := "result-" + hex.EncodeToString([]byte{byte(len(next.Debug.Results) + 1)})
	if err := h.debugJournalLocked("terminal_observed", id, nil, true); err != nil {
		return err
	}
	next.Debug.Results = append(next.Debug.Results, debugResult{ID: id, Thread: thread, Turn: turn, Outcome: outcome, Revision: 1, NotificationPending: true})
	return nil
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
			return errors.New("input_observation_stale")
		}
		kind := "disposition_terminal"
		if ack {
			kind = "ack_terminal"
			r.Ack = true
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
	return errors.New("input_observation_stale")
}

// New text and native effects remain inert until adapter qualification. Rights
// are checked independently before revealing that lack of implementation.
func (h *Host) debugInput(token, kind string, revision uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	right := "text"
	if kind == "answer_interaction" {
		right = "native_response"
	} else if kind != "follow_up" {
		return errors.New("input_operation_unsupported")
	}
	_, err := h.debugGrantLocked(token, "view", "writer_control", right)
	if err != nil {
		return err
	}
	if h.debug.viewer != token {
		return errors.New("attach_unauthorized")
	}
	if h.state.StopRequested || h.state.Debug.Retired {
		return errors.New("input_stopped")
	}
	h.debugExpireLeaseLocked()
	if h.state.Debug.Writer != "claimed" {
		return errors.New("input_writer_required")
	}
	if revision != h.state.Debug.WriterRevision {
		return errors.New("input_observation_stale")
	}
	if h.state.Fence != "open" || h.state.SecurityStopped || h.writeBlocked || h.protocolFailed || h.recovering {
		return errors.New("input_fenced")
	}
	if kind == "answer_interaction" {
		return errors.New("interaction_unsupported")
	}
	if h.state.InputState == InputBusy {
		return errors.New("input_busy")
	}
	if h.state.InputState != InputIdle {
		return errors.New("input_fenced")
	}
	return errors.New("input_operation_unsupported")
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
		next.Debug.Writer = "stopped"
		next.Debug.WriterRevision++
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
func (h *Host) debugRetire() error {
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
	h.debug.tail.close()
	clear(h.debug.grants)
	h.debug.viewer = ""
	if err := h.debug.journal.purge(); err != nil {
		return err
	}
	next := cloneState(h.state)
	next.Debug.Retired = true
	next.Debug.Writer = "stopped"
	return h.persistLocked(next)
}

// Exact Appendix B envelope; public classes cannot enter this private codec.
func debugRefusal(code string) ([]byte, error) {
	if !slices.Contains([]string{"input_writer_required", "input_busy", "input_observation_stale", "input_fenced", "input_stopped", "input_invalid", "input_conflict", "input_capacity_exceeded", "input_storage_unavailable", "input_effect_unknown", "input_operation_unsupported", "interaction_unsupported", "interaction_stale", "interaction_owner_required", "input_right_required"}, code) {
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
