package sessionhost

import (
	"encoding/json"
	"strconv"

	"github.com/relux-works/swarma-session-host/internal/localipc"
)

const notificationStateVersion = 4

// This is retained correlation, never an enrollment grant. Attachment authority
// is ephemeral and must come from the admitted manager plus its actual receipt.
// Version one retains one immutable binding; it implements no renewal/handover.
type notificationJournal struct {
	Binding  NotificationBinding  `json:"binding"`
	Attempts []notificationIntent `json:"attempts"`
}

type notificationIntent struct {
	NoticeID string `json:"notice_id"`
	Sequence uint64 `json:"sequence"`
}

func validNotificationState(s State) bool {
	j := s.Notification
	if j == nil {
		return s.Version != notificationStateVersion
	}
	if s.Version != notificationStateVersion || s.NoticeMode != WorkReference || s.Adapter != "codex" || s.Protocol == nil ||
		s.Protocol.ObservedRelease != "0.164.0" || !j.Binding.valid() || j.Binding.Execution != s.Execution ||
		j.Binding.RunnerInstance != s.RunnerInstance || len(j.Attempts) > MaxRecords {
		return false
	}
	epoch, _ := strconv.ParseUint(j.Binding.ControllerEpoch, 10, 64)
	if epoch > s.Epoch {
		return false
	}
	var prior uint64
	for _, a := range j.Attempts {
		if a.Sequence <= prior || a.Sequence > uint64(len(s.Records)) {
			return false
		}
		r := s.Records[a.Sequence-1]
		if r.Sequence != a.Sequence || r.Notice.ID != a.NoticeID || r.Notice.Reference != j.Binding.WorkflowReference ||
			(r.NativeAccepted && r.NativeSession != j.Binding.NativeThread) {
			return false
		}
		prior = a.Sequence
	}
	return true
}

// installNotificationBinding is deliberately private. A future admitted manager
// facade must verify actual Source enrollment/custody before calling it; these
// Host-local checks cannot establish that authority. No CLI or wire operation
// calls it, and persisted correlation alone never invokes it after replacement.
func (h *Host) installNotificationBinding(b NotificationBinding) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil || h.err != nil || h.notificationClosed || !h.notificationHostMatchesLocked(b) || h.state.StopRequested {
		return ErrNotificationUnresolved
	}
	if h.state.Notification != nil {
		if h.state.Notification.Binding != b {
			return ErrNotificationUnresolved
		}
	} else {
		next := cloneState(h.state)
		next.Version = notificationStateVersion
		next.Notification = &notificationJournal{Binding: b, Attempts: []notificationIntent{}}
		if !notificationStateFits(next) || h.persistLocked(next) != nil {
			return ErrNotificationUnresolved
		}
	}
	copy := b
	h.notificationBinding = &copy
	return nil
}

func (h *Host) notificationHostMatchesLocked(b NotificationBinding) bool {
	return b.valid() && h.driver != nil && h.state.Protocol != nil && h.state.Adapter == "codex" &&
		h.state.NoticeMode == WorkReference && h.state.Protocol.ObservedRelease == "0.164.0" &&
		b.Execution == h.state.Execution && b.ControllerEpoch == strconv.FormatUint(h.state.Epoch, 10) &&
		b.RunnerInstance == h.state.RunnerInstance && b.NativeThread == h.state.Protocol.View.NativeSession
}

func (h *Host) notificationCurrentLocked() bool {
	return !h.notificationClosed && h.notificationBinding != nil && h.state.Notification != nil &&
		*h.notificationBinding == h.state.Notification.Binding && h.notificationHostMatchesLocked(*h.notificationBinding)
}

// A lost attachment stays closed in this lifecycle. Earlier accepted/unknown
// records remain untouched; an explicit future handover is a separate contract.
func (h *Host) loseNotificationBinding() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.notificationBinding = nil
	h.notificationClosed = true
	next := cloneState(h.state)
	next.Fence, next.InputState = "unknown", InputUnknown
	_ = h.persistLocked(next)
}

func notificationStateFits(s State) bool {
	b, err := json.MarshalIndent(s, "", "  ")
	return err == nil && len(b)+1 <= localipc.MaxFrame
}

func (h *Host) notificationRecordLocked(id string) (Record, bool) {
	if h.state.Notification != nil {
		for _, a := range h.state.Notification.Attempts {
			if a.NoticeID == id && a.Sequence > 0 && a.Sequence <= uint64(len(h.state.Records)) {
				return h.state.Records[a.Sequence-1], true
			}
		}
	}
	return Record{}, false
}

func (h *Host) notificationGateLocked() string {
	if h.ctx.Err() != nil || h.err != nil || h.protocolFailed || h.writeBlocked || h.recovering ||
		h.state.Protocol == nil || h.state.Fence == "unknown" {
		return "observation-unavailable"
	}
	if h.state.SecurityStopped {
		return "security-stopped"
	}
	if h.state.Fence == "held" {
		return "external-held"
	}
	if h.state.StopRequested || h.state.InputState != InputIdle || len(h.state.Protocol.Pending) != 0 {
		return "input-not-ready"
	}
	for _, r := range h.state.Records {
		if r.State == "unknown" {
			return "input-not-ready"
		}
	}
	for _, a := range h.state.Notification.Attempts {
		r := h.state.Records[a.Sequence-1]
		if r.State != "applied" || r.Outcome != "completed" {
			return "input-not-ready"
		}
	}
	if len(h.state.Records) >= MaxRecords {
		return "capacity-full"
	}
	return "open"
}

func notificationAttempt(r Record, b NotificationBinding) (*NotificationAttempt, bool) {
	stage := "not-evidenced"
	if r.Outcome != "" && r.Outcome != "completed" {
		return nil, false
	}
	if r.NativeAccepted {
		stage = "native-accepted"
		if r.State == "applied" && r.Outcome == "completed" {
			stage = "native-completed"
		}
	} else if r.State == "unknown" {
		stage = "write-unresolved"
	}
	return &NotificationAttempt{NoticeID: r.Notice.ID, WorkflowReference: b.WorkflowReference, WorkflowSHA256: b.WorkflowSHA256,
		Sequence: strconv.FormatUint(r.Sequence, 10), Custody: "host-accepted", NativeStage: stage}, true
}

// notificationRequest has no transport caller yet. It serializes the complete
// admission check and intent with the same lock/store as the existing scheduler.
// Native submission remains the existing work-reference path, never a second
// worker, prompt renderer or direct write from this method.
func (h *Host) notificationRequest(q NotificationRequest) (NotificationResponse, error) {
	if !q.valid() {
		return NotificationResponse{}, ErrNotificationFrame
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	r := NotificationResponse{Contract: q.Contract, RequestID: q.RequestID, Method: q.Method}
	finish := func(result, reason string) (NotificationResponse, error) {
		r.Result, r.Reason = result, reason
		return r, nil
	}
	if !h.notificationCurrentLocked() || h.ctx.Err() != nil || h.err != nil {
		return finish("unresolved", "observation-unavailable")
	}
	b := *h.notificationBinding
	if q.Binding != b {
		if q.Binding.WorkflowReference != b.WorkflowReference || q.Binding.WorkflowSHA256 != b.WorkflowSHA256 {
			return finish("refused", "workflow-conflict")
		}
		if q.Binding.ControllerEpoch != b.ControllerEpoch || q.Binding.ConsumerGeneration != b.ConsumerGeneration {
			return finish("refused", "stale-binding")
		}
		return finish("refused", "binding-conflict")
	}
	if q.Method == "observe_binding" {
		r.View = &NotificationView{Binding: b, Adapter: "codex", DeclaredNativeRelease: "0.164.0", NoticeMode: WorkReference,
			Gate: h.notificationGateLocked(), RecordsRemaining: MaxRecords - len(h.state.Records)}
		return finish("observed", "")
	}
	// Same-attempt reconciliation precedes new-input gates and never mutates.
	if retained, ok := h.notificationRecordLocked(q.NoticeID); ok {
		var known bool
		r.Attempt, known = notificationAttempt(retained, b)
		if !known {
			return finish("unresolved", "attempt-unresolved")
		}
		return finish("existing-attempt", "")
	}
	if q.Method == "lookup_attempt" {
		return finish("unresolved", "attempt-unresolved")
	}
	for _, prior := range h.state.Records {
		if prior.Notice.ID == q.NoticeID {
			return finish("refused", "invalid-attempt") // No legacy reinterpretation.
		}
	}
	gate := h.notificationGateLocked()
	if gate == "observation-unavailable" {
		return finish("unresolved", gate)
	}
	if gate != "open" {
		return finish("refused", gate)
	}
	next := cloneState(h.state)
	next.Sequence++
	notice := Notice{ID: q.NoticeID, Reference: b.WorkflowReference}
	record := Record{Notice: notice, Digest: digestNoticeMode(notice, next.Selection, next.NoticeMode), Sequence: next.Sequence, State: "queued"}
	next.Records = append(next.Records, record)
	next.Notification.Attempts = append(next.Notification.Attempts, notificationIntent{NoticeID: notice.ID, Sequence: record.Sequence})
	if !notificationStateFits(next) {
		return finish("refused", "capacity-full") // Proven before any store mutation.
	}
	if h.persistLocked(next) != nil {
		return finish("unresolved", "attempt-unresolved")
	}
	r.Attempt, _ = notificationAttempt(record, b)
	h.wake()
	return finish("accepted", "")
}

func notificationOwnsRecord(s State, id string) bool {
	if s.Notification != nil {
		for _, a := range s.Notification.Attempts {
			if a.NoticeID == id {
				return true
			}
		}
	}
	return false
}
