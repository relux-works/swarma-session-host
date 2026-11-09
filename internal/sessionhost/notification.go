package sessionhost

import (
	"errors"
	"strconv"
	"unicode/utf8"
)

const NotificationContract = "curator-host-notification/1"

// These errors contain no received payload. Neither error is a negative receipt
// for an earlier call. A transport owner must close on a decode error and retain
// any possibly accepted obligation; this codec does not perform native input.
var (
	ErrNotificationFrame      = errors.New("invalid_notification_frame")
	ErrNotificationUnresolved = errors.New("notification_response_unresolved")
)

// NotificationBinding contains opaque references supplied by admitted custody.
// Valid syntax does not establish enrollment, current ownership or authority.
type NotificationBinding struct {
	Execution          string `json:"execution"`
	ControllerEpoch    string `json:"controller_epoch"`
	RunnerInstance     string `json:"runner_instance"`
	NativeThread       string `json:"native_thread"`
	MemberRef          string `json:"member_ref"`
	ConnectionRef      string `json:"connection_ref"`
	ConfigurationRef   string `json:"configuration_ref"`
	ProfileLockRef     string `json:"profile_lock_ref"`
	ConsumerGeneration string `json:"consumer_generation"`
	EnrollmentRef      string `json:"enrollment_ref"`
	WorkflowReference  string `json:"workflow_reference"`
	WorkflowSHA256     string `json:"workflow_sha256"`
}

func (b NotificationBinding) valid() bool {
	return scalar.MatchString(b.Execution) && notificationU64(b.ControllerEpoch, true) &&
		scalar.MatchString(b.RunnerInstance) && scalar.MatchString(b.NativeThread) &&
		notificationRef(b.MemberRef, 512) && notificationRef(b.ConnectionRef, 128) &&
		notificationRef(b.ConfigurationRef, 512) && notificationRef(b.ProfileLockRef, 512) &&
		notificationRef(b.ConsumerGeneration, 128) && notificationRef(b.EnrollmentRef, 512) &&
		scalar.MatchString(b.WorkflowReference) && notificationSHA256(b.WorkflowSHA256)
}

type NotificationRequest struct {
	Contract  string              `json:"contract"`
	RequestID string              `json:"request_id"`
	Method    string              `json:"method"`
	Binding   NotificationBinding `json:"binding"`
	NoticeID  string              `json:"notice_id,omitempty"`
}

func (r NotificationRequest) valid() bool {
	if r.Contract != NotificationContract || !notificationRef(r.RequestID, 128) || !r.Binding.valid() {
		return false
	}
	switch r.Method {
	case "observe_binding":
		return r.NoticeID == ""
	case "trigger", "lookup_attempt":
		return scalar.MatchString(r.NoticeID)
	}
	return false
}

// NotificationView is advisory metadata, never a future input grant or a binary
// attestation. The scheduler must recheck the binding and gates atomically.
type NotificationView struct {
	Binding               NotificationBinding `json:"binding"`
	Adapter               string              `json:"adapter"`
	DeclaredNativeRelease string              `json:"declared_native_release"`
	NoticeMode            string              `json:"notice_mode"`
	Gate                  string              `json:"gate"`
	RecordsRemaining      int                 `json:"records_remaining"`
}

func (v NotificationView) valid() bool {
	return v.Binding.valid() && v.Adapter == "codex" && v.DeclaredNativeRelease == "0.167.0" &&
		v.NoticeMode == WorkReference && v.RecordsRemaining >= 0 && v.RecordsRemaining <= 32 &&
		notificationOneOf(v.Gate, "open", "input-not-ready", "external-held", "security-stopped", "capacity-full", "observation-unavailable")
}

// NotificationAttempt reports retained Host custody and native evidence only.
// Inbox printing, mailbox receipt and a peer reply are separate observations.
type NotificationAttempt struct {
	NoticeID          string `json:"notice_id"`
	WorkflowReference string `json:"workflow_reference"`
	WorkflowSHA256    string `json:"workflow_sha256"`
	Sequence          string `json:"sequence"`
	Custody           string `json:"custody"`
	NativeStage       string `json:"native_stage"`
}

func (a NotificationAttempt) valid() bool {
	return scalar.MatchString(a.NoticeID) && scalar.MatchString(a.WorkflowReference) &&
		notificationSHA256(a.WorkflowSHA256) && notificationU64(a.Sequence, true) &&
		a.Custody == "host-accepted" && notificationOneOf(a.NativeStage,
		"not-evidenced", "write-unresolved", "native-accepted", "native-completed")
}

type NotificationResponse struct {
	Contract  string               `json:"contract"`
	RequestID string               `json:"request_id"`
	Method    string               `json:"method"`
	Result    string               `json:"result"`
	Reason    string               `json:"reason"`
	View      *NotificationView    `json:"view,omitempty"`
	Attempt   *NotificationAttempt `json:"attempt,omitempty"`
}

// validFor requires the exact call identity and successful payload correlation.
// A refusal applies to this call only; it does not settle an earlier uncertain
// call with the same notice. Matching bytes alone do not prove durable custody.
func (r NotificationResponse) validFor(q NotificationRequest) bool {
	if !q.valid() || r.Contract != q.Contract || r.RequestID != q.RequestID || r.Method != q.Method {
		return false
	}
	switch r.Result {
	case "observed":
		return q.Method == "observe_binding" && r.Reason == "" && r.Attempt == nil &&
			r.View != nil && r.View.valid() && r.View.Binding == q.Binding
	case "accepted", "existing-attempt":
		methodOK := q.Method == "trigger" || (r.Result == "existing-attempt" && q.Method == "lookup_attempt")
		return methodOK && r.Reason == "" && r.View == nil && r.Attempt != nil && r.Attempt.valid() &&
			r.Attempt.NoticeID == q.NoticeID && r.Attempt.WorkflowReference == q.Binding.WorkflowReference &&
			r.Attempt.WorkflowSHA256 == q.Binding.WorkflowSHA256
	case "refused":
		return r.View == nil && r.Attempt == nil && notificationOneOf(r.Reason,
			"unsupported-contract", "attachment-unavailable", "enrollment-unverified", "binding-conflict", "stale-binding",
			"input-not-ready", "external-held", "security-stopped", "capacity-full", "invalid-attempt", "workflow-conflict")
	case "unresolved":
		return r.View == nil && r.Attempt == nil && notificationOneOf(r.Reason, "attempt-unresolved", "observation-unavailable")
	}
	return false
}

func notificationRef(s string, maxBytes int) bool {
	if s == "" || len(s) > maxBytes || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return false
		}
	}
	return true
}

func notificationU64(s string, nonzero bool) bool {
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return err == nil && (!nonzero || n != 0)
}

func notificationSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func notificationOneOf(s string, values ...string) bool {
	for _, v := range values {
		if s == v {
			return true
		}
	}
	return false
}
