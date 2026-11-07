// Package providerprotocol is an inert, proposed Host/process-provider control
// protocol. Decoded metadata is not admission, enrollment proof or an a2a API.
// There is deliberately no process launcher, registry, broker or method service.
package providerprotocol

import (
	"errors"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	Version      = 1
	MaxFrame     = 32768
	MaxOperation = 60 * time.Second
	CleanupBound = 5 * time.Second
)

var (
	ErrVersion    = errors.New("provider_version_refused")
	ErrFrame      = errors.New("provider_frame_refused")
	ErrState      = errors.New("provider_state_refused")
	ErrUnresolved = errors.New("provider_observation_unresolved")
	ErrRefused    = errors.New("provider_operation_refused")
	ErrCleanup    = errors.New("provider_cleanup_unjoined")
)

// Binding preserves all twelve accepted fields. Only ConsumerGeneration and
// EnrollmentRef are absent in a Bootstrap prefix; the provider issues them.
type Binding struct {
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

type References struct {
	MemberRef        string `json:"member_ref"`
	ConnectionRef    string `json:"connection_ref"`
	ConfigurationRef string `json:"configuration_ref"`
	ProfileLockRef   string `json:"profile_lock_ref"`
}

// PrepareRequest contains selection/correlation hints, never a decoded origin.
// The source must independently retain and compare its own admitted origins.
type PrepareRequest struct {
	Connection        string `json:"connection"`
	ProfileLockRef    string `json:"profile_lock_ref"`
	WorkflowReference string `json:"workflow_reference"`
	WorkflowSHA256    string `json:"workflow_sha256"`
}

type BootstrapRequest struct {
	Binding       Binding `json:"binding"`
	AttachmentRef string  `json:"attachment_ref"`
	CallTimeoutMS int     `json:"call_timeout_ms"`
}

// Enrollment is an untrusted projection for the retained Host callback to
// verify. In particular, strings and process metadata prove no live custody.
type Enrollment struct {
	Binding        Binding `json:"binding"`
	AttachmentRef  string  `json:"attachment_ref"`
	SourceSession  string  `json:"source_session"`
	ConsumerRef    string  `json:"consumer_ref"`
	LeaseHolder    string  `json:"lease_holder"`
	LeaseHost      string  `json:"lease_host"`
	LeasePID       int64   `json:"lease_pid"`
	LeaseExpiresAt string  `json:"lease_expires_at"`
	ProcessBirth   string  `json:"process_birth"`
}

type Refusal struct {
	Reason string `json:"reason"`
}

// RefusedError contains only a validated closed reason, never received text.
type RefusedError struct{ Reason string }

func (r RefusedError) Error() string { return "provider_operation_refused: " + r.Reason }
func (r RefusedError) Unwrap() error { return ErrRefused }

func ref(s string, max int) bool {
	if s == "" || len(s) > max || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r == utf8.RuneError || r <= 0x1f || r >= 0x7f && r <= 0x9f {
			return false
		}
	}
	return true
}
func scalar(s string) bool {
	if !ref(s, 128) {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}
func digest(s string) bool {
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
func positive(s string) bool {
	if s == "" || s[0] == '0' {
		return false
	}
	n, e := strconv.ParseUint(s, 10, 64)
	return e == nil && n > 0
}
func (r References) valid() bool {
	return ref(r.MemberRef, 512) && ref(r.ConnectionRef, 128) && ref(r.ConfigurationRef, 512) && ref(r.ProfileLockRef, 512)
}
func (b Binding) references() References {
	return References{b.MemberRef, b.ConnectionRef, b.ConfigurationRef, b.ProfileLockRef}
}
func (b Binding) valid(full bool) bool {
	issued := b.ConsumerGeneration == "" && b.EnrollmentRef == ""
	if full {
		issued = ref(b.ConsumerGeneration, 128) && ref(b.EnrollmentRef, 512)
	}
	return scalar(b.Execution) && positive(b.ControllerEpoch) && scalar(b.RunnerInstance) && scalar(b.NativeThread) && b.references().valid() && scalar(b.WorkflowReference) && digest(b.WorkflowSHA256) && issued
}
func (p PrepareRequest) valid() bool {
	return ref(p.Connection, 128) && ref(p.ProfileLockRef, 512) && scalar(p.WorkflowReference) && digest(p.WorkflowSHA256)
}
func (b BootstrapRequest) valid() bool {
	return b.Binding.valid(false) && ref(b.AttachmentRef, 128) && b.CallTimeoutMS >= 1 && b.CallTimeoutMS <= 60000
}
func (e Enrollment) expiry() (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, e.LeaseExpiresAt)
	if err != nil || t.Location() != time.UTC || t.Format(time.RFC3339Nano) != e.LeaseExpiresAt {
		return time.Time{}, ErrFrame
	}
	return t, nil
}
func (e Enrollment) valid() bool {
	_, err := e.expiry()
	return e.Binding.valid(true) && ref(e.AttachmentRef, 128) && ref(e.SourceSession, 128) && ref(e.ConsumerRef, 128) && ref(e.LeaseHolder, 128) && ref(e.LeaseHost, 128) && e.LeasePID > 0 && ref(e.ProcessBirth, 128) && err == nil
}
func (r Refusal) valid() bool {
	switch r.Reason {
	case "unsupported-version", "admission-unavailable", "origin-unavailable", "binding-conflict", "stale-binding", "lease-unavailable", "consumer-conflict", "enrollment-unverified", "operation-expired", "observation-unavailable":
		return true
	}
	return false
}
