// Package sessiongoal owns session goals, independently of a task board.
// Admission supplies verified facts; this package neither authenticates a peer
// nor issues launch grants. Missing admission and budget suppliers fail closed.
package sessiongoal

import (
	"context"
	"errors"

	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type Namespace string

const (
	Assignment Namespace = "assignment"
	Working    Namespace = "working"
)

type Action string

const (
	Set    Action = "goal.set"
	Clear  Action = "goal.clear"
	Accept Action = "goal.accept"
)

type State string

const (
	Active   State = "active"
	Cleared  State = "cleared"
	Accepted State = "accepted"
)

type AcceptanceKind string

const (
	HolderAcceptance AcceptanceKind = "holder-acceptance"
	RelayedBarrier   AcceptanceKind = "holder-relayed-barrier"
)

var (
	ErrAuthorityUnavailable = errors.New("verified session caller supplier is unavailable")
	ErrForbidden            = errors.New("caller has no goal authority for this session and namespace")
	ErrInvalidCommand       = errors.New("invalid session goal command")
	ErrRevisionConflict     = errors.New("session goal revision conflict")
	ErrReplayConflict       = errors.New("source envelope reused with different content or authority")
	ErrJournalInvalid       = errors.New("invalid session goal journal")
	ErrCommitUncertain      = errors.New("session goal journal commit is uncertain; reopen required")
	ErrDeliveryUncertain    = errors.New("goal delivery is unresolved under another controller lease")
	ErrBudgetUnavailable    = errors.New("session goal budget binding is unavailable")
	ErrBindingUnavailable   = errors.New("session goal receipt binding is unavailable")
)

// VerifiedCaller is an admission result, never a command payload. The supplier
// must verify exact-session grants and current holder identity/generation and
// keep those facts fenced for the whole callback. Principal strings or an OS
// UID alone are not verification. No production supplier is provided here.
type VerifiedCaller struct {
	SessionID, Principal, Holder, WorkingPrincipal string
	Generation                                     uint64
	Operator, Assignment, Working, UnlimitedBudget bool
}
type Authority interface {
	WithVerifiedCaller(context.Context, string, func(VerifiedCaller) error) error
}

type Budget struct {
	Tokens    int64 `json:"tokens"`
	Unlimited bool  `json:"unlimited"`
}
type Content struct {
	Objective string `json:"objective"`
	Condition string `json:"condition"`
	Budget    Budget `json:"budget"`
}
type Acceptance struct {
	Kind     AcceptanceKind `json:"kind"`
	Evidence string         `json:"evidence"`
}

// Command is a typed host call. It carries no caller, owner, grant or signature
// claim. The bridge's eventual wire envelope is a separate integration boundary.
type Command struct {
	EnvelopeID       string      `json:"envelope_id"`
	SessionID        string      `json:"session_id"`
	Namespace        Namespace   `json:"namespace"`
	Action           Action      `json:"action"`
	ExpectedRevision int         `json:"expected_revision"`
	Content          *Content    `json:"content,omitempty"`
	Acceptance       *Acceptance `json:"acceptance,omitempty"`
}
type Revision struct {
	Ref              core.RevisionRef `json:"ref"`
	SessionID        string           `json:"session_id"`
	Namespace        Namespace        `json:"namespace"`
	SourceEnvelopeID string           `json:"source_envelope_id"`
	Principal        string           `json:"principal"`
	Holder           string           `json:"holder"`
	HolderGeneration uint64           `json:"holder_generation"`
	Content          Content          `json:"content"`
	State            State            `json:"state"`
	Acceptance       *Acceptance      `json:"acceptance,omitempty"`
}
type Receipt struct {
	Ref              core.RevisionRef `json:"ref"`
	SourceEnvelopeID string           `json:"source_envelope_id"`
	Holder           string           `json:"holder"`
	Pending          bool             `json:"pending"`
	Replayed         bool             `json:"-"`
}
type NativeRollback struct {
	Target  *core.RevisionRef `json:"target,omitempty"`
	Receipt core.InputReceipt `json:"receipt"`
}
type Head struct {
	DeliverySerial uint64             `json:"delivery_serial"`
	Rollbacks      []NativeRollback   `json:"rollbacks"`
	Latest         Revision           `json:"latest"`
	Applied        *core.RevisionRef  `json:"applied,omitempty"`
	AppliedReceipt *core.InputReceipt `json:"applied_receipt,omitempty"`
	History        []Revision         `json:"history"`
	Applications   []Application      `json:"applications"`
	Attempt        *deliveryAttempt   `json:"attempt,omitempty"`
	Rollback       *deliveryAttempt   `json:"rollback,omitempty"`
	Acknowledged   *acknowledged      `json:"acknowledged,omitempty"`
}
type deliveryAttempt struct {
	Serial   uint64               `json:"serial"`
	Input    core.InputSubmission `json:"input"`
	Ref      core.RevisionRef     `json:"ref"`
	Lease    core.ControllerLease `json:"lease"`
	InputID  string               `json:"input_id"`
	Provider string               `json:"provider"`
	Receipt  *core.InputReceipt   `json:"receipt,omitempty"`
}
type acknowledged struct {
	Ref     core.RevisionRef  `json:"ref"`
	Receipt core.InputReceipt `json:"receipt"`
}
type Application struct {
	Ref              core.RevisionRef  `json:"ref"`
	SourceEnvelopeID string            `json:"source_envelope_id"`
	Holder           string            `json:"holder"`
	HolderGeneration uint64            `json:"holder_generation"`
	Receipt          core.InputReceipt `json:"receipt"`
}

type acknowledgedEffectError struct{ cause error }

func (e *acknowledgedEffectError) Error() string { return e.cause.Error() }
func (e *acknowledgedEffectError) Unwrap() error { return e.cause }

// BudgetBinding preserves the selected provider's existing per-revision budget
// behavior under the exact controller fence before invoking deliver. Muse needs
// retained usage accounting and budget-driven pause; Codex uses rendered positive
// or unlimited budget text, and Claude uses validated condition/token delivery.
// This port does not invent a native spending cap for providers without one.
// No production supplier or canned success default is provided. A local fixture
// proves invocation/refusal propagation, not provider accounting or enforcement.
type BudgetBinding interface {
	WithBudget(context.Context, core.ControllerLease, Revision, func() error) error
}
