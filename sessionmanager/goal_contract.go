package sessionmanager

import (
	"context"
	"time"
)

// These goal and input ports carry references and observed input evidence.
// Canonical revisions and legacy application wire conversion stay with the caller.
type AckExpectation struct {
	Kind        AckKind
	Correlation string
}

type AckKind string

type ControllerEpoch uint64

type ControllerLease struct {
	SessionID string
	Epoch     ControllerEpoch
}

type GoalActivation struct {
	Ref     *RevisionRef
	Pending bool
}

// GoalBindingFactory binds one already-admitted session's actual controller and
// writer. Caller/holder authorization and budget admission belong to the supplied
// service; a raw owner string or OS UID does not supply either authority.
type GoalBindingFactory interface {
	BindGoal(context.Context, ControllerLease, string, HostInputWriter) (OwnerKey, GoalBinding, error)
}

type GoalBinding interface {
	Apply(*GoalEnvelope) error
	Rollback(*GoalEnvelope) error
}

type GoalDelivery interface {
	ApplyGoal(context.Context, *GoalEnvelope, ControllerLease, HostInputWriter) (InputObservation, error)
	RollbackGoal(context.Context, *GoalEnvelope, ControllerLease, HostInputWriter) (InputObservation, error)
}

type GoalEnvelope struct {
	Owner OwnerKey
	Ref   *RevisionRef
	Clear bool
}

type GoalGate interface {
	DisabledGoalPlane(OwnerKey) (string, error)
}

type GoalRead struct {
	Latest  *RevisionRef
	Applied *RevisionRef
	Pending bool
}

type GoalSource interface {
	Get(OwnerKey) (*GoalRead, error)
	Reconcile(OwnerKey, GoalBinding) (*GoalActivation, error)
	ConfirmNativeApplied(OwnerKey, RevisionRef) error
}

type HostInputController interface {
	CurrentLease(context.Context, string) (ControllerLease, error)
}

type HostInputWriter interface {
	Submit(context.Context, InputSubmission) (InputReceipt, error)
	Await(context.Context, InputReceipt) (InputObservation, error)
	Cancel(context.Context, InputReceipt) error
}

type InputObservation struct {
	Receipt     InputReceipt
	Kind        InputObservationKind
	Correlation string
	ObservedAt  time.Time
	MuseTurn    *MuseTurnObservation
	CodexTurn   *CodexTurnObservation

	EvidenceClass InputEvidenceClass
	ClaudeGoal    *ClaudeGoalObservation
	PTYRun        *PTYRunObservation
}

type InputObservationKind string

type InputReceipt struct {
	SessionID string
	Epoch     ControllerEpoch
	ID        string
}

type InputSubmission struct {
	Lease    ControllerLease
	ID       string
	Payload  RenderedInput
	Expected AckExpectation
}

type MuseGoalInput struct {
	CurrentWork     string
	NextWork        string
	Objective       string
	PercentComplete int
	Status          string
}

type OwnerKey string

type RenderedInput struct {
	Text      *string
	MuseGoal  *MuseGoalCommand
	MuseTurn  *MuseTurnCommand
	CodexTurn *CodexTurnCommand

	ClaudeGoal *ClaudeGoalCommand
}

type RevisionRef struct {
	ID       string
	Revision int
}

type NativeGoalObservation struct {
	SessionID  string
	Epoch      ControllerEpoch
	Objective  string
	Status     string
	Sequence   uint64
	ObservedAt time.Time
	// D6: actual native stated terms; empty means unstated. Observation only.
	EffectiveApprovalMode string
	EffectiveModelID      string
	// D7: actual native cumulative counters; nil means unstated, not zero.
	Cumulative *MuseRestoreUsage
}

// WithNativeGoal keeps the host writer fence across observation, decision and
// submission. Its callback writer expires when the callback returns.
type NativeGoalObserver interface {
	WithNativeGoal(context.Context, ControllerLease, func(NativeGoalObservation, HostInputWriter) error) error
}

type MuseGoalCommand struct {
	CommandID string
	Set       *MuseGoalSet
	Edit      *MuseGoalEdit
	Pause     *MuseGoalPause
	Clear     *MuseGoalClear
}

type MuseGoalSet struct{ Objective string }
type MuseGoalEdit struct{ Objective string }
type MuseGoalPause struct{ Sequence uint64 }
type MuseGoalClear struct{}

// Contract delta D3: native Muse turns use the same fenced writer and receipt.
type MuseTurnCommand struct {
	CommandID string
	Start     *MuseTurnStart
	Interrupt *MuseTurnInterrupt
	Steer     *MuseTurnSteer
}
type MuseTurnStart struct {
	Text   string
	IfBusy MuseTurnIfBusy
}
type MuseTurnInterrupt struct{ TurnID string }

// Contract delta D3b: exact-target steering is distinct from Start/IfBusy.
type MuseTurnSteer struct {
	Text           string
	ExpectedTurnID string
}
type MuseTurnIfBusy string
type MuseTurnObservation struct {
	CommandID      string
	TurnID         string
	Disposition    MuseTurnDisposition
	StartedNewTurn bool
}
type MuseTurnDisposition string

// Contract delta D10: native Codex assignment uses the same fenced writer.
type CodexTurnCommand struct {
	CommandID string
	Start     *CodexTurnStart
}
type CodexTurnStart struct{ Text string }
type CodexTurnObservation struct {
	CommandID      string
	ThreadID       string
	TurnID         string
	StartedNewTurn bool
}
