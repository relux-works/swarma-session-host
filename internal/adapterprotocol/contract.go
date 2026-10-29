// Package adapterprotocol defines host-side protocol transitions without owning
// a process, transport, mailbox, credentials or provider/model admission.
package adapterprotocol

import (
	"encoding/json"
	"errors"
)

const (
	Version       = 1
	MaxActions    = 4
	MaxFacts      = 16
	MaxWrite      = 4096
	MaxCheckpoint = 8192
)

type InputMode string

const (
	NextTurn        InputMode = "next-turn"
	Steer           InputMode = "steer"
	Interrupt       InputMode = "interrupt"
	RequestExit     InputMode = "request-exit"
	SetNativeGoal   InputMode = "native-goal-set"
	ClearNativeGoal InputMode = "native-goal-clear"
)

// Capabilities describe qualified adapter operations, not a model catalog or a
// promise that an upstream option is usable in the current runtime state.
type Capabilities struct {
	NextTurn                                            bool
	PreallocatedIdentity                                bool
	Resume, Fork, QueueWhileBusy, Steer, Interrupt      bool
	NativeTurnIdentity, Activity, TokenUsage            bool
	GracefulExit, IdleFence, NativeGoals, HumanTerminal bool
}

type Descriptor struct {
	Version int
	// Every enabled capability is qualified on this exact native release.
	// Contract fixtures use "schematic" and confer no native qualification.
	ObservedRelease  string
	Name             string
	PrimaryTransport string
	DeclaredFallback string // Declaration only; never authorizes substitution.
	Capabilities     Capabilities
}

type Selection struct{ Provider, Model, Effort string }
type Lifecycle string

const (
	Create Lifecycle = "create"
	Resume Lifecycle = "resume"
	Fork   Lifecycle = "fork"
)

type Config struct {
	Selection                Selection
	Workspace, NativeSession string
	Lifecycle                Lifecycle
	NativeRelease            string // Caller-supplied release evidence; no catalog/discovery.
}

// Lifecycle admission describes adapter semantics only, never launch authority.
func (d Descriptor) SupportsLifecycle(c Config) bool {
	switch c.Lifecycle {
	case Create:
		return c.NativeSession == "" || (d.Capabilities.PreallocatedIdentity && boundedID(c.NativeSession, true))
	case Resume:
		return d.Capabilities.Resume && boundedID(c.NativeSession, true)
	case Fork:
		return d.Capabilities.Fork && boundedID(c.NativeSession, true)
	}
	return false
}

type Input struct {
	ID, Reference string
	Mode          InputMode
	ExpectedTurn  string
}
type Chunk struct {
	Offset uint64
	Bytes  []byte
	EOF    bool
	AtTail bool // True only after observing the runner's actual retained tail.
}

type Activity string

const (
	Unknown      Activity = "unknown"
	Idle         Activity = "idle"
	Busy         Activity = "busy"
	Draft        Activity = "draft"
	Approval     Activity = "approval"
	SecurityStop Activity = "security-stop"
)

type View struct {
	InputID                    string
	Activity                   Activity
	NativeSession, CurrentTurn string
}

type Action struct {
	Key     string
	InputID string // Empty only for protocol control traffic.
	Bytes   []byte
}

type FactKind string

const (
	InputAccepted   FactKind = "input-accepted"
	TurnCompleted   FactKind = "turn-completed"
	TurnFailed      FactKind = "turn-failed"
	TurnInterrupted FactKind = "turn-interrupted"
)

// Acceptance and completion are separate facts. NativeID may be absent when a
// protocol has no native turn identity; the input and session must still match.
type Fact struct {
	Kind                             FactKind
	InputID, NativeSession, NativeID string
	ReceiptID                        string // Optional native command/input acknowledgement, not a turn ID.
}

type AttentionKind string

const AuthorityRequired AttentionKind = "authority-required"

// Attention is a typed escalation observation. It contains no native question,
// parameter payload or secret-dependent digest. A notifier routes owner-only or
// unclassified questions through the authorized escalation channel.
type Attention struct {
	Kind      AttentionKind
	Key       string
	Class     string
	OwnerOnly bool
}

type Usage struct {
	InputTokens, OutputTokens, CacheReadTokens uint64
	NativeSession, NativeTurn                  string
}

type Transition struct {
	Actions    []Action
	Facts      []Fact
	View       View
	Checkpoint json.RawMessage // Non-secret metadata only, never raw output.
	Attention  []Attention
	Usage      *Usage
}

// Driver is a local state machine. Each call returns a proposal with no I/O.
// The host persists the transition/sequence before writing actions to its runner.
// Unsupported modes return ErrUnsupported with no actions or checkpoint change.
// Restoring a checkpoint must not repeat initialization or uncertain input.
type Driver interface {
	Descriptor() Descriptor
	Begin() (Transition, error)
	Prepare(Input) (Transition, error)
	Observe(Chunk) (Transition, error)
	Snapshot() Transition
}

var ErrUnsupported = errors.New("adapter_operation_unsupported")

func (c Capabilities) Supports(mode InputMode) bool {
	switch mode {
	case NextTurn:
		return c.NextTurn
	case Steer:
		return c.Steer
	case Interrupt:
		return c.Interrupt
	case RequestExit:
		return c.GracefulExit
	case SetNativeGoal, ClearNativeGoal:
		return c.NativeGoals
	}
	return false
}

func boundedID(s string, required bool) bool {
	if len(s) == 0 {
		return !required
	}
	if len(s) > 128 {
		return false
	}
	for _, r := range s {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

// Validate checks bounded framing and shared invariants, never provider syntax.
// Facts refer to the native session but can concern an older queued input while
// View describes the newer active turn. The host correlates each fact to its
// durable record; adapters prove native semantics. Overall idle is not inferred
// from an individual completion, action, pipe write or liveness observation.
func Validate(d Descriptor, t Transition) error {
	if d.Version != Version {
		return errors.New("unsupported_adapter_boundary_version")
	}
	if !boundedID(d.ObservedRelease, true) {
		return errors.New("missing_adapter_observed_release")
	}
	if !boundedID(d.Name, true) || (d.PrimaryTransport != "pipe" && d.PrimaryTransport != "pty") || (d.DeclaredFallback != "" && d.DeclaredFallback != "pty") {
		return errors.New("invalid_adapter_descriptor")
	}
	if len(t.Actions) > MaxActions || len(t.Facts) > MaxFacts || len(t.Attention) > MaxFacts || len(t.Checkpoint) == 0 || len(t.Checkpoint) > MaxCheckpoint || !json.Valid(t.Checkpoint) {
		return errors.New("adapter_transition_bound")
	}
	switch t.View.Activity {
	case Unknown, Idle, Busy, Draft, Approval, SecurityStop:
	default:
		return errors.New("invalid_adapter_activity")
	}
	if !boundedID(t.View.NativeSession, t.View.Activity == Idle) || !boundedID(t.View.CurrentTurn, false) || !boundedID(t.View.InputID, false) {
		return errors.New("invalid_adapter_identity")
	}
	keys := map[string]bool{}
	for _, a := range t.Actions {
		if !boundedID(a.Key, true) || keys[a.Key] || !boundedID(a.InputID, false) || len(a.Bytes) == 0 || len(a.Bytes) > MaxWrite {
			return errors.New("invalid_adapter_action")
		}
		keys[a.Key] = true
		if a.InputID != "" && a.InputID != t.View.InputID {
			return errors.New("invalid_adapter_action_identity")
		}
		if a.InputID != "" && (t.View.Activity == Unknown || t.View.Activity == Draft || t.View.Activity == Approval || t.View.Activity == SecurityStop) {
			return errors.New("adapter_input_closed")
		}
	}
	for _, f := range t.Facts {
		if !boundedID(f.InputID, true) || !boundedID(f.NativeSession, true) || f.NativeSession != t.View.NativeSession || !boundedID(f.NativeID, false) || !boundedID(f.ReceiptID, false) {
			return errors.New("invalid_adapter_fact_identity")
		}
		switch f.Kind {
		case InputAccepted:
			if d.Capabilities.NativeTurnIdentity && f.NativeID == "" && f.ReceiptID == "" {
				return errors.New("missing_adapter_acceptance_identity")
			}
		case TurnFailed, TurnInterrupted:
		case TurnCompleted:
			if d.Capabilities.NativeTurnIdentity && f.NativeID == "" {
				return errors.New("missing_adapter_turn_identity")
			}
		default:
			return errors.New("invalid_adapter_fact")
		}
	}
	seen := map[string]bool{}
	for _, a := range t.Attention {
		if a.Kind != AuthorityRequired || !boundedID(a.Key, true) || seen[a.Key] || !a.OwnerOnly || (a.Class != "unclassified" && a.Class != "owner-only") || (t.View.Activity != Approval && t.View.Activity != Unknown) {
			return errors.New("invalid_adapter_attention")
		}
		seen[a.Key] = true
	}
	if u := t.Usage; u != nil {
		if !d.Capabilities.TokenUsage || !boundedID(u.NativeSession, true) || u.NativeSession != t.View.NativeSession || !boundedID(u.NativeTurn, false) || u.InputTokens > 1<<60 || u.OutputTokens > 1<<60 || u.CacheReadTokens > 1<<60 {
			return errors.New("invalid_adapter_usage")
		}
	}
	return nil
}
