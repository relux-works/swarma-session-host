package sessionrun

import (
	"context"
	"encoding/json"
	"fmt"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

// PTYDeliveryFactory retains only the actual provider's physical session ID.
// Host composition must obtain it from that provider under its captured lease;
// this constructor supplies no caller, holder or controller authority.
type PTYDeliveryFactory struct{ native string }

func NewPTYDeliveryFactory(nativeSession string) (*PTYDeliveryFactory, error) {
	if strings.TrimSpace(nativeSession) == "" {
		return nil, core.ErrHostCapabilityUnavailable
	}
	return &PTYDeliveryFactory{native: nativeSession}, nil
}

// DeliveryForRevision receives the full admitted stored revision from the goal
// service. The service retains journal/promotion ownership and passes a fresh
// checked writer for each invocation; no callback writer is retained here.
func (f *PTYDeliveryFactory) DeliveryForRevision(r goal.Revision) (core.GoalDelivery, error) {
	if f == nil || f.native == "" || r.SessionID == "" || r.Namespace != goal.Assignment {
		return nil, goal.ErrBindingUnavailable
	}
	if r.State != goal.Active && r.State != goal.Cleared && r.State != goal.Accepted {
		return nil, goal.ErrInvalidCommand
	}
	if r.State == goal.Active && (r.Ref.ID == "" || r.Ref.Revision <= 0 || strings.TrimSpace(r.Content.Condition) == "" || strings.TrimSpace(r.Content.Objective) == "" || r.Content.Budget.Unlimited && r.Content.Budget.Tokens != 0 || !r.Content.Budget.Unlimited && r.Content.Budget.Tokens <= 0) {
		return nil, goal.ErrInvalidCommand
	}
	return ptyGoalDelivery{revision: r, native: f.native}, nil
}

type ptyGoalDelivery struct {
	attempt  *core.InputSubmission
	revision goal.Revision
	native   string
}

func (d ptyGoalDelivery) ApplyGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	return d.deliver(ctx, e, l, w, "apply")
}
func (d ptyGoalDelivery) RollbackGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	return d.deliver(ctx, e, l, w, "rollback")
}

// DeliveryForAttempt resumes only the service's exact persisted Text input.
// Its data grants no authority; Apply/Rollback still requires the fresh lease,
// envelope phase, provider identity and current receipt-scoped host writer.
func (f *PTYDeliveryFactory) DeliveryForAttempt(r goal.Revision, in core.InputSubmission) (core.GoalDelivery, error) {
	delivery, err := f.DeliveryForRevision(r)
	if err != nil {
		return nil, err
	}
	d := delivery.(ptyGoalDelivery)
	text, err := d.text()
	if err != nil {
		return nil, err
	}
	p := in.Payload
	if in.Lease.SessionID != r.SessionID || in.Lease.Epoch == 0 || in.ID == "" || p.Text == nil || *p.Text != text || p.MuseGoal != nil || p.MuseTurn != nil || p.CodexTurn != nil || p.ClaudeGoal != nil || in.Expected.Kind != "run-observed-after-snapshot" || in.Expected.Correlation != f.native {
		return nil, goal.ErrDeliveryUncertain
	}
	value := *p.Text
	in.Payload.Text = &value
	d.attempt = &in
	return d, nil
}
func (d ptyGoalDelivery) text() (string, error) {
	r := d.revision
	text := "The session goal binding has been rolled back; reread the authoritative session goal before further work. End your turn while waiting, do not poll."
	if r.Ref.Revision > 0 {
		budget, err := json.Marshal(r.Content.Budget)
		if err != nil {
			return "", err
		}
		text = fmt.Sprintf("Session goal %s revision %d; state %s.\nProvider condition: %s\nObjective: %s\nBudget: %s\nEnd your turn while waiting, do not poll. If an external decision blocks progress, begin a line with BLOCKED: and name the required decision.", r.Ref.ID, r.Ref.Revision, r.State, r.Content.Condition, r.Content.Objective, budget)
	}
	return text, nil
}
func (d ptyGoalDelivery) deliver(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter, phase string) (core.InputObservation, error) {
	r := d.revision
	if e == nil || l.SessionID != r.SessionID || l.Epoch == 0 || e.Owner != goal.Owner(r.SessionID, r.Namespace) || (r.State != goal.Active) != e.Clear {
		return core.InputObservation{}, goal.ErrBindingUnavailable
	}
	if r.Ref.Revision > 0 {
		if e.Ref == nil || *e.Ref != r.Ref {
			return core.InputObservation{}, goal.ErrRevisionConflict
		}
	} else if phase != "rollback" || e.Ref != nil || !e.Clear {
		return core.InputObservation{}, goal.ErrInvalidCommand
	}
	text, err := d.text()
	if err != nil {
		return core.InputObservation{}, err
	}
	if w == nil || strings.ContainsAny(text, "\x1b\x00\r") {
		return core.InputObservation{}, core.ErrInvalidInput
	}
	id, err := commandID()
	if err != nil {
		return core.InputObservation{}, err
	}
	input := core.InputSubmission{Lease: l, ID: id, Payload: core.RenderedInput{Text: &text}, Expected: core.AckExpectation{Kind: "run-observed-after-snapshot", Correlation: d.native}}
	if d.attempt != nil {
		if d.attempt.Lease != l {
			return core.InputObservation{}, core.ErrStaleInputController
		}
		input = *d.attempt
	}
	originalID := input.ID
	input, err = core.PrepareGoalInput(e, phase, input)
	if err != nil {
		return core.InputObservation{}, err
	}
	if d.attempt != nil && input.ID != originalID {
		return core.InputObservation{}, goal.ErrDeliveryUncertain
	}
	receipt, err := w.Submit(ctx, input)
	if err != nil {
		return core.InputObservation{}, err
	}
	observed, err := w.Await(ctx, receipt)
	if err != nil {
		return observed, err
	}
	if receipt.SessionID != l.SessionID || receipt.Epoch != l.Epoch || receipt.ID != input.ID || observed.Receipt != receipt || observed.Kind != core.InputObservationKind(input.Expected.Kind) || observed.Correlation != d.native || !core.ValidPTYInputObservation(input, observed) {
		return observed, core.ErrInputAcknowledgementMismatch
	}
	return observed, nil
}
