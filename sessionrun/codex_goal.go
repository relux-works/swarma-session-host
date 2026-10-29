package sessionrun

import (
	"context"
	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
	"strings"
)

var _ core.OwnerRunGoalBindingFactory = (*Runtime)(nil)

// BindOwnerRunGoal creates the real service's binding for an explicitly
// captured owner operation. It retains no expired delivery writer. Ordinary
// providers keep their existing goal binding and owner submission paths.
func (r *Runtime) BindOwnerRunGoal(ctx context.Context, c core.CapturedOwnerRun, q core.ProviderRunRequest) (core.OwnerKey, core.GoalBinding, error) {
	if err := validateCapture(c); err != nil {
		return "", nil, err
	}
	if err := r.requireCurrentCapture(ctx, c); err != nil {
		return "", nil, err
	}
	if c.Record.Provider != "codex" {
		return r.BindGoal(ctx, c.Lease, c.Record.Provider, c.Input)
	}
	if strings.TrimSpace(q.Prompt) == "" {
		return "", nil, core.ErrInvalidInput
	}
	if c.Record.MaterializationCtxID != "" {
		return "", nil, core.ErrHostCapabilityUnavailable
	}
	if _, ok := c.Native.(core.ProviderOwnerRunner); !ok {
		return "", nil, core.ErrHostCapabilityUnavailable
	}
	if _, err := codexPhysical(c); err != nil {
		return "", nil, err
	}
	r.mu.Lock()
	budgets := r.budgets
	r.mu.Unlock()
	if budgets == nil {
		return "", nil, core.ErrHostCapabilityUnavailable
	}
	return budgets.BindCodexGoal(ctx, c.Lease, c.Input, &codexRevisionFactory{runtime: r, capture: c, prompt: strings.TrimSpace(q.Prompt)})
}

func codexPhysical(c core.CapturedOwnerRun) (string, error) {
	raw, err := committedProvider(c)
	if err != nil {
		return "", err
	}
	record, err := core.NewLegacyRecordCodec().DecodeProviderHost(context.Background(), raw)
	if err != nil {
		return "", err
	}
	native, err := c.Native.Snapshot()
	if err != nil {
		return "", err
	}
	if record.Provider != "codex" || record.Snapshot.Codex == nil || native.Kind != "codex" || native.Codex == nil || native.Codex.ThreadID == "" || native.Codex.ThreadID != record.Snapshot.Codex.ThreadID {
		return "", core.ErrLegacyViewStale
	}
	return native.Codex.ThreadID, nil
}

// This is the managed owner renderer: without an owner prompt it can only
// defer without Submit. Selecting it for ordinary BindGoal still requires the
// original retained owner-run fact; public owner/task labels cannot select it.
// The explicit RunOwner entrypoint carries its actual prompt and captured cut.
type codexRevisionFactory struct {
	runtime *Runtime
	capture core.CapturedOwnerRun
	prompt  string
}
type codexRevisionDelivery struct {
	factory  *codexRevisionFactory
	revision goal.Revision
	attempt  *core.InputSubmission
}

func (f *codexRevisionFactory) DeliveryForRevision(revision goal.Revision) (core.GoalDelivery, error) {
	if f == nil || f.runtime == nil || revision.SessionID != f.capture.Lease.SessionID || revision.Namespace != goal.Assignment || revision.Ref.ID == "" || revision.Ref.Revision < 1 {
		return nil, goal.ErrBindingUnavailable
	}
	if _, _, err := goal.RenderProviderGoal(revision, "codex", "apply", revision.State != goal.Active, nil); err != nil {
		return nil, err
	}
	return &codexRevisionDelivery{factory: f, revision: revision}, nil
}
func (f *codexRevisionFactory) DeliveryForAttempt(revision goal.Revision, in core.InputSubmission) (core.GoalDelivery, error) {
	delivery, err := f.DeliveryForRevision(revision)
	if err != nil {
		return nil, err
	}
	d := delivery.(*codexRevisionDelivery)
	if in.Lease != f.capture.Lease || in.ID == "" || in.Payload.CodexTurn == nil || in.Payload.CodexTurn.Start == nil || in.Payload.Text != nil || in.Payload.MuseGoal != nil || in.Payload.MuseTurn != nil || in.Payload.ClaudeGoal != nil || in.Expected.Kind != "provider-ack" || in.Expected.Correlation != in.Payload.CodexTurn.CommandID {
		return nil, goal.ErrDeliveryUncertain
	}
	text, err := d.text("apply", revision.State != goal.Active)
	if err != nil {
		return nil, err
	}
	if in.Payload.CodexTurn.Start.Text != text {
		return nil, goal.ErrDeliveryUncertain
	}
	copy := in
	command := *in.Payload.CodexTurn
	start := *command.Start
	command.Start = &start
	copy.Payload.CodexTurn = &command
	d.attempt = &copy
	return d, nil
}
func (d *codexRevisionDelivery) text(phase string, clear bool) (string, error) {
	payload, _, err := goal.RenderProviderGoal(d.revision, "codex", phase, clear, nil)
	if err != nil {
		return "", err
	}
	if payload.CodexTurn == nil || payload.CodexTurn.Start == nil {
		return "", core.ErrHostCapabilityUnavailable
	}
	text := payload.CodexTurn.Start.Text
	if d.factory.prompt != "" {
		text += "\n\n" + d.factory.prompt
	}
	return text, nil
}
func (d *codexRevisionDelivery) ApplyGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	return d.deliver(ctx, e, l, w, "apply")
}
func (d *codexRevisionDelivery) RollbackGoal(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter) (core.InputObservation, error) {
	// A failed/uncertain owner start is not silently converted into another
	// assignment. The existing service retains its rollback obligation.
	return core.InputObservation{}, goal.ErrDeliveryUncertain
}
func (d *codexRevisionDelivery) deliver(ctx context.Context, e *core.GoalEnvelope, l core.ControllerLease, w core.HostInputWriter, phase string) (core.InputObservation, error) {
	f := d.factory
	if e == nil || l != f.capture.Lease || e.Owner != goal.Owner(l.SessionID, goal.Assignment) || e.Ref == nil || *e.Ref != d.revision.Ref || (d.revision.State != goal.Active) != e.Clear {
		return core.InputObservation{}, goal.ErrRevisionConflict
	}
	if err := f.runtime.requireCurrentCapture(ctx, f.capture); err != nil {
		return core.InputObservation{}, err
	}
	thread, err := codexPhysical(f.capture)
	if err != nil {
		return core.InputObservation{}, err
	}
	if d.attempt == nil {
		native, err := f.capture.Native.Snapshot()
		if err != nil {
			return core.InputObservation{}, err
		}
		if native.Codex == nil || native.Codex.ManagedTurnID != "" {
			return core.InputObservation{}, goal.ErrDeliveryUncertain
		}
	}
	// Original deferred queuing is valid only before a managed turn exists.
	if f.prompt == "" {
		return core.InputObservation{}, goal.ErrNativeDeferred
	}
	text, err := d.text(phase, e.Clear)
	if err != nil {
		return core.InputObservation{}, err
	}
	var in core.InputSubmission
	if d.attempt != nil {
		in = *d.attempt
	} else {
		id, err := commandID()
		if err != nil {
			return core.InputObservation{}, err
		}
		in = core.InputSubmission{Lease: l, ID: id, Payload: core.RenderedInput{CodexTurn: &core.CodexTurnCommand{CommandID: id, Start: &core.CodexTurnStart{Text: text}}}, Expected: core.AckExpectation{Kind: "provider-ack", Correlation: id}}
	}
	in, err = core.PrepareGoalInput(e, phase, in)
	if err != nil {
		return core.InputObservation{}, err
	}
	observed, err := submitCodexInput(ctx, w, l, thread, in)
	if err != nil {
		return observed, err
	}
	native, err := f.capture.Native.Snapshot()
	if err != nil {
		return observed, err
	}
	if native.Kind != "codex" || native.Codex == nil || native.Codex.ThreadID != thread || native.Codex.ManagedTurnID != observed.CodexTurn.TurnID {
		return observed, core.ErrInputAcknowledgementMismatch
	}
	f.runtime.mu.Lock()
	if f.runtime.codexAssignments == nil {
		f.runtime.codexAssignments = map[core.ControllerLease]codexAssignment{}
	}
	f.runtime.codexAssignments[l] = codexAssignment{ref: d.revision.Ref, prompt: f.prompt, thread: thread, observation: observed}
	f.runtime.mu.Unlock()
	return observed, nil
}

type codexAssignment struct {
	ref            core.RevisionRef
	prompt, thread string
	observation    core.InputObservation
}

func (r *Runtime) combinedAssignmentMatches(ctx context.Context, c core.CapturedOwnerRun, q core.ProviderRunRequest, thread, turn string) error {
	r.mu.Lock()
	a, ok := r.codexAssignments[c.Lease]
	r.mu.Unlock()
	if !ok || c.Record.AppliedGoalRef == nil || a.ref != *c.Record.AppliedGoalRef || a.prompt != strings.TrimSpace(q.Prompt) || a.thread != thread || a.observation.CodexTurn == nil || a.observation.CodexTurn.TurnID != turn {
		return core.ErrHostCapabilityUnavailable
	}
	// Source promotion is independent; this is only the host's retained physical
	// receipt, re-observed under the captured lease before joining its native turn.
	observed, err := c.Input.Await(ctx, a.observation.Receipt)
	if err != nil {
		return err
	}
	if observed.Receipt != a.observation.Receipt || observed.Kind != "provider-ack" || observed.Correlation != a.observation.Correlation || observed.CodexTurn == nil || *observed.CodexTurn != *a.observation.CodexTurn {
		return core.ErrInputAcknowledgementMismatch
	}
	return nil
}
