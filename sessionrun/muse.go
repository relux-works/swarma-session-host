package sessionrun

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	budget "github.com/relux-works/swarma-session-host/sessionbudget"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

type museEvidence struct {
	lease            core.ControllerLease
	native           string
	feed             core.MuseObservationFeed
	last             uint64
	cursor           string
	lastFold         *core.MuseRestoreFold
	liveSinceFold    bool
	state            core.LegacyMuseAccountingSnapshot
	finals           *finals
	promisedApproval string
	promisedModel    string
	tainted          bool
}

func (e *museEvidence) next(ctx context.Context) (core.MuseObservation, error) {
	v, err := e.feed.Next(ctx)
	if err != nil {
		return v, err
	}
	return e.admit(v)
}

func (e *museEvidence) admit(v core.MuseObservation) (core.MuseObservation, error) {
	if v.Gap != nil {
		return v, core.ErrMuseObservationUncertain
	}
	if v.Lease != e.lease || e.last == math.MaxUint64 || v.Sequence != e.last+1 || v.ObservedAt.IsZero() || (v.Event == nil) == (v.RestoreFold == nil) {
		return v, core.ErrMuseObservationInvalid
	}
	if v.RestoreFold != nil {
		f := v.RestoreFold
		if e.lastFold != nil && f.Cursor == e.lastFold.Cursor {
			if e.liveSinceFold || !sameRestoreFold(e.lastFold, f) {
				return v, core.ErrMuseObservationUncertain
			}
			e.last = v.Sequence
			return v, nil
		}
		next, err := restoreMuseAccounting(e.state, f)
		if err != nil {
			return v, err
		}
		for _, raw := range f.Items {
			var item agentMessage
			if json.Unmarshal(raw, &item) != nil {
				return v, core.ErrMuseObservationInvalid
			}
			if item.Kind == "agentMessage" {
				var shape struct {
					Status string `json:"status"`
				}
				if json.Unmarshal(raw, &shape) != nil {
					return v, core.ErrMuseObservationInvalid
				}
				if shape.Status == "completed" {
					if err := e.finals.apply(item); err != nil {
						return v, err
					}
				}
			}
		}
		e.state = next
		if f.EffectiveApprovalMode != "" && f.EffectiveApprovalMode != e.promisedApproval || f.EffectiveModelID != "" && e.promisedModel != "" && f.EffectiveModelID != e.promisedModel {
			e.tainted = true
		}
		e.lastFold = copyRestoreFold(f)
		e.cursor = f.Cursor
		e.liveSinceFold = false
	} else {
		e.liveSinceFold = true
	}
	e.last = v.Sequence
	return v, nil
}

func (r *Runtime) runMuse(ctx context.Context, c core.CapturedOwnerRun, q core.ProviderRunRequest) (result *core.ProviderRunResult, runErr error) {
	nativeSnapshot, snapshotErr := c.Native.Snapshot()
	if snapshotErr != nil {
		return nil, snapshotErr
	}
	if nativeSnapshot.Kind == "muse-pty" || nativeSnapshot.Kind == "muse-pty-provenance" {
		return r.runPTY(ctx, c, q)
	}
	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	source, state, e := r.museSource(ctx, c)
	if e != nil {
		return nil, e
	}
	observer, ok := c.Input.(core.NativeGoalObserver)
	if !ok {
		return nil, core.ErrHostCapabilityUnavailable
	}
	resolver, ok := c.Input.(core.MusePendingResolver)
	if !ok || r.pendingTimeout <= 0 {
		return nil, core.ErrHostCapabilityUnavailable
	}
	subscription, ok := c.Input.(core.MuseObservationSource)
	if !ok {
		return nil, core.ErrHostCapabilityUnavailable
	}
	feed, e := subscription.SubscribeMuse(ctx, c.Lease)
	if e != nil {
		return nil, e
	}
	defer feed.Close()
	evidence := &museEvidence{lease: c.Lease, native: state.SessionID, feed: feed, state: state, finals: newFinals()}
	native, e := c.Native.Snapshot()
	if e != nil {
		return nil, e
	}
	if native.Kind != "muse-msp" || native.MuseMSP == nil || native.MuseMSP.SessionID != state.SessionID {
		return nil, core.ErrLegacyViewStale
	}
	evidence.promisedApproval, evidence.promisedModel = native.MuseMSP.ApprovalMode, native.MuseMSP.ModelID
	initial, e := evidence.next(ctx)
	if e != nil {
		return nil, e
	}
	if initial.RestoreFold == nil {
		return nil, core.ErrMuseObservationInvalid
	}
	// Restore the committed field group through a genuinely current native
	// observer. A Fold is evidence, never authority to install a canonical goal.
	if e = source.HydrateBudget(ctx, c.Lease, observer, r.store); e != nil {
		return nil, e
	}
	if e = r.checkBudget(ctx, c.Lease); e != nil {
		return nil, e
	}
	defer func() {
		clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		runErr = errors.Join(runErr, source.CommitBudget(clean, c.Lease, r.store))
	}()
	id, e := commandID()
	if e != nil {
		return nil, e
	}
	ack, e := submitMuse(ctx, c.Input, c.Lease, core.MuseTurnCommand{CommandID: id, Start: &core.MuseTurnStart{Text: strings.TrimSpace(q.Prompt), IfBusy: "queue"}})
	if e != nil {
		return nil, e
	}
	turn := ack.MuseTurn.TurnID
	type readResult struct {
		value core.MuseObservation
		err   error
	}
	reads := make(chan readResult, 1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			v, e := feed.Next(ctx)
			select {
			case reads <- readResult{v, e}:
			case <-ctx.Done():
				return
			}
			if e != nil {
				return
			}
		}
	}()
	var workers sync.WaitGroup
	defer func() { cancelRun(); feed.Close(); <-readDone; workers.Wait() }()
	pending := make(chan pendingResult, 1)
	var epoch uint64
	resolving, again := false, false
	kick := func() {
		if resolving {
			again = true
			return
		}
		resolving = true
		at := epoch
		workers.Add(1)
		go func() {
			defer workers.Done()
			err := resolvePending(ctx, resolver, c.Lease, r.pendingTimeout)
			select {
			case pending <- pendingResult{at, err}:
			case <-ctx.Done():
			}
		}()
	}
	for {
		var v core.MuseObservation
		var e error
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case p := <-pending:
			resolving = false
			if p.epoch == epoch {
				if result := pendingAbortResult(p.err); result != nil {
					return result, nil
				}
			}
			if again {
				again = false
				kick()
			}
			continue
		case read := <-reads:
			v, e = read.value, read.err
		}
		if e == nil {
			v, e = evidence.admit(v)
		}
		if e != nil {
			if v.Gap != nil {
				return aborted("view_gap"), e
			}
			return nil, e
		}
		if v.RestoreFold != nil {
			continue
		}
		switch event := v.Event.(type) {
		case core.MSPTurnCompletedNotification:
			if event.SessionID != state.SessionID || event.TurnID != turn {
				continue
			}
			return r.finishMuse(ctx, c, evidence, event)
		case *core.MSPTurnCompletedNotification:
			if event == nil {
				return nil, core.ErrMuseObservationInvalid
			}
			if event.SessionID == state.SessionID && event.TurnID == turn {
				return r.finishMuse(ctx, c, evidence, *event)
			}
		case core.MSPTurnUnqueuedNotification:
			if event.SessionID == state.SessionID && event.TurnID == turn {
				return aborted("turn_unqueued"), nil
			}
		case *core.MSPTurnUnqueuedNotification:
			if event == nil {
				return nil, core.ErrMuseObservationInvalid
			}
			if event.SessionID == state.SessionID && event.TurnID == turn {
				return aborted("turn_unqueued"), nil
			}
		case core.MSPRawNotification:
			if result, e := evidence.raw(event, turn); result != nil || e != nil {
				return result, e
			}
		case *core.MSPRawNotification:
			if event == nil {
				return nil, core.ErrMuseObservationInvalid
			}
			if result, e := evidence.raw(*event, turn); result != nil || e != nil {
				return result, e
			}
		case core.MSPTurnStartedNotification, *core.MSPTurnStartedNotification,
			core.MSPTurnRetractedNotification, *core.MSPTurnRetractedNotification,
			core.MSPTurnRetryScheduledNotification, *core.MSPTurnRetryScheduledNotification,
			core.MSPSessionGoalChangedNotification, *core.MSPSessionGoalChangedNotification:
			// Native accounting is owned by the existing budget observer/binding.
		case core.MSPSessionTokenUsageNotification, *core.MSPSessionTokenUsageNotification:
			if !resolving {
				if err := r.checkBudget(ctx, c.Lease); err != nil {
					return nil, err
				}
			}
		case core.MSPSessionStatusChangedNotification:
			if event.SessionID != state.SessionID {
				continue
			}
			if event.Status == "notLoaded" {
				return aborted("session_not_loaded"), nil
			}
			needs := false
			for _, attention := range event.Attention {
				if attention == "approvalPending" || attention == "inputPending" {
					needs = true
				}
			}
			if needs {
				kick()
			} else {
				epoch++
			}
		case *core.MSPSessionStatusChangedNotification:
			if event == nil {
				return nil, core.ErrMuseObservationInvalid
			}
			if event.SessionID != state.SessionID {
				continue
			}
			if event.Status == "notLoaded" {
				return aborted("session_not_loaded"), nil
			}
			needs := false
			for _, attention := range event.Attention {
				if attention == "approvalPending" || attention == "inputPending" {
					needs = true
				}
			}
			if needs {
				kick()
			} else {
				epoch++
			}
		case core.MSPServerRequest:
			var params struct {
				SessionID string `json:"sessionId"`
			}
			if json.Unmarshal(event.Params, &params) == nil && params.SessionID == state.SessionID && (event.Name == "approval/request" || event.Name == "userInput/request") {
				kick()
			}
		case *core.MSPServerRequest:
			if event == nil {
				return nil, core.ErrMuseObservationInvalid
			}
			var params struct {
				SessionID string `json:"sessionId"`
			}
			if json.Unmarshal(event.Params, &params) == nil && params.SessionID == state.SessionID && (event.Name == "approval/request" || event.Name == "userInput/request") {
				kick()
			}
		default:
			return nil, core.ErrMuseObservationInvalid
		}
	}
}
func (e *museEvidence) raw(v core.MSPRawNotification, turn string) (*core.ProviderRunResult, error) {
	var envelope struct {
		SessionID string          `json:"sessionId"`
		Item      json.RawMessage `json:"item"`
	}
	if err := json.Unmarshal(v.Params, &envelope); err != nil {
		return nil, core.ErrMuseObservationInvalid
	}
	if envelope.SessionID != e.native {
		return nil, nil
	}
	switch v.Name {
	case "item/started", "item/updated":
		return nil, nil
	case "item/completed":
		if len(envelope.Item) == 0 || string(envelope.Item) == "null" {
			return nil, core.ErrMuseObservationInvalid
		}
		var item agentMessage
		if err := json.Unmarshal(envelope.Item, &item); err != nil {
			return nil, err
		}
		if item.Kind != "agentMessage" {
			return nil, nil
		}
		if err := e.finals.apply(item); err != nil {
			return nil, err
		}
	case "view/gap":
		return aborted("view_gap"), core.ErrMuseObservationUncertain
	case "session/closed":
		return aborted("session_closed"), nil
	case "session/deleteCompleted":
		return aborted("session_deleted"), nil
	case "session/approvalModeChanged":
		var value struct {
			Mode string `json:"mode"`
		}
		if json.Unmarshal(v.Params, &value) != nil {
			return nil, core.ErrMuseObservationInvalid
		}
		if value.Mode != "" && value.Mode != e.promisedApproval {
			e.tainted = true
		}
	case "session/modelChanged":
		var value struct {
			ModelID string `json:"modelId"`
		}
		if json.Unmarshal(v.Params, &value) != nil {
			return nil, core.ErrMuseObservationInvalid
		}
		if value.ModelID == "" || e.promisedModel != "" && value.ModelID != e.promisedModel {
			e.tainted = true
		}
	case "session/modelRouteUnserved":
		e.tainted = true
	case "session/viewHealthChanged":
		var value struct {
			Health string `json:"health"`
		}
		if json.Unmarshal(v.Params, &value) != nil {
			return nil, core.ErrMuseObservationInvalid
		}
		if value.Health == "unavailable" {
			return aborted("view_unavailable"), budget.ErrAccountingUncertain
		}
	default:
		return nil, core.ErrMuseObservationInvalid
	}
	return nil, nil
}

func (r *Runtime) finishMuse(ctx context.Context, c core.CapturedOwnerRun, evidence *museEvidence, event core.MSPTurnCompletedNotification) (*core.ProviderRunResult, error) {
	if err := r.checkBudget(ctx, c.Lease); err != nil {
		return nil, err
	}
	output, _ := evidence.finals.render(event.TurnID)
	result := museRunResult(event, output)
	if evidence.tainted {
		result.Status = core.ProviderRunFailed
		result.ProviderStatus = "item_malformed"
		result.Detail = "session approval mode or model diverges from the launch configuration"
		result.Recoverable = true
	}
	accounting, err := r.store.LegacySnapshot(ctx, c.Lease)
	if err != nil {
		return nil, err
	}
	if accounting.BudgetPauseOutcome != nil && accounting.BudgetPauseOutcome.Code == "budget_pause_failed" {
		result.Status = core.ProviderRunFailed
		result.ProviderStatus = "budget_pause_failed"
		result.Detail = "budget goal/pause was rejected or timed out while the goal is over budget"
		result.Recoverable = true
	}
	return result, nil
}
