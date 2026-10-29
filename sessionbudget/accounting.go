package sessionbudget

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	goal "github.com/relux-works/swarma-session-host/sessiongoal"
	core "github.com/relux-works/swarma-session-host/sessionmanager"
)

// These are the sink's durable effect records, not a provider snapshot DTO or
// canonical goal journal. The codec-owned snapshot is hydrated separately.
type account struct {
	Lease           core.ControllerLease
	NativeID        string
	Serial          uint64
	Current         *allocation
	Ledger          map[string]*allocation
	Cumulative      *core.MuseRestoreUsage
	Pending         *attempt
	Recovery        *attempt
	Uncertain       bool
	Legacy          *core.LegacyMuseAccountingSnapshot
	Retired         map[string]bool
	SnapshotPending bool
}
type allocation struct {
	Ref           core.RevisionRef
	Objective     string
	Limit         int64
	Baseline      int64
	Spend         int64
	BudgetPaused  bool
	ExternalPause bool
	PauseCode     string
	PauseStatus   string
	NativeStatus  string
	Holder        string
	Generation    uint64
}
type attempt struct {
	Kind         string
	Target       *allocation
	Clear        bool
	Input        *core.InputSubmission
	Receipt      *core.InputReceipt
	Acknowledged *core.InputObservation
}

func (a *account) validate() error {
	if a.Legacy != nil && (a.Legacy.SessionID != a.NativeID || a.Legacy.GoalTransport != "native" || a.Legacy.TokenBudget < 0 || a.Legacy.TokenUsageTotal < 0 || a.Legacy.TokenUsageBaseline < 0 || a.Legacy.TokenUsageBaseline > a.Legacy.TokenUsageTotal) {
		return ErrAccountingUnavailable
	}
	if a.Ledger == nil {
		return ErrAccountingUnavailable
	}
	if a.Cumulative != nil && !validUsage(a.Cumulative) {
		return ErrAccountingUnavailable
	}
	for key, v := range a.Ledger {
		if v == nil || key != v.Objective || !validAllocation(v, a.Cumulative) {
			return ErrAccountingUnavailable
		}
	}
	if a.Current != nil && !validAllocation(a.Current, a.Cumulative) {
		return ErrAccountingUnavailable
	}
	for _, p := range []*attempt{a.Pending, a.Recovery} {
		if p == nil {
			continue
		}
		if p.Kind != "goal" && p.Kind != "pause" {
			return ErrAccountingUnavailable
		}
		if !p.Clear && (p.Target == nil || !validAllocation(p.Target, a.Cumulative)) {
			return ErrAccountingUnavailable
		}
		if p.Input != nil && (p.Input.Lease != a.Lease || p.Input.ID == "") {
			return ErrAccountingUnavailable
		}
		if p.Receipt != nil && (p.Input == nil || p.Receipt.ID != p.Input.ID || p.Receipt.SessionID != a.Lease.SessionID || p.Receipt.Epoch != a.Lease.Epoch) {
			return ErrAccountingUnavailable
		}
	}
	return nil
}
func validAllocation(v *allocation, u *core.MuseRestoreUsage) bool {
	return v.Ref.ID != "" && v.Ref.Revision > 0 && v.Objective != "" && v.Limit >= 0 && v.Baseline >= 0 && v.Spend >= 0 && (u == nil || v.Baseline <= u.TotalTokens) && !(v.BudgetPaused && v.ExternalPause)
}
func validUsage(u *core.MuseRestoreUsage) bool {
	return u != nil && u.OutputTokens >= 0 && u.PromptTokens >= 0 && u.TotalTokens >= 0
}
func accountUsage(a *account, u *core.MuseRestoreUsage) error {
	if !validUsage(u) {
		return ErrAccountingUnavailable
	}
	if old := a.Cumulative; old != nil && (u.TotalTokens < old.TotalTokens || u.OutputTokens < old.OutputTokens || u.PromptTokens < old.PromptTokens) {
		return ErrCounterRegression
	}
	copy := *u
	a.Cumulative = &copy
	return nil
}
func allocationUsage(a *account, v *allocation) int64 {
	if v == nil || a.Cumulative == nil || a.Cumulative.TotalTokens <= v.Baseline {
		return 0
	}
	return a.Cumulative.TotalTokens - v.Baseline
}
func copyAllocation(v *allocation) *allocation {
	if v == nil {
		return nil
	}
	copy := *v
	return &copy
}
func (b *binding) withAccount(ctx context.Context, fn func(*account) error) error {
	var refusal error
	err := b.store.transaction(ctx, true, func(j *journal) error {
		a := j.Sessions[b.lease.SessionID]
		if a == nil {
			return ErrAccountingUnavailable
		}
		if a.Lease != b.lease {
			return core.ErrStaleInputController
		}
		err := fn(a)
		if err == nil && a.Legacy != nil && !a.Uncertain {
			projected, e := projectLegacy(a)
			if e != nil {
				return e
			}
			a.SnapshotPending = !reflect.DeepEqual(*a.Legacy, projected)
		}
		if errors.Is(err, ErrCounterRegression) {
			a.Uncertain = true
			refusal = err
			return nil
		}
		return err
	})
	return errors.Join(err, refusal)
}
func (b *binding) requireAccount(ctx context.Context) error {
	return b.store.transaction(ctx, false, func(j *journal) error {
		a := j.Sessions[b.lease.SessionID]
		if a == nil {
			return ErrAccountingUnavailable
		}
		if a.Lease != b.lease {
			return core.ErrStaleInputController
		}
		if a.Uncertain {
			return ErrAccountingUncertain
		}
		if a.Legacy != nil && (b.prepareBudget == nil || b.commitBudget == nil) {
			return ErrAccountingUnavailable
		}
		return nil
	})
}
func (b *binding) markUncertain(ctx context.Context) error {
	clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return b.withAccount(clean, func(a *account) error { a.Uncertain = true; return nil })
}
func (b *binding) prepare(ctx context.Context, r goal.Revision, limit int64, o core.NativeGoalObservation) error {
	if o.SessionID != b.lease.SessionID || o.Epoch != b.lease.Epoch {
		return core.ErrStaleInputController
	}
	return b.withAccount(ctx, func(a *account) error {
		if a.Uncertain {
			return ErrAccountingUncertain
		}
		if e := accountUsage(a, o.Cumulative); e != nil {
			return e
		}
		clear := r.State != goal.Active
		var candidate *allocation
		if !clear {
			candidate = &allocation{Ref: r.Ref, Objective: goal.RevisionToken(r.Ref) + " " + strings.TrimSpace(r.Content.Condition), Limit: limit, Holder: r.Holder, Generation: r.HolderGeneration}
		}
		if p := a.Pending; p != nil {
			// The same exact native effect can be retried. Another lease/revision is
			// not a reset certificate; an explicit receipt-checked rollback is needed.
			if p.Kind != "goal" || p.Clear != clear || (!clear && (p.Target.Ref != r.Ref || p.Target.Objective != candidate.Objective || p.Target.Limit != limit)) {
				return ErrAccountingUncertain
			}
			return nil
		}
		if current := a.Current; current != nil && o.Objective != current.Objective {
			return ErrAccountingUncertain
		}
		if candidate != nil {
			if facts := a.Ledger[candidate.Objective]; facts != nil {
				candidate.Baseline = facts.Baseline
				candidate.BudgetPaused = facts.BudgetPaused
				candidate.ExternalPause = facts.ExternalPause
				candidate.PauseCode = facts.PauseCode
				candidate.PauseStatus = facts.PauseStatus
			}
			if a.Current == nil {
				candidate.Baseline = a.Cumulative.TotalTokens
			} else if a.Current.Ref.ID == r.Ref.ID {
				candidate.Baseline = a.Current.Baseline
			} else {
				candidate.Baseline = a.Cumulative.TotalTokens
				var spend int64
				for _, v := range a.Ledger {
					if v.Ref.ID == r.Ref.ID && v.Spend > spend {
						spend = v.Spend
					}
				}
				if spend > a.Cumulative.TotalTokens {
					return ErrAccountingUnavailable
				}
				candidate.Baseline = a.Cumulative.TotalTokens - spend
			}
			candidate.NativeStatus = "active"
		}
		if a.Recovery != nil && a.Recovery.Clear == clear && (clear || a.Recovery.Target.Ref == r.Ref) {
			a.Pending = a.Recovery
		} else {
			a.Pending = &attempt{Kind: "goal", Target: candidate, Clear: clear}
		}
		return nil
	})
}
func (b *binding) recordInput(ctx context.Context, r goal.Revision, i core.InputSubmission) error {
	return b.withAccount(ctx, func(a *account) error {
		p := a.Pending
		if p == nil || p.Kind != "goal" || (!p.Clear && (p.Target == nil || p.Target.Ref != r.Ref)) {
			return ErrAccountingUncertain
		}
		c := i.Payload.MuseGoal
		if c == nil || (p.Clear && c.Clear == nil) || (!p.Clear && !((c.Set != nil && c.Set.Objective == p.Target.Objective) || (c.Edit != nil && c.Edit.Objective == p.Target.Objective))) {
			return core.ErrInputAcknowledgementMismatch
		}
		if p.Input != nil && (p.Input.ID != i.ID || p.Input.Expected != i.Expected) {
			return ErrAccountingUncertain
		}
		copy := i
		p.Input = &copy
		return nil
	})
}
func (b *binding) recordReceipt(ctx context.Context, r core.InputReceipt) error {
	if r.SessionID != b.lease.SessionID || r.Epoch != b.lease.Epoch || r.ID == "" {
		return core.ErrInputAcknowledgementMismatch
	}
	return b.withAccount(ctx, func(a *account) error {
		if a.Pending == nil || a.Pending.Input == nil || a.Pending.Input.ID != r.ID {
			return core.ErrInputAcknowledgementMismatch
		}
		copy := r
		a.Pending.Receipt = &copy
		return nil
	})
}
func (b *binding) finish(ctx context.Context, r goal.Revision, limit int64, ack core.InputObservation, ackErr, effectErr error, possible bool) error {
	// Preserve a completed native effect even if caller cancellation follows its
	// ACK. This is local durable bookkeeping only, never fresh native I/O.
	clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return b.withAccount(clean, func(a *account) error {
		p := a.Pending
		if p == nil {
			if !possible {
				return nil
			}
			return ErrAccountingUncertain
		}
		if ackErr != nil {
			if !possible && p.Input == nil {
				a.Pending = nil
				return nil
			}
			return ErrAccountingUncertain
		}
		if p.Receipt == nil || *p.Receipt != ack.Receipt {
			return core.ErrInputAcknowledgementMismatch
		}
		copy := ack
		p.Acknowledged = &copy
		if old := a.Current; old != nil {
			saved := copyAllocation(old)
			saved.Spend = allocationUsage(a, old)
			a.Ledger[old.Objective] = saved
		}
		if p.Clear {
			if a.Current != nil {
				for key, v := range a.Ledger {
					if v.Ref.ID == a.Current.Ref.ID {
						delete(a.Ledger, key)
						if a.Retired == nil {
							a.Retired = map[string]bool{}
						}
						a.Retired[key] = true
					}
				}
			}
			a.Current = nil
		} else {
			a.Current = copyAllocation(p.Target)
			delete(a.Retired, a.Current.Objective)
			a.Ledger[a.Current.Objective] = copyAllocation(a.Current)
		}
		if effectErr != nil {
			a.Recovery = p
		} else {
			a.Recovery = nil
		}
		a.Pending = nil
		// A matched ACK permits accounting for what actually ran, but a callback
		// error still retains its acknowledged pending recovery obligation.
		return nil
	})
}

// Check re-reads current native state under the host's real writer fence. The
// feed is a wakeup, not authority to pause a cached or displaced goal.
func (b *binding) Check(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.check(ctx)
	if err == nil && b.commitBudget != nil {
		err = b.commitBudget(ctx, b.lease, b.store)
	}
	return err
}
func (b *binding) check(ctx context.Context) error {
	return b.observer.WithNativeGoal(ctx, b.lease, func(o core.NativeGoalObservation, w core.HostInputWriter) error {
		if o.SessionID != b.lease.SessionID || o.Epoch != b.lease.Epoch || o.Sequence == 0 || o.ObservedAt.IsZero() {
			return core.ErrStaleInputController
		}
		var input *core.InputSubmission
		e := b.withAccount(ctx, func(a *account) error {
			if a.Uncertain {
				return ErrAccountingUncertain
			}
			if e := accountUsage(a, o.Cumulative); e != nil {
				a.Uncertain = true
				return e
			}
			v := a.Current
			if v == nil {
				return nil
			}
			if o.Objective != v.Objective {
				return ErrAccountingUncertain
			}
			if o.Status != "active" {
				if o.Status == "paused" && !v.BudgetPaused {
					v.ExternalPause = true
				}
				return nil
			}
			v.NativeStatus = o.Status
			v.ExternalPause = false
			v.BudgetPaused = false
			if v.PauseCode == "budget_pause_not_applicable" {
				v.PauseCode = ""
			}
			if v.Limit == 0 || allocationUsage(a, v) < v.Limit {
				return nil
			}
			if p := a.Pending; p != nil {
				if p.Kind == "pause" {
					input = p.Input
					return nil
				}
				// A known ACK with a later journal error is still enforceable. A missing
				// ACK is never authority to pause a candidate or forgive prior spend.
				if p.Acknowledged == nil {
					return ErrAccountingUncertain
				}
				return ErrAccountingUncertain
			}
			if a.Serial == ^uint64(0) {
				return ErrAccountingUnavailable
			}
			a.Serial++
			candidate := makePauseInput(b.lease, a.Serial, o.Sequence)
			a.Pending = &attempt{Kind: "pause", Target: copyAllocation(v), Input: &candidate}
			input = &candidate
			return nil
		})
		if e != nil {
			return e
		}
		if input == nil {
			return nil
		}
		// A retained pause retry uses its exact stored submission/command identity.
		if input.Payload.MuseGoal == nil || input.Payload.MuseGoal.Pause == nil || input.Payload.MuseGoal.Pause.Sequence != o.Sequence {
			return ErrAccountingUncertain
		}
		receipt, e := w.Submit(ctx, *input)
		if receipt.ID != "" {
			clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			recordErr := b.recordReceipt(clean, receipt)
			cancel()
			if recordErr != nil {
				return errors.Join(e, recordErr)
			}
		}
		if e != nil {
			return e
		}
		ack, e := w.Await(ctx, receipt)
		if e != nil {
			return errors.Join(e, w.Cancel(ctx, receipt))
		}
		trace := &deliveryTrace{lease: b.lease, input: input, receipt: &receipt, ack: &ack}
		if _, e = trace.checked(); e != nil {
			return e
		}
		clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return b.withAccount(clean, func(a *account) error {
			if a.Pending == nil || a.Pending.Kind != "pause" || a.Pending.Receipt == nil || *a.Pending.Receipt != receipt || a.Current == nil || a.Current.Objective != o.Objective {
				return ErrAccountingUncertain
			}
			a.Current.BudgetPaused = true
			a.Current.ExternalPause = false
			a.Current.NativeStatus = "paused"
			a.Current.PauseCode = ""
			a.Ledger[a.Current.Objective] = copyAllocation(a.Current)
			a.Pending = nil
			return nil
		})
	})
}
